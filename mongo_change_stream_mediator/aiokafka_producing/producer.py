import asyncio
import logging
from functools import wraps
from typing import Callable

import aiokafka

from .kafka_wrapper import KafkaClient, KafkaClientNotRunning, KafkaClientCannotRestart
from .timeout import OperationTimeout


class BaseProducerError(Exception):
    def __str__(self):
        return repr(self)


class ProducerError(BaseProducerError):
    def __init__(self, exception):
        self.exception = exception


class ProducerTimeoutError(BaseProducerError):
    def __init__(self, exception):
        self.exception = exception


class ProducerNotRunningError(BaseProducerError):
    ...


class KafkaClientNotRunningError(ProducerNotRunningError):
    ...


class KafkaClientNotRestartable(ProducerNotRunningError):
    ...


def _wrap_in_producer_exceptions(func: Callable):
    @wraps(func)
    async def wrapped(self: 'Producer', *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except BaseProducerError:
            raise
        except OperationTimeout as ex:
            raise ProducerTimeoutError(ex)
        except KafkaClientCannotRestart as ex:
            raise KafkaClientNotRestartable()
        except KafkaClientNotRunning as ex:
            raise KafkaClientNotRunningError from ex
        except BaseException as ex:
            raise ProducerError(ex)
    return wrapped


def _check_status(func: Callable):
    @wraps(func)
    async def wrapped(self: 'Producer', *args, **kwargs):
        if not self._should_run:
            raise ProducerNotRunningError(
                f"Producer, task_id={self._task_id} is not running"
            )
        return await func(self, *args, **kwargs)
    return wrapped


class Producer:
    """Not thread safe class. Should call all methods from one thread"""
    def __init__(
        self,
        task_id: int,
        kafka_client: KafkaClient,
    ):
        self._task_id = task_id
        self._kafka_client = kafka_client
        self._wait_for_flush = 5.0
        self._should_run = False
        self._message_accumulator = set()

    @_wrap_in_producer_exceptions
    async def start(self):
        logging.info(f"Connecting to kafka, task_id={self._task_id}")
        await self._kafka_client.start()
        self._should_run = True
        logging.info(f"Connected to kafka, task_id={self._task_id}")

    @_wrap_in_producer_exceptions
    async def stop(self):
        logging.info(f"Disconnecting from kafka, task_id={self._task_id}")
        self._should_run = False
        await self._kafka_client.stop()
        logging.info(f"Disconnected from kafka, task_id={self._task_id}")

    def exit_gracefully(self):
        logging.info(
            f"Stop producing new messages to kafka, task_id={self._task_id}"
        )
        self._should_run = False

    @_wrap_in_producer_exceptions
    @_check_status
    async def create_topic(self, topic: str) -> None:
        raise NotImplementedError

    @_wrap_in_producer_exceptions
    @_check_status
    async def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable
    ) -> None:
        count = 0
        while self._should_run:
            try:
                if count > 0:
                    logging.warning(
                        f"Producer retry number {count} to "
                        f"send message, task_id={self._task_id}"
                    )

                future = await self._kafka_client.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery
                )
                self._message_accumulator.add(future)
                future.add_done_callback(self._message_accumulator.discard)
                break
            except aiokafka.errors.KafkaTimeoutError as ex:
                before_flush = len(self._message_accumulator)
                count += 1

                if before_flush == 0:
                    logging.warning(
                        f"Producer's queue task_id={self._task_id} "
                        f"got timeout error with empty buffer."
                    )
                    await asyncio.sleep(self._wait_for_flush)
                    continue

                wait_count = 0
                while self._should_run:
                    logging.warning(
                        f"Producer's queue task_id={self._task_id} "
                        f"is overcrowded ({before_flush} messages in queue). "
                        f"Wait number {wait_count} for flush  {self._wait_for_flush}s."
                    )

                    await self._kafka_client.flush(
                        timeout=self._wait_for_flush
                    )
                    after_flush = len(self._message_accumulator)
                    wait_count += 1
                    if after_flush < before_flush:
                        break

    @_wrap_in_producer_exceptions
    @_check_status
    async def get_topics(self) -> list[str]:
        logging.info(f"Request topics from kafka, task_id={self._task_id}")
        topics: list[str] = await self._kafka_client.list_topics()
        logging.info(
            f"Got {len(topics)} topics from kafka, task_id={self._task_id}"
        )
        return topics
