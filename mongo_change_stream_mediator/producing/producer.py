import logging
from concurrent.futures import Future
from functools import wraps
from typing import Callable

from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import NewTopic, TopicMetadata

from mongo_change_stream_mediator.settings import NewTopicConfiguration
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
    def wrapped(self: 'Producer', *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
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
    def wrapped(self: 'Producer', *args, **kwargs):
        if not self._should_run:
            raise ProducerNotRunningError(
                f"Producer, task_id={self._task_id} is not running"
            )
        return func(self, *args, **kwargs)
    return wrapped


class Producer:
    """Not thread safe class. Should call all methods from one thread"""
    def __init__(
        self,
        task_id: int,
        kafka_client: KafkaClient,
        new_topic_configuration: NewTopicConfiguration,
    ):
        self._task_id = task_id
        self._new_topic_configuration = new_topic_configuration
        self._kafka_client = kafka_client
        self._wait_for_flush = 5.0
        self._should_run = False

    @_wrap_in_producer_exceptions
    def start(self):
        logging.info(f"Connecting to kafka, task_id={self._task_id}")
        self._kafka_client.start()
        self._should_run = True
        logging.info(f"Connected to kafka, task_id={self._task_id}")

    @_wrap_in_producer_exceptions
    def stop(self):
        logging.info(f"Disconnecting from kafka, task_id={self._task_id}")
        self._should_run = False
        self._kafka_client.stop()
        logging.info(f"Disconnected from kafka, task_id={self._task_id}")

    def exit_gracefully(self):
        logging.info(
            f"Stop producing new messages to kafka, task_id={self._task_id}"
        )
        self._should_run = False

    @_wrap_in_producer_exceptions
    @_check_status
    def create_topic(self, topic: str) -> None:
        logging.info(f"Creating topic {topic}, task_id={self._task_id}")
        replication_factor = self._new_topic_configuration.new_topic_replication_factor
        num_partitions = self._new_topic_configuration.new_topic_num_partitions
        config = self._new_topic_configuration.new_topic_config
        new_topic = NewTopic(
            topic=topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=config,
        )
        result: dict[str, Future] = self._kafka_client.create_topics([new_topic])
        try:
            result[topic].result()
            logging.info(f"Created topic {topic}")
        except KafkaException as ex:
            kafka_error: KafkaError = ex.args[0]
            if kafka_error.code() == 36:  # TOPIC_ALREADY_EXISTS
                logging.info(
                    f"Topic {topic} already exists, task_id={self._task_id}"
                )
            else:
                logging.error(
                    f"Error when create topic {topic}: {ex!r}, task_id={self._task_id}"
                )
                raise

    @_wrap_in_producer_exceptions
    @_check_status
    def produce(
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

                self._kafka_client.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery
                )
                break
            except BufferError as ex:
                before_flush = len(self._kafka_client)
                count += 1

                wait_count = 0
                while self._should_run:
                    logging.warning(
                        f"Producer's queue task_id={self._task_id} "
                        f"is overcrowded ({before_flush} messages in queue). "
                        f"Wait number {wait_count} for flush  {self._wait_for_flush}s."
                    )
                    after_flush = self._kafka_client.flush(
                        timeout=self._wait_for_flush
                    )
                    wait_count += 1
                    if after_flush < before_flush:
                        break

    @_wrap_in_producer_exceptions
    @_check_status
    def get_topics(self) -> list[str]:
        logging.info(f"Request topics from kafka, task_id={self._task_id}")
        topics: dict[str, TopicMetadata] = self._kafka_client.list_topics().topics
        result = list(topics.keys())
        logging.info(
            f"Got {len(result)} topics from kafka, task_id={self._task_id}"
        )
        return result
