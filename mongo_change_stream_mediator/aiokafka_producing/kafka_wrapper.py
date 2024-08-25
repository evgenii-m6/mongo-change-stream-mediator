from typing import Callable, Any
from aiokafka import AIOKafkaProducer
import asyncio


from .timeout import OperationTimeout


class KafkaClientNotRunning(Exception):
    ...


class KafkaClientCannotRestart(Exception):
    ...


class KafkaClient:
    """Not thread safe class. Should call all methods from one thread"""
    def __init__(
        self,
        producer_config: dict[str, Any],
        kafka_connect_timeout: float = 10.0,
        stop_flush_timeout: float = 5.0,
    ):
        self._producer_config = producer_config
        self._kafka_producer = AIOKafkaProducer(**self._producer_config)

        self._kafka_connect_timeout = kafka_connect_timeout
        self._stop_flush_timeout = stop_flush_timeout
        self._should_run = False
        self._not_stopped = True

    @property
    def _producer(self) -> AIOKafkaProducer:
        if not self._should_run*self._not_stopped:
            raise KafkaClientNotRunning("Kafka client stopped of doesn't work")
        return self._kafka_producer

    async def start(self):
        if not self._not_stopped:
            raise KafkaClientCannotRestart()

        if not self._should_run:
            try:
                await asyncio.wait_for(
                    self._kafka_producer.start(),
                    timeout=self._kafka_connect_timeout
                )
            except (asyncio.TimeoutError, asyncio.CancelledError) as ex:
                raise OperationTimeout(ex)
            else:
                self._should_run = True

    async def stop(self):
        self._not_stopped = False
        self._should_run = False
        try:
            await asyncio.wait_for(
                self._kafka_producer.stop(),
                timeout=self._stop_flush_timeout
            )
        except (asyncio.TimeoutError, asyncio.CancelledError) as ex:
            raise OperationTimeout(ex)

    async def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable
    ) -> asyncio.Future:
        future: asyncio.Future = await self._producer.send(
            topic=topic,
            key=key,
            value=value,
        )
        future.add_done_callback(on_delivery)
        return future

    async def flush(self, timeout: float):
        try:
            await asyncio.wait_for(
                self._producer.flush(),
                timeout=timeout
            )
        except (asyncio.TimeoutError, asyncio.CancelledError) as ex:
            ...

    async def list_topics(self) -> list[str]:
        try:
            cluster_metadata = await asyncio.wait_for(
                self._producer.client.fetch_all_metadata(),
                timeout=self._kafka_connect_timeout
            )
        except (asyncio.TimeoutError, asyncio.CancelledError) as ex:
            raise OperationTimeout(ex)
        else:
            topics = cluster_metadata.topics(exclude_internal_topics=True)
            return list(topics)
