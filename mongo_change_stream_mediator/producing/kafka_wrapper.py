import logging
from concurrent.futures import Future
from threading import Thread
from typing import Callable

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata

from .timeout import WithTimeout


class KafkaClientNotRunning(Exception):
    ...


class KafkaClientCannotRestart(Exception):
    ...


class KafkaClient:
    """Not thread safe class. Should call all methods from one thread"""
    def __init__(
        self,
        producer_config: dict[str, str],
        admin_config: dict[str, str],
        kafka_connect_timeout: float = 10.0,
        poll_timeout: float = 5.0,
        stop_flush_timeout: float = 5.0,
    ):
        self._producer_config = producer_config
        self._admin_config = admin_config
        self._kafka_connect_timeout = kafka_connect_timeout
        self._kafka_producer: Producer | None = None
        self._kafka_admin: AdminClient | None = None
        self._poll_thread: Thread | None = None
        self._poll_timeout = poll_timeout
        self._stop_flush_timeout = stop_flush_timeout
        self._should_run = False
        self._not_stopped = True

    @property
    def _producer(self) -> Producer:
        if not self._should_run*self._not_stopped:
            raise KafkaClientNotRunning("Kafka client stopped of doesn't work")
        return self._kafka_producer

    @property
    def _admin(self) -> AdminClient:
        if not self._should_run*self._not_stopped:
            raise KafkaClientNotRunning("Kafka client stopped of doesn't work")
        return self._kafka_admin

    def start(self):
        if not self._not_stopped:
            raise KafkaClientCannotRestart()

        if not self._should_run:
            self._kafka_producer = Producer(self._producer_config)
            self._kafka_admin = AdminClient(self._admin_config)
            WithTimeout[ClusterMetadata](
                target=self._kafka_producer.list_topics,
                timeout=self._kafka_connect_timeout
            ).run()
            WithTimeout[ClusterMetadata](
                target=self._kafka_admin.list_topics,
                timeout=self._kafka_connect_timeout
            ).run()
            self._should_run = True
            self._poll_thread = Thread(target=self._poll_loop, daemon=True)
            self._poll_thread.start()

    def stop(self):
        self._not_stopped = False
        self._should_run = False
        if self._poll_thread is not None and self._poll_thread.is_alive():
            try:
                self._poll_thread.join(self._poll_timeout*2)
            except RuntimeError:
                ...
        if self._kafka_producer is not None:
            self._kafka_producer.flush(self._stop_flush_timeout)

    def create_topics(self, new_topics: list[NewTopic]) -> dict[str, Future]:
        def create_new_topics():
            return self._admin.create_topics(new_topics)

        return WithTimeout[dict[str, Future]](
            target=create_new_topics,
            timeout=self._kafka_connect_timeout
        ).run()

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable
    ) -> None:
        return self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=on_delivery
        )

    def __len__(self):
        return len(self._producer)

    def flush(self, timeout: float | None = None) -> int:
        return self._producer.flush(timeout=timeout)

    def _poll_loop(self):
        while self._should_run:
            try:
                self._producer.poll(timeout=self._poll_timeout)
            except BaseException as ex:
                self._not_stopped = False
                self._should_run = False
                logging.error(
                    f"Error in pool loop. Kafka client should not run. Error: {ex!r}"
                )

    def list_topics(self) -> ClusterMetadata:
        return WithTimeout[ClusterMetadata](
            target=self._producer.list_topics,
            timeout=self._kafka_connect_timeout
        ).run()
