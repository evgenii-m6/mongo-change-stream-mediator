import time
from concurrent.futures import Future
from threading import Thread
from unittest.mock import Mock, call

import pytest
from confluent_kafka.admin import ClusterMetadata, TopicMetadata
from confluent_kafka.cimpl import NewTopic, KafkaException

from mongo_change_stream_mediator.producing import Producer
from mongo_change_stream_mediator.producing.kafka_wrapper import \
    KafkaClientCannotRestart
from mongo_change_stream_mediator.producing.producer import ProducerNotRunningError, \
    ProducerError, ProducerTimeoutError, KafkaClientNotRestartable
from mongo_change_stream_mediator.producing.timeout import OperationTimeout
from mongo_change_stream_mediator.settings import NewTopicConfiguration


class KafkaClient:
    def __init__(self):
        self.create_topic_error = None
        self.produce = Mock(side_effect=self._produce)
        self.flush = Mock(side_effect=self._flush)
        self.create_topics = Mock(side_effect=self._create_topics)
        metadata = ClusterMetadata()
        topic = TopicMetadata()
        topic.topic = 'test'
        metadata.topics[topic.topic] = topic
        self.list_topics = Mock(return_value=metadata)
        self.produce_error = None
        self._flush_count = 0
        self._len = 10
        self.start = Mock()
        self.stop = Mock()

    def _create_topics(self, topics: list[NewTopic]) -> dict[str, Future]:
        result = {}
        for t in topics:
            f = Future()
            if self.create_topic_error:
                f.set_exception(self.create_topic_error)
            else:
                f.set_result(None)
            result[t.topic] = f
        return result

    def __len__(self):
        return self._len

    def _flush(self, timeout=None):
        self._flush_count += 1
        if isinstance(self.produce_error, BufferError) and self._flush_count >= 3:
            self.produce_error = None
            return len(self) - 1
        return len(self)

    def _produce(self, *args, **kwargs):
        if self.produce_error:
            raise self.produce_error


class KafkaError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


@pytest.fixture()
def kafka_client():
    return KafkaClient()


@pytest.fixture()
def producer_not_started(kafka_client):
    producer = Producer(
        task_id=1,
        kafka_client=kafka_client,
        new_topic_configuration=NewTopicConfiguration(
            new_topic_num_partitions=2,
            new_topic_replication_factor=2,
            new_topic_config={"test": "test"}
        )
    )

    try:
        yield producer
    finally:
        producer.stop()


@pytest.fixture()
def producer(producer_not_started):
    producer_not_started.start()
    return producer_not_started


def on_delivery(err, msg):
    ...


def test_start_stop(producer_not_started, kafka_client):
    kafka_client.start.assert_not_called()
    producer_not_started.start()
    kafka_client.start.assert_called_once()
    producer_not_started.start()
    assert len(kafka_client.start.call_args_list) == 2

    kafka_client.stop.assert_not_called()
    producer_not_started.stop()
    kafka_client.stop.assert_called_once()
    producer_not_started.stop()
    assert len(kafka_client.stop.call_args_list) == 2


def test_exit_gracefully(producer, kafka_client):
    producer.exit_gracefully()
    assert not producer._should_run
    kafka_client.stop.assert_not_called()


def test_methods_when_start_didnt_called(producer_not_started, kafka_client):
    with pytest.raises(ProducerNotRunningError):
        producer_not_started.create_topic("test")

    with pytest.raises(ProducerNotRunningError):
        producer_not_started.produce(
            topic='test', key=b'1', value=b'2', on_delivery=on_delivery
        )

    with pytest.raises(ProducerNotRunningError):
        producer_not_started.get_topics()


def test_create_topic_ok(producer, kafka_client: KafkaClient):
    producer.create_topic("test_1")
    kafka_client.create_topics.assert_called_once()
    kafka_client.create_topics.assert_called_once_with(
        [
            NewTopic(
                topic="test_1",
                num_partitions=2,
                replication_factor=2,
                config={"test": "test"},
            )
        ]
    )


def test_create_topic_already_exists(producer, kafka_client: KafkaClient):
    kafka_client.create_topic_error = KafkaException(KafkaError(36))
    producer.create_topic("test_1")
    kafka_client.create_topics.assert_called_once()
    kafka_client.create_topics.assert_called_once_with(
        [
            NewTopic(
                topic="test_1",
                num_partitions=2,
                replication_factor=2,
                config={"test": "test"},
            )
        ]
    )


def test_get_topics_ok(producer, kafka_client: KafkaClient):
    assert producer.get_topics() == ['test']


@pytest.mark.parametrize(
    "error, expected_error", [
        (KafkaException(KafkaError(10)), ProducerError),
        (OperationTimeout(), ProducerTimeoutError),
        (KafkaClientCannotRestart(), KafkaClientNotRestartable),
        (TimeoutError(), ProducerError),
        (BaseException(), ProducerError),
    ]
)
def test_handling_errors_in_kafka_client(
    producer, kafka_client: KafkaClient, error, expected_error
):
    kafka_client.list_topics.side_effect = error
    with pytest.raises(expected_error):
        assert producer.get_topics()

    kafka_client.create_topic_error = error
    with pytest.raises(expected_error):
        producer.create_topic("test_1")
    kafka_client.create_topics.assert_called_once()

    kafka_client.produce_error = error
    with pytest.raises(expected_error):
        producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_called_once()
    kafka_client.produce.assert_called_once_with(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )

    kafka_client.start.side_effect = error
    with pytest.raises(expected_error) as ex:
        producer.start()
    assert len(kafka_client.start.call_args_list) == 2

    kafka_client.stop.side_effect = error
    with pytest.raises(expected_error) as ex:
        producer.stop()
    kafka_client.stop.assert_called_once()

    kafka_client.stop = Mock()


def test_produce_ok(producer, kafka_client: KafkaClient):
    producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_called_once()
    kafka_client.produce.assert_called_once_with(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )


def test_produce_buffer_error(producer, kafka_client: KafkaClient):
    kafka_client.produce_error = BufferError()
    producer._wait_for_flush = 0.001

    producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)

    assert kafka_client.produce.call_args_list == [
        call(topic='test', key=b'1', value=b'2', on_delivery=on_delivery),
        call(topic='test', key=b'1', value=b'2', on_delivery=on_delivery),
    ]
    assert kafka_client.flush.call_args_list == [
        call(timeout=producer._wait_for_flush),
        call(timeout=producer._wait_for_flush),
        call(timeout=producer._wait_for_flush),
    ]


def test_stop_when_produce(producer, kafka_client: KafkaClient):
    producer._wait_for_flush = 0.001
    kafka_client.produce_error = BufferError()

    def flush(timeout):
        time.sleep(timeout)
        nonlocal is_ready_to_stop
        is_ready_to_stop = True
        return len(kafka_client)

    def stop():
        while not is_ready_to_stop:
            time.sleep(0.001)
        producer.stop()

    is_ready_to_stop = False
    kafka_client.flush.side_effect = flush

    t = Thread(target=stop, daemon=True)
    t.start()
    while not t.is_alive():
        time.sleep(0.001)

    kafka_client.stop.assert_not_called()
    kafka_client.flush.assert_not_called()
    kafka_client.produce.assert_not_called()

    producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_called_once()
    kafka_client.stop.assert_called_once()
    kafka_client.flush.assert_called()
    assert not producer._should_run
