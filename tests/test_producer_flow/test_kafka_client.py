import time
from concurrent.futures import Future
from collections import defaultdict

import pytest
from unittest.mock import patch

from confluent_kafka.admin import ClusterMetadata, TopicMetadata
from confluent_kafka.cimpl import NewTopic, KafkaException

from mongo_change_stream_mediator.producing.kafka_wrapper import KafkaClient, \
    KafkaClientNotRunning, KafkaClientCannotRestart
from mongo_change_stream_mediator.producing.timeout import OperationTimeout


@pytest.fixture()
def cluster_metadata():
    data = ClusterMetadata()
    data.topics = {'test': TopicMetadata()}
    return data


@pytest.fixture()
def producer_mock(cluster_metadata):
    class ProducerMock:
        def __init__(self, config: dict):
            self.cluster_metadata = cluster_metadata
            self.config = config
            self.called = defaultdict(int)

        def _list_topics(self):
            ...

        def list_topics(self):
            self.called['list_topics'] += 1
            self._list_topics()
            return self.cluster_metadata

        def _poll(self):
            ...

        def poll(self, timeout=None):
            self.called['poll'] += 1
            self._poll()
            time.sleep(0.001)

        def flush(self, timeout=None):
            self.called['flush'] += 1
            return len(self)

        def produce(self, topic, key, value, on_delivery):
            self.called['produce'] += 1
            return None

        def __len__(self):
            self.called['__len__'] += 1
            return 0

    return ProducerMock


@pytest.fixture()
def admin_client_mock(cluster_metadata):
    class AdminClientMock:
        def __init__(self, config: dict):
            self.cluster_metadata = cluster_metadata
            self.config = config
            self.called = defaultdict(int)

        def _list_topics(self):
            ...

        def list_topics(self):
            self.called['list_topics'] += 1
            self._list_topics()
            return self.cluster_metadata

        def _create_topics(self):
            ...

        def create_topics(self, new_topics: list[NewTopic], **kwargs):
            self.called['list_topics'] += 1
            self._create_topics()
            return {i.topic: Future() for i in new_topics}

    return AdminClientMock


def mock_wrapper(mock):
    def wrapper(**kwargs):
        return mock
    return wrapper


@pytest.fixture
def kafka_client(cluster_metadata, producer_mock, admin_client_mock):
    with patch(
        target='mongo_change_stream_mediator.producing.kafka_wrapper.AdminClient',
        new_callable=mock_wrapper(admin_client_mock)
    ), patch(
        target='mongo_change_stream_mediator.producing.kafka_wrapper.Producer',
        new_callable=mock_wrapper(producer_mock)
    ):
        client = KafkaClient(
                producer_config={},
                admin_config={},
                kafka_connect_timeout=1,
            )
        try:
            yield client
        finally:
            client.stop()


@pytest.fixture
def started_kafka_client(kafka_client) -> KafkaClient:
    kafka_client.start()
    return kafka_client


def test_start_ok(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    kafka_client = started_kafka_client
    assert kafka_client._kafka_producer is not None
    assert kafka_client._kafka_admin is not None

    assert kafka_client._producer.called['list_topics'] == 1
    assert kafka_client._admin.called['list_topics'] == 1


    assert kafka_client._kafka_producer.called['list_topics'] == 1
    assert kafka_client._kafka_admin.called['list_topics'] == 1

    assert kafka_client._should_run
    assert kafka_client._poll_thread.is_alive()


def test_start_producer_start_timeout(
    kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    kafka_client._kafka_connect_timeout = 0.001

    def _list_topics(*args, **kwargs):
        time.sleep(10)

    producer_mock._list_topics = _list_topics

    with pytest.raises(OperationTimeout) as err:
        kafka_client.start()

    assert kafka_client._kafka_producer.called['list_topics'] == 1
    assert kafka_client._kafka_admin.called['list_topics'] == 0

    assert kafka_client._kafka_producer is not None
    assert kafka_client._kafka_admin is not None

    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._producer

    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._admin

    assert not kafka_client._should_run
    assert not kafka_client._poll_thread


def test_start_admin_client_start_timeout(
    kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    kafka_client._kafka_connect_timeout = 0.001

    def _list_topics(*args, **kwargs):
        time.sleep(10)

    admin_client_mock._list_topics = _list_topics

    with pytest.raises(OperationTimeout) as err:
        kafka_client.start()

    assert kafka_client._kafka_producer.called['list_topics'] == 1
    assert kafka_client._kafka_admin.called['list_topics'] == 1

    assert kafka_client._kafka_producer is not None
    assert kafka_client._kafka_admin is not None

    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._producer

    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._admin

    assert not kafka_client._should_run
    assert not kafka_client._poll_thread


def test_start_twice(started_kafka_client):
    started_kafka_client.start()
    kafka_client = started_kafka_client

    assert kafka_client._kafka_producer is not None
    assert kafka_client._kafka_admin is not None

    assert kafka_client._producer.called['list_topics'] == 1
    assert kafka_client._admin.called['list_topics'] == 1


    assert kafka_client._kafka_producer.called['list_topics'] == 1
    assert kafka_client._kafka_admin.called['list_topics'] == 1

    assert kafka_client._should_run
    assert kafka_client._poll_thread.is_alive()


def test_stop_not_started(kafka_client):
    assert not kafka_client._poll_thread

    kafka_client.stop()
    assert not kafka_client._kafka_admin
    assert not kafka_client._kafka_producer
    assert not kafka_client._poll_thread

    kafka_client.stop()
    assert not kafka_client._kafka_admin
    assert not kafka_client._kafka_producer
    assert not kafka_client._poll_thread


def test_stop_started(started_kafka_client):
    kafka_client = started_kafka_client

    kafka_client.stop()
    assert kafka_client._kafka_producer.called['flush'] == 1
    kafka_client.stop()

    assert kafka_client._kafka_producer is not None
    assert kafka_client._kafka_admin is not None

    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._producer
    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._admin

    assert kafka_client._kafka_producer.called['list_topics'] == 1
    assert kafka_client._kafka_admin.called['list_topics'] == 1
    assert kafka_client._kafka_producer.called['flush'] == 2

    assert not kafka_client._should_run
    assert not kafka_client._poll_thread.is_alive()


def test_restart_error(started_kafka_client):
    started_kafka_client.stop()
    with pytest.raises(KafkaClientCannotRestart):
        started_kafka_client.start()


def test_error_in_poll(
    kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    should_raise = False
    def _poll(*args, **kwargs):
        if should_raise:
            raise Exception()

    producer_mock._poll = _poll

    kafka_client.start()

    assert kafka_client._kafka_producer is not None
    assert kafka_client._kafka_admin is not None

    assert kafka_client._producer.called['list_topics'] == 1
    assert kafka_client._admin.called['list_topics'] == 1
    assert kafka_client._kafka_producer.called['list_topics'] == 1
    assert kafka_client._kafka_admin.called['list_topics'] == 1

    assert kafka_client._should_run
    assert kafka_client._poll_thread.is_alive()

    should_raise = True
    while kafka_client._poll_thread.is_alive():
        time.sleep(0.005)

    assert not kafka_client._should_run

    assert kafka_client._kafka_producer is not None
    assert kafka_client._kafka_admin is not None

    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._producer
    with pytest.raises(KafkaClientNotRunning):
        assert kafka_client._admin
    with pytest.raises(KafkaClientCannotRestart):
        assert kafka_client.start()


def test_flush(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    started_kafka_client.start()
    assert 'flush' not in started_kafka_client._producer.called
    started_kafka_client.flush()
    assert started_kafka_client._producer.called['flush'] == 1


def test_not_started_calls(
    kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    with pytest.raises(KafkaClientNotRunning):
        kafka_client.flush()

    with pytest.raises(KafkaClientNotRunning):
        kafka_client.list_topics()

    with pytest.raises(KafkaClientNotRunning):
        kafka_client.create_topics([NewTopic(topic='test')])

    with pytest.raises(KafkaClientNotRunning):
        kafka_client.produce(key=b'', value=b'', topic='test', on_delivery=lambda: None)

    with pytest.raises(KafkaClientNotRunning):
        len(kafka_client)


def test_create_topic_ok(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    assert started_kafka_client.create_topics([NewTopic(topic='test')]).keys() == {
        'test': Future()
    }.keys()


def test_create_topic_error(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    def _create_topics(*args, **kwargs):
        raise KafkaException

    admin_client_mock._create_topics = _create_topics
    with pytest.raises(KafkaException):
        started_kafka_client.create_topics([NewTopic(topic='test')])


def test_create_topic_timeout(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    def _create_topics(*args, **kwargs):
        time.sleep(0.01)

    started_kafka_client._kafka_connect_timeout = 0.001

    admin_client_mock._create_topics = _create_topics
    with pytest.raises(OperationTimeout):
        started_kafka_client.create_topics([NewTopic(topic='test')])


def test_list_topics_ok(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    assert started_kafka_client.list_topics() == cluster_metadata


def test_list_topics_error(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    def _list_topics(*args, **kwargs):
        raise KafkaException()

    producer_mock._list_topics = _list_topics

    with pytest.raises(KafkaException) as err:
        started_kafka_client.list_topics()


def test_list_topics_timeout(
    started_kafka_client, cluster_metadata, producer_mock, admin_client_mock
):
    started_kafka_client._kafka_connect_timeout = 0.001

    def _list_topics(*args, **kwargs):
        time.sleep(0.01)

    producer_mock._list_topics = _list_topics

    with pytest.raises(OperationTimeout) as err:
        started_kafka_client.list_topics()
