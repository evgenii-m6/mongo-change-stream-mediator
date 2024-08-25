import asyncio
from collections import defaultdict
from copy import deepcopy
from unittest.mock import patch, Mock

import pytest

from mongo_change_stream_mediator.aiokafka_producing.kafka_wrapper import (
    KafkaClient,
    KafkaClientCannotRestart,
    KafkaClientNotRunning,
)
from mongo_change_stream_mediator.aiokafka_producing.timeout import OperationTimeout


@pytest.fixture()
def client_metadata():
    class ClientMetadata:
        _topics = {'test'}

        def topics(self, exclude_internal_topics=True):
            return deepcopy(self._topics)
    return ClientMetadata


@pytest.fixture()
def producer_client_mock(client_metadata):
    class ProducerClientMock:
        def __init__(self):
            self.fetch_all_metadata_error: Exception | None = None
            self.fetch_all_metadata_delay = 0.001
            self.called = defaultdict(int)

        async def fetch_all_metadata(self):
            self.called['fetch_all_metadata'] += 1

            await asyncio.sleep(self.fetch_all_metadata_delay)
            if self.fetch_all_metadata_error:
                raise self.fetch_all_metadata_error
            return client_metadata()

    return ProducerClientMock


@pytest.fixture()
def producer_mock(producer_client_mock):
    class ProducerMock:
        def __init__(self, *args, **kwargs: dict):
            self.args = args
            self.kwargs = kwargs
            self.called = defaultdict(int)
            self.send_mock = Mock()

            self.client = producer_client_mock()

            self.futures = set()

            self.send_delay = 0.001
            self.send_error: Exception | None = None

            self.flush_delay = 0.001
            self.flush_error: Exception | None = None

            self.start_delay = 0.001
            self.start_error: Exception | None = None

            self.stop_delay = 0.001
            self.stop_error: Exception | None = None

        async def start(self):
            self.called['start'] += 1
            await asyncio.sleep(self.start_delay)
            if self.start_error:
                raise self.start_error

        async def stop(self):
            self.called['stop'] += 1
            await asyncio.sleep(self.stop_delay)
            if self.stop_error:
                raise self.stop_error

        async def flush(self):
            self.called['flush'] += 1
            await asyncio.sleep(self.flush_delay)
            if self.flush_error:
                raise self.flush_error

        async def send(self, topic: str, key: bytes, value: bytes):
            self.called['send'] += 1
            self.send_mock(topic=topic,key=key,value=value)

            await asyncio.sleep(self.send_delay)
            if self.send_error:
                raise self.send_error
            loop = asyncio.get_event_loop()
            future = loop.create_future()
            self.futures.add(future)
            return future

    return ProducerMock


@pytest.fixture
def kafka_client(producer_mock) -> KafkaClient:
    with patch(
        target='mongo_change_stream_mediator.aiokafka_producing.kafka_wrapper.AIOKafkaProducer',
        new_callable=mock_wrapper(producer_mock)
    ):
        client = KafkaClient(
                producer_config={},
                kafka_connect_timeout=1.0,
                stop_flush_timeout=1.0,
            )
        try:
            yield client
        finally:
            ...


@pytest.fixture
async def kafka_client_started(kafka_client) -> KafkaClient:
    await kafka_client.start()
    try:
        yield kafka_client
    finally:
        await kafka_client.stop()
        assert not kafka_client._should_run
        assert not kafka_client._not_stopped


def mock_wrapper(mock):
    def wrapper(**kwargs):
        return mock
    return wrapper


async def test_producer_ok(kafka_client):
    on_delivery_called = 0

    def on_delivery(future):
        nonlocal on_delivery_called
        on_delivery_called += 1

    await kafka_client.start()
    await kafka_client.flush(1000)
    await kafka_client.list_topics()
    future = await kafka_client.produce(
        topic='test_topic', key=b'1', value=b'2', on_delivery=on_delivery
    )
    future.set_result(None)
    await kafka_client.stop()

    assert kafka_client._kafka_producer.called['start'] == 1
    assert kafka_client._kafka_producer.called['flush'] == 1
    assert kafka_client._kafka_producer.client.called['fetch_all_metadata'] == 1
    assert kafka_client._kafka_producer.called['send'] == 1
    kafka_client._kafka_producer.send_mock.assert_called_once_with(
        topic='test_topic', key=b'1', value=b'2'
    )
    assert kafka_client._kafka_producer.called['stop'] == 1
    assert on_delivery_called == 1


async def test_producer_error(kafka_client_started):
    kafka_client = kafka_client_started
    def on_delivery(future):
        ...

    exception = BaseException('test')

    kafka_client._kafka_producer.flush_error = exception
    kafka_client._kafka_producer.send_error = exception
    kafka_client._kafka_producer.client.fetch_all_metadata_error = exception


    with pytest.raises(BaseException):
        await kafka_client.flush(1000)
    with pytest.raises(BaseException):
        await kafka_client.produce(
            topic='test_topic', key=b'1', value=b'2', on_delivery=on_delivery
        )
    with pytest.raises(BaseException):
        await kafka_client.list_topics()

    assert kafka_client._kafka_producer.called['flush'] == 1
    assert kafka_client._kafka_producer.client.called['fetch_all_metadata'] == 1
    assert kafka_client._kafka_producer.called['send'] == 1
    kafka_client._kafka_producer.send_mock.assert_called_once_with(
        topic='test_topic', key=b'1', value=b'2'
    )


async def test_producer_timeout(kafka_client_started):
    kafka_client = kafka_client_started
    kafka_client._kafka_connect_timeout = 0.0000001
    kafka_client._stop_flush_timeout = 0.0000001
    def on_delivery(future):
        ...


    kafka_client._kafka_producer.flush_delay = 0.1
    kafka_client._kafka_producer.send_delay = 0.1
    kafka_client._kafka_producer.stop_delay = 0.1
    kafka_client._kafka_producer.client.fetch_all_metadata_delay = 0.1

    await kafka_client.flush(timeout=0.0000001)
    await kafka_client.produce(
        topic='test_topic', key=b'1', value=b'2', on_delivery=on_delivery
    )
    with pytest.raises(OperationTimeout):
        await kafka_client.list_topics()
    with pytest.raises(OperationTimeout):
        await kafka_client.stop()

    assert kafka_client._kafka_producer.called['flush'] == 1
    assert kafka_client._kafka_producer.called['stop'] == 1
    assert kafka_client._kafka_producer.client.called['fetch_all_metadata'] == 1
    assert kafka_client._kafka_producer.called['send'] == 1
    kafka_client._kafka_producer.send_mock.assert_called_once_with(
        topic='test_topic', key=b'1', value=b'2'
    )

    kafka_client._stop_flush_timeout = 1000
    assert not kafka_client._should_run
    assert not kafka_client._not_stopped


async def test_start_error(kafka_client):
    kafka_client._kafka_connect_timeout = 0.0000001
    kafka_client._kafka_producer.start_error = BaseException('test')

    with pytest.raises(BaseException):
        await kafka_client.start()

    assert kafka_client._kafka_producer.called['start'] == 1
    assert not kafka_client._should_run


async def test_start_timeout(kafka_client):
    kafka_client._kafka_connect_timeout = 0.0000001
    kafka_client._kafka_producer.start_delay = 0.1

    with pytest.raises(OperationTimeout):
        await kafka_client.start()

    assert kafka_client._kafka_producer.called['start'] == 1
    assert not kafka_client._should_run


async def test_start_stop_twice(kafka_client):
    await kafka_client.start()
    assert kafka_client._should_run
    assert kafka_client._not_stopped
    await kafka_client.start()
    assert kafka_client._should_run
    assert kafka_client._not_stopped
    await kafka_client.stop()
    assert not kafka_client._should_run
    assert not kafka_client._not_stopped
    await kafka_client.stop()
    assert not kafka_client._should_run
    assert not kafka_client._not_stopped

    assert kafka_client._kafka_producer.called['start'] == 1
    assert kafka_client._kafka_producer.called['stop'] == 2

async def test_restart(kafka_client):
    await kafka_client.start()
    assert kafka_client._should_run
    assert kafka_client._not_stopped
    await kafka_client.stop()
    assert not kafka_client._should_run
    assert not kafka_client._not_stopped

    with pytest.raises(KafkaClientCannotRestart):
        await kafka_client.start()

    assert kafka_client._kafka_producer.called['start'] == 1
    assert kafka_client._kafka_producer.called['stop'] == 1
    assert not kafka_client._should_run
    assert not kafka_client._not_stopped


async def test_stop_not_started(kafka_client):
    assert not kafka_client._should_run
    assert kafka_client._not_stopped
    await kafka_client.stop()
    assert not kafka_client._should_run
    assert not kafka_client._not_stopped

    with pytest.raises(KafkaClientCannotRestart):
        await kafka_client.start()

    assert not kafka_client._should_run
    assert not kafka_client._not_stopped


async def test_use_not_started(kafka_client):
    def on_delivery(future):
        ...

    with pytest.raises(KafkaClientNotRunning):
        await kafka_client.flush(timeout=1000)
    with pytest.raises(KafkaClientNotRunning):
        await kafka_client.produce(
            topic='test_topic', key=b'1', value=b'2', on_delivery=on_delivery
        )
    with pytest.raises(KafkaClientNotRunning):
        await kafka_client.list_topics()

    assert kafka_client._kafka_producer.called['flush'] == 0
    assert kafka_client._kafka_producer.called['stop'] == 0
    assert kafka_client._kafka_producer.client.called['fetch_all_metadata'] == 0
    assert kafka_client._kafka_producer.called['send'] == 0
