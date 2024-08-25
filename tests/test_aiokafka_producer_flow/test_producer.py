import asyncio
import time
from threading import Thread
from unittest.mock import call, AsyncMock

import aiokafka
import pytest

from mongo_change_stream_mediator.aiokafka_producing import Producer
from mongo_change_stream_mediator.aiokafka_producing.kafka_wrapper import (
    KafkaClientCannotRestart,
)
from mongo_change_stream_mediator.aiokafka_producing.producer import (
    ProducerNotRunningError,
    ProducerError,
    ProducerTimeoutError,
    KafkaClientNotRestartable,
)
from mongo_change_stream_mediator.aiokafka_producing.timeout import OperationTimeout


class KafkaClient:
    def __init__(self):
        self.create_topic_error = None
        self.start = AsyncMock(side_effect=self._start)
        self.stop = AsyncMock(side_effect=self._stop)
        self.produce = AsyncMock(side_effect=self._produce)
        self.flush = AsyncMock(side_effect=self._flush)
        self.list_topics = AsyncMock(side_effect=self._list_topics)
        self._flush_count = 0
        self._produce_error = None
        self._is_permanent_error = False
        self._saved_futures: list[asyncio.Future] = list()

    def recreate_produce_mock(self):
        self.produce = AsyncMock(side_effect=self._produce)

    async def _start(self):
        ...

    async def _stop(self):
        ...

    async def _list_topics(self):
        return ['test', 'test-topic', 'another-topic', 'test.topic']

    async def _flush(self, timeout: float):
        self._flush_count += 1
        if self._flush_count >= 3:
            if self._saved_futures:
                f = self._saved_futures.pop()
                f.set_result(None)
                await asyncio.sleep(timeout)
        else:
            await asyncio.sleep(timeout)

    async def _produce(self, topic, key, value, on_delivery) -> asyncio.Future:
        if not self._produce_error:
            f = asyncio.get_event_loop().create_future()
            self._saved_futures.append(f)
            await asyncio.sleep(0.001)
            return f
        else:
            await asyncio.sleep(0.001)
            if not self._is_permanent_error:
                error = self._produce_error
                self._produce_error = None
                raise error
            else:
                raise self._produce_error

class KafkaError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


@pytest.fixture()
def kafka_client():
    return KafkaClient()


@pytest.fixture()
def producer_not_started(kafka_client) -> Producer:
    producer = Producer(
        task_id=1,
        kafka_client=kafka_client,
    )

    try:
        yield producer
    finally:
        ...


@pytest.fixture()
async def producer(producer_not_started) -> Producer:
    await producer_not_started.start()
    try:
        yield producer_not_started
    finally:
        await producer_not_started.stop()


def on_delivery(err, msg):
    ...


async def test_start_stop(producer_not_started, kafka_client):
    kafka_client.start.assert_not_awaited()
    await producer_not_started.start()
    kafka_client.start.assert_awaited_once()
    await producer_not_started.start()
    assert len(kafka_client.start.call_args_list) == 2

    kafka_client.stop.assert_not_awaited()
    await producer_not_started.stop()
    kafka_client.stop.assert_awaited_once()
    await producer_not_started.stop()
    assert len(kafka_client.stop.call_args_list) == 2


async def test_exit_gracefully(producer, kafka_client):
    producer.exit_gracefully()
    assert not producer._should_run
    kafka_client.stop.assert_not_awaited()


async def test_methods_when_start_didnt_awaited(producer_not_started, kafka_client):
    with pytest.raises(ProducerNotRunningError):
        await producer_not_started.produce(
            topic='test', key=b'1', value=b'2', on_delivery=on_delivery
        )

    with pytest.raises(ProducerNotRunningError):
        await producer_not_started.get_topics()


async def test_get_topics_ok(producer, kafka_client: KafkaClient):
    assert await producer.get_topics() == [
        'test', 'test-topic', 'another-topic', 'test.topic'
    ]


@pytest.mark.parametrize(
    "error, expected_error", [
        (OperationTimeout(), ProducerTimeoutError),
        (KafkaClientCannotRestart(), KafkaClientNotRestartable),
        (TimeoutError(), ProducerError),
        (asyncio.TimeoutError(), ProducerError),
        (asyncio.CancelledError(), ProducerError),
        (BaseException(), ProducerError),
    ]
)
async def test_handling_errors_in_kafka_client(
    producer, kafka_client: KafkaClient, error, expected_error
):
    kafka_client.list_topics = AsyncMock(side_effect=error)
    with pytest.raises(expected_error):
        await producer.get_topics()

    kafka_client.produce = AsyncMock(side_effect=error)
    with pytest.raises(expected_error):
        await producer.produce(
            topic='test', key=b'1', value=b'2', on_delivery=on_delivery
        )
    kafka_client.produce.assert_awaited_once()
    kafka_client.produce.assert_awaited_once_with(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )

    kafka_client.start = AsyncMock(side_effect=error)
    with pytest.raises(expected_error) as ex:
        await producer.start()
    kafka_client.start.assert_awaited_once()

    kafka_client.stop = AsyncMock(side_effect=error)
    with pytest.raises(expected_error) as ex:
        await producer.stop()
    kafka_client.stop.assert_awaited_once()

    kafka_client.stop = AsyncMock()


async def test_produce_ok(producer, kafka_client: KafkaClient):
    await producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_awaited_once()
    kafka_client.produce.assert_awaited_once_with(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )


async def test_produce_buffer_error(
    producer, kafka_client: KafkaClient
):
    await producer.produce(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )
    kafka_client.recreate_produce_mock()

    kafka_client._produce_error = aiokafka.errors.KafkaTimeoutError()
    producer._wait_for_flush = 0.001

    await producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)

    assert kafka_client.produce.call_args_list == [
        call(topic='test', key=b'1', value=b'2', on_delivery=on_delivery),
        call(topic='test', key=b'1', value=b'2', on_delivery=on_delivery),
    ]
    assert kafka_client.flush.call_args_list == [
        call(timeout=producer._wait_for_flush),
        call(timeout=producer._wait_for_flush),
        call(timeout=producer._wait_for_flush),
    ]


async def test_produce_timeout_error(
    producer, kafka_client: KafkaClient
):
    kafka_client._produce_error = aiokafka.errors.KafkaTimeoutError()
    producer._wait_for_flush = 0.001

    await producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)

    assert kafka_client.produce.call_args_list == [
        call(topic='test', key=b'1', value=b'2', on_delivery=on_delivery),
        call(topic='test', key=b'1', value=b'2', on_delivery=on_delivery),
    ]
    assert kafka_client.flush.call_args_list == []


async def test_stop_when_produce(producer, kafka_client: KafkaClient):
    await producer.produce(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )
    kafka_client.recreate_produce_mock()
    kafka_client._is_permanent_error = True

    producer._wait_for_flush = 0.001
    kafka_client._produce_error = aiokafka.errors.KafkaTimeoutError()

    async def flush(timeout):
        nonlocal is_ready_to_stop
        await asyncio.sleep(timeout)
        is_ready_to_stop = True
        f = producer._message_accumulator.pop()
        f.set_result(None)
        await asyncio.sleep(timeout)

    is_task_started = False
    async def stop():
        nonlocal is_ready_to_stop, is_task_started
        is_task_started = True
        while not is_ready_to_stop:
            await asyncio.sleep(0.001)
        await producer.stop()

    is_ready_to_stop = False
    kafka_client.flush = AsyncMock(side_effect=flush)

    task = asyncio.get_event_loop().create_task(stop())
    while not is_task_started:
        await asyncio.sleep(0.001)

    kafka_client.stop.assert_not_awaited()
    kafka_client.flush.assert_not_awaited()
    kafka_client.produce.assert_not_awaited()

    await producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_awaited_once()
    kafka_client.stop.assert_awaited_once()
    kafka_client.flush.assert_awaited()
    assert not producer._should_run


async def test_stop_when_produce_no_first_message(producer, kafka_client: KafkaClient):
    producer._wait_for_flush = 0.001

    is_ready_to_stop = False
    is_task_started = False

    async def produce(*args, **kwargs):
        nonlocal is_ready_to_stop
        await asyncio.sleep(0.002)
        is_ready_to_stop = True
        raise aiokafka.errors.KafkaTimeoutError()

    kafka_client.produce = AsyncMock(side_effect=produce)
    async def stop():
        nonlocal is_ready_to_stop, is_task_started
        is_task_started = True
        while not is_ready_to_stop:
            await asyncio.sleep(0.001)
        await producer.stop()

    task = asyncio.get_event_loop().create_task(stop())
    while not is_task_started:
        await asyncio.sleep(0.001)

    kafka_client.stop.assert_not_awaited()
    kafka_client.flush.assert_not_awaited()
    kafka_client.produce.assert_not_awaited()

    await producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_awaited_once()
    kafka_client.stop.assert_awaited_once()
    kafka_client.flush.assert_not_awaited()
    assert not producer._should_run


async def test_exit_when_produce(producer, kafka_client: KafkaClient):
    await producer.produce(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )
    kafka_client.recreate_produce_mock()
    kafka_client._is_permanent_error = True

    producer._wait_for_flush = 0.001
    kafka_client._produce_error = aiokafka.errors.KafkaTimeoutError()

    async def flush(timeout):
        nonlocal is_ready_to_stop
        await asyncio.sleep(timeout)
        is_ready_to_stop = True
        f = producer._message_accumulator.pop()
        f.set_result(None)
        await asyncio.sleep(timeout)

    is_task_started = False
    async def stop():
        nonlocal is_ready_to_stop, is_task_started
        is_task_started = True
        while not is_ready_to_stop:
            await asyncio.sleep(0.001)
        producer.exit_gracefully()

    is_ready_to_stop = False
    kafka_client.flush = AsyncMock(side_effect=flush)

    task = asyncio.get_event_loop().create_task(stop())
    while not is_task_started:
        await asyncio.sleep(0.001)

    kafka_client.stop.assert_not_awaited()
    kafka_client.flush.assert_not_awaited()
    kafka_client.produce.assert_not_awaited()

    await producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_awaited_once()
    kafka_client.stop.assert_not_awaited()
    kafka_client.flush.assert_awaited()
    assert not producer._should_run


async def test_exit_when_produce_no_first_message(producer, kafka_client: KafkaClient):
    producer._wait_for_flush = 0.001

    is_ready_to_stop = False
    is_task_started = False

    async def produce(*args, **kwargs):
        nonlocal is_ready_to_stop
        await asyncio.sleep(0.002)
        is_ready_to_stop = True
        raise aiokafka.errors.KafkaTimeoutError()

    kafka_client.produce = AsyncMock(side_effect=produce)
    async def stop():
        nonlocal is_ready_to_stop, is_task_started
        is_task_started = True
        while not is_ready_to_stop:
            await asyncio.sleep(0.001)
        producer.exit_gracefully()

    task = asyncio.get_event_loop().create_task(stop())
    while not is_task_started:
        await asyncio.sleep(0.001)

    kafka_client.stop.assert_not_awaited()
    kafka_client.flush.assert_not_awaited()
    kafka_client.produce.assert_not_awaited()

    await producer.produce(topic='test', key=b'1', value=b'2', on_delivery=on_delivery)
    kafka_client.produce.assert_awaited_once()
    kafka_client.stop.assert_not_awaited()
    kafka_client.flush.assert_not_awaited()
    assert not producer._should_run
