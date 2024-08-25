import asyncio
import datetime
import time
import bson
from copy import deepcopy
from multiprocessing import Queue
from queue import Empty
from threading import Thread
from unittest.mock import Mock, call, AsyncMock

import pytest
from bson import json_util

from mongo_change_stream_mediator.commit_event_decoder import decode_commit_event, \
    encode_commit_event
from mongo_change_stream_mediator.models import DecodedChangeEvent, CommitEvent
from mongo_change_stream_mediator.aiokafka_producing import ChangeEventHandler
from mongo_change_stream_mediator.aiokafka_producing.change_event_handler import (
    EventDoesNotContainOperationType,
    CollectionNotFoundError,
    OperationNotAllowedError,
    NamespaceNotFoundError,
    DatabaseNotFoundError,
    DocumentKeyNotFoundError,
    EventHandlerNotRunning
)
from mongo_change_stream_mediator.aiokafka_producing.producer import (
    BaseProducerError,
    ProducerTimeoutError,
    KafkaClientNotRestartable,
    KafkaClientNotRunningError,
    ProducerError,
    ProducerNotRunningError
)
from mongo_change_stream_mediator.aiokafka_producing.timeout import OperationTimeout
from tests.mocks.events import (
    insert, update, delete, replace, drop, drop_database, invalidate,
)


@pytest.fixture()
def commit_queue():
    return Queue()


@pytest.fixture()
def topics_test_initial():
    return {'test', 'testA', 'test.A'}


@pytest.fixture()
def topics_initial(topics_test_initial):
    topics = deepcopy(topics_test_initial)
    topics.add("A")
    return topics


@pytest.fixture()
def topics(topics_initial):
    return deepcopy(topics_initial)


@pytest.fixture()
def producer(topics):
    async def get_topics():
        return list(topics)

    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.exit_gracefully = AsyncMock()
    producer.get_topics = AsyncMock(side_effect=get_topics)
    producer.produce = AsyncMock()
    return producer


@pytest.fixture()
def change_event_handler_not_started(commit_queue, producer) -> ChangeEventHandler:
    return ChangeEventHandler(
        producer=producer,
        committer_queue=commit_queue,
        kafka_prefix='test',
        put_queue_sleep_time=0.001,
    )


@pytest.fixture()
async def change_event_handler(change_event_handler_not_started) -> ChangeEventHandler:
    await change_event_handler_not_started.start()
    try:
        yield change_event_handler_not_started
    finally:
        await change_event_handler_not_started.stop()


@pytest.fixture()
def json_decoder():
    def decoder(value):
        return json_util.dumps(
            value,
            json_options=json_util.LEGACY_JSON_OPTIONS
        ).encode()

    return decoder


async def test_start_twice(change_event_handler_not_started, producer):
    assert not change_event_handler_not_started._should_run
    await change_event_handler_not_started.start()

    assert change_event_handler_not_started._should_run
    assert change_event_handler_not_started._created_topics == {
        'test', 'testA', 'test.A'
    }
    await change_event_handler_not_started.start()
    assert change_event_handler_not_started._should_run
    assert change_event_handler_not_started._created_topics == {
        'test', 'testA', 'test.A'
    }
    producer.exit_gracefully.assert_not_awaited()
    producer.stop.assert_not_awaited()


async def test_start(change_event_handler_not_started, producer):
    assert not change_event_handler_not_started._should_run
    await change_event_handler_not_started.start()
    assert change_event_handler_not_started._should_run
    assert change_event_handler_not_started._created_topics == {
        'test', 'testA', 'test.A'
    }

    producer.stop.assert_not_awaited()
    await change_event_handler_not_started.stop()
    assert not change_event_handler_not_started._should_run
    producer.start.assert_awaited_once()
    producer.stop.assert_awaited_once()

    await change_event_handler_not_started.stop()
    assert len(producer.stop.call_args_list) == 2
    producer.exit_gracefully.assert_not_awaited()


async def test_exit_gracefully(change_event_handler, producer):
    assert change_event_handler._should_run
    change_event_handler.exit_gracefully()
    assert not change_event_handler._should_run
    producer.stop.assert_not_awaited()
    producer.exit_gracefully.assert_called_once()

    change_event_handler.exit_gracefully()
    assert len(producer.exit_gracefully.call_args_list) == 2
    producer.stop.assert_not_awaited()


async def test_decoder(change_event_handler_not_started, json_decoder):
    value = {"test": [1, datetime.datetime.now(), 0.001, "test", True]}
    assert change_event_handler_not_started._key_value_encoder(
        value
    ) == json_decoder(value)


async def test_update_created_topics(
        change_event_handler, producer, topics, topics_initial, topics_test_initial
):
    assert topics == topics_initial
    assert change_event_handler._created_topics == topics_test_initial
    producer.get_topics.assert_called_once()

    async def get_topics():
        return ["testC"]

    producer.get_topics = AsyncMock(side_effect=get_topics)
    await change_event_handler._update_created_topics()
    assert change_event_handler._created_topics == {"testC"}

    producer.get_topics.assert_called_once()


async def test_produce_message(change_event_handler, producer):
    def on_delivery(future):
        ...

    await change_event_handler._produce_message(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery, count=1
    )

    producer.produce.assert_awaited_once_with(
        topic='test', key=b'1', value=b'2', on_delivery=on_delivery
    )



def decoded_change_event(event: dict):
    return DecodedChangeEvent(
        bson_document=event,
        count=1
    )


@pytest.mark.parametrize("prefix", ["test", ""])
@pytest.mark.parametrize("event", [insert(), update(), delete(), replace(), drop()])
def test_get_topic_from_event_ok(event, change_event_handler, prefix):
    change_event_handler._kafka_prefix = prefix
    topics_name = "test-database.TestCollection"
    if prefix:
        topics_name = prefix + "." + topics_name
    assert change_event_handler._get_topic_from_event(
        decoded_change_event(event)
    ) == topics_name


@pytest.mark.parametrize("prefix", ["test", ""])
@pytest.mark.parametrize("event", [drop_database(), invalidate()])
def test_get_topic_from_bad_event(change_event_handler, prefix, event):
    with pytest.raises(KeyError):
        change_event_handler._get_topic_from_event(
            DecodedChangeEvent(
                bson_document=event,
                count=1
            )
        )


def test_get_key_from_event(change_event_handler):
    event = replace()
    key = change_event_handler._get_key_from_event(decoded_change_event(event))
    assert key == change_event_handler._key_value_encoder(event["documentKey"])

    with pytest.raises(KeyError):
        del event["documentKey"]
        change_event_handler._get_key_from_event(decoded_change_event(event))


def test_get_value_from_event(change_event_handler):
    event = update()
    value = change_event_handler._get_value_from_event(decoded_change_event(event))

    new_doc = {
        'op': 'u',
        'before': change_event_handler._json_encoder(event['fullDocumentBeforeChange']),
        'updateDescription': {
            "updatedFields": change_event_handler._json_encoder(
                event['updateDescription']["updatedFields"]
            ),
            "removedFields": event['updateDescription']["removedFields"],
            "truncatedArrays": event['updateDescription']["truncatedArrays"],
        },
        'after': change_event_handler._json_encoder(event['fullDocument']),
    }
    assert value == change_event_handler._key_value_encoder(new_doc)

    del event["fullDocumentBeforeChange"]
    del event["updateDescription"]
    del event["fullDocument"]
    value = change_event_handler._get_value_from_event(decoded_change_event(event))
    new_doc = {
        'op': 'u',
    }
    assert value == change_event_handler._key_value_encoder(new_doc)

    with pytest.raises(KeyError):
        del event["op"]
        change_event_handler._get_value_from_event(decoded_change_event(event))


@pytest.mark.parametrize("operation, result", [
    (insert(), 'c'),
    (update(), 'u'),
    (replace(), 'u'),
    (delete(), 'd'),
])
def test_get_op(change_event_handler, operation, result):
    assert change_event_handler._get_op(operation['operationType']) == result


def test_validate_event(change_event_handler):
    event = decoded_change_event(update())

    modified_event = deepcopy(event)
    del modified_event.bson_document['operationType']
    with pytest.raises(EventDoesNotContainOperationType):
        change_event_handler._validate_event(modified_event)

    modified_event = deepcopy(event)
    del modified_event.bson_document['ns']
    with pytest.raises(NamespaceNotFoundError):
        change_event_handler._validate_event(modified_event)

    modified_event = deepcopy(event)
    del modified_event.bson_document['ns']['coll']
    with pytest.raises(CollectionNotFoundError):
        change_event_handler._validate_event(modified_event)

    modified_event = deepcopy(event)
    del modified_event.bson_document['ns']['db']
    with pytest.raises(DatabaseNotFoundError):
        change_event_handler._validate_event(modified_event)

    modified_event = deepcopy(event)
    del modified_event.bson_document['documentKey']
    with pytest.raises(DocumentKeyNotFoundError):
        change_event_handler._validate_event(modified_event)

    modified_event = deepcopy(event)
    modified_event.bson_document['operationType'] = 'test'
    with pytest.raises(OperationNotAllowedError):
        change_event_handler._validate_event(modified_event)


@pytest.mark.parametrize("operation", [insert(), update(), replace(), delete(), ])
def test_validate_event_ok(change_event_handler, operation):
    event = decoded_change_event(operation)
    change_event_handler._validate_event(event)


@pytest.mark.parametrize("operation", [drop(), drop_database(), invalidate(), ])
def test_validate_event_error(change_event_handler, operation):
    event = decoded_change_event(operation)
    with pytest.raises(OperationNotAllowedError):
        change_event_handler._validate_event(event)


@pytest.mark.parametrize("operation", [
    insert(), update(), replace(), delete(), drop(), drop_database(), invalidate(),
])
async def test_handle_if_not_running(change_event_handler_not_started, operation):
    with pytest.raises(EventHandlerNotRunning):
        await change_event_handler_not_started.handle(decoded_change_event(operation))


async def test_skip_wrong_event(change_event_handler, producer, commit_queue):
    event = decoded_change_event(update())

    def check_no_calls():
        producer.produce.assert_not_called()
        producer.get_topics.assert_called_once()
        producer.create_topic.assert_not_called()
        with pytest.raises(Empty):
            commit_queue.get(timeout=0.001)


    modified_event = deepcopy(event)
    del modified_event.bson_document['operationType']
    with pytest.raises(EventDoesNotContainOperationType):
        await change_event_handler.handle(modified_event)
    check_no_calls()

    modified_event = deepcopy(event)
    del modified_event.bson_document['ns']
    with pytest.raises(NamespaceNotFoundError):
        await change_event_handler.handle(modified_event)
    check_no_calls()

    modified_event = deepcopy(event)
    del modified_event.bson_document['ns']['coll']
    with pytest.raises(CollectionNotFoundError):
        await change_event_handler.handle(modified_event)
    check_no_calls()

    modified_event = deepcopy(event)
    del modified_event.bson_document['ns']['db']
    with pytest.raises(DatabaseNotFoundError):
        await change_event_handler.handle(modified_event)
    check_no_calls()

    modified_event = deepcopy(event)
    del modified_event.bson_document['documentKey']
    with pytest.raises(DocumentKeyNotFoundError):
        await change_event_handler.handle(modified_event)
    check_no_calls()

    modified_event = deepcopy(event)
    modified_event.bson_document['operationType'] = 'test'
    with pytest.raises(OperationNotAllowedError):
        await change_event_handler.handle(modified_event)
    check_no_calls()


test_errors = [
    (BaseProducerError(), BaseProducerError),
    (ProducerTimeoutError(OperationTimeout()), ProducerTimeoutError),
    (KafkaClientNotRestartable(), KafkaClientNotRestartable),
    (KafkaClientNotRunningError(), KafkaClientNotRunningError),
    (ProducerError(BaseException()), ProducerError),
    (ProducerNotRunningError(), ProducerNotRunningError)
]


@pytest.mark.parametrize("error, expected_error", test_errors)
async def test_errors_from_producer_produce(
    change_event_handler, error, expected_error, producer
):

    async def get_topics():
        return ["test.test-database.TestCollection"]

    producer.get_topics = AsyncMock(side_effect=get_topics)
    producer.produce = Mock(side_effect=error)
    event = decoded_change_event(update())
    with pytest.raises(expected_error):
        await change_event_handler.handle(event)


@pytest.mark.parametrize("error, expected_error", test_errors)
async def test_errors_from_producer_get_topics(
    change_event_handler, error, expected_error, producer
):
    producer.get_topics = Mock(side_effect=error)
    event = decoded_change_event(update())
    with pytest.raises(expected_error):
        await change_event_handler.handle(event)


@pytest.mark.parametrize("error, expected_error", test_errors)
async def test_errors_from_producer_start(
    change_event_handler_not_started, error, expected_error, producer
):
    producer.start = Mock(side_effect=error)
    with pytest.raises(expected_error):
        await change_event_handler_not_started.start()


@pytest.mark.parametrize("error, expected_error", test_errors)
async def test_errors_from_producer_stop(
    change_event_handler_not_started, error, expected_error, producer
):
    producer.stop = Mock(side_effect=error)
    with pytest.raises(expected_error):
        await change_event_handler_not_started.stop()

    producer.stop = AsyncMock()


@pytest.mark.parametrize('method', ['produce', 'get_topics'])
async def test_not_running_producer_when_event_handler_should_not_run(
    change_event_handler, producer, method
):
    event = decoded_change_event(update())
    if method != 'get_topics':
        change_event_handler._created_topics.add("test.test-database.TestCollection")

    async def func(*args, **kwargs):
        change_event_handler.exit_gracefully()
        raise ProducerNotRunningError()

    setattr(producer, method,  AsyncMock(side_effect=func))
    with pytest.raises(EventHandlerNotRunning):
        await change_event_handler.handle(event)


async def test_produce_to_not_created_topic(
    change_event_handler, producer
):
    change_event_handler._wait_for_topic_created = 0.001
    event = decoded_change_event(update())
    task = asyncio.create_task(change_event_handler.handle(event))
    while len(producer.get_topics.call_args_list) < 10:
        await asyncio.sleep(0.001)

    async def get_topics():
        return ["test.test-database.TestCollection"]

    producer.get_topics = AsyncMock(side_effect=get_topics)
    await task


def test_on_message_delivered_error(change_event_handler, producer):
    change_event_handler.exit_gracefully = Mock()
    future = asyncio.get_event_loop().create_future()
    future.set_exception(BaseException('test'))
    change_event_handler._on_message_delivered(count=1, resume_token=b'test')(future)
    change_event_handler.exit_gracefully.assert_called_once()


def test_on_message_delivered_ok(change_event_handler, commit_queue):
    future = asyncio.get_event_loop().create_future()
    future.set_result(None)
    change_event_handler._on_message_delivered(count=1, resume_token=b'test')(future)
    value = change_event_handler._put_queue.popleft()
    assert value == CommitEvent(
        count=1,
        need_confirm=False,
        resume_token=b'test'
    )
    assert len(change_event_handler._put_queue) == 0


def test_redirect_message(change_event_handler, commit_queue):
    change_event_handler._redirect_message()
    assert commit_queue.empty()

    change_event_handler._put_queue.append(CommitEvent(
        count=1,
        need_confirm=False,
        resume_token=b'test'
    ))
    change_event_handler._redirect_message()
    message = commit_queue.get()
    assert message == encode_commit_event(
        count=1,
        need_confirm=0,
        token=b'test',
    )


@pytest.mark.parametrize("operation", [insert(), update(), replace(), delete(), ])
async def test_handle_ok(
    change_event_handler, producer, commit_queue, operation
):
    change_event_handler._wait_for_topic_created = 0.001
    event_dict = operation
    event = decoded_change_event(event_dict)

    async def get_topics():
        return ["test.test-database.TestCollection"]

    callback = None
    async def produce(topic, key, value, on_delivery):
        nonlocal callback
        f = asyncio.get_event_loop().create_future()
        f.set_result(None)
        on_delivery(f)
        callback = on_delivery

    producer.get_topics = AsyncMock(side_effect=get_topics)
    producer.produce = AsyncMock(side_effect=produce)
    await change_event_handler.handle(event)
    message = commit_queue.get()

    producer.produce.assert_awaited_once_with(
        topic="test.test-database.TestCollection",
        key=change_event_handler._get_key_from_event(event),
        value=change_event_handler._get_value_from_event(event),
        on_delivery=callback
    )
    producer.get_topics.assert_awaited_once_with()
    assert message == encode_commit_event(
            count=1,
            need_confirm=0,
            token=bson.encode(event_dict["_id"]),
        )
