from multiprocessing import Queue
from unittest.mock import Mock

import pytest

from mongo_change_stream_mediator.change_event_decoder import (
    encode_change_event,
    decode_change_event,
)
from mongo_change_stream_mediator.producing import ProducerFlow
from mongo_change_stream_mediator.producing.change_event_handler import \
    EventHandlerNotRunning
from tests.mocks.events import events, to_raw_bson, update


@pytest.fixture
def producer_queue():
    return Queue()


@pytest.fixture
def event_handler():
    handler = Mock()
    handler.handle = Mock()
    handler.start = Mock()
    handler.stop = Mock()
    handler.exit_gracefully = Mock()
    return handler


@pytest.fixture
def producer_flow_not_started(producer_queue, event_handler):
    return ProducerFlow(
        producer_queue=producer_queue,
        event_handler=event_handler,
        queue_get_timeout=0.001,
    )


@pytest.fixture
def producer_flow(producer_flow_not_started):
    producer_flow_not_started.start()
    try:
        yield producer_flow_not_started
    finally:
        producer_flow_not_started.stop()


@pytest.mark.parametrize("event", events())
def test_get_change_event_and_process(
    producer_flow, event_handler, producer_queue, event
):
    producer_flow._get_change_event_and_process()
    event_handler.handle.assert_not_called()

    encoded = encode_change_event(
            count=100500,
            bson_document=to_raw_bson(event)
    )
    producer_queue.put(encoded)

    producer_flow._get_change_event_and_process()
    event_handler.handle.assert_called_once_with(decode_change_event(encoded))


@pytest.mark.parametrize("error, expected_error", [
    (BaseException(), BaseException),
    (EventHandlerNotRunning(), EventHandlerNotRunning),
])
def test_task_with_error(
    producer_flow, event_handler, producer_queue, error, expected_error
):
    encoded = encode_change_event(
            count=100500,
            bson_document=to_raw_bson(update())
    )
    producer_queue.put(encoded)
    event_handler.handle = Mock(side_effect=error)

    with pytest.raises(expected_error):
        producer_flow.task()


def test_not_running_flow_and_task_with_error(
    producer_flow, event_handler, producer_queue,
):
    def error(event):
        producer_flow.stop()
        raise EventHandlerNotRunning()

    encoded = encode_change_event(
            count=100500,
            bson_document=to_raw_bson(update())
    )
    producer_queue.put(encoded)
    event_handler.handle = Mock(side_effect=error)

    producer_flow.task()
    event_handler.handle.assert_called_once()


def test_start(producer_flow_not_started, event_handler):
    producer_flow_not_started._should_run = False
    event_handler.start.assert_not_called()
    event_handler.stop.assert_not_called()
    event_handler.exit_gracefully.assert_not_called()

    producer_flow_not_started.start()
    producer_flow_not_started._should_run = True
    event_handler.start.assert_called_once()
    event_handler.stop.assert_not_called()
    event_handler.exit_gracefully.assert_not_called()


def test_stop(producer_flow, event_handler):
    producer_flow._should_run = True
    event_handler.start.assert_called_once()
    event_handler.stop.assert_not_called()
    event_handler.exit_gracefully.assert_not_called()

    producer_flow.stop()
    producer_flow._should_run = False
    event_handler.start.assert_called_once()
    event_handler.stop.assert_called_once()
    event_handler.exit_gracefully.assert_not_called()


def test_exit_gracefully(producer_flow, event_handler):
    producer_flow._should_run = True
    event_handler.start.assert_called_once()
    event_handler.stop.assert_not_called()
    event_handler.exit_gracefully.assert_not_called()

    producer_flow.exit_gracefully(1,2)
    producer_flow._should_run = False
    event_handler.start.assert_called_once()
    event_handler.stop.assert_not_called()
    event_handler.exit_gracefully.assert_called_once()


def test_start_stop_twice(producer_flow, event_handler):
    producer_flow.start()
    producer_flow.stop()
    producer_flow.stop()
    assert len(event_handler.start.call_args_list) == 2
    assert len(event_handler.stop.call_args_list) == 2


def test_start_exit_gracefully_twice(producer_flow, event_handler):
    producer_flow.start()
    producer_flow.exit_gracefully(1,2)
    producer_flow.exit_gracefully(1,2)
    assert len(event_handler.start.call_args_list) == 2
    assert len(event_handler.exit_gracefully.call_args_list) == 2
