from multiprocessing import Queue

import pytest

from mongo_change_stream_mediator.change_event_decoder import decode_change_event
from mongo_change_stream_mediator.change_stream_reading import ChangeHandler
from mongo_change_stream_mediator.commit_event_decoder import decode_commit_event
from mongo_change_stream_mediator.models import ChangeEvent, CommitEvent, \
    DecodedChangeEvent
from tests.mocks.events import (
    insert, update, replace, delete, drop, drop_database, invalidate, to_raw_bson,
    get_resume_token_from_raw_event
)


@pytest.fixture
def commit_queue():
    return Queue()


@pytest.fixture
def producer_queue_0():
    return Queue()


@pytest.fixture
def producer_queue_1():
    return Queue()

@pytest.fixture
def producer_queue_2():
    return Queue()


@pytest.fixture
def change_handler(commit_queue, producer_queue_0, producer_queue_1, producer_queue_2):
    return ChangeHandler(
        committer_queue=commit_queue,
        producer_queues={0: producer_queue_0, 1: producer_queue_1, 2: producer_queue_2}
    )


@pytest.mark.parametrize("event, result", [
    (insert(), 1),
    (update(), 1),
    (replace(), 1),
    (delete(), 1),
    (drop(), 0),
    (drop_database(), 0),
    (invalidate(), 0)
])
def test_is_need_to_send_to_producer(change_handler, event, result):
    assert result == change_handler._is_need_to_send_to_producer(to_raw_bson(event))


@pytest.mark.parametrize("event, result", [
    (insert(), 2),
    (update(), 2),
    (replace(), 2),
    (delete(), 2),
])
def test_get_queue_number(change_handler, event, result):
    assert result == change_handler._get_queue_number(to_raw_bson(event))


@pytest.mark.parametrize("use_token", [
    True, False
])
@pytest.mark.parametrize("event, need_produce", [
    (insert(), 1),
    (update(), 1),
    (replace(), 1),
    (delete(), 1),
    (drop(), 0),
    (drop_database(), 0),
    (invalidate(), 0),
    (None, 0),
])
def test_handle(
    change_handler, event, need_produce,
    use_token, commit_queue, producer_queue_2,
):
    if event is None:
        token = (
            get_resume_token_from_raw_event(to_raw_bson(insert()))
            if use_token
            else None
        )
    else:
        token = (
            get_resume_token_from_raw_event(to_raw_bson(event))
            if use_token
            else None
        )

    change_handler.handle(
        ChangeEvent(
            bson_document=to_raw_bson(event) if event else None,
            token=token,
            count=1
        )
    )
    if need_produce:
        change_event_bytes = producer_queue_2.get()
        assert decode_change_event(change_event_bytes) == DecodedChangeEvent(
            bson_document=event,
            count=1,
        )

    commit_event_bytes = commit_queue.get()
    assert decode_commit_event(commit_event_bytes) == CommitEvent(
        count=1,
        need_confirm=bool(need_produce),
        resume_token=token.raw if token else None
    )
