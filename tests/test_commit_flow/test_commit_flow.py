from unittest.mock import Mock
from multiprocessing import Queue

import pytest

from mongo_change_stream_mediator.commit_event_decoder import encode_commit_event
from mongo_change_stream_mediator.committing import CommitFlow
from mongo_change_stream_mediator.models import RecheckCommitEvent


@pytest.fixture
def committer_queue():
    return Queue()


@pytest.fixture
def commit_flow(committer_queue):
    return CommitFlow(
        committer_queue=committer_queue,
        commit_event_handler=Mock(),
        queue_get_timeout=0.001
    )


@pytest.mark.parametrize(
    "token, token_result",
    [
        (None, None),
        (b"", None),
        (b"test", b"test"),
    ]
)
@pytest.mark.parametrize("need_confirm", [True, False])
@pytest.mark.parametrize("count", [0, 1, 2])
def test_iter_event(
    commit_flow, need_confirm, count, token, token_result, committer_queue
):
    data = encode_commit_event(
        count=count,
        need_confirm=need_confirm,
        token=token,
    )
    committer_queue.put(data)

    events = []
    for event in commit_flow.iter_event():
        assert event.count == count
        assert event.need_confirm == need_confirm
        assert event.resume_token == token_result
        events.append(event)

    assert len(events) == 1


@pytest.mark.parametrize("data", [None, b""])
def test_iter_none_event(
    commit_flow, committer_queue, data
):
    committer_queue.put(data)

    events = []
    for event in commit_flow.iter_event():
        events.append(event)

    assert len(events) == 0


def test_iter_empty_queue(
    commit_flow, committer_queue,
):
    events = []
    for event in commit_flow.iter_event():
        assert isinstance(event, RecheckCommitEvent)
        events.append(event)

    assert len(events) == 1
