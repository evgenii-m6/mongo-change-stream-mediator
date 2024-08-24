import pytest

from mongo_change_stream_mediator.committing import ProcessCommitEvent


@pytest.fixture
def process_commit_event():
    processor = ProcessCommitEvent(commit_interval=0)
    assert not processor._confirmed_events
    assert not processor._unconfirmed_events
    return processor
