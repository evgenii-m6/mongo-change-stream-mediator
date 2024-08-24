import time
from copy import deepcopy

import pytest

from mongo_change_stream_mediator.committing.commit_processing import ProcessCommitEvent
from mongo_change_stream_mediator.models import CommitEvent, RecheckCommitEvent
from mongo_change_stream_mediator.models import CommittableEvents


def test_move_from_unconfirmed_to_confirmed(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    event_1 = CommitEvent(count=count, need_confirm=True, resume_token=b"test")
    for _ in process_commit_event.process_event(event_1):
        ...
    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events == {count: event_1}

    event_2 = CommitEvent(count=count, need_confirm=False, resume_token=b"test2")
    for _ in process_commit_event.process_event(event_2):
        ...

    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._confirmed_events == {count: event_2}
    assert process_commit_event._last_sent_commit_event == 0


def test_not_move_from_confirmed_to_unconfirmed(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    for _ in process_commit_event.process_event(event_1):
        ...
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._confirmed_events == {count: event_1}

    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")
    for _ in process_commit_event.process_event(event_2):
        ...

    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._confirmed_events == {count: event_1}
    assert process_commit_event._last_sent_commit_event == 0


def test_replace_unconfirmed(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    event_1 = CommitEvent(count=count, need_confirm=True, resume_token=b"test")
    for _ in process_commit_event.process_event(event_1):
        ...
    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events == {count: event_1}

    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")
    for _ in process_commit_event.process_event(event_2):
        ...

    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events == {count: event_2}
    assert process_commit_event._last_sent_commit_event == 0


def test_replace_confirmed(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    for _ in process_commit_event.process_event(event_1):
        ...
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._confirmed_events == {count: event_1}

    event_2 = CommitEvent(count=count, need_confirm=False, resume_token=b"test2")
    for _ in process_commit_event.process_event(event_2):
        ...

    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._confirmed_events == {count: event_2}
    assert process_commit_event._last_sent_commit_event == 0


@pytest.mark.parametrize("need_confirm", [True, False])
def test_not_apply_already_handled(
    process_commit_event: ProcessCommitEvent,
    need_confirm
):
    process_commit_event._commit_interval = 1000
    process_commit_event._last_sent_commit_event = 1
    count = 1
    event_1 = CommitEvent(count=count, need_confirm=need_confirm, resume_token=b"test")
    for _ in process_commit_event.process_event(event_1):
        ...
    assert not process_commit_event._unconfirmed_events
    assert not process_commit_event._confirmed_events
    assert process_commit_event._last_sent_commit_event == 1


def test_confirmed_with_error(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    time.sleep(0.001)
    with pytest.raises(ValueError) as er:
        for commit in process_commit_event.process_event(event_1):
            assert commit.resume_token == b"test"
            assert commit.numbers == [1]
            assert count in process_commit_event._confirmed_events
            assert not process_commit_event._unconfirmed_events
            assert process_commit_event._last_recheck_time > initial_commit_time
            assert process_commit_event._last_sent_commit_event == 0
            time.sleep(0.001)
            raise ValueError("test")

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_recheck_time != initial_commit_time
    assert process_commit_event._last_sent_commit_event == 0

def test_confirmed_confirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=False, resume_token=b"test2")
    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_1):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_recheck_time > initial_commit_time
        initial_commit_time = process_commit_event._last_recheck_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1

    for event in [event_1, event_2]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)

        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events


def test_unconfirmed_confirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=True, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=False, resume_token=b"test2")
    r = []
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert not r

    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events[count] == event_1
    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_2):
        assert commit.resume_token == b"test2"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_recheck_time > initial_commit_time
        initial_commit_time = process_commit_event._last_recheck_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1

    for event in [event_1, event_2]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)

        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1

def test_unconfirmed_unconfirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=True, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")
    r = []
    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert process_commit_event._last_recheck_time > initial_commit_time
    assert not r

    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events[count] == event_1

    initial_commit_time = process_commit_event._last_recheck_time
    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_2):
        r.append(commit)

    assert process_commit_event._last_recheck_time > initial_commit_time
    assert not r

    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events[count] == event_2
    assert process_commit_event._last_sent_commit_event == 0


def test_confirmed_unconfirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")

    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_1):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_recheck_time > initial_commit_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)
    assert process_commit_event._last_sent_commit_event == 1
    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1

    for event in [event_1, event_2]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)

        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1

def test_confirmed_unconfirmed_without_commit(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")

    r = []

    for commit in process_commit_event.process_event(event_1):
        r.append(commit)
    assert initial_commit_time == process_commit_event._last_recheck_time

    assert not r

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0

    initial_commit_time = process_commit_event._last_recheck_time
    r = []
    for commit in process_commit_event.process_event(event_2):
        r.append(commit)

    assert initial_commit_time == process_commit_event._last_recheck_time

    assert not r

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0


def test_confirmed_unconfirmed_with_commit(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")

    r = []
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert initial_commit_time == process_commit_event._last_recheck_time
    assert not r
    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0

    process_commit_event._commit_interval = 0

    initial_commit_time = process_commit_event._last_recheck_time
    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_2):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_recheck_time > initial_commit_time
        initial_commit_time = process_commit_event._last_recheck_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1

    for event in [event_1, event_2]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)

        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events


def test_confirmed_and_recheck(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    initial_commit_time = process_commit_event._last_recheck_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = RecheckCommitEvent()

    r = []
    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert initial_commit_time == process_commit_event._last_recheck_time
    assert not r
    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0

    process_commit_event._commit_interval = 0

    initial_commit_time = process_commit_event._last_recheck_time
    time.sleep(0.001)
    for commit in process_commit_event.process_event(event_2):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_recheck_time > initial_commit_time
        initial_commit_time = process_commit_event._last_recheck_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1

    for event in [event_1, ]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)

        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events


def test_clear_previous_commit_events(process_commit_event: ProcessCommitEvent):
    events = CommittableEvents(numbers=[2, 3], resume_token=b"test")
    process_commit_event._confirmed_events = {
        1: CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
        3: CommitEvent(count=3, need_confirm=False, resume_token=b"test"),
        4: CommitEvent(count=4, need_confirm=False, resume_token=b"test"),
    }
    process_commit_event._unconfirmed_events = {
        2: CommitEvent(count=2, need_confirm=True, resume_token=b"test"),
        5: CommitEvent(count=5, need_confirm=True, resume_token=b"test"),
        6: CommitEvent(count=6, need_confirm=True, resume_token=b"test"),
    }
    process_commit_event._clear_previous_commit_events(events)

    assert process_commit_event._confirmed_events == {
        1: CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
        4: CommitEvent(count=4, need_confirm=False, resume_token=b"test"),
    }
    assert process_commit_event._unconfirmed_events == {
        5: CommitEvent(count=5, need_confirm=True, resume_token=b"test"),
        6: CommitEvent(count=6, need_confirm=True, resume_token=b"test"),
    }
    assert process_commit_event._last_sent_commit_event == 3


@pytest.mark.parametrize(
    "events, result",
    [
        (
            {
                CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
                CommitEvent(count=2, need_confirm=True, resume_token=b"test_2"),
            },
            None
        ),
        (
            {
                CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
                CommitEvent(count=2, need_confirm=False, resume_token=b"test_2"),
            },
            None
        ),
        (
            {
                CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
                CommitEvent(count=2, need_confirm=False, resume_token=b"test_2"),
            },
            CommittableEvents([1, 2], resume_token=b"test_2")
        ),
        (
            {
                CommitEvent(count=2, need_confirm=True, resume_token=b"test"),
            },
            None
        ),
        (
            {
                CommitEvent(count=2, need_confirm=False, resume_token=b"test"),
            },
            None
        ),
        (
            {
                CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
            },
            None
        ),
        (
            {
                CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
            },
            CommittableEvents([1], resume_token=b"test")
        ),
    ]
)
def test_get_committable_events(
    process_commit_event: ProcessCommitEvent,
    events, result
):
    process_commit_event._confirmed_events = {
        i.count: i for i in events if not i.need_confirm
    }
    process_commit_event._unconfirmed_events = {
        i.count: i for i in events if not i.need_confirm
    }
    assert process_commit_event._get_committable_events() == result


def test_2_3_1(process_commit_event: ProcessCommitEvent):
    initial_commit_time = process_commit_event._last_recheck_time

    event_2 = CommitEvent(count=2, need_confirm=True, resume_token=b"test")
    event_3 = CommitEvent(count=3, need_confirm=True, resume_token=b"test")
    event_1 = CommitEvent(count=1, need_confirm=True, resume_token=b"test")

    r = []
    for event in [event_2, event_3, event_1]:
        for commit in process_commit_event.process_event(event):
            r.append(commit)
    assert process_commit_event._last_recheck_time > initial_commit_time
    initial_commit_time = process_commit_event._last_recheck_time
    assert process_commit_event._last_sent_commit_event == 0

    event_2 = CommitEvent(count=2, need_confirm=False, resume_token=b"test")
    event_3 = CommitEvent(count=3, need_confirm=False, resume_token=b"test")
    event_1 = CommitEvent(count=1, need_confirm=False, resume_token=b"test")

    for event in [event_2, event_3]:
        for commit in process_commit_event.process_event(event):
            r.append(commit)

    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_recheck_time > initial_commit_time
    assert process_commit_event._last_sent_commit_event == 0

    initial_commit_time = process_commit_event._last_recheck_time
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert process_commit_event._last_sent_commit_event == 3
    assert process_commit_event._last_recheck_time > initial_commit_time
    assert process_commit_event._last_sent_commit_event == 3
