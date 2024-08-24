import time
from copy import deepcopy

import pytest

from mongo_change_stream_mediator.committing.commit_processing import ProcessCommitEvent
from mongo_change_stream_mediator.models import CommitEvent, RecheckCommitEvent
from mongo_change_stream_mediator.models import CommittableEvents


def test_is_need_commit_events_none(process_commit_event: ProcessCommitEvent):
    assert not process_commit_event._is_need_commit_events(None)


@pytest.mark.parametrize("numbers, result", [
    ([], False),
    ([1], False),
    ([1, 2], True),
    ([1, 2, 3], True),
])
def test_is_need_commit_many_events(
    process_commit_event: ProcessCommitEvent, numbers, result
):
    process_commit_event._commit_interval = 1000
    commit = CommittableEvents(numbers=numbers, resume_token=b"test")
    assert process_commit_event._is_need_commit_events(commit) == result


@pytest.mark.parametrize("numbers, last_commit_time, result", [
    ([], time.monotonic(), False),
    ([], time.monotonic()-10000, False),
    ([1], time.monotonic(), False),
    ([1], time.monotonic()-10000, True),
    ([1, 2], time.monotonic(), True),
    ([1, 2], time.monotonic()-10000, True),
    ([1, 2, 3], time.monotonic(), True),
    ([1, 2, 3], time.monotonic() - 10000, True),
])
def test_is_need_commit_old_events(
    process_commit_event: ProcessCommitEvent, numbers, result, last_commit_time
):
    process_commit_event._commit_interval = 1000
    process_commit_event._last_commit_time = last_commit_time

    commit = CommittableEvents(numbers=numbers, resume_token=b"test")
    assert process_commit_event._is_need_commit_events(commit) == result


@pytest.mark.parametrize("need_confirm", [True, False])
@pytest.mark.parametrize(
    "old_token, new_token, committable_events, actual_token",
    [
        (None, None, None, None),
        (b"test", None, CommittableEvents(numbers=[1], resume_token=b"test"), b"test"),
        (None, b"test", CommittableEvents(numbers=[1], resume_token=b"test"), b"test"),
        (b"test_1", b"test_2", CommittableEvents(numbers=[1], resume_token=b"test_2"), b"test_2"),
    ],
)
def test_process_events_with_different_tokens(
    process_commit_event: ProcessCommitEvent,
    old_token, new_token, committable_events, need_confirm, actual_token
):
    count = 1
    initial_commit_time = process_commit_event._last_commit_time
    event = CommitEvent(count=count, need_confirm=True, resume_token=old_token)
    commits = [i for i in process_commit_event.process_event(event)]
    assert not commits

    event = CommitEvent(count=count, need_confirm=need_confirm, resume_token=new_token)
    received_committable_events = None
    for commit in process_commit_event.process_event(event):
        assert not process_commit_event._unconfirmed_events
        assert count in process_commit_event._confirmed_events
        assert commit.numbers == [1]
        assert committable_events == commit
        assert process_commit_event._confirmed_events[count] == CommitEvent(
            count=count, need_confirm=False,
            resume_token=committable_events.resume_token
        )
        assert process_commit_event._last_sent_commit_event == 0
        assert process_commit_event._last_commit_time == initial_commit_time
        time.sleep(0.001)
        received_committable_events = commit

    if received_committable_events:
        assert not process_commit_event._confirmed_events
        assert process_commit_event._last_sent_commit_event == 1
        assert process_commit_event._last_commit_time > initial_commit_time
    else:
        if not need_confirm:
            assert not process_commit_event._unconfirmed_events
            assert count in process_commit_event._confirmed_events
            assert process_commit_event._confirmed_events[count] == CommitEvent(
                count=count, need_confirm=False,
                resume_token=None
            )
            assert process_commit_event._last_sent_commit_event == 0
            assert process_commit_event._last_commit_time == initial_commit_time
        else:
            assert process_commit_event._unconfirmed_events[count] == CommitEvent(
                count=count, need_confirm=True,
                resume_token=actual_token
            )


@pytest.mark.parametrize(
    "old_token, new_token, actual_token",
    [
        (None, None, None),
        (b"test", None, b"test"),
        (None, b"test", b"test"),
        (b"test_1", b"test_2", b"test_2"),
    ],
)
def test_get_actual_token(
    process_commit_event: ProcessCommitEvent, old_token, new_token, actual_token
):
    assert actual_token == process_commit_event._get_actual_token(old_token, new_token)


def test_confirmed_with_error(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_commit_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")

    with pytest.raises(ValueError) as er:
        for commit in process_commit_event.process_event(event_1):
            assert commit.resume_token == b"test"
            assert commit.numbers == [1]
            assert count in process_commit_event._confirmed_events
            assert not process_commit_event._unconfirmed_events
            assert process_commit_event._last_commit_time == initial_commit_time
            assert process_commit_event._last_sent_commit_event == 0
            time.sleep(0.001)
            raise ValueError("test")

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_commit_time == initial_commit_time

def test_confirmed_confirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_commit_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=False, resume_token=b"test2")
    for commit in process_commit_event.process_event(event_1):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_commit_time == initial_commit_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1
    assert process_commit_event._last_commit_time > initial_commit_time

    for event in [event_1, event_2]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)


        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events


def test_unconfirmed_confirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_commit_time
    event_1 = CommitEvent(count=count, need_confirm=True, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=False, resume_token=b"test2")
    r = []
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert not r

    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events[count] == event_1

    for commit in process_commit_event.process_event(event_2):
        assert commit.resume_token == b"test2"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_commit_time == initial_commit_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1
    assert process_commit_event._last_commit_time > initial_commit_time

    for event in [event_1, event_2]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)


        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events


def test_unconfirmed_unconfirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    event_1 = CommitEvent(count=count, need_confirm=True, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")
    r = []
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert not r

    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events[count] == event_1

    for commit in process_commit_event.process_event(event_2):
        r.append(commit)

    assert not r

    assert not process_commit_event._confirmed_events
    assert process_commit_event._unconfirmed_events[count] == event_2


def test_confirmed_unconfirmed(process_commit_event: ProcessCommitEvent):
    count = 1
    initial_commit_time = process_commit_event._last_commit_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")

    for commit in process_commit_event.process_event(event_1):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_commit_time == initial_commit_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1
    assert process_commit_event._last_commit_time > initial_commit_time

    for event in [event_1, event_2]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)

        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events


def test_confirmed_unconfirmed_without_commit(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    initial_commit_time = process_commit_event._last_commit_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")

    r = []
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert not r

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_commit_time == initial_commit_time

    r = []
    for commit in process_commit_event.process_event(event_2):
        r.append(commit)

    assert not r

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_commit_time == initial_commit_time


def test_confirmed_unconfirmed_with_commit(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    count = 1
    initial_commit_time = process_commit_event._last_commit_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = CommitEvent(count=count, need_confirm=True, resume_token=b"test2")

    r = []
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert not r

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_commit_time == initial_commit_time

    process_commit_event._commit_interval = 0

    for commit in process_commit_event.process_event(event_2):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_commit_time == initial_commit_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1
    assert process_commit_event._last_commit_time > initial_commit_time

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
    initial_commit_time = process_commit_event._last_commit_time
    event_1 = CommitEvent(count=count, need_confirm=False, resume_token=b"test")
    event_2 = RecheckCommitEvent()

    r = []
    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert not r

    assert count in process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_commit_time == initial_commit_time

    process_commit_event._commit_interval = 0

    for commit in process_commit_event.process_event(event_2):
        assert commit.resume_token == b"test"
        assert commit.numbers == [1]
        assert count in process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events
        assert process_commit_event._last_commit_time == initial_commit_time
        assert process_commit_event._last_sent_commit_event == 0
        time.sleep(0.001)

    assert not process_commit_event._confirmed_events
    assert not process_commit_event._unconfirmed_events
    assert process_commit_event._last_sent_commit_event == 1
    assert process_commit_event._last_commit_time > initial_commit_time

    for event in [event_1, ]:
        additional_events = []
        for commit in process_commit_event.process_event(event):
            additional_events.append(commit)

        assert not additional_events
        assert not process_commit_event._confirmed_events
        assert not process_commit_event._unconfirmed_events


def test_clear_previous_commit_events(process_commit_event: ProcessCommitEvent):
    initial_commit_time = process_commit_event._last_commit_time
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
    assert process_commit_event._last_commit_time > initial_commit_time


@pytest.mark.parametrize(
    "confirmed_events, result",
    [
        (
            {
                1: CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
                2: CommitEvent(count=2, need_confirm=True, resume_token=b"test_2"),
            },
            None
        ),
        (
            {
                1: CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
                2: CommitEvent(count=2, need_confirm=False, resume_token=b"test_2"),
            },
            None
        ),
        (
            {
                1: CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
                2: CommitEvent(count=2, need_confirm=False, resume_token=b"test_2"),
            },
            CommittableEvents([1, 2], resume_token=b"test_2")
        ),
        (
            {
                2: CommitEvent(count=2, need_confirm=True, resume_token=b"test"),
            },
            None
        ),
        (
            {
                2: CommitEvent(count=2, need_confirm=False, resume_token=b"test"),
            },
            None
        ),
        (
            {
                1: CommitEvent(count=1, need_confirm=True, resume_token=b"test"),
            },
            None
        ),
        (
            {
                1: CommitEvent(count=1, need_confirm=False, resume_token=b"test"),
            },
            CommittableEvents([1], resume_token=b"test")
        ),
    ]
)
def test_get_committable_events(
    process_commit_event: ProcessCommitEvent,
    confirmed_events, result
):
    process_commit_event._confirmed_events = confirmed_events
    assert process_commit_event._get_committable_events() == result


def test_2_3_1(process_commit_event: ProcessCommitEvent):
    process_commit_event._commit_interval = 1000
    initial_commit_time = process_commit_event._last_commit_time

    event_2 = CommitEvent(count=2, need_confirm=True, resume_token=b"test")
    event_3 = CommitEvent(count=3, need_confirm=True, resume_token=b"test")
    event_1 = CommitEvent(count=1, need_confirm=True, resume_token=b"test")

    r = []
    for event in [event_2, event_3, event_1]:
        for commit in process_commit_event.process_event(event):
            r.append(commit)

    event_2 = CommitEvent(count=2, need_confirm=False, resume_token=b"test")
    event_3 = CommitEvent(count=3, need_confirm=False, resume_token=b"test")
    event_1 = CommitEvent(count=1, need_confirm=False, resume_token=b"test")

    for event in [event_2, event_3]:
        for commit in process_commit_event.process_event(event):
            r.append(commit)

    assert process_commit_event._last_sent_commit_event == 0
    assert process_commit_event._last_commit_time == initial_commit_time

    for commit in process_commit_event.process_event(event_1):
        r.append(commit)

    assert process_commit_event._last_sent_commit_event == 3
    assert process_commit_event._last_commit_time > initial_commit_time
