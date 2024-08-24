from unittest.mock import Mock

import pytest

from mongo_change_stream_mediator.change_stream_reading import ChangeStreamWatch
from mongo_change_stream_mediator.change_stream_reading.watch import (
    DatabaseWasNotSet,
    CounterMaxValueExceedError, ChangeStreamWatcherWasNotStarted,
)
from mongo_change_stream_mediator.models import ChangeEvent
from tests.mocks.events import (
    update,
    to_raw_bson,
    events_raw_bson,
    get_resume_token_from_raw_event,
    insert,
)


@pytest.fixture
def collection():
    collection = Mock()
    collection.find_one = Mock()
    return collection


@pytest.fixture
def database(collection):
    database = Mock()
    database.get_collection = Mock(return_value=collection)
    return database


@pytest.fixture
def token_mongo_client_mock(database):
    client = Mock()
    client.server_info = Mock()
    client.close = Mock()
    client.get_database = Mock(return_value=database)
    return client


@pytest.fixture
def change_stream_watch_not_started(token_mongo_client_mock):
    return ChangeStreamWatch(
        mongo_client=token_mongo_client_mock,
        sleep_time_between_empty_events=0.001,
    )


@pytest.fixture
def change_stream_watch(change_stream_watch_not_started):
    change_stream_watch_not_started.start()
    try:
        yield change_stream_watch_not_started
    finally:
        change_stream_watch_not_started.stop()


def test_iter_not_started(change_stream_watch_not_started):
    with pytest.raises(ChangeStreamWatcherWasNotStarted):
        for i in change_stream_watch_not_started.iter(None):
            break


def test_start_stop(change_stream_watch_not_started, token_mongo_client_mock):
    assert not change_stream_watch_not_started._should_run
    token_mongo_client_mock.server_info.assert_not_called()

    change_stream_watch_not_started.start()
    assert change_stream_watch_not_started._should_run
    token_mongo_client_mock.server_info.assert_called_once_with()

    change_stream_watch_not_started.start()
    assert change_stream_watch_not_started._should_run
    assert len(token_mongo_client_mock.server_info.call_args_list) == 2

    token_mongo_client_mock.close.assert_not_called()
    change_stream_watch_not_started.stop()
    assert not change_stream_watch_not_started._should_run
    token_mongo_client_mock.close.assert_called_once_with()

    change_stream_watch_not_started.stop()
    assert not change_stream_watch_not_started._should_run
    assert len(token_mongo_client_mock.close.call_args_list) == 2
    assert len(token_mongo_client_mock.server_info.call_args_list) == 2


def test_start_and_exit_gracefully(
    change_stream_watch, token_mongo_client_mock
):
    token_mongo_client_mock.server_info.assert_called_once_with()
    change_stream_watch.exit_gracefully()
    assert not change_stream_watch._should_run
    token_mongo_client_mock.close.assert_not_called()


def test_get_watcher(
    change_stream_watch, database, collection, token_mongo_client_mock
):
    change_stream_watch._collection = None
    change_stream_watch._database = None

    assert change_stream_watch._get_watcher() is token_mongo_client_mock

    change_stream_watch._database = 'test'
    assert change_stream_watch._get_watcher() is database

    change_stream_watch._collection = 'Test'
    assert change_stream_watch._get_watcher() is collection

    change_stream_watch._collection = 'Bad'
    change_stream_watch._database = None

    with pytest.raises(DatabaseWasNotSet):
        change_stream_watch._get_watcher()

    with pytest.raises(DatabaseWasNotSet):
        for event in change_stream_watch.iter(None):
            break


def get_change_event(iterator) -> ChangeEvent:
    return [i for i in iterator][0]


@pytest.mark.parametrize("events", [
    [
        dict(
            change=to_raw_bson(insert()),
            resume_token=get_resume_token_from_raw_event(to_raw_bson(insert()))
        ),
        dict(
            change=to_raw_bson(update()),
            resume_token=None,
        ),
    ],
    [
        dict(
            change=to_raw_bson(update()),
            resume_token=None,
        ),
        dict(
            change=to_raw_bson(insert()),
            resume_token=get_resume_token_from_raw_event(to_raw_bson(insert()))
        ),
    ],
    [
        dict(
            change=None,
            resume_token=None,
        ),
        dict(
            change=to_raw_bson(insert()),
            resume_token=get_resume_token_from_raw_event(to_raw_bson(insert()))
        ),
    ],
    [
        dict(
            change=to_raw_bson(insert()),
            resume_token=get_resume_token_from_raw_event(to_raw_bson(insert()))
        ),
        dict(
            change=None,
            resume_token=None,
        ),
    ]
])
def test_iter_change_with_and_without_token(change_stream_watch, events):
    assert change_stream_watch._counter == 0
    assert change_stream_watch._last_resume_token is None

    counter = 0
    for i, event_dict in enumerate(events*2, start=1):
        if event_dict['resume_token'] or event_dict['change']:
            counter += 1

        previous_resume_token = change_stream_watch._last_resume_token
        change_events = list(change_stream_watch._iter_change_event(**event_dict))
        if change_events:
            change_event = change_events[0]
            expected_change_event = ChangeEvent(
                bson_document=event_dict['change'],
                token=event_dict['resume_token'],
                count=counter
            )
            assert expected_change_event == change_event
            assert change_stream_watch._counter == counter

            if event_dict['resume_token']:
                assert change_stream_watch._last_resume_token == event_dict['resume_token']
            else:
                assert change_stream_watch._last_resume_token == previous_resume_token


def test_iter_change_event_no_token(change_stream_watch):
    assert change_stream_watch._counter == 0
    assert change_stream_watch._last_resume_token is None

    events = events_raw_bson()
    for i, raw_event in enumerate(events, start=1):
        change_event = get_change_event(
            change_stream_watch._iter_change_event(
                change=raw_event, resume_token=None
            )
        )
        expected_change_event = ChangeEvent(
            bson_document=raw_event,
            token=None,
            count=i
        )
        assert expected_change_event == change_event
        assert change_stream_watch._counter == i
        assert change_stream_watch._last_resume_token is None


def test_iter_change_event_full(change_stream_watch):
    assert change_stream_watch._counter == 0
    assert change_stream_watch._last_resume_token is None

    events = events_raw_bson()
    for i, raw_event in enumerate(events, start=1):
        resume_token = get_resume_token_from_raw_event(raw_event)

        change_event = get_change_event(
            change_stream_watch._iter_change_event(
                change=raw_event,
                resume_token=resume_token
            )
        )
        expected_change_event = ChangeEvent(
            bson_document=raw_event,
            token=resume_token,
            count=i
        )
        assert expected_change_event == change_event
        assert change_stream_watch._counter == i
        assert change_stream_watch._last_resume_token == resume_token


def test_iter_change_event_no_change(change_stream_watch):
    assert change_stream_watch._counter == 0
    assert change_stream_watch._last_resume_token is None

    events = events_raw_bson()
    for i, raw_event in enumerate(events, start=1):
        resume_token = get_resume_token_from_raw_event(raw_event)

        change_event = get_change_event(
            change_stream_watch._iter_change_event(
                change=None,
                resume_token=resume_token
            )
        )
        expected_change_event = ChangeEvent(
            bson_document=None,
            token=resume_token,
            count=i
        )
        assert expected_change_event == change_event
        assert change_stream_watch._counter == i
        assert change_stream_watch._last_resume_token == resume_token


def test_iter_change_event_empty(change_stream_watch):
    assert change_stream_watch._counter == 0
    assert change_stream_watch._last_resume_token is None

    assert not list(
        change_stream_watch._iter_change_event(
        change=None,
        resume_token=None
    ))

    assert change_stream_watch._counter == 0
    assert change_stream_watch._last_resume_token is None


def test_event_validation(change_stream_watch):
    change_stream_watch._counter = 18446744073709551615 - 1

    change_events = list(change_stream_watch._iter_change_event(
        change=to_raw_bson(insert()),
        resume_token=get_resume_token_from_raw_event(to_raw_bson(insert()))
    ))

    with pytest.raises(CounterMaxValueExceedError):
        list(change_stream_watch._iter_change_event(
            change=to_raw_bson(insert()),
            resume_token=get_resume_token_from_raw_event(to_raw_bson(insert()))
        ))

    assert change_stream_watch._counter == 18446744073709551615 + 1
