from datetime import datetime
from unittest.mock import Mock, call

import pytest
from pymongo.errors import PyMongoError, NetworkTimeout

from mongo_change_stream_mediator.change_stream_reading import RetrieveResumeToken
from tests.mocks.events import update, get_resume_token_from_event, to_raw_bson


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
def retrieve_resume_token_not_started(token_mongo_client_mock):
    return RetrieveResumeToken(
        stream_reader_name="test",
        token_mongo_client=token_mongo_client_mock,
        token_database='test-database',
        token_collection='TestCollection',
        on_timeout_retry_count=3,
    )


@pytest.fixture
def retrieve_resume_token(retrieve_resume_token_not_started):
    retrieve_resume_token_not_started.start()
    try:
        yield retrieve_resume_token_not_started
    finally:
        retrieve_resume_token_not_started.stop()


def test_start_stop(retrieve_resume_token_not_started, token_mongo_client_mock):
    token_mongo_client_mock.server_info.assert_not_called()

    retrieve_resume_token_not_started.start()
    token_mongo_client_mock.server_info.assert_called_once_with()

    retrieve_resume_token_not_started.start()
    assert len(token_mongo_client_mock.server_info.call_args_list) == 2

    token_mongo_client_mock.close.assert_not_called()
    retrieve_resume_token_not_started.stop()
    token_mongo_client_mock.close.assert_called_once_with()

    retrieve_resume_token_not_started.stop()
    assert len(token_mongo_client_mock.close.call_args_list) == 2
    assert len(token_mongo_client_mock.server_info.call_args_list) == 2


date = datetime.now()


@pytest.mark.parametrize(
    "token_data" , [
        dict(
            stream_reader_name='test',
            token=b'test',
            date=date
        ),
        dict(
            stream_reader_name='test',
            token=b'',
            date=date
        ),
    ]
)
def test_get_token_from_db_ok(
    retrieve_resume_token, token_mongo_client_mock, token_data, collection
):
    collection.find_one = Mock(return_value=token_data)
    assert retrieve_resume_token._get_token_from_db() == token_data


@pytest.mark.parametrize(
    "token_data" , [
        dict(
            stream_reader_name='test',
            token=b'test',
            date=date
        ),
        dict(
            stream_reader_name='test',
            token=b'',
            date=date
        ),
    ]
)
def test_get_token_from_db_ok(
    retrieve_resume_token, token_mongo_client_mock, token_data, collection
):
    collection.find_one = Mock(return_value=token_data)
    assert retrieve_resume_token._get_token_from_db() == token_data


@pytest.mark.parametrize(
    "token_data, result", [
        (
            dict(
                stream_reader_name='test',
                token=to_raw_bson(get_resume_token_from_event(update())).raw,
                date=date
            ),
            get_resume_token_from_event(update())
        ),
        (
            dict(
                stream_reader_name='test',
                token=b'',
                date=date
            ),
            None,
        ),
        (None, None),
    ]
)
def test_get_token_ok(
    retrieve_resume_token, token_mongo_client_mock, token_data, collection, result
):
    token_mongo_client_mock.close.assert_not_called()
    collection.find_one = Mock(return_value=token_data)

    assert retrieve_resume_token.get_token() == result

    token_mongo_client_mock.close.assert_called_once_with()


def test_get_token_with_error(
    retrieve_resume_token, token_mongo_client_mock, collection
):
    counter = 0

    def find_one(filter):
        nonlocal counter
        counter += 1
        raise PyMongoError()

    collection.find_one = Mock(side_effect=find_one)
    with pytest.raises(PyMongoError) as ex:
        retrieve_resume_token.get_token()

    collection.find_one.assert_called_once_with(
        filter={"stream_reader_name": retrieve_resume_token._stream_reader_name}
    )
    assert counter == 1


def test_get_token_with_timeout(
    retrieve_resume_token, token_mongo_client_mock, collection
):
    counter = 0

    def find_one(filter):
        nonlocal counter
        counter += 1
        raise NetworkTimeout()

    collection.find_one = Mock(side_effect=find_one)
    with pytest.raises(NetworkTimeout) as ex:
        retrieve_resume_token.get_token()

    call_args = dict(
        filter={"stream_reader_name": retrieve_resume_token._stream_reader_name}
    )
    collection.find_one.assert_has_calls(
        [call(**call_args), call(**call_args), call(**call_args)],
    )
    assert counter == 3


def test_get_token_from_second_try(
    retrieve_resume_token, token_mongo_client_mock, collection
):
    counter = 0

    def find_one(filter):
        nonlocal counter
        counter += 1
        if counter >= 2:
            return None
        else:
            raise NetworkTimeout()

    collection.find_one = Mock(side_effect=find_one)
    retrieve_resume_token.get_token()

    call_args = dict(
        filter={"stream_reader_name": retrieve_resume_token._stream_reader_name}
    )
    collection.find_one.assert_has_calls(
        [call(**call_args), call(**call_args)],
    )
    assert counter == 2
