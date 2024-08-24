from datetime import datetime
from unittest.mock import Mock, call

import pytest
from bson import Binary
from pymongo.errors import PyMongoError, NetworkTimeout

from mongo_change_stream_mediator.committing import TokenSaving
from mongo_change_stream_mediator.models import SavedToken


@pytest.fixture
def collection():
    collection = Mock()
    collection.replace_one = Mock()
    collection.create_index = Mock()
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
def token_saver_not_started(token_mongo_client_mock):
    saver = TokenSaving(
        token_mongo_client=token_mongo_client_mock,
        token_database='test-database',
        token_collection='TestCollection'
    )
    return saver


@pytest.fixture
def token_saver(token_saver_not_started):
    token_saver_not_started.start()
    try:
        yield token_saver_not_started
    finally:
        token_saver_not_started.stop()


def test_start(token_saver_not_started, token_mongo_client_mock, collection):
    token_saver_not_started.start()
    token_mongo_client_mock.server_info.assert_called_once_with()
    collection.create_index.assert_called_once_with(
        keys="stream_reader_name",
        name="stream_reader_name",
        unique=True,
    )


def test_stop(token_saver, token_mongo_client_mock):
    token_saver.stop()
    token_mongo_client_mock.close.assert_called_once_with()


def test_transform_to_dict(token_saver):
    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()
    assert token_saver._transform_to_dict(
        SavedToken(
            stream_reader_name=stream_reader_name,
            token=token,
            date=dt,
        )
    ) == {
       'stream_reader_name': stream_reader_name,
       'token': Binary(token, subtype=0),
       'date': dt
    }


def test_normal_save(token_saver, collection):
    counter = 0

    def replace_one(filter, replacement, upsert):
        nonlocal counter
        counter += 1

    collection.replace_one = Mock(side_effect=replace_one)

    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)

    token_saver.save(saved_token)
    collection.replace_one.assert_called_once_with(
        filter={"stream_reader_name": replacement["stream_reader_name"]},
        replacement=replacement,
        upsert=True
    )
    assert counter == 1


def test_cannot_save(token_saver, collection):
    counter = 0

    def replace_one(filter, replacement, upsert):
        nonlocal counter
        counter += 1
        raise PyMongoError()

    collection.replace_one = Mock(side_effect=replace_one)

    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)
    with pytest.raises(PyMongoError) as ex:
        token_saver.save(saved_token)

    collection.replace_one.assert_called_once_with(
        filter={"stream_reader_name": replacement["stream_reader_name"]},
        replacement=replacement,
        upsert=True
    )
    assert counter == 1


def test_timeout_when_save(token_saver, collection):
    counter = 0

    def replace_one(filter, replacement, upsert):
        nonlocal counter
        counter += 1
        raise NetworkTimeout()

    collection.replace_one = Mock(side_effect=replace_one)

    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)
    with pytest.raises(NetworkTimeout) as ex:
        token_saver.save(saved_token)

    call_args = dict(
        filter={"stream_reader_name": replacement["stream_reader_name"]},
        replacement=replacement,
        upsert=True
    )
    collection.replace_one.assert_has_calls(
        [call(**call_args), call(**call_args), call(**call_args)],
    )
    assert counter == 3


def test_save_from_second_try(token_saver, collection):
    counter = 0

    def replace_one(filter, replacement, upsert):
        nonlocal counter
        counter += 1
        if counter >= 2:
            return
        else:
            raise NetworkTimeout()

    collection.replace_one = Mock(side_effect=replace_one)

    stream_reader_name = 'test-stream-reader-name'
    token = b"test"
    dt = datetime.now()

    saved_token = SavedToken(
        stream_reader_name=stream_reader_name,
        token=token,
        date=dt,
    )
    replacement = token_saver._transform_to_dict(saved_token)
    token_saver.save(saved_token)

    call_args = dict(
        filter={"stream_reader_name": replacement["stream_reader_name"]},
        replacement=replacement,
        upsert=True
    )
    collection.replace_one.assert_has_calls(
        [call(**call_args), call(**call_args)],
    )
    assert counter == 2
