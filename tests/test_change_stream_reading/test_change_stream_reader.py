from unittest.mock import Mock

import pytest

from mongo_change_stream_mediator.change_stream_reading import ChangeStreamReader


@pytest.fixture
def token_retriever():
    retriever = Mock()
    retriever.start = Mock()
    retriever.stop = Mock()
    retriever.get_token = Mock(return_value={"test": "test"})
    return retriever


@pytest.fixture
def watcher():
    def iter(resume_token):
        for i in ["value"]:
            yield i

    watcher = Mock()
    watcher.start = Mock()
    watcher.stop = Mock()
    watcher.exit_gracefully = Mock()
    watcher.iter = Mock(side_effect=iter)
    return watcher


@pytest.fixture
def change_handler():
    handler = Mock()
    handler.handle = Mock()
    return handler


@pytest.fixture
def change_stream_reader_not_started(token_retriever, watcher, change_handler):
    return ChangeStreamReader(
        token_retriever=token_retriever,
        watcher=watcher,
        change_handler=change_handler
    )


@pytest.fixture
def change_stream_reader(change_stream_reader_not_started):
    change_stream_reader_not_started.start()
    try:
        yield change_stream_reader_not_started
    finally:
        change_stream_reader_not_started.stop()


def test_task_ok(change_stream_reader, token_retriever, watcher, change_handler):
    token_retriever.get_token.assert_not_called()
    watcher.iter.assert_not_called()
    change_handler.handle.assert_not_called()

    change_stream_reader.task()

    token_retriever.get_token.assert_called_once_with()
    watcher.iter.assert_called_once_with({"test": "test"})
    change_handler.handle.assert_called_once_with("value")


def test_error_in_get_token(
    change_stream_reader, token_retriever, watcher, change_handler
):
    token_retriever.get_token = Mock(side_effect=BaseException())
    token_retriever.get_token.assert_not_called()
    watcher.iter.assert_not_called()
    change_handler.handle.assert_not_called()

    with pytest.raises(BaseException):
        change_stream_reader.task()

    token_retriever.get_token.assert_called_once_with()
    watcher.iter.assert_not_called()
    change_handler.handle.assert_not_called()


def test_error_in_iter_watcher(
    change_stream_reader, token_retriever, watcher, change_handler
):
    def iter(resume_token):
        for i in ["value"]:
            raise BaseException

    watcher.iter = Mock(side_effect=iter)
    token_retriever.get_token.assert_not_called()
    watcher.iter.assert_not_called()
    change_handler.handle.assert_not_called()

    with pytest.raises(BaseException):
        change_stream_reader.task()

    token_retriever.get_token.assert_called_once_with()
    watcher.iter.assert_called_once_with({"test": "test"})
    change_handler.handle.assert_not_called()


def test_error_in_handle(
    change_stream_reader, token_retriever, watcher, change_handler
):

    change_handler.handle = Mock(side_effect=BaseException())
    token_retriever.get_token.assert_not_called()
    watcher.iter.assert_not_called()
    change_handler.handle.assert_not_called()

    with pytest.raises(BaseException):
        change_stream_reader.task()

    token_retriever.get_token.assert_called_once_with()
    watcher.iter.assert_called_once_with({"test": "test"})
    change_handler.handle.assert_called_once_with("value")


def test_start_stop(
    change_stream_reader_not_started, token_retriever, watcher, change_handler
):
    token_retriever.start.assert_not_called()
    watcher.start.assert_not_called()

    change_stream_reader_not_started.start()

    token_retriever.start.assert_called_once_with()
    watcher.start.assert_called_once_with()

    change_stream_reader_not_started.start()

    assert len(token_retriever.start.call_args_list) == 2
    assert len(watcher.start.call_args_list) == 2

    token_retriever.stop.assert_not_called()
    watcher.stop.assert_not_called()

    change_stream_reader_not_started.stop()
    token_retriever.stop.assert_called_once_with()
    watcher.stop.assert_called_once_with()

    change_stream_reader_not_started.stop()
    assert len(token_retriever.stop.call_args_list) == 2
    assert len(watcher.stop.call_args_list) == 2

    watcher.exit_gracefully.assert_not_called()
    assert len(token_retriever.start.call_args_list) == 2
    assert len(watcher.start.call_args_list) == 2


def test_start_and_exit_gracefully(
    change_stream_reader, token_retriever, watcher, change_handler
):
    token_retriever.start.assert_called_once_with()
    watcher.start.assert_called_once_with()
    watcher.exit_gracefully.assert_not_called()

    change_stream_reader.exit_gracefully(1,2)
    watcher.exit_gracefully.assert_called_once_with()

    change_stream_reader.exit_gracefully(1,2)
    assert len(watcher.exit_gracefully.call_args_list) == 2

    watcher.stop.assert_not_called()
    token_retriever.stop.assert_not_called()
