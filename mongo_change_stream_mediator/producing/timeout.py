from threading import Thread
from typing import Callable, TypeVar, Generic


T = TypeVar('T')


class Future:
    def __init__(self):
        self._data = {}

    def set_result(self, result):
        if not self._data:
            self._data['result'] = result
        else:
            ValueError("Can't set result or exception twice")

    def set_exception(self, exception: BaseException):
        if not self._data:
            self._data['exception'] = exception
        else:
            ValueError("Can't set result or exception twice")

    def is_set(self) -> bool:
        if self._data:
            return True
        else:
            return False

    def result(self):
        if self.is_set():
            if 'exception' in self._data:
                raise self._data['exception']
            if 'result' in self._data:
                return self._data['result']
        else:
            raise ValueError("Result or exception are not set")


class OperationTimeout(Exception):
    ...


class WithTimeout(Generic[T]):
    def __init__(self, target: Callable[..., T], timeout: float | None = None):
        self._func = target
        self._timeout = timeout
        self.thread = Thread(target=self._task, daemon=True)
        self._future = Future()

    def run(self) -> T:
        self.thread.start()
        self.thread.join(self._timeout)
        if self._future.is_set():
            return self._future.result()
        else:
            raise OperationTimeout(self._func.__name__, self._timeout)

    def _task(self):
        try:
            r = self._func()
            self._future.set_result(r)
        except BaseException as ex:
            self._future.set_exception(ex)
