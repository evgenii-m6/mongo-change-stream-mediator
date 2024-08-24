import functools
import threading

from .models import Message


def check_command_format(func):
    @functools.wraps(func)
    def wrapper(command: dict) -> dict:
        message = Message.parse_obj(command)
        return func(message.dict())
    return wrapper


class TaskIdGenerator:
    counter: int = 1
    _lock = threading.Lock()

    @classmethod
    def get(cls) -> int:
        cls._lock.acquire()
        try:
            cls.counter += 1
            counter = cls.counter
        finally:
            cls._lock.release()
        return counter
