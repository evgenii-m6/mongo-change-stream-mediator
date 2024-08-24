from typing import Type

import psutil

from .messages import BaseMessage
from .models import ChangeStatuses, Statuses


class BaseApplicationError(Exception):
    def __str__(self):
        repr(self)


class WaitTimeoutError(BaseApplicationError):
    ...


class IncorrectManagerProcess(BaseApplicationError):
    def __init__(self, pid: int, create_time: float, process: psutil.Process):
        self.pid = pid
        self.create_time = create_time
        self.process = process

    def __repr__(self):
        return (
            f"Wrong parent process. "
            f"Expected Process(pid={self.pid}, create_time={self.create_time}), got "
            f"Process("
                f"pid={self.process.pid}, create_time={self.process.create_time()}"
            f")"
        )


class IncorrectReceiver(BaseApplicationError):
    def __init__(self, task_id: int, received: BaseMessage):
        self.task_id = task_id
        self.received = received

    def __repr__(self):
        return (
            f"Wrong receiver of message. "
            f"Expected receiver task_id={self.task_id}, "
            f"but received message: {self.received!r}"
        )


class UnexpectedMessageType(BaseApplicationError):
    def __init__(self, message_type: Type[BaseMessage], received: BaseMessage):
        self.message_type = message_type
        self.received = received

    def __repr__(self):
        return (
            f"Unexpected message received "
            f"Expected message of type message_type={self.message_type.__name__}, "
            f"but received message: {self.received!r}"
        )


class UnexpectedStatusChangeReceived(BaseApplicationError):
    def __init__(
        self, expected: ChangeStatuses, received: ChangeStatuses, task_id: int
    ):
        self.expected = expected
        self.received = received
        self.task_id = task_id

    def __repr__(self):
        return (
            f"Unexpected ChangeStatuses received from task_id={self.task_id}"
            f"Expected: {self.expected}, "
            f"Received: {self.received}"
        )


class UnexpectedStatusReceived(BaseApplicationError):
    def __init__(self, expected: Statuses, task_id, received: Statuses):
        self.expected = expected
        self.received = received
        self.task_id = task_id

    def __repr__(self):
        return (
            f"Unexpected Statuses received from task_id={self.task_id}"
            f"Expected: {self.expected}, "
            f"Received: {self.received}"
        )


class StopSubprocess(BaseApplicationError):
    def __init__(self, task_id: int):
        self.task_id = task_id

    def __repr__(self):
        return (
            f"Exit from task_id={self.task_id}"
        )


class SubprocessStoppedUnexpectedly(BaseApplicationError):
    def __init__(self, task_id: int):
        self.task_id = task_id

    def __repr__(self):
        return (
            f"Subprocess stopped unexpectedly. task_id={self.task_id}"
        )
