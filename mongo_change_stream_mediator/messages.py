from json import dumps, loads

from pydantic import BaseModel

from .models import Statuses, ChangeStatuses
from .utils import check_command_format


def serialize_message(command: 'BaseMessage') -> dict:
    return dict(
        type=command.__class__.__name__,
        payload=command.dict()
    )


@check_command_format
def deserialize_message(command: dict) -> 'BaseMessage':
    command_type = command["type"]
    payload = command["payload"]
    command_class = globals()[command_type]
    if issubclass(command_class, BaseMessage):
        return command_class.parse_obj(payload)
    else:
        raise KeyError(command_type)


def encode_message(message: 'BaseMessage'):
    message_bytes = dumps(serialize_message(message)).encode()
    return message_bytes


def decode_message(message: bytes) -> 'BaseMessage':
    message = deserialize_message(loads(message.decode()))
    return message


class BaseMessage(BaseModel):
    task_id: int


class Status(BaseMessage):
    task_id: int
    status: Statuses


class ChangeStatus(BaseMessage):
    task_id: int
    status: ChangeStatuses
