from datetime import datetime
from enum import Enum
from multiprocessing import Process
from typing import NamedTuple, Any

from bson.raw_bson import RawBSONDocument
from pydantic import BaseModel


class SavedToken(BaseModel):
    stream_reader_name: str
    token: bytes
    date: datetime


class CommitEvent(NamedTuple):
    count: int
    need_confirm: bool
    resume_token: bytes | None


class RecheckCommitEvent:
    def __repr__(self):
        return "RecheckCommitEvent"

    def __str__(self):
        return repr(self)


class CommittableEvents(NamedTuple):
    numbers: list[int]
    resume_token: bytes


class ChangeEvent(NamedTuple):
    bson_document: RawBSONDocument | None
    token: RawBSONDocument | None
    count: int


class DecodedChangeEvent(NamedTuple):
    bson_document: dict[str, Any]
    count: int


class Message(BaseModel):
    type: str
    payload: dict


class Statuses(str, Enum):
    starting = "starting"
    started = "started"
    running = "running"
    stopped = "stopped"


class ChangeStatuses(str, Enum):
    started = "started"
    running = "running"
    stopped = "stopped"


class ProcessData(NamedTuple):
    task_id: int
    process: Process
    kwargs: dict
