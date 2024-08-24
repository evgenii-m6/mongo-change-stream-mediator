import logging
from multiprocessing import Queue
from queue import Empty
from typing import Iterator

from pymongo import MongoClient

from mongo_change_stream_mediator.base_application import BaseApplication
from mongo_change_stream_mediator.commit_event_decoder import decode_commit_event
from mongo_change_stream_mediator.models import (
    CommitEvent, RecheckCommitEvent,
)
from .commit_event_handler import CommitEventHandler


class CommitFlow(BaseApplication):
    _token_mongo_client_cls = MongoClient

    def __init__(
        self,
        committer_queue: Queue,
        commit_event_handler: CommitEventHandler,
        queue_get_timeout: float = 10,
    ):
        super().__init__()
        self._commit_event_handler = commit_event_handler
        self._committer_queue = committer_queue
        self._queue_get_timeout = queue_get_timeout

    def exit_gracefully(self, signum, frame):
        self._should_run = False

    def _start_dependencies(self):
        self._commit_event_handler.start()

    def _stop_dependencies(self):
        self._commit_event_handler.stop()

    def task(self):
        while self._should_run:
            self._get_change_event_and_process()

    def _get_change_event_and_process(self) -> None:
        for event in self.iter_event():
            self._commit_event_handler.handle_event(event)

    def iter_event(self) -> Iterator[CommitEvent | RecheckCommitEvent]:
        try:
            result = self._committer_queue.get(
                timeout=self._queue_get_timeout
            )
        except Empty as ex:
            logging.debug("Got RecheckCommitEvent")
            yield RecheckCommitEvent()
        else:
            if result:
                event = decode_commit_event(result)
                logging.debug(
                    f"Got CommitEvent("
                    f"count={event.count},"
                    f"need_confirm={event.need_confirm})"
                )
                yield event
