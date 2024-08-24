from multiprocessing import Queue
from queue import Empty
from typing import Iterator

from mongo_change_stream_mediator.base_application import BaseApplication
from mongo_change_stream_mediator.models import DecodedChangeEvent
from mongo_change_stream_mediator.change_event_decoder import decode_change_event
from .change_event_handler import ChangeEventHandler, EventHandlerNotRunning


class ProducerFlow(BaseApplication):
    def __init__(
        self,
        producer_queue: Queue,
        event_handler: ChangeEventHandler,
        queue_get_timeout: float = 1,
    ):
        super().__init__()
        self._producer_queue = producer_queue
        self._queue_get_timeout = queue_get_timeout
        self._event_handler = event_handler

    def exit_gracefully(self, signum, frame):
        self._should_run = False
        self._event_handler.exit_gracefully()

    def _start_dependencies(self):
        self._event_handler.start()

    def _stop_dependencies(self):
        self._event_handler.stop()

    def task(self):
        try:
            self._task()
        except EventHandlerNotRunning as ex:
            if self._should_run:
                raise

    def _task(self):
        while self._should_run:
            self._get_change_event_and_process()

    def _get_change_event_and_process(self):
        for event in self._iter_change_event():
            self._event_handler.handle(event)

    def _iter_change_event(self) -> Iterator[DecodedChangeEvent]:
        try:
            result = self._producer_queue.get(timeout=self._queue_get_timeout)
        except Empty as ex:
            result = None

        self._event_handler.log_statistics()

        if result:
            event = decode_change_event(result)
            yield event
