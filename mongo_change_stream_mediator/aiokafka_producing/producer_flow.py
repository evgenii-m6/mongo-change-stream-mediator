import asyncio
import logging
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor
from queue import Empty
from typing import AsyncIterator

from mongo_change_stream_mediator.base_async_application import BaseAsyncApplication
from mongo_change_stream_mediator.models import DecodedChangeEvent
from mongo_change_stream_mediator.change_event_decoder import decode_change_event
from .change_event_handler import ChangeEventHandler, EventHandlerNotRunning


class ProducerFlow(BaseAsyncApplication):
    def __init__(
        self,
        producer_queue: Queue,
        event_handler: ChangeEventHandler,
        thread_pool_executor: ThreadPoolExecutor,
        queue_get_timeout: float = 1,
        max_internal_queue_size: int = 10000
    ):
        super().__init__()
        self._loop = asyncio.get_running_loop()
        self._thread_pool_executor = thread_pool_executor
        self._producer_queue = producer_queue
        self._queue_get_timeout = queue_get_timeout
        self._event_handler = event_handler
        self._max_internal_queue_size = max_internal_queue_size

    def exit_gracefully(self, signum, frame):
        self._should_run = False
        self._event_handler.exit_gracefully()

    async def _start_dependencies(self):
        await self._event_handler.start()

    async def _stop_dependencies(self):
        await self._event_handler.stop()

    async def task(self):
        try:
            await self._task()
        except EventHandlerNotRunning as ex:
            if self._should_run:
                raise

    async def _task(self):
        while self._should_run:
            await self._get_change_event_and_process()

    async def _get_change_event_and_process(self):
        internal_queue_size = self._event_handler.internal_queue_size()
        if internal_queue_size > self._max_internal_queue_size:
            wait_interval = 1
            logging.warning(
                f"Internal queue size limit exceed "
                f"{internal_queue_size}>{self._max_internal_queue_size}."
                f"Wait {wait_interval}s."
            )
            await asyncio.sleep(wait_interval)
        else:
            async for event in self._iter_change_event():
                await self._event_handler.handle(event)

    def _producer_queue_get(self):
        try:
            result = self._producer_queue.get(timeout=self._queue_get_timeout)
        except Empty as ex:
            result = None
        return result

    async def _iter_change_event(self) -> AsyncIterator[DecodedChangeEvent]:
        future = self._loop.run_in_executor(
            executor=self._thread_pool_executor,
            func=self._producer_queue_get
        )
        result = await future
        self._event_handler.log_statistics()

        if result:
            event = decode_change_event(result)
            yield event
