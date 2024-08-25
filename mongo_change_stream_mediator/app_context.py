import asyncio
import logging
from abc import abstractmethod
from asyncio import Protocol
from contextlib import contextmanager, asynccontextmanager
from multiprocessing import Queue
from typing import Callable, Awaitable

from .base_worker import BaseWorker
from .base_async_worker import BaseAsyncWorker
from .messages import (
    Status,
    encode_message,
)
from .models import Statuses


class ServiceApplicationProtocol(Protocol):
    @abstractmethod
    def run(self):
        raise NotImplementedError


class ApplicationContext:
    build_worker: Callable[..., BaseWorker]
    build_async_worker: Callable[..., Awaitable[BaseAsyncWorker]]

    def __init__(
        self,
        task_id: int,
        response_queue: Queue,
    ):
        self._task_id = task_id
        self._response_queue = response_queue

    @classmethod
    def run_application(cls, task_id: int, response_queue: Queue, **kwargs):
        appcontext = cls(task_id=task_id, response_queue=response_queue)
        with appcontext._context():
            worker = appcontext.build_worker(
                task_id=task_id,
                response_queue=response_queue,
                **kwargs
            )
            worker.run()

    @contextmanager
    def _context(self):
        try:
            yield
        except BaseException as ex:
            logging.error(
                f"Error in task {self._task_id}: {ex!r}"
            )
        finally:
            self._send_stopped()

    @classmethod
    def run_async_application(cls, task_id: int, response_queue: Queue, **kwargs):
        appcontext = cls(task_id=task_id, response_queue=response_queue)
        asyncio.run(appcontext._run_async_application(**kwargs))

    async def _run_async_application(self, **kwargs):
        async with self._async_context():
            worker = await self.build_async_worker(
                task_id=self._task_id,
                response_queue=self._response_queue,
                **kwargs
            )
            await worker.run()

    @asynccontextmanager
    async def _async_context(self):
        try:
            yield
        except BaseException as ex:
            logging.error(
                f"Error in task {self._task_id}: {ex!r}"
            )
        finally:
            self._send_stopped()

    def _send_stopped(self):
        logging.warning(
            f"Send stopped status to response queue from task: {self._task_id}"
        )
        message = Status(task_id=self._task_id, status=Statuses.stopped)
        message_bytes = encode_message(message)
        try:
            self._response_queue.put(message_bytes)
        except BaseException as ex:
            logging.error(
                f"Error when send stopped status "
                f"to response queue from task {self._task_id}: {ex!r}"
            )
        else:
            logging.warning(
                f"Successfully sent stopped status "
                f"to response queue from task {self._task_id}"
            )
