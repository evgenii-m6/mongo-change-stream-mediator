import asyncio
import logging
import signal
from abc import ABC
from multiprocessing import Queue

import psutil

from .base_async_application import BaseAsyncApplication
from .exceptions import IncorrectManagerProcess, IncorrectReceiver, \
    UnexpectedMessageType, UnexpectedStatusChangeReceived, StopSubprocess
from .messages import (
    Status,
    ChangeStatus,
    encode_message,
    decode_message,
)
from .models import Statuses, ChangeStatuses
from concurrent.futures import ThreadPoolExecutor


class BaseAsyncWorker(ABC):
    def __init__(
        self,
        manager_pid: int,
        manager_create_time: float,
        task_id: int,
        request_queue: Queue,
        response_queue: Queue,
        application: BaseAsyncApplication,
        thread_pool_executor: ThreadPoolExecutor,
        queue_get_timeout: float = 1,
    ):
        self._loop = asyncio.get_running_loop()
        self._manager_pid = manager_pid
        self._manager_create_time = manager_create_time
        self._manager_process = psutil.Process(self._manager_pid)
        self._task_id = task_id
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._queue_get_timeout = queue_get_timeout
        self._application = application
        self._thread_pool_executor = thread_pool_executor

        self._should_run = False

        signal.signal(signal.SIGINT, self._handle_interruption_signal)
        signal.signal(signal.SIGTERM, self._handle_interruption_signal)

        self._validate_manager()

    def _validate_manager(self) -> None:
        ident1 = (self._manager_pid, self._manager_create_time)
        ident2 = (self._manager_process.pid, self._manager_process.create_time())
        if not ident1 == ident2:
            raise IncorrectManagerProcess(
                pid=self._manager_pid,
                create_time=self._manager_create_time,
                process=self._manager_process
            )

    def _handle_interruption_signal(self, signum, frame):
        self._should_run = False
        self._application.exit_gracefully(signum, frame)

    @property
    def worker_name(self):
        return f"{type(self).__name__}(task_id={self._task_id})"

    async def _start(self):
        logging.info(f"Start worker {self.worker_name}")
        await self._application.start()
        logging.info(f"{self.worker_name} worker started")

    async def _stop(self):
        logging.info(f"Stop worker {self.worker_name}")
        self._should_run = False
        await self._application.stop()
        logging.info(f"{self.worker_name} worker stopped")

    async def _task(self):
        await self._application.task()

    async def _send_status(self, status: Statuses):
        message = Status(task_id=self._task_id, status=status)
        message_bytes = encode_message(message)

        def put():
            self._response_queue.put(message_bytes)

        future = self._loop.run_in_executor(self._thread_pool_executor, put)
        await future

    def _get_change_status(self, data: bytes) -> ChangeStatuses:
        message = decode_message(data)

        if not isinstance(message, ChangeStatus):
            raise UnexpectedMessageType(message_type=ChangeStatus, received=message)

        if message.task_id != self._task_id:
            raise IncorrectReceiver(task_id=self._task_id, received=message)

        return message.status

    async def _run(self):
        self._should_run = True
        logging.info(f"{self.worker_name} send status starting")
        await self._send_status(status=Statuses.starting)
        logging.info(f"{self.worker_name} wait for change status to: started")
        await self._wait_for_change_status_command(ChangeStatuses.started)
        logging.info(f"{self.worker_name} are starting")
        await self._start()
        logging.info(f"{self.worker_name} are started. Send status: started.")
        await self._send_status(status=Statuses.started)
        logging.info(f"{self.worker_name} wait for change status to: running")
        await self._wait_for_change_status_command(ChangeStatuses.running)
        logging.info(f"{self.worker_name} send status running")
        await self._send_status(status=Statuses.running)
        logging.info(f"{self.worker_name} are running")
        await self._task()

    async def run(self):
        logging.info(f"{self.worker_name} worker run")
        try:
            await self._run()
            logging.warning(f"Normal exit from {self.worker_name}")
        except BaseException as ex:
            logging.error(
                f"Error in {self.worker_name}: {ex!r}.",
                stack_info=True
            )
        finally:
            try:
                await self._stop()
            except BaseException as err:
                logging.error(
                    f"Error when EXIT in {self.worker_name}: {err}",
                    stack_info=True
                )
                raise err
        logging.info(f"{self.worker_name} worker stopped")

    async def _wait_for_change_status_command(self, status: ChangeStatuses):
        def get():
            result = self._request_queue.get(timeout=self._queue_get_timeout)
            return result

        while self._should_run and self._manager_process.is_running():
            future = self._loop.run_in_executor(self._thread_pool_executor, get)
            message_bytes = await future

            if message_bytes:
                received_status = self._get_change_status(message_bytes)

                if received_status == ChangeStatuses.stopped:
                    raise StopSubprocess(self._task_id)

                if received_status != status:
                    raise UnexpectedStatusChangeReceived(
                        expected=status,
                        received=received_status,
                        task_id=self._task_id,
                    )
                else:
                    break

        if not (self._should_run and self._manager_process.is_running()):
            raise StopSubprocess(self._task_id)
