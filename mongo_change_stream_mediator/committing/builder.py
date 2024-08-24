from multiprocessing import Process, Queue
from typing import Type

from pymongo import MongoClient

from mongo_change_stream_mediator.settings import Settings
from mongo_change_stream_mediator.utils import TaskIdGenerator
from mongo_change_stream_mediator.models import ProcessData
from mongo_change_stream_mediator.app_context import ApplicationContext
from mongo_change_stream_mediator.base_worker import BaseWorker
from .commit_processing import ProcessCommitEvent
from .commit_event_handler import CommitEventHandler
from .commit_flow import CommitFlow
from .token_saver import TokenSaving


def build_commit_process(
    application_context: Type[ApplicationContext],
    manager_pid: int,
    manager_create_time: float,
    task_id_generator: TaskIdGenerator,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    settings: Settings,
) -> ProcessData:
    task_id = task_id_generator.get()
    kwargs = {
        'manager_pid': manager_pid,
        'manager_create_time': manager_create_time,
        'task_id': task_id,
        'request_queue': request_queue,
        'response_queue': response_queue,
        'committer_queue': committer_queue,
        'stream_reader_name': settings.stream_reader_name,
        'token_mongo_uri': settings.token_mongo_uri,
        'token_database': settings.token_database,
        'token_collection': settings.token_collection,
        'token_operation_timeout': settings.token_operation_timeout,
        'token_operation_max_retry': settings.token_operation_max_retry,
        'commit_interval': settings.commit_interval,
        'queue_get_timeout': settings.queue_get_timeout,
    }
    process = Process(target=application_context.run_application, kwargs=kwargs)
    return ProcessData(task_id=task_id, process=process, kwargs=kwargs)


def build_commit_worker(
    manager_pid: int,
    manager_create_time: float,
    task_id: int,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    stream_reader_name: str,
    token_mongo_uri: str,
    token_database: str,
    token_collection: str,
    token_operation_timeout: float,
    token_operation_max_retry: int,
    commit_interval: int,
    queue_get_timeout: float,
) -> BaseWorker:
    token_mongo_client = MongoClient(host=token_mongo_uri)
    commit_event_processor = ProcessCommitEvent(
        commit_interval=commit_interval,
    )
    token_saver = TokenSaving(
        token_mongo_client=token_mongo_client,
        token_database=token_database,
        token_collection=token_collection,
        write_timeout=token_operation_timeout,
        on_timeout_retry_count=token_operation_max_retry,
    )
    commit_event_handler = CommitEventHandler(
        stream_reader_name=stream_reader_name,
        commit_event_processor=commit_event_processor,
        token_saver=token_saver,
    )
    application = CommitFlow(
        committer_queue=committer_queue,
        commit_event_handler=commit_event_handler,
        queue_get_timeout=queue_get_timeout,
    )
    worker = BaseWorker(
        manager_pid=manager_pid,
        manager_create_time=manager_create_time,
        task_id=task_id,
        request_queue=request_queue,
        response_queue=response_queue,
        queue_get_timeout=queue_get_timeout,
        application=application,
    )
    return worker


class CommitFlowContext(ApplicationContext):
    build_worker = staticmethod(build_commit_worker)
