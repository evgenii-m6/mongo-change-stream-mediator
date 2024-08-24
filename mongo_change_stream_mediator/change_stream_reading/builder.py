from multiprocessing import Process, Queue
from typing import Type

from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient

from mongo_change_stream_mediator.app_context import ApplicationContext
from mongo_change_stream_mediator.base_worker import BaseWorker
from mongo_change_stream_mediator.models import ProcessData
from mongo_change_stream_mediator.settings import (
    FullDocumentBeforeChange,
    FullDocument,
)
from mongo_change_stream_mediator.settings import Settings
from mongo_change_stream_mediator.utils import TaskIdGenerator
from .change_handler import ChangeHandler
from .change_stream_reader import ChangeStreamReader
from .resume_token import RetrieveResumeToken
from .watch import ChangeStreamWatch


def build_change_stream_reader_process(
    application_context: Type[ApplicationContext],
    manager_pid: int,
    manager_create_time: float,
    task_id_generator: TaskIdGenerator,
    producer_queues: dict[int, Queue],
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    settings: Settings,
) -> ProcessData:
    task_id = task_id_generator.get()
    full_document_before_change = (
        settings.full_document_before_change.value
        if settings.full_document_before_change is not None
        else None
    )
    full_document = (
        settings.full_document.value
        if settings.full_document is not None
        else None
    )

    kwargs = {
        'manager_pid': manager_pid,
        'manager_create_time': manager_create_time,
        'task_id': task_id,
        'producer_queues': producer_queues,
        'request_queue': request_queue,
        'response_queue': response_queue,
        'committer_queue': committer_queue,
        'mongo_uri': settings.mongo_uri,
        'token_mongo_uri': settings.token_mongo_uri,
        'token_database': settings.token_database,
        'token_collection': settings.token_collection,
        'token_operation_timeout': settings.token_operation_timeout,
        'token_operation_max_retry': settings.token_operation_max_retry,
        'stream_reader_name': settings.stream_reader_name,
        'database': settings.database,
        'collection': settings.collection,
        'pipeline': settings.cursor_pipeline.pipeline,
        'full_document_before_change': full_document_before_change,
        'full_document': full_document,
        'reader_batch_size': settings.reader_batch_size,
        'queue_get_timeout': settings.queue_get_timeout,
    }
    process = Process(target=application_context.run_application, kwargs=kwargs)
    return ProcessData(task_id=task_id, process=process, kwargs=kwargs)


def build_change_stream_reader_worker(
    manager_pid: int,
    manager_create_time: float,
    task_id: int,
    producer_queues: dict[int, Queue],
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    mongo_uri: str,
    token_mongo_uri: str,
    token_database: str,
    token_collection: str,
    token_operation_timeout: float,
    token_operation_max_retry: int,
    stream_reader_name: str,
    database: str | None = None,
    collection: str | None = None,
    pipeline: list[dict] | None = None,
    full_document_before_change: str | None = None,
    full_document: str | None = None,
    reader_batch_size: int | None = None,
    queue_get_timeout: float = 1,
) -> BaseWorker:
    token_mongo_client = MongoClient(
        host=token_mongo_uri,
    )
    mongo_client = MongoClient(
        host=mongo_uri,
        document_class=RawBSONDocument
    )
    token_retriever = RetrieveResumeToken(
        stream_reader_name=stream_reader_name,
        token_mongo_client=token_mongo_client,
        token_database=token_database,
        token_collection=token_collection,
        read_timeout=token_operation_timeout,
        on_timeout_retry_count=token_operation_max_retry,
    )
    full_document_before_change_parsed = (
        FullDocumentBeforeChange(full_document_before_change)
        if full_document_before_change is not None
        else None
    )
    full_document_parsed = (
        FullDocument(full_document)
        if full_document is not None
        else None
    )
    watcher = ChangeStreamWatch(
        mongo_client=mongo_client,
        collection=collection,
        database=database,
        pipeline=pipeline,
        full_document_before_change=full_document_before_change_parsed,
        full_document=full_document_parsed,
        reader_batch_size=reader_batch_size,
    )
    change_handler = ChangeHandler(
        committer_queue=committer_queue,
        producer_queues=producer_queues,
    )
    application = ChangeStreamReader(
        token_retriever=token_retriever,
        watcher=watcher,
        change_handler=change_handler,
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


class ChangeStreamReaderContext(ApplicationContext):
    build_worker = staticmethod(build_change_stream_reader_worker)
