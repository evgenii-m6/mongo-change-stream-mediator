from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue
from typing import Any, Type

from mongo_change_stream_mediator.app_context import ApplicationContext
from mongo_change_stream_mediator.base_async_worker import BaseAsyncWorker
from mongo_change_stream_mediator.models import ProcessData
from mongo_change_stream_mediator.settings import Settings
from mongo_change_stream_mediator.utils import TaskIdGenerator
from .change_event_handler import ChangeEventHandler
from .kafka_wrapper import KafkaClient
from .producer import Producer
from .producer_flow import ProducerFlow


def build_producer_process(
    application_context: Type[ApplicationContext],
    manager_pid: int,
    manager_create_time: float,
    task_id_generator: TaskIdGenerator,
    producer_queue: Queue,
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
        'producer_queue': producer_queue,
        'request_queue': request_queue,
        'response_queue': response_queue,
        'committer_queue': committer_queue,
        'stream_reader_name': settings.stream_reader_name,
        'kafka_bootstrap_servers': settings.kafka_bootstrap_servers,
        'kafka_prefix': settings.kafka_prefix,
        'kafka_producer_config': settings.aiokafka_producer_config_dict,
        'queue_get_timeout': settings.queue_get_timeout,
        'kafka_connect_timeout': settings.kafka_connect_timeout,
    }
    process = Process(target=application_context.run_async_application, kwargs=kwargs)
    return ProcessData(
        task_id=task_id,
        process=process,
        kwargs=kwargs
    )


async def build_producer_worker(
    manager_pid: int,
    manager_create_time: float,
    task_id: int,
    producer_queue: Queue,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    stream_reader_name: str,
    kafka_bootstrap_servers: str,
    kafka_prefix: str,
    kafka_producer_config: dict[str, Any],
    queue_get_timeout: float,
    kafka_connect_timeout: float,
) -> BaseAsyncWorker:
    producer_config: dict[str, Any] = {
        "request_timeout_ms": 40000,
        "compression_type": "lz4",
        "max_batch_size": 16384,
        "enable_idempotence": True,
        "acks": "all",
    }
    producer_config.update(kafka_producer_config)
    producer_config.update(
        {
            'bootstrap_servers': kafka_bootstrap_servers.split(','),
            'client_id': f"producer_{stream_reader_name}_{task_id}",
        }
    )
    thread_pool_executor = ThreadPoolExecutor(
        max_workers=2, thread_name_prefix="ProducerFlowExecutor"
    )
    kafka_client = KafkaClient(
        producer_config=producer_config,
        kafka_connect_timeout=kafka_connect_timeout,
    )
    producer = Producer(
        task_id=task_id,
        kafka_client=kafka_client,
    )
    change_event_handler = ChangeEventHandler(
        producer=producer,
        committer_queue=committer_queue,
        kafka_prefix=kafka_prefix,
    )
    application = ProducerFlow(
        producer_queue=producer_queue,
        event_handler=change_event_handler,
        thread_pool_executor=thread_pool_executor,
        queue_get_timeout=queue_get_timeout,
    )
    worker = BaseAsyncWorker(
        manager_pid=manager_pid,
        manager_create_time=manager_create_time,
        task_id=task_id,
        request_queue=request_queue,
        response_queue=response_queue,
        application=application,
        thread_pool_executor=thread_pool_executor,
        queue_get_timeout=queue_get_timeout,
    )
    return worker


class ProducerFlowContext(ApplicationContext):
    build_async_worker = staticmethod(build_producer_worker)
    process_builder = staticmethod(build_producer_process)
