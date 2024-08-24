from multiprocessing import Process, Queue
from typing import Any, Type

from confluent_kafka import Producer as KafkaProducer
from confluent_kafka.admin import AdminClient

from mongo_change_stream_mediator.app_context import ApplicationContext
from mongo_change_stream_mediator.base_worker import BaseWorker
from mongo_change_stream_mediator.models import ProcessData
from mongo_change_stream_mediator.settings import Settings, NewTopicConfiguration
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
        'new_topic_configuration': settings.new_topic_configuration.dict(),
        'kafka_prefix': settings.kafka_prefix,
        'kafka_producer_config': settings.kafka_producer_config_dict,
        'queue_get_timeout': settings.queue_get_timeout,
        'kafka_connect_timeout': settings.kafka_connect_timeout,
    }
    process = Process(target=application_context.run_application, kwargs=kwargs)
    return ProcessData(
        task_id=task_id,
        process=process,
        kwargs=kwargs
    )


def build_producer_worker(
    manager_pid: int,
    manager_create_time: float,
    task_id: int,
    producer_queue: Queue,
    request_queue: Queue,
    response_queue: Queue,
    committer_queue: Queue,
    stream_reader_name: str,
    kafka_bootstrap_servers: str,
    new_topic_configuration: dict[str, Any],
    kafka_prefix: str,
    kafka_producer_config: dict[str, str],
    queue_get_timeout: float,
    kafka_connect_timeout: float,
) -> BaseWorker:
    new_topic_config = NewTopicConfiguration.parse_obj(new_topic_configuration)

    producer_config = {
        "delivery.timeout.ms": 120000,
        "queue.buffering.max.messages": 1024,  # 1024 messages
        "queue.buffering.max.kbytes": 1024 * 1024,  # 1 Gb
    }
    producer_config.update(kafka_producer_config)
    producer_config.update(
        {
            'bootstrap.servers': kafka_bootstrap_servers,  # Kafka broker address
            'client.id': f"producer_{stream_reader_name}_{task_id}",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
        }
    )
    for i in ['retries']:
        producer_config.pop(i, None)

    admin_config = {'bootstrap.servers': kafka_bootstrap_servers}
    kafka_client = KafkaClient(
        producer_config=producer_config,
        admin_config=admin_config,
        kafka_connect_timeout=kafka_connect_timeout,
    )
    producer = Producer(
        task_id=task_id,
        kafka_client=kafka_client,
        new_topic_configuration=new_topic_config,
    )
    change_event_handler = ChangeEventHandler(
        producer=producer,
        committer_queue=committer_queue,
        kafka_prefix=kafka_prefix,
    )
    application = ProducerFlow(
        producer_queue=producer_queue,
        event_handler=change_event_handler,
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


class ProducerFlowContext(ApplicationContext):
    build_worker = staticmethod(build_producer_worker)
