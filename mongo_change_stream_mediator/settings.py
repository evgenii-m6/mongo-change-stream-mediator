import json
from enum import Enum

from pydantic import BaseModel, field_validator

from bson import json_util


def convert_bson(value: str) -> dict:
    return json.loads(
        value,
        object_hook=json_util.object_hook,
    )


class Pipeline(BaseModel):
    pipeline: list[dict]


class FullDocumentBeforeChange(str, Enum):
    when_available = 'whenAvailable'
    required = 'required'


class FullDocument(str, Enum):
    when_available = 'whenAvailable'
    update_lookup = 'updateLookup'
    required = 'required'


class NewTopicConfiguration(BaseModel):
    new_topic_num_partitions: int = 1
    new_topic_replication_factor: int = 1
    new_topic_config: dict[str, str] = {}


class Settings(BaseModel):
    stream_reader_name: str
    mongo_uri: str
    token_mongo_uri: str
    kafka_bootstrap_servers: str  # example: 'host1:9092,host2:9092'
    max_queue_size: int = 10000  # TODO: check 2 times as much self.producers_count
    producers_count: int = 1
    token_database: str = 'change-stream-database'
    token_collection: str = 'ChangeStreamTokens'
    token_operation_timeout: float = 5.0
    token_operation_max_retry: int = 3
    pipeline: str | None = None
    database: str | None = None
    collection: str | None = None
    full_document_before_change: FullDocumentBeforeChange | None = None
    full_document: FullDocument | None = None
    reader_batch_size: int | None = None
    queue_get_timeout: float = 1.0
    program_start_timeout: int = 60
    program_graceful_stop_timeout: int = 20
    commit_interval: int = 30
    max_uncommitted_events: int = 1000
    new_topic_num_partitions: int = 1
    new_topic_replication_factor: int = 1
    new_topic_config: str | None = None
    kafka_prefix: str = ""
    kafka_producer_config: str | None = None
    kafka_connect_timeout: float = 10.0

    @field_validator("pipeline")
    @classmethod
    def validate_mongo_pipeline(cls, v):
        if v is not None:
            Pipeline(pipeline=convert_bson(v))
        else:
            Pipeline(pipeline=[])
        return v

    @field_validator("new_topic_config", "kafka_producer_config")
    @classmethod
    def validate_kafka_config(cls, v):
        if v is not None:
            json.loads(v)
        return v

    @property
    def cursor_pipeline(self) -> Pipeline:
        if self.pipeline is not None:
            return Pipeline(pipeline=convert_bson(self.pipeline))
        else:
            return Pipeline(pipeline=[])

    @property
    def new_topic_config_dict(self) -> dict[str, str]:
        if self.new_topic_config is not None:
            return json.loads(self.new_topic_config)
        else:
            return {}

    @property
    def kafka_producer_config_dict(self) -> dict[str, str]:
        if self.kafka_producer_config is not None:
            return json.loads(self.kafka_producer_config)
        else:
            return {}

    @property
    def new_topic_configuration(self) -> NewTopicConfiguration:
        return NewTopicConfiguration(
            new_topic_num_partitions=self.new_topic_num_partitions,
            new_topic_replication_factor=self.new_topic_replication_factor,
            new_topic_config=self.new_topic_config_dict
        )
