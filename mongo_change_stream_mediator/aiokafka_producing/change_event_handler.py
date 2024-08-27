import asyncio
import logging
import time
from collections import deque
from copy import deepcopy
from enum import Enum
from multiprocessing import Queue
from typing import Callable, Any

from bson import json_util
import bson

from mongo_change_stream_mediator.commit_event_decoder import encode_commit_event
from mongo_change_stream_mediator.models import DecodedChangeEvent, CommitEvent
from .producer import Producer, ProducerNotRunningError
from threading import Lock, Thread
from ..operations import Operations


class BaseEventHandlerError(Exception):
    def __str__(self):
        return repr(self)


class EventHandlerNotRunning(BaseEventHandlerError):
    ...


class EventValidationError(BaseEventHandlerError):
    ...


class EventDoesNotContainOperationType(EventValidationError):
    ...


class OperationNotAllowedError(EventValidationError):
    def __init__(self, operation_type: str):
        self.operation_type = operation_type

    def __repr__(self):
        return f"{type(self).__name__}({self.operation_type})"


class NamespaceNotFoundError(EventValidationError):
    def __init__(self, operation_type: str):
        self.operation_type = operation_type

    def __repr__(self):
        return f"{type(self).__name__}({self.operation_type})"


class FieldNotFoundError(EventValidationError):
    def __init__(self, operation_type: str, namespace: dict[str, str]):
        self.operation_type = operation_type
        self.namespace = namespace

    def __repr__(self):
        return (
            f"{type(self).__name__}"
            f"(operation_type={self.operation_type}, namespace={self.namespace})"
        )


class DatabaseNotFoundError(FieldNotFoundError):
    ...


class CollectionNotFoundError(FieldNotFoundError):
    ...


class DocumentKeyNotFoundError(FieldNotFoundError):
    ...


def json_encoder(doc: dict[str, Any]) -> bytes:
    return json_string_encoder(doc).encode()


def json_string_encoder(doc: dict[str, Any]) -> str:
    return json_util.dumps(
        doc,
        json_options=json_util.LEGACY_JSON_OPTIONS
    )


class RedirectThreadStatus(str, Enum):
    initial = "initial"
    starting = "starting"
    running = "running"
    stopping = "stopping"


class ChangeEventHandler:
    _operation_map = deepcopy(Operations)

    def __init__(
        self,
        producer: Producer,
        committer_queue: Queue,
        kafka_prefix: str = "",
        put_queue_sleep_time: float = 1,
        wait_for_topic_created: float = 20.0
    ):
        self._kafka_prefix = kafka_prefix
        self._created_topics: set[str] = set()
        self._producer = producer
        self._committer_queue = committer_queue
        self._key_value_encoder = json_encoder
        self._json_encoder = json_string_encoder
        self._should_run = False

        self._produced = 0
        self._unconfirmed = set()
        self._confirmed = 0

        self._put_queue: deque[CommitEvent] = deque()
        self._put_queue_sleep_time = put_queue_sleep_time
        self._wait_for_topic_created = wait_for_topic_created
        self._redirect_thread = Thread(target=self._redirect_messages, daemon=True)
        self._redirect_thread_status = RedirectThreadStatus.initial

    def internal_queue_size(self):
        return len(self._put_queue)

    async def start(self):
        await self._producer.start()
        await self._update_created_topics()
        if self._redirect_thread_status == RedirectThreadStatus.initial:
            self._redirect_thread_status = RedirectThreadStatus.starting
            self._redirect_thread.start()
        while self._redirect_thread_status != RedirectThreadStatus.running:
            await asyncio.sleep(0.1)
        self._should_run = True

    async def _update_created_topics(self):
        topics: list[str] = await self._producer.get_topics()
        self._created_topics.clear()
        for topic_name in topics:
            if topic_name.startswith(self._kafka_prefix):
                self._created_topics.add(topic_name)

    async def stop(self):
        self._should_run = False
        self._redirect_thread_status = RedirectThreadStatus.stopping
        await self._producer.stop()
        await self._wait_for_redirect_thread_stopped()

    def exit_gracefully(self):
        self._should_run = False
        self._redirect_thread_status = RedirectThreadStatus.stopping
        self._producer.exit_gracefully()

    def log_statistics(self):
        logging.debug(
            f"Produced={self._produced}, "
            f"confirmed={self._confirmed}, "
            f"unconfirmed={self._unconfirmed}, "
            f"internal_queue_size={self.internal_queue_size()}"
        )

    async def handle(self, event: DecodedChangeEvent):
        if not self._should_run:
            raise EventHandlerNotRunning()

        try:
            await self._handle(event)
        except EventValidationError as ex:
            logging.exception(
                f"Event validation error: {ex!r}. Event skipped: {event!r}"
            )
            raise
        except ProducerNotRunningError as ex:
            if self._should_run:
                raise
            else:
                self.exit_gracefully()
                raise EventHandlerNotRunning()

    async def _handle(self, event: DecodedChangeEvent):
        self._validate_event(event)
        topic = self._get_topic_from_event(event)

        if topic not in self._created_topics:
            await self._update_created_topics()

        while topic not in self._created_topics:
            logging.warning(
                f"Topic {topic} does not exist! "
                f"You should create this topic to continue producing. "
                f"Wait {self._wait_for_topic_created}s."
            )
            await asyncio.sleep(self._wait_for_topic_created)
            await self._update_created_topics()

        await self._produce_message(
            topic=topic,
            key=self._get_key_from_event(event),
            value=self._get_value_from_event(event),
            on_delivery=self._on_message_delivered(
                count=event.count,
                resume_token=self._get_resume_token(event)
            ),
            count=event.count,
        )

    def _get_topic_from_event(self, event: DecodedChangeEvent) -> str:
        namespace = event.bson_document['ns']
        collection = namespace['coll']
        database = namespace['db']
        if self._kafka_prefix:
            return f"{self._kafka_prefix}.{database}.{collection}"
        else:
            return f"{database}.{collection}"

    async def _produce_message(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable,
        count: int
    ):
        await self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=on_delivery,
        )
        self._produced += 1
        self._unconfirmed.add(count)
        logging.debug(f"Produced+1={self._produced}, confirmed={self._confirmed}")

    def _validate_event(self, event: DecodedChangeEvent):
        if 'operationType' not in event.bson_document:
            raise EventDoesNotContainOperationType()

        operation_type = event.bson_document['operationType']
        if operation_type not in self._operation_map:
            raise OperationNotAllowedError(
                operation_type=event.bson_document['operationType']
            )

        if 'ns' not in event.bson_document:
            raise NamespaceNotFoundError(
                operation_type=operation_type
            )
        namespace = event.bson_document.get('ns', {})
        if 'coll' not in namespace:
            raise CollectionNotFoundError(
                operation_type=operation_type, namespace=namespace
            )
        if 'db' not in namespace:
            raise DatabaseNotFoundError(
                operation_type=operation_type, namespace=namespace
            )

        if 'documentKey' not in event.bson_document:
            raise DocumentKeyNotFoundError(
                operation_type=operation_type, namespace=namespace
            )

    def _get_key_from_event(self, event: DecodedChangeEvent) -> bytes:
        return self._key_value_encoder(event.bson_document["documentKey"])

    def _get_op(self, operation_type: str):
        return self._operation_map[operation_type]

    def _get_resume_token(self, event: DecodedChangeEvent) -> bytes | None:
        if '_id' in event.bson_document:
            return bson.encode(event.bson_document['_id'])
        else:
            return None

    def _get_value_from_event(self, event: DecodedChangeEvent) -> bytes:
        operation = self._get_op(event.bson_document['operationType'])
        new_doc = {'op': operation}
        if 'fullDocumentBeforeChange' in event.bson_document:
            new_doc['before'] = self._json_encoder(
                event.bson_document['fullDocumentBeforeChange']
            )
        if 'updateDescription' in event.bson_document:
            new_doc["updateDescription"] = {}
            update_description = event.bson_document['updateDescription']
            if "updatedFields" in update_description:
                new_doc["updateDescription"][
                    "updatedFields"
                ] = self._json_encoder(update_description["updatedFields"])
            if "removedFields" in update_description:
                new_doc["updateDescription"][
                    "removedFields"
                ] = update_description["removedFields"]
            if "truncatedArrays" in update_description:
                new_doc["updateDescription"][
                    "truncatedArrays"
                ] = update_description["truncatedArrays"]
        if 'fullDocument' in event.bson_document:
            new_doc['after'] = self._json_encoder(event.bson_document['fullDocument'])
        return self._key_value_encoder(new_doc)

    def _on_message_delivered(
        self, count: int, resume_token: bytes | None
    ) -> Callable[[asyncio.Future], ...]:
        message_count = count

        def wrapped(future: asyncio.Future):
            try:
                future.result()
            except BaseException as ex:
                logging.error(
                    f"Error in producing message {message_count}: {ex!r}. "
                    f"Produced={self._produced}, confirmed+1={self._confirmed}"
                )
                self.exit_gracefully()
            else:
                logging.debug(f"Message {message_count} is confirmed.")
                self._put_queue.append(
                    CommitEvent(
                        count=message_count,
                        need_confirm=False,
                        resume_token=resume_token
                    )
                )
                self._confirmed += 1
                self._unconfirmed.discard(message_count)
                logging.debug(
                    f"Produced={self._produced}, confirmed+1={self._confirmed}"
                )

        return wrapped

    def _redirect_messages(self):
        self._redirect_thread_status = RedirectThreadStatus.running
        while self._redirect_thread_status == RedirectThreadStatus.running:
            self._redirect_message()

    def _redirect_message(self):
        if len(self._put_queue) == 0:
            logging.debug(
                f"Put queue is empty. "
                f"Sleep {self._put_queue_sleep_time}."
            )
            time.sleep(self._put_queue_sleep_time)
        else:
            commit_event = self._put_queue.popleft()
            logging.debug(f"Got from put queue {commit_event.count}.")
            message = encode_commit_event(
                count=commit_event.count,
                need_confirm=int(commit_event.need_confirm),
                token=commit_event.resume_token,
            )
            self._committer_queue.put(message)
            logging.debug(f"Redirected from put queue {commit_event.count}.")

    async def _wait_for_redirect_thread_stopped(self):
        max_wait_count = 5
        wait_count = 0

        if self._redirect_thread.is_alive() and wait_count < max_wait_count:
            await asyncio.sleep(self._put_queue_sleep_time)
            wait_count += 1
