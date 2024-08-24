import logging
import time
from typing import Iterator, Mapping, Any

from bson.raw_bson import RawBSONDocument
from pymongo import MongoClient
from pymongo.change_stream import ChangeStream
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

from mongo_change_stream_mediator.models import ChangeEvent
from mongo_change_stream_mediator.settings import (
    FullDocumentBeforeChange,
    FullDocument,
)


class DatabaseWasNotSet(Exception):
    ...


class CounterMaxValueExceedError(Exception):
    ...


class ChangeStreamWatcherWasNotStarted(Exception):
    ...


class ChangeStreamWatch:
    def __init__(
        self,
        mongo_client: MongoClient,
        collection: str | None = None,
        database: str | None = None,
        pipeline: list[dict] | None = None,
        full_document_before_change: FullDocumentBeforeChange | None = None,
        full_document: FullDocument | None = None,
        reader_batch_size: int | None = None,
        sleep_time_between_empty_events: float = 0.5,
    ):
        self._collection = collection
        self._database = database
        self._mongo_client = mongo_client
        self._pipeline = pipeline
        self._full_document_before_change = full_document_before_change
        self._full_document = full_document
        self._reader_batch_size = reader_batch_size
        self._sleep_time_between_empty_events = sleep_time_between_empty_events
        self._counter = 0
        self._should_run = False
        self._last_resume_token: RawBSONDocument | None = None

    def _get_watcher(self) -> MongoClient | Database | Collection:
        if self._collection is not None and self._database is None:
            raise DatabaseWasNotSet("Can't use collection without database")

        if self._database is not None:
            db = self._mongo_client.get_database(self._database)
            if self._collection is not None:
                watcher = db.get_collection(self._collection)
            else:
                watcher = db
        else:
            watcher = self._mongo_client
        return watcher

    def exit_gracefully(self):
        self._should_run = False

    def start(self):
        logging.info("Connecting to mongo")
        server_info = self._mongo_client.server_info()
        logging.info(f"Connected to mongo server: {server_info}")
        self._should_run = True

    def stop(self):
        self._should_run = False
        self._mongo_client.close()

    def _get_stream_context_manager(
        self,
        resume_after: Mapping[str, Any] | None = None
    ) -> ChangeStream:
        watcher = self._get_watcher()
        full_document = (
            self._full_document.value
            if self._full_document is not None
            else None
        )
        full_document_before_change = (
            self._full_document_before_change.value
            if self._full_document_before_change is not None
            else None
        )
        return watcher.watch(
            pipeline=self._pipeline,
            resume_after=resume_after,
            full_document_before_change=full_document_before_change,
            full_document=full_document,
            batch_size=self._reader_batch_size
        )

    def iter(self, resume_token:  Mapping[str, Any] | None) -> Iterator[ChangeEvent]:
        if not self._should_run:
            raise ChangeStreamWatcherWasNotStarted()

        with self._get_stream_context_manager(resume_token) as stream_context:
            while self._should_run and stream_context.alive:
                try:
                    change = stream_context.try_next()
                except PyMongoError as ex:
                    logging.exception(
                        f"Error in change stream reader cursor: {ex!r}. Stop iteration"
                    )
                    self.exit_gracefully()
                else:
                    resume_token = stream_context.resume_token
                    yield from self._iter_change_event(change, resume_token)

    def _iter_change_event(
        self,
        change: RawBSONDocument | None,
        resume_token: RawBSONDocument | None
    ) -> Iterator[ChangeEvent]:
        event = self._build_change_event(
            change=change,
            resume_token=resume_token
        )
        self._is_valid_event(event)

        if event is not None:
            yield event
        else:
            # We end up here when there are no recent changes.
            # Sleep for a while before trying again to avoid flooding
            # the server with getMore requests when no changes are
            # available.
            time.sleep(self._sleep_time_between_empty_events)

    def _build_change_event(
        self,
        change: RawBSONDocument | None,
        resume_token: RawBSONDocument | None,
    ) -> ChangeEvent | None:
        # Note that the ChangeStream's resume token may be updated
        # even when no changes are returned.

        if resume_token is not None:
            is_new_token = self._last_resume_token != resume_token
            self._last_resume_token = resume_token
        else:
            is_new_token = False

        if change is not None:
            self._counter += 1
            event = ChangeEvent(
                bson_document=change,
                token=resume_token,
                count=self._counter
            )
        else:
            if is_new_token:
                self._counter += 1
                event = ChangeEvent(
                    bson_document=change,
                    token=resume_token,
                    count=self._counter
                )
            else:
                event = None
        return event

    def _is_valid_event(self, event: ChangeEvent | None):
        if event is not None:
            if self._is_greater_then_max_value(event.count):
                logging.error("Counter bigger then max value. Stop process")
                raise CounterMaxValueExceedError()
        return True

    @staticmethod
    def _is_greater_then_max_value(counter: int) -> bool:
        int64 = 18446744073709551615
        return counter > int64
