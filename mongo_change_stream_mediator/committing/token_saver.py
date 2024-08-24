import logging
from typing import Any

from bson import Binary
from pymongo import MongoClient

from mongo_change_stream_mediator.models import SavedToken
from mongo_change_stream_mediator.mongo_retry_handler import (
    mongo_with_retry_and_timeout,
)


class TokenSaving:
    def __init__(
        self,
        token_mongo_client: MongoClient,
        token_database: str,
        token_collection: str,
        write_timeout: float = 5.0,
        on_timeout_retry_count: int = 3,
    ):
        self._token_mongo_client = token_mongo_client
        self._token_database = token_database
        self._token_collection = token_collection
        self._write_timeout = write_timeout
        self._on_timeout_retry_count = on_timeout_retry_count

        self._collection = self._token_mongo_client.get_database(
            self._token_database
        ).get_collection(self._token_collection)

    def start(self):
        logging.info("Connecting to token mongo")
        token_server_info = self._token_mongo_client.server_info()
        logging.info(f"Connected to mongo token server: {token_server_info}")
        logging.info(f"Create index on token collection")
        self._collection.create_index(
            keys="stream_reader_name",
            name="stream_reader_name",
            unique=True,
        )
        logging.info(f"Index created on token collection")

    def stop(self):
        self._token_mongo_client.close()

    @staticmethod
    def _transform_to_dict(token_data: SavedToken) -> dict[str, Any]:
        return {
            'stream_reader_name': token_data.stream_reader_name,
            'token': Binary(token_data.token, subtype=0),
            'date': token_data.date
        }

    def save(self, token_data: SavedToken):
        replacement = self._transform_to_dict(token_data)
        operation = self._save(replacement=replacement)
        result = mongo_with_retry_and_timeout(
            func=operation,
            max_retry=self._on_timeout_retry_count,
            operation_timeout=self._write_timeout,
        )
        return result

    def _save(self, replacement: dict):
        def wrapper():
            self._collection.replace_one(
                filter={"stream_reader_name": replacement["stream_reader_name"]},
                replacement=replacement,
                upsert=True
            )
        return wrapper
