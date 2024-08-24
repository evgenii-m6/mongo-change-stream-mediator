import logging
from typing import Mapping, Any

import bson
from pymongo import MongoClient

from mongo_change_stream_mediator.models import SavedToken
from mongo_change_stream_mediator.mongo_retry_handler import (
    mongo_with_retry_and_timeout,
)


class RetrieveResumeToken:
    def __init__(
        self,
        stream_reader_name: str,
        token_mongo_client: MongoClient,
        token_database: str,
        token_collection: str,
        read_timeout: float = 5.0,
        on_timeout_retry_count: int = 3,
    ):
        self._stream_reader_name = stream_reader_name
        self._token_database = token_database
        self._token_collection = token_collection
        self._token_mongo_client = token_mongo_client
        self._read_timeout = read_timeout
        self._on_timeout_retry_count = on_timeout_retry_count

        self._collection = self._token_mongo_client.get_database(
            self._token_database
        ).get_collection(self._token_collection)

    def start(self):
        logging.info("Connecting to token mongo")
        token_server_info = self._token_mongo_client.server_info()
        logging.info(f"Connected to mongo token server: {token_server_info}")

    def stop(self):
        self._token_mongo_client.close()

    def get_token(self) -> Mapping[str, Any] | None:
        received_saved_token = self._get_token_from_db()

        logging.info(f"Close connection to mongo token server")
        self.stop()

        decoded_token: Mapping[str, Any] | None
        if received_saved_token:
            decoded_token = self._parse_token_model(received_saved_token)
            logging.info(
                f"Token decoded {decoded_token} "
                f"for stream_reader_name={self._stream_reader_name}"
            )

        else:
            logging.info(
                f"Last token for stream_reader_name={self._stream_reader_name} "
                f"wasn't found"
            )
            decoded_token = None
        return decoded_token

    def _get_token_from_db(self) -> Mapping[str, Any] | None:
        logging.info(
            f"Request last token for stream_reader_name={self._stream_reader_name}"
        )
        operation = self._find_one(filter={
            "stream_reader_name": self._stream_reader_name
        })

        received_saved_token: dict | None = mongo_with_retry_and_timeout(
            func=operation,
            max_retry=self._on_timeout_retry_count,
            operation_timeout=self._read_timeout,
        )
        logging.debug(
            f"Got token document: {received_saved_token} "
            f"for stream_reader_name={self._stream_reader_name}"
        )
        return received_saved_token

    def _find_one(self, filter: dict[str, str]):
        def wrapper():
            return self._collection.find_one(filter=filter)
        return wrapper

    def _parse_token_model(
        self,
        data: Mapping[str, Any]
    ) -> Mapping[str, Any] | None:
        saved_token = SavedToken.model_validate(data)
        return self._decode_resume_token(saved_token.token)

    @staticmethod
    def _decode_resume_token(resume_token: bytes | None) -> Mapping[str, Any] | None:
        if resume_token:
            return bson.decode(resume_token)
        else:
            return None
