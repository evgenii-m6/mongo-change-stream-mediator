import logging
from typing import Callable

from pymongo import timeout
from pymongo.errors import PyMongoError


def mongo_with_retry_and_timeout(
    func: Callable, max_retry: int, operation_timeout: float
):
    def increase_timeout(retry_count):
        return operation_timeout*(retry_count+1)

    count = 0
    while count < max_retry:
        try:
            with timeout(increase_timeout(count)):
                result = func()
            return result
        except PyMongoError as exc:
            if exc.timeout:
                logging.warning(f"Timeout when trying to save token: {exc!r}")
                count += 1
                if count >= max_retry:
                    logging.error(
                        f"Max retry count exceed when trying to save token: {exc!r}"
                    )
                    raise
            else:
                logging.error(
                    f"Failed to save token with non-timeout error: {exc!r}"
                )
                raise
