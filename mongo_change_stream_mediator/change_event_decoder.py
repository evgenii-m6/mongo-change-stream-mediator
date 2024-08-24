import bson
from bson.raw_bson import RawBSONDocument

from .models import DecodedChangeEvent


def decode_change_event(data: bytes) -> DecodedChangeEvent:
    """
    Bytes: 0-7 - number of message
    Bytes: 8-end - bson_document
    """
    count = int.from_bytes(data[0:8], byteorder='big')
    bson_document = bson.decode(data[8:])
    return DecodedChangeEvent(
        bson_document=bson_document,
        count=count,
    )


def encode_change_event(count: int, bson_document: RawBSONDocument) -> bytes:
    return count.to_bytes(length=8, byteorder='big') + bson_document.raw
