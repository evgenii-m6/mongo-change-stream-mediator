import pytest

from mongo_change_stream_mediator.change_event_decoder import (
    encode_change_event,
    decode_change_event,
)
from tests.mocks.events import events, to_raw_bson


@pytest.mark.parametrize("event", events())
def test_encode_decode(event):
    encoded = encode_change_event(count=100500, bson_document=to_raw_bson(event))
    decoded = decode_change_event(encoded)
    assert decoded.count == 100500
    assert decoded.bson_document == event
