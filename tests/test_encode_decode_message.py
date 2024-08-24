import pytest

from mongo_change_stream_mediator.messages import (
    Status, ChangeStatus, encode_message, decode_message
)
from mongo_change_stream_mediator.models import Statuses, ChangeStatuses


@pytest.mark.parametrize("message", [
    Status(task_id=1, status=Statuses.running),
    ChangeStatus(task_id=1, status=ChangeStatuses.running),
])
def test_encode_decode(message):
    data = encode_message(message)
    assert data == encode_message(decode_message(data))
    assert decode_message(data) == message
