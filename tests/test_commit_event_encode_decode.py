import pytest

from mongo_change_stream_mediator.commit_event_decoder import (
    decode_commit_event,
    encode_commit_event,
)


@pytest.mark.parametrize(
    "token, token_result",
    [
        (None, None),
        (b"", None),
        (b"test", b"test"),
    ]
)
@pytest.mark.parametrize("need_confirm", [True, False])
@pytest.mark.parametrize("count", [0, 1, 2])
def test_decode_commit_event_ok(need_confirm, count, token, token_result):
    data = encode_commit_event(
        count=count,
        need_confirm=need_confirm,
        token=token,
    )

    event = decode_commit_event(data)
    assert event.count == count
    assert event.need_confirm == need_confirm
    assert event.resume_token == token_result
