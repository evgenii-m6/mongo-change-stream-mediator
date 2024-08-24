from mongo_change_stream_mediator.models import CommitEvent


def decode_commit_event(data: bytes) -> CommitEvent:
    count = int.from_bytes(data[0:8], byteorder='big')
    need_confirm = bool.from_bytes(data[8:9], byteorder='big')

    resume_token: bytes | None

    if len(data) > 9:
        resume_token = data[9:]
    else:
        resume_token = None

    return CommitEvent(
        count=count,
        need_confirm=need_confirm,
        resume_token=resume_token
    )


def encode_commit_event(
    count: int,
    need_confirm: int,
    token: bytes | None,
):
    """
    Bytes: 0-7 - number of message
    Bytes: 8 - is need to confirm from producers
    Bytes: 9-end - resume_token
    """
    data: bytes = (
            count.to_bytes(length=8, byteorder='big') +
            need_confirm.to_bytes(length=1, byteorder='big')
    )
    if token:
        data = data + token
    return data
