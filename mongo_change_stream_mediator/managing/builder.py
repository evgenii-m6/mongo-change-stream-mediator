from mongo_change_stream_mediator.settings import (
    Settings,
)
from mongo_change_stream_mediator.change_stream_reading import ChangeStreamReaderContext
from mongo_change_stream_mediator.committing import CommitFlowContext
from mongo_change_stream_mediator.producing import ProducerFlowContext
from .manager import Manager


def build_manager(settings: Settings) -> Manager:
    return Manager(
        change_stream_reader=ChangeStreamReaderContext,
        producing=ProducerFlowContext,
        committing=CommitFlowContext,
        settings=settings,
    )
