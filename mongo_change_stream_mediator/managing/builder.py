from mongo_change_stream_mediator.settings import (
    Settings,
)

from .manager import Manager


def build_manager_with_confluent_kafka(settings: Settings) -> Manager:
    from mongo_change_stream_mediator.change_stream_reading import (
        ChangeStreamReaderContext,
    )
    from mongo_change_stream_mediator.committing import CommitFlowContext
    from mongo_change_stream_mediator.producing import ProducerFlowContext
    return Manager(
        change_stream_reader=ChangeStreamReaderContext,
        producing=ProducerFlowContext,
        committing=CommitFlowContext,
        settings=settings,
    )


def build_manager_with_aiokafka(settings: Settings) -> Manager:
    from mongo_change_stream_mediator.change_stream_reading import (
        ChangeStreamReaderContext,
    )
    from mongo_change_stream_mediator.committing import CommitFlowContext
    from mongo_change_stream_mediator.aiokafka_producing import ProducerFlowContext
    return Manager(
        change_stream_reader=ChangeStreamReaderContext,
        producing=ProducerFlowContext,
        committing=CommitFlowContext,
        settings=settings,
    )
