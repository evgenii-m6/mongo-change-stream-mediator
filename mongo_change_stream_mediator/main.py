from mongo_change_stream_mediator.managing import build_manager_with_confluent_kafka
from mongo_change_stream_mediator.managing import build_manager_with_aiokafka
from mongo_change_stream_mediator.settings import Settings


if __name__ == '__main__':
    settings = Settings(
    )
    manager = build_manager_with_aiokafka(settings)
    manager.run()
