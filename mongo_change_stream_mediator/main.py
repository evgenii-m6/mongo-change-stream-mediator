import logging.handlers

handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s  %(name)s  pid=%(process)d   %(levelname)s   %(message)s'
)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel("DEBUG")


from mongo_change_stream_mediator.managing import build_manager
from mongo_change_stream_mediator.settings import Settings


if __name__ == '__main__':
    settings = Settings(
    )
    manager = build_manager(settings)
    manager.run()
