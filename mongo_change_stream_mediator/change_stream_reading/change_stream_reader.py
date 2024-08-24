from mongo_change_stream_mediator.base_application import BaseApplication
from .change_handler import ChangeHandler
from .resume_token import RetrieveResumeToken
from .watch import ChangeStreamWatch


class ChangeStreamReader(BaseApplication):
    def __init__(
        self,
        token_retriever: RetrieveResumeToken,
        watcher: ChangeStreamWatch,
        change_handler: ChangeHandler,
    ):
        super().__init__()
        self._change_handler = change_handler
        self._watcher = watcher
        self._token_retriever = token_retriever

    def exit_gracefully(self, signum, frame):
        self._watcher.exit_gracefully()

    def _start_dependencies(self):
        self._token_retriever.start()
        self._watcher.start()

    def _stop_dependencies(self):
        self._watcher.stop()
        self._token_retriever.stop()

    def task(self):
        resume_token = self._token_retriever.get_token()
        for event in self._watcher.iter(resume_token):
            self._change_handler.handle(event)
