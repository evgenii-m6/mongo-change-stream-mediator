import logging
from datetime import datetime

from mongo_change_stream_mediator.models import (
    SavedToken,
    CommitEvent,
    CommittableEvents, RecheckCommitEvent,
)
from .commit_processing import ProcessCommitEvent
from .token_saver import TokenSaving


class CommitEventHandler:
    def __init__(
        self,
        stream_reader_name: str,
        commit_event_processor: ProcessCommitEvent,
        token_saver: TokenSaving,
    ):
        self._stream_reader_name = stream_reader_name
        self._commit_event_processor = commit_event_processor
        self._token_saver = token_saver

    def start(self):
        self._token_saver.start()

    def stop(self):
        self._token_saver.stop()

    def handle_event(self, event: RecheckCommitEvent | CommitEvent):
        for committable_event in self._commit_event_processor.process_event(event):
            self._commit_events(committable_event)

    def _commit_events(self, commit_events: CommittableEvents):
        token_data = self._build_token_model(commit_events)
        logging.info(f"Save token data to db: {token_data!r}")
        self._save_resume_token_to_db(token_data)
        logging.info(f"Saved token data to db: {token_data!r}")

    def _build_token_model(self, events: CommittableEvents) -> SavedToken:
        return SavedToken(
            stream_reader_name=self._stream_reader_name,
            token=events.resume_token,
            date=datetime.utcnow()
        )

    def _save_resume_token_to_db(self, token_data: SavedToken):
        self._token_saver.save(token_data)
