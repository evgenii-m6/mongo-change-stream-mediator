import time
from functools import singledispatchmethod
from typing import Iterator

from mongo_change_stream_mediator.models import (
    CommitEvent,
    CommittableEvents,
    RecheckCommitEvent,
)


class ProcessCommitEvent:
    def __init__(self, max_uncommitted_events: int, commit_interval: int) -> None:
        self._confirmed_events: dict[int, CommitEvent] = dict()
        self._unconfirmed_events: dict[int, CommitEvent] = dict()
        self._last_sent_commit_event = 0
        self._last_commit_time: float = time.monotonic()
        self._max_uncommitted_events = max_uncommitted_events
        self._commit_interval = commit_interval

    @singledispatchmethod
    def process_event(self, event) -> Iterator[CommittableEvents]:
        raise NotImplementedError(f"Not implemented event type {type(event)}")

    @process_event.register(RecheckCommitEvent)
    def process_recheck_event(
        self, event: RecheckCommitEvent
    ) -> Iterator[CommittableEvents]:
        yield from self._process_events_chain()

    @process_event.register(CommitEvent)
    def process_commit_event(self, event: CommitEvent) -> Iterator[CommittableEvents]:
        if event.need_confirm:
            self._process_unconfirmed_event(event)
        else:
            self._process_confirmed_event(event)
        yield from self._process_events_chain()

    def _process_events_chain(self) -> Iterator[CommittableEvents]:
        commit_events = self._get_committable_events()
        if self._is_need_commit_events(commit_events):
            yield commit_events  # type: ignore
            self._clear_previous_commit_events(commit_events)

    def _process_unconfirmed_event(self, event: CommitEvent) -> None:
        if event.count > self._last_sent_commit_event:
            if event.count not in self._confirmed_events:
                if event.count in self._unconfirmed_events:
                    token = self._get_actual_token(
                        old_token=self._unconfirmed_events[event.count].resume_token,
                        new_token=event.resume_token,
                    )
                    event = CommitEvent(
                        count=event.count, need_confirm=True, resume_token=token
                    )
                self._unconfirmed_events[event.count] = event

    def _process_confirmed_event(self, event: CommitEvent) -> None:
        if event.count > self._last_sent_commit_event:
            if event.count in self._unconfirmed_events:
                token = self._get_actual_token(
                    old_token=self._unconfirmed_events[event.count].resume_token,
                    new_token=event.resume_token,
                )
                self._confirmed_events[event.count] = CommitEvent(
                    count=event.count, need_confirm=False, resume_token=token
                )
                del self._unconfirmed_events[event.count]
            else:
                if event.count in self._confirmed_events:
                    token = self._get_actual_token(
                        old_token=self._confirmed_events[event.count].resume_token,
                        new_token=event.resume_token,
                    )
                    event = CommitEvent(
                        count=event.count, need_confirm=False, resume_token=token
                    )

                self._confirmed_events[event.count] = event

    @staticmethod
    def _get_actual_token(
        old_token: bytes | None, new_token: bytes | None
    ) -> bytes | None:
        if old_token and not new_token:
            return old_token
        elif not old_token and not new_token:
            return None
        elif not old_token and new_token:
            return new_token
        elif old_token and new_token:
            return new_token

    def _get_committable_events(self) -> CommittableEvents | None:
        confirmed_numbers_with_tokens: list[int] = []
        commit_event_count = self._last_sent_commit_event + 1
        while True:
            if commit_event_count in self._confirmed_events:
                event = self._confirmed_events[commit_event_count]
                if event.need_confirm:
                    break
                if event.resume_token:
                    confirmed_numbers_with_tokens.append(commit_event_count)
                commit_event_count += 1
            else:
                break

        if confirmed_numbers_with_tokens:
            last_number = confirmed_numbers_with_tokens[-1]
            event = self._confirmed_events[last_number]
            return CommittableEvents(
                numbers=list(range(self._last_sent_commit_event + 1, last_number + 1)),
                resume_token=event.resume_token,
            )
        else:
            return None

    def _is_need_commit_events(
        self, events: CommittableEvents | None
    ) -> bool:  # tested
        if events is None or len(events.numbers) == 0:
            return False
        if time.monotonic() - self._last_commit_time > self._commit_interval:
            return True
        if len(events.numbers) > self._max_uncommitted_events:
            return True
        return False

    def _clear_previous_commit_events(self, events: CommittableEvents) -> None:
        for number in events.numbers:
            if number in self._confirmed_events:
                del self._confirmed_events[number]
            if number in self._unconfirmed_events:
                del self._unconfirmed_events[number]
            self._last_sent_commit_event = number
            self._last_commit_time = time.monotonic()
