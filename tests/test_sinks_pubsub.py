"""SPEC-010 — GooglePubSubSink: publish-per-event + future flush on close (fake client)."""

from __future__ import annotations

import json

from log_foundry.sinks.base import Sink
from log_foundry.sinks.pubsub import GooglePubSubSink


class FakeFuture:
    def __init__(self, error: Exception | None = None) -> None:
        self._error = error

    def result(self, timeout=None) -> str:
        if self._error is not None:
            raise self._error
        return "message-id"


class FakePublisher:
    def __init__(self, errors: list[Exception | None] | None = None) -> None:
        self.published: list[tuple] = []
        self._errors = errors or []
        self._i = 0

    def publish(self, topic, data=None, **attrs) -> FakeFuture:
        self.published.append((topic, data))
        error = self._errors[self._i] if self._i < len(self._errors) else None
        self._i += 1
        return FakeFuture(error)


def test_is_a_sink() -> None:
    assert isinstance(GooglePubSubSink("projects/p/topics/t", client=FakePublisher()), Sink)


def test_publishes_one_message_per_event() -> None:
    client = FakePublisher()
    GooglePubSubSink("projects/p/topics/t", client=client).emit([{"a": 1}, {"a": 2}])
    assert client.published == [
        ("projects/p/topics/t", json.dumps({"a": 1}).encode("utf-8")),
        ("projects/p/topics/t", json.dumps({"a": 2}).encode("utf-8")),
    ]


def test_close_flushes_futures_and_counts_errors(capsys) -> None:
    client = FakePublisher(errors=[None, RuntimeError("publish failed")])
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.failed == 0  # not resolved until close
    sink.close()
    assert sink.failed == 1
    assert "lost 1 event(s)" in capsys.readouterr().err
    sink.close()  # idempotent; futures already cleared
    assert sink.failed == 1
