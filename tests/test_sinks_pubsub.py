"""SPEC-010/032 — GooglePubSubSink: publish-per-event, future flush on close, post-close."""

from __future__ import annotations

import json

import pytest

from log_foundry import SinkLosses

from log_foundry.sinks.base import Sink, SinkDeliveryError
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


def test_a_closed_sink_refuses_a_publish_without_moving_a_counter() -> None:
    """Refusing is a reported failure, not absorbed loss (SPEC-032 FR-001)."""
    client = FakePublisher()
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.close()

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])

    assert sink.losses() == SinkLosses(dropped=0, failed=0), "a refusal moved a loss counter"
    assert client.published == []


def test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise() -> None:
    """The window the second check closes: a future appended to a list the swap already took.

    Without the re-check under ``_futures_lock`` this future is appended to the fresh list and
    nothing ever calls ``result()`` on it — the silent loss ``close()`` was fixed for in
    SPEC-028, reached from outside it. It must not *raise* either: ``publish()`` was called on
    the first event and it may well land, so a retry would duplicate it (SPEC-018's rule).
    """
    closed_after_first: list[int] = []

    class ClosingPublisher(FakePublisher):
        def publish(self, topic, data=None, **attrs) -> FakeFuture:
            future = super().publish(topic, data, **attrs)
            closed_after_first.append(1)
            if len(closed_after_first) == 1:
                sink.close()
            return future

    client = ClosingPublisher()
    sink = GooglePubSubSink("projects/p/topics/t", client=client)

    sink.emit([{"a": 1}, {"a": 2}])

    assert len(client.published) == 2, "the loop stopped early instead of counting the rest"
    assert sink._futures == [], "a future was appended to a list nothing will resolve"
    assert sink.losses() == SinkLosses(dropped=0, failed=2), "the unconfirmed publishes were not counted"


def test_a_batch_every_event_of_which_was_refused_still_raises() -> None:
    """The total-failure raise tests refusals, not successes (SPEC-026 FR-001).

    Nothing left the process, so there is nothing downstream a retry could duplicate — which is
    exactly the condition that makes raising safe.
    """

    class RefusingPublisher:
        def publish(self, topic, data=None, **attrs):
            raise RuntimeError("refused")

    sink = GooglePubSubSink("projects/p/topics/t", client=RefusingPublisher())
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])
    assert sink.losses() == SinkLosses(dropped=2, failed=0)
