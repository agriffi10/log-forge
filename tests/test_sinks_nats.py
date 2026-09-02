"""SPEC-010 — NATSSink: sync-driven publish, JetStream path, drain-on-close (fake client)."""

from __future__ import annotations

import asyncio
import json
import threading
import time

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses
from log_foundry.sinks.nats import (
    DEFAULT_ACK_TIMEOUT,
    DEFAULT_PUBLISH_TIMEOUT,
    NATSSink,
)


class FakeJetStream:
    def __init__(self, owner: FakeNATS) -> None:
        self._owner = owner

    # `timeout` is keyword-only and recorded, because SPEC-047 FR-001 AC-3 asserts the value the
    # sink passes. A double that accepted **kwargs would swallow a wrong keyword name silently,
    # which is the failure `_publish_all` already hides by catching every per-event exception.
    # ASYNC109 wants `asyncio.timeout` instead, which does not apply: this signature MIRRORS
    # `JetStreamContext.publish`, so the parameter is the driver's API and not a design choice
    # of ours. Renaming it would stop the double catching a wrong keyword in the sink.
    async def publish(self, subject, payload, *, timeout=None) -> None:  # noqa: ASYNC109
        self._owner.js_timeouts.append(timeout)
        self._owner.js_published.append((subject, payload))


class FakeNATS:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple] = []
        self.js_published: list[tuple] = []
        self.js_timeouts: list[float | None] = []
        self.drained = False
        self._fail = fail

    async def publish(self, subject, payload) -> None:
        if self._fail:
            raise RuntimeError("no responders")
        self.published.append((subject, payload))

    def jetstream(self) -> FakeJetStream:
        return FakeJetStream(self)

    async def drain(self) -> None:
        self.drained = True


def test_is_a_sink() -> None:
    sink = NATSSink("subject", client=FakeNATS())
    assert isinstance(sink, Sink)
    sink.close()


def test_publishes_one_message_per_event() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    sink.close()
    assert client.published == [
        ("logs", json.dumps({"a": 1}).encode("utf-8")),
        ("logs", json.dumps({"a": 2}).encode("utf-8")),
    ]


def test_jetstream_path_publishes_via_jetstream() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client, jetstream=True)
    sink.emit([{"a": 1}])
    sink.close()
    assert client.js_published == [("logs", json.dumps({"a": 1}).encode("utf-8"))]
    assert client.published == []


def test_publish_errors_counted(capsys) -> None:
    client = FakeNATS(fail=True)
    sink = NATSSink("logs", client=client)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])  # nothing published (SPEC-026 FR-001)
    sink.close()
    assert sink.failed == 2
    assert capsys.readouterr().err.count("lost 1 event(s)") == 2


def test_close_drains_the_connection() -> None:
    client = FakeNATS()
    sink = NATSSink("logs", client=client)
    sink.close()
    assert client.drained is True
    sink.close()  # idempotent (loop already closed)


# -- SPEC-041 FR-004 AC-5: a disconnected client is reported, not absorbed --------------------


class DisconnectedNATS(FakeNATS):
    """A client that reports itself disconnected, as `nats-py` does while reconnecting."""

    is_connected = False


def test_a_disconnected_client_makes_emit_report_total_non_delivery() -> None:
    client = DisconnectedNATS()
    sink = NATSSink("subject", client=client)

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])

    # A core publish would have "succeeded" into the client's outbound buffer and been reported
    # as delivered -- measured against a real server as 1 of 6 events arriving with every counter
    # at zero. Nothing must reach the client at all.
    assert client.published == []


def test_refusing_moves_no_loss_counter() -> None:
    # SPEC-032's rule: a refusal is a failure REPORTED to the worker, which records it in
    # health().failed_batches, not one this sink absorbed. Counting it here reports it twice.
    sink = NATSSink("subject", client=DisconnectedNATS())

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])

    assert sink.losses().failed == 0
    assert sink.losses().dropped == 0


def test_a_client_that_says_nothing_about_connectedness_is_still_published_to() -> None:
    # The probe is by name because an injected client need not be `nats-py`. Assuming a silent
    # client is disconnected would fail batches that were going to succeed.
    client = FakeNATS()
    assert not hasattr(client, "is_connected")
    sink = NATSSink("subject", client=client)

    sink.emit([{"a": 1}])

    assert len(client.published) == 1


def test_a_client_whose_probe_raises_is_treated_as_connected() -> None:
    class Hostile(FakeNATS):
        @property
        def is_connected(self):
            raise RuntimeError("driver fault")

    client = Hostile()
    sink = NATSSink("subject", client=client)

    sink.emit([{"a": 1}])   # a diagnostic probe must never be the reason a batch fails

    assert len(client.published) == 1


# --- SPEC-047 FR-001: one deadline bounds the whole batch, not each event in it -------------


class SlowJetStream:
    """A JetStream double whose publish costs real time, so a per-event bound is visible."""

    def __init__(self, owner: SlowNATS) -> None:
        self._owner = owner

    # ASYNC109 wants `asyncio.timeout` instead, which does not apply: this signature MIRRORS
    # `JetStreamContext.publish`, so the parameter is the driver's API and not a design choice
    # of ours. Renaming it would stop the double catching a wrong keyword in the sink.
    async def publish(self, subject, payload, *, timeout=None) -> None:  # noqa: ASYNC109
        self._owner.timeouts.append(timeout)
        self._owner.calls += 1
        await asyncio.sleep(self._owner.per_event)
        if self._owner.fail:
            raise RuntimeError("no responders")
        self._owner.published.append((subject, payload))


class SlowNATS:
    def __init__(self, per_event: float = 0.05, fail: bool = False) -> None:
        self.per_event = per_event
        self.fail = fail
        self.calls = 0
        self.published: list[tuple] = []
        self.timeouts: list[float | None] = []
        self.is_connected = True

    async def publish(self, subject, payload) -> None:
        self.calls += 1
        await asyncio.sleep(self.per_event)
        if self.fail:
            raise RuntimeError("no responders")
        self.published.append((subject, payload))

    def jetstream(self) -> SlowJetStream:
        return SlowJetStream(self)

    async def drain(self) -> None:
        pass


def test_a_whole_batch_is_bounded_not_each_event_in_it() -> None:
    # FR-001 AC-1. 100 events at 0.05 s each is 5 s of per-event cost; the batch budget is 0.3 s.
    # The generous 2.0 s assertion is deliberate -- this proves something IS bounded, so the gap
    # between bounded (0.3) and unbounded (5.0) is what carries the test, not a tight budget.
    client = SlowNATS(per_event=0.05)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.3)
    began = time.monotonic()
    sink.emit([{"n": i} for i in range(100)])
    elapsed = time.monotonic() - began
    sink.close()

    assert elapsed < 2.0, f"the batch was not bounded: {elapsed:.2f}s"
    # Not vacuous in either direction: something published (so it is not a no-op that returns
    # instantly) and not everything did (so the deadline, not the batch running out, ended it).
    assert 0 < len(client.published) < 100


def test_the_unbounded_implementation_fails_that_bound(monkeypatch) -> None:
    # FR-001 AC-2. The pre-SPEC-047 loop, replanted: no deadline, no per-event timeout argument.
    # A bound whose test passes without the bound is the vacuity this repo keeps measuring.
    async def unbounded(self, batch):
        target = self._client.jetstream() if self._jetstream else self._client
        for event in batch:
            await target.publish(self._subject, json.dumps(event).encode("utf-8"))

    monkeypatch.setattr(NATSSink, "_publish_all", unbounded)
    client = SlowNATS(per_event=0.05)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.3)
    began = time.monotonic()
    sink.emit([{"n": i} for i in range(100)])
    elapsed = time.monotonic() - began
    sink.close()

    assert elapsed > 2.0, "the mutant must exceed the bound the real test asserts"
    assert len(client.published) == 100


def test_the_per_publish_timeout_is_capped_by_the_ack_ceiling() -> None:
    # FR-001 AC-3, first end: with budget to spare, the ceiling binds and the driver never sees
    # a longer ack wait than its own default.
    client = SlowNATS(per_event=0.0)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=60.0)
    sink.emit([{"n": 1}, {"n": 2}])
    sink.close()

    assert client.timeouts == [DEFAULT_ACK_TIMEOUT, DEFAULT_ACK_TIMEOUT]


def test_the_per_publish_timeout_shrinks_as_the_budget_is_spent() -> None:
    # FR-001 AC-3, second end: once the remaining budget is under the ceiling, it is the budget
    # that binds -- which is what proves the deadline is actually decreasing rather than a
    # constant passed once.
    client = SlowNATS(per_event=0.02)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.3)
    sink.emit([{"n": i} for i in range(50)])
    sink.close()

    assert client.timeouts[0] <= 0.3
    assert client.timeouts[-1] < client.timeouts[0], client.timeouts


def test_a_large_batch_against_a_healthy_server_is_not_truncated() -> None:
    # FR-001 AC-4. A hard cap that truncates a slow-but-succeeding exit backlog would satisfy
    # every other criterion here; this is the one that refuses it.
    client = SlowNATS(per_event=0.0)
    sink = NATSSink("logs", client=client, jetstream=True)
    sink.emit([{"n": i} for i in range(200)])
    sink.close()

    assert len(client.published) == 200
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_the_sink_declares_no_stop_signal_and_a_set_one_does_not_shorten_a_batch() -> None:
    # FR-001 AC-5. `_lifecycle.offer_stop_signal` probes by hasattr, so the absence IS the opt-out
    # -- a shutdown shortens a wait and never skips work, and this per-event await is work.
    # The positive half cannot fail today because nothing reads the attribute: it is a regression
    # guard, verified by adding the read and watching it redden, not by passing.
    client = SlowNATS(per_event=0.0)
    sink = NATSSink("logs", client=client, jetstream=True)
    assert not hasattr(sink, "log_foundry_stop_signal")

    signal = threading.Event()
    signal.set()
    sink.log_foundry_stop_signal = signal  # type: ignore[attr-defined]
    sink.emit([{"n": i} for i in range(20)])
    sink.close()

    assert len(client.published) == 20, "a set stop event must not skip work"


def test_an_expired_budget_with_nothing_published_books_only_what_it_attempted() -> None:
    # FR-001 AC-7, raising path. `Worker._emit` retries the whole batch on an exception, so
    # booking the never-attempted remainder here would report a loss that has not happened.
    client = SlowNATS(per_event=0.08, fail=True)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.25)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"n": i} for i in range(30)])
    sink.close()

    assert client.calls < 30, "the deadline must have stopped the loop short"
    assert sink.failed == client.calls, "only the attempts that raised are booked"


def test_an_expired_budget_with_something_published_books_the_remainder() -> None:
    # FR-001 AC-7, returning path. The worker will not retry a batch that returned, so the
    # unattempted remainder is a real loss and is counted.
    client = SlowNATS(per_event=0.08)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.25)
    sink.emit([{"n": i} for i in range(30)])
    sink.close()

    published = len(client.published)
    assert 0 < published < 30
    assert sink.failed == 30 - published, "every event the deadline skipped is counted once"


def test_the_core_path_delivers_a_whole_batch_with_a_publish_timeout_set() -> None:
    # FR-001 AC-8. `Client.publish` accepts no timeout, so the core branch must pass none -- and
    # the deadline is checked between events only, which is all there is to check on a path that
    # writes into the client's outbound buffer and returns.
    client = SlowNATS(per_event=0.0)
    sink = NATSSink("logs", client=client, jetstream=False, publish_timeout=5.0)
    sink.emit([{"n": i} for i in range(50)])
    sink.close()

    assert len(client.published) == 50
    assert client.timeouts == [], "the core path must not pass a timeout the driver cannot take"


@pytest.mark.parametrize("bad", [0, -1.0, float("inf"), float("nan")])
def test_a_publish_timeout_that_bounds_nothing_falls_back_to_the_default(bad: float) -> None:
    # FR-001 AC-9.
    sink = NATSSink("logs", client=FakeNATS(), publish_timeout=bad)
    assert sink.publish_timeout == DEFAULT_PUBLISH_TIMEOUT, f"{bad!r} bounds nothing"
    sink.close()


def test_publish_timeout_applies_to_an_injected_client() -> None:
    # FR-001 AC-10. It is this sink's own bound over its own loop, not a connect-time request, so
    # it is deliberately NOT one of the arguments FR-002 refuses alongside `client=`.
    client = SlowNATS(per_event=0.05)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.2)
    began = time.monotonic()
    sink.emit([{"n": i} for i in range(100)])
    elapsed = time.monotonic() - began
    sink.close()

    assert elapsed < 2.0
    assert sink.publish_timeout == 0.2
