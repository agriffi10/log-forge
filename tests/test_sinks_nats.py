"""SPEC-010 — NATSSink: sync-driven publish, JetStream path, drain-on-close (fake client)."""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
import threading
import time

import pytest

import log_foundry.sinks.nats as nats_sink_module
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


class AlternatingNATS(SlowNATS):
    """Fails every other publish, so a batch is genuinely mixed rather than all-or-nothing."""

    def __init__(self) -> None:
        super().__init__(per_event=0.0)
        self._n = 0

    def jetstream(self) -> AlternatingJetStream:
        return AlternatingJetStream(self)


class AlternatingJetStream:
    def __init__(self, owner: AlternatingNATS) -> None:
        self._owner = owner

    async def publish(self, subject, payload, *, timeout=None) -> None:  # noqa: ASYNC109
        self._owner.timeouts.append(timeout)
        self._owner.calls += 1
        self._owner._n += 1
        if self._owner._n % 2:
            raise RuntimeError("no responders")
        self._owner.published.append((subject, payload))


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
    # every other criterion here; this is the one that refuses it -- and the ONLY thing in the
    # suite that pins DEFAULT_PUBLISH_TIMEOUT's value, since every other test passes an explicit
    # `publish_timeout=`. It takes the default deliberately.
    #
    # The per-event cost is load-bearing. With a free double the batch costs microseconds, so no
    # *time* budget could ever truncate it and the test refuses only a *count* cap -- measured,
    # the whole suite stayed green with the default mutated to 0.02 s. The precondition below is
    # the test asserting its own sensitivity (the SPEC-038 FR-004/FR-005 idiom): a batch that
    # cost no time proves nothing about a bound measured in time.
    client = SlowNATS(per_event=0.005)
    sink = NATSSink("logs", client=client, jetstream=True)
    began = time.monotonic()
    sink.emit([{"n": i} for i in range(200)])
    elapsed = time.monotonic() - began
    sink.close()

    assert elapsed > 0.5, f"the batch cost no time, so no budget could truncate it: {elapsed:.3f}s"
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


def test_the_core_path_is_bounded_too_not_only_the_jetstream_one() -> None:
    # FR-001 AC-8, second clause. The first clause (a whole batch still delivers) cannot fail a
    # guard that skips the core path, so this is what pins it: measured, restricting the deadline
    # to `self._jetstream and remaining <= 0` left the whole suite green. `Client.publish` takes
    # no timeout and does not block, so what this defends is an injected `client=` that does.
    client = SlowNATS(per_event=0.05)
    sink = NATSSink("logs", client=client, jetstream=False, publish_timeout=0.3)
    sink.emit([{"n": i} for i in range(100)])
    sink.close()

    assert 0 < len(client.published) < 100, "the core path must honour the deadline too"


def test_a_mixed_batch_counts_each_failure_once_and_no_successes() -> None:
    # A batch where some publishes raise and some succeed, with the deadline NOT expired -- the
    # case no other test covers, and the reason a mutant moving `attempted += 1` into the success
    # branch survived the whole suite. That mutant makes `unattempted` count the failures a
    # second time, so five failures are reported as ten.
    client = AlternatingNATS()
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=30.0)
    sink.emit([{"n": i} for i in range(10)])
    sink.close()

    assert len(client.published) == 5
    assert sink.failed == 5, "a failure is booked once, never again as an unattempted event"


def test_the_deadline_drop_is_announced_not_only_counted(capsys) -> None:
    # An absorbed loss that moves a counter silently is what `_diag` exists to prevent, and
    # deleting the announcement survived the whole suite: `capsys` was asserted only for the
    # per-event publish error, never for the deadline drop.
    client = SlowNATS(per_event=0.08)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=0.25)
    sink.emit([{"n": i} for i in range(30)])
    sink.close()

    err = capsys.readouterr().err
    assert "publish_timeout" in err, "the events the deadline skipped must be announced"
    assert str(sink.failed) in err, "the announcement carries the count, not one line per event"


def test_an_unserializable_event_is_isolated_not_allowed_to_destroy_the_batch() -> None:
    # Per-event isolation covers SERIALIZATION, not only the publish. A revision of this file
    # hoisted `json.dumps` out of the `try` to build one payload for both branches, and measured
    # against main that turned 4-of-5 delivered into 2-of-5: the TypeError escaped `emit`, and
    # because it is not a SinkDeliveryError the worker retried the whole batch, delivering the
    # first two events four times each with every counter at zero -- duplicate delivery
    # (SPEC-018) and silent loss (SPEC-026) in one path.
    #
    # `build_event` makes this unreachable through `@trace`/`info` (SPEC-017), but `emit` is
    # public API and the docstring promises the isolation.
    client = SlowNATS(per_event=0.0)
    sink = NATSSink("logs", client=client, jetstream=True, publish_timeout=30.0)
    batch: list[dict[str, object]] = [{"n": 0}, {"n": 1}, {"bad": object()}, {"n": 3}, {"n": 4}]
    sink.emit(batch)          # must NOT raise: four of the five published
    sink.close()

    assert len(client.published) == 4
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


# --- SPEC-047: the driver-facing claims, gated on the extras being installed (SPEC-043's idiom) ---

_EXTRAS_EXPECTED = os.environ.get("LOG_FOUNDRY_EXTRAS") == "1"
"""Whether the optional extras are expected to be importable.

`LOG_FOUNDRY_EXTRAS=1` makes the checks below **fail** rather than skip, which is the point:
CI's gating leg deliberately has no extras, and a check that silently skips there is a check that
never runs anywhere. SPEC-043 added this gate after four `SentrySink` tests were found green only
because the extra was never installed.
"""


def test_our_ack_ceiling_still_mirrors_the_drivers_own_default() -> None:
    # DEFAULT_ACK_TIMEOUT's docstring claims a divergence "can only ever make the per-publish
    # timeout smaller than the driver would have used, never larger". That claim depends on a
    # third-party constant, and nothing noticed if either side moved -- measured, mutating our
    # constant 5.0 -> 50.0 left the whole suite green, because every assertion about it is
    # expressed IN TERMS of it. This turns the prose into a gate.
    if not _EXTRAS_EXPECTED:
        pytest.importorskip("nats", reason="the `nats` extra is not installed")
    import inspect

    from nats.js.client import JetStreamContext

    driver_default = inspect.signature(JetStreamContext.__init__).parameters["timeout"].default
    assert driver_default >= DEFAULT_ACK_TIMEOUT, (
        f"our ceiling {DEFAULT_ACK_TIMEOUT} exceeds the driver's own {driver_default}, so a "
        "publish would get a LONGER ack wait than the driver would have chosen"
    )


def test_the_core_publish_still_takes_no_timeout() -> None:
    # The other half of the same premise: `_publish_all`'s core branch passes no `timeout=`
    # because `Client.publish` has no such parameter. If the driver gained one, passing it would
    # be the better behaviour and this test is where that is noticed -- `_publish_all` catches
    # every per-event exception, so a wrong keyword would otherwise look like a counted failure.
    if not _EXTRAS_EXPECTED:
        pytest.importorskip("nats", reason="the `nats` extra is not installed")
    import inspect

    from nats.aio.client import Client

    assert "timeout" not in inspect.signature(Client.publish).parameters


# --- SPEC-047 FR-002: the driver's connect bounds are reachable from the constructor ----------


class FakeNatsModule:
    """A stand-in for the `nats` module, capturing what `connect` was asked for.

    Nothing in this suite drove the constructor's *build* path before SPEC-047 -- every test
    injects `client=` -- so the kwargs it forwards were unobservable.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[tuple, dict]] = []

    async def _connect(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return FakeNATS()

    def connect(self, *args, **kwargs):
        return self._connect(*args, **kwargs)


def _build(monkeypatch, **kwargs) -> FakeNatsModule:
    module = FakeNatsModule()
    monkeypatch.setitem(sys.modules, "nats", module)
    NATSSink("logs", servers="nats://example:4222", **kwargs).close()
    return module


def test_each_connect_bound_is_forwarded_to_the_driver(monkeypatch) -> None:
    # FR-002 AC-1.
    module = _build(
        monkeypatch,
        connect_timeout=1.5,
        max_reconnect_attempts=3,
        reconnect_time_wait=0.25,
        drain_timeout=4.0,
    )
    _, kwargs = module.calls[0]
    assert kwargs == {
        "connect_timeout": 1.5,
        "max_reconnect_attempts": 3,
        "reconnect_time_wait": 0.25,
        "drain_timeout": 4.0,
    }


def test_omitting_them_reproduces_todays_call_exactly(monkeypatch) -> None:
    # FR-002 AC-3. Asserted on the ABSENCE of the keys, not on their values: passing the driver's
    # own defaults explicitly would look identical in a value assertion while changing the call.
    module = _build(monkeypatch)
    args, kwargs = module.calls[0]
    assert args == ("nats://example:4222",)
    assert kwargs == {}


def test_a_falsy_connect_bound_is_still_forwarded(monkeypatch) -> None:
    # The omission test is `value is not None`, not truthiness, and zero is the case that tells
    # them apart. Both zeros reach the driver and change its behaviour, so dropping them is a
    # silent loss of what the caller asked for: `reconnect_time_wait=0` means retry immediately
    # (measured, dropping it made a failure 100x slower -- 2.03 s against 0.02 s), and
    # ~~`max_reconnect_attempts=0` makes the connect loop unbounded, which the constructor's
    # docstring warns about. Forwarding zero faithfully is the point either way -- this test is
    # about the boundary, not an endorsement of the value.~~
    #
    # **Superseded in part by SPEC-049 FR-004**, which architecture.md section 12 had already
    # named as this item's closure: `max_reconnect_attempts=0` is REFUSED now, because "forwarding
    # faithfully" forwarded a value that never terminates the connect loop. The reasoning above is
    # struck rather than deleted (SPEC-021) because the *boundary* it pins is still the point, and
    # `reconnect_time_wait=0` still demonstrates it -- that one works, so SPEC-049 FR-001's rule
    # leaves it alone. The refusal has its own test below.
    module = _build(monkeypatch, reconnect_time_wait=0)
    _, kwargs = module.calls[0]
    assert kwargs == {"reconnect_time_wait": 0}, (
        "a falsy value the driver can use is still forwarded, which is the boundary this pins"
    )


@pytest.mark.parametrize("bad", [0, -1])
def test_a_non_positive_reconnect_bound_is_refused(monkeypatch, bad: int) -> None:
    """SPEC-049 FR-004, closing the architecture.md section 12 item SPEC-047 opened.

    `nats-py` retires a server from its pool only under `max_reconnect_attempts > 0`, so a
    non-positive value never retires one and the connect loop does not terminate -- measured, both
    0 and -1 were still blocking at 30 s and one probe of 0 ran past 400 s. SPEC-047 forwarded it
    faithfully and documented the hazard; section 12 recorded that refusing it was "a small
    breaking change" waiting for a major version, and 1.0 is that version.
    """
    with pytest.raises(ValueError, match="max_reconnect_attempts"):
        _build(monkeypatch, max_reconnect_attempts=bad)


@pytest.mark.parametrize(
    "kwarg",
    ["connect_timeout", "max_reconnect_attempts", "reconnect_time_wait", "drain_timeout"],
)
def test_a_connect_bound_alongside_an_injected_client_is_refused(kwarg: str) -> None:
    # FR-002 AC-4. An injected client is already connected, so the argument can have no effect;
    # ignoring it would silently discard the caller's bound (SPEC-043's rule).
    with pytest.raises(ValueError, match=kwarg):
        NATSSink("logs", client=FakeNATS(), **{kwarg: 1})


def test_a_falsy_connect_bound_alongside_an_injected_client_is_also_refused() -> None:
    # The same `is not None` boundary on the refusal side: a truthiness test would accept
    # `reconnect_time_wait=0` against a client that cannot consume it, which is the silent ignore
    # SPEC-043 forbids.
    with pytest.raises(ValueError, match="reconnect_time_wait"):
        NATSSink("logs", client=FakeNATS(), reconnect_time_wait=0)


def test_publish_timeout_is_not_refused_alongside_an_injected_client() -> None:
    # FR-001 AC-10, as the counterpart to the rule above: publish_timeout is this sink's own
    # bound over its own loop, so it applies to any client. Every other test in this module
    # injects one, so folding it into the refusal set would break the whole file.
    sink = NATSSink("logs", client=FakeNATS(), publish_timeout=2.0)
    assert sink.publish_timeout == 2.0
    sink.close()


def test_a_refused_construction_creates_no_event_loop(monkeypatch) -> None:
    # The refusal is raised BEFORE `asyncio.new_event_loop()`, not after. Raising after it leaves
    # a loop nobody closes, which surfaces only as a PytestUnraisableExceptionWarning whenever the
    # GC happens to reach it -- a green run with a leak in it.
    #
    # Asserted by counting the CONSTRUCTOR CALL, not by counting live loop objects: a survey of
    # `gc.get_objects()` sees every other test's loops too, so its baseline moves depending on
    # what ran first -- measured, it passed alone and failed beside `test_sinks_kafka.py`. The
    # invariant is an ordering one, so the ordering is what the test observes.
    created = 0
    real = asyncio.new_event_loop

    def counting_new_event_loop():
        nonlocal created
        created += 1
        return real()

    monkeypatch.setattr(asyncio, "new_event_loop", counting_new_event_loop)
    with pytest.raises(ValueError):
        NATSSink("logs", client=FakeNATS(), connect_timeout=1.0)

    assert created == 0, "the refusal must come before the loop is built, or it leaks one"


def _forwarded_connect_kwargs() -> set[str]:
    """The kwarg names `NATSSink.__init__` actually forwards, read from its own source.

    Derived, never hand-listed. A literal list here is a second roster that rots: renaming the key
    in `nats.py` and the expectation in the forwarding test leaves a hand-written list still
    naming the old, valid name, so the check passes while every real construction raises
    `TypeError` -- measured, exactly that mutant was green across the whole file.
    """
    import ast

    source = pathlib.Path(nats_sink_module.__file__).read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "__init__":
            for sub in ast.walk(node):
                if isinstance(sub, ast.Dict) and any(
                    isinstance(k, ast.Constant) and k.value == "connect_timeout"
                    for k in sub.keys
                ):
                    return {
                        k.value for k in sub.keys if isinstance(k, ast.Constant)
                    }
    raise AssertionError("could not locate the forwarded-options dict in nats.py")


def test_the_forwarded_kwarg_roster_is_derived_and_not_empty() -> None:
    # The guard on the guard, kept to one assertion: a resolver that silently returned an empty
    # set would make the driver check below vacuous in the quietest possible way.
    assert len(_forwarded_connect_kwargs()) == 4


def test_every_connect_kwarg_we_forward_is_one_the_driver_accepts() -> None:
    # FR-002's forwarding is asserted elsewhere against a FAKE module whose
    # `connect(*args, **kwargs)` swallows anything, so a wrong keyword name passes the whole
    # suite and raises TypeError at the first real construction. That is the SPEC-043 shape --
    # four SentrySink tests were green only because CI never installed the extra.
    if not _EXTRAS_EXPECTED:
        pytest.importorskip("nats", reason="the `nats` extra is not installed")
    import inspect

    from nats.aio.client import Client

    accepted = set(inspect.signature(Client.connect).parameters)
    unknown = _forwarded_connect_kwargs() - accepted
    assert not unknown, f"NATSSink forwards {unknown}, which nats.connect does not accept"
