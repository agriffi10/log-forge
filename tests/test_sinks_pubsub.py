"""SPEC-010/032 — GooglePubSubSink: publish-per-event, its round's futures, post-close.

SPEC-094 FR-002 put the sink on the delivery engine: each round waits on its own futures, so the
reap, the overflow wait, the pending list and ``close()``'s resolution are gone, and the tests of
them went with them. ``tests/test_pubsub_round_wait.py`` holds the round's wait.
"""

from __future__ import annotations

import json
import time

import pytest

from conftest import cpu_window
from log_foundry import SinkLosses
from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.pubsub import DEFAULT_OVERFLOW_TIMEOUT, GooglePubSubSink


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


def test_a_futures_error_is_counted_in_its_round_and_close_counts_nothing(capsys) -> None:
    """SPEC-094 FR-002: the round waits on its futures, so the error is counted before it returns.

    1.x resolved the futures at ``close()``; now ``emit`` reads each in its round, counts the
    ``UNKNOWN`` one once with its line, and ``close()`` has nothing left to resolve.
    """
    client = FakePublisher(errors=[None, RuntimeError("publish failed")])
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink.failed == 1
    assert "lost 1 event(s); GooglePubSubSink publish unconfirmed (RuntimeError)" in (
        capsys.readouterr().err
    )
    sink.close()
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

    SPEC-094 FR-002 keeps this test: a publish whose hand-off finds the sink closed is ``UNKNOWN``
    in its round, counted once, never waited on. Its assertion on the pending list went with the
    list, since no verdict is outstanding between emits any more.
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
    assert sink.losses() == SinkLosses(dropped=0, failed=2), "the unconfirmed publishes were not counted"


def test_a_batch_every_event_of_which_was_refused_still_raises() -> None:
    """The total-failure raise tests refusals, not successes (SPEC-026 FR-001).

    Nothing left the process, so there is nothing downstream a retry could duplicate — which is
    exactly the condition that makes raising safe.
    """

    class RefusingPublisher:
        def __init__(self) -> None:
            self.calls = 0

        def publish(self, topic, data=None, **attrs):
            self.calls += 1
            raise RuntimeError("refused")

    client = RefusingPublisher()
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])
    assert client.calls == 2, "the premise: both events were offered and refused"
    assert sink.losses() == SinkLosses(dropped=0, failed=2), (
        "on the engine (SPEC-094 FR-002) the inline adapter counts a failed run's events in "
        "failed before it raises, as every engine sink's does"
    )


# --- SPEC-038 FR-004: the futures are reaped, and the pending list is bounded -------------


class PollableFuture(FakeFuture):
    """A future that reports its own state, as a real Pub/Sub future does.

    `FakeFuture` above deliberately has no `done()`, which is what pins the "cannot poll" branch
    of `_has_settled`; this one exercises the branch a real client takes.
    """

    def __init__(self, error: Exception | None = None, *, settled: bool = True) -> None:
        super().__init__(error)
        self._settled = settled
        self.resolved = 0

    def done(self) -> bool:
        return self._settled

    def settle(self) -> None:
        self._settled = True

    def result(self, timeout=None) -> str:
        self.resolved += 1
        return super().result(timeout)


class PollablePublisher:
    """A publisher handing out futures a test controls the settlement of."""

    def __init__(self, factory) -> None:
        self.futures: list[PollableFuture] = []
        self._factory = factory

    def publish(self, topic, data=None, **attrs) -> PollableFuture:
        future = self._factory(len(self.futures))
        self.futures.append(future)
        return future


def test_an_outage_is_visible_in_losses_while_it_is_happening() -> None:
    """AC-1. Before FR-004 `failed` stayed 0 and `health()` read clean through a whole outage."""
    client = PollablePublisher(lambda _i: PollableFuture(RuntimeError("unavailable")))
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"a": i} for i in range(5)])
    assert sink.losses() == SinkLosses(dropped=0, failed=5), (
        "the failures are counted without waiting for close()"
    )


def test_a_future_whose_done_raises_reads_as_pending_until_the_deadline() -> None:
    """SPEC-094 FR-002: ``done()`` that raises cannot vouch for a ``TimeoutError``, so it waits.

    Its ``result`` raises ``TimeoutError`` and its state query fails, so the round cannot tell its
    outcome from a pending future's and waits it out, then reads it ``UNKNOWN``.
    """

    class BrokenState(PollableFuture):
        def done(self) -> bool:
            raise RuntimeError("state unavailable")

        def result(self, timeout=None) -> str:
            self.resolved += 1
            raise TimeoutError

    client = PollablePublisher(lambda _i: BrokenState(settled=False))
    sink = GooglePubSubSink("projects/p/topics/t", client=client, overflow_timeout=0.2)
    began = time.monotonic()
    sink.emit([{"a": 1}])
    assert time.monotonic() - began >= 0.2
    assert client.futures[0].resolved > 1, "asked again until the deadline"
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


class _NeverSettles(PollableFuture):
    """A publish the destination never confirms, recording every timeout it was waited on with.

    An **unbounded** wait blocks for `CLIENT_DEADLINE`, standing in for the real client's 600 s
    publish deadline. That is what makes a bounded-close test able to fail: with `result(None)`
    returning instantly, an unbounded close finishes in ~0 s and every wall-clock assertion holds
    either way -- the vacuous shape this repo keeps finding.
    """

    def __init__(self) -> None:
        super().__init__(settled=False)
        self.waits: list[float | None] = []

    def result(self, timeout=None):
        self.waits.append(timeout)
        if timeout is not None:
            raise TimeoutError("still in flight")
        time.sleep(CLIENT_DEADLINE)
        return "message-id"


CLIENT_DEADLINE = 5.0
"""Seconds an unbounded wait blocks in these tests, standing in for the client's own 600 s.

Long enough that an unbounded `close()` blows every bound asserted below, short enough that a
failure costs seconds rather than minutes. A passing run never reaches it.
"""


def test_a_bound_of_zero_is_floored_so_publishing_does_not_become_synchronous() -> None:
    assert GooglePubSubSink("projects/p/topics/t", client=FakePublisher(), max_pending=0).max_pending == 1


class StalledFuture(PollableFuture):
    """Never settles, and honours a timeout by sleeping it out — as a real future would."""

    def __init__(self) -> None:
        super().__init__(settled=False)
        self.waits: list[float | None] = []

    def result(self, timeout=None):
        self.waits.append(timeout)
        if timeout is None:
            return "message-id"
        time.sleep(timeout)
        raise TimeoutError("still in flight")


def test_the_overflow_wait_is_bounded_per_emit_not_per_future() -> None:
    """SPEC-027. A per-future timeout is not a bound: it multiplies by the number over the limit.

    Measured before the fix: 2.07 s for ten futures at a 0.2 s timeout, which at the shipped
    30 s default is five minutes of the single drain thread inside one `emit`.

    The budget is deliberately loose relative to the operation — the point is that ~10x the
    per-future timeout must NOT be reachable, so a generous ceiling still fails the old code.
    The honest pass takes one ``overflow_timeout`` (0.2 s) and a per-future one ten (2.0 s); the
    ceiling is half the latter. At 0.05 s the per-future pass took only 0.5 s, too short for a
    ceiling under it to clear a CI stall.
    """
    timeout = 0.2
    over_the_limit = 10
    client = PollablePublisher(lambda _i: StalledFuture())
    sink = GooglePubSubSink("t", client=client, max_pending=2, overflow_timeout=timeout)
    start = time.monotonic()
    sink.emit([{"i": i} for i in range(2 + over_the_limit)])
    elapsed = time.monotonic() - start
    assert elapsed < over_the_limit * timeout / 2, (
        f"one deadline covers the whole overflow pass; blocked {elapsed:.2f}s for "
        f"{over_the_limit} over the limit at a {timeout}s budget"
    )


def test_a_future_whose_result_takes_no_timeout_is_unknown_at_once() -> None:
    """SPEC-094 FR-002: a ``result`` that takes no ``timeout`` cannot be waited on in a round.

    1.x put it back and waited on it, unbounded, at ``close()``. The round reads it ``UNKNOWN``
    at once, counted, never resent, and ``close()`` waits on nothing.
    """

    class NoTimeoutFuture:
        def __init__(self) -> None:
            self.resolved = 0

        def done(self) -> bool:
            return False

        def result(self):  # no timeout parameter at all
            self.resolved += 1
            return "message-id"

    client = PollablePublisher(lambda _i: NoTimeoutFuture())
    sink = GooglePubSubSink("t", client=client, overflow_timeout=5)
    began = time.monotonic()
    sink.emit([{"i": i} for i in range(4)])
    assert time.monotonic() - began < 1.0
    assert sink.losses() == SinkLosses(dropped=0, failed=4)
    sink.close()
    assert all(future.resolved == 0 for future in client.futures), "never called without timeout"


def test_the_slice_loop_does_not_spin_when_a_bounded_wait_fails_instantly() -> None:
    """SPEC-027 again. A slice the future does not consume must still be waited out.

    Nothing obliges `result(timeout=)` to block for its timeout: a client that raises
    `TimeoutError` immediately turned the slice loop into a hot spin — measured at 3.5 million
    `result()` calls and a pegged core for one second, which is thirty at the shipped default,
    on the worker's single drain thread with no delivery happening. The remainder of each slice
    now goes through `_retry.wait`, which is where SPEC-027 says a sink's waiting belongs and
    which the first version of this loop bypassed.

    Asserted on **CPU** time, not wall time: the wall bound was already correct while the loop
    was spinning, so a wall-clock assertion cannot see this at all.
    """

    class InstantlyExpiring(PollableFuture):
        def __init__(self) -> None:
            super().__init__(settled=False)
            self.calls = 0

        def result(self, timeout=None):
            self.calls += 1
            if timeout is None:
                return "message-id"
            raise TimeoutError("instant")

    client = PollablePublisher(lambda _i: InstantlyExpiring())
    sink = GooglePubSubSink("t", client=client, max_pending=1, overflow_timeout=0.3)
    with cpu_window() as cpu:
        sink.emit([{"i": i} for i in range(4)])
    burned = cpu.spent
    calls = sum(future.calls for future in client.futures)
    assert burned < 0.1, f"the slice loop spun: {burned:.3f}s of CPU for a 0.3s bounded wait"
    assert calls < 100, f"and it made {calls} result() calls where a handful should do"


def test_close_is_bounded_rather_than_running_to_the_clients_deadline() -> None:
    """SPEC-048 FR-004. `close()` waited `timeout=None` per future against a 600 s deadline.

    `Worker.shutdown` closes the live sink **inline**, so one unreachable destination held process
    exit for the client's publish deadline, per future. Measured before the fix: `flush()` raised
    at 0.50 s while `close()` with the stop signal already set ran to 3.50 s against a stand-in
    deadline, and would have been 600 s against a real one.

    The budget is deliberately **generous**. What carries the assertion is the *gap* — 3 s against
    a `CLIENT_DEADLINE` of 5 s — not a tight bound, which is what keeps it from failing on its own
    setup under load. Verified by reverting `close()` to the unbounded form: this test then fails
    on the wall clock rather than passing, which it did while the stand-in returned instantly.
    """
    client = PollablePublisher(lambda _i: _NeverSettles())
    sink = GooglePubSubSink(
        "projects/p/topics/t", client=client, max_pending=1, overflow_timeout=0.3
    )
    sink.emit([{"i": i} for i in range(5)])
    wall = time.monotonic()
    with cpu_window() as cpu:
        sink.close()
    elapsed = time.monotonic() - wall
    burned = cpu.spent

    assert elapsed < 3.0, (
        f"close() is bounded; ran {elapsed:.2f}s against a {CLIENT_DEADLINE}s stand-in deadline"
    )
    assert burned < 0.1, f"and waits rather than spinning; burned {burned:.2f}s of CPU"


def test_close_absorbs_a_future_whose_result_raises() -> None:
    """close() is an isolation boundary (FR-011): an unresolved future must not crash the worker."""

    class Exploding(PollableFuture):
        def __init__(self) -> None:
            super().__init__(settled=False)

        def result(self, timeout=None):
            raise RuntimeError("the client is broken")

    client = PollablePublisher(lambda _i: Exploding())
    sink = GooglePubSubSink("t", client=client, max_pending=1, overflow_timeout=0.05)
    sink.emit([{"i": i} for i in range(3)])
    sink.close()
    assert sink.losses().failed == 3, "counted as unconfirmed rather than raised"


# --- SPEC-049 FR-002: overflow_timeout is floored, the third usable_timeout caller -------------


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), 0.0, -1.0])
def test_a_degenerate_overflow_timeout_is_floored_not_refused(bad: float) -> None:
    """Floored, for `KafkaSink(flush_timeout=)`'s reason and by the same helper.

    `nan` made every deadline comparison False and `inf` removed the bound outright, yet both
    *deliver* against a healthy client, so under FR-001's rule they are floor candidates; refusing
    them here while `usable_timeout` floors them for Kafka would be two rules for one value.
    """
    sink = GooglePubSubSink("projects/p/topics/t", client=FakePublisher(), overflow_timeout=bad)
    assert sink.overflow_timeout == DEFAULT_OVERFLOW_TIMEOUT


def test_a_usable_overflow_timeout_is_kept() -> None:
    sink = GooglePubSubSink("projects/p/topics/t", client=FakePublisher(), overflow_timeout=2.5)
    assert sink.overflow_timeout == 2.5
