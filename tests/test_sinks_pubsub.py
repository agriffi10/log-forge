"""SPEC-010/032 — GooglePubSubSink: publish-per-event, future flush on close, post-close."""

from __future__ import annotations

import json
import threading
import time

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


def test_a_settled_future_is_reaped_by_the_next_emit_not_held_until_close() -> None:
    """AC-1. The list must not grow with events the client has already finished with."""
    client = PollablePublisher(lambda _i: PollableFuture(settled=True))
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"a": 1}, {"a": 2}])
    assert sink._futures == [], "a settled future is resolved and released during emit"
    assert all(future.resolved == 1 for future in client.futures)


def test_an_outage_is_visible_in_losses_while_it_is_happening() -> None:
    """AC-1. Before FR-004 `failed` stayed 0 and `health()` read clean through a whole outage."""
    client = PollablePublisher(lambda _i: PollableFuture(RuntimeError("unavailable")))
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"a": i} for i in range(5)])
    assert sink.losses() == SinkLosses(dropped=0, failed=5), (
        "the failures are counted without waiting for close()"
    )


def test_an_unsettled_future_is_kept_and_resolved_once_it_settles() -> None:
    client = PollablePublisher(lambda _i: PollableFuture(settled=False))
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"a": 1}])
    assert len(sink._futures) == 1, "an in-flight publish is not waited on"
    assert client.futures[0].resolved == 0
    client.futures[0].settle()
    sink.emit([{"a": 2}])
    assert client.futures[0].resolved == 1, "the next emit reaps what has since settled"


def test_the_pending_list_does_not_grow_with_total_events_logged() -> None:
    """AC-4. Asserts the list length under sustained load, not the shape of the code."""
    client = PollablePublisher(lambda _i: PollableFuture(settled=False))
    sink = GooglePubSubSink("projects/p/topics/t", client=client, max_pending=50)
    lengths = []
    for _ in range(100):
        sink.emit([{"a": 1}] * 10)
        lengths.append(len(sink._futures))
    assert len(client.futures) == 1000, "the load really was sustained"
    assert max(lengths) <= 50, f"the pending list exceeded its bound: {max(lengths)}"
    assert lengths[-1] == lengths[len(lengths) // 2], "and it is flat, not merely capped once"


def test_at_the_bound_emit_waits_on_the_oldest_rather_than_dropping_it() -> None:
    """AC-2. The event is already with the client, so discarding it here would invent a loss."""
    client = PollablePublisher(lambda _i: PollableFuture(settled=False))
    sink = GooglePubSubSink("projects/p/topics/t", client=client, max_pending=3)
    sink.emit([{"a": i} for i in range(5)])
    assert [future.resolved for future in client.futures] == [1, 1, 0, 0, 0], (
        "the two oldest were waited on; the newest three stay outstanding"
    )
    assert len(sink._futures) == 3
    assert sink.failed == 0, "waiting on a future that succeeds is not a loss"


def test_a_future_whose_done_raises_is_resolved_rather_than_held_forever() -> None:
    class BrokenState(PollableFuture):
        def done(self) -> bool:
            raise RuntimeError("state unavailable")

    client = PollablePublisher(lambda _i: BrokenState(settled=False))
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"a": 1}])
    assert sink._futures == [], "a future that cannot report its state does not occupy the bound"
    assert client.futures[0].resolved == 1


def test_close_still_resolves_whatever_is_left_outstanding() -> None:
    """AC-3. The reap narrows what close has to do; it must not replace it."""
    client = PollablePublisher(lambda _i: PollableFuture(RuntimeError("boom"), settled=False))
    sink = GooglePubSubSink("projects/p/topics/t", client=client, max_pending=100)
    sink.emit([{"a": i} for i in range(4)])
    assert sink.failed == 0 and len(sink._futures) == 4
    sink.close()
    assert sink.failed == 4
    assert sink._futures == []


class SettlingFuture(PollableFuture):
    """A future that reports done() False once, then True — a real driver state transition.

    `PollableFuture` above answers from a stable flag, so both halves of a two-pass partition
    always agree and the race below is invisible to it. This one flips on query, which is what a
    Pub/Sub client's commit thread does mid-scan.
    """

    def __init__(self, error: Exception | None = None, *, settle_after: int = 1) -> None:
        super().__init__(error, settled=False)
        self.queries = 0
        self._settle_after = settle_after

    def done(self) -> bool:
        self.queries += 1
        return self.queries > self._settle_after


def test_a_future_that_settles_mid_scan_is_never_dropped() -> None:
    """Every future is resolved or retained — never neither. FR-004's own defect, reintroduced.

    Partitioning with two comprehensions queried `done()` twice per future, so one settling
    between the queries landed in neither list: not resolved, and dropped by the reassignment.
    Measured, five failed publishes vanished with `losses()` reading clean, and under a thread
    race 41% of failures went uncounted — the silent loss this method exists to end.

    The assertion is conservation rather than a count, because how many futures settle mid-scan
    is a race. Conservation holds whichever way the race falls; a count would not.
    """
    client = PollablePublisher(lambda _i: SettlingFuture(RuntimeError("publish failed")))
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"i": i} for i in range(5)])

    resolved_now = sum(1 for future in client.futures if future.resolved)
    assert resolved_now + len(sink._futures) == 5, (
        f"{5 - resolved_now - len(sink._futures)} future(s) were neither resolved nor retained"
    )
    sink.close()
    assert all(future.resolved == 1 for future in client.futures), (
        "and close() reaches every one of them, exactly once"
    )
    assert sink.failed == 5


def test_the_reap_asks_each_future_for_its_state_once() -> None:
    """The property that makes the conservation above hold, pinned directly.

    A second query is a second chance for the answer to change, and the partition cannot act on
    two different answers about one future.
    """
    client = PollablePublisher(lambda _i: SettlingFuture(settle_after=99))
    sink = GooglePubSubSink("projects/p/topics/t", client=client)
    sink.emit([{"i": 0}])
    assert [future.queries for future in client.futures] == [1]


def test_an_overflow_wait_that_expires_puts_the_publish_back_rather_than_losing_it() -> None:
    """AC-2 + SPEC-027. The wait runs on the drain thread, so it is bounded — and a bound that
    discarded the publish would invent the loss AC-2 chose waiting to avoid.
    """

    class NeverSettles(PollableFuture):
        def __init__(self) -> None:
            super().__init__(settled=False)
            self.waits: list[float | None] = []

        def result(self, timeout=None):
            self.waits.append(timeout)
            raise TimeoutError("still in flight")

    client = PollablePublisher(lambda _i: NeverSettles())
    sink = GooglePubSubSink("projects/p/topics/t", client=client, max_pending=2, overflow_timeout=0.01)
    sink.emit([{"i": i} for i in range(5)])

    assert all(
        all(w is not None and w <= 0.01 for w in future.waits)
        for future in client.futures
        if future.waits
    ), "every wait is bounded by what is left of the emit's deadline, never indefinite"
    assert len(sink._futures) == 5, "an unfinished publish is retained, not dropped"
    assert sink.failed == 0, "an expired wait is not a failure; the publish is unfinished"


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


def _closed_with_stop(stopping: bool) -> tuple[float, SinkLosses, list[_NeverSettles]]:
    """Emits four never-settling publishes and closes, with or without the stop signal set.

    Returns the elapsed close, the losses and the futures, so a caller can compare the two runs
    rather than assert an absolute figure — the comparison is the rule under test.
    """
    client = PollablePublisher(lambda _i: _NeverSettles())
    sink = GooglePubSubSink(
        "projects/p/topics/t", client=client, max_pending=1, overflow_timeout=0.3
    )
    signal = threading.Event()
    if stopping:
        signal.set()
    sink.log_foundry_stop_signal = signal
    sink.emit([{"i": i} for i in range(4)])
    assert len(sink._futures) == 4, "everything is retained for close()"
    began = time.monotonic()
    sink.close()
    return time.monotonic() - began, sink.losses(), client.futures


def test_a_shutdown_defers_the_overflow_wait_to_close_rather_than_blocking_the_drain() -> None:
    """The stop signal skips the *wait*, not the work: close() does the same either way.

    This is the opposite of the mistake SPEC-038 FR-001 made — skipping delivery during the exit
    drain loses events, while deferring a wait to close() loses nothing, because close() is where
    an exit-time wait belongs.

    **This assertion replaces the one this test shipped with** (SPEC-048 FR-004). That was
    ``waits == [None]`` — close waits unbounded — which pinned the *mechanism* rather than the
    rule. Since close() is bounded, an unbounded wait is no longer how the rule is kept, and
    asserting it would have forced the design that reverses SPEC-038: `_out_of_time` returns True
    the instant the stop signal is set, and ``Worker.shutdown`` sets it *before* closing the sink
    inline, so a close sharing flush()'s guard abandons everything on every ordinary shutdown.
    Comparing the two runs asserts the rule directly, which is strictly stronger: it fails for any
    close that shortens itself because a shutdown is in progress, whatever the mechanism.
    """
    stopping, stopping_losses, stopping_futures = _closed_with_stop(True)
    running, running_losses, running_futures = _closed_with_stop(False)

    assert stopping_losses == running_losses, (
        "a shutdown must not change what close() gives up on"
    )
    assert stopping >= running * 0.5, (
        f"close() shortened itself because a shutdown was in progress: "
        f"{stopping:.2f}s stopping vs {running:.2f}s running"
    )
    stopping_waits = sum(len(f.waits) for f in stopping_futures)
    running_waits = sum(len(f.waits) for f in running_futures)
    assert stopping_waits >= running_waits // 2, (
        f"close() polled less because a shutdown was in progress: "
        f"{stopping_waits} waits stopping vs {running_waits} running"
    )
    assert all(
        None not in f.waits for f in stopping_futures
    ), "close() is bounded now: no future is waited on with timeout=None"
    assert stopping_futures[-1].waits == [0], (
        "one deadline covers the whole list, not one per future: once it expires the remaining "
        "futures get a single zero-second classification probe and no real wait, which is what "
        "keeps the bound a bound (SPEC-038: a bound applied per item is n x timeout) while still "
        "telling an expired future from an unboundable one"
    )


def test_a_bound_of_zero_is_floored_so_publishing_does_not_become_synchronous() -> None:
    assert GooglePubSubSink("projects/p/topics/t", client=FakePublisher(), max_pending=0).max_pending == 1


def test_the_reap_runs_even_when_every_publish_was_refused() -> None:
    """The reap is before the total-failure raise, so a wholly-refused batch still trims."""

    class Refusing:
        def __init__(self, held): self._held = held
        def publish(self, topic, data=None, **attrs):
            raise RuntimeError("refused")

    settled = PollableFuture(settled=True)
    sink = GooglePubSubSink("projects/p/topics/t", client=Refusing(settled))
    sink._futures.append(settled)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    assert settled.resolved == 1, "the earlier settled future was reaped despite the raise"
    assert sink._futures == []


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
    """
    client = PollablePublisher(lambda _i: StalledFuture())
    sink = GooglePubSubSink("t", client=client, max_pending=2, overflow_timeout=0.05)
    start = time.monotonic()
    sink.emit([{"i": i} for i in range(12)])
    elapsed = time.monotonic() - start
    assert elapsed < 0.05 * 4, (
        f"one deadline covers the whole overflow pass; blocked {elapsed:.2f}s for 10 over the "
        f"limit at a 0.05s budget"
    )


def test_a_shutdown_landing_mid_wait_stops_the_remaining_waits() -> None:
    """The stop signal is re-read each time round, not once before the loop.

    Read once, a shutdown arriving after the first future was not noticed until every one had
    been waited on — measured at 5.04 s for a signal set 0.3 s in.
    """
    client = PollablePublisher(lambda _i: StalledFuture())
    sink = GooglePubSubSink("t", client=client, max_pending=1, overflow_timeout=10.0)
    signal = threading.Event()
    sink.log_foundry_stop_signal = signal
    threading.Timer(0.05, signal.set).start()
    start = time.monotonic()
    sink.emit([{"i": i} for i in range(6)])
    elapsed = time.monotonic() - start
    assert elapsed < 1.0, f"the shutdown must cut the remaining waits short; blocked {elapsed:.2f}s"
    assert len(sink._futures) == 6, "and everything is retained for close()"


def test_a_future_whose_result_takes_no_timeout_is_not_counted_as_lost() -> None:
    """`client=` is a frozen public parameter, so a future without a timeout kwarg is reachable.

    Counting it invented loss on publishes that were going to succeed: three of four healthy
    ones reported `failed` with a TypeError line. It is put back and resolved unbounded at
    `close()` instead.
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
    sink = GooglePubSubSink("t", client=client, max_pending=1, overflow_timeout=0.01)
    sink.emit([{"i": i} for i in range(4)])
    assert sink.losses() == SinkLosses(dropped=0, failed=0), "no invented loss"
    sink.close()
    assert sink.failed == 0, "and they resolve cleanly once close() waits unbounded"
    assert all(future.resolved == 1 for future in client.futures)


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
    cpu = time.process_time()
    sink.emit([{"i": i} for i in range(4)])
    burned = time.process_time() - cpu
    calls = sum(future.calls for future in client.futures)
    assert burned < 0.1, f"the slice loop spun: {burned:.3f}s of CPU for a 0.3s bounded wait"
    assert calls < 100, f"and it made {calls} result() calls where a handful should do"


def test_an_unboundable_future_is_put_back_at_once_rather_than_waited_out() -> None:
    """A future whose `result()` takes no timeout cannot succeed under a bounded call.

    Retrying it every slice until the deadline is a pointless wait on the drain thread, so it
    breaks out immediately — distinct from "not settled yet", which must keep waiting.
    """

    class NoTimeout:
        def __init__(self) -> None:
            self.calls = 0

        def done(self) -> bool:
            return False

        def result(self):
            self.calls += 1
            return "message-id"

    client = PollablePublisher(lambda _i: NoTimeout())
    sink = GooglePubSubSink("t", client=client, max_pending=1, overflow_timeout=5.0)
    start = time.monotonic()
    sink.emit([{"i": i} for i in range(3)])
    elapsed = time.monotonic() - start
    assert elapsed < 1.0, f"an unboundable future must not be waited out; took {elapsed:.2f}s"
    assert sink.losses() == SinkLosses(dropped=0, failed=0), "and it is not counted as lost"
    assert len(sink._futures) == 3, "it is put back for close()"


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
    cpu = time.process_time()
    sink.close()
    elapsed = time.monotonic() - wall
    burned = time.process_time() - cpu

    assert elapsed < 3.0, (
        f"close() is bounded; ran {elapsed:.2f}s against a {CLIENT_DEADLINE}s stand-in deadline"
    )
    assert burned < 0.1, f"and waits rather than spinning; burned {burned:.2f}s of CPU"


def test_close_counts_the_publishes_it_abandoned(capsys) -> None:
    """What the bound gives up on is counted and announced once, with a count.

    `KafkaSink._flush_bounded`'s rule, for its reason: a bound that abandons silently trades one
    invisible loss for another.
    """
    client = PollablePublisher(lambda _i: _NeverSettles())
    sink = GooglePubSubSink(
        "projects/p/topics/t", client=client, max_pending=1, overflow_timeout=0.05
    )
    sink.emit([{"i": i} for i in range(5)])
    sink.close()

    assert sink.losses() == SinkLosses(dropped=0, failed=5), "every abandoned publish is counted"
    lines = [line for line in capsys.readouterr().err.splitlines() if "close bound" in line]
    assert len(lines) == 1, f"one line with a count, not one per publish; got {len(lines)}"
    assert "5 publish(es)" in lines[0]


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


def test_a_close_landing_mid_flush_bounds_and_counts_its_leftovers(capsys) -> None:
    """The flush-races-close tail took `timeout=None` too, one branch deeper.

    When a `close()` lands while `flush()` is resolving, the leftover futures are flush's and
    nothing else will resolve them — so that branch owns them, and before this it owned them
    unbounded. It shares `close()`'s tail now, which is also what keeps its counter.
    """
    holder: dict[str, object] = {"sink": None, "armed": False}

    class ClosesMidFlush(_NeverSettles):
        """Marks the sink closed the first time flush() waits on it, which is the race.

        Armed only after `emit` has returned: with the trigger live during emit, `_await_overflow`
        fires it instead and the test exercises the wrong tail.
        """

        def result(self, timeout=None):
            sink_obj = holder["sink"]
            if holder["armed"] and timeout is not None and not sink_obj._closed:
                with sink_obj._futures_lock:
                    sink_obj._closed = True
            return super().result(timeout)

    client = PollablePublisher(lambda _i: ClosesMidFlush())
    sink = GooglePubSubSink("t", client=client, max_pending=50, overflow_timeout=0.05)
    holder["sink"] = sink
    sink.emit([{"i": i} for i in range(4)])
    assert not sink._closed, "the race must start from an open sink, or flush refuses at its top"
    holder["armed"] = True
    began = time.monotonic()
    with pytest.raises(SinkDeliveryError):
        sink.flush()
    elapsed = time.monotonic() - began
    assert sink._closed, "the close landed during the pass, which is the branch under test"

    assert elapsed < 3.0, f"the leftover branch is bounded too; ran {elapsed:.2f}s"
    assert sink.losses().failed == 4, "and counts what it abandoned"
    assert any("close bound" in line for line in capsys.readouterr().err.splitlines())


def test_a_close_landing_mid_emit_bounds_the_overflow_tail_too(capsys) -> None:
    """The third close-race tail, and the one that could hang an application thread.

    `_await_overflow` runs on whichever thread called `emit` — which on the orphan path, a level
    call with no active span, is an **application** thread (SPEC-028). Its leftover branch
    resolved with `timeout=None` like the other two, so a close landing mid-emit could park the
    caller on the client's publish deadline. It shares `close()`'s bounded tail now.
    """
    holder: dict[str, object] = {"sink": None}

    class ClosesMidEmit(_NeverSettles):
        def result(self, timeout=None):
            sink_obj = holder["sink"]
            if timeout is not None and not sink_obj._closed:
                with sink_obj._futures_lock:
                    sink_obj._closed = True
            return super().result(timeout)

    client = PollablePublisher(lambda _i: ClosesMidEmit())
    sink = GooglePubSubSink("t", client=client, max_pending=1, overflow_timeout=0.05)
    holder["sink"] = sink

    began = time.monotonic()
    sink.emit([{"i": i} for i in range(4)])
    elapsed = time.monotonic() - began

    assert elapsed < 3.0, f"the overflow tail is bounded; ran {elapsed:.2f}s"
    assert sink.losses().failed > 0, "and counts what it abandoned"
    assert any("close bound" in line for line in capsys.readouterr().err.splitlines()), (
        "the overflow tail's own abandonment line, not close()'s"
    )
    # `emit` deliberately does NOT raise here: every `publish()` call succeeded, so the events may
    # well land and re-sending them would duplicate what did (SPEC-018's rule that only provable
    # non-delivery may be retried). What the close cost is the *confirmation*, which is counted as
    # unconfirmed rather than reported as a failure to the worker.


def test_close_is_not_bounded_for_a_future_that_cannot_be_waited_on(capsys) -> None:
    """The documented exception to close()'s bound, pinned so it is a decision and not a surprise.

    A future whose `result()` takes no `timeout` is resolved unbounded, because that is the only
    wait it accepts and SPEC-036 measured that counting it instead invents loss on publishes that
    were going to succeed. So a client handing out futures that are BOTH unboundable AND slow holds
    `close()` for its own deadline, once per future — measured by a reviewer at 27.0 s for nine
    against a 3 s stand-in. `google-cloud-pubsub`'s own future accepts a timeout, so only an
    injected `client=` reaches this; `client=` is a frozen public parameter, which is why it is
    tested rather than assumed away.

    The existing regression test uses a future that returns *immediately*, so unboundable-and-slow
    was untested until this. Recorded in `architecture.md` §12.
    """
    delay = 0.4

    class UnboundableAndSlow:
        def __init__(self) -> None:
            self.resolved = 0

        def done(self) -> bool:
            return False

        def result(self):  # no timeout parameter at all
            self.resolved += 1
            time.sleep(delay)
            return "message-id"

    client = PollablePublisher(lambda _i: UnboundableAndSlow())
    sink = GooglePubSubSink("t", client=client, max_pending=50, overflow_timeout=0.05)
    sink.emit([{"i": i} for i in range(3)])
    began = time.monotonic()
    sink.close()
    elapsed = time.monotonic() - began

    assert elapsed >= delay * 3 * 0.8, (
        f"close() waits on each unboundable future in turn, unbounded: {elapsed:.2f}s. If this "
        f"ever fails, the bound was extended to cover them and the docstring must follow."
    )
    assert sink.losses() == SinkLosses(dropped=0, failed=0), (
        "and they are still not counted as lost -- SPEC-036's rule, which is why they are waited "
        "on unbounded in the first place"
    )
    assert all(f.resolved == 1 for f in client.futures), "every one was actually resolved"
