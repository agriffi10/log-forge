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

    assert all(future.waits == [0.01] for future in client.futures if future.waits), (
        "the wait is bounded by overflow_timeout, not indefinite"
    )
    assert len(sink._futures) == 5, "an unfinished publish is retained, not dropped"
    assert sink.failed == 0, "an expired wait is not a failure; the publish is unfinished"


def test_a_shutdown_defers_the_overflow_wait_to_close_rather_than_blocking_the_drain() -> None:
    """The stop signal skips the *wait*, not the work: close() still resolves everything.

    This is the opposite of the mistake FR-001 made — skipping delivery during the exit drain
    loses events, while deferring a wait to close() loses nothing, because close() is where an
    exit-time wait belongs.
    """
    import threading

    class NeverSettles(PollableFuture):
        def __init__(self) -> None:
            super().__init__(settled=False)
            self.waits: list[float | None] = []

        def result(self, timeout=None):
            self.waits.append(timeout)
            if timeout is not None:
                raise TimeoutError("still in flight")
            return "message-id"

    client = PollablePublisher(lambda _i: NeverSettles())
    sink = GooglePubSubSink("projects/p/topics/t", client=client, max_pending=1)
    signal = threading.Event()
    signal.set()
    sink.log_foundry_stop_signal = signal
    sink.emit([{"i": i} for i in range(4)])
    assert all(future.waits == [] for future in client.futures), "no waiting while stopping"
    assert len(sink._futures) == 4, "everything is retained for close()"
    sink.close()
    assert all(future.waits == [None] for future in client.futures), "close waits unbounded"


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
