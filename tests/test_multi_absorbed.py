"""SPEC-058 FR-007 — ``MultiSink`` counts every child raise it swallows, and only when it returns.

``MultiSink`` is the third catcher of SPEC-058's principle: a raising ``emit`` counts nothing, and
whoever catches the raise owns the batch's count. When a child raises and a sibling delivered, the
fan-out swallows the raise, so neither the worker nor the orphan path ever sees it — which makes
the fan-out the only place that batch can be counted. The count is committed through the one
deferral helper, so a fan-out that goes on to re-raise (every child failed) counts nothing and
leaves the batch to the catcher above it.

The models that need a deferring child — (a) through ``HTTPSink``, (b) and (c) through
``TransformSink`` — live beside the sinks they exercise.
"""

from __future__ import annotations

import threading
from typing import Self

import pytest

import log_foundry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.worker import Worker


class Raising:
    """A child that reports nothing and raises its own exception object on every ``emit``."""

    def __init__(self) -> None:
        self.raised: list[BaseException] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        error = SinkDeliveryError("down")
        self.raised.append(error)
        raise error

    def close(self) -> None:
        pass


class Delivering:
    """A child that reports nothing and takes every batch."""

    def __init__(self) -> None:
        self.batches: list[list[dict[str, object]]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.batches.append(list(batch))

    def close(self) -> None:
        pass


class RaisesOnce(Delivering):
    """A child that raises on its first ``emit`` and delivers on every one after."""

    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.calls += 1
        if self.calls == 1:
            raise SinkDeliveryError("first attempt")
        super().emit(batch)


def _batch(n: int) -> list[dict[str, object]]:
    return [{"i": i} for i in range(n)]


def test_every_child_raising_counts_nothing_and_reraises_the_first(capsys) -> None:
    """The catcher above owns a re-raised batch, so the fan-out counts none of it."""
    first, second = Raising(), Raising()
    multi = MultiSink(first, second)

    with pytest.raises(SinkDeliveryError) as caught:
        multi.emit(_batch(3))

    assert caught.value is first.raised[0], "the first child's own exception, by identity"
    assert multi._absorbed == 0, "a re-raising fan-out commits nothing"
    assert multi.losses() is None, "children that count nothing, so the aggregate is unchanged"
    assert multi.failed == 2, "the public call counter still moves once per child call"
    line = (
        "log-foundry: absorbed a failure while emitting to a MultiSink child "
        "(SinkDeliveryError); Raising skipped"
    )
    lines = capsys.readouterr().err.splitlines()
    assert lines == [line, line], "one absorbed line per child raise, as before"


def test_each_raising_child_is_absorbed_whenever_a_sibling_delivers() -> None:
    """Two raising children beside one that delivered: the batch is absorbed once per child."""
    multi = MultiSink(Raising(), Delivering(), Raising())

    multi.emit(_batch(3))

    assert multi.losses() == SinkLosses(dropped=0, failed=6)
    assert multi.failed == 2


def test_a_retried_batch_is_absorbed_once() -> None:
    """Attempt 1 re-raises (both children down), attempt 2 delivers to one: counted once, at 3.

    Counting at catch time would read 9 — the first attempt's two catches and the second's one,
    each charged the whole three-event batch.
    """
    down, recovers = Raising(), RaisesOnce()
    multi = MultiSink(down, recovers)
    worker = Worker(multi, batch_size=1, max_retries=1)
    try:
        worker.submit(_batch(3))
        assert worker.flush(timeout=5.0), "the second attempt returned normally"
        assert worker.health().failed_batches == 0
    finally:
        worker.stop()

    assert len(down.raised) == 2 and recovers.calls == 2, "the premise: two attempts were made"
    assert recovers.batches == [_batch(3)]
    assert multi.losses() == SinkLosses(dropped=0, failed=3), (
        "the batch absorbed exactly once, for the child that was still down when it returned"
    )


def test_an_abandoned_batch_is_counted_only_by_the_worker(capsys) -> None:
    """Every child down on every attempt: the worker's abandon line is the only count."""
    multi = MultiSink(Raising(), Raising())
    worker = Worker(multi, batch_size=1, max_retries=2)
    try:
        worker.submit(_batch(3))
        assert not worker.flush(timeout=5.0)
        assert worker.health().failed_batches == 1
    finally:
        worker.stop()

    assert multi._absorbed == 0
    assert multi.losses() is None
    lost = [line for line in capsys.readouterr().err.splitlines() if " lost " in line]
    assert lost == ["log-foundry: lost 3 event(s); batch abandoned after 3 emit attempts"], lost


@pytest.mark.parametrize("shape", ["sibling-delivers", "all-raise"])
def test_the_orphan_path_counts_each_event_in_one_term(shape: str) -> None:
    """A one-event batch outside a span: the fan-out counts it, or ``orphan_lost`` does — once."""
    second = MemorySink() if shape == "sibling-delivers" else Raising()
    multi = MultiSink(Raising(), second)
    log_foundry.configure(service="t", sink=multi)

    log_foundry.info("outside a span")

    health = log_foundry.health()
    if shape == "sibling-delivers":
        assert multi._absorbed == 1
        assert health.orphan_lost == 0
        assert health.sink == SinkLosses(dropped=0, failed=1)
    else:
        assert multi._absorbed == 0, "the orphan path caught the re-raise and owns the event"
        assert health.orphan_lost == 1
        assert health.sink is None


@pytest.mark.parametrize("shape", ["inner-delivers", "inner-reraises"])
def test_nested_fan_outs_absorb_each_event_once(shape: str) -> None:
    """The innermost catcher that swallows the raise counts it; a re-raising inner counts none."""
    if shape == "inner-delivers":
        inner = MultiSink(Raising(), Delivering())
    else:
        inner = MultiSink(Raising(), Raising())
    outer = MultiSink(inner, Delivering())

    outer.emit(_batch(2))

    if shape == "inner-delivers":
        assert (inner._absorbed, outer._absorbed) == (2, 0)
    else:
        assert (inner._absorbed, outer._absorbed) == (0, 2)
    assert outer.losses() == SinkLosses(dropped=0, failed=2), "each event in one absorbed count"


def test_a_swallowed_raise_makes_losses_non_none() -> None:
    """With no reporting child at all, one swallowed raise is enough to report, in events."""
    multi = MultiSink(Raising(), Delivering())

    multi.emit(_batch(4))

    assert multi.losses() == SinkLosses(dropped=0, failed=4)


def test_a_fan_out_that_lost_nothing_reports_none() -> None:
    """Silent children and nothing absorbed: still "reports nothing", not a clean zero."""
    multi = MultiSink(Delivering(), Delivering())

    multi.emit(_batch(4))

    assert multi.losses() is None


class _ParkingCounter:
    """A data descriptor that, once armed, parks the first store inside the commit.

    The commit is ``setattr(owner, name, getattr(owner, name) + n)`` under the owner's lock, so a
    store parked here sits after the read and before the write — the window a lost increment
    needs. Unarmed it is an ordinary attribute, so construction and ``losses()`` behave as usual.
    """

    def __init__(self) -> None:
        self.armed = False
        self.parked = threading.Event()
        self.release = threading.Event()

    def __get__(self, obj: object, owner: type | None = None) -> object:
        if obj is None:
            return self
        return obj.__dict__.get("_parked_absorbed", 0)

    def __set__(self, obj: object, value: int) -> None:
        if self.armed:
            self.armed = False
            self.parked.set()
            self.release.wait(5.0)
        obj.__dict__["_parked_absorbed"] = value


class _SpyLock:
    """A real lock that says when a thread had to wait for it because another one held it.

    Only a *contended* acquisition is reported. ``MultiSink.emit`` takes this lock twice — once
    for the public call counter, once for the commit — so reporting every attempt would release
    the parked thread before the second thread reached its commit, and a mutant with the commit
    unlocked would then pass by ordering alone.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.contended = threading.Event()

    def __enter__(self) -> Self:
        if not self._lock.acquire(blocking=False):
            self.contended.set()
            self._lock.acquire()
        return self

    def __exit__(self, *exc: object) -> bool:
        self._lock.release()
        return False


def test_a_parked_absorbed_commit_loses_no_increment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second commit while the first is parked between its read and its write: both land.

    Locked, B blocks on the counter lock until A's store is done, then reads it. With the lock
    removed from the commit, B reads 0 and stores its own count while A is parked, and A's store
    then overwrites it — one batch missing. No sleep decides the outcome: the test waits for
    whichever comes first, B contending for the lock or B finishing.
    """
    counter = _ParkingCounter()
    monkeypatch.setattr(MultiSink, "_absorbed", counter, raising=False)
    multi = MultiSink(Raising(), Delivering())
    multi._counter_lock = _SpyLock()

    counter.armed = True
    a = threading.Thread(target=multi.emit, args=(_batch(2),), name="A")
    a.start()
    assert counter.parked.wait(5.0), "the premise: A reached its commit and parked there"
    b = threading.Thread(target=multi.emit, args=(_batch(3),), name="B")
    b.start()
    for _ in range(500):
        if multi._counter_lock.contended.is_set() or not b.is_alive():
            break
        b.join(0.01)
    counter.release.set()
    a.join(5.0)
    b.join(5.0)

    assert not a.is_alive() and not b.is_alive()
    assert multi._absorbed == 5, "both batches absorbed: 2 from A and 3 from B"
