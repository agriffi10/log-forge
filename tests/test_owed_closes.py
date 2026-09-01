"""SPEC-045 — every sink the orphan path owes a close for gets one.

The owed-close record was a single slot, so arming a second sink discarded the first and its
close went to nobody. The measured shape needs no concurrent `configure()` at all: every
`configure()` call sequential on one thread, racing only an ordinary `info()`, left the **live**
sink closed zero times while a superseded one was closed twice.

Two sequences here are pinned as **correct** rather than fixed — a sink handed over twice is
closed twice, and a sink written to after its close is owed another — because a fix that made
either of them one close would strand a buffer. Both were measured on `main` at `4dbb28f`
before anything changed.
"""

from __future__ import annotations

import os
import threading
import time

import pytest

import log_foundry
from log_foundry import _fork, _lifecycle

api = pytest.importorskip("log_foundry.api")
stdout_sink = pytest.importorskip("log_foundry.sinks.stdout")


class CountingSink:
    """Counts closes and events, and records the event count seen by each close.

    It declares `log_foundry_stop_signal` because `offer_stop_signal` probes for it with
    `hasattr` (SPEC-027 FR-002) — a sink without the attribute never receives one, so a double
    that omits it makes a signal assertion vacuously unreachable rather than false.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str = "sink") -> None:
        self.name = name
        self.closes = 0
        self.events = 0
        self.events_at_close: list[int] = []
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts the batch and keeps nothing; these tests assert on closes, not payloads."""
        with self._lock:
            self.events += len(batch)

    def close(self) -> None:
        """Records the close and how many events had arrived by it."""
        with self._lock:
            self.closes += 1
            self.events_at_close.append(self.events)

    def __repr__(self) -> str:
        """Names the sink and its counts, so an assertion failure reads without a debugger."""
        return f"<{self.name} closes={self.closes} events={self.events}>"


class BufferingSink(CountingSink):
    """A sink whose `close()` *is* its delivery, and which keeps accepting afterwards.

    `sinks/base.py` asks an implementation to refuse work once it has released something, but a
    sink that only flushes has released nothing and correctly keeps accepting — and nineteen
    shipped sink modules add no post-close guard at all. It is the shape for which a skipped
    close is lost data, so it is the shape the loss assertions use. A double that refuses
    post-close work cannot strand anything by construction, which would make those assertions
    unable to fail.
    """

    def __init__(self, name: str = "sink") -> None:
        super().__init__(name)
        self.delivered = 0
        self.buffered = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Buffers, as a client-batching sink does."""
        with self._lock:
            self.events += len(batch)
            self.buffered += len(batch)

    def close(self) -> None:
        """Delivers whatever is buffered — so a close that never happens is a loss."""
        with self._lock:
            self.closes += 1
            self.events_at_close.append(self.events)
            self.delivered += self.buffered
            self.buffered = 0


def _eventually(predicate, timeout: float = 10.0) -> bool:
    """Waits for a predicate that a detached closer thread satisfies.

    Args:
      predicate: Called repeatedly until it returns true.
      timeout: Seconds to wait before giving up.

    Returns:
      Whether it became true within the timeout.

    Raises:
      None.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def _emit(build_worker: bool, message: str) -> None:
    """Logs one event down the path under test.

    Args:
      build_worker: Whether to log inside a span, which is what creates the worker.
      message: The event's message.

    Returns:
      None.

    Raises:
      None.
    """
    if not build_worker:
        log_foundry.info(message)
        return

    @log_foundry.trace
    def work() -> None:
        log_foundry.info(message)

    work()


# --------------------------------------------------------------------------- FR-001


def test_a_preempted_emit_does_not_take_the_owed_close_from_the_live_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001 AC-1/AC-2. The deterministic reproduction, and the reason this is a defect.

    Every `configure()` here runs on the **main thread, one after another**, so serializing
    `configure()` changes nothing about this interleaving — which is what rules a lock out as the
    fix. The only concurrent party is an ordinary `info()`, doing nothing the docs forbid.

    Measured on the pre-fix tree: `C.closes == 0` — the sink every event was going to was closed
    by nobody, because the resumed emit re-armed A into the one slot the record had. Both halves
    are asserted here: A is still owed its close, and so is C. A test carrying only one of them
    passes an implementation that has moved the loss rather than removed it.
    """
    first = BufferingSink("A")
    second, third = BufferingSink("B"), BufferingSink("C")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("arm the orphan path")

    resolved, may_resume = threading.Event(), threading.Event()
    real_ensure_sink = api._ensure_sink

    def preempting_ensure_sink() -> object:
        """Parks the emit thread with the old sink already resolved."""
        sink = real_ensure_sink()
        if threading.current_thread().name == "preempted-emit":
            resolved.set()
            may_resume.wait(10.0)
        return sink

    monkeypatch.setattr(api, "_ensure_sink", preempting_ensure_sink)
    emitter = threading.Thread(
        target=lambda: log_foundry.info("preempted"), name="preempted-emit", daemon=True
    )
    emitter.start()
    assert resolved.wait(10.0), "the emit resolved A and parked"

    log_foundry.configure(sink=second)
    log_foundry.configure(sink=third)
    assert first.closes == 1, f"the first swap closed A, got {first!r}"

    may_resume.set()
    emitter.join(10.0)
    assert not emitter.is_alive(), "the preempted emit finished"

    owed = _lifecycle._state._orphan_owed
    assert owed.get(id(third)) is third, (
        f"the live sink is still owed its close — the resumed emit must not displace it. "
        f"Record holds {sorted(sink.name for sink in owed.values())}"
    )
    assert owed.get(id(first)) is first, (
        "and the sink the resumed emit reached is owed one too, since it took that event"
    )

    log_foundry.shutdown()

    assert third.closes == 1, (
        f"the live sink is closed by somebody — got {third!r}. This is the defect: it read "
        "closes=0 while a superseded sink was closed twice"
    )
    assert third.buffered == 0, f"so nothing it held is stranded, got {third!r}"
    assert first.buffered == 0, f"and neither is what the resumed emit put in A, got {first!r}"


@pytest.mark.parametrize("build_worker", [False, True], ids=["orphan", "worker"])
def test_every_sink_the_orphan_path_wrote_to_is_closed(build_worker: bool) -> None:
    """FR-001 AC-3. Three sinks written to in turn, all three closed, none twice.

    The single slot could only ever name the most recent, so this is the general form of the
    defect above rather than a second instance of it.
    """
    sinks = [BufferingSink(name) for name in "ABC"]
    log_foundry.configure(service="t", sink=sinks[0])
    for sink in sinks:
        log_foundry.configure(sink=sink)
        _emit(build_worker, f"to {sink.name}")
    log_foundry.shutdown()

    assert [sink.closes for sink in sinks] == [1, 1, 1], (
        f"each sink is closed exactly once — got {sinks}"
    )
    assert [sink.buffered for sink in sinks] == [0, 0, 0], (
        f"and nothing any of them held is stranded — got {sinks}"
    )


def test_a_swap_closes_every_superseded_sink_not_only_the_last() -> None:
    """FR-001 AC-5. The swap is a reader of the record, so it inherited the same bound.

    Found by mutation: a `_swap_sink` that superseded only the most recently armed sink passed
    the **entire** suite, leaking every other one — the single-slot defect surviving inside the
    fix for it, at the one site that had always handled exactly one sink because there had only
    ever been one.
    """
    first, second, third = BufferingSink("A"), BufferingSink("B"), BufferingSink("C")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(second)
    second.emit([{"message": "to B"}])
    assert len(_lifecycle._state._orphan_owed) == 2, "two sinks are owed, or this proves nothing"

    log_foundry.configure(sink=third)

    assert (first.closes, second.closes) == (1, 1), (
        f"both superseded sinks are closed, not just the last armed — got {first!r} {second!r}"
    )
    assert (first.buffered, second.buffered) == (0, 0), (
        f"so neither is left holding its buffer — got {first!r} {second!r}"
    )
    assert _lifecycle._state._orphan_owed == {id(third): third}, (
        "and the record now names only the sink that replaced them"
    )


def test_building_the_worker_releases_every_sink_it_did_not_adopt() -> None:
    """FR-001 AC-6. The third reader of the record, and the third to carry the same bound.

    All three sites that consume the owed record — the exit close, the swap, and the first
    `@trace`'s worker build — had always handled exactly one sink because there had only ever
    been one. Each was mutated to consume only the last, and the swap's and this one's mutants
    passed the **entire** suite before these tests existed. The rule is one test per reader, not
    one test for the record.
    """
    adopted, stale_one, stale_two = (
        BufferingSink("adopted"),
        BufferingSink("stale-1"),
        BufferingSink("stale-2"),
    )
    log_foundry.configure(service="t", sink=adopted)
    log_foundry.info("arms the adopted sink")
    _lifecycle._note_orphan_emit(stale_one)
    stale_one.emit([{"message": "to stale-1"}])
    _lifecycle._note_orphan_emit(stale_two)
    stale_two.emit([{"message": "to stale-2"}])
    assert len(_lifecycle._state._orphan_owed) == 3, "three are owed, or this proves nothing"

    @log_foundry.trace
    def work() -> int:
        return 1

    work()

    assert _eventually(lambda: (stale_one.closes, stale_two.closes) == (1, 1)), (
        "the worker adopted one sink, so every other owed sink is this transition's to close — "
        f"got {stale_one!r} {stale_two!r}"
    )
    assert (stale_one.buffered, stale_two.buffered) == (0, 0), (
        f"and neither is left holding its buffer — got {stale_one!r} {stale_two!r}"
    )
    assert adopted.closes == 0, (
        f"while the sink the worker adopted is the worker's to close, got {adopted!r}"
    )


def test_a_swap_with_a_live_worker_closes_every_superseded_sink() -> None:
    """FR-001 AC-7. The fourth loop the change created, and the one no test could see fail.

    `_swap_sink` has two branches and only the no-worker one was exercised with more than one
    owed sink. Truncating the **worker** branch's loop to the first stale sink passed the entire
    suite while stranding a buffer — the same mutation the no-worker branch already dies on.
    A consumer of the record needs its own test per branch, not per function.
    """
    adopted = BufferingSink("adopted")
    stale_one, stale_two = BufferingSink("stale-1"), BufferingSink("stale-2")
    replacement = BufferingSink("replacement")
    log_foundry.configure(service="t", sink=adopted)

    @log_foundry.trace
    def work() -> int:
        return 1

    work()
    worker = _lifecycle._state.worker_exists()
    assert worker is not None and worker.sink is adopted, "the worker holds the adopted sink"

    for stale in (stale_one, stale_two):
        _lifecycle._note_orphan_emit(stale)
        stale.emit([{"message": f"to {stale.name}"}])
    assert len(_lifecycle._state._orphan_owed) == 2, "two stale sinks are owed, or this is vacuous"

    log_foundry.configure(sink=replacement)

    assert _eventually(lambda: (stale_one.closes, stale_two.closes) == (1, 1)), (
        "the worker branch closes every sink the record named that it does not hold — "
        f"got {stale_one!r} {stale_two!r}"
    )
    assert (stale_one.buffered, stale_two.buffered) == (0, 0), (
        f"so neither is left holding its buffer — got {stale_one!r} {stale_two!r}"
    )


def test_the_record_holds_more_than_one_sink_at_a_time() -> None:
    """FR-001 AC-4. The structural half: a slot cannot hold two, and this is what a set buys.

    Asserted directly because the behavioural tests above would also pass on an implementation
    that happened to close everything for some other reason, and because a future change back to
    a single slot is exactly the regression this spec exists to prevent.
    """
    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(second)

    owed = _lifecycle._state._orphan_owed
    assert {sink.name for sink in owed.values()} == {"A", "B"}, (
        f"both sinks are owed a close at once — got {sorted(s.name for s in owed.values())}"
    )


# --------------------------------------------------------------------------- FR-002


@pytest.mark.parametrize("build_worker", [False, True], ids=["orphan", "worker"])
def test_a_sink_written_to_after_its_close_is_owed_another(build_worker: bool) -> None:
    """FR-002 AC-1/AC-2. Pinned as correct, not fixed — and it is why the record cannot veto.

    A sink closed by a swap and then written to again has something new to flush, so a second
    close is owed rather than spurious. An earlier draft of this spec had `release()` refuse the
    repeat close; measured, that stranded 2 of 3 events on this exact shape, and 31 of 80 seeds
    on a lifecycle fuzz against 0 on the tree before it.
    """
    first, second = BufferingSink("A"), BufferingSink("B")
    log_foundry.configure(service="t", sink=first)
    _emit(build_worker, "before")
    log_foundry.configure(sink=second)
    assert first.closes == 1, f"the swap closed A, got {first!r}"

    log_foundry.configure(sink=first)
    _emit(build_worker, "after A came back")
    log_foundry.shutdown()

    assert first.closes == 2, (
        f"A took an event after its close, so a second one is owed — got {first!r}"
    )
    assert first.buffered == 0 and first.delivered == first.events, (
        f"and it delivered everything, both times — got {first!r}"
    )
    assert first.events_at_close[1] > first.events_at_close[0], (
        f"the second close came after that event — closes saw {first.events_at_close}"
    )


def test_a_sink_shared_with_the_graph_that_replaces_it_is_not_stranded() -> None:
    """FR-002 AC-3. `configure(A)` then `configure(MultiSink(A, B))`, A live inside the wrapper.

    The swap closes A while A is a child of the new live sink, which predates this spec and is
    recorded in `architecture.md` §12. What must hold is that A is not left holding a buffer:
    it keeps taking events through the wrapper, so it is owed a further close. Measured at
    `A.LOST == 2` on a draft that vetoed the repeat close.
    """
    multi = pytest.importorskip("log_foundry.sinks.multi")
    first, second = BufferingSink("A"), BufferingSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("one")
    log_foundry.configure(sink=multi.MultiSink(first, second))
    log_foundry.info("two")
    log_foundry.info("three")
    log_foundry.shutdown()

    assert first.buffered == 0, f"nothing A took is stranded — got {first!r}"
    assert first.delivered == first.events == 3, (
        f"and A delivered every event that reached it — got {first!r}"
    )


# --------------------------------------------------------------------------- FR-003


def test_the_owed_close_record_is_emptied_by_one_transition() -> None:
    """FR-003 AC-1. Read-and-clear in a single step, under the lock.

    A caller that read the record and cleared it in two steps would let a second caller take the
    same sink and close it twice. `take_orphan_owed` is the one sanctioned emptying, and
    `tests/test_lifecycle_races.py` holds the AST roster that keeps it the only one.
    """
    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(second)

    with _lifecycle._state._lock:
        taken = _lifecycle._state.take_orphan_owed()

    assert [sink.name for sink in taken] == ["A", "B"], (
        f"it returns what it held, in arming order — got {[s.name for s in taken]}"
    )
    assert not _lifecycle._state._orphan_owed, "and leaves the record empty in the same step"


def test_concurrent_takers_of_the_record_split_it_rather_than_share_it() -> None:
    """FR-003 AC-2. Sixteen threads, every sink taken exactly once between them.

    This is the property a read-then-clear cannot satisfy: the take is what stops two lifecycle
    transitions both deciding they own the same sink's close.
    """
    from conftest import run_concurrently

    sinks = [CountingSink(f"s{index}") for index in range(40)]
    log_foundry.configure(service="t", sink=sinks[0])
    for sink in sinks:
        _lifecycle._note_orphan_emit(sink)

    taken: list[object] = []
    taken_lock = threading.Lock()

    def take(_index: int, _iteration: int) -> None:
        with _lifecycle._state._lock:
            got = _lifecycle._state.take_orphan_owed()
        with taken_lock:
            taken.extend(got)

    errors = run_concurrently(take, threads=16)

    assert not errors, f"no take raised, got {errors!r}"
    assert len(taken) == len({id(sink) for sink in taken}), (
        "no sink was handed to two takers, which would close it twice"
    )
    assert {id(sink) for sink in taken} == {id(sink) for sink in sinks}, (
        "and every armed sink was handed to exactly one of them"
    )


# --------------------------------------------------------------------------- FR-004


def test_flush_reaches_every_sink_the_orphan_path_still_owes() -> None:
    """FR-004 AC-1. A reader of the record has to handle more than one sink now.

    `flush()` drains the sink's own client buffer (SPEC-036 FR-002). It read the single slot, so
    with two sinks owed it drained one and reported success over the other.
    """
    class FlushableSink(BufferingSink):
        """Empties its client buffer on `flush()` — the optional protocol SPEC-036 probes for."""

        def flush(self) -> None:
            """Delivers whatever is buffered, without closing."""
            with self._lock:
                self.delivered += self.buffered
                self.buffered = 0

    first, second = FlushableSink("A"), FlushableSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(second)
    second.emit([{"message": "to B"}])
    assert first.buffered == 1 and second.buffered == 1, (
        f"both clients hold something, or the flush below proves nothing — {first!r} {second!r}"
    )

    assert log_foundry.flush(), "the flush reports success"
    assert first.buffered == 0 and second.buffered == 0, (
        f"and it emptied both clients — got {first!r} {second!r}"
    )


def test_a_forked_child_repairs_every_owed_sink_not_only_the_last() -> None:
    """FR-004 AC-2. `_inheritance_roots` reads the record, so it had the same single-slot bound.

    A child must mark every inherited sink foreign, or one it did not see becomes claimable and
    it can legitimately close the parent's transport (SPEC-042 FR-001).
    """
    from test_fork_lifecycle import run_in_child

    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(second)

    def in_child() -> str:
        return ",".join(
            str(_lifecycle.releasable(sink)) for sink in (first, second)
        )

    child = run_in_child(in_child)

    assert child.output == "False,False", (
        f"both inherited sinks are refused in the child, not just the last one armed. "
        f"Got {child.output!r}"
    )


def test_the_record_needs_no_fork_opt_out() -> None:
    """FR-004 AC-3. It holds only sinks still owed a close, never superseded ones.

    `_FORK_SKIP` exists for records that pin sinks the process has finished with, whose fork
    hooks would then run in a child — `_owned` and `_orphan_closed_sink` (SPEC-044 FR-005). This
    record drops a sink the moment its close is decided, so everything in it is live.
    """
    sink = CountingSink("live")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arm")

    assert _lifecycle._state._orphan_owed, "populated, so the claim below is about real contents"
    assert "_orphan_owed" not in _lifecycle._Lifecycle._FORK_SKIP, (
        "it is not opted out of the repair walk"
    )
    assert "_orphan_owed" not in _fork._skipped_names(_lifecycle._state), (
        "and the walk is not asked to skip it — every sink in it is one this process still owes"
    )


def test_health_still_answers_from_the_record_and_from_its_last_entry() -> None:
    """FR-004 AC-4. Both halves are discriminating, and the first draft was neither.

    `health().inherited_sink` asks whether the sink this process is delivering to is one it may
    not release. With no worker it consults the owed record. A first draft armed two sinks this
    process owned and asserted `False`, which is true whichever end the reader picks and true
    with the reader deleted outright — measured, both mutants passed the whole suite.

    So: the second sink is stamped as another process's, the way a `fork` marks everything
    inherited. Reading `True` then requires the reader to consult the record at all — the
    configured sink is the *first* one and is this process's — and to take the **last** entry.
    """
    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(second)
    with _lifecycle._owned_lock:
        _lifecycle._owned[id(second)] = (_lifecycle._FOREIGN, second)

    assert log_foundry.health().inherited_sink is True, (
        "the reader consults the record's last entry: the configured sink is A, which this "
        "process owns, so answering from the config or from the record's first entry reads False"
    )

    with _lifecycle._owned_lock:
        _lifecycle._owned[id(second)] = (os.getpid(), second)
    assert log_foundry.health().inherited_sink is False, (
        "and the answer follows the record rather than being constant"
    )
