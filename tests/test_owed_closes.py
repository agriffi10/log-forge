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
from log_foundry import _fork, _lifecycle, api


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

    owed = _lifecycle._state._owed
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
    assert len(_lifecycle._state._owed) == 2, "two sinks are owed, or this proves nothing"

    log_foundry.configure(sink=third)

    assert (first.closes, second.closes) == (1, 1), (
        f"both superseded sinks are closed, not just the last armed — got {first!r} {second!r}"
    )
    assert (first.buffered, second.buffered) == (0, 0), (
        f"so neither is left holding its buffer — got {first!r} {second!r}"
    )
    assert _lifecycle._state._owed == {id(third): third}, (
        "and the record now names only the sink that replaced them"
    )


def test_building_the_worker_arms_its_sink_and_releases_nothing() -> None:
    """SPEC-054 FR-002 supersedes SPEC-045 FR-001 AC-6, and this test inverts with it.

    ~~The worker build was the third reader of the owed record: it took every sink it did not
    adopt and released each one, detached.~~ Struck rather than deleted (SPEC-021), because the
    reasoning is what changed and not the bound. With one record there is nothing to *take* — a
    sink the build did not adopt simply stays owed, and is released by the `configure()` that
    superseded it, since a swap always follows the config write that made the build see a
    different sink, or at exit. FR-002 names this as a behaviour that disappears rather than
    leaving it to be discovered.

    What is asserted here is therefore the new rule and both of its halves: the build **arms**
    its own sink, and it releases nothing at all.
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
    assert len(_lifecycle._state._owed) == 3, "three are owed, or this proves nothing"

    @log_foundry.trace
    def work() -> int:
        return 1

    work()

    assert (stale_one.closes, stale_two.closes, adopted.closes) == (0, 0, 0), (
        "the build releases nothing — got "
        f"{stale_one!r} {stale_two!r} {adopted!r}"
    )
    worker = _lifecycle._state.worker_exists()
    assert worker is not None and id(worker.sink) in _lifecycle._state._owed, (
        "and it arms the sink it was built on"
    )
    assert {id(sink) for sink in (adopted, stale_one, stale_two)} <= set(_lifecycle._state._owed), (
        "every sink is still owed a close, including the two the build did not adopt"
    )

    log_foundry.shutdown(timeout=5.0)
    assert (stale_one.closes, stale_two.closes, adopted.closes) == (1, 1, 1), (
        "and the exit closes all three, once each — got "
        f"{stale_one!r} {stale_two!r} {adopted!r}"
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
    assert {id(stale_one), id(stale_two)} <= set(_lifecycle._state._owed), (
        "two stale sinks are owed, or this is vacuous"
    )
    assert id(adopted) in _lifecycle._state._owed, (
        "and the worker's own sink is owed too since SPEC-054 FR-002 — its build arms it, so "
        "the record holds three and the swap must still pick the right two"
    )

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

    owed = _lifecycle._state._owed
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
    from log_foundry.sinks import multi
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
    """FR-003 AC-1. The take and the discharge are one step, under the lock.

    A caller that read the record and cleared it in two steps would let a second caller take the
    same sink and close it twice. SPEC-054 FR-002/FR-003 moved that step out of a wholesale
    `take_orphan_owed` and into `_close_owed`'s critical section, which takes **per sink** and
    registers each close in the same section — so there is no instant at which a sink is neither
    owed nor in flight. `tests/test_lifecycle_races.py` holds the AST roster that keeps it the
    only such site.

    Asserted on the closes rather than on a returned list, which is what the take is *for*: a
    list handed back proves the record emptied, and the count proves nobody else emptied it too.
    """
    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(second)
    assert [sink.name for sink in _lifecycle._state._owed.values()] == ["A", "B"], (
        "the premise: both sinks are owed, in arming order"
    )

    _lifecycle._close_owed()

    assert not _lifecycle._state._owed, "the take leaves the record empty in the same step"
    assert (first.closes, second.closes) == (1, 1), "and each sink was closed exactly once"


def test_concurrent_takers_of_the_record_split_it_rather_than_share_it() -> None:
    """FR-003 AC-2. Sixteen closers racing, every sink closed exactly once between them.

    This is the property a read-then-clear cannot satisfy: the take is what stops two lifecycle
    transitions both deciding they own the same sink's close. It runs the **real** closer on
    every thread rather than a helper, because SPEC-054 FR-003 made the take part of that
    function and a test of a helper would no longer be a test of the mechanism.
    """
    from conftest import run_concurrently

    sinks = [CountingSink(f"s{index}") for index in range(40)]
    log_foundry.configure(service="t", sink=sinks[0])
    for sink in sinks:
        _lifecycle._note_orphan_emit(sink)

    def take(_index: int, _iteration: int) -> None:
        _lifecycle._close_owed()

    errors = run_concurrently(take, threads=16)

    assert not errors, f"no take raised, got {errors!r}"
    twice = [sink.name for sink in sinks if sink.closes > 1]
    assert not twice, f"these sinks were handed to two takers and closed twice: {twice}"
    never = [sink.name for sink in sinks if sink.closes == 0]
    assert not never, f"these sinks were taken by nobody: {never}"


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

    A child's walk must reach every inherited sink, or one it did not see and the parent never
    recorded becomes claimable and it can legitimately close the parent's transport (SPEC-042
    FR-001 AC-6).
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


def test_the_record_is_opted_out_of_the_repair_walk() -> None:
    """SPEC-054 FR-002/FR-006 supersedes SPEC-045 FR-004 AC-3, and this test inverts with it.

    ~~The record needed no `_FORK_SKIP` entry, because it dropped a sink the moment its close
    was decided, so everything in it was live.~~ Struck rather than deleted (SPEC-021). That was
    true of a record holding only what the *orphan* path owed. The merged record also holds a
    sink swapped out without a confirmed fence and a sink a worker was built on and then
    superseded, so it pins sinks this process has finished delivering to — which is exactly the
    shape `_FORK_SKIP` exists for (SPEC-044 FR-005): without the opt-out the repair walk reaches
    a superseded sink and runs its `reacquire_after_fork()` hook, and `reclaim` then overwrites
    the foreign-pid record the child holds for it.

    Nothing is lost by skipping it: a **live** sink is still reached by the walk through the
    config and through `worker.sink`, and `_inheritance_roots` reads this record directly, so
    marking still refuses an inherited superseded sink.
    """
    sink = CountingSink("live")
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arm")

    assert _lifecycle._state._owed, "populated, so the claim below is about real contents"
    assert "_owed" in _lifecycle._Lifecycle._FORK_SKIP, (
        "the record pins superseded sinks, so it is opted out of the repair walk"
    )
    assert "_owed" in _fork._skipped_names(_lifecycle._state), (
        "and the walk reads that declaration off the object that holds the attribute"
    )


def test_health_answers_the_inherited_question_from_the_config_not_the_record() -> None:
    """SPEC-054 FR-005 supersedes SPEC-045 FR-004 AC-4, and closes `architecture.md` §12's item.

    ~~`health().inherited_sink` … with no worker it consults the owed record … and takes the
    **last** entry.~~ — struck (SPEC-021). That reader could name a sink this process had stopped
    delivering to: `_swap_sink` inserts the new sink and a preempted emit then appends the
    superseded one, so the order can be `[live, superseded]`. Arming order is emit order, which
    is a different question from "installed", and §12 already named the config as the authority.

    Both halves stay discriminating, which is why the arrangement is inverted rather than the
    assertions flipped. A draft that stamped a *record* entry foreign and asserted `False` would
    be true with the reader deleted outright. So the **configured** sink is the one stamped as
    another process's — the way a `fork` leaves what it inherited recorded under a pid this
    process cannot match — and a second, owed-but-not-configured sink is stamped foreign too:
    reading `True` requires the reader to consult the config, and reading `False` after only the
    config's stamp is restored requires it to consult **nothing else**.
    """
    configured, owed_only = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=configured)
    log_foundry.info("to A")
    _lifecycle._note_orphan_emit(owed_only)
    with _lifecycle._owned_lock:
        _lifecycle._owned[id(configured)] = (_lifecycle._FOREIGN, configured)
        _lifecycle._owned[id(owed_only)] = (_lifecycle._FOREIGN, owed_only)

    assert log_foundry.health().inherited_sink is True, (
        "the reader consults the configured sink, which is stamped as another process's"
    )

    with _lifecycle._owned_lock:
        _lifecycle._owned[id(configured)] = (os.getpid(), configured)
    assert log_foundry.health().inherited_sink is False, (
        "and only the configured sink: B is still owed and still stamped foreign, so a reader "
        "that consulted the record at all would answer True here"
    )


# -- SPEC-050 FR-004: the worker's owed-swap record can name the orphan record's sink ----


def test_a_stranded_sink_re_armed_on_the_orphan_path_is_closed_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SPEC-050 FR-004's route, kept as a regression after SPEC-054 FR-002 removed its cause.

    ~~Two records can name the same sink~~ — struck (SPEC-021): there is one record now, so the
    cross-pruning that stopped the worker's owed-swap record and the orphan record both closing A
    has nothing left to prune. The **route** is still worth driving, because it is the one a
    reviewer could not construct and a probe then did: an unconfirmed `configure(sink=B)` leaves
    A held unfenced, a second `configure` supersedes B, and an orphan emit that resolved A before
    all of it and resumes after re-arms A. Measured at `A.closes == 2` before SPEC-050 FR-004.

    What it pins now is that one record answers it: A is taken once, by the closer, after the
    drain thread that may have been inside it has ended.

    The preemption point is injected at `_ensure_sink`, exactly as the sibling test above does,
    because this cannot be raced for reliably and a race test that passes against the bug is
    worse than none.
    """
    from log_foundry import worker as worker_mod
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.3)

    class _Slow(BufferingSink):
        """Emits slowly enough that a swap's drain cannot be confirmed inside its budget."""

        def emit(self, batch: list[dict[str, object]]) -> None:
            """Buffers, after a delay that outlasts the swap budget."""
            time.sleep(0.5)
            super().emit(batch)

    first, second, third = _Slow("A"), BufferingSink("B"), BufferingSink("C")
    log_foundry.configure(service="t", sink=first)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("in span")

    work()

    resolved, may_resume = threading.Event(), threading.Event()
    real_ensure_sink = api._ensure_sink

    def preempting_ensure_sink() -> object:
        """Parks the emit thread with A already resolved, before any swap has run."""
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
    worker = _lifecycle._state._worker
    assert worker is not None and worker.holds_unfenced(first), (
        "the premise: the swap could not confirm A's drain, so the worker still holds it"
    )
    log_foundry.configure(sink=third)

    may_resume.set()
    emitter.join(10.0)
    monkeypatch.setattr(api, "_ensure_sink", real_ensure_sink)
    assert any(s is first for s in _lifecycle._state._owed.values()), (
        "the premise: the resumed emit re-armed A, so both records now name it"
    )

    log_foundry.shutdown()

    assert first.closes == 1, f"A was closed {first.closes} times — got {first!r}"
    assert first.buffered == 0, f"and it still delivered its buffer — got {first!r}"
