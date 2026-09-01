"""SPEC-045 — the library closes a sink once per time it was handed the sink.

The defect is a sink closed twice while the live one is closed by nobody, and it needs no
concurrent `configure()`: the deterministic reproduction below makes every `configure()` call
sequentially on one thread and races only an ordinary `info()`, which is what a lock around
`configure()` cannot reach.

Two sequences here are **pinned before the fix rather than fixed**, because they are already
correct and FR-001 would break them: a sink handed over twice is closed twice, and the second
close flushes it after a further event landed. Their expected values were measured on `main` at
`4dbb28f` before any change.
"""

from __future__ import annotations

import os
import threading

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


class RefusingSink(CountingSink):
    """A sink that refuses work after its own close, the SPEC-032 contract.

    FR-001's wrapper-graph criterion rests on that refusal rather than assuming it, so the
    double has to implement it: a sink closed while it is still the live target discards
    nothing at a refused second close, because it accepted nothing after the first.
    """

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Refuses once closed, and counts otherwise."""
        with self._lock:
            if self.closes:
                raise RuntimeError("emit after close")
            self.events += len(batch)


def _is_docstring(statement: object) -> bool:
    """Whether a statement is a docstring, so a lint over calls does not read prose.

    Args:
      statement: A statement from a function body.

    Returns:
      Whether it is a bare string expression.

    Raises:
      None.
    """
    import ast

    return isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant)


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


# --------------------------------------------------------------------------- FR-002


@pytest.mark.parametrize("build_worker", [False, True], ids=["orphan", "worker"])
def test_a_sink_handed_over_twice_is_closed_twice(build_worker: bool) -> None:
    """FR-002 AC-1/AC-2, and the reason FR-002 exists at all.

    Pinned **before** the fix: this sequence is already right, and `architecture.md` §13 was
    wrong to record it as a double close. The first close releases A as the sink being swapped
    out; the second closes A as the *live* sink, after a further event reached it. FR-001 alone
    would spend A's only close on the first and lose the second — which is a lost flush, not a
    saved one.
    """
    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    _emit(build_worker, "before")
    log_foundry.configure(sink=second)

    assert first.closes == 1, f"the swap closed A once, got {first!r}"
    events_before_handback = first.events

    log_foundry.configure(sink=first)
    _emit(build_worker, "after the hand-back")
    log_foundry.shutdown()

    assert first.closes == 2, (
        f"A was handed over twice, so it is closed twice — got {first!r}. One close per "
        "acquisition, not one per object"
    )
    assert first.events > events_before_handback, (
        "the event logged after the hand-back reached A, so the second close has something "
        f"to flush — got {first!r}"
    )
    assert first.events_at_close[1] > first.events_at_close[0], (
        "A's second close happened after that event, so the flush is not lost — "
        f"closes saw {first.events_at_close}"
    )
    assert second.closes == 1, f"B was handed over once and closed once, got {second!r}"


@pytest.mark.parametrize("build_worker", [False, True], ids=["orphan", "worker"])
def test_a_sink_handed_over_once_is_closed_once(build_worker: bool) -> None:
    """FR-002 AC-3. The control for the test above: the fix spends no close that was not owed."""
    first, second = CountingSink("A"), CountingSink("B")
    log_foundry.configure(service="t", sink=first)
    _emit(build_worker, "before")
    log_foundry.configure(sink=second)
    log_foundry.shutdown()

    assert first.closes == 1, f"one acquisition, one close — got {first!r}"
    assert second.closes == 1, f"and the same for the sink that replaced it — got {second!r}"


def test_a_record_naming_another_process_keeps_its_released_mark() -> None:
    """FR-002 AC-4. `stamp()` is write-once, and clearing must not reach past it.

    A forked child must not be able to restore its own right to close an inherited sink by
    configuring its way back to it, which is the defect SPEC-042 FR-001's `_FOREIGN` exists to
    make terminal. The clear therefore keys on the record naming **this** process, not on the
    sink being reachable.
    """
    sink = CountingSink("inherited")
    with _lifecycle._owned_lock:
        _lifecycle._owned[id(sink)] = (_lifecycle._FOREIGN, sink)
        _lifecycle._released.add(id(sink))

    _lifecycle.stamp(sink)

    with _lifecycle._owned_lock:
        assert _lifecycle._owned[id(sink)][0] == _lifecycle._FOREIGN, (
            "stamp is write-once and did not claim a record naming another process"
        )
        assert id(sink) in _lifecycle._released, (
            "and it did not clear the released mark of a sink it does not own — doing so "
            "would let a child configure its way back to closing the parent's transport"
        )
    assert _lifecycle.release(sink) is None, "the sink is still refused"
    assert sink.closes == 0, f"and was not closed, got {sink!r}"


def test_reclaim_restores_the_release_it_re_stamps() -> None:
    """FR-002 AC-5. A sink that re-acquired its transport owes a close again.

    `reclaim()` is the one write that overrides an existing record (SPEC-042 FR-005), because a
    child's `reacquire_after_fork()` returning is a claim of ownership. The released mark has to
    move with it or the re-acquired transport is never released.
    """
    sink = CountingSink("reacquired")
    log_foundry.configure(service="t", sink=sink)
    assert _lifecycle.release(sink) is None, "an inline release returns None"
    assert sink.closes == 1, f"the first close happened, got {sink!r}"

    _lifecycle.reclaim(sink)

    assert not _lifecycle._was_released(sink), "reclaim cleared the mark"
    _lifecycle.release(sink)
    assert sink.closes == 2, f"so the re-acquired transport is released, got {sink!r}"


# --------------------------------------------------------------------------- FR-001


def test_a_second_release_of_an_owned_sink_does_not_close_it_again() -> None:
    """FR-001 AC-1. The whole mechanism in one assertion."""
    sink = CountingSink("owned")
    log_foundry.configure(service="t", sink=sink)

    _lifecycle.release(sink)
    _lifecycle.release(sink)

    assert sink.closes == 1, (
        f"the library performs one close per acquisition, got {sink!r} — a second close on a "
        "sink that partially released its resources is worse than an unclosed one"
    )


def test_concurrent_releases_of_one_sink_close_it_once() -> None:
    """FR-001 AC-2, the behavioural half — and it is the weaker half, deliberately.

    Sixteen threads releasing one sink must perform one close and none may raise. What this does
    **not** catch is the check-and-set being split across two acquires: that mutation was planted
    and this test passed against it, because the window between a release and a re-acquire is a
    few instructions and no unforced rate reaches it. That is SPEC-028's finding repeated — a
    bare `+=` lost zero across 1.6M concurrent increments — and its answer applies here too, so
    the structural property is asserted by the test below rather than hoped for by this one.
    Measured: the split mutant passed every behavioural test in this file and was killed only by
    that structural one.
    """
    from conftest import run_concurrently

    sink = CountingSink("contended")
    log_foundry.configure(service="t", sink=sink)

    errors = run_concurrently(lambda _index, _iteration: _lifecycle.release(sink), threads=16)

    assert not errors, f"no release raised, got {errors!r}"
    assert sink.closes == 1, f"sixteen concurrent releases performed one close, got {sink!r}"


def test_the_claim_tests_and_marks_under_one_acquire() -> None:
    """FR-001 AC-2, the half that bites. The property survives what a rate cannot reach.

    A `_claim_release` that tests `_released` under one acquire and sets it under another admits
    two threads past the test, and both then close. The behavioural test above cannot see it —
    measured, the split mutant passed every behavioural test in this file — so the invariant is
    asserted where it is decidable: the function takes `_owned_lock` exactly **once**, and both
    the membership test and the mark live inside that block.

    A nested second acquire is not the escape it looks like: `_owned_lock` is a plain `Lock` and
    not an `RLock` (SPEC-028), so a nested take deadlocks rather than passing this.
    """
    import ast
    import pathlib

    source = pathlib.Path(_lifecycle.__file__).read_text(encoding="utf-8")
    claim = next(
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name == "_claim_release"
    )
    acquires = [
        node
        for node in ast.walk(claim)
        if isinstance(node, ast.With)
        and any("_owned_lock" in ast.unparse(item.context_expr) for item in node.items)
    ]
    assert len(acquires) == 1, (
        f"_claim_release takes the record lock {len(acquires)} times; a check under one acquire "
        "and a set under another lets two threads past the test and closes the sink twice"
    )
    inside = ast.unparse(acquires[0])
    assert "in _released" in inside, "the released test is inside that acquire"
    assert "_released.add" in inside, "and so is the mark, so the two cannot be interleaved"


def test_a_detached_release_beaten_by_an_inline_one_still_returns_its_thread() -> None:
    """FR-001 AC-3. The claim is taken where the close happens, so neither two nor zero.

    `release(detached=True)` spawns without claiming and its thread body claims through
    `release()` like any other caller. This drives the losing order deliberately: the inline
    release claims first, so the detached body is skipped — and the requester's contract that it
    is handed a thread to join (SPEC-030 FR-003, SPEC-044 FR-002 AC-8) still holds.
    """
    sink = CountingSink("detached")
    log_foundry.configure(service="t", sink=sink)

    _lifecycle.release(sink)
    closer = _lifecycle.release(sink, detached=True)

    assert isinstance(closer, threading.Thread), (
        "a detached release still returns a thread its requester can join — the skip is decided "
        f"in the body, not by withholding the thread, got {closer!r}"
    )
    closer.join(10.0)
    assert not closer.is_alive(), "the closer finished"
    assert sink.closes == 1, f"and closed nothing a second time, got {sink!r}"


def test_a_sink_the_library_was_never_handed_is_released_every_time() -> None:
    """FR-001 AC-4. The library polices its own closes, never the caller's.

    `FilteringSink(inner).close()` is a documented public call. `inner` has no record, so it is
    the caller's object: refusing a second close there would turn a public API into a silent
    no-op, which is the failure SPEC-042 FR-001 corrected its own flat rule to avoid.
    """
    filtering = pytest.importorskip("log_foundry.sinks.filtering")
    inner = CountingSink("caller-owned")
    wrapper = filtering.FilteringSink(inner, min_level="debug")

    wrapper.close()
    wrapper.close()

    assert inner.closes == 2, (
        f"an unrecorded sink is released on the caller's say-so every time, got {inner!r}"
    )
    with _lifecycle._owned_lock:
        assert id(inner) not in _lifecycle._released, (
            "and is never marked, so the library starts pinning no graph it was not handed"
        )


def test_a_sink_recorded_to_another_process_is_still_refused() -> None:
    """FR-001 AC-5. The released record does not weaken the ownership refusal it sits beside.

    The cross-process refusal itself is owned by `tests/test_sink_ownership.py` and its real
    forked children; this asserts only that the new record did not change the answer.
    """
    sink = CountingSink("foreign")
    with _lifecycle._owned_lock:
        _lifecycle._owned[id(sink)] = (_lifecycle._FOREIGN, sink)

    assert _lifecycle.releasable(sink) is False, "ownership still refuses it"
    assert _lifecycle.release(sink) is None
    assert sink.closes == 0, f"and nothing closed it, got {sink!r}"
    with _lifecycle._owned_lock:
        assert id(sink) not in _lifecycle._released, (
            "a refused release marks nothing — the mark records a close that happened"
        )


def test_the_released_record_never_outlives_the_reference_that_pins_its_id() -> None:
    """FR-001's id-safety invariant, asserted rather than argued.

    A `set[int]` is only sound because `_owned` holds a strong reference to every sink it keys,
    so an id cannot be recycled while a mark stands. Any future site that drops a pin without
    dropping the mark breaks that silently — a fresh sink landing on the freed address would be
    refused its first close. This is the check that catches it.
    """
    sink = CountingSink("pinned")
    log_foundry.configure(service="t", sink=sink)
    _lifecycle.release(sink)

    with _lifecycle._owned_lock:
        assert id(sink) in _lifecycle._released, (
            "the record is populated by this test's own sink, so what follows is not vacuous"
        )
        record = _lifecycle._owned.get(id(sink))
    assert record is not None and record[1] is sink, (
        "the id this test just marked released is still pinned by _owned's strong reference, "
        "which is the whole reason an int is enough to key the mark"
    )


def test_a_refused_close_of_a_wrapper_child_discards_nothing() -> None:
    """FR-001 AC-7, the recorded residual — asserted against SPEC-032, not assumed.

    `configure(MultiSink(A, B))` then `configure(A)` closes A through the wrapper while A is the
    live sink. That is broken before this change and is not repaired by it; what this pins is
    that the *refused* second close costs nothing, because a sink that released its transport
    refuses work afterwards (SPEC-032) and so has nothing left to flush.
    """
    multi = pytest.importorskip("log_foundry.sinks.multi")
    first, second = RefusingSink("A"), RefusingSink("B")
    log_foundry.configure(service="t", sink=multi.MultiSink(first, second))
    log_foundry.info("before")
    events_before = first.events

    log_foundry.configure(sink=first)
    assert first.closes == 1, f"the wrapper's close reached A while A was live, got {first!r}"

    log_foundry.info("after")
    log_foundry.shutdown()

    assert first.closes == 1, f"and A's second close is refused, got {first!r}"
    assert first.events == events_before, (
        "A accepted nothing after its close (SPEC-032), so the refused close discards nothing — "
        f"got {first!r}"
    )


# --------------------------------------------------------------------------- FR-003


def test_a_preempted_emit_does_not_take_the_owed_close_from_the_live_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003 AC-1/AC-2. The deterministic reproduction, and the reason this is a defect.

    Every `configure()` here runs on the **main thread, one after another**, so serializing
    `configure()` changes nothing about this interleaving — which is what rules a lock out as the
    fix. The only concurrent party is an ordinary `info()`, doing nothing the docs forbid.

    Measured on the pre-fix tree: `A.closes == 2` and `C.closes == 0`. Both halves are asserted
    in one test on purpose. FR-001 alone makes the first true and leaves the second false, so a
    test carrying only the close count would pass against an implementation that has merely
    traded a double close for a lost one.
    """
    first = CountingSink("A")
    second, third = CountingSink("B"), CountingSink("C")
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

    assert _lifecycle._state._orphan_sink is not first, (
        "the resumed emit re-armed a sink this process had already released, taking the owed "
        f"close from the live one — record names {_lifecycle._state._orphan_sink!r}"
    )

    log_foundry.shutdown()

    assert first.closes == 1, f"A is closed once, not twice — got {first!r}"
    assert third.closes == 1, (
        f"and the live sink is closed by somebody — got {third!r}. Refusing A's second close "
        "without stopping the re-arm turns a double close into a lost one"
    )


def test_adopting_a_declined_swap_refuses_a_sink_already_released() -> None:
    """FR-003 AC-3. The second arming site, held to the same rule as the first.

    `_adopt_declined_swap` re-homes a sink a retired worker refused mid-swap (SPEC-035 FR-003).
    A sink this process has released is not one to re-home: nothing further is owed on it, and
    arming it displaces whatever the record names.
    """
    live, released = CountingSink("live"), CountingSink("released")
    log_foundry.configure(service="t", sink=live)
    log_foundry.info("arm the record")
    with _lifecycle._owned_lock:
        _lifecycle._owned[id(released)] = (os.getpid(), released)
        _lifecycle._released.add(id(released))

    _lifecycle._adopt_declined_swap(released)

    assert _lifecycle._state._orphan_sink is live, (
        "the record still names the sink that is actually being delivered to, got "
        f"{_lifecycle._state._orphan_sink!r}"
    )


def test_a_sink_left_open_by_a_swap_is_still_refused_a_re_arm_and_still_closed() -> None:
    """FR-003 AC-4. The two records answer different questions and are not conflated.

    `_orphan_closed_sink` names a sink a swap deliberately left **open** where its drain could
    not be confirmed (SPEC-044 FR-004), so it is not a released sink. It must still block a
    re-arm, and the sink must still be closed by whoever owns it — which the released record
    would prevent if the two were merged.
    """
    left_open = CountingSink("left-open")
    log_foundry.configure(service="t", sink=left_open)
    log_foundry.info("arm the record")

    with _lifecycle._state._lock:
        _lifecycle._state._orphan_sink = None
        _lifecycle._state._orphan_closed_sink = left_open

    assert not _lifecycle._was_released(left_open), (
        "a sink latched as 'do not re-arm' has not been released — the latch and the released "
        "record are different claims"
    )
    _lifecycle._note_orphan_emit(left_open)
    assert _lifecycle._state._orphan_sink is None, "and the latch still refuses the re-arm"

    assert _lifecycle.release(left_open) is None
    assert left_open.closes == 1, (
        f"while the sink itself is still closeable, got {left_open!r} — merging the two records "
        "would have left it open forever"
    )


def test_the_unlocked_fast_path_takes_no_further_lock() -> None:
    """FR-003 AC-5. The guard sits inside the critical section, not in front of it.

    `_note_orphan_emit` runs on every orphan log, and its pre-lock branch is the fast path that
    keeps a repeated log to one sink free of the process-wide lock. A released test hoisted above
    the `with` would take `_owned_lock` on every event.
    """
    import ast
    import pathlib

    source = pathlib.Path(_lifecycle.__file__).read_text(encoding="utf-8")
    note = next(
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name == "_note_orphan_emit"
    )
    before_lock = [
        stmt
        for stmt in note.body
        if not isinstance(stmt, ast.With) and not _is_docstring(stmt)
    ]
    on_the_fast_path = {
        ast.unparse(node.func)
        for stmt in before_lock
        for node in ast.walk(stmt)
        if isinstance(node, ast.Call)
    }
    assert "_was_released" not in on_the_fast_path, (
        "the released test is called on the unlocked fast path, which takes _owned_lock on "
        f"every orphan event. Calls there: {sorted(on_the_fast_path)}"
    )
    anywhere = {
        ast.unparse(node.func) for node in ast.walk(note) if isinstance(node, ast.Call)
    }
    assert "_was_released" in anywhere, (
        "and it is called somewhere in the function — a docstring mentioning it is not a guard, "
        "which is how the first draft of this assertion could be satisfied by prose"
    )


# --------------------------------------------------------------------------- FR-004


def test_the_released_record_needs_no_fork_repair_and_holds_no_sink() -> None:
    """FR-004 AC-1. Read after a release has populated it, so the structural half is not vacuous.

    An empty `set[int]` satisfies "holds no sink references" by construction, and so does an
    implementation that never writes to it, which is why the record is populated first.

    The record needs no `_FORK_SKIP` entry for the reason `_closing_now` does not: it holds ids,
    so the repair walk finds no primitive to replace and no sink to hook. It also needs no
    `_clear_closing_after_fork` equivalent — that registry brackets a call no thread in the child
    will finish, while this one records a fact that already completed, and every id a child
    inherits belongs to a sink `_mark_inherited` has marked `_FOREIGN`.
    """
    sink = CountingSink("recorded")
    log_foundry.configure(service="t", sink=sink)
    _lifecycle.release(sink)

    with _lifecycle._owned_lock:
        marks = set(_lifecycle._released)
    assert marks, "the record is populated, so what follows is a real check"
    assert all(isinstance(mark, int) for mark in marks), (
        f"every member is an id, never a sink — got {[type(m).__name__ for m in marks]}"
    )
    assert "_released" not in _lifecycle._FORK_SKIP, (
        "the module-level opt-out is for records that pin sinks; this one pins nothing"
    )
    assert "_released" not in _lifecycle._Lifecycle._FORK_SKIP, (
        "and it is not an attribute of _state either, so the instance opt-out does not apply"
    )
    assert "_released" not in _fork._skipped_names(_lifecycle), (
        "so the repair walk is not asked to skip it — there is nothing there to repair"
    )


def test_a_child_refuses_to_close_a_sink_the_parent_released() -> None:
    """FR-004 AC-2. Ownership refuses it before the released record is ever consulted.

    A child inherits both the sink object and the parent's marks. It must not close the sink —
    which is SPEC-042 FR-001's guarantee, reached here through `_mark_inherited` rather than
    through anything this spec added — and the parent's own close of a sink it has **not**
    released must still happen, so the child's refusal is not a rule the parent inherits.
    """
    from test_fork_lifecycle import run_in_child

    released, untouched = CountingSink("released"), CountingSink("untouched")
    log_foundry.configure(service="t", sink=released)
    _lifecycle.release(released)
    _lifecycle.stamp(untouched)

    def in_child() -> str:
        _lifecycle.release(released)
        _lifecycle.release(untouched)
        return f"{released.closes},{untouched.closes}"

    child = run_in_child(in_child)

    assert child.output == "1,0", (
        f"the child closed nothing it inherited — the released sink stayed at the parent's one "
        f"close and the untouched one at zero. Got {child.output!r}"
    )
    _lifecycle.release(untouched)
    assert untouched.closes == 1, (
        f"and the parent still closes what it never released, got {untouched!r}"
    )


class ReacquiringSink(stdout_sink.StdoutSink):
    """A sink that re-acquires its transport in a forked child, and counts its closes.

    It subclasses a shipped sink so `_fork._is_owned` is true and the child's repair walk reaches
    it — a plain double is not reached at all, which would make the hook assertion vacuous.
    """

    def __init__(self) -> None:
        super().__init__()
        self.closes = 0
        self.hooked = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps nothing; this test asserts on closes."""

    def close(self) -> None:
        """Counts the close."""
        self.closes += 1

    def reacquire_after_fork(self) -> None:
        """Claims the transport in the child, which is what makes it releasable there."""
        self.hooked += 1


def test_a_child_that_re_acquires_its_sink_closes_it_exactly_once() -> None:
    """FR-004 AC-3. `reclaim()` restores the release; the record then bounds it to one.

    Returning from `reacquire_after_fork()` **is** a claim of ownership (SPEC-042 FR-005), so
    the child may close what it now holds — and having claimed it, it must close it once, not
    once per arming. Both halves are asserted in the child, since a parent cannot see them.
    """
    from test_fork_lifecycle import run_in_child

    sink = ReacquiringSink()
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("arm the record")

    def in_child() -> str:
        _lifecycle.release(sink)
        _lifecycle.release(sink)
        return f"{sink.hooked},{sink.closes},{_lifecycle.releasable(sink)}"

    child = run_in_child(in_child)

    assert child.output == "1,1,True", (
        "the child's hook ran once, so the sink is the child's to close; it then closed exactly "
        f"once across two releases. Got {child.output!r}"
    )
    assert sink.closes == 0, (
        f"and the parent's own copy was untouched by any of it, got closes={sink.closes}"
    )
