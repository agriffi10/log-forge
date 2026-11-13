"""SPEC-081 FR-001, FR-002 and FR-003: the ownership record pins only what the library holds.

An entry holds its sink strongly while the library still holds it — reachable from a live
delivery root, owed a close, or being closed — and weakly afterwards, so a superseded sink is
collected once the application drops it. Before an entry goes weak the sinks beneath it are
captured; an entry whose sink has died answers for nothing; and the record's lock is re-entrant,
so a finalizer forwarding a close cannot hang on it. Every fork case is a real ``os.fork()``.
"""

from __future__ import annotations

import gc
import os
import threading
import weakref
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry import _lifecycle, config
from log_foundry import worker as worker_module
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.usefixtures("no_close_left_in_flight")

_CAPTURE_LINE = "recording the sinks beneath a superseded sink stopped short"


class RecordingSink:
    """A structural sink counting its closes, so a refusal reads as a zero."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Keeps the batch."""
        self.events.extend(batch)

    def close(self) -> None:
        """Counts the close."""
        self.closes += 1


class ParkingSink(RecordingSink):
    """A sink whose close parks until the test lets it go, then returns or raises."""

    def __init__(self, *, fail: bool = False) -> None:
        super().__init__()
        self.fail = fail
        self.entered = threading.Event()
        self.release = threading.Event()

    def close(self) -> None:
        """Parks inside the close, then counts it and raises when told to."""
        self.entered.set()
        self.release.wait(10.0)
        self.closes += 1
        if self.fail:
            raise OSError("the close failed")


class ThirdPartyWrapper:
    """A wrapper the library does not own, holding one sink and closing nothing."""

    def __init__(self, inner: object) -> None:
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers nothing."""

    def close(self) -> None:
        """Closes nothing beneath it."""


def entry_for(sink: object) -> _lifecycle._Entry | None:
    """The record's live entry for ``sink``, read under the record lock."""
    with _lifecycle._state._owned_lock:
        return _lifecycle._entry_for(sink)


def join_closers() -> None:
    """Joins every library close thread, so the re-pin each one ends with has run."""
    for thread in threading.enumerate():
        if thread.name in ("log-foundry-sink-close", "log-foundry-owed-close"):
            thread.join(10.0)


def dead_entry_and_lander(*, pid: int | None = None) -> tuple[int, RecordingSink]:
    """Leaves a weak entry whose sink has died, and a fresh sink that landed on its id.

    The fresh sink is built at once, with nothing allocated between the death and the build, which
    is the shape measured at 99.93%: a collection in between hands the slot to something else.
    Live ballast of the same types is built first: a sink that was alone in its allocator pool
    empties the pool when it dies, the pool goes back to its arena, and the next build is served
    from another pool — measured, the first such test in a fresh worker then never landed.
    """
    ballast = [_lifecycle._Entry(RecordingSink(), 0, 0, strong=True) for _ in range(64)]
    doomed = RecordingSink()
    entry = _lifecycle._Entry(doomed, os.getpid() if pid is None else pid, 0, strong=False)
    target = id(doomed)
    with _lifecycle._state._owned_lock:
        _lifecycle._state._owned[target] = entry
    del doomed
    fresh = land_on(target)
    del ballast
    assert entry.referent() is None, "the precondition: the entry's own sink is dead"
    return target, fresh


def dead_entry_id(*, pid: int | None = None) -> int:
    """Leaves a weak entry whose sink has died, and returns the id it sits on."""
    target, fresh = dead_entry_and_lander(pid=pid)
    del fresh
    return target


def stash(target: int) -> _lifecycle._Entry:
    """Takes a dead entry out of the record, so a re-pin before the write under test cannot prune it."""
    with _lifecycle._state._owned_lock:
        return _lifecycle._state._owned.pop(target)


def restore(target: int, entry: _lifecycle._Entry) -> None:
    """Puts a stashed dead entry back at its id."""
    with _lifecycle._state._owned_lock:
        _lifecycle._state._owned[target] = entry


def land_on(target: int, build: Callable[[], Any] = RecordingSink) -> Any:
    """Builds objects until one lands on ``target``, keeping the misses so the slot comes round."""
    misses: list[object] = []
    for _ in range(10_000):
        candidate = build()
        if id(candidate) == target:
            return candidate
        misses.append(candidate)
    pytest.fail("no object landed on the dead entry's id within 10,000 builds")


@pytest.fixture
def short_swap(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """A swap budget short enough that a parked close does not hold the test."""
    monkeypatch.setattr(worker_module, "DEFAULT_SWAP_TIMEOUT", 0.2)
    yield


# -- FR-001: the record pins only what the library still holds ----------------------------------


def test_five_hundred_reconfigures_leave_a_bounded_record() -> None:
    """AC-1. One entry per configure and every superseded sink alive was SPEC-081's Defect A."""
    watchers: list[weakref.ReferenceType[MemorySink]] = []
    for index in range(500):
        sink = MemorySink()
        log_foundry.configure(sink=sink)
        for _ in range(100):
            log_foundry.info("event")
        log_foundry.flush()
        if index < 499:
            watchers.append(weakref.ref(sink))
        del sink
    join_closers()
    gc.collect()
    with _lifecycle._state._owned_lock:
        size = len(_lifecycle._state._owned)
    alive = sum(1 for watcher in watchers if watcher() is not None)
    assert size <= 8, f"the record holds {size} entries after 500 configures"
    assert alive <= 1, f"{alive} of the 499 superseded sinks are still alive"


def test_superseded_sinks_never_logged_to_go_weak_and_unclosed() -> None:
    """AC-2. Five configures with nothing logged leave four sinks no close will ever reach."""
    sinks = [RecordingSink() for _ in range(5)]
    for sink in sinks:
        log_foundry.configure(sink=sink)
    for sink in sinks[:4]:
        entry = entry_for(sink)
        assert entry is not None and entry.strong is None, "a superseded entry is weak"
    current = entry_for(sinks[4])
    assert current is not None and current.strong is sinks[4], "the configured one is strong"
    assert [sink.closes for sink in sinks] == [0, 0, 0, 0, 0], "none was ever owed a close"
    watchers = [weakref.ref(sink) for sink in sinks[:4]]
    del sinks[:4], sink
    gc.collect()
    assert [watcher() for watcher in watchers] == [None] * 4, "each is collected once dropped"


@pytest.mark.parametrize("fail", [False, True], ids=["returns", "raises"])
def test_held_sinks_stay_strong_until_their_close_ends(fail: bool, short_swap: None) -> None:
    """AC-3. Configured, owed and closing each pin; a performed close then demotes, weak kept."""
    parked = ParkingSink(fail=fail)
    log_foundry.configure(sink=parked)
    log_foundry.info("owed to the parked sink")
    configured = RecordingSink()
    log_foundry.configure(sink=configured)
    assert parked.entered.wait(5.0), "the swap's detached close reached the parked sink"
    owed = RecordingSink()
    _lifecycle.stamp(owed)
    _lifecycle._note_orphan_emit(owed)
    unheld = RecordingSink()
    _lifecycle.stamp(unheld)
    _lifecycle._repin()

    for name, sink in (("configured", configured), ("owed", owed), ("closing", parked)):
        entry = entry_for(sink)
        assert entry is not None and entry.strong is sink, f"the {name} sink is held strongly"
    control = entry_for(unheld)
    assert control is not None and control.strong is None, "the control: an unheld one is weak"
    weak_before = entry_for(parked).weak  # type: ignore[union-attr]

    parked.release.set()
    join_closers()
    after = entry_for(parked)
    assert after is not None
    assert after.strong is None and after.closed is True, "a performed close demotes and marks"
    assert after.weak is weak_before, "the weak reference is the one written with the entry"
    assert parked.closes == 1


def test_a_sink_re_handed_while_its_close_is_parked_stays_strong_and_unclosed(
    short_swap: None,
) -> None:
    """AC-4. The epoch read before the close tells the settle the sink was handed over again."""
    parked = ParkingSink()
    log_foundry.configure(sink=parked)
    log_foundry.info("owed")
    log_foundry.configure(sink=RecordingSink())
    assert parked.entered.wait(5.0)
    log_foundry.configure(sink=parked)
    parked.release.set()
    join_closers()
    entry = entry_for(parked)
    assert entry is not None
    assert entry.strong is parked and entry.closed is False, (
        "a sink the library holds again is not marked finished by its earlier close"
    )


class Slotted:
    """A sink whose instances cannot be weakly referenced."""

    __slots__ = ("closes",)

    def __init__(self) -> None:
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers nothing."""

    def close(self) -> None:
        """Counts the close."""
        self.closes += 1


@pytest.mark.parametrize("kind", ["slots", "proxy"])
def test_a_sink_that_cannot_be_weakly_referenced_keeps_a_strong_entry(kind: str) -> None:
    """AC-5. No ``__weakref__`` slot, or a ``weakref.proxy``: strong for life, configure intact."""
    target = RecordingSink()
    sink: Any = Slotted() if kind == "slots" else weakref.proxy(target)
    log_foundry.configure(sink=sink)
    if kind == "slots":
        log_foundry.info("owed, so the supersession closes it")
    log_foundry.configure(sink=RecordingSink())
    join_closers()
    entry = entry_for(sink)
    assert entry is not None and entry.weak is None and entry.strong is sink
    if kind == "slots":
        assert entry.closed is True and sink.closes == 1


def test_a_closed_sink_still_held_is_releasable_where_it_was_closed() -> None:
    """AC-6. Closing twice behaves as before: recorded, so releasable again."""
    sink = RecordingSink()
    log_foundry.configure(sink=sink)
    log_foundry.info("owed")
    log_foundry.configure(sink=RecordingSink())
    join_closers()
    entry = entry_for(sink)
    assert entry is not None and entry.closed is True
    assert _lifecycle.releasable(sink) is True


def test_a_childs_inherited_entries_stay_strong_through_its_own_swap_and_release() -> None:
    """AC-7. In a child, entries naming the parent or ``_FOREIGN`` are never demoted."""
    first, second = RecordingSink(), RecordingSink()
    log_foundry.configure(sink=first)
    log_foundry.info("to the first")
    log_foundry.configure(sink=second)
    join_closers()

    def in_child() -> str:
        mine = RecordingSink()
        log_foundry.configure(sink=mine)
        log_foundry.info("to the child's own")
        log_foundry.configure(sink=RecordingSink())
        join_closers()
        here = os.getpid()
        with _lifecycle._state._owned_lock:
            inherited = [e for e in _lifecycle._state._owned.values() if e.pid != here]
        weak = sum(1 for entry in inherited if entry.strong is None)
        log_foundry.shutdown(3.0)
        return f"{len(inherited) > 0},{weak},{mine.closes},{first.closes},{second.closes}"

    child = run_in_child(in_child)
    assert child.output == "True,0,1,1,0", child.output


def test_the_repin_runs_on_each_early_return_of_the_swap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC-8. Both early returns of ``_swap_sink`` end in its outer ``finally``."""
    calls: list[str] = []
    original = _lifecycle._repin
    monkeypatch.setattr(_lifecycle, "_repin", lambda: (calls.append("repin"), original())[1])

    _lifecycle._swap_sink(RecordingSink(), 0.1)
    assert calls == ["repin"], "the return for a swap with no worker and nothing owed"

    drain = _lifecycle._Operation(_lifecycle._Op.STOP, 1.0)
    log_foundry.configure(sink=RecordingSink())
    log_foundry.info("owed")
    _lifecycle._state._stops_running.add(drain)
    before = len(calls)
    try:
        assert _lifecycle._state._column() is _lifecycle.State.DRAINING
        assert _lifecycle._state._take_ahead()
        _lifecycle._swap_sink(RecordingSink(), 0.1)
    finally:
        _lifecycle._state._stops_running.discard(drain)
    assert calls[before:] == ["repin"], "the decline during a drain"


class TrackingLock:
    """A stand-in for a library ``Lock`` that records which thread holds it."""

    def __init__(self) -> None:
        self._real = threading.Lock()
        self.owner: int | None = None

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquires the real lock and records the holder."""
        got = self._real.acquire(blocking, timeout)
        if got:
            self.owner = threading.get_ident()
        return got

    def release(self) -> None:
        """Forgets the holder and releases."""
        self.owner = None
        self._real.release()

    def locked(self) -> bool:
        """Whether some thread holds it."""
        return self._real.locked()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()


def test_no_library_lock_is_held_when_the_repin_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-8. At every entry to the re-pin, this thread holds none of the three library locks.

    Read as "held by the thread entering the re-pin", which is what a deadlock needs: a closer
    thread's re-pin can start while the swap that started it still holds ``_state._lock`` on the
    calling thread, and asserting a bare ``locked()`` there reported that other thread's hold.
    """
    state_lock, config_lock, leaf = TrackingLock(), TrackingLock(), TrackingLock()
    monkeypatch.setattr(_lifecycle._state, "_lock", state_lock)
    monkeypatch.setattr(config, "_config_lock", config_lock)
    monkeypatch.setattr(_lifecycle._state._closes, "_lock", leaf)
    seen: list[tuple[bool, bool, bool]] = []
    original = _lifecycle._repin

    def spy() -> None:
        me = threading.get_ident()
        seen.append((state_lock.owner == me, config_lock.owner == me, leaf.owner == me))
        original()

    monkeypatch.setattr(_lifecycle, "_repin", spy)
    log_foundry.configure(sink=RecordingSink())
    log_foundry.configure(sink=RecordingSink())
    log_foundry.info("owed, so the next swap releases it on a closer thread")
    log_foundry.configure(sink=RecordingSink())
    join_closers()
    log_foundry.info("owed again")
    log_foundry.shutdown(3.0)
    MultiSink(RecordingSink()).close()
    assert len(seen) >= 5, seen
    assert set(seen) == {(False, False, False)}, seen


class RaisingClose(RecordingSink):
    """A sink whose close raises ``OSError``."""

    def close(self) -> None:
        """Raises."""
        raise OSError("refused")


def test_a_failing_repin_is_absorbed_once_and_never_replaces_a_close_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC-9. The re-pin never raises; its fault is said once."""

    def broken() -> list[object]:
        raise RuntimeError("the roots cannot be read")

    monkeypatch.setattr(_lifecycle, "_delivering_sinks", broken)
    log_foundry.configure(sink=RecordingSink())
    with pytest.raises(OSError, match="refused"):
        _lifecycle.release(RaisingClose())
    log_foundry.configure(sink=RecordingSink())
    err = capsys.readouterr().err
    assert err.count("re-pinning the ownership record") == 1, err
    assert "RuntimeError" in err


def test_a_demoted_sinks_last_reference_drops_outside_every_lock() -> None:
    """AC-10. The demoted list outlives every section, so a finalizer meets no library lock."""
    observed: list[tuple[bool, bool, bool, bool]] = []

    class Finalized(RecordingSink):
        def __del__(self) -> None:
            observed.append(
                (
                    _lifecycle._state._lock.locked(),
                    config._config_lock.locked(),
                    _lifecycle._state._closes._lock.locked(),
                    _lifecycle._state._owned_lock._is_owned(),  # type: ignore[attr-defined]
                )
            )

    def configure_without_keeping() -> None:
        log_foundry.configure(sink=Finalized())

    configure_without_keeping()
    log_foundry.configure(sink=RecordingSink())
    gc.collect()
    assert observed == [(False, False, False, False)], observed


def test_a_sink_that_received_an_event_is_closed_once_with_a_weak_record() -> None:
    """AC-12, invariant 5: a weak record changes no close the owner performs."""
    first, second = RecordingSink(), RecordingSink()
    log_foundry.configure(sink=first)
    log_foundry.info("to the first")
    log_foundry.configure(sink=second)
    log_foundry.info("to the second")
    log_foundry.shutdown(3.0)
    join_closers()
    assert (first.closes, second.closes) == (1, 1)


# -- FR-002: before an entry is demoted, the sinks beneath it are captured ----------------------


def test_a_closed_wrappers_inner_sink_is_captured_and_refused_in_the_child() -> None:
    """AC-2. The wrapper's own close closed nothing beneath it, and it is dropped."""
    conn = RecordingSink()

    def configure_and_supersede() -> None:
        log_foundry.configure(sink=ThirdPartyWrapper(conn))
        log_foundry.info("owed to the wrapper")
        log_foundry.flush()
        log_foundry.configure(sink=RecordingSink())

    configure_and_supersede()
    join_closers()
    gc.collect()
    entry = entry_for(conn)
    assert entry is not None and entry.captured is True, "the capture recorded what was beneath"

    def in_child() -> str:
        log_foundry.configure(sink=conn)
        log_foundry.info("child")
        log_foundry.shutdown(3.0)
        return str(conn.closes)

    child = run_in_child(in_child)
    assert child.output == "0", child.output


def test_capturing_beneath_a_memory_sink_of_100k_events_takes_under_100_visits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC-3. Plain event dicts are read and skipped, never entered."""
    memory = MemorySink()
    memory.events.extend({"n": index, "fields": {"n": index}} for index in range(100_000))
    _found, visits = _lifecycle._capture([memory])
    assert visits < 100, visits

    counted: list[int] = []
    original = _lifecycle._capture

    def spy(demoted: list[object]) -> tuple[list[object], int]:
        found, entered = original(demoted)
        counted.append(entered)
        return found, entered

    monkeypatch.setattr(_lifecycle, "_capture", spy)
    log_foundry.configure(sink=memory)
    log_foundry.configure(sink=RecordingSink())
    assert counted and max(counted) < 100, counted


@pytest.mark.parametrize("failed", [False, True], ids=["normal", "marking-failed"])
def test_a_captured_entry_is_weak_unclosed_and_names_this_process(failed: bool) -> None:
    """AC-4. Captured, never closed, weak, and ``_FOREIGN`` only where the marking failed."""
    inner = RecordingSink()
    log_foundry.configure(sink=ThirdPartyWrapper(inner))
    _lifecycle._state._marking_failed = failed
    log_foundry.configure(sink=RecordingSink())
    entry = entry_for(inner)
    assert entry is not None
    assert entry.captured is True and entry.closed is False and entry.strong is None
    assert entry.pid == (_lifecycle._FOREIGN if failed else os.getpid())


def test_a_dropped_captured_sink_is_collected_and_pruned_at_the_next_configure() -> None:
    """AC-5. The capture keeps nothing alive."""

    def configure_and_supersede() -> tuple[weakref.ReferenceType[RecordingSink], int]:
        inner = RecordingSink()
        log_foundry.configure(sink=ThirdPartyWrapper(inner))
        log_foundry.configure(sink=RecordingSink())
        entry = entry_for(inner)
        assert entry is not None and entry.captured is True
        return weakref.ref(inner), id(inner)

    watcher, inner_id = configure_and_supersede()
    gc.collect()
    assert watcher() is None, "the captured sink is collected once dropped"
    held = RecordingSink()
    log_foundry.configure(sink=held)
    with _lifecycle._state._owned_lock:
        stale = _lifecycle._state._owned.get(inner_id)
    assert stale is None or stale.referent() is not None, "its dead entry is pruned"


class Chain:
    """A third-party wrapper holding another, so the capture enters more than one object."""

    def __init__(self, inner: object) -> None:
        self.inner = inner

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers nothing."""

    def close(self) -> None:
        """Closes nothing."""


def test_a_capture_that_trips_or_raises_is_absorbed_with_one_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC-6. A raise, then a trip; each configure returns, and the detail is written once."""
    from log_foundry import _fork

    announced: list[object] = []
    original_announce = _lifecycle._announce_capture
    monkeypatch.setattr(
        _lifecycle,
        "_announce_capture",
        lambda exc: (announced.append(exc), original_announce(exc))[1],
    )

    log_foundry.configure(sink=Chain([RecordingSink()]))
    real_iterators = _fork._storage_iterators

    def raising(container: object) -> list[Iterator[object]]:
        raise RuntimeError("unreadable")

    monkeypatch.setattr(_fork, "_storage_iterators", raising)
    log_foundry.configure(sink=RecordingSink())
    monkeypatch.setattr(_fork, "_storage_iterators", real_iterators)

    log_foundry.configure(sink=Chain(Chain(Chain(RecordingSink()))))
    monkeypatch.setattr(_lifecycle, "_MARKING_CEILING", 1)
    log_foundry.configure(sink=RecordingSink())

    assert [type(exc).__name__ for exc in announced] == ["RuntimeError", "NoneType"], announced
    err = capsys.readouterr().err
    assert err.count(_CAPTURE_LINE) == 1, err
    assert "recording the sinks beneath a superseded sink" in err and "RuntimeError" in err
    assert "reading a container while marking" not in err, "the fork walk's line, in a parent"


# -- FR-003: a dead entry answers for nothing; the record's lock survives a finalizer ----------


def test_a_fresh_sink_on_a_dead_entrys_id_is_recorded_and_closed_by_its_owner() -> None:
    """AC-1. A same-type sink lands on a dead sink's id almost every time (measured 99.93%)."""
    _target, fresh = dead_entry_and_lander()
    assert _lifecycle.releasable(fresh) is True
    log_foundry.configure(sink=fresh)
    log_foundry.info("to the fresh sink")
    log_foundry.shutdown(3.0)
    with _lifecycle._state._owned_lock:
        entry = _lifecycle._state._owned[id(fresh)]
    assert entry.pid == os.getpid() and entry.referent() is fresh
    assert fresh.closes == 1


def test_a_dead_foreign_entry_lends_no_refusal() -> None:
    """AC-1's reader half: ``releasable`` reads through ``_entry_for``, never the raw entry."""
    _target, fresh = dead_entry_and_lander(pid=_lifecycle._FOREIGN)
    assert _lifecycle.releasable(fresh) is True


def test_a_dead_owner_entry_answers_as_no_owner_entry() -> None:
    """AC-2. A wrapper on a dead entry's id is unrecorded, so its child is the caller's."""
    _target, owner = dead_entry_and_lander()
    inner = RecordingSink()
    assert _lifecycle.releasable(inner, owner=owner) is True


@pytest.mark.parametrize("writer", ["stamp", "capture", "walk"])
def test_each_writer_replaces_a_dead_entry(writer: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-3. Every writer overwrites what ``_entry_for`` reads as absent."""
    target, fresh = dead_entry_and_lander()
    if writer == "stamp":
        _lifecycle.stamp(fresh)
        entry = entry_for(fresh)
        assert entry is not None and entry.pid == os.getpid()
        return
    dead = stash(target)
    log_foundry.configure(sink=ThirdPartyWrapper(fresh))
    if writer == "capture":
        original = _lifecycle._capture

        def capture_then_restore(demoted: list[object]) -> tuple[list[object], int]:
            found = original(demoted)
            restore(target, dead)
            return found

        monkeypatch.setattr(_lifecycle, "_capture", capture_then_restore)
        log_foundry.configure(sink=RecordingSink())
        entry = entry_for(fresh)
        assert entry is not None and entry.pid == os.getpid() and entry.captured is True
        return
    restore(target, dead)
    with _lifecycle._state._owned_lock:
        before = _lifecycle._state._owned.get(id(fresh))
    assert before is not None and before.referent() is None, "the dead entry is still there"

    def in_child() -> str:
        entry = entry_for(fresh)
        return "absent" if entry is None else str(entry.pid)

    child = run_in_child(in_child)
    assert child.output == str(_lifecycle._FOREIGN), child.output


def test_one_configure_prunes_every_dead_entry() -> None:
    """AC-4. Pruned lazily, by the re-pin, with no weak-reference callback."""
    held = RecordingSink()
    log_foundry.configure(sink=held)
    dead_entry_id()
    log_foundry.configure(sink=RecordingSink())
    with _lifecycle._state._owned_lock:
        dead = [e for e in _lifecycle._state._owned.values() if e.referent() is None]
    assert dead == []
    assert entry_for(held) is not None


def test_no_weak_reference_in_the_record_carries_a_callback() -> None:
    """AC-5. A callback runs inside the collector on any thread; none is ever created."""
    inner = RecordingSink()
    log_foundry.configure(sink=MultiSink(ThirdPartyWrapper(inner)))
    log_foundry.info("owed")
    log_foundry.configure(sink=RecordingSink())
    join_closers()
    with _lifecycle._state._owned_lock:
        refs = [e.weak for e in _lifecycle._state._owned.values() if e.weak is not None]
    assert refs, "the precondition: weak references exist"
    assert all(ref.__callback__ is None for ref in refs)


def _run_holding_the_record_lock(work: Callable[[], None]) -> bool:
    """Runs ``work`` on a thread inside the record lock; whether it returned within 5 s."""

    def run() -> None:
        with _lifecycle._state._owned_lock:
            work()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(5.0)
    return not thread.is_alive()


def test_a_collection_under_the_record_lock_returns() -> None:
    """AC-6. A weakly recorded sink in a cycle is finalized inside a record section."""
    asked: list[bool] = []

    class Cyclic(RecordingSink):
        def __init__(self) -> None:
            super().__init__()
            self.me = self

        def __del__(self) -> None:
            asked.append(_lifecycle.releasable(self))

    holder = [Cyclic()]
    _lifecycle.stamp(holder[0])
    _lifecycle._repin()
    entry = entry_for(holder[0])
    assert entry is not None and entry.strong is None, "the precondition: weakly recorded"

    def drop_and_collect() -> None:
        holder.clear()
        gc.collect()

    assert _run_holding_the_record_lock(drop_and_collect), "the thread hung on the record lock"
    assert len(asked) == 1


def test_a_finalizer_forwarding_a_close_under_the_record_lock_returns() -> None:
    """AC-7. The reviewer's ``finalizer.py`` shape, which hung on a plain ``Lock``."""
    inner = RecordingSink()

    class Closing(FilteringSink):
        def __init__(self) -> None:
            super().__init__(inner)  # type: ignore[arg-type]
            self.me = self

        def __del__(self) -> None:
            self.close()

    holder = [Closing()]

    def drop_and_collect() -> None:
        holder.clear()
        gc.collect()

    assert _run_holding_the_record_lock(drop_and_collect), "the thread hung on the record lock"
    assert inner.closes == 1


def test_the_record_lock_is_reentrant_in_the_parent_and_a_child() -> None:
    """AC-8. The roster mints an ``RLock`` for it in a child."""
    rlock = type(threading.RLock())
    assert isinstance(_lifecycle._state._owned_lock, rlock)
    child = run_in_child(lambda: str(isinstance(_lifecycle._state._owned_lock, rlock)))
    assert child.output == "True", child.output
