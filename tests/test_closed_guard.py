"""SPEC-076 FR-001 to FR-003: the closed guard against doubles, in both modes where asked.

Every park is an Event wait with a limit, never a sleep, and every lower bound on a duration is
read from a delay armed after its clock: the guard's own ``Event.wait`` inside the measured call,
or ``conftest.release_after_clock``. CPU time is read through ``test_worker_exit_budget._measured``,
which asserts the wait was idle. Serves invariants 1, 3, 5 and 7.
"""

from __future__ import annotations

import ast
import os
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import release_after_clock
from log_foundry.sinks import _guard
from log_foundry.sinks._guard import CLOSE_OVERLAP_WAIT, ClosedGuard, GuardMode
from test_worker_exit_budget import _measured

MODES = [GuardMode.SERIAL, GuardMode.CLOSE_ONLY]
LIMIT = 2.0


class Release:
    """A release double that records each call and the instant it returned."""

    def __init__(self, raises: BaseException | None = None) -> None:
        self.calls = 0
        self.done_at: float | None = None
        self.done = threading.Event()
        self.raises = raises

    def __call__(self) -> None:
        self.calls += 1
        self.done_at = time.monotonic()
        self.done.set()
        if self.raises is not None:
            raise self.raises


class GatedRelease:
    """A release double that parks until the test lets it return."""

    def __init__(self) -> None:
        self.calls = 0
        self.entered = threading.Event()
        self.go = threading.Event()
        self.done_at: float | None = None

    def __call__(self) -> None:
        self.calls += 1
        self.entered.set()
        assert self.go.wait(LIMIT)
        self.done_at = time.monotonic()


class Parked:
    """A thread holding one admitted section until the test lets it leave."""

    def __init__(self, guard: ClosedGuard) -> None:
        self.guard = guard
        self.inside = threading.Event()
        self.leave = threading.Event()
        self.left_at: float | None = None
        self.section: Any = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        with self.guard.enter() as section:
            self.section = section
            if not section:
                return
            self.inside.set()
            self.leave.wait(LIMIT * 3)
            self.left_at = time.monotonic()

    def start(self) -> Parked:
        self.thread.start()
        assert self.inside.wait(LIMIT), "the parked section was never admitted"
        return self

    def finish(self) -> None:
        self.leave.set()
        self.thread.join(LIMIT)


def _closing(guard: ClosedGuard, release: Any) -> threading.Thread:
    """Starts a close on another thread and returns once the guard reads closed."""
    thread = threading.Thread(target=guard.close, args=(release,), daemon=True)
    thread.start()
    deadline = time.monotonic() + LIMIT
    while not guard.closed:
        assert time.monotonic() < deadline, "the close never began"
        time.sleep(0.001)
    return thread


def _admitted(guard: ClosedGuard) -> bool:
    with guard.enter() as section:
        return bool(section)


# -- FR-001 -------------------------------------------------------------------------------------


@pytest.mark.parametrize("mode", MODES, ids=lambda m: m.name)
def test_open_admits_and_closing_and_closed_refuse(mode: GuardMode) -> None:
    """AC-1: OPEN admits; CLOSING and CLOSED refuse, and ``closed`` reads true in both."""
    guard = ClosedGuard(mode)
    assert not guard.closed and _admitted(guard)
    parked = Parked(guard).start()
    release = Release()
    closer = _closing(guard, release)
    assert not _admitted(guard), "a section was admitted while CLOSING"
    assert guard.closed and release.calls == 0
    parked.finish()
    closer.join(LIMIT)
    assert release.calls == 1
    assert not _admitted(guard) and guard.closed


@pytest.mark.parametrize("mode", MODES, ids=lambda m: m.name)
def test_eight_closers_release_once_and_none_returns_through_the_release(mode: GuardMode) -> None:
    """AC-2: eight concurrent closes call ``release`` once and each returns after it returned.

    The release parks, so a closer that returned through the close still in flight is seen
    while it is parked (the plan review's SF-1: a release that returns at once leaves the
    "returned through" window empty, and a mutant releasing outside the lock survived).
    """
    guard = ClosedGuard(mode)
    release = GatedRelease()
    start = threading.Barrier(8)
    returned: list[float] = []
    one_returned = threading.Event()

    def closer() -> None:
        start.wait()
        guard.close(release)
        returned.append(time.monotonic())
        one_returned.set()

    threads = [threading.Thread(target=closer, daemon=True) for _ in range(8)]
    for thread in threads:
        thread.start()
    assert release.entered.wait(LIMIT)
    assert not one_returned.wait(0.2), "a closer returned while the release was still running"
    release.go.set()
    for thread in threads:
        thread.join(LIMIT)
    assert release.calls == 1 and len(returned) == 8
    assert release.done_at is not None and all(at >= release.done_at for at in returned)


@pytest.mark.parametrize("kind", [RuntimeError, KeyboardInterrupt])
@pytest.mark.parametrize("mode", MODES, ids=lambda m: m.name)
def test_a_raising_release_propagates_by_identity_and_stays_closed(
    mode: GuardMode, kind: type[BaseException]
) -> None:
    """AC-3: the release's exception propagates by identity; a second close calls nothing."""
    guard = ClosedGuard(mode)
    error = kind("boom")
    release = Release(raises=error)
    with pytest.raises(kind) as caught:
        guard.close(release)
    assert caught.value is error
    assert guard.closed and not _admitted(guard)
    guard.close(release)
    assert release.calls == 1


@pytest.mark.parametrize("mode", MODES, ids=lambda m: m.name)
def test_enter_raises_nothing_in_any_state(mode: GuardMode) -> None:
    """AC-4: ``enter()`` raises nothing open, closing or closed."""
    guard = ClosedGuard(mode)
    assert _admitted(guard)
    parked = Parked(guard).start()
    closer = _closing(guard, Release())
    assert not _admitted(guard)
    parked.finish()
    closer.join(LIMIT)
    assert not _admitted(guard)


@pytest.mark.parametrize("mode", MODES, ids=lambda m: m.name)
def test_a_section_refused_at_the_first_read_holds_no_lock(mode: GuardMode) -> None:
    """AC-4: inside a section refused before the lock, the lock is free."""
    guard = ClosedGuard(mode)
    guard.close(Release())
    with guard.enter() as section:
        assert not section
        assert guard._lock.acquire(blocking=False)
        guard._lock.release()


class SteeredLock:
    """Wraps a guard's lock so the queued caller wins it before the closer may try."""

    def __init__(self, inner: threading.Lock, caller: str, closer: str) -> None:
        self._inner = inner
        self.caller, self.closer = caller, closer
        self.caller_queued = threading.Event()
        self.caller_acquired = threading.Event()
        self.closer_queued = threading.Event()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        name = threading.current_thread().name
        if name == self.closer:
            self.closer_queued.set()
            self.caller_acquired.wait(LIMIT)
        if name == self.caller:
            self.caller_queued.set()
        got = self._inner.acquire(blocking, timeout)
        if name == self.caller:
            self.caller_acquired.set()
        return got

    def release(self) -> None:
        self._inner.release()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()


def test_a_caller_refused_after_acquiring_the_lock_holds_none_in_its_block() -> None:
    """AC-4: a caller queued on the lock and refused at the second read holds no lock.

    The caller passes the first read, queues, and is steered to win the lock before the closer;
    its refused block then waits for the release. Holding the lock in that block would leave the
    close unable to release, and the block's wait would time out.
    """
    guard = ClosedGuard(GuardMode.SERIAL)
    steered = SteeredLock(guard._lock, caller="queued", closer="closer")
    guard._lock = steered  # type: ignore[assignment]
    parked = Parked(guard).start()
    release = Release()
    outcome: dict[str, object] = {}

    def queued() -> None:
        with guard.enter() as section:
            outcome["admitted"] = bool(section)
            if not section:
                outcome["saw_release"] = release.done.wait(LIMIT)

    caller = threading.Thread(target=queued, name="queued", daemon=True)
    caller.start()
    assert steered.caller_queued.wait(LIMIT)
    closer = threading.Thread(target=guard.close, args=(release,), name="closer", daemon=True)
    closer.start()
    assert steered.closer_queued.wait(LIMIT)
    parked.finish()
    caller.join(LIMIT * 2)
    closer.join(LIMIT * 2)
    assert outcome == {"admitted": False, "saw_release": True}


class InterruptOnce:
    """A lock or event double that raises ``KeyboardInterrupt`` the first time it is waited on."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.fired = False

    def _maybe(self) -> None:
        if not self.fired:
            self.fired = True
            raise KeyboardInterrupt

    def __enter__(self) -> Any:
        self._maybe()
        return self._inner.__enter__()

    def __exit__(self, *exc: object) -> Any:
        return self._inner.__exit__(*exc)

    def acquire(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe()
        return self._inner.acquire(*args, **kwargs)

    def release(self) -> None:
        self._inner.release()

    def wait(self, timeout: float | None = None) -> Any:
        self._maybe()
        return self._inner.wait(timeout)

    def set(self) -> None:
        self._inner.set()

    def clear(self) -> None:
        self._inner.clear()

    def is_set(self) -> bool:
        return bool(self._inner.is_set())


@pytest.mark.parametrize("mode", MODES, ids=lambda m: m.name)
def test_a_closer_interrupted_waiting_for_the_lock_leaves_it_closing(mode: GuardMode) -> None:
    """AC-5: interrupted on the lock, the guard refuses and the next close releases once."""
    guard = ClosedGuard(mode)
    guard._lock = InterruptOnce(guard._lock)  # type: ignore[assignment]
    release = Release()
    with pytest.raises(KeyboardInterrupt):
        guard.close(release)
    assert not _admitted(guard)
    assert release.calls == 0 and guard.closed
    guard.close(release)
    assert release.calls == 1


def test_a_close_only_closer_interrupted_waiting_for_idle_leaves_it_closing() -> None:
    """AC-5: interrupted on ``_idle``, the guard refuses and the next close releases once."""
    guard = ClosedGuard(GuardMode.CLOSE_ONLY)
    parked = Parked(guard).start()
    guard._idle = InterruptOnce(guard._idle)  # type: ignore[assignment]
    release = Release()
    with pytest.raises(KeyboardInterrupt):
        guard.close(release)
    assert not _admitted(guard)
    assert release.calls == 0
    parked.finish()
    guard.close(release)
    assert release.calls == 1


_GUARD_SOURCE = Path(_guard.__file__)


def leaf_findings(source: str) -> list[str]:
    """Every way a module breaks the guard's leaf rule: a package import, an RLock, a Condition."""
    findings: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and (
            node.level or (node.module or "").split(".")[0] == "log_foundry"
        ):
            findings.append(f"line {node.lineno}: imports from the package")
        if isinstance(node, ast.Import) and any(
            alias.name.split(".")[0] == "log_foundry" for alias in node.names
        ):
            findings.append(f"line {node.lineno}: imports the package")
        name = (
            node.attr if isinstance(node, ast.Attribute) else node.id
            if isinstance(node, ast.Name) else None
        )
        if name in ("RLock", "Condition"):
            findings.append(f"line {node.lineno}: names threading.{name}")
    return findings


def test_the_guard_is_a_leaf_with_no_rlock_and_no_condition() -> None:
    """AC-6: ``sinks/_guard.py`` imports nothing from the package and holds no RLock/Condition."""
    assert leaf_findings(_GUARD_SOURCE.read_text(encoding="utf-8")) == []


@pytest.mark.parametrize(
    ("plant", "reason"),
    [
        ("from log_foundry import _diag\n", "imports from the package"),
        ("from . import _engine\n", "imports from the package"),
        ("import log_foundry.sinks\n", "imports the package"),
        ("import threading\nX = threading.RLock()\n", "names threading.RLock"),
        ("import threading\nX = threading.Condition()\n", "names threading.Condition"),
        ("from threading import RLock\nX = RLock()\n", "names threading.RLock"),
    ],
    ids=["from-package", "relative", "import-package", "rlock", "condition", "bare-rlock"],
)
def test_the_leaf_check_reports_each_plant(plant: str, reason: str) -> None:
    """The leaf check's corpus: each plant is reported with its reason; a clean module is silent."""
    findings = leaf_findings(plant)
    assert findings and all(reason in f for f in findings), findings
    assert leaf_findings("import threading\nX = threading.Lock()\n") == []


# -- FR-002 -------------------------------------------------------------------------------------


class Announcing:
    """Wraps a guard's lock and records which named threads have come to wait on it."""

    def __init__(self, inner: threading.Lock) -> None:
        self._inner = inner
        self.queued: set[str] = set()
        self._names = threading.Lock()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        with self._names:
            self.queued.add(threading.current_thread().name)
        return self._inner.acquire(blocking, timeout)

    def release(self) -> None:
        self._inner.release()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()


def test_a_serial_close_waits_for_the_one_inside_and_refuses_the_queue() -> None:
    """AC-1: one section parked, five callers queued; the close waits for it and refuses them."""
    guard = ClosedGuard(GuardMode.SERIAL)
    announcing = Announcing(guard._lock)
    guard._lock = announcing  # type: ignore[assignment]
    parked = Parked(guard).start()
    driver: list[str] = []
    refused: list[str] = []

    def caller(name: str) -> None:
        with guard.enter() as section:
            if not section:
                refused.append(name)
                return
            driver.append(name)

    names = [f"queued-{i}" for i in range(5)]
    callers = [threading.Thread(target=caller, args=(n,), name=n, daemon=True) for n in names]
    for thread in callers:
        thread.start()
    deadline = time.monotonic() + LIMIT
    while not set(names) <= announcing.queued:
        assert time.monotonic() < deadline, "the callers never queued on the lock"
        time.sleep(0.001)
    release = Release()
    closed_at: list[float] = []

    def closer() -> None:
        guard.close(release)
        closed_at.append(time.monotonic())

    close_thread = threading.Thread(target=closer, daemon=True)
    close_thread.start()
    assert not release.done.wait(0.05), "the close released under the section inside"
    parked.finish()
    close_thread.join(LIMIT)
    for thread in callers:
        thread.join(LIMIT)
    assert release.calls == 1 and parked.left_at is not None and closed_at[0] >= parked.left_at
    assert sorted(refused) == names and driver == []


def test_a_caller_arriving_while_a_serial_close_waits_is_refused_at_once() -> None:
    """AC-2: refused within 0.05 s, while another thread still holds the lock."""
    guard = ClosedGuard(GuardMode.SERIAL)
    parked = Parked(guard).start()
    closer = _closing(guard, Release())
    began = time.monotonic()
    with guard.enter() as section:
        took = time.monotonic() - began
        admitted = bool(section)
        lock_held_elsewhere = guard._lock.locked()
    assert not admitted and lock_held_elsewhere
    assert took < 0.05, took
    parked.finish()
    closer.join(LIMIT)


@pytest.mark.parametrize("mode", MODES, ids=lambda m: m.name)
def test_a_close_with_nothing_inside_is_prompt(mode: GuardMode) -> None:
    """AC-3: a close with no section inside returns within 0.05 s and releases once."""
    guard = ClosedGuard(mode)
    release = Release()
    began = time.monotonic()
    guard.close(release)
    took = time.monotonic() - began
    assert release.calls == 1
    assert took < 0.05, took


def test_a_serial_section_is_never_released_during() -> None:
    """A ``SERIAL`` section always reads ``released_during`` false."""
    guard = ClosedGuard(GuardMode.SERIAL)
    parked = Parked(guard).start()
    closer = _closing(guard, Release())
    parked.finish()
    closer.join(LIMIT)
    assert parked.section is not None and parked.section.released_during is False


# -- FR-003 -------------------------------------------------------------------------------------


def test_a_close_only_close_waits_for_a_section_that_leaves() -> None:
    """AC-1: the section leaves 0.2 s after the close began; the release follows it, under 0.3 s."""
    guard = ClosedGuard(GuardMode.CLOSE_ONLY)
    parked = Parked(guard).start()
    release = Release()
    with release_after_clock(0.2, parked.leave.set) as watch:
        _measured(lambda: guard.close(release))
    parked.thread.join(LIMIT)
    assert release.calls == 1 and release.done_at is not None and parked.left_at is not None
    assert release.done_at >= parked.left_at, "the release ran before the section left"
    assert watch.elapsed < 0.3, watch.elapsed
    assert parked.section.released_during is False
    assert not _admitted(guard)


def test_a_close_only_close_stops_waiting_at_the_bound() -> None:
    """AC-1: a section that never leaves is released under between 0.45 s and 0.75 s."""
    guard = ClosedGuard(GuardMode.CLOSE_ONLY)
    parked = Parked(guard).start()
    release = Release()
    elapsed = _measured(lambda: guard.close(release))
    assert release.calls == 1
    assert 0.45 <= elapsed < 0.75, elapsed
    parked.finish()
    assert parked.section.released_during is True
    assert CLOSE_OVERLAP_WAIT == 0.5


def test_a_close_only_close_refuses_new_sections_while_it_waits() -> None:
    """AC-1: while the close waits on the section inside, a new ``enter()`` is refused at once."""
    guard = ClosedGuard(GuardMode.CLOSE_ONLY)
    parked = Parked(guard).start()
    closer = _closing(guard, Release())
    began = time.monotonic()
    admitted = _admitted(guard)
    took = time.monotonic() - began
    assert not admitted
    assert took < 0.05, took
    parked.finish()
    closer.join(LIMIT)


def test_two_close_only_sections_run_at_once_and_a_close_waits_for_both() -> None:
    """AC-2: two sections are inside together, and a third thread's close waits for both."""
    guard = ClosedGuard(GuardMode.CLOSE_ONLY)
    one = Parked(guard).start()
    two = Parked(guard).start()
    assert one.inside.is_set() and two.inside.is_set()
    release = Release()
    closer = threading.Thread(target=guard.close, args=(release,), daemon=True)
    closer.start()
    one.finish()
    assert not release.done.wait(0.05), "the close released with a section still inside"
    two.finish()
    closer.join(LIMIT)
    assert release.calls == 1 and release.done_at is not None
    assert one.left_at is not None and two.left_at is not None
    assert release.done_at >= max(one.left_at, two.left_at)


def test_a_section_leaving_during_the_release_reads_released_during() -> None:
    """``CLOSED`` is set before the release runs, so a section it overtook is told so."""
    guard = ClosedGuard(GuardMode.CLOSE_ONLY)
    parked = Parked(guard).start()
    seen: list[bool] = []

    def release() -> None:
        parked.finish()
        seen.append(parked.section.released_during)

    guard.close(release)
    assert seen == [True]


# -- FR-006, at the guard: the pid adoption -----------------------------------------------------


def _as_child(monkeypatch: pytest.MonkeyPatch) -> None:
    real = os.getpid()
    monkeypatch.setattr(_guard.os, "getpid", lambda: real + 100_000)


def test_a_child_does_not_wait_for_a_parent_section(monkeypatch: pytest.MonkeyPatch) -> None:
    """A guard read in another pid counts no parent section inside, so its close is prompt."""
    guard = ClosedGuard(GuardMode.CLOSE_ONLY)
    parked = Parked(guard).start()
    _as_child(monkeypatch)
    release = Release()
    began = time.monotonic()
    guard.close(release)
    took = time.monotonic() - began
    monkeypatch.undo()
    parked.finish()
    assert release.calls == 1
    assert took < 0.25, took


def test_a_child_of_a_closing_parent_admits_and_of_a_closed_one_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``CLOSING`` adopted in another pid reads ``OPEN``; ``CLOSED`` stays ``CLOSED``."""
    closing = ClosedGuard(GuardMode.SERIAL)
    parked = Parked(closing).start()
    closer = _closing(closing, Release())
    held = closing._lock
    closed = ClosedGuard(GuardMode.SERIAL)
    closed.close(Release())
    _as_child(monkeypatch)
    closing._lock = threading.Lock()
    try:
        assert not closing.closed
        assert _admitted(closing), "a child refused because of a closer only its parent runs"
        assert closed.closed and not _admitted(closed)
    finally:
        monkeypatch.undo()
        closing._lock = held
        parked.finish()
        closer.join(LIMIT)
