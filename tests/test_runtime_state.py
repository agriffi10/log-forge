"""SPEC-077 FR-002: ``State`` is read off today's flags, is total, and costs no lock.

``Runtime.state`` is an observer — nothing in ``src/`` reads it — so these tests set the fields it
reads directly, the way ``conftest.install_worker`` does, and drive the two calls that add a token
(``_swap_sink`` and ``_shutdown_worker``) through every exit, including a ``KeyboardInterrupt`` that
only a ``finally`` survives. The lock counts are the spec's, re-measured on the base commit
``3d65cf32`` before the token sets existed, and equal there; the token sets must not move them.
"""

from __future__ import annotations

import ast
import itertools
import pathlib
import threading
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import install_worker
from log_foundry import _lifecycle
from log_foundry._lifecycle import State
from log_foundry.worker import Worker
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    from collections.abc import Callable

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent
GATE_WAIT = 5.0
"""Seconds a test waits for a thread to reach a gate; generous, since a miss fails rather than hangs."""


class Sink:
    """A sink that keeps what it is handed and counts its closes."""

    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.closes = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.events.extend(batch)

    def close(self) -> None:
        self.closes += 1


@log_foundry.trace
def _span() -> None:
    log_foundry.info("in")


def _live_worker() -> Worker:
    """Builds the process worker the way the first span does, and returns it."""
    log_foundry.configure(service="state", sink=Sink())
    _span()
    worker = _lifecycle._state.live_worker()
    assert worker is not None
    return worker


# -- the derivation, one case per rule -------------------------------------------------------


def test_state_has_exactly_five_members() -> None:
    """AC-1: the plan's five, no more."""
    assert [m.name for m in State] == [
        "UNCONFIGURED",
        "RUNNING",
        "SWAPPING",
        "DRAINING",
        "CLOSED",
    ]


def _rule1() -> None:
    _lifecycle._state._stops_running.add(object())


def _rule2() -> None:
    _lifecycle._state._swaps_running.add(object())


def _rule3() -> None:
    install_worker(Worker(Sink()))


def _rule3_after_retirement() -> None:
    _lifecycle._state.retirements = 1
    install_worker(Worker(Sink()))


def _rule4_retired_worker() -> None:
    install_worker(Worker(Sink()))
    _lifecycle._state.retirements += 1


def _rule4_no_worker() -> None:
    _lifecycle._state.retirements = 1


def _rule5_owed() -> None:
    sink = Sink()
    _lifecycle._state._closes.owed[id(sink)] = sink


def _rule5_resolved() -> None:
    log_foundry.configure(sink=Sink())
    assert not _lifecycle._state._closes.owed, (
        "configure() alone owes nothing; this case is rule 5's half"
    )


def _rule6() -> None:
    return None


RULES: list[tuple[str, Callable[[], None], State]] = [
    ("1-a-stop-token", _rule1, State.DRAINING),
    ("2-a-swap-token", _rule2, State.SWAPPING),
    ("3-a-live-worker", _rule3, State.RUNNING),
    ("3-a-worker-built-after-a-retirement", _rule3_after_retirement, State.RUNNING),
    ("4-a-retired-worker", _rule4_retired_worker, State.CLOSED),
    ("4-retired-with-no-worker", _rule4_no_worker, State.CLOSED),
    ("5-something-owed", _rule5_owed, State.RUNNING),
    ("5-a-sink-resolved", _rule5_resolved, State.RUNNING),
    ("6-nothing", _rule6, State.UNCONFIGURED),
]


@pytest.mark.parametrize(("setup", "expected"), [r[1:] for r in RULES], ids=[r[0] for r in RULES])
def test_each_rule_reads_its_state(setup: Callable[[], None], expected: State) -> None:
    """AC-1: each numbered rule, rules 3 and 4 apart (a worker built after a retirement is live)."""
    setup()
    assert _lifecycle._state.state is expected


PRECEDENCE: list[tuple[str, tuple[Callable[[], None], ...], State]] = [
    ("stop-over-a-live-worker", (_rule3, _rule1), State.DRAINING),
    ("stop-over-a-swap", (_rule2, _rule1), State.DRAINING),
    ("swap-over-running", (_rule3, _rule2), State.SWAPPING),
    ("swap-over-closed", (_rule4_no_worker, _rule2), State.SWAPPING),
]


@pytest.mark.parametrize(
    ("setups", "expected"), [p[1:] for p in PRECEDENCE], ids=[p[0] for p in PRECEDENCE]
)
def test_precedence(setups: tuple[Callable[[], None], ...], expected: State) -> None:
    """AC-2: a stop token outranks everything, a swap token outranks RUNNING and CLOSED.

    A ``state`` that tests the swap before the stop turns ``stop-over-a-swap`` red; no sequential
    run reaches that mutant, which is why it is killed here (FR-004 hands it to this FR).
    """
    for setup in setups:
        setup()
    assert _lifecycle._state.state is expected


# -- the swap token on every exit --------------------------------------------------------------


class _Recording(set[object]):
    """A token set that remembers every token ever added to it."""

    def __init__(self) -> None:
        super().__init__()
        self.added: list[object] = []

    def add(self, token: object) -> None:
        self.added.append(token)
        super().add(token)


def _inside_reads(monkeypatch: pytest.MonkeyPatch, owner: type, name: str) -> list[int]:
    """Wraps a method so each call records how many tokens of each kind are held as it runs."""
    seen: list[int] = []
    real = getattr(owner, name)

    def spy(self: Any, *args: Any, **kwargs: Any) -> Any:
        state = _lifecycle._state
        seen.append(len(state._swaps_running) + 10 * len(state._stops_running))
        return real(self, *args, **kwargs)

    monkeypatch.setattr(owner, name, spy)
    return seen


def test_a_swap_holds_its_token_inside_and_none_after(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-3, the normal return: the token is held across the retarget and gone afterwards."""
    _live_worker()
    seen = _inside_reads(monkeypatch, Worker, "retarget")
    _lifecycle._swap_sink(Sink(), timeout=5.0)
    assert seen == [1], f"retarget ran with {seen} tokens held; expected one swap token"
    assert not _lifecycle._state._swaps_running


def test_a_swap_with_no_work_never_adds_a_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-3: the early return (no live worker, nothing owed) leaves ``SWAPPING`` unentered."""
    recording = _Recording()
    monkeypatch.setattr(_lifecycle._state, "_swaps_running", recording)
    _lifecycle._swap_sink(Sink(), timeout=5.0)
    assert recording.added == [], "the early return added a token, so a no-op swap read SWAPPING"


def test_an_interrupted_retarget_leaves_no_swap_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-3: a ``KeyboardInterrupt`` from ``Worker.retarget`` passes, and the token is discarded."""
    _live_worker()

    def interrupted(self: Worker, *args: object, **kwargs: object) -> object:
        raise KeyboardInterrupt

    monkeypatch.setattr(Worker, "retarget", interrupted)
    with pytest.raises(KeyboardInterrupt):
        _lifecycle._swap_sink(Sink(), timeout=5.0)
    assert not _lifecycle._state._swaps_running, "the discard is not in a finally"


class _InterruptingJoin:
    """A closer whose ``join`` lets the real close finish, then raises ``KeyboardInterrupt``."""

    def __init__(self, thread: threading.Thread) -> None:
        self.thread = thread

    def join(self, timeout: float | None = None) -> None:
        self.thread.join(GATE_WAIT)
        raise KeyboardInterrupt


def test_an_interrupted_closer_join_leaves_no_swap_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-3: a ``KeyboardInterrupt`` while the swap joins a stale sink's closer."""
    log_foundry.configure(service="state", sink=Sink())
    log_foundry.info("owed")
    real = _lifecycle.release

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        closer = real(*args, **kwargs)
        return None if closer is None else _InterruptingJoin(closer)

    monkeypatch.setattr(_lifecycle, "release", wrapped)
    with pytest.raises(KeyboardInterrupt):
        _lifecycle._swap_sink(Sink(), timeout=5.0)
    assert not _lifecycle._state._swaps_running, "the discard is not in a finally"


# -- the stop token on every exit --------------------------------------------------------------


def test_a_worker_branch_shutdown_holds_its_token_inside(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-4, the worker branch: the token is held across ``Worker.stop`` and gone afterwards."""
    _live_worker()
    seen = _inside_reads(monkeypatch, Worker, "stop")
    _lifecycle._shutdown_worker(5.0)
    assert seen == [10], f"stop ran with {seen} tokens held; expected one stop token"
    assert not _lifecycle._state._stops_running


def test_a_no_worker_shutdown_with_a_late_worker_leaves_no_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC-4, the no-worker branch with a late worker built inside the shutdown."""
    log_foundry.configure(service="state", sink=Sink())
    real = _lifecycle._close_owed
    built: list[Worker] = []

    def build_inside(*args: Any, **kwargs: Any) -> bool:
        if not built:
            built.append(_lifecycle._get_worker())
        return real(*args, **kwargs)

    monkeypatch.setattr(_lifecycle, "_close_owed", build_inside)
    seen = _inside_reads(monkeypatch, Worker, "stop")
    _lifecycle._shutdown_worker(5.0)
    assert built and _lifecycle._state._late_worker is None, "the late worker was not taken"
    assert seen == [10], f"the late worker's stop saw {seen} tokens; expected the shutdown's one"
    assert not _lifecycle._state._stops_running


def test_an_interrupted_worker_stop_leaves_no_stop_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-4: a ``KeyboardInterrupt`` from ``Worker.stop``, after the real stop has run."""
    _live_worker()
    real = Worker.stop

    def interrupted(self: Worker, *args: Any, **kwargs: Any) -> None:
        real(self, *args, **kwargs)
        raise KeyboardInterrupt

    monkeypatch.setattr(Worker, "stop", interrupted)
    with pytest.raises(KeyboardInterrupt):
        _lifecycle._shutdown_worker(5.0)
    assert not _lifecycle._state._stops_running, "the discard is not in a finally"


def test_an_interrupted_owed_close_leaves_no_stop_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-4: a ``KeyboardInterrupt`` from ``_close_owed``, on the no-worker branch."""
    log_foundry.configure(service="state", sink=Sink())
    log_foundry.info("owed")
    real = _lifecycle._close_owed

    def interrupted(*args: Any, **kwargs: Any) -> bool:
        real(*args, **kwargs)
        raise KeyboardInterrupt

    monkeypatch.setattr(_lifecycle, "_close_owed", interrupted)
    with pytest.raises(KeyboardInterrupt):
        _lifecycle._shutdown_worker(5.0)
    assert not _lifecycle._state._stops_running, "the discard is not in a finally"
    assert _lifecycle._state._shutdown_running == 0


# -- two calls inside at once ------------------------------------------------------------------


class _Gates:
    """Holds each calling thread at a gate until the test opens it, one gate per thread."""

    def __init__(self) -> None:
        self.entered: dict[str, threading.Event] = {}
        self.release: dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def arm(self, name: str) -> None:
        self.entered[name] = threading.Event()
        self.release[name] = threading.Event()

    def hold(self) -> None:
        name = threading.current_thread().name
        with self._lock:
            entered = self.entered.get(name)
            release = self.release.get(name)
            if entered is None or release is None or entered.is_set():
                return
            entered.set()
        assert release.wait(GATE_WAIT), "the test never opened this gate"


def _overlap(gates: _Gates, call: Callable[[], None], tokens: Callable[[], set[object]]) -> None:
    """Runs two calls that each stop at a gate, and checks the token count at every point."""
    threads = [threading.Thread(target=call, name=f"call-{n}", daemon=True) for n in (1, 2)]
    for thread in threads:
        gates.arm(thread.name)
    for thread in threads:
        thread.start()
        assert gates.entered[thread.name].wait(GATE_WAIT), f"{thread.name} never reached its gate"
    assert len(tokens()) == 2, "two calls inside hold two tokens"
    gates.release["call-1"].set()
    threads[0].join(GATE_WAIT)
    assert not threads[0].is_alive()
    assert len(tokens()) == 1, "the second call is still inside after the first returns"
    gates.release["call-2"].set()
    threads[1].join(GATE_WAIT)
    assert not threads[1].is_alive()
    assert len(tokens()) == 0, "none after both return"


def test_two_overlapping_swaps_hold_two_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-5, for ``_swap_sink``: each call's token is its own, so the first to leave clears one."""
    _live_worker()
    gates = _Gates()
    real = Worker.retarget

    def gated(self: Worker, *args: Any, **kwargs: Any) -> Any:
        gates.hold()
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Worker, "retarget", gated)
    _overlap(
        gates,
        lambda: _lifecycle._swap_sink(Sink(), timeout=5.0),
        lambda: _lifecycle._state._swaps_running,
    )


def test_two_overlapping_shutdowns_hold_two_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    """AC-5, for ``_shutdown_worker``, on the no-worker branch where both reach ``_close_owed``."""
    gates = _Gates()
    real = _lifecycle._close_owed

    def gated(*args: Any, **kwargs: Any) -> bool:
        gates.hold()
        return real(*args, **kwargs)

    monkeypatch.setattr(_lifecycle, "_close_owed", gated)
    _overlap(
        gates,
        lambda: _lifecycle._shutdown_worker(5.0),
        lambda: _lifecycle._state._stops_running,
    )


# -- the pinned lock counts --------------------------------------------------------------------


class _CountingLock:
    """Wraps the state lock and counts every acquisition, by ``with`` or ``acquire``."""

    def __init__(self, real: Any) -> None:
        self.real = real
        self.count = 0

    def __enter__(self) -> bool:
        self.count += 1
        return bool(self.real.__enter__())

    def __exit__(self, *exc: object) -> None:
        self.real.__exit__(*exc)

    def acquire(self, *args: Any, **kwargs: Any) -> bool:
        self.count += 1
        return bool(self.real.acquire(*args, **kwargs))

    def release(self) -> None:
        self.real.release()

    def locked(self) -> bool:
        return bool(self.real.locked())


def _counted(call: Callable[[], object]) -> int:
    """How many times ``call`` takes ``_state._lock``."""
    counter = _CountingLock(_lifecycle._state._lock)
    _lifecycle._state._lock = counter  # type: ignore[assignment]
    try:
        call()
    finally:
        _lifecycle._state._lock = counter.real
    return counter.count


def _configure_new_sink() -> None:
    log_foundry.configure(sink=Sink())


def _setup_live_worker() -> None:
    _live_worker()


def _setup_owed() -> None:
    log_foundry.configure(service="state", sink=Sink())
    log_foundry.info("owed")


def _setup_retired_worker() -> None:
    _live_worker()
    log_foundry.shutdown(timeout=5.0)


def _setup_configured() -> None:
    log_foundry.configure(service="state", sink=Sink())


def _info() -> None:
    log_foundry.info("x")


def _two_infos() -> None:
    log_foundry.configure(service="state", sink=Sink())
    log_foundry.info("first")


LOCK_COUNTS: list[tuple[str, Callable[[], None], Callable[[], object], int]] = [
    ("configure-with-a-live-worker", _setup_live_worker, _configure_new_sink, 2),
    ("configure-with-no-worker-and-something-owed", _setup_owed, _configure_new_sink, 1),
    ("configure-with-nothing-owed-and-no-live-worker", _rule6, _configure_new_sink, 1),
    (
        "configure-after-a-shutdown-with-a-retired-worker",
        _setup_retired_worker,
        _configure_new_sink,
        1,
    ),
    (
        "shutdown-on-the-worker-branch",
        _setup_live_worker,
        lambda: log_foundry.shutdown(timeout=5.0),
        2,
    ),
    (
        "shutdown-on-the-no-worker-branch-owed",
        _setup_owed,
        lambda: log_foundry.shutdown(timeout=5.0),
        3,
    ),
    ("shutdown-on-the-no-worker-branch-cold", _rule6, lambda: log_foundry.shutdown(timeout=5.0), 3),
    ("exit-drain-on-the-worker-branch", _setup_live_worker, _lifecycle._shutdown_worker, 2),
    ("flush-with-a-worker", _setup_live_worker, lambda: log_foundry.flush(timeout=5.0), 1),
    ("a-first-orphan-info", _setup_configured, _info, 1),
    ("a-second-orphan-info", _two_infos, _info, 0),
    ("a-first-trace-call-builds", _setup_configured, _span, 1),
    ("a-later-trace-call", _setup_live_worker, _span, 0),
]


@pytest.mark.parametrize(
    ("setup", "call", "expected"),
    [c[1:] for c in LOCK_COUNTS],
    ids=[c[0] for c in LOCK_COUNTS],
)
def test_lock_counts_are_pinned(
    setup: Callable[[], None], call: Callable[[], object], expected: int
) -> None:
    """AC-6, and invariants 1, 3 and 10: no call gains a state-lock acquisition.

    The numbers are SPEC-077 FR-002's, measured at ``fda134a`` and again at ``3d65cf32`` before
    the token sets existed; the two agree. A token discarded under the lock, rather than by one
    lock-free ``set.discard``, raises the swap and shutdown rows by one.
    """
    setup()
    assert _counted(call) == expected


# -- no lock, no reader, the hot path, totality ------------------------------------------------


def test_state_returns_while_another_thread_holds_the_lock() -> None:
    """AC-7: ``state`` takes no lock, so it answers while the lock is held elsewhere."""
    answers: list[State] = []
    reader = threading.Thread(target=lambda: answers.append(_lifecycle._state.state), daemon=True)
    with _lifecycle._state._lock:
        reader.start()
        reader.join(GATE_WAIT)
        answered = not reader.is_alive()
    reader.join(GATE_WAIT)
    assert answered, "state blocked on _state._lock, which several guards hold when they ask"
    assert answers == [State.UNCONFIGURED]


def _reads_of_state_outside_runtime(tree: ast.AST) -> list[int]:
    """Lines reading an attribute named ``state`` anywhere but inside ``class Runtime``."""
    lines: list[int] = []

    def visit(node: ast.AST, inside: bool) -> None:
        for child in ast.iter_child_nodes(node):
            here = inside or (isinstance(child, ast.ClassDef) and child.name == "Runtime")
            if isinstance(child, ast.Attribute) and child.attr == "state" and not here:
                lines.append(child.lineno)
            visit(child, here)

    visit(tree, False)
    return lines


def test_nothing_in_src_outside_runtime_reads_state() -> None:
    """AC-7: an observer only while nothing in the library acts on it."""
    readers = {
        str(path.relative_to(_SRC)): lines
        for path in sorted(_SRC.rglob("*.py"))
        if (lines := _reads_of_state_outside_runtime(ast.parse(path.read_text(encoding="utf-8"))))
    }
    assert not readers, (
        f"src/ reads `.state` outside Runtime, so it is no longer an observer: {readers}"
    )


def test_the_scan_sees_a_reader_it_must_refuse() -> None:
    """The scan above is not vacuous: it finds a read placed outside ``Runtime``."""
    tree = ast.parse("class Runtime:\n    x = 1\n\ndef f(s):\n    return s.state\n")
    assert _reads_of_state_outside_runtime(tree) == [5]
    inside = ast.parse("class Runtime:\n    def f(self):\n        return self.state\n")
    assert _reads_of_state_outside_runtime(inside) == []


def test_the_hot_path_check_is_still_unlocked() -> None:
    """AC-9: ``_get_worker`` asks the bare existence question before it takes any lock."""
    tree = ast.parse((_SRC / "_lifecycle.py").read_text(encoding="utf-8"))
    get_worker = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_get_worker"
    )
    body = get_worker.body[1:]
    assert ast.unparse(body[0]) == "worker = _state.worker_exists()", ast.unparse(body[0])
    first_with = min(n.lineno for n in ast.walk(get_worker) if isinstance(n, ast.With))
    assert body[0].lineno < first_with, "the outer check must come before the lock"


def test_a_child_drops_the_tokens_it_inherited() -> None:
    """AC-8, invariant 12: the calls that added them did not survive the fork.

    Removing the two clears from ``_clear_after_fork`` turns this red: the child reads both sets
    non-empty and ``DRAINING``.
    """
    swap, stop = object(), object()
    _lifecycle._state._swaps_running.add(swap)
    _lifecycle._state._stops_running.add(stop)
    try:
        child = run_in_child(
            lambda: (
                f"{len(_lifecycle._state._swaps_running)},"
                f"{len(_lifecycle._state._stops_running)},"
                f"{_lifecycle._state.state.name}"
            )
        )
    finally:
        _lifecycle._state._swaps_running.discard(swap)
        _lifecycle._state._stops_running.discard(stop)
    assert child.finished, child.output
    swaps, stops, name = child.output.split(",")
    assert (swaps, stops) == ("0", "0"), f"the child inherited tokens: {child.output}"
    assert name not in {"SWAPPING", "DRAINING"}, child.output


def test_state_never_raises_for_any_combination() -> None:
    """AC-10, invariant 1: every combination of the inputs above answers a ``State``."""
    worker = Worker(Sink())
    sink = Sink()
    state = _lifecycle._state
    try:
        for stop, swap, which, retired, owed, resolved in itertools.product(
            (False, True),
            (False, True),
            ("none", "live", "retired"),
            (False, True),
            (False, True),
            (False, True),
        ):
            state._stops_running.clear()
            state._swaps_running.clear()
            if stop:
                state._stops_running.add(object())
            if swap:
                state._swaps_running.add(object())
            state._worker = None if which == "none" else worker
            state.retirements = worker._epoch + (1 if which == "retired" or retired else 0)
            state._closes.owed.clear()
            if owed:
                state._closes.owed[id(sink)] = sink
            log_foundry.config._config = log_foundry.config.Config(sink=sink if resolved else None)
            assert isinstance(state.state, State)
    finally:
        state._worker = worker
