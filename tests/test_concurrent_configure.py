"""Two ``configure(sink=...)`` calls at once close each sink once, and config and delivery agree.

Both swaps retarget the live worker, which ``Worker.retarget`` serializes, so the worker goes A to C
and then C to B. Each swap then decides what to arm and what to release from what its own retarget
saw. B's swap closed C as the sink it displaced, and C's swap, deciding afterwards, re-armed C, so
the exit closed C a second time with nothing written in between. The config write and the retarget
were also ordered independently, so ``get_config()`` could name one sink while delivery went to the
other. Measured at ``641dfc86`` in a fresh process per run: 12 of 100 runs closed a sink twice and
15 of 100 disagreed. ``config._sink_lock`` now holds a sink-changing ``configure()`` from the config
write to the end of the swap. Serves invariants 5 and 3.
"""

from __future__ import annotations

import ast
import gc
import io
import math
import os
import pathlib
import select
import signal
import sys
import threading
import time
from typing import Any

import pytest

import log_foundry
from conftest import _reset_worker, cpu_window
from log_foundry import _fork, _lifecycle, config, context
from log_foundry import worker as worker_mod
from log_foundry.worker import Worker
from test_fork_lifecycle import _held_by_a_parent_thread, run_in_child

pytestmark = pytest.mark.usefixtures("no_close_left_in_flight")

BUDGET = 1.0
"""The patched swap budget where a test waits it out: a second, so half clears the stall rule."""

STRESS_ROUNDS = 60
"""Unforced rounds in the stress test.

At ``641dfc86``, before the lock, about 18% of rounds were defective (see the stress test), so a
60-round run passes there with a probability far below 1%.
"""


class CountingSink:
    """Counts events and closes, and records how many events had arrived by each close.

    Two closes at the same event count are a double close: nothing was written between them, so
    the second had nothing to flush. A second close after a write is owed (SPEC-045) and is not
    what these tests look for.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str) -> None:
        self.name = name
        self.events = 0
        self.at_close: list[int] = []
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts the batch."""
        with self._lock:
            self.events += len(batch)

    def close(self) -> None:
        """Records the close and the event count it saw."""
        with self._lock:
            self.at_close.append(self.events)

    @property
    def closes(self) -> int:
        """How many times it was closed."""
        return len(self.at_close)

    def __repr__(self) -> str:
        """Names the sink and its closes, so a failure reads without a debugger."""
        return f"<{self.name} closes at events {self.at_close}>"


class SlowCloseSink(CountingSink):
    """A sink whose close takes twice the patched budget, so a swap joining it shows its budget."""

    def close(self) -> None:
        """Takes its time, then records."""
        time.sleep(2 * BUDGET)
        super().close()


@log_foundry.trace
def _traced() -> None:
    log_foundry.info("span")


def _worker_on(sink: CountingSink) -> Worker:
    """Configures ``sink`` and builds a live worker on it through one span."""
    log_foundry.configure(service="concurrent-configure", sink=sink)
    _traced()
    worker = _lifecycle._state.live_worker()
    assert worker is not None and worker.sink is sink
    return worker


def _defects(sinks: list[CountingSink]) -> list[str]:
    """Every sink closed twice with nothing written between, and every sink never closed."""
    found = []
    for sink in sinks:
        if sink.closes == 0:
            found.append(f"{sink.name} never closed")
        elif len(set(sink.at_close)) != sink.closes:
            found.append(f"{sink.name} closed twice with nothing written between: {sink.at_close}")
    return found


def test_a_swap_landing_between_anothers_retarget_and_its_decision_closes_each_sink_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The measured interleaving, forced: C's swap retargets and is parked before it decides.

    Without the lock B's swap runs straight through in that gap, retargeting C to B and closing
    C, and C's swap then re-arms C for a second close at exit. B's first critical section also
    releases A, already displaced and no longer held, as a stale owed sink, and C's swap then
    releases A again as its own displaced one. Measured at ``641dfc86``, 5 of 5 runs: ``A`` closed
    at events ``[3, 3]`` and ``C`` at ``[0, 0]``. With the lock B's call waits on
    ``config._sink_lock`` for the whole of C's swap, and 20 of 20 runs are green.
    """
    a, b, c = CountingSink("A"), CountingSink("B"), CountingSink("C")
    worker = _worker_on(a)
    decided = threading.Event()
    release = threading.Event()
    real = Worker.retarget

    def parked_after(self: Worker, new_sink: Any, deadline: float | None = None) -> Any:
        outcome = real(self, new_sink, deadline)
        if new_sink is c:
            decided.set()
            assert release.wait(10.0), "the parked swap was never released"
        return outcome

    monkeypatch.setattr(Worker, "retarget", parked_after)
    first = threading.Thread(target=lambda: log_foundry.configure(sink=c), daemon=True)
    first.start()
    try:
        assert decided.wait(10.0), "C's swap reached the gap after its retarget"
        second = threading.Thread(target=lambda: log_foundry.configure(sink=b), daemon=True)
        second.start()
        second.join(0.5)
        waited = second.is_alive()
    finally:
        release.set()
    first.join(10.0)
    second.join(10.0)
    assert not first.is_alive() and not second.is_alive()
    agreed = worker.sink is log_foundry.get_config().sink
    log_foundry.shutdown(timeout=5.0)
    assert _defects([a, b, c]) == [], [a, b, c]
    assert agreed, "the config names the sink delivery goes to"
    assert waited, "B's call waited for C's whole swap rather than running in its gap"
    assert worker.sink is b, "B's call ran second, so it retargeted last"


def test_unforced_concurrent_sink_changes_close_each_sink_once_and_agree_with_delivery() -> None:
    """Invariant 5 under the real race, no preemption point: many rounds, none defective.

    Each round builds a worker on a fresh A in a reset process, as the autouse fixture resets
    one between tests, lets two threads pass a barrier into
    ``configure(sink=B)`` and ``configure(sink=C)``, checks that the config names the worker's
    sink, then shuts down and checks every sink closed exactly once. Measured on this test at
    ``641dfc86``, before the lock: 55 of 300 rounds defective over five runs (4, 13, 11, 11 and
    16 of 60), about 18% a round, with 41 double closes and 24 disagreements, and 5 of 5 runs
    red. With the lock, 20 of 20 runs green.
    """
    defects: list[str] = []
    for round_ in range(STRESS_ROUNDS):
        a, b, c = CountingSink("A"), CountingSink("B"), CountingSink("C")
        _worker_on(a)
        barrier = threading.Barrier(2)

        def change(sink: CountingSink, gate: threading.Barrier = barrier) -> None:
            gate.wait(5.0)
            log_foundry.configure(sink=sink)

        threads = [threading.Thread(target=change, args=(s,), daemon=True) for s in (b, c)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10.0)
        assert not any(thread.is_alive() for thread in threads), "a configure() never returned"
        live = _lifecycle._state.live_worker()
        if live is not None and live.sink is not log_foundry.get_config().sink:
            defects.append(f"round {round_}: the config names one sink, delivery goes to another")
        log_foundry.shutdown(timeout=5.0)
        defects.extend(f"round {round_}: {d}" for d in _defects([a, b, c]))
        config._config = config.Config()
        context.reset_context()
        _reset_worker()
    assert defects == [], defects


def test_a_wait_for_the_lock_is_spent_from_the_swap_budget_and_then_proceeds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invariant 3: a holder that overruns delays this call by its budget, not more, and idly.

    The call proceeds without the lock rather than hang or drop the configure: the config and the
    worker both move to the new sink, and one ``pending`` line says the call ran unserialized.
    The displaced sink's close outlasts the budget, so the swap's join of it spends whatever the
    wait left: handed the whole budget again instead of the remainder, this call took 2.01 s
    against 1.00 s (frame-1 review), which the ceiling below refuses.
    """
    a, b = SlowCloseSink("A"), CountingSink("B")
    worker = _worker_on(a)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", BUDGET)
    released, holder = _held_by_a_parent_thread(config._sink_lock)
    try:
        wall = time.monotonic()
        with cpu_window() as cpu:
            log_foundry.configure(sink=b)
        wall = time.monotonic() - wall
    finally:
        released.set()
        holder.join(5.0)
    assert 0.9 * BUDGET <= wall < 1.5 * BUDGET, f"took {wall:.2f}s against a {BUDGET}s budget"
    assert cpu.spent < 0.5 * BUDGET, f"the wait spun: {cpu.spent:.2f}s of CPU"
    assert worker.sink is b and log_foundry.get_config().sink is b, "the configure still landed"
    err = capsys.readouterr().err
    assert "could not wait for a concurrent sink change within its swap budget" in err, err
    assert "lost" not in err, "the line is pending, not a loss"


def test_a_configure_that_changes_no_sink_does_not_take_the_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stamp-only call and the orphan path pay nothing: neither waits on a held lock.

    Taking the lock would cost the patched budget, a second, so half of it separates the two.
    """
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", BUDGET)
    released, holder = _held_by_a_parent_thread(config._sink_lock)
    try:
        start = time.monotonic()
        log_foundry.configure(service="stamp-only", env="test")
        log_foundry.info("orphan")
        elapsed = time.monotonic() - start
    finally:
        released.set()
        holder.join(5.0)
    assert elapsed < 0.5 * BUDGET, f"a call that changes no sink waited {elapsed:.2f}s"
    assert log_foundry.get_config().service == "stamp-only"


def test_a_configure_passing_the_factory_already_configured_does_not_take_the_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Passing the configured factory again changes nothing, so it waits on nothing either."""

    def factory() -> CountingSink:
        return CountingSink("built")

    log_foundry.configure(sink_factory=factory)
    built = log_foundry.get_config().sink
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", BUDGET)
    released, holder = _held_by_a_parent_thread(config._sink_lock)
    try:
        start = time.monotonic()
        log_foundry.configure(sink_factory=factory)
        elapsed = time.monotonic() - start
    finally:
        released.set()
        holder.join(5.0)
    assert elapsed < 0.5 * BUDGET, f"a call that changes nothing waited {elapsed:.2f}s"
    assert log_foundry.get_config().sink is built, "and it built no second sink"


@pytest.mark.parametrize("budget", [math.inf, 1e12], ids=["inf", "above-TIMEOUT_MAX"])
def test_a_budget_above_the_lock_maximum_waits_without_bound_rather_than_raising(
    budget: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Invariant 1: ``acquire`` raises ``OverflowError`` past ``threading.TIMEOUT_MAX``.

    Frame-1 review measured every ``configure(sink=...)`` raising it from the timed acquire with
    the budget at ``inf`` or ``1e12``. At ``641dfc86`` the call returned, but the swap absorbed an
    ``OverflowError`` of its own and left the worker on the old sink. Such a budget is read as
    ``None``, ``sinks._retry.wait_budget``'s rule, for the wait and for the swap.
    """
    a, b = CountingSink("A"), CountingSink("B")
    worker = _worker_on(a)
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", budget)
    log_foundry.configure(sink=b)
    assert worker.sink is b and log_foundry.get_config().sink is b
    err = capsys.readouterr().err
    assert "could not wait" not in err and "absorbed" not in err, err


def test_an_uncontended_sink_change_takes_the_lock_and_leaves_it_free() -> None:
    """The positive control for the test above: a ``sink=`` call takes the lock and releases it.

    Without it, a lock no call ever took would pass the stamp-only test vacuously.
    """
    a, b = CountingSink("A"), CountingSink("B")
    _worker_on(a)
    seen: list[bool] = []
    real = Worker.retarget

    def probe(self: Worker, new_sink: Any, deadline: float | None = None) -> Any:
        seen.append(config._sink_lock.locked())
        return real(self, new_sink, deadline)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Worker, "retarget", probe)
        log_foundry.configure(sink=b)
    assert seen == [True], "the swap ran under the lock"
    assert not config._sink_lock.locked(), "and the call released it"


def test_a_child_forked_while_a_parent_thread_holds_the_lock_changes_its_sink_at_once() -> None:
    """Invariant 12: the lock is on the fork roster, so the child's copy starts unlocked."""
    released, holder = _held_by_a_parent_thread(config._sink_lock)

    def child() -> str:
        sink = CountingSink("child")
        start = time.monotonic()
        log_foundry.configure(sink=sink)
        return f"{time.monotonic() - start:.3f}:{log_foundry.get_config().sink is sink}"

    try:
        result = run_in_child(child)
    finally:
        released.set()
        holder.join(5.0)
    elapsed, landed = result.output.split(":")
    assert landed == "True"
    assert float(elapsed) < 1.0, f"the child waited {elapsed}s on a lock its parent's thread held"


def test_without_its_roster_entry_a_child_waits_out_its_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The control for the test above: off the roster, the child's copy is held, and it waits.

    The wait is bounded, so the child does not block as the other roster controls do; it spends
    its whole swap budget and then proceeds, which is what makes the test above discriminate.
    """
    monkeypatch.setattr(
        _fork,
        "_GLOBAL_PRIMITIVES",
        tuple(pair for pair in _fork._GLOBAL_PRIMITIVES if pair != ("config", "_sink_lock")),
    )
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", BUDGET)
    released, holder = _held_by_a_parent_thread(config._sink_lock)

    def child() -> str:
        sink = CountingSink("child")
        start = time.monotonic()
        log_foundry.configure(sink=sink)
        return f"{time.monotonic() - start:.3f}:{log_foundry.get_config().sink is sink}"

    try:
        result = run_in_child(child)
    finally:
        released.set()
        holder.join(5.0)
    elapsed, landed = result.output.split(":")
    assert landed == "True", "the configure still landed after the wait"
    assert float(elapsed) >= 0.9 * BUDGET, (
        f"the child did not wait on the inherited lock: {elapsed}s"
    )


class _ReenteringSink(CountingSink):
    """A sink whose own code calls ``configure(sink=target)`` once, from the hook it is built for.

    ``where`` is ``"setter"`` (its ``log_foundry_stop_signal`` setter, run by the retarget on the
    thread holding the lock), ``"close"`` (on a closer thread the swap joins) or ``"emit"`` (on the
    drain thread a retarget's flush waits for).
    """

    def __init__(self, name: str, where: str, target: CountingSink) -> None:
        super().__init__(name)
        self.where = where
        self.target = target
        self.fired = False

    def _reenter(self, where: str) -> None:
        if self.where == where and not self.fired:
            self.fired = True
            log_foundry.configure(sink=self.target)

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        """Holds nothing; the setter is the hook."""
        return None

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, value: threading.Event | None) -> None:
        if value is not None:
            self._reenter("setter")

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Counts, then re-enters once if built for it."""
        super().emit(batch)
        self._reenter("emit")

    def close(self) -> None:
        """Re-enters once if built for it, then records the close."""
        self._reenter("close")
        super().close()


@pytest.mark.parametrize("budget", [BUDGET, None], ids=["bounded", "unbounded"])
@pytest.mark.parametrize("where", ["setter", "close", "emit"])
def test_a_sinks_own_code_reentering_configure_neither_waits_nor_closes_twice(
    where: str, budget: float | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A re-entrant call does not wait on the swap waiting on it, and swaps unconfirmed.

    Waiting would stall the outer call a whole budget past its own, or hang it with ``None``
    (frame-2 review, measured). At ``641dfc86``, which had no lock, the setter case closed B at
    events ``[0, 0]`` and the unbounded ``emit`` case stalled 5 s and never closed A. Here every
    case returns at once, closes each sink once, and leaves config and delivery agreed.

    The scenario runs in a forked child, because the regression it guards is a hang: measured
    with the re-entry rule removed, the unbounded case hung the test process past its failed
    assertion, in teardown, where no timeout reaches. ``run_in_child`` kills a child that hangs.
    """
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", budget)

    def scenario() -> str:
        sys.stderr = io.StringIO()
        x = CountingSink("X")
        a = _ReenteringSink("A", where, x) if where in ("close", "emit") else CountingSink("A")
        b = _ReenteringSink("B", where, x) if where == "setter" else CountingSink("B")
        _worker_on(a)

        def act() -> None:
            if where == "emit":
                _traced()
                log_foundry.flush(timeout=5.0)
            else:
                log_foundry.configure(sink=b)

        start = time.monotonic()
        call = threading.Thread(target=act, daemon=True)
        call.start()
        call.join(5.0)
        elapsed = time.monotonic() - start
        if call.is_alive():
            return "hung"
        live = _lifecycle._state.live_worker()
        agreed = live is None or live.sink is log_foundry.get_config().sink
        log_foundry.shutdown(timeout=2.0)
        defects = _defects([s for s in (a, b, x) if s.events or s.closes])
        announced = "was called by a sink's own code" in sys.stderr.getvalue()
        return f"{elapsed:.3f}|{agreed}|{defects}|{announced}"

    child = run_in_child(scenario)
    assert child.finished, f"the child hung: it {child.ending}, {child.output!r}"
    assert child.output != "hung", "the outer call hung on its own re-entrant configure()"
    elapsed, agreed, defects, announced = child.output.split("|")
    assert float(elapsed) < 0.5 * BUDGET, f"the outer call stalled {elapsed}s"
    assert defects == "[]", defects
    assert agreed == "True", "the config names the sink delivery goes to"
    assert announced == "True", "the re-entrant call says it ran unserialized"


def test_a_thread_that_forks_inside_a_sink_change_leaves_the_childs_configure_returning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invariant 1 in a child: the release goes to the lock that was acquired, not the global.

    The fork repair rebinds ``config._sink_lock`` to a fresh, unlocked lock. A child forked by the
    thread inside ``_change_sink`` continues into its ``finally``, which, releasing through the
    global, raised ``RuntimeError: release unlocked lock`` out of ``configure()`` (frame-2 review,
    measured with a stop-signal setter that forked). The fork here is planted after the retarget.
    """
    a, b = CountingSink("A"), CountingSink("B")
    _worker_on(a)
    parent = os.getpid()
    read_end, write_end = os.pipe()
    forked: list[int] = []
    real = Worker.retarget

    def forking(self: Worker, new_sink: Any, deadline: float | None = None) -> Any:
        outcome = real(self, new_sink, deadline)
        if not forked:
            gc.collect()
            forked.append(os.fork())
            if os.getpid() != parent:
                signal.alarm(10)
        return outcome

    monkeypatch.setattr(Worker, "retarget", forking)
    try:
        try:
            log_foundry.configure(sink=b)
            result = "returned"
        except BaseException as exc:
            result = f"raised {type(exc).__name__}"
        if os.getpid() != parent:
            os.write(write_end, result.encode())
            os._exit(0)
    finally:
        if os.getpid() != parent:
            os._exit(1)
    os.close(write_end)
    child = forked[0]
    try:
        ready, _, _ = select.select([read_end], [], [], 15.0)
        report = os.read(read_end, 200).decode() if ready else "no report"
    finally:
        os.close(read_end)
        if not ready:
            os.kill(child, signal.SIGKILL)
        os.waitpid(child, 0)
    assert result == "returned", f"the parent's configure(): {result}"
    assert report == "returned", f"the child's configure(): {report}"


def _sink_lock_sites(source: dict[str, str]) -> list[str]:
    """Every place a module in ``source`` reads ``_sink_lock``, as ``file:function:shape``.

    A bare name and an attribute both count, so ``config._sink_lock`` reached from another module
    is seen as well as the global inside ``config``. The function is the innermost one enclosing
    the read, ``<module>`` for none. The shape is ``bind`` for a plain assignment of the name to a
    local and ``use`` for anything else.
    """
    sites = []
    for path, text in sorted(source.items()):
        tree = ast.parse(text)
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(tree):
            hit = (isinstance(node, ast.Name) and node.id == "_sink_lock") or (
                isinstance(node, ast.Attribute) and node.attr == "_sink_lock"
            )
            if not hit or not isinstance(getattr(node, "ctx", None), ast.Load):
                continue
            parent = parents.get(node)
            bound = (
                isinstance(parent, ast.Assign)
                and parent.value is node
                and all(isinstance(target, ast.Name) for target in parent.targets)
            )
            scope = parent
            while scope is not None and not isinstance(
                scope, (ast.FunctionDef, ast.AsyncFunctionDef)
            ):
                scope = parents.get(scope)
            name = scope.name if scope is not None else "<module>"
            sites.append(f"{path}:{name}:{'bind' if bound else 'use'}")
    return sorted(sites)


def test_the_lock_is_taken_at_one_site_through_a_local() -> None:
    """The one site, pinned: ``_change_sink`` binds the lock to a local and uses nothing else.

    ``tests/test_runtime_holder.py``'s ``LOCK_SITES_AT_BASE`` sees only ``with`` statements in
    ``_lifecycle.py`` and ``worker.py``, so it can see neither this module nor an
    ``acquire``/``release`` pair. A second site would serialize something the docs do not say, and
    a use of the global past the bind would release the child's fresh lock after a fork.
    """
    src = pathlib.Path(config.__file__).resolve().parent
    source = {
        str(path.relative_to(src)): path.read_text(encoding="utf-8") for path in src.rglob("*.py")
    }
    assert _sink_lock_sites(source) == ["config.py:_change_sink:bind"]


@pytest.mark.parametrize(
    ("planted", "site"),
    [
        ("    _sink_lock.release()\n", "config.py:_change_sink:use"),
        ("    with config._sink_lock:\n        pass\n", "_lifecycle.py:planted:use"),
    ],
    ids=["a-use-past-the-bind", "a-second-site"],
)
def test_the_site_pin_sees_a_planted_site(planted: str, site: str) -> None:
    """The pin's control: a planted use of the global, here or in another module, is seen."""
    text = pathlib.Path(config.__file__).read_text(encoding="utf-8")
    source = {"config.py": text}
    if site.startswith("config.py"):
        anchor = "    lock = _sink_lock\n"
        source["config.py"] = text.replace(anchor, anchor + planted, 1)
    else:
        source["_lifecycle.py"] = "def planted():\n" + planted
    assert site in _sink_lock_sites(source)
