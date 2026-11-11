"""Two ``configure(sink=...)`` calls at once close each sink once and leave config and delivery agreed.

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
"""The patched swap budget where a test waits it out: a second, so half of it clears the stall rule."""

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
    """
    a, b = CountingSink("A"), CountingSink("B")
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
    assert 0.9 * BUDGET <= wall < BUDGET + 1.0, f"waited {wall:.2f}s against a {BUDGET}s budget"
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
    assert elapsed < 0.5 * BUDGET, f"a call that changes no sink waited {elapsed:.2f}s on the lock"
    assert log_foundry.get_config().service == "stamp-only"


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
