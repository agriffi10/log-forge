"""The model test behind ``docs/invariants.md``: random interleavings, one accounting identity.

Every lifecycle race this repo has shipped a fix for — SPEC-033's two same-day regressions,
SPEC-044's five, SPEC-045's owed-close slot — was found by a scratch fuzz *after* the diff
reviews had passed, and was then pinned by a test written from its own reproduction. A test
written that way inherits the reproduction's blind spot (``tests/README.md``, and the memory
this repo keeps of it), and the fuzz that found the defect was thrown away each time. This file
is that fuzz made permanent, and it pins no single defect: it drives random interleavings of the
public lifecycle calls from several threads and checks the invariants that hold *regardless* of
the interleaving — the ones ``docs/invariants.md`` numbers, cited beside each assertion.

What it is not: a bound test. The timeouts here are generous on purpose, because a tight budget
fails on its setup under a loaded ``-n 12`` run (``tests/test_worker.py`` measures the bounds
against a wedged sink, one at a time). A fork is not in the operation set either — it needs a
subprocess harness and its own repair is covered by ``tests/test_fork_lifecycle.py``.

To soak locally, widen ``SEEDS`` and run this file alone with ``-n 12``; a failing seed prints
the whole ledger so the interleaving can be replayed from the seed alone.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field

import pytest

import log_foundry
from conftest import run_concurrently

SEEDS = range(16)
THREADS = 4
OPS_PER_THREAD = 60
FLUSH_TIMEOUT = 2.0
SHUTDOWN_TIMEOUT = 3.0
FINAL_SHUTDOWN_TIMEOUT = 5.0
BOUND_SLACK = 10.0


class Recorder:
    """A sink that keeps every event, counts closes, and can be slow, so the drain has a window.

    It keeps accepting after ``close()`` — the ``MemorySink`` half of SPEC-032's rule — because
    the library is *allowed* to write to a closed sink on the orphan path (SPEC-030 accepts
    logging after ``shutdown()``, and SPEC-045 owes such a sink a second close). A refusing double
    would turn that accepted behaviour into ``orphan_lost``, which the identity below also
    reconciles, but a keeping double lets the ledger say where every event went. It declares
    ``log_foundry_stop_signal`` so the stop-signal offer has somewhere to land (SPEC-027 FR-002).
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, emit_delay: float) -> None:
        self.events: list[dict[str, object]] = []
        self.closes = 0
        self.emit_delay = emit_delay
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        if self.emit_delay:
            time.sleep(self.emit_delay)
        with self._lock:
            self.events.extend(batch)

    def close(self) -> None:
        with self._lock:
            self.closes += 1

    def count(self, message: str) -> int:
        with self._lock:
            return sum(1 for event in self.events if event.get("message") == message)


@dataclass
class Ledger:
    """What the actors did, counted where they did it, so the identity has a left-hand side."""

    spans: int = 0
    orphans: int = 0
    slowest_flush: float = 0.0
    slowest_shutdown: float = 0.0
    sinks: list[Recorder] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_sink(self, sink: Recorder) -> None:
        with self._lock:
            self.sinks.append(sink)

    def note(self, **counts: int) -> None:
        with self._lock:
            for name, value in counts.items():
                setattr(self, name, getattr(self, name) + value)

    def timed(self, name: str, elapsed: float) -> None:
        with self._lock:
            setattr(self, name, max(getattr(self, name), elapsed))


@log_foundry.trace
def _inner() -> None:
    log_foundry.info("in")


@log_foundry.trace
def _outer(nested: bool) -> None:
    log_foundry.info("in")
    if nested:
        _inner()


def _actor(ledger: Ledger, rng: random.Random, *, shutdown_in_mix: bool) -> None:
    """One thread's worth of the operation mix; the weights favour the calls that carry events.

    Half the seeds keep ``shutdown()`` out of the mix, so the only shutdown is the final one and
    the clean path is exercised alone. With a second call always following, the idempotent
    branch's own close masks a clean-path close that never ran — measured: deleting the clean
    path's ``_close_if_owed`` survived every seed until this split existed.
    """
    for _ in range(OPS_PER_THREAD):
        roll = rng.random()
        if roll < 0.55 or (roll >= 0.95 and not shutdown_in_mix):
            nested = rng.random() < 0.3
            _outer(nested)
            ledger.note(spans=2 if nested else 1)
        elif roll < 0.75:
            log_foundry.info("orphan")
            ledger.note(orphans=1)
        elif roll < 0.85:
            started = time.monotonic()
            log_foundry.flush(timeout=FLUSH_TIMEOUT)
            ledger.timed("slowest_flush", time.monotonic() - started)
        elif roll < 0.95:
            sink = Recorder(emit_delay=rng.choice([0.0, 0.0, 0.002]))
            ledger.add_sink(sink)
            log_foundry.configure(service="model", sink=sink)
        else:
            started = time.monotonic()
            log_foundry.shutdown(timeout=SHUTDOWN_TIMEOUT)
            ledger.timed("slowest_shutdown", time.monotonic() - started)


@pytest.mark.parametrize("seed", SEEDS)
def test_the_accounting_identity_holds_under_every_interleaving(seed: int) -> None:
    """Invariants 1, 2, 3, 5 and 6 of ``docs/invariants.md``, over one seeded interleaving.

    The identity is the whole test: every span the actors started is either delivered as a
    ``span.end``, still queued where a ``shutdown()`` left it, or counted as dropped; every
    orphan is delivered or counted in ``orphan_lost``; nothing is missing and nothing is counted
    twice. A regression in the lifecycle plumbing shows up here as a seed whose ledger does not
    balance, before anyone has written the test that names it.
    """
    ledger = Ledger()
    first = Recorder(emit_delay=random.Random(seed).choice([0.0, 0.0, 0.002]))  # noqa: S311
    ledger.add_sink(first)
    log_foundry.configure(service="model", sink=first)

    def work(index: int, _iteration: int) -> None:
        _actor(ledger, random.Random(seed * 10 + index), shutdown_in_mix=seed % 2 == 0)  # noqa: S311

    escaped = run_concurrently(work, threads=THREADS)
    log_foundry.shutdown(timeout=FINAL_SHUTDOWN_TIMEOUT)
    health = log_foundry.health()

    ends = sum(sink.count("span.end") for sink in ledger.sinks)
    orphans = sum(sink.count("orphan") for sink in ledger.sinks)
    unclosed = [i for i, sink in enumerate(ledger.sinks) if sink.events and sink.closes == 0]
    picture = (
        f"seed={seed} spans={ledger.spans} span.end={ends} queued={health.queued} "
        f"dropped={health.dropped} in_span_lost={health.in_span_lost} | "
        f"orphans={ledger.orphans} delivered={orphans} orphan_lost={health.orphan_lost} | "
        f"failed_batches={health.failed_batches} incomplete_swaps={health.incomplete_swaps} "
        f"sinks={len(ledger.sinks)} unclosed={unclosed} escaped={escaped!r}"
    )

    assert not escaped, f"invariant 1 (never fail the caller): {picture}"
    assert health.in_span_lost == 0, f"nothing here can fail assembly: {picture}"
    assert health.failed_batches == 0, f"the recorder never raises: {picture}"
    assert ends + health.queued + health.dropped == ledger.spans, f"invariant 2 (spans): {picture}"
    assert orphans + health.orphan_lost == ledger.orphans, f"invariant 2 (orphans): {picture}"
    assert health.retired, f"invariant 2 (a shutdown ran, so retired reports it): {picture}"
    assert health.submitted_after_shutdown >= health.queued, (
        f"invariant 2 (what a shutdown left queued was counted as submitted after it): {picture}"
    )
    assert not unclosed, f"invariant 5 (every sink written to is closed): {picture}"
    assert ledger.slowest_flush < FLUSH_TIMEOUT + BOUND_SLACK, f"invariant 3 (flush): {picture}"
    assert ledger.slowest_shutdown < SHUTDOWN_TIMEOUT + BOUND_SLACK, (
        f"invariant 3 (shutdown): {picture}"
    )
