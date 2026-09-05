"""The model test behind ``docs/invariants.md``: random interleavings, one accounting identity.

The lifecycle defects this repo has fixed came in two classes. One needs an injected preemption
point — a window a few instructions wide, which `tests/test_lifecycle_races.py` pins race by race
(its own docstring measures 0 of 120 unforced runs for one of them). The other is reached at an
unforced rate under ordinary concurrency: during SPEC-045 a candidate fix that refused a repeat
close was built and lost on 31 of 80 seeds of a scratch fuzz, in the second diff review, after
the reading passes had accepted it — and that fuzz was then thrown away, with the shipped fix
pinned by a test written from its own reproduction. This file is that second class made
permanent. It drives random interleavings of the public lifecycle calls
from several threads and checks the invariants that hold *regardless* of the interleaving — the
ones ``docs/invariants.md`` numbers, cited beside each assertion. It cannot see the first class,
and does not claim to.

What it is not: a bound test. The timeouts here are generous on purpose, because a tight budget
fails on its setup under a loaded ``-n 12`` run (``tests/test_worker.py`` measures the bounds
against a wedged sink, one at a time). A fork is not in the operation set either — it needs a
subprocess harness and its own repair is covered by ``tests/test_fork_lifecycle.py``.

To soak locally, widen ``SEEDS`` and run this file alone with ``-n 12``; a failing seed prints
the whole ledger so the interleaving can be replayed from the seed alone.
"""

from __future__ import annotations

import pathlib
import random
import re
import threading
import time
from dataclasses import dataclass, field

import pytest

import log_foundry
from conftest import run_concurrently
from log_foundry import _lifecycle

SEEDS = range(16)
THREADS = 4
OPS_PER_THREAD = 60
FLUSH_TIMEOUT = 2.0
SHUTDOWN_TIMEOUT = 3.0
FINAL_SHUTDOWN_TIMEOUT = 5.0
BOUND_SLACK = 10.0

_REPO = pathlib.Path(__file__).resolve().parent.parent
_PAGE = _REPO / "docs" / "invariants.md"


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


def _stranded_markers() -> int:
    """Counts the flush markers left on the retired worker's queue after the final shutdown.

    ``Health.queued`` is ``qsize()`` and counts them alongside real submissions — a ``flush()``
    marker stranded by a racing ``shutdown()`` is answered and then counted for the life of the
    process, as its docstring says (SPEC-050 FR-001). The identity subtracts them: they are not
    spans, and a marker's own ``FlushResult`` cannot be used instead, since the ledger keeps no
    per-call results and a stranded marker's caller reads ``thread-died`` or ``abandoned``, the
    same words a swept one can read. Measured before this existed: about one seed in eight read
    ``queued`` one or two markers high and the ledger did not balance. The ``_SHUTDOWN`` sentinel
    is counted too — it is stranded only by a terminal drain failure, which the recorder cannot
    cause, and it is not a span either.
    """
    worker = _lifecycle._state.worker_exists()
    if worker is None:
        return 0
    with worker._queue.mutex:
        return sum(1 for item in worker._queue.queue if not isinstance(item, list))


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
    markers = _stranded_markers()
    queued = health.queued - markers

    ends = sum(sink.count("span.end") for sink in ledger.sinks)
    orphans = sum(sink.count("orphan") for sink in ledger.sinks)
    unclosed = [i for i, sink in enumerate(ledger.sinks) if sink.events and sink.closes == 0]
    picture = (
        f"seed={seed} spans={ledger.spans} span.end={ends} queued={health.queued} "
        f"markers={markers} dropped={health.dropped} in_span_lost={health.in_span_lost} | "
        f"orphans={ledger.orphans} delivered={orphans} orphan_lost={health.orphan_lost} | "
        f"failed_batches={health.failed_batches} incomplete_swaps={health.incomplete_swaps} "
        f"stopped_reason={health.stopped_reason} after_shutdown={health.submitted_after_shutdown} "
        f"sinks={len(ledger.sinks)} "
        f"unclosed={unclosed} escaped={escaped!r}"
    )

    assert not escaped, f"invariant 1 (never fail the caller): {picture}"
    assert health.in_span_lost == 0, f"nothing here can fail assembly: {picture}"
    assert health.failed_batches == 0, f"the recorder never raises: {picture}"
    assert ends + queued + health.dropped == ledger.spans, f"invariant 2 (spans): {picture}"
    assert orphans + health.orphan_lost == ledger.orphans, f"invariant 2 (orphans): {picture}"
    assert health.retired, f"invariant 2 (a shutdown ran, so retired reports it): {picture}"
    assert health.submitted_after_shutdown >= queued, (
        f"invariant 2 (what a shutdown left queued was counted as submitted after it): {picture}"
    )
    assert not unclosed, f"invariant 5 (every sink written to is closed): {picture}"
    assert ledger.slowest_flush < FLUSH_TIMEOUT + BOUND_SLACK, f"invariant 3 (flush): {picture}"
    assert ledger.slowest_shutdown < SHUTDOWN_TIMEOUT + BOUND_SLACK, (
        f"invariant 3 (shutdown): {picture}"
    )


def test_the_page_cites_tests_that_exist_and_this_file_cites_invariants_that_do() -> None:
    """The page's ``Guarded:`` paths and this file's ``invariant N`` labels cannot rot silently.

    A renamed test file would otherwise leave the page pointing at nothing, and a renumbered
    invariant would leave an assertion message naming the wrong promise; neither fails anything
    on its own. The failure text names the missing path or number.
    """
    page = _PAGE.read_text(encoding="utf-8")
    missing = sorted(
        {path for path in re.findall(r"`(tests/[\w./-]+)`", page) if not (_REPO / path).exists()}
    )
    assert not missing, f"docs/invariants.md cites tests that do not exist: {missing}"

    headings = [int(n) for n in re.findall(r"^## (\d+)\. ", page, flags=re.MULTILINE)]
    source = pathlib.Path(__file__).read_text(encoding="utf-8")
    cited = {int(n) for n in re.findall(r"invariant (\d+)", source)}
    assert cited <= set(headings), (
        f"this file cites invariants the page lacks: {sorted(cited - set(headings))}"
    )
    assert headings == list(range(1, len(headings) + 1)), (
        f"the page's numbering is not 1..n in order without repeats: {headings}"
    )
