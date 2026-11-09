"""The model tests behind ``docs/invariants.md``: random interleavings, and a sequential oracle.

The lifecycle defects this repo has fixed came in two classes. One needs an injected preemption
point — a window a few instructions wide, which `tests/test_lifecycle_races.py` pins race by race
(its own docstring measures 0 of 120 unforced runs for one of them). The other is reached at an
unforced rate under ordinary concurrency: during SPEC-045 a candidate fix that refused a repeat
close was built and lost on 31 of 80 seeds of a scratch fuzz, in the second diff review, after
the reading passes had accepted it — and that fuzz was then thrown away, with the shipped fix
pinned by a test written from its own reproduction. This file is that second class made
permanent, twice over.

**The concurrent test** drives random interleavings of the public lifecycle calls from several
threads and checks the invariants that hold *regardless* of the interleaving — the ones
``docs/invariants.md`` numbers, cited beside each assertion. Under concurrency the state after a
step is not determined, so it asserts no exact state: only that ``Runtime.state`` always answers
a ``State`` and ends ``CLOSED`` with no token held. It cannot see the first class, and does not
claim to.

**The sequential driver** (SPEC-077 FR-004) is one thread, seeded, drawing ``configure(sink=…)``,
an orphan ``info()``, a span, ``flush()``, ``shutdown()``, the exit drain
(``_lifecycle._shutdown_worker()`` with no argument, what ``atexit`` calls) and a fork through
``run_in_child``. With one caller the state after a step is determined, so it carries its own
**state table** (:data:`TABLE`), independent of ``Runtime.state``, and checks the library against
it after every step, together with the facts the table is keyed by — whether something is owed
and whether the worker is live are read back from the runtime, not trusted. Every sink is a
recorder, a default-resolved one included: ``StdoutSink`` is replaced by a factory, so the real
``_ensure_sink`` path, stamp and all, builds one. Coverage of every cell is asserted over the seed
range rather than left to chance (:func:`test_the_seed_range_covers_every_cell`).

**What the driver checks, by invariant**: 1 (no step raises, ``BaseException`` included); 2
(orphans balance after every step, spans after every truthy ``flush()`` and every shutdown); 3,
the wall-clock half (each step inside its budget plus the slack); 4 (every ``span.end`` arrives on
a drain thread and every orphan on the caller's); 5 (after each shutdown every recorder written
to is closed, nothing is owed, no two closes overlapped, and no child closed an inherited sink);
6 by construction, since one sequence drives both delivery paths and both drains; and 12, the
child's half (its state, its three calls, its event in a child recorder) and the parent's ledger
unchanged by the fork.

**What it does not check, and why**: the **CPU half of 3** — a CPU measure over a hundred steps
under ``-n 12`` is the flake ``tests/test_worker_exit_budget.py`` measures one case at a time;
**7** needs a sink that raises, and the recorder never does (``tests/test_worker.py`` and
``tests/test_sink_losses.py`` own it); **8** needs a hostile value and no step supplies one
(``tests/test_sanitize_corpus.py``); **9** needs baggage or an adopted context and no step sets
one; **10** is the public surface and ``Runtime`` is private; **11** needs a value that could leak
into stderr and none is supplied; **13** needs a bad argument and every step is valid.

What neither is: a bound test. The timeouts here are generous on purpose, because a tight budget
fails on its setup under a loaded ``-n 12`` run (``tests/test_worker.py`` measures the bounds
against a wedged sink, one at a time).

To soak locally, widen ``SEEDS`` or ``SEQ_SEEDS`` and run this file alone with ``-n 12``; a
failing seed prints its whole history so it replays from the seed alone.
"""

from __future__ import annotations

import json
import pathlib
import random
import re
import threading
import time
import weakref
from dataclasses import dataclass, field
from typing import Any

import pytest

import log_foundry
from conftest import run_concurrently
from log_foundry import _lifecycle
from log_foundry._lifecycle import State
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
from test_fork_lifecycle import CHILD_TIMEOUT, run_in_child
from test_sinks_http import FakeResponse

SEEDS = range(16)
THREADS = 4
OPS_PER_THREAD = 60
CLOSE_SECONDS = 0.002
"""How long a ``Recorder.close()`` takes, so two of them can be observed overlapping.

Small enough not to slow the run — the closes are concurrent by design — and large enough that
an overlap is reachable at all: at zero the window is a few bytecodes and the counter measures
nothing. Measured at this value, the defect the counter exists for failed 180 of 601 seeds.
"""

FLUSH_TIMEOUT = 2.0
SHUTDOWN_TIMEOUT = 3.0
FINAL_SHUTDOWN_TIMEOUT = 5.0
BOUND_SLACK = 10.0

_REPO = pathlib.Path(__file__).resolve().parent.parent
_PAGE = _REPO / "docs" / "invariants.md"


class Recorder:
    """A sink that keeps every event, counts closes, and records whether two ever overlapped.

    The overlap counter is not decoration. `invariant 5`'s headline is that a sink is closed
    "**once**, by one owner, and a close in flight is waited for", and the assertion below used
    to read `sink.events and sink.closes == 0` — it could see a sink closed **zero** times and
    nothing else, so a sink closed twice, or closed twice *at the same instant*, passed. That is
    an assertion coarser than the claim it cites. With the counter, `_swap_sink`'s fenced branch
    releasing a sink another caller was already closing failed 180 of 601 seeds against a 2 ms
    `close()` (SPEC-054, second diff review); the fix is one `held` term, and this is what would
    have caught it.

    `close()` takes measurable time for the same reason: an instantaneous close cannot overlap
    with anything, so a zero-cost double is indistinguishable from correct behaviour.

    It keeps accepting after ``close()`` — the ``MemorySink`` half of SPEC-032's rule — because
    the library is *allowed* to write to a closed sink on the orphan path (SPEC-030 accepts
    logging after ``shutdown()``, and SPEC-045 owes such a sink a second close). A refusing double
    would turn that accepted behaviour into ``orphan_lost``, which the identity below also
    reconciles, but a keeping double lets the ledger say where every event went. It declares
    ``log_foundry_stop_signal`` so the stop-signal offer has somewhere to land (SPEC-027 FR-002).
    """

    log_foundry_stop_signal: threading.Event | None = None
    live: weakref.WeakSet[Recorder] = weakref.WeakSet()

    def __init__(self, emit_delay: float) -> None:
        Recorder.live.add(self)
        self.events: list[dict[str, object]] = []
        self.closes = 0
        self.inside = 0
        self.overlapped = 0
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
            self.inside += 1
            if self.inside > 1:
                self.overlapped += 1
        time.sleep(CLOSE_SECONDS)
        with self._lock:
            self.inside -= 1

    def count(self, message: str) -> int:
        with self._lock:
            return sum(1 for event in self.events if event.get("message") == message)

    def forget(self) -> None:
        """Drops the kept events, for :class:`HTTPRecorder`'s reason (:func:`_forget_records`)."""
        with self._lock:
            self.events.clear()


_RAISED = "raised"
_MALFORMED = "malformed"
_OUTCOME_WEIGHTS = (
    (Outcome.ACCEPTED, 0.55),
    (Outcome.RETRYABLE, 0.15),
    (Outcome.REJECTED, 0.08),
    (Outcome.DROPPED, 0.08),
    (Outcome.UNKNOWN, 0.07),
    (Outcome.SKIPPED, 0.07),
)
"""Each event's outcome per round in the ``deliver`` form, drawn per event and round."""

_ROUND_FAULT = 0.06
"""The share of rounds that raise, and again the share that return a malformed verdict."""


class DeliverRecorder(Recorder):
    """The ``deliver`` form of :class:`Recorder`: mixed verdicts, and its own ground truth.

    Every round's outcome per event is drawn from a ``random.Random`` seeded by the test seed,
    the event's ``log_id`` and the round that event is in, so a seed replays exactly; a share of
    rounds raise or return a malformed verdict instead. ``fates`` records the last mark each event
    was given — an ``Outcome``, ``"raised"`` or ``"malformed"`` — which is the engine's final fate
    for it, since the engine offers an event again only while its last mark is ``RETRYABLE``.
    ``accepted`` lists every acceptance, so a second one for an event is visible. It declares
    ``max_retries = 0``: on the worker path that is ``w + 1`` one-attempt cycles at the worker's
    0.01-0.04 s waits, on the orphan path one attempt, which keeps the file's runtime.
    """

    max_retries = 0

    def __init__(self, emit_delay: float, seed: int) -> None:
        super().__init__(emit_delay)
        self.log_foundry_counters = SinkCounters()
        self.seed = seed
        self.fates: dict[str, object] = {}
        self.accepted: list[str] = []
        self.offered: dict[str, int] = {}

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        if self.emit_delay:
            time.sleep(self.emit_delay)
        ids = [str(event.get("log_id")) for event in batch]
        with self._lock:
            rounds = [self.offered.get(log_id, 0) for log_id in ids]
            for log_id, seen, event in zip(ids, rounds, batch, strict=True):
                self.offered[log_id] = seen + 1
                self.events.append(event)
        fault = random.Random(f"{self.seed}:{ids[0]}:{rounds[0]}:round").random()  # noqa: S311
        if fault < _ROUND_FAULT:
            self._mark(ids, _RAISED)
            raise ConnectionError("model-secret")
        if fault < 2 * _ROUND_FAULT:
            self._mark(ids, _MALFORMED)
            return Verdict(outcomes=())  # the wrong length: malformed for any non-empty batch
        outcomes = tuple(
            _draw(random.Random(f"{self.seed}:{log_id}:{seen}"))  # noqa: S311
            for log_id, seen in zip(ids, rounds, strict=True)
        )
        with self._lock:
            for log_id, outcome in zip(ids, outcomes, strict=True):
                self.fates[log_id] = outcome
                if outcome is Outcome.ACCEPTED:
                    self.accepted.append(log_id)
        return Verdict(outcomes=outcomes)

    def _mark(self, ids: list[str], mark: str) -> None:
        with self._lock:
            for log_id in ids:
                self.fates[log_id] = mark

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)


_HTTP_ANSWERS = ((200, 0.7), (503, 0.15), (400, 0.1))
"""Each request's answer in the ``http`` form; the remaining share raises ``RuntimeError``."""


class HTTPRecorder(HTTPSink):
    """The ``http`` form: a shipped family sink behind a scripted opener (SPEC-073 FR-005).

    Each request's answer is drawn from a ``random.Random`` seeded by the test seed, the first
    ``log_id`` the request carried and how many times that event had been offered, so a seed
    replays exactly: a ``200``, a retryable ``503``, a ``400`` refused for good, or an opener
    ``RuntimeError`` the predicate refuses. ``accepted`` lists every event a ``2xx`` carried, so a
    second acceptance is visible; ``fates`` the last answer each event's request had; ``events``
    every event the opener was handed, which the identity reads as "offered". It declares
    ``max_retries=0``, as :class:`DeliverRecorder` does, to keep the file's runtime. It keeps
    :class:`Recorder`'s close counting for invariant 5, and defines no ``emit``, so the worker
    and the orphan path deliver to it through ``deliver``, as to any shipped family sink.

    **Its records are dropped when the test ends** (:meth:`forget`, from :func:`_forget_records`).
    A sink handed to the library stays in ``_lifecycle._state._owned`` for the life of the process, and
    a forked child's marking walk visits the objects that record reaches, up to a cap of 100,000. Sixteen seeds of decoded events kept here added about 140,000 objects to
    that walk in the pytest worker that ran them, measured, enough to trip the cap in
    ``tests/test_fork_lifecycle.py`` when it ran later in the same worker.
    """

    live: weakref.WeakSet[HTTPRecorder] = weakref.WeakSet()

    def __init__(self, emit_delay: float, seed: int) -> None:
        super().__init__("http://model", max_retries=0, max_batch_count=3, opener=self._answer)
        HTTPRecorder.live.add(self)
        self.emit_delay = emit_delay
        self.seed = seed
        self.events: list[dict[str, object]] = []
        self.accepted: list[str] = []
        self.fates: dict[str, object] = {}
        self.offered: dict[str, int] = {}
        self.closes = 0
        self.inside = 0
        self.overlapped = 0
        self._lock = threading.Lock()

    def _answer(self, request: Any, timeout: float | None = None) -> FakeResponse:
        if self.emit_delay:
            time.sleep(self.emit_delay)
        batch = [json.loads(line) for line in request.data.decode().splitlines() if line]
        ids = [str(event.get("log_id")) for event in batch]
        with self._lock:
            seen = self.offered.get(ids[0], 0)
            for log_id, event in zip(ids, batch, strict=True):
                self.offered[log_id] = self.offered.get(log_id, 0) + 1
                self.events.append(event)
        roll = random.Random(f"{self.seed}:{ids[0]}:{seen}:http").random()  # noqa: S311
        status = 0
        for code, weight in _HTTP_ANSWERS:
            if roll < weight:
                status = code
                break
            roll -= weight
        with self._lock:
            for log_id in ids:
                self.fates[log_id] = status
            if status == 200:
                self.accepted.extend(ids)
        if not status:
            raise RuntimeError("model-secret")
        return FakeResponse(status, b"")

    def close(self) -> None:
        with self._lock:
            self.closes += 1
            self.inside += 1
            if self.inside > 1:
                self.overlapped += 1
        time.sleep(CLOSE_SECONDS)
        with self._lock:
            self.inside -= 1

    def count(self, message: str) -> int:
        with self._lock:
            return sum(1 for event in self.events if event.get("message") == message)

    def forget(self) -> None:
        """Drops every record, so nothing this recorder saw outlives the test that read it."""
        with self._lock:
            self.events.clear()
            self.accepted.clear()
            self.fates.clear()
            self.offered.clear()


@pytest.fixture(autouse=True)
def _forget_records() -> Any:
    """Clears every recorder's records once its test has asserted on them.

    A recorder handed to the library stays in ``_lifecycle._state._owned`` for the life of the
    process, and a later fork test's marking walk visits the objects that record reaches, up to
    ``_MARKING_CEILING``. :class:`HTTPRecorder` measured that hazard; the sequential driver hands
    the library a recorder per ``configure`` and per default resolution, so both kinds are cleared.
    """
    yield
    for recorder in list(HTTPRecorder.live):
        recorder.forget()
    for kept in list(Recorder.live):
        kept.forget()


def _draw(rng: random.Random) -> Outcome:
    """One outcome by :data:`_OUTCOME_WEIGHTS`."""
    roll = rng.random()
    for outcome, weight in _OUTCOME_WEIGHTS:
        if roll < weight:
            return outcome
        roll -= weight
    return Outcome.ACCEPTED


@dataclass
class Ledger:
    """What the actors did, counted where they did it, so the identity has a left-hand side."""

    spans: int = 0
    orphans: int = 0
    slowest_flush: float = 0.0
    slowest_shutdown: float = 0.0
    sinks: list[Recorder] = field(default_factory=list)
    form: str = "emit"
    seed: int = 0
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
            sink = _recorder(ledger, rng.choice([0.0, 0.0, 0.002]))
            ledger.add_sink(sink)
            log_foundry.configure(service="model", sink=sink)
        else:
            started = time.monotonic()
            log_foundry.shutdown(timeout=SHUTDOWN_TIMEOUT)
            ledger.timed("slowest_shutdown", time.monotonic() - started)


def _recorder(ledger: Ledger, emit_delay: float) -> Recorder:
    """A recorder of the ledger's form."""
    if ledger.form == "deliver":
        return DeliverRecorder(emit_delay, ledger.seed)
    if ledger.form == "http":
        return HTTPRecorder(emit_delay, ledger.seed)  # type: ignore[return-value]
    return Recorder(emit_delay)


@dataclass
class _Runs:
    """Every ``run_delivery`` call: its batch's ids, its path, its result and the worker."""

    calls: list[tuple[list[str], str, _engine.Delivery]] = field(default_factory=list)
    workers: dict[int, object] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)


def _spy_on_runs(monkeypatch: pytest.MonkeyPatch) -> _Runs:
    """Wraps ``_engine.run_delivery`` so each call's batch, path and ``Delivery`` are recorded."""
    runs = _Runs()
    real = _engine.run_delivery

    def spy(sink: object, batch: list[dict[str, object]], **kwargs: object) -> object:
        ids = [str(event.get("log_id")) for event in batch]
        path = "orphan" if kwargs.get("worker_retries") is None else "worker"
        result = real(sink, batch, **kwargs)  # type: ignore[arg-type]
        if result is not None:
            with runs._lock:
                runs.calls.append((ids, path, result))
                owner = getattr(kwargs.get("decline"), "__self__", None)
                if owner is not None:
                    runs.workers[id(owner)] = owner
        return result

    monkeypatch.setattr(_engine, "run_delivery", spy)
    return runs


class _StateSampler:
    """Reads ``Runtime.state`` in a loop on its own thread while the actors run.

    Under concurrency the state is not determined, so the only claim is that the observer always
    answers a ``State`` and never raises (SPEC-077 FR-004).
    """

    def __init__(self) -> None:
        self.reads = 0
        self.wrong: list[object] = []
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._run, name="state-sampler", daemon=True)

    def _run(self) -> None:
        while not self._done.is_set():
            try:
                read = _lifecycle._state.state
            except BaseException as exc:
                self.wrong.append(exc)
                return
            if not isinstance(read, State):
                self.wrong.append(read)
            self.reads += 1
            self._done.wait(0.001)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._done.set()
        self._thread.join(BOUND_SLACK)


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


@pytest.mark.parametrize("form", ["emit", "deliver", "http"])
@pytest.mark.parametrize("seed", SEEDS)
def test_the_accounting_identity_holds_under_every_interleaving(
    seed: int,
    form: str,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    """Invariants 1, 2, 3, 5 and 6 of ``docs/invariants.md``, over one seeded interleaving.

    The identity is the whole test: every span the actors started is either delivered as a
    ``span.end``, still queued where a ``shutdown()`` left it, or counted as dropped; every
    orphan is delivered or counted in ``orphan_lost``; nothing is missing and nothing is counted
    twice. A regression in the lifecycle plumbing shows up here as a seed whose ledger does not
    balance, before anyone has written the test that names it.

    The ``deliver`` form (SPEC-072 FR-005) runs the same seeds with :class:`DeliverRecorder`,
    whose mixed verdicts make "delivered" a per-event fate, and holds each counted term to its
    exact share (:func:`_assert_the_deliver_ledger`). The ``http`` form (SPEC-073 FR-005) runs
    them with :class:`HTTPRecorder`, a shipped family sink behind a scripted opener
    (:func:`_assert_the_http_ledger`).
    """
    ledger = Ledger(form=form, seed=seed)
    runs = _spy_on_runs(monkeypatch) if form != "emit" else None
    if form == "http":
        monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)
    first = _recorder(ledger, random.Random(seed).choice([0.0, 0.0, 0.002]))  # noqa: S311
    ledger.add_sink(first)
    log_foundry.configure(service="model", sink=first)

    def work(index: int, _iteration: int) -> None:
        _actor(ledger, random.Random(seed * 10 + index), shutdown_in_mix=seed % 2 == 0)  # noqa: S311

    sampler = _StateSampler()
    sampler.start()
    try:
        escaped = run_concurrently(work, threads=THREADS)
    finally:
        sampler.stop()
    log_foundry.shutdown(timeout=FINAL_SHUTDOWN_TIMEOUT)
    final_state = _lifecycle._state.state
    tokens_left = len(_lifecycle._state._swaps_running) + len(_lifecycle._state._stops_running)
    health = log_foundry.health()
    markers = _stranded_markers()
    queued = health.queued - markers

    ends = sum(sink.count("span.end") for sink in ledger.sinks)
    orphans = sum(sink.count("orphan") for sink in ledger.sinks)
    if runs is not None:
        # In the deliver form an event offered to the sink has a fate rather than a delivery: a
        # span.end offered once is out of the queue whatever its fate (a failed worker run is
        # failed_batches, held below), and an orphan whose run failed is the orphan_lost term,
        # so "delivered" is the orphans whose run settled.
        ends = len({e for s in ledger.sinks for e in _ids(s, "span.end")})
        failed_orphans = sum(1 for _, path, d in runs.calls if path == "orphan" and d.failed)
        orphans = len({e for s in ledger.sinks for e in _ids(s, "orphan")}) - failed_orphans
    unclosed = [i for i, sink in enumerate(ledger.sinks) if sink.events and sink.closes == 0]
    overlapped = [i for i, sink in enumerate(ledger.sinks) if sink.overlapped]
    picture = (
        f"seed={seed} spans={ledger.spans} span.end={ends} queued={health.queued} "
        f"markers={markers} dropped={health.dropped} in_span_lost={health.in_span_lost} | "
        f"orphans={ledger.orphans} delivered={orphans} orphan_lost={health.orphan_lost} | "
        f"failed_batches={health.failed_batches} incomplete_swaps={health.incomplete_swaps} "
        f"stopped_reason={health.stopped_reason} after_shutdown={health.submitted_after_shutdown} "
        f"sinks={len(ledger.sinks)} "
        f"unclosed={unclosed} overlapped={overlapped} escaped={escaped!r}"
    )

    assert not escaped, f"invariant 1 (never fail the caller): {picture}"
    assert not sampler.wrong, f"invariant 1 (state always answers a State): {sampler.wrong[:3]}"
    assert sampler.reads, "the sampler never read the state, so it checked nothing"
    assert final_state is State.CLOSED, f"after the final shutdown: {final_state} {picture}"
    assert tokens_left == 0, f"a token outlived its call: {tokens_left} {picture}"
    assert health.in_span_lost == 0, f"nothing here can fail assembly: {picture}"
    if runs is None:
        assert health.failed_batches == 0, f"the recorder never raises: {picture}"
    elif form == "http":
        _assert_the_http_ledger(ledger, runs, health.orphan_lost, capfd.readouterr().err, picture)
    else:
        _assert_the_deliver_ledger(
            ledger, runs, health.orphan_lost, capfd.readouterr().err, picture
        )
    assert ends + queued + health.dropped == ledger.spans, f"invariant 2 (spans): {picture}"
    assert orphans + health.orphan_lost == ledger.orphans, f"invariant 2 (orphans): {picture}"
    assert health.retired, f"invariant 2 (a shutdown ran, so retired reports it): {picture}"
    assert health.submitted_after_shutdown >= queued, (
        f"invariant 2 (what a shutdown left queued was counted as submitted after it): {picture}"
    )
    assert not unclosed, f"invariant 5 (every sink written to is closed): {picture}"
    assert not overlapped, (
        f"invariant 5 (closed **once**, and a close in flight is waited for): {picture}"
    )
    assert ledger.slowest_flush < FLUSH_TIMEOUT + BOUND_SLACK, f"invariant 3 (flush): {picture}"
    assert ledger.slowest_shutdown < SHUTDOWN_TIMEOUT + BOUND_SLACK, (
        f"invariant 3 (shutdown): {picture}"
    )


# -- the sequential driver (SPEC-077 FR-004) ----------------------------------------------------

SEQ_SEEDS = range(24)
SEQ_STEPS = 40
STEP_WEIGHTS = (
    ("configure", 0.17),
    ("info", 0.18),
    ("span", 0.2),
    ("flush", 0.1),
    ("shutdown", 0.12),
    ("exit", 0.1),
    ("fork", 0.13),
)
"""How often each step is drawn. The fork is weighted up because it must be reached from each of
the three resting states, ``UNCONFIGURED`` included, which only the first few steps of a seed
are in; :func:`test_the_seed_range_covers_every_cell` is what says the weights are enough."""

_U, _R, _C = State.UNCONFIGURED, State.RUNNING, State.CLOSED

TABLE: dict[tuple[str, State, str], tuple[State, State | None]] = {
    ("configure", _U, "no-swap-work"): (_R, None),
    ("configure", _R, "swap"): (_R, State.SWAPPING),
    ("configure", _R, "direct"): (_R, None),
    ("configure", _C, "swap"): (_C, State.SWAPPING),
    ("configure", _C, "direct"): (_C, None),
    ("info", _U, ""): (_R, None),
    ("info", _R, ""): (_R, None),
    ("info", _C, ""): (_C, None),
    ("span", _U, "build"): (_R, None),
    ("span", _R, "build"): (_R, None),
    ("span", _R, "existing"): (_R, None),
    ("span", _C, "build"): (_R, None),
    ("span", _C, "retired-worker"): (_C, None),
    ("flush", _U, ""): (_U, None),
    ("flush", _R, ""): (_R, None),
    ("flush", _C, ""): (_C, None),
    ("shutdown", _U, ""): (_C, State.DRAINING),
    ("shutdown", _R, ""): (_C, State.DRAINING),
    ("shutdown", _C, ""): (_C, State.DRAINING),
    ("exit", _U, ""): (_C, State.DRAINING),
    ("exit", _R, ""): (_C, State.DRAINING),
    ("exit", _C, ""): (_C, State.DRAINING),
    ("fork", _U, ""): (_U, None),
    ("fork", _R, ""): (_R, None),
    ("fork", _C, ""): (_C, None),
}
"""The state table: (step, state before, which variant the facts select) to (state after, the
transient state the step passes through). Today's behaviour, not a proposal (SPEC-077 FR-004);
for a fork, "after" is the **child's** initial state and the parent's is unchanged. ``CLOSED`` is
not terminal — a span after a no-worker shutdown builds a live worker — and SPEC-078 decides which
of these transitions stay."""

ROWS = {(before, after) for (_, before, _), (after, _) in TABLE.items()}


@dataclass
class _Model:
    """The four facts the table is keyed by, plus the retirement the state is read from.

    ``owed`` is set by an orphan ``info()``, a worker's build and a swap that does work, and cleared
    by a shutdown; it is what decides whether a ``configure(sink=…)`` has work, since ``_swap_sink``
    returns early with no live worker and nothing owed.
    """

    resolved: bool = False
    owed: bool = False
    worker: bool = False
    live: bool = False
    retired: bool = False

    def state(self) -> State:
        if self.live:
            return _R
        if self.retired:
            return _C
        if self.owed or self.resolved:
            return _R
        return _U

    def cell(self, step: str) -> tuple[str, State, str]:
        """Which table cell a step taken now lands in."""
        before = self.state()
        variant = ""
        if step == "configure":
            variant = (
                "no-swap-work" if before is _U else ("swap" if self.owed or self.live else "direct")
            )
        elif step == "span":
            variant = (
                "build" if not self.worker else ("existing" if self.live else "retired-worker")
            )
        return (step, before, variant)

    def take(self, step: str) -> tuple[str, State, str]:
        """Applies a step to the facts and returns the cell it was taken in."""
        cell = self.cell(step)
        if step == "configure":
            self.resolved = True
            self.owed = self.owed or cell[2] == "swap"
        elif step == "info":
            self.resolved = self.owed = True
        elif step == "span":
            self.resolved = True
            if cell[2] == "build":
                self.worker = self.live = self.owed = True
        elif step in {"shutdown", "exit"}:
            self.owed = self.live = False
            self.retired = True
        after = self.state() if step != "fork" else TABLE[cell][0]
        assert after is TABLE[cell][0], f"the model and its own table disagree at {cell}"
        return cell


def _draws(seed: int) -> list[tuple[str, bool]]:
    """The seed's steps, each with whether a span nests; drawn independently of any state."""
    rng = random.Random(f"sequential:{seed}")  # noqa: S311
    names = [name for name, _ in STEP_WEIGHTS]
    weights = [weight for _, weight in STEP_WEIGHTS]
    return [(rng.choices(names, weights)[0], rng.random() < 0.3) for _ in range(SEQ_STEPS)]


def test_the_seed_range_covers_every_cell() -> None:
    """FR-004 AC-2: over the seed range every cell of the table is visited.

    Pure — no library call — because the draws do not depend on the library's state and every live
    seed asserts, step by step, that the library is where this model says it is. So the cells the
    model visits are the cells the live runs visit, and the coverage is asserted here once rather
    than summed across ``-n 12`` workers.
    """
    visited: set[tuple[str, State, str]] = set()
    for seed in SEQ_SEEDS:
        model = _Model()
        for step, _ in _draws(seed):
            visited.add(model.take(step))
    missing = sorted(set(TABLE) - visited, key=str)
    assert not missing, f"no seed reaches these cells; widen SEQ_SEEDS or the weights: {missing}"
    assert visited <= set(TABLE), (
        f"the model reached a cell the table lacks: {visited - set(TABLE)}"
    )


_BRACKET: list[str | None] = [None]
"""The step the driver is inside, set before each call and cleared after it returns."""

_SAMPLES: list[tuple[str | None, str, int, bool, State]] = []
"""(bracket, close or emit, thread, whether ``_stop`` was set, the state read) per recorder call."""


class StateRecorder(Recorder):
    """A :class:`Recorder` that samples the state in its own ``close()`` and ``emit()``.

    It reads ``_stop`` **before** ``state``: ``_shutdown_worker`` adds its token before it sets the
    event, in one critical section, so a sample that saw the event set was taken after the token.
    It also notes the thread each event arrived on, for invariant 4.
    """

    def __init__(self) -> None:
        super().__init__(0.0)
        self.threads: list[tuple[int, object]] = []

    def emit(self, batch: list[dict[str, object]]) -> None:
        _sample("emit")
        with self._lock:
            self.threads.extend((threading.get_ident(), e.get("message")) for e in batch)
        super().emit(batch)

    def close(self) -> None:
        _sample("close")
        super().close()

    def forget(self) -> None:
        super().forget()
        with self._lock:
            self.threads.clear()


def _sample(kind: str) -> None:
    """Records what the state read at this instant of a recorder call."""
    stop = _lifecycle._state._stop.is_set()
    _SAMPLES.append((_BRACKET[0], kind, threading.get_ident(), stop, _lifecycle._state.state))


@dataclass
class _Run:
    """One seed's ledger and history."""

    seed: int
    sinks: list[StateRecorder] = field(default_factory=list)
    spans: int = 0
    orphans: int = 0
    drains: set[int] = field(default_factory=set)
    history: list[str] = field(default_factory=list)

    def recorder(self) -> StateRecorder:
        sink = StateRecorder()
        self.sinks.append(sink)
        return sink

    def where(self, index: int) -> str:
        return f"seed={self.seed} step={index} history={self.history}"


def _budget(step: str) -> float:
    """The budget a step is bounded by, before the slack (invariant 3)."""
    return {
        "configure": _lifecycle.DEFAULT_SWAP_TIMEOUT,
        "flush": FLUSH_TIMEOUT,
        "shutdown": SHUTDOWN_TIMEOUT,
        "exit": _lifecycle.DEFAULT_SHUTDOWN_TIMEOUT,
        "fork": float(CHILD_TIMEOUT),
    }.get(step, 0.0)


def _fork_step(run: _Run, expected: State, where: str) -> None:
    """Forks at a step boundary and checks the child's half of invariant 12 and the parent's."""
    before = [(len(s.threads), s.closes) for s in run.sinks]
    parent_state = _lifecycle._state.state
    inherited = len(run.sinks)

    def child() -> str:
        closes = [s.closes for s in run.sinks]
        initial = _lifecycle._state.state.name
        started = time.monotonic()
        log_foundry.info("child-orphan")
        log_foundry.flush(timeout=FLUSH_TIMEOUT)
        log_foundry.shutdown(timeout=SHUTDOWN_TIMEOUT)
        elapsed = time.monotonic() - started
        return json.dumps(
            {
                "initial": initial,
                "elapsed": elapsed,
                "in_inherited": sum(s.count("child-orphan") for s in run.sinks[:inherited]),
                "in_own": sum(s.count("child-orphan") for s in run.sinks[inherited:]),
                "closed_inherited": [
                    s.closes - c for s, c in zip(run.sinks[:inherited], closes, strict=True)
                ],
            }
        )

    reaped = run_in_child(child)
    assert reaped.finished, f"invariant 12 (the child finished): {reaped.output} {where}"
    report = json.loads(reaped.output)
    assert report["initial"] == expected.name, f"invariant 12 (child state): {report} {where}"
    assert report["elapsed"] < FLUSH_TIMEOUT + SHUTDOWN_TIMEOUT + BOUND_SLACK, (
        f"invariant 12 (the child's three calls returned within a bound): {report} {where}"
    )
    assert report["in_inherited"] + report["in_own"] == 1, (
        f"invariant 12 (the child's event landed in a child recorder): {report} {where}"
    )
    assert not any(report["closed_inherited"]), (
        f"invariant 5 (no child closed a sink it inherited): {report} {where}"
    )
    assert _lifecycle._state.state is parent_state, f"invariant 12 (parent state) {where}"
    driver = threading.get_ident()
    for sink, (seen, closes) in zip(run.sinks, before, strict=False):
        assert sink.closes == closes, f"invariant 12 (the fork closed no parent sink) {where}"
        arrived = sink.threads[seen:]
        assert all(thread != driver and message != "child-orphan" for thread, message in arrived), (
            f"invariant 12 (the parent's ledger moved only by its own drain thread, which keeps "
            f"delivering spans queued before the fork while the child runs): {arrived} {where}"
        )


def _take(run: _Run, step: str, nested: bool, label: str) -> object:
    """Performs one step on the driver's thread, inside its bracket."""
    _BRACKET[0] = label
    try:
        if step == "configure":
            log_foundry.configure(service="model", sink=run.recorder())
        elif step == "info":
            log_foundry.info("orphan")
            run.orphans += 1
        elif step == "span":
            _outer(nested)
            run.spans += 2 if nested else 1
        elif step == "flush":
            return log_foundry.flush(timeout=FLUSH_TIMEOUT)
        elif step == "shutdown":
            log_foundry.shutdown(timeout=SHUTDOWN_TIMEOUT)
        elif step == "exit":
            _lifecycle._shutdown_worker()
        return None
    finally:
        _BRACKET[0] = None


def _check_samples(label: str, step: str, driver: int, where: str) -> None:
    """The transient states, asserted only where they have provably begun."""
    for bracket, kind, thread, stop, read in _SAMPLES:
        if bracket != label:
            continue
        if kind == "close" and step == "configure":
            assert read is State.SWAPPING, f"a close inside a swap read {read} {where}"
        if kind == "close" and step in {"shutdown", "exit"}:
            assert read is State.DRAINING, f"a close inside a shutdown read {read} {where}"
        if kind == "emit" and step in {"shutdown", "exit"} and stop and thread != driver:
            assert read is State.DRAINING, f"a drain emit after the stop read {read} {where}"


_TRANSIENTS = frozenset({State.SWAPPING, State.DRAINING})

UNSAMPLED = frozenset(
    {
        ("shutdown", _U, ""),
        ("exit", _U, ""),
        ("configure", _U, "no-swap-work"),
        ("configure", _R, "direct"),
        ("configure", _C, "direct"),
    }
)
"""Cells whose transient the driver cannot observe, so the samples do not assert their column.

From ``UNCONFIGURED`` a shutdown has no sink to close and no drain thread to deliver, and a
``configure(sink=…)`` with no swap work closes nothing and runs no drain, so no recorder runs
inside any of them and nothing samples the state. The driver asserts only that such a step samples
nothing, so a change that made one observable has to revisit this set rather than pass unseen. Their
column is held instead by :func:`test_the_table_transients_follow_the_spec_rules`, and the tokens
behind it by ``tests/test_runtime_state.py``."""


def test_the_table_transients_follow_the_spec_rules() -> None:
    """The transient column states SPEC-077 FR-002's rules, including for cells no recorder sees.

    A ``configure(sink=…)`` passes through ``SWAPPING`` exactly when ``_swap_sink`` has work — the
    early return for no live worker and nothing owed comes before the token — a shutdown or the exit
    drain always passes through ``DRAINING``, and no other step passes through either.
    """
    for (step, _, variant), (_, transient) in TABLE.items():
        if step == "configure":
            expected = State.SWAPPING if variant == "swap" else None
        elif step in {"shutdown", "exit"}:
            expected = State.DRAINING
        else:
            expected = None
        assert transient is expected, f"({step}, {variant}) reads {transient}, not {expected}"


def _check_transient(
    cell: tuple[str, State, str], label: str, transient: State | None, where: str
) -> None:
    """The table's transient column, against what the recorders sampled inside the step.

    A cell with no transient must sample neither ``SWAPPING`` nor ``DRAINING``; a cell that passes
    through one must sample it at least once whenever its bracket sampled anything. A bracket can
    sample nothing — a swap or a shutdown of a ``CLOSED`` process with nothing owed — and then
    the column is not decidable for that step, so it is not asserted there; a cell that can never
    sample is in :data:`UNSAMPLED`.
    """
    reads = [read for bracket, _, _, _, read in _SAMPLES if bracket == label]
    if cell in UNSAMPLED:
        assert not reads, f"{cell} is listed as unobservable but sampled {reads} {where}"
        return
    if transient is None:
        seen = sorted({r.name for r in reads if r in _TRANSIENTS})
        assert not seen, f"a cell with no transient state sampled {seen} {where}"
    elif reads:
        assert transient in reads, (
            f"a cell through {transient.name} sampled only {sorted({r.name for r in reads})} {where}"
        )


def _check_invariants(run: _Run, step: str, result: object, driver: int, where: str) -> None:
    """Invariants 2, 4 and 5 after a step, each where it is decidable."""
    health = log_foundry.health()
    delivered_orphans = sum(s.count("orphan") for s in run.sinks)
    assert delivered_orphans + health.orphan_lost == run.orphans, (
        f"invariant 2 (orphans): delivered={delivered_orphans} lost={health.orphan_lost} "
        f"called={run.orphans} {where}"
    )
    worker = _lifecycle._state.worker_exists()
    if worker is not None and worker._thread.ident is not None:
        run.drains.add(worker._thread.ident)
    for sink in run.sinks:
        for thread, message in sink.threads:
            if message == "span.end":
                assert thread in run.drains and thread != driver, (
                    f"invariant 4 (a span.end arrived off the drain thread) {where}"
                )
            elif message == "orphan":
                assert thread == driver, f"invariant 4 (an orphan left the caller's thread) {where}"
    if (step == "flush" and result) or step in {"shutdown", "exit"}:
        ends = sum(s.count("span.end") for s in run.sinks)
        queued = health.queued - _stranded_markers()
        assert ends + queued + health.dropped == run.spans, (
            f"invariant 2 (spans): span.end={ends} queued={queued} dropped={health.dropped} "
            f"started={run.spans} {where}"
        )
    if step in {"shutdown", "exit"}:
        unclosed = [i for i, s in enumerate(run.sinks) if s.events and s.closes == 0]
        overlapped = [i for i, s in enumerate(run.sinks) if s.overlapped]
        assert not unclosed, f"invariant 5 (every sink written to is closed): {unclosed} {where}"
        assert not _lifecycle._state._owed, f"invariant 5 (nothing owed after a shutdown) {where}"
        assert not overlapped, f"invariant 5 (closed once, never overlapping): {overlapped} {where}"


@pytest.mark.parametrize("seed", SEQ_SEEDS)
def test_the_sequential_driver_holds_the_state_table(
    seed: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-004: every step against the table, and invariants 1-6 and 12 after each.

    After every step the state is the table's, both token sets are empty, the (before, after) pair
    is a row, and the two facts the table is keyed by that the runtime holds — something owed, a
    live worker — are read back rather than trusted to the model.
    """
    run = _Run(seed)
    monkeypatch.setattr("log_foundry.sinks.stdout.StdoutSink", run.recorder)
    _SAMPLES.clear()
    driver = threading.get_ident()
    model = _Model()
    for index, (step, nested) in enumerate(_draws(seed)):
        cell = model.cell(step)
        expected, transient = TABLE[cell]
        label = f"{index}:{step}"
        run.history.append(f"{step}@{cell[1].name}")
        where = run.where(index)
        if step == "fork":
            _fork_step(run, expected, where)
            model.take(step)
            continue
        started = time.monotonic()
        try:
            result = _take(run, step, nested, label)
        except BaseException as exc:
            pytest.fail(f"invariant 1 (never fail the caller): {step} raised {exc!r} {where}")
        elapsed = time.monotonic() - started
        model.take(step)
        state = _lifecycle._state.state
        assert state is expected, f"state {state.name} is not the table's {expected.name} {where}"
        assert (cell[1], state) in ROWS, f"({cell[1].name}, {state.name}) is no row {where}"
        assert not _lifecycle._state._swaps_running and not _lifecycle._state._stops_running, (
            f"a token outlived its call {where}"
        )
        assert bool(_lifecycle._state._owed) == model.owed, f"owed is not the model's {where}"
        assert (_lifecycle._state.live_worker() is not None) == model.live, (
            f"liveness is not the model's {where}"
        )
        bound = _budget(step) + BOUND_SLACK
        assert elapsed < bound, f"invariant 3 (bounded): {step} took {elapsed:.2f}s {where}"
        _check_samples(label, step, driver, where)
        _check_transient(cell, label, transient, where)
        _check_invariants(run, step, result, driver, where)


def _ids(sink: Recorder, message: str) -> list[str]:
    """The ``log_id`` of every event of one message a recorder was offered."""
    with sink._lock:
        return [str(e.get("log_id")) for e in sink.events if e.get("message") == message]


_WORKER_LOST = re.compile(
    r"^log-foundry: lost (\d+) event\(s\); (?:batch abandoned after \d+ emit attempts|"
    r"retry declined after \d+ emit attempt\(s\); the next was predicted)",
    re.MULTILINE,
)
_ORPHAN_LOST = re.compile(
    r"^log-foundry: lost 1 event\(s\); orphan log not delivered after \d+ delivery round",
    re.MULTILINE,
)


def _assert_the_deliver_ledger(
    ledger: Ledger, runs: _Runs, orphan_lost: int, err: str, picture: str
) -> None:
    """Holds each counted term of the ``deliver`` form to its exact share of the recorders' truth.

    The recorders know every event's final fate; the spy knows which run it was in and whether
    that run failed. From the two: no event accepted twice; ``dropped_events`` is exactly the
    events whose fate was ``DROPPED`` in a settled run; ``failed_events`` is exactly the events
    neither accepted, ``DROPPED`` nor ``SKIPPED``, in any run; every failed worker-path run is one
    ``failed_batches`` and one lost line naming its batch's size; every failed orphan run is one
    ``orphan_lost``; and every event offered has exactly one final fate.
    """
    recorders = [s for s in ledger.sinks if isinstance(s, DeliverRecorder)]
    assert runs.calls and recorders, f"the spy saw no engine run, so nothing is held: {picture}"
    accepted = [log_id for s in recorders for log_id in s.accepted]
    assert len(accepted) == len(set(accepted)), f"invariant 2 (accepted twice): {picture}"
    fates: dict[str, object] = {}
    for sink in recorders:
        fates.update(sink.fates)
    failed_run: dict[str, bool] = {}
    for ids, _path, delivery in runs.calls:
        for log_id in ids:
            failed_run[log_id] = delivery.failed
    offered = {log_id for s in recorders for log_id in s.offered}
    assert offered == set(fates) == set(failed_run), f"invariant 2 (one fate each): {picture}"
    dropped = sum(s.log_foundry_counters.losses().dropped for s in recorders)
    failed = sum(s.log_foundry_counters.losses().failed for s in recorders)
    assert dropped == sum(
        1 for log_id, fate in fates.items() if fate is Outcome.DROPPED and not failed_run[log_id]
    ), f"invariant 2 (dropped_events is exact): {picture}"
    unconfirmed = (Outcome.ACCEPTED, Outcome.DROPPED, Outcome.SKIPPED)
    assert failed == sum(
        1 for fate in fates.values() if not any(fate is ok for ok in unconfirmed)
    ), f"invariant 2 (failed_events is each unconfirmed event once): {picture}"
    worker_failed = [len(ids) for ids, path, d in runs.calls if path == "worker" and d.failed]
    orphan_failed = sum(1 for _ids_, path, d in runs.calls if path == "orphan" and d.failed)
    batches = sum(getattr(w, "failed_batches", 0) for w in runs.workers.values())
    assert batches == len(worker_failed), f"invariant 6 (one failed batch per run): {picture}"
    assert sorted(int(n) for n in _WORKER_LOST.findall(err)) == sorted(worker_failed), (
        f"invariant 2 (a failed run's events are in its lost line): {picture}"
    )
    assert orphan_lost == orphan_failed == len(_ORPHAN_LOST.findall(err)), (
        f"invariant 2 (a failed orphan run is one orphan_lost): {picture}"
    )


def _assert_the_http_ledger(
    ledger: Ledger, runs: _Runs, orphan_lost: int, err: str, picture: str
) -> None:
    """Holds the ``http`` form: nothing accepted twice, every event accepted or counted once.

    The opener knows every request's answer; the spy knows which run each event was in and
    whether it failed. No event is in two ``2xx`` bodies; ``failed_events`` is exactly the events
    whose last request was not a ``2xx``, each once however many rounds offered it; nothing is
    dropped; and the catchers' terms match the failed runs, as in the ``deliver`` form.
    """
    recorders = [s for s in ledger.sinks if isinstance(s, HTTPRecorder)]
    assert runs.calls and recorders, f"the spy saw no engine run, so nothing is held: {picture}"
    accepted = [log_id for s in recorders for log_id in s.accepted]
    assert len(accepted) == len(set(accepted)), f"invariant 2 (accepted twice): {picture}"
    fates: dict[str, object] = {}
    for sink in recorders:
        fates.update(sink.fates)
    failed_run = {log_id: d.failed for ids, _path, d in runs.calls for log_id in ids}
    assert set(fates) == set(failed_run), f"invariant 2 (one fate each): {picture}"
    assert set(accepted) == {log_id for log_id, fate in fates.items() if fate == 200}, (
        f"invariant 2 (accepted exactly when its last request was a 2xx): {picture}"
    )
    counted = [s.log_foundry_counters.losses() for s in recorders]
    assert sum(c.dropped for c in counted) == 0, f"nothing here is dropped: {picture}"
    assert sum(c.failed for c in counted) == sum(1 for fate in fates.values() if fate != 200), (
        f"invariant 2 (failed_events is each unconfirmed event once): {picture}"
    )
    worker_failed = [len(ids) for ids, path, d in runs.calls if path == "worker" and d.failed]
    orphan_failed = sum(1 for _ids_, path, d in runs.calls if path == "orphan" and d.failed)
    batches = sum(getattr(w, "failed_batches", 0) for w in runs.workers.values())
    assert batches == len(worker_failed), f"invariant 6 (one failed batch per run): {picture}"
    assert sorted(int(n) for n in _WORKER_LOST.findall(err)) == sorted(worker_failed), (
        f"invariant 2 (a failed run's events are in its lost line): {picture}"
    )
    assert orphan_lost == orphan_failed == len(_ORPHAN_LOST.findall(err)), (
        f"invariant 2 (a failed orphan run is one orphan_lost): {picture}"
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
