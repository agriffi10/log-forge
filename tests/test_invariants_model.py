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
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
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

    def __init__(self, emit_delay: float) -> None:
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
    """Clears every :class:`HTTPRecorder`'s records once its test has asserted on them."""
    yield
    for recorder in list(HTTPRecorder.live):
        recorder.forget()


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

    escaped = run_concurrently(work, threads=THREADS)
    log_foundry.shutdown(timeout=FINAL_SHUTDOWN_TIMEOUT)
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
