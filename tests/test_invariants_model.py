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

**Since SPEC-078 the settled state is stored**, in ``Runtime._current``, and the driver also checks
it after every step against :func:`derived_current` — rules 3 to 6 without the configured-sink
term, written here and evaluated over the runtime's own fields, with liveness computed from the
worker's epoch directly rather than through ``live_worker()``, so the oracle shares no expression
with the code it checks (SPEC-078 FR-003 AC-2). ``state`` reports that stored value, so the
invariants below are now checked against the stored state. Deleting any of the three ``_move``
calls this driver reaches reddens it: the build's and the orphan arm's only here, since ``state``
already reports ``RUNNING`` from the config.

**What the driver checks, by invariant**: 1 (no step raises, ``BaseException`` included); 2
(orphans balance after every step, spans after every truthy ``flush()`` and every shutdown); 3,
the wall-clock half (every step, the fork included, and each of a forked child's three calls,
returns within its own timeout plus ``model_oracles.EPSILON``, one second, with a budget of zero
for an ``info()`` and a traced call); 4 (every ``span.end`` arrives on a drain thread and every
orphan on the caller's); 5 (after each shutdown every recorder written to is closed, nothing is
owed, no two closes overlapped, and no child closed an inherited sink; after **every** step no
close of a sink enters at the write count of the close before it, and after every shutdown and
exit drain no sink was written after its last close returned); 6 by construction, since one
sequence drives both delivery paths and both drains; and 12, the child's half (its state, its
three calls, its event in a child recorder) and the parent's ledger unchanged by the fork.

**The close count** (SPEC-086 FR-001). Every recorder counts the delivery calls that enter it at
the first statement of the method the library calls (``emit``, or ``deliver`` for the ``deliver``
and ``http`` forms), and records that count at each ``close``'s entry and return. Counted any
later, a delivery already inside the sink when a close begins would read as a write after it. With
one caller nothing can arm a sink between the take of a close and the close, so a second close at
one count is a double close and a write after the last close returned is a write nobody owes:
the sink's own record is exact. Under four callers neither holds, because a writer can arm the
sink again between two takes (invariant 5: "a sink written to after its close is owed a second
close"), so the concurrent test keeps its two checks, never closed and overlapping, and the count
runs there only as the soak below.

**What it does not check, and why**: the **CPU half of 3** — a CPU measure over a hundred steps
under ``-n 12`` is the flake ``tests/test_worker_exit_budget.py`` measures one case at a time;
**7** needs a sink that raises, and the recorder never does (``tests/test_worker.py`` and
``tests/test_sink_losses.py`` own it); **8** needs a hostile value and no step supplies one
(``tests/test_sanitize_corpus.py``); **9** needs baggage or an adopted context and no step sets
one; **10** is the public surface and ``Runtime`` is private; **11** needs a value that could leak
into stderr and none is supplied; **13** needs a bad argument and every step is valid.

**The bound is one second past each call's own timeout, in both halves** (SPEC-086 FR-001), not
ten: the passive sink waits on nothing, so a call that spends more than its timeout plus
``EPSILON`` overran it. The session prints the worst ``elapsed - budget`` over the passing runs
(``model_oracles.gate_margin_line``), so a margin creeping toward the second is seen before it
fails. ``tests/test_worker.py`` still owns the bounds against a sink that wedges.

**Which runs exist is a setting** (FR-002). ``LOG_FOUNDRY_MODEL_SEEDS`` holds a comma-separated
list of integers and inclusive ``a-b`` ranges, such as ``0-999``; set, both halves run exactly those
seeds, and unset they run 0 to 15 and 0 to 23. Only the default range gates. A value that does not
parse is refused by ``tests/conftest.py`` before any test runs. A concurrent seed replays its draw,
the operations each thread makes, and not its schedule, which is the operating system's; a
sequential seed replays exactly. Every failure ends with the command that re-runs its item alone.

**The soak** (FR-003). ``LOG_FOUNDRY_MODEL_SOAK=1`` adds one item per passive form and seed that
runs the concurrent test as the gating item does, with the library's own record of what it armed
and took read through four wrapped private names, and judges every sink by two rules: an
unowed repeat close (two closes at one count with no writer's arm between their takes) and a write
after the last close returned. A red soak item is a finding, not a regression, and it gates
nothing: no workflow sets the switch. ``test_the_soak_is_wired`` runs the soak's body for one seed
in every session and asserts its wiring, never its verdict.

```
LOG_FOUNDRY_MODEL_SEEDS=0-999 poetry run pytest tests/test_invariants_model.py
LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=0-1999 poetry run pytest tests/test_invariants_model.py
```
"""

from __future__ import annotations

import ast
import collections
import contextlib
import functools
import json
import os
import pathlib
import random
import re
import threading
import time
import weakref
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import install_worker, retire_process, run_concurrently
from log_foundry import _diag, _lifecycle
from log_foundry._lifecycle import State
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
from log_foundry.worker import Worker
from model_oracles import (
    EPSILON,
    GATE_MARGIN,
    Arm,
    BoundExceeded,
    Call,
    Close,
    CloseBroken,
    Closes,
    SinkEvent,
    Stalled,
    Take,
    Write,
    check_call,
    check_late_write,
    check_repeat,
    check_unowed_repeat,
    rerun_command,
    seeds_from,
    soak_from,
    take_of_each_close,
)
from model_sinks import span_ends_queued
from test_fork_lifecycle import CHILD_TIMEOUT, run_in_child
from test_sinks_http import FakeResponse

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

FORMS = ("emit", "deliver", "http")
DEFAULT_SEEDS = range(16)
"""The concurrent half's gating seeds; only this range gates (SPEC-086 FR-002)."""

SEEDS = seeds_from(os.environ, DEFAULT_SEEDS)
"""The concurrent seeds this session runs: ``LOG_FOUNDRY_MODEL_SEEDS``, else the default."""

SOAK = soak_from(os.environ)
"""Whether ``LOG_FOUNDRY_MODEL_SOAK=1`` asked for the soak items (SPEC-086 FR-003)."""

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
SAMPLER_JOIN = 10.0
"""How long the state sampler's thread is joined for: a test's own bookkeeping, not a library bound."""

_REPO = pathlib.Path(__file__).resolve().parent.parent
_PAGE = _REPO / "docs" / "invariants.md"


class _SoakRecord:
    """One soak run's record of every arm, take, write and close of the sinks it handed over.

    SPEC-086 FR-003. The arms and takes come from wrappers on ``_CloseRegistry.arm`` and
    ``.take``, which the library calls holding ``_state._lock``, and the writes and closes from
    the recorders, under each recorder's own lock; this record's lock is a leaf taken inside
    either and never around anything, so one sink's events land in one list in the order they
    happened. An arm is a **writer's** when it arms the sink a writer is about to write: an arm
    of the very sink an ``api._note_orphan_emit`` on this thread was called for, or any arm on a
    thread inside ``_lifecycle._get_worker``, whose build arms the configured sink for the worker
    to write. The orphan path's sink is known, its argument, so its arm is matched by identity;
    the build's sink is resolved inside it, where a wrapper cannot see it without racing a
    concurrent ``configure()``, so a build's arm is attributed by the thread being inside it, and
    a build arms nothing but the sink it builds on.

    Attributes:
      installed: The private names wrapped, in order.
      orphan_entries: How many times ``api._note_orphan_emit`` was entered.
      builds: How many times ``_lifecycle._get_worker`` was entered.
      judged_repeat: The sinks the unowed-repeat rule was evaluated over.
      judged_late: The sinks the late-write rule was evaluated over.
      report: The failure text of a red run, empty for a clean one.
    """

    def __init__(self) -> None:
        """Starts an empty record.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.installed: list[str] = []
        self.orphan_entries = 0
        self.builds = 0
        self.judged_repeat = 0
        self.judged_late = 0
        self.report = ""
        self._events: dict[int, list[SinkEvent]] = {}
        self._sinks: dict[int, object] = {}
        self._lock = threading.Lock()
        self._local = threading.local()

    def register(self, sink: object) -> None:
        """Starts recording one sink, before the library is handed it.

        Args:
          sink: The sink.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._sinks[id(sink)] = sink
            self._events[id(sink)] = []

    def add(self, sink: object, event: SinkEvent) -> None:
        """Appends one event for a registered sink; an unregistered one is not this run's.

        Args:
          sink: The sink the event happened to.
          event: The event.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            if self._sinks.get(id(sink)) is sink:
                self._events[id(sink)].append(event)

    def events_of(self, sink: object) -> list[SinkEvent]:
        """Returns a copy of one sink's events, in the order recorded.

        Args:
          sink: The sink.

        Returns:
          Its events.

        Raises:
          None.
        """
        with self._lock:
            return list(self._events.get(id(sink), ()))

    def all_events(self) -> list[SinkEvent]:
        """Returns every registered sink's events, one sink after another.

        Args:
          None.

        Returns:
          The events.

        Raises:
          None.
        """
        with self._lock:
            return [event for events in self._events.values() for event in events]

    def _frames(self) -> list[object]:
        """Returns this thread's stack of writers in progress: sinks, or :data:`_BUILD`."""
        frames = getattr(self._local, "frames", None)
        if frames is None:
            frames = self._local.frames = []
        return frames

    def entering(self, writer: object) -> None:
        """Marks this thread as inside a writer: the orphan path for a sink, or a worker build.

        Args:
          writer: The sink ``api._note_orphan_emit`` was called for, or :data:`_BUILD`.

        Returns:
          None.

        Raises:
          None.
        """
        self._frames().append(writer)
        with self._lock:
            if writer is _BUILD:
                self.builds += 1
            else:
                self.orphan_entries += 1

    def leaving(self) -> None:
        """Marks this thread as having left the writer it entered last.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._frames().pop()

    def by_writer(self, sink: object) -> bool:
        """Returns whether an arm of *sink* recorded now is a writer's.

        Args:
          sink: The sink being armed.

        Returns:
          Whether this thread is inside the orphan path for this very sink, or inside a build.

        Raises:
          None.
        """
        return any(frame is sink or frame is _BUILD for frame in self._frames())


_BUILD = object()
"""The writer frame :func:`_install` pushes for ``_lifecycle._get_worker``."""


_SOAK: dict[str, _SoakRecord | None] = {"run": None}
"""The soak record of the run in progress, or ``None``; set for one test by :func:`_install`."""


class _CloseRecord:
    """One recorder's write count, taken at the delivery method's first statement, and its closes.

    SPEC-086 FR-001. A recorder calls :meth:`wrote` as the first statement of the method the
    library calls, and :meth:`closing` and :meth:`closed` as the first and last of ``close``.
    Counted any later, a delivery already inside the sink when a close began reads as a write
    after it: counting at ``HTTPRecorder``'s opener rather than at ``deliver`` failed the
    late-write rule on 113 of 1,000 ``http`` runs that are clean counted at entry (the spec's
    evidence). It holds no reference to its recorder, which passes itself to each call, so the
    record keeps nothing alive. While a soak runs it also feeds :data:`_SOAK`.
    """

    def __init__(self) -> None:
        """Starts at no writes and no closes.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._writes = 0
        self._at_entry: list[int] = []
        self._at_return: list[int] = []
        self._lock = threading.Lock()

    def wrote(self, sink: object) -> None:
        """Counts one delivery call entering the sink.

        Args:
          sink: The recorder it entered.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._writes += 1
            soak = _SOAK["run"]
            if soak is not None:
                soak.add(sink, Write(threading.current_thread().name))

    def closing(self, sink: object) -> None:
        """Records the count as a close enters.

        Args:
          sink: The recorder being closed.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._at_entry.append(self._writes)
            soak = _SOAK["run"]
            if soak is not None:
                name = threading.current_thread().name
                soak.add(sink, Close(self._writes, name, time.monotonic()))

    def closed(self) -> None:
        """Records the count as a close returns.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._at_return.append(self._writes)

    def closes(self) -> Closes:
        """Returns the record as the oracles read it.

        Args:
          None.

        Returns:
          The writes and the count at each close's entry and return.

        Raises:
          None.
        """
        with self._lock:
            return Closes(self._writes, tuple(self._at_entry), tuple(self._at_return))


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
        self.record = _CloseRecord()
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.record.wrote(self)
        self._keep(batch)

    def _keep(self, batch: list[dict[str, object]]) -> None:
        """The body of :meth:`emit` past its count, so an override counts once."""
        if self.emit_delay:
            time.sleep(self.emit_delay)
        with self._lock:
            self.events.extend(batch)

    def close(self) -> None:
        self.record.closing(self)
        self._shut()
        self.record.closed()

    def _shut(self) -> None:
        """The body of :meth:`close` between its two records, so an override records once."""
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
        self.record.wrote(self)
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
    A forked child's marking walk visits the objects the ownership record reaches from a live
    entry, up to a cap of 100,000, and :attr:`live` keeps a recorder alive, so its entry with
    it. Sixteen seeds of decoded events kept here added about 140,000 objects to that walk in
    the pytest worker that ran them, measured before SPEC-081, enough to trip the cap in
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
        self.record = _CloseRecord()
        self._lock = threading.Lock()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Counts the delivery call at its first statement, then runs ``HTTPSink``'s round.

        The library calls ``deliver`` for this class, not the opener, and a class whose own body
        defines ``deliver`` keeps being offered it: ``HTTPSink.__init_subclass__`` leaves such a
        class alone, and the probe finds this ``deliver`` no later in the MRO than the ``emit``
        it inherits (SPEC-086 FR-001).
        """
        self.record.wrote(self)
        return super().deliver(batch)

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
        self.record.closing(self)
        with self._lock:
            self.closes += 1
            self.inside += 1
            if self.inside > 1:
                self.overlapped += 1
        time.sleep(CLOSE_SECONDS)
        with self._lock:
            self.inside -= 1
        self.record.closed()

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

    A recorder handed to the library keeps an entry in ``_lifecycle._state._owned`` while it is
    alive, and a later fork test's marking walk visits the objects a live, unclosed entry
    reaches, up to ``_MARKING_CEILING``. :class:`HTTPRecorder` measured that hazard; the sequential driver hands
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
    sinks: list[Recorder] = field(default_factory=list)
    calls: list[Call] = field(default_factory=list)
    form: str = "emit"
    seed: int = 0
    soak: _SoakRecord | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_sink(self, sink: Recorder) -> None:
        """Keeps a sink before the library is handed it, and starts the soak's record of it."""
        if self.soak is not None:
            self.soak.register(sink)
        with self._lock:
            self.sinks.append(sink)

    def note(self, **counts: int) -> None:
        with self._lock:
            for name, value in counts.items():
                setattr(self, name, getattr(self, name) + value)

    def timed(self, name: str, budget: float, call: Callable[[], object]) -> None:
        """Makes one library call and records it, for the bound judged after the run (FR-001).

        Judged after the run rather than here, because an assertion raised on an actor's thread
        would reach ``run_concurrently`` and read as an escaped exception, invariant 1's failure.
        """
        began = time.monotonic()
        call()
        ended = time.monotonic()
        with self._lock:
            self.calls.append(Call(name, threading.get_ident(), budget, began, ended))


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
            ledger.timed("span", 0.0, functools.partial(_outer, nested))
            ledger.note(spans=2 if nested else 1)
        elif roll < 0.75:
            ledger.timed("info", 0.0, functools.partial(log_foundry.info, "orphan"))
            ledger.note(orphans=1)
        elif roll < 0.85:
            flush = functools.partial(log_foundry.flush, timeout=FLUSH_TIMEOUT)
            ledger.timed("flush", FLUSH_TIMEOUT, flush)
        elif roll < 0.95:
            sink = _recorder(ledger, rng.choice([0.0, 0.0, 0.002]))
            ledger.add_sink(sink)
            change = functools.partial(log_foundry.configure, service="model", sink=sink)
            ledger.timed("configure", _lifecycle.DEFAULT_SWAP_TIMEOUT, change)
        else:
            stop = functools.partial(log_foundry.shutdown, timeout=SHUTDOWN_TIMEOUT)
            ledger.timed("shutdown", SHUTDOWN_TIMEOUT, stop)


def _recorder(ledger: Ledger, emit_delay: float) -> Recorder:
    """A recorder of the ledger's form."""
    if ledger.form == "deliver":
        return DeliverRecorder(emit_delay, ledger.seed)
    if ledger.form == "http":
        return HTTPRecorder(emit_delay, ledger.seed)  # type: ignore[return-value]
    return Recorder(emit_delay)


@dataclass
class _Runs:
    """Every ``run_delivery`` call and every exit batch declined unattempted, with the workers.

    ``declined`` is each declined batch's size and its events' ``(log_id, message)``: such a
    batch never reaches ``run_delivery`` or a recorder, yet it is one ``failed_batches``, its
    events are in a lost line (SPEC-090 FR-003), and its ``span.end`` events have left the queue.
    The events are read from the ``_emit_pending`` call that declined it, on its own thread.
    """

    calls: list[tuple[list[str], str, _engine.Delivery]] = field(default_factory=list)
    declined: list[tuple[int, list[tuple[str, object]]]] = field(default_factory=list)
    workers: dict[int, object] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)


def _ends_out_of_the_queue(offered: set[str], runs: _Runs, where: str) -> int:
    """Counts the ``span.end`` events that left the worker's queue, offered or declined.

    An event offered to a recorder is out of the queue whatever its fate; one in an exit batch
    declined unattempted was never offered, yet it left the queue as a ``failed_batches`` too
    (SPEC-090 FR-003). The two sets are disjoint, since an event offered and then declined
    would be one handled twice, and an overlap is reported rather than absorbed by a union.
    """
    declined = [i for _, batch in runs.declined for i, m in batch if m == "span.end"]
    both = offered.intersection(declined)
    assert not both, f"invariant 2 (a span.end offered and declined): {sorted(both)[:5]} {where}"
    return len(offered) + len(set(declined))


def _spy_on_runs(monkeypatch: pytest.MonkeyPatch) -> _Runs:
    """Records each ``run_delivery`` call's batch, path and ``Delivery``, and each declined batch."""
    runs = _Runs()
    real = _engine.run_delivery
    real_decline = Worker._decline_exit_batch
    real_pending = Worker._emit_pending
    held = threading.local()

    def emit_pending(self: Worker, pending: list[list[dict[str, object]]]) -> None:
        held.batch = [(str(e.get("log_id")), e.get("message")) for es in pending for e in es]
        try:
            real_pending(self, pending)
        finally:
            held.batch = None

    def decline(self: Worker, size: int) -> None:
        real_decline(self, size)
        with runs._lock:
            runs.declined.append((size, getattr(held, "batch", None) or []))
            runs.workers[id(self)] = self

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
    monkeypatch.setattr(Worker, "_decline_exit_batch", decline)
    monkeypatch.setattr(Worker, "_emit_pending", emit_pending)
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
        self._thread.join(SAMPLER_JOIN)


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


def _concurrent_params(seeds: tuple[int, ...]) -> list[Any]:
    """One ``(form, seed)`` per passive form and seed, each id naming both (SPEC-086 FR-002)."""
    return [pytest.param(form, seed, id=f"{form}-seed{seed}") for seed in seeds for form in FORMS]


@contextlib.contextmanager
def _rerun_on_failure(
    request: pytest.FixtureRequest, seed: int, *, concurrent: bool, soak: bool = False
) -> Iterator[None]:
    """Ends any failure inside it with the one command that re-runs its item alone (FR-002).

    It catches every ``Exception`` and pytest's ``fail()`` alike, a ``ValueError`` from reading
    a forked child's report included, and adds the command as a note before re-raising, so the
    original traceback and message survive and the command is the last thing the failure says.
    """
    try:
        yield
    except (Exception, pytest.fail.Exception) as exc:
        lines = []
        if concurrent:
            lines.append(
                "A concurrent seed replays its draw, the operations each thread makes, and not "
                "its schedule, which is the operating system's: a re-run can pass."
            )
        lines.append(f"Re-run it alone: {rerun_command(request.node.nodeid, seed, soak=soak)}")
        exc.add_note("\n".join(lines))
        raise


def _bounded(call: Call, where: str) -> float:
    """Holds one call to ``elapsed <= budget + EPSILON`` and returns its margin (FR-001).

    No sink-code interval is subtracted: a passive recorder waits on nothing, so all of a call's
    time is the library's.
    """
    try:
        check_call(call, ())
    except (BoundExceeded, Stalled) as exc:
        raise AssertionError(
            f"invariant 3 (within its timeout plus {EPSILON:.1f} s): "
            f"{' '.join(exc.signatures)}, {exc.detail}; {_machine()} {where}"
        ) from exc
    return (call.ended - call.began) - call.budget


def _machine() -> str:
    """How loaded this machine was when a bound failed, so starvation reads apart from a stall.

    Under 40 busy-loops on a 10-core machine, a load of 81 to 112, 4 of 6 runs of this file
    failed a zero-budget call at 1.03 to 1.35 s of own time; at a load of 50 to 55 it was green 13
    of 13. The bound is not skipped or widened under load, since that would let a real stall pass
    as starvation; the failure says how starved the run was, and a reader decides.
    """
    try:
        one, five, fifteen = os.getloadavg()
    except OSError:
        return f"load average unavailable on {os.cpu_count()} CPUs"
    return f"load average {one:.1f} / {five:.1f} / {fifteen:.1f} on {os.cpu_count()} CPUs"


_WRAPPED: tuple[tuple[object, str, str], ...] = (
    (_lifecycle._CloseRegistry, "arm", "_lifecycle._CloseRegistry.arm"),
    (_lifecycle._CloseRegistry, "take", "_lifecycle._CloseRegistry.take"),
    (log_foundry.api, "_note_orphan_emit", "log_foundry.api._note_orphan_emit"),
    (_lifecycle, "_get_worker", "_lifecycle._get_worker"),
)
"""The four private names the soak wraps, by owner, attribute and the name it reports.

``api._note_orphan_emit`` is called as a global of ``api`` and ``_lifecycle._get_worker`` as an
attribute of ``_lifecycle`` (``decorator.py``), the only callers of either, so a patch of each
intercepts every call, here and at ``31dda162``.
"""


def _install(monkeypatch: pytest.MonkeyPatch, soak: _SoakRecord) -> None:
    """Wraps the four private names for the length of one test, by name (SPEC-086 FR-003).

    A name that no longer exists fails the item instead of silencing it: a rename would
    otherwise leave the soak reading no arms or takes, and every repeat close would look unowed,
    or none would be recorded at all.
    """
    for owner, name, label in _WRAPPED:
        if name not in vars(owner):
            pytest.fail(f"the soak cannot wrap {label}: the name no longer exists")
    real_arm = _lifecycle._CloseRegistry.arm
    real_take = _lifecycle._CloseRegistry.take
    real_note = log_foundry.api._note_orphan_emit
    real_build = _lifecycle._get_worker

    def arm(registry: Any, sink: Any) -> None:
        soak.add(sink, Arm(by_writer=soak.by_writer(sink)))
        real_arm(registry, sink)

    def take(registry: Any, sink: Any, rule: Any) -> Any:
        soak.add(sink, Take())
        return real_take(registry, sink, rule)

    def note(sink: Any) -> None:
        soak.entering(sink)
        try:
            real_note(sink)
        finally:
            soak.leaving()

    def build() -> Any:
        soak.entering(_BUILD)
        try:
            return real_build()
        finally:
            soak.leaving()

    monkeypatch.setitem(_SOAK, "run", soak)
    monkeypatch.setattr(_lifecycle._CloseRegistry, "arm", arm)
    monkeypatch.setattr(_lifecycle._CloseRegistry, "take", take)
    monkeypatch.setattr(log_foundry.api, "_note_orphan_emit", note)
    monkeypatch.setattr(_lifecycle, "_get_worker", build)
    soak.installed = [label for _, _, label in _WRAPPED]


def _judge_soak(soak: _SoakRecord, ledger: Ledger, err: str) -> str:
    """Judges every sink the run handed the library by the soak's two rules (SPEC-086 FR-003).

    No run and no sink is exempt. Returns the failure text, empty when every sink is clean.
    """
    found: list[str] = []
    for index, sink in enumerate(ledger.sinks):
        events = soak.events_of(sink)
        closes = sink.record.closes()
        keys: list[str] = []
        soak.judged_repeat += 1
        try:
            check_unowed_repeat(events)
        except CloseBroken as exc:
            keys.extend(exc.signatures)
        soak.judged_late += 1
        try:
            check_late_write(closes)
        except CloseBroken as exc:
            keys.extend(exc.signatures)
        if keys:
            found.append(_sink_record(index, keys, events, closes))
    if not found:
        return ""
    stderr = [line for line in err.splitlines() if line.startswith("log-foundry")]
    return "\n".join(
        [
            (
                f"soak {ledger.form} seed={ledger.seed}: invariant 5 (a close per write, owed by "
                f"its writer) broke on {len(found)} of {len(ledger.sinks)} sinks"
            ),
            *found,
            f"the library's stderr ({len(stderr)} lines):",
            *(f"  {line}" for line in stderr),
            f"log_foundry.__file__={log_foundry.__file__}",
        ]
    )


def _sink_record(index: int, keys: list[str], events: list[SinkEvent], closes: Closes) -> str:
    """One failing sink's record: each close's counts and thread, the gaps, and what lay between."""
    lines = [f"sink {index}: {' '.join(keys)}; writes={closes.writes}"]
    marks: list[str] = []
    seen = 0
    previous: float | None = None
    for event in events:
        if isinstance(event, Close):
            returned = closes.at_return[seen] if seen < len(closes.at_return) else None
            gap = "" if previous is None else f", {1000 * (event.at - previous):.1f} ms after"
            between = " ".join(marks) or "nothing"
            lines.append(
                f"  close {seen}: entered at {event.count}, returned at {returned}, on "
                f"{event.thread}{gap}; arms, writes and takes before it: {between}"
            )
            previous = event.at
            seen += 1
            marks = []
        elif isinstance(event, Arm):
            marks.append("arm(writer)" if event.by_writer else "arm(change)")
        elif isinstance(event, Take):
            marks.append("take")
        elif isinstance(event, Write):
            marks.append(f"write({event.thread})")
    return "\n".join(lines)


def _interleaving(
    seed: int,
    form: str,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    soak: _SoakRecord | None,
) -> tuple[Ledger, float]:
    """Runs one seeded interleaving and makes every gating assertion; the soak judges too.

    Shared by the gating item, the soak item and the wiring test, so a soak runs the concurrent
    half exactly as the gate does. With a soak record its two rules are judged right after the
    final shutdown, before the gating assertions, and a gating failure carries the soak's text
    as a note, so neither can hide the other; the soak's own verdict is left in ``soak.report``.

    Returns:
      The ledger, and the worst ``elapsed - budget`` over every call the run made.
    """
    ledger = Ledger(form=form, seed=seed, soak=soak)
    runs = _spy_on_runs(monkeypatch) if form != "emit" else None
    if form == "http":
        monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)
    if soak is not None:
        _install(monkeypatch, soak)
    first = _recorder(ledger, random.Random(seed).choice([0.0, 0.0, 0.002]))  # noqa: S311
    ledger.add_sink(first)
    change = functools.partial(log_foundry.configure, service="model", sink=first)
    ledger.timed("configure", _lifecycle.DEFAULT_SWAP_TIMEOUT, change)

    def work(index: int, _iteration: int) -> None:
        _actor(ledger, random.Random(seed * 10 + index), shutdown_in_mix=seed % 2 == 0)  # noqa: S311

    sampler = _StateSampler()
    sampler.start()
    try:
        escaped = run_concurrently(work, threads=THREADS)
    finally:
        sampler.stop()
    stop = functools.partial(log_foundry.shutdown, timeout=FINAL_SHUTDOWN_TIMEOUT)
    ledger.timed("final-shutdown", FINAL_SHUTDOWN_TIMEOUT, stop)
    final_state = _lifecycle._state.state
    tokens_left = len(_lifecycle._state._swaps_running) + len(_lifecycle._state._stops_running)
    health = log_foundry.health()
    markers = _stranded_markers()
    queued = health.queued - markers
    ends_queued = span_ends_queued()
    err = capfd.readouterr().err
    if soak is not None:
        soak.report = _judge_soak(soak, ledger, err)

    ends = sum(sink.count("span.end") for sink in ledger.sinks)
    orphans = sum(sink.count("orphan") for sink in ledger.sinks)
    if runs is not None:
        # In the deliver form an event offered to the sink has a fate rather than a delivery: a
        # span.end offered once is out of the queue whatever its fate (a failed worker run is
        # failed_batches, held below), and an orphan whose run failed is the orphan_lost term,
        # so "delivered" is the orphans whose run settled.
        offered = {e for s in ledger.sinks for e in _ids(s, "span.end")}
        ends = _ends_out_of_the_queue(offered, runs, f"seed={seed} form={form}")
        failed_orphans = sum(1 for _, path, d in runs.calls if path == "orphan" and d.failed)
        orphans = len({e for s in ledger.sinks for e in _ids(s, "orphan")}) - failed_orphans
    unclosed = [i for i, sink in enumerate(ledger.sinks) if sink.events and sink.closes == 0]
    overlapped = [i for i, sink in enumerate(ledger.sinks) if sink.overlapped]
    picture = (
        f"seed={seed} spans={ledger.spans} span.end={ends} queued={health.queued} "
        f"span.end_queued={ends_queued} "
        f"markers={markers} dropped={health.dropped} in_span_lost={health.in_span_lost} | "
        f"orphans={ledger.orphans} delivered={orphans} orphan_lost={health.orphan_lost} | "
        f"failed_batches={health.failed_batches} incomplete_swaps={health.incomplete_swaps} "
        f"stopped_reason={health.stopped_reason} after_shutdown={health.submitted_after_shutdown} "
        f"sinks={len(ledger.sinks)} "
        f"unclosed={unclosed} overlapped={overlapped} escaped={escaped!r}"
    )

    try:
        assert not escaped, f"invariant 1 (never fail the caller): {picture}"
        assert not sampler.wrong, f"invariant 1 (state always answers a State): {sampler.wrong[:3]}"
        assert sampler.reads, "the sampler never read the state, so it checked nothing"
        assert final_state is State.CLOSED, f"after the final shutdown: {final_state} {picture}"
        assert tokens_left == 0, f"a token outlived its call: {tokens_left} {picture}"
        assert health.in_span_lost == 0, f"nothing here can fail assembly: {picture}"
        if runs is None:
            assert health.failed_batches == 0, f"the recorder never raises: {picture}"
        elif form == "http":
            _assert_the_http_ledger(ledger, runs, health.orphan_lost, err, picture)
        else:
            _assert_the_deliver_ledger(ledger, runs, health.orphan_lost, err, picture)
        assert ends + ends_queued + health.dropped == ledger.spans, (
            f"invariant 2 (spans): {picture}"
        )
        assert orphans + health.orphan_lost == ledger.orphans, f"invariant 2 (orphans): {picture}"
        assert health.retired, f"invariant 2 (a shutdown ran, so retired reports it): {picture}"
        assert health.submitted_after_shutdown >= queued, (
            f"invariant 2 (what a shutdown left queued was counted as submitted after it): "
            f"{picture}"
        )
        assert not unclosed, f"invariant 5 (every sink written to is closed): {picture}"
        assert not overlapped, (
            f"invariant 5 (closed **once**, and a close in flight is waited for): {picture}"
        )
        worst = max(_bounded(call, picture) for call in ledger.calls)
    except AssertionError as exc:
        if soak is not None and soak.report:
            exc.add_note(soak.report)
        raise
    return ledger, worst


@pytest.mark.parametrize(("form", "seed"), _concurrent_params(SEEDS))
def test_the_accounting_identity_holds_under_every_interleaving(
    seed: int,
    form: str,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    request: pytest.FixtureRequest,
    record_property: Callable[[str, object], None],
) -> None:
    """Invariants 1, 2, 3, 5 and 6 of ``docs/invariants.md``, over one seeded interleaving.

    The identity is the whole test: every span the actors started is either delivered as a
    ``span.end``, still queued where a ``shutdown()`` left it, or counted as dropped; every
    orphan is delivered or counted in ``orphan_lost``; nothing is missing and nothing is counted
    twice. A regression in the lifecycle plumbing shows up here as a seed whose ledger does not
    balance, before anyone has written the test that names it. Every call the run makes, the
    first ``configure`` and the final ``shutdown`` included, returns within its own timeout plus
    ``EPSILON`` (SPEC-086 FR-001), and the worst margin is recorded for the session's line.

    The ``deliver`` form (SPEC-072 FR-005) runs the same seeds with :class:`DeliverRecorder`,
    whose mixed verdicts make "delivered" a per-event fate, and holds each counted term to its
    exact share (:func:`_assert_the_deliver_ledger`). The ``http`` form (SPEC-073 FR-005) runs
    them with :class:`HTTPRecorder`, a shipped family sink behind a scripted opener
    (:func:`_assert_the_http_ledger`).
    """
    with _rerun_on_failure(request, seed, concurrent=True):
        _, worst = _interleaving(seed, form, monkeypatch, capfd, None)
    record_property(GATE_MARGIN, f"{worst:.6f}")


if SOAK:

    @pytest.mark.parametrize(("form", "seed"), _concurrent_params(SEEDS))
    def test_soak_the_close_count_under_concurrency(
        seed: int,
        form: str,
        monkeypatch: pytest.MonkeyPatch,
        capfd: pytest.CaptureFixture[str],
        request: pytest.FixtureRequest,
        record_property: Callable[[str, object], None],
    ) -> None:
        """SPEC-086 FR-003: the close count under four callers, collected only on request.

        The concurrent half exactly as the gating item runs it, with the library's arms and
        takes recorded, and every sink it was handed judged after the final shutdown for an
        unowed repeat close and a write after its last close returned. A red item is a finding,
        not a regression in the change under test, and gates nothing; it records no margin.
        """
        record_property("model_tree", log_foundry.__file__)
        soak = _SoakRecord()
        with _rerun_on_failure(request, seed, concurrent=True, soak=True):
            _interleaving(seed, form, monkeypatch, capfd, soak)
            if soak.report:
                raise AssertionError(soak.report)


WIRING_SEED = 2
"""The seed the wiring test soaks: measured among the fastest in all three forms."""


@pytest.mark.parametrize("form", FORMS)
def test_the_soak_is_wired(
    form: str, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    """SPEC-086 FR-003: the soak's body for one seed, asserting its wiring and never its verdict.

    Once per passive form, because each form counts its writes in a method of its own:
    ``emit``, ``DeliverRecorder.deliver`` and ``HTTPRecorder.deliver``. With only the ``emit``
    form here, deleting the count from either ``deliver`` left every soak item green, since a
    sink with no writes recorded has no write after its last close and no repeat it can blame.
    Over the seeded run, only what its draw makes certain whatever the schedule: the four
    wrappers are installed; arms, takes, writes and closes are recorded; the orphan path's
    wrapper was entered; both rules were evaluated once per sink; and every close discharged a
    take, since a close with no take means the take wrapper missed one. Whether either rule found
    anything is not asserted, and neither is which thread wrote: under 40 busy-loops seed 2's run
    delivered no batch on the drain thread in 2 of 6 runs. So this cannot fail at a race's rate.

    The coda, on the runtime the final shutdown left with nothing owed, holds the orphan path's
    attribution, which the run cannot be relied on to exercise: one orphan log's arm of a fresh
    sink is a writer's, and its write is on this thread. The worker path's attribution and a write
    on the drain thread are :func:`test_the_soak_records_the_worker_path`'s, on a fresh runtime,
    because a traced call after the final shutdown meets the retired worker and builds none.
    """
    soak = _SoakRecord()
    ledger, _ = _interleaving(WIRING_SEED, form, monkeypatch, capfd, soak)
    assert soak.installed == [label for _, _, label in _WRAPPED]
    events = soak.all_events()
    kinds = collections.Counter(type(event).__name__ for event in events)
    assert {kinds[name] > 0 for name in ("Arm", "Take", "Write", "Close")} == {True}, kinds
    assert soak.orphan_entries > 0, "the orphan path's wrapper was never entered"
    assert soak.judged_repeat == soak.judged_late == len(ledger.sinks), (
        soak.judged_repeat,
        soak.judged_late,
        len(ledger.sinks),
    )
    untaken = [
        index
        for index, sink in enumerate(ledger.sinks)
        if None in take_of_each_close(soak.events_of(sink))
    ]
    assert not untaken, f"a close with no take: the take wrapper missed one on sinks {untaken}"

    fresh = Recorder(0.0)
    soak.register(fresh)
    log_foundry.configure(service="model", sink=fresh)
    log_foundry.info("coda")
    assert soak.events_of(fresh) == [
        Arm(by_writer=True),
        Write(threading.current_thread().name),
    ], soak.events_of(fresh)


def test_the_soak_records_the_worker_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """SPEC-086 FR-003: a worker build's arm is a writer's, and its write is on the drain thread.

    Deterministic, one step at a time, on a fresh runtime with the soak's wrappers installed: a
    sink is registered and configured, a traced call builds the worker, whose arm of that sink is
    recorded as a writer's, and a ``flush()`` makes the drain thread write the span. The seeded
    run's own drain-thread writes are not asserted, because under load a run can deliver none.
    """
    soak = _SoakRecord()
    _install(monkeypatch, soak)
    sink = Recorder(0.0)
    soak.register(sink)
    log_foundry.configure(service="model", sink=sink)
    _outer(False)
    assert soak.builds > 0, "the traced call never entered the worker build's wrapper"
    assert log_foundry.flush(timeout=FLUSH_TIMEOUT), "the flush did not settle"
    events = soak.events_of(sink)
    assert events[:1] == [Arm(by_writer=True)], f"the worker build's arm: {events}"
    assert Write("log-foundry-worker") in events, f"no write on the drain thread: {events}"
    log_foundry.shutdown(timeout=SHUTDOWN_TIMEOUT)
    assert isinstance(soak.events_of(sink)[-1], Close), soak.events_of(sink)


def test_a_bound_failure_names_the_machines_load() -> None:
    """SPEC-086 FR-001: a call over its bound fails naming the load average and the CPU count."""
    with pytest.raises(AssertionError) as caught:
        _bounded(Call("info", 0, 0.0, 0.0, 1.5), "where=here")
    text = str(caught.value)
    assert "Stalled:info" in text, text
    assert re.search(r"load average [0-9.]+ / [0-9.]+ / [0-9.]+ on \d+ CPUs", text), text
    assert text.endswith("where=here"), text
    assert _bounded(Call("flush", 0, 2.0, 0.0, 2.5), "") == 0.5


def test_a_writers_arm_is_matched_to_its_sink() -> None:
    """SPEC-086 FR-003: inside the orphan path only that sink's arm is a writer's; in a build, any."""
    soak, mine, other = _SoakRecord(), object(), object()
    assert not soak.by_writer(mine)
    soak.entering(mine)
    assert soak.by_writer(mine)
    assert not soak.by_writer(other)
    soak.entering(_BUILD)
    assert soak.by_writer(other)
    soak.leaving()
    soak.leaving()
    assert not soak.by_writer(mine)
    assert (soak.orphan_entries, soak.builds) == (1, 1)


class _Node:
    """A stand-in for ``request.node``: the one attribute the rerun note reads."""

    nodeid = "tests/test_invariants_model.py::test_x[emit-seed7]"


class _Request:
    """A stand-in for ``pytest.FixtureRequest`` in :func:`test_a_failure_ends_with_its_rerun`."""

    node = _Node()


@pytest.mark.parametrize(
    ("raised", "concurrent", "soak"),
    [
        (AssertionError("an invariant broke"), True, False),
        (ValueError("a child's report did not parse"), False, False),
        (AssertionError("a soak report"), True, True),
    ],
    ids=["assertion-concurrent", "exception-sequential", "assertion-soak"],
)
def test_a_failure_ends_with_its_rerun(raised: Exception, concurrent: bool, soak: bool) -> None:
    """SPEC-086 FR-002: a failure inside the wrapper is re-raised unchanged, with the command last.

    The same exception comes out, message intact, and its last note ends with the command that
    re-runs the item alone; a concurrent item's note also says the seed replays its draw and not
    its schedule, and a sequential one's does not.
    """
    request: Any = _Request()
    with (
        pytest.raises(type(raised)) as caught,
        _rerun_on_failure(request, 7, concurrent=concurrent, soak=soak),
    ):
        raise raised
    assert caught.value is raised
    note = (getattr(caught.value, "__notes__", None) or [""])[-1]
    expected = rerun_command(_Node.nodeid, 7, soak=soak)
    assert note.endswith(f"Re-run it alone: {expected}"), note
    assert ("replays its draw" in note) is concurrent, note


@pytest.mark.parametrize(
    ("owner", "name", "label"), _WRAPPED, ids=[label for _, _, label in _WRAPPED]
)
def test_a_name_the_soak_cannot_wrap_fails_it(
    owner: object, name: str, label: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC-086 FR-003: a rename of any wrapped name fails the soak item instead of silencing it."""
    monkeypatch.delattr(owner, name)
    with pytest.raises(pytest.fail.Exception, match=re.escape(label)):
        _install(monkeypatch, _SoakRecord())


# -- the sequential driver (SPEC-077 FR-004) ----------------------------------------------------

DEFAULT_SEQ_SEEDS = range(24)
"""The sequential half's gating seeds; only this range gates (SPEC-086 FR-002)."""

SEQ_SEEDS = seeds_from(os.environ, DEFAULT_SEQ_SEEDS)
"""The sequential seeds this session runs: ``LOG_FOUNDRY_MODEL_SEEDS``, else the default."""

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
    than summed across ``-n 12`` workers. It reads the **default** range whatever
    ``LOG_FOUNDRY_MODEL_SEEDS`` holds (SPEC-086 FR-002), because the default is what gates.
    """
    visited: set[tuple[str, State, str]] = set()
    for seed in DEFAULT_SEQ_SEEDS:
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
        self.record.wrote(self)
        _sample("emit")
        with self._lock:
            self.threads.extend((threading.get_ident(), e.get("message")) for e in batch)
        self._keep(batch)

    def close(self) -> None:
        self.record.closing(self)
        _sample("close")
        self._shut()
        self.record.closed()

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
    """The budget a step is bounded by, before ``EPSILON`` (invariant 3)."""
    return {
        "configure": _lifecycle.DEFAULT_SWAP_TIMEOUT,
        "flush": FLUSH_TIMEOUT,
        "shutdown": SHUTDOWN_TIMEOUT,
        "exit": _lifecycle.DEFAULT_SHUTDOWN_TIMEOUT,
        "fork": float(CHILD_TIMEOUT),
    }.get(step, 0.0)


def derived_current(runtime: Any) -> State:
    """The settled state the runtime's fields describe, computed here rather than imported.

    Rules 3 to 6 of SPEC-077 without the configured-sink term: a live worker is ``RUNNING``, else a
    retirement is ``CLOSED``, else something owed is ``RUNNING``, else ``UNCONFIGURED``. Liveness is
    read from the worker's ``_epoch`` against the retirement count directly, never through
    ``Runtime.live_worker()``, so a defect in that method cannot hide here.

    Args:
      runtime: The ``Runtime`` to read, in this process or in a forked child.

    Returns:
      The settled state its fields describe.
    """
    worker = runtime._worker
    if worker is not None and runtime.retirements <= worker._epoch:
        return _R
    if runtime.retirements > 0:
        return _C
    if runtime._closes.owed:
        return _R
    return _U


_CHILD_BUDGETS = {"info": 0.0, "flush": FLUSH_TIMEOUT, "shutdown": SHUTDOWN_TIMEOUT}
"""The budget of each of a forked child's three calls, each held to its own (SPEC-086 FR-001)."""


def _fork_step(run: _Run, expected: State, where: str) -> float:
    """Forks at a step boundary and checks the child's half of invariant 12 and the parent's.

    Returns the worst ``elapsed - budget`` over the child's three calls.
    """
    before = [(len(s.threads), s.closes) for s in run.sinks]
    parent_state = _lifecycle._state.state
    inherited = len(run.sinks)

    def child() -> str:
        closes = [s.closes for s in run.sinks]
        initial = _lifecycle._state.state.name
        stored = _lifecycle._state._current.name
        derived = derived_current(_lifecycle._state).name
        calls: list[tuple[str, float]] = []
        for name, call in (
            ("info", functools.partial(log_foundry.info, "child-orphan")),
            ("flush", functools.partial(log_foundry.flush, timeout=FLUSH_TIMEOUT)),
            ("shutdown", functools.partial(log_foundry.shutdown, timeout=SHUTDOWN_TIMEOUT)),
        ):
            started = time.monotonic()
            call()
            calls.append((name, time.monotonic() - started))
        return json.dumps(
            {
                "initial": initial,
                "stored": stored,
                "derived": derived,
                "calls": calls,
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
    assert report["stored"] == report["derived"], (
        f"SPEC-078 FR-003 (the child's stored state is its derived one): {report} {where}"
    )
    names = [name for name, _ in report["calls"]]
    assert names == list(_CHILD_BUDGETS), f"invariant 12 (the child made its three calls): {where}"
    worst = max(
        _bounded(Call(f"child-{name}", 0, _CHILD_BUDGETS[name], 0.0, elapsed), f"{report} {where}")
        for name, elapsed in report["calls"]
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
    return worst


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
        queued = span_ends_queued()
        assert ends + queued + health.dropped == run.spans, (
            f"invariant 2 (spans): span.end={ends} span.end_queued={queued} "
            f"dropped={health.dropped} started={run.spans} {where}"
        )
    if step in {"shutdown", "exit"}:
        unclosed = [i for i, s in enumerate(run.sinks) if s.events and s.closes == 0]
        overlapped = [i for i, s in enumerate(run.sinks) if s.overlapped]
        assert not unclosed, f"invariant 5 (every sink written to is closed): {unclosed} {where}"
        assert not _lifecycle._state._closes.owed, (
            f"invariant 5 (nothing owed after a shutdown) {where}"
        )
        assert not overlapped, f"invariant 5 (closed once, never overlapping): {overlapped} {where}"


def _check_close_count(run: _Run, step: str, where: str) -> None:
    """SPEC-086 FR-001's two close rules, which one caller makes exact (invariant 5).

    After every step, no close of a sink enters at the write count of the close before it
    (``check_repeat``); after every ``shutdown`` and ``exit`` step, no sink has a write above its
    count when its last close returned (``check_late_write``). The late-write rule is read only
    after a step that closes, because a log made after a shutdown leaves its sink written and owed
    until the next one, which invariant 5 records as correct.
    """
    for index, sink in enumerate(run.sinks):
        closes = sink.record.closes()
        try:
            check_repeat(closes)
            if step in {"shutdown", "exit"}:
                check_late_write(closes)
        except CloseBroken as exc:
            raise AssertionError(
                f"invariant 5 (a sink is closed once per write, and nothing writes it after its "
                f"last close): sink {index} {' '.join(exc.signatures)} {exc.detail} {where}"
            ) from exc


@pytest.mark.parametrize("seed", [pytest.param(seed, id=f"seed{seed}") for seed in SEQ_SEEDS])
def test_the_sequential_driver_holds_the_state_table(
    seed: int,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    record_property: Callable[[str, object], None],
) -> None:
    """FR-004: every step against the table, and invariants 1-6 and 12 after each.

    After every step the state is the table's, both token sets are empty, the (before, after) pair
    is a row, and the two facts the table is keyed by that the runtime holds — something owed, a
    live worker — are read back rather than trusted to the model. Every step returns within its
    budget plus ``EPSILON`` and the close count holds after it (SPEC-086 FR-001); the worst margin
    is recorded for the session's line.
    """
    with _rerun_on_failure(request, seed, concurrent=False):
        worst = _drive(seed, monkeypatch)
    record_property(GATE_MARGIN, f"{worst:.6f}")


def _drive(seed: int, monkeypatch: pytest.MonkeyPatch) -> float:
    """Runs one sequential seed, asserting after every step; returns the worst margin."""
    margins: list[float] = []
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
            started = time.monotonic()
            margins.append(_fork_step(run, expected, where))
            ended = time.monotonic()
            margins.append(_bounded(Call(step, driver, _budget(step), started, ended), where))
            model.take(step)
            _check_close_count(run, step, where)
            continue
        started = time.monotonic()
        try:
            result = _take(run, step, nested, label)
        except BaseException as exc:
            pytest.fail(f"invariant 1 (never fail the caller): {step} raised {exc!r} {where}")
        ended = time.monotonic()
        model.take(step)
        state = _lifecycle._state.state
        assert state is expected, f"state {state.name} is not the table's {expected.name} {where}"
        assert (cell[1], state) in ROWS, f"({cell[1].name}, {state.name}) is no row {where}"
        assert not _lifecycle._state._swaps_running and not _lifecycle._state._stops_running, (
            f"a token outlived its call {where}"
        )
        assert bool(_lifecycle._state._closes.owed) == model.owed, (
            f"owed is not the model's {where}"
        )
        stored, derived = _lifecycle._state._current, derived_current(_lifecycle._state)
        assert stored is derived, (
            f"the stored state {stored.name} is not the derived {derived.name} {where}"
        )
        assert (_lifecycle._state.live_worker() is not None) == model.live, (
            f"liveness is not the model's {where}"
        )
        margins.append(_bounded(Call(step, driver, _budget(step), started, ended), where))
        _check_samples(label, step, driver, where)
        _check_transient(cell, label, transient, where)
        _check_invariants(run, step, result, driver, where)
        _check_close_count(run, step, where)
    return max(margins)


def _ids(sink: Recorder, message: str) -> list[str]:
    """The ``log_id`` of every event of one message a recorder was offered."""
    with sink._lock:
        return [str(e.get("log_id")) for e in sink.events if e.get("message") == message]


_WORKER_SOURCE = _REPO / "src" / "log_foundry" / "worker.py"
_TALLY_WRITER = "_write_exit_tallies"
"""The one ``Worker`` method whose lost lines each announce a tally of several batches."""


def _detail_pattern(node: ast.expr, *, tally: bool) -> str:
    """Renders a lost line's ``detail`` argument as a regex, every interpolation any text.

    A tally's first interpolation, its batch count, is the one group, ``(\\d+)``. The detail's
    literal text must fit ``_diag._MAX_DETAIL``, past which the line is cut and no shape could
    read it. Only the literal is held: an interpolation that renders a line past the cut is not
    caught here, and every worker interpolation today is a count.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        parts: list[ast.expr] = [node]
    else:
        assert isinstance(node, ast.JoinedStr), f"a detail not derivable: {ast.unparse(node)}"
        parts = list(node.values)
    literal = "".join(str(p.value) for p in parts if isinstance(p, ast.Constant))
    assert len(literal) <= _diag._MAX_DETAIL, f"a detail longer than _diag cuts: {literal!r}"
    pattern, counted = "", not tally
    for part in parts:
        if isinstance(part, ast.Constant):
            pattern += re.escape(str(part.value))
        elif counted:
            pattern += ".+?"
        else:
            pattern, counted = pattern + r"(\d+)", True
    assert counted, f"a tally names no batch count: {ast.unparse(node)}"
    return pattern


def _worker_lost_shapes() -> dict[re.Pattern[str], bool]:
    """Every lost line ``worker.py`` can write for an event, mapped to whether it is a tally.

    Derived from the source, not hand-listed, so a lost line added there is read here without
    an edit: SPEC-090 added two, the hand-listed pattern read neither, and a ``deliver`` or
    ``http`` run whose exit drain wrote one failed invariant 2. A call whose noun is not a
    literal, or whose detail is neither a literal nor an f-string, fails here rather than being
    skipped. A line written in :data:`_TALLY_WRITER` is a tally whose detail's first number is
    its batch count; every other line is one batch. Two signals must agree on which is which,
    the writer and the count: a one-batch line counts the bare name ``size``, the batch it
    lost, and a tally counts something else. A tally written anywhere else, or a one-batch line
    written in the tally writer, therefore fails here, naming the call, rather than being read
    as the wrong kind in the rare run that writes it.
    """
    tree = ast.parse(_WORKER_SOURCE.read_text(encoding="utf-8"))
    writer: dict[int, tuple[ast.Call, str]] = {}
    for function in ast.walk(tree):
        if isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            for node in ast.walk(function):
                if isinstance(node, ast.Call):
                    writer[id(node)] = (node, function.name)
    shapes: dict[str, bool] = {}
    for call, name in writer.values():
        if not (
            isinstance(call.func, ast.Attribute)
            and call.func.attr == "lost"
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "_diag"
        ):
            continue
        what = call.args[0]
        assert isinstance(what, ast.Constant), f"a lost line's noun: {ast.unparse(what)}"
        if what.value != "event":
            continue
        details = call.args[2:] + [k.value for k in call.keywords if k.arg == "detail"]
        assert len(details) == 1, f"a worker lost line without one detail: {ast.unparse(call)}"
        tally = name == _TALLY_WRITER
        count = call.args[1] if len(call.args) > 1 else None
        one_batch = isinstance(count, ast.Name) and count.id == "size"
        assert tally != one_batch, (
            f"a worker lost line in {name} counts {ast.unparse(count) if count else None}, so "
            f"either the writer and the count disagree on whether it is a tally, or it is not "
            f"a batch loss, which this model does not account: {ast.unparse(call)}"
        )
        detail = _detail_pattern(details[0], tally=tally)
        line = rf"^log-foundry: lost (\d+) event\(s\); {detail}$"
        assert shapes.get(line, tally) == tally, f"one line read as both kinds: {line}"
        shapes[line] = tally
    return {re.compile(line, re.MULTILINE): tally for line, tally in shapes.items()}


_WORKER_LOST = _worker_lost_shapes()


def _worker_lost(err: str) -> tuple[list[int], list[tuple[int, int]]]:
    """The worker's lost lines in ``err``: each one-batch line's events, each tally's two counts."""
    singles: list[int] = []
    tallies: list[tuple[int, int]] = []
    for pattern, tally in _WORKER_LOST.items():
        for match in pattern.finditer(err):
            if tally:
                tallies.append((int(match.group(1)), int(match.group(2))))
            else:
                singles.append(int(match.group(1)))
    return singles, tallies


_EVENT_LOST = re.compile(r"^log-foundry: lost \d+ event\(s\);.*$", re.MULTILINE)


def _unread_event_lines(err: str) -> list[str]:
    """Up to five event lost lines no worker shape reads, for a failure message to show.

    They are mostly the sinks' and the engine's own lines, which are not the worker's to
    account, but a worker line rendered past its derived shape is among them too, and naming it
    is what tells a stale shape from a library defect.
    """
    read = {m.start() for p in _WORKER_LOST for m in p.finditer(err)}
    return [m.group(0) for m in _EVENT_LOST.finditer(err) if m.start() not in read][:5]


def _assert_the_worker_lost_lines(runs: _Runs, err: str, picture: str) -> None:
    """Holds the worker path's failed batches to ``failed_batches`` and to its lost lines.

    A failed batch is a worker run that failed or an exit batch declined unattempted. Each is
    one ``failed_batches`` (invariant 6). Each one-batch lost line names one of them by its
    size, and the batches no such line names are exactly the tallies' (SPEC-090 FR-003): as
    many as the tallies count, holding as many events as they name (invariant 2).
    """
    unseen = [(size, len(batch)) for size, batch in runs.declined if size != len(batch)]
    assert not unseen, f"a declined batch's events were not the ones it held: {unseen} {picture}"
    failed = [len(ids) for ids, path, d in runs.calls if path == "worker" and d.failed]
    failed += [size for size, _batch in runs.declined]
    batches = sum(getattr(w, "failed_batches", 0) for w in runs.workers.values())
    assert batches == len(failed), f"invariant 6 (one failed batch per run): {picture}"
    singles, tallies = _worker_lost(err)
    unread = _unread_event_lines(err)
    rest = collections.Counter(failed)
    rest.subtract(singles)
    assert min(rest.values(), default=0) >= 0, (
        f"invariant 2 (a one-batch lost line names a failed batch): {singles} "
        f"unread={unread} {picture}"
    )
    left = list(rest.elements())
    assert (sum(left), len(left)) == (sum(e for e, _ in tallies), sum(b for _, b in tallies)), (
        f"invariant 2 (a failed run's events are in its lost line): {tallies} "
        f"unread={unread} {picture}"
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
    orphan_failed = sum(1 for _ids_, path, d in runs.calls if path == "orphan" and d.failed)
    _assert_the_worker_lost_lines(runs, err, picture)
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
    orphan_failed = sum(1 for _ids_, path, d in runs.calls if path == "orphan" and d.failed)
    _assert_the_worker_lost_lines(runs, err, picture)
    assert orphan_lost == orphan_failed == len(_ORPHAN_LOST.findall(err)), (
        f"invariant 2 (a failed orphan run is one orphan_lost): {picture}"
    )


def test_every_worker_lost_line_is_derived_and_read() -> None:
    """The derivation finds both kinds of worker lost line, and each tally names its batches.

    A tally writer renamed out from under :data:`_TALLY_WRITER` would read every tally as one
    batch, and a derivation that found nothing would hold an empty set of lines to the runs.
    """
    tallies = [p.pattern for p, tally in _WORKER_LOST.items() if tally]
    singles = [p.pattern for p, tally in _WORKER_LOST.items() if not tally]
    assert singles, "worker.py writes no one-batch lost line the derivation found"
    assert tallies, f"worker.py writes no lost line in {_TALLY_WRITER}"
    assert all(re.compile(p).groups >= 2 for p in tallies), tallies


class _ExitDeliverSink:
    """A ``deliver`` sink whose first call waits for the stop and accepts; later ones retry."""

    max_retries = 0

    def __init__(self) -> None:
        self.log_foundry_counters = SinkCounters()
        self.calls = 0
        self.offered: set[str] = set()
        self.first_waits_on: threading.Event | None = None

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        self.calls += 1
        self.offered.update(str(e.get("log_id")) for e in batch)
        if self.calls == 1:
            assert self.first_waits_on is not None
            self.first_waits_on.wait(30.0)
            return Verdict(outcomes=(Outcome.ACCEPTED,) * len(batch))
        return Verdict(outcomes=(Outcome.RETRYABLE,) * len(batch))

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)

    def close(self) -> None:
        pass


@pytest.mark.parametrize(("timeout", "declines"), [(30.0, False), (0.0, True)])
def test_an_exit_drain_tally_balances_the_worker_ledger(
    timeout: float,
    declines: bool,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    """The worker-path ledger reads SPEC-090's exit tallies, which the gating seeds rarely reach.

    Ten spans wait behind a pre-stop batch the sink holds until the stop, then the exit drain
    sends them in batches of one span against a sink that never accepts again. A shutdown with
    time left abandons every exit batch, the first in its own line and the rest in one tally; a
    shutdown at ``timeout=0`` declines the first exit batch's retry and every later batch
    unattempted, in a tally. Every span left the queue either way, so the spans identity's
    ``ends`` term counts all eleven, the declined ones too. Before the lines were derived from
    ``worker.py``, a ``deliver`` or
    ``http`` run that met either failed invariant 2: 4 of 16,000 such items, seeds 0 to 7999,
    against the hand-listed pattern this file carried at ``03dbf502``.
    """
    monkeypatch.setattr("log_foundry.worker._BACKLOG_BATCH_CAP", 1)
    runs = _spy_on_runs(monkeypatch)
    sink = _ExitDeliverSink()
    worker = Worker(sink, batch_size=1, flush_interval=3_600.0, max_retries=1)
    sink.first_waits_on = worker._stop
    install_worker(worker)
    worker.submit([{"message": "span.end", "log_id": "held"}])
    for _ in range(1_000):
        if sink.calls:
            break
        threading.Event().wait(0.01)
    for seq in range(10):
        worker.submit([{"message": "span.end", "log_id": f"exit-{seq}"}])
    retire_process(worker, timeout)
    worker._thread.join(10.0)
    assert not worker._thread.is_alive(), "the drain thread outlived the test, and its lines too"
    err = capfd.readouterr().err
    picture = f"timeout={timeout} declined={runs.declined} err={err!r}"
    assert _worker_lost(err)[1], f"the run wrote no tally the derived shapes read: {picture}"
    assert bool(runs.declined) == declines, f"the run did not decline as it forces: {picture}"
    held = [i for _, batch in runs.declined for i, _m in batch]
    assert held == [f"exit-{seq}" for seq in range(10 - len(held), 10)], picture
    _assert_the_worker_lost_lines(runs, err, picture)
    assert _ends_out_of_the_queue(sink.offered, runs, picture) == 11, (
        f"invariant 2 (every span.end left the queue, offered or declined): {picture}"
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
