"""SPEC-088 FR-001, FR-003 and FR-004: the event ledger's terms, its check and its surface."""

import threading

import pytest

import ledger_check
import log_foundry as lf
from conftest import install_worker
from ledger_sinks import GatedSink, PlanSink, every, ledger, queued_events, reading
from log_foundry import Outcome, _lifecycle, api, decorator
from log_foundry import worker as worker_mod
from log_foundry.sinks.multi import MultiSink


@pytest.fixture(autouse=True)
def _fresh_ledger(_reset_config):
    """Starts each test on an empty ledger, after the suite's reset has run."""
    ledger_check.begin()
    yield


def _balanced(asked: int) -> None:
    """Asserts the ledger at rest: everything placed, nothing pending, ``asked`` accepted."""
    seen = reading()
    e = seen.ledger
    assert e.unaccounted == 0, seen.ledger
    assert e.pending == 0, e
    assert e.accepted == asked, e
    assert e.accepted == e.pending + e.delivered + e.skipped + e.lost, e
    assert seen.orphan_residual == 0, seen.orphan_residual


def _fast_worker(sink, **settings) -> worker_mod.Worker:
    """Installs a process worker that drains promptly and does not retry."""
    settings.setdefault("max_retries", 0)
    settings.setdefault("batch_size", 1)
    settings.setdefault("flush_interval", 0.02)
    worker = worker_mod.Worker(sink, **settings)
    install_worker(worker)
    return worker


# ── FR-001 ───────────────────────────────────────────────────────────────────────────────


@lf.trace
def _plain() -> None:
    pass


@lf.trace
async def _plain_async() -> None:
    pass


@lf.trace
def _logs_once() -> None:
    lf.info("inside")


async def test_a_traced_call_moves_accepted_by_two_and_a_level_call_by_one() -> None:
    """FR-001 AC-1, for the sync and async wrappers, the in-span and the orphan level call."""
    lf.configure(sink=GatedSink())
    _plain()
    assert ledger().accepted == 2
    await _plain_async()
    assert ledger().accepted == 4
    lf.info("orphan")
    assert ledger().accepted == 5
    _logs_once()
    assert ledger().accepted == 8
    assert lf.flush(5)
    _balanced(8)


def test_a_level_call_whose_event_cannot_be_built_moves_accepted_by_one(monkeypatch) -> None:
    """FR-001 AC-1: a credit precedes the build, so a failed build is accepted and lost."""
    lf.configure(sink=GatedSink())
    _plain()

    def boom(*args: object, **kwargs: object) -> dict:
        """Fails where the event is built."""
        raise RuntimeError("cannot build")

    monkeypatch.setattr(api, "build_event", boom)
    lf.info("orphan")
    _logs_once()
    monkeypatch.undo()
    assert ledger().accepted == 2 + 1 + 3
    assert lf.flush(5)
    e = ledger()
    assert (e.orphan_lost, e.in_span_lost, e.delivered) == (1, 1, 4), e
    _balanced(6)


def test_the_overflow_scenario_balances_in_events() -> None:
    """FR-001 AC-2: ``p01_overflow.py``'s scenario, every event in one term."""
    sink = GatedSink()
    lf.configure(sink=sink)
    sink.gate.clear()
    for _ in range(12_000):
        _logs_once()
    sink.gate.set()
    assert lf.flush(60)
    e = ledger()
    health = lf.health()
    assert (e.accepted, e.delivered, e.overflow_lost) == (36_000, 30_030, 5_970), e
    assert e.overflow_lost == 3 * health.dropped
    assert (e.batch_lost, e.sink_lost, e.drain_lost, e.stranded, e.skipped) == (0, 0, 0, 0, 0)
    assert (e.in_span_lost, e.orphan_lost) == (0, 0)
    assert len(sink.events) == e.delivered
    _balanced(36_000)


def test_abandoned_batches_and_a_failed_orphan_land_in_their_terms(capsys) -> None:
    """FR-001 AC-3: ``p05_raise_nth.py``'s two scenarios."""
    sink = GatedSink()
    seen: dict[str, object] = {"batch": None, "number": 0}

    def every_third(call: int, batch: list) -> BaseException | None:
        """Fails every third logical batch for good, as p05 does."""
        if seen["batch"] is not batch:
            seen["batch"] = batch
            seen["number"] = int(seen["number"]) + 1  # type: ignore[call-overload]
        return ConnectionError("down") if int(seen["number"]) % 3 == 0 else None  # type: ignore[call-overload]

    sink.raise_on = every_third
    _fast_worker(sink, batch_size=10, flush_interval=60.0)
    for _ in range(300):
        _logs_once()
    lf.flush(30)
    err = capsys.readouterr().err
    lines = [line for line in err.splitlines() if "batch abandoned" in line]
    lost = sum(int(line.split("lost ")[1].split(" event")[0]) for line in lines)
    e = ledger()
    assert (e.delivered, e.batch_lost) == (600, 300), e
    assert lost == 300, err
    _balanced(900)

    sink.raise_on = lambda call, batch: ConnectionError("once") if call == sink.calls else None
    orphans = PlanSink(every(Outcome.ACCEPTED))
    failures = iter([True, False])

    def plan(batch: list) -> list:
        """Raises for the first orphan log and accepts the second."""
        if next(failures):
            raise ConnectionError("down")
        return [Outcome.ACCEPTED] * len(batch)

    orphans.plan = plan
    lf.configure(sink=orphans)
    before = ledger()
    lf.info("first")
    lf.info("second")
    after = ledger()
    assert after.orphan_lost - before.orphan_lost == 1
    assert after.delivered - before.delivered == 1


_FATES = {
    "accepted": (every(Outcome.ACCEPTED), "delivered", "delivered"),
    "skipped": (every(Outcome.SKIPPED), "skipped", "skipped"),
    "dropped": (every(Outcome.DROPPED), "sink_lost", "sink_lost"),
    "unknown": (every(Outcome.UNKNOWN), "sink_lost", "sink_lost"),
    "rejected": (every(Outcome.REJECTED), "batch_lost", "orphan_lost"),
    "retryable": (every(Outcome.RETRYABLE), "batch_lost", "orphan_lost"),
    "raising": (None, "batch_lost", "orphan_lost"),
    "unreadable": (lambda batch: "unreadable", "sink_lost", "sink_lost"),
}

_TERMS = (
    "delivered", "skipped", "in_span_lost", "orphan_lost", "overflow_lost", "batch_lost",
    "sink_lost", "drain_lost", "stranded",
)


def _raise(batch: list) -> list:
    """A ``deliver`` that raises."""
    raise ConnectionError("down")


@pytest.mark.parametrize("path", ["worker", "orphan"])
@pytest.mark.parametrize("fate", sorted(_FATES))
def test_each_outcome_moves_one_term_on_both_paths(fate: str, path: str, capsys) -> None:
    """FR-001 AC-4: the same single term on both paths, except a failed run's catcher."""
    plan, on_worker, on_orphan = _FATES[fate]
    sink = PlanSink(plan or _raise)
    _fast_worker(sink)
    lf.configure(sink=sink)
    before = ledger()
    if path == "worker":
        _plain()
        assert lf.flush(5) is not None
        moved, expected = 2, on_worker
    else:
        lf.info("orphan")
        moved, expected = 1, on_orphan
    after = ledger()
    capsys.readouterr()
    deltas = {term: getattr(after, term) - getattr(before, term) for term in _TERMS}
    assert deltas == {term: (moved if term == expected else 0) for term in _TERMS}, deltas
    assert after.unaccounted == 0 and after.pending == 0, after


def test_an_unknown_batch_moves_sink_lost_only(capsys) -> None:
    """FR-001 AC-5."""
    sink = PlanSink(every(Outcome.UNKNOWN))
    _fast_worker(sink, batch_size=5, flush_interval=60.0)
    for _ in range(5):
        _logs_once()
    assert lf.flush(5) is not None
    capsys.readouterr()
    e = ledger()
    assert e.sink_lost == 15, e
    assert (e.delivered, e.batch_lost, e.drain_lost, e.overflow_lost, e.skipped) == (0, 0, 0, 0, 0)
    _balanced(15)


class _Raises:
    """A child sink that always raises."""

    def emit(self, batch: list) -> None:
        raise ConnectionError("down")

    def close(self) -> None:
        pass


def test_a_multisink_with_a_raising_child_counts_the_batch_delivered(capsys) -> None:
    """FR-001 AC-6: the fan-out returned, so its batch is delivered; the child's loss is the sink's."""
    good = GatedSink()
    multi = MultiSink(good, _Raises())
    _fast_worker(multi, batch_size=3, flush_interval=60.0)
    lf.configure(sink=multi)
    for _ in range(3):
        _logs_once()
    assert lf.flush(5)
    capsys.readouterr()
    e = ledger()
    assert e.delivered == 9 and e.lost == 0, e
    assert lf.health().sink.failed == 9
    _balanced(9)


@pytest.mark.parametrize("stalled", [False, True], ids=["healthy", "stalled"])
def test_a_swap_with_spans_in_flight_balances(stalled: bool, monkeypatch, capsys) -> None:
    """FR-001 AC-7: across ``configure(sink=...)``, ``delivered`` is what the two sinks received."""
    monkeypatch.setattr(worker_mod, "DEFAULT_SWAP_TIMEOUT", 0.3)
    monkeypatch.setattr(_lifecycle, "DEFAULT_SWAP_TIMEOUT", 0.3)
    old, new = GatedSink(), GatedSink()
    lf.configure(sink=old)
    stop = threading.Event()
    counts = [0] * 4

    def loop(index: int) -> None:
        while not stop.is_set() and counts[index] < 400:
            _plain()
            counts[index] += 1

    threads = [threading.Thread(target=loop, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    if stalled:
        old.gate.clear()
        old.entered.clear()
        old.entered.wait(5)
    lf.configure(sink=new)
    old.gate.set()
    for thread in threads:
        thread.join(30)
    assert lf.flush(10)
    capsys.readouterr()
    e = ledger()
    assert e.delivered == len(old.events) + len(new.events), e
    _balanced(2 * sum(counts))


def test_a_flush_inside_an_open_span_hands_its_buffer_over() -> None:
    """FR-001 AC-8."""
    sink = GatedSink()
    lf.configure(sink=sink)
    inside: dict[str, object] = {}

    @lf.trace
    def work() -> None:
        lf.info("one")
        lf.info("two")
        before = ledger().delivered
        assert lf.flush(5)
        inside["ledger"] = ledger()
        inside["moved"] = ledger().delivered - before

    work()
    assert inside["moved"] == 3
    assert inside["ledger"].open_spans == 0  # type: ignore[attr-defined]
    before = ledger().delivered
    assert lf.flush(5)
    assert ledger().delivered - before == 1
    _balanced(4)


def test_queued_and_stranded_match_the_queue_under_its_mutex(capsys) -> None:
    """FR-001 AC-9: with the drain alive and stalled, and after it has ended."""
    sink = GatedSink()
    worker = _fast_worker(sink, batch_size=1, flush_interval=60.0)
    sink.gate.clear()
    _plain()
    assert sink.entered.wait(5)
    for _ in range(5):
        _plain()
    e = ledger()
    assert worker._thread.is_alive()
    assert e.queued + e.stranded == queued_events(worker) == 10, e
    assert e.stranded == 0 and e.in_flight == 2, e

    sink.raise_on = lambda call, batch: SystemExit(1)
    sink.gate.set()
    worker._thread.join(5)
    assert not worker._thread.is_alive()
    for _ in range(3):
        _plain()
    capsys.readouterr()
    e = ledger()
    assert e.queued + e.stranded == queued_events(worker), e
    assert e.queued == 0 and e.drain_lost == 2, e
    assert e.unaccounted == 0, e


def test_a_snapshot_during_a_dying_drain_reads_no_unaccounted(monkeypatch, capsys) -> None:
    """FR-001 AC-11: held after ``_drain_finished`` is set, before and after the terminal debit."""
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    held, release, debited, finish = (threading.Event() for _ in range(4))
    real = worker_mod.Worker._terminal_failure

    def hooked(self, exc: BaseException, undrained: int) -> None:
        """Holds the dying thread on both sides of its debit."""
        held.set()
        release.wait(10)
        real(self, exc, undrained)
        debited.set()
        finish.wait(10)

    monkeypatch.setattr(worker_mod.Worker, "_terminal_failure", hooked)
    worker = _fast_worker(sink, batch_size=1, flush_interval=60.0)
    _plain()
    assert held.wait(5)
    assert worker._drain_finished.is_set()
    before = reading()
    release.set()
    assert debited.wait(5)
    after = reading()
    finish.set()
    worker._thread.join(5)
    capsys.readouterr()
    assert before.ledger.unaccounted == 0 and before.ledger.in_flight == 2, before.ledger
    assert after.ledger.unaccounted == 0 and after.ledger.drain_lost == 2, after.ledger
    _balanced(2)


# ── FR-004 ───────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("lock", ["runtime", "worker", "queue", "loss"])
def test_the_snapshot_returns_while_each_lock_is_held(lock: str) -> None:
    """FR-004 AC-5: the snapshot takes none of these four locks."""
    worker = _fast_worker(GatedSink())
    _plain()
    held = {
        "runtime": _lifecycle._state._lock,
        "worker": worker._lock,
        "queue": worker._queue.mutex,
        "loss": decorator._loss_lock,
    }[lock]
    got: list[object] = []
    taken, done = threading.Event(), threading.Event()

    def holder() -> None:
        with held:
            taken.set()
            done.wait(10)

    thread = threading.Thread(target=holder)
    thread.start()
    assert taken.wait(5)
    reader = threading.Thread(target=lambda: got.append(_lifecycle._ledger_snapshot()))
    reader.start()
    reader.join(5)
    returned = not reader.is_alive()
    done.set()
    thread.join(5)
    reader.join(5)
    assert returned, f"the snapshot waited behind {lock}"
    assert got[0].accepted == 2  # type: ignore[attr-defined]


# ── FR-003 ───────────────────────────────────────────────────────────────────────────────

_LINE = "does not balance"


def _lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if _LINE in line]


def test_shutdown_reports_events_in_flight_on_an_ended_drain(capsys) -> None:
    """FR-003 AC-1: an ended drain holds nothing, so what it reads as held is unaccounted."""
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = _fast_worker(sink)
    _plain()
    worker._thread.join(5)
    assert not worker._thread.is_alive()
    capsys.readouterr()
    worker._in_flight = 3
    try:
        lf.shutdown(1)
        assert lf.health().events.unaccounted == 3
    finally:
        worker._in_flight = 0
    lines = _lines(capsys.readouterr().err)
    assert len(lines) == 1 and "lost 3 event(s)" in lines[0], lines


def _lose_inside_submit(monkeypatch) -> None:
    """Makes ``Worker.submit`` return without queueing or counting (the review's ``meet_gap``)."""
    monkeypatch.setattr(worker_mod.Worker, "submit", lambda self, events: None)


@lf.trace
def _five_logs() -> None:
    for _ in range(5):
        lf.info("inside")


def test_a_submission_lost_inside_submit_is_unaccounted(monkeypatch, capsys) -> None:
    """FR-003 AC-3: the meeting of the caller's hand-overs with the queue's count."""
    _fast_worker(GatedSink())
    _lose_inside_submit(monkeypatch)
    _five_logs()
    assert lf.health().events.unaccounted == 7
    lf.shutdown(1)
    lines = _lines(capsys.readouterr().err)
    assert len(lines) == 1 and "lost 7 event(s)" in lines[0], lines


def test_the_shutdown_line_is_written_once_per_distinct_imbalance(monkeypatch, capsys) -> None:
    """FR-003 AC-5."""
    _fast_worker(GatedSink())
    _lose_inside_submit(monkeypatch)
    _five_logs()
    lf.shutdown(1)
    lf.shutdown(1)
    assert len(_lines(capsys.readouterr().err)) == 1
    _plain()
    lf.shutdown(1)
    lines = _lines(capsys.readouterr().err)
    assert len(lines) == 1 and "lost 9 event(s)" in lines[0], lines


def test_a_negative_remainder_is_published_as_zero_and_counted() -> None:
    """FR-003 AC-6: a reader never meets a negative count; the deficit is in ``unaccounted``."""
    with decorator._loss_lock:
        decorator._in_span_lost += 2
    try:
        e = ledger()
        assert (e.open_spans, e.unaccounted) == (0, 2), e
    finally:
        with decorator._loss_lock:
            decorator._in_span_lost -= 2


def test_a_fault_in_the_check_is_absorbed_and_shutdown_still_closes(monkeypatch, capsys) -> None:
    """FR-003 AC-7."""
    closed: list[bool] = []
    sink = GatedSink()
    sink.close = lambda: closed.append(True)  # type: ignore[method-assign]
    lf.configure(sink=sink)
    _plain()

    def broken() -> object:
        raise RuntimeError("check fault")

    monkeypatch.setattr(_lifecycle, "_ledger_reading", broken)
    lf.shutdown(2)
    err = capsys.readouterr().err
    assert closed == [True]
    assert "checking the event ledger (RuntimeError)" in err, err
    assert "check fault" not in err


def test_an_expired_shutdown_with_a_batch_in_the_sink_balances(capsys) -> None:
    """FR-003 AC-8: the batch is in ``in_flight``, which a live drain may still settle."""
    sink = GatedSink()
    worker = _fast_worker(sink)
    sink.gate.clear()
    _plain()
    assert sink.entered.wait(5)
    lf.shutdown(0.2)
    e = ledger()
    sink.gate.set()
    worker._thread.join(5)
    assert e.unaccounted == 0 and e.in_flight == 2, e
    assert not _lines(capsys.readouterr().err)
    _balanced(2)


def test_shutdown_inside_a_traced_function_balances(capsys) -> None:
    """FR-003 AC-9: events buffered on the caller's open span are legitimately pending."""
    lf.configure(sink=GatedSink())
    seen: dict[str, object] = {}

    @lf.trace
    def work() -> None:
        lf.info("buffered")
        lf.shutdown(2)
        seen["ledger"] = ledger()

    work()
    e = seen["ledger"]
    assert e.unaccounted == 0 and e.open_spans == 2, e  # type: ignore[attr-defined]
    assert not _lines(capsys.readouterr().err)


def test_refused_orphans_racing_shutdown_write_no_false_line(capsys) -> None:
    """FR-003 AC-4: 1,000 shutdowns and 1,000 readings while six threads log refused orphans.

    The same race runs with a no-op call between each read of a pass and with the sink
    accepting, in ``test_event_ledger_threads.py``'s orphan-residual test; this is the shutdown
    half, where a false positive would be a line.
    """
    from conftest import run_concurrently
    from log_foundry import _ledger

    for between, outcome in ((None, Outcome.REJECTED), (lambda n, i: None, Outcome.REJECTED),
                             (None, Outcome.ACCEPTED), (lambda n, i: None, Outcome.ACCEPTED)):
        ledger_check.begin()
        lf.configure(sink=PlanSink(every(outcome)))
        _ledger._between = between
        stop = threading.Event()
        readings: list[int] = []

        def work(index: int, iteration: int, stop=stop, readings=readings) -> None:
            if index == 6:
                try:
                    for _ in range(1_000):
                        _lifecycle._shutdown_worker(1)
                        readings.append(ledger().unaccounted)
                finally:
                    stop.set()
                return
            while not stop.is_set():
                lf.info("orphan")

        try:
            errors = run_concurrently(work, 7)
        finally:
            _ledger._between = None
        assert not errors, errors
        assert not _lines(capsys.readouterr().err), outcome
        assert len(readings) == 1_000 and not any(readings), outcome


# ── FR-004 ───────────────────────────────────────────────────────────────────────────────


def test_health_built_by_hand_carries_an_empty_ledger() -> None:
    """FR-004 AC-1."""
    assert lf.Health(queued=0, dropped=0, failed_batches=0).events == lf.EventLedger()


def test_a_process_that_never_logged_reads_an_empty_ledger() -> None:
    """FR-004 AC-1, asked of ``health()``, which builds no worker to answer."""
    assert lf.health().events == lf.EventLedger()
    assert _lifecycle._state.worker_exists() is None


def test_event_ledger_is_frozen_keyword_only_and_one_object_at_both_paths() -> None:
    """FR-004 AC-2."""
    import dataclasses

    assert lf.EventLedger is worker_mod.EventLedger
    params = dataclasses.fields(lf.EventLedger)
    assert all(f.type == "int" and f.default == 0 and f.kw_only for f in params), params
    with pytest.raises(dataclasses.FrozenInstanceError):
        lf.EventLedger().accepted = 1  # type: ignore[misc]
    with pytest.raises(TypeError):
        lf.EventLedger(1)  # type: ignore[misc]
    e = lf.EventLedger(open_spans=1, queued=2, in_flight=3, stranded=4, drain_lost=5)
    assert (e.pending, e.lost) == (6, 9)


def test_the_two_loss_fields_read_the_same_in_the_ledger(monkeypatch, capsys) -> None:
    """FR-004 AC-4, in a process that has lost one of each."""
    lf.configure(sink=GatedSink())

    def boom(*args: object, **kwargs: object) -> dict:
        raise RuntimeError("cannot build")

    monkeypatch.setattr(api, "build_event", boom)
    lf.info("orphan")
    _logs_once()
    monkeypatch.undo()
    capsys.readouterr()
    health = lf.health()
    assert (health.orphan_lost, health.in_span_lost) == (1, 1)
    assert health.events.orphan_lost == health.orphan_lost
    assert health.events.in_span_lost == health.in_span_lost


# ── FR-005, the forked child ─────────────────────────────────────────────────────────────


def _terms(e) -> str:
    return " ".join(f"{f}={getattr(e, f)}" for f in (
        "accepted", "open_spans", "queued", "in_flight", "delivered", "skipped", "in_span_lost",
        "orphan_lost", "overflow_lost", "batch_lost", "sink_lost", "drain_lost", "stranded",
        "unaccounted",
    ))


def test_a_forked_child_starts_with_what_it_holds(capsys) -> None:
    """FR-005 AC-6: ``p15_fork_child_counters.py``, with the child's balance asserted (m6)."""
    from test_fork_lifecycle import run_in_child

    sink = PlanSink(lambda batch: [
        Outcome.REJECTED if e["message"] == "refuse me" else Outcome.ACCEPTED for e in batch
    ])
    lf.configure(sink=sink)
    lf.info("refuse me")
    for _ in range(5):
        _plain()
    assert lf.flush(5)
    child: dict[str, object] = {}

    @lf.trace
    def work() -> None:
        lf.info("buffered before the fork")

        def in_child() -> str:
            return _terms(lf.health().events)

        child["at_fork"] = run_in_child(in_child).output
        child["parent"] = lf.health().events.orphan_lost

    work()
    capsys.readouterr()
    assert child["at_fork"] == _terms(lf.EventLedger(accepted=2, open_spans=2)), child
    assert child["parent"] == 1


def _probe(name: str, *args: str) -> str:
    """Runs one of the audit's core-pipeline probes in a fresh interpreter against this tree."""
    import os
    import pathlib
    import subprocess
    import sys

    root = pathlib.Path(__file__).resolve().parents[1]
    probe = root / "docs/audits/2026-11-14-probes/core-pipeline" / name
    env = {**os.environ, "PYTHONPATH": str(root / "src")}
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-B", str(probe), *args], capture_output=True, text=True, timeout=120,
        env=env, check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    return done.stdout


def _ledger_on(line: str) -> dict[str, int]:
    """The ``EventLedger(...)`` a probe printed on ``line``, as a dict."""
    import re

    body = line.split("EventLedger(", 1)[1].split(")", 1)[0]
    return {key: int(value) for key, value in re.findall(r"(\w+)=(-?\d+)", body)}


def test_p15_reads_a_forked_childs_ledger_as_what_it_holds() -> None:
    """FR-005 AC-6: the child at the fork, the child after its span closes, the parent's loss."""
    out = _probe("p15_fork_child_counters.py").splitlines()
    at_fork = _ledger_on(next(line for line in out if line.startswith("child  at the fork")))
    after = _ledger_on(next(line for line in out if line.startswith("child  after")))
    parent = next(line for line in out if line.startswith("parent after"))
    zeros = dict.fromkeys(at_fork, 0)
    assert at_fork == {**zeros, "accepted": 2, "open_spans": 2}, at_fork
    assert after["accepted"] == 3 and after["delivered"] == 3 and after["unaccounted"] == 0, after
    assert "orphan_lost=1 " in parent, parent


def test_a_child_forked_with_the_registration_lock_held_does_not_wait() -> None:
    """FR-005 AC-7: the roster replaces the lock in the child, and the child's ledger restarts."""
    from log_foundry import _ledger
    from test_fork_lifecycle import run_in_child

    lf.configure(sink=GatedSink())
    _plain()
    taken, done = threading.Event(), threading.Event()

    def holder() -> None:
        with _ledger._registry.lock:
            taken.set()
            done.wait(30)

    thread = threading.Thread(target=holder)
    thread.start()
    assert taken.wait(5)
    try:

        def in_child() -> str:
            before = lf.health().events
            _plain()
            lf.info("orphan from the child")
            after = lf.health().events
            return f"{before.accepted} {after.accepted} {after.unaccounted}"

        child = run_in_child(in_child, timeout=10)
    finally:
        done.set()
        thread.join(5)
    assert child.output == "0 3 0", child.output


def test_a_retired_parents_child_strands_its_submissions() -> None:
    """FR-005 AC-8: a fork does not undo a ``shutdown()``, and what the child logs is stranded."""
    from test_fork_lifecycle import run_in_child

    lf.configure(sink=GatedSink())
    _plain()
    lf.shutdown(2)

    def in_child() -> str:
        _plain()
        e = lf.health().events
        return f"{e.stranded} {e.queued} {e.unaccounted}"

    child = run_in_child(in_child, timeout=10)
    assert child.output == "2 0 0", child.output
