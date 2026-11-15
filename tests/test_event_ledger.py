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
