"""SPEC-088 FR-006: the suite's balance fixture, held to a corpus like any gate.

Each check of `ledger_check.problems` is made to fire on a state planted for it, and asserted by
its reason; the states the identity admits — a stalled sink, a killed drain, an `lf` run — are
asserted silent. Each planted state is cleared before the test ends, so the fixture that runs
after it reads a balanced process.
"""

import ast
import collections
import os
import pathlib
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import ledger_check
import log_foundry as lf
from ledger_sinks import GatedSink
from log_foundry import _ledger, decorator
from log_foundry import worker as worker_mod

_ROOT = pathlib.Path(__file__).resolve().parents[1]


@lf.trace
def _plain() -> None:
    pass


def _reasons() -> list[str]:
    return ledger_check.problems()


def test_unaccounted_fires_with_its_count() -> None:
    with decorator._loss_lock:
        decorator._in_span_lost += 2
    try:
        assert any(r.startswith("unaccounted=2") for r in _reasons()), _reasons()
    finally:
        ledger_check.begin()


def test_a_negative_orphan_residual_fires_though_unaccounted_is_zero() -> None:
    with decorator._loss_lock:
        decorator._orphan_lost += 1
    try:
        reasons = _reasons()
        assert reasons == ["orphan residual=-1"], reasons
    finally:
        ledger_check.begin()


def test_an_open_spans_remainder_no_open_span_holds_fires() -> None:
    _ledger.cell().span += 1
    try:
        reasons = _reasons()
        assert reasons == ["open_spans=1 with 0 buffered on this context's open spans"], reasons
    finally:
        ledger_check.begin()


def test_a_span_left_open_on_the_tests_own_context_fires() -> None:
    lf.configure(sink=GatedSink())
    span, token, scope = decorator._begin("left open", None)
    try:
        reasons = _reasons()
        assert reasons == ["open_spans: the test left 1 span(s) open on its own context"], reasons
    finally:
        decorator._end(span, token, scope, "ok", None)
    assert lf.flush(5)
    assert _reasons() == []


def _stalled_worker() -> tuple[GatedSink, worker_mod.Worker]:
    sink = GatedSink()
    sink.gate.clear()
    worker = worker_mod.Worker(sink, batch_size=1, flush_interval=60.0)
    worker.submit([{"message": "one"}])
    assert sink.entered.wait(5)
    return sink, worker


def _ended_worker() -> worker_mod.Worker:
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1)
    worker = worker_mod.Worker(sink, batch_size=1, flush_interval=60.0)
    worker.submit([{"message": "one"}])
    worker._thread.join(5)
    assert not worker._thread.is_alive()
    return worker


def test_a_stalled_sink_and_a_killed_drain_are_silent() -> None:
    sink, worker = _stalled_worker()
    worker.submit([{"message": "two"}])
    try:
        assert ledger_check.worker_problems(worker) == []
        assert ledger_check.worker_problems(_ended_worker()) == []
    finally:
        sink.gate.set()
        worker.stop(5)


def test_an_uncounted_queue_entry_fires() -> None:
    """A live drain's entry no count admits is still reported once the settle bound is spent."""
    sink, worker = _stalled_worker()
    try:
        with worker._queue.mutex:
            worker._queue.queue.append([{"message": "slipped in"}])
        found = ledger_check.worker_problems(worker, settle=0.1)
        assert found == ["a worker's queued+stranded reads 0 with 1 events in its queue"], found
        with worker._queue.mutex:
            worker._queue.queue.pop()
    finally:
        sink.gate.set()
        worker.stop(5)


def test_events_held_by_an_ended_drain_fire() -> None:
    worker = _ended_worker()
    worker._in_flight = 2
    try:
        found = ledger_check.worker_problems(worker)
        assert "a worker's ended drain holds 2 events in flight" in found, found
    finally:
        worker._in_flight = 0


def test_a_negative_drain_residual_fires() -> None:
    """An exit counted twice at rest reads the drain residual below zero (diff review frame 2)."""
    worker = _ended_worker()
    worker._delivered += 1
    try:
        assert ledger_check.worker_problems(worker) == ["a worker's drain residual is -1"]
    finally:
        worker._delivered -= 1


def test_a_positive_drain_residual_fires() -> None:
    worker = _ended_worker()
    worker._taken += 1
    worker._queue.entered += 1
    try:
        assert ledger_check.worker_problems(worker) == ["a worker's drain residual is 1"]
    finally:
        worker._taken -= 1
        worker._queue.entered -= 1


def test_a_negative_in_flight_fires() -> None:
    worker = _ended_worker()
    worker._in_flight = -1
    try:
        assert "a worker's in_flight reads -1" in ledger_check.worker_problems(worker)
    finally:
        worker._in_flight = 0


_PAUSES = {
    "between get and _hold": "a worker's queued+stranded reads 2 with 0 events in its queue",
    "inside _settle": "a worker's drain residual is -2",
}
"""Each point a live, balancing drain can be paused at, and what one reading there reports."""


def _paused_drain(monkeypatch, where: str) -> tuple[worker_mod.Worker, threading.Event]:
    """A live worker whose drain is held at ``where`` with a two-event submission, and its release.

    ``between get and _hold`` is the window `main` went red in at `97a03f45`: the queue is empty
    and the take not yet counted. ``inside _settle`` runs the real `_settle` twice, the exits with
    no gauge change and then the gauge with no exits, and pauses between: the exits are written
    and ``in_flight`` not yet lowered.
    """
    paused, release = threading.Event(), threading.Event()
    if where == "between get and _hold":
        real_hold = worker_mod.Worker._hold

        def hold(self, events) -> None:  # type: ignore[no-untyped-def]
            paused.set()
            release.wait(10)
            real_hold(self, events)

        monkeypatch.setattr(worker_mod.Worker, "_hold", hold)
    else:
        real_settle = worker_mod.Worker._settle

        def settle(self, size, **exits) -> None:  # type: ignore[no-untyped-def]
            real_settle(self, 0, **exits)
            paused.set()
            release.wait(10)
            real_settle(self, size)

        monkeypatch.setattr(worker_mod.Worker, "_settle", settle)
    worker = worker_mod.Worker(GatedSink(), batch_size=1, flush_interval=60.0)
    worker.submit([{"message": "one"}, {"message": "two"}])
    assert paused.wait(5)
    return worker, release


def _counting_readings(monkeypatch) -> tuple[list[list[str]], threading.Event]:
    """Records every reading `worker_problems` makes, and an event set once the first is made."""
    readings: list[list[str]] = []
    first = threading.Event()
    real_read = ledger_check._read_worker

    def read(worker):  # type: ignore[no-untyped-def]
        got = real_read(worker)
        readings.append(got[-1])
        first.set()
        return got

    monkeypatch.setattr(ledger_check, "_read_worker", read)
    return readings, first


@pytest.mark.parametrize("where", sorted(_PAUSES))
def test_one_reading_of_a_paused_live_drain_misreads_it(monkeypatch, where: str) -> None:
    """The flake, reproduced: a single reading, as the fixture made before the settle, fails."""
    worker, release = _paused_drain(monkeypatch, where)
    try:
        assert ledger_check.worker_problems(worker, settle=0) == [_PAUSES[where]]
    finally:
        release.set()
        worker.stop(5)


@pytest.mark.parametrize("where", sorted(_PAUSES))
def test_a_paused_live_drain_released_within_the_bound_reads_balanced(
    monkeypatch, where: str
) -> None:
    """The fix: the drain's first reading fails, it is read again, and once released it passes."""
    worker, release = _paused_drain(monkeypatch, where)
    readings, first = _counting_readings(monkeypatch)
    got: list[list[str]] = []
    reader = threading.Thread(target=lambda: got.append(ledger_check.worker_problems(worker)))
    reader.start()
    try:
        assert first.wait(5)
    finally:
        release.set()
        reader.join(5)
    assert not reader.is_alive(), "the settle outlived its bound"
    assert readings[0] == [_PAUSES[where]], readings
    assert got == [[]], got
    worker.stop(5)


@pytest.mark.parametrize("where", sorted(_PAUSES))
def test_a_drain_paused_past_the_bound_is_reported_with_its_reason(monkeypatch, where: str) -> None:
    worker, release = _paused_drain(monkeypatch, where)
    readings, _ = _counting_readings(monkeypatch)
    try:
        assert ledger_check.worker_problems(worker, settle=0.1) == [_PAUSES[where]]
        assert len(readings) > 1, "a live drain that failed was read once"
    finally:
        release.set()
        worker.stop(5)


def _parked_worker() -> worker_mod.Worker:
    """An idle live worker whose drain has been seen waiting in ``queue.get``."""
    worker = worker_mod.Worker(GatedSink(), batch_size=1, flush_interval=60.0)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with worker._queue.mutex:
            if worker._queue.not_empty._waiters:
                return worker
        time.sleep(0.001)
    raise AssertionError("the drain never parked in get")


def test_a_parked_drain_is_read_once_though_it_would_correct_itself(monkeypatch) -> None:
    """A drain at rest in ``get`` is judged on one reading: the lazy-hold class stays caught.

    The diff review's mutant moved `_hold` from the take to the top of `_emit_pending`, so events
    sat in the drain's ``pending`` counted ``queued`` while it waited in ``get``, and the drain
    corrected the count itself when its flush interval emitted them. A settle that re-read a
    parked drain hid it. Here the count is planted on a parked drain and corrected after the first
    reading, as that drain would; a check that read it again would report it balanced.
    """
    worker = _parked_worker()
    readings, _ = _counting_readings(monkeypatch)
    real_read = ledger_check._read_worker
    planted = [True]

    def read_then_correct(w):  # type: ignore[no-untyped-def]
        got = real_read(w)
        if planted[0]:
            w._queue.entered -= 2
            planted[0] = False
        return got

    monkeypatch.setattr(ledger_check, "_read_worker", read_then_correct)
    worker._queue.entered += 2
    try:
        found = ledger_check.worker_problems(worker)
    finally:
        if planted[0]:
            worker._queue.entered -= 2
        worker.stop(5)
    assert found == ["a worker's queued+stranded reads 2 with 0 events in its queue"], found
    assert len(readings) == 1, readings


def test_the_terms_are_read_under_the_queues_mutex(monkeypatch) -> None:
    """A drain notified at teardown, as on `main` at `97a03f45`, reads balanced (frame 2's M11).

    The test holds the mutex, queues a submission and notifies the parked drain, which cannot take
    it until the test lets go. Read under the mutex, the terms wait for it too, and whichever of
    the reading and the drain takes the mutex first, the reading is consistent or re-read. A
    reading whose terms were taken before the mutex would see ``taken`` from before the take: the
    wrapped `_drain_terms` then holds those stale terms until the drain has taken, held, emitted
    and parked again, so the reading finds a parked drain and judges the stale terms at once. The
    wrapper waits only when it runs before the test has let go, which a mutex-scoped read never
    does, so the correct path costs the bounded wait for a read that never comes.
    """
    worker = _parked_worker()
    q = worker._queue
    real_terms = worker._drain_terms
    released, read_early = threading.Event(), threading.Event()

    def terms():  # type: ignore[no-untyped-def]
        got = real_terms()
        if not released.is_set():
            read_early.set()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with q.mutex:
                    if worker._delivered == 2 and q.not_empty._waiters:
                        break
                time.sleep(0.001)
        return got

    monkeypatch.setattr(worker, "_drain_terms", terms)
    got: list[list[str]] = []
    reader = threading.Thread(target=lambda: got.append(ledger_check.worker_problems(worker)))
    with q.mutex:
        q._put([{"message": "one"}, {"message": "two"}])
        q.unfinished_tasks += 1
        q.not_empty.notify()
        reader.start()
        read_early.wait(0.5)
        released.set()
    reader.join(10)
    worker.stop(5)
    assert not reader.is_alive(), "the reading outlived its bound"
    assert got == [[]], f"the reading judged terms taken outside the mutex: {got}"


class _PausingWaiters(collections.deque):  # type: ignore[type-arg]
    """A condition's waiter deque whose truth, asked once by one thread off the mutex, pauses."""

    def __init__(self, items, mutex, asked, go) -> None:  # type: ignore[no-untyped-def]
        super().__init__(items)
        self.mutex, self.asked, self.go = mutex, asked, go
        self.reader: threading.Thread | None = None

    def __bool__(self) -> bool:
        answer = len(self) > 0
        if threading.current_thread() is self.reader and not self.asked.is_set():
            self.asked.set()
            if not self.mutex.locked():
                self.go.wait(5)
        return answer


def test_parked_is_read_under_the_queues_mutex(monkeypatch) -> None:
    """A drain woken between ``parked`` and the terms is not judged as parked (frame 2's M9).

    The waiter deque's truth is asked once by the reader. Asked under the mutex, as it must be,
    it answers at once and the reading is the parked drain at rest, balanced. Asked before the
    mutex, it pauses after answering "parked" while the test submits: the drain wakes, takes the
    submission and is held in `_hold`, so the reading would judge the take-to-`_hold` window, the
    CI shape, as a drain at rest. Nothing here races: the pause is entered only off the mutex,
    and the test moves on each step's event.
    """
    worker = _parked_worker()
    q = worker._queue
    asked, go, paused, release = (threading.Event() for _ in range(4))
    real_hold = worker_mod.Worker._hold

    def hold(self, events) -> None:  # type: ignore[no-untyped-def]
        paused.set()
        release.wait(10)
        real_hold(self, events)

    monkeypatch.setattr(worker_mod.Worker, "_hold", hold)
    with q.mutex:
        waiters = _PausingWaiters(q.not_empty._waiters, q.mutex, asked, go)
        q.not_empty._waiters = waiters
    got: list[list[str]] = []
    reader = threading.Thread(target=lambda: got.append(ledger_check.worker_problems(worker)))
    waiters.reader = reader
    try:
        reader.start()
        assert asked.wait(5)
        worker.submit([{"message": "one"}, {"message": "two"}])
        assert paused.wait(5)
        go.set()
        reader.join(10)
    finally:
        go.set()
        release.set()
        worker.stop(5)
        with q.mutex:
            q.not_empty._waiters = collections.deque(q.not_empty._waiters)
    assert not reader.is_alive(), "the reading outlived its bound"
    assert got == [[]], f"a drain woken before the mutex was judged as parked: {got}"


def test_one_settle_bound_is_shared_by_every_worker_a_teardown_reads(monkeypatch) -> None:
    """Three drains held out of ``get`` and out of balance spend one bound between them, not three.

    The bound is driven by a fake clock that only the settle's pause advances, so what is asserted
    is the time the settle asked for, with no wall clock in it.
    """
    now = [0.0]
    monkeypatch.setattr(ledger_check, "_clock", lambda: now[0])
    monkeypatch.setattr(ledger_check, "_sleep", lambda s: now.__setitem__(0, now[0] + s))
    lock, entered, all_paused, release = threading.Lock(), [0], threading.Event(), threading.Event()
    real_hold = worker_mod.Worker._hold

    def hold(self, events) -> None:  # type: ignore[no-untyped-def]
        with lock:
            entered[0] += 1
            if entered[0] == 3:
                all_paused.set()
        release.wait(10)
        real_hold(self, events)

    monkeypatch.setattr(worker_mod.Worker, "_hold", hold)
    workers = [worker_mod.Worker(GatedSink(), batch_size=1, flush_interval=60.0) for _ in range(3)]
    for worker in workers:
        worker.submit([{"message": "one"}, {"message": "two"}])
    try:
        assert all_paused.wait(5)
        found = ledger_check.problems()
    finally:
        release.set()
        for worker in workers:
            worker.stop(5)
    assert found == [_PAUSES["between get and _hold"]] * 3, found
    assert now[0] == pytest.approx(ledger_check.SETTLE, abs=2 * ledger_check._PAUSE), now[0]


def test_a_dead_drain_and_a_first_passing_reading_are_read_once(monkeypatch) -> None:
    """Neither waits: an ended drain will not move again, and a stalled sink balances at once."""
    readings, _ = _counting_readings(monkeypatch)
    worker = _ended_worker()
    worker._in_flight = 2
    try:
        assert ledger_check.worker_problems(worker) == [
            "a worker's ended drain holds 2 events in flight",
            "a worker's drain residual is -2",
        ]
    finally:
        worker._in_flight = 0
    assert len(readings) == 1, readings
    sink, stalled = _stalled_worker()
    try:
        assert ledger_check.worker_problems(stalled) == []
        assert len(readings) == 2, readings
    finally:
        sink.gate.set()
        stalled.stop(5)


def test_an_lf_run_is_silent_and_a_meeting_residual_past_it_fires(lf) -> None:
    """The `lf` hand-over is allowed up to the events it emitted directly, and no further."""

    @lf.trace
    def work() -> None:
        lf.info("inside")

    work()
    assert ledger_check._direct[0] == 3
    assert _reasons() == []
    span, token, scope = decorator._begin("swept", None)
    try:
        lf.info("swept by a real worker")
        assert lf.flush(5)
    finally:
        decorator._end(span, token, scope, "ok", None)
    assert _reasons() == [], "a real hand-over meets the queue, an lf one is allowed for"
    _ledger.cell().handed += 4
    try:
        reasons = _reasons()
        assert any("exceeds the lf fixture's 4" in r for r in reasons), reasons
    finally:
        ledger_check.begin()


@pytest.mark.parametrize(
    ("found", "marked", "expected"),
    [
        ([], False, None),
        (["unaccounted=1"], False, "the event ledger does not balance: unaccounted=1"),
        (["unaccounted=1"], True, None),
        ([], True, "marked ledger_exempt but the event ledger balances: remove the marker"),
    ],
)
def test_judge(found: list[str], marked: bool, expected: str | None) -> None:
    assert ledger_check.judge(found, marked) == expected


def _run_suite_on(tmp_path: pathlib.Path, body: str) -> subprocess.CompletedProcess[str]:
    """Runs pytest on one generated test file with this suite's conftest loaded as a plugin."""
    test = tmp_path / "test_generated.py"
    test.write_text(textwrap.dedent(body))
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(_ROOT / "tests"), str(_ROOT / "src")])}
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "pytest", "-p", "conftest", "-p", "no:cacheprovider", "-n", "0",
         "-q", "-c", str(_ROOT / "pyproject.toml"), "--rootdir", str(tmp_path), str(test)],
        capture_output=True, text=True, timeout=120, env=env, cwd=tmp_path, check=False,
    )


def test_a_span_left_open_fails_the_fixture_and_a_closed_one_passes(tmp_path) -> None:
    """FR-006 AC-4, through the real fixture.

    The closed one runs first: a span left open stays current for every later test in the same
    context, which is the leak the check exists to catch.
    """
    done = _run_suite_on(tmp_path, '''
        import log_foundry as lf
        from log_foundry import decorator

        def test_closes_it():
            from log_foundry.sinks.null import NullSink
            lf.configure(sink=NullSink())
            span, token, scope = decorator._begin("closed", None)
            decorator._end(span, token, scope, "ok", None)
            assert lf.flush(5)

        def test_leaves_it_open():
            from log_foundry.sinks.null import NullSink
            lf.configure(sink=NullSink())
            decorator._begin("left open", None)
    ''')
    out = done.stdout + done.stderr
    assert "2 passed, 1 error" in out, out
    assert "ERROR at teardown of test_leaves_it_open" in out, out
    assert "ERROR at teardown of test_closes_it" not in out, out
    assert "the test left 1 span(s) open on its own context" in out, out


def test_a_marked_test_that_balances_fails(tmp_path) -> None:
    """FR-006 AC-5, through the real fixture: the marker is strict."""
    done = _run_suite_on(tmp_path, '''
        import pytest
        import log_foundry as lf

        @pytest.mark.ledger_exempt("claims an imbalance it does not have")
        def test_balances():
            from log_foundry.sinks.null import NullSink
            lf.configure(sink=NullSink())
            lf.info("orphan")
    ''')
    out = done.stdout + done.stderr
    assert "ERROR at teardown of test_balances" in out, out
    assert "marked ledger_exempt but the event ledger balances" in out, out


def _lf_users() -> set[tuple[str, str]]:
    """Every test function in the suite that takes the `lf` fixture, by file and name."""
    found: set[tuple[str, str]] = set()
    for path in sorted((_ROOT / "tests").glob("test_*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and any(
                arg.arg == "lf" for arg in node.args.args
            ):
                found.add((path.name, node.name))
    return found


def _marked() -> set[tuple[str, str]]:
    """Every test function carrying the `ledger_exempt` marker, by file and name."""
    found: set[tuple[str, str]] = set()
    for path in sorted((_ROOT / "tests").glob("test_*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and any(
                ast.unparse(decorator).startswith("pytest.mark.ledger_exempt(")
                for decorator in node.decorator_list
            ):
                found.add((path.name, node.name))
    return found


def test_no_test_using_lf_carries_the_marker() -> None:
    """FR-006 AC-6, and AC-2's seven, by name."""
    marked = _marked()
    assert len(_lf_users()) > 50, "the scan reads the lf users"
    assert not marked & _lf_users(), marked & _lf_users()
    assert marked == {
        ("test_api.py", "test_an_in_span_log_is_unaffected_by_a_broken_sink"),
        ("test_decorator_sync.py", "test_a_raising_traced_call_leaves_no_reference_cycle"),
        ("test_delivery_paths.py", "test_the_workers_emit_loop_survives_a_huge_max_retries"),
        ("test_span_sweep.py", "test_two_threads_sweeping_one_shared_span_deliver_each_event_once"),
        ("test_synchronous_loss.py", "test_the_increment_happens_under_the_lock"),
        ("test_synchronous_loss.py", "test_the_lock_is_a_module_global_the_fork_walk_can_reach"),
        ("test_task_outliving_span.py", "test_the_span_is_left_a_fresh_list_rather_than_a_cleared_one"),
    }, marked


def test_the_fixture_adds_no_wait_over_a_stalled_sink(monkeypatch) -> None:
    """FR-006 AC-7: the check reads and returns while a sink is stalled, and reads it balanced.

    Timed by the thread that runs it never having to be waited for: it runs on its own thread,
    which is joined with a generous bound, rather than against a clock with a tight one. The join
    is longer than `ledger_check.SETTLE`, so it alone would pass a check that spent the settle on
    the stall; the stalled worker being read exactly once is what says the check did not.
    """
    readings, _ = _counting_readings(monkeypatch)
    sink = GatedSink()
    lf.configure(sink=sink)
    sink.gate.clear()
    _plain()
    assert sink.entered.wait(5)
    _plain()
    got: list[list[str]] = []
    reader = threading.Thread(target=lambda: got.append(ledger_check.problems()))
    reader.start()
    reader.join(5)
    returned = not reader.is_alive()
    sink.gate.set()
    reader.join(5)
    assert returned, "the check waited on the stalled sink"
    assert got == [[]], got
    assert readings == [[]], "the stalled worker was read more than once"
    assert lf.flush(5)
