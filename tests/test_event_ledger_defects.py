"""SPEC-088 FR-002: the six accounting defects, one scenario per reproduction.

Each test ends by asserting the ledger places every event (FR-003 AC-10). The probes p03, p11,
p14, p16 and p17 are run unedited in a fresh interpreter against this tree and their printed
numbers read, p03, p14 and p17 with ``health().events`` read once the probe ends; ~~where a
criterion names one of the audit's probes, the probe is run~~ — p01, p05 and p08 are reproduced
in process instead, at their numbers, and p02 is both (diff review frame 1).
"""

import asyncio
import contextvars
import threading

import pytest

import log_foundry as lf
from conftest import run_concurrently
from ledger_sinks import GatedSink, ledger, reading
from log_foundry import _lifecycle, api, context, decorator
from test_event_ledger import _fast_worker, _ledger_on, _probe, _probe_ledger


def _placed(asked: int | None = None) -> None:
    """Asserts every event placed, and, when given, the count asked for."""
    seen = reading()
    assert seen.ledger.unaccounted == 0, seen.ledger
    assert seen.orphan_residual == 0, seen.orphan_residual
    if asked is not None:
        assert seen.ledger.accepted == asked, seen.ledger


@lf.trace
def _plain() -> None:
    pass


@lf.trace
def _logs_once() -> None:
    lf.info("inside")


# ── item 1, finding 16 ───────────────────────────────────────────────────────────────────


def test_overflow_after_shutdown_is_counted_once(capsys) -> None:
    """FR-002 AC-1: with no drain, N spans into a queue of C leave each in one term."""
    worker = _fast_worker(GatedSink(), max_queue=3)
    _lifecycle._shutdown_worker(2)
    assert not worker._thread.is_alive()
    for _ in range(5):
        _plain()
    capsys.readouterr()
    health = lf.health()
    e = health.events
    assert (health.dropped, health.submitted_after_shutdown) == (2, 3), health
    assert (e.overflow_lost, e.stranded) == (4, 6), e
    _placed(10)


def test_every_late_submission_is_in_one_of_two_counts(capsys) -> None:
    """FR-002 AC-2 as revised: no late submission is in both ``submitted_after_shutdown`` and ``dropped``.

    ~~``p03`` prints ``submitted_after_shutdown`` below ``queued`` plus ``dropped``~~ — struck by
    diff review frame 1: under load the final drain carries more late submissions than overflow,
    and the field counts those by design, so the comparison failed in 3 of 10 concurrent runs.
    Here every submission is made after the drain has ended, so each is late, and each lands in
    exactly one of the two counts whatever the load.
    """
    worker = _fast_worker(GatedSink(), max_queue=500)
    _lifecycle._shutdown_worker(2)
    assert not worker._thread.is_alive()
    errors = run_concurrently(lambda index, iteration: _plain(), 8, per_thread=300)
    assert not errors, errors
    capsys.readouterr()
    health = lf.health()
    e = health.events
    assert health.submitted_after_shutdown + health.dropped == 2_400, health
    assert (health.submitted_after_shutdown, health.dropped) == (500, 1_900), health
    assert (e.stranded, e.overflow_lost) == (1_000, 3_800), e
    _placed(4_800)


def test_p03_ends_with_every_event_placed() -> None:
    """FR-003 AC-10 for ``p03_shutdown_race.py 2000``, whose racing counts are only recorded."""
    _out, events = _probe_ledger("p03_shutdown_race.py", "2000")
    assert events["unaccounted"] == 0, events


# ── item 2, finding 17 ───────────────────────────────────────────────────────────────────


def test_p11_counts_every_assembly_fault() -> None:
    """FR-002 AC-3: ``p11_assembly.py`` (a) 2, (b) moved by 3, (c) nothing unaccounted."""
    out = _probe("p11_assembly.py").splitlines()
    first = next(line for line in out if line.startswith("(a)"))
    assert "in_span_lost=2 " in first, first
    assert any("in_span_lost moved by 3" in line for line in out), out
    third = next(line for line in out if "delivered+counted" in line)
    assert third.rstrip().endswith("unaccounted=0"), third


@pytest.mark.parametrize("kind", ["sync", "async"])
def test_a_traced_call_whose_span_cannot_be_built_moves_accepted_by_two(kind, monkeypatch, capsys) -> None:
    """FR-001 AC-1: the open guard credits the ``span.end`` the call will never ask for."""
    lf.configure(sink=GatedSink())

    def boom(span: object) -> dict:
        raise MemoryError

    monkeypatch.setattr(decorator, "start_event", boom)

    @lf.trace
    def work() -> None:
        pass

    @lf.trace
    async def work_async() -> None:
        pass

    if kind == "sync":
        work()
    else:
        asyncio.run(work_async())
    capsys.readouterr()
    e = ledger()
    assert (e.accepted, e.in_span_lost) == (2, 2), e
    _placed(2)


def test_a_failed_close_then_a_flush_from_a_context_holding_the_span(monkeypatch, capsys) -> None:
    """FR-002 AC-4: the guard takes the buffer, so a later flush cannot deliver what it counted."""
    lf.configure(sink=GatedSink())

    def boom(*args: object, **kwargs: object) -> dict:
        raise RuntimeError("no end event")

    monkeypatch.setattr(decorator, "end_event", boom)
    held: dict[str, contextvars.Context] = {}

    @lf.trace
    def work() -> None:
        lf.info("inside")
        held["ctx"] = contextvars.copy_context()

    work()
    monkeypatch.undo()
    assert held["ctx"].run(lf.flush, 5) is not None
    capsys.readouterr()
    e = ledger()
    assert (e.in_span_lost, e.delivered) == (3, 0), e
    _placed(3)


def test_a_span_built_but_not_pushed_delivers_both_boundary_events(monkeypatch, capsys) -> None:
    """FR-002 AC-5: a span that exists is closed and flushed as any span is."""
    sink = GatedSink()
    lf.configure(sink=sink)

    def refuse(span: object) -> object:
        raise RuntimeError("cannot push")

    monkeypatch.setattr(context, "push_span", refuse)
    _plain()
    monkeypatch.undo()
    assert lf.flush(5)
    capsys.readouterr()
    e = ledger()
    assert e.delivered == 2 and e.lost == 0, e
    assert sink.messages() == ["span.start", "span.end"]
    _placed(2)


# ── item 3, finding 7 ────────────────────────────────────────────────────────────────────


def test_a_dying_drain_debits_what_it_held(capsys) -> None:
    """FR-002 AC-6: ``p08_worker_death.py``'s scenario, the 20 held events in ``drain_lost``."""
    sink = GatedSink()
    sink.raise_on = lambda call, batch: SystemExit(1) if call == 2 else None
    lf.configure(sink=sink)
    for _ in range(10):
        _plain()
    assert lf.flush(10)
    for _ in range(10):
        _plain()
    lf.flush(10)
    worker = _lifecycle._state.worker_exists()
    worker._thread.join(5)
    for _ in range(30):
        _plain()
    err = capsys.readouterr().err
    e = ledger()
    assert (e.accepted, e.delivered, e.drain_lost, e.stranded) == (100, 20, 20, 60), e
    assert "lost 20 event(s) in 10 undrained event-list(s) it held" in err, err
    _placed(100)


# ── item 4, finding 9 ────────────────────────────────────────────────────────────────────


def test_an_expired_shutdown_leaves_the_batch_in_flight(capsys) -> None:
    """FR-002 AC-7 and AC-8: ``p02_shutdown_stalled.py``'s numbers, and the line that names them."""
    sink = GatedSink()
    lf.configure(sink=sink)
    sink.gate.clear()
    for _ in range(500):
        _plain()
    assert sink.entered.wait(5)
    lf.shutdown(0.5)
    at_expiry = ledger()
    err = capsys.readouterr().err
    sink.gate.set()
    _lifecycle._state.worker_exists()._thread.join(10)
    after = ledger()
    assert (at_expiry.queued, at_expiry.in_flight) == (980, 20), at_expiry
    assert at_expiry.unaccounted == 0, at_expiry
    assert "20 event(s) in flight (health().events.in_flight)" in err, err
    assert "counted when the emit returns" not in err
    assert after.delivered == 1000 and after.queued == 0 and after.in_flight == 0, after
    _placed(1000)


# ── item 5 ───────────────────────────────────────────────────────────────────────────────


def test_p14_delivers_span_end_on_both_rows() -> None:
    """FR-002 AC-9: the close builds ``span.end`` before it reads the buffer."""
    out, events = _probe_ledger("p14_end_races_sweep.py")
    rows = [line for line in out.splitlines() if line.startswith("[")]
    assert len(rows) == 2, rows
    assert events["unaccounted"] == 0, events
    for row in rows:
        received = row.split("sink got ", 1)[1].split("]", 1)[0]
        assert "'span.end'" in received, row


# ── item 6, the BaseException guards ─────────────────────────────────────────────────────


def test_p16_counts_an_interrupt_through_each_guard() -> None:
    """FR-002 AC-10: three ``KeyboardInterrupt`` rows, every event in one term, nothing on stderr."""
    out = _probe("p16_interrupt_through_a_guard.py")
    rows = [line for line in out.splitlines() if line.startswith("[")]
    assert len(rows) == 3 and all("caller got KeyboardInterrupt" in row for row in rows), rows
    last = _ledger_on(rows[-1])
    assert (last["accepted"], last["orphan_lost"], last["in_span_lost"]) == (5, 1, 4), last
    assert last["delivered"] == 0 and last["unaccounted"] == 0, last
    assert last["open_spans"] + last["queued"] + last["in_flight"] == 0, last
    assert "log-foundry" not in out


def test_an_interrupted_in_span_build_moves_in_span_lost(monkeypatch, capsys) -> None:
    """FR-002 AC-11: the interrupt reaches the caller unchanged, and the event is in a term."""
    lf.configure(sink=GatedSink())

    def interrupt(*args: object, **kwargs: object) -> dict:
        raise KeyboardInterrupt

    @lf.trace
    def work() -> None:
        monkeypatch.setattr(api, "build_event", interrupt)
        try:
            lf.info("interrupted")
        finally:
            monkeypatch.undo()

    with pytest.raises(KeyboardInterrupt):
        work()
    assert lf.flush(5) is not None
    assert capsys.readouterr().err == ""
    e = ledger()
    assert e.in_span_lost == 1, e
    _placed()


def test_a_missing_orphan_interrupt_debit_is_unaccounted(monkeypatch, capsys) -> None:
    """FR-003 AC-2: with the orphan guard's debit removed, the check sees the missing debit."""

    class Interrupts:
        def emit(self, batch: list) -> None:
            raise KeyboardInterrupt

        def close(self) -> None:
            pass

    lf.configure(sink=Interrupts())
    monkeypatch.setattr(api, "_note_orphan_loss", lambda: None)
    with pytest.raises(KeyboardInterrupt):
        lf.info("interrupted")
    e = ledger()
    assert (e.unaccounted, e.in_flight) == (1, 0), e
    lf.shutdown(1)
    lines = [line for line in capsys.readouterr().err.splitlines() if "does not balance" in line]
    assert len(lines) == 1 and "lost 1 event(s)" in lines[0], lines
    monkeypatch.undo()
    with decorator._loss_lock:
        decorator._orphan_lost += 1


def test_p17_logs_after_an_interrupted_close_keep_the_span_ids() -> None:
    """FR-002 AC-12: the handler row and the task row on the span's ids, the control row fresh."""
    import ast

    out, events = _probe_ledger("p17_log_after_an_interrupted_close.py")
    rows = [line for line in out.splitlines() if line.startswith("row ")]
    assert len(rows) == 3, rows
    assert events["unaccounted"] == 0, events
    for row in rows[:2]:
        span_ids = ast.literal_eval(row.split("span ", 1)[1].split(" |", 1)[0])
        got = ast.literal_eval(row.split("sink ", 1)[1])
        assert got and all(entry[1:] == span_ids for entry in got), row
    control_span = ast.literal_eval(rows[2].split("span ", 1)[1].split(" |", 1)[0])
    control_got = ast.literal_eval(rows[2].split("sink ", 1)[1])
    assert control_got and all(entry[1] != control_span[0] for entry in control_got), rows[2]


def test_a_log_after_an_interrupted_close_ships_as_an_orphan_on_the_spans_ids(monkeypatch) -> None:
    """FR-002, the abandoned span's orphan, in process: counted once, on the orphan path."""
    sink = GatedSink()
    lf.configure(sink=sink)
    ids: dict[str, str] = {}

    def interrupt(*args: object, **kwargs: object) -> dict:
        raise KeyboardInterrupt

    @lf.trace
    def work() -> None:
        span = context.current_span()
        ids["trace"], ids["span"] = span.trace_id, span.span_id  # type: ignore[union-attr]
        monkeypatch.setattr(decorator, "end_event", interrupt)

    try:
        work()
    except KeyboardInterrupt:
        monkeypatch.undo()
        lf.info("from the handler")
    orphan = [event for event in sink.events if event["message"] == "from the handler"]
    assert [(event["trace_id"], event["span_id"]) for event in orphan] == [
        (ids["trace"], ids["span"])
    ], orphan
    e = ledger()
    assert (e.accepted, e.in_span_lost, e.delivered) == (3, 2, 1), e
    _placed(3)


def test_a_flush_from_another_thread_during_the_close_still_ships_span_end() -> None:
    """FR-002 item 5 in process, the shape ``p14`` forces: a sweep inside ``end_event``."""
    sink = GatedSink()
    lf.configure(sink=sink)
    real = decorator.end_event
    held: dict[str, contextvars.Context] = {}

    def sweeping(*args: object, **kwargs: object) -> dict:
        thread = threading.Thread(target=lambda: held["ctx"].run(lf.flush, 5))
        thread.start()
        thread.join(5)
        return real(*args, **kwargs)

    @lf.trace
    def work() -> None:
        lf.info("in-span")
        held["ctx"] = contextvars.copy_context()
        decorator.end_event = sweeping  # type: ignore[assignment]

    try:
        work()
    finally:
        decorator.end_event = real  # type: ignore[assignment]
    assert lf.flush(5)
    assert sorted(sink.messages()) == ["in-span", "span.end", "span.start"]
    _placed(3)


def test_the_expiry_line_of_the_stalled_shutdown_probe_names_the_gauge() -> None:
    """FR-002 AC-8, against ``p04``'s shape in process, at a size the suite can afford.

    ``p04_exit.py 0.02 10000`` itself waits out the 30 s ``atexit`` budget; its run is recorded in
    the delivery doc. ``p02_shutdown_stalled.py`` writes the same line from an expired
    ``shutdown()`` on a sink stalled inside its emit.
    """
    out = _probe("p02_shutdown_stalled.py")
    line = next(line for line in out.splitlines() if "shutdown timed out" in line)
    assert "event(s) in flight (health().events.in_flight)" in line, line
    assert "counted when the emit returns" not in line, line
