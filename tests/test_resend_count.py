"""SPEC-095 FR-001: the engine counts each resend of an event a send may have landed.

A round marks a ``RETRYABLE`` event with ``RunContext.may_have_landed`` when its call may have
reached the destination; each later offer of that event in the run adds one to the run's
``resent``, which goes into ``Delivery.resent``, ``SinkCounters.resent_events``,
``SinkLosses.resent`` and so ``health().sink.resent``, with one ``_diag.resent`` line per run.
Every ``SinkLosses`` the package builds passes ``resent=``, held by an AST guard with no
allowlist, and every shipped ``losses()`` reports its counters' count, held by a roster derived
from SPEC-092's population. Serves invariants 2, 6, 7, 10 and 11.
"""

from __future__ import annotations

import ast
import dataclasses
import io
import pathlib
import re
import sys
from typing import TYPE_CHECKING, Any

import pytest

import conformance_backend as cb
import conformance_classes as cc
import conformance_doubles as cd
import log_foundry
from conftest import install_worker, retire_process
from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict
from log_foundry.worker import Worker
from test_public_surface import _sink_classes_with_emit
from verdict_sinks import Scripted, events, metaclass_named

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

A, R, J, U = Outcome.ACCEPTED, Outcome.RETRYABLE, Outcome.REJECTED, Outcome.UNKNOWN
SRC = pathlib.Path(log_foundry.__file__).parent
RESENT_LINE = re.compile(r"^log-foundry: resent (\d+) event\(s\) ")


def marking(outcomes: Iterable[Outcome] | Outcome, marks: Iterable[object] | None = None) -> Any:
    """A script step that marks ``marks`` (every index by default) and answers ``outcomes``.

    Args:
      outcomes: One outcome for every event, or one per event.
      marks: The indices to mark, as given, or ``None`` for every index of the round.

    Returns:
      A callable step for ``Scripted``.

    Raises:
      None.
    """

    def step(batch: list[dict[str, object]]) -> Verdict:
        _engine.run_context().may_have_landed(range(len(batch)) if marks is None else marks)
        if isinstance(outcomes, Outcome):
            return Verdict.uniform(outcomes, len(batch))
        return Verdict(outcomes=tuple(outcomes))

    return step


def run(sink: Any, batch: list[dict[str, object]], **kwargs: Any) -> _engine.Delivery:
    """One engine run of ``sink.deliver`` over ``batch`` into its counters, with no waits.

    Args:
      sink: A sink with ``deliver`` and ``log_foundry_counters``.
      batch: The events.
      **kwargs: Overrides of ``sink_retries``, ``worker_retries``, ``decline``.

    Returns:
      The run's ``Delivery``.

    Raises:
      BaseException: Whatever the run raises.
    """
    options: dict[str, Any] = {
        "sink_retries": 1,
        "worker_retries": 0,
        "stop": None,
        "decline": None,
    }
    options.update(kwargs)
    return _engine._run(sink, sink.deliver, sink.log_foundry_counters, batch, **options)


def resent_lines(text: str) -> list[int]:
    """The counts of every resend line in ``text``, in order.

    Args:
      text: Captured stderr.

    Returns:
      Each line's count.

    Raises:
      None.
    """
    return [int(m.group(1)) for line in text.splitlines() if (m := RESENT_LINE.match(line))]


class Idempotent(Scripted):
    """A scripted sink that declares itself idempotent and retries every exception."""

    log_foundry_idempotent = True

    def _retryable_error(self, err: Exception) -> bool:
        """Accepts every exception, so an escaped round is resent."""
        return True


class WriteHook(io.StringIO):
    """A stderr stand-in that records, at each write, what ``probe`` read then."""

    def __init__(self, probe: Callable[[], object]) -> None:
        super().__init__()
        self.probe = probe
        self.seen: list[tuple[str, object]] = []

    def write(self, text: str) -> int:
        """Records the write with the probe's reading, then keeps the text."""
        self.seen.append((text, self.probe()))
        return super().write(text)


# --- AC1, AC2, AC12: one resend, counted everywhere, on every path ---------------------------


def _worker(sink: object) -> Worker:
    worker = Worker(sink, batch_size=10, flush_interval=60.0)  # type: ignore[arg-type]
    install_worker(worker)
    return worker


def test_a_marked_round_then_accepted_counts_n_everywhere_and_writes_one_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC1: ``losses()``, ``health().sink`` and ``Delivery`` move by N; one line, counted first."""
    sink = Scripted([marking(R), A], max_retries=1)
    deliveries: list[_engine.Delivery] = []
    original = _engine._run

    def recording(*args: Any, **kwargs: Any) -> _engine.Delivery:
        delivery = original(*args, **kwargs)
        deliveries.append(delivery)
        return delivery

    monkeypatch.setattr(_engine, "_run", recording)
    hook = WriteHook(lambda: sink.log_foundry_counters.resent_events)
    monkeypatch.setattr(sys, "stderr", hook)
    log_foundry.configure(service="resend", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("m")

    work()
    assert log_foundry.flush(timeout=20.0)
    monkeypatch.setattr(sys, "stderr", sys.__stderr__)
    assert [d.resent for d in deliveries] == [3], "span.start, the event and span.end"
    assert sink.losses() == SinkLosses(dropped=0, failed=0, resent=3)
    health = log_foundry.health()
    assert health.sink == SinkLosses(dropped=0, failed=0, resent=3)
    assert health.failed_batches == 0
    lines = [(text, seen) for text, seen in hook.seen if text.startswith("log-foundry")]
    assert len(lines) == 1, hook.seen
    assert resent_lines(lines[0][0]) == [3]
    assert lines[0][1] == 3, "the count was committed before its line was written"


def _path_run(path: str) -> tuple[Scripted, str]:
    sink = Scripted([marking(R), A], max_retries=1)
    if path == "worker":
        worker = _worker(sink)
        worker.submit(events(1))
        assert worker.flush(timeout=20.0)
    elif path == "orphan":
        log_foundry.configure(service="resend", sink=sink)
        log_foundry.info("orphan")
    else:
        _engine.emit_via_deliver(sink, events(1))
    return sink, path


@pytest.mark.parametrize("path", ["worker", "orphan", "adapter"])
def test_every_path_counts_the_same_resend(path: str, capsys: pytest.CaptureFixture[str]) -> None:
    """AC2: one marked event through the worker, an orphan call and the adapter counts alike."""
    sink, _ = _path_run(path)
    assert sink.calls == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0, resent=1)
    assert resent_lines(capsys.readouterr().err) == [1]


def _ledger_moves(script: list[Any]) -> dict[str, int]:
    before = log_foundry.health().events
    sink = Scripted(script, max_retries=1)
    log_foundry.configure(service="resend", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("m")

    work()
    assert log_foundry.flush(timeout=20.0)
    after = log_foundry.health().events
    assert after.unaccounted == 0
    return {
        f.name: getattr(after, f.name) - getattr(before, f.name)
        for f in dataclasses.fields(after)
        if f.name not in ("unaccounted", "open_spans", "queued", "in_flight")
    }


def test_a_resend_moves_no_ledger_term() -> None:
    """AC12: the marked run's ledger moves equal the same run's without the mark."""
    marked = _ledger_moves([marking(R), A])
    plain = _ledger_moves([R, A])
    retire_process()
    assert marked == plain
    assert marked["delivered"] == 3


# --- AC3: which offers count -----------------------------------------------------------------


def test_two_marked_offers_count_two() -> None:
    """AC3: an event marked in two rounds and offered after each counts 2."""
    sink = Scripted([marking(R), marking(R), A])
    delivery = run(sink, events(2), sink_retries=2)
    assert delivery.attempts == 3 and delivery.resent == 4
    assert sink.losses().resent == 4


def test_a_mark_in_the_last_round_counts_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    """AC3: a mark in the run's last round is never followed by an offer, so it counts 0."""
    sink = Scripted([marking(R)])
    delivery = run(sink, events(2), sink_retries=0)
    assert delivery.attempts == 1 and delivery.failed and delivery.resent == 0
    assert sink.losses().resent == 0
    assert resent_lines(capsys.readouterr().err) == []


def test_a_mark_before_a_declined_round_counts_nothing() -> None:
    """AC3: the decline is asked after the wait and before the count, so a declined offer is 0."""
    sink = Scripted([marking(R), A])
    delivery = run(sink, events(2), decline=lambda longest: True)
    assert delivery.declined and delivery.attempts == 1 and delivery.resent == 0
    assert sink.losses().resent == 0


def test_a_mark_counts_only_the_offer_after_the_round_that_set_it() -> None:
    """AC3: marked in round 1, unmarked in round 2, so only round 2's offer counts, not round 3's.

    The rule is the last round that carried the event: a mark is not sticky across a later
    round that left the event resendable without marking it.
    """
    sink = Scripted([marking(R), R, A])
    delivery = run(sink, events(2), sink_retries=2)
    assert delivery.attempts == 3 and not delivery.failed
    assert delivery.resent == 2


def test_only_the_marked_events_count() -> None:
    """AC3: a mark names events, not the round: two of three marked count two per resend."""
    sink = Scripted([marking(R, [0, 2]), A])
    delivery = run(sink, events(3))
    assert delivery.resent == 2


# --- AC4, AC5: a mark that does not fit, and a round set aside -------------------------------


@pytest.mark.parametrize(
    ("outcomes", "marks"),
    [
        ([R, R, R], [True]),
        ([R, R, R], ["0"]),
        ([R, R, R], [-1]),
        ([R, R, R], [3]),
        ([A, R, R], [0]),
        ([J, R, R], [0]),
        ([U, R, R], [0]),
    ],
    ids=["a-bool", "a-str", "negative", "past-the-end", "accepted", "rejected", "unknown"],
)
def test_a_mark_that_does_not_fit_fails_the_round_and_counts_nothing(
    outcomes: list[Outcome], marks: list[object], capsys: pytest.CaptureFixture[str]
) -> None:
    """AC4: the round fails as an escaped exception does, and the run counts no resend."""
    sink = Scripted([marking(outcomes, marks), A])
    delivery = run(sink, events(3))
    assert delivery.attempts == 1 and delivery.raised == 3 and delivery.resent == 0
    assert sink.losses().resent == 0
    err = capsys.readouterr().err
    assert "absorbed a failure while delivering a batch (ValueError)" in err
    assert resent_lines(err) == []


def test_an_idempotent_sinks_misfit_mark_is_resent_and_counts_nothing() -> None:
    """AC4: even where the escaped round is resent, the misfit round's marks count nothing."""
    sink = Idempotent([marking([A, R], [0]), A])
    delivery = run(sink, events(2))
    assert delivery.attempts == 2 and delivery.resent == 0


def _raise_after_marking(batch: list[dict[str, object]]) -> Verdict:
    _engine.run_context().may_have_landed(range(len(batch)))
    raise ConnectionError("after marking")


def _marked_unreadable(batch: list[dict[str, object]]) -> object:
    _engine.run_context().may_have_landed(range(len(batch)))
    return None


@pytest.mark.parametrize(
    "second", [_raise_after_marking, _marked_unreadable], ids=["raised", "unreadable"]
)
def test_a_round_set_aside_leaves_no_mark_standing(second: Any) -> None:
    """AC5: round 1 marks (one counted offer); round 2 marks and is set aside; round 3 counts 0."""
    sink = Idempotent([marking(R), second, A])
    delivery = run(sink, events(2), sink_retries=2)
    assert delivery.attempts == 3
    assert delivery.resent == 2, "round 2's offer counts; round 3's does not"


def _wrapper_run(wrapper: Any, n: int, retries: int = 1) -> _engine.Delivery:
    """One engine run of a wrapper's ``deliver`` into its counters, with no waits.

    Args:
      wrapper: A wrapper over an engine sink.
      n: How many events.
      retries: The sink retries the run allows.

    Returns:
      The run's ``Delivery``.

    Raises:
      BaseException: Whatever the run raises.
    """
    return _engine._run(
        wrapper,
        wrapper.deliver,
        wrapper.log_foundry_counters,
        events(n),
        sink_retries=retries,
        worker_retries=0,
        stop=None,
        decline=None,
    )


def test_a_wrappers_unreadable_inner_round_leaves_no_mark_standing() -> None:
    """AC5 through a wrapper: the inner round marks and returns no verdict, so ``delegate``
    rewinds the call.

    ``FilteringSink`` declines event 0 of 3 and forwards two; the inner sink marks both and answers
    ``None``. The rewind discards the inner marks, so the wrapper's own verdict, which turns the
    forwarded events ``UNKNOWN``, is read as given. A mark left standing would sit on positions
    whose outcome is not ``RETRYABLE`` and fail the whole round as an escaped exception would.
    """
    inner = Scripted([_marked_unreadable])
    wrapper = FilteringSink(inner, predicate=lambda event: event["i"] != 0)
    delivery = _wrapper_run(wrapper, 3)
    assert delivery.raised == 0 and delivery.unknown == 2 and delivery.skipped == 1
    assert delivery.resent == 0


# --- AC6: a failed run --------------------------------------------------------------------


def test_a_failed_run_commits_its_resent_and_its_line(capsys: pytest.CaptureFixture[str]) -> None:
    """AC6: every round marked and none landed: the run fails, and its resends are still counted."""
    sink = Scripted([marking(R)])
    delivery = run(sink, events(2), sink_retries=2)
    assert delivery.failed and delivery.attempts == 3 and delivery.resent == 4
    assert sink.losses() == SinkLosses(dropped=0, failed=2, resent=4)
    assert resent_lines(capsys.readouterr().err) == [4]


# --- AC7: a wrapper moves the mark --------------------------------------------------------


def test_a_wrapper_moves_the_mark_into_its_own_positions() -> None:
    """AC7: ``FilteringSink`` declining event 0 of 3 over an inner marking its 0 counts 1."""
    inner = Scripted([marking([R, A], [0]), A])
    wrapper = FilteringSink(inner, predicate=lambda event: event["i"] != 0)
    delivery = _engine._run(
        wrapper,
        wrapper.deliver,
        wrapper.log_foundry_counters,
        events(3),
        sink_retries=1,
        worker_retries=0,
        stop=None,
        decline=None,
    )
    assert delivery.resent == 1
    assert [len(r) for r in inner.rounds] == [2, 1]
    assert inner.rounds[1][0]["i"] == 1, "the wrapper's event 1 was the one resent"
    assert wrapper.losses() == SinkLosses(dropped=0, failed=0, resent=1)


# --- AC8: the field --------------------------------------------------------------------------


def test_sinklosses_without_resent_equals_resent_zero_and_match_args_stay_empty() -> None:
    """AC8: the appended keyword defaults to 0 and leaves ``__match_args__`` empty."""
    assert SinkLosses(dropped=1, failed=2) == SinkLosses(dropped=1, failed=2, resent=0)
    assert SinkLosses.__match_args__ == ()
    assert [f.name for f in dataclasses.fields(SinkLosses)] == ["dropped", "failed", "resent"]


# --- AC9: every SinkLosses the package builds passes resent= ---------------------------------

FLOOR = 15
"""The ``SinkLosses`` calls under ``src/`` when SPEC-095 was built: the fifteen it names."""


def sinklosses_findings(sources: Iterable[tuple[str, str]], floor: int = FLOOR) -> list[str]:
    """One line per ``SinkLosses(...)`` call without a ``resent=`` keyword, plus the floor.

    A call is one whose callee is the bare name or any attribute named ``SinkLosses``. A
    ``**mapping`` argument does not count as passing ``resent``, because nothing static says it
    holds one. There is no allowlist: a call that means zero passes ``resent=0``.

    Args:
      sources: ``(where, source)`` pairs.
      floor: The fewest calls the walk must find.

    Returns:
      The findings, empty when every call passes ``resent`` and the floor is met.

    Raises:
      SyntaxError: When a source does not parse.
    """
    findings: list[str] = []
    found = 0
    for where, source in sources:
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name != "SinkLosses":
                continue
            found += 1
            if not any(keyword.arg == "resent" for keyword in node.keywords):
                findings.append(f"{where}:{node.lineno} builds a SinkLosses without resent=")
    if found < floor:
        findings.append(f"found {found} SinkLosses call(s), fewer than the floor of {floor}")
    return findings


def _package_sources() -> list[tuple[str, str]]:
    return [
        (path.relative_to(SRC).as_posix(), path.read_text(encoding="utf-8"))
        for path in sorted(SRC.rglob("*.py"))
    ]


def test_every_sinklosses_call_in_src_passes_resent() -> None:
    """AC9: the walk over every module under ``src/log_foundry/`` finds nothing to report."""
    assert sinklosses_findings(_package_sources()) == []


@pytest.mark.parametrize(
    ("source", "finding"),
    [
        ("SinkLosses(dropped=0, failed=0)\n", "m.py:1 builds a SinkLosses without resent="),
        ("base.SinkLosses(dropped=0, failed=0)\n", "m.py:1 builds a SinkLosses without resent="),
        ("SinkLosses(**counts)\n", "m.py:1 builds a SinkLosses without resent="),
    ],
    ids=["a-bare-name", "an-attribute", "a-mapping"],
)
def test_the_guard_names_a_call_without_resent(source: str, finding: str) -> None:
    """AC9: the reason, for each spelling of the call."""
    assert sinklosses_findings([("m.py", source)], floor=1) == [finding]


def test_the_guard_is_silent_on_a_call_with_resent() -> None:
    """AC9: a call that passes ``resent`` is no finding."""
    source = "SinkLosses(dropped=0, failed=0, resent=0)\nother(dropped=0)\n"
    assert sinklosses_findings([("m.py", source)], floor=1) == []


def test_the_guard_reports_a_walk_below_its_floor() -> None:
    """AC9: fourteen conforming calls fail the floor of fifteen."""
    source = "SinkLosses(dropped=0, failed=0, resent=0)\n" * 14
    assert sinklosses_findings([("m.py", source)]) == [
        "found 14 SinkLosses call(s), fewer than the floor of 15"
    ]


# --- AC10: every shipped losses() reports its counters' resent -------------------------------


def _scripted_run(sink: object, counters: SinkCounters, n: int) -> None:
    """Runs a scripted deliver that marks every event, then accepts, into ``counters``."""
    script = iter([marking(R), A])

    def deliver(batch: list[dict[str, object]]) -> Verdict:
        step = next(script)
        if isinstance(step, Outcome):
            return Verdict.uniform(step, len(batch))
        return step(batch)  # type: ignore[no-any-return]

    delivery = _engine._run(
        sink,
        deliver,
        counters,
        events(n),
        sink_retries=1,
        worker_retries=0,
        stop=None,
        decline=None,
    )
    assert delivery.resent == n


def _first_variants() -> list[cc.Variant]:
    """The first variant of each class in SPEC-092's population, less the abstract ones."""
    population = {cls.name for _, cls in _sink_classes_with_emit()} - {"Sink"} - set(cc.ABSTRACT)
    first: dict[str, cc.Variant] = {}
    for variant in cc.VARIANTS:
        first.setdefault(variant.sink, variant)
    assert set(first) == population, population ^ set(first)
    return [first[name] for name in sorted(first)]


def _build(variant: cc.Variant, monkeypatch: pytest.MonkeyPatch) -> object:
    cd.install_drivers(monkeypatch)
    sink, _ = cd.build(cc.Row(variant, "clean"), cb.Backend("clean"), monkeypatch)
    return sink


def _counters(sink: object) -> SinkCounters | None:
    found = getattr(sink, "log_foundry_counters", None)
    return found if type(found) is SinkCounters else None


NO_LOSSES = {
    "CallbackSink": "a caller's callable, which reports nothing of its own",
    "LoggingSink": "a stdlib handler absorbs its failures, so there is nothing to report",
    "MemorySink": "an in-process list that never fails",
    "NullSink": "discarding is what it is for (SPEC-026)",
}
"""The classes of the population with no ``losses()``, each with why, so none is passed over."""


def _reports(sink: object) -> bool:
    """Whether the class's ``losses``, resolved through its MRO, exists to call."""
    return callable(getattr(type(sink), "losses", None))


@pytest.mark.parametrize("variant", _first_variants(), ids=lambda v: v.id)
def test_every_losses_reports_its_counters_resent(
    variant: cc.Variant, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC10: bare, and under ``FilteringSink``, ``TransformSink`` and ``MultiSink``."""
    sink = _build(variant, monkeypatch)
    if not _reports(sink):
        assert variant.sink in NO_LOSSES, f"{variant.id} reports no losses() and is not listed"
        return
    assert variant.sink not in NO_LOSSES, f"{variant.id} reports losses() but is listed as not"
    counters = _counters(sink)
    children = getattr(sink, "_sinks", None)
    if counters is None and isinstance(sink, MultiSink):
        engine_children = [c for c in children or () if _counters(c) is not None]
        assert engine_children, f"{variant.id} has no engine child to count through"
        for child in engine_children:
            _scripted_run(child, _counters(child), 2)  # type: ignore[arg-type]
        assert sink.losses().resent == 2 * len(engine_children)  # type: ignore[attr-defined]
        return
    if counters is None:
        losses = sink.losses()  # type: ignore[attr-defined]
        assert losses is None or losses.resent == 0, (variant.id, losses)
        return
    _scripted_run(sink, counters, 3)
    assert counters.resent_events == 3
    assert sink.losses().resent == 3, variant.id  # type: ignore[attr-defined]
    for wrapper in (
        FilteringSink(sink, predicate=lambda event: True),  # type: ignore[arg-type]
        TransformSink(sink, lambda event: event),  # type: ignore[arg-type]
        MultiSink(sink),  # type: ignore[arg-type]
    ):
        reported = wrapper.losses()
        assert reported is not None and reported.resent == 3, (variant.id, type(wrapper))


def test_a_wrapper_with_drops_of_its_own_still_reports_the_inner_resent(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC10: ``FilteringSink``'s own drop and its inner sink's resend are reported together."""

    def predicate(event: dict[str, object]) -> bool:
        if event["i"] == 0:
            raise ValueError("predicate failed")
        return True

    inner = Scripted([marking(R), A])
    wrapper = FilteringSink(inner, predicate=predicate)
    delivery = _wrapper_run(wrapper, 3)
    assert delivery.resent == 2
    assert wrapper.losses() == SinkLosses(dropped=1, failed=0, resent=2)
    capsys.readouterr()


def test_multisink_sums_its_childrens_resent() -> None:
    """AC10: a ``MultiSink``'s ``losses().resent`` is the sum of its children's."""
    first, second = Scripted(), Scripted()
    _scripted_run(first, first.log_foundry_counters, 2)
    _scripted_run(second, second.log_foundry_counters, 5)
    losses = MultiSink(first, second).losses()  # type: ignore[arg-type]
    assert losses == SinkLosses(dropped=0, failed=0, resent=7)


# --- AC11: HTTPSink._send and SentrySink ------------------------------------------------------


def test_send_counts_its_resend_once_and_writes_the_line_after_counting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC11: a ``TimeoutError`` then a ``200`` adds 1, with one line written after the count."""
    from log_foundry.sinks.http import HTTPSink
    from test_http_rounds import Scripted as Opener
    from test_http_rounds import _ok

    sink = HTTPSink("http://h", max_retries=1, opener=Opener(TimeoutError("timed out"), _ok()))
    hook = WriteHook(lambda: sink.log_foundry_counters.resent_events)
    monkeypatch.setattr(sys, "stderr", hook)
    assert sink._send(b"x", content_type="text/plain") == b""
    monkeypatch.setattr(sys, "stderr", sys.__stderr__)
    assert sink.log_foundry_counters.resent_events == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0, resent=1)
    lines = [(text, seen) for text, seen in hook.seen if RESENT_LINE.match(text)]
    assert [resent_lines(text) for text, _ in lines] == [[1]], hook.seen
    assert lines[0][1] == 1, "resent_events had moved before the line was written"
    assert [text for text, _ in hook.seen if text.strip()] == [lines[0][0]]


def test_sentry_reports_its_http_backends_resent() -> None:
    """AC11: ``SentrySink.losses().resent`` is its HTTP backend's count, in events."""
    from log_foundry.sinks.sentry import SentrySink
    from test_http_rounds import Scripted as Opener
    from test_http_rounds import _ok, _status

    opener = Opener(_status(503), _ok())
    sink = SentrySink("http://k@sentry.local/1", backend="http", max_retries=1, opener=opener)
    sink.emit([{"log_id": "a", "level": "ERROR", "message": "m"}])
    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0, resent=1)


def _losses_at_the_owner_commit(monkeypatch: pytest.MonkeyPatch, sink: Any) -> list[SinkLosses]:
    """Records ``sink.losses()`` each time a run reaches its owners' commit, ``_engine._commit``.

    Args:
      monkeypatch: Where the seam is set.
      sink: The sink whose ``losses()`` is read.

    Returns:
      The list the readings are appended to.

    Raises:
      None.
    """
    seen: list[SinkLosses] = []
    original = _engine._commit

    def commit(owners: Any, lines: Any) -> None:
        seen.append(sink.losses())
        original(owners, lines)

    monkeypatch.setattr(_engine, "_commit", commit)
    return seen


def test_an_http_runs_resends_commit_with_its_failed_request(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC1 and AC10: ``losses()`` read between a run's commits never sees half the run.

    Two ``503``s with one resend allowed: one resend counted, and the request abandoned in
    ``failed``. Both are in one commit, under one lock, so the reading taken at the owners'
    commit is already the run's final reading.
    """
    from log_foundry.sinks.http import HTTPSink
    from test_http_rounds import Scripted as Opener
    from test_http_rounds import _status

    sink = HTTPSink("http://h", max_retries=1, opener=Opener(_status(503)))
    seen = _losses_at_the_owner_commit(monkeypatch, sink)
    delivery = run(sink, events(2))
    assert delivery.failed and delivery.resent == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=1, resent=2)
    assert seen == [sink.losses()]
    capsys.readouterr()


def test_sends_resend_commits_with_its_failed_request(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC11: ``_send``'s resend and its abandoned request reach ``losses()`` together."""
    from log_foundry.sinks.base import SinkDeliveryError
    from log_foundry.sinks.http import HTTPSink
    from test_http_rounds import Scripted as Opener
    from test_http_rounds import _status

    sink = HTTPSink("http://h", max_retries=1, opener=Opener(_status(503)))
    seen = _losses_at_the_owner_commit(monkeypatch, sink)
    with pytest.raises(SinkDeliveryError):
        sink._send(b"x", content_type="text/plain")
    assert sink.losses() == SinkLosses(dropped=0, failed=1, resent=1)
    assert seen == [SinkLosses(dropped=0, failed=0, resent=0)], "nothing before the one commit"
    capsys.readouterr()


def test_a_commit_that_raises_reports_the_resends_it_could_not_count(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC1: a raising run commit names its uncounted resends, as it names its events."""
    D = Outcome.DROPPED
    sink = Scripted([marking([R, D], [0]), A])
    counters = sink.log_foundry_counters
    del counters.dropped_events
    delivery = run(sink, events(2))
    assert delivery.resent == 1 and delivery.dropped == 1
    err = capsys.readouterr().err
    assert "1 resend(s) not counted" in err, err
    assert "1 event(s) not counted" in err, err


# --- AC13: the line --------------------------------------------------------------------------


def test_the_resend_line_names_the_stored_class_name(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC13: a metaclass ``__name__`` property neither renames the line nor runs."""
    calls: list[str] = []
    meta = metaclass_named("Impostor", calls)
    cls = meta("StoredName", (Scripted,), {})
    sink = cls([marking(R), A])
    run(sink, events(2))
    err = capsys.readouterr().err
    assert err == (
        "log-foundry: resent 2 event(s) a lost reply may have landed; they may be duplicated; "
        "StoredName\n"
    )
    assert calls == []


def test_only_diag_writes_the_resend_line() -> None:
    """AC13: no module but ``_diag`` spells the line's text."""
    spelled = [
        path.relative_to(SRC).as_posix()
        for path in SRC.rglob("*.py")
        if "a lost reply may have landed" in path.read_text(encoding="utf-8")
    ]
    assert spelled == ["_diag.py"]
    assert _diag.resent.__module__ == "log_foundry._diag"
