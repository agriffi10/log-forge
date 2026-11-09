"""The engine announces every loss no sink line covers, once (invariants 2 and 11).

A round that registers a line the run will write owns the announcement of every loss its verdict
decided; the engine's settled line names each loss decided in a round that registered none. Before
this, the engine wrote no settled line once any sink line committed in the run, so a ``deliver``
sink outside the HTTP family that announced a loss in one round and returned another silently in
a later round left the second on ``log_foundry_counters`` alone. The key is the round, never a
count of what a line covers, since a line carries no event identity (the decision in
``docs/decisions/sink-delivery/the-http-family-delivers-in-rounds-and-a-run-context-keeps-its-
counters-units.md``). The population of shipped engine sinks is derived from the tree by the
roster in ``tests/test_engine_sinks_roster.py``, never listed here.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

import log_foundry.sinks
import nats_engine_matrix_harness as nats_harness
from log_foundry import SinkCounters, _diag, emit_via_deliver
from log_foundry.sinks._engine import When, outcomes_of, run_context, run_delivery
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.nats import NATSSink, _Published, _Report
from log_foundry.sinks.verdict import Outcome, Verdict
from sink_rigs import ok, unencodable
from test_engine_sinks_roster import NO_EMIT, SINKS, _build, engine_sink_findings
from test_http_rounds import Scripted as Opener
from test_http_rounds import _ok, _status
from verdict_sinks import Scripted, WaitRecorder, events

A, R, J, D, U = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
)

_SPLIT = {"_RedisSink": ("RedisStreamsSink", "RedisListSink")}
"""A derived class the roster's builder reaches only through its concrete subclasses."""

POPULATION = sorted(
    built
    for name in engine_sink_findings(SINKS)[0]
    if name not in NO_EMIT
    for built in _SPLIT.get(name, (name,))
)
"""Every shipped engine sink the roster derives from ``src/log_foundry/sinks/``, less ``NO_EMIT``.

``NO_EMIT`` is the roster's own table of transports that are not structural sinks: the engine
runs ``SocketTransport`` only inside ``SyslogSink`` or ``LogstashSink``, and never probes it.
"""


def _engine_line(lost: int, rounds: int, name: str, **terms: int) -> str:
    """The engine's settled line, every term zero unless named."""
    counts = {"rejected": 0, "unresolved": 0, "raised": 0, "unknown": 0, "dropped": 0} | terms
    detail = ", ".join(f"{term} {n}" for term, n in counts.items())
    return f"log-foundry: lost {lost} event(s); {detail} after {rounds} delivery round(s) to {name}"


def _announce_drop(n: int = 1) -> None:
    """Registers a sink's own line for ``n`` dropped events, as a shipped drop site does."""
    run_context().announce(
        lambda: _diag.lost("event", n, "ThirdParty, dropped by its own rule"), when=When.SETTLED
    )


_OWN_LINE = "log-foundry: lost 1 event(s); ThirdParty, dropped by its own rule"


def _run(sink: object, batch: list[dict[str, object]], worker_retries: int = 0) -> Any:
    delivery = run_delivery(sink, batch, worker_retries=worker_retries, stop=WaitRecorder())
    assert delivery is not None, "the premise: the sink offers deliver"
    return delivery


def test_a_third_party_sink_announcing_one_loss_and_returning_another_silently_announces_both(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Round one drops event 0 with its own line; round two rejects event 1 with none."""

    def first(batch: list[dict[str, object]]) -> Verdict:
        _announce_drop()
        return Verdict(outcomes=(D, R, A))

    sink = Scripted([first, J], max_retries=1)
    delivery = _run(sink, events(3))
    assert (delivery.dropped, delivery.rejected, delivery.failed) == (1, 1, False)
    assert capsys.readouterr().err.splitlines() == [
        _engine_line(1, 2, "Scripted", rejected=1),
        _OWN_LINE,
    ]
    assert sink.log_foundry_counters.losses().dropped == 1
    assert sink.log_foundry_counters.losses().failed == 1


def test_a_silent_loss_in_an_earlier_round_is_announced_beside_a_later_rounds_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The key is the round that decided the loss, not the run: round one is silent."""

    def second(batch: list[dict[str, object]]) -> Verdict:
        _announce_drop()
        return Verdict(outcomes=(D,))

    sink = Scripted([(J, R, A), second], max_retries=1)
    _run(sink, events(3))
    assert capsys.readouterr().err.splitlines() == [
        _engine_line(1, 1, "Scripted", rejected=1),
        _OWN_LINE,
    ]


def test_a_round_whose_line_is_not_written_owns_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A ``LAST`` line of a round that was not the last is never written, so it covers nothing."""

    def first(batch: list[dict[str, object]]) -> Verdict:
        run_context().announce(
            lambda: _diag.lost("event", 1, "ThirdParty, still retrying"), when=When.LAST
        )
        return Verdict(outcomes=(J, R))

    sink = Scripted([first, A], max_retries=1)
    _run(sink, events(2))
    assert capsys.readouterr().err.splitlines() == [_engine_line(1, 1, "Scripted", rejected=1)]


def test_a_round_that_registers_a_line_owns_every_loss_it_decided(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The contract the key keeps: a line is never counted against events, so it covers its round.

    A round that announces some of its losses and returns others silently owns the silent ones
    too, as an HTTP hook owns its chunk; the engine adds no line beside the sink's, so nothing is
    announced twice.
    """

    def only(batch: list[dict[str, object]]) -> Verdict:
        _announce_drop()
        return Verdict(outcomes=(D, J, A))

    _run(Scripted([only]), events(3))
    assert capsys.readouterr().err.splitlines() == [_OWN_LINE]


@pytest.mark.parametrize(
    ("step", "terms"),
    [
        (RuntimeError("never shown"), {"raised": 1}),
        (U, {"unknown": 1}),
        (R, {"unresolved": 1}),
        (D, {"dropped": 1}),
    ],
    ids=["raised", "unknown", "unresolved", "dropped"],
)
def test_every_loss_class_of_an_unlined_round_is_announced_beside_a_lined_one(
    step: object, terms: dict[str, int], capsys: pytest.CaptureFixture[str]
) -> None:
    """Round one settles event 0 with a line and event 1 ``ACCEPTED``; round two loses event 2."""

    def first(batch: list[dict[str, object]]) -> Verdict:
        _announce_drop()
        return Verdict(outcomes=(D, A, R))

    sink = Scripted([first, step], max_retries=1)
    delivery = _run(sink, events(3))
    assert not delivery.failed, "the premise: an accepted event settles the run"
    lines = capsys.readouterr().err.splitlines()
    assert lines[:2] == [_engine_line(1, 2, "Scripted", **terms), _OWN_LINE]
    assert len(lines) == 2 + (1 if "raised" in terms else 0)


def test_a_run_with_no_sink_line_names_every_loss_under_the_round_that_decided_it(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With no sink line anywhere, every loss is in an engine line, one per deciding round.

    Round one decides the drop and the rejection; event 2 is resent and round two decides its
    ``UNKNOWN``. One line naming the run's two rounds said the first two were lost after the
    second round, which is not when they were lost (invariant 11).
    """
    _run(Scripted([(D, J, R), U], max_retries=1), events(3))
    assert capsys.readouterr().err.splitlines() == [
        _engine_line(2, 1, "Scripted", rejected=1, dropped=1),
        _engine_line(1, 2, "Scripted", unknown=1),
    ]


def test_a_loss_decided_in_round_one_of_a_four_round_run_reads_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The line names the round that decided the loss, not how many rounds the run took."""
    delivery = _run(Scripted([(J, R), R, R, A], max_retries=3), events(2))
    assert (delivery.attempts, delivery.rejected, delivery.failed) == (4, 1, False)
    assert capsys.readouterr().err.splitlines() == [_engine_line(1, 1, "Scripted", rejected=1)]


def test_a_failed_run_leaves_its_unlined_losses_to_the_catcher(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed run writes no settled line; its catcher owns the batch, lined round or not."""

    def first(batch: list[dict[str, object]]) -> Verdict:
        run_context().announce(lambda: _diag.lost("event", 1, "ThirdParty, said"), when=When.ALWAYS)
        return Verdict(outcomes=(J, R))

    delivery = _run(Scripted([first, J], max_retries=1), events(2))
    assert delivery.failed
    assert capsys.readouterr().err.splitlines() == [
        "log-foundry: lost 1 event(s); ThirdParty, said"
    ]


# --- a real engine sink, through a subclass -----------------------------------------------------


def _partial(sink: Any) -> Any:
    """Re-classes ``sink`` so round one is its real ``deliver`` and round two rejects silently.

    Round one hands the real round ``[unencodable, ok, ok]``: the shipped encode step drops the
    first with its own line and the third lands, so the run settles. The subclass returns the
    second ``RETRYABLE`` whatever happened to it, and round two returns it ``REJECTED`` without
    calling the sink, registering nothing.
    """
    base = type(sink)
    seen: list[tuple[Outcome, ...]] = []

    def deliver(self: Any, batch: list[dict[str, object]]) -> Verdict:
        if run_context().round == 1:
            outcomes = outcomes_of(base.deliver(self, batch), len(batch))
            assert outcomes is not None
            seen.append(outcomes)
            return Verdict(outcomes=(outcomes[0], R, outcomes[2]))
        return Verdict(outcomes=(J,) * len(batch))

    sink.__class__ = type(f"Partial{base.__name__}", (base,), {"deliver": deliver})
    sink.seen = seen
    return sink


def test_the_population_is_the_derived_roster() -> None:
    """The cases below are the roster's derivation, so a new engine sink joins them unlisted."""
    assert len(POPULATION) >= 13, POPULATION


@pytest.mark.parametrize("name", POPULATION)
def test_a_subclass_of_every_engine_sink_has_its_silent_loss_announced(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The real sink's drop line is written once, and the subclass's silent rejection beside it."""
    sink = _partial(_build(name, monkeypatch, tmp_path))
    try:
        capsys.readouterr()
        delivery = _run(sink, [unencodable(), ok("a"), ok("b")], worker_retries=1)
        lines = capsys.readouterr().err.splitlines()
    finally:
        sink.close()
    first = sink.seen[0]
    assert first[0] is not A and first[2] is A, "the premise: the real round lost one, landed one"
    assert (delivery.attempts, delivery.failed) == (2, False)
    engine = _engine_line(1, 2, type(sink).__name__, rejected=1)
    assert lines.count(engine) == 1, lines
    others = [line for line in lines if line != engine]
    assert len(others) == 1 and others[0].startswith("log-foundry: lost 1 event(s); "), lines


# --- the HTTP round's own announcement is not doubled ------------------------------------------


def test_an_http_loss_the_round_announced_is_not_announced_again_by_the_engine(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``_announce_unlined`` covers its chunk, so the engine's key sees the round as lined."""

    class Silent(HTTPSink):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return tuple(J if item.event.get("i") == 1 else A for item in items)

    sink = Silent("http://h", max_batch_count=1, max_retries=0, opener=Opener(_ok()))
    sink.emit([{"i": 0}, {"i": 1}])
    assert capsys.readouterr().err.splitlines() == [
        "log-foundry: lost 1 event(s); Silent, refused for good; not retried"
    ]


# --- the frame-1 review: a refused round's drop, and a round the engine set aside ---------------


def test_a_refused_rounds_drop_is_announced_since_the_refusal_line_counts_only_losses(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A refused round's ``DROPPED`` is not in the refusal line, so the settled line names it."""

    def refused(batch: list[dict[str, object]]) -> Verdict:
        run_context().refuse(RuntimeError("never shown"))
        return Verdict(outcomes=(A, D))

    sink = Scripted([refused], max_retries=0)
    delivery = _run(sink, events(2))
    assert not delivery.failed
    assert sink.log_foundry_counters.losses().dropped == 1
    assert capsys.readouterr().err.splitlines() == [_engine_line(1, 1, "Scripted", dropped=1)]


@pytest.mark.parametrize(
    ("step", "terms", "absorbed"),
    [
        (None, {"unknown": 2}, 0),
        (RuntimeError("never shown"), {"raised": 2}, 1),
    ],
    ids=["unreadable", "raised"],
)
def test_a_round_whose_verdict_the_engine_set_aside_owns_nothing(
    step: object, terms: dict[str, int], absorbed: int, capsys: pytest.CaptureFixture[str]
) -> None:
    """Round two writes a line, then returns nothing readable or raises: its line is discarded.

    The engine decided the round's fates, so it names both events, and the line the round wrote
    about a drop that never became an outcome is not written beside it (frame-2 review).
    """

    def second(batch: list[dict[str, object]]) -> Verdict:
        _announce_drop()
        if isinstance(step, Exception):
            raise step
        return step  # type: ignore[return-value]

    sink = Scripted([(A, R, R), second], max_retries=1)
    delivery = _run(sink, events(3))
    assert not delivery.failed
    lines = capsys.readouterr().err.splitlines()
    assert lines[0] == _engine_line(2, 2, "Scripted", **terms)
    assert _OWN_LINE not in lines
    assert len(lines) == 1 + absorbed


def test_a_set_aside_rounds_count_is_discarded_so_a_resent_drop_counts_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An idempotent sink counts a drop and raises; the resend drops it: counted and named once."""
    rounds: list[int] = []

    def deliver(batch: list[dict[str, object]]) -> Verdict:
        rounds.append(len(batch))
        if len(rounds) == 1:
            run_context().drops(sink.log_foundry_counters).count("dropped_events", 1)
            _announce_drop()
            raise ConnectionError("never shown")
        return Verdict(outcomes=(D, A))

    class Idempotent(Scripted):
        log_foundry_idempotent = True

        def _retryable_error(self, exc: BaseException) -> bool:
            return isinstance(exc, ConnectionError)

    sink = Idempotent([deliver], max_retries=1)
    _run(sink, events(2))
    assert rounds == [2, 2], "the premise: the raise was resent"
    assert sink.log_foundry_counters.losses().dropped == 1
    lines = capsys.readouterr().err.splitlines()
    assert lines[0] == _engine_line(1, 2, "Idempotent", dropped=1)
    assert _OWN_LINE not in lines


def _drop_and_leave(batch: list[dict[str, object]]) -> Verdict:
    """Drops event 0 with a ``SETTLED`` line, leaves event 1 retryable and lands event 2."""
    _announce_drop()
    return Verdict(outcomes=(D, R, A))


@pytest.mark.parametrize(
    "ending",
    [{"worker_retries": 0}, {"decline": True}],
    ids=["budget", "declined"],
)
def test_a_retryable_event_the_run_ended_on_is_named_beside_a_non_last_line(
    ending: dict[str, object], capsys: pytest.CaptureFixture[str]
) -> None:
    """The drop line does not cover event 1: it became a loss only when the run ended."""
    sink = Scripted([_drop_and_leave], max_retries=0 if "worker_retries" in ending else 3)
    delivery = run_delivery(
        sink,
        events(3),
        worker_retries=0,
        stop=WaitRecorder(),
        decline=(lambda _longest: True) if "decline" in ending else None,
    )
    assert delivery is not None and delivery.attempts == 1 and not delivery.failed
    assert capsys.readouterr().err.splitlines() == [
        _engine_line(1, 1, "Scripted", unresolved=1),
        _OWN_LINE,
    ]


def test_a_last_line_of_the_final_round_owns_its_retryable_events(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A sink that announces the events it leaves retryable, ``LAST``, gets no engine line."""

    def leave(batch: list[dict[str, object]]) -> Verdict:
        run_context().announce(
            lambda: _diag.lost("event", 1, "ThirdParty, still retrying"), when=When.LAST
        )
        return Verdict(outcomes=(R, A))

    _run(Scripted([leave], max_retries=0), events(2))
    assert capsys.readouterr().err.splitlines() == [
        "log-foundry: lost 1 event(s); ThirdParty, still retrying"
    ]


def test_an_idempotent_unknown_the_run_ended_on_is_named_beside_a_non_last_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An idempotent sink's ``UNKNOWN`` is resendable, so only a ``LAST`` line owns it."""

    class Idempotent(Scripted):
        log_foundry_idempotent = True

    def unknown(batch: list[dict[str, object]]) -> Verdict:
        _announce_drop()
        return Verdict(outcomes=(D, U, A))

    _run(Idempotent([unknown], max_retries=0), events(3))
    assert capsys.readouterr().err.splitlines() == [
        _engine_line(1, 1, "Idempotent", unknown=1),
        _OWN_LINE,
    ]


def test_an_http_retryable_outcome_is_named_alike_through_emit_and_the_worker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A reader's ``RETRYABLE`` beside a refused chunk is named on both twins (frame-2 review)."""

    class RetryReader(HTTPSink):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return tuple(R if item.event.get("i") == 1 else A for item in items)

    def build() -> RetryReader:
        opener = Opener(_status(400), _ok(), _ok(), _ok(), _ok())
        return RetryReader("http://h", max_batch_count=1, max_retries=0, opener=opener)

    batch: list[dict[str, object]] = [{"i": 0}, {"i": 1}, {"i": 2}]
    inline = build()
    inline.emit(list(batch))
    through_emit = capsys.readouterr().err.splitlines()
    worker = build()
    _run(worker, list(batch), worker_retries=1)
    through_worker = capsys.readouterr().err.splitlines()
    assert _engine_line(1, 1, "RetryReader", unresolved=1) in through_emit, through_emit
    assert _engine_line(1, 2, "RetryReader", unresolved=1) in through_worker, through_worker
    assert inline.log_foundry_counters.losses() == worker.log_foundry_counters.losses()


class _Delegate:
    """A third-party sink forwarding ``deliver`` to a ``NATSSink``, declaring itself idempotent."""

    log_foundry_idempotent = True

    def __init__(self, inner: Any, max_retries: int) -> None:
        self.inner = inner
        self.max_retries = max_retries
        self.log_foundry_counters = SinkCounters()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        return self.inner.deliver(batch)  # type: ignore[no-any-return]

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)


def _scripted_nats(*rounds: tuple[str, ...]) -> NATSSink:
    """A ``NATSSink`` whose coroutine is replaced, round by round, by the publishes given.

    Each round reports one fate per event: ``"ok"``, ``"abandoned"`` (counted where it was
    abandoned, as JetStream's is), ``"raised"`` or ``"unattempted"``. Everything around the
    coroutine — the refusal guards, :func:`~log_foundry.sinks.nats._verdict_of` and
    ``_register`` — is the shipped code.
    """
    sink = NATSSink("logs", client=nats_harness.Client(nats_harness.Script([])))
    reports = iter(rounds)
    shipped = sink._run

    def run(coro: Any, bound: float) -> Any:
        if coro.cr_code.co_name != "_round_on_loop":
            return shipped(coro, bound)
        coro.close()
        fates = [(state, "RuntimeError" if state == "raised" else "") for state in next(reports)]
        return _Report(published=_Published(fates, True, "JetStream publish"))

    sink._run = run  # type: ignore[method-assign]
    return sink


@pytest.fixture
def no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


@pytest.mark.usefixtures("no_wait")
def test_a_nats_event_left_unattempted_by_a_settled_runs_last_round_is_counted_and_named_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Round one lands event 0 and abandons event 1, which the idempotent run resends; round two
    publishes nothing, the deadline leaving event 1 unattempted, and the run ends there, settled.

    The round marks event 1 sink-counted, as 1.x's ``emit`` raised and its catcher owned the
    batch. But a settled run has no catcher: before, nothing counted event 1 and no line named it.
    """
    inner = _scripted_nats(("ok", "abandoned"), ("unattempted",))
    outer = _Delegate(inner, max_retries=1)
    try:
        delivery = _run(outer, events(2))
    finally:
        inner.close()
    assert (delivery.attempts, delivery.accepted, delivery.unresolved) == (2, 1, 1)
    assert not delivery.failed
    failed = outer.log_foundry_counters.losses().failed + inner.losses().failed
    assert failed == 1, "event 1 is counted once, by one of the two ledgers"
    lines = [line for line in capsys.readouterr().err.splitlines() if "lost" in line]
    assert lines == [
        f"log-foundry: lost 1 event(s); NATSSink publish_timeout {inner.publish_timeout}s"
    ]


@pytest.mark.usefixtures("no_wait")
def test_a_nats_event_left_unattempted_before_its_resend_lands_is_counted_nowhere(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The unattempted event of a round that is not the last is resent, so it is no loss."""
    inner = _scripted_nats(("ok", "abandoned"), ("unattempted",), ("ok",))
    outer = _Delegate(inner, max_retries=2)
    try:
        delivery = _run(outer, events(2))
    finally:
        inner.close()
    assert (delivery.attempts, delivery.accepted, delivery.failed) == (3, 2, False)
    assert outer.log_foundry_counters.losses().failed + inner.losses().failed == 0
    assert "lost" not in capsys.readouterr().err


@pytest.mark.usefixtures("no_wait")
def test_a_settled_runs_last_round_counts_its_raised_and_unattempted_publishes_once_each(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The remainder counted ``SETTLED_LAST`` is the unattempted events only, never the round.

    Round two raises one publish and leaves the other unattempted; the raised one is counted by
    1.x's own per-publish count, so a count of the whole round would book it twice.
    """
    inner = _scripted_nats(("ok", "abandoned", "abandoned"), ("raised", "unattempted"))
    outer = _Delegate(inner, max_retries=1)
    try:
        delivery = _run(outer, events(3))
    finally:
        inner.close()
    assert (delivery.attempts, delivery.accepted, delivery.unresolved, delivery.failed) == (
        2,
        1,
        2,
        False,
    )
    assert outer.log_foundry_counters.losses().failed + inner.losses().failed == 2
    lines = [line for line in capsys.readouterr().err.splitlines() if "lost" in line]
    assert lines == [
        "log-foundry: lost 1 event(s); NATSSink publish, RuntimeError",
        f"log-foundry: lost 1 event(s); NATSSink publish_timeout {inner.publish_timeout}s",
    ]


class _PartlyItsOwn:
    """A non-idempotent wrapper that accepts event 0 itself and forwards the rest to ``NATSSink``.

    The run settles on the wrapper's own acceptance, so a NATS round that published nothing can
    end a settled run with no ``UNKNOWN`` resend at all.
    """

    def __init__(self, inner: NATSSink, max_retries: int) -> None:
        self.inner = inner
        self.max_retries = max_retries
        self.log_foundry_counters = SinkCounters()

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        own = 0 if run_context().round == 1 else -1
        forwarded = [event for at, event in enumerate(batch) if at != own]
        positions = [at for at in range(len(batch)) if at != own]
        _result, outcomes = run_context().delegate(self.inner.deliver, forwarded, positions)
        assert outcomes is not None
        mapped = dict(zip(positions, outcomes, strict=True))
        return Verdict(outcomes=tuple(mapped.get(at, A) for at in range(len(batch))))

    def emit(self, batch: list[dict[str, object]]) -> None:
        emit_via_deliver(self, batch)


@pytest.mark.usefixtures("no_wait")
def test_a_non_idempotent_run_settled_by_its_wrapper_counts_the_unattempted_event_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The frame-1 review's probe: the gap is reachable without an idempotent run."""
    inner = _scripted_nats(("unattempted",), ("unattempted",))
    outer = _PartlyItsOwn(inner, max_retries=1)
    try:
        delivery = _run(outer, events(2))
    finally:
        inner.close()
    assert (delivery.attempts, delivery.accepted, delivery.unresolved, delivery.failed) == (
        2,
        1,
        1,
        False,
    )
    assert outer.log_foundry_counters.losses().failed + inner.losses().failed == 1
    lines = [line for line in capsys.readouterr().err.splitlines() if "lost" in line]
    assert lines == [
        f"log-foundry: lost 1 event(s); NATSSink publish_timeout {inner.publish_timeout}s"
    ]


@pytest.mark.usefixtures("no_wait")
def test_a_nats_event_left_unattempted_by_a_failed_run_is_its_catchers_alone(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed run keeps 1.x's ledger: the raised publish counts, the unattempted event does not.

    The run's catcher — here the caller of ``run_delivery`` — owns the batch, as the worker's
    abandon line and the NATS matrix's ``raise-after-the-deadline`` rows record.
    """
    inner = _scripted_nats(("raised", "unattempted"))
    outer = _Delegate(inner, max_retries=0)
    try:
        delivery = _run(outer, events(2))
    finally:
        inner.close()
    assert delivery.failed
    assert inner.losses().failed == 1 and outer.log_foundry_counters.losses().failed == 0
    lines = [line for line in capsys.readouterr().err.splitlines() if "lost" in line]
    assert lines == ["log-foundry: lost 1 event(s); NATSSink publish, RuntimeError"]


SINK_COUNTED_SITES = [("nats.py", "_round")]
"""Every ``sink_counted`` call in ``src/log_foundry/sinks/``, by module and innermost function.

A mark excludes its events from the engine's count and its settled line, so each site must
itself count and announce every event it marks in every run ending the mark excludes it from —
including a settled run's last round. ``NATSSink._round`` once did not (its unattempted events,
above). A new site joins this table only after that check.
"""


def test_the_sink_counted_population_is_the_reviewed_one() -> None:
    """Every ``sink_counted`` call, found in the tree, is one whose registrations were checked."""
    root = Path(log_foundry.sinks.__file__).parent
    found: list[tuple[str, str]] = []

    def visit(node: ast.AST, module: str, function: str) -> None:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            function = node.name
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "sink_counted"
        ):
            found.append((module, function))
        for child in ast.iter_child_nodes(node):
            visit(child, module, function)

    for path in sorted(root.glob("**/*.py")):
        visit(ast.parse(path.read_text(encoding="utf-8")), str(path.relative_to(root)), "")
    assert found == SINK_COUNTED_SITES


def test_a_settled_last_line_owns_the_resendable_loss_its_round_ended_the_run_on(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``SETTLED_LAST`` is written exactly when the run ends on its round, as a ``LAST`` line is.

    So it owns an event the round left ``RETRYABLE``, and the engine adds no line beside it.
    """

    def last(batch: list[dict[str, object]]) -> Verdict:
        run_context().announce(
            lambda: _diag.lost("event", 1, "ThirdParty, out of rounds"), when=When.SETTLED_LAST
        )
        return Verdict(outcomes=(R, A))

    _run(Scripted([last], max_retries=0), events(2))
    assert capsys.readouterr().err.splitlines() == [
        "log-foundry: lost 1 event(s); ThirdParty, out of rounds"
    ]
