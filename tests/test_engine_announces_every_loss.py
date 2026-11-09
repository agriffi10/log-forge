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

from typing import TYPE_CHECKING, Any

import pytest

from log_foundry import _diag
from log_foundry.sinks._engine import When, outcomes_of, run_context, run_delivery
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.verdict import Outcome, Verdict
from sink_rigs import ok, unencodable
from test_engine_sinks_roster import NO_EMIT, SINKS, _build, engine_sink_findings
from test_http_rounds import Scripted as Opener
from test_http_rounds import _ok
from verdict_sinks import Scripted, WaitRecorder, events

if TYPE_CHECKING:
    from pathlib import Path

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
        _engine_line(1, 2, "Scripted", rejected=1),
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
    assert capsys.readouterr().err.splitlines() == [_engine_line(1, 2, "Scripted", rejected=1)]


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


def test_a_run_with_no_sink_line_keeps_the_one_engine_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Unchanged: with no sink line anywhere, every loss is in the one engine line."""
    _run(Scripted([(D, J, R), U], max_retries=1), events(3))
    assert capsys.readouterr().err.splitlines() == [
        _engine_line(3, 2, "Scripted", rejected=1, unknown=1, dropped=1)
    ]


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
