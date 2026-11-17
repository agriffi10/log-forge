"""SPEC-102 — a NATSSink round resends the tail its deadline left unattempted beside a publish.

Every case drives a real ``NATSSink`` over :mod:`nats_engine_matrix_harness`'s doubles, with the
round deadline moved by the harness's ``Clock`` and never by wall time. Two local subclasses add
what the harness, which must run unchanged against 1.x, does not record: :class:`RoundScript` gives
each ``deliver`` call its own steps, where the harness's ``Script`` replays the same steps every
call, and :class:`RecordingClient` records which event each publish carried. Each round's batch is
observed by a patch of ``NATSSink.deliver`` of this file's own, as ``run_case`` counts calls, so
``run_case``'s recorded shape stays as 1.x recorded it. Every expected value is a literal from
SPEC-102, never derived from the production expression. Serves invariants 2, 3, 6, 7 and 11.
"""

from __future__ import annotations

import contextlib
import io
import json
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry.sinks.nats as nats_module
from log_foundry.sinks import _engine
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.nats import NATSSink, _Published, _verdict_of
from log_foundry.sinks.transform import TransformSink
from log_foundry.sinks.verdict import Outcome
from log_foundry.worker import Worker
from nats_engine_matrix_harness import LINE_PREFIX, PUBLISH_TIMEOUT, Client, Script

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

A, J, U, R = Outcome.ACCEPTED, Outcome.REJECTED, Outcome.UNKNOWN, Outcome.RETRYABLE

TIMEOUT_LINE = "log-foundry: lost {} event(s); NATSSink publish_timeout 0.2s"
RAISED_LINE = "log-foundry: lost 1 event(s); NATSSink publish, RuntimeError"
REFUSED_LINE = (
    "log-foundry: lost 1 event(s); refused by NATSSink after others landed, SinkDeliveryError"
)
ABANDONED_LINE = "log-foundry: lost 1 event(s); NATSSink unconfirmed, JetStream publish abandoned"


@dataclass
class RoundScript(Script):
    """A harness ``Script`` whose steps are chosen per ``deliver`` call, not replayed each call.

    Attributes:
      rounds: The steps of call 1, call 2 and so on; a call past the list runs ``default``.
    """

    rounds: list[list[str]] = field(default_factory=list)

    def begin_call(self) -> None:
        super().begin_call()
        self.steps = list(self.rounds[self.calls - 1]) if self.calls <= len(self.rounds) else []


class _RecordingJetStream:
    def __init__(self, client: RecordingClient) -> None:
        self.client = client

    async def publish(self, subject: str, payload: bytes, *, timeout: float | None = None) -> None:  # noqa: ASYNC109
        self.client.sent.append(json.loads(payload)["n"])
        await self.client.script.publish()


class RecordingClient(Client):
    """The harness ``Client``, recording each publish's event number before the script runs it.

    A publish that raises or is held is recorded too, since the double saw it.

    Attributes:
      sent: The event number of every publish, in the order the double saw them.
    """

    def __init__(self, script: Script) -> None:
        super().__init__(script)
        self.sent: list[object] = []

    async def publish(self, subject: str, payload: bytes) -> None:
        self.sent.append(json.loads(payload)["n"])
        await self.script.publish()

    def jetstream(self) -> Any:
        return _RecordingJetStream(self)


@dataclass
class Run:
    """What one case observed: each round's batch, the publishes, the counts and the lines."""

    offered: list[list[object]]
    sent: list[object]
    calls: int
    publishes: int
    failed: int
    failed_batches: int
    lines: list[str]


@contextlib.contextmanager
def _nats(
    rounds: list[list[str]],
    monkeypatch: pytest.MonkeyPatch,
    *,
    jetstream: bool = True,
    after_round: Callable[[NATSSink, RoundScript, int], None] | None = None,
) -> Iterator[tuple[NATSSink, RoundScript, RecordingClient, list[list[object]]]]:
    """Builds an injected ``NATSSink`` on the harness clock, its ``deliver`` recording each batch.

    Args:
      rounds: The steps of each ``deliver`` call.
      monkeypatch: The test's patcher, which undoes the clock and the ``deliver`` patch.
      jetstream: Whether the sink publishes through JetStream.
      after_round: Called with the sink, the script and the call number after each call returns.

    Yields:
      The sink, its script, its client and the list each call's event numbers are appended to.
    """
    script = RoundScript([], rounds=rounds)
    client = RecordingClient(script)
    offered: list[list[object]] = []
    original = vars(NATSSink)["deliver"]

    def recorded(self: NATSSink, batch: list[dict[str, object]]) -> Any:
        offered.append([event["n"] for event in batch])
        script.begin_call()
        verdict = original(self, batch)
        if after_round is not None:
            after_round(self, script, script.calls)
        return verdict

    monkeypatch.setattr(nats_module, "time", script.clock)
    monkeypatch.setattr(NATSSink, "deliver", recorded)
    sink = NATSSink("logs", client=client, jetstream=jetstream, publish_timeout=PUBLISH_TIMEOUT)
    try:
        yield sink, script, client, offered
    finally:
        if script.gate is not None and sink._loop.is_running():
            sink._loop.call_soon_threadsafe(script.gate.set)
        with contextlib.redirect_stderr(io.StringIO()), contextlib.suppress(Exception):
            sink.close()


def _lines(stderr: io.StringIO) -> list[str]:
    return [line for line in stderr.getvalue().splitlines() if line.startswith(LINE_PREFIX)]


def _events(n: int) -> list[dict[str, object]]:
    return [{"n": i} for i in range(n)]


def _run_worker(
    rounds: list[list[str]],
    n: int,
    monkeypatch: pytest.MonkeyPatch,
    *,
    max_retries: int = 2,
    wrap: Callable[[NATSSink], Any] | None = None,
    after_round: Callable[[NATSSink, RoundScript, int], None] | None = None,
) -> Run:
    """Drives one batch of ``n`` events through ``Worker(sink, max_retries)._emit``.

    Args:
      rounds: The steps of each ``deliver`` call.
      n: The batch's size, its events numbered from 0.
      monkeypatch: The test's patcher.
      max_retries: The worker's ``max_retries``.
      wrap: Builds the sink the worker is handed around the ``NATSSink``, or ``None``.
      after_round: Passed to :func:`_nats`.

    Returns:
      What the run observed; ``failed`` is the ``NATSSink``'s own ``losses().failed``.
    """
    with _nats(rounds, monkeypatch, after_round=after_round) as (sink, script, client, offered):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            worker = Worker(sink if wrap is None else wrap(sink), max_retries=max_retries)
            try:
                worker._emit([_events(n)])
                failed_batches = worker.health().failed_batches
            finally:
                worker.stop()
        return Run(
            offered=offered,
            sent=list(client.sent),
            calls=script.calls,
            publishes=script.publishes,
            failed=sink.losses().failed,
            failed_batches=failed_batches,
            lines=_lines(stderr),
        )


@pytest.mark.parametrize(
    ("fates", "abandoned_counted", "unconfirmed", "outcomes", "counted"),
    [
        (("ok", "unattempted"), True, False, (A, R), ()),
        (("ok", "unattempted"), True, True, (U, R), ()),
        (("ok", "abandoned", "unattempted"), True, False, (A, U, R), ()),
        (("ok", "abandoned", "unattempted"), False, False, (A, A, R), ()),
        (("ok", "abandoned", "unattempted"), False, True, (U, U, R), ()),
        (("ok", "raised", "unattempted"), True, False, (A, J, R), ()),
        (("abandoned", "unattempted", "unattempted"), True, False, (U, R, R), ()),
        (("ok", "raised"), True, False, (A, J), ()),
        (("raised", "unattempted"), True, False, (R, R), (0, 1)),
        (("unattempted",), True, False, (R,), (0,)),
        (("raised", "raised"), True, False, (R, R), (0, 1)),
    ],
)
def test_verdict_of_maps_each_fate_as_the_spec_tables_it(
    fates: tuple[str, ...],
    abandoned_counted: bool,
    unconfirmed: bool,
    outcomes: tuple[Outcome, ...],
    counted: tuple[int, ...],
) -> None:
    """FR-001: beside a publish, unattempted is ``RETRYABLE`` and uncounted; all else as before.

    The rows where something was published read SPEC-102's Data Model table; the rows where
    nothing was are the unchanged branch, every event ``RETRYABLE`` and sink-counted.
    """
    published = _Published(
        [(state, "RuntimeError" if state == "raised" else "") for state in fates],
        abandoned_counted,
        "JetStream publish",
    )
    assert _verdict_of(published, unconfirmed) == (outcomes, frozenset(counted))


def test_every_event_the_first_round_left_unattempted_is_resent_and_lands_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001: round one publishes event 0 and spends the budget; round two is offered 1 to 4."""
    run = _run_worker([["late-ok"], ["ok"]], 5, monkeypatch)
    assert run.offered == [[0, 1, 2, 3, 4], [1, 2, 3, 4]]
    assert Counter(run.sent) == {0: 1, 1: 1, 2: 1, 3: 1, 4: 1}, "each event published once"
    assert (run.calls, run.failed, run.failed_batches, run.lines) == (2, 0, 0, [])


def test_the_unattempted_event_beside_a_raised_one_is_resent_and_the_raised_one_is_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001: event 0 lands, event 1 raises past the deadline, event 2 is never attempted."""
    run = _run_worker([["ok", "late-raise"], ["ok"]], 3, monkeypatch)
    assert run.offered == [[0, 1, 2], [2]], "the raised event 1 stays REJECTED, event 2 is resent"
    assert all(1 not in batch for batch in run.offered[1:])
    assert (run.failed, run.lines) == (1, [RAISED_LINE])


def test_events_later_rounds_delivered_are_in_no_counter_and_no_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: each resend round publishes something and leaves a tail; the last round lands it."""
    run = _run_worker([["late-ok"], ["ok", "late-ok"], ["ok"]], 5, monkeypatch)
    assert run.offered == [[0, 1, 2, 3, 4], [1, 2, 3, 4], [3, 4]]
    assert Counter(run.sent) == {0: 1, 1: 1, 2: 1, 3: 1, 4: 1}
    assert (run.failed, run.failed_batches, run.lines) == (0, 0, [])


def test_a_resend_round_whose_only_publish_raised_resends_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: the wider JetStream asymmetry SPEC-102 records rather than removes.

    Round two resends the tail and publishes nothing, its one publish raising after the budget,
    so the unchanged nothing-published branch answers it whole and round three sends event 1
    again: counted once for the round it raised in, though a JetStream ack wait that expired may
    have persisted it.
    """
    run = _run_worker([["late-ok"], ["late-raise"], ["ok"]], 5, monkeypatch)
    assert run.offered == [[0, 1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 3, 4]]
    assert run.publishes == 6
    assert Counter(run.sent)[1] == 2, "event 1 was published in round two and again in three"
    assert (run.failed, run.lines) == (1, [RAISED_LINE])


def test_an_event_unresolved_when_the_rounds_run_out_is_named_by_the_last_rounds_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: every round lands one event and spends the budget; two are left after round three."""
    run = _run_worker([["late-ok"]] * 3, 5, monkeypatch)
    assert run.offered == [[0, 1, 2, 3, 4], [1, 2, 3, 4], [2, 3, 4]]
    assert (run.calls, run.failed, run.failed_batches) == (3, 2, 0)
    assert run.lines == [TIMEOUT_LINE.format(2)], "counted once, named once, by round three"


def test_a_tail_refused_behind_an_abandoned_publish_is_named_by_the_engine_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002 and FR-003: the latch refuses both resends while the abandoned publish is pending.

    Event 1's publish is held past the deadline and abandoned, so event 2 is unattempted. Rounds
    two and three are refused before they publish, the run ends on a refused round, and the
    engine's refusal line names event 2 in place of the ``publish_timeout`` line.
    """
    run = _run_worker([["ok", "hold"]], 3, monkeypatch)
    assert run.offered == [[0, 1, 2], [2], [2]]
    assert run.publishes == 2, "the refused rounds publish nothing"
    assert run.failed == 2
    assert run.lines == [REFUSED_LINE, ABANDONED_LINE]
    assert not any("publish_timeout" in line for line in run.lines)


def test_a_tail_behind_an_abandoned_publish_lands_once_that_publish_clears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-001: once the abandoned publish finishes, the next resend round delivers the tail.

    After round one returns, the held publish is released and the test waits until the latch has
    cleared, so round two meets a clear latch whatever the runner's speed.
    """

    def release(sink: NATSSink, script: RoundScript, call: int) -> None:
        if call != 1 or script.gate is None:
            return
        sink._loop.call_soon_threadsafe(script.gate.set)
        give_up = time.monotonic() + 2.0
        while sink._stuck and time.monotonic() < give_up:
            time.sleep(0.001)
        assert not sink._stuck, "the released publish did not clear the latch"

    run = _run_worker([["ok", "hold"], ["ok"]], 3, monkeypatch, after_round=release)
    assert run.offered == [[0, 1, 2], [2]]
    assert Counter(run.sent) == {0: 1, 1: 1, 2: 1}, "the abandoned event 1 is never resent"
    assert (run.failed, run.lines) == (1, [ABANDONED_LINE])


def test_a_declined_run_counts_and_names_the_tail_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002: a run SPEC-062's prediction ends after round one ends on the round with the line."""
    with _nats([["late-ok"]], monkeypatch) as (sink, _script, _client, offered):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            delivery = _engine.run_delivery(
                sink,
                _events(5),
                worker_retries=2,
                stop=threading.Event(),
                decline=lambda _longest: True,
            )
        assert delivery is not None
        assert (delivery.declined, delivery.failed) == (True, False)
        assert (delivery.attempts, delivery.accepted, delivery.unresolved) == (1, 1, 4)
        assert offered == [[0, 1, 2, 3, 4]]
        assert sink.losses().failed == 4
        assert _lines(stderr) == [TIMEOUT_LINE.format(4)]


@pytest.mark.parametrize("max_retries", [0, 1, 2])
def test_a_batch_makes_at_most_max_retries_plus_one_deliver_calls(
    max_retries: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FR-003: the resends are the engine's existing rounds, however many stay unattempted."""
    run = _run_worker([["late-ok"]] * 3, 10, monkeypatch, max_retries=max_retries)
    assert run.calls == max_retries + 1
    assert run.failed == 10 - (max_retries + 1)
    assert run.lines == [TIMEOUT_LINE.format(10 - (max_retries + 1))]


def test_a_direct_deliver_outside_a_run_returns_retryable_and_writes_the_line_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Characterises a direct ``deliver`` outside any run, the detached round SPEC-102 names.

    A detached round commits as its own last, so the ``LAST`` line is written at once; its
    caller is handed the verdict and owns the events' fate, so the round counts nothing, which is
    what it counted before SPEC-102 too (``ACCEPTED`` then four ``REJECTED``, ``failed`` 0, the
    same line, measured at ``dd358b0e``). This pins the engine's detached-round rule as it
    stands; it does not argue that the rule is right.
    """
    with _nats([["late-ok"]], monkeypatch) as (sink, _script, _client, offered):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            verdict = sink.deliver(_events(5))
        assert verdict.outcomes == (A, R, R, R, R)
        assert offered == [[0, 1, 2, 3, 4]]
        assert sink.losses().failed == 0
        assert _lines(stderr) == [TIMEOUT_LINE.format(4)]


def _drop_event_2(event: dict[str, object]) -> dict[str, object] | None:
    """A ``TransformSink`` function that drops event 2 and passes every other event on unchanged.

    Args:
      event: The event the wrapper was handed.

    Returns:
      ``None`` for event 2, else a copy of the event.

    Raises:
      None.
    """
    return None if event["n"] == 2 else dict(event)


@pytest.mark.parametrize(
    "wrap",
    [
        lambda inner: FilteringSink(inner, predicate=lambda event: event["n"] != 2),
        lambda inner: TransformSink(inner, _drop_event_2),
    ],
    ids=["filtering", "transform"],
)
def test_a_wrapper_around_nats_resends_the_tail(
    wrap: Callable[[NATSSink], Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wrapper offers ``deliver`` exactly when ``NATSSink`` does (SPEC-075), so it resends too.

    The wrapper drops event 2, so the inner batch is the outer one without it and the wrapper
    must map the inner verdict back by index: the unattempted tail is outer events 1, 3 and 4,
    inner positions 1, 2 and 3. A wrong mapping would resend the dropped event or skip one of
    the tail; here event 2 is never published and each tail event lands once.
    """
    run = _run_worker([["late-ok"], ["ok"]], 5, monkeypatch, wrap=wrap)
    assert run.offered == [[0, 1, 3, 4], [1, 3, 4]]
    assert 2 not in run.sent, "the event the wrapper dropped is never published"
    assert Counter(run.sent) == {0: 1, 1: 1, 3: 1, 4: 1}
    assert (run.failed, run.failed_batches, run.lines) == (0, 0, [])
