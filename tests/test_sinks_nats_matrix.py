"""SPEC-061 FR-005 AC-9 — NATSSink's wait and latch matrix, one parametrized cell per row.

Every cell is one row of the sibling sweep diff review frame 3 ran: a client the sink connected
or an injected one, core or JetStream, the call made (``emit`` of one event or of three,
``flush``, ``close``) and the point where the budget expires (the probe, the first publish, a
mid-batch publish, the confirmation, an already-spent deadline, a JetStream ack wait), plus the
degenerate rows. A cell asserts the outcome (a return or the exception type and its words), the
exact ``losses()`` counts, every announcement line in order, and for a stall the latency of the
next caller (made while another thread holds the lock, so only the check before the lock can
refuse it) and of a caller queued on the lock behind the stalled call. The expectations are
derived from the spec, not read off the code, and the module is gated by mutation: every guard in
``nats.py``'s wait and latch sites turns at least one cell red on its stated reason.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import math
import pathlib
import re
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry.sinks.nats as nats_sink_module
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.nats import (
    DEFAULT_CLOSE_TIMEOUT,
    DEFAULT_PUBLISH_TIMEOUT,
    LOOP_JOIN_TIMEOUT,
    NATSSink,
)
from test_sinks_nats_liveness import (
    CounterlessFakeNATS,
    FlushlessFakeNATS,
    HostileCounterNATS,
    LiveFakeNATS,
    _build,
    _on_loop,
    _timed,
    _wait_until,
)

if TYPE_CHECKING:
    from collections.abc import Callable

P = 0.4
"""The ``publish_timeout`` every stall cell runs under: small enough to keep the corpus fast."""

D = 0.3
"""The ``drain_timeout`` the close cells pass, so a close against a stall costs 0.3 s, not 1 s."""

NEXT_BOUND = 0.1
"""Seconds the next caller may take to be refused while another thread holds the lock.

A refusal made before the lock takes microseconds; a caller that reaches the lock instead waits
for the holder, which releases after :data:`HOLD` seconds, so the gap is more than a second.
"""

HOLD = 1.5
"""Seconds the lock-holding thread keeps the lock, the cost a missing pre-lock refusal pays."""

QUEUED_BOUND = 1.5 * P
"""Seconds a caller queued on the lock behind a stalled call may take, from its own start.

It waits out the stalled call, at most ``P``, and is then refused at once; a caller that instead
spends its own budget takes ``2 * P``. The bound sits half a budget clear of both.
"""

UNATTEMPTED = f"NATSSink publish_timeout {P}s"
NO_PONG = "NATSSink unconfirmed, no PONG after the batch"
ABANDONED = "NATSSink unconfirmed, publish abandoned"
JS_ABANDONED = "NATSSink unconfirmed, JetStream publish abandoned"
ACK_RAISED = "NATSSink publish, TimeoutError"
STUCK_OR_LATCHED = "abandoned|has not answered a PING"


class StarvedJetStream:
    """A JetStream context whose ack never comes: the driver's ``wait_for(reply, timeout)``."""

    def __init__(self, owner: MatrixFake) -> None:
        self._owner = owner

    async def publish(self, subject: str, payload: bytes, *, timeout=None) -> None:  # noqa: ASYNC109
        self._owner._record("publish")
        self._owner.publish_entered.set()
        never = asyncio.get_running_loop().create_future()
        await asyncio.wait_for(never, timeout)


class MatrixFake(LiveFakeNATS):
    """One configurable ``nats-py``-shaped client for every row of the matrix.

    Attributes:
      silent_after: How many PINGs the server answers before it falls silent; None answers all.
      block_at: The zero-based publish that blocks in the driver's force-flush, its bytes
        already queued as ``_send_command`` queues them, until :attr:`release` is set.
      hold_at: The zero-based publish that parks before queueing anything until :attr:`release`
        is set, so it is in flight at the deadline by construction while the buffer reads drained.
      starve: Whether a JetStream ack never arrives, so the driver's own ack timeout raises.
      first_pong_delay: Seconds before the first PING's PONG arrives, the rest as configured.
      cancelled_flushes: How many of this client's ``flush`` calls were cancelled.
      release: The event that lets a blocked publish finish.
    """

    def __init__(
        self,
        *,
        silent_after: int | None = None,
        block_at: int | None = None,
        hold_at: int | None = None,
        park: bool = False,
        starve: bool = False,
        hold_close: bool = False,
        first_pong_delay: float = 0.0,
    ) -> None:
        super().__init__()
        self.silent_after = silent_after
        self.block_at = block_at
        self.hold_at = hold_at
        self.flusher_parked = park
        self.starve = starve
        self.cancelled_flushes = 0
        self.first_pong_delay = first_pong_delay
        self.release: asyncio.Event | None = None
        if hold_close:
            self.close_gate = asyncio.Event()

    async def flush(self, timeout: float = 10) -> None:  # noqa: ASYNC109
        if self.silent_after is not None and self.calls.count("flush") >= self.silent_after:
            self.answering = False
        if self.first_pong_delay and not self.calls.count("flush"):
            self.answering = False
            asyncio.get_running_loop().call_later(self.first_pong_delay, self.deliver_pong)
        try:
            await super().flush(timeout)
        except asyncio.CancelledError:
            self.cancelled_flushes += 1
            raise

    def jetstream(self) -> Any:
        return StarvedJetStream(self) if self.starve else super().jetstream()

    async def _enter_publish(self, payload: bytes) -> None:
        index = self.calls.count("publish")
        self._record("publish")
        self.publish_entered.set()
        if index == self.hold_at:
            self.release = asyncio.Event()
            await self.release.wait()
        if index == self.block_at:
            self._add_pending(len(payload))
            self.release = asyncio.Event()
            await self.release.wait()
            self.published.append(payload)
            self._kick_flusher()
            return
        self.published.append(payload)
        self._add_pending(len(payload))
        self._kick_flusher()


@dataclass(frozen=True)
class Cell:
    """One row of the matrix: its configuration and everything it must observe."""

    owned: bool
    js: bool
    call: str
    fake: dict[str, Any] = field(default_factory=dict)
    raises: type[BaseException] | None = None
    match: str = ""
    failed: int = 0
    lines: tuple[tuple[int, str], ...] = ()
    prelude: bool = False
    next_refused: str | None = None
    queued_refused: bool = False
    released_accepts: bool = False
    pings: int | None = None
    ceiling: float | None = None


def _cells() -> dict[str, Cell]:
    """The matrix, keyed by cell id; every expectation is written from the spec's rules."""
    cells: dict[str, Cell] = {}

    def add(cell_id: str, **kw: Any) -> None:
        cells[cell_id] = Cell(**kw)

    for js in (False, True):
        mode = "js" if js else "core"
        for owned in (True, False):
            who = "owned" if owned else "injected"
            base = {"owned": owned, "js": js}
            stem = f"{who}-{mode}"
            add(f"{stem}-emit1-healthy", call="emit1", **base)
            add(f"{stem}-emit3-healthy", call="emit3", **base)
            add(f"{stem}-flush-healthy", call="flush", pings=1 if owned else None, **base)
            add(f"{stem}-close-healthy", call="close", pings=1 if owned else 0, **base)
            abandoned = JS_ABANDONED if js else (NO_PONG if owned else ABANDONED)
            # An owned core client's stuck publish also leaves its buffer undrained, so the probe
            # left behind latches too, from the call's start; which refusal a later caller meets
            # depends on whether that PING's PONG has landed, and both are the correct refusal.
            stuck = STUCK_OR_LATCHED if owned and not js else "abandoned"
            add(
                f"{stem}-emit1-first-publish",
                call="emit1",
                fake={"block_at": 0},
                failed=1,
                lines=((1, abandoned),),
                next_refused=stuck,
                queued_refused=True,
                released_accepts=True,
                **base,
            )
            add(
                f"{stem}-emit3-first-publish",
                call="emit3",
                fake={"block_at": 0},
                failed=3,
                lines=((2, UNATTEMPTED), (1, abandoned)),
                next_refused=stuck,
                **base,
            )
            second = 2 if owned and not js else 1
            add(
                f"{stem}-emit3-mid-batch",
                call="emit3",
                fake={"block_at": 1},
                failed=1 + second,
                lines=((1, UNATTEMPTED), (second, abandoned)),
                next_refused=stuck,
                **base,
            )
            # The second publish is held open before it queues a byte, so the deadline passes with
            # it in flight and the buffer drained: an owned core confirmation then meets a spent
            # deadline (AC-3), not mid-batch's undrained buffer.
            add(
                f"{stem}-emit3-deadline-spent",
                call="emit3",
                fake={"hold_at": 1},
                failed=1 + second,
                lines=((1, UNATTEMPTED), (second, abandoned)),
                **base,
            )
            add(
                f"{stem}-flush-after-stuck",
                call="flush",
                fake={"block_at": 0},
                prelude=True,
                raises=SinkDeliveryError,
                match=stuck,
                failed=1,
                pings=0 if owned else None,
                **base,
            )
            add(
                f"{stem}-close-after-stuck",
                call="close",
                fake={"block_at": 0},
                prelude=True,
                failed=1,
                pings=0,
                **base,
            )
        add(
            f"owned-{mode}-emit1-probe",
            owned=True,
            js=js,
            call="emit1",
            fake={"silent_after": 0},
            raises=SinkDeliveryError,
            match="no PONG",
            next_refused="has not answered a PING",
            queued_refused=True,
        )
        add(
            f"owned-{mode}-emit3-probe",
            owned=True,
            js=js,
            call="emit3",
            fake={"silent_after": 0},
            raises=SinkDeliveryError,
            match="no PONG",
            next_refused="has not answered a PING",
        )
        add(
            f"owned-{mode}-flush-probe",
            owned=True,
            js=js,
            call="flush",
            fake={"silent_after": 0},
            raises=SinkDeliveryError,
            match="no PONG",
            pings=1,
            next_refused="has not answered a PING",
            queued_refused=True,
        )
        add(
            f"owned-{mode}-close-probe",
            owned=True,
            js=js,
            call="close",
            fake={"silent_after": 0},
            raises=TimeoutError,
            match="closing PING",
            pings=1,
        )
        for owned in (True, False):
            who = "owned" if owned else "injected"
            add(
                f"{who}-js-emit1-ack-starved",
                owned=owned,
                js=True,
                call="emit1",
                fake={"starve": True},
                raises=SinkDeliveryError,
                match="delivered none of 1",
                failed=1,
                lines=((1, ACK_RAISED),),
            )
            add(
                f"{who}-js-emit3-ack-starved",
                owned=owned,
                js=True,
                call="emit3",
                fake={"starve": True},
                ceiling=0.05,
                raises=SinkDeliveryError,
                match="delivered none of 3",
                failed=3,
                lines=((1, ACK_RAISED), (1, ACK_RAISED), (1, ACK_RAISED)),
            )
    for size in (1, 3):
        add(
            f"owned-core-emit{size}-confirmation",
            owned=True,
            js=False,
            call=f"emit{size}",
            fake={"silent_after": 1},
            failed=size,
            lines=((size, NO_PONG),),
            next_refused="has not answered a PING",
            queued_refused=size == 1,
        )
        add(
            f"owned-core-emit{size}-undrained-silent",
            owned=True,
            js=False,
            call=f"emit{size}",
            fake={"silent_after": 1, "park": True},
            failed=size,
            lines=((size, NO_PONG),),
            next_refused="has not answered a PING",
            queued_refused=size == 1,
        )
        add(
            f"owned-js-emit{size}-confirmation",
            owned=True,
            js=True,
            call=f"emit{size}",
            fake={"silent_after": 1},
        )
        add(
            f"injected-core-emit{size}-silent-server",
            owned=False,
            js=False,
            call=f"emit{size}",
            fake={"silent_after": 0},
        )
    add(
        "owned-core-emit1-slow-probe-then-undrained",
        owned=True,
        js=False,
        call="emit1",
        fake={"first_pong_delay": 2 * P / 3, "silent_after": 1, "park": True},
        failed=1,
        lines=((1, NO_PONG),),
        pings=2,
        next_refused="has not answered a PING",
        queued_refused=True,
    )
    add(
        "owned-core-emit1-undrained-answering",
        owned=True,
        js=False,
        call="emit1",
        fake={"park": True},
        failed=1,
        lines=((1, NO_PONG),),
        pings=2,
    )
    add(
        "owned-core-flush-undrained-answering",
        owned=True,
        js=False,
        call="flush",
        fake={"park": True},
        prelude=True,
        raises=SinkDeliveryError,
        match="no PONG confirmed",
        failed=1,
        pings=1,
    )
    add(
        "owned-core-close-undrained-answering",
        owned=True,
        js=False,
        call="close",
        fake={"park": True},
        prelude=True,
        raises=TimeoutError,
        match="closing PING",
        failed=1,
        pings=0,
    )
    add(
        "owned-core-close-undrained-silent",
        owned=True,
        js=False,
        call="close",
        fake={"park": True, "silent_after": 1},
        prelude=True,
        failed=1,
        pings=0,
    )
    add(
        "owned-core-flush-undrained-silent",
        owned=True,
        js=False,
        call="flush",
        fake={"park": True, "silent_after": 1},
        prelude=True,
        raises=SinkDeliveryError,
        match="has not answered a PING",
        failed=1,
        pings=0,
    )
    add(
        "injected-core-flush-silent-server",
        owned=False,
        js=False,
        call="flush",
        fake={"silent_after": 0},
        raises=SinkDeliveryError,
        match="did not return",
    )
    return cells


CELLS = _cells()


def _make(cell: Cell, monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> tuple[NATSSink, Any]:
    """Builds the cell's sink, through the fake ``nats`` module or with the client injected."""
    options = {"publish_timeout": P, "jetstream": cell.js, **kwargs}
    if cell.owned:
        sink, fake, _ = _build(
            monkeypatch, factory=lambda: MatrixFake(**cell.fake), drain_timeout=D, **options
        )
        return sink, fake
    fake = MatrixFake(**cell.fake)
    return NATSSink("logs", client=fake, **options), fake


def _run(sink: NATSSink, call: str) -> Callable[[], object]:
    """Returns the cell's call as a no-argument callable."""
    if call == "emit1":
        return lambda: sink.emit([{"n": 0}])
    if call == "emit3":
        return lambda: sink.emit([{"n": i} for i in range(3)])
    if call == "flush":
        return sink.flush
    return sink.close


def _lost_lines(err: str) -> list[str]:
    """The announcement lines a run wrote, in order."""
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost ")]


def _refused_while_locked(sink: NATSSink) -> tuple[float, BaseException | None]:
    """Calls ``emit`` while another thread holds the sink's lock, so only a pre-lock check refuses.

    Waits briefly for either latch to read held, since the probe a stalled call leaves behind is
    aged from the start of the call whose budget it spent and holds by the call's return; a sink
    whose latch never holds fails the call's own assertion instead, after :data:`HOLD` seconds.
    """
    _wait_until(lambda: sink._latched() or sink._publish_stuck(), within=NEXT_BOUND)
    holding = threading.Event()
    release = threading.Event()

    def hold() -> None:
        with sink._lock:
            holding.set()
            release.wait(HOLD)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(2.0)
    try:
        return _timed(lambda: sink.emit([{"n": "next"}]))
    finally:
        release.set()
        holder.join(5.0)


def _close_quietly(sink: NATSSink) -> None:
    with contextlib.suppress(Exception):
        sink.close()


@pytest.mark.parametrize("cell_id", sorted(CELLS))
def test_matrix_cell(cell_id: str, monkeypatch, capsys) -> None:
    cell = CELLS[cell_id]
    if cell.ceiling is not None:
        monkeypatch.setattr(nats_sink_module, "DEFAULT_ACK_TIMEOUT", cell.ceiling)
    sink, fake = _make(cell, monkeypatch)
    try:
        if cell.prelude:
            sink.emit([{"n": "prelude"}])
            if cell.fake.get("park") and cell.fake.get("silent_after") is None:
                assert _wait_until(lambda: sink._probe is None or sink._probe[0].done())
            else:
                _wait_until(lambda: sink._latched() or sink._publish_stuck(), within=NEXT_BOUND)
        capsys.readouterr()
        pings_before = fake.calls.count("flush")
        queued: list[tuple[float, BaseException | None]] = []
        waiter = None
        if cell.queued_refused:
            outcome: list[tuple[float, BaseException | None]] = []
            caller = threading.Thread(target=lambda: outcome.append(_timed(_run(sink, cell.call))))
            caller.start()
            assert _wait_until(
                lambda: bool(fake.calls[pings_before:]) or fake.calls.count("publish") > 0
            )
            waiter = threading.Thread(
                target=lambda: queued.append(_timed(lambda: sink.emit([{"n": "queued"}])))
            )
            waiter.start()
            caller.join(10.0)
            waiter.join(10.0)
            elapsed, error = outcome[0]
        else:
            elapsed, error = _timed(_run(sink, cell.call))
        losses = sink.losses()
        pings = fake.calls.count("flush") - pings_before
        cancelled = fake.cancelled_flushes
        lines = _lost_lines(capsys.readouterr().err)

        if cell.raises is None:
            assert error is None, f"{cell_id}: {error!r}"
        else:
            assert isinstance(error, cell.raises), f"{cell_id}: {error!r}"
            assert re.search(cell.match, str(error)), f"{cell_id}: {error}"
            assert "wedged" not in str(error), f"{cell_id}: the backstop fired: {error}"
        assert losses == SinkLosses(dropped=0, failed=cell.failed), f"{cell_id}: {losses}"
        assert lines == [f"log-foundry: lost {n} event(s); {why}" for n, why in cell.lines], lines
        if cell.pings is not None:
            assert pings == cell.pings, f"{cell_id}: {pings} PING(s), {fake.calls}"
        assert elapsed <= P + 1.0 or cell.call == "close", f"{cell_id}: {elapsed:.2f}s"
        if cell.call == "close":
            assert elapsed <= D + LOOP_JOIN_TIMEOUT + 0.5, f"{cell_id}: close {elapsed:.2f}s"
            assert "close" in fake.calls
            assert not sink._thread.is_alive(), f"{cell_id}: the loop thread outlived close()"
        else:
            assert cancelled == 0, f"{cell_id}: the sink cancelled a PONG wait"
        if cell.queued_refused:
            q_elapsed, q_error = queued[0]
            assert isinstance(q_error, SinkDeliveryError), f"{cell_id}: queued {q_error!r}"
            assert q_elapsed <= QUEUED_BOUND, f"{cell_id}: queued caller {q_elapsed:.2f}s"
        if cell.next_refused is not None:
            n_elapsed, n_error = _refused_while_locked(sink)
            assert isinstance(n_error, SinkDeliveryError), f"{cell_id}: next {n_error!r}"
            assert re.search(cell.next_refused, str(n_error)), f"{cell_id}: next {n_error}"
            assert n_elapsed < NEXT_BOUND, f"{cell_id}: next caller {n_elapsed:.3f}s"
        if cell.released_accepts:

            async def release() -> None:
                assert fake.release is not None
                fake.release.set()

            _on_loop(sink, release())
            assert _wait_until(lambda: not sink._publish_stuck()), f"{cell_id}: latch held"
            assert _wait_until(lambda: sink._stuck == ()), (
                f"{cell_id}: a finished publish was left in the latch's record, which then grows "
                "by one task per abandoned publish for the life of an outage"
            )
            fake.answering = True
            fake.silent_after = None
            after = _timed(lambda: sink.emit([{"n": "after"}]))[1]
            assert after is None, f"{cell_id}: not accepted after the release: {after!r}"
    finally:
        if waiter is not None:
            waiter.join(5.0)
        _close_quietly(sink)


# --- Degenerate rows -----------------------------------------------------------------------------


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")], ids=repr)
def test_matrix_publish_timeout_that_bounds_nothing_falls_back(monkeypatch, value) -> None:
    sink, _fake, _ = _build(monkeypatch, publish_timeout=value)
    try:
        assert sink.publish_timeout == DEFAULT_PUBLISH_TIMEOUT
        sink.emit([{"n": 0}])
        assert sink.losses() == SinkLosses(dropped=0, failed=0)
    finally:
        sink.close()


@pytest.mark.parametrize("js", [False, True], ids=["core", "js"])
def test_matrix_publish_timeout_at_its_floor_refuses_and_counts_nothing(
    monkeypatch, capsys, js: bool
) -> None:
    # The smallest positive budget leaves no time for the probe's PONG: a refusal, reported to
    # the worker and counted nowhere, never a publish the sink then has to guess about.
    sink, fake, _ = _build(monkeypatch, publish_timeout=1e-9, jetstream=js)
    try:
        elapsed, error = _timed(lambda: sink.emit([{"n": 0}]))
        assert isinstance(error, SinkDeliveryError) and "wedged" not in str(error), error
        assert elapsed < NEXT_BOUND
        assert sink.losses() == SinkLosses(dropped=0, failed=0)
        assert "publish" not in fake.calls
        assert _lost_lines(capsys.readouterr().err) == []
    finally:
        _close_quietly(sink)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")], ids=repr)
def test_matrix_drain_timeout_that_bounds_nothing_falls_back(monkeypatch, value) -> None:
    sink, _fake, module = _build(
        monkeypatch, factory=lambda: MatrixFake(silent_after=0), drain_timeout=value
    )
    assert sink._drain_timeout == DEFAULT_CLOSE_TIMEOUT
    forwarded = module.calls[0][1]["drain_timeout"]
    assert forwarded == value or (math.isnan(value) and math.isnan(forwarded))
    elapsed, error = _timed(sink.close)
    assert isinstance(error, TimeoutError) and "closing PING" in str(error), error
    assert elapsed <= DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + 0.5, f"{elapsed:.2f}s"
    assert not sink._thread.is_alive()


class NoCloseNoFlushNATS:
    """A client offering ``publish`` alone."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def publish(self, subject: str, payload: bytes) -> None:
        self.calls.append("publish")


@pytest.mark.parametrize("owned", [True, False], ids=["owned", "injected"])
@pytest.mark.parametrize(
    "factory",
    [CounterlessFakeNATS, HostileCounterNATS, FlushlessFakeNATS, NoCloseNoFlushNATS],
    ids=["no-pending-counter", "pending-counter-raises", "no-flush", "no-flush-no-close"],
)
def test_matrix_a_client_missing_a_capability_is_never_refused_for_it(
    monkeypatch, capsys, factory, owned: bool
) -> None:
    # The sink does not invent a loss or a refusal for a capability the client never claimed:
    # every call returns, nothing is counted, nothing is announced, and the loop thread ends.
    if owned:
        sink, client, _ = _build(monkeypatch, factory=factory, publish_timeout=P)
    else:
        client = factory()
        sink = NATSSink("logs", client=client, publish_timeout=P)
    outcomes = [
        _timed(lambda: sink.emit([{"n": i} for i in range(3)])),
        _timed(sink.flush),
        _timed(sink.close),
    ]
    for elapsed, error in outcomes:
        assert error is None, error
        assert elapsed < NEXT_BOUND, f"{elapsed:.3f}s"
    assert client.calls.count("publish") == 3
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert _lost_lines(capsys.readouterr().err) == []
    assert not sink._thread.is_alive()


def test_matrix_backstop_expiry_on_a_finished_future_still_closes(monkeypatch) -> None:
    # The backstop's bounded wait can report expiry for a coroutine that finished on the last
    # tick; close() must take the outcome, mark the sink closed and stop the loop, so a second
    # close() does not reach the client again.
    real_submit = NATSSink._submit

    class ExpiredButDone:
        def __init__(self, inner: Any) -> None:
            self._inner = inner
            self.first = True

        def result(self, timeout: float | None = None) -> Any:
            if timeout is not None and self.first:
                self.first = False
                self._inner.result(timeout)
                raise TimeoutError("the bound expired on a finished future")
            return self._inner.result(timeout)

        def done(self) -> bool:
            return self._inner.done()

    monkeypatch.setattr(
        NATSSink, "_submit", lambda self, coro: ExpiredButDone(real_submit(self, coro))
    )
    sink, fake, _ = _build(monkeypatch)
    error = _timed(sink.close)[1]
    assert error is None, f"a finished close() read as a wedged loop: {error!r}"
    assert sink._closed and not sink._thread.is_alive()
    sink.close()
    assert fake.calls.count("close") == 1


def test_matrix_a_client_close_that_swallows_a_cancel_ends_inside_the_sequence(
    monkeypatch,
) -> None:
    # Step two's cancel must be delivered before step three cancels again: two cancels made before
    # a task next runs coalesce, and a client that swallows one (the driver's `_close` does)
    # would then be ended only by the loop's stop.
    ended_in_sequence: list[bool] = []
    real_close_on_loop = NATSSink._close_on_loop

    async def spy(self: NATSSink) -> Any:
        outcome = await real_close_on_loop(self)
        ended_in_sequence.append(self._client.close_ended)
        return outcome

    monkeypatch.setattr(NATSSink, "_close_on_loop", spy)
    sink, fake, _ = _build(
        monkeypatch, factory=lambda: MatrixFake(hold_close=True), drain_timeout=D
    )
    elapsed, error = _timed(sink.close)
    assert error is None, error
    assert elapsed <= D + LOOP_JOIN_TIMEOUT + 0.5, f"{elapsed:.2f}s"
    assert fake._cancels_swallowed == 1
    assert ended_in_sequence == [True], "step three's cancel merged into step two's"


def test_matrix_a_liveness_ping_never_confirms_the_buffer(monkeypatch) -> None:
    # A PING sent over an undrained buffer may have overtaken the publishes. Here the buffer then
    # drains and that PING's PONG lands during a flush: the flush takes it as liveness only and
    # confirms with a fresh PING sent after the drain. The probe an undrained emit leaves behind
    # is already a budget old, so the young one is sent through the sink's own `_send_probe` on
    # the loop, over the still-parked buffer, once the left-behind one is answered.
    sink, fake, _ = _build(
        monkeypatch, factory=lambda: MatrixFake(silent_after=1, park=True), publish_timeout=P
    )
    try:
        sink.emit([{"n": 1}])
        assert fake.pubs_at_ping == [0, 0]

        async def young_liveness_probe() -> None:
            assert sink._probe is not None
            fake.deliver_pong()
            await asyncio.wait({sink._probe[0]}, timeout=2.0)
            assert sink._outstanding_probe() is None
            sink._send_probe(silent_since=time.monotonic())

        _on_loop(sink, young_liveness_probe())
        assert fake.pubs_at_ping == [0, 0, 0]

        async def drain_and_answer_late() -> None:
            fake.flusher_parked = False
            fake._run_flusher()
            fake.silent_after = None
            fake.answering = True
            asyncio.get_running_loop().call_later(0.1, fake.deliver_pong)

        _on_loop(sink, drain_and_answer_late())
        error = _timed(sink.flush)[1]
        assert error is None, error
        assert fake.pubs_at_ping == [0, 0, 0, 1], "the confirmation must be sent after the drain"
    finally:
        _close_quietly(sink)


def test_matrix_an_abandoned_injected_flush_is_not_cancelled_and_does_not_latch() -> None:
    # The injected flush is a PONG wait: abandoned at the budget, left running, and the sink stays
    # open and accepts the next emit, because nothing queues behind a PONG wait.
    fake = MatrixFake(silent_after=0)
    sink = NATSSink("logs", client=fake, publish_timeout=P)
    try:
        elapsed, error = _timed(sink.flush)
        _on_loop(sink, asyncio.sleep(0.01))
        assert isinstance(error, SinkDeliveryError) and "did not return" in str(error), error
        assert P <= elapsed < P + 0.5, f"{elapsed:.2f}s"
        assert fake.cancelled_flushes == 0, "the sink cancelled the injected flush's PONG wait"
        assert _timed(lambda: sink.emit([{"n": 1}]))[1] is None
    finally:
        _close_quietly(sink)


# --- The timing rule, held on the source ---------------------------------------------------------


def test_every_driver_timeout_ends_inside_the_sinks_wait_or_outlives_it_by_design() -> None:
    """``DRIVER_DEADLINE_MARGIN``'s rule, checked on the AST rather than trusted.

    Every ``timeout=`` the sink hands a driver call is either ``_driver_timeout(...)`` (an expiry
    the driver must raise inside the sink's wait) or :data:`PROBE_FLUSH_TIMEOUT` (a PONG wait that
    must outlive it). The sink's own waits (``asyncio.wait``) are the other side of the rule and
    may take any bound. The roster of driver sites is pinned, so a new one fails here until it is
    routed and added.
    """
    source = pathlib.Path(nats_sink_module.__file__).read_text(encoding="utf-8")
    sites = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        callee = ast.unparse(node.func)
        for keyword in node.keywords:
            if keyword.arg != "timeout" or callee == "asyncio.wait":
                continue
            value = ast.unparse(keyword.value)
            assert value == "PROBE_FLUSH_TIMEOUT" or value.startswith("_driver_timeout("), (
                f"{callee}(timeout={value}) does not route through the timing rule"
            )
            sites.append((callee, value.split("(")[0]))
    assert sorted(sites) == [
        ("self._client.flush", "PROBE_FLUSH_TIMEOUT"),
        ("target.publish", "_driver_timeout"),
    ], sites


@pytest.mark.parametrize("remaining", [1e-9, 1e-3, 0.05, 0.1, 0.4, 10.0])
def test_a_driver_timeout_is_positive_and_strictly_inside_what_remains(remaining: float) -> None:
    timeout = nats_sink_module._driver_timeout(remaining, nats_sink_module.DEFAULT_ACK_TIMEOUT)
    assert 0 < timeout < remaining
    assert timeout <= nats_sink_module.DEFAULT_ACK_TIMEOUT
    if remaining >= 2 * nats_sink_module.DRIVER_DEADLINE_MARGIN:
        assert remaining - timeout >= nats_sink_module.DRIVER_DEADLINE_MARGIN - 1e-12


def _stall_cells() -> list[str]:
    """The cells whose next or queued caller carries a latency bound (for the module docstring)."""
    return [k for k, c in CELLS.items() if c.next_refused or c.queued_refused]


def test_the_matrix_covers_each_axis() -> None:
    # A guard on the corpus itself: a refactor that silently drops a dimension shows here.
    calls = {c.call for c in CELLS.values()}
    assert calls == {"emit1", "emit3", "flush", "close"}
    assert {(c.owned, c.js) for c in CELLS.values()} == {
        (True, False),
        (True, True),
        (False, False),
        (False, True),
    }
    assert len(_stall_cells()) >= 15
