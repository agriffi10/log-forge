"""SPEC-076 FR-004 — a closed or closing sink answers without reaching its transport.

An engine sink's refused round calls ``RunContext.refuse`` with the ``SinkDeliveryError`` its 1.x
``emit`` raised and answers ``REJECTED`` for every event, so a run that fails moves no sink
counter and the adapter raises that error by identity; an emit-only adopter raises its 1.x
refusal. Every case also asserts the driver double saw no call. Each sink is built over the
recording rig ``tests/sink_rigs.py`` already has for it, or ``test_engine_sinks_roster._build``
for ``NATSSink``. Serves invariants 2, 6 and 7.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
import sink_rigs
from conftest import install_worker
from log_foundry import SinkLosses
from log_foundry.sinks import _engine
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.verdict import Outcome, Verdict
from log_foundry.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

LIMIT = 5.0

ENGINE_CLOSED: dict[str, Callable[[int], str]] = {
    "ClickHouseSink": lambda n: f"ClickHouseSink inserted none of {n} event(s): the sink is closed",
    "PostgresSink": lambda n: f"PostgresSink inserted none of {n} event(s): the sink is closed",
    "AzureEventHubsSink": lambda n: f"AzureEventHubsSink sent none of {n} event(s): the sink is closed",
    "RabbitMQSink": lambda n: f"RabbitMQSink published none of {n} message(s): the sink is closed",
    "SocketTransport": lambda n: (
        f"SocketTransport delivered none of {n} message(s): the transport is closed"
    ),
    "NATSSink": lambda n: f"NATSSink published none of {n} event(s): the sink is closed",
}
"""Each engine adopter's closed message, fda134a's text (``SocketTransport`` counts the batch, as
SPEC-074 made it). ``NATSSink`` keeps its own closed state (SPEC-061) and answers the same way."""

ENGINE_CLOSING: dict[str, Callable[[int], str]] = {
    **ENGINE_CLOSED,
    "NATSSink": lambda n: f"NATSSink published none of {n} event(s): the sink is closing",
}
"""While a close is in flight: the closed message, except ``NATSSink``'s own closing one."""

EMIT_ONLY_CLOSED: dict[str, Callable[[int], str]] = {
    "FileSink": lambda n: f"FileSink wrote none of {n} event(s): the sink is closed",
    "RotatingFileSink": lambda n: f"RotatingFileSink wrote none of {n} event(s): the sink is closed",
    "SQLiteSink": lambda n: f"SQLiteSink inserted none of {n} event(s): the sink is closed",
}
"""Each emit-only adopter's 1.x refusal, raised from ``emit``."""


class Built:
    """One engine sink under test: the object whose ``deliver`` refuses, and what to wrap."""

    def __init__(self, name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        self.name = name
        if name == "NATSSink":
            published: list[bytes] = []

            class Client:
                async def publish(self, subject: str, payload: bytes) -> None:
                    published.append(payload)

                async def close(self) -> None:
                    pass

            self.target: Any = NATSSink("logs", client=Client(), publish_timeout=1.0)
            self.outer: Any = self.target
            self.calls: Callable[[], int] = lambda: len(published)
            return
        rig = sink_rigs.RIGS["SyslogSink" if name == "SocketTransport" else name](
            monkeypatch, tmp_path
        )
        self.rig = rig
        self.outer = rig.sink
        self.target = rig.sink._socket if name == "SocketTransport" else rig.sink
        self.calls = rig.calls

    def counters(self) -> tuple[int, int, SinkLosses]:
        """``failed_events``, ``.failed`` and ``losses()`` of the sink that keeps the counters."""
        counters = self.target.log_foundry_counters
        return counters.failed_events, int(getattr(self.target, "failed", 0)), self.target.losses()


class RefuseSpy:
    """Records every error a round hands to ``RunContext.refuse``, then delegates."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.errors: list[Exception] = []
        original = _engine.RunContext.refuse
        spy = self

        def refuse(context: _engine.RunContext, error: Exception) -> None:
            spy.errors.append(error)
            original(context, error)

        monkeypatch.setattr(_engine.RunContext, "refuse", refuse)


class DeliverCount:
    """Counts the calls a class's ``deliver`` receives."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, cls: type) -> None:
        self.calls = 0
        original = cls.deliver  # type: ignore[attr-defined]
        counter = self

        def deliver(sink: Any, batch: list[dict[str, object]]) -> Any:
            counter.calls += 1
            return original(sink, batch)

        monkeypatch.setattr(cls, "deliver", deliver)


def _batch(n: int = 2) -> list[dict[str, object]]:
    return [sink_rigs.ok(f"e{i}") for i in range(n)]


def _hold_a_section(built: Built) -> tuple[threading.Thread, threading.Event]:
    """Puts a close in flight behind a section that holds the transport; returns it and its gate.

    A ``SERIAL`` guard's section is the holder of its lock, so the test takes that lock, through
    the 1.x name, on another thread. A ``CLOSE_ONLY`` guard's section is entered for real, and its
    close then waits at most ``CLOSE_OVERLAP_WAIT``, inside which the test acts. ``NATSSink``'s
    close sets its own closing flag before it takes its own lock.
    """
    target = built.target
    holding, leave = threading.Event(), threading.Event()
    guard = getattr(target, "_guard", None)

    def hold() -> None:
        if guard is not None and guard._mode.name == "CLOSE_ONLY":
            with guard.enter():
                holding.set()
                leave.wait(LIMIT)
            return
        with target._lock:
            holding.set()
            leave.wait(LIMIT)

    threading.Thread(target=hold, daemon=True).start()
    assert holding.wait(LIMIT)
    closer = threading.Thread(target=target.close, daemon=True)
    closer.start()

    def closing() -> bool:
        return bool(target._closing) if guard is None else bool(guard.closed)

    began = time.monotonic()
    while not closing():
        assert time.monotonic() - began < LIMIT, "the close never began"
        time.sleep(0.001)
    return closer, leave


@pytest.mark.parametrize("state", ["closed", "closing"])
@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_a_closed_or_closing_engine_sink_refuses_through_its_run_context(
    name: str, state: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-1: ``deliver`` refuses with the sink's own message, ``REJECTED``, and no driver call."""
    built = Built(name, monkeypatch, tmp_path)
    spy = RefuseSpy(monkeypatch)
    closer: threading.Thread | None = None
    leave: threading.Event | None = None
    if state == "closed":
        built.target.close()
    else:
        closer, leave = _hold_a_section(built)
    before = built.calls()
    try:
        verdict = built.target.deliver(_batch(3))
    finally:
        if leave is not None and closer is not None:
            leave.set()
            closer.join(LIMIT)
    expected = (ENGINE_CLOSED if state == "closed" else ENGINE_CLOSING)[name](3)
    assert isinstance(verdict, Verdict)
    assert verdict.outcomes == (Outcome.REJECTED,) * 3
    assert len(spy.errors) == 1
    assert type(spy.errors[0]) is SinkDeliveryError and str(spy.errors[0]) == expected
    assert built.calls() == before, f"{name} reached its driver while {state}"


@pytest.mark.parametrize("path", ["worker", "orphan", "emit"])
@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_a_closed_engine_sink_costs_one_deliver_and_moves_no_counter(
    name: str,
    path: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-2: one ``deliver``; the catcher counts the batch; no sink counter moves; no line."""
    built = Built(name, monkeypatch, tmp_path)
    spy = RefuseSpy(monkeypatch)
    count = DeliverCount(monkeypatch, type(built.target))
    built.target.close()
    before_counters = built.counters()
    before_calls = built.calls()
    if path == "worker":
        worker = Worker(built.outer, batch_size=1000, flush_interval=60.0)
        install_worker(worker)
        worker.submit(_batch(1))
        assert not worker.flush(timeout=LIMIT), "a refused batch reported a flush success"
        assert log_foundry.health().failed_batches == 1
    elif path == "orphan":
        log_foundry.configure(service="t", version="0", env="test", sink=built.outer)
        log_foundry.info("after close")
        assert log_foundry.health().orphan_lost == 1
    else:
        outer = built.outer
        with pytest.raises(SinkDeliveryError) as raised:
            if name == "SocketTransport":
                outer.emit(_batch(1))
            else:
                emit_via_deliver(outer, _batch(1))
        assert spy.errors and raised.value is spy.errors[-1], "the 1.x error was not raised by identity"
        assert str(raised.value) == ENGINE_CLOSED[name](1)
    assert count.calls == 1, f"{name}: {count.calls} deliver call(s) for one refused batch"
    assert built.counters() == before_counters, "a refusal moved a sink counter"
    assert built.calls() == before_calls
    assert "SinkDeliveryError" not in capsys.readouterr().err


def test_a_closed_socket_mode_logstash_raises_the_transports_error_by_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-2's twin: socket-mode ``LogstashSink`` refuses through the ``SocketTransport`` it holds."""
    rig = sink_rigs.RIGS["LogstashSink"](monkeypatch, tmp_path)
    spy = RefuseSpy(monkeypatch)
    rig.sink.close()
    before = rig.calls()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.emit(_batch(1))
    assert spy.errors and raised.value is spy.errors[-1]
    assert str(raised.value) == ENGINE_CLOSED["SocketTransport"](1)
    assert rig.calls() == before
    assert rig.sink._socket.losses() == SinkLosses(dropped=0, failed=0)


@pytest.mark.parametrize("name", sorted(EMIT_ONLY_CLOSED))
def test_an_emit_only_adopter_raises_its_1x_refusal(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-3: exactly ``SinkDeliveryError`` with today's message; no count, no driver call."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    rig.sink.close()
    before_calls = rig.calls()
    before_losses = rig.sink.losses()
    with pytest.raises(SinkDeliveryError) as raised:
        rig.sink.emit(_batch(2))
    assert type(raised.value) is SinkDeliveryError
    assert str(raised.value) == EMIT_ONLY_CLOSED[name](2)
    assert rig.calls() == before_calls
    assert rig.sink.losses() == before_losses


BETWEEN_ROUNDS = ("ClickHouseSink", "RabbitMQSink", "SyslogSink")


def _two_round_sink(name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    """Builds a per-event sink with one resend allowed, whose second driver call fails once."""
    rig = sink_rigs.RIGS[name](monkeypatch, tmp_path)
    calls = {"n": 0}
    if name == "ClickHouseSink":
        from log_foundry.sinks.clickhouse import ClickHouseSink

        double: Any = sink_rigs._ClickHouse(rig.state)
        sink: Any = ClickHouseSink("t", client=double, max_retries=1, chunk_size=1)
        cls, method = sink_rigs._ClickHouse, "insert"
    elif name == "RabbitMQSink":
        from log_foundry.sinks.rabbitmq import RabbitMQSink

        sink = RabbitMQSink(
            exchange="x", routing_key="k", connection=sink_rigs._Connection(rig.state),
            max_retries=1,
        )
        cls, method = sink_rigs._Channel, "basic_publish"
    else:
        from log_foundry.sinks.syslog import SyslogSink

        sink = SyslogSink("loghost", 514, transport="udp", max_retries=1)
        cls, method = sink_rigs._Socket, "sendto"
    original = getattr(cls, method)

    def second_fails(self: Any, *args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 2:
            rig.state.log.append(("refused", None))
            raise ConnectionRefusedError(111, "Connection refused")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(cls, method, second_fails)
    return sink, rig, calls


@pytest.mark.parametrize("name", BETWEEN_ROUNDS)
def test_a_sink_closed_between_two_rounds_settles_and_counts_the_refused_round_once(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-4: round one lands one event; the close lands in the backoff; round two calls nothing.

    The run settles, so no catcher sees it: the refused round's event is counted once in
    ``failed_events`` with one line, and ``flush()`` is truthy.
    """
    sink, rig, calls = _two_round_sink(name, monkeypatch, tmp_path)

    def close_in_the_backoff(delay: float, stop: Any) -> Any:
        sink.close()
        return stop

    monkeypatch.setattr(_engine, "_wait", close_in_the_backoff)
    counters = sink.log_foundry_counters
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=0)
    install_worker(worker)
    worker.submit(_batch(2))
    assert worker.flush(timeout=LIMIT), "a run that landed an event did not settle"
    assert calls["n"] == 2, "the round after the close made a driver call"
    assert rig.delivered() == ["e0"], "an event was offered twice or the wrong one landed"
    assert counters.failed_events == 1
    lines = [line for line in capsys.readouterr().err.splitlines() if "lost" in line]
    assert len(lines) == 1, lines


@pytest.mark.parametrize("name", sorted(ENGINE_CLOSED))
def test_the_worker_and_orphan_paths_leave_one_ledger(
    name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """AC-7: one event to a closed engine sink leaves the same ``SinkLosses`` on either path."""
    ledgers: list[SinkLosses] = []
    for path in ("worker", "orphan"):
        (tmp_path / path).mkdir()
        built = Built(name, monkeypatch, tmp_path / path)
        built.target.close()
        if path == "worker":
            worker = Worker(built.outer, batch_size=1000, flush_interval=60.0)
            install_worker(worker)
            worker.submit(_batch(1))
            worker.flush(timeout=LIMIT)
        else:
            log_foundry.configure(service="t", version="0", env="test", sink=built.outer)
            log_foundry.info("after close")
        ledgers.append(built.target.losses())
        log_foundry.shutdown(timeout=LIMIT)
    assert ledgers[0] == ledgers[1] == SinkLosses(dropped=0, failed=0)
