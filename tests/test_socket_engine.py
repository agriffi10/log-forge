"""SPEC-074 FR-005 — the socket transport gains ``deliver``, and the two socket sinks share it.

``SocketTransport`` frames each event with its caller's frame function and sends every remaining
frame once per round under its lock for that round only; ``SyslogSink.deliver`` forwards to it
and shares its counters, and socket-mode ``LogstashSink.emit`` runs the inline adapter over it.
The socket here is a double that decodes each frame back to the event's ``n`` and answers by
script. Serves invariants 2, 3, 6 and 7.
"""

from __future__ import annotations

import errno
import re
import threading
from typing import Any

import pytest

import log_foundry
import log_foundry.sinks._socket as socket_mod
from conftest import install_worker
from log_foundry.sinks.base import Sink, SinkDeliveryError, SinkLosses
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.syslog import SyslogSink
from log_foundry.sinks.verdict import read_deliver
from log_foundry.worker import Worker

_N = re.compile(rb'"n": (\d+)')


class Socket:
    """A UDP or TCP double: ``plan`` maps a send's 0-based number to the error it raises."""

    def __init__(self, plan: dict[int, Exception] | None = None, refuse: Exception | None = None):
        self.plan = dict(plan or {})
        self.refuse = refuse
        self.attempts: list[int | None] = []
        self.received: list[int | None] = []

    def _go(self, data: bytes) -> None:
        found = _N.search(data)
        n = int(found.group(1)) if found else None
        error = self.plan.pop(len(self.attempts), None) or self.refuse
        self.attempts.append(n)
        if error is not None:
            raise error
        self.received.append(n)

    def sendto(self, data: bytes, addr: tuple[object, ...]) -> None:
        self._go(data)

    def sendall(self, data: bytes) -> None:
        self._go(data)

    def close(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


def install(monkeypatch: pytest.MonkeyPatch, sock: Socket) -> None:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: sock)
    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: sock)


def events(n: int) -> list[dict[str, object]]:
    return [{"n": i} for i in range(1, n + 1)]


def syslog(**kwargs: Any) -> SyslogSink:
    kwargs.setdefault("max_retries", 3)
    return SyslogSink("loghost", 514, transport="udp", **kwargs)


def logstash(**kwargs: Any) -> LogstashSink:
    kwargs.setdefault("max_retries", 3)
    return LogstashSink(host="loghost", port=5000, transport="udp", **kwargs)


def lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost")]


def _shared(sink: SyslogSink) -> None:
    """The one-ledger claims, checked after every case below."""
    assert sink.log_foundry_counters is sink._socket.log_foundry_counters
    assert sink.losses() == sink._socket.losses()
    assert not isinstance(sink._socket, Sink)
    bound = read_deliver(sink)
    assert bound is not None and bound.__self__ is sink  # type: ignore[attr-defined]


REFUSED = OSError(errno.ECONNREFUSED, "Connection refused")


def test_syslog_spends_four_rounds_then_raises(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    sock = Socket(refuse=REFUSED)
    install(monkeypatch, sock)
    sink = syslog()
    with pytest.raises(SinkDeliveryError):
        sink.emit(events(5))
    assert len(sock.attempts) == 20, "4 rounds of 5 frames"
    assert sink.failed == 5
    lines = lost(capsys.readouterr().err)
    assert len(lines) == 1 and "lost 5 message(s)" in lines[0] and "4 attempt(s)" in lines[0]
    _shared(sink)


def test_syslog_resends_only_the_failed_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    sock = Socket(plan={2: OSError(errno.ECONNRESET, "reset")})
    install(monkeypatch, sock)
    sink = syslog()
    sink.emit(events(5))
    assert sock.received == [1, 2, 4, 5, 3]
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    _shared(sink)


def test_a_permanent_errno_costs_its_frame_only(monkeypatch: pytest.MonkeyPatch) -> None:
    sock = Socket(plan={1: OSError(errno.EMSGSIZE, "Message too long")})
    install(monkeypatch, sock)
    sink = syslog()
    sink.emit(events(3))
    assert sock.attempts == [1, 2, 3]
    assert sock.received == [1, 3]
    assert sink.failed == 1
    _shared(sink)


def test_a_value_error_costs_its_frame_only(monkeypatch: pytest.MonkeyPatch) -> None:
    sock = Socket(plan={1: ValueError("I/O operation on closed file")})
    install(monkeypatch, sock)
    sink = syslog()
    sink.emit(events(3))
    assert sock.attempts == [1, 2, 3]
    assert sink.failed == 1
    _shared(sink)


def test_an_oversized_datagram_counts_on_a_settled_run_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch: list[dict[str, object]] = [{"n": 1}, {"n": 2, "pad": "x" * 2_000}]
    sock = Socket()
    install(monkeypatch, sock)
    settled = syslog(max_datagram_bytes=600)
    settled.emit(batch)
    assert (settled._socket.dropped_oversized, settled.losses().dropped) == (1, 1)
    _shared(settled)
    sock.refuse = REFUSED
    failed = syslog(max_datagram_bytes=600, max_retries=0)
    with pytest.raises(SinkDeliveryError):
        failed.emit(batch)
    assert (failed._socket.dropped_oversized, failed.losses().dropped) == (0, 0)


@pytest.mark.parametrize(
    "case", ["refused", "transient", "permanent", "oversized"], ids=lambda c: c
)
def test_socket_logstash_matches_syslog(case: str, monkeypatch: pytest.MonkeyPatch) -> None:
    def run(build: Any) -> tuple[str | None, SinkLosses]:
        plan: dict[int, Exception] = {}
        refuse = None
        if case == "refused":
            refuse = REFUSED
        elif case == "transient":
            plan = {1: OSError(errno.ECONNRESET, "reset")}
        elif case == "permanent":
            plan = {1: OSError(errno.EMSGSIZE, "too long")}
        install(monkeypatch, Socket(plan, refuse))
        sink = build(max_datagram_bytes=600)
        batch = events(3) + ([{"n": 9, "pad": "x" * 2_000}] if case == "oversized" else [])
        try:
            sink.emit(batch)
            raised = None
        except SinkDeliveryError as err:
            raised = type(err).__name__
        return raised, sink.losses()

    assert run(logstash) == run(syslog)


def test_a_close_during_a_backoff_refuses_the_next_round(monkeypatch: pytest.MonkeyPatch) -> None:
    waiting = threading.Event()
    release = threading.Event()
    sock = Socket(refuse=REFUSED)
    install(monkeypatch, sock)
    sink = syslog()
    held: list[bool] = []

    def wait(delay: float, stop: object = None) -> None:
        held.append(sink._socket._lock.locked())
        waiting.set()
        assert release.wait(10.0)

    monkeypatch.setattr("log_foundry.sinks._retry.wait", wait)
    outcome: list[BaseException | None] = []

    def emit() -> None:
        try:
            sink.emit(events(2))
            outcome.append(None)
        except BaseException as err:
            outcome.append(err)

    thread = threading.Thread(target=emit)
    thread.start()
    assert waiting.wait(10.0)
    closer = threading.Thread(target=sink.close)
    closer.start()
    closer.join(2.0)
    assert not closer.is_alive(), "close() waited on a lock held across the backoff"
    release.set()
    thread.join(10.0)
    assert len(sock.attempts) == 2, "the round after the close sent nothing"
    assert isinstance(outcome[0], SinkDeliveryError)
    assert "the transport is closed" in str(outcome[0])
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert held == [False], "the transport lock is free during the engine's wait"


def _orphan(sink: Any) -> SinkLosses:
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("one")
    log_foundry.info("two")
    return sink.losses()


def _worked(sink: Any) -> SinkLosses:
    worker = Worker(sink, batch_size=1000, flush_interval=60.0, max_retries=0)  # type: ignore[arg-type]
    install_worker(worker)
    for _ in range(2):
        worker.submit(events(1))
        worker.flush(timeout=30.0)
    return sink.losses()


@pytest.mark.parametrize("case", ["total", "partial"])
def test_the_twins_keep_one_ledger(case: str, monkeypatch: pytest.MonkeyPatch) -> None:
    def make() -> SyslogSink:
        if case == "total":
            install(monkeypatch, Socket(refuse=REFUSED))
        else:
            install(monkeypatch, Socket(plan={0: OSError(errno.EMSGSIZE, "too long")}))
        return syslog(max_retries=1)

    assert _orphan(make()) == _worked(make())
