"""SPEC-059 FR-002 — the socket transport's oversize drop counts only when the sink's emit returns.

``SocketTransport._sendable`` discarded a UDP datagram over ``max_datagram_bytes`` and counted it at
once, so when the sendable remainder then failed totally ``send_all`` raised, the worker retried
the batch and the same frame was counted again (Probe A: four for one frame when the destination
is down). ``SyslogSink.emit`` and ``LogstashSink.emit``'s socket branch now enter a ``Deferral``
owned by the transport and hand it to ``send_all``, so the drop commits only when ``emit`` returns.

Each case runs for both sinks over UDP with ``max_datagram_bytes=600`` and a 2,000-byte event, the
sink's own ``max_retries=0``, through the real ``Worker(sink, batch_size=len(batch),
max_retries=3)`` where it says so.
"""

from __future__ import annotations

from typing import Any

import pytest

import log_foundry
import log_foundry.sinks._socket as socket_mod
from log_foundry import _diag
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.syslog import SyslogSink
from log_foundry.worker import Worker
from test_sinks_logstash import FakeSocket

DOWN = set(range(1, 100))
ONCE = {1}
_LIMIT = 600
_BIG = 2_000
_MARKER = f"SocketTransport, over the {_LIMIT}-byte datagram limit"
_SINKS = ("logstash", "syslog")


class _NumberedSocket(FakeSocket):
    """A socket whose sends raise a real ``OSError`` on chosen call numbers, and count the calls."""

    def __init__(self, fail_on: set[int]) -> None:
        super().__init__()
        self.calls = 0
        self.fail_on = fail_on

    def _go(self, item: object) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise OSError(111, "Connection refused")
        self.sent.append(item)

    def sendall(self, data: bytes) -> None:
        self._go(data)

    def sendto(self, data: bytes, addr: tuple) -> None:
        self._go((data, addr))


@pytest.fixture
def sock(monkeypatch) -> _NumberedSocket:
    """The one socket every transport in the test opens; tests set ``fail_on`` on it."""
    fake = _NumberedSocket(set())
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: fake)
    monkeypatch.setattr(socket_mod, "wait", lambda _delay, _stop=None: None)
    return fake


def _build(name: str, transport: str = "udp") -> Any:
    if name == "syslog":
        return SyslogSink("loghost", transport=transport, max_retries=0, max_datagram_bytes=_LIMIT)
    return LogstashSink(
        host="loghost", port=5000, transport=transport, max_retries=0, max_datagram_bytes=_LIMIT
    )


def _pair() -> list[dict[str, object]]:
    """One event whose frame is over the datagram limit, then one ordinary event."""
    return [{"big": "x" * _BIG}, {"i": 1}]


def _drop_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if _MARKER in line]


def _through_the_worker(sink: Any, batch: list[dict[str, object]]) -> tuple[bool, int]:
    """Hands ``batch`` to a real worker as one batch; whether it was delivered, and abandonments."""
    worker = Worker(sink, batch_size=len(batch), max_retries=3)
    try:
        worker.submit(batch)
        delivered = bool(worker.flush(timeout=30.0))
        failed_batches = worker.health().failed_batches
    finally:
        worker.stop()
    return delivered, failed_batches


@pytest.mark.parametrize("name", _SINKS)
def test_an_abandoned_batch_counts_no_datagram_drop(name: str, sock, capsys) -> None:
    """Probe A, down: four attempts each decided the drop; none of them returned, so none counts."""
    sock.fail_on = DOWN
    sink = _build(name)

    delivered, failed_batches = _through_the_worker(sink, _pair())

    err = capsys.readouterr().err
    assert not delivered and failed_batches == 1
    assert sock.calls == 4, "the premise: the ordinary frame was offered on every attempt"
    assert sink._socket.dropped_oversized == 0
    assert sink.losses().dropped == 0
    assert _drop_lines(err) == [], "no datagram-limit line from an emit that raised"
    assert "lost 2 event(s); batch abandoned after 4 emit attempts" in err, (
        "the abandon line is the only count of the oversized frame"
    )


@pytest.mark.parametrize("name", _SINKS)
def test_a_batch_delivered_on_retry_counts_its_datagram_drop_once(name: str, sock, capsys) -> None:
    """Probe A, once: attempt 1 raises and counts nothing, attempt 2 returns and counts one."""
    sock.fail_on = ONCE
    sink = _build(name)

    delivered, failed_batches = _through_the_worker(sink, _pair())

    err = capsys.readouterr().err
    assert delivered and failed_batches == 0
    assert sock.calls == 2, "the premise: one failed attempt, then one that delivered"
    assert sink._socket.dropped_oversized == 1
    assert sink.losses().dropped == 1
    assert _drop_lines(err) == [f"log-foundry: lost 1 message(s); {_MARKER}"]


@pytest.mark.parametrize("name", _SINKS)
def test_an_all_oversized_udp_batch_sends_nothing_and_counts_all(name: str, sock, capsys) -> None:
    """Unchanged: nothing sendable is not a send failure, so it returns and commits every drop."""
    sock.fail_on = DOWN
    sink = _build(name)

    sink.emit([{"big": "x" * _BIG, "n": n} for n in range(3)])

    assert sock.calls == 0
    assert sink._socket.dropped_oversized == 3
    assert sink.losses().dropped == 3
    assert _drop_lines(capsys.readouterr().err) == [f"log-foundry: lost 3 message(s); {_MARKER}"]


@pytest.mark.parametrize("name", _SINKS)
def test_tcp_drops_nothing_however_large_the_frame(name: str, sock, capsys) -> None:
    """Unchanged: TCP is a stream with no datagram limit, so every frame is sent."""
    sink = _build(name, transport="tcp")

    sink.emit(_pair())

    assert sock.calls == 2 and len(sock.sent) == 2
    assert sink._socket.dropped_oversized == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert _drop_lines(capsys.readouterr().err) == []


@pytest.mark.parametrize("name", _SINKS)
def test_a_closed_transport_raises_and_counts_nothing(name: str, sock, capsys) -> None:
    """Unchanged: a closed transport refuses before deciding anything."""
    sink = _build(name)
    sink.close()

    with pytest.raises(SinkDeliveryError, match="closed"):
        sink.emit(_pair())

    assert sock.calls == 0
    assert sink._socket.dropped_oversized == 0
    assert _drop_lines(capsys.readouterr().err) == []


@pytest.mark.parametrize("name", _SINKS)
def test_the_datagram_line_is_written_outside_the_send_lock(name: str, sock, monkeypatch) -> None:
    """The commit runs when the sink's emit returns, after ``send_all`` released its lock."""
    sink = _build(name)
    transport = sink._socket
    real = _diag.lost
    held: list[bool] = []

    def spy(unit: str, n: int, detail: str | None = None) -> None:
        if detail == _MARKER:
            held.append(transport._lock.locked())
        real(unit, n, detail)

    monkeypatch.setattr(_diag, "lost", spy)

    sink.emit(_pair())

    assert held == [False], "the datagram-limit line was written while the send lock was held"


def test_send_all_and_sendable_refuse_a_call_without_a_ledger(sock) -> None:
    """The ledger is required, never defaulted: a caller that forgets it cannot count at once."""
    transport = socket_mod.SocketTransport(
        "loghost", 514, transport="udp", max_datagram_bytes=_LIMIT
    )

    with pytest.raises(TypeError, match="pending"):
        transport.send_all([b"x" * _BIG])
    with pytest.raises(TypeError, match="pending"):
        transport._sendable([b"x" * _BIG])

    assert transport.dropped_oversized == 0
    with Deferral(transport) as pending:
        transport.send_all([b"x" * _BIG], pending)
    assert transport.dropped_oversized == 1, "the premise: with a ledger the frame is dropped"


@pytest.mark.parametrize("name", _SINKS)
def test_an_orphan_oversized_frame_is_counted_once(name: str, sock) -> None:
    """Unchanged: outside a span the batch is the one frame, which never raises and counts once."""
    sink = _build(name)
    log_foundry.configure(service="t", sink=sink, max_value_bytes=_BIG * 2)

    log_foundry.info("outside a span", big="x" * _BIG)

    assert sock.calls == 0, "the premise: the frame was over the limit, so nothing was sent"
    assert sink._socket.dropped_oversized == 1
    assert sink.losses().dropped == 1
    assert log_foundry.health().orphan_lost == 0


@pytest.mark.parametrize("name", _SINKS)
def test_a_fanout_absorbs_the_oversized_frame_once(name: str, sock) -> None:
    """The child raised, its sibling delivered: the fan-out owns both events, the child neither."""
    sock.fail_on = DOWN
    child = _build(name)
    multi = MultiSink(child, MemorySink())

    multi.emit(_pair())

    own = child.losses()
    assert own == SinkLosses(dropped=0, failed=1), "the child's raise discarded its drop"
    assert multi.losses() == SinkLosses(dropped=0, failed=own.failed + 2)
