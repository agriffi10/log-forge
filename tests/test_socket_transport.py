"""SPEC-049 FR-002 — `SocketTransport` refuses a timeout that cannot bound anything."""

from __future__ import annotations

import errno

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks._socket import SocketTransport
from log_foundry.sinks.base import SinkDeliveryError


@pytest.mark.parametrize("transport", ["tcp", "udp"])
@pytest.mark.parametrize("bad", [-1.0, 0.0, float("nan"), float("inf")])
def test_an_unusable_timeout_is_refused_for_both_transports(transport: str, bad: float) -> None:
    """Over TCP it reached `create_connection` and raised from inside `send_all` on every message.

    Over UDP the value was never used, so this is a **new** refusal of a previously harmless call
    — stated in the spec rather than discovered, and pinned here so it does not read as a slip.
    """
    with pytest.raises(ValueError, match="SocketTransport timeout") as info:
        SocketTransport("127.0.0.1", 9, transport=transport, timeout=bad)
    assert repr(bad) in str(info.value)


@pytest.mark.parametrize("transport", ["tcp", "udp"])
def test_a_usable_timeout_constructs(transport: str) -> None:
    assert SocketTransport("127.0.0.1", 9, transport=transport, timeout=2.5)._timeout == 2.5


# --- SPEC-049 FR-006: the abandonment line reports the attempts actually made -----------------


class _RaisingSocket:
    """A UDP socket whose every ``sendto`` fails with one errno."""

    def __init__(self, code: int) -> None:
        self.code = code
        self.attempts = 0

    def sendto(self, data: bytes, addr: tuple) -> None:
        self.attempts += 1
        raise OSError(self.code, "boom")

    def close(self) -> None:
        return None


def _abandonment_lines(capsys) -> list[str]:
    return [line for line in capsys.readouterr().err.splitlines() if "attempt(s)" in line]


def test_a_permanent_errno_reports_the_one_attempt_it_made(monkeypatch, capsys) -> None:
    """`EMSGSIZE` skips the remaining attempts by design, and the line used to say four anyway.

    Injected from the socket for a frame **under** `max_datagram_bytes`: an oversized frame is
    dropped by `_sendable` and never reaches an attempt, so it cannot exercise this line.
    """
    sock = _RaisingSocket(errno.EMSGSIZE)
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: sock)
    monkeypatch.setattr(socket_mod, "wait", lambda _delay, _stop=None: None)
    transport = SocketTransport("h", 9, transport="udp", max_retries=3, max_datagram_bytes=1000)

    with pytest.raises(SinkDeliveryError, match="delivered none of 1"):
        transport.send_all([b"x" * 10])
    assert sock.attempts == 1
    lines = _abandonment_lines(capsys)
    assert len(lines) == 1 and "1 attempt(s)" in lines[0], lines


def test_a_retryable_errno_still_reports_every_attempt(monkeypatch, capsys) -> None:
    """Same `max_retries`, so a change collapsing the two cases fails one of them."""
    sock = _RaisingSocket(errno.ECONNREFUSED)
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: sock)
    monkeypatch.setattr(socket_mod, "wait", lambda _delay, _stop=None: None)
    transport = SocketTransport("h", 9, transport="udp", max_retries=3, max_datagram_bytes=1000)

    with pytest.raises(SinkDeliveryError, match="delivered none of 1"):
        transport.send_all([b"x" * 10])
    assert sock.attempts == 4
    lines = _abandonment_lines(capsys)
    assert len(lines) == 1 and "4 attempt(s)" in lines[0], lines


# --- SPEC-049, system-frame review: the port and the datagram bound ---------------------------


@pytest.mark.parametrize("port", [-1, 65536, 70000])
def test_an_out_of_range_port_is_refused(port: int) -> None:
    """UDP raised a raw OverflowError out of every `sendto` — not an OSError, so the retry guard
    never saw it — while TCP counted the same value as a resolution failure."""
    with pytest.raises(ValueError, match="SocketTransport port must be 0-65535") as info:
        SocketTransport("h", port, transport="udp")
    assert repr(port) in str(info.value)


@pytest.mark.parametrize("transport", ["tcp", "udp"])
@pytest.mark.parametrize("bad", [0, -1])
def test_a_non_positive_datagram_bound_is_refused(transport: str, bad: int) -> None:
    """Over UDP every frame was over the limit, so `emit` returned having delivered nothing."""
    with pytest.raises(ValueError, match="SocketTransport max_datagram_bytes") as info:
        SocketTransport("h", 9, transport=transport, max_datagram_bytes=bad)
    assert repr(bad) in str(info.value)
