"""SPEC-009 — SyslogSink RFC 5424 framing over UDP and octet-counted TCP (fake socket)."""

from __future__ import annotations

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.syslog import SyslogSink
from test_sinks_logstash import FakeSocket


def test_is_a_sink(monkeypatch) -> None:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: FakeSocket())
    assert isinstance(SyslogSink("loghost"), Sink)


def test_udp_frame_is_rfc5424_with_derived_pri(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    sink = SyslogSink("loghost", 514, transport="udp", facility="local0")
    sink.emit([{"level": "ERROR", "timestamp": "2026-07-11T00:00:00.000Z", "message": "boom"}])
    data, addr = fake.sent[0]
    text = data.decode("utf-8")
    # local0=16, ERROR severity=3 -> PRI = 16*8 + 3 = 131
    assert text.startswith("<131>1 2026-07-11T00:00:00.000Z ")
    assert '"message": "boom"' in text
    assert addr == ("loghost", 514)


def test_tcp_uses_octet_counted_framing(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: fake)
    SyslogSink("loghost", transport="tcp", facility="user").emit(
        [{"level": "INFO", "message": "x"}]
    )
    frame = fake.sent[0]
    prefix, _, message = frame.partition(b" ")
    assert prefix.isdigit()
    assert len(message) == int(prefix)  # the count prefixes exactly the message byte length


def test_unknown_level_uses_default_severity(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    SyslogSink("loghost", facility="user").emit([{"level": "WEIRD", "message": "x"}])
    # user=1, default severity=5 -> PRI = 1*8 + 5 = 13
    assert fake.sent[0][0].decode("utf-8").startswith("<13>1 ")


def test_invalid_facility_raises() -> None:
    with pytest.raises(ValueError):
        SyslogSink("loghost", facility="bogus")


# -- SPEC-031 FR-002: the UDP address family is resolved, not assumed ------------------------


def _loopback_receiver(family: int):
    """Bind a real UDP socket on the loopback of one family, or skip if it is unavailable."""
    import socket as socket_lib

    address = "::1" if family == socket_lib.AF_INET6 else "127.0.0.1"
    try:
        sock = socket_lib.socket(family, socket_lib.SOCK_DGRAM)
        sock.bind((address, 0))
    except OSError as exc:  # pragma: no cover - depends on the host's stack
        pytest.skip(f"no {address} loopback here: {exc}")
    sock.settimeout(5.0)
    return sock, sock.getsockname()[1]


def test_make_udp_picks_the_family_the_host_resolves_to() -> None:
    """The defect itself: an unconditional AF_INET made every IPv6 send fail."""
    import socket as socket_lib

    v6 = socket_mod._make_udp("::1")
    v4 = socket_mod._make_udp("127.0.0.1")
    try:
        assert v6.family == socket_lib.AF_INET6
        assert v4.family == socket_lib.AF_INET
    finally:
        v6.close()
        v4.close()


def test_udp_syslog_delivers_to_an_ipv6_destination() -> None:
    import socket as socket_lib

    receiver, port = _loopback_receiver(socket_lib.AF_INET6)
    try:
        SyslogSink("::1", port, transport="udp", facility="user").emit(
            [{"level": "INFO", "message": "over-v6"}]
        )
        assert b"over-v6" in receiver.recv(4096)
    finally:
        receiver.close()


def test_udp_logstash_delivers_to_an_ipv6_destination() -> None:
    import socket as socket_lib

    from log_foundry.sinks.logstash import LogstashSink

    receiver, port = _loopback_receiver(socket_lib.AF_INET6)
    try:
        LogstashSink(host="::1", port=port, transport="udp").emit([{"message": "ls-v6"}])
        assert b"ls-v6" in receiver.recv(4096)
    finally:
        receiver.close()


def test_udp_delivery_to_ipv4_is_unchanged() -> None:
    import socket as socket_lib

    receiver, port = _loopback_receiver(socket_lib.AF_INET)
    try:
        SyslogSink("127.0.0.1", port, transport="udp").emit(
            [{"level": "INFO", "message": "over-v4"}]
        )
        assert b"over-v4" in receiver.recv(4096)
    finally:
        receiver.close()


def test_udp_delivery_to_a_hostname_is_unchanged() -> None:
    """``localhost`` may resolve to either family; whichever it is, the datagram arrives."""
    import socket as socket_lib

    family = socket_lib.getaddrinfo("localhost", None, type=socket_lib.SOCK_DGRAM)[0][0]
    receiver, port = _loopback_receiver(family)
    try:
        SyslogSink("localhost", port, transport="udp").emit(
            [{"level": "INFO", "message": "by-name"}]
        )
        assert b"by-name" in receiver.recv(4096)
    finally:
        receiver.close()


def test_a_host_resolving_to_neither_family_is_counted_and_announced(
    monkeypatch, capsys
) -> None:
    """A ``gaierror`` is an ``OSError``, so it fails exactly as an unreachable host already did."""
    import socket as socket_lib

    def unresolvable(*args: object, **kwargs: object) -> list:
        raise socket_lib.gaierror(-2, "Name or service not known")

    monkeypatch.setattr(socket_lib, "getaddrinfo", unresolvable)
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)
    sink = SyslogSink("nowhere.invalid", transport="udp", max_retries=0)

    with pytest.raises(SinkDeliveryError):  # SPEC-026 FR-001, not the gaierror itself
        sink.emit([{"level": "INFO", "message": "x"}])

    assert sink._socket.losses().failed == 1
    err = capsys.readouterr().err
    assert "lost 1 message(s)" in err
    assert "Name or service not known" not in err, "the exception's message is never written"


def test_the_family_is_resolved_once_per_socket_not_once_per_message(monkeypatch) -> None:
    import socket as socket_lib

    real = socket_lib.getaddrinfo
    calls: list[object] = []

    def counting(*args: object, **kwargs: object) -> list:
        calls.append(args)
        return real(*args, **kwargs)  # type: ignore[arg-type]

    receiver, port = _loopback_receiver(socket_lib.AF_INET)
    monkeypatch.setattr(socket_lib, "getaddrinfo", counting)
    try:
        sink = SyslogSink("127.0.0.1", port, transport="udp")
        sink.emit([{"level": "INFO", "message": f"m{i}"} for i in range(3)])
        sink.emit([{"level": "INFO", "message": "m3"}])
    finally:
        receiver.close()
    assert len(calls) == 1, "the socket outlives every send made through it"


# -- SPEC-029 FR-002/FR-003: SocketTransport's abandonment line ------------------------------


class RefusingSocket(FakeSocket):
    """A socket whose sends fail with a real ``OSError``, message and all."""

    def __init__(self, errno: int = 111) -> None:
        super().__init__()
        self._exc = OSError(errno, "Connection refused to loghost.internal:514")

    def sendall(self, data: bytes) -> None:
        raise self._exc

    def sendto(self, data: bytes, addr: tuple) -> None:
        raise self._exc


class RaisingStderr:
    """A ``sys.stderr`` whose ``write`` fails, as a closed fd or a broken pipe does."""

    def __init__(self) -> None:
        self.calls = 0

    def write(self, text: str) -> int:
        self.calls += 1
        raise ValueError("I/O operation on closed file")

    def flush(self) -> None:
        return None


def _refusing_sink(monkeypatch) -> SyslogSink:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: RefusingSocket())
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)  # no real sleeping in the retry loop
    return SyslogSink("loghost", transport="udp", max_retries=1)


def test_an_abandoned_message_reports_the_errno_not_the_message(monkeypatch, capsys) -> None:
    """An ``OSError`` type name alone cannot tell "refused" from "host unknown"; the code can."""
    sink = _refusing_sink(monkeypatch)

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "INFO", "message": "x"}])  # nothing landed (SPEC-026 FR-001)

    err = capsys.readouterr().err
    assert "Connection refused" not in err, "the exception's message is never written (arch §6)"
    assert "loghost.internal" not in err
    # Derived, not hardcoded: CPython maps an errno to an ``OSError`` *subclass* at construction,
    # and the mapping is per-platform — 111 is ECONNREFUSED on Linux (so the type is
    # ``ConnectionRefusedError``) and something else on macOS (so it stays ``OSError``).
    assert type(RefusingSocket()._exc).__name__ in err
    assert "errno=111" in err, "the OS code is what makes the line actionable"
    assert "lost 1 message(s)" in err
    assert "2 attempt(s)" in err, "the attempt count survived the conversion"
    assert sink._socket.failed == 1


def test_a_broken_stderr_does_not_reach_the_caller(monkeypatch) -> None:
    """This runs on the worker thread, where the write used to be bare (SPEC-029 FR-003)."""
    import sys

    sink = _refusing_sink(monkeypatch)
    stream = RaisingStderr()
    monkeypatch.setattr(sys, "stderr", stream)

    # The *delivery* failure propagates (SPEC-026 FR-001); the stderr fault must not — a
    # ``RuntimeError`` here would mean the diagnostic became the failure.
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "INFO", "message": "x"}])

    assert stream.calls == 1, "it tried, and the guard absorbed the fault"
    assert sink._socket.failed == 1, "the counter moved before the announcement"
