"""SPEC-009 — SyslogSink RFC 5424 framing over UDP and octet-counted TCP (fake socket)."""

from __future__ import annotations

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks.base import Sink
from log_foundry.sinks.syslog import SyslogSink
from test_sinks_logstash import FakeSocket


def test_is_a_sink(monkeypatch) -> None:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda: FakeSocket())
    assert isinstance(SyslogSink("loghost"), Sink)


def test_udp_frame_is_rfc5424_with_derived_pri(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda: fake)
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
    monkeypatch.setattr(socket_mod, "_make_udp", lambda: fake)
    SyslogSink("loghost", facility="user").emit([{"level": "WEIRD", "message": "x"}])
    # user=1, default severity=5 -> PRI = 1*8 + 5 = 13
    assert fake.sent[0][0].decode("utf-8").startswith("<13>1 ")


def test_invalid_facility_raises() -> None:
    with pytest.raises(ValueError):
        SyslogSink("loghost", facility="bogus")


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
    monkeypatch.setattr(socket_mod, "_make_udp", RefusingSocket)
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)  # no real sleeping in the retry loop
    return SyslogSink("loghost", transport="udp", max_retries=1)


def test_an_abandoned_message_reports_the_errno_not_the_message(monkeypatch, capsys) -> None:
    """An ``OSError`` type name alone cannot tell "refused" from "host unknown"; the code can."""
    sink = _refusing_sink(monkeypatch)

    sink.emit([{"level": "INFO", "message": "x"}])

    err = capsys.readouterr().err
    assert "Connection refused" not in err, "the exception's message is never written (arch §6)"
    assert "loghost.internal" not in err
    assert "OSError" in err
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

    sink.emit([{"level": "INFO", "message": "x"}])  # must not raise

    assert stream.calls == 1, "it tried, and the guard absorbed the fault"
    assert sink._socket.failed == 1, "the counter moved before the announcement"
