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
    SyslogSink("loghost", transport="tcp", facility="user").emit([{"level": "INFO", "message": "x"}])
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
