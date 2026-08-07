"""SPEC-009 — LogstashSink over HTTP (fake opener) and raw TCP/UDP sockets (fake socket)."""

from __future__ import annotations

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks.base import Sink
from log_foundry.sinks.logstash import LogstashSink
from test_sinks_http import FakeOpener


class FakeSocket:
    """Records ``sendall`` (TCP) and ``sendto`` (UDP) calls; tracks close."""

    def __init__(self) -> None:
        self.sent: list = []
        self.closed = False

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)

    def sendto(self, data: bytes, addr: tuple) -> None:
        self.sent.append((data, addr))

    def close(self) -> None:
        self.closed = True


def test_http_mode_is_a_sink() -> None:
    assert isinstance(LogstashSink(url="http://ls:8080"), Sink)


def test_http_mode_sends_ndjson() -> None:
    opener = FakeOpener()
    LogstashSink(url="http://ls:8080", opener=opener).emit([{"a": 1}, {"b": 2}])
    assert opener.calls[0]["body"].decode("utf-8") == '{"a": 1}\n{"b": 2}\n'


def test_tcp_socket_mode_sends_json_lines(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: fake)
    sink = LogstashSink(host="ls", port=5044, transport="tcp")
    sink.emit([{"a": 1}, {"b": 2}])
    assert fake.sent == [b'{"a": 1}\n', b'{"b": 2}\n']
    sink.close()
    assert fake.closed


def test_udp_socket_mode_sends_datagrams(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    sink = LogstashSink(host="ls", port=5044, transport="udp")
    sink.emit([{"a": 1}])
    assert fake.sent == [(b'{"a": 1}\n', ("ls", 5044))]


def test_requires_url_or_host_port() -> None:
    with pytest.raises(ValueError):
        LogstashSink()
