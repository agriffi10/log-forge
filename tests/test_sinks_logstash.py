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


def test_http_mode_sends_a_json_array_by_default() -> None:
    # SPEC-041 FR-003. The old default was NDJSON as `application/x-ndjson`, which a stock
    # Logstash `http` input maps to the `plain` codec -- measured, a batch of three arrived as
    # ONE event with the payload as text in `message`. A JSON array is what a stock input parses
    # per element, so it is the default; the content type moves with the body format.
    opener = FakeOpener()
    LogstashSink(url="http://ls:8080", opener=opener).emit([{"a": 1}, {"b": 2}])
    assert opener.calls[0]["body"].decode("utf-8") == '[{"a": 1},{"b": 2}]'
    assert opener.calls[0]["headers"]["content-type"] == "application/json"


def test_http_mode_ndjson_remains_reachable_for_a_configured_input() -> None:
    # Not legacy support: `additional_codecs => {"application/x-ndjson" => "json_lines"}`
    # REPLACES the default map rather than merging with it, so an input configured with the
    # documented workaround parses this body correctly and a JSON array incorrectly. The two
    # configurations are mutually exclusive, which is why this is a parameter and not a
    # silent change of wire form.
    opener = FakeOpener()
    sink = LogstashSink(url="http://ls:8080", body_format="ndjson", opener=opener)
    sink.emit([{"a": 1}, {"b": 2}])
    assert opener.calls[0]["body"].decode("utf-8") == '{"a": 1}\n{"b": 2}\n'
    assert opener.calls[0]["headers"]["content-type"] == "application/x-ndjson"


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


def test_socket_mode_forwards_the_datagram_limit(monkeypatch) -> None:
    """SPEC-038 FR-007. A UDP Logstash user must be able to set the limit that now drops frames.

    Without the forward the limit is fixed at 65507 with no way to lower or raise it, and
    deleting the line passed the entire suite — there was no logstash test for it at all.
    """
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    sink = LogstashSink(host="lh", port=5000, transport="udp", max_datagram_bytes=200)
    sink.emit([{"message": "x" * 500}, {"message": "ok"}])
    assert len(fake.sent) == 1, "the oversized frame is dropped, not sent"
    assert b"ok" in fake.sent[0][0]
    assert sink.losses().dropped == 1


def test_http_mode_ignores_the_datagram_limit(monkeypatch) -> None:
    """It is a named parameter, so it no longer falls into `**http_kwargs` and raises."""
    sink = LogstashSink(url="http://lh:8080", max_datagram_bytes=200)
    assert sink is not None


def test_an_unrecognised_body_format_is_refused_rather_than_shipped() -> None:
    # `HTTPSink._body` treats anything that is not "json_array" as NDJSON, so a typo would
    # silently ship the exact body FR-003 exists to stop shipping, to a destination that cannot
    # parse it. A parameter documented as taking one of two values refuses the third.
    with pytest.raises(ValueError, match="body_format"):
        LogstashSink(url="http://ls:8080", body_format="json-array")
