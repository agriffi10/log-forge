"""A subclass of a converted sink writes the class name its 1.x lines wrote (SPEC-074, review).

Where 1.x wrote a literal class name — the four AWS sinks' loss lines — a subclass's line still
reads ``SQSSink`` and so on; where 1.x wrote ``type(self).__name__`` — SPEC-069's encode line on
``RabbitMQSink``, ``SyslogSink`` and socket-mode ``LogstashSink`` — it names the subclass. Each
case was measured against the same subclass at ``ddc78707``. Serves invariant 11.
"""

from __future__ import annotations

import errno
from typing import Any

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.syslog import SyslogSink
from test_sinks_broker_engine import _rabbit
from test_sinks_queue_engine import ALL, STREAMS, build, events, failing, lost


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


def _subclassed(sink: Any) -> Any:
    sink.__class__ = type(f"Mine{type(sink).__name__}", (type(sink),), {})
    return sink


def _garbage(kind: str) -> Any:
    if kind in STREAMS:
        key = (
            ("FailedRecordCount", "Records")
            if kind == "KinesisSink"
            else (
                "FailedPutCount",
                "RequestResponses",
            )
        )
        return lambda entries, call: {key[0]: 1, key[1]: [{}]}
    return lambda entries, call: {"Failed": "garbage"}


@pytest.mark.parametrize("reply", ["raise", "coded", "unread"])
@pytest.mark.parametrize("kind", ALL)
def test_an_aws_subclass_line_names_the_shipped_class(
    kind: str, reply: str, capsys: pytest.CaptureFixture[str]
) -> None:
    replies = {
        "raise": lambda entries, call: (_ for _ in ()).throw(ConnectionError("down")),
        "coded": failing(kind, lambda e: True),
        "unread": _garbage(kind),
    }
    sink, _ = build(kind, replies[reply], max_retries=1)
    _subclassed(sink)
    try:
        sink.emit(events(2))
    except Exception:
        pass
    lines = lost(capsys.readouterr().err)
    assert lines, "the case writes a line"
    assert all(f"; {kind}" in line and "Mine" not in line for line in lines), lines


def test_a_rabbitmq_subclass_encode_line_names_the_subclass(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sink, _ = _rabbit({}, max_retries=1)
    _subclassed(sink)
    sink.emit([{"i": 0}, {"i": 1, "b": b"\x00"}])
    assert lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); MineRabbitMQSink, encoding raised TypeError"
    ]


class _Sock:
    def sendto(self, data: bytes, addr: object) -> None:
        pass

    def sendall(self, data: bytes) -> None:
        raise OSError(errno.ECONNREFUSED, "refused")

    def close(self) -> None:
        pass


@pytest.mark.parametrize("base", [SyslogSink, LogstashSink], ids=lambda c: c.__name__)
def test_a_socket_subclass_encode_line_names_the_subclass(
    base: type, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: _Sock())
    mine = type(f"Mine{base.__name__}", (base,), {})
    sink = (
        mine("loghost", 514, transport="udp")
        if base is SyslogSink
        else mine(host="loghost", port=5000, transport="udp")
    )
    sink.emit([{"n": 1}, {"n": 2, "b": b"\x00"}])
    assert lost(capsys.readouterr().err) == [
        f"log-foundry: lost 1 event(s); Mine{base.__name__}, encoding raised TypeError"
    ]
