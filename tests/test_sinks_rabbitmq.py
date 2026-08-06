"""SPEC-010 — RabbitMQSink: persistent publish, reconnect on error, close (fake connection)."""

from __future__ import annotations

import json

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.rabbitmq import RabbitMQSink


class FakeChannel:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple] = []
        self.closed = False
        self._fail = fail

    def basic_publish(self, *, exchange, routing_key, body, properties) -> None:
        if self._fail:
            raise RuntimeError("channel closed")
        self.published.append((exchange, routing_key, body, properties))

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, fail_channels: int = 0) -> None:
        self.channels: list[FakeChannel] = []
        self._fail_channels = fail_channels
        self.closed = False

    def channel(self) -> FakeChannel:
        channel = FakeChannel(fail=len(self.channels) < self._fail_channels)
        self.channels.append(channel)
        return channel

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.rabbitmq.wait", lambda _delay, _stop=None: None)


def test_is_a_sink() -> None:
    assert isinstance(
        RabbitMQSink(exchange="logs", routing_key="rk", connection=FakeConnection()), Sink
    )


def test_publishes_persistent_message_per_event() -> None:
    conn = FakeConnection()
    RabbitMQSink(exchange="logs", routing_key="rk", connection=conn).emit([{"a": 1}, {"a": 2}])
    channel = conn.channels[0]
    assert len(channel.published) == 2
    exchange, routing_key, body, properties = channel.published[0]
    assert exchange == "logs"
    assert routing_key == "rk"
    assert json.loads(body) == {"a": 1}
    assert properties.delivery_mode == 2  # persistent


def test_reconnects_after_channel_error() -> None:
    conn = FakeConnection(fail_channels=1)  # first channel fails, reconnect succeeds
    sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn, max_retries=2)
    sink.emit([{"a": 1}])  # the reconnect published it, so nothing failed
    assert sink.failed == 0
    assert len(conn.channels) == 2
    assert len(conn.channels[1].published) == 1


def test_persistent_failure_is_counted(capsys) -> None:
    conn = FakeConnection(fail_channels=99)  # every reconnect also fails
    sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn, max_retries=2)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])  # the broker took nothing (SPEC-026 FR-001)
    assert len(conn.channels) == 3  # initial + 2 retries, each a fresh channel
    assert sink.failed == 1
    assert "lost 1 message(s)" in capsys.readouterr().err


def test_close_closes_channel_and_connection() -> None:
    conn = FakeConnection()
    sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn)
    sink.emit([{"a": 1}])
    channel = conn.channels[0]
    sink.close()
    assert channel.closed is True
    assert conn.closed is True
    sink.close()  # idempotent
