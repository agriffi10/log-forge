"""SPEC-010 — RabbitMQSink: persistent publish, reconnect on error, close (fake connection)."""

from __future__ import annotations

import json
import sys
import types
from urllib.parse import parse_qs, urlsplit

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.rabbitmq import DEFAULT_BLOCKED_CONNECTION_TIMEOUT, RabbitMQSink


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


# --- SPEC-049 FR-005: pika's unbounded blocked-connection wait is bounded ---------------------


class FakeParameters:
    """A ``pika`` parameters stand-in with the driver's documented defaults.

    ``URLParameters`` parses ``?blocked_connection_timeout=`` from the URL query into the
    attribute; that is the driver's documented behaviour re-read from pika 1.4.4 and encoded
    here, not measured against a broker (architecture.md §12 records it as such).
    """

    def __init__(self, url: str | None = None) -> None:
        self.url = url
        self.blocked_connection_timeout: float | None = None
        self.socket_timeout = 10.0
        self.stack_timeout = 15.0
        if url is not None:
            query = parse_qs(urlsplit(url).query)
            if "blocked_connection_timeout" in query:
                self.blocked_connection_timeout = float(query["blocked_connection_timeout"][0])


def _pika_stub(monkeypatch, connections: list[FakeConnection] | None = None) -> list:
    """Installs a ``pika`` stand-in; returns the parameters each ``BlockingConnection`` received."""
    built: list = []
    pool = list(connections or [])

    def blocking_connection(params):
        built.append(params)
        return pool.pop(0) if pool else FakeConnection()

    module = types.SimpleNamespace(
        URLParameters=FakeParameters,
        ConnectionParameters=lambda: FakeParameters(),
        BlockingConnection=blocking_connection,
        BasicProperties=lambda **kwargs: types.SimpleNamespace(**kwargs),
    )
    monkeypatch.setitem(sys.modules, "pika", module)
    return built


def test_a_url_naming_no_bound_gets_the_library_default(monkeypatch) -> None:
    built = _pika_stub(monkeypatch)
    RabbitMQSink(exchange="logs", routing_key="rk", url="amqp://h/")
    assert built[0].blocked_connection_timeout == DEFAULT_BLOCKED_CONNECTION_TIMEOUT == 30.0
    assert (built[0].socket_timeout, built[0].stack_timeout) == (10.0, 15.0), "driver's, untouched"


def test_a_url_naming_the_bound_keeps_it(monkeypatch) -> None:
    """The library's default never overrides a value the caller wrote — pika lets the two be told
    apart where libpq did not, which is why this is not `PostgresSink`'s behaviour."""
    built = _pika_stub(monkeypatch)
    RabbitMQSink(exchange="logs", routing_key="rk", url="amqp://h/?blocked_connection_timeout=60")
    assert built[0].blocked_connection_timeout == 60.0


def test_an_explicit_bound_overrides_the_url(monkeypatch) -> None:
    built = _pika_stub(monkeypatch)
    RabbitMQSink(
        exchange="logs",
        routing_key="rk",
        url="amqp://h/?blocked_connection_timeout=60",
        blocked_connection_timeout=7,
    )
    assert built[0].blocked_connection_timeout == 7


def test_no_url_at_all_gets_the_default_too(monkeypatch) -> None:
    """The bare ``ConnectionParameters()`` path is the twin of the URL one."""
    built = _pika_stub(monkeypatch)
    RabbitMQSink(exchange="logs", routing_key="rk", socket_timeout=3, stack_timeout=4)
    assert built[0].url is None
    assert built[0].blocked_connection_timeout == 30.0
    assert (built[0].socket_timeout, built[0].stack_timeout) == (3, 4)


def test_a_reconnect_carries_the_bound(monkeypatch) -> None:
    """The bound is set inside ``_connect``, so a connection reopened after a failure has it too.

    Set anywhere else it would be lost on the first reconnect while every construction-time
    assertion stayed green — the twin the plan review named.
    """
    built = _pika_stub(monkeypatch, [FakeConnection(fail_channels=1), FakeConnection()])
    sink = RabbitMQSink(exchange="logs", routing_key="rk", url="amqp://h/")
    sink.emit([{"a": 1}])
    assert len(built) == 2, "the failed channel dropped the owned connection and reconnected"
    assert [p.blocked_connection_timeout for p in built] == [30.0, 30.0]


@pytest.mark.parametrize("name", ["blocked_connection_timeout", "socket_timeout", "stack_timeout"])
@pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), float("inf")])
def test_an_unusable_bound_is_refused(monkeypatch, name: str, bad: float) -> None:
    built = _pika_stub(monkeypatch)
    with pytest.raises(ValueError, match=f"RabbitMQSink {name}"):
        RabbitMQSink(exchange="logs", routing_key="rk", url="amqp://h/", **{name: bad})
    assert built == [], "refused before any connection was opened"


def test_a_bound_alongside_an_injected_connection_is_refused() -> None:
    with pytest.raises(ValueError, match="cannot apply blocked_connection_timeout, stack_timeout"):
        RabbitMQSink(
            exchange="logs",
            routing_key="rk",
            connection=FakeConnection(),
            blocked_connection_timeout=1,
            stack_timeout=1,
        )
