"""SPEC-010 — RabbitMQSink: persistent publish, reconnect on error, close (fake connection).

SPEC-107 — ``close()`` closes the channel the sink opened and leaves a borrowed connection open.
"""

from __future__ import annotations

import ast
import json
import sys
import types
from pathlib import Path
from typing import NamedTuple
from urllib.parse import parse_qs, urlsplit

import pytest

import log_foundry
from conftest import FakeSink
from log_foundry.sinks import rabbitmq as rabbitmq_module
from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.rabbitmq import DEFAULT_BLOCKED_CONNECTION_TIMEOUT, RabbitMQSink
from log_foundry.sinks.verdict import Outcome


class FakeChannel:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[tuple] = []
        self.closed = False
        self.closes = 0
        self._fail = fail

    def basic_publish(self, *, exchange, routing_key, body, properties) -> None:
        if self._fail:
            raise ConnectionError("channel closed")
        self.published.append((exchange, routing_key, body, properties))

    def close(self) -> None:
        self.closed = True
        self.closes += 1


class FakeConnection:
    def __init__(self, fail_channels: int = 0) -> None:
        self.channels: list[FakeChannel] = []
        self._fail_channels = fail_channels
        self.closed = False
        self.closes = 0

    def channel(self) -> FakeChannel:
        channel = FakeChannel(fail=len(self.channels) < self._fail_channels)
        self.channels.append(channel)
        return channel

    def close(self) -> None:
        self.closed = True
        self.closes += 1


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks._retry.wait", lambda _delay, _stop=None: None)


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


# --- SPEC-107 FR-001: close() closes the channel, and the connection only when the sink opened it


def _borrowed_sink_after_one_emit() -> tuple[RabbitMQSink, FakeConnection, FakeChannel]:
    """A sink on a borrowed connection that has published once, with the channel it opened."""
    conn = FakeConnection()
    sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn)
    sink.emit([{"a": 1}])
    [channel] = conn.channels
    assert len(channel.published) == 1, "the sink published, or its close proves nothing"
    return sink, conn, channel


def test_close_leaves_a_borrowed_connection_open_and_closes_its_channel() -> None:
    """FR-001: the channel is the sink's own; the connection it was handed is the caller's."""
    sink, conn, channel = _borrowed_sink_after_one_emit()
    sink.close()
    assert channel.closed is True
    assert conn.closed is False, "close() closed a connection the caller passed in"


def test_a_second_close_closes_nothing_again() -> None:
    """FR-001: the release runs once, so the channel is closed once and the connection never."""
    sink, conn, channel = _borrowed_sink_after_one_emit()
    sink.close()
    sink.close()
    assert channel.closes == 1
    assert conn.closes == 0


def test_closing_a_borrowed_sink_that_never_emitted_touches_neither_channel_nor_connection() -> (
    None
):
    """FR-001: with no channel opened there is nothing of the sink's own to close."""
    conn = FakeConnection()
    sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn)
    sink.close()
    assert conn.channels == [], "close() opened a channel"
    assert conn.closes == 0


def test_a_channel_close_that_raises_is_absorbed_and_the_connection_is_left_alone() -> None:
    """FR-001: a broken channel does not stop the close, nor send it on to the connection."""
    sink, conn, channel = _borrowed_sink_after_one_emit()
    attempts: list[str] = []

    def refuse() -> None:
        attempts.append("close")
        raise RuntimeError("the channel is already gone")

    channel.close = refuse  # type: ignore[method-assign]
    sink.close()
    assert attempts == ["close"]
    assert conn.closes == 0
    assert sink._channel is None


def test_close_closes_an_owned_connection_and_drops_its_reference(monkeypatch) -> None:
    """FR-001: a connection the sink opened is still closed, which no test asserted before."""
    conn = FakeConnection()
    built = _pika_stub(monkeypatch, [conn])
    sink = RabbitMQSink(exchange="logs", routing_key="rk", url="amqp://h/")
    sink.emit([{"a": 1}])
    [channel] = conn.channels
    sink.close()
    assert len(built) == 1, "the sink opened the one connection the stub handed it"
    assert conn.closed is True, "close() left open a connection the sink opened"
    assert channel.closed is True
    assert sink._connection is None


# --- SPEC-107 FR-002: a borrowed reference is kept, so the sink never connects on its own -------


def test_a_closed_borrowed_sink_keeps_the_callers_connection() -> None:
    """FR-002: the reference survives the close; only the channel's is dropped."""
    sink, conn, _ = _borrowed_sink_after_one_emit()
    sink.close()
    assert sink._connection is conn
    assert sink._channel is None


def test_a_borrowed_sink_never_opens_a_connection_of_its_own() -> None:
    """FR-002: neither a failed publish nor the close leaves the reconnect anything to do.

    ``_active_channel`` connects whenever it finds no connection, and a borrowed sink has no URL,
    so a release that dropped the reference would have it open one nothing ever closes.
    """
    conn = FakeConnection(fail_channels=1)
    sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn, max_retries=2)
    connects: list[FakeConnection] = []

    def record() -> FakeConnection:
        connects.append(FakeConnection())
        return connects[-1]

    sink._connect = record  # type: ignore[method-assign]
    sink.emit([{"a": 1}])
    assert len(conn.channels) == 2, "the first publish raised and the retry opened a new channel"
    sink.close()
    sink._active_channel()
    assert connects == [], "a borrowed sink opened a connection of its own"


@pytest.mark.parametrize("mode", ["borrowed", "owned"])
def test_a_closed_sink_refuses_alike_borrowed_or_owned(mode: str, monkeypatch) -> None:
    """FR-002: the refusal is keyed on the close, never on who owns the connection."""
    conn = FakeConnection()
    built = _pika_stub(monkeypatch, [conn] if mode == "owned" else None)
    if mode == "owned":
        sink = RabbitMQSink(exchange="logs", routing_key="rk", url="amqp://h/")
    else:
        sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn)
    sink.emit([{"a": 1}])
    sink.close()
    losses = sink.losses()
    channels = len(conn.channels)
    connections = len(built)

    with pytest.raises(SinkDeliveryError) as raised:
        sink.emit([{"a": 2}])
    assert str(raised.value) == "RabbitMQSink published none of 1 message(s): the sink is closed"
    assert sink.deliver([{"a": 3}]).outcomes == (Outcome.REJECTED,)
    assert sink.losses() == losses
    assert len(conn.channels) == channels, "a closed sink opened a channel"
    assert len(built) == connections, "a closed sink opened a connection"


# --- SPEC-107 FR-003: every close the library makes of the sink leaves the connection open ------


def _traced_call() -> None:
    """Logs once inside a span, which is what builds the worker."""

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("inside a span")

    work()


@pytest.mark.parametrize(
    "path",
    ["traced-then-shutdown", "level-then-shutdown", "displaced-then-shutdown", "multisink-member"],
)
def test_every_library_close_leaves_a_borrowed_connection_open(path: str) -> None:
    """FR-003: shutdown, the orphan path, a swap and a wrapper's forward all reach one close."""
    conn = FakeConnection()
    sink = RabbitMQSink(exchange="logs", routing_key="rk", connection=conn)
    configured = MultiSink(sink, FakeSink()) if path == "multisink-member" else sink
    log_foundry.configure(service="t", version="0", env="test", sink=configured)
    if path in ("traced-then-shutdown", "multisink-member"):
        _traced_call()
    else:
        log_foundry.info("outside any span")
    if path == "displaced-then-shutdown":
        log_foundry.configure(sink=FakeSink())
    log_foundry.shutdown()

    [channel] = conn.channels
    assert channel.published, "the sink published, or the close below proves nothing"
    assert channel.closed is True
    assert channel.closes == 1
    assert conn.closed is False, f"{path} closed a connection the caller passed in"


# --- SPEC-107 FR-001: the derived guard over the module's release sites -------------------------


class Site(NamedTuple):
    """One statement that releases ``self._connection``: its line and its enclosing method."""

    line: int
    method: str


def _is_self_attribute(node: ast.AST, name: str) -> bool:
    """Whether ``node`` is exactly ``self.<name>``."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr == name
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    )


def _releases_the_connection(node: ast.AST) -> bool:
    """Whether ``node`` closes ``self._connection`` or assigns ``None`` to it."""
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name) and node.func.id == "_safe_close":
            return any(_is_self_attribute(value, "_connection") for value in node.args)
        return (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "close"
            and _is_self_attribute(node.func.value, "_connection")
        )
    if isinstance(node, ast.Assign):
        targets = node.targets
    elif isinstance(node, ast.AnnAssign):
        targets = [node.target]
    else:
        return False
    return (
        isinstance(node.value, ast.Constant)
        and node.value.value is None
        and any(_is_self_attribute(target, "_connection") for target in targets)
    )


def _tests_ownership(test: ast.expr) -> bool:
    """Whether an ``if`` test is ``self._owns_connection``, alone or as an ``and`` operand."""
    if _is_self_attribute(test, "_owns_connection"):
        return True
    return (
        isinstance(test, ast.BoolOp)
        and isinstance(test.op, ast.And)
        and any(_is_self_attribute(value, "_owns_connection") for value in test.values)
    )


def connection_release_sites(source: str) -> tuple[list[Site], list[Site]]:
    """Every statement in ``source`` that releases ``self._connection``, and the offending ones.

    A statement is collected when it passes ``self._connection`` to ``_safe_close``, calls
    ``self._connection.close()``, or assigns ``None`` to ``self._connection``. It is allowed only
    inside the ``body``, never the ``orelse``, of an ``if`` whose test is exactly
    ``self._owns_connection`` or an ``and`` with that as a direct operand; a negated test, an
    ``or`` and an ``else`` branch are therefore offences.

    Its bound: it reads those three shapes and no other. It does not police an alias
    (``c = self._connection; c.close()``), a keyword argument (``_safe_close(resource=...)``), a
    tuple assignment (``self._channel, self._connection = None, None``), ``del self._connection``,
    a bound ``self._connection.close`` handed to another call, or a ``with`` statement that closes
    the connection. It also reports a correct release that follows an early ``return`` on
    ``not self._owns_connection``, since it reads only the enclosing ``if``. It is not the proof
    that a release is right, which is the behavioural tests above. It catches a release site
    written without the ownership test.

    Every clause has a case below that fails without it; a clause with no such case was removed
    rather than left unproven, which is why a keyword argument is in the bound and not the rule.

    Args:
      source: A module's source text.

    Returns:
      ``(collected, offending)``, each in source order; a site is its statement's first line and
      its enclosing ``def``'s name.
    """
    collected: list[Site] = []
    offending: list[Site] = []

    def visit(node: ast.AST, method: str, line: int, owned: bool) -> None:
        if isinstance(node, ast.FunctionDef):
            method = node.name
        if isinstance(node, ast.stmt):
            line = node.lineno
        if _releases_the_connection(node):
            collected.append(Site(line, method))
            if not owned:
                offending.append(Site(line, method))
        covered = node.body if isinstance(node, ast.If) and _tests_ownership(node.test) else []
        for child in ast.iter_child_nodes(node):
            visit(child, method, line, owned or any(child is held for held in covered))

    visit(ast.parse(source), "<module>", 0, False)
    return collected, offending


def _named(source: str, sites: list[Site]) -> list[tuple[str, str]]:
    """Each site as its method and the text of the source line it names."""
    lines = source.splitlines()
    return [(site.method, lines[site.line - 1].strip()) for site in sites]


_UNGUARDED = """
class Sink:
    def _release(self):
        if self._channel is not None:
            _safe_close(self._channel)
            self._channel = None
        if self._connection is not None:
            _safe_close(self._connection)
            self._connection = None
"""
"""The release as it stood before SPEC-107: the connection closed under no ownership test."""

_ANNOTATED = """
class Sink:
    def _forget(self):
        self._connection: object = None
"""
"""``None`` assigned under an annotation, the form ``__init__`` uses for its other attributes."""

_WRONG_POLARITY = {
    "negated": (
        """
class Sink:
    def _drop(self):
        if not self._owns_connection:
            self._connection.close()
""",
        ("_drop", "self._connection.close()"),
    ),
    "else-branch": (
        """
class Sink:
    def _drop(self):
        if self._owns_connection:
            self._channel = None
        else:
            self._connection = None
""",
        ("_drop", "self._connection = None"),
    ),
    "or": (
        """
class Sink:
    def _drop(self):
        if self._owns_connection or self._closing:
            _safe_close(self._connection)
""",
        ("_drop", "_safe_close(self._connection)"),
    ),
    "nested-operand": (
        """
class Sink:
    def _drop(self):
        if self._closing and not self._owns_connection:
            self._connection.close()
""",
        ("_drop", "self._connection.close()"),
    ),
    "other-receiver": (
        """
class Sink:
    def _drop(self, other):
        if other._owns_connection:
            self._connection.close()
""",
        ("_drop", "self._connection.close()"),
    ),
}
"""A release under an ownership test that does not establish ownership, and its one offence."""

_BARE_TEST = """
class Sink:
    def _drop(self):
        if self._owns_connection:
            _safe_close(self._connection)
            self._connection.close()
            self._connection = None
"""
"""Each release shape under the bare ownership test, a form the live module does not use."""

_NESTED_IF = """
class Sink:
    def _drop(self):
        if self._owns_connection:
            if self._connection is not None:
                _safe_close(self._connection)
"""
"""A release under an ``if`` that is itself inside the ownership test's body."""


def test_the_guard_reports_a_connection_closed_under_no_ownership_test() -> None:
    """FR-001: a release site written without the ownership test is an offence, by name."""
    collected, offending = connection_release_sites(_UNGUARDED)
    assert offending == collected
    assert _named(_UNGUARDED, offending) == [
        ("_release", "_safe_close(self._connection)"),
        ("_release", "self._connection = None"),
    ]


def test_the_guard_reports_an_annotated_assignment_of_none() -> None:
    """FR-001: ``None`` assigned under an annotation is a release like the plain assignment."""
    _, offending = connection_release_sites(_ANNOTATED)
    assert _named(_ANNOTATED, offending) == [("_forget", "self._connection: object = None")]


@pytest.mark.parametrize("shape", sorted(_WRONG_POLARITY))
def test_the_guard_reports_a_close_under_the_wrong_polarity(shape: str) -> None:
    """FR-001: only ``self._owns_connection``, bare or a direct ``and`` operand, is ownership.

    A negated test, an ``else`` branch, an ``or``, the name nested inside an ``and`` operand and
    another object's attribute of the same name each fail to establish it.
    """
    source, expected = _WRONG_POLARITY[shape]
    _, offending = connection_release_sites(source)
    assert _named(source, offending) == [expected]


def test_the_guard_allows_a_close_under_the_bare_ownership_test() -> None:
    """FR-001: silence is asserted too, over sites the guard did collect."""
    collected, offending = connection_release_sites(_BARE_TEST)
    assert offending == []
    assert _named(_BARE_TEST, collected) == [
        ("_drop", "_safe_close(self._connection)"),
        ("_drop", "self._connection.close()"),
        ("_drop", "self._connection = None"),
    ]


def test_the_guard_allows_a_close_under_an_if_nested_in_the_ownership_test() -> None:
    """FR-001: a release inside the ownership test's body stays inside it under a further ``if``."""
    collected, offending = connection_release_sites(_NESTED_IF)
    assert offending == []
    assert _named(_NESTED_IF, collected) == [("_drop", "_safe_close(self._connection)")]


def test_the_live_module_closes_its_connection_only_under_the_ownership_test() -> None:
    """FR-001: every release site in ``sinks/rabbitmq.py`` sits under the ownership test."""
    source = Path(rabbitmq_module.__file__).read_text(encoding="utf-8")
    collected, offending = connection_release_sites(source)
    assert offending == [], f"released without the ownership test: {_named(source, offending)}"
    assert ("_reset", "_safe_close(self._connection)") in _named(source, collected), (
        "the guard collected nothing from _reset, so its silence above proves nothing"
    )


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
