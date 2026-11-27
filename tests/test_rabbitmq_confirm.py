"""SPEC-093 FR-004 — ``RabbitMQSink`` asks the broker to confirm each publish, when told to.

``confirm=True`` puts every channel the sink opens in confirm mode, publishes with
``mandatory``, and probes an owned connection before a round's first publish. A confirming
publish that returns is ``ACCEPTED``; ``UnroutableError`` and ``NackError`` are the broker's
refusal, ``REJECTED`` on a kept channel; a connection lost before the confirm is ``UNKNOWN``,
counted with a may-have-landed line and never sent again. ``confirm=False``, the default, is 1.x
exactly. Serves invariants 2, 7, 10 and 13.
"""

from __future__ import annotations

import inspect
import sys
import types
from typing import Any

import pytest

from log_foundry.sinks import _engine
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.rabbitmq import RabbitMQSink

_PIKA_ERRORS = {
    "AMQPError": (),
    "AMQPConnectionError": ("AMQPError",),
    "StreamLostError": ("AMQPConnectionError",),
    "ConnectionWrongStateError": ("AMQPConnectionError",),
    "ConnectionClosedByBroker": ("AMQPConnectionError",),
    "AMQPHeartbeatTimeout": ("AMQPConnectionError",),
    "ConnectionBlockedTimeout": ("AMQPConnectionError",),
    "ChannelWrongStateError": ("AMQPError",),
    "ChannelClosedByBroker": ("AMQPError",),
    "UnroutableError": ("AMQPError",),
    "NackError": ("AMQPError",),
}
"""The ``pika.exceptions`` classes these tests raise, with pika 1.4.4's bases."""


@pytest.fixture
def pika_errors(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Installs a stub ``pika.exceptions``, which ``driver_classes`` reads from ``sys.modules``.

    Args:
      monkeypatch: The test's patch, which restores the module.

    Returns:
      The stub.

    Raises:
      None.
    """
    module = types.ModuleType("pika.exceptions")
    for name, bases in _PIKA_ERRORS.items():
        parents = tuple(getattr(module, b) for b in bases) or (Exception,)
        setattr(module, name, type(name, parents, {"__module__": "pika.exceptions"}))
    monkeypatch.setitem(sys.modules, "pika.exceptions", module)
    return module


class _Channel:
    """A channel that records each call and raises what its script says, once per publish."""

    def __init__(self, log: list[str], script: list[Exception | None], *, confirms: bool) -> None:
        """Keeps the shared call log and the publish script.

        Args:
          log: Every call the sink makes, in order.
          script: One entry per publish: ``None`` to return, an exception to raise.
          confirms: Whether this channel offers ``confirm_delivery``.

        Returns:
          None.

        Raises:
          None.
        """
        self.log = log
        self.script = script
        self.closed = False
        if confirms:
            self.confirm_delivery = self._confirm_delivery

    def _confirm_delivery(self) -> None:
        """Records the call.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.log.append("confirm_delivery")

    def basic_publish(self, **kwargs: Any) -> None:
        """Records the publish and its ``mandatory``, then answers the script.

        Args:
          **kwargs: The publish's arguments.

        Returns:
          None.

        Raises:
          Exception: The scripted one.
        """
        self.log.append(f"publish mandatory={kwargs.get('mandatory')}")
        outcome = self.script.pop(0) if self.script else None
        if outcome is not None:
            raise outcome

    def close(self) -> None:
        """Records the close.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.closed = True
        self.log.append("channel.close")


class _Connection:
    """A connection whose channels share one script, and whose probe raises when told to."""

    def __init__(
        self,
        log: list[str],
        script: list[Exception | None],
        *,
        confirms: bool = True,
        probe_raises: Exception | None = None,
        confirm_raises: Exception | None = None,
    ) -> None:
        """Keeps the log, the publish script and what the probe and confirm do.

        Args:
          log: The shared call log.
          script: The publish script every channel reads.
          confirms: Whether its channels offer ``confirm_delivery``.
          probe_raises: What ``process_data_events`` raises, or ``None``.
          confirm_raises: What ``confirm_delivery`` raises, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        self.log = log
        self.script = script
        self.confirms = confirms
        self.probe_raises = probe_raises
        self.confirm_raises = confirm_raises
        self.channels: list[_Channel] = []
        self.is_open = True

    def channel(self) -> _Channel:
        """Opens a channel.

        Args:
          None.

        Returns:
          The channel.

        Raises:
          None.
        """
        self.log.append("channel")
        chan = _Channel(self.log, self.script, confirms=self.confirms)
        if self.confirm_raises is not None:
            error = self.confirm_raises

            def refuse() -> None:
                self.log.append("confirm_delivery")
                raise error

            chan.confirm_delivery = refuse
        self.channels.append(chan)
        return chan

    def process_data_events(self, time_limit: float = 0) -> None:
        """Records the probe, then raises when told to.

        Args:
          time_limit: Recorded.

        Returns:
          None.

        Raises:
          Exception: ``probe_raises``.
        """
        self.log.append(f"process_data_events time_limit={time_limit}")
        if self.probe_raises is not None:
            error, self.probe_raises = self.probe_raises, None
            raise error

    def close(self) -> None:
        """Records the close.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.is_open = False
        self.log.append("connection.close")


def _owned(monkeypatch: pytest.MonkeyPatch, *connections: _Connection, **kwargs: Any) -> Any:
    """A sink that opens its own connections, handed out in order through a stub ``pika``.

    Args:
      monkeypatch: The test's patch.
      *connections: The connections ``BlockingConnection`` returns, in order.
      **kwargs: More constructor arguments.

    Returns:
      The sink.

    Raises:
      None.
    """
    pool = list(connections)
    module = types.SimpleNamespace(
        URLParameters=lambda url: types.SimpleNamespace(blocked_connection_timeout=None),
        ConnectionParameters=lambda: types.SimpleNamespace(blocked_connection_timeout=None),
        BlockingConnection=lambda params: pool.pop(0),
        BasicProperties=lambda **kw: types.SimpleNamespace(**kw),
        exceptions=sys.modules.get("pika.exceptions"),
    )
    monkeypatch.setitem(sys.modules, "pika", module)
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)
    return RabbitMQSink(exchange="x", routing_key="k", url="amqp://h/", **kwargs)


def _borrowed(monkeypatch: pytest.MonkeyPatch, conn: _Connection, **kwargs: Any) -> Any:
    """A sink on a borrowed connection.

    Args:
      monkeypatch: The test's patch.
      conn: The connection.
      **kwargs: More constructor arguments.

    Returns:
      The sink.

    Raises:
      None.
    """
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)
    return RabbitMQSink(exchange="x", routing_key="k", connection=conn, **kwargs)


def _lost(err: str) -> list[str]:
    """The ``lost`` lines of a stderr capture.

    Args:
      err: The capture.

    Returns:
      The lines.

    Raises:
      None.
    """
    return [line for line in err.splitlines() if "lost" in line]


def test_a_confirming_owned_sink_confirms_each_channel_publishes_mandatory_and_probes(
    monkeypatch: pytest.MonkeyPatch, pika_errors: types.ModuleType
) -> None:
    """Criterion 3, owned: one ``confirm_delivery`` per channel, ``mandatory``, a probe a round."""
    log: list[str] = []
    sink = _owned(monkeypatch, _Connection(log, []), confirm=True)
    sink.emit([{"i": 1}, {"i": 2}])
    sink.emit([{"i": 3}])
    assert log == [
        "process_data_events time_limit=0",
        "channel",
        "confirm_delivery",
        "publish mandatory=True",
        "publish mandatory=True",
        "process_data_events time_limit=0",
        "publish mandatory=True",
    ]


def test_a_confirming_borrowed_sink_confirms_and_publishes_mandatory_but_never_probes(
    monkeypatch: pytest.MonkeyPatch, pika_errors: types.ModuleType
) -> None:
    """Criterion 3, borrowed: probing would run the caller's own consumer callbacks."""
    log: list[str] = []
    sink = _borrowed(monkeypatch, _Connection(log, []), confirm=True)
    sink.emit([{"i": 1}])
    sink.emit([{"i": 2}])
    assert log == [
        "channel",
        "confirm_delivery",
        "publish mandatory=True",
        "publish mandatory=True",
    ]


def test_a_confirming_borrowed_sink_closes_its_channel_and_leaves_the_connection_open(
    monkeypatch: pytest.MonkeyPatch, pika_errors: types.ModuleType
) -> None:
    """SPEC-107 FR-001: the close ends the confirming channel and never the caller's connection."""
    log: list[str] = []
    conn = _Connection(log, [])
    sink = _borrowed(monkeypatch, conn, confirm=True)
    sink.emit([{"i": 1}])
    sink.close()
    assert log == ["channel", "confirm_delivery", "publish mandatory=True", "channel.close"]
    assert conn.is_open is True


@pytest.mark.parametrize("build", ["owned", "borrowed"])
def test_a_default_sink_makes_none_of_the_three_calls(
    build: str, monkeypatch: pytest.MonkeyPatch, pika_errors: types.ModuleType
) -> None:
    """Criterion 3, default: no confirm, no ``mandatory``, no probe: 1.x exactly."""
    log: list[str] = []
    conn = _Connection(log, [])
    sink = _owned(monkeypatch, conn) if build == "owned" else _borrowed(monkeypatch, conn)
    sink.emit([{"i": 1}])
    sink.emit([{"i": 2}])
    assert log == ["channel", "publish mandatory=None", "publish mandatory=None"]


def test_an_owned_connection_that_died_idle_is_replaced_before_the_first_publish(
    monkeypatch: pytest.MonkeyPatch,
    pika_errors: types.ModuleType,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Criterion 4: the probe's ``StreamLostError`` reconnects; every event ``ACCEPTED``, no line."""
    log: list[str] = []
    first = _Connection(log, [])
    sink = _owned(monkeypatch, first, _Connection(log, []), confirm=True)
    sink.emit([{"i": 0}])
    first.probe_raises = pika_errors.StreamLostError("Stream connection lost")
    capsys.readouterr()
    log.clear()
    sink.emit([{"i": 1}, {"i": 2}])
    assert log == [
        "process_data_events time_limit=0",
        "channel.close",
        "connection.close",
        "channel",
        "confirm_delivery",
        "publish mandatory=True",
        "publish mandatory=True",
    ]
    assert sink.losses().failed == 0
    assert _lost(capsys.readouterr().err) == []


@pytest.mark.parametrize("name", ["UnroutableError", "NackError"])
def test_a_broker_refusal_is_rejected_once_and_the_channel_is_kept(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
    pika_errors: types.ModuleType,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Criterion 5: ``REJECTED``, published once, the channel open for the next message."""
    log: list[str] = []
    conn = _Connection(log, [getattr(pika_errors, name)("refused"), None])
    sink = _borrowed(monkeypatch, conn, confirm=True)
    sink.emit([{"i": 1}, {"i": 2}])
    assert log.count("publish mandatory=True") == 2
    assert log.count("channel") == 1 and "channel.close" not in log
    assert sink.losses().failed == 1
    [line] = _lost(capsys.readouterr().err)
    assert f"RabbitMQSink, 1 attempt(s), {name}" in line


def test_a_lost_confirm_is_unknown_published_once_and_the_next_message_reconnects(
    monkeypatch: pytest.MonkeyPatch,
    pika_errors: types.ModuleType,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Criterion 5: a confirming ``StreamLostError`` is ``UNKNOWN``, never resent; a reconnect."""
    log: list[str] = []
    script: list[Exception | None] = [pika_errors.StreamLostError("Stream connection lost")]
    first, second = _Connection(log, script), _Connection(log, script)
    sink = _owned(monkeypatch, first, second, confirm=True)
    sink.emit([{"i": 1}, {"i": 2}])
    assert log.count("publish mandatory=True") == 2
    assert [c.is_open for c in (first, second)] == [False, True]
    assert sink.losses().failed == 1
    [line] = _lost(capsys.readouterr().err)
    assert (
        "lost 1 message(s); RabbitMQSink, no confirm before the connection was lost "
        "(StreamLostError); may have landed, not retried" in line
    )


@pytest.mark.parametrize("retryable", [True, False], ids=["stream-lost", "closed-by-broker"])
def test_a_confirm_delivery_that_raises_closes_its_channel_and_leaves_the_predicate_to_decide(
    retryable: bool, monkeypatch: pytest.MonkeyPatch, pika_errors: types.ModuleType
) -> None:
    """Criterion 6: the channel is closed; its events ``RETRYABLE`` or ``REJECTED`` as answered.

    ``ConnectionWrongStateError`` is an ``AMQPConnectionError`` the predicate retries;
    ``ChannelClosedByBroker`` it fails at once.
    """
    log: list[str] = []
    error = (
        pika_errors.ConnectionWrongStateError("closed")
        if retryable
        else pika_errors.ChannelClosedByBroker("closed")
    )
    conn = _Connection(log, [], confirm_raises=error)
    sink = _borrowed(monkeypatch, conn, confirm=True, max_retries=1)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"i": 1}])
    assert all(chan.closed for chan in conn.channels)
    assert log.count("confirm_delivery") == (2 if retryable else 1)
    assert "publish mandatory=True" not in log


def test_a_channel_without_confirm_delivery_publishes_unconfirmed_as_in_1x(
    monkeypatch: pytest.MonkeyPatch, pika_errors: types.ModuleType
) -> None:
    """Criterion 7: probed by name, so a stand-in lacking it publishes and is ``ACCEPTED``."""
    log: list[str] = []
    sink = _borrowed(monkeypatch, _Connection(log, [], confirms=False), confirm=True)
    sink.emit([{"i": 1}])
    assert log == ["channel", "publish mandatory=None"]
    assert sink.losses().failed == 0


def test_the_signature_gains_only_a_trailing_keyword_only_confirm() -> None:
    """Criterion 8: the 1.x parameters, then ``confirm``, keyword-only, defaulting to ``False``."""
    params = list(inspect.signature(RabbitMQSink).parameters.values())
    assert [p.name for p in params] == [
        "exchange",
        "routing_key",
        "connection",
        "url",
        "max_retries",
        "blocked_connection_timeout",
        "socket_timeout",
        "stack_timeout",
        "confirm",
    ]
    last = params[-1]
    assert last.kind is inspect.Parameter.KEYWORD_ONLY and last.default is False


@pytest.mark.parametrize("build", ["owned", "borrowed"])
@pytest.mark.parametrize("value", ["false", 0, 1.0], ids=["str", "int", "float"])
def test_a_non_bool_confirm_is_refused_naming_it_before_any_connection(
    build: str, value: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Criterion 9 (the orchestrator's ruling on the plan): a new argument is refused, not read."""
    opened: list[object] = []
    module = types.SimpleNamespace(
        URLParameters=lambda url: opened.append(url),
        BlockingConnection=lambda params: opened.append(params),
    )
    monkeypatch.setitem(sys.modules, "pika", module)
    kwargs: dict[str, Any] = (
        {"url": "amqp://h/"} if build == "owned" else {"connection": _Connection([], [])}
    )
    with pytest.raises(TypeError) as raised:
        RabbitMQSink(exchange="x", routing_key="k", confirm=value, **kwargs)  # type: ignore[arg-type]
    message = str(raised.value)
    assert "confirm" in message and type(value).__name__ in message, message
    assert opened == []


@pytest.mark.parametrize("value", [True, False])
def test_a_bool_confirm_is_accepted(value: bool) -> None:
    """``True`` and ``False`` build."""
    RabbitMQSink(exchange="x", routing_key="k", connection=_Connection([], []), confirm=value)
