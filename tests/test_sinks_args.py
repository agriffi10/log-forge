"""SPEC-071 FR-003 — every option a sink builds its client from is refused beside an injected one.

`RedisListSink(client=c, url=...)`, `AzureEventHubsSink(producer=p, connection_str=...)` and seven
more constructed and never read the option, so what the caller configured was not what was in
use. Each now goes through `sinks/_args.refuse_with_injected`, the one copy of the check five
constructors carried by hand.

Serves invariant 13 ("no argument is silently ignored because another was given") and invariant
10: `PostgresSink.connect_timeout`'s one signature change only widens its type.
"""

from __future__ import annotations

import asyncio
import inspect
import sqlite3
import sys
import types
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks import eventhubs
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks.clickhouse import ClickHouseSink
from log_foundry.sinks.eventhubs import AzureEventHubsSink
from log_foundry.sinks.kafka import KafkaSink
from log_foundry.sinks.mongodb import MongoDBSink
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.postgres import DEFAULT_CONNECT_TIMEOUT, PostgresSink
from log_foundry.sinks.rabbitmq import RabbitMQSink
from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink
from log_foundry.sinks.sqlite import SQLiteSink
from test_sinks_nats import FakeNATS

if TYPE_CHECKING:
    from collections.abc import Callable


# --- the helper ----------------------------------------------------------------------------------


class _Hostile:
    """A value every dunder of which raises, counting the attempts."""

    touched = 0

    def _refuse(self, *_args: object) -> Any:
        type(self).touched += 1
        raise AssertionError("the value's own code ran")

    __bool__ = __eq__ = __repr__ = __str__ = __hash__ = __len__ = _refuse


def test_nothing_injected_returns_whatever_was_supplied() -> None:
    refuse_with_injected("S", "client", None, {"a": 1, "b": "x"})


def test_an_injected_object_with_every_value_none_returns() -> None:
    refuse_with_injected("S", "client", object(), {"a": None, "b": None})
    refuse_with_injected("S", "client", object(), {})


def test_falsy_values_are_supplied_values() -> None:
    with pytest.raises(ValueError) as info:
        refuse_with_injected("S", "client", object(), {"b": 0, "a": None, "c": {}})
    assert str(info.value) == (
        "S cannot apply b, c to an injected client; pass them where the client is built, "
        "or drop client="
    )
    for value in (False, "", [], 0.0):
        with pytest.raises(ValueError, match=r"^S cannot apply a to an injected producer;"):
            refuse_with_injected("S", "producer", object(), {"a": value})


@pytest.mark.parametrize(
    "injected", [0, {}, False, ""], ids=["zero", "empty-dict", "False", "empty-str"]
)
def test_a_falsy_injected_object_is_still_injected(injected: object) -> None:
    with pytest.raises(ValueError, match="drop connection="):
        refuse_with_injected("S", "connection", injected, {"url": "x"})


def test_no_code_of_the_injected_object_or_a_value_runs() -> None:
    _Hostile.touched = 0
    with pytest.raises(ValueError) as info:
        refuse_with_injected("S", "client", _Hostile(), {"z": _Hostile(), "y": None})
    assert _Hostile.touched == 0
    assert str(info.value).startswith("S cannot apply z to an injected client;")


# --- the ten newly refused options --------------------------------------------------------------


def _counting(monkeypatch: pytest.MonkeyPatch) -> tuple[list[str], Callable[[str], Any]]:
    calls: list[str] = []

    def factory(label: str) -> Any:
        def build(*_args: Any, **_kwargs: Any) -> Any:
            calls.append(label)
            return {"d": {"c": object()}} if label == "pymongo" else types.SimpleNamespace()

        return build

    def install(name: str, **attrs: Any) -> None:
        monkeypatch.setitem(sys.modules, name, types.SimpleNamespace(**attrs))

    install("redis", Redis=types.SimpleNamespace(from_url=factory("redis")))
    install(
        "azure.eventhub",
        EventHubProducerClient=types.SimpleNamespace(from_connection_string=factory("azure")),
        EventData=object,
    )
    install("psycopg", connect=factory("psycopg"))
    install("clickhouse_connect", get_client=factory("clickhouse"))
    install("confluent_kafka", Producer=factory("kafka"))
    install("pymongo", MongoClient=factory("pymongo"))
    parameters = lambda *_a: types.SimpleNamespace(blocked_connection_timeout=None)  # noqa: E731
    install(
        "pika",
        URLParameters=parameters,
        ConnectionParameters=parameters,
        BlockingConnection=factory("pika"),
    )
    return calls, factory


_REFUSED: dict[str, tuple[str, Callable[[], Any], Callable[[], Any]]] = {
    "RedisListSink.url": (
        "RedisListSink cannot apply url to an injected client",
        lambda: RedisListSink("k", client=object(), url="redis://x"),
        lambda: RedisListSink("k", url="redis://x"),
    ),
    "RedisStreamsSink.url": (
        "RedisStreamsSink cannot apply url to an injected client",
        lambda: RedisStreamsSink("s", client=object(), url="redis://x"),
        lambda: RedisStreamsSink("s", url="redis://x"),
    ),
    "AzureEventHubsSink.connection_str": (
        "AzureEventHubsSink cannot apply connection_str to an injected producer",
        lambda: AzureEventHubsSink(producer=object(), connection_str="Endpoint=sb://x/"),
        lambda: AzureEventHubsSink(connection_str="Endpoint=sb://x/"),
    ),
    "AzureEventHubsSink.eventhub": (
        "AzureEventHubsSink cannot apply eventhub to an injected producer",
        lambda: AzureEventHubsSink(producer=object(), eventhub="h"),
        lambda: AzureEventHubsSink(connection_str="Endpoint=sb://x/", eventhub="h"),
    ),
    "PostgresSink.dsn": (
        "PostgresSink cannot apply dsn to an injected connection",
        lambda: PostgresSink("t", connection=object(), dsn="postgresql://x"),
        lambda: PostgresSink("t", dsn="postgresql://x"),
    ),
    "PostgresSink.connect_timeout": (
        "PostgresSink cannot apply connect_timeout to an injected connection",
        lambda: PostgresSink("t", connection=object(), connect_timeout=30),
        lambda: PostgresSink("t", dsn="postgresql://x", connect_timeout=30),
    ),
    "ClickHouseSink.dsn": (
        "ClickHouseSink cannot apply dsn to an injected client",
        lambda: ClickHouseSink("t", client=object(), dsn="clickhouse://x"),
        lambda: ClickHouseSink("t", dsn="clickhouse://x"),
    ),
    "KafkaSink.bootstrap_servers": (
        "KafkaSink cannot apply bootstrap_servers to an injected producer",
        lambda: KafkaSink("t", producer=object(), bootstrap_servers="b:9092"),
        lambda: KafkaSink("t", bootstrap_servers="b:9092"),
    ),
    "MongoDBSink.uri": (
        "MongoDBSink cannot apply uri to an injected client",
        lambda: MongoDBSink(
            client={"d": {"c": object()}}, uri="mongodb://x", database="d", collection="c"
        ),
        lambda: MongoDBSink(uri="mongodb://x", database="d", collection="c"),
    ),
    "RabbitMQSink.url": (
        "RabbitMQSink cannot apply url to an injected connection",
        lambda: RabbitMQSink(exchange="e", routing_key="r", connection=object(), url="amqp://x"),
        lambda: RabbitMQSink(exchange="e", routing_key="r", url="amqp://x"),
    ),
}
"""Each option ignored beside an injected object at ``fda134a``: the refusal's prefix, the refused
construction, and the same argument without the injected object."""


@pytest.fixture
def builds(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls, _ = _counting(monkeypatch)
    monkeypatch.setattr(eventhubs, "_event_data_cls", lambda: object)
    return calls


@pytest.mark.parametrize("case", sorted(_REFUSED))
def test_a_build_option_beside_an_injected_object_is_refused_before_anything_is_built(
    builds: list[str], case: str
) -> None:
    prefix, refused, _ = _REFUSED[case]
    with pytest.raises(ValueError) as info:
        refused()
    assert str(info.value).startswith(prefix + ";"), info.value
    assert "drop " in str(info.value)
    assert builds == [], f"{case}: a client was built before the refusal"


@pytest.mark.parametrize("case", sorted(_REFUSED))
def test_the_same_option_without_the_injected_object_still_builds(
    builds: list[str], case: str
) -> None:
    _, _, accepted = _REFUSED[case]
    accepted()
    assert len(builds) == 1, f"{case}: the option no longer reaches the build"


def test_nats_refuses_before_building_its_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    loops: list[object] = []
    real = asyncio.new_event_loop

    def counting() -> asyncio.AbstractEventLoop:
        loops.append(object())
        return real()

    monkeypatch.setattr(asyncio, "new_event_loop", counting)
    with pytest.raises(ValueError, match=r"^NATSSink cannot apply servers to an injected client;"):
        NATSSink("s", client=FakeNATS(), servers="nats://x:4222")
    assert loops == []


# --- PostgresSink.connect_timeout's "not given" default ------------------------------------------


def _psycopg(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []

    def connect(_dsn: str, **kwargs: Any) -> Any:
        seen.append(kwargs)
        return types.SimpleNamespace()

    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=connect))
    return seen


@pytest.mark.parametrize(
    ("kwargs", "connects_with"),
    [
        ({"connect_timeout": 30}, 30),
        ({}, DEFAULT_CONNECT_TIMEOUT),
        ({"connect_timeout": None}, DEFAULT_CONNECT_TIMEOUT),
        ({"connect_timeout": 0}, 2),
        ({"connect_timeout": True}, 2),
    ],
    ids=["30", "omitted", "explicit-None", "zero-floors", "True-floors"],
)
def test_postgres_connects_with_the_given_or_default_timeout(
    monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, Any], connects_with: int
) -> None:
    seen = _psycopg(monkeypatch)
    sink = PostgresSink("t", dsn="postgresql://x", **kwargs)
    assert seen == [{"connect_timeout": connects_with}]
    assert sink.connect_timeout == connects_with
    assert type(sink.connect_timeout) is int


def test_postgres_default_timeout_is_still_five() -> None:
    assert DEFAULT_CONNECT_TIMEOUT == 5
    sink = PostgresSink("t", connection=object())
    assert sink.connect_timeout == 5


def test_postgres_accepts_an_explicit_none_beside_a_connection() -> None:
    assert PostgresSink("t", connection=object(), connect_timeout=None).connect_timeout == 5


def test_postgres_refuses_a_zero_beside_a_connection() -> None:
    with pytest.raises(ValueError, match="cannot apply connect_timeout to an injected connection"):
        PostgresSink("t", connection=object(), connect_timeout=0)


def test_postgres_judges_the_type_before_refusing_the_combination() -> None:
    """SPEC-066's ``TypeError`` still wins beside ``connection=``."""
    with pytest.raises(
        TypeError, match=r"^PostgresSink connect_timeout must be an integer, not float$"
    ):
        PostgresSink("t", connection=object(), connect_timeout=2.5)  # type: ignore[arg-type]


def test_postgres_connect_timeout_only_widens_its_type() -> None:
    parameter = inspect.signature(PostgresSink, eval_str=True).parameters["connect_timeout"]
    assert parameter.default is None
    assert parameter.annotation == int | None


# --- the recorded exception ----------------------------------------------------------------------


def test_sqlite_still_takes_its_required_database_beside_a_connection() -> None:
    """Invariant 13's recorded exception: required, positional, documented as ignored."""
    connection = sqlite3.connect(":memory:")
    try:
        sink = SQLiteSink(":never-opened:", connection=connection)
        sink.emit([{"log_id": "a", "level": "INFO", "trace_id": "t", "span_id": "s"}])
        rows = connection.execute("SELECT COUNT(*) FROM log_events").fetchone()
        assert rows == (1,)
    finally:
        connection.close()
