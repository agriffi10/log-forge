"""SPEC-066 — numeric doors: every int sink argument refuses a non-int, and the public waits.

Every refusal is asserted on its message, never on the exception type alone: before SPEC-066 a
`str` or `None` already raised a `TypeError` here — out of the comparison inside `max()`, naming
neither the argument nor the type — so a bare `pytest.raises(TypeError)` would pass against the
defect. An "exact int" is asserted with `type(x) is int`, because `max(True, 1)` returns `True`
and `True == 1`, so an equality alone would pass against the bare floor.

The module imports only names that existed before SPEC-066 at module level, so the file still
collects against the old tree for the fail-first run; the helpers are imported where used.
"""

from __future__ import annotations

import asyncio
import decimal
import fractions
import inspect
import math
import os
import sys
import threading
import time
import types
from typing import TYPE_CHECKING, Any, ClassVar

import pytest

import log_foundry
from conftest import release_after_clock
from log_foundry.sinks._socket import SocketTransport
from log_foundry.sinks.base import Sink
from log_foundry.sinks.clickhouse import ClickHouseSink
from log_foundry.sinks.eventhubs import AzureEventHubsSink
from log_foundry.sinks.file import RotatingFileSink
from log_foundry.sinks.firehose import FirehoseSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.kinesis import KinesisSink
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.mongodb import MongoDBSink
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.postgres import PostgresSink
from log_foundry.sinks.pubsub import DEFAULT_MAX_PENDING, GooglePubSubSink
from log_foundry.sinks.rabbitmq import RabbitMQSink
from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink
from log_foundry.sinks.syslog import SyslogSink
from log_foundry.worker import Worker

if TYPE_CHECKING:
    from collections.abc import Callable

DSN = "https://pubkey@o123.ingest.sentry.io/456"


@pytest.fixture(autouse=True)
def _event_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """SPEC-071 FR-002: `AzureEventHubsSink` resolves `EventData` at construction, extra or not."""
    monkeypatch.setattr("log_foundry.sinks.eventhubs._event_data_cls", lambda: object)


class Lie(int):
    """An int subclass whose `<` and `>` return False: `max(Lie(-3), 0)` returns the Lie itself."""

    def __lt__(self, other: object) -> bool:
        return False

    def __gt__(self, other: object) -> bool:
        return False


class TrueLie(int):
    """An int subclass whose every ordering comparison returns True: `0 <= x <= 65535` passes."""

    def __lt__(self, other: object) -> bool:
        return True

    def __le__(self, other: object) -> bool:
        return True

    def __gt__(self, other: object) -> bool:
        return True

    def __ge__(self, other: object) -> bool:
        return True


class LeLie(int):
    """An int subclass whose `<=` returns False, so a bare range check refuses a good port."""

    def __le__(self, other: object) -> bool:
        return False


class _Response:
    """A minimal 200 response for the HTTP opener."""

    status = 200
    headers: ClassVar[dict[str, str]] = {}

    def read(self, n: int = -1) -> bytes:
        return b"{}"

    def getcode(self) -> int:
        return 200


class _Opener:
    """A `urlopen`-shaped callable that records each request body and answers 200."""

    def __init__(self) -> None:
        self.bodies: list[bytes] = []

    def __call__(self, request: Any, timeout: float | None = None) -> _Response:
        self.bodies.append(request.data)
        return _Response()


class _Quiet(Sink):
    """A sink that keeps what it is given, for the worker."""

    def emit(self, batch: list[dict[str, object]]) -> None:
        return None

    def close(self) -> None:
        return None


# One factory per concrete class with a `max_retries` door, built as the existing construction
# tests build it (an injected client, opener or transport), and the attribute that stores the
# floored value. The roster test in `test_numeric_door_roster.py` imports this dict; `cls` is in
# the tuple so that test can resolve each class through its MRO.
MAX_RETRIES_DOORS: dict[str, tuple[type, Callable[..., Any], str]] = {
    "SocketTransport": (
        SocketTransport,
        lambda **kw: SocketTransport("h", 514, **kw),
        "max_retries",
    ),
    "ClickHouseSink": (
        ClickHouseSink,
        lambda **kw: ClickHouseSink("t", client=object(), **kw),
        "max_retries",
    ),
    "AzureEventHubsSink": (
        AzureEventHubsSink,
        lambda **kw: AzureEventHubsSink(producer=object(), **kw),
        "max_retries",
    ),
    "FirehoseSink": (
        FirehoseSink,
        lambda **kw: FirehoseSink("s", client=object(), **kw),
        "max_retries",
    ),
    "HTTPSink": (
        HTTPSink,
        lambda **kw: HTTPSink("http://x", opener=_Opener(), **kw),
        "max_retries",
    ),
    "KinesisSink": (
        KinesisSink,
        lambda **kw: KinesisSink("s", client=object(), **kw),
        "max_retries",
    ),
    "MongoDBSink": (
        MongoDBSink,
        lambda **kw: MongoDBSink(client={"d": {"c": object()}}, database="d", collection="c", **kw),
        "max_retries",
    ),
    "PostgresSink": (
        PostgresSink,
        lambda **kw: PostgresSink("t", connection=object(), **kw),
        "max_retries",
    ),
    "RabbitMQSink": (
        RabbitMQSink,
        lambda **kw: RabbitMQSink(exchange="e", routing_key="r", connection=object(), **kw),
        "max_retries",
    ),
    "RedisStreamsSink": (
        RedisStreamsSink,
        lambda **kw: RedisStreamsSink("s", client=object(), **kw),
        "max_retries",
    ),
    "RedisListSink": (
        RedisListSink,
        lambda **kw: RedisListSink("k", client=object(), **kw),
        "max_retries",
    ),
    "SNSSink": (SNSSink, lambda **kw: SNSSink("arn", client=object(), **kw), "max_retries"),
    "SQSSink": (
        SQSSink,
        lambda **kw: SQSSink("https://q", client=object(), **kw),
        "max_retries",
    ),
}

_NON_INTS = [
    (2.5, "float"),
    (math.nan, "float"),
    (math.inf, "float"),
    ("2", "str"),
    (None, "NoneType"),
]
_NON_INT_IDS = ["2.5", "nan", "inf", "str", "None"]


def _worker(max_retries: Any) -> Worker:
    return Worker(_Quiet(), max_retries=max_retries)


def _stored_max_retries(door: str, value: Any) -> Any:
    """Builds the door's class with `max_retries=value` and returns what it stored."""
    if door == "Worker":
        worker = _worker(value)
        try:
            return worker.max_retries
        finally:
            worker.stop()
    _cls, factory, attr = MAX_RETRIES_DOORS[door]
    return getattr(factory(max_retries=value), attr)


def _raises_section(cls: type) -> str:
    """Returns the `Raises:` section of a constructor's docstring, up to the next section."""
    doc = inspect.getdoc(cls.__init__) or ""
    _head, sep, tail = doc.partition("\nRaises:\n")
    assert sep, f"{cls.__name__}.__init__ has no Raises: section"
    return tail


def _type_error_entry(raises: str) -> str:
    """Returns the `TypeError:` entry of a `Raises:` section, continuation lines included."""
    lines = raises.splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("TypeError:"):
            indent = len(line) - len(line.lstrip())
            entry = [line]
            for more in lines[i + 1 :]:
                if more.strip() and len(more) - len(more.lstrip()) <= indent:
                    break
                entry.append(more)
            return "\n".join(entry)
    return ""


# --- FR-001: max_retries ----------------------------------------------------------------------


@pytest.mark.parametrize(("bad", "type_name"), _NON_INTS, ids=_NON_INT_IDS)
@pytest.mark.parametrize("door", sorted(MAX_RETRIES_DOORS))
def test_max_retries_refuses_a_non_int_at_every_door(door: str, bad: Any, type_name: str) -> None:
    """FR-001 AC-1: the floats constructed before; "2" and None raised an unnamed comparison error."""
    _cls, factory, _attr = MAX_RETRIES_DOORS[door]
    with pytest.raises(TypeError) as info:
        factory(max_retries=bad)
    message = str(info.value)
    assert "max_retries" in message, message
    assert f"not {type_name}" in message, message


def test_http_sink_refuses_a_float_max_retries() -> None:
    """FR-001 AC-2: `HTTPSink(max_retries=2.5)` raised out of `range()` on every batch, uncounted."""
    with pytest.raises(TypeError, match=r"^HTTPSink max_retries must be an integer, not float$"):
        HTTPSink("http://x", max_retries=2.5)  # type: ignore[arg-type]


_DELEGATING = {
    "syslog": lambda **kw: SyslogSink("h", **kw),
    "logstash-socket": lambda **kw: LogstashSink(host="h", port=5000, **kw),
    "logstash-url": lambda **kw: LogstashSink("http://x", **kw),
    "sentry-http": lambda **kw: SentrySink(DSN, backend="http", **kw),
}


@pytest.mark.parametrize("build", list(_DELEGATING.values()), ids=list(_DELEGATING))
def test_a_delegating_sink_refuses_a_float_max_retries(build: Callable[..., Any]) -> None:
    """FR-001 AC-3: each is probed through its own constructor, not only the class that judges."""
    with pytest.raises(TypeError, match="max_retries must be an integer, not float"):
        build(max_retries=2.5)


@pytest.mark.parametrize(
    ("cls", "names"),
    [
        (SyslogSink, ("port", "max_retries")),
        (LogstashSink, ("port", "max_retries")),
        (SentrySink, ("max_retries",)),
    ],
    ids=["SyslogSink", "LogstashSink", "SentrySink"],
)
def test_a_delegating_sink_documents_the_type_error(cls: type, names: tuple[str, ...]) -> None:
    """FR-001 AC-3 and FR-003 AC-4: callers read the class they built, not the delegate."""
    entry = _type_error_entry(_raises_section(cls))
    assert entry, f"{cls.__name__}'s Raises: does not name TypeError"
    for name in names:
        assert f"``{name}``" in entry, (cls.__name__, name, entry)


def _counting(result: Any = None) -> tuple[list[tuple[Any, ...]], Callable[..., Any]]:
    calls: list[tuple[Any, ...]] = []

    def factory(*args: Any, **kwargs: Any) -> Any:
        calls.append(args)
        return result if result is not None else types.SimpleNamespace()

    return calls, factory


def _boto3(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls, factory = _counting()
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=factory))
    return calls


def _mongo(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls, factory = _counting({"d": {"c": object()}})
    monkeypatch.setitem(sys.modules, "pymongo", types.SimpleNamespace(MongoClient=factory))
    return calls


def _eventhubs(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls, factory = _counting()
    client = types.SimpleNamespace(from_connection_string=factory)
    monkeypatch.setitem(
        sys.modules, "azure.eventhub", types.SimpleNamespace(EventHubProducerClient=client)
    )
    return calls


def _redis(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls, factory = _counting()
    redis = types.SimpleNamespace(Redis=types.SimpleNamespace(from_url=factory))
    monkeypatch.setitem(sys.modules, "redis", redis)
    return calls


def _clickhouse(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls, factory = _counting()
    monkeypatch.setitem(
        sys.modules, "clickhouse_connect", types.SimpleNamespace(get_client=factory)
    )
    return calls


def _psycopg(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls: list[tuple[Any, ...]] = []

    def connect(*args: Any, **kwargs: Any) -> Any:
        calls.append((*args, kwargs))
        return types.SimpleNamespace()

    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=connect))
    return calls


def _pika(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls, factory = _counting()

    def parameters(*_args: Any) -> Any:
        return types.SimpleNamespace(blocked_connection_timeout=None)

    module = types.SimpleNamespace(
        URLParameters=parameters, ConnectionParameters=parameters, BlockingConnection=factory
    )
    monkeypatch.setitem(sys.modules, "pika", module)
    return calls


def _pubsub(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    calls, factory = _counting()
    cloud = types.SimpleNamespace(pubsub_v1=types.SimpleNamespace(PublisherClient=factory))
    monkeypatch.setitem(sys.modules, "google.cloud", cloud)
    # SPEC-071 FR-002: the sink imports `google.cloud.pubsub_v1` whole, not off `google.cloud`.
    monkeypatch.setitem(sys.modules, "google.cloud.pubsub_v1", cloud.pubsub_v1)
    return calls


# Each sink that builds its own client, the stand-in that counts the build, and a construction
# that reaches the build path. ClickHouse, Postgres and RabbitMQ already judged before building at
# 7dc881f; they are here so the order cannot regress.
_BUILDS: dict[str, tuple[Callable[..., list[tuple[Any, ...]]], Callable[..., Any]]] = {
    "MongoDBSink": (
        _mongo,
        lambda **kw: MongoDBSink(uri="mongodb://x", database="d", collection="c", **kw),
    ),
    "AzureEventHubsSink": (
        _eventhubs,
        lambda **kw: AzureEventHubsSink(connection_str="Endpoint=sb://x/", eventhub="h", **kw),
    ),
    "RedisStreamsSink": (_redis, lambda **kw: RedisStreamsSink("s", url="redis://x", **kw)),
    "RedisListSink": (_redis, lambda **kw: RedisListSink("k", url="redis://x", **kw)),
    "FirehoseSink": (_boto3, lambda **kw: FirehoseSink("s", **kw)),
    "KinesisSink": (_boto3, lambda **kw: KinesisSink("s", **kw)),
    "SNSSink": (_boto3, lambda **kw: SNSSink("arn", **kw)),
    "SQSSink": (_boto3, lambda **kw: SQSSink("https://q", **kw)),
    "ClickHouseSink": (_clickhouse, lambda **kw: ClickHouseSink("t", dsn="clickhouse://x", **kw)),
    "PostgresSink": (_psycopg, lambda **kw: PostgresSink("t", dsn="postgresql://x", **kw)),
    "RabbitMQSink": (
        _pika,
        lambda **kw: RabbitMQSink(exchange="e", routing_key="r", url="amqp://x", **kw),
    ),
}


@pytest.mark.parametrize("door", sorted(_BUILDS))
def test_max_retries_is_judged_before_the_client_is_built(
    monkeypatch: pytest.MonkeyPatch, door: str
) -> None:
    """FR-001 AC-4: a refusal raised after the build would leave an unclosed client behind."""
    stub, build = _BUILDS[door]
    calls = stub(monkeypatch)
    build(max_retries=1)
    assert len(calls) == 1, "the premise: a good value reaches the stand-in's build exactly once"
    calls.clear()
    with pytest.raises(TypeError, match="max_retries must be an integer, not float"):
        build(max_retries=2.5)
    assert calls == [], f"{door} built its client before refusing max_retries"


def test_worker_refuses_a_float_max_retries_and_floors_a_negative() -> None:
    """FR-001 AC-5: the worker stored `max_retries` unjudged; the floor was only in `_emit`."""
    with pytest.raises(TypeError, match=r"^Worker max_retries must be an integer, not float$"):
        _worker(2.5)
    stored = _stored_max_retries("Worker", -1)
    assert stored == 0
    assert type(stored) is int


def test_a_negative_max_retries_still_floors_and_delivers() -> None:
    """FR-001 AC-6: the floor is the rule's working half and stays."""
    opener = _Opener()
    sink = HTTPSink("http://x", max_retries=-1, opener=opener)
    sink.emit([{"a": 1}])
    assert len(opener.bodies) == 1, "the batch is posted once"
    assert sink.max_retries == 0
    assert type(sink.max_retries) is int


_DOORS_AND_WORKER = [*sorted(MAX_RETRIES_DOORS), "Worker"]


@pytest.mark.parametrize(("value", "held"), [(True, 1), (False, 0)], ids=["True", "False"])
@pytest.mark.parametrize("door", _DOORS_AND_WORKER)
def test_a_bool_max_retries_is_stored_as_the_exact_int(door: str, value: bool, held: int) -> None:
    """FR-001 AC-7: `max(True, 0)` returns `True`, so a bare floor stored the bool."""
    stored = _stored_max_retries(door, value)
    assert stored == held
    assert type(stored) is int, f"{door} stored {type(stored).__name__}"


@pytest.mark.parametrize(("value", "held"), [(Lie(-3), 0), (Lie(3), 3)], ids=["Lie(-3)", "Lie(3)"])
@pytest.mark.parametrize("door", _DOORS_AND_WORKER)
def test_an_int_subclass_max_retries_is_judged_by_the_number_it_holds(
    door: str, value: int, held: int
) -> None:
    """FR-001 AC-8: `max(Lie(-3), 0)` returned the Lie, keeping a negative the floor exists for."""
    stored = _stored_max_retries(door, value)
    assert stored == held
    assert type(stored) is int, f"{door} stored {type(stored).__name__}"


# --- FR-002: the other int floors -------------------------------------------------------------


@pytest.mark.parametrize(
    ("bad", "type_name"),
    [(2.5, "float"), (math.nan, "float"), (math.inf, "float"), ("2", "str")],
    ids=["2.5", "nan", "inf", "str"],
)
@pytest.mark.parametrize("argument", ["max_bytes", "backup_count"])
def test_rotating_file_sink_refuses_a_non_int_count_and_creates_no_file(
    tmp_path: Any, argument: str, bad: Any, type_name: str
) -> None:
    """FR-002 AC-1 and FR-001 AC-4: judged before the file is opened, so nothing is left on disk."""
    path = tmp_path / "app.log"
    with pytest.raises(TypeError) as info:
        RotatingFileSink(str(path), **{argument: bad})
    assert str(info.value) == f"RotatingFileSink {argument} must be an integer, not {type_name}"
    assert not os.path.exists(path), "a refused construction created the file"


def test_an_int_backup_count_keeps_rotating_and_closes(tmp_path: Any) -> None:
    """FR-002 AC-2: the `2.5` that used to rotate once and then fail every emit raises at the call."""
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, max_bytes=40, backup_count=2)
    for i in range(8):
        sink.emit([{"message": f"event number {i:04d}"}])
    sink.close()
    assert os.path.exists(f"{path}.1")
    assert os.path.exists(f"{path}.2")
    assert not os.path.exists(f"{path}.3"), "two backups retained, not three"


def test_rotating_file_sink_floors_a_negative_count_to_the_exact_zero(tmp_path: Any) -> None:
    """FR-002 AC-3: a negative still behaves as the documented 0."""
    sink = RotatingFileSink(str(tmp_path / "a.log"), max_bytes=-1, backup_count=-1)
    try:
        for stored in (sink._max_bytes, sink._backup_count):
            assert stored == 0
            assert type(stored) is int
    finally:
        sink.close()
    sink = RotatingFileSink(str(tmp_path / "b.log"), max_bytes=True, backup_count=True)
    try:
        for stored in (sink._max_bytes, sink._backup_count):
            assert stored == 1
            assert type(stored) is int
    finally:
        sink.close()


@pytest.mark.parametrize(
    ("bad", "type_name"),
    [(2.5, "float"), (math.nan, "float"), (math.inf, "float"), ("3", "str")],
    ids=["2.5", "nan", "inf", "str"],
)
def test_http_max_batch_count_refuses_a_non_int(bad: Any, type_name: str) -> None:
    """FR-002 AC-4: `2.5` chunked by three, `nan` by one and `inf` without a limit, silently."""
    with pytest.raises(TypeError) as info:
        HTTPSink("http://x", max_batch_count=bad)
    assert str(info.value) == f"HTTPSink max_batch_count must be an integer, not {type_name}"


@pytest.mark.parametrize("value", [-3, 0])
def test_http_max_batch_count_floors_to_one_and_sends_one_event_per_request(value: int) -> None:
    """FR-002 AC-4: the floor at one is unchanged."""
    opener = _Opener()
    sink = HTTPSink("http://x", max_batch_count=value, opener=opener)
    assert sink.max_batch_count == 1
    assert type(sink.max_batch_count) is int
    sink.emit([{"a": 1}, {"a": 2}, {"a": 3}])
    assert len(opener.bodies) == 3


def test_http_max_batch_count_bool_and_none() -> None:
    """FR-002 AC-4: `True` is the exact 1; `None` is the class's own figure."""
    sink = HTTPSink("http://x", max_batch_count=True)
    assert sink.max_batch_count == 1
    assert type(sink.max_batch_count) is int
    assert HTTPSink("http://x").max_batch_count == HTTPSink.MAX_BATCH_COUNT


@pytest.mark.parametrize(
    ("bad", "type_name"),
    [(2.5, "float"), (math.nan, "float"), (math.inf, "float"), ("3", "str")],
    ids=["2.5", "nan", "inf", "str"],
)
def test_pubsub_max_pending_refuses_a_non_int(bad: Any, type_name: str) -> None:
    """FR-002 AC-5: `inf` used to mean no bound; `nan` raised at every reap."""
    with pytest.raises(TypeError) as info:
        GooglePubSubSink("t", client=object(), max_pending=bad)
    assert str(info.value) == f"GooglePubSubSink max_pending must be an integer, not {type_name}"


def test_pubsub_max_pending_floors_bools_and_defaults() -> None:
    """FR-002 AC-5: an int is floored as before; a large int is the way to ask for no bound."""
    for value, expected in ((0, 1), (-3, 1), (True, 1), (sys.maxsize, sys.maxsize)):
        stored = GooglePubSubSink("t", client=object(), max_pending=value).max_pending
        assert stored == expected
        assert type(stored) is int
    assert GooglePubSubSink("t", client=object()).max_pending == DEFAULT_MAX_PENDING


def test_pubsub_max_pending_is_judged_before_the_client_is_built(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: every judgement precedes any owned resource."""
    calls = _pubsub(monkeypatch)
    GooglePubSubSink("t", max_pending=5)
    assert len(calls) == 1, "the premise: a good value reaches the stand-in's build once"
    calls.clear()
    with pytest.raises(TypeError, match="max_pending must be an integer"):
        GooglePubSubSink("t", max_pending=2.5)  # type: ignore[arg-type]
    assert calls == []


@pytest.mark.parametrize(
    ("bad", "type_name"),
    [(2.5, "float"), (math.nan, "float"), (math.inf, "float"), ("5", "str")],
    ids=["2.5", "nan", "inf", "str"],
)
@pytest.mark.parametrize("path", ["injected", "built"])
def test_postgres_connect_timeout_refuses_a_non_int(
    monkeypatch: pytest.MonkeyPatch, path: str, bad: Any, type_name: str
) -> None:
    """FR-002 AC-6: refused with an injected connection too — the declared type is violated."""
    calls = _psycopg(monkeypatch)
    kwargs: dict[str, Any] = (
        {"connection": object()} if path == "injected" else {"dsn": "postgresql://x"}
    )
    with pytest.raises(TypeError) as info:
        PostgresSink("t", connect_timeout=bad, **kwargs)
    assert str(info.value) == f"PostgresSink connect_timeout must be an integer, not {type_name}"
    assert calls == [], "no connection is opened before the refusal"


def test_postgres_connect_timeout_floors_to_two(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002 AC-6: libpq's minimum is kept; `True` was already the exact 2 (a pin)."""
    _psycopg(monkeypatch)
    for value, expected in ((0, 2), (1, 2), (True, 2), (30, 30)):
        stored = PostgresSink("t", dsn="postgresql://x", connect_timeout=value).connect_timeout
        assert stored == expected
        assert type(stored) is int


def _other_floor(
    argument: str, value: Any, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> Any:
    if argument in ("max_bytes", "backup_count"):
        sink = RotatingFileSink(str(tmp_path / f"{argument}.log"), **{argument: value})
        try:
            return getattr(sink, f"_{argument}")
        finally:
            sink.close()
    if argument == "max_batch_count":
        return HTTPSink("http://x", max_batch_count=value).max_batch_count
    if argument == "max_pending":
        return GooglePubSubSink("t", client=object(), max_pending=value).max_pending
    _psycopg(monkeypatch)
    return PostgresSink("t", dsn="postgresql://x", connect_timeout=value).connect_timeout


_FLOORS = {"max_bytes": 0, "backup_count": 0, "max_batch_count": 1, "max_pending": 1}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            Lie(-3),
            {
                "max_bytes": 0,
                "backup_count": 0,
                "max_batch_count": 1,
                "max_pending": 1,
                "connect_timeout": 2,
            },
        ),
        (Lie(3), dict.fromkeys([*_FLOORS, "connect_timeout"], 3)),
    ],
    ids=["Lie(-3)", "Lie(3)"],
)
@pytest.mark.parametrize("argument", [*_FLOORS, "connect_timeout"])
def test_an_int_subclass_is_stored_exact_at_every_other_floor(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    argument: str,
    value: int,
    expected: dict[str, int],
) -> None:
    """FR-002 AC-7: the floor is taken on the number held, and that exact int is stored."""
    stored = _other_floor(argument, value, tmp_path, monkeypatch)
    assert stored == expected[argument]
    assert type(stored) is int, f"{argument} stored {type(stored).__name__}"


class _RetryAfterOpener:
    """Answers 429 with `Retry-After: 1` once, then 200, recording how many requests it saw."""

    def __init__(self) -> None:
        self.requests = 0

    def __call__(self, request: Any, timeout: float | None = None) -> _Response:
        import email.message
        import io
        import urllib.error

        self.requests += 1
        if self.requests == 1:
            headers = email.message.Message()
            headers["Retry-After"] = "1"
            raise urllib.error.HTTPError(request.full_url, 429, "busy", headers, io.BytesIO())
        return _Response()


class FloatLie(float):
    """A float subclass whose `<` and `>` both return True, whatever it holds."""

    def __lt__(self, other: object) -> bool:
        return True

    def __gt__(self, other: object) -> bool:
        return True


def _retry_after_waits(monkeypatch: pytest.MonkeyPatch, ceiling: Any) -> tuple[int, list[Any]]:
    """Emits one event through a 429-then-200 opener; returns the request count and the waits.

    The waits are the delivery engine's (SPEC-073), which calls ``_retry.wait`` through the module.
    """
    from log_foundry.sinks import _retry

    waits: list[Any] = []
    monkeypatch.setattr(_retry, "wait", lambda delay, stop=None: waits.append(delay))
    opener = _RetryAfterOpener()
    sink = HTTPSink("http://x", opener=opener, max_retries=1, max_retry_after=ceiling)
    sink.emit([{"a": 1}])
    assert sink.failed == 0
    return opener.requests, waits


@pytest.mark.parametrize(
    ("bad", "type_name"),
    [("30", "str"), (None, "NoneType"), (fractions.Fraction(30), "Fraction"), (b"30", "bytes")],
    ids=["str", "None", "Fraction", "bytes"],
)
def test_http_max_retry_after_refuses_a_non_number_at_construction(
    bad: Any, type_name: str
) -> None:
    """FR-002: `"30"` and `None` constructed, then raised uncounted out of every 429's `emit`."""
    with pytest.raises(TypeError) as info:
        HTTPSink("http://x", opener=_Opener(), max_retry_after=bad)
    assert type(info.value) is TypeError
    assert str(info.value) == (
        f"HTTPSink max_retry_after must be a number of seconds, not {type_name}"
    )


# The seven HTTPSink subclasses that forward `max_retry_after` through their `**http_kwargs`:
# module, and the arguments each needs besides the forwarded keyword.
_RETRY_AFTER_FORWARDERS: dict[str, tuple[str, tuple[Any, ...], dict[str, Any]]] = {
    "DatadogSink": ("datadog", (), {"api_key": "k"}),
    "ElasticsearchSink": ("elasticsearch", ("http://x",), {"index": "i"}),
    "HoneycombSink": ("honeycomb", (), {"api_key": "k", "dataset": "d"}),
    "LokiSink": ("loki", ("http://x",), {}),
    "NewRelicSink": ("newrelic", (), {"api_key": "k"}),
    "OTLPSink": ("otlp", (), {}),
    "SplunkHECSink": ("splunk", ("http://x",), {"token": "t"}),
}


def _http_family(owner: str) -> type:
    """Imports one of the forwarding HTTPSink subclasses by class name."""
    import importlib

    module = _RETRY_AFTER_FORWARDERS[owner][0]
    cls: type = getattr(importlib.import_module(f"log_foundry.sinks.{module}"), owner)
    return cls


@pytest.mark.parametrize("owner", sorted(_RETRY_AFTER_FORWARDERS))
def test_a_forwarding_subclass_refuses_a_str_max_retry_after_naming_itself(owner: str) -> None:
    """FR-002: each subclass forwards the keyword, so each is probed through its own constructor."""
    _module, args, kwargs = _RETRY_AFTER_FORWARDERS[owner]
    with pytest.raises(TypeError) as info:
        _http_family(owner)(*args, max_retry_after="30", **kwargs)
    assert str(info.value) == f"{owner} max_retry_after must be a number of seconds, not str"


def test_logstash_http_mode_refuses_a_str_max_retry_after() -> None:
    """FR-002: LogstashSink forwards it to the HTTPSink it builds in HTTP mode."""
    with pytest.raises(TypeError, match=r"^HTTPSink max_retry_after must be a number of seconds"):
        LogstashSink("http://x", max_retry_after="30")  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "cls_name",
    [*sorted(_RETRY_AFTER_FORWARDERS), "HTTPSink", "LogstashSink"],
)
def test_every_class_forwarding_max_retry_after_documents_the_type_error(cls_name: str) -> None:
    """FR-002: a caller reads the class it built, so each one's Raises: names the argument."""
    if cls_name == "HTTPSink":
        cls: type = HTTPSink
    elif cls_name == "LogstashSink":
        cls = LogstashSink
    else:
        cls = _http_family(cls_name)
    entry = _type_error_entry(_raises_section(cls))
    assert "``max_retry_after``" in entry, (cls_name, entry)


def test_a_float_subclass_max_retry_after_is_judged_by_the_number_it_holds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: a lying `-1.0` passed both `min()` calls and paused a day on `Retry-After: 1`."""
    from log_foundry.sinks import _engine

    stored = HTTPSink("http://x", opener=_Opener(), max_retry_after=FloatLie(-1.0)).max_retry_after
    assert stored == -1.0
    assert type(stored) is float
    requests, waits = _retry_after_waits(monkeypatch, FloatLie(-1.0))
    assert requests == 2
    assert waits == [_engine.BACKOFF_BASE], "an unusable ceiling falls back to backoff"
    assert type(waits[0]) is float


@pytest.mark.parametrize(
    ("ceiling", "expected"),
    [
        (math.nan, "backoff"),
        (math.inf, 1.0),
        (2.5, 1.0),
        (0.5, 0.5),
        (0, "backoff"),
        (-1.0, "backoff"),
        (True, 1.0),
        (10**400, 1.0),
    ],
    ids=["nan", "inf", "2.5", "0.5", "0", "-1.0", "True", "10**400"],
)
def test_a_numeric_max_retry_after_is_still_judged_floor_side(
    monkeypatch: pytest.MonkeyPatch, ceiling: Any, expected: Any
) -> None:
    """FR-002: a control — a number constructs and `clamp_server_delay` answers it, as before."""
    from log_foundry.sinks import _engine

    requests, waits = _retry_after_waits(monkeypatch, ceiling)
    assert requests == 2, "the 429 is retried and the batch delivered"
    assert waits == [_engine.BACKOFF_BASE if expected == "backoff" else expected]


@pytest.mark.parametrize(
    ("when", "interval"),
    [("S", 10**400), ("D", 10**305), ("s", 10**5000)],
    ids=["S-10**400", "D-10**305", "s-10**5000"],
)
def test_rotating_file_sink_refuses_a_period_too_large_for_a_float(
    tmp_path: Any, when: str, interval: int
) -> None:
    """FR-002: a raw OverflowError came out of the deadline sum after the file was opened."""
    path = tmp_path / "app.log"
    with pytest.raises(ValueError) as info:
        RotatingFileSink(str(path), when=when, interval=interval)
    assert type(info.value) is ValueError
    assert str(info.value).startswith("RotatingFileSink interval is too large: "), info.value
    assert not os.path.exists(path), "a refused construction created the file"


def test_a_long_period_that_fits_a_float_is_still_accepted(tmp_path: Any) -> None:
    """FR-002: a control — a period that converts is a time trigger that never fires, which works."""
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, when="S", interval=10**300)
    sink.emit([{"message": "kept"}])
    sink.close()
    assert not os.path.exists(f"{path}.1")
    with open(path, encoding="utf-8") as handle:
        assert "kept" in handle.read()


# --- FR-003: the port and NATS reconnect attempts ----------------------------------------------


@pytest.mark.parametrize(
    ("bad", "type_name"),
    [(2.5, "float"), ("514", "str"), (math.nan, "float"), (math.inf, "float")],
    ids=["2.5", "str", "nan", "inf"],
)
@pytest.mark.parametrize("transport", ["udp", "tcp"])
def test_socket_transport_refuses_a_non_int_port(transport: str, bad: Any, type_name: str) -> None:
    """FR-003 AC-1: `2.5` passed the range and raised from every emit; nan/inf were misnamed."""
    with pytest.raises(TypeError) as info:
        SocketTransport("h", bad, transport=transport)
    assert str(info.value) == f"SocketTransport port must be an integer, not {type_name}"


def test_socket_transport_port_bool_bounds_and_range() -> None:
    """FR-003 AC-2: the range check is unchanged and reads the exact int."""
    stored = SocketTransport("h", True)._port
    assert stored == 1
    assert type(stored) is int
    assert SocketTransport("h", 0)._port == 0
    assert SocketTransport("h", 65535)._port == 65535
    for bad in (-1, 65536):
        with pytest.raises(ValueError, match=f"SocketTransport port must be 0-65535, not {bad}"):
            SocketTransport("h", bad)


def test_a_port_whose_comparisons_lie_is_judged_by_the_number_it_holds() -> None:
    """FR-003 AC-3: the range check runs on the exact int, not the caller's operators."""
    with pytest.raises(ValueError, match=r"not 70000$"):
        SocketTransport("h", TrueLie(70000))
    stored = SocketTransport("h", LeLie(514))._port
    assert stored == 514
    assert type(stored) is int


@pytest.mark.parametrize(
    "build",
    [lambda: SyslogSink("h", 2.5), lambda: LogstashSink(host="h", port=2.5)],
    ids=["syslog", "logstash"],
)
def test_a_delegating_sink_refuses_a_float_port(build: Callable[[], Any]) -> None:
    """FR-003 AC-4: the transport is the door the delegating sinks share."""
    with pytest.raises(TypeError, match=r"^SocketTransport port must be an integer, not float$"):
        build()


class _FakeNatsClient:
    async def drain(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeNatsModule:
    """A stand-in `nats` module recording the keyword arguments `connect` received."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def _connect(self, *args: Any, **kwargs: Any) -> _FakeNatsClient:
        self.calls.append(kwargs)
        return _FakeNatsClient()

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        return self._connect(*args, **kwargs)


@pytest.mark.parametrize(
    ("bad", "type_name"),
    [(2.5, "float"), (math.nan, "float"), (math.inf, "float"), ("3", "str")],
    ids=["2.5", "nan", "inf", "str"],
)
def test_nats_max_reconnect_attempts_refuses_a_non_int(
    monkeypatch: pytest.MonkeyPatch, bad: Any, type_name: str
) -> None:
    """FR-003 AC-5: the floats passed `<= 0` and reached nats-py as given."""
    module = _FakeNatsModule()
    monkeypatch.setitem(sys.modules, "nats", module)
    created = 0
    real = asyncio.new_event_loop

    def counting_new_event_loop() -> asyncio.AbstractEventLoop:
        nonlocal created
        created += 1
        return real()

    monkeypatch.setattr(asyncio, "new_event_loop", counting_new_event_loop)
    with pytest.raises(TypeError) as info:
        NATSSink("logs", servers="nats://x:4222", max_reconnect_attempts=bad)
    assert str(info.value) == (
        f"NATSSink max_reconnect_attempts must be an integer, not {type_name}"
    )
    assert created == 0, "the refusal must come before the loop is built"
    assert module.calls == []


@pytest.mark.parametrize(("value", "held"), [(3, 3), (True, 1)], ids=["3", "True"])
def test_nats_max_reconnect_attempts_reaches_the_driver_as_the_exact_int(
    monkeypatch: pytest.MonkeyPatch, value: int, held: int
) -> None:
    """FR-003 AC-5: what nats-py receives is the exact int the caller's value holds."""
    module = _FakeNatsModule()
    monkeypatch.setitem(sys.modules, "nats", module)
    NATSSink("logs", servers="nats://x:4222", max_reconnect_attempts=value).close()
    forwarded = module.calls[0]["max_reconnect_attempts"]
    assert forwarded == held
    assert type(forwarded) is int


@pytest.mark.parametrize("value", [0, -1])
def test_nats_max_reconnect_attempts_still_refuses_a_non_positive(value: int) -> None:
    """FR-003 AC-5: the existing refusal and its message are kept."""
    with pytest.raises(ValueError, match="max_reconnect_attempts must be positive"):
        NATSSink("logs", max_reconnect_attempts=value)


_HUGE = 10**5000
_HUGE_TEXT = "<int: ~5001 digits>"

# Each door whose refusal names the value it refuses, the huge int that reaches that message,
# and the exact message. Before SPEC-066's second diff review each raised CPython's own
# "Exceeds the limit (4300 digits) for integer string conversion" out of the f-string,
# naming neither the argument nor the class.
_HUGE_DOORS: dict[str, tuple[Callable[[], Any], str]] = {
    "port-high": (
        lambda: SocketTransport("h", _HUGE),
        f"SocketTransport port must be 0-65535, not {_HUGE_TEXT}",
    ),
    "port-low": (
        lambda: SocketTransport("h", -_HUGE),
        f"SocketTransport port must be 0-65535, not {_HUGE_TEXT}",
    ),
    "require_positive": (
        lambda: HTTPSink("http://x", max_batch_bytes=-_HUGE),
        f"HTTPSink max_batch_bytes must be a positive integer, not {_HUGE_TEXT}",
    ),
    "require_timeout": (
        lambda: HTTPSink("http://x", timeout=-_HUGE),
        f"HTTPSink timeout must be a positive, finite number of seconds, not {_HUGE_TEXT}",
    ),
    "interval": (
        lambda: RotatingFileSink(os.devnull, interval=-_HUGE),
        f"RotatingFileSink interval must be a positive integer, not {_HUGE_TEXT}",
    ),
    "max_reconnect_attempts": (
        lambda: NATSSink("logs", max_reconnect_attempts=-_HUGE),
        f"NATSSink max_reconnect_attempts must be positive, not {_HUGE_TEXT}",
    ),
    "configure": (
        lambda: log_foundry.configure(max_value_bytes=-_HUGE),
        f"max_value_bytes must be >= 1, got {_HUGE_TEXT}",
    ),
}


@pytest.mark.parametrize("door", sorted(_HUGE_DOORS))
def test_a_refusal_names_an_int_too_long_to_convert(door: str) -> None:
    """FR-003: the message renders a huge int as a placeholder rather than raising out of itself."""
    build, expected = _HUGE_DOORS[door]
    with pytest.raises(ValueError) as info:
        build()
    assert type(info.value) is ValueError
    assert str(info.value).startswith(expected), str(info.value)[:200]


def test_the_int_renderer_keeps_digits_caps_the_text_and_ignores_a_subclass() -> None:
    """FR-003: digits within the limit, the placeholder beyond it even with the limit lifted."""
    from log_foundry.sanitize import _int_text

    class LoudRepr(int):
        def __repr__(self) -> str:
            return "x" * 10_000

    assert _int_text(70000) == "70000"
    assert _int_text(-5) == "-5"
    assert _int_text(LoudRepr(7)) == "7"
    assert _int_text(10**4000) == str(10**4000)
    assert _int_text(_HUGE) == _HUGE_TEXT
    previous = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(0)
        assert _int_text(10**6000) == "<int: ~6001 digits>", "a lifted limit still caps the text"
        sys.set_int_max_str_digits(1000)
        assert _int_text(10**2000) == "<int: ~2001 digits>", "a lowered limit is honoured"
    finally:
        sys.set_int_max_str_digits(previous)


@pytest.mark.parametrize(
    "number",
    [-(10**4299), 10**4299, 10**4300, -(10**4300), 10**5000],
    ids=["-10**4299", "10**4299", "10**4300", "-10**4300", "10**5000"],
)
def test_the_int_renderer_replaces_what_the_coercer_and_otel_replace(number: int) -> None:
    """FR-003: a refusal, an event value and an OTLP body agree, the sign counted, at the edge.

    ``-10**4299`` is the case that split them: 4,300 digits and a sign, which ``_bounded_int``
    and ``_otel._int_digits`` replaced and ``_int_text`` rendered in full.
    """
    from log_foundry._otel import _int_digits
    from log_foundry.config import Config
    from log_foundry.sanitize import _Coercer, _int_text

    assert sys.get_int_max_str_digits() == sys.int_info.default_max_str_digits
    bounded = _Coercer(Config())._bounded_int(number)[0]
    coerced = bounded if isinstance(bounded, str) else int.__repr__(bounded)
    otel = _int_digits(number)[0]
    assert _int_text(number) == coerced == otel
    if number == -(10**4299):
        assert otel == "<int: ~4300 digits>"
    if number == 10**4299:
        assert otel == int.__repr__(number)


# --- FR-004: the public waits -----------------------------------------------------------------


class PosLie(float):
    """A float subclass holding 0.0 whose comparisons and `__float__` all say it is positive."""

    def __float__(self) -> float:
        return 5.0

    def __gt__(self, other: object) -> bool:
        return True

    def __ge__(self, other: object) -> bool:
        return True

    def __lt__(self, other: object) -> bool:
        return False

    def __le__(self, other: object) -> bool:
        return False


class _Slow(Sink):
    """A sink that takes `delay` seconds per batch and counts what it received and closes."""

    def __init__(self, delay: float = 0.3) -> None:
        self.delay = delay
        self.got = 0
        self.closed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        time.sleep(self.delay)
        self.got += len(batch)

    def close(self) -> None:
        self.closed += 1


class _Gated(Sink):
    """A sink whose `emit` blocks until released, so a wait that does not wait is observable."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.closed = 0
        self.got = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        self.entered.set()
        self.release.wait(10.0)
        self.got += len(batch)

    def close(self) -> None:
        self.closed += 1


def _log_through(sink: Sink, events: int = 3) -> None:
    """Installs the sink and logs `events` events inside a span, which builds the worker."""
    log_foundry.configure(sink=sink)

    @log_foundry.trace
    def work() -> None:
        for i in range(events):
            log_foundry.info("x", i=i)

    work()


def _counters() -> tuple[object, ...]:
    """The health fields a refused call must not move."""
    h = log_foundry.health()
    return (h.retired, h.dropped, h.failed_batches, h.stopped_reason, h.submitted_after_shutdown)


def test_flush_with_an_infinite_timeout_waits_for_the_drain() -> None:
    """FR-004 AC-1: at `ce2e1fe` `flush(math.inf)` returned `thread-died` at 0.00 s."""
    sink = _Gated()
    _log_through(sink)
    # The emit is held until a timer started after the clock releases it. A sink that slept
    # instead could begin its 0.3 s on the worker's own 1.0 s interval before `flush` was called,
    # and a clock started after that would not see the whole of it.
    with release_after_clock(0.3, sink.release.set) as watch:
        result = log_foundry.flush(timeout=math.inf)
    elapsed = watch.elapsed
    assert result, f"flush(inf) reported {result!r}"
    assert sink.got >= 3, "the sink received the events"
    assert elapsed >= 0.25, "it waited for the drain rather than returning at once"
    assert log_foundry.health().queued == 0


@pytest.mark.parametrize("value", [math.inf, 1e300, 10**400], ids=["inf", "1e300", "10**400"])
def test_an_unbounded_timeout_shutdown_drains_and_closes(value: float) -> None:
    """FR-004 AC-2: at `ce2e1fe` `inf` and `1e300` raised OverflowError into the caller."""
    sink = _Slow()
    _log_through(sink)
    assert log_foundry.shutdown(timeout=value) is None
    assert sink.closed == 1
    assert sink.got >= 3
    assert log_foundry.health().retired


def test_an_unbounded_timeout_flush_drains() -> None:
    """FR-004 AC-2: `flush(10**400)` reported `thread-died` before."""
    sink = _Slow()
    _log_through(sink)
    assert log_foundry.flush(timeout=10**400)
    assert sink.got >= 3


def test_timeout_max_behaves_as_none() -> None:
    """FR-004 AC-3: TIMEOUT_MAX already worked on both calls and must still."""
    sink = _Slow(0.05)
    _log_through(sink)
    assert log_foundry.flush(timeout=threading.TIMEOUT_MAX)
    log_foundry.shutdown(timeout=threading.TIMEOUT_MAX)
    assert sink.closed == 1


@pytest.mark.parametrize("call", ["flush", "shutdown"])
def test_a_nan_timeout_is_refused_and_nothing_moves(call: str) -> None:
    """FR-004 AC-4: at `fda134a` `shutdown(nan)` retired the process before raising ValueError."""
    sink = _Slow(0.05)
    _log_through(sink)
    before = _counters()
    with pytest.raises(ValueError, match=r"^timeout must not be NaN$"):
        getattr(log_foundry, call)(timeout=math.nan)
    assert _counters() == before
    assert not log_foundry.health().retired
    if call == "flush":
        assert log_foundry.flush(timeout=2.0), "a retry with a good value works"
    else:
        log_foundry.shutdown(timeout=2.0)
        assert sink.closed == 1, "a retry with a good value drains and closes"


@pytest.mark.parametrize(
    ("call", "bad", "type_name"),
    [
        ("flush", "5", "str"),
        ("flush", b"5", "bytes"),
        ("shutdown", "5", "str"),
        ("shutdown", object(), "object"),
    ],
    ids=["flush-str", "flush-bytes", "shutdown-str", "shutdown-object"],
)
def test_a_non_number_timeout_is_refused(call: str, bad: object, type_name: str) -> None:
    """FR-004 AC-5: `flush("5")` returned `thread-died`; `shutdown("5")` raised a raw TypeError."""
    sink = _Slow(0.05)
    _log_through(sink)
    before = _counters()
    with pytest.raises(TypeError) as info:
        getattr(log_foundry, call)(timeout=bad)
    assert "timeout" in str(info.value)
    assert f"not {type_name}" in str(info.value)
    assert _counters() == before
    assert sink.closed == 0


@pytest.mark.parametrize(
    ("call", "bad", "type_name"),
    [("flush", fractions.Fraction(2), "Fraction"), ("shutdown", decimal.Decimal(2), "Decimal")],
    ids=["flush-Fraction", "shutdown-Decimal"],
)
def test_a_number_that_only_works_by_duck_typing_is_refused(
    call: str, bad: object, type_name: str
) -> None:
    """FR-004 AC-6: at `7dc881f` `flush(Fraction(2))` returned `ok=True`."""
    _log_through(_Slow(0.05))
    with pytest.raises(TypeError) as info:
        getattr(log_foundry, call)(timeout=bad)
    assert type_name in str(info.value)
    assert "int or float" in str(info.value)


def test_true_is_read_as_one() -> None:
    """FR-004 AC-7: a control — it held before; the recorded-argument test discriminates."""
    sink = _Slow(0.05)
    _log_through(sink)
    assert log_foundry.flush(timeout=True)
    log_foundry.shutdown(timeout=True)
    assert sink.closed == 1


@pytest.mark.parametrize(
    "value", [0, -1, -(10**400), PosLie(0.0)], ids=["0", "-1", "-10**400", "PosLie(0.0)"]
)
def test_a_flush_that_may_not_wait_times_out(value: float) -> None:
    """FR-004 AC-8: `flush(-10**400)` reported `thread-died` at `7dc881f`, not `timed-out`."""
    sink = _Gated()
    _log_through(sink)
    assert sink.entered.wait(5.0), "the drain thread is inside emit"
    try:
        result = log_foundry.flush(timeout=value)
        assert not result
        assert result.reason == "timed-out"
    finally:
        sink.release.set()


@pytest.mark.parametrize(
    "value", [0, -1, -math.inf, -(10**400)], ids=["0", "-1", "-inf", "-10**400"]
)
def test_a_shutdown_that_may_not_wait_expires_and_leaves_the_sink_open(value: float) -> None:
    """FR-004 AC-8: `shutdown(-10**400)` raised OverflowError at `7dc881f`."""
    sink = _Gated()
    _log_through(sink)
    assert sink.entered.wait(5.0), "the drain thread is inside emit"
    try:
        assert log_foundry.shutdown(timeout=value) is None
        assert log_foundry.health().stopped_reason == "ShutdownTimeout"
        assert sink.closed == 0
    finally:
        sink.release.set()


def test_flush_none_against_a_fast_sink_is_truthy() -> None:
    """FR-004 AC-8: `None` is unchanged."""
    _log_through(_Slow(0.0))
    assert log_foundry.flush(timeout=None)


@pytest.mark.parametrize(
    ("call", "bad"), [("flush", math.nan), ("shutdown", "5")], ids=["flush-nan", "shutdown-str"]
)
def test_a_refused_wait_writes_nothing_to_stderr(
    capsys: pytest.CaptureFixture[str], call: str, bad: object
) -> None:
    """FR-004 AC-9: the refusal is the caller's exception, not a diagnostic line."""
    _log_through(_Slow(0.0))
    capsys.readouterr()
    with pytest.raises((TypeError, ValueError)):
        getattr(log_foundry, call)(timeout=bad)
    assert capsys.readouterr().err == ""


def test_the_wait_defaults_are_unchanged() -> None:
    """FR-004 AC-9: no signature moves (invariant 10)."""
    from log_foundry.worker import DEFAULT_SHUTDOWN_TIMEOUT

    assert inspect.signature(log_foundry.flush).parameters["timeout"].default == 5.0
    default = inspect.signature(log_foundry.shutdown).parameters["timeout"].default
    assert default is DEFAULT_SHUTDOWN_TIMEOUT


@pytest.mark.parametrize(
    ("call", "target"),
    [("flush", "_flush_worker"), ("shutdown", "_shutdown_worker")],
    ids=["flush", "shutdown"],
)
def test_what_reaches_the_lifecycle_is_never_inf_or_nan(
    monkeypatch: pytest.MonkeyPatch, call: str, target: str
) -> None:
    """FR-004 AC-3, AC-7 and AC-10: the internals see None or an exact finite builtin."""
    from log_foundry import _lifecycle

    seen: list[object] = []
    monkeypatch.setattr(_lifecycle, target, seen.append)
    for value in (math.inf, 1e300, 2.0, True, PosLie(0.0), threading.TIMEOUT_MAX):
        getattr(log_foundry, call)(timeout=value)
    assert seen[:3] == [None, None, 2.0]
    assert seen[3] == 1
    assert type(seen[3]) is int, "True reaches the internals as the exact int"
    assert seen[4] == 0.0
    assert type(seen[4]) is float, "a float subclass reaches the internals as the exact float"
    assert seen[5] == threading.TIMEOUT_MAX, "TIMEOUT_MAX is passed on unrewritten"
