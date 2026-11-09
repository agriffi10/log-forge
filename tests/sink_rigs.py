"""SPEC-069 — one recording rig per record-building sink, for the tests that run over the roster.

Each rig builds the real sink over a double of its client, stream, socket or connection. Every
double writes into one shared log: ``("handoff", payload)`` for a call that reached the
destination, ``("refused", None)`` for one the rig made fail. A test that also logs each encode
into the same list can therefore see whether anything was handed over before every event was
encoded. ``down`` makes every client call fail until it is cleared, which is how a test makes a
bulk call fail every attempt; every sink with a retry bound is built with ``max_retries=0`` and
its backoff patched out, so a down client costs one attempt per ``emit``.

The table :data:`RIGS` is keyed by class name and is held equal to the roster the lint in
``tests/test_encode.py`` derives from the tree, so a sink cannot join the helper without a rig.
"""

from __future__ import annotations

import io
import json
import sqlite3
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import pytest

EVENTS = [
    {"log_id": "a", "level": "INFO", "timestamp": "2026-01-01T00:00:00.000Z", "message": "one"},
    {"log_id": "b", "level": "ERROR", "timestamp": "2026-01-01T00:00:01.000Z", "message": "two"},
]
"""The clean two-event batch every golden wire form is captured from."""


def unencodable(log_id: str = "bad") -> dict[str, object]:
    """An event ``json.dumps`` cannot encode: a ``bytes`` value, reachable only past assembly."""
    return {"log_id": log_id, "level": "INFO", "blob": b"\x00raw"}


def ok(log_id: str) -> dict[str, object]:
    """A small encodable event with the given id."""
    return {"log_id": log_id, "level": "INFO", "message": f"m-{log_id}"}


@dataclass
class State:
    """What the doubles share: the log, and whether every client call fails.

    ``error``, when set, is raised by every client call in place of the rig's ``ConnectionError``
    — the hook SPEC-070's tests use to put a chosen exception at each retry loop's catch.
    """

    log: list[tuple[str, Any]] = field(default_factory=list)
    down: bool = False
    fail_next: int = 0
    error: BaseException | None = None

    def call(self, payload: Any) -> None:
        """Records one client call, raising when the rig is down or has failures queued."""
        if self.error is not None:
            self.log.append(("refused", None))
            raise self.error
        if self.fail_next > 0:
            self.fail_next -= 1
            self.log.append(("refused", None))
            raise ConnectionError("rig refused this call")
        if self.down:
            self.log.append(("refused", None))
            raise ConnectionError("rig is down")
        self.log.append(("handoff", payload))


@dataclass
class Rig:
    """A built sink, the state its double writes to, and how to read what it delivered.

    ``owner`` and ``counter`` name where the sink keeps its unencodable count: SPEC-069's
    ``_unencodable`` on the sink, or, for a sink on the delivery engine (SPEC-074), the engine's
    ``dropped_events`` on its ``log_foundry_counters``.
    """

    sink: Any
    state: State
    decode: Callable[[Any], list[dict[str, Any]]]
    owner: Any
    counter: str

    def handoffs(self) -> list[Any]:
        """Every payload that reached the destination, in order."""
        return [payload for kind, payload in self.state.log if kind == "handoff"]

    def delivered(self) -> list[str]:
        """The ``log_id`` of every event delivered, in order."""
        return [event.get("log_id") for p in self.handoffs() for event in self.decode(p)]

    def calls(self) -> int:
        """How many client calls were made, refused ones included."""
        return len(self.state.log)

    def count(self) -> int:
        """The unencodable count, read from wherever this sink keeps it."""
        return int(getattr(self.owner, self.counter))


def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patches every sink module's bound ``wait`` to return at once."""
    import log_foundry.sinks._socket as socket_mod
    from log_foundry.sinks import (
        clickhouse,
        eventhubs,
        firehose,
        kinesis,
        mongodb,
        postgres,
        rabbitmq,
        redis,
        sns,
        sqs,
    )

    for mod in (
        clickhouse,
        eventhubs,
        firehose,
        kinesis,
        mongodb,
        postgres,
        rabbitmq,
        redis,
        sns,
        sqs,
        socket_mod,
    ):
        if hasattr(mod, "wait"):
            monkeypatch.setattr(mod, "wait", lambda *_a, **_k: None)


def _lines(payload: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in payload.splitlines() if line]


class _Stream:
    def __init__(self, state: State) -> None:
        self._state = state

    def write(self, data: str) -> int:
        if self._state.down:
            self._state.log.append(("refused", None))
            raise OSError(28, "No space left on device")
        self._state.log.append(("handoff", data))
        return len(data)

    def flush(self) -> None:
        self._state.log.append(("flush", None))
        if self._state.down:
            raise OSError(28, "No space left on device")

    def close(self) -> None:
        pass

    def fileno(self) -> int:
        raise io.UnsupportedOperation("no descriptor")


def stdout(mp: pytest.MonkeyPatch, tmp: Path, cls_name: str = "StdoutSink") -> Rig:
    from log_foundry.sinks import stdout as mod

    state = State()
    sink = getattr(mod, cls_name)(stream=_Stream(state))
    return Rig(sink, state, _lines, sink, "_unencodable")


def stderr(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    return stdout(mp, tmp, "StderrSink")


def file(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.file import FileSink

    state = State()
    sink = FileSink(str(tmp / "f.ndjson"))
    sink._stream.close()
    sink._stream = _Stream(state)
    return Rig(sink, state, _lines, sink, "_unencodable")


def rotating(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.file import RotatingFileSink

    state = State()
    sink = RotatingFileSink(str(tmp / "r.ndjson"), max_bytes=10**9, backup_count=1)
    sink._stream.close()
    sink._stream = _Stream(state)
    return Rig(sink, state, _lines, sink, "_unencodable")


class _SQLiteProxy:
    """A real in-memory connection whose inserts go through the rig's state first."""

    def __init__(self, state: State) -> None:
        self._state = state
        self.real = sqlite3.connect(":memory:", check_same_thread=False)

    def __enter__(self) -> Any:
        return self.real.__enter__()

    def __exit__(self, *exc: object) -> Any:
        return self.real.__exit__(*exc)

    def executemany(self, sql: str, rows: list[tuple[object, ...]]) -> Any:
        rows = list(rows)
        if self._state.down:
            self._state.log.append(("refused", None))
            raise sqlite3.OperationalError("database is locked")
        self._state.log.append(("handoff", rows))
        return self.real.executemany(sql, rows)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.real, name)


def sqlite(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.sqlite import SQLiteSink

    state = State()
    proxy = _SQLiteProxy(state)
    sink = SQLiteSink(":memory:", connection=proxy.real)
    sink._conn = proxy
    return Rig(sink, state, lambda rows: [json.loads(r[-1]) for r in rows], sink, "_unencodable")


class _ClickHouse:
    def __init__(self, state: State) -> None:
        self._state = state

    def insert(self, table: str, data: Any = None, column_names: Any = None) -> None:
        self._state.call([list(row) for row in data])

    def close(self) -> None:
        pass


def clickhouse(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.clickhouse import ClickHouseSink

    _no_wait(mp)
    state = State()
    sink = ClickHouseSink("t", client=_ClickHouse(state), max_retries=0, chunk_size=1)
    return Rig(
        sink,
        state,
        lambda rows: [json.loads(r[-1]) for r in rows],
        sink.log_foundry_counters,
        "dropped_events",
    )


class _Channel:
    def __init__(self, state: State) -> None:
        self._state = state

    def basic_publish(
        self, *, exchange: str, routing_key: str, body: bytes, properties: Any
    ) -> None:
        self._state.call(body)

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self, state: State) -> None:
        self._state = state

    def channel(self) -> _Channel:
        return _Channel(self._state)

    def close(self) -> None:
        pass


def rabbitmq(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.rabbitmq import RabbitMQSink

    _no_wait(mp)
    state = State()
    sink = RabbitMQSink(exchange="x", routing_key="k", connection=_Connection(state), max_retries=0)
    return Rig(
        sink, state, lambda body: [json.loads(body)], sink.log_foundry_counters, "dropped_events"
    )


class _Producer:
    def __init__(self, state: State) -> None:
        self._state = state

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        if self._state.down:
            self._state.log.append(("refused", None))
            raise BufferError("queue full")
        self._state.log.append(("handoff", (value, key)))

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        return 0


def kafka(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.kafka import KafkaSink

    state = State()
    sink = KafkaSink("topic", producer=_Producer(state))
    return Rig(sink, state, lambda vk: [json.loads(vk[0])], sink, "rejected")


class _EHBatch:
    def __init__(self, capacity: int) -> None:
        self.items: list[bytes] = []
        self._capacity = capacity

    def add(self, data: bytes) -> None:
        if len(self.items) >= self._capacity:
            raise ValueError("batch full")
        self.items.append(data)

    def __len__(self) -> int:
        return len(self.items)


class _EHProducer:
    def __init__(self, state: State, capacity: int) -> None:
        self._state = state
        self._capacity = capacity

    def create_batch(self) -> _EHBatch:
        return _EHBatch(self._capacity)

    def send_batch(self, batch: _EHBatch) -> None:
        self._state.call(list(batch.items))

    def close(self) -> None:
        pass


def eventhubs(mp: pytest.MonkeyPatch, tmp: Path, capacity: int = 1) -> Rig:
    from log_foundry.sinks.eventhubs import AzureEventHubsSink

    _no_wait(mp)
    mp.setattr("log_foundry.sinks.eventhubs._event_data_cls", lambda: lambda body: body)
    state = State()
    sink = AzureEventHubsSink(producer=_EHProducer(state, capacity), max_retries=0)
    return Rig(
        sink,
        state,
        lambda items: [json.loads(b) for b in items],
        sink.log_foundry_counters,
        "dropped_events",
    )


class _Kinesis:
    def __init__(self, state: State) -> None:
        self._state = state

    def put_records(self, *, StreamName: str, Records: list[dict[str, Any]]) -> dict[str, Any]:
        self._state.call([dict(r) for r in Records])
        return {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "1"} for _ in Records]}


def kinesis(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.kinesis import KinesisSink

    _no_wait(mp)
    state = State()
    sink = KinesisSink("s", client=_Kinesis(state), max_retries=0)
    return Rig(
        sink,
        state,
        lambda rs: [json.loads(r["Data"]) for r in rs],
        sink.log_foundry_counters,
        "dropped_events",
    )


class _Firehose:
    def __init__(self, state: State) -> None:
        self._state = state

    def put_record_batch(
        self, *, DeliveryStreamName: str, Records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        self._state.call([dict(r) for r in Records])
        return {"FailedPutCount": 0, "RequestResponses": [{"RecordId": "r"} for _ in Records]}


def firehose(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.firehose import FirehoseSink

    _no_wait(mp)
    state = State()
    sink = FirehoseSink("d", client=_Firehose(state), max_retries=0)
    return Rig(
        sink,
        state,
        lambda rs: [json.loads(r["Data"]) for r in rs],
        sink.log_foundry_counters,
        "dropped_events",
    )


class _SNS:
    def __init__(self, state: State) -> None:
        self._state = state

    def publish_batch(
        self, *, TopicArn: str, PublishBatchRequestEntries: list[dict[str, Any]]
    ) -> dict[str, Any]:
        self._state.call([dict(e) for e in PublishBatchRequestEntries])
        return {"Successful": [{"Id": e["Id"]} for e in PublishBatchRequestEntries], "Failed": []}


def sns(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.sns import SNSSink

    _no_wait(mp)
    state = State()
    sink = SNSSink("arn", client=_SNS(state), max_retries=0)
    return Rig(
        sink,
        state,
        lambda es: [json.loads(e["Message"]) for e in es],
        sink.log_foundry_counters,
        "dropped_events",
    )


class _SQS:
    def __init__(self, state: State) -> None:
        self._state = state

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:
        self._state.call([dict(e) for e in Entries])
        return {"Successful": [{"Id": e["Id"], "MessageId": "m"} for e in Entries], "Failed": []}


def sqs(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.sqs import SQSSink

    _no_wait(mp)
    state = State()
    sink = SQSSink("https://sqs/q", client=_SQS(state), max_retries=0)
    return Rig(
        sink,
        state,
        lambda es: [json.loads(e["MessageBody"]) for e in es],
        sink.log_foundry_counters,
        "dropped_events",
    )


class _Collection:
    def __init__(self, state: State) -> None:
        self._state = state

    def insert_many(self, documents: list[dict[str, Any]], ordered: bool = True) -> None:
        self._state.call([dict(d) for d in documents])


class _MongoClient:
    def __init__(self, state: State) -> None:
        self._collection = _Collection(state)

    def __getitem__(self, name: str) -> Any:
        return {"c": self._collection}

    def close(self) -> None:
        pass


def mongodb(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.mongodb import MongoDBSink

    _no_wait(mp)
    state = State()
    sink = MongoDBSink(client=_MongoClient(state), database="d", collection="c", max_retries=0)
    return Rig(sink, state, lambda docs: list(docs), sink.log_foundry_counters, "dropped_events")


class _Socket:
    def __init__(self, state: State) -> None:
        self._state = state

    def sendto(self, data: bytes, addr: tuple[str, int]) -> None:
        if self._state.error is not None:
            self._state.log.append(("refused", None))
            raise self._state.error
        if self._state.down:
            self._state.log.append(("refused", None))
            raise OSError(111, "Connection refused")
        self._state.log.append(("handoff", data))

    def sendall(self, data: bytes) -> None:
        self.sendto(data, ("", 0))

    def close(self) -> None:
        pass


def _syslog_event(frame: bytes) -> list[dict[str, Any]]:
    return [json.loads(frame.decode("utf-8").split(" - - ", 1)[1])]


def syslog(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    import log_foundry.sinks._socket as socket_mod
    from log_foundry.sinks.syslog import SyslogSink

    _no_wait(mp)
    state = State()
    mp.setattr(socket_mod, "_make_udp", lambda host: _Socket(state))
    sink = SyslogSink("loghost", 514, transport="udp", max_retries=0)
    return Rig(sink, state, _syslog_event, sink._socket, "_unencodable")


def logstash(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    import log_foundry.sinks._socket as socket_mod
    from log_foundry.sinks.logstash import LogstashSink

    _no_wait(mp)
    state = State()
    mp.setattr(socket_mod, "_make_udp", lambda host: _Socket(state))
    sink = LogstashSink(host="ls", port=5000, transport="udp", max_retries=0)
    return Rig(sink, state, lambda d: [json.loads(d)], sink._socket, "_unencodable")


class _Cursor:
    def __init__(self, state: State) -> None:
        self._state = state

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        pass

    def executemany(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        rows = [tuple(r) for r in rows]
        if any(_postgres_refuses(row) for row in rows):
            self._state.log.append(("refused", None))
            raise _UntranslatableCharacter("unsupported Unicode escape sequence")
        self._state.call(rows)


class _UntranslatableCharacter(Exception):
    """Stands in for psycopg's ``UntranslatableCharacter`` (SQLSTATE 22P05) and ``DataError``."""


def _holds_nul(value: object) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_holds_nul(k) or _holds_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_holds_nul(v) for v in value)
    return False


def _postgres_refuses(row: tuple[object, ...]) -> bool:
    """What PostgreSQL refuses, modelled without reading the sink's own detector.

    The JSONB parameter is parsed and walked for U+0000 in any string or key — a ``\\u0000``
    escape parses to one, the six characters written as text do not — and every string column
    is checked for U+0000, which psycopg refuses client-side.
    """
    *columns, document = row
    if any(_holds_nul(column) for column in columns):
        return True
    return isinstance(document, str) and _holds_nul(json.loads(document))


class _PGConnection:
    def __init__(self, state: State) -> None:
        self._state = state
        self.closed = False
        self.broken = False

    def cursor(self) -> _Cursor:
        return _Cursor(self._state)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def postgres(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.postgres import PostgresSink

    _no_wait(mp)
    state = State()
    sink = PostgresSink("t", connection=_PGConnection(state), max_retries=0)
    return Rig(
        sink,
        state,
        lambda rows: [json.loads(r[-1]) for r in rows],
        sink.log_foundry_counters,
        "dropped_events",
    )


class _Pipeline:
    def __init__(self, state: State) -> None:
        self._state = state
        self.ops: list[Any] = []

    def xadd(self, name: str, fields: dict[str, str], **options: Any) -> None:
        self.ops.append(fields["event"])

    def rpush(self, name: str, value: str) -> None:
        self.ops.append(value)

    def ltrim(self, name: str, start: int, end: int) -> None:
        pass

    def execute(self) -> list[Any]:
        self._state.call(list(self.ops))
        return []


class _Redis:
    def __init__(self, state: State) -> None:
        self._state = state

    def pipeline(self) -> _Pipeline:
        return _Pipeline(self._state)

    def close(self) -> None:
        pass


def _redis(mp: pytest.MonkeyPatch, cls_name: str) -> Rig:
    from log_foundry.sinks import redis as mod

    _no_wait(mp)
    state = State()
    sink = getattr(mod, cls_name)("k", client=_Redis(state), max_retries=0)
    return Rig(
        sink,
        state,
        lambda ops: [json.loads(o) for o in ops],
        sink.log_foundry_counters,
        "dropped_events",
    )


def redis_streams(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    return _redis(mp, "RedisStreamsSink")


def redis_list(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    return _redis(mp, "RedisListSink")


class _Future:
    def done(self) -> bool:
        return True

    def result(self, timeout: float | None = None) -> str:
        return "id"


class _Publisher:
    def __init__(self, state: State) -> None:
        self._state = state

    def publish(self, topic: str, data: bytes | None = None, **attrs: Any) -> _Future:
        self._state.call(data)
        return _Future()


def pubsub(mp: pytest.MonkeyPatch, tmp: Path) -> Rig:
    from log_foundry.sinks.pubsub import GooglePubSubSink

    state = State()
    sink = GooglePubSubSink("projects/p/topics/t", client=_Publisher(state))
    return Rig(sink, state, lambda data: [json.loads(data)], sink, "rejected")


RIGS: dict[str, Callable[[pytest.MonkeyPatch, Path], Rig]] = {
    "StdoutSink": stdout,
    "StderrSink": stderr,
    "FileSink": file,
    "RotatingFileSink": rotating,
    "SQLiteSink": sqlite,
    "ClickHouseSink": clickhouse,
    "RabbitMQSink": rabbitmq,
    "KafkaSink": kafka,
    "AzureEventHubsSink": eventhubs,
    "KinesisSink": kinesis,
    "FirehoseSink": firehose,
    "SNSSink": sns,
    "SQSSink": sqs,
    "MongoDBSink": mongodb,
    "SyslogSink": syslog,
    "LogstashSink": logstash,
    "PostgresSink": postgres,
    "RedisStreamsSink": redis_streams,
    "RedisListSink": redis_list,
    "GooglePubSubSink": pubsub,
}
"""Every public record-building sink on the helper, by class name."""

EIGHT = (
    "StdoutSink",
    "StderrSink",
    "FileSink",
    "RotatingFileSink",
    "ClickHouseSink",
    "RabbitMQSink",
    "KafkaSink",
    "AzureEventHubsSink",
)
"""The classes that handed earlier events over before an unencodable one raised (spec Overview)."""
