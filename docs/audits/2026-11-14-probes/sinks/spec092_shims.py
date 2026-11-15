"""SPEC-092 prototype: one client double per shipped sink, each writing through the row's backend.

Written on 2026-11-15 while authoring SPEC-092, against 6bd0b66e; not one of the audit's probes.
A double knows only its client's call and reply shapes. What it takes and how it answers is the
backend's fault script (`spec092_corpus.Backend.decide`); the double translates the action into
its client's own exception or reply. No extra is installed: every SDK sink gets an injected
client, as the audit's probes did.
"""

from __future__ import annotations

import errno
import io
import json
import logging
import sqlite3
import urllib.error
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from collections.abc import Callable

from spec092_corpus import Backend, tokens

REFUSED = ConnectionRefusedError(errno.ECONNREFUSED, "refused before anything was taken")


def _timeout() -> TimeoutError:
    return TimeoutError("the reply did not arrive")


# ------------------------------------------------------------------ keyed reply


class SQSClient:
    """`send_message_batch`, with a FIFO queue's 5-minute de-duplication when `fifo`."""

    def __init__(self, backend: Backend, *, fifo: bool = False) -> None:
        self.backend = backend
        self.fifo = fifo
        self.dedup: set[str] = set()

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        return _keyed(self.backend, Entries, "MessageBody", self._store)

    def _store(self, entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            key = entry.get("MessageDeduplicationId")
            if self.fifo and key is not None:
                if key in self.dedup:
                    continue
                self.dedup.add(key)
            self.backend.take(tokens(entry["MessageBody"]))


class SNSClient:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[Any]) -> Any:
        return _keyed(
            self.backend,
            PublishBatchRequestEntries,
            "Message",
            lambda es: [self.backend.take(tokens(e["Message"])) for e in es],
        )


def _keyed(
    backend: Backend, entries: list[dict[str, Any]], body: str, store: Callable[[list[Any]], Any]
) -> dict[str, Any]:
    carried = [t for e in entries for t in tokens(e[body])]
    action = backend.decide(carried)
    by_token = {tokens(e[body])[0]: e for e in entries}
    if action == "refuse":
        raise REFUSED
    if action == "noshape":
        store([by_token[t] for t in carried[: (len(carried) + 1) // 2]])
        return {}
    permanent = backend.permanent()
    failed: list[dict[str, Any]] = []
    taken: list[dict[str, Any]] = []
    retry: set[str] = set()
    if action == "partial":
        _, retry_list, _ = backend.partial_split(carried)
        retry = set(retry_list)
    half = carried[: (len(carried) + 1) // 2]
    for token in carried:
        entry = by_token[token]
        if token in permanent:
            failed.append({"Id": entry["Id"], "SenderFault": True, "Code": "InvalidParameterValue"})
        elif token in retry:
            failed.append({"Id": entry["Id"], "SenderFault": False, "Code": "ServiceUnavailable"})
        elif action == "taken-raise" and token not in half:
            continue
        else:
            taken.append(entry)
    store(taken)
    if action in ("taken-raise", "reply-lost"):
        raise _timeout()
    names = {f["Id"] for f in failed}
    return {
        "Successful": [{"Id": e["Id"], "MessageId": "m"} for e in entries if e["Id"] not in names],
        "Failed": failed,
    }


class MongoClient:
    """`insert_many(ordered=False)` with a unique `_id` the client stamps, as pymongo does."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.ids: set[object] = set()
        self.next_id = 0

    def __getitem__(self, name: str) -> Any:
        return {"c": self}

    def close(self) -> None:
        pass

    def insert_many(self, documents: list[dict[str, Any]], ordered: bool = True) -> Any:
        for document in documents:
            if "_id" not in document:
                self.next_id += 1
                document["_id"] = f"oid{self.next_id}"
        carried = [t for d in documents for t in tokens({k: v for k, v in d.items() if k != "_id"})]
        action = self.backend.decide(carried)
        if action == "refuse":
            raise REFUSED
        if action == "noshape":
            for document in documents[: (len(documents) + 1) // 2]:
                self.ids.add(document["_id"])
                self.backend.take(tokens({k: v for k, v in document.items() if k != "_id"}))
            raise _BulkWriteError({"writeErrors": "not a list"})
        permanent = self.backend.permanent()
        retry: set[str] = set()
        if action == "partial":
            retry = set(self.backend.partial_split(carried)[1])
        errors = []
        for index, document in enumerate(documents):
            token = tokens({k: v for k, v in document.items() if k != "_id"})[0]
            if action == "taken-raise" and index >= (len(documents) + 1) // 2:
                continue
            if token in permanent or token in retry:
                errors.append({"index": index, "code": 121, "errmsg": "validation"})
                continue
            if document["_id"] in self.ids:
                errors.append({"index": index, "code": 11000, "errmsg": "duplicate key"})
                continue
            self.ids.add(document["_id"])
            self.backend.take([token])
        if action in ("taken-raise", "reply-lost"):
            raise _timeout()
        if errors:
            raise _BulkWriteError({"writeErrors": errors})
        return object()


class _BulkWriteError(Exception):
    def __init__(self, details: dict[str, Any]) -> None:
        super().__init__("bulk write error")
        self.details = details


# ------------------------------------------------------------------ positional reply


def _positional(backend: Backend, carried: list[str]) -> tuple[str, list[bool]]:
    """The action and, per entry, whether it failed; the entries the backend took are taken."""
    action = backend.decide(carried)
    if action == "noshape":
        backend.take(carried[: (len(carried) + 1) // 2])
    if action in ("refuse", "noshape"):
        return action, []
    permanent = backend.permanent()
    retry: set[str] = set()
    if action == "partial":
        retry = set(backend.partial_split(carried)[1])
    failed = []
    for index, token in enumerate(carried):
        bad = token in permanent or token in retry
        if action == "taken-raise" and index >= (len(carried) + 1) // 2:
            failed.append(True)
            continue
        if not bad:
            backend.take([token])
        failed.append(bad)
    return action, failed


class KinesisClient:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def put_records(self, *, StreamName: str, Records: list[dict[str, Any]]) -> Any:
        carried = [tokens(r["Data"])[0] for r in Records]
        action, failed = _positional(self.backend, carried)
        if action == "refuse":
            raise REFUSED
        if action == "noshape":
            return {}
        if action in ("taken-raise", "reply-lost"):
            raise _timeout()
        return {
            "FailedRecordCount": sum(failed),
            "Records": [
                {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "x"}
                if bad
                else {"SequenceNumber": "1", "ShardId": "s"}
                for bad in failed
            ],
        }


class FirehoseClient:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def put_record_batch(self, *, DeliveryStreamName: str, Records: list[dict[str, Any]]) -> Any:
        carried = [tokens(r["Data"])[0] for r in Records]
        action, failed = _positional(self.backend, carried)
        if action == "refuse":
            raise REFUSED
        if action == "noshape":
            return {}
        if action in ("taken-raise", "reply-lost"):
            raise _timeout()
        return {
            "FailedPutCount": sum(failed),
            "RequestResponses": [
                {"ErrorCode": "ServiceUnavailableException", "ErrorMessage": "x"}
                if bad
                else {"RecordId": "r"}
                for bad in failed
            ],
        }


# ------------------------------------------------------------------ HTTP


class _Response:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body
        self.headers: dict[str, str] = {}

    def read(self, n: int = -1) -> bytes:
        return self._body if n < 0 else self._body[:n]

    def getcode(self) -> int:
        return self.status

    def close(self) -> None:
        pass


class Opener:
    """`opener(request, timeout)`: the body's events are the call; the reply is the class's.

    `reply` builds a 2xx body from the per-entry failures for a positional reader
    (Elasticsearch, Honeycomb), or answers the request as a whole.
    """

    def __init__(self, backend: Backend, reply: str = "whole") -> None:
        self.backend = backend
        self.reply = reply

    def __call__(self, request: Any, timeout: float | None = None) -> Any:
        carried = tokens(request.data or b"")
        if self.reply == "whole":
            action = self.backend.decide(carried)
            if action == "refuse":
                raise urllib.error.URLError(REFUSED)
            if action == "taken-raise":
                self.backend.take(carried[: (len(carried) + 1) // 2])
                raise _timeout()
            self.backend.take(carried)
            if action == "reply-lost":
                raise _timeout()
            return _Response(200, b"{}")
        action, failed = _positional(self.backend, carried)
        if action == "refuse":
            raise urllib.error.URLError(REFUSED)
        if action == "noshape":
            return _Response(200, b"{}")
        if action in ("taken-raise", "reply-lost"):
            raise _timeout()
        if self.reply == "bulk":
            items = [
                {"index": {"status": 429, "error": {"type": "es_rejected"}}}
                if bad
                else {"index": {"status": 201}}
                for bad in failed
            ]
            return _Response(200, json.dumps({"errors": any(failed), "items": items}).encode())
        statuses = [{"status": 429 if bad else 202} for bad in failed]
        return _Response(200, json.dumps(statuses).encode())


# ------------------------------------------------------------------ request reply: SDKs


class ClickHouseClient:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def insert(self, table: str, data: Any = None, column_names: Any = None) -> None:
        _whole(self.backend, tokens([list(r) for r in data]))

    def close(self) -> None:
        pass


def _whole(backend: Backend, carried: list[str]) -> None:
    """One call answered as a whole: raise, or take and return."""
    action = backend.decide(carried)
    if action == "refuse":
        raise REFUSED
    if action == "taken-raise":
        backend.take(carried[: (len(carried) + 1) // 2])
        raise _timeout()
    backend.take(carried)
    if action == "reply-lost":
        raise _timeout()


class EHBatch:
    def __init__(self) -> None:
        self.items: list[Any] = []

    def add(self, data: Any) -> None:
        if len(self.items) >= 100:
            raise ValueError("batch full")
        self.items.append(data)

    def __len__(self) -> int:
        return len(self.items)


class EHProducer:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def create_batch(self) -> EHBatch:
        return EHBatch()

    def send_batch(self, batch: EHBatch) -> None:
        _whole(self.backend, tokens(list(batch.items)))

    def close(self) -> None:
        pass


class RedisPipeline:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.ops: list[Any] = []

    def xadd(self, name: str, fields: dict[str, str], **options: Any) -> None:
        self.ops.append(fields)

    def rpush(self, name: str, value: str) -> None:
        self.ops.append(value)

    def ltrim(self, name: str, start: int, end: int) -> None:
        pass

    def execute(self) -> list[Any]:
        _whole(self.backend, tokens(self.ops))
        return []


class RedisClient:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def pipeline(self) -> RedisPipeline:
        return RedisPipeline(self.backend)

    def close(self) -> None:
        pass


# ------------------------------------------------------------------ transactional


class PGCursor:
    def __init__(self, conn: PGConnection) -> None:
        self.conn = conn

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        pass

    def executemany(self, sql: str, rows: list[Any]) -> None:
        carried = tokens([list(r) for r in rows])
        action = self.conn.backend.decide(carried)
        if self.conn.autocommit:
            if self.conn.backend.fault == "autocommit" and self.conn.statements == 1:
                self.conn.statements += 1
                raise _timeout()
            self.conn.statements += 1
            self.conn.backend.take(carried)
            return
        self.conn.statements += 1
        if action == "refuse":
            raise REFUSED
        if action == "taken-raise":
            self.conn.txn.extend(carried[: (len(carried) + 1) // 2])
            raise _timeout()
        self.conn.txn.extend(carried)
        if action == "reply-lost":
            self.conn.lose_commit_reply = True


class PGConnection:
    """A psycopg-shaped connection whose transaction is real: a rollback discards its rows."""

    def __init__(self, backend: Backend, *, autocommit: bool = False) -> None:
        self.backend = backend
        self.autocommit = autocommit
        self.txn: list[str] = []
        self.closed = False
        self.broken = False
        self.statements = 0
        self.lose_commit_reply = False

    def cursor(self) -> PGCursor:
        return PGCursor(self)

    def commit(self) -> None:
        self.backend.take(self.txn)
        self.txn = []
        if self.lose_commit_reply:
            self.lose_commit_reply = False
            raise ConnectionResetError(errno.ECONNRESET, "the connection dropped after COMMIT")

    def rollback(self) -> None:
        self.txn = []

    def close(self) -> None:
        self.closed = True


class SQLiteProxy:
    """A real in-memory SQLite connection; the backend reads what it holds after the row."""

    def __init__(self, backend: Backend, *, autocommit: bool = False) -> None:
        self.backend = backend
        self.real = sqlite3.connect(
            ":memory:", check_same_thread=False, isolation_level=None if autocommit else ""
        )
        self.faulted = False
        backend.on_close.append(self.count)

    def __enter__(self) -> Any:
        return self.real.__enter__()

    def __exit__(self, *exc: object) -> Any:
        return self.real.__exit__(*exc)

    def executemany(self, sql: str, rows: Any) -> Any:
        rows = list(rows)
        carried = tokens([list(r) for r in rows])
        fault = self.backend.fault
        action = self.backend.decide(carried, knowable=True)
        if action == "refuse":
            raise sqlite3.OperationalError("database is locked")
        if (action == "taken-raise" or fault == "autocommit") and not self.faulted:
            self.faulted = True
            self.real.executemany(sql, rows[: (len(rows) + 1) // 2])
            raise sqlite3.OperationalError("database is locked")
        return self.real.executemany(sql, rows)

    def count(self) -> None:
        with_rows = self.real.execute("SELECT event FROM log_events").fetchall()
        for (event,) in with_rows:
            self.backend.take(tokens(event))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.real, name)


# ------------------------------------------------------------------ local write


class FileHandle:
    """The file sinks' unbuffered handle: `write(bytes)` returns what it took."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.partial_done = False

    def write(self, data: bytes) -> int:
        carried = tokens(data)
        action = self.backend.decide(carried, knowable=True)
        if action == "refuse":
            raise OSError(errno.EAGAIN, "Resource temporarily unavailable")
        if action == "taken-raise" and not self.partial_done:
            self.partial_done = True
            lines = bytes(data).splitlines(keepends=True)
            kept = b"".join(lines[: (len(lines) + 1) // 2])
            self.backend.take(tokens(kept))
            return len(kept)
        if action == "taken-raise":
            raise OSError(errno.EAGAIN, "Resource temporarily unavailable")
        self.backend.take(carried)
        return len(data)

    def close(self) -> None:
        pass

    def flush(self) -> None:
        pass


class Stream:
    """A text stream: each `write` lands, or the call raises having taken nothing."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.writes = 0

    def write(self, data: str) -> int:
        carried = tokens(data)
        if not carried:
            return len(data)
        action = self.backend.decide(carried)
        if action == "refuse":
            self.backend.calls[-1].knowable = False
            raise OSError(errno.EAGAIN, "Resource temporarily unavailable")
        if action == "taken-raise":
            self.backend.take(carried)
            raise OSError(errno.EIO, "the stream took the line, then failed")
        self.backend.take(carried)
        return len(data)

    def flush(self) -> None:
        pass

    def fileno(self) -> int:
        raise io.UnsupportedOperation("no descriptor")


# ------------------------------------------------------------------ sockets


class TCPSocket:
    """`sendall`: a frame lands unless the peer closed, when the kernel takes it anyway."""

    def __init__(self, backend: Backend, state: dict[str, int]) -> None:
        self.backend = backend
        self.state = state

    def sendall(self, data: bytes) -> None:
        carried = tokens(data)
        if self.backend.peer_closed and self.state["generation"] == 0:
            self.backend.decide(carried, knowable=False)
            self.state["blind"] += 1
            if self.state["blind"] > 3:
                raise BrokenPipeError(errno.EPIPE, "Broken pipe")
            return
        action = self.backend.decide(carried)
        if action == "refuse":
            raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")
        self.backend.take(carried)
        if action == "taken-raise":
            raise _timeout()

    def sendto(self, data: bytes, addr: Any) -> None:
        self.sendall(data)

    def settimeout(self, value: float | None) -> None:
        pass

    def close(self) -> None:
        self.state["generation"] += 1


class UDPSocket:
    """`sendto`: a datagram lands, or with no listener it is gone and nothing says so."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def sendto(self, data: bytes, addr: Any) -> None:
        carried = tokens(data)
        if self.backend.peer_closed:
            self.backend.decide(carried, knowable=False)
            return
        action = self.backend.decide(carried)
        if action == "refuse":
            raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")
        self.backend.take(carried)

    def close(self) -> None:
        pass


class RabbitChannel:
    def __init__(self, backend: Backend, state: dict[str, int]) -> None:
        self.backend = backend
        self.state = state

    def basic_publish(
        self, *, exchange: str, routing_key: str, body: bytes, properties: Any
    ) -> None:
        TCPSocket(self.backend, self.state).sendall(body)

    def close(self) -> None:
        self.state["generation"] += 1


class RabbitConnection:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.state = {"generation": 0, "blind": 0}
        self.is_open = True

    def channel(self) -> RabbitChannel:
        return RabbitChannel(self.backend, self.state)

    def close(self) -> None:
        pass


# ------------------------------------------------------------------ NATS


class NATSJetStream:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    async def publish(self, subject: str, payload: bytes, *, timeout: float | None = None) -> None:  # noqa: ASYNC109
        carried = tokens(payload)
        action = self.backend.decide(carried)
        if action == "refuse":
            raise REFUSED
        self.backend.take(carried)
        if action == "reply-lost":
            raise TimeoutError("ack wait expired")


class NATSClient:
    """Core publishes buffer in the client; `flush` is the PING the server answers with a PONG."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.is_connected = True
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}
        self.buffer: list[str] = []

    async def publish(self, subject: str, payload: bytes) -> None:
        carried = tokens(payload)
        action = self.backend.decide(carried, knowable=self.backend.fault != "never-acknowledged")
        if action == "refuse":
            raise REFUSED
        if self.backend.fault != "never-acknowledged":
            self.backend.take(carried)

    def jetstream(self) -> NATSJetStream:
        return NATSJetStream(self.backend)

    async def flush(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        if self.backend.fault == "never-acknowledged" and self.backend.calls:
            raise TimeoutError("no PONG")

    async def close(self) -> None:
        return None

    async def drain(self) -> None:
        return None


# ------------------------------------------------------------------ client-buffered


class KafkaError:
    def code(self) -> int:
        return -192

    def str(self) -> str:
        return "Local: Message timed out"

    def __str__(self) -> str:
        return "Local: Message timed out"


class KafkaProducer:
    """Buffers on `produce`; the delivery report for each message runs at `poll` or `flush`."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.queue: list[tuple[list[str], Any]] = []

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        carried = tokens(value)
        action = self.backend.decide(carried, knowable=True)
        if action == "refuse":
            raise BufferError("Local: Queue full")
        self.queue.append((carried, callback))

    def poll(self, timeout: float = 0) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        queue, self.queue = self.queue, []
        for carried, callback in queue:
            if self.backend.fault == "never-acknowledged":
                if callback is not None:
                    callback(KafkaError(), None)
            else:
                self.backend.take(carried)
                if callback is not None:
                    callback(None, None)
        return 0

    def __len__(self) -> int:
        return len(self.queue)


class PubSubFuture:
    def __init__(self, backend: Backend, carried: list[str]) -> None:
        self.backend = backend
        self.carried = carried
        self.settled: bool | None = None

    def done(self) -> bool:
        return self.settled is not None

    def result(self, timeout: float | None = None) -> str:
        if self.settled is None:
            self.settled = self.backend.fault != "never-acknowledged"
            if self.settled:
                self.backend.take(self.carried)
        if not self.settled:
            raise RuntimeError("the publish failed after the client took it")
        return "id"

    def exception(self, timeout: float | None = None) -> BaseException | None:
        try:
            self.result()
        except Exception as err:
            return err
        return None


class PubSubPublisher:
    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def publish(self, topic: str, data: bytes | None = None, **attrs: Any) -> PubSubFuture:
        carried = tokens(data)
        action = self.backend.decide(carried, knowable=True)
        if action == "refuse":
            raise REFUSED
        return PubSubFuture(self.backend, carried)


class SentrySDK:
    """`capture_event` returns the event id, or `None` when the SDK discarded the event."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend

    def capture_event(self, event: Any) -> Any:
        carried = tokens(event)
        action = self.backend.decide(carried, knowable=True)
        if action == "refuse":
            raise REFUSED
        if self.backend.fault == "never-acknowledged":
            return None
        self.backend.take(carried)
        return "event-id"

    def flush(self, timeout: float | None = None) -> None:
        pass


# ------------------------------------------------------------------ in-process


def callback(backend: Backend) -> Callable[[list[dict[str, object]]], None]:
    def fn(batch: list[dict[str, object]]) -> None:
        carried = tokens(batch)
        action = backend.decide(carried)
        if action == "refuse":
            raise REFUSED
        if action == "taken-raise":
            backend.take(carried[: (len(carried) + 1) // 2])
            raise RuntimeError("the callable kept half, then raised")
        backend.take(carried)

    return fn


class Handler(logging.Handler):
    """A handler shaped as the stdlib's are: a failure inside `emit` goes to `handleError`."""

    def __init__(self, backend: Backend) -> None:
        super().__init__()
        self.backend = backend

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._keep(record)
        except Exception:
            self.handleError(record)

    def _keep(self, record: logging.LogRecord) -> None:
        carried = tokens(json.dumps(record.__dict__, default=repr))
        action = self.backend.decide(carried)
        if action == "refuse":
            raise REFUSED
        self.backend.take(carried)
        if action == "taken-raise":
            raise RuntimeError("the handler kept the record, then raised")

    def handleError(self, record: logging.LogRecord) -> None:
        """The stdlib default prints a traceback to stderr and returns; this returns quietly."""


def logger(backend: Backend) -> logging.Logger:
    log = logging.getLogger(f"spec092.{id(backend)}")
    log.handlers[:] = [Handler(backend)]
    log.propagate = False
    log.setLevel(logging.DEBUG)
    return log
