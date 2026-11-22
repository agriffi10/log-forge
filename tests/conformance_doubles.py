"""SPEC-092's client doubles, driver stubs and variant builders: every shipped sink on a fake backend.

A helper, not collected. A double knows only its client's call and reply shapes: on each client
call it asks the row's `Backend` for an action, takes the events the action says, and answers in
its client's own exception or reply. What "taken" means is the backend's alone. No row needs an
extra: each SDK, driver and socket sink gets an injected double, or a patched seam where it
builds its own client (`_socket._make_tcp`/`_make_udp`, `eventhubs._event_data_cls`, a `nats`
module, stub `psycopg` and `pika` modules). Where a sink's retry predicate reads driver classes
through `_classify.driver_classes`, which reads `sys.modules`, a stub of the driver module is
installed for the row and the double raises its class. Every seam is set through the row's
`MonkeyPatch`, so a renamed seam fails the row rather than leaving it unpatched.
"""

from __future__ import annotations

import errno
import importlib
import io
import itertools
import json
import logging
import shutil
import sqlite3
import sys
import tempfile
import types
import urllib.error
from collections.abc import Callable, Mapping
from typing import Any, Self

import pytest

from conformance_backend import Backend, tokens
from conformance_classes import Row, Variant

REFUSED = ConnectionRefusedError(errno.ECONNREFUSED, "refused before anything was taken")


def _timeout() -> TimeoutError:
    """The builtin a client raises when a reply never arrives.

    Args:
      None.

    Returns:
      The error.

    Raises:
      None.
    """
    return TimeoutError("the reply did not arrive")


DRIVERS: dict[str, dict[str, tuple[str, ...]]] = {
    "botocore.exceptions": {
        "BotoCoreError": (),
        "ConnectionError": ("BotoCoreError",),
        "HTTPClientError": ("BotoCoreError",),
        "EndpointConnectionError": ("ConnectionError",),
        "ReadTimeoutError": ("HTTPClientError",),
        "ClientError": (),
    },
    "clickhouse_connect.driver.exceptions": {"Error": (), "OperationalError": ("Error",)},
    "azure.eventhub.exceptions": {
        "EventHubError": (),
        "ConnectError": ("EventHubError",),
        "AuthenticationError": ("ConnectError",),
        "ConnectionLostError": ("EventHubError",),
        "OperationTimeoutError": ("EventHubError",),
    },
    "pymongo.errors": {
        "PyMongoError": (),
        "ConnectionFailure": ("PyMongoError",),
        "AutoReconnect": ("ConnectionFailure",),
        "NetworkTimeout": ("AutoReconnect",),
        "BulkWriteError": ("PyMongoError",),
    },
    "pika.exceptions": {
        "AMQPError": (),
        "AMQPConnectionError": ("AMQPError",),
        "AuthenticationError": ("AMQPConnectionError",),
        "StreamLostError": ("AMQPConnectionError",),
        "ConnectionWrongStateError": ("AMQPConnectionError",),
        "ChannelWrongStateError": ("AMQPError",),
        "UnroutableError": ("AMQPError",),
        "NackError": ("AMQPError",),
        "ChannelClosedByBroker": ("AMQPError",),
    },
    "google.api_core.exceptions": {
        "GoogleAPIError": (),
        "GoogleAPICallError": ("GoogleAPIError",),
        "ClientError": ("GoogleAPICallError",),
        "BadRequest": ("ClientError",),
        "InvalidArgument": ("BadRequest",),
        "Forbidden": ("ClientError",),
        "PermissionDenied": ("Forbidden",),
        "ServerError": ("GoogleAPICallError",),
        "GatewayTimeout": ("ServerError",),
        "DeadlineExceeded": ("GatewayTimeout",),
    },
    "psycopg.errors": {"Error": (), "OperationalError": ("Error",)},
    "redis.exceptions": {
        "RedisError": (),
        "ConnectionError": ("RedisError",),
        "TimeoutError": ("RedisError",),
    },
}
"""Each driver module a sink's retry predicate reads, with the classes the doubles raise: module,
then class, then its bases, in an order where a base is declared before its subclass. No class
subclasses a builtin other than `Exception`, so a raise reaches the sink's driver-class branch
rather than `TRANSIENT_BUILTINS`."""


class _Params:
    """Stands in for pika's parameter classes: keeps whatever it is given."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Keeps whatever it is given, as pika's parameter classes do.

        Args:
          *args: Ignored.
          **kwargs: Kept as attributes.

        Returns:
          None.

        Raises:
          None.
        """
        self.blocked_connection_timeout = None
        vars(self).update(kwargs)


def install_drivers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Puts a stub of every `DRIVERS` module, and `pika` and `psycopg`, in `sys.modules`.

    In place of a real driver too, since the doubles are fakes either way, so a row reads
    the same with every extra installed as with none.

    Args:
      monkeypatch: The row's patch, which restores each entry.

    Returns:
      None.

    Raises:
      None.
    """
    for module_name, classes in DRIVERS.items():
        module = types.ModuleType(module_name)
        for cls_name, bases in classes.items():
            parents = tuple(getattr(module, b) for b in bases) or (Exception,)
            setattr(module, cls_name, type(cls_name, parents, {"__module__": module_name}))
        monkeypatch.setitem(sys.modules, module_name, module)
    pika = types.ModuleType("pika")
    pika.exceptions = sys.modules["pika.exceptions"]  # type: ignore[attr-defined]
    pika.URLParameters = _Params  # type: ignore[attr-defined]
    pika.ConnectionParameters = _Params  # type: ignore[attr-defined]
    pika.BasicProperties = _Params  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pika", pika)
    psycopg = types.ModuleType("psycopg")
    psycopg.errors = sys.modules["psycopg.errors"]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg", psycopg)


def driver_error(module: str, name: str, message: str, **attrs: Any) -> Exception:
    """An instance of the driver class `module.name`, as installed for this row.

    Args:
      module: The driver module.
      name: The class.
      message: Its message.
      **attrs: Attributes set on the instance, such as `sqlstate`.

    Returns:
      The exception, not raised.

    Raises:
      KeyError: When the module is not installed.
    """
    err: Exception = getattr(sys.modules[module], name)(message)
    vars(err).update(attrs)
    return err


# Keyed reply.


def _aws_refused() -> Exception:
    """botocore's error for a connection that was refused before anything was sent.

    Args:
      None.

    Returns:
      The exception, not raised.

    Raises:
      None.
    """
    return driver_error("botocore.exceptions", "EndpointConnectionError", "could not connect")


def _aws_timeout() -> Exception:
    """botocore's error for a reply that never arrived.

    Args:
      None.

    Returns:
      The exception, not raised.

    Raises:
      None.
    """
    return driver_error("botocore.exceptions", "ReadTimeoutError", "read timed out")


class SQSClient:
    """`send_message_batch`, with a FIFO queue's de-duplication when `fifo`."""

    def __init__(self, backend: Backend, *, fifo: bool = False) -> None:
        """Keeps the backend, and a FIFO queue's de-duplication ids when `fifo`.

        Args:
          backend: The row's backend.
          fifo: Whether the queue de-duplicates.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.fifo = fifo
        self.dedup: set[str] = set()

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, Any]]) -> Any:
        """One batch call, answered with `Successful` and `Failed` by entry id.

        Args:
          QueueUrl: The queue.
          Entries: The entries.

        Returns:
          The reply.

        Raises:
          Exception: botocore's refusal or timeout, as the fault script says.
        """
        return _keyed(self.backend, Entries, "MessageBody", self._store)

    def _store(self, entries: list[dict[str, Any]]) -> None:
        """Takes each entry, once per de-duplication id on a FIFO queue.

        Args:
          entries: The entries the call took.

        Returns:
          None.

        Raises:
          None.
        """
        for entry in entries:
            key = entry.get("MessageDeduplicationId")
            if self.fifo and key is not None:
                if key in self.dedup:
                    continue
                self.dedup.add(key)
            self.backend.take(tokens(entry["MessageBody"]))


class SNSClient:
    """`publish_batch`, answered as SQS's batch call is."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[Any]) -> Any:
        """One batch call, answered as SQS's batch call is.

        Args:
          TopicArn: The topic.
          PublishBatchRequestEntries: The entries.

        Returns:
          The reply.

        Raises:
          Exception: botocore's refusal or timeout, as the fault script says.
        """
        return _keyed(
            self.backend,
            PublishBatchRequestEntries,
            "Message",
            lambda es: [self.backend.take(tokens(e["Message"])) for e in es],
        )


def _keyed(
    backend: Backend, entries: list[dict[str, Any]], body: str, store: Callable[[list[Any]], Any]
) -> dict[str, Any]:
    """One AWS batch call: a reply naming each failed entry by id, or a raise.

    Args:
      backend: The row's backend.
      entries: The call's entries.
      body: The key of each entry's payload.
      store: Takes the entries the backend keeps.

    Returns:
      The reply: `Successful` and `Failed`, or `{}` under `no-verdict-shape`.

    Raises:
      Exception: botocore's refusal or timeout, as the fault script says.
    """
    carried = [t for e in entries for t in tokens(e[body])]
    action = backend.decide(carried)
    by_token = {tokens(e[body])[0]: e for e in entries}
    if action == "refuse":
        backend.apply()
        raise _aws_refused()
    if action == "noshape":
        backend.apply()
        store([by_token[t] for t in carried[: (len(carried) + 1) // 2]])
        return {}
    permanent = backend.permanent()
    failed: list[dict[str, Any]] = []
    taken: list[dict[str, Any]] = []
    retry: set[str] = set()
    if action == "partial":
        backend.apply()
        retry = set(backend.partial_split(carried)[1])
    half = carried[: (len(carried) + 1) // 2]
    for token in carried:
        entry = by_token[token]
        if token in permanent:
            failed.append({"Id": entry["Id"], "SenderFault": True, "Code": "InvalidParameterValue"})
        elif token in retry:
            failed.append({"Id": entry["Id"], "SenderFault": False, "Code": "ServiceUnavailable"})
        elif action == "taken-raise" and token not in half:
            backend.apply()
            continue
        else:
            taken.append(entry)
    store(taken)
    if action in ("taken-raise", "reply-lost"):
        backend.apply()
        raise _aws_timeout()
    names = {f["Id"] for f in failed}
    return {
        "Successful": [{"Id": e["Id"], "MessageId": "m"} for e in entries if e["Id"] not in names],
        "Failed": failed,
    }


class MongoClient:
    """`insert_many(ordered=False)` with a unique `_id` the client stamps, as pymongo does."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend and the `_id`s the collection holds.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.ids: set[object] = set()
        self.next_id = 0

    def __getitem__(self, name: str) -> Any:
        """The database; its one collection is this double.

        Args:
          name: The database's name.

        Returns:
          A mapping from the collection's name to this double.

        Raises:
          None.
        """
        return {"c": self}

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def insert_many(self, documents: list[dict[str, Any]], ordered: bool = True) -> Any:
        """One insert; failures arrive as a `BulkWriteError` whose `writeErrors` name an index.

        Args:
          documents: The documents, each stamped with an `_id` first.
          ordered: Ignored.

        Returns:
          An insert result.

        Raises:
          Exception: pymongo's `AutoReconnect`, `NetworkTimeout` or `BulkWriteError`.
        """
        for document in documents:
            if "_id" not in document:
                self.next_id += 1
                document["_id"] = f"oid{self.next_id}"

        def body(document: dict[str, Any]) -> list[str]:
            """The tokens a document carries, its `_id` left out.

            Args:
              document: A document.

            Returns:
              The tokens.

            Raises:
              None.
            """
            return tokens({k: v for k, v in document.items() if k != "_id"})

        carried = [t for d in documents for t in body(d)]
        action = self.backend.decide(carried)
        if action == "refuse":
            self.backend.apply()
            raise driver_error("pymongo.errors", "AutoReconnect", "connection refused")
        if action == "noshape":
            self.backend.apply()
            for document in documents[: (len(documents) + 1) // 2]:
                self.ids.add(document["_id"])
                self.backend.take(body(document))
            raise _bulk_error({"writeErrors": "not a list"})
        permanent = self.backend.permanent()
        retry: set[str] = set()
        if action == "partial":
            self.backend.apply()
            retry = set(self.backend.partial_split(carried)[1])
        errors = []
        for index, document in enumerate(documents):
            token = body(document)[0]
            if action == "taken-raise" and index >= (len(documents) + 1) // 2:
                self.backend.apply()
                continue
            if token in permanent or token in retry:
                errors.append({"index": index, "code": 121, "errmsg": "validation"})
                continue
            if document["_id"] in self.ids:
                errors.append(_duplicate_id(index, document["_id"]))
                continue
            self.ids.add(document["_id"])
            self.backend.take([token])
        if action in ("taken-raise", "reply-lost"):
            self.backend.apply()
            raise driver_error("pymongo.errors", "NetworkTimeout", "timed out")
        if errors:
            raise _bulk_error({"writeErrors": errors})
        return object()


def _duplicate_id(index: int, value: object) -> dict[str, Any]:
    """A `writeErrors` entry in the server's shape for a duplicate `_id` (SPEC-095).

    MongoDB names the index both in `keyPattern` and in the `errmsg` text, and gives the
    duplicate value in `keyValue`.

    Args:
      index: The document's position in the insert.
      value: Its `_id`.

    Returns:
      The entry.

    Raises:
      None.
    """
    return {
        "index": index,
        "code": 11000,
        "errmsg": (
            f"E11000 duplicate key error collection: d.c index: _id_ dup key: {{ _id: {value} }}"
        ),
        "keyPattern": {"_id": 1},
        "keyValue": {"_id": value},
    }


def _bulk_error(details: dict[str, Any]) -> Exception:
    """pymongo's `BulkWriteError` with its `details`.

    Args:
      details: The error's details.

    Returns:
      The exception, not raised.

    Raises:
      None.
    """
    return driver_error("pymongo.errors", "BulkWriteError", "bulk write error", details=details)


# Positional reply.


def _positional(backend: Backend, carried: list[str]) -> tuple[str, list[bool]]:
    """The action and, per entry, whether it failed; the entries the backend took are taken.

    Args:
      backend: The row's backend.
      carried: The call's tokens, one per entry.

    Returns:
      The action, and one failure flag per entry (none on a refusal or no verdict).

    Raises:
      None.
    """
    action = backend.decide(carried)
    if action == "noshape":
        backend.apply()
        backend.take(carried[: (len(carried) + 1) // 2])
    if action in ("refuse", "noshape"):
        backend.apply()
        return action, []
    permanent = backend.permanent()
    retry: set[str] = set()
    if action == "partial":
        backend.apply()
        retry = set(backend.partial_split(carried)[1])
    failed = []
    for index, token in enumerate(carried):
        bad = token in permanent or token in retry
        if action == "taken-raise" and index >= (len(carried) + 1) // 2:
            backend.apply()
            failed.append(True)
            continue
        if not bad:
            backend.take([token])
        failed.append(bad)
    return action, failed


class KinesisClient:
    """`put_records`, answered with a failed count and one result per record."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def put_records(self, *, StreamName: str, Records: list[dict[str, Any]]) -> Any:
        """One batch call, answered with a failed count and one result per record.

        Args:
          StreamName: The stream.
          Records: The records.

        Returns:
          The reply.

        Raises:
          Exception: botocore's refusal or timeout, as the fault script says.
        """
        carried = [tokens(r["Data"])[0] for r in Records]
        action, failed = _positional(self.backend, carried)
        if action == "refuse":
            self.backend.apply()
            raise _aws_refused()
        if action == "noshape":
            self.backend.apply()
            return {}
        if action in ("taken-raise", "reply-lost"):
            self.backend.apply()
            raise _aws_timeout()
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
    """`put_record_batch`, answered with a failed count and one response per record."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def put_record_batch(self, *, DeliveryStreamName: str, Records: list[dict[str, Any]]) -> Any:
        """One batch call, answered with a failed count and one response per record.

        Args:
          DeliveryStreamName: The stream.
          Records: The records.

        Returns:
          The reply.

        Raises:
          Exception: botocore's refusal or timeout, as the fault script says.
        """
        carried = [tokens(r["Data"])[0] for r in Records]
        action, failed = _positional(self.backend, carried)
        if action == "refuse":
            self.backend.apply()
            raise _aws_refused()
        if action == "noshape":
            self.backend.apply()
            return {}
        if action in ("taken-raise", "reply-lost"):
            self.backend.apply()
            raise _aws_timeout()
        return {
            "FailedPutCount": sum(failed),
            "RequestResponses": [
                {"ErrorCode": "ServiceUnavailableException", "ErrorMessage": "x"}
                if bad
                else {"RecordId": "r"}
                for bad in failed
            ],
        }


# HTTP.


class _Response:
    """An HTTP response as `urlopen` returns one."""

    def __init__(self, status: int, body: bytes) -> None:
        """Keeps the status and body.

        Args:
          status: The HTTP status.
          body: The body.

        Returns:
          None.

        Raises:
          None.
        """
        self.status = status
        self._body = body
        self.headers: dict[str, str] = {}

    def read(self, n: int = -1) -> bytes:
        """The body, or its first `n` bytes.

        Args:
          n: How many bytes, or -1 for all.

        Returns:
          The bytes.

        Raises:
          None.
        """
        return self._body if n < 0 else self._body[:n]

    def getcode(self) -> int:
        """The status.

        Args:
          None.

        Returns:
          The HTTP status.

        Raises:
          None.
        """
        return self.status

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


class Opener:
    """`opener(request, timeout)`: the body's events are the call; the reply is the class's.

    `reply` builds a 2xx body from the per-entry failures for a positional reader
    (`"bulk"` for Elasticsearch and OpenSearch, `"statuses"` for Honeycomb), or answers the
    request as a whole (`"whole"`).
    """

    def __init__(self, backend: Backend, reply: str = "whole") -> None:
        """Keeps the row's backend and the reply shape.

        Args:
          backend: The row's backend.
          reply: `"whole"`, `"bulk"` or `"statuses"`.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.reply = reply

    def __call__(self, request: Any, timeout: float | None = None) -> Any:
        """One request: the body's events are the call, and the reply is the class's.

        Args:
          request: The request.
          timeout: Ignored.

        Returns:
          A response.

        Raises:
          Exception: `URLError` on a refusal, `TimeoutError` for a lost reply.
        """
        carried = tokens(request.data or b"")
        if self.reply == "whole":
            action = self.backend.decide(carried)
            if action == "refuse":
                self.backend.apply()
                raise urllib.error.URLError(REFUSED)
            if action == "taken-raise":
                self.backend.apply()
                self.backend.take(carried[: (len(carried) + 1) // 2])
                raise _timeout()
            self.backend.take(carried)
            if action == "reply-lost":
                self.backend.apply()
                raise _timeout()
            return _Response(200, b"{}")
        action, failed = _positional(self.backend, carried)
        if action == "refuse":
            self.backend.apply()
            raise urllib.error.URLError(REFUSED)
        if action == "noshape":
            self.backend.apply()
            return _Response(200, b"{}")
        if action in ("taken-raise", "reply-lost"):
            self.backend.apply()
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


# Request reply through an SDK.


WHOLE_ERRORS: Mapping[str, tuple[tuple[str, str], tuple[str, str]]] = {
    "clickhouse": (
        ("clickhouse_connect.driver.exceptions", "OperationalError"),
        ("clickhouse_connect.driver.exceptions", "OperationalError"),
    ),
    "eventhubs": (
        ("azure.eventhub.exceptions", "ConnectError"),
        ("azure.eventhub.exceptions", "OperationTimeoutError"),
    ),
    "redis": (("redis.exceptions", "ConnectionError"), ("redis.exceptions", "TimeoutError")),
}
"""Per driver, the class a refused call raises and the class a lost reply raises."""


def _whole(backend: Backend, carried: list[str], driver: str) -> None:
    """One call answered as a whole: raise, or take and return.

    Args:
      backend: The row's backend.
      carried: The call's tokens.
      driver: The key of `WHOLE_ERRORS`.

    Returns:
      None.

    Raises:
      Exception: The driver's refusal or timeout, as the fault script says.
    """
    refused, timed_out = WHOLE_ERRORS[driver]
    action = backend.decide(carried)
    if action == "refuse":
        backend.apply()
        raise driver_error(*refused, "connection refused")
    if action == "taken-raise":
        backend.apply()
        backend.take(carried[: (len(carried) + 1) // 2])
        raise driver_error(*timed_out, "timed out")
    backend.take(carried)
    if action == "reply-lost":
        backend.apply()
        raise driver_error(*timed_out, "timed out")


class ClickHouseClient:
    """clickhouse-connect's `insert`."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def insert(self, table: str, data: Any = None, column_names: Any = None) -> None:
        """One insert, answered as a whole.

        Args:
          table: The table.
          data: The rows.
          column_names: Ignored.

        Returns:
          None.

        Raises:
          Exception: clickhouse-connect's `OperationalError`, as the fault script says.
        """
        _whole(self.backend, tokens([list(r) for r in data]), "clickhouse")

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


class EHBatch:
    """An Event Hubs `EventDataBatch`."""

    def __init__(self) -> None:
        """Starts empty.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.items: list[Any] = []

    def add(self, data: Any) -> None:
        """Adds one event, refusing past a hundred as a full batch does.

        Args:
          data: The event.

        Returns:
          None.

        Raises:
          ValueError: When the batch is full.
        """
        if len(self.items) >= 100:
            raise ValueError("batch full")
        self.items.append(data)

    def __len__(self) -> int:
        """How many events it holds.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return len(self.items)


class EHProducer:
    """An Event Hubs producer client."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def create_batch(self) -> EHBatch:
        """A new, empty batch.

        Args:
          None.

        Returns:
          The batch.

        Raises:
          None.
        """
        return EHBatch()

    def send_batch(self, batch: EHBatch) -> None:
        """One send, answered as a whole.

        Args:
          batch: The batch.

        Returns:
          None.

        Raises:
          Exception: Event Hubs' `ConnectError` or `OperationTimeoutError`.
        """
        _whole(self.backend, tokens(list(batch.items)), "eventhubs")

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


class RedisPipeline:
    """A redis-py pipeline: commands queue, and `execute` is the one call."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend and the queued commands.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.ops: list[Any] = []

    def xadd(self, name: str, fields: dict[str, str], **options: Any) -> None:
        """Queues one stream entry.

        Args:
          name: The stream.
          fields: The entry.
          **options: Ignored.

        Returns:
          None.

        Raises:
          None.
        """
        self.ops.append(fields)

    def rpush(self, name: str, value: str) -> None:
        """Queues one list push.

        Args:
          name: The list.
          value: The value.

        Returns:
          None.

        Raises:
          None.
        """
        self.ops.append(value)

    def ltrim(self, name: str, start: int, end: int) -> None:
        """Queues a trim, which carries no event.

        Args:
          name: The list.
          start: Ignored.
          end: Ignored.

        Returns:
          None.

        Raises:
          None.
        """

    def execute(self) -> list[Any]:
        """The one call, answered as a whole.

        Args:
          None.

        Returns:
          The commands' results, none of which the sink reads.

        Raises:
          Exception: redis' `ConnectionError` or `TimeoutError`.
        """
        _whole(self.backend, tokens(self.ops), "redis")
        return []


class RedisClient:
    """A redis-py client that hands out pipelines."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def pipeline(self) -> RedisPipeline:
        """A new pipeline.

        Args:
          None.

        Returns:
          The pipeline.

        Raises:
          None.
        """
        return RedisPipeline(self.backend)

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


# Transactional.


def _pg_error(message: str, sqlstate: str | None) -> Exception:
    """psycopg's `OperationalError` with its `sqlstate`.

    Args:
      message: Its message.
      sqlstate: Its SQLSTATE, or `None`.

    Returns:
      The exception, not raised.

    Raises:
      None.
    """
    return driver_error("psycopg.errors", "OperationalError", message, sqlstate=sqlstate)


class PGCursor:
    """A psycopg cursor over `PGConnection`."""

    def __init__(self, conn: PGConnection) -> None:
        """Keeps its connection.

        Args:
          conn: The connection.

        Returns:
          None.

        Raises:
          None.
        """
        self.conn = conn

    def __enter__(self) -> Self:
        """Enters the cursor.

        Args:
          None.

        Returns:
          The cursor.

        Raises:
          None.
        """
        return self

    def __exit__(self, *exc: object) -> bool:
        """Leaves the cursor, suppressing nothing.

        Args:
          *exc: The exception, if any.

        Returns:
          False.

        Raises:
          None.
        """
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        """A statement that carries no event, such as the table's creation.

        Args:
          sql: The statement.
          params: Ignored.

        Returns:
          None.

        Raises:
          Exception: psycopg's error on a closed connection.
        """
        self.conn.check()

    def executemany(self, sql: str, rows: list[Any]) -> None:
        """One statement, taken into the transaction or, under autocommit, made durable at once.

        `refuse` and `autocommit` raise a serialization failure (`40001`), which leaves the
        connection usable; a lost connection (`08006`) breaks it.

        Args:
          sql: The statement.
          rows: The rows.

        Returns:
          None.

        Raises:
          Exception: psycopg's `OperationalError`, as the fault script says.
        """
        self.conn.check()
        carried = tokens([list(r) for r in rows])
        action = self.conn.backend.decide(carried, knowable=None if self.conn.autocommit else True)
        if action == "reply-lost":
            self.conn.backend.apply()
            self.conn.backend.calls[-1].knowable = False
        if self.conn.autocommit:
            if self.conn.backend.fault == "autocommit" and self.conn.statements == 1:
                self.conn.statements += 1
                raise _pg_error("could not serialize access", "40001")
            self.conn.statements += 1
            self.conn.backend.take(carried)
            return
        self.conn.statements += 1
        if not self.conn.txn:
            self.conn.txn_from = len(self.conn.backend.calls) - 1
        if action == "refuse":
            self.conn.backend.apply()
            raise _pg_error("could not serialize access", "40001")
        if action == "taken-raise":
            self.conn.backend.apply()
            self.conn.txn.extend(carried[: (len(carried) + 1) // 2])
            self.conn.drop()
            raise _pg_error("server closed the connection unexpectedly", "08006")
        self.conn.txn.extend(carried)
        if action == "reply-lost":
            self.conn.backend.apply()
            self.conn.lose_commit_reply = True


class PGConnection:
    """A psycopg-shaped connection whose transaction is real and whose loss is permanent.

    A rollback discards the transaction's rows. A lost connection sets `broken` and `closed`, as
    psycopg's does, and every later call on it raises "the connection is closed".
    """

    def __init__(self, backend: Backend, *, autocommit: bool = False) -> None:
        """Keeps the row's backend, and whether the connection is in autocommit.

        Args:
          backend: The row's backend.
          autocommit: Whether each statement is durable at once.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.autocommit = autocommit
        self.txn: list[str] = []
        self.closed = False
        self.broken = False
        self.statements = 0
        self.lose_commit_reply = False
        self.txn_from = 0

    def check(self) -> None:
        """Raises as psycopg does on a closed connection.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: psycopg's error when the connection is closed.
        """
        if self.closed:
            raise _pg_error("the connection is closed", None)

    def drop(self) -> None:
        """Loses the connection for good, and the transaction with it.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.txn = []
        self.broken = self.closed = True

    def cursor(self) -> PGCursor:
        """A cursor.

        Args:
          None.

        Returns:
          The cursor.

        Raises:
          Exception: When the connection is closed.
        """
        self.check()
        return PGCursor(self)

    def commit(self) -> None:
        """Makes the transaction durable; under `reply-lost`, then drops the connection.

        A lost commit reply leaves every call of the transaction unknowable, not only its first
        statement, since the commit applied every chunk (SPEC-095).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: psycopg's `08006` error under `reply-lost`, or on a closed connection.
        """
        self.check()
        self.backend.take(self.txn)
        self.txn = []
        if self.lose_commit_reply:
            self.lose_commit_reply = False
            for call in self.backend.calls[self.txn_from :]:
                call.knowable = False
            self.drop()
            raise _pg_error("server closed the connection unexpectedly", "08006")

    def rollback(self) -> None:
        """Discards the transaction.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: When the connection is closed.
        """
        self.check()
        self.txn = []

    def close(self) -> None:
        """Closes it.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.closed = True


class SQLiteProxy:
    """A real in-memory SQLite connection; the backend reads what it holds after the row."""

    def __init__(self, backend: Backend, *, autocommit: bool = False) -> None:
        """Keeps a real in-memory database, in autocommit or not.

        Args:
          backend: The row's backend.
          autocommit: Whether each statement is durable at once.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.real = sqlite3.connect(
            ":memory:", check_same_thread=False, isolation_level=None if autocommit else ""
        )
        self.faulted = False
        backend.on_close.append(self.count)

    def __enter__(self) -> Any:
        """Opens the real connection's transaction.

        Args:
          None.

        Returns:
          The real connection's context.

        Raises:
          None.
        """
        return self.real.__enter__()

    def __exit__(self, *exc: object) -> Any:
        """Commits or rolls back the real connection's transaction.

        Args:
          *exc: The exception, if any.

        Returns:
          Whatever the real connection's exit returns.

        Raises:
          None.
        """
        return self.real.__exit__(*exc)

    def executemany(self, sql: str, rows: Any) -> Any:
        """One statement, applied whole, or half and then raising under the fault.

        `taken-then-raised` and `autocommit` apply the first half, rounded up, and then raise
        "database is locked" once.

        Args:
          sql: The statement.
          rows: The rows.

        Returns:
          The real cursor.

        Raises:
          sqlite3.OperationalError: "database is locked", as the fault script says.
        """
        rows = list(rows)
        carried = tokens([list(r) for r in rows])
        fault = self.backend.fault
        action = self.backend.decide(carried, knowable=True)
        if action == "refuse":
            self.backend.apply()
            raise sqlite3.OperationalError("database is locked")
        if (action == "taken-raise" or fault == "autocommit") and not self.faulted:
            self.backend.apply()
            self.faulted = True
            self.real.executemany(sql, rows[: (len(rows) + 1) // 2])
            raise sqlite3.OperationalError("database is locked")
        return self.real.executemany(sql, rows)

    def count(self) -> None:
        """Hands the backend every row the database holds, then closes it.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        for (event,) in self.real.execute("SELECT event FROM log_events").fetchall():
            self.backend.take(tokens(event))
        self.real.close()

    def __getattr__(self, name: str) -> Any:
        """Everything else, from the real connection.

        Args:
          name: The attribute.

        Returns:
          The real connection's attribute.

        Raises:
          AttributeError: When it has none.
        """
        return getattr(self.real, name)


# Local write.


class FileHandle:
    """The file sinks' unbuffered handle: `write(bytes)` returns what it took."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.partial_done = False

    def write(self, data: bytes) -> int:
        """One write; `taken-then-raised` takes the first half of the lines, then raises.

        Args:
          data: The bytes.

        Returns:
          The bytes taken.

        Raises:
          OSError: On a refusal, and on the write after a partial one.
        """
        carried = tokens(data)
        action = self.backend.decide(carried, knowable=True)
        if action == "refuse":
            self.backend.apply()
            raise OSError(errno.EAGAIN, "Resource temporarily unavailable")
        if action == "taken-raise" and not self.partial_done:
            self.backend.apply()
            self.partial_done = True
            lines = bytes(data).splitlines(keepends=True)
            kept = b"".join(lines[: (len(lines) + 1) // 2])
            self.backend.take(tokens(kept))
            return len(kept)
        if action == "taken-raise":
            self.backend.apply()
            raise OSError(errno.EAGAIN, "Resource temporarily unavailable")
        self.backend.take(carried)
        return len(data)

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def flush(self) -> None:
        """Nothing buffered.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


class Stream:
    """A text stream: each `write` lands, or the call raises and its contract says nothing."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def write(self, data: str) -> int:
        """One line, taken whole, or raising with its contract saying nothing of what was kept.

        Args:
          data: The text.

        Returns:
          The characters written.

        Raises:
          OSError: On a refusal, and after taking the line under `taken-then-raised`.
        """
        carried = tokens(data)
        if not carried:
            return len(data)
        action = self.backend.decide(carried)
        if action == "refuse":
            self.backend.apply()
            self.backend.calls[-1].knowable = False
            raise OSError(errno.EAGAIN, "Resource temporarily unavailable")
        if action == "taken-raise":
            self.backend.apply()
            self.backend.take(carried)
            raise OSError(errno.EIO, "the stream took the line, then failed")
        self.backend.take(carried)
        return len(data)

    def flush(self) -> None:
        """Nothing buffered.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def fileno(self) -> int:
        """No descriptor.

        Args:
          None.

        Returns:
          Nothing.

        Raises:
          io.UnsupportedOperation: Always.
        """
        raise io.UnsupportedOperation("no descriptor")


# Sockets.


class TCPSocket:
    """`sendall`: a frame lands unless the peer closed, when the kernel takes it anyway."""

    def __init__(self, backend: Backend, state: dict[str, int]) -> None:
        """Keeps the row's backend and the connection state its socket shares.

        Args:
          backend: The row's backend.
          state: The generation and the blind writes since the peer closed.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.state = state

    def sendall(self, data: bytes) -> None:
        """One frame; after `peer-closed`, three return having reached nobody and the fourth raises.

        Args:
          data: The frame.

        Returns:
          None.

        Raises:
          OSError: On a refusal, a broken pipe, or a timeout after taking the frame.
        """
        carried = tokens(data)
        if self.backend.peer_closed and self.state["generation"] == 0:
            self.backend.decide(carried, knowable=False)
            self.state["blind"] += 1
            if self.state["blind"] > 3:
                raise BrokenPipeError(errno.EPIPE, "Broken pipe")
            return
        action = self.backend.decide(carried)
        if action == "refuse":
            self.backend.apply()
            raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")
        self.backend.take(carried)
        if action == "taken-raise":
            self.backend.apply()
            raise _timeout()

    def sendto(self, data: bytes, addr: Any) -> None:
        """As `sendall`.

        Args:
          data: The frame.
          addr: Ignored.

        Returns:
          None.

        Raises:
          OSError: As `sendall`.
        """
        self.sendall(data)

    def settimeout(self, value: float | None) -> None:
        """No timeout to keep.

        Args:
          value: Ignored.

        Returns:
          None.

        Raises:
          None.
        """

    def close(self) -> None:
        """The next connection is a new one, which the closed peer does not affect.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.state["generation"] += 1


class UDPSocket:
    """`sendto`: a datagram lands, or with no listener it is gone and nothing says so."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def sendto(self, data: bytes, addr: Any) -> None:
        """One datagram; with no listener it is gone and nothing says so.

        Args:
          data: The datagram.
          addr: Ignored.

        Returns:
          None.

        Raises:
          ConnectionRefusedError: On a refusal.
        """
        carried = tokens(data)
        if self.backend.peer_closed:
            self.backend.decide(carried, knowable=False)
            return
        action = self.backend.decide(carried)
        if action == "refuse":
            self.backend.apply()
            raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")
        self.backend.take(carried)

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


class RabbitChannel:
    """`basic_publish` writes a frame to its connection's socket; with confirms on, it waits.

    A channel `confirm_delivery` put in confirm mode (SPEC-093 FR-005) answers as pika's does:
    a refusal raises `ChannelWrongStateError` before anything is taken, a frame taken and then
    lost raises `StreamLostError`, and a write to the generation-0 connection after `peer-closed`
    raises `StreamLostError` without taking anything, marked knowable, since the connection was
    dead before the frame was written. An unconfirmed channel behaves as SPEC-092's did.
    """

    def __init__(self, conn: RabbitConnection) -> None:
        """Keeps its connection.

        Args:
          conn: The connection.

        Returns:
          None.

        Raises:
          None.
        """
        self.conn = conn
        self.confirming = False

    def confirm_delivery(self) -> None:
        """Puts the channel in confirm mode, refused on a lost connection as pika refuses it.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: pika's `ChannelWrongStateError` on a lost connection.
        """
        if self.conn.broken:
            raise driver_error("pika.exceptions", "ChannelWrongStateError", "Channel is closed")
        self.confirming = True

    def basic_publish(
        self,
        *,
        exchange: str,
        routing_key: str,
        body: bytes,
        properties: Any,
        mandatory: bool = False,
    ) -> None:
        """One publish: a frame written to the connection's socket, confirmed when confirming.

        Args:
          exchange: The exchange.
          routing_key: The routing key.
          body: The message.
          properties: Ignored.
          mandatory: Ignored: the double routes every message.

        Returns:
          None.

        Raises:
          Exception: pika's channel, connection or stream error, as the fault script says.
        """
        conn = self.conn
        if conn.broken:
            raise driver_error("pika.exceptions", "ChannelWrongStateError", "Channel is closed")
        carried = tokens(body)
        backend = conn.backend
        if backend.peer_closed and conn.generation == 0:
            backend.decide(carried, knowable=self.confirming)
            if self.confirming:
                conn.broken = True
                raise driver_error("pika.exceptions", "StreamLostError", "Stream connection lost")
            conn.blind += 1
            if conn.blind > 3:
                conn.broken = True
                raise driver_error("pika.exceptions", "StreamLostError", "Stream connection lost")
            return
        action = backend.decide(carried)
        if action == "refuse":
            backend.apply()
            conn.broken = True
            if self.confirming:
                raise driver_error("pika.exceptions", "ChannelWrongStateError", "Channel is closed")
            raise driver_error("pika.exceptions", "AMQPConnectionError", "connection refused")
        backend.take(carried)
        if action == "taken-raise" or (self.confirming and action == "reply-lost"):
            backend.apply()
            conn.broken = True
            raise driver_error("pika.exceptions", "StreamLostError", "Stream connection lost")

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


class RabbitConnection:
    """A pika `BlockingConnection`: once its stream is lost it stays lost, as pika's does."""

    def __init__(self, backend: Backend, generation: int = 0) -> None:
        """Keeps the row's backend and which connection this is.

        Args:
          backend: The row's backend.
          generation: 0 for the first connection, which `peer-closed` affects.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.generation = generation
        self.blind = 0
        self.broken = False

    @property
    def is_open(self) -> bool:
        """Whether the stream is still up.

        Args:
          None.

        Returns:
          The answer.

        Raises:
          None.
        """
        return not self.broken

    def process_data_events(self, time_limit: float = 0) -> None:
        """Reads the socket once: a connection whose peer closed is found dead here.

        Args:
          time_limit: Ignored.

        Returns:
          None.

        Raises:
          Exception: pika's `StreamLostError` on a lost connection, or on the generation-0
            connection once the peer has closed, which this marks broken.
        """
        if self.broken:
            raise driver_error("pika.exceptions", "StreamLostError", "Stream connection lost")
        if self.backend.peer_closed and self.generation == 0:
            self.broken = True
            raise driver_error("pika.exceptions", "StreamLostError", "Stream connection lost")

    def channel(self) -> RabbitChannel:
        """A channel, refused on a lost connection.

        Args:
          None.

        Returns:
          The channel.

        Raises:
          Exception: pika's `ConnectionWrongStateError` on a lost connection.
        """
        if self.broken:
            raise driver_error(
                "pika.exceptions", "ConnectionWrongStateError", "Connection is closed"
            )
        return RabbitChannel(self)

    def close(self) -> None:
        """Closes it.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.broken = True


# NATS.


class NATSJetStream:
    """A JetStream context: each publish waits for its acknowledgement."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    async def publish(self, subject: str, payload: bytes, *, timeout: float | None = None) -> None:  # noqa: ASYNC109
        """One publish; under `reply-lost` the stream keeps it and the ack never arrives.

        Args:
          subject: The subject.
          payload: The message.
          timeout: Ignored.

        Returns:
          None.

        Raises:
          Exception: A refusal, or `TimeoutError` for a lost ack.
        """
        carried = tokens(payload)
        action = self.backend.decide(carried)
        if action == "refuse":
            self.backend.apply()
            raise REFUSED
        self.backend.take(carried)
        if action == "reply-lost":
            self.backend.apply()
            raise TimeoutError("ack wait expired")


class NATSClient:
    """Core publishes buffer in the client; `flush` is the PING the server answers with a PONG."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend and a connected state.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.is_connected = True
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}

    async def publish(self, subject: str, payload: bytes) -> None:
        """One core publish; under `never-acknowledged` nothing reaches the server.

        Args:
          subject: The subject.
          payload: The message.

        Returns:
          None.

        Raises:
          ConnectionRefusedError: On a refusal.
        """
        carried = tokens(payload)
        action = self.backend.decide(carried, knowable=self.backend.fault != "never-acknowledged")
        if action == "refuse":
            self.backend.apply()
            raise REFUSED
        if action == "nack":
            self.backend.apply()
            return
        self.backend.take(carried)

    def jetstream(self) -> NATSJetStream:
        """A JetStream context over this client.

        Args:
          None.

        Returns:
          The context.

        Raises:
          None.
        """
        return NATSJetStream(self.backend)

    async def flush(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        """The PING; under `never-acknowledged` its PONG never comes.

        Args:
          timeout: Ignored.

        Returns:
          None.

        Raises:
          TimeoutError: Under `never-acknowledged`.
        """
        if self.backend.fault == "never-acknowledged" and self.backend.calls:
            raise TimeoutError("no PONG")

    async def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    async def drain(self) -> None:
        """Nothing to drain.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


# Client-buffered.


class KafkaError:
    """A delivery report's error.

    `87` is `INVALID_RECORD`, definitive: no attempt could have written the message. `-192` is
    `_MSG_TIMED_OUT`, which may follow a write the broker kept.

    Attributes:
      value: The error code.
    """

    def __init__(self, value: int) -> None:
        """Keeps the code.

        Args:
          value: The error code.

        Returns:
          None.

        Raises:
          None.
        """
        self.value = value

    def code(self) -> int:
        """The error code.

        Args:
          None.

        Returns:
          The code.

        Raises:
          None.
        """
        return self.value

    def __str__(self) -> str:
        """The error as confluent-kafka renders one.

        Args:
          None.

        Returns:
          The text.

        Raises:
          None.
        """
        return f"KafkaError {self.value}"


class KafkaProducer:
    """Buffers on `produce`; the delivery report for each message runs at `poll` or `flush`."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend and its local queue.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.queue: list[tuple[list[str], Any, bool]] = []

    def produce(self, topic: str, value: Any = None, key: Any = None, callback: Any = None) -> None:
        """Queues one message, or raises `BufferError` as a full local queue does.

        Args:
          topic: The topic.
          value: The message.
          key: Ignored.
          callback: Its report.

        Returns:
          None.

        Raises:
          BufferError: On a refusal.
        """
        carried = tokens(value)
        indefinite = self.backend.fault == "report-indefinite"
        action = self.backend.decide(carried, knowable=not indefinite)
        if action == "refuse":
            self.backend.apply()
            raise BufferError("Local: Queue full")
        if action == "nack":
            self.backend.apply()
        self.queue.append((carried, callback, action == "nack"))

    def poll(self, timeout: float = 0) -> int:
        """Serves no report; they wait for `flush`.

        Args:
          timeout: Ignored.

        Returns:
          0.

        Raises:
          None.
        """
        return 0

    def flush(self, timeout: float | None = None) -> int:
        """Serves every queued message's report.

        Args:
          timeout: Ignored.

        Returns:
          0, the messages still queued.

        Raises:
          None.
        """
        queue, self.queue = self.queue, []
        for carried, callback, refused in queue:
            if refused:
                if callback is not None:
                    callback(KafkaError(87), None)
            elif self.backend.fault == "report-indefinite":
                self.backend.take(carried)
                if callback is not None:
                    callback(KafkaError(-192), None)
            else:
                self.backend.take(carried)
                if callback is not None:
                    callback(None, None)
        return 0

    def __len__(self) -> int:
        """The messages still queued.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return len(self.queue)


class PermissionDenied(Exception):
    """A local stand-in for `google.api_core.exceptions.PermissionDenied`, which no fault raises.

    The double raises the stub module's `InvalidArgument` and `DeadlineExceeded` (SPEC-094 FR-003),
    installed through `DRIVERS` with google-api-core's hierarchy. This local class is kept because
    SPEC-094 FR-003 plants it again as a mutant, to show the definitive set is read by class.
    """


class PubSubFuture:
    """A publish future: its result, or its error, is the report."""

    def __init__(self, backend: Backend, carried: list[str], *, refused: bool = False) -> None:
        """Keeps the row's backend, the message's tokens, and whether its report refuses it.

        Args:
          backend: The row's backend.
          carried: The message's tokens.
          refused: Whether the publish was decided `nack`, so its report is a refusal.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend
        self.carried = carried
        self.refused = refused
        self.settled: bool | None = None

    def done(self) -> bool:
        """Whether the report is in.

        Args:
          None.

        Returns:
          The answer.

        Raises:
          None.
        """
        return self.settled is not None

    def result(self, timeout: float | None = None) -> str:
        """The message id, or the report's error.

        Args:
          timeout: Ignored.

        Returns:
          The message id.

        Raises:
          Exception: `InvalidArgument` or `DeadlineExceeded`, as the fault script says.
        """
        fault = self.backend.fault
        if self.settled is None:
            self.settled = not self.refused and fault != "report-indefinite"
            if self.settled or fault == "report-indefinite":
                self.backend.take(self.carried)
        if self.refused:
            raise driver_error(
                "google.api_core.exceptions", "InvalidArgument", "the topic refused the publish"
            )
        if fault == "report-indefinite":
            raise driver_error(
                "google.api_core.exceptions", "DeadlineExceeded", "the publish deadline passed"
            )
        return "id"

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """The report's error, or `None`.

        Args:
          timeout: Ignored.

        Returns:
          The error, or `None`.

        Raises:
          None.
        """
        try:
            self.result()
        except Exception as err:
            return err
        return None


class PubSubPublisher:
    """A Pub/Sub publisher client."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def publish(self, topic: str, data: bytes | None = None, **attrs: Any) -> PubSubFuture:
        """Queues one message and hands back its future.

        Args:
          topic: The topic.
          data: The message.
          **attrs: Ignored.

        Returns:
          The future.

        Raises:
          ConnectionRefusedError: On a refusal.
        """
        carried = tokens(data)
        action = self.backend.decide(carried, knowable=self.backend.fault != "report-indefinite")
        if action == "refuse":
            self.backend.apply()
            raise REFUSED
        if action == "nack":
            self.backend.apply()
        return PubSubFuture(self.backend, carried, refused=action == "nack")


class SentrySDK:
    """`capture_event` returns the event id, or `None` when the SDK discarded the event.

    Under `report-indefinite` it returns an id and its transport then drops the event, which the
    SDK never reports back (`SentrySink.flush`'s docstring).
    """

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        self.backend = backend

    def capture_event(self, event: Any) -> Any:
        """One capture: the event id, or `None` when the SDK discarded the event.

        Args:
          event: The event.

        Returns:
          The event id, or `None` under `never-acknowledged`.

        Raises:
          ConnectionRefusedError: On a refusal.
        """
        carried = tokens(event)
        indefinite = self.backend.fault == "report-indefinite"
        action = self.backend.decide(carried, knowable=not indefinite)
        if action == "refuse":
            self.backend.apply()
            raise REFUSED
        if action == "nack":
            self.backend.apply()
            return None
        if indefinite:
            return "event-id"
        self.backend.take(carried)
        return "event-id"

    def flush(self, timeout: float | None = None) -> None:
        """Nothing it can report.

        Args:
          timeout: Ignored.

        Returns:
          None.

        Raises:
          None.
        """


# In-process.


def callback(backend: Backend) -> Callable[[list[dict[str, object]]], None]:
    """A caller's callable; under `taken-then-raised` it keeps half, then raises.

    Args:
      backend: The row's backend.

    Returns:
      The callable.

    Raises:
      None.
    """

    def fn(batch: list[dict[str, object]]) -> None:
        """Keeps a batch, or refuses it, as the fault script says.

        Args:
          batch: The batch.

        Returns:
          None.

        Raises:
          Exception: A refusal, or `RuntimeError` after keeping half.
        """
        carried = tokens(batch)
        action = backend.decide(carried)
        if action == "refuse":
            backend.apply()
            raise REFUSED
        if action == "taken-raise":
            backend.apply()
            backend.take(carried[: (len(carried) + 1) // 2])
            raise RuntimeError("the callable kept half, then raised")
        backend.take(carried)

    return fn


class Handler(logging.Handler):
    """A handler shaped as the stdlib's are: a failure inside `emit` goes to `handleError`."""

    def __init__(self, backend: Backend) -> None:
        """Keeps the row's backend.

        Args:
          backend: The row's backend.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__()
        self.backend = backend

    def emit(self, record: logging.LogRecord) -> None:
        """Keeps one record, or hands its failure to `handleError`.

        Args:
          record: The record.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            self._keep(record)
        except Exception:
            self.handleError(record)

    def _keep(self, record: logging.LogRecord) -> None:
        """One record.

        Args:
          record: The record.

        Returns:
          None.

        Raises:
          Exception: A refusal, or `RuntimeError` after keeping the record.
        """
        carried = tokens(json.dumps(record.__dict__, default=repr))
        action = self.backend.decide(carried)
        if action == "refuse":
            self.backend.apply()
            raise REFUSED
        self.backend.take(carried)
        if action == "taken-raise":
            self.backend.apply()
            raise RuntimeError("the handler kept the record, then raised")

    def handleError(self, record: logging.LogRecord) -> None:
        """The stdlib default prints a traceback to stderr and returns; this returns quietly.

        Args:
          record: The record.

        Returns:
          None.

        Raises:
          None.
        """


def logger(backend: Backend) -> logging.Logger:
    """A logger with one `Handler` and no propagation.

    Args:
      backend: The row's backend.

    Returns:
      The logger.

    Raises:
      None.
    """
    log = logging.getLogger(f"spec092.{id(backend)}")
    log.handlers[:] = [Handler(backend)]
    log.propagate = False
    log.setLevel(logging.DEBUG)
    return log


# Builders.

Built = tuple[object, object]
Builder = Callable[[Variant, Backend, pytest.MonkeyPatch], Built]


def _sink_class(path: str) -> Any:
    """The sink class at a dotted path under `log_foundry.sinks`, imported when a row builds it.

    Args:
      path: `<module>.<class>`, relative to `log_foundry.sinks`.

    Returns:
      The class.

    Raises:
      ImportError: When the module is gone.
      AttributeError: When the class is gone.
    """
    module, name = path.rsplit(".", 1)
    return getattr(importlib.import_module(f"log_foundry.sinks.{module}"), name)


def _injected(
    path: str,
    double: Callable[[Backend], object],
    keyword: str,
    *args: object,
    **kwargs: object,
) -> Builder:
    """A builder for a sink that takes its client double as one keyword argument.

    Args:
      path: The sink class, as `_sink_class` takes it.
      double: Builds the client double over the row's backend.
      keyword: The constructor keyword the double is passed as.
      *args: The constructor's other positional arguments.
      **kwargs: Its other keyword arguments.

    Returns:
      The builder.

    Raises:
      None.
    """

    def build(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
        """Builds the sink over a new double.

        Args:
          variant: The row's variant.
          backend: The row's backend.
          monkeypatch: The row's patch, unused.

        Returns:
          The sink, twice: it is its own judged sink.

        Raises:
          None.
        """
        sink = _sink_class(path)(*args, **{keyword: double(backend)}, **kwargs)
        return sink, sink

    return build


def _opener(reply: str = "whole") -> Callable[[Backend], object]:
    """A factory of `Opener` doubles that answer in one reply shape.

    Args:
      reply: `"whole"`, `"bulk"` or `"statuses"`.

    Returns:
      The factory.

    Raises:
      None.
    """
    return lambda backend: Opener(backend, reply)


def _sqs(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`SQSSink`, on a FIFO URL with a de-duplicating double when its pick says `fifo`.

    Args:
      variant: The row's variant, whose `fifo` pick is passed to the constructor.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    fifo = bool(variant.picks["fifo"])
    url = "https://sqs/q.fifo" if fifo else "https://sqs/q"
    sink = _sink_class("sqs.SQSSink")(url, client=SQSClient(backend, fifo=fifo), fifo=fifo)
    return sink, sink


def _socket_seams(backend: Backend, monkeypatch: pytest.MonkeyPatch, transport: str) -> None:
    """Patches the socket a socket-mode sink builds to the row's double.

    Args:
      backend: The row's backend.
      monkeypatch: The row's patch.
      transport: `"tcp"` or `"udp"`.

    Returns:
      None.

    Raises:
      AttributeError: When the seam is gone.
    """
    import log_foundry.sinks._socket as socket_mod

    if transport == "udp":
        monkeypatch.setattr(socket_mod, "_make_udp", lambda host: UDPSocket(backend))
        return
    state = {"generation": 0, "blind": 0}
    monkeypatch.setattr(
        socket_mod, "_make_tcp", lambda host, port, timeout: TCPSocket(backend, state)
    )


def _logstash(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`LogstashSink` over HTTP when its pick is a `url`, else over its socket `transport`.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, for the socket seam.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    cls = _sink_class("logstash.LogstashSink")
    if "url" in variant.picks:
        sink = cls(str(variant.picks["url"]), opener=Opener(backend))
        return sink, sink
    transport = str(variant.picks["transport"])
    _socket_seams(backend, monkeypatch, transport)
    sink = cls(host=str(variant.picks["host"]), port=5000, transport=transport)
    return sink, sink


def _syslog(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`SyslogSink` over its `transport`.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, for the socket seam.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    transport = str(variant.picks["transport"])
    _socket_seams(backend, monkeypatch, transport)
    sink = _sink_class("syslog.SyslogSink")(host="h", port=514, transport=transport)
    return sink, sink


def _sentry(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`SentrySink` through the SDK double or its own HTTP request, as its `backend` pick says.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    cls = _sink_class("sentry.SentrySink")
    if variant.picks["backend"] == "sdk":
        sink = cls(backend="sdk", client=SentrySDK(backend), min_level="DEBUG")
    else:
        sink = cls(
            "https://key@o0.ingest.sentry.io/1",
            backend="http",
            opener=Opener(backend),
            min_level="DEBUG",
        )
    return sink, sink


def _eventhubs(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`AzureEventHubsSink`, its `EventData` seam answering the body unwrapped.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, for the seam.

    Returns:
      The sink, twice.

    Raises:
      AttributeError: When the seam is gone.
    """
    from log_foundry.sinks import eventhubs

    monkeypatch.setattr(eventhubs, "_event_data_cls", lambda: lambda body: body)
    sink = eventhubs.AzureEventHubsSink(producer=EHProducer(backend))
    return sink, sink


def _postgres(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`PostgresSink` with `chunk_size=3`, so eight events are three statements.

    A `dsn` pick builds through the stub `psycopg.connect`, a new connection each time, so the
    sink reconnects a lost one; a `connection` pick hands it one, which it never reopens.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, for the stub's `connect`.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    cls = _sink_class("postgres.PostgresSink")
    if "dsn" in variant.picks:
        monkeypatch.setattr(
            sys.modules["psycopg"], "connect", lambda *a, **k: PGConnection(backend), raising=False
        )
        sink = cls("t", dsn=str(variant.picks["dsn"]), chunk_size=3)
        return sink, sink
    conn = PGConnection(backend, autocommit=variant.picks["connection"] == "autocommit")
    sink = cls("t", connection=conn, chunk_size=3)
    return sink, sink


def _sqlite(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`SQLiteSink` over a real in-memory database behind `SQLiteProxy`.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, for the sink's connection.

    Returns:
      The sink, twice.

    Raises:
      AttributeError: When the sink no longer keeps its connection as `_conn`.
    """
    proxy = SQLiteProxy(backend, autocommit=variant.picks["connection"] == "autocommit")
    sink = _sink_class("sqlite.SQLiteSink")(":memory:", connection=proxy.real)
    monkeypatch.setattr(sink, "_conn", proxy)
    return sink, sink


def _file_sink(rotating: bool) -> Builder:
    """A builder for `FileSink` or `RotatingFileSink`, its handle replaced by `FileHandle`.

    Args:
      rotating: Whether to build a `RotatingFileSink`.

    Returns:
      The builder.

    Raises:
      None.
    """

    def build(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
        """Builds the file sink in a temporary directory the backend removes after the row.

        Args:
          variant: The row's variant.
          backend: The row's backend.
          monkeypatch: The row's patch, for the sink's handle.

        Returns:
          The sink, twice.

        Raises:
          AttributeError: When the sink no longer keeps its handle as `_stream`.
        """
        directory = tempfile.mkdtemp(prefix="spec092-")
        backend.on_close.append(lambda: shutil.rmtree(directory, ignore_errors=True))
        path = directory + "/f.ndjson"
        if rotating:
            sink = _sink_class("file.RotatingFileSink")(path, max_bytes=10**9, backup_count=1)
        else:
            sink = _sink_class("file.FileSink")(path)
        sink._stream.close()
        monkeypatch.setattr(sink, "_stream", FileHandle(backend))
        return sink, sink

    return build


def _rabbitmq(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`RabbitMQSink`, owning its connection on a `url` pick, borrowing one on `connection`.

    A `url` pick builds through the stub `pika.BlockingConnection`, a new connection each time;
    a `connection` pick hands it one, which its `_reset` keeps. `confirm` is passed only when a
    variant picks it, so the default variants build with the 1.x arguments and the corpus still
    runs against a tree whose `RabbitMQSink` has no `confirm`.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, for the stub's `BlockingConnection`.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    cls = _sink_class("rabbitmq.RabbitMQSink")
    confirm = {"confirm": True} if variant.picks.get("confirm") else {}
    if "url" in variant.picks:
        generations = itertools.count()
        monkeypatch.setattr(
            sys.modules["pika"],
            "BlockingConnection",
            lambda params: RabbitConnection(backend, next(generations)),
            raising=False,
        )
        sink = cls(exchange="x", routing_key="k", url=str(variant.picks["url"]), **confirm)
        return sink, sink
    sink = cls(exchange="x", routing_key="k", connection=RabbitConnection(backend), **confirm)
    return sink, sink


def _nats(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`NATSSink` over a client double, injected or connected by the sink itself.

    A `servers` pick builds it through a `nats` module whose `connect` returns the double, as
    `tests/nats_engine_matrix_harness.py` does: only a client the sink connected itself is
    confirmed by a round trip.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, for the `nats` module.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    cls = _sink_class("nats.NATSSink")
    client = NATSClient(backend)
    jetstream = bool(variant.picks["jetstream"])
    if "client" in variant.picks:
        sink = cls("logs", client=client, jetstream=jetstream)
        return sink, sink
    module = types.ModuleType("nats")

    async def connect(*args: Any, **kwargs: Any) -> object:
        """`nats.connect`, answering the row's client double.

        Args:
          *args: Ignored.
          **kwargs: Ignored.

        Returns:
          The double.

        Raises:
          None.
        """
        return client

    module.connect = connect  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nats", module)
    sink = cls(
        "logs", servers=str(variant.picks["servers"]), jetstream=jetstream, drain_timeout=0.2
    )
    return sink, sink


def _inner_callback(backend: Backend) -> object:
    """A `CallbackSink` over the row's callable.

    Args:
      backend: The row's backend.

    Returns:
      The sink.

    Raises:
      None.
    """
    return _sink_class("callback.CallbackSink")(callback(backend))


def _inner_http(backend: Backend) -> object:
    """An `HTTPSink` over the row's opener.

    Args:
      backend: The row's backend.

    Returns:
      The sink.

    Raises:
      None.
    """
    return _sink_class("http.HTTPSink")("http://x/in", opener=Opener(backend))


_INNER: Mapping[str, Callable[[Backend], object]] = {
    "HTTPSink": _inner_http,
    "CallbackSink": _inner_callback,
}


def _alone(make: Callable[[Backend], object]) -> Builder:
    """A builder for a sink that one function of the backend makes whole.

    Args:
      make: Makes the sink over the row's backend.

    Returns:
      The builder.

    Raises:
      None.
    """

    def build(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
        """Builds the sink.

        Args:
          variant: The row's variant.
          backend: The row's backend.
          monkeypatch: The row's patch, unused.

        Returns:
          The sink, twice.

        Raises:
          None.
        """
        sink = make(backend)
        return sink, sink

    return build


def _memory(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`MemorySink`; the backend reads what it holds after the row.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    sink = _sink_class("memory.MemorySink")()
    backend.on_close.append(lambda: [backend.take(tokens(e)) for e in sink.events])
    return sink, sink


def _raising_on(token_index: int) -> Callable[[dict[str, object]], Any]:
    """Caller code that raises for the event whose `log_id` ends in `token_index`.

    Args:
      token_index: The event's index in its batch.

    Returns:
      The function, which hands back every other event unchanged.

    Raises:
      None.
    """

    def fn(event: dict[str, object]) -> Any:
        """Hands back the event, or raises for the chosen one.

        Args:
          event: An event.

        Returns:
          The event.

        Raises:
          ValueError: For the chosen event.
        """
        if str(event.get("log_id", "")).endswith(f"{token_index:02d}"):
            raise ValueError("caller code failed for this event")
        return event

    return fn


def _filtering(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`FilteringSink` over its inner variant; under `caller-code-raises` the predicate raises.

    Args:
      variant: The row's variant, which names its inner variant.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    assert variant.wraps is not None
    raising = _raising_on(3) if backend.fault == "caller-code-raises" else (lambda e: e)

    def predicate(event: dict[str, object]) -> bool:
        """Keeps every event, unless the row's caller code raises for it.

        Args:
          event: An event.

        Returns:
          True.

        Raises:
          ValueError: Under `caller-code-raises`, for the chosen event.
        """
        return bool(raising(event))

    sink = _sink_class("filtering.FilteringSink")(
        _INNER[variant.wraps](backend), predicate=predicate
    )
    return sink, sink


def _transform(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`TransformSink` over its inner variant; under `caller-code-raises` the function raises.

    Args:
      variant: The row's variant, which names its inner variant.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The sink, twice.

    Raises:
      None.
    """
    assert variant.wraps is not None
    fn = _raising_on(3) if backend.fault == "caller-code-raises" else (lambda e: e)
    sink = _sink_class("transform.TransformSink")(_INNER[variant.wraps](backend), fn)
    return sink, sink


def _multi(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`MultiSink` over a clean `HTTPSink` and the judged one; the judged sink is the second.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The `MultiSink`, and the judged `HTTPSink` behind it.

    Raises:
      None.
    """
    http = _sink_class("http.HTTPSink")
    first = http("http://a/in", opener=Opener(Backend("clean")))
    judged = http("http://b/in", opener=Opener(backend))
    return _sink_class("multi.MultiSink")(first, judged), judged


def _factory(variant: Variant, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """`_FactorySink`, what `configure(sink_factory=…)` installs; the judged sink is its child.

    Args:
      variant: The row's variant.
      backend: The row's backend.
      monkeypatch: The row's patch, unused.

    Returns:
      The `_FactorySink`, and the judged `HTTPSink` it builds.

    Raises:
      None.
    """
    judged = _inner_http(backend)
    return _sink_class("_factory._FactorySink")(lambda: judged), judged


BUILDERS: dict[str, Builder] = {
    "sqs": _sqs,
    "sns": _injected("sns.SNSSink", SNSClient, "client", "arn:aws:sns:r:1:t"),
    "mongodb": _injected(
        "mongodb.MongoDBSink", MongoClient, "client", database="d", collection="c"
    ),
    "kinesis": _injected("kinesis.KinesisSink", KinesisClient, "client", "s"),
    "firehose": _injected("firehose.FirehoseSink", FirehoseClient, "client", "d"),
    "elasticsearch": _injected(
        "elasticsearch.ElasticsearchSink", _opener("bulk"), "opener", "http://es", index="i"
    ),
    "opensearch": _injected(
        "elasticsearch.OpenSearchSink", _opener("bulk"), "opener", "http://os", index="i"
    ),
    "honeycomb": _injected(
        "honeycomb.HoneycombSink", _opener("statuses"), "opener", "k", "ds", url="http://hc"
    ),
    "http": _injected("http.HTTPSink", _opener(), "opener", "http://x/in"),
    "datadog": _injected("datadog.DatadogSink", _opener(), "opener", "k"),
    "splunk": _injected("splunk.SplunkHECSink", _opener(), "opener", "http://splunk", "tok"),
    "newrelic": _injected("newrelic.NewRelicSink", _opener(), "opener", "k"),
    "loki": _injected("loki.LokiSink", _opener(), "opener", "http://loki"),
    "otlp": _injected("otlp.OTLPSink", _opener(), "opener", "http://otlp/v1/logs"),
    "logstash": _logstash,
    "sentry": _sentry,
    "clickhouse": _injected("clickhouse.ClickHouseSink", ClickHouseClient, "client", "t"),
    "eventhubs": _eventhubs,
    "redis_streams": _injected("redis.RedisStreamsSink", RedisClient, "client", "s"),
    "redis_list": _injected("redis.RedisListSink", RedisClient, "client", "k"),
    "postgres": _postgres,
    "sqlite": _sqlite,
    "file": _file_sink(rotating=False),
    "rotating_file": _file_sink(rotating=True),
    "stdout": _injected("stdout.StdoutSink", Stream, "stream"),
    "stderr": _injected("stdout.StderrSink", Stream, "stream"),
    "syslog": _syslog,
    "rabbitmq": _rabbitmq,
    "nats": _nats,
    "kafka": _injected("kafka.KafkaSink", KafkaProducer, "producer", "t"),
    "pubsub": _injected(
        "pubsub.GooglePubSubSink", PubSubPublisher, "client", "projects/p/topics/t"
    ),
    "callback": _alone(_inner_callback),
    "logging": _alone(lambda backend: _sink_class("logging_sink.LoggingSink")(logger(backend))),
    "memory": _memory,
    "null": _alone(lambda backend: _sink_class("null.NullSink")()),
    "filtering": _filtering,
    "transform": _transform,
    "multi": _multi,
    "factory": _factory,
}
"""Each `Variant.build` name's builder. A builder returns `(sink, target)`: the sink the worker
is given, and the sink whose engine runs are judged per event, which differ only for a wrapper
with no `deliver` of its own (`MultiSink`, `_FactorySink`)."""


def build(row: Row, backend: Backend, monkeypatch: pytest.MonkeyPatch) -> Built:
    """Builds a row's sink.

    Args:
      row: The row.
      backend: Its backend.
      monkeypatch: Where the builder sets its seams.

    Returns:
      `(sink, target)`.

    Raises:
      KeyError: When the row's variant names no builder.
    """
    return BUILDERS[row.variant.build](row.variant, backend, monkeypatch)
