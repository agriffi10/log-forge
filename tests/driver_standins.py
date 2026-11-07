"""SPEC-070 — stand-in driver exception modules, so the unit suite can test without the extras.

Each stand-in is a ``types.ModuleType`` installed under the key of the module that *defines* the
classes in the real driver — the key each sink's ``_retryable_error`` reads through
``driver_classes`` — with the class names and the parent chains the locked drivers carry
(botocore 1.43.119, clickhouse-connect 1.4.2, pymongo 4.19.0, redis 8.1.0, pika 1.4.4,
azure-eventhub 5.15.1, psycopg 3.3.6). The hierarchy matters as much as the names: a class the
predicate excludes from a retried parent must be a subclass of it here, as it is there, or the
exclusion would be tested against a model the driver does not share. The extras leg checks the
real drivers carry every name and every one of those parent links
(``tests/test_classify.py``).
"""

from __future__ import annotations

import sys
import types
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pytest


def _module(
    name: str, tree: dict[str, tuple[str, ...]], extra: dict[str, Any] | None = None
) -> types.ModuleType:
    """Builds a module of exception classes from ``{name: (parent names...)}``, in order."""
    module = types.ModuleType(name)
    for cls_name, parents in tree.items():
        bases = tuple(getattr(module, p) for p in parents) or (Exception,)
        namespace = dict((extra or {}).get(cls_name, {}))
        namespace["__module__"] = name
        setattr(module, cls_name, type(cls_name, bases, namespace))
    return module


def _client_error_init(self: Any, error_response: Any = None, operation_name: str = "Op") -> None:
    Exception.__init__(self, "client error")
    self.response = error_response
    self.operation_name = operation_name


def _labelled_init(self: Any, message: str = "", labels: tuple[str, ...] = ()) -> None:
    Exception.__init__(self, message)
    self._labels = set(labels)


def _has_error_label(self: Any, label: str) -> bool:
    return label in getattr(self, "_labels", set())


def _sqlstate_init(self: Any, message: str = "", sqlstate: object = None) -> None:
    Exception.__init__(self, message)
    self.sqlstate = sqlstate


BOTOCORE = "botocore.exceptions"
CLICKHOUSE = "clickhouse_connect.driver.exceptions"
PYMONGO = "pymongo.errors"
REDIS = "redis.exceptions"
PIKA = "pika.exceptions"
AZURE = "azure.eventhub.exceptions"
PSYCOPG = "psycopg.errors"

TREES: dict[str, dict[str, tuple[str, ...]]] = {
    BOTOCORE: {
        "BotoCoreError": (),
        "ConnectionError": ("BotoCoreError",),
        "HTTPClientError": ("BotoCoreError",),
        "ParamValidationError": ("BotoCoreError",),
        "NoCredentialsError": ("BotoCoreError",),
        "ClientError": (),
    },
    CLICKHOUSE: {
        "ClickHouseError": (),
        "Error": ("ClickHouseError",),
        "DatabaseError": ("Error",),
        "OperationalError": ("DatabaseError",),
        "DataError": ("DatabaseError",),
        "ProgrammingError": ("DatabaseError",),
        "InterfaceError": ("Error",),
    },
    PYMONGO: {
        "PyMongoError": (),
        "ConnectionFailure": ("PyMongoError",),
        "AutoReconnect": ("ConnectionFailure",),
        "NetworkTimeout": ("AutoReconnect",),
        "NotPrimaryError": ("AutoReconnect",),
        "ServerSelectionTimeoutError": ("AutoReconnect",),
        "WaitQueueTimeoutError": ("ConnectionFailure",),
        "OperationFailure": ("PyMongoError",),
        "WriteError": ("OperationFailure",),
        "DuplicateKeyError": ("WriteError",),
        "WriteConcernError": ("OperationFailure",),
        "WTimeoutError": ("WriteConcernError",),
        "InvalidDocument": (),
        "DocumentTooLarge": ("InvalidDocument",),
        "InvalidOperation": ("PyMongoError",),
        "ConfigurationError": ("PyMongoError",),
    },
    REDIS: {
        "RedisError": (),
        "ConnectionError": ("RedisError",),
        "AuthenticationError": ("ConnectionError",),
        "AuthorizationError": ("ConnectionError",),
        "ExternalAuthProviderError": ("ConnectionError",),
        "TimeoutError": ("RedisError",),
        "ResponseError": ("RedisError",),
        "TryAgainError": ("ResponseError",),
        "ClusterError": ("RedisError",),
        "ClusterDownError": ("ClusterError", "ResponseError"),
        "DataError": ("RedisError",),
    },
    PIKA: {
        "AMQPError": (),
        "AMQPConnectionError": ("AMQPError",),
        "AuthenticationError": ("AMQPConnectionError",),
        "ProbableAuthenticationError": ("AMQPConnectionError",),
        "ProbableAccessDeniedError": ("AMQPConnectionError",),
        "IncompatibleProtocolError": ("AMQPConnectionError",),
        "ConnectionOpenAborted": ("AMQPConnectionError",),
        "ConnectionClosed": ("AMQPConnectionError",),
        "ConnectionClosedByClient": ("ConnectionClosed",),
        "StreamLostError": ("AMQPConnectionError",),
        "AMQPChannelError": ("AMQPError",),
        "ChannelWrongStateError": ("AMQPChannelError",),
        "ChannelClosed": ("AMQPChannelError",),
        "ChannelClosedByBroker": ("ChannelClosed",),
        "ProtocolSyntaxError": ("AMQPError",),
    },
    AZURE: {
        "EventHubError": (),
        "ConnectionLostError": ("EventHubError",),
        "ConnectError": ("EventHubError",),
        "AuthenticationError": ("ConnectError",),
        "OperationTimeoutError": ("EventHubError",),
        "EventDataError": ("EventHubError",),
        "EventDataSendError": ("EventHubError",),
        "ClientClosedError": ("EventHubError",),
    },
    PSYCOPG: {
        "Error": (),
        "DatabaseError": ("Error",),
        "OperationalError": ("DatabaseError",),
        "DataError": ("DatabaseError",),
        "StringDataRightTruncation": ("DataError",),
        "IntegrityError": ("DatabaseError",),
        "ProgrammingError": ("DatabaseError",),
        "InternalError": ("DatabaseError",),
        "NotSupportedError": ("DatabaseError",),
        "InterfaceError": ("Error",),
    },
}
"""Every stand-in module: class name to its parents, parents listed first."""

_EXTRA: dict[str, dict[str, dict[str, Any]]] = {
    BOTOCORE: {"ClientError": {"__init__": _client_error_init}},
    PYMONGO: {"PyMongoError": {"__init__": _labelled_init, "has_error_label": _has_error_label}},
    PSYCOPG: {"Error": {"__init__": _sqlstate_init}},
}


def build(key: str) -> types.ModuleType:
    """A fresh stand-in module for ``key``."""
    return _module(key, TREES[key], _EXTRA.get(key))


def install(monkeypatch: pytest.MonkeyPatch, key: str) -> types.ModuleType:
    """Installs a fresh stand-in under ``key`` for the test's duration, and returns it."""
    module = build(key)
    monkeypatch.setitem(sys.modules, key, module)
    return module


def client_error(module: types.ModuleType, status: object = 400, code: object = "X") -> Exception:
    """A botocore ``ClientError`` stand-in with the documented response shape."""
    response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}
    error: Exception = module.ClientError(response, "PutRecords")
    return error
