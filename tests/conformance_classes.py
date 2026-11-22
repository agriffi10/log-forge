"""SPEC-092's acknowledgement classes: every shipped sink, per constructor variant, as data.

A helper, not collected. It imports nothing from `log_foundry`, so the declaration can be read
without building a sink, and without an extra installed. A sink's **acknowledgement class** is
what it can know about whether its backend took each event, and it belongs to a constructor
variant rather than to a Python class, because the constructor picks what the sink can know: a
`LogstashSink` over HTTP is a `request-reply` sink, the same class over TCP a `socket-stream` one.

The data lives here rather than on the sinks (SPEC-092 FR-001): a public attribute would add to
the surface of 39 classes for good, could not tell one `LogstashSink` from another without
becoming an instance property, and the corpus is its only reader.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, get_args

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

AckClass = Literal[
    "keyed-reply",
    "positional-reply",
    "request-reply",
    "transactional",
    "local-write",
    "socket-stream",
    "per-event-ack",
    "client-buffered",
    "in-process",
    "wrapper",
]

Fault = Literal[
    "clean",
    "refused-once",
    "refused-always",
    "partial",
    "taken-then-raised",
    "reply-lost",
    "peer-closed",
    "no-verdict-shape",
    "never-acknowledged",
    "report-indefinite",
    "autocommit",
    "caller-code-raises",
]

ACK_CLASSES: tuple[AckClass, ...] = get_args(AckClass)
"""The ten classes, in the order SPEC-092 FR-001's table lists them."""

FAULTS: tuple[Fault, ...] = get_args(Fault)
"""Every fault script, in the order SPEC-092 FR-002's table lists them."""

APPLIES: Mapping[AckClass, tuple[Fault, ...]] = MappingProxyType(
    {
        "keyed-reply": (
            "clean",
            "refused-once",
            "refused-always",
            "partial",
            "taken-then-raised",
            "reply-lost",
            "no-verdict-shape",
        ),
        "positional-reply": (
            "clean",
            "refused-once",
            "refused-always",
            "partial",
            "taken-then-raised",
            "reply-lost",
            "no-verdict-shape",
        ),
        "request-reply": (
            "clean",
            "refused-once",
            "refused-always",
            "taken-then-raised",
            "reply-lost",
        ),
        "transactional": (
            "clean",
            "refused-once",
            "refused-always",
            "taken-then-raised",
            "reply-lost",
            "autocommit",
        ),
        "local-write": ("clean", "refused-once", "refused-always", "taken-then-raised"),
        "socket-stream": (
            "clean",
            "refused-once",
            "refused-always",
            "taken-then-raised",
            "peer-closed",
        ),
        "per-event-ack": ("clean", "refused-once", "refused-always", "reply-lost"),
        "client-buffered": (
            "clean",
            "refused-once",
            "refused-always",
            "never-acknowledged",
            "report-indefinite",
        ),
        "in-process": ("clean", "refused-once", "refused-always", "taken-then-raised"),
        "wrapper": ("caller-code-raises",),
    }
)
"""The faults each class runs (SPEC-092 FR-002). A wrapper runs its inner variant's class's faults
first, then its own entry here, so `wrapper` lists only what a wrapper adds."""


@dataclass(frozen=True, kw_only=True)
class Variant:
    """One shipped sink class under one constructor variant, and the acknowledgement class it has.

    Attributes:
      sink: The class name under `sinks/`.
      variant: The variant's name, or `""` where the class has one variant.
      ack: Its acknowledgement class.
      build: The name of its builder in `tests/conformance_doubles.py`'s `BUILDERS`.
      picks: The constructor arguments that select the variant. A literal value is passed to the
        constructor as given; under `client` and `connection` the value names the double the
        builder injects, and under a wrapper's inner parameter it names the inner variant.
      wraps: A wrapper's inner variant, `"HTTPSink"` or `"CallbackSink"`.
      skips: The faults its class runs that this variant does not, each with its reason.
      only: The faults a variant that exists for one fault runs, in place of its class's.
      adds: The faults it runs beyond its class's, each with its reason (SPEC-093 FR-005).
    """

    sink: str
    variant: str
    ack: AckClass
    build: str
    picks: Mapping[str, object] = MappingProxyType({})
    wraps: str | None = None
    skips: Mapping[Fault, str] = MappingProxyType({})
    only: tuple[Fault, ...] = ()
    adds: Mapping[Fault, str] = MappingProxyType({})

    @property
    def id(self) -> str:
        """`<sink>[<variant>]`, or the bare class name for a class with one variant.

        Args:
          None.

        Returns:
          The variant's id.

        Raises:
          None.
        """
        return f"{self.sink}[{self.variant}]" if self.variant else self.sink


def _v(
    sink: str,
    variant: str,
    ack: AckClass,
    build: str,
    *,
    picks: Mapping[str, object] | None = None,
    wraps: str | None = None,
    skips: Mapping[Fault, str] | None = None,
    only: tuple[Fault, ...] = (),
    adds: Mapping[Fault, str] | None = None,
) -> Variant:
    """A `Variant` with its mappings frozen.

    Args:
      sink: The class name.
      variant: The variant's name, or `""`.
      ack: Its acknowledgement class.
      build: Its builder's name.
      picks: The constructor arguments that select it.
      wraps: A wrapper's inner variant.
      skips: The faults it does not run, with reasons.
      only: The faults it runs in place of its class's.
      adds: The faults it runs beyond its class's, with reasons.

    Returns:
      The variant.

    Raises:
      None.
    """
    return Variant(
        sink=sink,
        variant=variant,
        ack=ack,
        build=build,
        picks=MappingProxyType(dict(picks or {})),
        wraps=wraps,
        skips=MappingProxyType(dict(skips or {})),
        only=only,
        adds=MappingProxyType(dict(adds or {})),
    )


_AUTOCOMMIT_ELSEWHERE = "the autocommit variant runs it"
_NO_REPLY = "an in-process database has no reply to lose"
_WHOLE_DATAGRAM = "a datagram is sent whole or not at all"
_NO_FAILURE = "an append to an in-process list has no failure to script"
_CALLS_NOTHING = "it calls nothing"
_PING_IS_THE_CASE = "its never-acknowledged is a PING never answered, which is this case"
_NO_CALLER_CODE = "it runs no caller code"
_CONFIRMED_ADDS: Mapping[Fault, str] = {
    "taken-then-raised": "a confirmed publish writes its frame before it waits",
    "peer-closed": "a connection is held between batches",
}

VARIANTS: tuple[Variant, ...] = (
    _v("SQSSink", "standard", "keyed-reply", "sqs", picks={"fifo": False}),
    _v("SQSSink", "fifo", "keyed-reply", "sqs", picks={"fifo": True}),
    _v("SNSSink", "", "keyed-reply", "sns"),
    _v("MongoDBSink", "", "keyed-reply", "mongodb"),
    _v("KinesisSink", "", "positional-reply", "kinesis"),
    _v("FirehoseSink", "", "positional-reply", "firehose"),
    _v("ElasticsearchSink", "", "positional-reply", "elasticsearch"),
    _v("OpenSearchSink", "", "positional-reply", "opensearch"),
    _v("HoneycombSink", "", "positional-reply", "honeycomb"),
    _v("HTTPSink", "", "request-reply", "http"),
    _v("DatadogSink", "", "request-reply", "datadog"),
    _v("SplunkHECSink", "", "request-reply", "splunk"),
    _v("NewRelicSink", "", "request-reply", "newrelic"),
    _v("LokiSink", "", "request-reply", "loki"),
    _v("OTLPSink", "", "request-reply", "otlp"),
    _v("LogstashSink", "http", "request-reply", "logstash", picks={"url": "http://ls"}),
    _v("SentrySink", "http", "request-reply", "sentry", picks={"backend": "http"}),
    _v("ClickHouseSink", "", "request-reply", "clickhouse"),
    _v("AzureEventHubsSink", "", "request-reply", "eventhubs"),
    _v("RedisStreamsSink", "", "request-reply", "redis_streams"),
    _v("RedisListSink", "", "request-reply", "redis_list"),
    _v(
        "PostgresSink",
        "owned",
        "transactional",
        "postgres",
        picks={"dsn": "postgresql://h/db"},
        skips={"autocommit": _AUTOCOMMIT_ELSEWHERE},
    ),
    _v(
        "PostgresSink",
        "borrowed",
        "transactional",
        "postgres",
        picks={"connection": "borrowed"},
        skips={"autocommit": _AUTOCOMMIT_ELSEWHERE},
    ),
    _v(
        "PostgresSink",
        "autocommit",
        "transactional",
        "postgres",
        picks={"connection": "autocommit"},
        only=("autocommit",),
    ),
    _v(
        "SQLiteSink",
        "",
        "transactional",
        "sqlite",
        picks={"connection": "transactional"},
        skips={"reply-lost": _NO_REPLY, "autocommit": _AUTOCOMMIT_ELSEWHERE},
    ),
    _v(
        "SQLiteSink",
        "autocommit",
        "transactional",
        "sqlite",
        picks={"connection": "autocommit"},
        only=("autocommit",),
    ),
    _v("FileSink", "", "local-write", "file"),
    _v("RotatingFileSink", "", "local-write", "rotating_file"),
    _v("StdoutSink", "", "local-write", "stdout"),
    _v("StderrSink", "", "local-write", "stderr"),
    _v("SyslogSink", "tcp", "socket-stream", "syslog", picks={"transport": "tcp"}),
    _v(
        "SyslogSink",
        "udp",
        "socket-stream",
        "syslog",
        picks={"transport": "udp"},
        skips={"taken-then-raised": _WHOLE_DATAGRAM},
    ),
    _v(
        "LogstashSink",
        "tcp",
        "socket-stream",
        "logstash",
        picks={"host": "h", "transport": "tcp"},
    ),
    _v(
        "LogstashSink",
        "udp",
        "socket-stream",
        "logstash",
        picks={"host": "h", "transport": "udp"},
        skips={"taken-then-raised": _WHOLE_DATAGRAM},
    ),
    _v("RabbitMQSink", "owned", "socket-stream", "rabbitmq", picks={"url": "amqp://h/"}),
    _v("RabbitMQSink", "borrowed", "socket-stream", "rabbitmq", picks={"connection": "borrowed"}),
    _v(
        "RabbitMQSink",
        "confirmed",
        "per-event-ack",
        "rabbitmq",
        picks={"url": "amqp://h/", "confirm": True},
        adds=_CONFIRMED_ADDS,
    ),
    _v(
        "RabbitMQSink",
        "confirmed-borrowed",
        "per-event-ack",
        "rabbitmq",
        picks={"connection": "borrowed", "confirm": True},
        adds=_CONFIRMED_ADDS,
    ),
    _v(
        "NATSSink",
        "jetstream",
        "per-event-ack",
        "nats",
        picks={"servers": "nats://example:4222", "jetstream": True},
    ),
    _v(
        "NATSSink",
        "jetstream-injected",
        "per-event-ack",
        "nats",
        picks={"client": "injected", "jetstream": True},
    ),
    _v(
        "NATSSink",
        "core",
        "client-buffered",
        "nats",
        picks={"servers": "nats://example:4222", "jetstream": False},
        skips={"report-indefinite": _PING_IS_THE_CASE},
    ),
    _v(
        "NATSSink",
        "core-injected",
        "client-buffered",
        "nats",
        picks={"client": "injected", "jetstream": False},
        skips={"report-indefinite": _PING_IS_THE_CASE},
    ),
    _v("KafkaSink", "", "client-buffered", "kafka"),
    _v("GooglePubSubSink", "", "client-buffered", "pubsub"),
    _v("SentrySink", "sdk", "client-buffered", "sentry", picks={"backend": "sdk"}),
    _v("CallbackSink", "", "in-process", "callback"),
    _v("LoggingSink", "", "in-process", "logging"),
    _v(
        "MemorySink",
        "",
        "in-process",
        "memory",
        skips={
            "refused-once": _NO_FAILURE,
            "refused-always": _NO_FAILURE,
            "taken-then-raised": _NO_FAILURE,
        },
    ),
    _v(
        "NullSink",
        "",
        "in-process",
        "null",
        skips={
            "refused-once": _CALLS_NOTHING,
            "refused-always": _CALLS_NOTHING,
            "taken-then-raised": _CALLS_NOTHING,
        },
    ),
    _v(
        "FilteringSink",
        "over-http",
        "wrapper",
        "filtering",
        picks={"inner": "HTTPSink"},
        wraps="HTTPSink",
    ),
    _v(
        "FilteringSink",
        "over-callback",
        "wrapper",
        "filtering",
        picks={"inner": "CallbackSink"},
        wraps="CallbackSink",
    ),
    _v(
        "TransformSink",
        "over-http",
        "wrapper",
        "transform",
        picks={"inner": "HTTPSink"},
        wraps="HTTPSink",
    ),
    _v(
        "TransformSink",
        "over-callback",
        "wrapper",
        "transform",
        picks={"inner": "CallbackSink"},
        wraps="CallbackSink",
    ),
    _v(
        "MultiSink",
        "over-http",
        "wrapper",
        "multi",
        picks={"*sinks": "HTTPSink"},
        wraps="HTTPSink",
        skips={"caller-code-raises": _NO_CALLER_CODE},
    ),
    _v(
        "_FactorySink",
        "over-http",
        "wrapper",
        "factory",
        picks={"factory": "HTTPSink"},
        wraps="HTTPSink",
        skips={"caller-code-raises": _NO_CALLER_CODE},
    ),
)
"""The 53 variants of SPEC-092 FR-001. A variant is named only where the class, the outcome or a
departure differs, and `PostgresSink` and `RabbitMQSink` split on whether the sink built the
connection, since it reopens one it built and never one it was handed."""

ABSTRACT: Mapping[str, str] = MappingProxyType(
    {
        "_RedisSink": (
            "the private base of RedisStreamsSink and RedisListSink, which no caller builds; "
            "both subclasses are variants"
        ),
    }
)
"""Rostered classes with no variant, each with the reason none is built."""

PARAMETERS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "AzureEventHubsSink": ("producer", "connection_str", "eventhub", "max_retries"),
        "CallbackSink": ("fn", "on_close"),
        "ClickHouseSink": (
            "table",
            "client",
            "dsn",
            "create_table",
            "chunk_size",
            "max_retries",
            "send_receive_timeout",
        ),
        "DatadogSink": ("api_key", "site", "service", "ddtags", "**http_kwargs"),
        "ElasticsearchSink": ("url", "index", "auth", "**http_kwargs"),
        "FileSink": ("path", "encoding"),
        "FilteringSink": ("inner", "predicate", "min_level"),
        "FirehoseSink": ("delivery_stream", "client", "max_retries"),
        "GooglePubSubSink": ("topic", "client", "max_pending", "overflow_timeout"),
        "HTTPSink": (
            "url",
            "method",
            "headers",
            "auth",
            "body_format",
            "timeout",
            "gzip",
            "max_retries",
            "max_retry_after",
            "max_batch_count",
            "max_batch_bytes",
            "opener",
        ),
        "HoneycombSink": ("api_key", "dataset", "url", "**http_kwargs"),
        "KafkaSink": (
            "topic",
            "producer",
            "bootstrap_servers",
            "key_field",
            "flush_timeout",
            "producer_config",
        ),
        "KinesisSink": ("stream_name", "client", "partition_key_field", "max_retries"),
        "LoggingSink": ("logger", "default_level"),
        "LogstashSink": (
            "url",
            "host",
            "port",
            "transport",
            "body_format",
            "timeout",
            "max_retries",
            "max_datagram_bytes",
            "ack_wait",
            "**http_kwargs",
        ),
        "LokiSink": ("url", "labels", "**http_kwargs"),
        "MemorySink": ("maxlen",),
        "MongoDBSink": (
            "client",
            "uri",
            "database",
            "collection",
            "max_retries",
            "socket_timeout",
            "server_selection_timeout",
        ),
        "MultiSink": ("*sinks",),
        "NATSSink": (
            "subject",
            "client",
            "jetstream",
            "servers",
            "publish_timeout",
            "connect_timeout",
            "max_reconnect_attempts",
            "reconnect_time_wait",
            "drain_timeout",
        ),
        "NewRelicSink": ("api_key", "region", "**http_kwargs"),
        "NullSink": (),
        "OTLPSink": ("endpoint", "**http_kwargs"),
        "OpenSearchSink": ("url", "index", "auth", "**http_kwargs"),
        "PostgresSink": (
            "table",
            "connection",
            "dsn",
            "create_table",
            "chunk_size",
            "max_retries",
            "connect_timeout",
        ),
        "RabbitMQSink": (
            "exchange",
            "routing_key",
            "connection",
            "url",
            "max_retries",
            "blocked_connection_timeout",
            "socket_timeout",
            "stack_timeout",
            "confirm",
        ),
        "RedisListSink": ("key", "client", "url", "max_retries", "maxlen"),
        "RedisStreamsSink": ("stream", "client", "url", "max_retries", "maxlen"),
        "RotatingFileSink": ("path", "max_bytes", "backup_count", "when", "interval"),
        "SNSSink": ("topic_arn", "client", "max_retries", "max_message_bytes"),
        "SQLiteSink": ("database", "table", "connection", "create_table"),
        "SQSSink": (
            "queue_url",
            "client",
            "max_retries",
            "fifo",
            "message_group_id",
            "message_deduplication_id",
            "max_message_bytes",
        ),
        "SentrySink": ("dsn", "min_level", "backend", "client", "opener", "max_retries"),
        "SplunkHECSink": ("url", "token", "host", "source", "**http_kwargs"),
        "StderrSink": ("stream",),
        "StdoutSink": ("stream",),
        "SyslogSink": (
            "host",
            "port",
            "transport",
            "facility",
            "app_name",
            "timeout",
            "max_retries",
            "max_datagram_bytes",
            "ack_wait",
        ),
        "TransformSink": ("inner", "fn"),
        "_FactorySink": ("factory",),
    }
)
"""Every variant class's `__init__` parameter names, read by AST and inherited where a class
defines none (`ast_parameters`): 170 names over 39 classes. A `*` or `**` prefix marks a
variadic parameter. A new parameter fails the parameter test until it is a variant's pick or
declared in `NEUTRAL`."""


NEUTRAL: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "AzureEventHubsSink": frozenset({"connection_str", "eventhub", "max_retries", "producer"}),
        "CallbackSink": frozenset({"fn", "on_close"}),
        "ClickHouseSink": frozenset(
            {
                "chunk_size",
                "client",
                "create_table",
                "dsn",
                "max_retries",
                "send_receive_timeout",
                "table",
            }
        ),
        "DatadogSink": frozenset({"**http_kwargs", "api_key", "ddtags", "service", "site"}),
        "ElasticsearchSink": frozenset({"**http_kwargs", "auth", "index", "url"}),
        "FileSink": frozenset({"encoding", "path"}),
        "FilteringSink": frozenset({"min_level", "predicate"}),
        "FirehoseSink": frozenset({"client", "delivery_stream", "max_retries"}),
        "GooglePubSubSink": frozenset({"client", "max_pending", "overflow_timeout", "topic"}),
        "HTTPSink": frozenset(
            {
                "auth",
                "body_format",
                "gzip",
                "headers",
                "max_batch_bytes",
                "max_batch_count",
                "max_retries",
                "max_retry_after",
                "method",
                "opener",
                "timeout",
                "url",
            }
        ),
        "HoneycombSink": frozenset({"**http_kwargs", "api_key", "dataset", "url"}),
        "KafkaSink": frozenset(
            {
                "bootstrap_servers",
                "flush_timeout",
                "key_field",
                "producer",
                "producer_config",
                "topic",
            }
        ),
        "KinesisSink": frozenset({"client", "max_retries", "partition_key_field", "stream_name"}),
        "LoggingSink": frozenset({"default_level", "logger"}),
        "LogstashSink": frozenset(
            {
                "**http_kwargs",
                "ack_wait",
                "body_format",
                "max_datagram_bytes",
                "max_retries",
                "port",
                "timeout",
            }
        ),
        "LokiSink": frozenset({"**http_kwargs", "labels", "url"}),
        "MemorySink": frozenset({"maxlen"}),
        "MongoDBSink": frozenset(
            {
                "client",
                "collection",
                "database",
                "max_retries",
                "server_selection_timeout",
                "socket_timeout",
                "uri",
            }
        ),
        "MultiSink": frozenset(),
        "NATSSink": frozenset(
            {
                "connect_timeout",
                "drain_timeout",
                "max_reconnect_attempts",
                "publish_timeout",
                "reconnect_time_wait",
                "subject",
            }
        ),
        "NewRelicSink": frozenset({"**http_kwargs", "api_key", "region"}),
        "NullSink": frozenset(),
        "OTLPSink": frozenset({"**http_kwargs", "endpoint"}),
        "OpenSearchSink": frozenset({"**http_kwargs", "auth", "index", "url"}),
        "PostgresSink": frozenset(
            {
                "chunk_size",
                "connect_timeout",
                "create_table",
                "max_retries",
                "table",
            }
        ),
        "RabbitMQSink": frozenset(
            {
                "blocked_connection_timeout",
                "exchange",
                "max_retries",
                "routing_key",
                "socket_timeout",
                "stack_timeout",
            }
        ),
        "RedisListSink": frozenset({"client", "key", "max_retries", "maxlen", "url"}),
        "RedisStreamsSink": frozenset({"client", "max_retries", "maxlen", "stream", "url"}),
        "RotatingFileSink": frozenset({"backup_count", "interval", "max_bytes", "path", "when"}),
        "SNSSink": frozenset({"client", "max_message_bytes", "max_retries", "topic_arn"}),
        "SQLiteSink": frozenset({"create_table", "database", "table"}),
        "SQSSink": frozenset(
            {
                "client",
                "max_message_bytes",
                "max_retries",
                "message_deduplication_id",
                "message_group_id",
                "queue_url",
            }
        ),
        "SentrySink": frozenset({"client", "dsn", "max_retries", "min_level", "opener"}),
        "SplunkHECSink": frozenset({"**http_kwargs", "host", "source", "token", "url"}),
        "StderrSink": frozenset({"stream"}),
        "StdoutSink": frozenset({"stream"}),
        "SyslogSink": frozenset(
            {
                "ack_wait",
                "app_name",
                "facility",
                "host",
                "max_datagram_bytes",
                "max_retries",
                "port",
                "timeout",
            }
        ),
        "TransformSink": frozenset({"fn"}),
        "_FactorySink": frozenset(),
    }
)
"""Per class, the parameters that never change what the sink can know, each named here as a
verdict rather than derived, so a parameter a sink gains fails the parameter test until someone
decides whether it makes a variant. `max_message_bytes` is neutral for `SQSSink` and `SNSSink`: a
size limit changes which events are sent, never what the sink can know about one it sent. `ack_wait`
is neutral for `SyslogSink` and `LogstashSink` for another reason: it does change what a TCP sink can
know, but the corpus's fake socket cannot be polled and has no send queue to read, so both settings
read it alike (SPEC-093 FR-005, measured with the default flipped); its effect is held by
`tests/test_socket_close_corpus.py` instead. An
injected `client`, `opener` or `producer` is neutral too, except where it selects a variant, as
NATS' `client` beside `servers` does."""


def variant_named(name: str, variants: Iterable[Variant] = VARIANTS) -> Variant:
    """The variant whose id is `name`.

    Args:
      name: A variant id, such as `"HTTPSink"` or `"SQSSink[fifo]"`.
      variants: The table to search.

    Returns:
      The variant.

    Raises:
      KeyError: When no variant has that id.
    """
    for variant in variants:
        if variant.id == name:
            return variant
    raise KeyError(name)


def faults_of(variant: Variant, variants: Iterable[Variant] = VARIANTS) -> tuple[Fault, ...]:
    """The faults a variant runs, in order: `only` or else its class's, then `adds`, less `skips`.

    A wrapper's class's faults are its inner variant's class's, then `APPLIES["wrapper"]`.

    Args:
      variant: The variant.
      variants: The table its `wraps` is looked up in.

    Returns:
      The faults, each once.

    Raises:
      KeyError: When a wrapper's inner variant is not in the table.
    """
    if variant.only:
        base = variant.only
    elif variant.ack == "wrapper":
        assert variant.wraps is not None, f"{variant.id}: a wrapper names its inner variant"
        inner = variant_named(variant.wraps, variants)
        base = (*APPLIES[inner.ack], *APPLIES["wrapper"])
    else:
        base = APPLIES[variant.ack]
    base = (*base, *variant.adds)
    return tuple(fault for fault in base if fault not in variant.skips)


@dataclass(frozen=True)
class Row:
    """One variant under one fault script.

    Attributes:
      variant: The variant.
      fault: The fault script.
    """

    variant: Variant
    fault: Fault

    @property
    def id(self) -> str:
        """`<sink>[<variant>]-<fault>`, as a pin or departure names it.

        Args:
          None.

        Returns:
          The row's id.

        Raises:
          None.
        """
        return f"{self.variant.id}-{self.fault}"

    @property
    def batches(self) -> int:
        """Batches the row submits: two for `peer-closed`, which closes the peer between them.

        Args:
          None.

        Returns:
          1 or 2.

        Raises:
          None.
        """
        return 2 if self.fault == "peer-closed" else 1


def rows(variants: Iterable[Variant] = VARIANTS) -> tuple[Row, ...]:
    """Every row: each variant under each fault it runs, in table order.

    Args:
      variants: The variant table.

    Returns:
      The rows.

    Raises:
      None.
    """
    table = tuple(variants)
    return tuple(Row(variant, fault) for variant in table for fault in faults_of(variant, table))


def roster_gap(derived: Iterable[str], declared: Iterable[str]) -> list[str]:
    """One failure line per class name on either side alone.

    Args:
      derived: The class names the source defines (the roster).
      declared: The class names `VARIANTS` and `ABSTRACT` declare.

    Returns:
      The failure lines, sorted; empty when the sets are equal.

    Raises:
      None.
    """
    left, right = set(derived), set(declared)
    lines = [
        f"{name} ships under sinks/ with no entry in VARIANTS or ABSTRACT"
        for name in sorted(left - right)
    ]
    lines += [
        f"{name} has an entry in VARIANTS or ABSTRACT but no class under sinks/ defines it"
        for name in sorted(right - left)
    ]
    return lines


def ast_parameters(
    nodes: Mapping[str, ast.ClassDef],
    base_names: Callable[[ast.ClassDef], list[str]],
    names: Iterable[str],
) -> dict[str, tuple[str, ...]]:
    """Each named class's `__init__` parameter names, by AST, inherited where it defines none.

    The node map and the base-name reader are passed in, so the caller can hand it the ones
    `tests/test_public_surface.py` derives the sink roster from, and this module stays importable
    in front of an older tree. A base is followed recursively through the map; a class whose
    chain defines no `__init__` takes none.

    Args:
      nodes: Every class under `sinks/`, by name.
      base_names: The immediate base names of a class node.
      names: The classes to read.

    Returns:
      `{class name: parameter names}`, `self` left out, a variadic one prefixed `*` or `**`.

    Raises:
      KeyError: When a named class is not in the map.
    """

    def read(name: str, seen: frozenset[str]) -> tuple[str, ...] | None:
        """One class's parameters, following its bases until one defines `__init__`.

        Args:
          name: The class.
          seen: The classes already on this chain, so a cycle ends.

        Returns:
          The parameter names, or `None` when no class on the chain defines `__init__`.

        Raises:
          None.
        """
        node = nodes.get(name)
        if node is None or name in seen:
            return None
        for item in node.body:
            if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                args = item.args
                every = [*args.posonlyargs, *args.args, *args.kwonlyargs]
                found = [arg.arg for arg in every][1:]
                if args.vararg is not None:
                    found.append("*" + args.vararg.arg)
                if args.kwarg is not None:
                    found.append("**" + args.kwarg.arg)
                return tuple(found)
        for base in base_names(node):
            inherited = read(base, seen | {name})
            if inherited is not None:
                return inherited
        return None

    out: dict[str, tuple[str, ...]] = {}
    for name in names:
        if name not in nodes:
            raise KeyError(name)
        out[name] = read(name, frozenset()) or ()
    return out


def parameter_faults(
    derived: Mapping[str, tuple[str, ...]], recorded: Mapping[str, tuple[str, ...]]
) -> list[str]:
    """One line per class and parameter the source and `PARAMETERS` disagree on.

    Args:
      derived: The parameters read from the source.
      recorded: `PARAMETERS`.

    Returns:
      The failure lines; empty when the two agree.

    Raises:
      None.
    """
    lines: list[str] = []
    for name in sorted(set(derived) | set(recorded)):
        have, want = set(derived.get(name, ())), set(recorded.get(name, ()))
        lines += [f"{name} gained the parameter {p}" for p in sorted(have - want)]
        lines += [f"{name} lost the parameter {p}" for p in sorted(want - have)]
    return lines


def coverage_faults(
    parameters: Mapping[str, tuple[str, ...]],
    variants: Iterable[Variant],
    neutral: Mapping[str, frozenset[str]],
) -> list[str]:
    """One line per parameter that is neither a variant's pick nor neutral, or is both.

    Args:
      parameters: `PARAMETERS`.
      variants: The variant table.
      neutral: `NEUTRAL`.

    Returns:
      The failure lines; empty when, for every class, its parameters are exactly the union of its
      variants' `picks` keys and its `NEUTRAL` entry, and the two are disjoint.

    Raises:
      None.
    """
    table = tuple(variants)
    lines: list[str] = []
    for name in sorted(parameters):
        picked = {key for v in table if v.sink == name for key in v.picks}
        calm = set(neutral.get(name, frozenset()))
        have = set(parameters[name])
        lines += [
            f"{name}: the parameter {p} is neither a variant's pick nor in NEUTRAL"
            for p in sorted(have - picked - calm)
        ]
        lines += [
            f"{name}: {p} is a pick or in NEUTRAL but not a parameter"
            for p in sorted((picked | calm) - have)
        ]
        lines += [
            f"{name}: the parameter {p} is both a variant's pick and in NEUTRAL"
            for p in sorted(picked & calm)
        ]
    return lines


def adds_faults(variants: Iterable[Variant]) -> list[str]:
    """One line per misuse of `adds`, naming the variant and the fault; empty when there is none.

    A fault in `adds` must be one its class does not already run, on a variant that does not set
    `only`, and not one its `skips` removes again, and it must be a `Fault` at all (SPEC-093
    FR-005). Each of those would make `adds` say something `faults_of` does not do.

    Args:
      variants: The variant table.

    Returns:
      The failure lines.

    Raises:
      None.
    """
    table = tuple(variants)
    lines: list[str] = []
    for variant in table:
        if variant.ack == "wrapper":
            inner = next((v for v in table if v.id == variant.wraps), None)
            runs = (*APPLIES[inner.ack], *APPLIES["wrapper"]) if inner is not None else ()
        else:
            runs = APPLIES[variant.ack]
        for fault in variant.adds:
            if fault not in FAULTS:
                lines.append(f"{variant.id} adds {fault!r}, which is not a fault")
            elif variant.only:
                lines.append(f"{variant.id} adds {fault} beside only, which replaces its faults")
            elif fault in runs:
                lines.append(f"{variant.id} adds {fault}, which its class already runs")
            if fault in variant.skips:
                lines.append(f"{variant.id} adds {fault} and also skips it")
    return lines
