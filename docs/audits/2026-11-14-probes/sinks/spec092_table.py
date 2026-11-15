"""SPEC-092 prototype: every shipped sink class, its variants, its acknowledgement class, its rows.

Written on 2026-11-15 while authoring SPEC-092, against 6bd0b66e; not one of the audit's probes.
`VARIANTS` is the declaration the spec puts in `tests/conformance/classes.py`: one entry per
shipped sink class and constructor variant, each naming exactly one class. `NOT_RUN` names the
faults a variant skips although its class runs them, each with its reason.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import spec092_shims as shims
from spec092_corpus import APPLIES, Backend, Row

Build = Callable[[Backend], tuple[object, object]]


def _same(make: Callable[[Backend], object]) -> Build:
    def build(backend: Backend) -> tuple[object, object]:
        sink = make(backend)
        return sink, sink

    return build


def _patch(module: str, name: str, value: Any) -> None:
    import importlib

    setattr(importlib.import_module(module), name, value)


def _sqs(fifo: bool) -> Build:
    def make(backend: Backend) -> object:
        from log_foundry.sinks.sqs import SQSSink

        url = "https://sqs/q.fifo" if fifo else "https://sqs/q"
        return SQSSink(url, client=shims.SQSClient(backend, fifo=fifo))

    return _same(make)


def _http(cls_path: str, *args: Any, reply: str = "whole", **kwargs: Any) -> Build:
    def make(backend: Backend) -> object:
        import importlib

        module, name = cls_path.rsplit(".", 1)
        cls = getattr(importlib.import_module(module), name)
        return cls(*args, opener=shims.Opener(backend, reply), **kwargs)

    return _same(make)


def _socket_sink(cls_path: str, transport: str, **kwargs: Any) -> Build:
    def make(backend: Backend) -> object:
        import importlib

        import log_foundry.sinks._socket as socket_mod

        state = {"generation": 0, "blind": 0}
        saved = (socket_mod._make_udp, socket_mod._make_tcp)

        def restore() -> None:
            socket_mod._make_udp, socket_mod._make_tcp = saved

        backend.on_close.append(restore)
        if transport == "udp":
            socket_mod._make_udp = lambda host: shims.UDPSocket(backend)  # type: ignore[assignment]
        else:

            def tcp(host: str, port: int, timeout: float) -> object:
                return shims.TCPSocket(backend, state)

            socket_mod._make_tcp = tcp  # type: ignore[assignment]
        module, name = cls_path.rsplit(".", 1)
        cls = getattr(importlib.import_module(module), name)
        return cls(transport=transport, **kwargs)

    return _same(make)


def _eventhubs(backend: Backend) -> object:
    from log_foundry.sinks import eventhubs
    from log_foundry.sinks.eventhubs import AzureEventHubsSink

    saved = eventhubs._event_data_cls
    eventhubs._event_data_cls = lambda: lambda body: body  # type: ignore[assignment]
    backend.on_close.append(lambda: setattr(eventhubs, "_event_data_cls", saved))
    return AzureEventHubsSink(producer=shims.EHProducer(backend))


def _postgres(autocommit: bool) -> Build:
    def make(backend: Backend) -> object:
        from log_foundry.sinks.postgres import PostgresSink

        conn = shims.PGConnection(backend, autocommit=autocommit)
        return PostgresSink("t", connection=conn, chunk_size=3)

    return _same(make)


def _sqlite(autocommit: bool) -> Build:
    def make(backend: Backend) -> object:
        from log_foundry.sinks.sqlite import SQLiteSink

        proxy = shims.SQLiteProxy(backend, autocommit=autocommit)
        sink = SQLiteSink(":memory:", connection=proxy.real)
        sink._conn = proxy  # type: ignore[assignment]
        return sink

    return _same(make)


def _file(rotating: bool) -> Build:
    def make(backend: Backend) -> object:
        import shutil
        import tempfile

        from log_foundry.sinks.file import FileSink, RotatingFileSink

        directory = tempfile.mkdtemp(prefix="spec092-")
        backend.on_close.append(lambda: shutil.rmtree(directory, ignore_errors=True))
        path = directory + "/f.ndjson"
        sink: Any = (
            RotatingFileSink(path, max_bytes=10**9, backup_count=1) if rotating else FileSink(path)
        )
        sink._stream.close()
        sink._stream = shims.FileHandle(backend)
        return sink

    return _same(make)


def _stream(name: str) -> Build:
    def make(backend: Backend) -> object:
        from log_foundry.sinks import stdout

        return getattr(stdout, name)(stream=shims.Stream(backend))

    return _same(make)


def _nats(jetstream: bool, owned: bool) -> Build:
    """`owned` builds the sink over `servers=`, with a `nats` module whose `connect` returns the
    double, as `tests/nats_engine_matrix_harness.py` does: only a client the sink connected itself
    is confirmed by a round trip (`NATSSink._probing`)."""

    def make(backend: Backend) -> object:
        import sys
        import types

        from log_foundry.sinks.nats import NATSSink

        client = shims.NATSClient(backend)
        if not owned:
            return NATSSink("logs", client=client, jetstream=jetstream)
        module = types.ModuleType("nats")

        async def connect(*args: Any, **kwargs: Any) -> object:
            return client

        module.connect = connect  # type: ignore[attr-defined]
        saved = sys.modules.get("nats")
        sys.modules["nats"] = module
        try:
            return NATSSink(
                "logs", servers="nats://example:4222", jetstream=jetstream, drain_timeout=0.2
            )
        finally:
            if saved is None:
                sys.modules.pop("nats", None)
            else:
                sys.modules["nats"] = saved

    return _same(make)


def _sentry(backend_name: str) -> Build:
    def make(backend: Backend) -> object:
        from log_foundry.sinks.sentry import SentrySink

        if backend_name == "sdk":
            return SentrySink(backend="sdk", client=shims.SentrySDK(backend), min_level="DEBUG")
        return SentrySink(
            "https://key@o0.ingest.sentry.io/1",
            backend="http",
            opener=shims.Opener(backend),
            min_level="DEBUG",
        )

    return _same(make)


def _memory(backend: Backend) -> object:
    from log_foundry.sinks.memory import MemorySink

    sink = MemorySink()
    backend.on_close.append(lambda: [backend.take(shims.tokens(e)) for e in sink.events])
    return sink


def _inner_http(backend: Backend) -> object:
    from log_foundry.sinks.http import HTTPSink

    return HTTPSink("http://x/in", opener=shims.Opener(backend))


def _inner_callback(backend: Backend) -> object:
    from log_foundry.sinks.callback import CallbackSink

    return CallbackSink(shims.callback(backend))


def _raising_on(token_index: int) -> Callable[[dict[str, object]], Any]:
    def fn(event: dict[str, object]) -> Any:
        if str(event.get("log_id", "")).endswith(f"{token_index:02d}"):
            raise ValueError("caller code failed for this event")
        return event

    return fn


def _filtering(inner: Callable[[Backend], object]) -> Build:
    def build(backend: Backend) -> tuple[object, object]:
        from log_foundry.sinks.filtering import FilteringSink

        if backend.fault == "caller-code-raises":
            raising = _raising_on(3)
            predicate = lambda e: bool(raising(e))  # noqa: E731
        else:
            predicate = lambda e: True  # noqa: E731
        sink = FilteringSink(inner(backend), predicate=predicate)
        return sink, sink

    return build


def _transform(inner: Callable[[Backend], object]) -> Build:
    def build(backend: Backend) -> tuple[object, object]:
        from log_foundry.sinks.transform import TransformSink

        fn = _raising_on(3) if backend.fault == "caller-code-raises" else (lambda e: e)
        sink = TransformSink(inner(backend), fn)
        return sink, sink

    return build


def _multi(backend: Backend) -> tuple[object, object]:
    from log_foundry.sinks.http import HTTPSink
    from log_foundry.sinks.multi import MultiSink

    clean = Backend("clean")
    first = HTTPSink("http://a/in", opener=shims.Opener(clean))
    judged = HTTPSink("http://b/in", opener=shims.Opener(backend))
    return MultiSink(first, judged), judged


def _factory(backend: Backend) -> tuple[object, object]:
    from log_foundry.sinks._factory import _FactorySink

    judged = _inner_http(backend)
    return _FactorySink(lambda: judged), judged


VARIANTS: dict[tuple[str, str], tuple[str, Build]] = {
    ("SQSSink", "standard"): ("keyed-reply", _sqs(False)),
    ("SQSSink", "fifo"): ("keyed-reply", _sqs(True)),
    ("SNSSink", ""): (
        "keyed-reply",
        _same(
            lambda b: __import__("log_foundry.sinks.sns", fromlist=["SNSSink"]).SNSSink(
                "arn:aws:sns:r:1:t", client=shims.SNSClient(b)
            )
        ),
    ),
    ("MongoDBSink", ""): (
        "keyed-reply",
        _same(
            lambda b: __import__("log_foundry.sinks.mongodb", fromlist=["x"]).MongoDBSink(
                client=shims.MongoClient(b), database="d", collection="c"
            )
        ),
    ),
    ("KinesisSink", ""): (
        "positional-reply",
        _same(
            lambda b: __import__("log_foundry.sinks.kinesis", fromlist=["x"]).KinesisSink(
                "s", client=shims.KinesisClient(b)
            )
        ),
    ),
    ("FirehoseSink", ""): (
        "positional-reply",
        _same(
            lambda b: __import__("log_foundry.sinks.firehose", fromlist=["x"]).FirehoseSink(
                "d", client=shims.FirehoseClient(b)
            )
        ),
    ),
    ("ElasticsearchSink", ""): (
        "positional-reply",
        _http(
            "log_foundry.sinks.elasticsearch.ElasticsearchSink",
            "http://es",
            index="i",
            reply="bulk",
        ),
    ),
    ("OpenSearchSink", ""): (
        "positional-reply",
        _http(
            "log_foundry.sinks.elasticsearch.OpenSearchSink", "http://os", index="i", reply="bulk"
        ),
    ),
    ("HoneycombSink", ""): (
        "positional-reply",
        _http(
            "log_foundry.sinks.honeycomb.HoneycombSink",
            "k",
            "ds",
            url="http://hc",
            reply="statuses",
        ),
    ),
    ("HTTPSink", ""): ("request-reply", _http("log_foundry.sinks.http.HTTPSink", "http://x/in")),
    ("DatadogSink", ""): ("request-reply", _http("log_foundry.sinks.datadog.DatadogSink", "k")),
    ("SplunkHECSink", ""): (
        "request-reply",
        _http("log_foundry.sinks.splunk.SplunkHECSink", "http://splunk", "tok"),
    ),
    ("NewRelicSink", ""): ("request-reply", _http("log_foundry.sinks.newrelic.NewRelicSink", "k")),
    ("LokiSink", ""): ("request-reply", _http("log_foundry.sinks.loki.LokiSink", "http://loki")),
    ("OTLPSink", ""): (
        "request-reply",
        _http("log_foundry.sinks.otlp.OTLPSink", "http://otlp/v1/logs"),
    ),
    ("LogstashSink", "http"): (
        "request-reply",
        _http("log_foundry.sinks.logstash.LogstashSink", "http://ls"),
    ),
    ("SentrySink", "http"): ("request-reply", _sentry("http")),
    ("ClickHouseSink", ""): (
        "request-reply",
        _same(
            lambda b: __import__("log_foundry.sinks.clickhouse", fromlist=["x"]).ClickHouseSink(
                "t", client=shims.ClickHouseClient(b)
            )
        ),
    ),
    ("AzureEventHubsSink", ""): ("request-reply", _same(_eventhubs)),
    ("RedisStreamsSink", ""): (
        "request-reply",
        _same(
            lambda b: __import__("log_foundry.sinks.redis", fromlist=["x"]).RedisStreamsSink(
                "s", client=shims.RedisClient(b)
            )
        ),
    ),
    ("RedisListSink", ""): (
        "request-reply",
        _same(
            lambda b: __import__("log_foundry.sinks.redis", fromlist=["x"]).RedisListSink(
                "k", client=shims.RedisClient(b)
            )
        ),
    ),
    ("PostgresSink", ""): ("transactional", _postgres(False)),
    ("PostgresSink", "autocommit"): ("transactional", _postgres(True)),
    ("SQLiteSink", ""): ("transactional", _sqlite(False)),
    ("SQLiteSink", "autocommit"): ("transactional", _sqlite(True)),
    ("FileSink", ""): ("local-write", _file(False)),
    ("RotatingFileSink", ""): ("local-write", _file(True)),
    ("StdoutSink", ""): ("local-write", _stream("StdoutSink")),
    ("StderrSink", ""): ("local-write", _stream("StderrSink")),
    ("SyslogSink", "tcp"): (
        "socket-stream",
        _socket_sink("log_foundry.sinks.syslog.SyslogSink", "tcp", host="h", port=514),
    ),
    ("SyslogSink", "udp"): (
        "socket-stream",
        _socket_sink("log_foundry.sinks.syslog.SyslogSink", "udp", host="h", port=514),
    ),
    ("LogstashSink", "tcp"): (
        "socket-stream",
        _socket_sink("log_foundry.sinks.logstash.LogstashSink", "tcp", host="h", port=5000),
    ),
    ("LogstashSink", "udp"): (
        "socket-stream",
        _socket_sink("log_foundry.sinks.logstash.LogstashSink", "udp", host="h", port=5000),
    ),
    ("RabbitMQSink", ""): (
        "socket-stream",
        _same(
            lambda b: __import__("log_foundry.sinks.rabbitmq", fromlist=["x"]).RabbitMQSink(
                exchange="x", routing_key="k", connection=shims.RabbitConnection(b)
            )
        ),
    ),
    ("NATSSink", "jetstream"): ("per-event-ack", _nats(True, owned=True)),
    ("NATSSink", "jetstream-injected"): ("per-event-ack", _nats(True, owned=False)),
    ("NATSSink", "core"): ("client-buffered", _nats(False, owned=True)),
    ("NATSSink", "core-injected"): ("client-buffered", _nats(False, owned=False)),
    ("KafkaSink", ""): (
        "client-buffered",
        _same(
            lambda b: __import__("log_foundry.sinks.kafka", fromlist=["x"]).KafkaSink(
                "t", producer=shims.KafkaProducer(b)
            )
        ),
    ),
    ("GooglePubSubSink", ""): (
        "client-buffered",
        _same(
            lambda b: __import__("log_foundry.sinks.pubsub", fromlist=["x"]).GooglePubSubSink(
                "projects/p/topics/t", client=shims.PubSubPublisher(b)
            )
        ),
    ),
    ("SentrySink", "sdk"): ("client-buffered", _sentry("sdk")),
    ("CallbackSink", ""): ("in-process", _same(_inner_callback)),
    ("LoggingSink", ""): (
        "in-process",
        _same(
            lambda b: __import__("log_foundry.sinks.logging_sink", fromlist=["x"]).LoggingSink(
                shims.logger(b)
            )
        ),
    ),
    ("MemorySink", ""): ("in-process", _same(_memory)),
    ("NullSink", ""): (
        "in-process",
        _same(lambda b: __import__("log_foundry.sinks.null", fromlist=["x"]).NullSink()),
    ),
    ("FilteringSink", "over-http"): ("wrapper", _filtering(_inner_http)),
    ("FilteringSink", "over-callback"): ("wrapper", _filtering(_inner_callback)),
    ("TransformSink", "over-http"): ("wrapper", _transform(_inner_http)),
    ("TransformSink", "over-callback"): ("wrapper", _transform(_inner_callback)),
    ("MultiSink", "over-http"): ("wrapper", _multi),
    ("_FactorySink", "over-http"): ("wrapper", _factory),
}

WRAPS: dict[tuple[str, str], str] = {
    ("FilteringSink", "over-http"): "request-reply",
    ("FilteringSink", "over-callback"): "in-process",
    ("TransformSink", "over-http"): "request-reply",
    ("TransformSink", "over-callback"): "in-process",
    ("MultiSink", "over-http"): "request-reply",
    ("_FactorySink", "over-http"): "request-reply",
}
"""The class whose faults a wrapper variant runs, beside its own `caller-code-raises`."""

NOT_RUN: dict[tuple[str, str], dict[str, str]] = {
    ("SQLiteSink", ""): {
        "reply-lost": "an in-process database has no reply to lose",
        "autocommit": "the autocommit variant runs it",
    },
    ("SQLiteSink", "autocommit"): {"reply-lost": "an in-process database has no reply to lose"},
    ("SyslogSink", "udp"): {"taken-then-raised": "a datagram is sent whole or not at all"},
    ("LogstashSink", "udp"): {"taken-then-raised": "a datagram is sent whole or not at all"},
    ("MemorySink", ""): {
        "refused-once": "an append to an in-process list has no failure to script",
        "refused-always": "an append to an in-process list has no failure to script",
        "taken-then-raised": "an append to an in-process list has no failure to script",
    },
    ("NullSink", ""): {
        "refused-once": "it calls nothing",
        "refused-always": "it calls nothing",
        "taken-then-raised": "it calls nothing",
    },
    ("PostgresSink", ""): {"autocommit": "the autocommit variant runs it"},
}

ONLY: dict[tuple[str, str], tuple[str, ...]] = {
    ("PostgresSink", "autocommit"): ("autocommit",),
    ("SQLiteSink", "autocommit"): ("autocommit",),
}
"""A variant that exists for one fault runs only that fault."""


def rows() -> list[Row]:
    out: list[Row] = []
    for (sink, variant), (klass, build) in VARIANTS.items():
        faults = APPLIES[WRAPS[(sink, variant)]] if klass == "wrapper" else APPLIES[klass]
        if klass == "wrapper" and sink in ("FilteringSink", "TransformSink"):
            faults = (*faults, "caller-code-raises")
        if (sink, variant) in ONLY:
            faults = ONLY[(sink, variant)]
        skip = NOT_RUN.get((sink, variant), {})
        for fault in faults:
            if fault in skip:
                continue
            batches = 2 if fault == "peer-closed" else 1
            out.append(Row(sink, variant, klass, fault, build, batches))
    return out
