# log-forge

Consistent, structured (JSON) logs for every decorated function call — correlated by shared
trace/span IDs, ready to ship to any of 30-plus built-in sinks (stdout by default; SQS → ELK is
the headline production path).

`log-forge` owns the **logs** pillar of observability. You decorate a function with `@trace`;
it emits one identically-shaped JSON record when the call starts and another when it ends
(with duration and status), stitched together by W3C-compatible trace and span IDs so nested
calls form a tree you can query later.

- **Zero runtime dependencies** — the core pulls in nothing; every sink that needs a third-party client (boto3, kafka, redis, …) sits behind its own optional extra, lazily imported.
- **Fully typed** — `mypy --strict`, ships a PEP 561 `py.typed` marker.
- **Structured, never free-form** — every event is the same named-field JSON shape.
- **Safe by default** — never captures your arguments or return values (no accidental PII/secret leakage), and the decorator **never swallows exceptions**.
- **Correct under threads and asyncio** — context propagates via `contextvars`.
- **Non-blocking delivery** — finished spans are handed to a background worker; your code never
  blocks on sink I/O, and a graceful drain at exit means buffered events aren't lost.

---

## Requirements

- **Python ≥ 3.13**

## Installation

`log-forge` is not yet published to PyPI. Install from source:

```bash
# from a clone of this repo
poetry install                 # or: pip install .

# with an optional sink extra (e.g. the AWS sinks)
poetry install -E aws          # or: pip install '.[aws]'
```

Once published, the intended install will be:

```bash
pip install log-foundry          # core, zero dependencies
pip install 'log-foundry[aws]'   # + boto3 for the SQS/SNS/Kinesis/Firehose sinks
```

> **Note the name split.** The distribution is **`log-foundry`** on PyPI, but the import name
> stays **`log_forge`** — `pip install log-foundry`, then `import log_forge`. PyPI rejects
> `log-forge` as too similar to the unrelated, pre-existing `logforge` project.

### Optional extras

The core is dependency-free. Each sink built on a third-party client lives behind its own extra
(the client is imported lazily, only when you construct that sink). All other sinks — stdout,
file, SQLite, the stdlib-`logging` bridge, and every HTTP/socket platform sink (Elasticsearch,
Loki, Logstash, Syslog, Datadog, Splunk, New Relic, Honeycomb) — need **no** extra.

| Extra | Installs | Enables |
|---|---|---|
| `aws` | `boto3` | `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink` |
| `sentry` | `sentry-sdk` | `SentrySink` via the SDK (a raw-HTTP fallback works without it) |
| `kafka` | `confluent-kafka` | `KafkaSink` |
| `redis` | `redis` | `RedisStreamsSink`, `RedisListSink` |
| `amqp` | `pika` | `RabbitMQSink` |
| `nats` | `nats-py` | `NATSSink` |
| `gcp-pubsub` | `google-cloud-pubsub` | `GooglePubSubSink` |
| `azure-eventhubs` | `azure-eventhub` | `AzureEventHubsSink` |
| `mongo` | `pymongo` | `MongoDBSink` |
| `postgres` | `psycopg[binary]` | `PostgresSink` |
| `clickhouse` | `clickhouse-connect` | `ClickHouseSink` |

## Quickstart

```python
import log_forge as lf

# Call once at startup. These values are stamped onto every event.
lf.configure(service="billing-api", version="1.4.2", env="prod")

@lf.trace
def charge(order_id: str) -> int:
    return compute_tax(order_id)

@lf.trace(name="tax.compute", defaults={"component": "tax"})
def compute_tax(order_id: str) -> int:
    return 42

charge("ord_123")
```

With no sink configured, events are written as JSON lines to stdout. The call above emits
four events — a `span.start` / `span.end` pair per function — all sharing one `trace_id`,
with the child span pointing at its parent via `parent_span_id`:

```json
{"timestamp": "2026-07-10T00:57:10.411Z", "level": "INFO", "message": "span.start", "trace_id": "8ab2add1480f8f6a52fe97cd23ae6f36", "span_id": "6aeb63c0eba85bf4", "parent_span_id": "b02197e75f40eb81", "log_id": "754adb40e10c445f9ec9e23a2f3dcbf2", "function": "tax.compute", "service": "billing-api", "version": "1.4.2", "env": "prod", "fields": {"component": "tax"}}
{"timestamp": "2026-07-10T00:57:10.411Z", "level": "INFO", "message": "span.end", "trace_id": "8ab2add1480f8f6a52fe97cd23ae6f36", "span_id": "6aeb63c0eba85bf4", "parent_span_id": "b02197e75f40eb81", "log_id": "3af73c51540848afbeaba9fdf7a9dce8", "function": "tax.compute", "service": "billing-api", "version": "1.4.2", "env": "prod", "fields": {"component": "tax"}, "duration_ms": 0.018, "status": "ok"}
{"timestamp": "2026-07-10T00:57:10.411Z", "level": "INFO", "message": "span.start", "trace_id": "8ab2add1480f8f6a52fe97cd23ae6f36", "span_id": "b02197e75f40eb81", "parent_span_id": null, "log_id": "e789f7e5268b46d8b779c9cbcdde8656", "function": "charge", "service": "billing-api", "version": "1.4.2", "env": "prod", "fields": {}}
{"timestamp": "2026-07-10T00:57:10.411Z", "level": "INFO", "message": "span.end", "trace_id": "8ab2add1480f8f6a52fe97cd23ae6f36", "span_id": "b02197e75f40eb81", "parent_span_id": null, "log_id": "8f3dbfcfcf4a45f688c738eefef882b0", "function": "charge", "service": "billing-api", "version": "1.4.2", "env": "prod", "fields": {}, "duration_ms": 0.326, "status": "ok"}
```

> **Note on ordering:** the child span (`tax.compute`) finishes first, so its events flush
> before the parent's. Correlate by `trace_id` / `parent_span_id`, not by line order.

## How it works

![log-forge pipeline: a traced call opens a span, gathers events, then closes and hands off to a background worker that batches events and ships them to a sink. Steps 1–4 run on your thread; the worker and sink run on a background thread. Support modules — config, ids, model, context, console — assist every step.](docs/assets/pipeline.svg)

A traced call travels through a small pipeline. The first four steps run on your own thread
and are deliberately fast; the last two run on a background thread so your code never waits on
the destination.

1. **You call the code** — a `@trace` function, or one of the `debug`/`info`/… emitters.
2. **A span opens** — a record of this one call. It inherits the current trace and parent (see
   below), or starts a fresh trace if nothing is active.
3. **Events gather on the span** — an automatic `span.start`, then any events you emit, held in
   memory as one bundle rather than written line by line.
4. **The span closes and hands off** — on return *or* exception, a `span.end` event is added
   (with duration and status), and the whole bundle is handed to the background worker. This
   hand-off is instant and never blocks; on an exception the original error is re-raised unchanged.
5. **The worker batches** — the worker groups bundles and flushes them together (see below).
6. **The sink ships them out** — `StdoutSink` by default, or `SQSSink` in production.

Supporting this path are a handful of single-concept modules: `config` (the process-wide
`service`/`version`/`env` and the sink), `ids` (trace/span/log ids), `model` (assembles the one
JSON shape), `context` (holds the current span and baggage), and `console` (the optional
instant `echo=` line).

### Building the trace tree

Nested calls form a tree through a stack of open spans kept in a `contextvars` context — the
top of the stack is the "current" span. When a traced function starts, it reads the current
span: if one exists, the new span copies its `trace_id` and records its `span_id` as
`parent_span_id`; if the stack is empty, the new span starts a fresh trace with no parent. The
new span is then pushed, so anything it calls sees *it* as the parent. On exit the span is
popped by restoring the stack to its exact prior state (via a token, not a blind pop), which
stays correct even when code branches into concurrent tasks. Because the stack lives in a
context variable, every thread and asyncio task gets its own isolated copy — so `asyncio.gather`
children share their parent's trace, and baggage set in one task never leaks into a sibling.

### When the worker flushes

The worker is one background thread with a bounded queue in front of it. `submit` drops a
finished span's events into the queue and returns; the worker drains the queue into a small
pending pile and flushes that pile to the sink on whichever of two triggers fires first:

- **By count** — once ~10 span bundles have accumulated (note: that's 10 *spans*, and each span
  carries at least its start/end pair, so a flush is usually well over 10 records). All pending
  bundles are flattened into a single `sink.emit` call.
- **By time** — once ~1 second has passed since the last flush, so an idle app never holds logs
  indefinitely. (The loop advances its flush timestamp even when idle, so an empty queue sleeps
  quietly instead of busy-spinning.)

A failing `sink.emit` is retried a few times with growing backoff; past that the batch is
abandoned with a counted warning and draining continues — a broken sink degrades logging but
never crashes the worker or the app. If the bounded queue fills completely, new submissions are
dropped (newest-first) and counted rather than blocking your code. On `shutdown()` the worker
stops, sweeps anything still queued into one final batch, emits it, and closes the sink.

## Usage

### `configure(...)`

Set process-wide settings once at startup. Every argument is keyword-only and optional;
repeated calls **compose** (only what you pass is applied) rather than reset.

```python
lf.configure(
    service="billing-api",     # stamped as "service" on every event
    version="1.4.2",           # stamped as "version"
    env="prod",                # stamped as "env"
    sink=MyCustomSink(),       # defaults to StdoutSink if never set
    defaults={"region": "us-east-1"},  # base fields merged into every event's "fields"
)
```

If you never set a `sink`, the first decorated call falls back to `StdoutSink()`, so `@trace`
works with zero configuration.

### `@trace`

Decorate any **synchronous** function. Usable bare or with arguments:

```python
@lf.trace                                  # span name = func.__qualname__
def handler(): ...

@lf.trace(name="checkout", defaults={"component": "cart"})
def process(): ...
```

- `name` — override the span name (defaults to the function's `__qualname__`).
- `defaults` — per-decorator fields merged into every event this span emits.

The **outermost** decorated call starts a new trace; every nested decorated call becomes a
child span within it. On an exception, the decorator records `status="error"` plus the
exception type and formatted stack, then **re-raises the original exception unchanged** — it
never swallows errors.

**Async is supported.** Apply `@trace` to an `async def` and it traces the coroutine's actual
run — the span opens when the coroutine starts and closes when the awaits complete, not when the
coroutine object is created. `contextvars` keeps the trace correct across `await` points and
concurrent tasks: children awaited under one parent (e.g. via `asyncio.gather`) share the
parent's `trace_id` and link to its `span_id`, and baggage set in one task never leaks into a
sibling. A cancelled coroutine is recorded as `status="error"` and the `CancelledError` re-raised.

```python
@lf.trace
async def fetch(user_id: int) -> dict:
    lf.info("fetching", user_id=user_id)
    return await load(user_id)

@lf.trace
async def load(user_id: int) -> dict:
    ...

await fetch(4127)   # one trace_id; load's parent_span_id == fetch's span_id
```

### Logging inside a span

Emit your own structured events from inside a decorated call with the level functions
`debug` / `info` / `warning` / `error` / `critical`. Each appends one event to the current
span, so the whole call's logs flush together and share its `trace_id` / `span_id`. Keyword
arguments land in the event's `fields`; the function name is captured, but arguments and
return values never are.

```python
@lf.trace
def process_payment(user_id: int) -> str:
    lf.set_baggage(request_id="req-123")     # rides every event emitted below, in this trace
    lf.info("charging card", user_id=user_id)
    lf.info("payment complete", echo=True)   # also printed to the console, immediately
    return "ok"
```

- **`set_baggage(**kv)`** — attach trace-scoped context that is merged into the `fields` of
  every subsequent event in the same execution flow. Precedence, lowest to highest: config
  `defaults` → span `defaults` → baggage → per-call `fields`.
- **`echo=True`** — *additionally* write a human-readable `LEVEL   message` line to the console
  (`sys.stderr` by default), synchronously, without waiting for the async flush. The event
  still rides the normal pipeline to the sink — echo never redirects.
- **Orphan logs** — a level call made with no active span is not dropped: it emits a standalone
  one-event span with a fresh `trace_id`, flushed straight to the sink.

### Sinks

A **sink** is the swappable output transport — any object satisfying the `Sink` protocol. It
receives already-built, batched event dicts and knows nothing about spans or context:

```python
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None: ...
    def close(self) -> None: ...
```

Wire one up by passing an instance to `configure(sink=...)`; if you never do, the first decorated
call falls back to `StdoutSink()`. Sinks are **not** re-exported at the top level — import each
from its own module, e.g. `from log_forge.sinks.sqs import SQSSink`.

A few conventions hold across every sink below:

- **Extras.** The core is dependency-free. A sink built on a third-party client sits behind the
  optional extra named in its table (blank = zero-dependency, stdlib only); the client is imported
  lazily, so `import log_forge.sinks.<x>` never fails for a missing dependency — only *constructing*
  the sink without an injected client does. See [Optional extras](#optional-extras).
- **Injection.** Sinks backed by an external resource accept an injected client/connection/stream
  (`client=`, `connection=`, `producer=`, `stream=`, `opener=`) for testing or bespoke configuration.
  The tables show the destination-defining arguments only; sinks that retry also take `max_retries`.
- **Ownership.** A resource the sink opens itself is closed on `shutdown()`; an injected one is left
  open for you to manage.
- **Never crashes the app.** A failing sink is retried with backoff and then counted (`.failed`,
  `.dropped_oversized`, …) rather than raised — a broken destination degrades logging, nothing more.

#### Built-in, zero-dependency

| Sink | Import from | Configure |
|---|---|---|
| `StdoutSink` | `log_forge.sinks.stdout` | `StdoutSink(stream=sys.stdout)` — one JSON line per event; the zero-config default |
| `StderrSink` | `log_forge.sinks.util` | `StderrSink(stream=sys.stderr)` — same, on stderr (twelve-factor) |
| `NullSink` | `log_forge.sinks.util` | `NullSink()` — discard everything; `.dropped` counts events |
| `MemorySink` | `log_forge.sinks.util` | `MemorySink(maxlen=None)` — collect into `.events` (a bounded ring when `maxlen` is set) |

```python
from log_forge.sinks.stdout import StdoutSink
lf.configure(sink=StdoutSink())          # explicit; also the zero-config default
```

#### Composition & adapters (zero-dependency)

`configure(sink=...)` takes a single sink, so compose these to filter, reshape, fan out, or bridge
to a plain callable.

| Sink | Import from | Configure |
|---|---|---|
| `MultiSink` | `log_forge.sinks.multi` | `MultiSink(*sinks)` — forward each batch to every child; a failing child is isolated and counted on `.failed` |
| `FilteringSink` | `log_forge.sinks.filtering` | `FilteringSink(inner, *, predicate=None, min_level=None)` — forward only events passing `predicate` and/or at/above `min_level` |
| `TransformSink` | `log_forge.sinks.transform` | `TransformSink(inner, fn)` — map each event through `fn` before forwarding; return `None` to drop one |
| `CallbackSink` | `log_forge.sinks.callback` | `CallbackSink(fn, *, on_close=None)` — hand each batch to any callable |

```python
from log_forge.sinks.multi import MultiSink
from log_forge.sinks.filtering import FilteringSink
from log_forge.sinks.stdout import StdoutSink
from log_forge.sinks.sqs import SQSSink

lf.configure(sink=MultiSink(
    StdoutSink(),                                                    # echo everything locally
    FilteringSink(SQSSink(queue_url="…"), min_level="WARNING"),      # only WARNING+ to SQS
))
```

`min_level` is one of `DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL` (case-insensitive); an event whose
level is unknown or missing fails open (is forwarded).

#### Standard-library `logging` bridge (zero-dependency)

| Sink | Import from | Configure |
|---|---|---|
| `LoggingSink` | `log_forge.sinks.logging_sink` | `LoggingSink(logger=None, *, default_level="INFO")` — emit each event as a `logging.LogRecord` |

Hands every event to a `logging.Logger` (default `logging.getLogger("log_forge")`) so your existing
handlers, formatters, and `logging.config` apply. Identity fields and the nested `fields` are
attached to each record; the sink never configures or tears down logging itself.

#### Local file & embedded (zero-dependency)

| Sink | Import from | Configure |
|---|---|---|
| `FileSink` | `log_forge.sinks.file` | `FileSink(path, *, encoding="utf-8")` — append NDJSON to one file |
| `RotatingFileSink` | `log_forge.sinks.file` | `RotatingFileSink(path, *, max_bytes=0, backup_count=0, when=None, interval=1)` — rotate by size and/or time, keeping `backup_count` numbered backups |
| `SQLiteSink` | `log_forge.sinks.sqlite` | `SQLiteSink(database, *, table="log_events", create_table=True)` — batch-insert into an embedded SQLite DB |

`RotatingFileSink`'s time trigger uses a `when` unit code — `"S"`/`"M"`/`"H"`/`"D"` — times `interval`
(either trigger, or both, can be enabled). `SQLiteSink` stores each event as full JSON plus projected
`log_id`/`trace_id`/`span_id`/`timestamp`/`level`/`function` columns; pass `create_table=False` when
you provision the table yourself.

```python
from log_forge.sinks.file import RotatingFileSink
lf.configure(sink=RotatingFileSink("app.log.jsonl", max_bytes=10_000_000, backup_count=5))
```

#### HTTP & self-hosted platforms (zero-dependency)

All build on `HTTPSink` (stdlib `urllib`): they POST batches with bounded `429`/`5xx` retry
(honoring `Retry-After`) and need **no** extra. On the specialized sinks, `**http_kwargs` forwards
to `HTTPSink` (`headers=`, `auth=`, `gzip=`, `timeout=`, `max_retries=`).

| Sink | Import from | Configure |
|---|---|---|
| `HTTPSink` | `log_forge.sinks.http` | `HTTPSink(url, *, method="POST", headers=None, auth=None, body_format="ndjson", timeout=5.0, gzip=False, max_retries=3)` — generic POST. `auth` is a bearer-token `str` or `(user, pass)` for basic; `body_format` is `"ndjson"` or `"json_array"` |
| `ElasticsearchSink` | `log_forge.sinks.elasticsearch` | `ElasticsearchSink(url, *, index, auth=None, **http_kwargs)` — POST to `_bulk`, parsing per-item errors (`.item_errors`) |
| `OpenSearchSink` | `log_forge.sinks.elasticsearch` | same signature as `ElasticsearchSink` (identical bulk protocol) |
| `LokiSink` | `log_forge.sinks.loki` | `LokiSink(url, *, labels=("service", "env", "level"), **http_kwargs)` — Grafana Loki push API |
| `LogstashSink` | `log_forge.sinks.logstash` | `LogstashSink(url=…, **http_kwargs)` for HTTP, **or** `LogstashSink(host=…, port=…, transport="tcp")` for a raw TCP/UDP socket |
| `SyslogSink` | `log_forge.sinks.syslog` | `SyslogSink(host, port=514, *, transport="udp", facility="user", app_name="log-forge")` — RFC 5424 over UDP/TCP |

```python
from log_forge.sinks.elasticsearch import ElasticsearchSink
lf.configure(sink=ElasticsearchSink("https://es.internal:9200", index="app-logs",
                                     auth=("elastic", "…")))
```

#### SaaS platforms

Also HTTP-based. All are zero-dependency **except** `SentrySink`, which prefers the `sentry-sdk`
(the `sentry` extra) and falls back to raw HTTP envelopes when it isn't installed.

| Sink | Import from | Extra | Configure |
|---|---|---|---|
| `DatadogSink` | `log_forge.sinks.datadog` | — | `DatadogSink(api_key, *, site="datadoghq.com", service=None, ddtags=None)` |
| `SplunkHECSink` | `log_forge.sinks.splunk` | — | `SplunkHECSink(url, token, *, host=None, source="log-forge")` — HTTP Event Collector |
| `NewRelicSink` | `log_forge.sinks.newrelic` | — | `NewRelicSink(api_key, *, region="US")` — `region` is `"US"` or `"EU"` |
| `HoneycombSink` | `log_forge.sinks.honeycomb` | — | `HoneycombSink(api_key, dataset, *, url="https://api.honeycomb.io")` |
| `SentrySink` | `log_forge.sinks.sentry` | `sentry` | `SentrySink(dsn=None, *, min_level="ERROR")` — sends only `min_level`+ events |

With the `sentry` extra installed, `SentrySink` captures via `sentry_sdk.capture_event` (initialize
the SDK yourself with `sentry_sdk.init(...)`); without it, pass `dsn=` and events are POSTed as
Sentry envelopes over HTTP.

#### AWS — the durable-buffer path (`aws` extra)

`pip install 'log-foundry[aws]'` (pulls `boto3`). Credentials and region come from boto3's standard
chain — log-forge adds none of its own. Each re-chunks every batch to the service's hard per-request
limits, retries partial failures, and drops any single event too large to ever fit (counted on
`.dropped_oversized`).

| Sink | Import from | Configure |
|---|---|---|
| `SQSSink` | `log_forge.sinks.sqs` | `SQSSink(queue_url, *, max_retries=3)` — the headline production path: a durable buffer in front of ELK, absorbing downstream spikes/outages |
| `SNSSink` | `log_forge.sinks.sns` | `SNSSink(topic_arn, *, max_retries=3)` |
| `KinesisSink` | `log_forge.sinks.kinesis` | `KinesisSink(stream_name, *, partition_key_field="trace_id", max_retries=3)` |
| `FirehoseSink` | `log_forge.sinks.firehose` | `FirehoseSink(delivery_stream, *, max_retries=3)` |

```python
from log_forge.sinks.sqs import SQSSink
lf.configure(service="payments",
             sink=SQSSink(queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/logs"))
```

Consuming from the buffer and indexing into ELK is a separate component, outside this library.

#### Queue & stream

Each needs its own extra (lazy-imported). All publish + retry within a bound and close cleanly.

| Sink | Import from | Extra | Configure |
|---|---|---|---|
| `KafkaSink` | `log_forge.sinks.kafka` | `kafka` | `KafkaSink(topic, *, bootstrap_servers="…", key_field="trace_id")` |
| `RedisStreamsSink` | `log_forge.sinks.redis` | `redis` | `RedisStreamsSink(stream, *, url=None)` — `XADD` |
| `RedisListSink` | `log_forge.sinks.redis` | `redis` | `RedisListSink(key, *, url=None)` — `RPUSH` |
| `RabbitMQSink` | `log_forge.sinks.rabbitmq` | `amqp` | `RabbitMQSink(*, exchange, routing_key, url=None)` — persistent messages |
| `NATSSink` | `log_forge.sinks.nats` | `nats` | `NATSSink(subject, *, jetstream=False, servers=None)` |
| `GooglePubSubSink` | `log_forge.sinks.pubsub` | `gcp-pubsub` | `GooglePubSubSink(topic)` |
| `AzureEventHubsSink` | `log_forge.sinks.eventhubs` | `azure-eventhubs` | `AzureEventHubsSink(*, connection_str="…", eventhub=None)` |

```python
from log_forge.sinks.kafka import KafkaSink
lf.configure(sink=KafkaSink("app-logs", bootstrap_servers="broker:9092"))
```

#### Databases

Write-only inserts (querying is the downstream tool's job); each needs its own extra.

| Sink | Import from | Extra | Configure |
|---|---|---|---|
| `MongoDBSink` | `log_forge.sinks.mongodb` | `mongo` | `MongoDBSink(*, uri="…", database="…", collection="…")` |
| `PostgresSink` | `log_forge.sinks.postgres` | `postgres` | `PostgresSink(table, *, dsn="…", create_table=False)` — JSONB `event` column + extracted columns |
| `ClickHouseSink` | `log_forge.sinks.clickhouse` | `clickhouse` | `ClickHouseSink(table, *, dsn="…", create_table=False)` — MergeTree, columnar insert |

`PostgresSink` / `ClickHouseSink` default `create_table=False` (you own the schema and indexes); set
it `True` for an idempotent `CREATE TABLE IF NOT EXISTS` convenience.

Prefer a destination not listed here? Implement the two-method `Sink` protocol yourself, or wrap any
callable in `CallbackSink`.

### Flushing and shutdown

Delivery is off the hot path. When a span ends, its events are handed to a per-process
background worker via a fast, non-blocking submit — your function returns without waiting on
the sink. The worker batches events (by count and time), emits them on its own thread, retries
a failing sink with backoff, and applies backpressure so a slow or down sink can never block or
back-pressure the app: when its bounded queue is full it drops the newest submissions and counts
them (`worker.dropped`) rather than stalling.

Because delivery is asynchronous, drain before the process exits:

```python
import log_forge as lf

lf.shutdown()   # flush buffered events and close the sink; blocks until drained
```

`shutdown()` is also registered via `atexit`, so a normal exit flushes automatically — call it
explicitly when you need to be certain the tail reached the sink before a fast exit (e.g. at the
end of a short script or an AWS Lambda handler). It is idempotent.

## Event schema

Every event is the same shape (arch §6). Boundary events add a few fields:

| Field | Always | Description |
|---|---|---|
| `timestamp` | ✓ | UTC ISO-8601, millisecond precision, `Z` suffix |
| `level` | ✓ | `INFO` / `ERROR` / … |
| `message` | ✓ | `span.start` / `span.end` for boundaries |
| `trace_id` | ✓ | 16 bytes / 32 hex — shared across a trace (W3C-compatible) |
| `span_id` | ✓ | 8 bytes / 16 hex — unique per call |
| `parent_span_id` | ✓ | parent's `span_id`, or `null` at the trace root |
| `log_id` | ✓ | UUID, unique per event |
| `function` | ✓ | span name |
| `service` / `version` / `env` | ✓ | from `configure(...)` |
| `fields` | ✓ | merged user fields (config `defaults` → span `defaults` → …) |
| `duration_ms` | span.end | wall time from a monotonic delta |
| `status` | span.end | `"ok"` or `"error"` |
| `error` | on failure | `{"type": ..., "stack": ...}` |

IDs are [W3C Trace Context](https://www.w3.org/TR/trace-context/)-compatible by design, so the
logs can later correlate with distributed traces cheaply.

## Development

```bash
poetry install --with dev      # set up (Python 3.13)
poetry run pytest              # test
poetry run ruff check .        # lint (line-length 100)
poetry run mypy                # typecheck (strict, over src/)
```

**CI** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs ruff → mypy → pytest on
every pull request and on push to `main`. A second workflow
([`spec-lint.yml`](.github/workflows/spec-lint.yml)) lints the design specs under `docs/specs/`.

The library uses a src layout (`src/log_forge/`) with a single concept per module: `config`,
`ids`, `model`, `context`, `decorator`, `api`, `console`, `worker`, and the `sinks/` package (the
`base` protocol, `stdout`, and one module per sink family — see [Sinks](#sinks)).
Deeper design docs live in [`docs/`](docs/) — start with [`docs/architecture.md`](docs/architecture.md).

## License

[MIT](LICENSE) © Andrew Griffith
