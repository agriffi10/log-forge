# Log Foundry

Consistent, structured (JSON) logs for every decorated function call — correlated by shared
trace/span IDs, ready to ship to any of 30-plus built-in sinks (stdout by default; SQS → ELK is
the headline production path).

`log-foundry` owns the **logs** pillar of observability. You decorate a function with `@trace`;
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

- **Python ≥ 3.12** — the full gate (ruff, mypy, pytest) runs on 3.12 and 3.13 in CI.

## Installation

Published on PyPI as **[`log-foundry`](https://pypi.org/project/log-foundry/)**:

```bash
pip install log-foundry          # core, zero dependencies
pip install 'log-foundry[aws]'   # + boto3 for the SQS/SNS/Kinesis/Firehose sinks
```

> **Renamed in 0.2.0: `log_forge` → `log_foundry`.** The import package now matches the
> distribution name — `pip install log-foundry`, then `import log_foundry`. If you are on
> `0.1.x`, update your imports; there is no compatibility shim. The project was originally
> called *log-forge*, but PyPI rejects that name as too similar to the unrelated, pre-existing
> [`logforge`](https://pypi.org/project/logforge/) project — its similarity check collapses
> separators, so `log-forge` and `logforge` count as the same name. Rather than keep a
> distribution and an import name that disagreed, everything is now `log-foundry` /
> `log_foundry`.
>
> Migrating from `0.1.x` is a find-and-replace on `log_forge` → `log_foundry`; no module moved
> and no public API changed. A handful of *emitted* defaults carry the name and shift with it:
> `LoggingSink`'s default logger (`logging.getLogger("log_foundry")`), `SyslogSink(app_name=…)`,
> `SplunkHECSink(source=…)`, Datadog's `ddsource`, and Sentry's client tag. Override them
> explicitly if a downstream query or dashboard pins the old string.

```python
import log_foundry

print(log_foundry.__version__)     # the installed version
```

To work on the library itself, install from a clone:

```bash
# the version is derived from Git tags, so clone with history (not --depth 1)
poetry self add "poetry-dynamic-versioning[plugin]"   # one-time, resolves the version locally
poetry install --with dev                             # or: pip install -e .
```

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
import log_foundry as lf

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

![log-foundry pipeline: a traced call opens a span, gathers events, then closes and hands off to a background worker that batches events and ships them to a sink. Steps 1–4 run on your thread; the worker and sink run on a background thread. Support modules — config, ids, model, context, console — assist every step.](docs/assets/pipeline.svg)

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

Both triggers can be pre-empted: `flush()` puts a marker in the queue and the worker emits the
pending pile the moment it reaches it, ignoring the count and time triggers. Because the queue
is FIFO, everything submitted before the call is necessarily ahead of that marker — which is
exactly why the guarantee is "events submitted before this call", and why concurrent
submissions from other threads may or may not be included.

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

### Continuing a trace across processes

A trace stops at the process boundary: `@trace` mints a fresh `trace_id` whenever no span is
open, so two processes cooperating on one logical operation produce two unrelated traces. Pass
the context across and they join up. Nothing here is serverless-specific — the same two calls
join an HTTP client to its server, or a Celery caller to its worker.

**The producer publishes where it is:**

```python
@lf.trace
def enqueue_check(location: str) -> None:
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({
            "location": location,
            "traceparent": lf.current_traceparent(),      # "00-<trace_id>-<span_id>-01"
            "baggage": lf.current_baggage_header(),       # "request_id=req-123,tenant=acme"
        }),
    )
```

**The consumer adopts it — one line, and make it the first line:**

```python
@lf.trace
def handler(event, context):
    lf.continue_trace(event.get("traceparent"), baggage=event.get("baggage"))
    lf.info("inspecting")          # same trace_id as the producer; parent is its span
    try:
        return inspect(event)
    finally:
        lf.flush()
```

| Call | Does |
|---|---|
| `continue_trace(traceparent=None, *, trace_id=None, parent_span_id=None, baggage=None)` | Adopt an inbound context. `True` if adopted, `False` if nothing valid was supplied. Never raises. |
| `current_traceparent()` | This span as a W3C `traceparent` string, or `None` if no span is active. |
| `current_trace_context()` | `(trace_id, span_id)`, for when moving two fields beats moving a string. |
| `current_baggage_header()` | Current baggage in W3C `baggage` format (`""` when empty). |

Details worth knowing:

- **Call `continue_trace()` on the first line.** `@trace` opens the handler's span *before* the
  body runs, so the call re-parents that span in place and rewrites the events it has already
  buffered. A child span that already finished has been handed to the worker and can no longer
  be moved.
- **Only a root span is re-parented.** A nested span already belongs to an in-process trace, and
  moving it would sever it from its own parent. The adopted context still applies to the next
  root span opened in that context.
- **Your `span_id` is never overwritten.** The adopting span keeps its own identity and takes the
  inbound span as its `parent_span_id` — otherwise two processes would share a span id.
- **`parent_span_id` may be omitted.** With only `trace_id` you join the trace as another root,
  which beats being in a fresh trace when you know the trace but not the specific parent.
- **Inbound context is untrusted and validated strictly** — 32/16 lowercase hex, all-zero ids
  rejected, higher `traceparent` versions accepted per the W3C forward-compatibility rule.
  Anything unusable is ignored with a single bounded warning on stderr and a fresh trace is
  minted; a malformed id never reaches the event stream. Adopting a context grants **nothing**
  — it selects a correlation id and confers no authority.
- **Baggage fails independently of the trace.** A malformed `baggage` header is skipped with a
  warning while the trace is still adopted: losing correlating fields is bad, losing the trace
  join because one field was malformed is worse. Headers over 8192 bytes are rejected. Values
  are percent-encoded, so `,` `=` and non-ASCII round-trip; non-string values are serialized
  with `str()`, so a dict arrives as its repr.
- **Sampling is not honoured.** `traceparent`'s flags byte is parsed and ignored, and outbound
  is always `01`: this library records every span, so respecting another system's sampling
  decision would mean dropping them.

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
from its own module, e.g. `from log_foundry.sinks.sqs import SQSSink`.

A few conventions hold across every sink below:

- **Extras.** The core is dependency-free. A sink built on a third-party client sits behind the
  optional extra named in its table (blank = zero-dependency, stdlib only); the client is imported
  lazily, so `import log_foundry.sinks.<x>` never fails for a missing dependency — only *constructing*
  the sink without an injected client does. See [Optional extras](#optional-extras).
- **Injection.** Sinks backed by an external resource accept an injected client/connection/stream
  (`client=`, `connection=`, `producer=`, `stream=`, `opener=`) for testing or bespoke configuration.
  The tables show the destination-defining arguments only; sinks that retry also take `max_retries`.
- **Ownership.** A resource the sink opens itself is closed on `shutdown()`; an injected one is left
  open for you to manage.
- **Never crashes the app.** A failing sink is retried with backoff and then counted (`.failed`,
  `.dropped_oversized`, `.dropped_unadjudicated`, …) rather than raised — a broken destination
  degrades logging, nothing more.
  The one deliberate exception is a `MultiSink` whose children *all* failed: it re-raises so the
  worker's retry engages, since nothing was delivered and there are no duplicates to risk. That
  still doesn't reach your code — the worker is what catches it.

#### Built-in, zero-dependency

| Sink | Import from | Configure |
|---|---|---|
| `StdoutSink` | `log_foundry.sinks.stdout` | `StdoutSink(stream=sys.stdout)` — one JSON line per event; the zero-config default |
| `StderrSink` | `log_foundry.sinks.util` | `StderrSink(stream=sys.stderr)` — same, on stderr (twelve-factor) |
| `NullSink` | `log_foundry.sinks.util` | `NullSink()` — discard everything; `.dropped` counts events |
| `MemorySink` | `log_foundry.sinks.util` | `MemorySink(maxlen=None)` — collect into `.events` (a bounded ring when `maxlen` is set) |

```python
from log_foundry.sinks.stdout import StdoutSink
lf.configure(sink=StdoutSink())          # explicit; also the zero-config default
```

#### Composition & adapters (zero-dependency)

`configure(sink=...)` takes a single sink, so compose these to filter, reshape, fan out, or bridge
to a plain callable.

| Sink | Import from | Configure |
|---|---|---|
| `MultiSink` | `log_foundry.sinks.multi` | `MultiSink(*sinks)` — forward each batch to every child; a failing child is isolated and counted on `.failed` |
| `FilteringSink` | `log_foundry.sinks.filtering` | `FilteringSink(inner, *, predicate=None, min_level=None)` — forward only events passing `predicate` and/or at/above `min_level` |
| `TransformSink` | `log_foundry.sinks.transform` | `TransformSink(inner, fn)` — map each event through `fn` before forwarding; return `None` to drop one |
| `CallbackSink` | `log_foundry.sinks.callback` | `CallbackSink(fn, *, on_close=None)` — hand each batch to any callable |

```python
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.filtering import FilteringSink
from log_foundry.sinks.stdout import StdoutSink
from log_foundry.sinks.sqs import SQSSink

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
| `LoggingSink` | `log_foundry.sinks.logging_sink` | `LoggingSink(logger=None, *, default_level="INFO")` — emit each event as a `logging.LogRecord` |

Hands every event to a `logging.Logger` (default `logging.getLogger("log_foundry")`) so your existing
handlers, formatters, and `logging.config` apply. Identity fields and the nested `fields` are
attached to each record; the sink never configures or tears down logging itself.

#### Local file & embedded (zero-dependency)

| Sink | Import from | Configure |
|---|---|---|
| `FileSink` | `log_foundry.sinks.file` | `FileSink(path, *, encoding="utf-8")` — append NDJSON to one file |
| `RotatingFileSink` | `log_foundry.sinks.file` | `RotatingFileSink(path, *, max_bytes=0, backup_count=0, when=None, interval=1)` — rotate by size and/or time, keeping `backup_count` numbered backups |
| `SQLiteSink` | `log_foundry.sinks.sqlite` | `SQLiteSink(database, *, table="log_events", create_table=True)` — batch-insert into an embedded SQLite DB |

`RotatingFileSink`'s time trigger uses a `when` unit code — `"S"`/`"M"`/`"H"`/`"D"` — times `interval`
(either trigger, or both, can be enabled). `SQLiteSink` stores each event as full JSON plus projected
`log_id`/`trace_id`/`span_id`/`timestamp`/`level`/`function` columns; pass `create_table=False` when
you provision the table yourself.

```python
from log_foundry.sinks.file import RotatingFileSink
lf.configure(sink=RotatingFileSink("app.log.jsonl", max_bytes=10_000_000, backup_count=5))
```

#### HTTP & self-hosted platforms (zero-dependency)

All build on `HTTPSink` (stdlib `urllib`): they POST batches with bounded `429`/`5xx` retry
(honoring `Retry-After`) and need **no** extra. On the specialized sinks, `**http_kwargs` forwards
to `HTTPSink` (`headers=`, `auth=`, `gzip=`, `timeout=`, `max_retries=`).

| Sink | Import from | Configure |
|---|---|---|
| `HTTPSink` | `log_foundry.sinks.http` | `HTTPSink(url, *, method="POST", headers=None, auth=None, body_format="ndjson", timeout=5.0, gzip=False, max_retries=3)` — generic POST. `auth` is a bearer-token `str` or `(user, pass)` for basic; `body_format` is `"ndjson"` or `"json_array"` |
| `ElasticsearchSink` | `log_foundry.sinks.elasticsearch` | `ElasticsearchSink(url, *, index, auth=None, **http_kwargs)` — POST to `_bulk`, parsing per-item errors (`.item_errors`) |
| `OpenSearchSink` | `log_foundry.sinks.elasticsearch` | same signature as `ElasticsearchSink` (identical bulk protocol) |
| `LokiSink` | `log_foundry.sinks.loki` | `LokiSink(url, *, labels=("service", "env", "level"), **http_kwargs)` — Grafana Loki push API |
| `LogstashSink` | `log_foundry.sinks.logstash` | `LogstashSink(url=…, **http_kwargs)` for HTTP, **or** `LogstashSink(host=…, port=…, transport="tcp")` for a raw TCP/UDP socket |
| `SyslogSink` | `log_foundry.sinks.syslog` | `SyslogSink(host, port=514, *, transport="udp", facility="user", app_name="log-foundry")` — RFC 5424 over UDP/TCP |

```python
from log_foundry.sinks.elasticsearch import ElasticsearchSink
lf.configure(sink=ElasticsearchSink("https://es.internal:9200", index="app-logs",
                                     auth=("elastic", "…")))
```

#### SaaS platforms

Also HTTP-based. All are zero-dependency **except** `SentrySink`, which prefers the `sentry-sdk`
(the `sentry` extra) and falls back to raw HTTP envelopes when it isn't installed.

| Sink | Import from | Extra | Configure |
|---|---|---|---|
| `DatadogSink` | `log_foundry.sinks.datadog` | — | `DatadogSink(api_key, *, site="datadoghq.com", service=None, ddtags=None)` |
| `SplunkHECSink` | `log_foundry.sinks.splunk` | — | `SplunkHECSink(url, token, *, host=None, source="log-foundry")` — HTTP Event Collector |
| `NewRelicSink` | `log_foundry.sinks.newrelic` | — | `NewRelicSink(api_key, *, region="US")` — `region` is `"US"` or `"EU"` |
| `HoneycombSink` | `log_foundry.sinks.honeycomb` | — | `HoneycombSink(api_key, dataset, *, url="https://api.honeycomb.io")` |
| `SentrySink` | `log_foundry.sinks.sentry` | `sentry` | `SentrySink(dsn=None, *, min_level="ERROR")` — sends only `min_level`+ events |

With the `sentry` extra installed, `SentrySink` captures via `sentry_sdk.capture_event` (initialize
the SDK yourself with `sentry_sdk.init(...)`); without it, pass `dsn=` and events are POSTed as
Sentry envelopes over HTTP.

#### AWS — the durable-buffer path (`aws` extra)

`pip install 'log-foundry[aws]'` (pulls `boto3`). Credentials and region come from boto3's standard
chain — log-foundry adds none of its own. Each re-chunks every batch to the service's hard per-request
limits, retries partial failures, and drops any single event too large to ever fit (counted on
`.dropped_oversized`).

`KinesisSink` and `FirehoseSink` learn which records failed **positionally** — the response carries a
parallel array with no ids — so they check that it describes as many records as were sent before
acting on it. A response that doesn't is not used to adjudicate any record in the chunk: the chunk is
abandoned rather than re-sent (some of it almost certainly landed), counted on
`.dropped_unadjudicated`, and named on stderr. A non-zero value there is real loss, and normally
means the client isn't AWS-shaped. `SQSSink` and `SNSSink` correlate by explicit `Id` instead, so
they can't mis-pair and have no such counter.

| Sink | Import from | Configure |
|---|---|---|
| `SQSSink` | `log_foundry.sinks.sqs` | `SQSSink(queue_url, *, max_retries=3, fifo=None, message_group_id=None, message_deduplication_id=None)` — the headline production path: a durable buffer in front of ELK, absorbing downstream spikes/outages. Standard **and** FIFO queues |
| `SNSSink` | `log_foundry.sinks.sns` | `SNSSink(topic_arn, *, max_retries=3)` |
| `KinesisSink` | `log_foundry.sinks.kinesis` | `KinesisSink(stream_name, *, partition_key_field="trace_id", max_retries=3)` |
| `FirehoseSink` | `log_foundry.sinks.firehose` | `FirehoseSink(delivery_stream, *, max_retries=3)` |

```python
from log_foundry.sinks.sqs import SQSSink
lf.configure(service="payments",
             sink=SQSSink(queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/logs"))
```

Consuming from the buffer and indexing into ELK is a separate component, outside this library.

`SQSSink` does not retry a message SQS rejects as a **sender fault** — the retry would re-send it
byte-identical, so it can only fail the same way. Those are counted on `.failed` immediately and
the SQS error code is named on stderr. Throttles and internal errors are still retried up to
`max_retries`.

##### FIFO queues

A queue URL ending in `.fifo` switches `SQSSink` into FIFO mode automatically — AWS requires the
suffix on every FIFO queue, so nothing needs configuring:

```python
SQSSink(queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/logs.fifo")
```

Each message then carries a **`MessageGroupId`**, which defaults to the event's own `trace_id`.
SQS guarantees ordering *within* a group, and a trace is exactly the unit whose events should stay
ordered — while separate traces land in separate groups, so the queue delivers them in parallel
instead of serializing your whole process behind one group. (`KinesisSink` partitions on `trace_id`
by default for the same reason.) The **`MessageDeduplicationId`** defaults to the event's `log_id`,
already a per-event UUID, so SQS's five-minute deduplication window never collapses two distinct
records.

Override the group with a constant or a callable:

```python
# One group for the whole process — strict global ordering, capped at ~300 msg/s.
SQSSink(queue_url=FIFO_URL, message_group_id="payments")

# Group by anything on the event. Baggage lands in `fields`, so this groups by tenant
# and falls back to per-trace when unset:
SQSSink(queue_url=FIFO_URL,
        message_group_id=lambda e: str(e["fields"].get("tenant_id") or e["trace_id"]))
```

Pass `fifo=True` or `fifo=False` to override the URL-based detection. Standard queues are entirely
unaffected — their messages carry neither parameter.

Two things worth knowing:

- **Ordering is best-effort across a retry.** If one message fails and a same-group message ahead
  of it succeeded, the retry lands after it. Holding a whole group back on a single failure would
  trade log delivery for ordering you can rebuild from `timestamp`, so the sink doesn't.
- **FIFO queues cap throughput** at 300 messages/second (3,000 with batching), or higher in
  high-throughput mode. That's queue-side configuration, not something the library sets.

#### Queue & stream

Each needs its own extra (lazy-imported). All publish + retry within a bound and close cleanly.

| Sink | Import from | Extra | Configure |
|---|---|---|---|
| `KafkaSink` | `log_foundry.sinks.kafka` | `kafka` | `KafkaSink(topic, *, bootstrap_servers="…", key_field="trace_id")` |
| `RedisStreamsSink` | `log_foundry.sinks.redis` | `redis` | `RedisStreamsSink(stream, *, url=None)` — `XADD` |
| `RedisListSink` | `log_foundry.sinks.redis` | `redis` | `RedisListSink(key, *, url=None)` — `RPUSH` |
| `RabbitMQSink` | `log_foundry.sinks.rabbitmq` | `amqp` | `RabbitMQSink(*, exchange, routing_key, url=None)` — persistent messages |
| `NATSSink` | `log_foundry.sinks.nats` | `nats` | `NATSSink(subject, *, jetstream=False, servers=None)` |
| `GooglePubSubSink` | `log_foundry.sinks.pubsub` | `gcp-pubsub` | `GooglePubSubSink(topic)` |
| `AzureEventHubsSink` | `log_foundry.sinks.eventhubs` | `azure-eventhubs` | `AzureEventHubsSink(*, connection_str="…", eventhub=None)` |

```python
from log_foundry.sinks.kafka import KafkaSink
lf.configure(sink=KafkaSink("app-logs", bootstrap_servers="broker:9092"))
```

#### Databases

Write-only inserts (querying is the downstream tool's job); each needs its own extra.

| Sink | Import from | Extra | Configure |
|---|---|---|---|
| `MongoDBSink` | `log_foundry.sinks.mongodb` | `mongo` | `MongoDBSink(*, uri="…", database="…", collection="…")` |
| `PostgresSink` | `log_foundry.sinks.postgres` | `postgres` | `PostgresSink(table, *, dsn="…", create_table=False)` — JSONB `event` column + extracted columns |
| `ClickHouseSink` | `log_foundry.sinks.clickhouse` | `clickhouse` | `ClickHouseSink(table, *, dsn="…", create_table=False)` — MergeTree, columnar insert |

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
them rather than stalling.

Those losses are deliberate, so the library gives you a way to notice them. `log_foundry.health()`
returns a snapshot of the worker's counters:

```python
h = log_foundry.health()
if h.dropped or h.failed_batches or h.stopped_reason:
    ...  # logs were silently lost — worth an alert
```

The three tell you different things, and they want different responses:

| Field | Means | What to do |
|---|---|---|
| `dropped` | The queue filled — the destination is not keeping up. Delivery continues. | Tune `batch_size`/`flush_interval`, or scale the sink. |
| `failed_batches` | A sink stayed broken through the whole retry budget. Delivery continues. | Fix the destination. |
| `stopped_reason` | The background thread **died** on that exception type. Nothing further will be delivered, ever. | Restart the process; investigate the named exception. |

`stopped_reason` is a type name (e.g. `"SystemExit"`), never the exception's message — a sink's
error text can carry event data. It reads `None` for a healthy worker, for a process that has never
logged, and after a clean `shutdown()`, so a plain truthiness check is safe. Without it a dead
thread showed up only indirectly, as `dropped` climbing once the queue filled — the wrong signal,
pointing at the wrong fix.

Read a snapshot by attribute (`h.dropped`), as above. `Health` is a `NamedTuple` and gained a
fourth field in `v0.7.0`, so unpacking it whole — `queued, dropped, failed = health()` — raises
`ValueError` from that version on.

`dropped` counts submissions discarded because the queue filled; `failed_batches` counts batches
abandoned after the retry budget was spent. Overflow also warns on stderr — on the first drop and
every thousandth after it, since overflow is a high-rate condition and a line per drop would be its
own outage. A process that has never logged has no worker, and asking after its health does not
create one.

Because delivery is asynchronous, drain before the process exits. There are two drains, and
which one you want depends on whether the process is about to end:

```python
import log_foundry as lf

lf.flush()      # drain to the sink and keep going; returns True when everything landed
lf.shutdown()   # drain, close the sink, and stop for good; blocks until drained
```

| | `flush()` | `shutdown()` |
|---|---|---|
| Drains buffered events | yes | yes |
| Closes the sink | no | yes |
| Worker survives | yes | **no** — it never comes back |
| Repeatable | yes | idempotent, but only the first call does anything |
| Use it | before returning from a handler, or at a checkpoint | once, as the process exits |

`shutdown()` is also registered via `atexit`, so a normal exit flushes automatically — call it
explicitly when you need to be certain the tail reached the sink before a fast exit, e.g. at the
end of a short script. It is idempotent.

`flush(timeout=5.0)` returns `True` when the events submitted before the call **reached the
sink**, and `False` otherwise: on timeout, when the worker was already shut down or has died, and
when a batch was abandoned while the call was outstanding. A `True` is evidence of delivery, not
merely that a drain took place — which matters most in the serverless case below, where the return
value is all you get.

It answers for its own window: the drain it forces, plus anything the worker abandoned while it
waited its turn. A batch lost *before* you called it is deliberately outside that window — that
loss is already counted in `health().failed_batches` and reported on stderr, and folding it in
would make every later `flush()` in the process report a failure it did not incur. Use `flush()`
for "did this invocation's logs get out", and `health()` for "has anything been lost at all".

It never raises — a logging call must not be the reason your function fails. Passing
`timeout=None` waits indefinitely, which is unsafe anywhere with an execution deadline.

#### Serverless / short-lived processes

In AWS Lambda (and anything else that freezes rather than exits) the rules are different, and
getting them wrong is silent:

- **Flush before the handler returns.** Lambda freezes the execution environment the instant
  your handler returns, so the worker's interval-based flush stops mid-interval and whatever is
  still queued is lost when the container is eventually reaped. `atexit` does not save you —
  a frozen environment is killed without running exit handlers, so `flush()` is the *only*
  guaranteed drain there.
- **Put it in a `finally`.** A flush written as the last line of the handler body is precisely
  the line that does not run when the handler raises, and the invocation whose logs are most
  worth having is the one that failed.
- **Never call `shutdown()` per invocation.** It is terminal: the worker does not come back, so
  the first invocation on a warm container would log and every later one would silently log
  nothing. That failure reads as "works locally, broken in production".

```python
import log_foundry as lf
from log_foundry.sinks.sqs import SQSSink

lf.configure(service="billing-api", env="prod", sink=SQSSink(queue_url=QUEUE_URL))

@lf.trace
def handler(event, context):
    lf.info("received", records=len(event["Records"]))
    try:
        return do_work(event)
    finally:
        lf.flush()      # in `finally`: the failed invocation is the one worth logging.
                        # NEVER shutdown() here — the worker does not come back, and every
                        # later invocation on this warm container would log nothing.
```

By default each invocation is its own trace, so N invocations produce N `trace_id`s. To join
them into one — a step function, a producer and its consumer — pass the context across with
[`continue_trace()`](#continuing-a-trace-across-processes).

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
| `truncated` | when a ceiling fired | `true`; absent otherwise, never `false` |

IDs are [W3C Trace Context](https://www.w3.org/TR/trace-context/)-compatible by design, so the
logs can later correlate with distributed traces cheaply.

### Field values are coerced and bounded

Every value you pass is made JSON-safe and given a size ceiling once, when the event is assembled
— so no sink can be handed a payload JSON refuses, and no single field can grow without limit.
Strings are clipped to `max_value_bytes` (8192 by default), mappings and sequences to `max_keys`
entries and `max_depth` levels. `datetime`, `UUID`, `Decimal`, `bytes` and friends render as
strings; anything with no JSON form becomes `<unserializable: TypeName>` — the type name only,
never a `repr`, so coercion can never leak a value the library was careful not to capture.

Integers are the one case worth knowing about. They are passed through unchanged — an ID or an
amount stays a number, at full precision — but an integer too long to *render* is replaced by
`<int: ~N digits>`. CPython refuses to convert an integer past `sys.get_int_max_str_digits()`
decimal digits (**4300** by default) and raises, and `json.dumps` inherits that refusal, so with
the default configuration the interpreter's limit — not `max_value_bytes` — is what binds. You are
unlikely to meet it deliberately; `int.from_bytes(blob, "big")` over a couple of kilobytes gets
there. Any ceiling firing sets `truncated: true` on the event.

## Development

```bash
poetry install --with dev      # set up (Python 3.12+)
poetry run pytest              # test
poetry run ruff check .        # lint (line-length 100)
poetry run mypy                # typecheck (strict, over src/)
```

**CI** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs ruff → mypy → pytest on
every pull request and on push to `main`. A second workflow
([`spec-lint.yml`](.github/workflows/spec-lint.yml)) lints the design specs under `docs/specs/`.

The library uses a src layout (`src/log_foundry/`) with a single concept per module: `config`,
`ids`, `model`, `context`, `decorator`, `api`, `console`, `worker`, and the `sinks/` package (the
`base` protocol, `stdout`, and one module per sink family — see [Sinks](#sinks)).
Deeper design docs live in [`docs/`](docs/) — start with [`docs/architecture.md`](docs/architecture.md).

## Releasing

**The version is never hand-edited.** It is derived from Git tags at build time by
`poetry-dynamic-versioning`, so `pyproject.toml` carries no literal version and the published
number can't drift from what Git says.

[`release.yml`](.github/workflows/release.yml) reuses the CI suite as a gate, then builds an
sdist and a wheel:

| Trigger | Version built | Published to PyPI as |
|---|---|---|
| merge to `main` | `X.Y.Z.devN` | dev pre-release |
| push tag `vX.Y.Z` | `X.Y.Z` | stable release |

Dev pre-releases keep the upload path exercised on every merge, so a real release is never the
first time it runs. `pip install log-foundry` still resolves to the latest **stable** version —
pip ignores pre-releases unless you pass `--pre`.

Cutting a release is one tag:

```bash
git tag -a v0.2.0 -m "log-foundry 0.2.0"
git push origin v0.2.0
```

Uploads authenticate with PyPI [Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC) through the `pypi` GitHub Environment — there is no API token stored in the repository.
A tagged build refuses to publish if the derived version doesn't match the tag, and the tagged
job deliberately omits `skip-existing` so re-pushing an already-published version fails loudly.

## License

[MIT](LICENSE) © Andrew Griffith
