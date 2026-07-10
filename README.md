# log-forge

Consistent, structured (JSON) logs for every decorated function call — correlated by shared
trace/span IDs, ready to ship to a downstream sink (stdout today; SQS → ELK on the roadmap).

`log-forge` owns the **logs** pillar of observability. You decorate a function with `@trace`;
it emits one identically-shaped JSON record when the call starts and another when it ends
(with duration and status), stitched together by W3C-compatible trace and span IDs so nested
calls form a tree you can query later.

- **Zero runtime dependencies** — the core pulls in nothing; `boto3` lives behind an optional `sqs` extra.
- **Fully typed** — `mypy --strict`, ships a PEP 561 `py.typed` marker.
- **Structured, never free-form** — every event is the same named-field JSON shape.
- **Safe by default** — never captures your arguments or return values (no accidental PII/secret leakage), and the decorator **never swallows exceptions**.
- **Correct under threads and asyncio** — context propagates via `contextvars`.
- **Non-blocking delivery** — finished spans are handed to a background worker; your code never
  blocks on sink I/O, and a graceful drain at exit means buffered events aren't lost.

> **Status:** the core arc (SPEC-001 → 005) is complete. Shipped: the `@trace` decorator,
> `configure()`, and `StdoutSink` (SPEC-001); the `debug`/`info`/`warning`/`error`/`critical`
> emitters, `echo=` console output, and `set_baggage` (SPEC-002); async `@trace` over `async def`
> (SPEC-003); the non-blocking background flush worker with graceful `shutdown()` (SPEC-004); and
> the `SQSSink` behind the optional `sqs` extra (SPEC-005). Not yet done: publishing to PyPI (see
> [Roadmap](#roadmap)).

---

## Requirements

- **Python ≥ 3.13**

## Installation

`log-forge` is not yet published to PyPI (see [Roadmap](#roadmap)). Install from source:

```bash
# from a clone of this repo
poetry install                 # or: pip install .

# with the (planned) SQS sink extra
poetry install -E sqs          # or: pip install '.[sqs]'
```

Once published, the intended install will be:

```bash
pip install log-forge          # core, zero dependencies
pip install 'log-forge[sqs]'   # + boto3 for the SQS sink
```

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

A sink is the swappable output transport — any object satisfying the `Sink` protocol. It
receives already-built, batched event dicts and knows nothing about spans:

```python
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None: ...
    def close(self) -> None: ...
```

Pass an instance to `configure(sink=...)`. Two sinks ship with the library:

- **`StdoutSink`** (default, zero-dependency) — writes one JSON line per event to a stream
  (default `sys.stdout`); construct it with `StdoutSink(stream=...)` to redirect.
- **`SQSSink`** — the production path: ships events to an Amazon SQS queue that acts as a
  durable buffer in front of your indexer (e.g. ELK), absorbing downstream spikes and outages.
  It re-chunks each batch to SQS's hard limits (≤ 10 messages and ≤ 256 KB per request), retries
  partial failures, and drops any single event too large to ever fit (with a warning). It lives
  behind the optional `sqs` extra so the core stays dependency-free:

  ```python
  import log_forge
  from log_forge.sinks.sqs import SQSSink

  log_forge.configure(service="payments", sink=SQSSink(queue_url="https://sqs.../q"))
  ```

  Install with `pip install 'log-forge[sqs]'` (pulls `boto3`). AWS credentials and region are
  resolved by `boto3`'s standard chain — log-forge adds no credential configuration of its own.
  Consuming from SQS and indexing into ELK is a separate component, outside this library.

You can also implement your own sink (file, HTTP, Kafka, …) — just satisfy the two methods.

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
`ids`, `model`, `context`, `decorator`, `api`, `console`, `worker`, and `sinks/{base,stdout,sqs}`.
Deeper design docs live in [`docs/`](docs/) — start with [`docs/architecture.md`](docs/architecture.md).

## Roadmap

The core arc (SPEC-001 → 005; see [`docs/specs/INDEX.md`](docs/specs/INDEX.md)) is **complete**:
core span pipeline, logging API + console echo + baggage, async `@trace`, the background flush
worker with graceful `shutdown()`, and the `SQSSink`. What remains:

- **Publishing to PyPI** — not yet done; there is no release workflow today. Until then, install
  from source (see [Installation](#installation)).
- **Deferred by design** (IDs are already W3C-compatible, so these stay cheap to add later):
  async is in; cross-process trace continuation, cross-process baggage, and tail sampling
  (a `should_send` seam is reserved) are not built.

**Out of scope** (by design): metrics or OTel-native traces · querying / dashboards / alerting
(that's ELK downstream) · more than one configured sink per process · cross-process trace
continuation.

## License

[MIT](LICENSE) © Andrew Griffith
