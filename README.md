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

> **Status:** early implementation. **Shipped today:** the `@trace` decorator, `configure()`,
> the zero-dependency `StdoutSink` (SPEC-001); and the `debug`/`info`/`warning`/`error`/`critical`
> emitters, `echo=` console output, and `set_baggage` (SPEC-002). **Not yet shipped:** async
> `@trace`, the background flush worker, `shutdown()`, and the SQS sink. See [Roadmap](#roadmap).

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

> Async support (`@trace` over `async def`) is on the roadmap; today `@trace` targets
> synchronous callables.

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

### Custom sinks

A sink is any object satisfying the `Sink` protocol — it receives already-built event dicts
and knows nothing about spans:

```python
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None: ...
    def close(self) -> None: ...
```

Pass an instance to `configure(sink=...)`. The built-in `StdoutSink` writes one JSON line per
event to a stream (default `sys.stdout`); construct it with `StdoutSink(stream=...)` to
redirect.

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
`ids`, `model`, `context`, `decorator`, `api`, `console`, and `sinks/{base,stdout}`. Deeper
design docs live in [`docs/`](docs/) — start with [`docs/architecture.md`](docs/architecture.md).

## Roadmap

Built in spec order (SPEC-001 → 005; see [`docs/specs/INDEX.md`](docs/specs/INDEX.md)).
SPEC-001 (core span pipeline) and SPEC-002 (logging API + console echo + baggage) are shipped;
what's next:

- **SPEC-003** — async `@trace` support (span stack + baggage propagated across `async` tasks).
- **SPEC-004** — background, non-blocking flush worker + graceful `shutdown()` / `atexit` drain.
- **SPEC-005** — the `SQSSink` (behind the `sqs` extra) for the SQS → ELK path.
- **Not yet planned:** **publishing to PyPI** (no release workflow exists today).

**Out of scope** (by design): metrics or OTel-native traces · querying / dashboards / alerting
(that's ELK downstream) · more than one configured sink per process · cross-process trace
continuation.

## License

[MIT](LICENSE) © Andrew Griffith
