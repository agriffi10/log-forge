# Spec: Core Span Pipeline

**ID:** SPEC-001
**Status:** Draft
**Last Updated:** 2026-07-09
**Depends On:** None

## Overview

log-forge turns each decorated function call into a **span**: a structured record of the
call's identity, timing, outcome, and any events emitted while it ran. This spec builds the
minimum end-to-end slice that makes that real — configuration, W3C-compatible IDs, the JSON
event schema, `contextvars`-based context propagation, a pluggable output sink with a
zero-dependency stdout implementation, and the synchronous `@trace` decorator that ties them
together. When complete, decorating two nested functions and running them prints JSON
span-start/span-end lines that share one `trace_id` and correctly link parent to child — the
architecture working from call site to sink. Flushing is direct/synchronous here; the
non-blocking background worker (SPEC-004) later swaps only *where* a finished span goes.

## Scope

### In Scope

- Global process configuration: `configure(...)` / `get_config()` (`service`, `version`,
  `env`, `sink`, `defaults`), with `StdoutSink` applied as the lazy default.
- W3C Trace Context-compatible ID generation: `trace_id` (16B/32hex), `span_id` (8B/16hex),
  `log_id` (UUID).
- The `Span` data model and the JSON log-event schema, including `build_event`, span
  `start_event` / `end_event`, ISO-8601 UTC timestamps, and `duration_ms` from a monotonic
  clock.
- Field-precedence merge: config `defaults` → per-decorator `defaults` → baggage → per-call
  fields.
- Context propagation via `contextvars`: a span stack (`push_span`/`pop_span`/`current_span`)
  and baggage storage (`get_baggage`/`set_baggage`), correct under threads and asyncio tasks.
- The `Sink` protocol and `StdoutSink` (JSON lines to a stream).
- The synchronous `@trace` decorator: opens a span on enter, records the end on success *or*
  exception, re-raises unchanged, maintains parent/child hierarchy, and flushes directly to
  the configured sink.
- A minimal public façade exposing `configure` and `trace`.

### Out of Scope

- The user logging API (`debug`/`info`/`warning`/`error`/`critical`) and console `echo` —
  SPEC-002. This spec emits only span-boundary events, not user log calls.
- The public `set_baggage` re-export on the façade — SPEC-002 (the underlying `context`
  function is built here, but is not yet exposed on `log_forge`).
- Async `@trace` support — SPEC-003.
- The background flush worker and `shutdown()` — SPEC-004. Flushing here is synchronous and
  inline by design.
- `SQSSink` and the `boto3` optional extra — SPEC-005.
- Sampling / `should_send`, orphan-log handling, follows-from relationships — deferred
  (architecture §10, §5, §3.2).

---

## Functional Requirements

### FR-001: Global configuration

#### Description:

A single module-level `Config` singleton holds process-wide settings, patched once at startup
via `configure(...)` and read everywhere through `get_config()`. `configure` must not create
an import cycle with `sinks`.

#### Acceptance Criteria:

- [ ] `configure(service=, version=, env=, sink=, defaults=)` sets each provided field on the
      singleton; omitted arguments leave the existing value untouched (repeated calls compose).
- [ ] `get_config()` returns the same singleton instance on every call.
- [ ] After `configure()` with no `sink` ever set, `get_config().sink` is a `StdoutSink`
      instance (defaulted lazily inside `configure`, via a local import — no top-level
      `sinks` import in `config.py`).
- [ ] Passing `defaults=` replaces the defaults dict with a copy of the supplied mapping (the
      caller's dict is not aliased).
- [ ] `service`/`version`/`env` default to `"unknown"`/`"0.0.0"`/`"dev"` before any
      `configure` call.

### FR-002: W3C-compatible ID generation

#### Description:

Generate identifiers in the W3C Trace Context wire formats so future cross-service trace
adoption is a cheap header parse.

#### Acceptance Criteria:

- [ ] `new_trace_id()` returns 32 lowercase hex characters (16 random bytes).
- [ ] `new_span_id()` returns 16 lowercase hex characters (8 random bytes).
- [ ] `new_log_id()` returns a UUID4 hex string.
- [ ] IDs are sourced from `os.urandom` / `uuid4` (cryptographically random); successive calls
      return distinct values.

### FR-003: Span model and event schema

#### Description:

Represent a span and serialize its events into the exact JSON schema (architecture §6),
centralizing serialization so every event has identical shape.

#### Acceptance Criteria:

- [ ] `Span` carries `trace_id`, `span_id`, `parent_span_id`, `name`, `start_ts`, `defaults`,
      and an `events` list.
- [ ] `build_event(span, level, message, *, fields, baggage)` returns a dict with exactly the
      base keys: `timestamp`, `level`, `message`, `trace_id`, `span_id`, `parent_span_id`,
      `log_id`, `function`, `service`, `version`, `env`, `fields`.
- [ ] `fields` in the built event is the precedence merge — config `defaults`, then
      `span.defaults`, then `baggage`, then per-call `fields` (later wins on key conflict).
- [ ] `timestamp` is UTC ISO-8601 with millisecond precision and a trailing `Z`
      (e.g. `2024-01-15T14:23:01.842Z`).
- [ ] `start_event(span)` builds a `level="INFO"` event named for span start; `end_event(span,
      status, exc=None)` builds the end event carrying `duration_ms`, `status`
      (`"ok"`/`"error"`), and — when `exc` is not `None` — `error.type` and `error.stack`.
- [ ] `duration_ms` is computed from `time.monotonic()` deltas (never wall-clock), so it can
      never be negative.
- [ ] `model.py` imports neither `context` nor `decorator` (it only builds records).

### FR-004: Context propagation (span stack + baggage)

#### Description:

Track the active span and baggage per execution flow using `contextvars`, correct under both
threads and asyncio, with no manual passing.

#### Acceptance Criteria:

- [ ] `current_span()` returns the top of the span stack, or `None` when the stack is empty.
- [ ] `push_span(span)` returns a token; `pop_span(token)` restores the exact prior stack via
      `ContextVar.reset(token)`.
- [ ] `get_baggage()` returns the current baggage dict; `set_baggage(**kv)` merges keys by
      replacing the ContextVar with a *new* dict (never mutating in place).
- [ ] The ContextVar defaults (`()` / `{}`) are never mutated; nested pushes/pops in one flow
      do not corrupt a sibling flow's stack or baggage.

### FR-005: Sink protocol and StdoutSink

#### Description:

Define the output interface and a zero-dependency implementation that writes JSON lines.

#### Acceptance Criteria:

- [ ] A `Sink` Protocol declares `emit(batch: list[dict]) -> None` and `close() -> None`.
- [ ] `StdoutSink.emit(batch)` writes each event as one `json.dumps(event)` line terminated by
      `\n` to its stream, then flushes.
- [ ] `StdoutSink.close()` flushes the stream.
- [ ] `StdoutSink` accepts an injectable stream (default `sys.stdout`) so tests can capture
      output.
- [ ] The sink references no span/context types — it operates purely on event dicts.

### FR-006: Synchronous `@trace` decorator

#### Description:

Open a span on enter and close it on exit (success or exception), maintaining trace/parent
hierarchy, re-raising all exceptions unchanged, and flushing the finished span directly to the
configured sink.

#### Acceptance Criteria:

- [ ] `@trace` works bare (`@trace`) and parameterized (`@trace(name=..., defaults=...)`).
- [ ] On enter: if a parent span is active, the new span inherits its `trace_id` and sets
      `parent_span_id` to the parent's `span_id`; otherwise a fresh `trace_id` is minted and
      `parent_span_id` is `None`.
- [ ] The span `name` (and the event `function` field) defaults to the wrapped function's
      `__qualname__` unless `name=` overrides it.
- [ ] A start event is recorded on enter and an end event on exit; the end event is appended
      **before** the flush so the flushed queue is complete.
- [ ] On success the end event has `status="ok"`; on exception it has `status="error"` with
      `error.type`/`error.stack`, and the original exception is re-raised unchanged.
- [ ] The decorator catches `BaseException` (so `KeyboardInterrupt`/timeouts are recorded) but
      always re-raises; it never swallows.
- [ ] The span is popped from the context stack in a `finally` block regardless of outcome.
- [ ] For two nested decorated calls, all emitted events share one `trace_id` and the child's
      `parent_span_id` equals the parent's `span_id`.

### FR-007: Minimal public façade

#### Description:

Expose the smallest public surface needed to run the end-to-end demo.

#### Acceptance Criteria:

- [ ] `import log_forge` exposes `log_forge.configure` and `log_forge.trace`.
- [ ] `log_forge.__all__` lists exactly the intended public names for this spec.
- [ ] Running the architecture §11 example (two nested `@trace` functions) with a `StdoutSink`
      prints JSON span-start/span-end lines exhibiting the shared-`trace_id` / linked-parent
      behavior from FR-006.

---

## Data Model

```
# src/log_forge/config.py
Config {
  service: str = "unknown"
  version: str = "0.0.0"
  env: str = "dev"
  sink: Sink | None = None          # defaulted to StdoutSink() lazily in configure()
  defaults: dict[str, object] = {}
}

# src/log_forge/model.py
Span {
  trace_id: str
  span_id: str
  parent_span_id: str | None
  name: str
  start_ts: float                    # time.monotonic() at span open
  defaults: dict[str, object] = {}
  events: list[dict[str, object]] = []
}

# One serialized log event (the sink's unit of work)
LogEvent {
  timestamp: str                     # ISO-8601 UTC, ms precision, trailing "Z"
  level: str                         # "INFO" | "DEBUG" | ...
  message: str
  trace_id: str
  span_id: str
  parent_span_id: str | None
  log_id: str
  function: str
  service: str
  version: str
  env: str
  fields: dict[str, object]          # merged defaults→span.defaults→baggage→call fields
  # end event only, additionally:
  # duration_ms: float, status: "ok"|"error", error: {type, stack}?
}
```

---

## API / Interface Contract

```python
# config.py
def configure(*, service=None, version=None, env=None, sink=None, defaults=None) -> None
def get_config() -> Config

# ids.py
def new_trace_id() -> str      # 32 hex
def new_span_id() -> str       # 16 hex
def new_log_id() -> str        # uuid4 hex

# model.py
def build_event(span: Span, level: str, message: str, *, fields: dict, baggage: dict) -> dict
def start_event(span: Span) -> dict
def end_event(span: Span, status: str, exc: BaseException | None = None) -> dict

# context.py
def current_span() -> Span | None
def push_span(span: Span) -> Token
def pop_span(token: Token) -> None
def get_baggage() -> dict
def set_baggage(**kv) -> None

# sinks/base.py
class Sink(Protocol):
    def emit(self, batch: list[dict]) -> None: ...
    def close(self) -> None: ...

# sinks/stdout.py
class StdoutSink:
    def __init__(self, stream=sys.stdout) -> None: ...

# decorator.py / __init__.py
def trace(func=None, *, name=None, defaults=None): ...

# Example (architecture §11)
import log_forge
from log_forge.sinks.stdout import StdoutSink

log_forge.configure(service="payments", version="2.14", env="prod", sink=StdoutSink())

@log_forge.trace
def process_payment(user_id: int) -> str:
    write_ledger(user_id)
    return "ok"

@log_forge.trace
def write_ledger(user_id: int) -> None:
    ...

process_payment(4127)   # prints span-start/end JSON for both calls, one shared trace_id
```

## Configuration / Environment

No new environment variables. Runtime configuration is entirely via `log_forge.configure(...)`
(FR-001). No new dependencies — the core stays dependency-free.

## File & Folder Structure

```
src/log_forge/
├── __init__.py        # public façade: configure, trace
├── config.py          # Config + configure/get_config          (exists — WIP folded in)
├── ids.py             # new_trace_id/new_span_id/new_log_id
├── model.py           # Span + build_event/start_event/end_event
├── context.py         # contextvars span stack + baggage
├── decorator.py       # sync @trace
└── sinks/
    ├── __init__.py
    ├── base.py        # Sink protocol                          (exists)
    └── stdout.py      # StdoutSink
tests/
├── test_config.py     # exists
├── test_ids.py
├── test_model.py
├── test_context.py
├── test_sinks_stdout.py
└── test_decorator.py
```

## Implementation Phases

### Phase 1: Config, IDs, and model (pure units)

- Reconcile the existing `config.py` WIP against FR-001 (confirm compose-on-repeat and the
  lazy `StdoutSink` default now that `stdout.py` lands in Phase 3).
- Implement `ids.py` (FR-002).
- Implement `model.py`: `Span`, `_iso_now`, `build_event`, `start_event`, `end_event`,
  precedence merge, monotonic `duration_ms` (FR-003).
- Unit-test each in isolation with no context/decorator involvement.

### Phase 2: Context

- Implement `context.py` span stack + baggage over `contextvars` (FR-004).
- Test push/pop nesting, token-based reset, and baggage merge; assert no cross-flow corruption.

### Phase 3: Sink protocol + StdoutSink

- Confirm the existing `sinks/base.py` protocol matches FR-005.
- Implement `sinks/stdout.py` with an injectable stream (FR-005).
- Test `emit` line formatting and flush behavior against a captured stream.

### Phase 4: Sync `@trace` + façade → first runnable demo

- Implement `decorator.py` sync `trace` with span lifecycle, hierarchy, non-swallowing
  re-raise, and direct `sink.emit` flush (FR-006).
- Wire `__init__.py` to export `configure` and `trace` (FR-007).
- Add a decorator test asserting nested spans share `trace_id` and link parent→child (use a
  fake sink capturing dicts); run the architecture §11 example end to end.
