# Spec: Logging API and Console Echo

**ID:** SPEC-002
**Status:** Completed
**Last Updated:** 2026-07-09
**Depends On:** SPEC-001

## Overview

With the span pipeline in place (SPEC-001), user code needs a way to emit its own structured
log events *inside* a decorated call. This spec adds the level methods
(`debug`/`info`/`warning`/`error`/`critical`) that append a structured event to the current
span's queue, the `set_baggage` public entry point for trace-scoped dynamic context, and a
second, synchronous output path — console `echo` — for surfacing a human-readable line
immediately (to a terminal user or a Lambda's stdout → CloudWatch) without waiting for the
async flush. Echo is *additive*: an echoed event still rides the normal pipeline to the sink.
It also resolves the orphan-log question — a level call made with no active span emits a
standalone single-event span with a fresh `trace_id` so nothing is silently dropped.

## Scope

### In Scope

- Level functions `debug`, `info`, `warning`, `error`, `critical`, each appending an event
  (built via SPEC-001 `build_event`, stamped with current baggage) to the current span.
- `echo=` per-call flag: also write the event, human-readable and immediately, to the console.
- A `ConsoleWriter` (default stream `sys.stderr`) that renders `LEVEL   message` lines,
  separate from the async `Sink`.
- Public `set_baggage(**kv)` on the façade, re-exporting the SPEC-001 context function.
- Orphan-log handling: a level call with no active span emits a standalone one-event span with
  a fresh `trace_id`, flushed directly to the configured sink.
- Extending `log_foundry.__all__` with the new public names.

### Out of Scope

- An `echo_level` auto-echo threshold (e.g. auto-echo everything `>= WARNING`) — deferred; only
  explicit per-call `echo=` ships in this spec. Global echo destination/format overrides beyond
  the documented defaults are also deferred.
- Async decorator support — SPEC-003.
- The background worker — SPEC-004 (this spec's events still flush via the span's existing
  path from SPEC-001; orphan events flush directly to `sink.emit`).
- Any change to the span-boundary events or schema defined in SPEC-001.

---

## Functional Requirements

### FR-001: Level logging functions

#### Description:

Provide `debug`/`info`/`warning`/`error`/`critical`, each appending one structured event to the
current span's event queue with the correct level.

#### Acceptance Criteria:

- [ ] Each function has signature `(message: str, *, echo: bool = False, **fields) -> None`.
- [ ] Called inside an active span, the function appends exactly one event to that span's
      `events`, built via SPEC-001 `build_event` with the current baggage
      (`context.get_baggage()`) and the passed `fields`.
- [ ] The event's `level` is the uppercase level name (`"DEBUG"`, `"INFO"`, `"WARNING"`,
      `"ERROR"`, `"CRITICAL"`).
- [ ] The appended event carries the span's `trace_id`/`span_id`/`parent_span_id` and a unique
      `log_id`, per the SPEC-001 schema.
- [ ] Field precedence follows SPEC-001 (config defaults → span defaults → baggage → call
      `fields`).

### FR-002: Console echo

#### Description:

`echo=True` additionally writes the event to the console synchronously and human-readable, in
addition to (not instead of) appending it to the span queue.

#### Acceptance Criteria:

- [ ] With `echo=True`, the event is appended to the span queue **and** written to the console
      writer in the same call.
- [ ] With `echo=False` (default), nothing is written to the console.
- [ ] The console line is human-readable (default format `f'{level:<7} {message}\n'`), not
      JSON, and is written to `sys.stderr` by default.
- [ ] The console write is synchronous and flushed immediately (independent of any sink or
      worker).
- [ ] Echo never redirects: the event still reaches the sink via the normal path.

### FR-003: Public `set_baggage`

#### Description:

Expose baggage setting on the public façade, re-exporting the SPEC-001 `context.set_baggage`.

#### Acceptance Criteria:

- [ ] `log_foundry.set_baggage(**kv)` merges the keys into the current trace's baggage (via the
      SPEC-001 context function; new dict, no in-place mutation).
- [ ] Keys set via `set_baggage` appear in the `fields` of every subsequent event emitted at or
      below that point in the same execution flow (respecting FR-001 precedence).
- [ ] `set_baggage` is listed in `log_foundry.__all__`.

### FR-004: Orphan-log handling

#### Description:

A level call made when no span is active must still be recorded, not dropped.

#### Acceptance Criteria:

- [ ] When `current_span()` is `None`, the level function builds a standalone one-event span
      with a fresh `trace_id`, a new `span_id`, and `parent_span_id=None`.
- [ ] The orphan event carries the requested level, message, and merged fields, and is flushed
      directly to the configured sink (`get_config().sink.emit([...])`).
- [ ] `echo=True` on an orphan log still writes the human-readable console line.
- [ ] No exception is raised and no event is lost when logging outside any span.

---

## Data Model

No new persistent types. Events use the SPEC-001 `LogEvent` schema. The orphan path constructs
a transient SPEC-001 `Span` (fresh `trace_id`, `parent_span_id=None`) solely to build and flush
its single event.

---

## API / Interface Contract

```python
# console.py
class ConsoleWriter:
    def __init__(self, stream=sys.stderr) -> None: ...
    def write(self, event: dict) -> None: ...      # human-readable "LEVEL   message" line

# api.py
def debug(message: str, *, echo: bool = False, **fields) -> None
def info(message: str, *, echo: bool = False, **fields) -> None
def warning(message: str, *, echo: bool = False, **fields) -> None
def error(message: str, *, echo: bool = False, **fields) -> None
def critical(message: str, *, echo: bool = False, **fields) -> None
def set_baggage(**kv) -> None                       # re-exports context.set_baggage

# Example
@log_foundry.trace
def process_payment(user_id: int) -> str:
    log_foundry.set_baggage(request_id="req-123")     # rides every log below
    log_foundry.info("charging card", user_id=user_id)
    log_foundry.info("payment complete", echo=True)   # also printed to console now
    return "ok"
```

## Configuration / Environment

No new environment variables or dependencies. Console defaults (stream `sys.stderr`, format
`LEVEL   message`) are fixed constants in this spec; configurable overrides are deferred (see
Out of Scope).

## File & Folder Structure

```
src/log_foundry/
├── __init__.py        # façade: + debug/info/warning/error/critical, set_baggage
├── api.py             # level functions + _log helper + set_baggage re-export   (new)
└── console.py         # ConsoleWriter                                           (new)
tests/
├── test_api.py        # level append, precedence, orphan path                   (new)
└── test_console_echo.py  # echo additivity + format                            (new)
```

## Implementation Phases

### Phase 1: Console writer + level API

- Implement `console.py` `ConsoleWriter` (FR-002 formatting/stream).
- Implement `api.py` `_log` + the five level functions, appending to the current span and
  honoring `echo=` (FR-001, FR-002).
- Test append-to-span, level correctness, precedence, and echo additivity with a fake sink and
  a captured console stream.

### Phase 2: Orphan handling + façade wiring

- Implement the orphan branch in `_log` (fresh-trace standalone span → direct flush) (FR-004).
- Add `set_baggage` re-export and wire all new names into `__init__.py` / `__all__`
  (FR-003).
- Test orphan flush and that baggage set via the façade lands in downstream event `fields`.
