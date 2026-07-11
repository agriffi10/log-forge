# Spec: Stdlib Logging Bridge Sink

**ID:** SPEC-007
**Status:** In Progress
**Last Updated:** 2026-07-10
**Depends On:** SPEC-001

## Overview

Rather than reimplement rotating files, syslog, or the dozens of third-party log handlers that
already exist, `LoggingSink` bridges log-forge into Python's standard `logging` framework: each
built event dict is turned into a `logging.LogRecord` and dispatched through a configurable logger.
That single adapter hands users the entire stdlib logging ecosystem — their existing handler/format
configuration, `logging.config` setup, and any third-party handler (Sentry, Datadog, systemd) — for
free, with no new dependency. It also lets a team funnel structured span events into the same logging
pipeline they already operate. Like every sink (arch §8) it receives already-built event dicts and
knows nothing about spans or context.

## Scope

### In Scope

- `LoggingSink(logger=None, *, default_level="INFO")` implementing the SPEC-001 `Sink` protocol.
- Dispatching one `LogRecord` per event to the target logger (default `logging.getLogger("log_forge")`).
- Mapping the event's `level` string (`DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`) to stdlib numeric
  levels, falling back to `default_level` for unknown/missing levels.
- Carrying the event's structured `fields` and identity keys (`trace_id`, `span_id`, `function`,
  `service`, …) onto the record so structured formatters can render them, without clobbering
  reserved `LogRecord` attributes.
- `close()` as a no-op — the sink does not own or tear down the user's logging configuration.
- Zero runtime dependency (stdlib `logging` only).

### Out of Scope

- Configuring loggers, handlers, formatters, or levels on the user's behalf — the user owns their
  `logging` setup; `LoggingSink` only *emits* records into it.
- The reverse bridge (capturing stdlib `logging` output back into spans) — out of scope.
- Guaranteeing a particular on-disk format — that is whatever handler/formatter the user has attached
  to their logger.

---

## Functional Requirements

### FR-001: Dispatch each event as a LogRecord

#### Description:

Every event becomes exactly one `LogRecord` handed to a logger, so log-forge output flows through the
user's existing logging pipeline.

#### Acceptance Criteria:

- [ ] `LoggingSink()` targets `logging.getLogger("log_forge")` by default; `LoggingSink(logger)`
      targets the injected logger.
- [ ] `emit(batch)` dispatches one record per event, in batch order, via the logger's handling path
      (e.g. `logger.handle(record)`), so attached handlers receive them.
- [ ] A test can attach a capturing handler to an injected logger and assert one record per event
      with no network/handler side effects.
- [ ] `isinstance(LoggingSink(), Sink)` is `True`.

### FR-002: Level mapping

#### Description:

The event's textual level maps to a stdlib numeric level so handler-level filtering works as users
expect.

#### Acceptance Criteria:

- [ ] `DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL` map to `logging.DEBUG`…`logging.CRITICAL`
      respectively; the resulting record's `levelno`/`levelname` reflect the mapping.
- [ ] An unknown or missing `level` maps to `default_level` (itself defaulting to `INFO`).
- [ ] Level comparison/mapping is case-insensitive on the incoming string.

### FR-003: Structured fields on the record (reserved-attr safe)

#### Description:

Structured data rides on the record so structured formatters can render it, without corrupting the
record's built-in attributes.

#### Acceptance Criteria:

- [ ] Identity keys (`trace_id`, `span_id`, `parent_span_id`, `log_id`, `function`, `service`,
      `version`, `env`) are attached to the record as flat attributes (all known non-reserved) and
      are readable by a formatter.
- [ ] Each `event["fields"]` entry is attached as a flat record attribute, **skipping** any key that
      collides with a reserved `LogRecord` attribute or an identity key; **and** the complete
      `event["fields"]` dict is always attached as `record.fields`, so a skipped collision is never
      lost (flat + nested).
- [ ] Attaching never overwrites reserved `LogRecord` attributes (`name`, `msg`, `args`, `levelname`,
      `levelno`, `pathname`, `lineno`, `message`, `asctime`, etc.) and never raises
      `KeyError`/`AttributeError`.
- [ ] A JSON formatter reading these record attributes (flat fields, or the nested `record.fields`)
      can reproduce the event's structured fields — including a field whose key collides with a
      reserved attribute (recoverable via `record.fields`).

### FR-004: Message passed verbatim (no %-formatting surprise)

#### Description:

The record's message is the event's `message` as-is; logging must not try to treat it as a `%`
format template.

#### Acceptance Criteria:

- [ ] The record's rendered message equals `event["message"]` exactly, including when the message
      contains literal `%` characters (no `TypeError`/`ValueError` from format interpolation).
- [ ] No positional `args` are passed such that logging would attempt interpolation.

### FR-005: close() is a no-op

#### Description:

`LoggingSink` must not shut down or flush the user's logging framework on `close()`.

#### Acceptance Criteria:

- [ ] `LoggingSink.close()` returns without error and does not call `logging.shutdown()` or remove
      handlers from the target logger.

---

## Data Model

```
# src/log_forge/sinks/logging_sink.py
LoggingSink {
  logger: logging.Logger           # default logging.getLogger("log_forge")
  default_level: int               # resolved from "INFO" (or the constructor arg)
}

# Level name -> logging numeric level
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
```

Events are the SPEC-001 `LogEvent` dicts. The record's `msg` is `event["message"]`; `levelno` comes
from `event["level"]`; structured/identity keys are attached as record attributes.

---

## API / Interface Contract

```python
# sinks/logging_sink.py
class LoggingSink:
    def __init__(self, logger=None, *, default_level="INFO") -> None: ...
    def emit(self, batch: list[dict]) -> None: ...   # one LogRecord per event -> logger.handle
    def close(self) -> None: ...                     # no-op

# Usage — funnel span events through the app's existing logging config
import logging, log_forge
from log_forge.sinks.logging_sink import LoggingSink

logging.basicConfig(level=logging.INFO)              # user's own logging setup / handlers
log_forge.configure(sink=LoggingSink(logging.getLogger("app.telemetry")))
```

## Configuration / Environment

None. Stdlib-only; no new config keys, env vars, or dependencies.

## File & Folder Structure

```
src/log_forge/sinks/
└── logging_sink.py     # LoggingSink (module named *_sink to avoid shadowing stdlib `logging`) (new)
tests/
└── test_sinks_logging.py   # dispatch, level mapping, reserved-attr safety, literal-% message (new)
```

## Implementation Phases

### Phase 1: Dispatch + level mapping + message safety

- Implement `LoggingSink` construction, the level-string→numeric map with `default_level` fallback,
  and per-event dispatch to the target logger with the message passed verbatim (FR-001, FR-002,
  FR-004, FR-005).
- Test dispatch count/order against a capturing handler, level mapping (including unknown→default),
  and a literal-`%` message.

### Phase 2: Structured fields on the record

- Attach `fields` + identity keys to each record without clobbering reserved attributes (FR-003).
- Test that a formatter can read the attached structured data and that reserved attributes are never
  overwritten.
