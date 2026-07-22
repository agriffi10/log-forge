# Spec Index

One row per spec. **Status** here mirrors the spec file header (the header is authoritative). Keep this
to status only — no prose.

| Spec | Title | Status | Depends On |
|------|-------|--------|------------|
| [SPEC-001](SPEC-001-core-span-pipeline.md) | Core Span Pipeline | Completed | None |
| [SPEC-002](SPEC-002-logging-api-and-console-echo.md) | Logging API and Console Echo | Completed | SPEC-001 |
| [SPEC-003](SPEC-003-async-trace.md) | Async `@trace` Support | Completed | SPEC-001, SPEC-002 |
| [SPEC-004](SPEC-004-background-worker.md) | Background Flush Worker and Graceful Shutdown | Completed | SPEC-001 |
| [SPEC-005](SPEC-005-sqs-sink.md) | SQSSink and Optional `sqs` Extra | Completed | SPEC-004 |
| [SPEC-006](SPEC-006-composition-and-adapter-sinks.md) | Composition and Adapter Sinks | Completed | SPEC-001 |
| [SPEC-007](SPEC-007-stdlib-logging-sink.md) | Stdlib Logging Bridge Sink | Completed | SPEC-001 |
| [SPEC-008](SPEC-008-local-file-and-embedded-sinks.md) | Local File and Embedded Sinks | Completed | SPEC-001 |
| [SPEC-009](SPEC-009-http-and-platform-sinks.md) | HTTP and Log-Platform Sinks | Completed | SPEC-001, SPEC-005 |
| [SPEC-010](SPEC-010-queue-and-stream-sinks.md) | Queue and Stream Buffer Sinks | Completed | SPEC-005 |
| [SPEC-011](SPEC-011-database-sinks.md) | Database Sinks | Completed | SPEC-001, SPEC-005 |
| [SPEC-012](SPEC-012-pypi-publishing-and-dynamic-versioning.md) | PyPI Publishing and Dynamic Versioning | Completed | None |
| [SPEC-013](SPEC-013-aws-lambda-compatibility.md) | AWS Lambda Compatibility — Python 3.12 Support and a Repeatable `flush()` | Completed | SPEC-004, SPEC-012 |
| [SPEC-014](SPEC-014-cross-process-trace-continuation.md) | Cross-Process Trace Continuation (W3C `traceparent` + baggage) | Completed | SPEC-001, SPEC-002, SPEC-013 |
| [SPEC-015](SPEC-015-baggage-on-boundary-events.md) | Baggage on Boundary Events | Completed | SPEC-002, SPEC-014 |

## Arcs (build order)

Group related specs and record the order to build them in. Delete this section if you don't use arcs.

- **Core logging pipeline:** SPEC-001 → SPEC-002 → SPEC-003 → SPEC-004 → SPEC-005
- **Sink expansion (pluggable destinations):** SPEC-006 → SPEC-007 → SPEC-008 → SPEC-009 → SPEC-010 → SPEC-011.
  SPEC-006..008 are independent zero-dependency specs (any order); SPEC-009..011 reuse the SPEC-005
  optional-extra + lazy-import + bounded-retry pattern for third-party transports.
- **Release and distribution:** SPEC-012 — standalone; depends on no prior spec and touches only
  packaging config and CI, not the library runtime.
- **Serverless usability:** SPEC-013 — standalone. Two changes with one cause: the library assumes a
  process that starts, runs and exits, and a Lambda gives it one that is frozen, thawed and killed
  without warning. Nothing can *install* it there (`requires-python >=3.13` excludes the 3.12
  runtime), and nothing can *flush* it there (`shutdown()` is terminal — the worker never comes back,
  so a handler that flushes the obvious way logs only its first invocation per warm container).
  Adds a repeatable `flush()` and lowers the floor to 3.12. Driven by a real consumer.
- **Cross-process correlation:** SPEC-014 — cashes in the `architecture.md` §12 deferral ("adopting
  an inbound `trace_id` from a `traceparent` header, plus cross-process baggage"), which `ids.py`'s
  W3C-compatible formats were chosen to make cheap. Ships both halves — `current_traceparent()` to
  publish, `continue_trace()` to adopt — because a context nobody can read is a context nobody can
  propagate. Build **after** SPEC-013: 013 is what makes the library usable in the multi-process
  environment 014 exists to correlate.
