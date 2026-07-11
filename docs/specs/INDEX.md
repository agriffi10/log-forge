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
| [SPEC-006](SPEC-006-composition-and-adapter-sinks.md) | Composition and Adapter Sinks | In Progress | SPEC-001 |
| [SPEC-007](SPEC-007-stdlib-logging-sink.md) | Stdlib Logging Bridge Sink | Draft | SPEC-001 |
| [SPEC-008](SPEC-008-local-file-and-embedded-sinks.md) | Local File and Embedded Sinks | Draft | SPEC-001 |
| [SPEC-009](SPEC-009-http-and-platform-sinks.md) | HTTP and Log-Platform Sinks | Draft | SPEC-001, SPEC-005 |
| [SPEC-010](SPEC-010-queue-and-stream-sinks.md) | Queue and Stream Buffer Sinks | Draft | SPEC-005 |
| [SPEC-011](SPEC-011-database-sinks.md) | Database Sinks | Draft | SPEC-001, SPEC-005 |

## Arcs (build order)

Group related specs and record the order to build them in. Delete this section if you don't use arcs.

- **Core logging pipeline:** SPEC-001 → SPEC-002 → SPEC-003 → SPEC-004 → SPEC-005
- **Sink expansion (pluggable destinations):** SPEC-006 → SPEC-007 → SPEC-008 → SPEC-009 → SPEC-010 → SPEC-011.
  SPEC-006..008 are independent zero-dependency specs (any order); SPEC-009..011 reuse the SPEC-005
  optional-extra + lazy-import + bounded-retry pattern for third-party transports.
