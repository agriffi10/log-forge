# Spec Index

One row per spec. **Status** here mirrors the spec file header (the header is authoritative). Keep this
to status only — no prose.

| Spec | Title | Status | Depends On |
|------|-------|--------|------------|
| [SPEC-001](SPEC-001-core-span-pipeline.md) | Core Span Pipeline | Draft | None |
| [SPEC-002](SPEC-002-logging-api-and-console-echo.md) | Logging API and Console Echo | Draft | SPEC-001 |
| [SPEC-003](SPEC-003-async-trace.md) | Async `@trace` Support | Draft | SPEC-001, SPEC-002 |
| [SPEC-004](SPEC-004-background-worker.md) | Background Flush Worker and Graceful Shutdown | Draft | SPEC-001 |
| [SPEC-005](SPEC-005-sqs-sink.md) | SQSSink and Optional `sqs` Extra | Draft | SPEC-004 |

## Arcs (build order)

Group related specs and record the order to build them in. Delete this section if you don't use arcs.

- **Core logging pipeline:** SPEC-001 → SPEC-002 → SPEC-003 → SPEC-004 → SPEC-005
