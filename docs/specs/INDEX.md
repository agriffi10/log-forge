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
| [SPEC-016](SPEC-016-sqs-fifo-support.md) | FIFO Queue Support for `SQSSink` | Completed | SPEC-005 |
| [SPEC-017](SPEC-017-payload-and-failure-safety.md) | Payload and Failure Safety | Completed | SPEC-001, SPEC-004, SPEC-006 |
| [SPEC-018](SPEC-018-batch-response-adjudication.md) | Batch Response Adjudication | Completed | SPEC-010, SPEC-017 |
| [SPEC-019](SPEC-019-worker-liveness.md) | Worker Liveness and Terminal-Failure Reporting | Completed | SPEC-004, SPEC-017 |
| [SPEC-020](SPEC-020-integer-value-bounds.md) | Integer Value Bounds | Completed | SPEC-017 |
| [SPEC-021](SPEC-021-open-item-cleanup.md) | Open-Item Cleanup | Completed | SPEC-013, SPEC-017, SPEC-019, SPEC-020 |
| [SPEC-022](SPEC-022-security-scanning.md) | Security Scanning in CI | Draft | SPEC-012 |

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
- **Robustness:** SPEC-017 — standalone, and the first spec driven by an audit rather than a missing
  feature. Every item is a case where the library breaks its own stated promise (logging never breaks
  the app; a broken destination degrades logging and nothing more): an unserializable field raises
  into the caller, an unbounded value gets a whole event discarded downstream, and an all-children-down
  `MultiSink` reports success so the retry never runs. Buildable at any point; nothing depends on it.
  **SPEC-018** continues it into the two sinks 017 did not reach: `KinesisSink` and `FirehoseSink`
  adjudicate a batch response by position without checking the arrays line up, so a short response
  truncates the retry list and the chunk reports success. Same promise, same failure shape, found by
  a linter rather than an audit. Standalone; nothing depends on it.
  **SPEC-019** closes the arc one level up, at the thread all three run on: the drain loop has no
  terminal-failure path, so an escape stops delivery with nothing recorded and `health()` — the
  detector SPEC-017 added — keeps reporting a healthy snapshot. Same promise, same failure shape,
  found by reading the code that SPEC-018's own review had just been pointed at. Standalone.
  **SPEC-020** closes the last hole in SPEC-017 itself: `int` is the one type left unbounded, and
  CPython 3.11+ refuses to render one past 4300 digits, so `json.dumps` raises — into the caller on
  the orphan path, and into a whole abandoned batch inside a span. The same promise again, this time
  breached by the spec that made the promise. Standalone.
  **SPEC-021** closes the arc's paperwork as well as its last wart: four specs of "Notes for the
  next spec" plus `architecture.md` §12 left 20 items a reader cannot triage, two of them now false.
  Every item ends fixed, settled, or recorded as a constraint — and `flush()` stops returning `True`
  for a drain that was abandoned. Standalone; nothing depends on it.
- **Supply-chain and code security:** SPEC-022 — depends on SPEC-012 only because that spec built the
  publish path this one protects: `release.yml` exchanges an OIDC token for the right to ship
  `log-foundry` to PyPI, and every action it calls is pinned to a mutable tag. Touches only `.github/`
  and `SECURITY.md`, never `src/`. Buildable at any point; nothing depends on it.
