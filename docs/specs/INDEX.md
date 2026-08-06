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
| [SPEC-022](SPEC-022-security-scanning.md) | Security Scanning in CI | Completed | SPEC-012 |
| [SPEC-023](SPEC-023-supply-chain-transparency.md) | Supply-Chain Transparency and Dependency Auditing | Completed | SPEC-012, SPEC-022 |
| [SPEC-024](SPEC-024-context-lifetime.md) | Context Lifetime — Scoping Baggage and Adopted Trace Context | Completed | SPEC-014, SPEC-015 |
| [SPEC-025](SPEC-025-never-fail-the-caller.md) | The Library Must Not Fail the Caller | Completed | SPEC-004, SPEC-017 |
| [SPEC-026](SPEC-026-sink-loss-visibility.md) | Sink Loss Visibility | Completed | SPEC-017, SPEC-018, SPEC-019, SPEC-021 |
| [SPEC-027](SPEC-027-bounded-interruptible-retry.md) | Bounded, Interruptible Retry | Completed | SPEC-004, SPEC-009, SPEC-013 |
| [SPEC-028](SPEC-028-sink-concurrency-contract.md) | The Sink Concurrency Contract | Draft | SPEC-002, SPEC-004, SPEC-008 |
| [SPEC-029](SPEC-029-diagnostic-output-safety.md) | Diagnostic Output Safety | Completed | SPEC-017, SPEC-019 |
| [SPEC-030](SPEC-030-lifecycle-signals.md) | Lifecycle Signals — Post-Shutdown Logging and Late Reconfiguration | Draft | SPEC-013, SPEC-019 |
| [SPEC-031](SPEC-031-audit-small-corrections.md) | Audit Small Corrections | Draft | SPEC-008, SPEC-009, SPEC-020 |

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
  **SPEC-023** turns that arc outward. SPEC-022's scanners all look *inward* — CodeQL at this
  source, zizmor at these workflows, `dependency-review` at what a PR *introduces* — so nothing
  describes what already shipped, and nothing re-examines the eleven extras after the merge that
  pinned them. A CVE published against a dependency nobody is currently touching produces no diff
  for `dependency-review` to fail. Adds a CycloneDX SBOM per release, a scheduled `pip-audit` over
  the full extras surface, and OpenSSF Scorecard as standing measurement of what SPEC-022 built.
  Depends on SPEC-012 for the same reason 022 does — it extends the publish path — and on SPEC-022
  for the pinning and least-privilege conventions it inherits. Touches no `src/` file.
- **Audit remediation (SPEC-024..031):** the output of the 2026-08-05 full-codebase audit, whose
  findings were validated in a second fresh context before being written up. The suite was green
  throughout (568 tests, `ruff`, `mypy --strict`), so every item here is behaviour no gate catches.
  Build order is by blast radius, not by number:
  **SPEC-024** first and alone (**shipped**) — baggage and the adopted trace context were never
  taken back out of `contextvars`, so a request's data appeared on the next request's events and a
  handler kept joining a trace whose process had exited. It was the only finding that puts *wrong
  data* in the log stream rather than losing it, and the fix was small and self-contained.
  **SPEC-025** next (**shipped**) — three surviving instances of the SPEC-017 shape, where the
  exception a caller received was one the library invented: an unguarded `_close_span` failed a
  function that had already returned (and emitted a contradictory second `span.end`), the orphan
  path propagated a sink's failure, and `shutdown()` raised out of `atexit` while leaving the sink
  open forever. It also brought `_open_span` into scope and shipped `_diag.py`.
  **SPEC-029** before SPEC-026 (**shipped**) — both use `_diag.py`, which SPEC-025 shipped and 029
  owns. Twelve sink sites printed `repr(exception)` against the arch §6 rule that
  `_terminal_failure` cites for not doing it, and two unguarded stderr writes (`_emit` and
  `SocketTransport`) killed the drain thread on a broken stream. All 28 sites are converted and a
  test forbids any other module writing to stderr.
  **SPEC-026** after SPEC-029 (**shipped**) — the largest. Every remote transport absorbed its own failures and
  returned normally, so `failed_batches`, the worker's retry and SPEC-021's `flush()` contract were
  all inert against a down destination, while the counters that *did* record the loss had no
  accessor. SPEC-017 FR-004's rule, generalized to the whole sink family.
  **SPEC-027** (**shipped**) and **SPEC-028** may go in either order after 026, and both touch
  every sink: 027 bounded a server-supplied `Retry-After` (a measured 22 s `shutdown()` hang, and
  24 h was reachable) and made every sink wait interruptible; 028 states the concurrency contract
  the orphan path has been violating since SPEC-002 and locks the sinks that hold mutable
  transport state.
  **SPEC-030** after 026 (it appends to the same `Health`) — two documented user errors that produce
  total silent loss with no signal: logging after `shutdown()`, and a late `configure(sink=...)`.
  **SPEC-031** last, and independent of all of them — the residue too small to spec individually,
  handled per SPEC-021's rule that an open item is fixed, settled, or recorded as a constraint.
