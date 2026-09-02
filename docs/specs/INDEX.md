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
| [SPEC-028](SPEC-028-sink-concurrency-contract.md) | The Sink Concurrency Contract | Completed | SPEC-002, SPEC-004, SPEC-008 |
| [SPEC-029](SPEC-029-diagnostic-output-safety.md) | Diagnostic Output Safety | Completed | SPEC-017, SPEC-019 |
| [SPEC-030](SPEC-030-lifecycle-signals.md) | Lifecycle Signals — Post-Shutdown Logging and Late Reconfiguration | Completed | SPEC-013, SPEC-019 |
| [SPEC-031](SPEC-031-audit-small-corrections.md) | Audit Small Corrections | Completed | SPEC-002, SPEC-004, SPEC-008, SPEC-009, SPEC-020, SPEC-025, SPEC-030, SPEC-032 |
| [SPEC-032](SPEC-032-post-close-sink-behaviour.md) | Post-Close Sink Behaviour | Completed | SPEC-026, SPEC-028, SPEC-030 |
| [SPEC-033](SPEC-033-orphan-path-sink-handoff.md) | Orphan-Path Sink Handoff | Completed | SPEC-026, SPEC-027, SPEC-028, SPEC-030, SPEC-031 |
| [SPEC-034](SPEC-034-public-api-freeze.md) | The Public API Freeze | Completed | SPEC-026, SPEC-030, SPEC-033 |
| [SPEC-035](SPEC-035-shutdown-and-fork-lifecycle.md) | Shutdown Lifecycle | Completed | SPEC-027, SPEC-028, SPEC-030, SPEC-033 |
| [SPEC-036](SPEC-036-flush-and-buffer-visibility.md) | Flush and Buffer Visibility | Completed | SPEC-013, SPEC-021, SPEC-026, SPEC-030, SPEC-034, SPEC-037 |
| [SPEC-037](SPEC-037-caller-safety-and-serialization.md) | Caller Safety and Serialization | Completed | SPEC-017, SPEC-020, SPEC-025, SPEC-034 |
| [SPEC-038](SPEC-038-sink-correctness.md) | Sink Correctness | Completed | SPEC-018, SPEC-026, SPEC-027, SPEC-032 |
| [SPEC-039](SPEC-039-fork-lifecycle.md) | Fork Lifecycle | Completed | SPEC-027, SPEC-028, SPEC-030, SPEC-033, SPEC-035 |
| [SPEC-040](SPEC-040-lifecycle-ownership.md) | Lifecycle Ownership — One Owner for the Worker and the Sink | Completed | SPEC-030, SPEC-031, SPEC-033, SPEC-035, SPEC-039 |
| [SPEC-041](SPEC-041-sink-integration-verification.md) | Sink Integration Verification | Completed | SPEC-026, SPEC-027, SPEC-038 |
| [SPEC-042](SPEC-042-forked-child-sink-ownership.md) | Forked-Child Sink Ownership | Completed | SPEC-027, SPEC-030, SPEC-032, SPEC-033, SPEC-034, SPEC-039 |
| [SPEC-043](SPEC-043-sentry-backend-selection.md) | Sentry Backend Selection | Completed | SPEC-026, SPEC-032, SPEC-041 |
| [SPEC-044](SPEC-044-lifecycle-races.md) | Lifecycle Races | Completed | SPEC-027, SPEC-030, SPEC-032, SPEC-033, SPEC-035, SPEC-039, SPEC-040, SPEC-042 |
| [SPEC-045](SPEC-045-every-owed-close-is-performed.md) | Every Owed Close Is Performed | Completed | SPEC-030, SPEC-032, SPEC-033, SPEC-042, SPEC-044 |
| [SPEC-046](SPEC-046-concurrent-owed-closes.md) | Concurrent Owed Closes | Completed | SPEC-027, SPEC-030, SPEC-031, SPEC-033, SPEC-045 |
| [SPEC-047](SPEC-047-bounded-delivery-kafka-and-nats.md) | Bounded Delivery for `KafkaSink` and `NATSSink` | Draft | SPEC-026, SPEC-027, SPEC-032, SPEC-038, SPEC-041 |

## Arcs (build order)

Group related specs and record the order to build them in. Keep this section: a spec split for size
(`process.md` §4) always records its order here, even if you group nothing else.

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
- **Post-close sink behaviour:** SPEC-032 — the arc's coda, and the only spec here whose subject
  was found *by* the arc rather than by the audit. SPEC-028 hit it while locking the sinks that hold
  transport state and could not fix it: neither offending sink takes a lock, so neither was in the
  roster it enforced. SPEC-030 established it was sink-level loss rather than lifecycle signalling
  (`retired` describes the worker) and handed it on again. Depends on SPEC-026 for the rule it
  generalizes, SPEC-028 for the lint and roster it widens, and SPEC-030 for the finding that its own
  signals cannot reach this. Also absorbs the lint-scope item SPEC-028 had provisionally left to
  SPEC-031, since the post-close roster derives from that gate. Build after 030; nothing depends
  on it. **It handed one item on:** a process that only ever uses the orphan path builds no worker,
  so `shutdown()` is a no-op, `atexit` is never registered and the sink is never closed — losing
  every event on a locally-buffering sink, the flush and the resource on a synchronous one — with
  `health()` reading all-clear, because each field describes a worker that does not exist. Found in SPEC-032's review and out of its scope (it is `decorator.py` lifecycle, not sink
  behaviour); now **SPEC-031 FR-006**, which is why that spec's dependency list gained SPEC-004 and
  SPEC-030 and why it is no longer purely residue.
- **Orphan-path sink handoff:** SPEC-033 — the arc's last item, and the half of SPEC-031 FR-006's
  root cause that spec explicitly scoped out and recorded in `architecture.md` §13 so the
  strike-through above it would not be read as covering it. A late `configure(sink=...)` in a
  process that never opened a span leaves the previous sink open with `incomplete_swaps` at zero,
  because `_swap_sink` returns early on a null worker. Depends on SPEC-031 for the arming it
  extends from a boolean to an identity, SPEC-030 for the bounded closer it reuses and the field it
  declines to widen, SPEC-028 for the concurrent-`close()` contract that lets it skip the fence,
  SPEC-027 for the stop signal this path never receives, and SPEC-026 for why a sink that raised is
  still a sink to close. Review of its first draft widened it twice, both from the same boolean:
  a sink configured after `shutdown()` is never closed, and an orphan-only process never hands its
  sink a stop signal — so SPEC-027's "a shutdown cuts a backoff short" is false on this path. Build
  after 031; nothing depends on it.
- **The 2026-08-07 pre-1.0 audit:** SPEC-034..041, recorded in
  [`docs/audits/2026-08-07-pre-1.0.md`](../audits/2026-08-07-pre-1.0.md). Four surfaces were
  audited in parallel while preparing the `v1.0.0` tag — public API, silent data loss,
  concurrency, and the sink family — and the tag was held. SPEC-035 shipped first and is done.

  **Build order: 034 → 037 → 038 → 036 → 039 → 041 → 040.** Everything but **041** and **040**
  has shipped. It is not the numbering, and it is
  **not** the order first recorded here (035 → 036 → 037 → 034 → 038). The reversal is deliberate
  and its reasoning is in SPEC-034's header:

  - **034 first, not last.** FR-008 converts `Health` from a `NamedTuple` to a frozen dataclass.
    Scheduled last, that forced 036 and 037 each to append a field *as a tuple* and prove indices
    0..8 unchanged, and then forced 034 to undo both — nine acceptance criteria and two test
    rewrites that existed only to serve the ordering. Converted first, a `Health` field is a plain
    append. FR-007's `FlushResult` moves with it for the same reason: 036 invents two new
    `flush()` reasons and needs somewhere to put them.
  - **037 next.** The smallest spec in the arc — a `try/except` around `api._log`'s in-span branch
    and a `math.isfinite` check in `sanitize` — and it clears six `xfail` cells. It was third only
    because of a counter it borrowed from 036; that borrowing is now split out (its AC-5c), so it
    is buildable on its own.
  - **038 before 036.** Independent of everything else here, ten one-file fixes, and FR-001 is a
    measured 5,980-event `emit` that abandons the whole exit backlog. Nothing about it needs to
    wait.
  - **036 after 034 and 037**, from which it takes the dataclass, the result type and 037's
    deferred counter. It is the largest and riskiest spec in the arc — FR-001's span sweep has
    twelve criteria and three landmines the spec found itself — so it goes after the cheap work
    rather than in front of it. Its FR-005 (a dead `MultiSink` child, invisible to `health()`) and
    its AC-1a (the README recipe) are Phase 0 and shippable before any of that.
  - **039 (fork) and 041 (sink integration)** are independent of the rest and of each other.
  - **040 last, and not before `1.0.0`.** It is a behaviour-preserving refactor of the lifecycle
    state, and the arc above is what happens when that state is edited.

  **The 1.0 cut line.** Most of this arc does **not** have to precede the tag, and that follows
  from taking 034 first: with `Health` a frozen dataclass and the `Sink` members probed by name,
  every remaining counter, hook and reason is *additive* and free in `1.x`. What genuinely freezes
  at 1.0 is **SPEC-034 entire**, plus **SPEC-038 FR-012** (`RotatingFileSink`'s default) and
  **FR-013** (the utility sinks' module). Everything else is urgent because it is data loss, not
  because of the tag — a distinction worth keeping, because treating the whole arc as
  release-blocking is how a tag stays held indefinitely.

  `tests/test_promises.py` remains the harness that keeps the audit honest: `strict=True` means a
  spec cannot land without removing the `xfail` markers it fixes.

- **Fork:** SPEC-039 — was SPEC-035 FR-005, moved to its own spec once that spec's other five FRs
  had shipped. `os.fork()` is unhandled anywhere in the tree: the child inherits a worker whose
  thread does not exist, and sink locks held by a thread that does not exist — **19 of 60 forked
  children hung permanently** inside `FileSink.emit`, on the application's own thread. It is the
  largest single piece of the audit arc and the only one needing a new module, which is why
  holding SPEC-035 open for it was costing more than the split. Its four prepared measurements
  moved with it and must not be re-derived.

- **Forked-child sink ownership:** SPEC-042 — the behaviour half of the finding SPEC-039 could
  only document and SPEC-040 may only record. A forked child closes transports it never opened:
  measured, a child's `configure()` sends a connection sink's goodbye and the parent's next write
  fails with `ECONNRESET`, and a child's `shutdown()` closes the inherited object. Build it
  **after SPEC-039** (done) and in either order against SPEC-040 — it does not wait on that
  refactor, because a correctness fix should not queue behind a tidy-up, and if SPEC-040 lands
  first the release path is one method on its owner rather than a module helper.

- **Lifecycle ownership:** SPEC-040 — the only spec here that fixes no bug. `decorator.py` owns
  the process's delivery lifecycle in seven loose globals with no state machine over them, and
  seven pieces of work have now come out of that one fact (SPEC-030, SPEC-031 FR-006, SPEC-033,
  audit C1 and C2, SPEC-035 FR-001/003, and SPEC-035 FR-002's roster). The roster is the right
  response to a defect that recurs at *sites* and it works — it caught a regression during its own
  review — but it makes the absence of a state machine survivable rather than removing it. Built
  after `1.0.0`: it is behaviour-preserving by construction, so it is the one thing here with no
  reason to be rushed.

- **Lifecycle races:** SPEC-044 — the six races SPEC-040's execution frame found over its own diff
  and its Out of Scope forbade it to fix. Each reproduces byte-identically on the pre-SPEC-040
  tree, so the refactor is not their cause and SPEC-044 does not depend on it for correctness — it
  depends on it for *shape*, since the guards are now four methods on one owner rather than seven
  loose globals. Build it after SPEC-040 (done). It closes five and documents the sixth, which
  §13 records as a deliberate design limit rather than a defect.

- **Every owed close is performed:** SPEC-045 — the residual SPEC-044 measured, recorded and
  deliberately did not fix. Build it after SPEC-044, which owns `_orphan_closed_sink` and is what
  makes the distinction this spec rests on legible: that slot answers "do not re-arm", not "this
  was closed", and is left alone. The subject turned out to be narrower and worse than the note
  that produced it — not a sink closed twice, but the **live** sink closed by nobody, because the
  orphan path's owed-close record was a single slot. It reproduces with every `configure()` call
  sequential on one thread, racing only an ordinary `info()`, so a lock around `configure()` is
  not the fix and neither is refusing a repeat close: both were built and measured to lose data.

- **Concurrent owed closes:** SPEC-046 — the one item SPEC-045 *introduced* rather than inherited,
  and the reason it is a spec rather than a note: making the owed-close record a set stopped the
  live sink going unclosed, but left `_close_orphan_sink` draining that set inline and in
  sequence, so `shutdown()` now costs one slow close times the number of sinks owed one.
  Build after SPEC-045. Its first design reused SPEC-030's detached closer and capped grace, and
  the spec review built that and measured it losing 3 of 4 closes; the shipped design runs the
  owed closes concurrently and joins every one, which is strictly better than today on both axes
  — cost falls from `sum` to `max`, loss stays zero. It deliberately does **not** narrow the §13
  constraint that a single `Sink.close` cannot be bounded at all.
