# Arcs

The narrative behind each arc: why its specs were cut and ordered as they were. **The build order and
every spec's status have one home, each spec's frontmatter** (`arc`, `arc_order`, `spec_status`), listed
in the generated `docs/specs/index.yaml`. What follows moved here unchanged from the old spec index when
the indexes became generated, so where it states an order or says a spec has shipped, that is the
reasoning as it stood, not a second authority. New narrative states neither (`docs/process/spec-lifecycle.md`).

## Contents

- [Core logging pipeline](#core-logging-pipeline) — arc `core-logging-pipeline`
- [Sink expansion](#sink-expansion) — arc `sink-expansion`
- [Release and distribution](#release-and-distribution) — arc `release-and-distribution`
- [Serverless usability](#serverless-usability) — arc `serverless-usability`
- [Cross-process correlation](#cross-process-correlation) — arc `cross-process-correlation`
- [Robustness](#robustness) — arc `robustness`
- [Supply-chain and code security](#supply-chain-and-code-security) — arc `supply-chain-and-code-security`
- [Audit remediation](#audit-remediation) — arc `audit-remediation`
- [Post-close sink behaviour](#post-close-sink-behaviour) — arc `post-close-sink-behaviour`
- [Orphan-path sink handoff](#orphan-path-sink-handoff) — arc `orphan-path-sink-handoff`
- [The 2026-08-07 pre-1.0 audit](#the-2026-08-07-pre-10-audit) — arc `audit-2026-08-07`
- [Forked-child sink ownership](#forked-child-sink-ownership) — arc `forked-child-sink-ownership`
- [Lifecycle races](#lifecycle-races) — arc `lifecycle-races`
- [Every owed close is performed](#every-owed-close-is-performed) — arc `every-owed-close-is-performed`
- [Concurrent owed closes](#concurrent-owed-closes) — arc `concurrent-owed-closes`
- [Sink delivery correctness](#sink-delivery-correctness) — arc `sink-delivery-correctness`
- [One lifecycle owner](#one-lifecycle-owner) — arc `one-lifecycle-owner`
- [Assembly, decoration and echo residue](#assembly-decoration-and-echo-residue) — arc `assembly-decoration-and-echo-residue`
- [Coercer and invariant 8](#coercer-and-invariant-8) — arc `coercer-and-invariant-8`
- [OpenTelemetry-shaped output](#opentelemetry-shaped-output) — arc `opentelemetry-shaped-output`
- [1.2.0 stability redesign](#120-stability-redesign) — arc `stability-redesign`

## Core logging pipeline

- **Core logging pipeline:** SPEC-001 → SPEC-002 → SPEC-003 → SPEC-004 → SPEC-005

## Sink expansion

- **Sink expansion (pluggable destinations):** SPEC-006 → SPEC-007 → SPEC-008 → SPEC-009 → SPEC-010 → SPEC-011.
  SPEC-006..008 are independent zero-dependency specs (any order); SPEC-009..011 reuse the SPEC-005
  optional-extra + lazy-import + bounded-retry pattern for third-party transports.

## Release and distribution

- **Release and distribution:** SPEC-012 — standalone; depends on no prior spec and touches only
  packaging config and CI, not the library runtime.

## Serverless usability

- **Serverless usability:** SPEC-013 — standalone. Two changes with one cause: the library assumes a
  process that starts, runs and exits, and a Lambda gives it one that is frozen, thawed and killed
  without warning. Nothing can *install* it there (`requires-python >=3.13` excludes the 3.12
  runtime), and nothing can *flush* it there (`shutdown()` is terminal — the worker never comes back,
  so a handler that flushes the obvious way logs only its first invocation per warm container).
  Adds a repeatable `flush()` and lowers the floor to 3.12. Driven by a real consumer.

## Cross-process correlation

- **Cross-process correlation:** SPEC-014 — cashes in the `architecture.md` §12 deferral ("adopting
  an inbound `trace_id` from a `traceparent` header, plus cross-process baggage"), which `ids.py`'s
  W3C-compatible formats were chosen to make cheap. Ships both halves — `current_traceparent()` to
  publish, `continue_trace()` to adopt — because a context nobody can read is a context nobody can
  propagate. Build **after** SPEC-013: 013 is what makes the library usable in the multi-process
  environment 014 exists to correlate.

## Robustness

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

## Supply-chain and code security

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

## Audit remediation

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

## Post-close sink behaviour

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

## Orphan-path sink handoff

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

## The 2026-08-07 pre-1.0 audit

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
    > **Superseded on the timing, not the ordering.** 040 did go last, but it landed at `4fea8f3`
    > and no tag contains it, so it ships **in** `1.0.0` rather than after it. What overtook the
    > note is that the tag was held for the audit arcs while the arc kept closing, and three
    > specs then declared a dependency on it — SPEC-044, SPEC-051 and SPEC-054, which is every
    > spec whose `depends_on` names SPEC-040 — so deferring it past the tag
    > would have meant deferring them too. The reasoning above still says why it was scheduled
    > last; it no longer says when it shipped.

  **The 1.0 cut line.** Most of this arc does **not** have to precede the tag, and that follows
  from taking 034 first: with `Health` a frozen dataclass and the `Sink` members probed by name,
  every remaining counter, hook and reason is *additive* and free in `1.x`. What genuinely freezes
  at 1.0 is **SPEC-034 entire**, plus **SPEC-038 FR-012** (`RotatingFileSink`'s default),
  **FR-013** (the utility sinks' module) and **SPEC-051**, which is the same test applied to
  what 034 left: a shape that is free to change today and breaking to change after the tag.
  Everything else is urgent because it is data loss, not
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

- **Lifecycle ownership:** SPEC-040 — the only spec here that fixes no bug. `decorator.py` owns
  the process's delivery lifecycle in seven loose globals with no state machine over them, and
  seven pieces of work have now come out of that one fact (SPEC-030, SPEC-031 FR-006, SPEC-033,
  audit C1 and C2, SPEC-035 FR-001/003, and SPEC-035 FR-002's roster). The roster is the right
  response to a defect that recurs at *sites* and it works — it caught a regression during its own
  review — but it makes the absence of a state machine survivable rather than removing it. Built
  after `1.0.0`: it is behaviour-preserving by construction, so it is the one thing here with no
  reason to be rushed.
  > **Superseded** (see the note under **040 last** in the build order above): it landed at
  > `4fea8f3`, before the tag, and is carried by `1.0.0`. It was not rushed — it was reached,
  > and three later specs declared a dependency on the shape it left.

## Forked-child sink ownership

- **Forked-child sink ownership:** SPEC-042 — the behaviour half of the finding SPEC-039 could
  only document and SPEC-040 may only record. A forked child closes transports it never opened:
  measured, a child's `configure()` sends a connection sink's goodbye and the parent's next write
  fails with `ECONNRESET`, and a child's `shutdown()` closes the inherited object. Build it
  **after SPEC-039** (done) and in either order against SPEC-040 — it does not wait on that
  refactor, because a correctness fix should not queue behind a tidy-up, and if SPEC-040 lands
  first the release path is one method on its owner rather than a module helper.

## Lifecycle races

- **Lifecycle races:** SPEC-044 — the six races SPEC-040's execution frame found over its own diff
  and its Out of Scope forbade it to fix. Each reproduces byte-identically on the pre-SPEC-040
  tree, so the refactor is not their cause and SPEC-044 does not depend on it for correctness — it
  depends on it for *shape*, since the guards are now four methods on one owner rather than seven
  loose globals. Build it after SPEC-040 (done). It closes five and documents the sixth, which
  §13 records as a deliberate design limit rather than a defect.

## Every owed close is performed

- **Every owed close is performed:** SPEC-045 — the residual SPEC-044 measured, recorded and
  deliberately did not fix. Build it after SPEC-044, which owns `_orphan_closed_sink` and is what
  makes the distinction this spec rests on legible: that slot answers "do not re-arm", not "this
  was closed", and is left alone. The subject turned out to be narrower and worse than the note
  that produced it — not a sink closed twice, but the **live** sink closed by nobody, because the
  orphan path's owed-close record was a single slot. It reproduces with every `configure()` call
  sequential on one thread, racing only an ordinary `info()`, so a lock around `configure()` is
  not the fix and neither is refusing a repeat close: both were built and measured to lose data.

## Concurrent owed closes

- **Concurrent owed closes:** SPEC-046 — the one item SPEC-045 *introduced* rather than inherited,
  and the reason it is a spec rather than a note: making the owed-close record a set stopped the
  live sink going unclosed, but left `_close_orphan_sink` draining that set inline and in
  sequence, so `shutdown()` now costs one slow close times the number of sinks owed one.
  Build after SPEC-045. Its first design reused SPEC-030's detached closer and capped grace, and
  the spec review built that and measured it losing 3 of 4 closes; the shipped design runs the
  owed closes concurrently and joins every one, which is strictly better than today on both axes
  — cost falls from `sum` to `max`, loss stays zero. It deliberately does **not** narrow the §13
  constraint that a single `Sink.close` cannot be bounded at all.

## Sink delivery correctness

- **Sink delivery correctness:** SPEC-048 then SPEC-049 — the nine sink findings of the 2026-09-02
  pre-1.0 audit. The seam is **when the library refuses**, not what the refusal prevents: SPEC-048
  is everything that reaches the wire wrongly or twice at run time, SPEC-049 is everything the
  constructor should have refused, plus the three driver defaults SPEC-041 and SPEC-047 did not
  reach. That is a correction — the pair was first cut on *consequence*, which put two
  construction-time refusals in SPEC-048 while its own Out of Scope forbade them and SPEC-049 did
  not name them. Build SPEC-048 first: it is the pair's blocking half (a followed redirect that
  loses every batch and forwards the bearer token; four AWS sinks whose partial failure makes the
  worker's retry duplicate what landed), and SPEC-049 rebases onto it rather than running beside
  it, since both touch `http.py`, `clickhouse.py` and `file.py`. SPEC-049 also closes the two
  construction-time open items `architecture.md` §12 records from SPEC-047 and defers to "a major
  version that can refuse it" — 1.0 is that version, and a spec claiming every construction-time
  refusal cannot leave the identical case open two sections away.

## One lifecycle owner

- **One lifecycle owner:** SPEC-054 — the question SPEC-040 was named for and did not answer.
  That spec moved the orphan path's seven globals onto one object and left the worker's twin
  state on `Worker`, so every lifecycle decision still exists twice: measured at `98c7e78`,
  thirteen twin mechanisms, 13 of the 36 FRs across the seven lifecycle-fixing specs edited both
  sides, and a probe found `health().sink` reported on one path and `None` on the other against
  the same sink. Build it **after SPEC-050** (done) and after nothing else: it depends on the
  owed-close record being a set (SPEC-045), the closes running concurrently (SPEC-046) and the
  in-flight gate existing on both paths (SPEC-050), all of which it merges rather than
  re-derives. It is four phases, each leaving `main` and `tests/test_invariants_model.py` green,
  and the two emit paths — synchronous outside a span, queued inside one — are out of its scope
  by decision (SPEC-028, SPEC-030), so invariant 6 keeps them as twins. It supersedes the
  four-question guard set (SPEC-035 FR-002, SPEC-040 FR-002) with three, and says so.

## Assembly, decoration and echo residue

- **Assembly, decoration and echo residue:** SPEC-055 — the five runtime findings of the second
  round of the 2026-09-02 pre-1.0 audit, at the two edges the library promises most about: what
  leaves `build_event` and what `@trace` accepts. Standalone; nothing depends on it, and its four
  edge fixes are independent of one another. One FR carries a product decision that is not the
  spec's to make — whether a generator function is *refused* at decoration or its iteration is
  *wrapped* — and the spec recommends refusing, because a refusal can later be lifted into a wrap
  while a wrap shipped first freezes its semantics at 1.0. ~~If wrapping is chosen it is a spec of
  its own, SPEC-056, built after SPEC-055 and replacing its FR-003.~~ — **superseded 2026-10-05:**
  refusing was chosen and shipped (SPEC-055 FR-003), so no wrapping spec exists and the number
  SPEC-056 names the OTLP log export spec below.

## Coercer and invariant 8

- **Coercer and invariant 8 (SPEC-057, SPEC-058, SPEC-063):** all edit `sanitize.py`'s coercer and
  `docs/invariants.md` §8. SPEC-058 landed first and SPEC-057 rebased onto it. They did interact:
  SPEC-057's budgets would have cut the output SPEC-058 promises not to clip, so a transform's
  coercion runs outside them.

- **Assembly work budget:** SPEC-057 — a node budget, a byte budget and reads bounded by the ceiling,
  all in `sanitize.py`, closing a shared-sublist hang on the caller's thread. It depends on nothing
  unbuilt and built first of the two specs it touches: SPEC-056, the OTLP log export spec,
  specifies an equal budget for its own mapping — one number, one placeholder text, one stop rule.
  Keeping the two equal was SPEC-056's to settle, and it settled it with a test that
  pins `_otel.MAX_NODES` to `sanitize`'s private `_MAX_NODES` and imports nothing from `sanitize`;
  SPEC-057 defines its own constant and asks for no edit to that spec or to `_otel`.

- **Assembly keys and scalars:** SPEC-063, third in the arc — the two items SPEC-057 recorded as
  open in `architecture.md` §12 and left alone because no budget depended on them. A container
  mapping key is coerced under the pass budgets and written as the JSON of its coerced form, where
  SPEC-057 charged it only after `repr` had run; and no route trusts a type test the caller's object
  controls. It edits the same coercer and the same §8 as the other two, after both had landed, and
  supersedes SPEC-057's statement that keys cost no node. Three siblings of the same
  shape ride with it in `api.py`, `model.py` and `_diag.py`.

## OpenTelemetry-shaped output

- **OpenTelemetry-shaped output:** SPEC-056, then three planned items that have no spec number yet.
  The arc follows a direction Andrew decided on 2026-10-05 (`docs/decisions/public-api/`): what
  goes outward standardizes on the OpenTelemetry data model and semantic-convention names, the
  transports stay plural, and OTLP is produced natively with no OpenTelemetry SDK dependency.
  Build order: **SPEC-056** — the private mapping and an OTLP/HTTP JSON logs sink, first because
  the ClickHouse layout and the traces item both reuse its mapping, and after SPEC-057, whose
  `sanitize._MAX_NODES` it pins its own record budget equal to (see the *Assembly work budget*
  entry above; SPEC-056 imports nothing from `sanitize` and settles the equality with a test) —
  then **a `ClickHouseSink` `otel_logs` layout**, which reuses
  that mapping and is the first possible second consumer of it (the register keeps the mapping
  private until a second consumer has proven its shape) — then **an Astra profile on the `_bulk`
  path**, which is independent of the OTel work and may move earlier, since Astra ingests through an
  OpenSearch-compatible `_bulk` API and has no OTLP ingestion — then **OTLP traces from `span.end`**
  (plus an `otel_traces` layout), last because it carries the most design risk: a `span.end`
  dropped under queue backpressure orphans child spans in a trace view. In-library metrics are not
  an item; they are derived downstream.

## 1.2.0 stability redesign

- **1.2.0 stability redesign (SPEC-069 to SPEC-079):** the plan Andrew settled on 2026-11-04, after
  lifecycle had taken 14 specs and sink delivery 13. Both kept breaking for structural reasons:
  lifecycle state had no single owner, and the sink contract left each of 34 sinks to decide alone
  what a delivery meant, which is why one question, "did the batch land, and is it safe to send
  again?", was answered three times in three places. All 34 sinks stay, and the release is
  additive: `emit`'s contract, `losses()`, `SinkLosses` and every documented counter stay true for
  1.x callers.

  The arc is cut along the seams the system already has. Two specs carry rules that need no new
  mechanism: what a sink does with an event it cannot encode, and which failures it retries. A kit
  spec centralises the optional imports, the injected-client refusal and the chunker rule. It also
  pins the AST-derived rosters, so that the later refactors cannot shrink them silently. The plan's
  single "verdict engine" step is split where something inert goes live. SPEC-072 defines
  `deliver`, `Verdict` and the engine against test sinks, and no shipped sink offers `deliver`
  until SPEC-073 puts the HTTP family on it. The closed-guard helper is a spec of its own because
  only the engine settles what a transport lock must span. Lifecycle is split the same way: one
  spec moves today's state into `Runtime` with no behaviour change, the next makes that state the
  authority through the owed-close registry and checked transitions, and fork's one walk is
  separate because it walks from the runtime's roots.

  Three decisions were made while the arc was written, each by Andrew with the question tool on
  2026-11-04. A batch with nothing encodable settles, counted, rather than raising. A destination's
  own refusal (PostgreSQL JSONB's U+0000, MongoDB's integers beyond 64 bits) is repaired and
  delivered rather than dropped. The engine keeps the 1.x total retry budget,
  `(sink max_retries + 1) × (worker max_retries + 1) − 1` on the worker path, and the 1.x wait
  schedule, rather than the plan's sink-only budget. A fourth, also his, fixes when `sink_factory=`
  runs: lazily on first use in every process.

  Three peer specs fix code this arc then restructures, and none is in any `depends_on`, because
  spec-lint refuses a dependency on a spec not yet in the directory. They are SPEC-061 (NATS
  liveness and bounded close), SPEC-066 (numeric doors) and SPEC-068 (shutdown expiry). Each arc
  spec that builds on one says so in its Overview, and the dependency joins `depends_on` once the
  peer's file lands.

