# Spec: Lifecycle Races

**ID:** SPEC-044  
**Status:** Completed  
**Last Updated:** 2026-08-31  
**Depends On:** SPEC-027, SPEC-030, SPEC-032, SPEC-033, SPEC-035, SPEC-039, SPEC-040, SPEC-042

## Overview

The library's delivery lifecycle — build a worker, swap a sink, retire the process — is driven by
one small piece of shared state that concurrent application threads reach at the same time. Six
races in that state are already reproduced and recorded in `architecture.md` §13, and every one of
them ends the same way: the library believes it did something it did not do, and `health()` agrees
with the belief rather than with the process. A `shutdown()` returns having stopped nothing, a
configured sink is never closed, a backoff a shutdown was supposed to cut short runs to term, a
released sink is released again, and a forked child re-opens a transport its parent abandoned. This
spec closes five of them and corrects the documentation of the sixth, which is behaviour the design
chose on evidence and describes wrongly.

The races were found by the execution frame over SPEC-040's diff and each **reproduces
byte-identically on the pre-SPEC-040 tree**, so none is that refactor's doing; SPEC-040's Out of
Scope forbade fixing them, which is why they were recorded rather than closed. They are worth a spec
of their own because the shape they share is the one this codebase keeps paying for: an unlocked
read of a lifecycle question, acted on after the answer stopped being true.

## Scope

### In Scope

- The five behavioural races recorded at `architecture.md` §13 under *Six lifecycle races are open*:
  `shutdown()` racing a first `@trace`, `configure(sink=…)` racing a first `@trace`, a racing log
  call cancelling a shutdown's stop signal, the worker-path double `close()` on a swapped-out sink,
  and a forked child running fork hooks on a superseded sink.
- The sixth, which is a documentation defect: `shutdown(timeout=…)` justifies its own existence
  with the case it does not cover.
- A roster row for every worker guard this **adds, moves, or re-locks**
  (`tests/test_worker_predicate_roster.py`, `architecture.md` §9.2), each with a category and a
  reason. The roster's per-module floor must rise with the sites, not merely hold.
- Committed reproductions. Each of the five is preserved as a test rather than as prose, because a
  criterion phrased against a harness the repo does not carry is a criterion measured by whatever
  the implementer rebuilds from it.

### Out of Scope

- **Bounding the live sink's `close()`.** FR-006 documents the limit rather than removing it. The
  two available mechanisms were already built and reverted — a daemon closer for this close
  (`Worker._close_if_owed`'s docstring) and a joinable one (SPEC-030) — and bounding it properly
  requires `Sink.close` to be interruptible, which is a change to the published sink contract and
  its own spec.
- **Changing what a worker built *after* `shutdown()` returned does.** A sequential
  `orphan → shutdown() → @trace` builds a fresh live worker that genuinely delivers against a
  permissive sink, detected through `failed_batches` rather than SPEC-030's pair; that is settled in
  `_worker_health`'s docstring and pinned by `tests/test_api.py`. FR-001 fences only the worker
  built **while `shutdown()` is still running**, and leaves the sequential case exactly as it is.
- **The append race on `span.events`** and the other `contextvars` constraints in §13. They are
  declined on cost (a per-span lock on the hottest path), not pending.
- **Retiring `tests/test_worker_predicate_roster.py`**, which §13 leaves open on purpose until a
  year of maintenance says whether it earns its weight. This spec adds to it and does not judge it.
- **Splitting `_lifecycle.py` or `worker.py`.** Both are recorded as unsplit in §13 and neither is
  a defect.
- Any change to `Sink`, to `Health`'s fields, or to the four questions of §9.2. This spec changes
  *which* question a site asks and *when* it may ask it, never the set.

---

## Functional Requirements

### FR-001: A `shutdown()` finishes the worker built while it was running

#### Description:

`_shutdown_worker` reads the existence question unlocked, so a worker built after that read sends it
down the no-worker branch: the drain thread is never stopped and the worker's sink is never closed,
while `health()` reports `retired=True` and later logs are delivered by a live worker with
`submitted_after_shutdown` at zero. `atexit` recovers it in a process that exits; a frozen
serverless container never does, and that is the deployment `flush()`/`shutdown()` exist for.

Closing the read window alone only narrows it — a worker built one instruction later is the same
defect. The fence is therefore a **shutdown-in-progress** state, not a permanent one: the flag goes
up with the retirement latch under `_state._lock`, `_get_worker` registers under the same lock any
worker it builds while the flag is up, and the shutdown re-reads that registration in its last
critical section and drains what it finds. Bounded at one extra check, because the flag comes down
in the same critical section that reads it.

Deliberately *not* a permanent retirement fence. A worker built after `shutdown()` has **returned**
is the sequential case `_worker_health`'s docstring settles, and it still delivers; making every
later worker born retired would supersede that decision and lose events that land today.

The sink is the one thing both branches can claim. Where the orphan branch already closed it,
`_get_worker` constructs the late worker over a sink recorded as released, so the exit close does
not perform a second `close()` on it — the library performs one close and does not rely on a
sink's release being idempotent, which `sinks/base.py` asks for but cannot enforce (SPEC-032). Where
the worker was registered first, `_close_orphan_sink`'s existing ownership guard declines and the
worker performs the only close.

#### Acceptance Criteria:

- [ ] With a preemption point held inside `_get_worker`'s critical section and `shutdown()` racing
      it, then joining both threads: no drain thread is alive, and `health()` reports `retired`.
- [ ] The same test fails on the pre-fix tree, where the drain thread is still alive.
- [ ] Across that race the sink records exactly one `close()` — neither zero nor two — in both
      orderings: the worker registered before the orphan close, and after it.
- [ ] `_shutdown_worker` raises the flag, latches retirement and reads the worker in one critical
      section, and calls neither `Worker.shutdown` nor `_close_orphan_sink` while holding the lock.
- [ ] A sequential `configure(sink=…)` → `info()` → `shutdown()` → `@trace` still builds a live
      worker that delivers, and `submitted_after_shutdown` stays at zero. FR-001 must not reach it.
- [ ] The racing `shutdown()` returns within its own `timeout`, so the added drain cannot extend a
      bounded call without bound.

### FR-002: A lifecycle transition never discards an unclosed sink's close record

#### Description:

`_get_worker` clears `_state._orphan_sink` unconditionally once it has built a worker, on the
premise that the worker adopted the sink the record named. A `configure(sink=B)` that writes the
config and then blocks on the lifecycle lock breaks that premise: `_ensure_sink()` returns B, the
worker captures B, and the record for A — which has events and has never been closed — is
discarded. A is never closed and `incomplete_swaps` stays at zero, because every field of `Health`
describes a worker and the worker is fine. Natural rate 6/400.

The rule is that a transition may clear the record only after deciding who performs the close it
was holding. Where the new owner did not adopt the sink, the transition owns the close itself — the
shape `_swap_sink`'s no-worker branch already uses, a detached release with the sink latched closed,
reported live through `health().closing_sinks`. That latch is FR-004's rule, and this new close site
is subject to it.

Enforced by enumeration rather than by fixing the cited line: every site that clears or re-points
the record states which of the two outcomes it takes.

#### Acceptance Criteria:

- [ ] With a preemption point held inside `_get_worker`'s critical section and a `configure(sink=B)`
      blocked on the lifecycle lock, A is closed exactly once and the worker's sink is B.
- [ ] The same test fails on the pre-fix tree, where A is closed zero times.
- [ ] A worker that *does* adopt the recorded sink still leaves exactly one close, performed by the
      worker — the mixed-process guarantee SPEC-031 FR-006 shipped is unchanged.
- [ ] No close is performed **inline** while `_state._lock` is held; a detached release started
      under it is the existing shape and is allowed.
- [ ] A test enumerates every site that clears or re-points `_state._orphan_sink` and asserts each
      one's disposition, so a new site cannot be added silently.

### FR-003: A close in flight keeps the stop signal it was given

#### Description:

`_offer_orphan_signal` replaces a stop event that is already set with a fresh unset one, so that a
sink is not left holding a permanently-set event that collapses every later backoff to zero. That
replacement is right, and SPEC-033 FR-004 pins it — including **after** `shutdown()`, where a newly
configured sink must still back off. What it must not do is cancel a signal a close is *currently
waiting on*: an `info()` landing while the sink is closing hands it an unset event, and the close
then serves its wait in full. Measured 8.01 s against an 8 s backoff, versus 0.00 s with no racing
log — SPEC-027's guarantee failing on a race, on both the orphan path and the worker path.

The discriminator is therefore not retirement but the **moment**: while a release of this sink is in
flight, the signal it holds is not replaced. That is the *ownership ∧ moment* shape §9.2 already
carries for the worker (`worker_owns_now`), applied to the close. `_lifecycle.release` is the one
path by which the library ever closes a sink (SPEC-042 FR-002), so one registration there covers the
orphan close, the worker's `_close_if_owed`, the swap's detached closer and every wrapper.

#### Acceptance Criteria:

- [ ] A sink whose `close()` performs an interruptible 8 s backoff, with an `info()` to that same
      sink landing inside the close, finishes its wait in under a second; on the pre-fix tree it
      waits the full backoff.
- [ ] The same holds on the worker path, where the close is `Worker._close_if_owed`.
- [ ] `tests/test_orphan_sink_handoff.py`'s two post-shutdown signal tests still pass unchanged: a
      sink adopted after `shutdown()` gets an **unset** signal, and a sink emitted to after
      `shutdown()` still backs off for the full `_retry.wait`. FR-003 must not reach them.
- [ ] The in-flight registration is removed when the release returns, on the raising path as well
      as the returning one.

### FR-004: The closed-sink latch moves with every close the library performs

#### Description:

`_swap_sink`'s worker branch clears `_state._orphan_sink` without setting
`_state._orphan_closed_sink`, so an orphan emit that resolved the old sink before the swap and
resumes after it re-arms a sink `Worker.swap_sink` has already closed. The exit close then performs
a second `close()` on it, which the library must not rely on a sink surviving. It needs an injected
preemption
point — 0/120 without one — which is why it is a latch defect rather than a rate to argue about.

The orphan branch already latches, and `_note_orphan_emit` and `_adopt_declined_swap` both refuse to
re-arm a latched sink. The worker branch is the one place the library closes a sink without
recording that it did.

The latch is a **single slot**, so it protects against re-arming the most recent close only: after
closing A then B, A is forgettable and re-armable again. That bound is pre-existing and is stated
rather than removed — a set would pin every sink ever closed against garbage collection, which is
the cost SPEC-042 already refused for the ownership record's siblings.

#### Acceptance Criteria:

- [ ] With an orphan emit preempted after it resolved sink A and resumed after a
      `configure(sink=B)`, A records exactly one `close()`; on the pre-fix tree the same test
      observes two.
- [ ] A test enumerates every place the library closes a sink and asserts that each either records
      the close where a later re-arm would consult it, or declares why it records nothing —
      `Worker._close_if_owed` and `Worker._close_swapped_out` are safe for a different reason and
      must say so, the way SPEC-032's post-close roster does.
- [ ] The pre-existing single-slot bound is stated in `architecture.md` §13, so a reader does not
      take FR-004's title literally.

### FR-005: A forked child does not run fork hooks on a superseded sink

#### Description:

`_state._orphan_closed_sink` pins the sink a swap or an exit close already released, and `_fork`'s
repair walk reaches it, so a forked child calls `reacquire_after_fork()` on a closed sink — a
`FileSink` there would have its file re-opened on every fork for the life of the process. This is
the hazard `_fork._SKIP_ATTRIBUTE`'s own docstring describes and that `_lifecycle._FORK_SKIP`
declares an opt-out for; the opt-out names `_owned` only, and `_owned` is a module global while this
slot is an attribute of `_state`, so the module-level declaration never reaches it —
`_fork._skipped_names(_lifecycle._state)` returns an empty set.

The declaration therefore has to sit where the walk will read it — on the holder of the slot. The
module-level `_FORK_SKIP = ("_owned",)` stays exactly as it is; this adds a second declaration
rather than moving the first. Marking is unaffected: `_inheritance_roots` reads the slot directly,
so the walk still reaches an inherited superseded sink and it is still refused — on the stamp
`configure()` left, since `_mark_inherited` `setdefault`s rather than overwriting it, or on
`_FOREIGN` for a sink held inside it that the bounded stamp walk never reached.

#### Acceptance Criteria:

- [ ] After `configure(A)` → `info()` → `configure(B)` → `info()` → `fork()`, the child runs the
      fork hook on B only; on the pre-fix tree it runs it on both.
- [ ] A child still refuses to close an inherited superseded sink — SPEC-042's marking walk is not
      narrowed by this.
- [ ] A test asserts `_fork._skipped_names` returns the slot's name for the holder of the slot, so
      moving the slot to a different holder cannot silently un-skip it.

### FR-006: `shutdown(timeout=…)` states what it bounds and what it does not

#### Description:

The timeout bounds the drain thread's join and the grace granted to a sink still closing after a
late `configure(sink=…)`. It does **not** bound the live sink's own `close()`, which stays inline by
the decision recorded in `architecture.md` §9 — and the parameter's docstring justifies itself with
exactly the case it does not cover: "unsafe anywhere with an execution deadline — `atexit` is one
such place, where a sink blocked in a network call would hold the process open". A sink blocked in
`close()` holds the process open whatever the timeout says. Measured 6.01 s against
`shutdown(timeout=2.0)` with a 6-second close, on **both** the worker and the orphan paths, where
§13 records only the live sink's inline close and a 30-second figure from a 30-second close.

This is a documentation fix and not a behavioural one, deliberately: both mechanisms that would
bound it were built and reverted, and the third — an interruptible `Sink.close` — is a change to the
published sink contract that belongs in its own spec. Recording the limit honestly is what lets the
next reader decide whether to write that spec.

#### Acceptance Criteria:

- [ ] `shutdown()`'s docstring states what `timeout` bounds and names the live sink's `close()` as
      outside it, without the claim that the timeout makes an execution deadline safe.
- [ ] `architecture.md` §13's entry says the gap reaches the orphan path as well as the worker's.
- [ ] A test pins the behaviour on both paths: a `close()` that sleeps `N` outlasts
      `shutdown(timeout=t)` with `t` well under `N`, asserting elapsed ≥ `N`. `N` is under a second,
      so the pin costs the suite no meaningful time and still fails if the close is ever bounded.
- [ ] §13's list of six records which five were fixed here and that this one was not.

---

## Data Model

No new public types. The change is to the private lifecycle state's invariants:

```python
# src/log_foundry/_lifecycle.py

_FORK_SKIP = ("_owned",)             # FR-005 — unchanged; the module-level declaration stays

class _Lifecycle:
    _FORK_SKIP: tuple[str, ...]      # FR-005 — new, read by _fork._skipped_names on the instance

    _worker: Worker | None           # FR-001 — read with the flag below in one critical section
    _shutdown_running: bool          # FR-001 — new; up only while _shutdown_worker is executing
    _late_worker: Worker | None      # FR-001 — new; a worker built while that flag was up
    _orphan_sink: Sink | None        # FR-002 — cleared only with a close decision recorded
    _orphan_closed_sink: Sink | None # FR-004 — set by every close the library performs
    _orphan_stop: threading.Event    # FR-003 — not replaced under an in-flight close

_closing_now: set[int]               # FR-003 — new; ids of sinks a release is running against
```

---

## API / Interface Contract

No public signature changes. `Worker` gains one construction-time state, so a worker built over a
sink the lifecycle has already released does not close it a second time:

```python
Worker(sink, *, sink_released: bool = False, ...)   # True: the close is already discharged
```

`log_foundry.shutdown`, `configure`, `flush`, `health` and every sink signature are untouched.

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── _lifecycle.py          # FR-001..FR-005
├── worker.py              # FR-001 (sink_released construction)
└── __init__.py            # FR-006 (shutdown's docstring)
docs/
├── architecture.md        # §9.2 categories, §13 the six-race entry
└── specs/SPEC-044-lifecycle-races.md
tests/
├── test_lifecycle_races.py            # new — the five committed reproductions
└── test_worker_predicate_roster.py    # a row per added, moved or re-locked guard
```

## Implementation Phases

### Phase 1: The inert fixes

- FR-005: declare the fork opt-out on the holder of the slot, with the child-refusal test proving
  marking is unnarrowed.
- FR-006: correct `shutdown()`'s docstring and `architecture.md`, with the test that pins the
  measured behaviour on both paths.

### Phase 2: The close records

- FR-004: latch the closed sink on the worker branch, and enumerate every close the library
  performs. First, because FR-002's new close site depends on this latch being in place.
- FR-002: decide the close before clearing the record, and enumerate every site that clears it.

### Phase 3: The shutdown fence and the in-flight close

- FR-001: the shutdown-in-progress flag, the late-worker registration, and the released-sink
  construction.
- FR-003: register an in-flight release and skip the refresh under one.
- Re-decide the roster row for every guard this added, moved or re-locked, and raise the per-module
  floor with them.
