# Spec: Shutdown Lifecycle

~~Shutdown **and Fork** Lifecycle~~ — the fork half is SPEC-039. The **filename keeps `-and-fork-`**
deliberately: it is what every link, commit message and delivery doc in the arc already points at,
and a rename to tidy a title would break them to save nothing.

**ID:** SPEC-035  
**Status:** Completed  
**Last Updated:** 2026-08-09  
**Depends On:** SPEC-027, SPEC-028, SPEC-030, SPEC-033

## Overview

Four defects in the process-lifecycle plumbing, found by the concurrency surface of the
2026-08-07 audit (`docs/audits/2026-08-07-pre-1.0.md`, C1–C4). Two are **regressions SPEC-033
introduced and are on `main` now**, which is why this spec is first in the arc: `main` currently
has a worse shutdown story than it did before that spec landed.

The third is older and larger: `atexit` can return through `shutdown()`'s idempotent path and
abandon a drain that is still running — measured, **nothing delivered and the process gone in
0.39 s**.

The fourth was `os.fork()`, and it is now [SPEC-039](SPEC-039-fork-lifecycle.md). It was the
largest piece of work here and the only one needing a new mechanism; it left after everything
else had shipped, so that this spec could complete rather than stay open across a build as big
again. FR-005 below is struck in place with the reasoning.

Lock ordering, counter synchronisation and `contextvars` were audited alongside these and are
**clean**; that is recorded in the audit so this spec does not have to re-establish it.

## Scope

### In Scope

- The two SPEC-033 regressions: a stolen stop signal, and a swap that leaves its new sink owned
  by nobody.
- `shutdown()`'s idempotent path waiting for the drain it did not start.
- ~~`os.fork()`: the inherited worker, and the inherited locks.~~ — moved to SPEC-039.

### Out of Scope

- **Making the orphan path non-blocking**, or any other item in `architecture.md` §13's recorded
  constraints. Those are accepted trades, not defects.
- **Restarting a worker after a terminal failure.** SPEC-019 settled that a thread which
  resurrects itself fights a process trying to exit. ~~FR-004 rebuilds a worker after a **fork**~~
  — that distinction moved to SPEC-039 with the fork FR, and it was mis-numbered here in any case
  (FR-005, not FR-004). Nothing in this spec rebuilds a worker.
- **`shutdown()`'s unbounded close of the live sink.** Recorded in §13, and narrowing it needs
  the sink contract to change (SPEC-027 FR-004).
- **The stderr write under `_worker_lock`** (audit C5). An error path only, and the fix —
  returning a flag and writing after the release — spreads a diagnostic decision across two
  functions to save a write that happens only when a sink's `stop_signal` setter objects. It is
  **recorded as a constraint** rather than fixed, and FR-006 is the AC that makes that recording
  happen: a first draft of this bullet said "recorded in §13 by FR-005", and FR-005 is the fork
  FR whose criteria are all fork — so the item would have been downgraded to a constraint that
  nothing actually wrote down.

---

## Functional Requirements

### FR-001: The stop signal is offered on ownership, not liveness

#### Description:

`_offer_orphan_signal` skips the offer when `_live_worker()` returns a worker owning the sink.
`_live_worker()` returns `None` the instant `Worker._shutdown_done` latches — which is *entry* to
`shutdown`, not its completion — so for the whole of the drain the skip stops applying. An orphan
log in that window then hits SPEC-033 FR-004's fresh-event rule and replaces the sink's
`stop_signal` with a **new, unset** event: precisely the event the drain thread is about to wait
on in `_retry.wait`.

Measured on `734a9b2`:

```
before shutdown: sink.stop_signal is worker._stop -> True
after one orphan log: sink.stop_signal is worker._stop -> False   (replacement unset)

with a 20 s backoff and shutdown(timeout=3):
  control        backoff cut at 0.00 s   shutdown 0.30 s   sink closed
  one orphan log backoff still running   shutdown 3.01 s   stopped_reason='ShutdownTimeout', sink left open
```

That is SPEC-027's global pause, reintroduced by the spec that was supposed to extend SPEC-027's
guarantee to this path.

**This is the fourth time this distinction has been got wrong**, and the process change in
`docs/process.md` ("re-audit the rule, not the line") exists because of it. Three reviewers each
named a different call site; each was fixed; this one shipped. So the fix is not a fourth
one-line correction — it is FR-002's enumeration.

#### Acceptance Criteria:

- [x] AC-1: `_offer_orphan_signal` skips on ownership **conjoined with the moment** —
      `_worker.sink is sink and _worker.draining`, the second a new property true while the drain
      loop is running and has not been abandoned. The ownership term stays and must: without it an
      orphan log to sink Y is skipped merely because a live worker is draining into sink X.
      `_live_worker()` is not consulted. **A draft of this AC prescribed bare ownership
      (`_worker is not None and _worker.sink is sink`, the guard `_close_orphan_sink` uses) with
      no second term, and that is measurably wrong**: it skips for a worker whose shutdown has *finished*, leaving a
      sink still being written to holding a `_stop` that is set forever and can never clear, so
      every later backoff collapses to zero. That is SPEC-033 FR-004's tight retry loop, and
      `test_a_sink_a_retired_worker_holds_still_gets_a_usable_signal` fails against it — this
      spec's own fix would have re-broken the guarantee it cites in AC-4. Struck rather than
      silently replaced (SPEC-021), because "ownership, not liveness" is the rule the whole arc
      has been repeating and this is the one site where it is not the answer. Both wrong
      predicates are pinned by mutation: liveness fails the three tests below, bare ownership
      fails SPEC-033's.
- [x] AC-1a: The two directions are covered by different tests, and neither alone is sufficient —
      one holds the shutdown's drain window, the other holds the post-shutdown window. An
      abandoned drain (`ShutdownTimeout`) counts as **not** draining: the thread is wedged, the
      shutdown has already given up on it and left the sink open by SPEC-027 FR-004, so nothing
      will cut its backoff, where the retry loop goes on costing the running application.
- [x] AC-2: An orphan log during `shutdown()`'s drain leaves `sink.stop_signal is worker._stop`.
- [x] AC-3: End to end: with a sink whose backoff is far longer than the shutdown budget, one
      concurrent orphan log does not change the outcome — the backoff is still cut short, and
      `stopped_reason` stays `None`. The test keeps the backoff-to-budget **gap** wide rather
      than the budget tight.
- [x] AC-4: The sink adopted *after* a retired worker still receives a signal (SPEC-033 FR-004
      AC-4's second case), so this fix does not re-break what that one fixed.

### FR-002: One enumeration decides every liveness-or-ownership call site

#### Description:

FR-001 is one instance. The rule it belongs to is stated in SPEC-033's delivery doc — *liveness
answers who **performs**, ownership answers who **owns** a close* — and enforced nowhere, which
is why a fourth site could exist unnoticed.

Add a test that enumerates every call site of both predicates in `decorator.py` and asserts each
one's category, deriving the list from the module rather than hand-writing it, exactly as the
sink rosters do. A new call site must either match a declared category or fail the test.

#### Acceptance Criteria:

- [x] AC-1: A test walks `decorator.py`'s AST and finds every form: `_live_worker()`, every
      `_worker.sink is …` comparison, every bare `_worker is not None`, **and the two FR-001 and
      FR-003 added — `worker.draining` and `_swap_sink`'s `if not adopted:`, which carries the
      answer in a return value rather than a predicate.** A walk looking only for the first two
      would miss the bare form, which is the phrasing SPEC-033's docstrings warn about; one
      looking only for the first three would ship missing both forms this spec itself
      introduced, which is SPEC-032's roster lesson repeated inside a single spec. Line numbers
      are not cited — an earlier draft named `decorator.py:475` and Phase 1 moved it.
- [x] AC-1b: The categories are **four**, not two: exists-at-all (existence — `_get_worker`,
      `_shutdown_worker`, `_flush_worker`, `_worker_health`), performs-a-swap (liveness),
      owns-a-close (ownership), and **offers-a-signal**, which FR-001 as built makes
      *ownership conjoined with a moment* (`_worker.sink is sink and worker.draining`) rather
      than ownership alone. ~~three~~ — struck: the draft was written before FR-001 discovered
      that neither existing category classifies that site.
- [x] AC-2: The category table is **in the test**, one line per site with the reason, so adding a
      site forces a decision rather than defaulting silently.
- [x] AC-3: The test fails when FR-001's fix is reverted, and fails when a new call site is added
      without a category. Both are demonstrated by mutation.
- [x] AC-4: `docs/architecture.md` §9 states the rule, so it is discoverable from the design doc
      and not only from a delivery doc.

### FR-003: A swap racing a shutdown leaves its sink owned

#### Description:

`_swap_sink`'s worker branch clears `_orphan_sink = None` unconditionally, then delegates outside
the lock. `Worker.swap_sink` re-checks retirement after its first `flush()` and returns early once
`_shutdown_done`. If `shutdown()` latches in that window, the new sink is written to the config,
installed nowhere, and recorded nowhere.

Measured: `config.sink is B` → `True`, `A closed` → `True`, **`B closed` → `False`**,
`_orphan_sink` → `None`, and `health()` completely clean (`retired=True, incomplete_swaps=0,
stopped_reason=None, failed_batches=0`). For a sink whose `close()` *is* its delivery — `KafkaSink`
flushing its producer — that is a silently lost buffer, the shape SPEC-033 exists to close.

#### Acceptance Criteria:

- [x] AC-1: In that interleaving, B is closed exactly once by the time the process exits.
- [x] AC-2: A is still closed exactly once, and not closed by both paths.
- [x] AC-3: The test uses an injected preemption point inside `Worker.swap_sink`'s first
      `flush()`, since the window is a few instructions wide.
- [x] AC-4: The uncontended swap paths of SPEC-033 are unchanged — its whole test file still
      passes, and the transition table in that spec is amended rather than contradicted. Its
      `_swap_sink(N)` / worker-exists row reads "`Worker.swap_sink` owns it", which the declined
      branch makes false, so the table gains a declined row.

### FR-004: `shutdown()`'s idempotent path waits for the drain it found running

#### Description:

`Worker.shutdown` latches `_shutdown_done` on **entry**, so a second caller takes the `if not
first:` branch, calls `_close_if_owed()` (which declines, the thread being alive), joins closers,
and returns — typically in under a millisecond. It never waits for the first shutdown.

The common shape is not two user calls: it is a `shutdown()` on one thread and `atexit` on the
main thread. Measured with a 2 s-emit sink and 3 traced calls: **nothing delivered, the
sink never closed, the process gone in 0.39 s, and no `_diag` line.** ~~0 of 9 events~~ — the
count is struck rather than deleted (SPEC-021): a traced call buffers exactly two events, so
three of them are six, re-measured 2026-08-09, and the harness's other three were not recorded.
The shape reproduces; the figure does not. The control — no concurrent
`shutdown` — delivered all 9 and closed the sink in 2.09 s.

The fix is to wait, bounded by *this* call's own timeout, before `_close_if_owed()` — on a
`_drain_settled` event set both where the drain loop stops **and** where a shutdown gives up
on it. ~~`_drain_finished`~~ is struck: waiting on it alone lets a first caller that abandons
*after* the second has committed to the wait hang the exit, measured at 20.01 s against a
20 s budget and indefinitely with `timeout=None`. Only an event already being waited on can
release a waiter that has committed, and `_drain_finished` cannot be widened — `flush()` and
the sentinel gate read it as "the loop stopped reading the queue". That preserves every existing property: a first shutdown that expired still
returns early, and the grace still runs once (SPEC-033).

#### Acceptance Criteria:

- [x] AC-1: A second `shutdown()` entered while the first is draining waits for that drain,
      bounded by its own timeout, and the events are delivered.
- [x] AC-2: `shutdown(timeout=0)` on the second call still returns promptly — the wait is bounded
      by the caller's budget, not by the drain.
- [x] AC-3: A second `shutdown()` after the first **completed** still returns immediately;
      idempotency is not traded for correctness here.
- [x] AC-4: The closer grace is granted exactly once **per call**, so twice across two calls,
      and a test asserts the count rather than timing it. ~~exactly once across both calls~~ —
      struck because SPEC-033's invariant is once per `shutdown()`, not once per process, and an
      AC whose test had to be read the other way is an AC to amend rather than reinterpret.
- [x] AC-5: A first shutdown that **expired** still returns early from the second call, since the
      drain thread is wedged and waiting on it would hang the exit — the case
      `Worker._join_closers`'s docstring already reasons about.

### ~~FR-005: `os.fork()` is handled, or refused loudly~~ — moved to SPEC-039

**Struck in place rather than deleted** (SPEC-021's rule), because a requirement that simply
disappears takes its reasoning with it and a reader cannot tell a descoped item from a dropped
one. The subject is unchanged and nothing is abandoned: the two measured failures, the
`after_in_child`-only settlement, the struck "file sinks are immune" claim, the derived lock
roster, and the four measurements taken while preparing it are all carried into
[SPEC-039](SPEC-039-fork-lifecycle.md) — which restates them as six FRs rather than one FR with
eleven criteria.

**Why it moved.** It is the largest single piece of work in this spec, the only one needing a new
mechanism and a new module (`_fork.py`), and the only one whose subject is not a shutdown path.
The other five FRs were finished; holding a completed spec open for it would have kept SPEC-035
`In Progress` through a build at least as large as everything already shipped here, and would have
put a fork mechanism and a shutdown fix under one delivery doc. It also inverts a dependency
usefully: SPEC-039 depends on this spec's FR-002 roster, which its own new guards must satisfy.

Nothing else in this spec depended on it.


### FR-006: The accepted constraint is written down

#### Description:

Audit C5 is not fixed (see Out of Scope). SPEC-021's rule is that an open item is closed by being
fixed, settled, or **recorded** — never dropped — and a bullet in a spec's Out of Scope is not a
record a reader of `architecture.md` will ever find.

#### Acceptance Criteria:

- [x] AC-1: `architecture.md` §13 records that `_note_orphan_emit` and `_swap_sink` can write a
      `_diag` line while holding the process-wide `_worker_lock`, that this stalls every orphan
      emit and every first `@trace` behind a wedged console, that it is an error path only, and
      why the fix was judged worse than the trade.
- [x] AC-2: The entry names `Worker.submit` as the counter-example — it deliberately writes
      *outside* its lock for exactly this reason — so a later reader can see the inconsistency is
      known rather than accidental.

---

## Data Model

No new state and no `Health` field, on any FR that shipped. The fork handler this section used to
specify moved with FR-005 to [SPEC-039](SPEC-039-fork-lifecycle.md).

## Implementation Phases

### Phase 1: The two SPEC-033 regressions (FR-001, FR-003)

Smallest and most urgent — they are on `main`. Land as its own PR ahead of the rest.

### Phase 2: The enumeration (FR-002)

The test that stops FR-001 recurring, plus the §9 statement.

### Phase 3: The idempotent shutdown (FR-004)

### Phase 4: The recorded constraint (FR-006)

~~Fork (FR-005)~~ — moved to SPEC-039, which is its own spec and its own phases. FR-006 is what
remained: the §13 entry for audit C5, which grew from two named sites to three while being
written.
