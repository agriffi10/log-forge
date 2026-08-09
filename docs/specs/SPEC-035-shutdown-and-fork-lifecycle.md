# Spec: Shutdown and Fork Lifecycle

**ID:** SPEC-035  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-027, SPEC-028, SPEC-030, SPEC-033

## Overview

Four defects in the process-lifecycle plumbing, found by the concurrency surface of the
2026-08-07 audit (`docs/audits/2026-08-07-pre-1.0.md`, C1–C4). Two are **regressions SPEC-033
introduced and are on `main` now**, which is why this spec is first in the arc: `main` currently
has a worse shutdown story than it did before that spec landed.

The other two are older and larger. `atexit` can return through `shutdown()`'s idempotent path
and abandon a drain that is still running — measured, **0 of 9 events delivered and the process
gone in 0.39 s**. And `os.fork()` is unhandled anywhere in the tree: the child inherits a worker
whose thread does not exist, and — worse — sink locks held by a thread that no longer exists.
**19 of 60 forked children hung permanently** inside `FileSink.emit`. Prefork servers
(gunicorn, uWSGI, Celery) are a mainstream deployment for a logging library.

Lock ordering, counter synchronisation and `contextvars` were audited alongside these and are
**clean**; that is recorded in the audit so this spec does not have to re-establish it.

## Scope

### In Scope

- The two SPEC-033 regressions: a stolen stop signal, and a swap that leaves its new sink owned
  by nobody.
- `shutdown()`'s idempotent path waiting for the drain it did not start.
- `os.fork()`: the inherited worker, and the inherited locks.

### Out of Scope

- **Making the orphan path non-blocking**, or any other item in `architecture.md` §13's recorded
  constraints. Those are accepted trades, not defects.
- **Restarting a worker after a terminal failure.** SPEC-019 settled that a thread which
  resurrects itself fights a process trying to exit. FR-004 rebuilds a worker after a **fork**,
  which is a different event: the process is new, and nothing is trying to exit.
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

- [ ] AC-1: `_offer_orphan_signal` skips only for the sink a worker **holds** —
      `_worker is not None and _worker.sink is sink` — the guard `_close_orphan_sink` already
      uses. `_live_worker()` is not consulted.
- [ ] AC-2: An orphan log during `shutdown()`'s drain leaves `sink.stop_signal is worker._stop`.
- [ ] AC-3: End to end: with a sink whose backoff is far longer than the shutdown budget, one
      concurrent orphan log does not change the outcome — the backoff is still cut short, and
      `stopped_reason` stays `None`. The test keeps the backoff-to-budget **gap** wide rather
      than the budget tight.
- [ ] AC-4: The sink adopted *after* a retired worker still receives a signal (SPEC-033 FR-004
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

- [ ] AC-1: A test walks `decorator.py`'s AST and finds every call to `_live_worker()`, every
      `_worker.sink is …` comparison, **and every bare `_worker is not None`** — that third form
      exists today at `decorator.py:475` and an AST walk looking only for the first two would
      never see it, making AC-3's guarantee false for exactly the phrasing SPEC-033's docstrings
      warn about.
- [ ] AC-1b: The categories are **three**, not two: performs-a-swap (liveness), owns-a-close
      (ownership), and **offers-a-signal** (ownership) — FR-001 turns `_offer_orphan_signal` into
      a site that is neither of the first two, so a two-category table could not classify the very
      site this spec creates.
- [ ] AC-2: The category table is **in the test**, one line per site with the reason, so adding a
      site forces a decision rather than defaulting silently.
- [ ] AC-3: The test fails when FR-001's fix is reverted, and fails when a new call site is added
      without a category. Both are demonstrated by mutation.
- [ ] AC-4: `docs/architecture.md` §9 states the rule, so it is discoverable from the design doc
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

- [ ] AC-1: In that interleaving, B is closed exactly once by the time the process exits.
- [ ] AC-2: A is still closed exactly once, and not closed by both paths.
- [ ] AC-3: The test uses an injected preemption point inside `Worker.swap_sink`'s first
      `flush()`, since the window is a few instructions wide.
- [ ] AC-4: The uncontended swap paths of SPEC-033 are unchanged — its whole test file still
      passes, and the transition table in that spec is amended rather than contradicted.

### FR-004: `shutdown()`'s idempotent path waits for the drain it found running

#### Description:

`Worker.shutdown` latches `_shutdown_done` on **entry**, so a second caller takes the `if not
first:` branch, calls `_close_if_owed()` (which declines, the thread being alive), joins closers,
and returns — typically in under a millisecond. It never waits for the first shutdown.

The common shape is not two user calls: it is a `shutdown()` on one thread and `atexit` on the
main thread. Measured with a 2 s-emit sink and 3 traced calls: **0 of 9 events delivered, the
sink never closed, the process gone in 0.39 s, and no `_diag` line.** The control — no concurrent
`shutdown` — delivered all 9 and closed the sink in 2.09 s.

The fix is to wait on `_drain_finished`, bounded by *this* call's own timeout, before
`_close_if_owed()`. That preserves every existing property: a first shutdown that expired still
returns early, and the grace still runs once (SPEC-033).

#### Acceptance Criteria:

- [ ] AC-1: A second `shutdown()` entered while the first is draining waits for that drain,
      bounded by its own timeout, and the events are delivered.
- [ ] AC-2: `shutdown(timeout=0)` on the second call still returns promptly — the wait is bounded
      by the caller's budget, not by the drain.
- [ ] AC-3: A second `shutdown()` after the first **completed** still returns immediately;
      idempotency is not traded for correctness here.
- [ ] AC-4: The closer grace is still granted exactly once across both calls (SPEC-033), and a
      test asserts the count rather than timing it.
- [ ] AC-5: A first shutdown that **expired** still returns early from the second call, since the
      drain thread is wedged and waiting on it would hang the exit — the case
      `Worker._join_closers`'s docstring already reasons about.

### FR-005: `os.fork()` is handled, or refused loudly

#### Description:

Nothing in `src/`, `docs/` or `tests/` mentions `fork`. Two distinct failures, both measured:

**The child inherits a worker whose thread does not exist.** `submit` still enqueues; nothing
drains. The child's 6 events were never delivered, `atexit` closed the sink without draining, and
`health()` read `queued=2, dropped=0, failed_batches=0, stopped_reason=None, retired=False` —
the documented alert idiom is blind. `flush()` returning `False` was the only honest surface, and
the library wrote nothing to stderr across the whole run.

**The child inherits a sink lock held by a thread that no longer exists.** Fork while the drain
thread is inside `FileSink.emit` and the child's first `log_foundry.info()` blocks forever, on the
application's own thread. Measured: **19 of 60 forked children hung permanently**, `faulthandler`
showing `file.py:78 in emit ← api.py:93 in _log`. This reaches every sink SPEC-028 locked —
`FileSink`, `SQLiteSink`, `PostgresSink`, `ClickHouseSink`, the socket sinks.

The second is the one that matters: losing a child's logs is bad, hanging the application is
worse, and a logging call is the last place an application expects to deadlock.

`os.register_at_fork` is the mechanism. The **design decision this FR must settle** is what the
child does, and the options are not equivalent:

- *Rebuild the worker in the child.* Delivery continues, and each child gets its own drain
  thread — which for a prefork server is what the user wants. But the child inherits the parent's
  sink object, and two processes writing one socket or one SQLite handle is its own defect.
- *Retire the worker in the child and record why.* Nothing is delivered, but nothing is lost
  silently either: `stopped_reason` says the process forked, and the operator sees it.

The recommendation is **rebuild the queue and thread, re-initialise every lock, and leave the
sink alone** — the sink is the caller's object and the caller's choice, `Sink` already documents
concurrent use, and a child that silently stops logging is the failure this arc exists to remove.
A `stopped_reason` of `"Forked"` is *not* right for a child that then works.

#### Acceptance Criteria:

- [ ] AC-0: **The `before` handler is bounded.** Measured: acquiring a sink's SPEC-028 transport
      lock while the drain thread is mid-`emit` blocked the fork for 1.20 s, and with `HTTPSink`'s
      documented 90 s worst case it would block gunicorn's master thread for that long — with no
      shutdown in progress, so the stop signal cannot cut it. The handler try-acquires with a
      short deadline and proceeds **without** the lock when it expires; the FR states the
      consequence for child consistency, which is that the child may inherit a half-written sink
      buffer and must therefore not reuse it (AC-7).
- [ ] AC-0b: The design does not *depend* on `before` running. `os.register_at_fork(before=)` is
      not invoked for a C-level fork — uWSGI calls `PyOS_AfterFork_Child` only — so a design that
      needs `before` degrades silently on one of the three deployments this spec names. The child
      handler alone must be sufficient for AC-1.
- [ ] AC-1: After a fork, the child's first log call does not block. A test forks repeatedly
      (≥50 iterations) with the drain thread actively emitting into a locking sink, and every
      child completes within a timeout. The pre-fix version of this test hangs, which is
      demonstrated.
- [ ] AC-2: Every lock the library owns is re-initialised in the child — `decorator._worker_lock`,
      `_lifecycle._closers_lock`, `Worker._lock`, the counter locks, and each locking sink's
      pair. Derived from a roster, not hand-listed (FR-002's lesson).
- [ ] AC-3: A child that logs after forking delivers those events.
- [ ] AC-4: The parent is unaffected: its worker, queue and counters are unchanged across the
      fork, and a test asserts the parent's delivery continues.
- [ ] AC-5: Events queued in the parent but undelivered at fork time are **not** delivered twice.
      The child's queue starts empty; the parent keeps them. A test asserts the total across both
      processes.
- [ ] AC-6: `architecture.md` §9 and the README state the fork behaviour. A user running gunicorn
      preload needs to be able to find it.
- [ ] AC-7: A sink shared across a fork is documented as the caller's responsibility, with the
      concrete hazard named (one socket or one SQLite handle, two processes).

### FR-006: The accepted constraint is written down

#### Description:

Audit C5 is not fixed (see Out of Scope). SPEC-021's rule is that an open item is closed by being
fixed, settled, or **recorded** — never dropped — and a bullet in a spec's Out of Scope is not a
record a reader of `architecture.md` will ever find.

#### Acceptance Criteria:

- [ ] AC-1: `architecture.md` §13 records that `_note_orphan_emit` and `_swap_sink` can write a
      `_diag` line while holding the process-wide `_worker_lock`, that this stalls every orphan
      emit and every first `@trace` behind a wedged console, that it is an error path only, and
      why the fix was judged worse than the trade.
- [ ] AC-2: The entry names `Worker.submit` as the counter-example — it deliberately writes
      *outside* its lock for exactly this reason — so a later reader can see the inconsistency is
      known rather than accidental.

---

## Data Model

No new state, no `Health` field. FR-005 adds fork handlers registered at import:

```python
# src/log_foundry/_lifecycle.py — or a new _fork.py if the roster grows
os.register_at_fork(
    before=_acquire_library_locks,        # ordered as §9 requires, so the child is consistent
    after_in_parent=_release_library_locks,
    after_in_child=_reinit_after_fork,    # fresh locks, fresh queue + thread, sink untouched
)
```

## Implementation Phases

### Phase 1: The two SPEC-033 regressions (FR-001, FR-003)

Smallest and most urgent — they are on `main`. Land as its own PR ahead of the rest.

### Phase 2: The enumeration (FR-002)

The test that stops FR-001 recurring, plus the §9 statement.

### Phase 3: The idempotent shutdown (FR-004)

### Phase 4: Fork (FR-005)

Largest, and the only one needing a new mechanism. The lock roster comes first, since AC-2's
derivation decides how much of the rest is mechanical.
