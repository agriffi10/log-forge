# Spec: Orphan-Path Sink Handoff

**ID:** SPEC-033  
**Status:** Completed  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-026, SPEC-027, SPEC-028, SPEC-030, SPEC-031

## Overview

`configure(sink=...)` promises that the previous sink is drained, closed, and must not be handed
back to a later call. That promise holds only when a background worker exists. A process that logs
exclusively through the level calls — `info()`, `warning()` and friends, with no `@trace` anywhere —
never builds one, so the promise silently does not apply to it: the previous sink is left open, its
locally-buffered events are never delivered, and `health()` reports nothing, because the field that
would report it (`incomplete_swaps`) describes a worker that does not exist.

This is the last item from the 2026-08-05 audit arc. SPEC-031 FR-006 fixed the *shutdown* half of
the same root cause — a process with no worker now closes its sink exactly once at exit — and
explicitly scoped this half out, recording it in `architecture.md` §13 so that striking through the
paragraph it replaced would not be read as closing this variant too. This spec is the home §13 says
it needs.

The library already knows which sink an orphan log reached — it resolves one immediately before
emitting — but records only *that* one was reached, not *which*. Recording the identity is what lets
a late `configure(sink=...)` close it, and it turns out to be worth more than the swap: the same
boolean is why a sink configured **after** `shutdown()` is never closed at all, and why an
orphan-only process never hands its sink the stop signal SPEC-027 built. Both are fixed here with
*less* state, not more, and both were found by independent review of this spec's first draft.

**Measured, on `f17edd4`**, `configure(sink=A)` → `info()` → `configure(sink=B)` → `info()` →
`shutdown()`:

```
A.closed = False   A.held = 1     <- one event, never delivered, sink never released
B.closed = True    B.held = 1
incomplete_swaps = 0   closing_sinks = 0   retired = True   failed_batches = 0
```

The same sequence with one `@trace` call ahead of it — which builds the worker — closes A correctly
and is the control that isolates the defect to the no-worker path.

**Also measured, and the reason FR-002 re-points rather than clears:** `configure(A)` → `info()` →
`configure(B)` → `shutdown()`, *with no second `info()`*, closes B today (`B.closes = 1`) precisely
because the boolean is path-agnostic and `_ensure_sink()` still names B at exit. A fix that cleared
the record at the swap would trade A's leak for B's. This one keeps both closed.

**And the second defect, measured:** `configure(A)` → `info()` → `shutdown()` → `configure(B)` →
`info()` leaves B open at exit, losing a locally-buffering sink's whole batch, while every counter
reads clean:

```
B accepted the event: held=1   (no exception, no refusal)
health(): retired=True  submitted_after_shutdown=0  failed_batches=0  dropped=0
AT EXIT -> B.closed=False  delivered=0  still-held=1
```

SPEC-030's `retired` + `submitted_after_shutdown` pair cannot fire, because there is no worker to
count a submission — the arc's signature failure shape, and strictly less visible than the worker
path, where the same mistake at least queues and counts.

## Scope

### In Scope

- Recording the **identity** of the sink an orphan emit reached, not merely that one was reached.
- Closing that sink when a late `configure(sink=...)` retargets a process that has no worker, and
  re-pointing the record at the new one.
- Making the once-only close **per sink** rather than per process, which is what closes a sink
  configured after `shutdown()`.
- Handing an orphan-only process's sink the SPEC-027 stop signal, which today it never receives.
- Bounding the swapped-out close on the same terms as the worker path's, and granting it the same
  exit grace.
- Making `health().closing_sinks` reachable on the no-worker path.
- Extracting the shared sink-lifecycle machinery into one module both paths use.
- Recording the resolution in `architecture.md` §13 and splitting `configure()`'s swap paragraph by
  path, since it currently describes a drain this path does not have.

### Out of Scope

- **Any new `Health` field.** SPEC-030 settled that vocabulary and SPEC-031 declined to extend it
  for the same root cause. `closing_sinks` becomes *reachable* on this path; it is not new, and
  nothing else is added.
- **Widening `incomplete_swaps`.** FR-006 settles it in the other direction — it keeps its
  worker-only meaning. That is a decision, not an omission.
- **Creating a worker to perform the swap.** The refusal SPEC-030 FR-003 made, SPEC-031 FR-006
  repeated, and `_flush_worker` and `_worker_health` also make: standing up a thread to prove there
  is nothing to drain is pure cost.
- **Draining or fencing the orphan path.** There is nothing buffered to drain — an orphan emit is
  synchronous and has returned before `configure()` is entered on the same thread — and the one
  concurrent writer a fence could not exclude is the same one the *worker* path cannot exclude
  either. FR-002 states the contract that already covers it.
- **Making `configure()` thread-safe.** It remains a startup call, as its docstring says and as
  `Worker.swap_sink` restates. FR-002 AC-10 closes one specific race that this spec would otherwise
  *introduce*; it does not make a concurrent orphan emit during a swap deterministic.
- **Bounding the *live* sink's close at shutdown.** SPEC-031 FR-006's `_close_orphan_sink` closes
  inline and unbounded, matching `Worker._close_if_owed`, and arch §13 records why running that one
  on a daemon was built and reverted. Only a **swapped-out** sink's close is bounded here.
- **Closing a sink more than once when the caller hands back a sink already swapped out.**
  `configure(A)` → `info()` → `configure(B)` → `configure(A)` closes A twice. This is a documented
  user error — `configure()`'s own docstring says the previous sink "must not be handed back to a
  later call" — `sinks/base.py:138` requires `close()` to be idempotent, and **the worker path
  behaves identically** (measured: `A.closes=2, B.closes=1`). Tracking every sink ever closed would
  pin them all against collection to fix a case the sibling path does not fix either. Recorded by
  FR-007, not built.
- **Reviving the worker path's swap semantics.** No double drain, no `_record_incomplete_swap`.
  What is shared is the *close*, not the swap protocol.

---

## Functional Requirements

### FR-001: The orphan path records which sink it wrote to, and which it has closed

#### Description:

`decorator._orphan_close_owed` and `_orphan_sink_closed` are both booleans. The first records that
*a* sink was written to, which suffices for a close at exit (where `_ensure_sink()` still names that
sink) and fails at swap time, because `configure()` assigns `_config.sink` at `config.py:129`
*before* calling `_swap_live_sink` at `config.py:144` — by the time anything could close the old
sink, the config no longer names it. The second makes the close once per **process**, which is why a
sink configured after `shutdown()` is never closed.

Replace both with sink references. `api._log` resolves `_ensure_sink()` immediately before emitting
and is the only place in the library that holds that identity; it passes it to `_note_orphan_emit`.

- `_orphan_sink` — the sink the orphan path owns the close of. `None` means nothing is owed.
- `_orphan_closed_sink` — the most recently closed orphan-owned sink, which is refused re-arming.

That second reference is what makes the rule "each sink is closed at most once" instead of "at most
one close ever", and it is strictly less state than the boolean it replaces: it prevents the
double-close hazard the boolean existed for *and* lets a post-`shutdown()` `configure(sink=B)` →
`info()` be closed at exit.

**An alternative that keeps the booleans was considered and rejected:** `configure()` could capture
`_config.sink` before reassigning it and pass the old sink to `_swap_sink`. It needs no module-level
reference and no conftest change, but it is strictly less precise — a second swap with no
intervening emit would then close a sink that was configured and never written to, which AC-3
forbids — and it does nothing for the post-`shutdown()` case or the stop signal, both of which fall
out of the identity for free.

The two properties SPEC-031 FR-006 established are preserved verbatim, and both are load-bearing:
arming is keyed on an event **reaching** a sink rather than on a sink being **configured** (because
`configure()` runs `_ensure_sink()` unconditionally and would arm a close over a `StdoutSink`
nothing was written to), and arming happens **before** the emit rather than after it (because
SPEC-026 FR-001 makes a total failure raise, and a sink that raised is still a sink whose socket
must be released).

#### Acceptance Criteria:

- [ ] AC-1: After an orphan emit, the library holds a reference to the exact sink object that emit
      was made against — identity, not a lookup deferred to close time.
- [ ] AC-2: Reassigning `_config.sink` between the emit and the close does not change which sink is
      closed. A test asserts this directly, since it is the property the boolean lacked.
- [ ] AC-3: A process that calls `configure()` and never makes an orphan emit records nothing, and
      closes nothing at swap or at exit (SPEC-031 FR-006 AC-8, unchanged).
- [ ] AC-4: A sink whose orphan `emit` **raised** is still recorded and still closed.
- [ ] AC-5: An orphan emit against the sink recorded in `_orphan_closed_sink` does not re-arm it, so
      a refused post-`shutdown()` emit cannot cause a second `close()` — the outcome SPEC-031 FR-006
      ranks worse than an unclosed sink.
- [ ] AC-6: `configure(sink=B)` → `info()` after `shutdown()` closes B at exit — in an orphan-only
      process **and** in one that built a worker and retired it. The second is a separate test
      because it is a separate mechanism (FR-002 AC-12) and because the first draft's criterion was
      false there: measured on `f17edd4`, `configure(A)` → `@trace` → `shutdown()` →
      `configure(B)` → `info()` gives `A.closes=1, B.closes=0, B.held=1` with
      `retired=True, submitted_after_shutdown=0, failed_batches=0`. Both tests use a sink whose
      `close()` is its delivery and assert the event lands, since an unclosed `close()`-delivers
      sink is the loss this criterion exists to stop.
- [ ] AC-7: The record is cleared when the sink is closed and when a worker takes ownership. The
      reason is correctness, not collectability — a stale reference is a reference the *next* close
      would target after it was already closed.
- [ ] AC-8: The hot path stays one unlocked read plus an identity comparison; `_worker_lock` is
      taken only when the recorded sink is not the one being emitted to. The docstring states the
      replacement invariant, since SPEC-031's ("written once and never cleared") no longer holds:
      a reference read is atomic, so the unlocked read is **stale, never invalid**; a stale mismatch
      self-corrects under the lock; and a stale *match* is reachable only when an emit races a
      close, which is the lifecycle error SPEC-030 documents rather than a new one.
- [ ] AC-9: The Data Model comment in `decorator.py` names which reads are unlocked and why, rather
      than claiming `_worker_lock` guards every access.
- [ ] AC-10: `tests/conftest.py`'s reset fixture covers both new references, `_orphan_stop`, **and**
      `_lifecycle._closers`. The last is new process-global state, so a hung closer left by one test
      — the existing capped-grace tests deliberately create them — otherwise leaks a non-zero
      `closing_sinks` into the next test.

### FR-002: A late `configure(sink=...)` with no worker closes the old sink and adopts the new one

#### Description:

`decorator._swap_sink` returns early when `_worker is None` (`decorator.py:368-370`). That early
return is correct on its own terms — a process with no worker has captured no sink to *swap* — but
it is currently the whole function for this path, so nothing performs the handoff. Give it a second
branch: with no worker and a recorded orphan sink that is not the incoming one, **re-point the
record at the new sink**, record the old one as closed, and close it.

Re-pointing rather than clearing is the correction the first draft needed. Clearing would leave
nothing armed until the next orphan emit, and a process that swaps and then exits without logging
again would leak the *new* sink — measured, that case closes B correctly today, so clearing would
trade one leak for another. Re-pointing is also what the worker path does: `Worker.shutdown` closes
`self.sink` whether or not anything was emitted to it since the swap.

No drain and no fence. The worker path drains twice because events sit in a queue and because the
drain thread may be inside the old sink's `emit`; neither is true here. Orphan events are emitted
synchronously on the caller's thread and have returned before `configure()` is entered. The one
writer that could still be inside the old sink's `emit` is an orphan emitter on *another*
application thread — and that is precisely the writer `Worker._close_swapped_out` documents itself
as **not** covering either, which is why `sinks/base.py` requires `close()` to tolerate a concurrent
`emit` (SPEC-028 FR-001). This path inherits that contract unchanged; it does not weaken it.

**The `_worker` read must move under `_worker_lock`.** Today's unlocked read at `decorator.py:368`
is harmless because the no-worker branch does nothing. Once it closes a sink it is the race
`_close_orphan_sink`'s docstring (`decorator.py:286-290`) describes: a first `@trace` on another
thread can be inside `Worker.__init__` — having already resolved `_ensure_sink()` to A — while this
thread reads `_worker` as `None` and detaches a close of the sink that worker is about to deliver
to. `_get_worker` assigns `_worker` under that lock, which is what makes the locked read sufficient.

#### Acceptance Criteria:

- [ ] AC-1: `configure(sink=A)` → `info()` → `configure(sink=B)` calls `A.close()` exactly once,
      with no `@trace` anywhere in the process.
- [ ] AC-2: After that swap, `info()` reaches B and not A.
- [ ] AC-3: `configure(A)` → `info()` → `configure(B)` → `shutdown()`, **with no second `info()`**,
      closes both A and B, exactly once each. This is the case the first draft regressed and it is
      the reason the record is re-pointed rather than cleared.
- [ ] AC-4: `configure(sink=A)` → `configure(sink=B)` with no orphan emit between them closes
      nothing (FR-001 AC-3, asserted end to end).
- [ ] AC-5: `configure(sink=A)` → `info()` → `configure(sink=A)` — the same object — closes nothing
      and leaves the record armed, mirroring `Worker.swap_sink`'s `self.sink is new_sink` no-op.
- [ ] AC-6: With a worker present, this branch closes nothing, `Worker.swap_sink` still owns the
      close, and the orphan record is cleared so nothing closes it a second time. A test covers the
      mixed process in **both** orders — orphan-then-`@trace` and `@trace`-then-orphan — and asserts
      exactly one `close()` on the old sink. This is the case an independent review of SPEC-031
      FR-006 found uncovered, and the one most likely to double-close.
- [ ] AC-7: A `close()` that raises is absorbed and announced through `_diag.absorbed`; `configure()`
      returns normally and the swap still stands. The test **joins the closer before asserting**,
      since with FR-003's detached close the line may otherwise be written after `configure()`
      returns and the assertion would pass vacuously.
- [ ] AC-8: A `Thread.start` that fails announces, leaves the old sink open, and **still re-points**
      the record — matching `Worker._close_swapped_out`, which leaves the sink open rather than
      falling back to an inline close. The order is specified because both readings of "close and
      re-point" satisfy prose and they leak different sinks.
- [ ] AC-9: A three-sink chain `A → info → B → C → info → shutdown` closes each of A, B and C
      exactly once.
- [ ] AC-10: `_swap_sink` reads `_worker` and both records under `_worker_lock`. A test reproduces
      the first-`@trace` race with an injected preemption point — the technique SPEC-028 uses and
      `_close_orphan_sink` was built against — and asserts the worker's live sink is not closed.
- [ ] AC-11: The record mutation and `Thread.start` happen under `_worker_lock`; the closer's
      **join does not**. `close_detached` waits up to `DEFAULT_SWAP_TIMEOUT`, and holding the
      process-wide lock across it would park every concurrent `_note_orphan_emit`, `_get_worker` and
      `_close_orphan_sink` for the whole budget — a far larger version of the cost arch §13 already
      records. `Worker.swap_sink` holds `self._lock` only for its guards, and this matches it. A
      test asserts a concurrent orphan emit is not blocked for the swap budget. This is why
      `close_detached` returns the thread instead of joining it — the one-call form cannot express
      the split.
- [ ] AC-12: `_shutdown_worker`'s worker branch **falls through** to `_close_orphan_sink()` instead
      of returning, and that function's guard is `_worker.sink is _orphan_sink` rather than
      `_worker is not None`. A test covers the retired-worker sequence of FR-001 AC-6, and another
      asserts an **expired** `shutdown()` still declines to close — the case the original guard
      existed for, which the identity form must not regress.
- [ ] AC-13: Each assertion above is mutation-tested — the branch is stashed and the test re-run —
      so no criterion is ticked by a test that passes against the defect it claims to catch.

### FR-003: The swapped-out sink's close is bounded, abandonable, and granted the exit grace

#### Description:

Close it on the shared daemon closer of FR-005 rather than inline. The reason is SPEC-030 FR-003's,
measured again here: a sink that hangs in `close()` blocks the caller for as long as it hangs, and
`configure()` is on the application's startup path. **Measured on `f17edd4`** against a sink whose
`close()` sleeps 8 s, the worker path returns in 5.00 s — its `DEFAULT_SWAP_TIMEOUT` — and reports
`closing_sinks = 1`. An inline close here would take the full 8 s and report nothing, reintroducing
on this path the exact gap arch §13 records as closed for the other one.

Everything SPEC-030 settled about that closer carries over unchanged and is not re-litigated: the
thread is a daemon; the join is capped; **an expired join derives no signal** — no counter moves and
no line is written, which is what dissolved SPEC-028's wrong-signal objection — and the live fact is
published as `health().closing_sinks` instead.

**The grace belongs in `_shutdown_worker`, not inside `_close_orphan_sink`.** Placing it inside the
close would skip it in exactly the cases that need it: a shutdown where nothing is armed, and the
idempotent second `shutdown()` — the explicit call followed by `atexit` — which
`Worker._join_closers` (`worker.py:766-772`) documents at length as load-bearing, because a first
shutdown that expired returns before reaching the grace and the `atexit` call is the only one left
to grant it. Without the grace a daemon closer that is slow but *succeeding* is killed at
interpreter exit, losing the buffer of a sink whose `close()` is its delivery.

#### Acceptance Criteria:

- [ ] AC-1: A swapped-out orphan sink whose `close()` sleeps well past the budget does not hold
      `configure()` beyond `DEFAULT_SWAP_TIMEOUT`. The test keeps the gap between the budget and the
      sleep wide, not the budget tight.
- [ ] AC-2: An expired join increments no counter and writes no `_diag` line.
- [ ] AC-3: `health().closing_sinks` reads 1 while such a close is running **in a process with no
      worker**, and returns to 0 once it finishes.
- [ ] AC-4: `shutdown()` on an orphan-only process joins outstanding closers for at most
      `DEFAULT_CLOSER_GRACE`, and for no more than what remains of its own timeout.
- [ ] AC-5: The grace runs when **nothing is armed** and on the **second** `shutdown()` call, not
      only on a shutdown that performs a close. Two tests, one per case.
- [ ] AC-6: The grace runs **after** the live orphan sink's inline close, matching
      `Worker.shutdown`'s order, and a test pins the ordering rather than only the outcome.
- [ ] AC-7: The live orphan sink's own close at shutdown stays inline and unbounded — unchanged from
      SPEC-031 FR-006, and asserted so a future refactor cannot quietly move it onto the closer.
- [ ] AC-8: A `Thread.start` that fails leaves the sink open and announces it; there is no inline
      fallback, for the reason `Worker._close_swapped_out` gives — the fallback reintroduces the
      unbounded wait in the one situation where the process is already under resource pressure.

### FR-004: An orphan-only process hands its sink the stop signal

#### Description:

`Worker._offer_stop_signal` (`worker.py:242`) is called only from `Worker.__init__` and
`Worker.swap_sink`. A process with no worker therefore never gives its sink a stop event, so
SPEC-027's guarantee — every sink wait is cut short by a shutdown — is **false on the path this
spec is about**, and a retrying sink's backoff on an application thread runs to completion during
shutdown and interpreter exit. That matters here rather than in the abstract: FR-003 deliberately
leaves the live sink's close inline and unbounded, and SPEC-028 made `close()` take the sink's emit
lock, so that close can now sit behind an uninterruptible backoff held by another orphan writer —
the constraint arch §13 already records, with its ceiling removed.

This is a widening of the spec's original subject, taken deliberately. It is the same roster lesson
SPEC-027 and SPEC-028 both record — a facility built for one caller and never wired to the other —
found in the same function this spec is rewiring, and leaving it unfixed while writing three FRs
about bounding this path's waits would be recording a ceiling that does not exist.

Arm a module-level `threading.Event` at the same point FR-001 arms the record, offered with the same
`hasattr` probe and the same absorbed failure.

Three details are the whole of it, and each was a defect in the draft that omitted it.

**The offer is skipped only for the sink a worker actually owns** — `_worker is not None and
_worker.sink is sink`, the same ownership test FR-002 AC-12 makes for the close, for the same
reason. `_offer_stop_signal` is a bare assignment, so whoever offers last wins: an orphan emit made
*after* a worker was built would otherwise overwrite the worker's own `_stop` on the sink the
worker is delivering to, and the drain thread parked in that sink's `wait()` would serve its full
backoff across `Worker.shutdown`'s join — the global pause SPEC-027 exists to remove, reintroduced
by the fix for it. Skipping on mere *existence* is wrong in the case FR-001 AC-6 adds: after a
retired worker, `Worker.swap_sink` early-returns on `_shutdown_done`, so `_worker.sink` stays the
old sink forever while every orphan event goes to the new one — measured, `worker.sink is A: True`
with `B.held 1` and `B.stop_signal: None`. The worker will never offer to B, so a skip on existence
leaves a live, delivering sink uninterruptible for the rest of the process.

**`_orphan_stop` is set before delegating to `_worker.shutdown`, not after.** The reason is the
same ordering, from the other side.

**The rule is a property of the sink, not of the record: on an orphan emit to a sink no live worker
owns, its signal must be a non-set event** — so whenever `_orphan_stop.is_set()`, mint a fresh one
and offer it. An Event is set once and never cleared, and `sinks/_retry.wait` returns immediately on
a set one — measured, `wait(5.0)` on a set event takes 0.000 s against 0.405 s for `wait(0.4)` on an
unset one — so a sink still holding the shutdown's event has every backoff collapsed to zero,
turning a rate-limited or flapping destination into a tight retry loop.

Keying this on *arming* was the draft's error and it misses the likelier sequence: `configure(A)` →
`info()` → `shutdown()` → `info()`. That last emit reaches A — measured, `held` goes 1 → 2 against a
sink with `closes=1` — but FR-001 AC-5 refuses to re-arm the latched sink, so an arming-keyed rule
never fires and A keeps the set event. Against a stateless sink that genuinely still delivers
(`HTTPSink.close()` is a no-op, and SPEC-032 keeps it accepting) that is a **regression** on today,
where `stop_signal` is `None` and the sink backs off correctly. The fast path costs one `is_set()`
read. SPEC-027's contract is "cut short *by a shutdown*", not "never wait again", and
post-`shutdown()` logging is a supported path (SPEC-030), so these are live sinks and not corners.

#### Acceptance Criteria:

- [ ] AC-1: After an orphan emit with no worker, a sink advertising `stop_signal` has been given one.
- [ ] AC-2: `shutdown()` sets it, and a sink parked in `sinks/_retry.py`'s wait returns promptly
      rather than serving its full backoff. The test measures the gap between the backoff and the
      budget, not a tight budget.
- [ ] AC-3: A sink with no `stop_signal` attribute is unaffected, and one whose assignment raises is
      absorbed and announced — the sink loses interruptibility rather than the emit failing, exactly
      as `Worker._offer_stop_signal` behaves.
- [ ] AC-4: For the sink a live worker owns, the orphan path offers nothing and that sink still
      holds the worker's own `_stop`. A test asserts the drain thread's backoff is cut short by
      `Worker.shutdown` in a mixed process where an orphan emit happened **after** the worker was
      built — the sequence that would otherwise overwrite it. A second test covers the retired
      worker of FR-001 AC-6: `_worker.sink` is still A while events go to B, and B **is** offered a
      signal, since a skip on mere existence would leave it uninterruptible for the process's life.
- [ ] AC-5: `_shutdown_worker` sets `_orphan_stop` **before** delegating to `_worker.shutdown`, and
      a test pins the order rather than only the outcome.
- [ ] AC-6: A sink emitted to **after** `shutdown()` holds an unset event and still backs off — a
      test measures that its retry wait is not collapsed to zero. Two cases, because the rule is
      keyed on the sink rather than on the record: a **newly configured** sink (FR-001 AC-6), and
      the **same** sink logged to again, which FR-001 AC-5 refuses to re-arm and which an
      arming-keyed rule would therefore miss. The second is a regression test against today's
      behaviour, where that sink has no signal at all and backs off correctly.
- [ ] AC-7: The offer is made on the swap too, so the sink adopted by FR-002 receives one without
      waiting for the next emit.
- [ ] AC-8: The probe and the absorbed failure are the shared helper of FR-005, not a second copy of
      `Worker._offer_stop_signal`'s body.

### FR-005: One process-wide sink-lifecycle module, used by both paths

#### Description:

`DEFAULT_CLOSER_GRACE`, the live-closer list, the daemon spawn, the guarded close body, the capped
grace join and the stop-signal probe currently live on `Worker` (`worker.py:29,229,242-267,561-649,
746-793`) and are reachable only through a worker instance. Move them to a leaf module —
`src/log_foundry/_lifecycle.py`, on the precedent of `_diag.py`, `sinks/_retry.py` and
`sinks/_batch.py` — holding process-global state, and have `Worker` and `decorator` both call it.

**The load-bearing requirement is that the state is process-global, not that the file is new.**
A per-path registry is measurably blind in a mixed process: a closer started before a worker existed
would be invisible to `health().closing_sinks` and would be denied the exit grace once the worker
owned `shutdown()` — the "every field describes a worker that does not exist" shape this arc has now
fixed three times. `closing_sinks` is inherently process-scoped; there is only ever one worker.
Duplicating the machinery instead is the mistake SPEC-029 diagnosed, where twelve of twenty-eight
diagnostic sites drifted from the other eight.

The separate module is the secondary argument and is taste plus file size: `decorator` already
imports `worker` at module scope, so module-scope functions in `worker.py` would be equally correct
and a smaller diff. The file is chosen anyway because a facility whose purpose here is to serve a
path with no worker does not belong to the worker, and `worker.py` is 1176 lines.

#### Acceptance Criteria:

- [ ] AC-1: `health().closing_sinks` reports the same number whether or not a worker exists, and a
      closer started before the worker was built is still counted after one is built.
- [ ] AC-2: `shutdown()` joins closers started on either path, under one shared grace.
- [ ] AC-3: `_lifecycle.py` imports nothing from the package but `_diag`, and `Sink` only under
      `TYPE_CHECKING`; `mypy --strict` and the import-cycle expectations are unchanged.
- [ ] AC-4: Every SPEC-030 behaviour currently asserted through `Worker._close_swapped_out`,
      `Worker._join_closers`, `Worker._closers` and `worker.DEFAULT_CLOSER_GRACE` still holds; those
      tests are re-pointed at the new seams rather than deleted.
- [ ] AC-5: The re-point is verified by **asserting the patched value takes effect**, not by a
      `pytest --collect-only` name diff alone. `tests/test_config.py:530` monkeypatches
      `worker.DEFAULT_CLOSER_GRACE` to 0.3 and asserts an elapsed time under 5.0 — left pointing at
      a stale module that test passes with the real 2.0 grace, under an unchanged name, so a name
      diff cannot see it. The name diff is still run, for the separate hazard of a rewrite silently
      dropping tests.
- [ ] AC-6: `Worker.health()` no longer prunes the closer list under the worker's own lock; the
      registry's lock is its own, and `health()` remains safe to call during an emit (SPEC-026).

### FR-006: `incomplete_swaps` keeps its worker-only meaning

#### Description:

SPEC-030 defines `incomplete_swaps` as a **drain** that could not be confirmed, paired with a
specific consequence: queued items may have reached the new sink instead of the old one, and the old
sink was left open. Neither half exists on this path. There is no queue and no drain, so there is
nothing to confirm; and an expired *close* join is explicitly not a signal, by the decision that
made SPEC-030's bounded close available at all.

Moving the counter here would give one field two meanings and make the alert idiom ambiguous — a
non-zero `incomplete_swaps` would no longer tell an operator whether events were misrouted or merely
whether a close was slow. It stays where it is, and this is recorded rather than left to inference,
because the obvious reading of "the swap didn't fully complete" points the wrong way.

The criteria below are deliberately framed as *this spec adds no increment*, not as *the field is
always zero*. On the no-worker path the field is a `NamedTuple` default (`worker.py:134`) that no
implementation could make non-zero, so a bare `assert incomplete_swaps == 0` there passes against
every mutant; and in the mixed process of FR-002 AC-6 the worker legitimately increments it on an
unconfirmed drain, which this must not be read as forbidding.

#### Acceptance Criteria:

- [ ] AC-1: No code path added by this spec increments `incomplete_swaps` — including a close that
      raises, a close whose join expires, and a `Thread.start` that fails.
- [ ] AC-2: The test asserting this drives an orphan-path swap whose close hangs past the budget and
      **first asserts the scenario really occurred** (`closing_sinks` reached 1, `configure()` was
      bounded) before asserting `incomplete_swaps == 0`, so it is not a tautology over a default.
- [ ] AC-3: The mixed-process case is exempt and stated as such: `Worker.swap_sink` still increments
      on its own unconfirmed drain, and a test pins that it still does.
- [ ] AC-4: `Health`'s docstring states that `incomplete_swaps` describes the worker's drain and does
      not cover the orphan path.
- [ ] ~~AC-5: No field is added to `Health`.~~ — **superseded by SPEC-036 FR-003**, which
      appends `orphan_lost` and `in_span_lost`. This spec's own finding still needs no field:
      the orphan-path sink handoff reports through `incomplete_swaps`. What SPEC-036 adds is a
      different finding on the same path — the synchronous emit's own loss, which no existing
      field can carry, because every one of them describes a worker and this path has none.

### FR-007: Record the resolution

#### Description:

Per SPEC-021's rule, an open item is closed by being fixed, settled, or recorded — never deleted.
The §13 paragraph recording this variant is struck through in place and marked with the spec that
closed it, as SPEC-031 did to the paragraph above it. `configure()`'s docstring currently describes
one swap contract; there are two, and they differ in what they promise.

#### Acceptance Criteria:

- [ ] AC-1: The "One variant is not fixed and needs its own home" paragraph in `architecture.md` §13
      is struck through in place, marked **closed by SPEC-033**, with its reasoning left readable.
- [ ] AC-2: `architecture.md` §7 and §9 describe the swap as covering both delivery paths, and state
      that the close is shared while the swap protocol is not.
- [ ] AC-3: `configure()`'s swap paragraph is **split by path**. It cannot hold unchanged for a
      process with no worker: it promises a bounded drain and `health().incomplete_swaps` on an
      unconfirmed one, and FR-006 removes both from this path.
- [ ] AC-4: `architecture.md` §13 records the residuals this spec accepts rather than fixes —
      handing back an already-swapped-out sink closes it twice, symmetrically with the worker path
      (measured `A.closes=2`); and the orphan path's own emit can still block a shutdown-time close
      via the emit lock, now bounded by FR-004's stop signal rather than unbounded.
- [ ] AC-5: The record of the first residual states that `_orphan_closed_sink` is a **single slot**,
      which is what makes it reachable: the latch *moving* to a second sink re-admits the first. The
      non-obvious case is not the hand-back but an emit that resolved sink A, was preempted, and
      resumes after **two** swaps — it then finds the latch on B, re-arms A, and orphans the live
      sink. It sits under `configure()`'s documented "not thread-safe", and is worth naming because
      the single slot is what permits it.
- [ ] AC-7: The residual list also records the double-close the ownership guard newly admits: an
      orphan emit that resolved the old sink and arrives after `_swap_sink`'s worker branch cleared
      the record re-arms it, `Worker.swap_sink` closes that sink, and at exit the guard finds
      `_worker.sink` is the *new* sink and closes the old one again. It needs a concurrent emit
      during `configure()`, and `close()` is required idempotent — so it is recorded, not fixed, and
      it is a different shape from AC-5's orphaning race.
- [ ] AC-8: `sh scripts/spec-lint.sh` passes.

---

## Data Model

```python
# src/log_foundry/decorator.py — module state
_worker: Worker | None                # unchanged
_worker_lock: threading.Lock          # unchanged
_atexit_registered: bool              # unchanged
_orphan_sink: Sink | None             # REPLACES _orphan_close_owed: bool.
                                      #   the sink the orphan path owns the close of.
                                      #   read unlocked on the emit hot path (FR-001 AC-8);
                                      #   written only under _worker_lock.
_orphan_closed_sink: Sink | None      # REPLACES _orphan_sink_closed: bool.
                                      #   the most recently closed orphan-owned sink; refused
                                      #   re-arming, which is what makes the close once *per
                                      #   sink* rather than once per process.
_orphan_stop: threading.Event         # NEW (FR-004): the stop signal handed to a sink when no
                                      #   worker exists; set by _shutdown_worker on both branches.
_orphan_retired: bool                 # unchanged: synthesizes health().retired

# src/log_foundry/_lifecycle.py — new leaf module, process-global state
DEFAULT_CLOSER_GRACE: float = 2.0     # moved from worker.py
_closers: list[threading.Thread]
_closers_lock: threading.Lock
```

**The state transitions, which are the whole design:**

| Event | Guard | `_orphan_sink` | `_orphan_closed_sink` | Close performed |
|---|---|---|---|---|
| `_get_worker()` builds a worker | — | `None` | unchanged | none |
| orphan emit to `S` | `S is _orphan_sink` | unchanged | unchanged | none |
| orphan emit to `S` | `S is _orphan_closed_sink` | unchanged | unchanged | none (FR-001 AC-5) |
| orphan emit to `S` | otherwise | `S` | unchanged | none |
| `_swap_sink(N)` | worker exists, adopts | `None` | unchanged | `Worker.swap_sink` owns it |
| `_swap_sink(N)` | worker exists, **declines** | `N` | unchanged | orphan path, at exit (SPEC-035 FR-003) |
| `_swap_sink(N)` | declines, `N is _orphan_closed_sink` | unchanged | unchanged | none — no re-arm (SPEC-035 FR-003) |
| `_swap_sink(N)` | `_orphan_sink is None` | `None` | unchanged | none (FR-001 AC-3) |
| `_swap_sink(N)` | `_orphan_sink is N` | unchanged | unchanged | none (FR-002 AC-5) |
| `_swap_sink(N)` | otherwise | `N` | old | old, detached + bounded |
| `_close_orphan_sink()` | `_worker is not None and _worker.sink is _orphan_sink` | unchanged | unchanged | worker owns it |
| `_close_orphan_sink()` | `_orphan_sink is None` | `None` | unchanged | none |
| `_close_orphan_sink()` | otherwise | `None` | old | old, inline + unbounded |

The `_get_worker()` row is load-bearing rather than housekeeping: it is what lets
`_close_orphan_sink`'s guard be an **ownership** test without double-closing a live worker's sink.
The clear happens **after** `Worker(...)` returns, not before — a construction that raises would
otherwise drop the record and leave nothing to close the sink.

**The guard is `_worker.sink is _orphan_sink`, not `_worker is not None`** (FR-002 AC-12). "A worker
exists" and "a worker still owns this sink" stop being the same question the moment the worker is
retired: a retired worker owns nothing and never will, and `Worker.swap_sink` returns early on
`_shutdown_done`, so nothing anywhere closes what comes after it. The identity form still declines
in the case SPEC-031 FR-006 built the guard for — an **expired** `shutdown()` leaves the sink open
because the drain thread may still be inside `emit`, and there `_worker.sink is _orphan_sink` holds.

## API / Interface Contract

```python
# src/log_foundry/_lifecycle.py
def close_detached(sink: Sink) -> threading.Thread | None:
    """Starts a daemon close of a sink no longer delivered to; None if the thread could not start.

    Returns the thread rather than joining, so a caller holding a lock can start under it and
    join after releasing (FR-002 AC-11). `Worker._close_swapped_out` starts and joins in one
    call today, which is why this splits them.
    """

def join_closers(timeout: float | None) -> None:
    """Gives outstanding closes their last chance, capped at DEFAULT_CLOSER_GRACE."""

def closing_count() -> int:
    """Closes running at this instant — the gauge behind Health.closing_sinks."""

def offer_stop_signal(sink: Sink, stop: threading.Event) -> None:
    """Hands a sink an interruptible-wait signal if it advertises one (SPEC-027 FR-002)."""

# src/log_foundry/decorator.py
def _note_orphan_emit(sink: Sink) -> None: ...     # was _note_orphan_emit() -> None
def _swap_sink(new_sink: Sink, timeout: float | None = DEFAULT_SWAP_TIMEOUT) -> None: ...
                                                   # signature unchanged; gains the no-worker branch

# src/log_foundry/api.py — the one call site that changes
sink = _ensure_sink()
_note_orphan_emit(sink)
sink.emit([event])
```

`decorator._worker_health()`'s no-worker branch must fill `closing_sinks` from
`_lifecycle.closing_count()` rather than leave it at its `NamedTuple` default, or FR-003 AC-3 and
FR-005 AC-1 cannot hold. It is the second field synthesized without a worker, alongside `retired`,
and for the same reason: it describes the process, not the thread.

No public API changes. `configure`, `shutdown`, `flush` and `health` keep their signatures, and
`Health` keeps its fields.

## Configuration / Environment

None. No new config keys, env vars or settings; `DEFAULT_CLOSER_GRACE` and `DEFAULT_SWAP_TIMEOUT`
keep their values.

## File & Folder Structure

```
src/log_foundry/
├── _lifecycle.py        # NEW — shared closer registry, grace join, gauge, stop-signal probe
├── _diag.py             # unchanged
├── api.py               # one call site: pass the sink to _note_orphan_emit
├── config.py            # docstring only
├── decorator.py         # the two records, the stop event, the no-worker swap branch
└── worker.py            # lifecycle machinery delegates to _lifecycle

tests/
├── conftest.py                  # reset fixture covers both records and the stop event
├── test_config.py               # SPEC-030 closer tests re-pointed at _lifecycle
└── test_orphan_sink_handoff.py  # NEW — FR-001..FR-006
docs/
├── architecture.md                                    # §7, §9, §13
├── specs/{INDEX.md,SPEC-033-orphan-path-sink-handoff.md}
├── spec-delivery/SPEC-033-orphan-path-sink-handoff.md  # on completion
└── component-inventory.md                              # a row for _lifecycle.py
```

## Implementation Phases

### Phase 1: Extract the lifecycle facilities

- Create `_lifecycle.py` with the process-global registry, `close_detached`, `join_closers`,
  `closing_count` and `offer_stop_signal`, moving `DEFAULT_CLOSER_GRACE` and the guarded bodies out
  of `worker.py`.
- Point `Worker._close_swapped_out`, `Worker._join_closers`, `Worker._offer_stop_signal` and
  `Worker.health()` at it.
- Re-point the SPEC-030 closer tests in `test_config.py`; assert the patched grace takes effect
  (FR-005 AC-5) and diff `--collect-only` names.
- Green on `pytest`, `ruff`, `mypy` before touching behaviour.

### Phase 2: The two records

- `_orphan_close_owed` → `_orphan_sink`; `_orphan_sink_closed` → `_orphan_closed_sink`; thread the
  sink through `_note_orphan_emit` from `api._log`.
- Update `_close_orphan_sink` to the transition table, and `tests/conftest.py`'s reset fixture.
- Tests for FR-001, including the reassignment case (AC-2), the refused re-arm (AC-5) and the
  post-`shutdown()` reconfigure (AC-6).

### Phase 3: The swap branch and the mixed process

- Give `_swap_sink` its no-worker branch under `_worker_lock`: no-op checks, re-point, close via
  `_lifecycle.close_detached`.
- Tests for FR-002 — AC-3 (swap then shutdown with no second emit), AC-6 (mixed, both orders) and
  AC-10 (the first-`@trace` race) first, since they are the three most likely to be wrong.

### Phase 4: Bounding, the grace, and the stop signal

- Grant the grace join in `_shutdown_worker`, on all three paths of FR-003 AC-5.
- Arm and set `_orphan_stop`; wire the offer into the emit and the swap.
- Tests for FR-003, FR-004 and FR-006; mutation-test every new assertion (FR-002 AC-11).

### Phase 5: Documentation

- `architecture.md` §13 strike-through and the two recorded residuals; §7 and §9; `configure()`'s
  swap paragraph split by path.
