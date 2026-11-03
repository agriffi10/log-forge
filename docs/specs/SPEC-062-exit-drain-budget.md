---
type: Spec
id: SPEC-062
title: "The Exit Drain's Retries Fit the Caller's Budget"
description: "Makes Worker._emit decline a retry predicted to end after shutdown()'s deadline, so the join returns on time and the sink is closed rather than left open."
spec_status: In Progress
depends_on: [SPEC-027, SPEC-038, SPEC-047, SPEC-054]
---

## Overview

`shutdown(timeout)` is meant to return within its budget so that the sink is closed and any loss is
announced before a process exits. For a sink whose `emit` is slow and failing it does not, and the
cost lands on the two things an exiting process needs most. `Worker._emit` retries a failing batch
`max_retries + 1` times, and its wait between attempts is `self._stop.wait(...)`, which returns at
once because the stop event is set for the whole of `_final_drain`. So the exit drain costs up to
`max_retries + 1` times one bounded `emit`, and when that exceeds the join the join expires and the
sink is left open (SPEC-027 FR-004). The open item in `architecture.md` §12 ("`NATSSink`'s exit
drain can still exceed `shutdown()`'s join", SPEC-047 FR-001) names NATS because that is where it
was measured, but it is every sink's question: the retry loop is the worker's.

Every figure below is anchored to `71e42dd` and was measured there by the orchestrating session. The
probe is a sink whose `emit` waits 0.5 s on a fresh `threading.Event` and then raises
`RuntimeError`, installed with `configure(sink=s)`, one `@trace`d call carrying an `info()`, then
`shutdown(timeout=1.2)` at the default `max_retries=3`:

- `shutdown` returned at 1.21 s with CPU about 0.000 s, after **3** attempts, with `close()` called
  **0** times.
- Its stderr line read `lost 0 item(s); shutdown timed out after 1.2s; the sink is left open because
  the worker thread is still using it` — **0**, although three events were inside the emit being
  retried.
- After the thread finished: 4 attempts, `lost 3 event(s); batch abandoned after 4 emit attempts`,
  `failed_batches=1`, `stopped_reason='ShutdownTimeout'`, and `close()` **still 0**. Nothing closes
  the sink later in that process. In a process leaving through `atexit` the daemon thread dies with
  the interpreter, so those three events are never announced (invariant 2) and a sink with
  client-side buffering, NATS or Kafka, is never flushed by its close (invariant 5).

The same sink driven by `info()` outside any span, on a second thread, while `shutdown(timeout=1.2)`
runs, returned in 0.00 s after one attempt, with `close()` called once and `orphan_lost=1`. The orphan
path has no retry loop (`api.py` hands the one event to one `sink.emit([event])`), so it needs no
mechanism here.

**The change.** The caller's deadline is recorded on the worker before any stop event the worker
holds is set. `_emit` times each attempt and, before each retry, declines it when it is *predicted
to end after the deadline*: the batch's longest attempt so far, plus a small fixed join margin, would
pass it. A declined retry is counted and announced once, saying why. The one way the join can still
expire, an attempt longer than every earlier one, no longer hides the events inside it.

**What the budget does and does not buy.** It buys the join returning on time. The worker's own sink
is closed *inline on the calling thread* after the join returns (`_lifecycle._close_owed`, with
`_inline_close_choice` picking the worker's sink first), and every other owed close is joined with no
timeout (`closer.join()` in `_close_owed`, then `join_closers`). `closer_grace` bounds only a
bystander's wait on somebody else's close and a detached close. So `close()` runs whenever the join
returns, and how long it then takes past the timeout is invariant 3's existing recorded exception,
which this spec does not touch. The budget does **not** reserve time for the close; the join margin
reserves only the moments the drain thread needs, after its last attempt returns, to be seen to end.

**The join margin.** `_JOIN_MARGIN = 0.1` s, a module constant in `worker.py`. After the last attempt
returns the drain thread still counts the batch, writes the `_diag` line and answers the flush
markers, and `Thread.join` must observe the thread end inside the timeout. The margin is that window
and nothing more; it is not a close budget and is not a parameter.

**Reconciling the fence "A shutdown shortens a *wait*; it must never skip *work*"**
(`docs/decisions/sink-lifecycle/`). This spec does not break it, for four reasons. The budget never
shortens or cancels an attempt in flight and sets nothing a sink reads, so no sink does less inside
an attempt. The first attempt of every batch is always made. The only thing declined is the *start*
of a retry predicted to end after the caller's deadline; a retry predicted to end inside it is made,
which the fail-once case in FR-001 pins. And a retry predicted to end after the deadline is not work
the process gets: the join expires at the deadline, the sink is left open, and once `shutdown()`
returns the process exits (`atexit` kills the daemon thread) or is frozen. The one process that could
gain is one that keeps running after an expired `shutdown()`. It loses the late delivery of that
batch, and a retry whose real duration would have been shorter than the batch's longest attempt can
be declined though it would have fitted. The trade is taken because the alternative is a sink left
open for the life of the process (measured `close()` 0, above), and `shutdown()` is terminal.

**The decision to record at completion** (to be lifted into a new file in
`docs/decisions/sink-lifecycle/`, extending the fence above rather than reversing it): *the exit
drain makes every batch's first attempt, and declines a retry only when that batch's longest attempt
so far, plus the join margin, is predicted to end past the caller's `shutdown()` deadline; the
budget is the caller's `timeout`, so no knob is added, since one would let a caller recreate the
overrun.*

## Scope

### In Scope

- The deadline recorded on the worker on every route that sets a stop event it holds, and
  `Worker._emit` declining a retry against it (FR-001).
- The counting and announcement of a declined retry (FR-002).
- Proof that nothing else changes: no deadline, `flush()`, the swap (FR-003).
- The twin sites: a stop landing mid-sequence in `_drain`, a late worker, concurrent stoppers, and
  the orphan path (FR-004).
- The `ShutdownTimeout` line reporting the events in the batch being emitted (FR-005).
- The §12 item moved to Resolved, struck through and marked SPEC-062, never deleted, including its
  NATS-specific alternative "or by the caller lowering `publish_timeout` to 7.0".
- Invariant 3's *Guarded* line corrected: it says `flush()` and `shutdown()` have no CPU-time
  assertion, which is already stale (`tests/test_concurrent_owed_closes.py` asserts the CPU of a
  `shutdown()`), and it names both that test and this spec's.
- `Worker.__init__`'s `max_retries` docstring, and any statement of the retry count during shutdown
  found by grepping `max_retries` across `src/`, `README.md` and `docs/`, corrected.

### Out of Scope

- **Budgeting `flush()` or the sink swap (`Worker.retarget`).** A `flush()` leaves everything
  running, so a late answer is still delivery ("Two drains, deliberately distinct", pipeline), and an
  unfenced swap already leaves the old sink for a later `shutdown()` (SPEC-050 FR-004). Their
  behaviour is pinned unchanged by FR-003.
- **The orphan path.** It makes one attempt by construction (measured above).
- **A sink's own retry loop inside one `emit`.** `sinks/_retry.py` holds helpers only
  (`wait`, `clamp_server_delay`, `usable_timeout`, `require_timeout`, `require_positive`), no loop.
  The loops are in the sinks, `HTTPSink` per chunk among them (`sinks/http.py`, `for attempt in
  range(retries + 1)`), and such a loop **can** exceed one request's bound inside one `emit`, with
  its backoffs already cut by the stop event. The budget measures a whole `emit` as one attempt, so
  a long emit raises the next retry's prediction, but the emit itself stays the sink's bound
  (invariant 3's recorded exception).
- **`Sink.close()` being unbounded** (invariant 3's recorded exception), and reserving time for it.
- **The `queued` figure on the expiry line counting the `_SHUTDOWN` sentinel and flush markers while
  they sit in the queue.** That is existing behaviour of `_queued_or_unknown`, not touched here.
- **Invariant 2's span ledger stating no term for a failed batch.** The page is left alone; wording
  it is a docs question for completion (FR-002 states the ledger it tests).
- **Any public API change.** No new parameter on `configure()`, `shutdown()`, `Worker` or any sink.
  The budget is the caller's `timeout`; a knob would let a caller recreate the overrun.

---

## Functional Requirements

### FR-001: The exit drain's retries fit the caller's budget

#### Description:

**Recording the deadline.** The caller's deadline is a `time.monotonic()` instant, `None` for
`timeout=None`, held on the worker and recorded **before any stop event the worker holds is set, on
every route that sets one**:

- `_lifecycle._shutdown_worker` sets `_state._stop` under `_state._lock` *before* it calls
  `worker.stop(timeout)`, and a live worker's `_stop` is that same object. So it reads the worker
  first, claims its stop through `Worker.claim_stop(deadline)`, then sets `_state._stop`, all
  inside the one critical section that moves the retirement count, so SPEC-044 FR-001's "the
  count and the worker read are one critical section" is kept. It passes the claim's verdict to
  `worker.stop(timeout, claimed=...)`.
- `Worker.stop` called without a verdict claims for itself before its own `self._stop.set()`.
  This covers a late worker (`late_worker.stop(remaining)`), which holds a different event than
  `_state._stop`, and a direct caller.

**The claim and the record are one decision.** `Worker.claim_stop(deadline) -> bool` sets the
once-only `_stopped` gate and records the deadline only if it wins, under `Worker._lock`, and
calls nothing while holding it. A caller that won does the first caller's work in `stop` —
sentinel, set, join against **its own** timeout, expiry; a caller that lost does the second
caller's wait, also against its own timeout, so each `shutdown()` still returns within what it
asked for. The record is therefore **write-once** without a second flag: a losing claim writes
nothing, and the deadline the drain declines against is always that of the caller whose join
follows. Deciding the gate in one critical section and recording in another was measured (plan
review, 1 run in 60) handing the 0.9 s caller the join while the drain declined against 1.8 s:
that join expired, the 1.8 s caller returned as second, and `close()` was never called. The
nesting `_state._lock` then `Worker._lock` is added; `worker.py` takes `_state._lock` nowhere
(grep at `71e42dd`), so there is no reverse nesting.

**The rule.** `_emit` times each attempt (monotonic around `sink.emit` only, never the backoff) and,
**immediately before each retry attempt and after its backoff wait has returned**, declines that
retry when

    now + longest attempt observed for this batch + _JOIN_MARGIN > deadline

The **first attempt of a batch is never declined**, whatever the deadline. A deadline of `None`
declines nothing. "Longest" is per batch: another batch's attempts do not enter it.

**Worked case, NATS at its defaults** (`publish_timeout=10`, `max_retries=3`, join 30). Attempts run
at 0 s and 10 s. Before the third, `20 + 10 + 0.1 > 30`, so it is declined at 20 s, the drain ends
there and the close runs. Today that third attempt ends exactly at the join's deadline, so the join
expires or does not on a race, and the fourth would end at 40 s.

**The rule's prediction.** For a sink whose `emit` takes D and always fails, attempt `j` starts at
about `(j - 1)·D` and is made when `j·D + 0.1 <= T`, up to `max_retries + 1` attempts and always at
least one.

**Choosing timings in tests.** Overrun in a measured attempt only pushes towards declining, so a
test must give headroom to every decision that has to *permit* a retry and little to one that has to
decline. A decision made with `k` attempts completed must permit by at least `50 ms × k`, which
absorbs a loaded `-n 12` run. A decision that must decline needs at least 50 ms of arithmetic
margin, because overrun only helps it. The pairs below satisfy this.

#### Acceptance Criteria:

- [ ] Always-failing sink, D=0.5 s, `shutdown(timeout=1.2)`: the sink sees exactly **2** attempts
      (the third is predicted to end at 1.6 s), `shutdown` returns before 1.2 s plus 0.3 s of slack,
      `stopped_reason` is not `"ShutdownTimeout"`, and `close()` was called exactly once by the time
      it returned. At `71e42dd` the same sink makes 3 attempts and the join expires.
- [ ] Same sink, D=0.5 s, `shutdown(timeout=0.9)`: exactly **1** attempt and `close()` once.
- [ ] Same sink, D=0.5 s, `shutdown(timeout=1.8)`: exactly **3** attempts and `close()` once.
- [ ] **A retry that fits is made.** A sink that fails its first `emit` (0.5 s) and succeeds on its
      second, `shutdown(timeout=1.2)`: all 3 events are delivered, `failed_batches == 0`, no `lost`
      line is written, and `close()` is called once, by about 1.0 s. At `71e42dd` this delivers and
      closes at 1.00 s, so a rule that declines it breaks invariant 2.
- [ ] A sink whose failures are fast (D about 0) under `shutdown(timeout=5.0)` still receives all
      `max_retries + 1` attempts: the budget costs nothing when it is ample.
- [ ] With `shutdown(timeout=0)` and an always-failing sink (D=0.05 s), exactly 1 attempt is made and
      no retry.
- [ ] **The estimator is the longest attempt.** A sink whose attempts take 0.1 s, 0.8 s, 0.3 s and
      then fail the rest, `shutdown(timeout=1.95)`: exactly **3** attempts. Before the fourth,
      `1.2 + 0.8 + 0.1 > 1.95` declines it, whereas the last attempt (0.3), the first (0.1) and the
      mean would each permit it.
- [ ] Stand-in for the NATS case at one tenth scale, margin unscaled (`emit` takes 1.0 s,
      `max_retries=3`, `shutdown(timeout=3.0)`): exactly 2 attempts, the third declined at
      `2.0 + 1.0 + 0.1 > 3.0`, and the drain ends at about 2.0 s.
- [ ] CPU time over each `shutdown()` call above (`time.process_time()` delta, run serially) is
      under 0.05 s, so a busy-spin could not pass (working rule "A bound is only a bound if it is
      measured where it binds").
- [ ] **Order on the real path.** With the worker's event wrapped so the first `set` records what it
      sees, a public `log_foundry.shutdown(timeout=T)` finds the worker's deadline already recorded
      (not `None`) when `_state._stop.set()` runs. A direct `Worker.stop(T)` finds it recorded when
      its own `set` runs.
- [ ] The deadline is write-once: a second `Worker.stop` or `_shutdown_worker` with a later deadline
      leaves the first recorded value unchanged.
- [ ] Serves invariants 3 and 5: the wait is bounded by the caller's timeout, and the join returning
      on time is what lets the sink be closed rather than left open.

### FR-002: A declined retry is counted and announced once, saying why

#### Description:

A batch whose retry is declined counts once in `failed_batches`, and `_diag.lost("event", n, ...)`
announces it with a reason distinct from the exhausted-attempts wording: it names how many attempts
were made and that another was predicted to end after the shutdown's deadline. A `flush()` marker
answered by that drain reports falsy through `_nothing_lost_since`, because `failed_batches` moved.
The abandon line the exhausted path writes is not written as well.

The ledger these criteria test: with spans that emit only their `span.end` event, **delivered
`span.end` + `queued` + `dropped` + events announced lost by the abandon or decline line = spans
started**. Invariant 2's span ledger states no failed-batch term, and
`tests/test_invariants_model.py` balances without one only because `failed_batches == 0` there.

#### Acceptance Criteria:

- [ ] After the D=0.5 s, `shutdown(timeout=1.2)` case, `failed_batches == 1` and stderr carries
      exactly one `lost 3 event(s)` line, which states 2 attempts made and the shutdown's deadline,
      and does not contain `abandoned after`.
- [ ] After a case that exhausts its attempts inside the budget (fast failure, `timeout=5.0`) the line
      is the existing `batch abandoned after 4 emit attempts`, byte for byte, and does not mention
      the deadline.
- [ ] `failed_batches` and the number of `lost` lines are each 1 per batch whichever way the batch
      ends, and the ledger above balances for both endings.
- [ ] **The marker's answer.** A `flush()` is queued behind the events before the stop and taken by
      `_drain`; the stop then lands mid-sequence (FR-004's ordering) and the retry is declined. The
      `flush()` returns `delivered` falsy with `reason == "abandoned"`. The test fails against a
      mutant in which the declined retry does not move `failed_batches` (the marker then reads
      delivered).
- [ ] The line names a type and counts only, never a value from an event or an exception message.
- [ ] Serves invariants 2 and 11.

### FR-003: No deadline, no change

#### Description:

Only a claimed stop records a deadline — `Worker.claim_stop`, reached from `_shutdown_worker`
and `Worker.stop` — so every other drain is unaffected:
`shutdown(timeout=None)`, ordinary draining with no stop requested, `flush(timeout)` and a sink swap
(`Worker.retarget`) retry and back off exactly as before. `flush()` and the swap are deliberately
not budgeted (see Out of Scope), which this requirement pins rather than leaves implicit.

#### Acceptance Criteria:

- [ ] Against an always-failing sink with D=0.2 s, `shutdown(timeout=None)` makes exactly
      `max_retries + 1` attempts and writes the existing `abandoned after N emit attempts` line.
- [ ] Ordinary draining (no stop requested) of an always-failing sink makes exactly
      `max_retries + 1` attempts per batch, and the recorded deadline stays `None` throughout.
- [ ] `flush(timeout=0.5)` against an always-failing sink with D=0.4 s returns on its own timeout as
      before, the drain continues retrying after it returns, and the batch makes `max_retries + 1`
      attempts.
- [ ] A swap through `Worker.retarget` while the old sink is mid-retry does not decline any retry of
      that batch, and leaves the old sink for a later `shutdown()` as SPEC-050 FR-004 has it.
- [ ] Serves invariant 3, and the sink-lifecycle fence that a shutdown shortens a wait and never
      skips work: no sink's `emit` is called fewer times than before on any path with no recorded
      deadline.

### FR-004: The budget binds wherever the stop lands

#### Description:

One retry site serves both `_drain` and `_final_drain`, so the rule holds when the stop arrives while
a retry sequence is already under way in `_drain`, and for a late worker (`_shutdown_worker` calls
`late_worker.stop(remaining)`; its deadline is the one `Worker.stop` claims, before it sets the
late worker's own event). Two concurrent `shutdown()` callers share one worker: the deadline that
governs is the one recorded by the caller that won the once-only gate, because the gate and the
record are one decision (FR-001); that caller's join follows, so the drain ends inside the join it
is measured against and `close()` runs once on every interleaving, while the caller that lost
waits against its own timeout. The orphan path keeps its single attempt and needs no mechanism.

**The residual, stated rather than hidden.** A backoff cut short by the stop always sees the
deadline, because it was recorded before the event was set. A retry check that read no deadline
because its backoff ended naturally just before the record costs at most one attempt, which
invariant 3's observable already allows ("plus the sink's one in-flight call"). Likewise the first
attempt of the final batch is made whatever the deadline, so a stop landing during a slow in-flight
batch with a non-empty tail behind it can cost that one further emit. Both are bounded by one emit
and neither is new.

#### Acceptance Criteria:

- [ ] **Stop mid-sequence in `_drain`.** `batch_size=1`, nothing queued behind the batch. A sink whose
      `emit` blocks on an event the test controls and then fails. `shutdown(timeout=0.5)` is called
      0.1 s after attempt 1 starts, and the event is released 0.2 s after the record is observed
      through FR-001's hook (t_rec + 0.2), never on wall time from the call: with δ the latency
      from the call to the record, attempt 1 has then run for at least `0.3 + δ`, the prediction
      is at least `0.6 + 2δ` past the call against a deadline 0.5 past it, so the decline margin is
      at least 0.1 s and grows with thread-start latency under `-n 12`. Exactly 1 attempt is made,
      `shutdown` returns within 0.5 s plus slack, and `close()` is called once.
- [ ] The same ordering with `shutdown(timeout=30.0)` makes all `max_retries + 1` attempts.
- [ ] A late worker (built during a `shutdown()`, stopped by `late_worker.stop(remaining)`) has its
      deadline recorded before its own event is set, declines against the remaining budget it was
      given, and the overall `shutdown` returns within the original T plus slack.
- [ ] Two concurrent `shutdown()` calls through `tests/conftest.py::run_concurrently`, with different
      timeouts: the attempts made match the deadline the worker recorded (asserted against that
      recorded value, not against "the first caller"), both calls return, the recording caller's
      join does not expire, and `close()` is called once — on every interleaving, because the
      recording caller is the joining caller.
- [ ] A `run_concurrently` run of `shutdown()`, `flush()` and `health()` completes with no thread
      left blocked past a bound, so the new lock nesting deadlocks nowhere.
- [ ] One test drives both delivery paths, an in-span `info()` through the worker and an out-of-span
      `info()` through the orphan path, against the same always-failing slow sink under one
      `shutdown(timeout=T)`: each ends within T plus slack, the orphan event is one attempt, and the
      ledger balances: delivered + `orphan_lost` equals the orphan calls made, and FR-002's ledger
      holds for the spans.
- [ ] Serves invariants 6 and 3: the twins are the `_drain` loop and `_final_drain`, the worker and
      orphan paths, and the late and ordinary worker.

### FR-005: The expiry line counts what is still in flight

#### Description:

When the join still expires (an attempt longer than every earlier one, or an emit wedged past its
own bound), the `ShutdownTimeout` line must not report `lost 0 item(s)` while a batch is inside
`_emit`. The worker tracks the size of the batch `_emit` is holding, from before its first attempt
until `_emit` returns, delivered, abandoned or declined. The line keeps its queued-items count as
today and adds, separately, the number of events in that batch, worded as *in flight, counted when
the emit returns*. They are not reported as lost: their future is still open (a late return can
still deliver them), and counting them now would double-count when the abandon line fires.

#### Acceptance Criteria:

- [ ] A sink wedged inside its first `emit` of a 3-event batch **in `_final_drain`** (`batch_size`
      above 3 and a long `flush_interval`, so nothing emits before the stop), `shutdown(timeout=0.5)`:
      the line still reads `lost 0 item(s)` for the queue, and also states that 3 events are in
      flight and will be counted when the emit returns.
- [ ] Releasing the sink so the emit fails: the retry is declined (FR-002's line), `lost 3 event(s)`
      is written once, `failed_batches == 1`, and the *lost* figures across the expiry line and that
      line sum to 3 events, not 6.
- [ ] Releasing the sink so the emit succeeds: no loss line follows and `failed_batches == 0`.
- [ ] When nothing is in flight at expiry the line is unchanged from `71e42dd` byte for byte.
- [ ] The in-flight size is zero again after `_emit` returns by every route (delivered, abandoned,
      declined), including an exception that escapes it, and after `_reinit_after_fork`.
- [ ] Serves invariants 2 and 11: every count a line carries reproduces from the ledger.

---

## Data Model

```
# src/log_foundry/worker.py
_JOIN_MARGIN: float = 0.1             # module constant: the drain thread's moments after its last attempt
Worker._exit_deadline: float | None   # monotonic instant; written only by claim_stop, in the
                                      # decision that wins the _stopped gate, under Worker._lock;
                                      # reached from _shutdown_worker (live worker, first critical
                                      # section) and Worker.stop (direct and late-worker routes),
                                      # before any stop event the worker holds is set
Worker.claim_stop(deadline) -> bool   # the gate and the record as one decision; True = this caller
                                      # does the first caller's work in stop()
Worker.stop(timeout, *, claimed=None) # claimed: a verdict already taken under _state._lock, or
                                      # None to claim here
Worker._in_flight: int                # events in the batch _emit is holding; 0 when none
```

`_exit_deadline` is reset on the **resume** branch of `_reinit_after_fork` and left alone on the
retired one: a worker stopped by a direct `Worker.stop()` never moved the retirement count, so
`_lifecycle._rebuild_worker_after_fork` still resumes it, and without the reset every retry in that
child would be declined against the parent's deadline forever; a retired parent forks a retired
child, whose record nothing reads. `_in_flight` is reset on both branches beside the other
per-drain fields (`dropped`, `failed_batches`, `stopped_reason`, `_taken_markers`), because a child
inherits the parent's value for a drain thread that does not exist in it (invariant 12). It is
written by the drain thread and read without the lock by the stopper, as `draining` is read: a single
assignment, so a reader sees one of two answers, both true at the instant.

## API / Interface Contract

No public change. `shutdown(timeout)`, `configure()` and `Worker(...)` keep their signatures.
`Worker.__init__`'s `max_retries` docstring gains: on the exit drain a retry is declined when it is
predicted to end after the `shutdown()` deadline.

## File & Folder Structure

```
src/log_foundry/worker.py                 # _JOIN_MARGIN, stop(), _emit(), the ShutdownTimeout line, docstrings
src/log_foundry/_lifecycle.py             # _shutdown_worker: claim_stop before _state._stop.set()
tests/test_worker_exit_budget.py          # new: FR-001 through FR-005
docs/architecture.md                      # §12 item moved to Resolved
docs/invariants.md                        # invariant 3 Guarded line
```

## Implementation Phases

### Phase 1: The budget and its announcement (FR-001, FR-002)

- Record `_exit_deadline` on both routes; time attempts and decline a retry in `_emit`.
- Count and announce a declined retry; tests with the D and T pairs above, asserting CPU.

### Phase 2: The twins and the unchanged paths (FR-003, FR-004)

- Pin the no-deadline paths; drive the mid-`_drain` stop with an injected preemption point.
- Late worker, concurrent stoppers, and the two-path ledger test.

### Phase 3: The in-flight count and the records (FR-005)

- Track `_in_flight`; amend the expiry line.
- Move the §12 item to Resolved, correct invariant 3's *Guarded* line, correct the `max_retries`
  docstring and any retry-count statement the grep finds.
