---
type: Spec
id: SPEC-062
title: "The Exit Drain's Retries Fit the Caller's Budget"
description: "Makes Worker._emit decline a retry that cannot finish inside shutdown()'s timeout, so the exit drain ends in time for the sink to be closed and the loss to be announced."
spec_status: Draft
depends_on: [SPEC-027, SPEC-038, SPEC-047, SPEC-054]
---

## Overview

`shutdown(timeout)` promises to return within its budget so that the sink can be closed and any loss
announced before a process exits. For a sink whose `emit` is slow and failing it does not keep that
promise, and the cost lands on the two things an exiting process needs most. `Worker._emit` retries
a failing batch `max_retries + 1` times, and its wait between attempts is `self._stop.wait(...)`,
which returns at once because the stop event is set for the whole of `_final_drain`. So the exit
drain costs up to `(max_retries + 1)` times one bounded `emit`, and when that exceeds the join the
join expires and the sink is left open (SPEC-027 FR-004). The open item in `architecture.md` §12
("`NATSSink`'s exit drain can still exceed `shutdown()`'s join", SPEC-047 FR-001) names NATS because
that is where it was measured, but it is every sink's question: the retry loop is the worker's.

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

**The change.** `Worker.stop` records the caller's deadline on the worker. `_emit` times each
attempt and, before each retry, declines it when the attempt's own measured cost says it cannot
finish with the close's grace to spare. A declined retry is counted and announced once, saying why.
The one remaining way the join can still expire, an attempt longer than every earlier one, no longer
hides the events inside it.

**Reconciling the fence "A shutdown shortens a *wait*; it must never skip *work*"**
(`docs/decisions/sink-lifecycle/`). This spec does not break it, for four reasons. The budget never
shortens or cancels an attempt in flight and sets nothing a sink reads, so no sink does less inside
an attempt. The first attempt of every batch is always made. The only thing declined is the *start*
of a retry that the batch's own measured cost says cannot end before the caller's budget does. And
that retry is not work a serverless process gets: once `shutdown()` returns, the process exits
(`atexit` kills the daemon thread) or is frozen. What the overrun does cost is work that process
needs, the close, which flushes a buffered client, and the announcement of the loss. The one process
that could gain from the overrun is one that keeps running after an expired `shutdown()`. It loses
the late delivery of that batch, and the trade is taken because the alternative is a sink left open
for the life of the process (measured `close()` 0, above) and `shutdown()` is terminal.

**The decision to record at completion** (to be lifted into a new file in
`docs/decisions/sink-lifecycle/`, extending the fence above rather than reversing it): *the exit
drain makes every batch's first attempt, and declines a retry only when that batch's longest attempt
so far plus the close's reserve would end past the caller's `shutdown()` deadline; the budget is the
caller's `timeout`, so no knob is added, since one would let a caller recreate the overrun.*

## Scope

### In Scope

- `Worker.stop` recording its deadline, and `Worker._emit` declining a retry against it (FR-001).
- The counting and announcement of a declined retry (FR-002).
- Proof that nothing else changes: no deadline, `flush()`, the swap (FR-003).
- The twin sites: a stop landing mid-sequence in `_drain`, a late worker, concurrent stoppers, and
  the orphan path (FR-004).
- The `ShutdownTimeout` line reporting the events in the batch being emitted (FR-005).
- The §12 item moved to Resolved, struck through and marked SPEC-062, never deleted, including its
  NATS-specific alternative "or by the caller lowering `publish_timeout` to 7.0". Invariant 3's
  *Guarded* line, which says `flush()` and `shutdown()` have no CPU-time assertion, updated once a
  test asserts one. `Worker.__init__`'s `max_retries` docstring, and any statement of the retry
  count during shutdown found by grepping `max_retries` across `src/`, `README.md` and `docs/`,
  corrected.

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
- **`Sink.close()` being unbounded** (invariant 3's recorded exception). The reserve is the existing
  `DEFAULT_CLOSER_GRACE`, not a new bound on the close.
- **Any public API change.** No new parameter on `configure()`, `shutdown()`, `Worker` or any sink.
  The budget is the caller's `timeout`; a knob would let a caller recreate the overrun.

---

## Functional Requirements

### FR-001: The exit drain's retries fit the caller's budget

#### Description:

`Worker.stop(timeout)` records its deadline on the worker, as a `time.monotonic()` instant and
`None` for `timeout=None`, inside the once-only critical section that already decides `first` and
strictly before `self._stop.set()`. `_emit` times each attempt (monotonic around `sink.emit` only,
never the backoff) and, **immediately before each retry attempt and after its backoff wait has
returned**, declines that retry when

    now + longest attempt observed for this batch + reserve > deadline

where `reserve = min(_lifecycle.DEFAULT_CLOSER_GRACE, max(timeout, 0) / 2)`. `DEFAULT_CLOSER_GRACE`
is 2.0 s at `71e42dd`. The reserve keeps the close's grace (`_lifecycle.closer_grace` and
`join_closers`, carved from the same deadline in `_lifecycle.shutdown`) inside the budget. The half
cap stops a short budget from declining every retry of a fast failure. The **first attempt of a
batch is never declined**, whatever the deadline. A deadline of `None` declines nothing.

Worked case, NATS at its defaults (`publish_timeout=10`, `max_retries=3`, join 30). Attempts run at
0 s and 10 s. Before the third, `20 + 10 + 2 > 30`, so it is declined; the drain ends at about 20 s
and the close gets its 2 s. Today the third attempt ends exactly at the 30 s join, the join expires
and the sink stays open.

The rule's prediction for a sink whose `emit` takes D and always fails, with reserve `r`: attempt
`k + 1` starts at about `k·D` and is made when `(k + 1)·D + r <= T`, up to `max_retries + 1`
attempts, and always at least one. Two instances: D=0.5 s, T=1.2 s (so `r=0.6`) makes **1** attempt,
where `71e42dd` makes 3 within the join; D=0.3 s, T=2.0 s (`r=1.0`) makes **3** attempts, where the
fourth would end at 1.2 s but its start is declined at `0.9 + 0.3 + 1.0 > 2.0`. Tests pick D and T
away from the boundary, with at least 0.1 s of margin on both sides of the inequality.

#### Acceptance Criteria:

- [ ] With an always-failing sink whose `emit` takes D=0.5 s, `shutdown(timeout=1.2)` returns within
      1.2 s plus 0.3 s of slack, `close()` has been called exactly once before it returns,
      `stopped_reason` is not `"ShutdownTimeout"`, and the sink saw exactly 1 attempt.
- [ ] With D=0.3 s and `shutdown(timeout=2.0)` the sink sees exactly 3 attempts and `close()` is
      called once before `shutdown` returns.
- [ ] A sink whose failures are fast (D about 0) under `shutdown(timeout=5.0)` still receives all
      `max_retries + 1` attempts: the budget costs nothing when it is ample.
- [ ] With `shutdown(timeout=0)` and an always-failing sink, the first attempt is made (exactly 1
      attempt) and no retry is made.
- [ ] CPU time over each `shutdown()` call above (`time.process_time()` delta, run serially) is
      under 0.05 s, so a busy-spin could not pass (working rule "A bound is only a bound if it is
      measured where it binds").
- [ ] The deadline is written before the stop event is set: a test that wraps the event's `set`
      reads the recorded deadline from inside it and finds it not `None` for a finite timeout.
- [ ] A stand-in for the worked NATS case that keeps the reserve at 2.0 s (`emit` takes 1.0 s,
      `shutdown(timeout=4.5)`, `max_retries=3`): the sink sees exactly 2 attempts, since the third
      is declined at `2.0 + 1.0 + 2.0 > 4.5`, and the drain ends at about 2.0 s.
- [ ] Serves invariants 3 and 5: the wait is bounded by the caller's timeout, and the sink was closed
      by the shutdown rather than left open.

### FR-002: A declined retry is counted and announced once, saying why

#### Description:

A batch whose retry is declined counts once in `failed_batches`, and `_diag.lost("event", n, ...)`
announces it with a reason distinct from the exhausted-attempts wording: it names how many attempts
were made and that another could not finish within the shutdown's budget. A `flush()` marker
answered by that drain reports falsy through `_nothing_lost_since`, because `failed_batches` moved.
The abandon line the exhausted path writes is not written as well.

#### Acceptance Criteria:

- [ ] After the D=0.5 s, `timeout=1.2` case, `failed_batches == 1` and stderr carries exactly one
      `lost 3 event(s)` line, which names `1` attempt made and the shutdown's budget, and does not
      contain `abandoned after`.
- [ ] After a case that exhausts its attempts inside the budget (fast failure, ample timeout) the
      line is the existing `batch abandoned after 4 emit attempts`, byte for byte, and does not
      mention the budget.
- [ ] `failed_batches` and the number of `lost` lines are each 1 per batch whichever way the batch
      ends, so the ledger balances: delivered + `queued` + `dropped` equals spans started.
- [ ] A `flush()` racing the `shutdown()` and answered by the final drain returns a falsy result
      when the final drain declined a retry.
- [ ] The line names a type and counts only, never a value from the event or the exception message.
- [ ] Serves invariants 2 and 11.

### FR-003: No deadline, no change

#### Description:

Only `Worker.stop` records a deadline, so every other drain is unaffected: `shutdown(timeout=None)`,
ordinary draining with no stop requested, `flush(timeout)` and a sink swap (`Worker.retarget`) retry
and back off exactly as before. `flush()` and the swap are deliberately not budgeted (see Out of
Scope), which this requirement pins rather than leaves implicit.

#### Acceptance Criteria:

- [ ] Against an always-failing sink with D=0.2 s, `shutdown(timeout=None)` makes exactly
      `max_retries + 1` attempts and writes the existing `abandoned after N emit attempts` line.
- [ ] Ordinary draining (no stop requested) of an always-failing sink makes exactly
      `max_retries + 1` attempts per batch, and `Worker._exit_deadline` (or the attribute the
      implementation names for it) stays `None` throughout.
- [ ] `flush(timeout=0.5)` against an always-failing sink with D=0.4 s returns on its own timeout
      as before, the drain continues retrying after it returns, and the batch makes
      `max_retries + 1` attempts.
- [ ] A swap through `Worker.retarget` while the old sink is mid-retry does not decline any retry
      of that batch, and leaves the old sink for a later `shutdown()` as SPEC-050 FR-004 has it.
- [ ] Serves invariant 3, and the sink-lifecycle fence that a shutdown shortens a wait and never
      skips work: no sink's `emit` is called fewer times than before on any path that has no
      recorded deadline.

### FR-004: The budget binds wherever the stop lands

#### Description:

One retry site serves both `_drain` and `_final_drain`, so the rule holds when the stop arrives
while a retry sequence is already under way in `_drain`, and for a late worker (`_lifecycle.shutdown`
calls `late_worker.stop(remaining)`; the deadline is the one `stop` receives). Two concurrent
`shutdown()` callers share one worker: the first caller's deadline governs, because `_stopped` is
once-only. The orphan path keeps its single attempt and needs no mechanism.

**The residual, stated rather than hidden.** A retry decision that read no deadline just before the
stop recorded one costs at most one attempt, which invariant 3's observable already allows ("plus
the sink's one in-flight call"). Likewise the first attempt of the final batch is made whatever the
deadline, so a stop landing during a slow in-flight batch that has a non-empty tail behind it can
cost that one further emit. Both are bounded by one emit and neither is new.

#### Acceptance Criteria:

- [ ] Stop landing mid-sequence in `_drain`: a sink whose `emit` blocks on an event the test
      controls and fails on release, the batch under way in `_drain` with attempt 1 in flight,
      `shutdown(timeout=T)` called from another thread, the event released only after the stop has
      recorded its deadline (observed through the FR-001 hook). The batch makes no retry the rule
      declines, `shutdown` returns within T plus slack, and `close()` is called once.
- [ ] The same ordering with a deadline that is ample makes all `max_retries + 1` attempts.
- [ ] A late worker (built during a `shutdown()`, stopped by `late_worker.stop(remaining)`) declines
      against the remaining budget it was given, and the overall `shutdown` returns within the
      original T plus slack.
- [ ] Two concurrent `shutdown()` calls through `tests/conftest.py::run_concurrently`, with different
      timeouts: the attempts made match the first caller's deadline, both calls return, and `close()`
      is called once.
- [ ] One test drives both delivery paths (an in-span `info()` through the worker and an
      out-of-span `info()` through the orphan path) against the same always-failing slow sink under
      one `shutdown(timeout=T)`: each ends within T plus slack, the orphan event is one attempt, and
      the ledger balances, with `orphan_lost` plus `failed_batches`-counted events equal to the
      events emitted.
- [ ] Serves invariants 6 and 3: the twins are the `_drain` loop and `_final_drain`, the worker and
      orphan paths, and the late and ordinary worker.

### FR-005: The expiry line counts what is still in flight

#### Description:

When the join still expires (an attempt longer than every earlier one, or an emit wedged past its
own bound), the `ShutdownTimeout` line must not report `lost 0 item(s)` while a batch is inside
`_emit`. The worker tracks the size of the batch `_emit` is holding, from before its first attempt
until `_emit` returns, delivered or abandoned. The line keeps its queued-items count as today and
adds, separately, the number of events in that batch, worded as *in flight, counted when the emit
returns*. They are not reported as lost: their future is still open (a late return can still
deliver them), and counting them now would double-count when the abandon line fires.

#### Acceptance Criteria:

- [ ] A sink wedged inside its first `emit` of a 3-event batch, `shutdown(timeout=0.5)`: the line
      still reads `lost 0 item(s)` for the queue, and also states that 3 events are in flight and
      will be counted when the emit returns.
- [ ] Releasing the sink so the emit fails and the retries run out: the abandon line reads
      `lost 3 event(s)`, `failed_batches == 1`, and the sum of the two lines' *lost* figures is
      3 events, not 6.
- [ ] Releasing the sink so the emit succeeds: no loss line follows and `failed_batches == 0`.
- [ ] When nothing is in flight at expiry the line is unchanged from `71e42dd` byte for byte.
- [ ] The in-flight size is zero again after `_emit` returns by every route (delivered, abandoned,
      declined), including an exception that escapes it.
- [ ] Serves invariants 2 and 11: every count a line carries reproduces from the ledger.

---

## Data Model

```
# src/log_foundry/worker.py
Worker._exit_deadline: float | None   # monotonic instant; set only by stop(), before _stop.set()
Worker._in_flight: int                # events in the batch _emit is holding; 0 when none
```

Both are written by one thread at a time (`stop`'s first caller, the drain thread) and read without
the lock, as `draining` is; each is a single assignment, and a reader sees one of two answers, both
true at the instant.

## API / Interface Contract

No public change. `shutdown(timeout)`, `configure()` and `Worker(...)` keep their signatures.
`Worker.__init__`'s `max_retries` docstring gains: on the exit drain a retry is declined when it
cannot finish inside the `shutdown()` budget.

## File & Folder Structure

```
src/log_foundry/worker.py                 # stop(), _emit(), the ShutdownTimeout line, docstrings
tests/test_worker_exit_budget.py          # new: FR-001 through FR-005
docs/architecture.md                      # §12 item moved to Resolved
docs/invariants.md                        # invariant 3 Guarded line
```

## Implementation Phases

### Phase 1: The budget and its announcement (FR-001, FR-002)

- Record `_exit_deadline` in `stop`; time attempts and decline a retry in `_emit`.
- Count and announce a declined retry; tests with the D and T pairs above, asserting CPU.

### Phase 2: The twins and the unchanged paths (FR-003, FR-004)

- Pin the no-deadline paths; drive the mid-`_drain` stop with an injected preemption point.
- Late worker, concurrent stoppers, and the two-path ledger test.

### Phase 3: The in-flight count and the records (FR-005)

- Track `_in_flight`; amend the expiry line.
- Move the §12 item to Resolved, update invariant 3's *Guarded* line, correct the `max_retries`
  docstring and any retry-count statement the grep finds.
