---
type: Spec
id: SPEC-068
title: "Shutdown-Expiry Bookkeeping: One Announcement per Queued Item, an Expiry That Is Final, a Shutdown From the Drain Thread, a Fork That Resets the Stop Gate"
description: "Stops an expired shutdown() calling queued items lost, records that an expired shutdown is final, makes a shutdown() on the drain thread return without raising or waiting on itself, and gives a resumed forked child its own stop gate and stop event."
spec_status: Draft
depends_on: [SPEC-062]
---

## Overview

SPEC-062 made the exit drain's retries fit the caller's `shutdown()` budget and left three items
in `architecture.md` §12 that share one moment: the join has expired, the drain thread is still
inside the sink, and the library has to say and do the right thing about what happens next. Each
is a lie or a leak at that moment. Every figure below was measured at `fda134a` with probes run
from this worktree.

**Queued items are announced as lost and then announced again.** The expiry line is written with
`_diag.lost("item", queued, ...)`, so it calls the queue lost. When the stuck emit returns, the
drain takes those same items and, if their batch is abandoned or its retry declined, `_emit` writes
a second `lost` line for them. Measured: one 1-event span wedged in the sink and two more spans
(3 events) queued behind it, `shutdown(timeout=0.3)`, then the sink released to fail. Stderr read
`lost 3 item(s); shutdown timed out after 0.3s; ...` and then `lost 1 event(s)` and `lost 3
event(s)`. The `3 item(s)` were the two queued spans plus the `_SHUTDOWN` sentinel `stop()` had
just queued, because the count is `qsize()`. So the same three events were announced lost twice,
and with nothing queued at all the line still reads `lost 1 item(s)` (the sentinel).
Had the sink delivered, the first line would have been a false loss.

**The swap's line has the same shape.** `_record_incomplete_swap` writes
`_diag.lost("item", qsize, "the previous sink could not be confirmed drained ...")` while those
items are still queued on a live drain and may be delivered, and `qsize()` counts the swap's own
unanswered flush marker. The line is also longer than `_diag`'s 200-character detail bound, so it
ships truncated: measured at `fda134a`, it ends `... and queued items may reach the ne…`.

**A second `shutdown()` after an expired one returns at once with the sink open, and that is now
decided rather than open.** Its losing branch in `Worker.stop` waits on `_drain_settled`, which
the expiry has already set, so it returns at once. The closer then sees a live thread and leaves
the sink. Measured: a first `shutdown(timeout=0.3)` expires, and a second `shutdown(timeout=3.0)`
returns in 0.00 s with `close()` called 0 times. Only a call made after the thread ends closes the
sink. §12 offered two closures, a later caller that waits on the thread or an expired state that
is final, and **this spec takes the second**. SPEC-035 FR-004 AC-5 already says a first shutdown
that expired still returns early from the second call; it stands. **The cost, stated plainly:** in
an `atexit` exit after an expired `shutdown()`, a buffering sink whose drain thread is still inside
an emit is neither flushed nor closed, because `atexit`'s call returns at once and the interpreter
exits. **The reason:** an exit that has already spent the caller's budget does not spend a second
one. A wedged sink would otherwise cost every such process another `DEFAULT_SHUTDOWN_TIMEOUT`
(30 s) at exit, on top of the budget the caller chose.

**A `shutdown()` called on the drain thread itself raises or waits on itself.** A sink's `emit`
can call `shutdown()`, and then the drain thread is the caller. Measured at `fda134a`:

- **As the first caller**, it raises `RuntimeError` ("cannot join current thread") out of
  `Thread.join`, out of a public call documented to raise nothing.
- **As a losing caller**, while a main-thread `shutdown()` is joining, it waits on
  `_drain_settled`. Only this same thread's exit or the first caller's expiry can set that event.
  With the main thread at `timeout=3.0` and the inner call at `timeout=1.0`, the inner call blocked
  for 1.01 s, its whole budget. With the main thread at `timeout=1.0` and the inner call at
  `timeout=5.0`, it blocked 0.90 s, until the main thread's join expired. That join expired only
  because the drain thread was the one waiting: the main call wrote `shutdown timed out` and set
  `ShutdownTimeout`. Whether the sink was then closed was a race; it was closed 0 times in one run
  and once in another.

A prototype of the behaviour FR-006 decides was measured on the same arrangements. The inner call
returned in 0.00 s and raised nothing. The batch and the span queued behind it, 3 events, were
delivered. The main thread's join did not expire, and the sink was closed exactly once, by the
main-thread call or by a later one after the thread ended.

**A worker stopped directly and then forked resumes a drain that never runs.** `Worker.stop()`
does not move the retirement count, so `_rebuild_worker_after_fork` resumes such a worker. The child
inherits `_stopped = True`, which `_reinit_after_fork` does not reset. It also inherits the worker's
`_stop` event **set**, because `_fork._fresh_primitive` carries an event's set state across. The
resumed drain loop tests `_stop` first, so it exits at once. Measured in a real forked child: the
drain thread had ended before the child's first submission, that submission was never delivered
and never counted (`qsize()` 1), and the child's `shutdown(timeout=2)` returned in 0.00 s with no
deadline recorded. Resetting `_stopped` alone (the fix §12 names) was measured too and is not
enough: the deadline is then recorded, but the drain thread is still dead and the child delivers
nothing. Resetting `_stopped` **and** giving the worker an unset stop event, as `__init__` does,
was measured to work. The child's drain thread runs, delivers, and, against a 0.5 s failing sink
under `shutdown(timeout=1.2)`, makes two attempts and declines the third against the child's own
deadline.

**The change.** The expiry line and the swap line stop claiming a loss. They are written by a new
`_diag` writer for an outcome not yet decided, and they count only queued submissions, never the
sentinel or a flush marker. An expired `shutdown()` is final: a later caller waits only for a stop
still in progress, and the tests pin that shape and its cost. The first caller's recorded deadline
stays write-once. A `shutdown()` on the drain thread neither joins nor waits on that thread.
On the resume branch, `_reinit_after_fork` resets the stop gate and replaces a set stop event,
exactly as a freshly built worker gets one.

**The decision to record at completion** (a new file in `docs/decisions/pipeline/`): *an expired
shutdown is final; a later caller waits only for a stop still in progress; the first caller's
deadline stays write-once.* Its reasoning is FR-003's and FR-004's: an exit that already spent the
caller's budget does not spend a second one, and a deadline written by a caller that lost the gate
is one the joining caller never budgeted for, SPEC-062's measured race.

## Scope

### In Scope

- A `_diag` writer for an outcome not yet decided, and the expiry line rewritten on it with a
  submissions-only queued count (FR-001).
- The unconfirmed-swap line, the twin, rewritten the same way and brought inside `_diag`'s bound
  (FR-002).
- The expired state recorded as final and pinned by tests, with its cost (FR-003).
- One expiry line and one recorded deadline however many callers race, pinned with
  `run_concurrently` and an injected preemption point (FR-004).
- The resume branch of `_reinit_after_fork` resetting `_stopped` and replacing a set `_stop`
  (FR-005).
- A `shutdown()` on the drain thread that never raises and never waits on that thread, on either
  branch of the gate (FR-006).
- The docstrings that state the old behaviour, corrected in the same change: `Worker.stop` (its
  losing branch is unchanged, and it gains the drain-thread case), `Worker._record_incomplete_swap`,
  `Worker._reinit_after_fork` (its note that the drain thread never reads `self._thread` stops
  being true under FR-006), `Worker.__init__`'s `_exit_deadline` paragraph, and `_diag`'s module
  docstring if it enumerates the writers.
- **The shipped tests whose assertions this spec inverts are updated, never deleted.** Collected
  names (`pytest --collect-only -q`) are diffed between `fda134a` and the branch, and every name
  present before is present after:
  - `tests/test_worker_exit_budget.py::test_the_expiry_line_is_unchanged_when_nothing_is_in_flight`
    (byte for byte: `lost 1 item(s); ...` becomes FR-001's line with no queued clause, since the
    1 was the sentinel).
  - `tests/test_worker_exit_budget.py::test_the_expiry_line_counts_the_batch_in_flight` (it reads
    the expiry line as a `lost ... item(s)` line; that line is no longer a loss line).
  - `tests/test_sink_retry.py::test_the_expired_shutdown_line_counts_what_is_still_queued`
    (`"lost 3 item(s)"` becomes `2 submission(s) still queued`, the sentinel excluded).
  - `tests/test_worker.py::test_the_incomplete_swap_line_no_longer_says_the_sink_stays_open`
    (`"shutdown() that finds the drain thread ended closes it"` becomes FR-002's wording).
  - `tests/test_worker_exit_budget.py::test_in_flight_is_reset_in_the_forked_child` (its resumed
    thread no longer exits at once after FR-005, so the test stops it; and its comment "a real fork
    replaces that event before this handler runs" is false — the replacement is set — and is
    corrected).
  - Any other test the implementer finds asserting `lost N item(s)` from either line or the old
    swap wording. The search is
    `grep -rn "item(s)\|could not be confirmed drained\|_LOST_LINE\|_lost_lines" tests/` plus a
    full suite run, not a reading.
- **At completion**, as `docs/process/completion-ritual.md` work:
  - the three §12 items moved to *Resolved*, struck through and never deleted. The queued-items
    item and the fork item are marked **closed by SPEC-068**. The second-`shutdown()` item is
    marked **decided final by SPEC-068**, with the cost above;
  - invariant 5's *Recorded exceptions* gain the expired case. An expired `shutdown()` leaves the
    sink open, and only a later call that finds the drain thread ended closes it (SPEC-027 FR-004,
    decided final by SPEC-068). The page records no such exception today, although the behaviour
    is shipped;
  - the decision above, as one file in `docs/decisions/pipeline/`.

### Out of Scope

- **`inf`, `nan` and huge timeouts in `shutdown()` and `flush()`** (SPEC-066/SPEC-067's subject,
  being changed in another session). Nothing here changes how a timeout is turned into a wait;
  whichever of the two specs lands second reconciles `stop()`'s edits with the other's handling.
- **A later caller that waits on the drain thread after an expiry.** Decided against (FR-003).
- **The pessimistic `flush()` marker answers on an expired shutdown** (SPEC-050 FR-001). Unchanged.
- **Any `Health` field.** `Health.queued` stays `qsize()` and keeps counting a stranded marker, as
  its docstring records; `stopped_reason` stays `"ShutdownTimeout"` after an expiry.
- **The terminal-failure line's queued figure** (`_terminal_failure`, `"... queued item(s)
  undelivered"`). It counts the queue as undelivered on a dead thread, which is true, and is
  unchanged.
- **Re-opening declined retries for a later caller.** The first caller's deadline is write-once
  (FR-004).
- **Changing the in-flight clause** SPEC-062 FR-005 added. It is kept byte for byte.
- **A forked child closing a sink its parent acquired.** Invariant 12 forbids it, and FR-005 keeps
  that zero rather than changing it.
- **The closer, its grace and `_close_owed`'s choice.** Every route here reaches the existing
  closer unchanged.

---

## Functional Requirements

### FR-001: The expiry line reports what is pending, never a loss

#### Description:

**A writer for an outcome not yet decided.** `_diag.pending(detail: str) -> None` writes
`log-foundry: <detail>\n`, the detail escaped and truncated to `_MAX_DETAIL` exactly as `_suffix`
does it (escape first, then cut, then `…`), with no `lost N ...` prefix. An empty detail writes
nothing. A stream fault (`Exception` from the write) is swallowed and a `BaseException` passes, as
for every `_diag` writer. It joins `_diag.__all__` and `tests/test_diag.py`'s roster of writers.

**The count.** `Worker._queued_submissions() -> int` replaces `_queued_or_unknown` (whose only two
callers are this line and FR-002's). It counts the entries in the queue that are neither
`_SHUTDOWN` nor a `_FlushMarker`, the same classification `_final_drain` applies, read under
`self._queue.mutex` over `self._queue.queue` (the access `_release_waiters` already uses and
`architecture.md` §13 sanctions). Any exception gives 0 and is never raised.

**The line.** With `T = _bounded_seconds(timeout)`, `n = self._in_flight` and
`m = self._queued_submissions()`, the expiry branch of `Worker.stop` calls `_diag.pending` once
with:

    shutdown timed out after {T}; the sink is left open because the worker thread is still using it
    [; {n} event(s) in flight, counted when the emit returns]      only when n > 0
    [; {m} submission(s) still queued]                            only when m > 0

with each bracketed clause appended directly after the line before it, in that order. The first
two parts are `fda134a`'s text unchanged. The queued clause
names submissions, the unit it now counts, rather than items. It deliberately promises no later
count: when the wedged emit is `_final_drain`'s own, the queued entries arrived after that drain's
last read and will never be taken (on the `shutdown()` route they are the ones
`submitted_after_shutdown` counts), so "counted when the drain takes them" would be false there.
The record-then-announce order, `stopped_reason`, `_drain_settled` and `_release_waiters` are
unchanged.

**At most once.** The expiry line now announces no loss. Every later `lost` line about these
events is `_emit`'s, one per batch, so each queued event is announced lost at most once across the
expiry line and every later line.

#### Acceptance Criteria:

- [ ] `_diag.pending("a; b")` writes exactly `log-foundry: a; b\n`. A detail containing `\n`
      writes one line with `\\n` in it. A detail of 5000 characters is cut to `_MAX_DETAIL`
      characters plus `…`. `pending("")` writes nothing. With `sys.stderr` replaced by an object
      whose `write` raises `OSError`, `pending` returns `None` and raises nothing.
- [ ] **Case A, the stuck emit is `_drain`'s.** `Worker(sink, batch_size=1, flush_interval=60.0)`
      with a sink whose `emit` blocks on an event the test controls. One 1-event span is inside the
      emit, and two more spans (2 and 1 events) are queued behind it. `shutdown(timeout=0.3)`
      (through `retire_process`) writes exactly
      `log-foundry: shutdown timed out after 0.3s; the sink is left open because the worker thread
      is still using it; 1 event(s) in flight, counted when the emit returns; 2 submission(s) still
      queued\n` as one line.
- [ ] Case A released to **fail**: the later lines are `lost 1 event(s)` and `lost 3 event(s)`, the
      `lost` counts across all of stderr sum to **4**, the number of events submitted, and no line
      contains `item(s)`. At `fda134a` they sum to 7 (measured).
- [ ] Case A released to **succeed**: all 4 events are delivered, `failed_batches == 0`, and stderr
      contains no `lost`.
- [ ] Expiry with nothing in flight and nothing queued except the sentinel (the drain parked before
      `_emit`, as `test_the_expiry_line_is_unchanged_when_nothing_is_in_flight` arranges) writes
      exactly `log-foundry: shutdown timed out after 0.2s; the sink is left open because the worker
      thread is still using it\n`.
- [ ] **Markers are not submissions.** A `flush(timeout=0.05)` issued while the emit is wedged
      leaves its marker queued; the expiry line then carries no queued clause although `qsize()` is
      2 (marker and sentinel).
- [ ] With `worker._queue` replaced by an object whose `mutex` raises on entry, the expiry branch
      raises nothing and writes the line with no queued clause.
- [ ] **The line fits the bound.** Rendered with a 14-character `T` (`_bounded_seconds` of
      `-1.23456789e-100`) and `n = m = 999999`, the detail is at most `_MAX_DETAIL` characters, so
      the written line contains no `…`.
- [ ] Serves invariants 2 and 11: each event is announced lost at most once, and every count the
      line carries reproduces from the queue and the in-flight batch.

### FR-002: The unconfirmed-swap line reports what is pending, never a loss

#### Description:

`_record_incomplete_swap` is the twin site: its queued items sit on a **live** drain, may be
delivered to the new sink, and are counted by `_emit` if they are later abandoned. It counts
`incomplete_swaps` as today, then calls `_diag.pending` once with:

    the previous sink could not be confirmed drained within {T}; it is left open until a
    shutdown() finds the drain thread ended[; {m} submission(s) still queued, which may reach
    the new sink]

where each wrap in the block is one space, the bracketed clause is appended directly,
`T = _bounded_seconds(timeout)`, `m = _queued_submissions()`, and the clause is present only when
`m > 0`. The wording is shortened from `fda134a`'s so that the
whole line fits `_MAX_DETAIL`. The shipped line does not: at a 4-character `T` its detail is 214
characters, written truncated (`... may reach the ne…`). The retarget's own unanswered flush
marker is not a submission and is not counted. The substrings `could not be confirmed drained`
and `left open`, which `tests/test_config.py` asserts, are kept.

#### Acceptance Criteria:

- [ ] A swap whose fence cannot be confirmed (the drain wedged in the old sink's `emit`, the
      `_strand` arrangement in `tests/test_worker.py`) with 2 spans queued behind the wedged one
      writes exactly `log-foundry: the previous sink could not be confirmed drained within {T}; it
      is left open until a shutdown() finds the drain thread ended; 2 submission(s) still queued,
      which may reach the new sink\n` for the swap's `T`, and `incomplete_swaps == 1`.
- [ ] The same swap with nothing queued but the retarget's marker writes the line with no queued
      clause, and contains no `lost`.
- [ ] Releasing that old sink so that the queued spans are then emitted to a new sink that always
      fails: the `lost` counts across all of stderr equal the events in those spans, and no line
      contains `item(s)`.
- [ ] Rendered with a 14-character `T` and `m = 999999`, the line contains no `…`.
- [ ] Serves invariants 2, 6 and 11: the swap line is the expiry line's twin and keeps the same
      at-most-once promise and the same truthful count.

### FR-003: An expired shutdown is final

#### Description:

**This is a recorded decision, not a code change**, and the requirement is that the shipped shape
is pinned so it cannot drift. A `stop()` that loses the once-only gate keeps today's branch,
except on the drain thread itself (FR-006). While the drain is still running, it waits on
`_drain_settled` within its own budget, and an abandonment releases that wait. After an expiry, `draining` is false and it returns at once. It writes no line,
sets no `stopped_reason` and calls no `_release_waiters`. `_shutdown_worker`'s closer then closes
the sink only if it finds the drain thread already ended; otherwise the sink is left open for a
later call.

**The cost.** In an `atexit` exit after an expired `shutdown()`, a buffering sink whose drain
thread is still inside an emit is neither flushed nor closed. **The reason.** An exit that has
already spent the caller's budget does not spend a second one. Waiting on the thread instead would
add `atexit`'s `DEFAULT_SHUTDOWN_TIMEOUT` (30 s), or forever under `timeout=None`, to every exit
over a wedged sink, after the budget the caller chose had run out. SPEC-035 FR-004 AC-5 made the
same call, and it stands.

Most of this is already held by shipped tests, which stay green and unchanged unless a criterion
below says it extends one:
`tests/test_shutdown_lifecycle.py::test_an_expired_first_shutdown_does_not_make_the_second_wait`
(time only), `::test_an_abandoning_first_shutdown_releases_the_second_callers_wait`,
`::test_a_second_shutdown_waits_for_the_drain_already_running`,
`::test_a_second_shutdown_is_still_bounded_by_its_own_budget`,
`tests/test_sink_retry.py::test_a_later_shutdown_closes_a_sink_an_expired_one_left_open` and
`tests/test_worker.py::test_an_expired_shutdown_leaves_a_stranded_sink_for_the_next_call`.

#### Acceptance Criteria:

- [ ] After a `shutdown(timeout=0.3)` that expired on a sink that stays wedged, a second
      `shutdown(timeout=3.0)` returns within 0.1 s, its CPU time (`time.process_time()` delta, run
      serially) is under 0.05 s, stderr gains nothing, and `close()` has been called 0 times.
      `test_an_expired_first_shutdown_does_not_make_the_second_wait` holds the time half today;
      the CPU, stderr and close assertions are added to it.
- [ ] The stuck emit is then released and the thread ends. A third `shutdown()` calls `close()`
      exactly once, and a fourth leaves the count at 1 (held by
      `test_a_later_shutdown_closes_a_sink_an_expired_one_left_open`).
- [ ] A losing caller that arrives **while the first caller is still joining** waits for the drain
      within its own budget (held by `test_a_second_shutdown_waits_for_the_drain_already_running`
      and `test_a_second_shutdown_is_still_bounded_by_its_own_budget`). One that had committed to
      that wait is released by the first caller's expiry (held by
      `test_an_abandoning_first_shutdown_releases_the_second_callers_wait`).
- [ ] **The cost, pinned in a real interpreter exit.** A subprocess configures a sink that blocks in
      `emit` and prints a marker from `close()`, makes one traced call, calls
      `shutdown(timeout=0.3)`, and exits normally, so the library's `atexit` handler makes the
      second call. The marker is never printed, and the subprocess exits within 5 s, far under the
      30 s a waiting `atexit` would take.
- [ ] Serves invariants 3 and 5: every later caller is bounded and idle, and the sink an expired
      call left open is closed exactly once by a call that finds the thread ended. That is the
      recorded exception to invariant 5 written at completion.

### FR-004: One expiry line and one deadline, however many callers race

#### Description:

The once-only gate decides everything that must happen once. Only the caller that won
`claim_stop` queues the sentinel, sets the stop event, joins against its own timeout, and on
expiry records `ShutdownTimeout` and writes FR-001's line. Every other caller is a losing caller
(FR-003), which writes nothing. The recorded deadline is write-once (SPEC-062 FR-001, unchanged):
`claim_stop` writes it only in the decision that wins the gate. **This is a decision, and its
reason is SPEC-062's measured race:** a deadline written by a caller that lost the gate is one the
joining caller never budgeted for. If a later, larger budget extended the record while the first
caller was still joining, the drain would make retries against the later deadline. The first
caller's join would expire over them, announcing a timeout and leaving the sink open because of
work it never asked for. So a later caller never re-opens a retry the first caller's deadline
declined.

#### Acceptance Criteria:

- [ ] Two concurrent `shutdown(timeout=0.3)` calls through `tests/conftest.py::run_concurrently`
      against a wedged sink: stderr carries exactly **one** `shutdown timed out` line,
      `stopped_reason == "ShutdownTimeout"`, both calls return within 0.3 s plus 0.3 s of slack
      with no exception, and the CPU time across the run is under 0.05 s. No shipped test asserts
      this today. `tests/test_worker.py::test_the_expiry_branch_still_reports_shutdown_timeout`
      asserts one line for a **single** caller.
- [ ] **An injected preemption point after a lost claim.** The losing caller is parked after its
      claim has lost and before it reads `draining`. It is released only after the winning caller's
      join has expired and its line is written. It then returns within 0.1 s, writes nothing, and
      `close()` has been called 0 times; stderr holds exactly one `shutdown timed out` line.
- [ ] **A later caller does not re-open retries.** A sink whose first `emit` blocks until released
      and then fails, and whose later attempts fail at once. A first `shutdown(timeout=0.3)`
      expires, and a second `shutdown(timeout=5.0)` returns within 0.1 s. `_exit_deadline` still
      equals the first caller's value. The sink is then released. Exactly **1** attempt is made,
      `failed_batches == 1`, one `lost` line names the `shutdown()` deadline, the thread ends within
      1.0 s of the release, and a later `shutdown()` calls `close()` once. A mutant in which the
      losing claim overwrites the deadline makes 4 attempts.
- [ ] `test_two_concurrent_shutdowns_follow_the_recorded_deadline` and
      `test_the_recording_caller_is_the_joining_caller_whichever_arrives_first`
      (`tests/test_worker_exit_budget.py`) stay green with their assertions unchanged.
- [ ] Serves invariants 3, 5 and 11: every caller is bounded by its own budget, the close happens
      once, and the timeout is announced once.

### FR-005: A resumed child gets a stop gate and a stop event of its own

#### Description:

On the **resume** branch of `Worker._reinit_after_fork`, before the drain thread is started and
before the existing `offer_stop_signal(self.sink, self._stop)`, the worker:

- sets `self._stopped = False`, beside the existing `self._exit_deadline = None`; and
- sets `self._stop = _lifecycle._state.refresh_stop_signal()`, the call `__init__` makes. It returns
  the owner's current event when that is unset, so a worker that was never stopped keeps the event
  it had and the identities `worker._stop is _state._stop is sink.log_foundry_stop_signal` are
  unchanged. Only a set event is replaced, and the owner and the worker then hold the same new one.
  It is called without `_state._lock`, for the reason `_rebuild_worker_after_fork` already gives
  for not taking it (the child's only thread, its locks re-initialised moments earlier).

The **retired** branch is untouched: a retired parent forks a retired child, whose `_stopped`,
`_stop` and `_exit_deadline` stay as inherited. The start-failure branch keeps its behaviour; the
resets above have already run, so a later `shutdown()` in that child wins the gate and, finding
`_drain_finished` set, queues no sentinel.

Resetting `_stopped` alone is not the fix. Measured at `fda134a` with only that reset, the child's
drain thread still exits at once on the inherited set event: 0 attempts, nothing delivered.

#### Acceptance Criteria:

- [ ] **Observed in a real forked child.** The parent runs a hand-built
      `Worker(sink, batch_size=10, flush_interval=30.0)`, installed with `install_worker`, that has
      delivered one span and been stopped by a direct `worker.stop(5.0)`. The premise is asserted in
      the parent: `_state.live_worker() is worker` and `worker._stop.is_set()`. The child reports
      through a pipe and its exit code. A **positive control** comes first: the child writes a fixed
      token before anything else, and the parent fails if the token or the exit status 0 is
      missing, so a child that crashed cannot read as a pass.
- [ ] In that child: the drain thread is alive after the fork, a span submitted in the child is
      delivered by the child (the sink records the child's pid), `shutdown(timeout=2.0)` returns in
      under 0.5 s, well under both its budget and `flush_interval`, with CPU under 0.05 s, and
      `_exit_deadline` is not `None` after it. At `fda134a` the drain thread is dead, 0 child
      deliveries are made, and the deadline stays `None` (measured).
- [ ] In the same arrangement, with a sink that in the child takes 0.5 s per `emit` and fails, the
      child's `shutdown(timeout=1.2)` makes exactly **2** attempts in the child, writes one `lost`
      line naming the `shutdown()` deadline, and leaves `failed_batches == 1`. Both `fda134a` and
      the `_stopped`-only reset make **0** attempts (measured), so this criterion fails against
      either.
- [ ] The child calls `close()` on the parent-acquired sink 0 times, as at `fda134a`.
- [ ] Unit level: `_reinit_after_fork(resume=True)` on a directly stopped worker leaves
      `_stopped is False`, `_exit_deadline is None`, `_stop.is_set() is False`,
      `worker._stop is _lifecycle._state._stop`, and, for a sink that declares
      `log_foundry_stop_signal`, `sink.log_foundry_stop_signal is worker._stop`. On a worker that
      was never stopped, `_stop` is the same object before and after.
- [ ] `_reinit_after_fork(resume=False)` leaves `_stopped`, `_stop` (object and set state) and
      `_exit_deadline` as they were.
- [ ] Serves invariants 12, 2 and 3: the child is repaired so it delivers its own events, an event
      it accepts is delivered or counted, and its `shutdown()` is bounded by the child's budget.

### FR-006: A shutdown on the drain thread never raises and never waits on itself

#### Description:

`Worker.stop` asks `threading.current_thread() is self._thread` once, after the claim.

- **As the first caller** on the drain thread, it does the first caller's work up to the join. It
  claims the gate and records its deadline (`claim_stop`), queues the sentinel if the drain has not
  finished, and sets the stop event. It then returns **without joining**. It writes no expiry line
  and sets no `stopped_reason`, because nothing has timed out, and it calls no `_release_waiters`,
  because the drain thread's own exit sweeps the markers in `_run`'s `finally`.
- **As a losing caller** on the drain thread, it returns at once without waiting on
  `_drain_settled`.

The drain ends once the current emit returns. `_emit` treats that emit's outcome as usual: a
delivery returns, and a failure retries or declines against the recorded deadline. `_drain` then
sees the stop event, and `_final_drain` takes the queue. The rest of `_shutdown_worker` runs as
today on the drain thread. `_close_owed` finds the thread alive, its own, and leaves the sink. A
later `shutdown()` from another thread (`atexit`, for instance) closes it exactly once: if the
drain is still running, as a losing caller waiting within its budget; if it has ended, at once.
The retirement count moves as for any `shutdown()`.

**One window is stated rather than closed.** In a forked child, `_reinit_after_fork` assigns
`self._thread` just after the new thread starts, so for an instant the attribute still names the
inherited dead thread. A `shutdown()` the new drain thread made inside that instant would read the
comparison as false and join the dead thread. That join returns at once, so the call neither
raises nor waits. This is why `_reinit_after_fork`'s note that the drain thread never reads
`self._thread` is corrected rather than relied on.

#### Acceptance Criteria:

- [ ] **First caller.** A sink whose first `emit` calls `shutdown(timeout=5.0)`, with one more
      2-event span queued behind it: the call returns in under 0.5 s with CPU under 0.05 s and
      raises nothing; `_exit_deadline` is recorded; no `shutdown timed out` line is written;
      `stopped_reason is None`; and all 3 events are delivered. At `fda134a` the call raises
      `RuntimeError` (measured).
- [ ] The same arrangement with a sink that always fails: the `lost` counts across all of stderr
      sum to the 3 events, and `failed_batches` equals the number of batches abandoned or declined.
- [ ] After the first-caller case the sink has been closed 0 times, and a later main-thread
      `shutdown()` calls `close()` exactly once whether it runs before or after the drain thread
      ends; a further call leaves the count at 1.
- [ ] **Losing caller.** A main-thread `shutdown(timeout=3.0)` is joining while the sink's `emit`
      calls `shutdown(timeout=1.0)`. The inner call returns within 0.1 s, raises nothing and writes
      nothing, the main call's join does not expire, and `close()` is called once. At `fda134a` the
      inner call blocked for 1.01 s (measured).
- [ ] **Losing caller with the longer budget.** The main thread at `timeout=1.0` and the inner call
      at `timeout=5.0`: the main call's join does not expire (`stopped_reason is None`, no
      `shutdown timed out` line) and `close()` is called once. At `fda134a` the inner wait made that
      join expire (measured).
- [ ] Serves invariants 1, 2 and 3: the call raises nothing, the batch and the queue are delivered
      or counted once by the normal lines, and no wait is spent on a thread that cannot end while
      it waits.

---

## Data Model

```
# src/log_foundry/_diag.py
__all__ = ["WARN_EVERY", "absorbed", "errno_of", "lost", "pending", "rejected"]
def pending(detail: str) -> None      # "log-foundry: <escaped, truncated detail>\n";
                                      # "" writes nothing

# src/log_foundry/worker.py
Worker._queued_submissions() -> int   # replaces _queued_or_unknown: queue entries that are neither
                                      # _SHUTDOWN nor a _FlushMarker, under self._queue.mutex; 0 on
                                      # any failure
Worker.stop(timeout, *, claimed=None) # expiry line via _diag.pending (FR-001); on the drain thread
                                      # itself: first caller claims, queues the sentinel, sets the
                                      # stop event and returns; losing caller returns (FR-006);
                                      # otherwise both branches unchanged (FR-003)
Worker._reinit_after_fork(*, resume)  # resume branch adds: _stopped = False;
                                      # _stop = _lifecycle._state.refresh_stop_signal()
```

## API / Interface Contract

No public change. `shutdown()`, `flush()`, `configure()` and `Health` keep their signatures and
fields. What a caller can observe changing is stderr wording (FR-001, FR-002), a `shutdown()`
inside a sink's `emit` that returns instead of raising or blocking (FR-006), and a resumed child
delivering (FR-005).

## File & Folder Structure

```
src/log_foundry/_diag.py                  # pending()
src/log_foundry/worker.py                 # _queued_submissions, stop()'s line, drain-thread case,
                                          # _record_incomplete_swap, _reinit_after_fork, docstrings
tests/test_shutdown_expiry_bookkeeping.py # new: FR-001, FR-002, FR-004, FR-005, FR-006
tests/test_shutdown_lifecycle.py          # FR-003: the AC-5 test extended, the exit subprocess test
tests/test_diag.py                        # pending() in the writer roster
tests/test_worker_exit_budget.py          # inverted tests named in Scope
tests/test_sink_retry.py                  # inverted test named in Scope
tests/test_worker.py                      # inverted test named in Scope
```

## Implementation Phases

### Phase 1: The two lines (FR-001, FR-002)

- Add `_diag.pending` and its roster entry; replace `_queued_or_unknown` with
  `_queued_submissions`.
- Rewrite the expiry line and the swap line on it; update the named tests that assert the old
  lines; add the case A, marker, failure and bound criteria.

### Phase 2: The callers (FR-003, FR-004, FR-006)

- Add the drain-thread case to `stop()` and correct its docstring; the first-caller, failing-sink,
  later-close and two losing-caller criteria.
- Extend the AC-5 test and add the exit subprocess test; the concurrency, parked-loser and
  no-re-open criteria, with CPU asserted.

### Phase 3: The fork (FR-005)

- Add the two resets to the resume branch; correct `_reinit_after_fork`'s and `__init__`'s
  docstrings and `test_in_flight_is_reset_in_the_forked_child`.
- The forked-child tests with the pipe, exit code and positive control, and the unit criteria.
