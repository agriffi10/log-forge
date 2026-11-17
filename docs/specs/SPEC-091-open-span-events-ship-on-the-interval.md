---
type: Spec
id: SPEC-091
title: "An Open Span's Events Ship on the Worker's Interval"
description: "Has the worker take every open span's buffered events about once a flush interval, on its own thread and exactly once, so a process killed mid-handler loses at most about one interval of a span's events rather than the whole span, and states plainly what a kill still loses."
spec_status: In Progress
depends_on: [SPEC-015, SPEC-024, SPEC-036, SPEC-039, SPEC-050, SPEC-085, SPEC-088, SPEC-089, SPEC-090]
arc: audit-2026-11-14
arc_order: 7
---

## Overview

A traced function's events wait on its span until the span closes. A process that is killed while
a handler is still running, which is what a Lambda timeout or an out-of-memory kill does, loses
every event that handler logged: the audit's probe delivered 0 of 51 events from a root span
killed 3.5 s after it opened, and nothing on stderr said so (finding 8 of the 2026-11-14 audit).
Andrew decided on 2026-11-15. Asked whether an open span's events should leave before the span
closes, he answered **"Ship every ~1s (Recommended)"**. This spec does that with the worker's
existing interval and no new setting. The worker exists from the moment the first span opens,
and about once an interval, on its own thread, it takes the events every open span has buffered
and ships them like any other submission. Each event still ships exactly once, and a span's
`span.start` and `span.end` still carry what they carry today. A `flush()` now reaches every open
span, not only the caller's, and the exit drain takes what open spans hold before it drains. The
documents then say plainly what a kill still loses: about one interval of an open span's events,
plus whatever is queued and not yet delivered.

## Scope

### In Scope

- A registry of open spans, and one rule by which a sweep from another thread takes a span's
  buffer: in place, never its `span.start`, never once its close has begun, one submission per
  span, a fault after a take counted, no stderr written under the sweep lock, and exactly once
  against every other reader and writer of that buffer (FR-001).
- The worker built before the first span opens, and a sweep on the drain thread on a clock of its
  own that makes the time trigger due, so an event leaves within about one interval (FR-002).
- `flush()`, `shutdown()`, the exit drain, events recorded after the worker has stopped, sink
  swaps and a forked child, each stated against the sweep (FR-003).
- The sweep in SPEC-088's event ledger, ~~with SPEC-088 FR-001's snapshot read order refined~~
  crediting each sweep's hand-over in the sweeping thread's cell, which the two-pass read order
  SPEC-088 built already reads soundly (revised 2026-11-15), and in SPEC-089's flush window, with the SPEC-089 criterion it inverts rewritten; the model harnesses'
  span identity restated in `span.end` events; and the SPEC-085 pin the move resolves (FR-004).
- The traced call's added cost, measured, and the locks the open and per-event paths may take
  (FR-005).
- The record: a new decision quoting Andrew's answer, the decisions this refines, the §12 item,
  and the README and architecture sentences on what a kill loses (FR-006).

### Out of Scope

- **A signal handler that sweeps on `SIGTERM`.** The evidence does not require one. The default
  `SIGTERM` disposition ends the process without running `atexit`, and `p12_kill.py`'s `SIGTERM`
  case lost 15 events of five spans that had already **closed**: they were queued, so no sweep of
  open spans reaches them. A handler that calls `shutdown()` is finding 3's deadlock (§12). The
  documents state the loss instead (FR-006).
- **Sink acknowledgement fixes** (findings 1, 11, 12, 14 and the RabbitMQ item). A later spec of
  this arc.
- **SPEC-090's batching rule.** A swept submission is a submission, and SPEC-090's rule batches it
  as it batches any other; nothing here changes `_take`, `_complete`, the size target or the cap.
- **Any new `configure()` argument, environment variable or public setting**, including one for
  the sweep's cadence. The sweep uses `flush_interval`. `Worker` gains one keyword its owner
  passes (FR-002), and `Worker` is not exported from `log_foundry`.
- **Shipping `span.start` before its span closes.** It stays on the span until the close or an
  in-span `flush()`, so it carries the span's final baggage as *Boundary events take the span's
  final baggage* (`docs/decisions/trace-model/`) requires. A killed span's logs therefore arrive
  without its `span.start` and `span.end`.
- **Sweeping the forked child's copy of the parent's open spans.** The child keeps today's
  behaviour for them (FR-003). SPEC-088 FR-005 records the duplication of the forking thread's
  buffer that a child closing that span makes today, and this spec neither adds to it nor fixes it.
- **`api._log`'s append window** (`architecture.md` §13). The worker's sweep takes in place and so
  adds no instance of it (FR-001); the owner's `flush()` sweep still detaches by swap, as today.
- **A free-threaded interpreter as a supported build.** FR-001 lists the operations the take
  needs to be atomic and leaves the free-threaded ordering argument unproven; nothing was run on
  one.
- **A loss no process survives to count**: `SIGKILL`, `SIGTERM` without a `shutdown()`,
  `os._exit`, and a Lambda reaped while frozen. Stated, not counted (FR-006).

---

## Functional Requirements

### FR-001: Every open span is registered, and a sweep takes its buffer in place, past its `span.start`, before its close begins

#### Description:

**Today, at `a2cd4b36`.** A span's buffer is `Span.events`, a list on the `Span` dataclass
(`model.py:69`). `_open_span` puts `span.start` in it (`decorator.py:90`), `api._log` appends each
in-span event to the current span's list with no lock (`api.py:134-137`), and two things move it to
the worker. The close appends `span.end` (`decorator.py:721`), backfills the boundary events'
baggage by iterating `span.events` (`decorator.py:722`, `model.py:377-386`), sets `closed`, and
`_flush` rebinds `span.events` to a fresh list under `_sweep_lock` and submits the old one
(`decorator.py:581-584`). An in-span `flush()` runs `_sweep_open_spans`, which reaches only the
calling context's span stack (`context._live_span_stack`, `context.py:75-93`), backfills, sets
`swept` and rebinds under the same lock (`decorator.py:448-465`). `_sweep_lock` is a module
`threading.Lock` (`decorator.py:33`) in the fork roster (`_fork.py:771`). Nothing in the library
can enumerate another thread's open spans, and the worker's triggers read only its queue
(`worker.py:1719-1744`). `continue_trace` reads `swept` and then re-parents with no lock
(`decorator.py:197-207`).

**The registry.** `decorator._open_spans` is a `dict[int, Span]` keyed by `id(span)`. `_begin`
registers the span after `context.push_span` returns, and `_close_span` removes it as its **first**
statement, before `span.end` is built or appended. Both are single dict operations with no lock. A
`Span` stays registered from its push until its close begins, so a span whose push failed is never
registered and is still closed as today.

**What a sweep may take.** `Span.held` is the number of leading buffered events a sweep leaves on
the span: `_open_span` sets it to 1 after it buffers `span.start`, and the owner's `flush()` sweep,
which ships `span.start` backfilled as today, sets it to 0. The worker's sweep,
`decorator._sweep_registered(hand_in)`, holds `_sweep_lock` for one pass over a copy of the
registry (`_open_spans.copy()`). For each span it reads `n = len(span.events)` **first**, then
skips the span if it is no longer the registered value for its id or is `closed`, and otherwise
takes `span.events[held:n]` and deletes that slice from the same list. It then sets `swept` and
calls `hand_in` with that span's take as **its own submission**, still holding the lock, and moves
to the next span. It returns the events it handed in, the overflow and retirement totals each
`hand_in` returned (below), and any exception a `hand_in` raised. One submission per span keeps
SPEC-090's cap meaningful: a concatenated sweep of many spans made one submission past
`_BACKLOG_BATCH_CAP` that the rule must send whole, where a span's own take is no larger than
the submission its close makes today. It costs the hold: with a `put_nowait` per span, one sweep
of 1,000 spans of ten events held the lock 428 to 433 µs, against 157 to 159 µs for one
concatenated put (Evidence). `dropped` stays in submissions, so a sweep that meets a full queue
moves it by one per span whose take it could not hand in.

**A fault after a take is a counted loss.** The slice and the `del` run outside the `try`, so a
fault there leaves the events where they were. A fault after the `del`, in `hand_in` or in setting
`swept`, means the taken events are on neither the span nor the queue: the sweep debits them at once
to `in_span_lost` (`_note_in_span_loss(len(take))`) and keeps the exception, and the line written
for it, after the lock is released, says the events taken were lost. The reviewer's
`raise_handin.py`, with `hand_in` raising: the first cut delivered 0 of 5, moved `in_span_lost` by
0 and wrote "their events stay on the spans"; the prototype as specified delivers 0 of 5, moves
`in_span_lost` by 5 and writes "the events taken were lost".

**Nothing is written to stderr while `_sweep_lock` is held.** `worker.py:627` and `:691` already
keep `submit`'s lines outside `Worker._lock` so that a wedged console cannot stall the drain path;
under `_sweep_lock` the same write stalls every span close in the process. So `Worker.submit` is
split into `_enqueue(events, *, count_retired)`, which puts the submission and counts `dropped` and
`submitted_after_shutdown` under `Worker._lock` exactly as `submit` does today and returns the two
totals it moved, and `_announce(totals)`, which writes the throttled lines; `submit` is the two in
turn, unchanged to its callers. Every hand-in made under `_sweep_lock` calls `_enqueue` and keeps
the totals, and the caller announces them, and any `hand_in` fault, after the lock is released:
`Worker._run_sweep` for the drain thread's sweep, `_flush_worker` for its registry sweep, and
`_sweep_open_spans`, whose `worker.submit` under the lock (`decorator.py:462`) is today's instance
of the same stall. The reviewer's `wedged.py`, with the queue at one slot and full and stderr
blocked on the overflow line: the first cut's traced call on another thread did not close within
4 s (`close returned=False after=4.01s`); the prototype as specified closes it at 0.00 s.

**Why this is exactly once.** Four readers or writers meet one buffer:

- *The owner's append.* The take never rebinds `span.events`, so an append that read the
  attribute before the take lands on the same list, after index `n`, and is taken by the next
  sweep or the close. A rebinding take lost an event to exactly this race: 1 of 36,015 in one of
  three runs of `spec091_exactly_once.py` under a switch interval of 1e-6 s, where the in-place
  take lost none in ten runs (Evidence).
- *The close.* The owner removes the span from the registry before it appends `span.end`. If the
  sweep's `n` counts `span.end`, the removal came first, so the membership read that follows sees
  it and the span is skipped. Otherwise the slice ends before `span.end`. Without the membership
  read, `span.end` left early: 25 and 29 spans per run ended before their own logs, and 3 to 7
  boundary events lost their baggage (Evidence). The close's swap takes `_sweep_lock`, so it
  waits for a take in progress and submits only what the take left.
- *The close's backfill.* It mutates only `span.start` and `span.end`, which a worker sweep never
  takes, but a take that deletes a slice while the backfill iterates the live list shifts the
  elements under the iterator. `backfill_baggage` therefore iterates a snapshot,
  `list(span.events)`. With a 0.2 ms pause injected between the sweep's check and its take, the
  live iteration left 1 and 3 boundary events without their baggage; the snapshot left none.
- *The owner's `flush()` and `continue_trace`.* `_sweep_open_spans` already holds `_sweep_lock`.
  `continue_trace` now holds it across its `swept` check, `set_adopted_context` and
  `_reparent_current_span`, and writes its rejection line after releasing it. With a 50 ms pause
  injected inside the re-parent and a 1 ms interval, 19 of 20 root spans reached the sink with two
  trace ids without the lock and 0 of 20 with it (Evidence). That also closes the §13 residual
  "`Span.swept` is not a synchronization primitive", for both sweeps.

**What must be atomic, and the site rule.** Each of these must complete as one operation against
a concurrent one on the same object: `list.append`, `len(list)`, a slice read `lst[a:b]` and a
slice delete `del lst[a:b]`, and the dict's `__setitem__`, `pop`, `get` and `copy`. Under the GIL
each is one C call with no eval-breaker check inside it, so it is. The rule that makes the rest
hold is a **site rule**: every mutation of `span.events` other than `append`, and every rebinding of
the attribute, happens under `_sweep_lock`. Today that is the close's swap (`decorator.py:583`) and
the owner's sweep (`:461`); this spec adds the worker's take. `span.events.append` in `api._log`,
`_open_span` and `_close_span` is the only write allowed outside it. A test derives every write to
an `events` attribute of a `Span` from the AST of `src/` (an assignment, an augmented assignment, a
`del` or a call of a mutating `list` method on it) and fails naming one that is neither an
`append` nor inside a `with _sweep_lock:` block, so a later edit cannot add a fifth writer
silently.

**The ordering argument, and what it rests on.** Under the GIL, a thread switch orders everything
one thread did before it against everything the next thread does after it, so the owner's registry
removal, made before its `span.end` append, is seen by any sweep whose `len` sees that append. On a
free-threaded build the same conclusion would need each object's operations to publish their
effects in an order other threads observe, through the per-object locks CPython uses there, and
this spec has not established that `len` and the dict's `get` read through those locks. The
free-threaded case is therefore **unproven**: no free-threaded run was made, because the
interpreter on this machine was not usable, and the argument is not claimed for it.

#### Acceptance Criteria:

- [ ] `spec091_exactly_once.py` reports `dup=0 missing=0 bad_boundary=0 end_not_last=0` and a
      positive `shipped_before_their_close` in each of five runs, with and without `swap`
      (invariants 2 and 6).
- [ ] With a `list` subclass installed as `span.events` whose `append` pauses on an event the test
      controls, before it inserts in one case and after in the other, the test forces the sweep into
      each pause: with the take replaced by a slice-and-rebind the paused append's event is lost,
      with the membership read removed `span.end` leaves before its span's logs, and with the
      backfill iterating the live list (a pause injected between the check and the take) a
      boundary event lacks its baggage; each failure names its event, and with the specified
      code each passes (invariant 2).
- [ ] The AST site-rule test passes on the build, and fails naming the site when a
      `span.events.clear()` is added to `api._log` (invariant 2).
- [ ] A `hand_in` made to raise for one span's take moves `in_span_lost` by exactly that take's
      events, writes one line saying the events taken were lost, and leaves every other span's
      take delivered (invariants 2 and 11).
- [ ] A span opened and closed while no sweep runs is registered between its push and its close,
      and absent from `_open_spans` once `_close_span` has begun (invariant 2).
- [ ] A worker sweep never takes `span.start`: in a span that logs ten events and is swept, the
      sink's `span.start` arrives in the same submission as `span.end` and both carry baggage set
      after the sweep (invariants 2 and 8).
- [ ] An in-span `flush()` still ships `span.start` with the baggage as of the flush, sets `held`
      to 0, and a later worker sweep then takes from index 0 (invariants 2 and 6).
- [ ] `spec091_continue_trace.py` reports `split_spans=0`, and a `continue_trace()` made after a
      worker sweep took events from the current root span is refused with the existing rejection
      line and `reason="rejected"` (invariant 9).
- [ ] The sync and async wrappers register and remove alike: the same test passes through both
      (invariant 6).

### FR-002: The worker exists while a span is open, and sweeps on a clock of its own that makes the time trigger due

#### Description:

**Today.** `_lifecycle._get_worker` builds the process worker (`_lifecycle.py:3677-3808`, the
build at `:3796`), and the first caller is a span's close (`decorator.py:581`) or a `flush()` sweep
that found events (`decorator.py:465`). Inside a process's first traced call there is no worker,
so nothing could sweep: the probe's root span had a worker only because a child span closed.

**The build moves to the open.** `_begin` reads `_lifecycle._state._worker` as its first
statement, before SPEC-088's `span.start` credit (`decorator._begin`, before its `try`), and when it is `None` calls `_lifecycle._get_worker()` in a guard of its own that announces
a fault by type ("starting the worker for a span") and goes on to open the span. The build runs
**before** the span is pushed, so the sink's stop-signal setter, which the build runs, logs on the
orphan route as it does today at the close, where the span is already `closed`: built after the
push, the setter's `info()` became an in-span event and two SPEC-085 cells
(`reentrant-setter-worker-shutdown+info` and `shutdown-none+info`) went red as `LedgerBroken:orphans-1`.
A build that fails is retried by the close, as today, which counts the buffer in `in_span_lost` if
it fails again (`decorator.py:687`). Sitting ahead of the credit is what keeps the ledger whole when
the sink's setter raises a `BaseException` during the build: the guard catches only `Exception`, so
a `KeyboardInterrupt` leaves `_begin` before anything is credited, and no `span.start` is left
credited with no debit.

**The sweep runs on the drain thread.** `Worker.__init__` takes a keyword `sweep`, a callable or
`None`, stored as `_sweep`; `_get_worker` passes `decorator._sweep_registered` and nothing else
passes one, so a worker a test builds by hand sweeps nothing and `worker.py` still knows nothing of
spans. At the top of each pass of the drain loop, when `_sweep` is set and `flush_interval` has
passed since the last sweep, `_run_sweep()` calls it with `_hand_in` and absorbs any `Exception` it
raises, announced by type, leaving the events not yet taken on their spans, and announces after
the sweep returns every total and fault the sweep handed back (FR-001). `_hand_in` is
`_enqueue(events, count_retired=False)`: `submit` without the two retirement reads and without its
stderr writes. The retirement reads are left out because the drain thread
that hands a submission in is the thread whose exit drain takes it, so a `shutdown()` never
strands it; a drain that dies or an exit that expires first leaves it in SPEC-088's `stranded` or
`in_flight`, as any queued submission. Counting it in `submitted_after_shutdown` would print "nothing will drain these"
over events the exit drain then delivers.

**Its own clock, and the trigger made due.** The sweep keeps `last_sweep` beside SPEC-090's
`last_flush`. When a sweep hands anything in, the drain sets `last_flush` back by one interval, so
the time trigger fires on the next take and the swept submission leaves with it. The loop's wait is
the smaller of the two remaining intervals, and is computed from `last_sweep` only when `_sweep` is
set. Measured on `spec091_latency.py` with the default 1.0 s interval: every event reached the sink
within 1.003 s of its level call, idle or with another thread closing a span every 2 ms. Two
alternatives were built and measured. A sweep tied to the time trigger never ran while the count
trigger kept firing: every sample hit the 5 s cap. A sweep that did not make the trigger due
waited for the next one: up to 2.006 s.

The hand-off and the hot path are unchanged in kind: the caller's thread builds the worker once,
as it did at a close, and never carries a sweep; the sweep reads span state only on the drain
thread and never calls sink code while it holds `_sweep_lock`.

#### Acceptance Criteria:

- [ ] Under a clock the test controls, an event logged inside the process's first open span, with
      no child span and no `flush()`, reaches the sink after the clock has advanced one interval
      and before it advances a second, idle and while another thread closes a span on every
      advance; with the sweep tied to the time trigger, or not making it due, the test fails
      (invariant 2).
- [ ] `p12_kill.py` prints `delivered=50` of the root span's 51 events for the `SIGKILL` case, the
      missing one being its `span.start`, where today it prints 0 (invariant 2).
- [ ] Inside the first traced call of a process, `_lifecycle._state._worker` is not `None` before
      the body runs, and a sink's stop-signal setter that calls `info()` during that build delivers
      the event on the orphan path (invariants 4 and 6).
- [ ] A `_get_worker` made to raise on the open is announced once, the call runs traced, and its
      close retries the build, as today (invariant 1).
- [ ] With `_hand_in` made to raise, the line is written after `_sweep_lock` is released, the
      drain thread keeps running, the taken events are in `in_span_lost`, and events logged after
      the fault leave at the next sweep; with the registry copy made to raise instead, nothing is
      taken and the events leave at the close (invariants 1 and 2).
- [ ] An idle worker with `_sweep` set makes no more `get()` calls than one per interval, and one
      built by hand without it behaves exactly as today in `tests/test_worker.py`'s busy-spin test
      (invariant 3).
- [ ] A swept submission handed in between a `shutdown()` moving the retirement count and the exit
      drain moves neither `submitted_after_shutdown` nor the stderr line, and is delivered
      (invariants 2 and 11).

### FR-003: `flush()`, `shutdown()`, a stopped worker, a sink swap and a fork each meet the sweep as stated

#### Description:

- **`flush()`** (`_lifecycle._flush_worker`, `_lifecycle.py:5720-5779`). It runs
  `_sweep_open_spans()` first, as today, so the caller's own spans ship whole with their
  `span.start` backfilled. Then, when a worker exists and that sweep did not fail, it runs
  `_sweep_registered` for every other registered span with a `hand_in` of
  `_enqueue(events, count_retired=True)`, under the same guard, and announces the totals and
  faults it hands back after the lock is released. A sweep that raises, or any `hand_in` fault,
  reports `"abandoned"`, as a failed own-context sweep does. The retirement reads are kept here: a
  `flush()` after `shutdown()` hands to a retired worker and is counted as such. This widens
  SPEC-036 FR-001's bound, "the calling context", which `contextvars` forced and the registry
  removes.
- **`shutdown()` and the exit drain.** `_final_drain` calls `_run_sweep()` once, as its first
  step, before SPEC-090's exit drain fixes the bound on the entries it will take (the entries the
  queue held when it began, SPEC-090 FR-002), so the swept submissions are inside that bound and
  are taken with the queue's tail, so the events open spans hold when the drain stops ship in the exit drain's
  batches, by SPEC-090's rule and under its budget. `span.start` stays held, so a span still open
  then loses its boundary events: counted in `submitted_after_shutdown` if it closes later, and in
  no counter if the process ends first, as a daemon thread's span does. Measured with `spec091_exit.py`: a
  daemon thread's span with 5 buffered events when the main thread returned delivered 5, where
  today it delivers 0.
- **A stopped worker.** Once the drain thread has ended, nothing sweeps. A span that closes then
  submits to the retired worker and is counted in `submitted_after_shutdown`, as today; a `flush()`
  then sweeps every registered span into that worker with the same count. A drain thread that died
  leaves the events on their spans, and their close strands them in its queue (SPEC-088's
  `stranded`).
- **`shutdown()` inside the process's first traced call.** The worker now exists when it runs, so
  it retires it, and that span's `span.start`, `span.end` and later logs are counted as submitted
  after shutdown, as they already are for every traced call after the first; the logs it had
  buffered ship in the exit drain. `spec091_exit.py`'s `inside` case delivers 5 of 8 where today it
  delivers all 8 through a worker built after the `shutdown()`, and its `inside-warm` case, with a
  worker already built, delivers 5 of 8 where today it delivers 0 of 8.
- **A sink swap.** A swept submission goes to whatever sink the worker emits it to, as any
  submission does, so a span's events can be split across a `configure(sink=…)`: those swept
  before the swap's fence reach the old sink and the rest the new one. `retarget` is unchanged.
- **`continue_trace()` after a log.** Its refusal now depends on timing: once a sweep, the
  worker's or another thread's `flush()`, has taken from the root span, a later
  `continue_trace()` there is refused, and a sweep runs every interval and on every `flush()`. In
  the review's adversarial runs, with a thread flushing in a loop, up to 143 of 144 calls made after
  a log were refused. Called on the entry point's first line, as its docstring says, it is never
  refused: nothing has been logged to take. The rejection line changes from "the current span has
  already been flushed; trace context refused" (`decorator.py:199`) to "the current span's events
  have already been handed to the worker; trace context refused", and
  `tests/test_inbound_totality.py:585` asserts the new text.
- **A sweep waits for an emit.** The sweep runs on the drain thread between emits, so while the
  drain is inside an emit, or inside a retry backoff, no sweep runs, and a process killed then
  loses everything logged on open spans since the last sweep, however long the emit has taken.
- **Threads and asyncio tasks.** The registry is process-wide, so every thread's and task's open
  spans are swept. A span that child tasks share through `contextvars` is one `Span` object,
  registered once, and the tasks' appends land on its one list.
- **A fork.** The child forgets every registered span before its drain thread resumes. The step
  is a handler, `_lifecycle._forget_open_spans_after_fork`, that imports `decorator` lazily and
  clears `_open_spans`, registered with `_fork.register_child_handler` in `_lifecycle.py` between
  ~~`:5911` (`_adopt_factory_after_fork`) and `:5912` (`_rebuild_worker_after_fork`)~~
  `_adopt_factory_after_fork` and `_rebuild_worker_after_fork` (revised 2026-11-15: SPEC-088
  registers `_restart_ledger_after_fork` between those two, so the order on its tree is
  `_reclaim_after_fork`, `_clear_after_fork`, `_adopt_factory_after_fork`,
  `_restart_ledger_after_fork`, `_rebuild_worker_after_fork`, `_lifecycle.py:6514-6518` at
  SPEC-088's `d0361104`). The step goes immediately before `_rebuild_worker_after_fork`, after the
  ledger's restart; either side of the restart would do, because the restart reads the ledger's
  cells and never the registry this step clears. Not from
  `decorator.py`: `decorator` imports `_lifecycle`, so a registration there runs after the rebuild,
  when the child's drain thread is already running; and not ahead of `_reclaim_after_fork`, which
  `tests/test_fork_lifecycle.py:2133-2137` requires to run first. Never `os.register_at_fork`
  (`tests/test_fork_lifecycle.py` holds the package to one registration). The child's sweep therefore never ships a parent's pending event, which
  is *A value the child inherits is stranded, never merely detached*, and the forking thread's
  copied spans ship at their close in the child as today. Measured with `spec091_fork.py`: the
  child wrote none of the parent's 8 pre-fork events; without the step it wrote all 8 again.

#### Acceptance Criteria:

- [ ] A `flush()` made outside every span while another thread's span holds 5 buffered logs
      delivers those 5 logs before it returns, its `span.start` staying held, and a `flush()` from
      inside a span does the same for a sibling task's span (invariant 2).
- [ ] `spec091_exit.py` prints `daemon-log` 5 for `exit`, `before-shutdown` 5 for `inside` and for
      `inside-warm`, with `after_shutdown=1` in both of the latter (invariants 2 and 5).
- [ ] After `shutdown()` has returned, a span that was open across it adds nothing to the sink when
      it closes and is counted in `submitted_after_shutdown` (invariant 2).
- [ ] `spec091_fork.py` reports no `child` row for any event logged before the fork, and a test
      forks while a sweep is due and finds nothing the parent buffered in the child's sink
      (invariant 12).
- [ ] A test reads `_fork._child_handlers` and fails unless the forgetting step comes after
      `_reclaim_after_fork` and before `_rebuild_worker_after_fork`; with the registration moved to
      `decorator.py` it fails naming the order, and so it does with the step registered first
      (invariant 12).
- [ ] A child forked from inside a traced call that logs and then leaves by `os._exit` delivers
      nothing of the inherited span, as today (invariant 12).
- [ ] Across a `configure(sink=…)` made while spans are open and being swept, the two sinks
      together receive every event exactly once (invariants 2 and 5).

### FR-004: The ledger accounts for every swept event in one term, and the harnesses count spans in `span.end` events

#### Description:

**SPEC-088's terms.** A sweep is a hand-over, like the close's and the owner's sweep. The events of
each span's take leave `open_spans`, the remainder, when that take's hand-over is credited, and the
credit is written after its `hand_in` returns, in the calling thread's own cell: the drain
thread's for a worker sweep, the flushing thread's for a `flush()`'s registry sweep. By then each event is in the queue's entering count, written in `_put`, or in
`overflow_lost`, so SPEC-088's meeting residual stays zero or negative while a sweep runs and zero
at rest. `queued` then holds them until the drain takes them into `in_flight`, and they leave by
`delivered`, `skipped` or a loss term as any queued event does. `in_span_lost` moves only for a take
whose `hand_in` raised, by that take's events, written before the fault is announced (FR-001); a
sweep that fails before it takes leaves its events on their spans, in `open_spans`. The
take and the hand-in happen in one hold of `_sweep_lock`, and the close's swap takes the same lock,
so an event is on its span or handed over and never both. That keeps every accepted event in
exactly one term. A held `span.start` stays in `open_spans` until its close. A forked child's
forgotten spans stay in its `open_spans`, as SPEC-088 FR-005 records for spans of other threads,
and `open_spans` stays a remainder: the registry is not exact for it (held events, a child's
forgotten spans, the append window).

~~**SPEC-088's snapshot read order is refined.** SPEC-088 FR-001 reads, cell by cell, each cell's
hand-over and then its span credits, and argues that a remainder gauge cannot read below zero
because, within one cell, the hand-over is written after the credit it debits. A sweep breaks the
premise: it credits its hand-over in the drain thread's or the flusher's cell, while the span
credits it debits sit in the owner's cell. A snapshot that walks the owner's cell first, then a
sweep that takes and credits, then the sweeper's cell, reads the owner's credit of 1 and the
sweeper's hand-over of 3, so `open_spans` reads 1 − 3 = −2 and `unaccounted` reads 2. So the
snapshot reads `in_span_lost` first, as SPEC-088 orders it, then **every cell's hand-over in one
pass, then every cell's span credits in a second pass**. The argument, restated: an event's span
credit is written by its owner before it appends the event; a sweep takes the event only after
that append, under `_sweep_lock`, and credits the hand-over only after the take. So for every
hand-over the first pass reads, the span credit it debits was written earlier still, and the
second pass, which runs later, reads it. A debit to `in_span_lost` is likewise written after its
credit and read before every credit. `open_spans` therefore reads zero or more while sweeps run.
The orphan cells and the worker's terms keep SPEC-088's order after the two passes. This is a
refinement of SPEC-088 FR-001's read order, made here because this spec is the one that adds a
hand-over in a cell other than the owner's.~~

**SPEC-088's snapshot read order already reads a sweep soundly** (revised 2026-11-15, after
SPEC-088 was built). The refinement above was built in SPEC-088 itself, because a copied context
such as `asyncio.to_thread` already hands over in a cell other than the owner's: an in-span log on
the executor thread is credited in that thread's cell while the close's hand-over is credited in
the owner's. As built, `log_foundry.worker._read_ledger` reads the process worker; then its
drain's liveness, taking count, held events and exits; then `in_span_lost`; then **every cell's
hand-over in one pass**; then the queue's entered count and `overflow_lost`; then **every cell's
span credits in a second pass**, from a fresh snapshot of the cells; then the orphan cells, cell by
cell; then `orphan_lost`. It differs from the sentence struck above in one place: the worker's
terms are read **before** the two passes, not after them, so that every snapshot has an `accepted`
no less than the worker path's exits and losses. The argument for a sweep is the one struck above,
unchanged: a sweep's hand-over is written after its take, the take after the append, the append
after the owner's credit, so the second pass reads every credit a first-pass hand-over debits.
What this spec still has to do is credit each sweep's hand-over in the sweeping thread's cell after
its `hand_in` returns, at the two new sites (FR-004's first paragraph); the read order needs no
change.

**SPEC-089's window** starts before `_sweep_open_spans()`, so the registry sweep that `flush()` now
makes runs inside it, and an overflow it causes is counted in that call's counts. A worker sweep's
loss inside the window is counted like any other loss there. SPEC-089's statement that events on
other threads' open spans are swept by no `flush()` is superseded: after this spec, the events a
`flush()` leaves in `open_spans` are only the held `span.start` events and what was appended after
its sweep. One SPEC-089 criterion asserts the old bound and inverts: "A span left open on another
thread with 2 buffered events is in no count, and `health().events.open_spans == 2` after the
call" (`docs/specs/SPEC-089-flush-loss-counts.md:213-215`). Its test, the one SPEC-089's build adds
with that assertion (`grep -rn 'events.open_spans == 2' tests/`), is rewritten: the other thread's
span buffers its `span.start` and 2 logs, the 2 logs are delivered by the call and are in its window,
and `health().events.open_spans == 1` after it, the held `span.start`.

**The harnesses.** Both span identities count submissions: `tests/test_invariants_model.py:567`,
`:605` and `:1034-1035` read `health().queued` less markers, and `tests/model_oracles.py:483`
reads the same. A swept submission holds no `span.end`, so with the prototype between 9 and 19 of
the model's 48 rows went red, depending on how many rows a sweep ran in, every one reading `queued`
above the spans left (Evidence). Each identity is
restated over `span.end` events: those delivered, plus those in submissions still in the worker's
queue, counted under its mutex, plus `dropped` and those abandoned, as today. `dropped` stays exact
there because a swept submission overflows only into a full queue of 10,000 submissions, which
neither harness comes near; a swept submission that did overflow would move `dropped` by one with
no `span.end` in it, and the build counts the `span.end` events of an overflowed submission instead
if a harness ever fills its queue. With that restatement all 48 rows pass, every failing one having
read between 1 and 4 above its spans before it.

**The SPEC-085 pin.** The eight `empty-submission` cells (`tests/model_pins.py:211-221`) pass at the
prototype: the setter's `flush` and `shutdown` now run before the span opens, so no sweep empties
a closing span's buffer. The defect is still reachable by a close whose open-time build failed, so
the build closes it as §12 says it is closed: `_flush` submits nothing when the list it detached is
empty. The pin is removed and its §12 item moved to Resolved.

#### Acceptance Criteria:

- [ ] With eight threads tracing, the worker's interval cut to 1 ms and a ninth thread calling
      `flush()` in a loop, every `health().events` snapshot has `unaccounted == 0`, and once the
      threads have joined and a `flush()` has returned, `pending == 0` (invariant 2).
- [ ] A snapshot held by an injected hook after it has read the owner's cell and before it reads
      the sweeper's, while a sweep takes 3 events from the owner's span and credits them, reads
      `unaccounted == 0` and `open_spans >= 0`; ~~with the cell-by-cell order of SPEC-088 as written
      it reads `unaccounted == 2`~~ with SPEC-088's two passes swapped, so the span credits are read
      before the hand-overs, and the snapshot held through `_ledger._between` in the span pass after
      the owner's cell (the owner holding `span.start` and 3 logs, of which the reader has seen 1),
      it reads `unaccounted == 2` (revised 2026-11-15: SPEC-088 built the two-pass order, so the
      cell-by-cell order exists only as this mutant) (invariant 2).
- [ ] A sweep of two open spans into a full queue moves `overflow_lost` by the events of both
      takes, `dropped` by 2 and `open_spans` down by the same events (invariant 2).
- [ ] With `_hand_in` made to raise for a span's take, `in_span_lost` moves by that take's events,
      `open_spans` moves down by the same, and no other term moves (invariant 2).
- [ ] A `flush()` whose registry sweep meets a full queue returns counts that include the
      overflowed events (invariant 2).
- [ ] `tests/test_invariants_model.py` and `tests/model_oracles.py` state each span identity over
      `span.end` events, and every model row passes; with the restatement reverted, a row that
      forces a sweep (a `flush()` from another thread while a span is open) fails (invariant 2).
- [ ] SPEC-089's `open_spans == 2` test is rewritten as described and passes (invariant 2).
- [ ] The `empty-submission` pin is gone, its eight cells pass, a close whose detached list is
      empty submits nothing, and every other cell's verdict equals the verdict at `a2cd4b36`
      (invariants 2 and 6).

### FR-005: The traced call's added cost is measured, and neither the open nor a level call takes a lock

#### Description:

What a traced call adds, and the whole list: at the open, one read of `_lifecycle._state._worker`,
one registry write, and `span.held = 1` in `_open_span`; at the close, one registry removal, a list
copy in the backfill, which runs only when baggage is set, and `_flush`'s `if events:` guard before
its submit. A level call adds nothing. Measured in isolation by `spec091_hot_path.py
isolated`, best of 7 over 2M iterations, on a machine other sessions loaded: the write 28 to 65 ns,
write and removal together 79 to 149 ns, the read 3 to 5 ns, and the copy of a three-event buffer
25 to 94 ns, on CPython 3.12.12 and 3.13.1. A whole traced call with one log measured 26 to 42 µs
on the unchanged tree and 26 to 32 µs on the prototype across three interleaved runs, so the
end-to-end runs could not resolve the change. No criterion states a ratio for that reason.

What a closing span may wait for: a sweep in progress, which holds `_sweep_lock` while it takes
from every registered span and hands each take in with a `put_nowait`, and never anything else.
No stderr write happens under the lock (FR-001), so a wedged console cannot lengthen the wait.
`spec091_hot_path.py hold`, on a quiet machine (load average 2), measured one sweep of 10, 100 and
1,000 spans of ten events each at 4.9, 43.5 to 43.9 and 428 to 433 µs on 3.13.1, and 5.4, 47.0 and
489 µs on 3.12.12. A close waits at most that once per interval, and only when it lands on a
sweep.

#### Acceptance Criteria:

- [ ] A traced call's body starts, and a level call inside it returns, while another thread holds
      `_sweep_lock`; the close, which takes it today, is not in that set (invariant 4).
- [ ] The delivery doc records `spec091_hot_path.py isolated` and `hold` run on the build on 3.12
      and 3.13, and the build adds no statement to the open, the close or `api._log` beyond the
      list above (invariant 4).
- [ ] With the worker's queue full and `sys.stderr` blocked on the sweep's overflow line, a traced
      call on another thread closes while that line is still pending, its close waiting on no event
      the stderr write sets; on the first cut of the prototype it did not close until stderr was
      released (invariants 3 and 4).
- [ ] Once a worker exists, the open calls no `_lifecycle` function: with `_get_worker` wrapped
      in a counter, 1,000 traced calls call it exactly 1,000 times, once per close as today
      (invariant 4).

### FR-006: The record names the decision, states what a kill loses, and refines what it contradicts

#### Description:

The completion ritual writes the record, and this FR is its checklist.

- **A new decision**, `docs/decisions/pipeline/an-open-spans-events-ship-on-the-workers-interval.md`.
  It quotes the question, whether an open span's events should leave before the span closes, and
  Andrew's answer of 2026-11-15, "Ship every ~1s (Recommended)", word for word. Its fence states
  FR-001's take rule, FR-002's clock, and that `span.start` stays held. Its body records the
  rejected alternatives with their measurements: a hand-off on the owner's next append, which a
  handler that logs and then blocks never makes; a sweep tied to the time trigger; a take that
  rebinds the list; one concatenated hand-in per sweep, which bypassed SPEC-090's cap; a stderr
  write under `_sweep_lock`, which stalled every close behind a wedged console; shipping
  `span.start` early; and a per-span lock, which the in-place take makes unnecessary. Its History is dated 2026-11-15 for the decision and the build's date for the
  mechanism.
- **Refined in place, each with a dated History entry naming this spec:**
  *`flush()` reports delivery, and answers from the drain that carried the events*
  (`docs/decisions/pipeline/`), whose "The bound is the calling context, because `contextvars`
  cannot enumerate another thread's" becomes every registered span; *Buffer-then-flush, background,
  non-blocking*, whose "span queue flushed at span end" gains the interval sweep; and *A value the
  child inherits is stranded, never merely detached*, which gains the child forgetting the registry.
  *Boundary events take the span's final baggage* and *Two drains, deliberately distinct* are kept
  unchanged, and the new decision says so.
- **`docs/architecture.md` §12.** The item "A span's events stay on the span until it closes, so a
  process killed mid-handler loses the whole root span" is struck as closed by this spec, with
  `p12_kill.py`'s numbers before and after. The `empty-submission` item moves to Resolved (FR-004).
  §13's "`Span.swept` is not a synchronization primitive" paragraph is struck as closed by FR-001's
  lock. §4's diagram, §9's three-places paragraph and its "Its bound is the calling context"
  paragraph (`architecture.md:575-600`) are rewritten.
- **What a kill loses, in plain words**, in `README.md`'s *Flushing and shutdown* section beside
  the Lambda list (`README.md:1455-1512`) and in `architecture.md` §13's Known constraints:
  `SIGKILL`, `SIGTERM` without a `shutdown()`, and `os._exit` still lose what is buffered. That is
  up to about one interval of an open span's events, a still-open span's `span.start`, and whatever
  is queued or in a batch the sink has not yet accepted, and no counter survives to show it. And
  because the sweep runs between emits, a kill that lands while the drain is inside an emit or a
  retry backoff loses everything logged on open spans since the last sweep, for as long as that
  emit has run.
- **`continue_trace()`'s timing**, in its docstring and the README's *Continuing a trace across
  processes* section: called after the root span has logged, it can be refused, more often the
  longer the call waits and always once a `flush()` has run; on the entry point's first line it is
  not. The rejection line's new text (FR-003) is the one the record quotes.
- **The README's other sentences**, at `6db09b86`: step 4 of *How it works* (`README.md:194-196`),
  *When the worker flushes* (`:219-246`), the sweep paragraph's calling-context bound (`:253-260`),
  `:470-475` ("sweeps every open span in the calling context"), `:1240-1245` ("it is a span closing,
  or a `flush()` sweeping one, that builds it"), `:1275` (the `dropped` row's "one per span close, or
  per open span a `flush()` sweeps", which gains a worker sweep's one per span's take) and the
  recipe's comment at `:1491-1493`. Each is rewritten for FR-001 to FR-003.
- **The other sites**, at `6db09b86`: `architecture.md:48` (the vocabulary's "queue … flushed
  together at span end"), `component-inventory.md:91` (the span-sweep row, which gains
  `_sweep_registered` and the registry), the public `flush()` docstring (`__init__.py:42-71`, whose
  "events submitted before this call" gains the open spans it sweeps), `Worker.submit`'s docstring
  (`worker.py:618`, "a finished span's events", and its split into `_enqueue` and `_announce`), and
  the docstrings of `Span`, `_sweep_open_spans`, `_flush_worker`, `_flush` and `continue_trace` that
  state the old bound or build site, each rewritten; and `ARCS.md:440` and `:474`, which say an open
  span's shipping still waits on Andrew, each given a superseded marker naming this spec and the
  new decision.
- **§12's finding-3 item** ("`shutdown()` called from a signal handler deadlocks for good when the
  signal interrupts a library lock section") adds `decorator._sweep_lock` to the locks it lists: a
  handler that calls `flush()` or closes a span on a thread inside a sweep waits on its own thread,
  and the drain thread now takes that lock every interval.
- **Invariant 2's observable.** SPEC-088 FR-001 restates it in events, and its last criterion
  removes the submission-unit sentence ("For spans, `span.end` delivered + `queued` + `dropped`
  equals spans started", `invariants.md:59-60`), which a swept submission would make false. This
  spec's record checks that the sentence is gone, and rewrites it over `span.end` events if the
  build finds it standing.
- **The release notes**, by `completion-ritual.md` step 6. A pending file exists today,
  `docs/release-notes/v1.2.1.md`, untagged. The entry goes in the pending file under
  `docs/release-notes/` if one exists on `main` when this spec completes, and otherwise under a
  "Release notes owed" heading in the delivery doc, as SPEC-082 did. It lists: an open span's events reach the sink about once an
  interval, before `span.start` and `span.end`; `flush()` reaches every thread's and task's open
  spans; the exit drain ships what open spans hold; the worker is built when the first span opens;
  a `shutdown()` inside the first traced call now strands that call's later events, counted;
  `continue_trace()` is refused once a sweep has taken from the root span, so a late call's verdict
  depends on timing, with the rejection line's new text; a take whose hand-in fails is counted in
  `in_span_lost`; `dropped` moves by one per open span a sweep could not hand in; a span's events
  can be split across a sink swap; and an empty batch is no longer submitted.

#### Acceptance Criteria:

- [ ] The new decision file exists, quotes the question and Andrew's answer word for word with its
      date, and `sh scripts/gen-index.sh` lists its fence under `pipeline`; `sh scripts/docs-lint.sh`
      passes.
- [ ] The three refined decisions carry a dated History entry naming this spec, and the two kept
      ones are named in the new decision.
- [ ] The §12 finding-8 item is struck as closed with `p12_kill.py`'s before and after, the
      `empty-submission` item is under Resolved, and the §13 `Span.swept` paragraph is struck
      (invariant 2).
- [ ] `README.md` and `architecture.md` §13 each contain the sentence that `SIGKILL`, `SIGTERM`
      without a `shutdown()` and `os._exit` lose up to about one interval of an open span's events
      plus what is queued or not yet accepted, and that a kill during an emit or a backoff loses
      everything since the last sweep; and each README site listed above is rewritten
      (invariant 2).
- [ ] Each of the other sites listed above is rewritten, `ARCS.md:440` and `:474` each carry a
      superseded marker, and §12's finding-3 item names `decorator._sweep_lock` (invariant 3).
- [ ] `docs/invariants.md` states no span identity in submissions (invariant 2).
- [ ] `continue_trace()`'s docstring and the README say that a call after the root span has logged
      can be refused, and quote the new rejection text (invariant 9).
- [ ] The release-notes entry exists, in the pending file under `docs/release-notes/` if one exists
      on `main` when this spec completes and otherwise under a "Release notes owed" heading in the
      delivery doc, and names each change listed above (invariant 10).

---

## Data Model

```python
# src/log_foundry/model.py
@dataclass
class Span:
    ...                     # unchanged fields
    held: int = 0           # leading buffered events a worker sweep leaves: 1 while span.start is
                            # buffered, 0 once an in-span flush() shipped it

# src/log_foundry/decorator.py, private
_open_spans: dict[int, Span] = {}   # id(span) -> span, from push to the start of its close

# src/log_foundry/worker.py, private attributes of Worker
_sweep: Callable[[HandIn], SweepResult] | None

# the shapes, private, in decorator.py
Totals = tuple[int, int]            # (dropped total moved, or 0; submitted_after_shutdown total, or 0)
HandIn = Callable[[list[dict[str, object]]], Totals]
SweepResult = tuple[int, list[Totals], list[Exception]]   # events handed in, totals, hand-in faults
```

## API / Interface Contract

```python
# decorator.py, private: the worker sweep. Holds _sweep_lock for the whole pass, calls hand_in
# once per span's take under it, debits a take whose hand_in raised to in_span_lost, writes no
# stderr, and hands back what the caller announces after the lock is released.
def _sweep_registered(hand_in: HandIn) -> SweepResult: ...
def _forget_open_spans_in_child() -> None: ...       # clears _open_spans

# worker.py
class Worker:
    def __init__(self, sink, *, batch_size=10, flush_interval=1.0, max_queue=10_000,
                 max_retries=3, offer_signal=True, sweep=None) -> None: ...
    def submit(self, events) -> None: ...             # self._announce(self._enqueue(events,
                                                      # count_retired=True)), unchanged to callers
    def _enqueue(self, events, *, count_retired: bool) -> Totals: ...  # no stderr
    def _announce(self, totals: Totals) -> None: ...  # the throttled lines; no library lock held
    def _hand_in(self, events) -> Totals: ...         # _enqueue(events, count_retired=False)
    def _run_sweep(self) -> int: ...                  # _sweep(_hand_in), then the announcements

# _lifecycle.py
worker = Worker(sink, offer_signal=False, sweep=_sweep_registered)   # the one caller of sweep=
def _forget_open_spans_after_fork() -> None: ...     # lazy import; registered just before
                                                     # _rebuild_worker_after_fork (FR-003)
```

## File & Folder Structure

```
src/log_foundry/
├── decorator.py      # _open_spans, _sweep_registered, the open-time build, the close's removal,
│                     # continue_trace under _sweep_lock and its new line, _forget_open_spans_in_child,
│                     # _flush's empty guard, _sweep_open_spans announcing after the lock
├── model.py          # Span.held; backfill_baggage over a snapshot
├── worker.py         # sweep=, _enqueue, _announce, _hand_in, _run_sweep, the sweep clock in _drain,
│                     # one sweep at the start of _final_drain
└── _lifecycle.py     # _get_worker passes sweep; _flush_worker's registry sweep; the fork handler
tests/                # new tests for FR-001 to FR-005; the harness, roster and pin changes FR-004 and
                      # the Evidence list
docs/                 # FR-006's record
```

## Implementation Phases

### Phase 1: The registry and the take (FR-001)

- `_open_spans`, `Span.held`, `_sweep_registered` with one hand-in per span and the fault debit,
  `submit` split into `_enqueue` and `_announce` with every line written after the lock, the
  close's removal first, the backfill snapshot, `continue_trace` under the lock.
- The exactly-once tests, their forced mutants, the AST site-rule test.

### Phase 2: The worker sweeps (FR-002, FR-003)

- The open-time build, `sweep=`, `_run_sweep`, `_hand_in`, the clock in `_drain`, the exit
  drain's sweep, `flush()`'s registry sweep, the child's forgetting.
- The roster updates the prototype's suite run named (Evidence), re-measured on the tree with
  SPEC-088 to SPEC-090 landed: `tests/test_runtime_holder.py`'s lock sites (the overflow count's
  `self._lock` moving from `Worker.submit` to `Worker._enqueue`), `tests/test_worker_predicate_roster.py`'s
  new predicates, `tests/test_lifecycle_inventory.py`'s `Worker._sweep` row, and the premises of
  `tests/test_span_sweep.py::test_a_sweep_that_finds_events_creates_the_worker`,
  `::test_the_sweep_reaches_only_the_calling_context` and
  `tests/test_lifecycle_transitions.py::test_a_worker_a_flush_sweep_builds_moves_the_state_by_the_build_row`,
  each rewritten for the build at the open and the widened bound.

### Phase 3: The ledger and the harnesses (FR-004, FR-005)

- The hand-over credit at the two new sites, ~~the two-pass snapshot read order~~ (built by
  SPEC-088), the restated span
  identities, SPEC-089's inverted test, the pin removed and `_flush`'s empty guard, the cost tests
  and measurements.

### Phase 4: The record (FR-006)

- The decision, the refinements, §12 and §13, the README and docstrings, the release notes owed.

## Evidence

Measured on 2026-11-15 on macOS (Darwin, 10 cores) with CPython 3.13.1 and, where marked, 3.12.12,
while other sessions loaded the machine (load averages 2 to 34). Every premise about today's code
is cited at `a2cd4b36`; `src/` is identical at `6db09b86`, the base this spec was rebased onto. No
change to `src/` is committed.

**What this was not measured against.** SPEC-088, SPEC-089 and SPEC-090 are Draft specs this one
depends on, and none is built at `a2cd4b36`. So every number below is against a tree without the
event ledger, the flush window's counts or the backlog batching; no ledger criterion here could be
run, and the suite and cell lists below are the prototype's against today's tree. **The build
re-measures both lists on the tree with SPEC-088 to SPEC-090 landed**, and the delivery doc records
what differs.

**The prototype, in two cuts.** The **first cut**, committed with this spec's first draft at
`53dceb58`, concatenated every span's take into one hand-in, wrote `_hand_in`'s overflow line under
`_sweep_lock`, said a raising hand-in left its events on their spans, and registered its fork step
with `os.register_at_fork` from `decorator.py`. The spec review found all four, and the **second
cut** is the mechanism this revision specifies:
`docs/audits/2026-11-14-probes/core-pipeline/spec091_prototype.py` now rewrites a scratch copy of
`src/log_foundry` at `a2cd4b36` into it, and applied to a fresh `git archive a2cd4b36 src` it
reproduces the measured tree byte for byte. It leaves out what needs SPEC-088 (the credits, the
cells and the snapshot), the AST site-rule test and the new rejection text. Where a row below
says only "prototype", the second cut was measured; the first cut's numbers are marked.

**The review's harnesses**, kept in the session's scratch and not committed: `wedged.py` (a
traced call's close against a sweep whose overflow line blocks on stderr), `raise_handin.py` (a
`hand_in` that raises), `idle_gets.py` (the drain's `get()` calls when idle) and `adversary.py`
(random pauses injected at twelve step boundaries of a hooked first cut, with nested spans, in-span
flushes, `continue_trace()`, spans shared by threads and by tasks, an outside flusher, a 1 ms
interval and a switch interval of 1e-6). `adversary.py`'s ten runs were clean: no duplicate,
missing, split or early event. Its `continue_trace()` calls made after a log were refused in up to
143 of 144 cases with a busy flusher. The take it exercised is unchanged in the second cut.

**The probes**, committed under `docs/audits/2026-11-14-probes/core-pipeline/`, each headed by what
it shows: `spec091_exactly_once.py`, `spec091_latency.py`, `spec091_fork.py`, `spec091_exit.py`,
`spec091_continue_trace.py` and `spec091_hot_path.py`. `p12_kill.py` is the audit's.

| Probe | `a2cd4b36` | Prototype |
|---|---|---|
| `p12_kill.py`, `SIGKILL` 3.5 s into the root span | root 0 of 51, children 9 of 9 | root 50 of 51 (`span.start` held), children 9 of 9, in both cuts |
| `p12_kill.py`, `SIGTERM` after five closed spans | 0 of 15 | 0 of 15: queued, not open |
| `spec091_exactly_once.py`, 36,015 events | `dup=0 missing=0`, 0 shipped early | first cut: 6 runs on 3.13 (3 without the switch interval) and 1 on 3.12; second cut: 3 runs on 3.13; every run `dup=0 missing=0 bad_boundary=0 end_not_last=0`, 4,570 to 21,348 shipped before their close |
| the same, `swap` | — | first cut 3 runs, second cut 1: `dup=0 missing=0 bad_boundary=0` |
| `wedged.py`, queue full, stderr blocked | close at 0.00 s | first cut: not closed after 4.01 s; second cut: closed at 0.00 s |
| `raise_handin.py`, 5 logs | — | first cut: 0 of 5 delivered, `in_span_lost=0`, "their events stay on the spans"; second cut: 0 of 5, `in_span_lost=5`, "the events taken were lost" |
| `spec091_latency.py`, idle and `busy` | every sample at the 5 s cap | first cut, 8 samples: max 1.003 s idle, 1.000 s busy; second cut, 6: 1.009 s and 1.000 s |
| `spec091_fork.py` | child writes nothing | child writes nothing, in both cuts |
| `spec091_exit.py` `exit` / `inside` / `inside-warm` | 0 / all 8 / 0 of 8 delivered | 5 / 5 of 8 / 5 of 8 delivered, in both cuts |
| `spec091_continue_trace.py`, 20 trials | — | `split_spans=0 refused=0`, in both cuts |

**The mutants**, each one edit to the first cut, whose take, clock and lock rules the second cut keeps:

| Edit | Result |
|---|---|
| the take rebinds `span.events` instead of deleting in place | `missing=1` in 1 of 3 runs |
| no membership read before the take | `end_not_last` 25 and 29, `bad_boundary` 3 and 7 |
| `span.held` ignored (take from index 0) | `bad_boundary` 1,391 and 1,557 |
| backfill over the live list, with a 0.2 ms pause between the check and the take | `bad_boundary` 1 and 3; with the snapshot, 0 and 0 |
| backfill over the live list, no pause | 0 in 2 runs: the window is too narrow to meet unforced |
| the sweep on the time trigger's clock (`latency`) | every sample at the 5 s cap, `busy` and idle |
| the sweep not making the time trigger due (`latency`) | max 2.006 s |
| no fork step (`fork`) | the child writes all 8 of the parent's pre-fork events again |
| (second cut) the fork handler's order | `_reclaim_after_fork`, `_clear_after_fork`, `_adopt_factory_after_fork`, `_forget_open_spans_after_fork`, `_rebuild_worker_after_fork`, on a tree without SPEC-088, whose `_restart_ledger_after_fork` now sits between `_adopt_factory_after_fork` and the forgetting step (FR-003) |
| no lock around `continue_trace`'s check and re-parent | `split_spans=19 refused=1` |

**The SPEC-085 cells** (`tests/test_model_cells.py`). `a2cd4b36`: 451 passed, 21 expected
failures. An early draft of the first cut, which built the worker after the span's push and had no
empty guard in `_flush`: 6 failed, four of the eight `empty-submission` cells (the `flush` before
`shutdown` orders), each reporting "the pin 'empty-submission' no longer shows its defect", and
`reentrant-setter-worker-shutdown+info` and `shutdown-none+info` at `LedgerBroken:orphans-1`. The
first cut and the second: 8 failed, the eight pinned cells for that reason alone, 451 passed, 13
expected failures, so every other verdict is unchanged (the second cut in each of two suite runs).
The first cut with `flush_interval` defaulted to 2 ms, so that the drain thread sweeps during each
cell: the same 8, 451 and 13, against 451 passed and 21 expected failures on `a2cd4b36` at 2 ms.
The cells' traced calls are short, so they meet the sweep incidentally; `spec091_exactly_once.py`
and the review's `adversary.py` are what drive it.

**The full suite** (`pytest -n 8`, from a `git archive` of the base with the prototype's `src/`).
`a2cd4b36`: 10,683 passed, 35 skipped, 21 expected failures. The first cut: 24 failed in the
review's run and 33 in this session's; the second cut: 33 and 32 failed in two runs, with 10,658
and 10,659 passed, 35 skipped and 13 expected failures. The count moves with one test:

- **9 to 19 rows** of `test_invariants_model.py::test_the_accounting_identity_holds_under_every_interleaving`
  (9 in the review's run, 18 in this session's first-cut run, 19 and 18 in the second cut's), each
  the submission-unit span identity reading a swept submission, the number depending on how many
  rows a sweep happened to run in. All 75 tests of that file pass once the identity counts
  `span.end` events (FR-004).

The rest are the same in every run, 15 for the first cut and 14 for the second:

- the eight pinned `empty-submission` cells (FR-004);
- `test_span_sweep.py::test_a_sweep_that_finds_events_creates_the_worker` and
  `::test_the_sweep_reaches_only_the_calling_context`, and
  `test_lifecycle_transitions.py::test_a_worker_a_flush_sweep_builds_moves_the_state_by_the_build_row`,
  whose premises FR-002 and FR-003 change;
- three rosters: `test_runtime_holder.py` (the overflow count's `self._lock` moved from
  `Worker.submit` to `Worker._enqueue`; in the first cut, a new site in `Worker._hand_in`),
  `test_worker_predicate_roster.py` (the new predicates in `_begin`, `_flush_worker`,
  `_sweep_open_spans` and `_sweep_registered`) and `test_lifecycle_inventory.py` (`Worker._sweep`);
- in the first cut only, `test_fork_lifecycle.py::test_nothing_else_in_the_package_registers_a_fork_handler[decorator.py]`,
  for its own `os.register_at_fork`, which the second cut moved to `_lifecycle` as FR-003 says.

An earlier draft also failed `test_worker.py`'s busy-spin test, 384,494 `get()` calls, because the
wait read the sweep clock on a worker with no sweep; FR-002 reads it only when `_sweep` is set. As
the opening paragraph says, the build re-measures these lists with SPEC-088 to SPEC-090 landed.

**Cost** (FR-005), `spec091_hot_path.py`: `isolated` on 3.13.1, excluding three runs made while the full suite ran, which read two to four
times higher: register 31 to 65 ns, register and
remove 79 to 149 ns, worker read 3 to 5 ns, snapshot of three events 64 ns (26 to 28 ns in a
separate run); on 3.12.12, register 28 to 56 ns, register and remove 100 to 107 ns, snapshot 94 ns.
`hold`, first cut, loaded, with a hand-in that did nothing: 7.5, 74 and 308 µs on 3.13.1 and 7.4,
80 and 336 µs on 3.12.12, for 10, 100 and 1,000 spans. `hold` with a `put_nowait` per hand-in, on a
quiet machine (load average 2): one concatenated put (first cut) 2.5 to 2.7, 17.6 to 17.7 and 157
to 159 µs; one put per span (second cut) 4.9, 43.5 to 43.9 and 428 to 433 µs on 3.13.1, and 5.4,
47.0 and 489 µs on 3.12.12. Per span costs about 2.7 times the hold at 1,000 spans, at most half a
millisecond once an interval; FR-001 takes it for the cap.
`whole`, interleaved: 41.90, 32.82 and 25.98 µs on `a2cd4b36` against 32.47, 29.06 and 25.68 µs on
the prototype.

## Revision history

- **2026-11-15, revised in place after SPEC-088 was built**, each change struck in its section.
  SPEC-088 already reads its snapshot in two passes, with the worker's terms read before them
  rather than after, so FR-004's refinement is restated as built and this spec's remaining work
  is the sweep's hand-over credit (the scope bullet and Phase 3 follow it). The fork order now
  names SPEC-088's `_restart_ledger_after_fork`, and the stale `:5911`/`:5912` citations are
  replaced by function names. FR-004's `unaccounted == 2` criterion is kept as a mutant that
  swaps the two passes.

- **2026-11-15, after the spec review.** Thirteen findings, all taken. The mechanism changed in
  four places, each prototyped and measured as the second cut (Evidence): no stderr write under
  `_sweep_lock` (`submit` split into `_enqueue` and `_announce`, and `_sweep_open_spans`' own write
  moved out); a fault after a take debited to `in_span_lost`; one hand-in per span, for SPEC-090's
  cap; and the fork step registered in `_lifecycle` between `_adopt_factory_after_fork` and
  `_rebuild_worker_after_fork`. The spec now also refines SPEC-088 FR-001's snapshot read order to
  two passes, rewrites SPEC-089's inverted criterion, lists the operations that must be atomic with
  an AST site-rule test and leaves the free-threaded case unproven, places the open-time build
  ahead of SPEC-088's `span.start` credit and the exit drain's sweep ahead of SPEC-090's bound,
  states the kill loss during an emit and `continue_trace()`'s timing with a new rejection line,
  names the missed record sites and the pending `v1.2.1.md`, makes four criteria deterministic, and
  corrects the suite counts, which the first draft gave as one run's 33 where the review measured
  24. The optional data-based guard for an early `span.end` was not added: the membership read is
  measured and forced by FR-001's criterion, and a data check would add a read per take.
