---
type: Spec
id: SPEC-091
title: "An Open Span's Events Ship on the Worker's Interval"
description: "Has the worker take every open span's buffered events about once a flush interval, on its own thread and exactly once, so a process killed mid-handler loses at most about one interval of a span's events rather than the whole span, and states plainly what a kill still loses."
spec_status: Draft
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
  buffer: in place, never its `span.start`, never once its close has begun, and exactly once
  against every other reader and writer of that buffer (FR-001).
- The worker built before the first span opens, and a sweep on the drain thread on a clock of its
  own that makes the time trigger due, so an event leaves within about one interval (FR-002).
- `flush()`, `shutdown()`, the exit drain, events recorded after the worker has stopped, sink
  swaps and a forked child, each stated against the sweep (FR-003).
- The sweep in SPEC-088's event ledger and SPEC-089's flush window, the model harnesses' span
  identity restated in `span.end` events, and the SPEC-085 pin the move resolves (FR-004).
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
- **A free-threaded interpreter as a supported build.** FR-001 says what the take relies on there,
  and it was not measured on one.
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
takes `span.events[held:n]` and deletes that slice from the same list. It sets `swept` on any span
it took from, concatenates every take into one submission, calls `hand_in` with it while still
holding the lock, and returns the number of events taken.

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

**What the GIL is and is not relied on for.** The take, the append and the registry write are
each one list or dict operation, which the GIL makes atomic and a free-threaded build makes atomic
with each object's own lock. The ordering argument above needs only that the owner's registry
removal is visible before its `span.end` append, which both builds give one thread's program
order. No free-threaded run was made: the interpreter on this machine was not usable.

#### Acceptance Criteria:

- [ ] `spec091_exactly_once.py` reports `dup=0 missing=0 bad_boundary=0 end_not_last=0` and a
      positive `shipped_before_their_close` in each of five runs, with and without `swap`
      (invariants 2 and 6).
- [ ] With the take replaced by a slice-and-rebind, with the membership read removed, and with
      the backfill iterating the live list under an injected pause between the check and the take,
      a test fails naming the lost event, the early `span.end` and the boundary event without its
      baggage, respectively; each is restored and the test passes (invariant 2).
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

**The build moves to the open.** `_begin` reads `_lifecycle._state._worker` before it opens the
span, and when it is `None` calls `_lifecycle._get_worker()` in a guard of its own that announces
a fault by type ("starting the worker for a span") and goes on to open the span. The build runs
**before** the span is pushed, so the sink's stop-signal setter, which the build runs, logs on the
orphan route as it does today at the close, where the span is already `closed`: built after the
push, the setter's `info()` became an in-span event and two SPEC-085 cells
(`reentrant-setter-worker-shutdown+info` and `shutdown-none+info`) went red as `LedgerBroken:orphans-1`.
A build that fails is retried by the close, as today, which counts the buffer in `in_span_lost` if
it fails again (`decorator.py:687`).

**The sweep runs on the drain thread.** `Worker.__init__` takes a keyword `sweep`, a callable or
`None`, stored as `_sweep`; `_get_worker` passes `decorator._sweep_registered` and nothing else
passes one, so a worker a test builds by hand sweeps nothing and `worker.py` still knows nothing of
spans. At the top of each pass of the drain loop, when `_sweep` is set and `flush_interval` has
passed since the last sweep, `_run_sweep()` calls it with `_hand_in` and absorbs any `Exception` it
raises, announced by type, leaving the events on their spans. `_hand_in` is `submit` without
the two retirement reads: it puts the submission with `put_nowait` and counts a full queue in
`dropped` exactly as `submit` does. The retirement reads are left out because the drain thread
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
- [ ] A sweep made to raise is announced by type, the drain thread keeps running, and the events
      stay on their spans and leave at the close (invariants 1 and 2).
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
  `_sweep_registered(worker.submit)` for every other registered span, under the same guard, so a
  failure reports `"abandoned"` as a failed own-context sweep does. `submit`'s retirement reads are
  kept here: a `flush()` after `shutdown()` hands to a retired worker and is counted as such. This
  widens SPEC-036 FR-001's bound, "the calling context", which `contextvars` forced and the
  registry removes.
- **`shutdown()` and the exit drain.** `_final_drain` calls `_run_sweep()` once before it takes
  the queue's tail, so the events open spans hold when the drain stops ship in the exit drain's
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
- **Threads and asyncio tasks.** The registry is process-wide, so every thread's and task's open
  spans are swept. A span that child tasks share through `contextvars` is one `Span` object,
  registered once, and the tasks' appends land on its one list.
- **A fork.** The child forgets every registered span before its drain thread resumes: a handler
  registered through `_fork.register_child_handler` ahead of `_rebuild_worker_after_fork`
  (`_lifecycle.py:5909-5912`), never `os.register_at_fork` (`tests/test_fork_lifecycle.py` holds
  the one registration). The child's sweep therefore never ships a parent's pending event, which
  is *A value the child inherits is stranded, never merely detached*, and the forking thread's
  copied spans ship at their close in the child as today. Measured with `spec091_fork.py`: the
  child wrote none of the parent's 8 pre-fork events; without the step it wrote all 8 again.

#### Acceptance Criteria:

- [ ] A `flush()` made outside every span while another thread's span holds 5 buffered events
      delivers those 5 before it returns, and a `flush()` from inside a span does the same for a
      sibling task's span (invariant 2).
- [ ] `spec091_exit.py` prints `daemon-log` 5 for `exit`, `before-shutdown` 5 for `inside` and for
      `inside-warm`, with `after_shutdown=1` in both of the latter (invariants 2 and 5).
- [ ] After `shutdown()` has returned, a span that was open across it adds nothing to the sink when
      it closes and is counted in `submitted_after_shutdown` (invariant 2).
- [ ] `spec091_fork.py` reports no `child` row for any event logged before the fork, and a test
      forks while a sweep is due and finds nothing the parent buffered in the child's sink
      (invariant 12).
- [ ] A child forked from inside a traced call that logs and then leaves by `os._exit` delivers
      nothing of the inherited span, as today (invariant 12).
- [ ] Across a `configure(sink=…)` made while spans are open and being swept, the two sinks
      together receive every event exactly once (invariants 2 and 5).

### FR-004: The ledger accounts for every swept event in one term, and the harnesses count spans in `span.end` events

#### Description:

**SPEC-088's terms.** A sweep is a hand-over, like the close's and the owner's sweep. The events it
takes leave `open_spans`, the remainder, when the hand-over is credited, and the credit is written
after `_hand_in` or `submit` returns, in the calling thread's own cell, the drain thread's for a
worker sweep. By then each event is in the queue's entering count, written in `_put`, or in
`overflow_lost`, so SPEC-088's meeting residual stays zero or negative while a sweep runs and zero
at rest. `queued` then holds them until the drain takes them into `in_flight`, and they leave by
`delivered`, `skipped` or a loss term as any queued event does. `in_span_lost` does not move: a
sweep builds nothing, and one that fails leaves its events on their spans, in `open_spans`. The
take and the hand-in happen in one hold of `_sweep_lock`, and the close's swap takes the same lock,
so an event is on its span or handed over and never both. That keeps every accepted event in
exactly one term. A held `span.start` stays in `open_spans` until its close. A forked child's
forgotten spans stay in its `open_spans`, as SPEC-088 FR-005 records for spans of other threads,
and `open_spans` stays a remainder: the registry is not exact for it (held events, a child's
forgotten spans, the append window).

**SPEC-089's window** starts before `_sweep_open_spans()`, so the registry sweep that `flush()` now
makes runs inside it, and an overflow it causes is counted in that call's counts. A worker sweep's
loss inside the window is counted like any other loss there. SPEC-089's statement that events on
other threads' open spans are swept by no `flush()` is superseded: after this spec, the events a
`flush()` leaves in `open_spans` are only the held `span.start` events and what was appended after
its sweep.

**The harnesses.** Both span identities count submissions: `tests/test_invariants_model.py:567`,
`:605` and `:1034-1035` read `health().queued` less markers, and `tests/model_oracles.py:483`
reads the same. A swept submission holds no `span.end`, so with the prototype 18 of the model's 48
rows went red, every one reading `queued` above the spans left (Evidence). Each identity is
restated over `span.end` events: those delivered, plus those in submissions still in the worker's
queue, counted under its mutex, plus `dropped` and those abandoned, as today. `dropped` stays exact
there because a swept submission overflows only into a full queue of 10,000 submissions, which
neither harness comes near. With that restatement all 48 rows pass, every one of the 18 having
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
- [ ] A sweep into a full queue moves `overflow_lost` by the events it took and `dropped` by 1, and
      nothing else (invariant 2).
- [ ] A sweep made to raise leaves `open_spans` where it was and moves no other term (invariant 2).
- [ ] A `flush()` whose registry sweep meets a full queue returns counts that include the
      overflowed events (invariant 2).
- [ ] `tests/test_invariants_model.py` and `tests/model_oracles.py` state each span identity over
      `span.end` events, and every model row passes; with the restatement reverted, a row with a
      swept submission fails (invariant 2).
- [ ] The `empty-submission` pin is gone, its eight cells pass, a close whose detached list is
      empty submits nothing, and every other cell's verdict equals the verdict at `a2cd4b36`
      (invariants 2 and 6).

### FR-005: The traced call's added cost is measured, and neither the open nor a level call takes a lock

#### Description:

What a traced call adds: one registry write at the open, one registry removal at the close, one
read of `_lifecycle._state._worker` at the open, and a list copy in the backfill, which runs only
when baggage is set. A level call adds nothing. Measured in isolation by `spec091_hot_path.py
isolated`, best of 7 over 2M iterations, on a machine other sessions loaded: the write 28 to 65 ns,
write and removal together 79 to 149 ns, the read 3 to 5 ns, and the copy of a three-event buffer
25 to 94 ns, on CPython 3.12.12 and 3.13.1. A whole traced call with one log measured 26 to 42 µs
on the unchanged tree and 26 to 32 µs on the prototype across three interleaved runs, so the
end-to-end runs could not resolve the change. No criterion states a ratio for that reason.

What a closing span may wait for: a sweep in progress, which holds `_sweep_lock` while it takes
from every registered span. `spec091_hot_path.py hold` measured one sweep of 10, 100 and 1,000
spans of ten events each at 7, 74 to 80, and 308 to 336 µs. A close waits at most that once per
interval, and only when it lands on a sweep.

#### Acceptance Criteria:

- [ ] A traced call's body starts, and a level call inside it returns, while another thread holds
      `_sweep_lock`; the close, which takes it today, is not in that set (invariant 4).
- [ ] The delivery doc records `spec091_hot_path.py isolated` and `hold` run on the build on 3.12
      and 3.13, and the build adds no statement to the open, the close or `api._log` beyond those
      this FR lists (invariant 4).
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
  rebinds the list; shipping `span.start` early; and a per-span lock, which the in-place take makes
  unnecessary. Its History is dated 2026-11-15 for the decision and the build's date for the
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
  is queued or in a batch the sink has not yet accepted, and no counter survives to show it.
- **The README's other sentences**, at `6db09b86`: step 4 of *How it works* (`README.md:194-196`),
  *When the worker flushes* (`:219-246`), the sweep paragraph's calling-context bound (`:253-260`),
  `:470-475` ("sweeps every open span in the calling context"), `:1240-1245` ("it is a span closing,
  or a `flush()` sweeping one, that builds it") and the recipe's comment at `:1491-1493`. Each is
  rewritten for FR-001 to FR-003, and so are the docstrings of `Span`, `_sweep_open_spans`,
  `_flush_worker`, `_flush` and `continue_trace` that state the old bound or build site.
- **Release notes owed.** No pending file exists under `docs/release-notes/`, so, as SPEC-082
  did, the delivery doc carries a "Release notes owed" heading, or the pending file if one exists
  on `main` when this spec completes. It lists: an open span's events reach the sink about once an
  interval, before `span.start` and `span.end`; `flush()` reaches every thread's and task's open
  spans; the exit drain ships what open spans hold; the worker is built when the first span opens;
  a `shutdown()` inside the first traced call now strands that call's later events, counted;
  `continue_trace()` is refused once a sweep has taken from the root span; a span's events can be
  split across a sink swap; and an empty batch is no longer submitted.

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
      plus what is queued or not yet accepted, and each README site listed above is rewritten
      (invariant 2).
- [ ] The release-notes entry exists where the description says and names each listed change
      (invariant 10).

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
_sweep: Callable[[Callable[[list[dict[str, object]]], None]], int] | None
```

## API / Interface Contract

```python
# decorator.py, private: the worker sweep. Holds _sweep_lock for the whole pass and calls
# hand_in once, under it, with every event it took. Returns how many it took.
def _sweep_registered(hand_in: Callable[[list[dict[str, object]]], None]) -> int: ...

# worker.py
class Worker:
    def __init__(self, sink, *, batch_size=10, flush_interval=1.0, max_queue=10_000,
                 max_retries=3, offer_signal=True, sweep=None) -> None: ...
    def _run_sweep(self) -> int: ...      # calls _sweep(_hand_in), absorbing an Exception
    def _hand_in(self, events: list[dict[str, object]]) -> None: ...  # submit without the
                                          # retirement reads; a full queue counts in dropped

# _lifecycle.py: the one caller that passes sweep
worker = Worker(sink, offer_signal=False, sweep=_sweep_registered)
```

## File & Folder Structure

```
src/log_foundry/
├── decorator.py      # _open_spans, _sweep_registered, the open-time build, the close's removal,
│                     # continue_trace under _sweep_lock, the child's forgetting, _flush's empty guard
├── model.py          # Span.held; backfill_baggage over a snapshot
├── worker.py         # sweep=, _run_sweep, _hand_in, the sweep clock in _drain, one sweep in _final_drain
└── _lifecycle.py     # _get_worker passes sweep; _flush_worker's registry sweep
tests/                # new tests for FR-001 to FR-005; the harness, roster and pin changes FR-004 and
                      # the Evidence list
docs/                 # FR-006's record
```

## Implementation Phases

### Phase 1: The registry and the take (FR-001)

- `_open_spans`, `Span.held`, `_sweep_registered`, the close's removal first, the backfill
  snapshot, `continue_trace` under the lock.
- The exactly-once tests and their three mutants.

### Phase 2: The worker sweeps (FR-002, FR-003)

- The open-time build, `sweep=`, `_run_sweep`, `_hand_in`, the clock in `_drain`, the exit
  drain's sweep, `flush()`'s registry sweep, the child's forgetting.
- The roster updates the prototype's suite run named (Evidence): `tests/test_runtime_holder.py`'s
  lock sites (`Worker._hand_in`, `self._lock`), `tests/test_worker_predicate_roster.py`'s three new
  predicates, `tests/test_lifecycle_inventory.py`'s `Worker._sweep` row, and the premises of
  `tests/test_span_sweep.py::test_a_sweep_that_finds_events_creates_the_worker`,
  `::test_the_sweep_reaches_only_the_calling_context` and
  `tests/test_lifecycle_transitions.py::test_a_worker_a_flush_sweep_builds_moves_the_state_by_the_build_row`,
  each rewritten for the build at the open and the widened bound.

### Phase 3: The ledger and the harnesses (FR-004, FR-005)

- The hand-over credit at the two new sites, the restated span identities, the pin removed and
  `_flush`'s empty guard, the cost tests and measurements.

### Phase 4: The record (FR-006)

- The decision, the refinements, §12 and §13, the README and docstrings, the release notes owed.

## Evidence

Measured on 2026-11-15 on macOS (Darwin, 10 cores) with CPython 3.13.1 and, where marked, 3.12.12,
while other sessions loaded the machine (load averages 11 to 34). Every premise about today's code
is cited at `a2cd4b36`; `src/` is identical at `6db09b86`, the base this spec was rebased onto. No
change to `src/` is committed.

**The prototype.** `docs/audits/2026-11-14-probes/core-pipeline/spec091_prototype.py` rewrites a
scratch copy of `src/log_foundry` at `a2cd4b36` into the mechanism FR-001 to FR-004 specify, and
its header says how. Applied to a fresh `git archive a2cd4b36 src`, it reproduces the measured
tree byte for byte. It differs from the spec in one place: it registers its fork step with
`os.register_at_fork` in `decorator.py`, which ran ahead of `_fork`'s handler and so ahead of the
worker's rebuild; FR-003 puts the step in `_fork`'s list instead, because
`tests/test_fork_lifecycle.py` holds the package to one registration.

**The probes**, committed under `docs/audits/2026-11-14-probes/core-pipeline/`, each headed by what
it shows: `spec091_exactly_once.py`, `spec091_latency.py`, `spec091_fork.py`, `spec091_exit.py`,
`spec091_continue_trace.py` and `spec091_hot_path.py`. `p12_kill.py` is the audit's.

| Probe | `a2cd4b36` | Prototype |
|---|---|---|
| `p12_kill.py`, `SIGKILL` 3.5 s into the root span | root 0 of 51, children 9 of 9 | root 50 of 51 (`span.start` held), children 9 of 9 |
| `p12_kill.py`, `SIGTERM` after five closed spans | 0 of 15 | 0 of 15: queued, not open |
| `spec091_exactly_once.py`, 36,015 events | `dup=0 missing=0`, 0 shipped early | 6 runs on 3.13 (3 without the switch interval) and 1 on 3.12: `dup=0 missing=0 bad_boundary=0 end_not_last=0`, 4,570 to 11,784 shipped before their close |
| the same, `swap` | — | 3 runs: `dup=0 missing=0 bad_boundary=0` |
| `spec091_latency.py`, 8 samples, idle and `busy` | every sample at the 5 s cap | max 1.003 s idle, 1.000 s busy |
| `spec091_fork.py` | child writes nothing | child writes nothing |
| `spec091_exit.py` `exit` / `inside` / `inside-warm` | 0 / all 8 / 0 of 8 delivered | 5 / 5 of 8 / 5 of 8 delivered |
| `spec091_continue_trace.py`, 20 trials | — | `split_spans=0 refused=0` |

**The mutants**, each one edit to the prototype:

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
| no lock around `continue_trace`'s check and re-parent | `split_spans=19 refused=1` |

**The SPEC-085 cells** (`tests/test_model_cells.py`, run whole). `a2cd4b36`: 451 passed, 21
expected failures. A first cut of the prototype, which built the worker after the span's push and
had no empty guard in `_flush`: 6 failed, four of the eight `empty-submission` cells (the `flush`
before `shutdown` orders), each reporting "the pin 'empty-submission' no longer shows its defect",
and `reentrant-setter-worker-shutdown+info` and `shutdown-none+info` at `LedgerBroken:orphans-1`. The prototype as specified: 8 failed, the eight pinned cells for that
reason alone, 451 passed, 13 expected failures, so every other verdict is unchanged. The same
run with `flush_interval` defaulted to 2 ms, so that the drain thread sweeps during each cell:
the same 8, 451 and 13, and 451 passed, 21 expected failures on `a2cd4b36` at 2 ms. The cells'
traced calls are short, so they meet the sweep incidentally; `spec091_exactly_once.py` is the
probe that drives it.

**The full suite** (`pytest -n 8`, from a `git archive` of the base with the prototype's `src/`).
`a2cd4b36`: 10,683 passed, 35 skipped, 21 expected failures. The prototype as specified: 33 failed,
10,658 passed. The 33: 18 rows of `test_invariants_model.py::test_the_accounting_identity_holds_under_every_interleaving`,
each the submission-unit span identity reading a swept submission (all 75 of that file pass once
the identity counts `span.end` events, FR-004); the eight pinned cells; the two `test_span_sweep.py`
tests and the `test_lifecycle_transitions.py` test whose premises FR-002 and FR-003 change; and four
rosters, `test_runtime_holder.py` (the lock site in `Worker._hand_in`), `test_worker_predicate_roster.py`
(three predicates), `test_lifecycle_inventory.py` (`Worker._sweep`) and `test_fork_lifecycle.py`
(the prototype's own fork registration). An earlier cut also failed `test_worker.py`'s busy-spin
test, 384,494 `get()` calls, because the wait read the sweep clock on a worker with no sweep; FR-002
reads it only when `_sweep` is set.

**Cost** (FR-005), `spec091_hot_path.py`: `isolated` on 3.13.1, excluding three runs made while the full suite ran, which read two to four
times higher: register 31 to 65 ns, register and
remove 79 to 149 ns, worker read 3 to 5 ns, snapshot of three events 64 ns (26 to 28 ns in a
separate run); on 3.12.12, register 28 to 56 ns, register and remove 100 to 107 ns, snapshot 94 ns.
`hold`: 7.5, 74 and 308 µs on 3.13.1, 7.4, 80 and 336 µs on 3.12.12, for 10, 100 and 1,000 spans.
`whole`, interleaved: 41.90, 32.82 and 25.98 µs on `a2cd4b36` against 32.47, 29.06 and 25.68 µs on
the prototype.
