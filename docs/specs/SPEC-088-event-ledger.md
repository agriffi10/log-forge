---
type: Spec
id: SPEC-088
title: "One Event Ledger: Every Accepted Event Is in Exactly One Term"
description: "Counts every event in one unit from the moment it is accepted to the one term it leaves through, exposes the terms on health(), and checks the balance at shutdown() and after every test, so that an accounting defect fails a check where today an audit has to find it."
spec_status: In Progress
depends_on: [SPEC-017, SPEC-019, SPEC-030, SPEC-036, SPEC-039, SPEC-050, SPEC-054, SPEC-062, SPEC-068, SPEC-072]
arc: audit-2026-11-14
arc_order: 4
---

## Overview

The library promises that every accepted event is delivered, or counted and announced, exactly
once (invariant 2). Today nobody can check that from the library's own numbers. `health()` counts
queue overflow in submissions, abandoned work in batches, and sink, orphan and in-span losses in
events; nothing counts what was accepted or what was delivered; and the balance closes only by
adding up stderr lines (finding 18 of the 2026-11-14 audit). Because nothing checks the balance,
accounting defects have shipped and stayed until an audit ran a probe at each one (findings 7, 9,
16 and 17). This spec adds one ledger in one unit, events. Every event is credited when it is
accepted and leaves through exactly one term: delivered, skipped, a named loss, or a gauge of what
is still on its way. The terms are a new field of `health()`, the balance is checked when
`shutdown()` ends and after every test in the suite, and a term that does not balance is reported
and never raised. The accounting defects the audit found are closed with it, and so are the ones
the ledger's prototypes found while this spec was written and reviewed.

## Scope

### In Scope

- The ledger's terms, the identity they keep, and what "accepted" means for a span's boundary
  events, an in-span log and an orphan log, on every twin path (FR-001).
- Six accounting defects closed, each held to the numbers of the reproduction that shows it:
  findings 16, 17 and 7 and the accounting half of finding 9, a `span.end` appended to a buffer
  already handed over, and the events of a call a `BaseException` cuts short (FR-002).
- The balance checked at the end of every `shutdown()`, reported by one stderr line and one
  field, and never raised (FR-003).
- The public surface: one field appended to `Health`, one new public type, the snapshot the
  arc's next spec reads, and the record rewritten in place (FR-004).
- Exact counters that add no lock to the accept path, their measured cost, and what a forked
  child's ledger starts as (FR-005).
- An autouse test fixture that fails the test whose process state does not balance (FR-006).

### Out of Scope

- **`FlushResult`'s loss counts, and the README's handler idioms that read them.** Andrew decided
  on 2026-11-15 that `flush()` stays truthy over a counted loss and `FlushResult` gains counts
  ("Keep truthy, add counts"). That is the arc's next spec. This spec gives it the snapshot
  FR-004 describes and changes nothing `flush()` returns.
- **Larger batches under a backlog.** Andrew decided on 2026-11-15 that capacity is not a setting
  and the worker sends larger batches when a backlog exists ("Larger batches, no setting"). That
  is a later spec of this arc. The queue and batch sizes are unchanged here.
- **The exit drain sending the backlog as one batch** (the capacity half of finding 9). Only the
  count of the batch in flight and the line that names it are in scope.
- **Shipping an open span's events before the span closes** (finding 8), which waits on Andrew.
  The ledger shows those events in `open_spans` and moves none of them.
- **Restarting a dead drain thread.** It is reported, not restarted
  (`docs/decisions/failure-paths/`). The events queued behind it are counted in `stranded` and
  stay undelivered.
- **Any change to the meaning or the unit of an existing `Health` field.** `dropped` stays in
  submissions (invariant 2's recorded exception), `failed_batches` in batches, and `queued` keeps
  counting markers. Three existing counters are corrected to the meaning they already document,
  and FR-002 and FR-005 name each correction; nothing else an existing field returns changes.
- **What a sink reports having lost inside an `emit` that returned.** For a sink without
  `deliver` the ledger ends where `emit` returns, so that loss stays in `health().sink`, whose
  `failed` is an upper bound and cannot be an exact term.
- **A loss no process survives to count**: `SIGKILL`, `SIGTERM`, `os._exit`, and a Lambda reaped
  while frozen.
- **Popping a span from its context when a `BaseException` interrupts its close.** The span stays
  current in that context (`docs/architecture.md` §12, filed with this spec). FR-002 counts the
  span's buffer and marks the span closed, and does not change what the context holds.
- **A free-threaded interpreter as a supported build.** FR-005 says what the ledger does there.

---

## Functional Requirements

### FR-001: Every event is credited once, when it is accepted, and leaves through exactly one term

#### Description:

**Accepted means asked for.** An event is credited at the call that asks for it, before the
library tries to build it, so a guard that forgets its debit leaves a remainder a check can see.
A credit that waited for the build would hide exactly the events finding 17 lost.

| Event | Credited when | Site at `ae0b7674` |
|---|---|---|
| `span.start` | the traced call begins | `decorator._begin`, before its `try` (`decorator.py:625`) |
| `span.end` | the traced call ends and a span exists | `decorator._end` (`decorator.py:683`) |
| in-span log | a level call finds an open span | `api._log` (`api.py:134`) |
| orphan log | a level call finds no span, or a closed one | `api._log` (`api.py:141`) |

The sync and async wrappers call the same `_begin` and `_end`, so they credit alike. A call
whose span fails to open never reaches the second row, so the open guard credits the `span.end`
that call will never ask for together with its debit (FR-002).

**The terms.** All count events. A gauge can fall; every other term only rises in a process.

| Term | Kind | An event is in it when | Moves at |
|---|---|---|---|
| `accepted` | credit | it has been asked for | the four sites above |
| `open_spans` | gauge | it is buffered on a span and not yet handed over | computed (below) |
| `queued` | gauge | it is in the worker's queue and the drain thread has not ended | the queue's `_put`; `worker.py:1723`, `:1807` |
| `in_flight` | gauge | the drain thread has taken it and not finished with it, or its orphan log call is still running | `worker.py:1739`, `:1815`, `:1931`; `api.py:141-167` |
| `delivered` | exit | an `emit` returned for its batch, or `deliver` answered `ACCEPTED` | `worker.py:1940`, `:1963`; `api.py:156-158` |
| `skipped` | exit | `deliver` answered `SKIPPED` | the same two engine sites |
| `in_span_lost` | loss | it could not be built or handed over inside a span | `decorator.py:630`, `:687`; `api.py:139` |
| `orphan_lost` | loss | an orphan log could not be built, or its run failed or raised | `api.py:159`, `:166` |
| `overflow_lost` | loss | its submission met a full queue | `worker.py:676` |
| `batch_lost` | loss | its batch was abandoned, or its retry declined | `worker.py:1943`, `:1968`, `:1978` |
| `sink_lost` | loss | its run settled and it ended neither `ACCEPTED` nor `SKIPPED` | `worker.py:1940`; `api.py:158` |
| `drain_lost` | loss | the drain thread died holding it | `worker.py:1679` |
| `stranded` | loss | it is in the queue of a worker whose drain thread has ended | read, not counted |

**Three gauges, three shapes.** `open_spans` is what is left of the events credited inside a
span once `in_span_lost` and the events handed to `Worker.submit` (`decorator.py:584`, `:462`)
are taken away. The library keeps no list of open spans, so this gauge is a remainder; it is the
one term only the fixture can check, against the spans its own context holds (FR-006). `queued`
is the queue's entering count less the drain's taking count. The entering count is kept by the
queue: the worker's queue is a `queue.Queue` subclass whose `_put` adds a submission's length
under the mutex `put_nowait` already holds, so `submit` writes no counter of its own and makes no
second cell lookup. An orphan log's part of `in_flight` is **measured**, not a remainder: the
calling thread's cell is raised as the call begins and lowered in a `finally`. So an orphan event
that was credited, is not in progress and is in no exit is an imbalance the check sees (FR-003),
where a remainder would have hidden a missing debit as an orphan log forever in flight.

**The drain thread has ended** means `Worker._thread.is_alive()` returns `False`, read before any
of the worker's counts. Not `_drain_finished`, which `_run` sets before `_terminal_failure` debits
what the thread held (`worker.py:1533` against `:1536`), and not read after the counts, which lets
a debit land between the two reads (Evidence). Once the drain has ended nothing will take a queued
event, so `queued` becomes `stranded`, the third future invariant 2 names. Flush markers and the
shutdown sentinel are not events and are in neither.

**The identity.** At rest, meaning no library call is in progress on any thread:

```
accepted == open_spans + queued + in_flight                               (pending)
          + delivered + skipped                                           (exits)
          + in_span_lost + orphan_lost + overflow_lost + batch_lost
          + sink_lost + drain_lost + stranded                             (lost)
```

`open_spans` and `queued` are remainders ~~, and so is the worker's part of `in_flight`~~ —
revised during the build: the worker's `in_flight` is one counter only the drain thread writes,
raised before its taking count when it takes a submission and lowered after the exit when it
settles one, which is what SPEC-090 asks of it ("one read of a counter only the drain thread
writes"). Measured, it has the orphan gauge's twin check: the drain residual, taken less held less
exits, reads zero at rest and never positive while the drain runs, and a positive one is in
`unaccounted` (FR-003). So the sum holds only where the remainders meet: the events the caller handed over must equal the events the
queue took in plus `overflow_lost`. Nothing else ties the caller's side to the worker's, so that
meeting is checked (below), and with it the identity holds. Each remainder can also read below
zero, which FR-003 counts. So the criteria below assert `unaccounted == 0` and, at rest after a
`flush()` with every span closed, `pending == 0` with `accepted` equal to the events the test
asked for.

**The ledger is kept as two balances that meet at `Worker.submit`.** The caller's side balances
what it credited against what it lost, what the orphan path did, and what it handed over. The
worker's side balances what its queue took in, plus what overflowed, against the queue, the drain
and its exits. Kept apart, each can be asserted alone: 383 existing tests hand a worker events
they built by hand, and they balance on the worker's side without touching the caller's
(Evidence). The **meeting residual** is the events handed over less the process worker's taken-in
and overflowed counts. The hand-over is credited after `Worker.submit` returns, never before, at
both sites that call it (`decorator.py:584`, `:462`), so every event the hand-over has counted is
already in the queue's count or in `overflow_lost`. A submission dropped inside `submit` without
either count leaves a positive residual at rest. The residual counts the hand-overs made since the
process worker was built, which records the hand-over count when it is built, and is not computed
while no process worker exists. The library builds one worker per process and never replaces it
(`_lifecycle.py:3797` is the one assignment) except in place in a forked child, whose ledger
restarts, so outside a test that replaces the worker by hand the record is zero.

**The snapshot's read order, and why it is sound.** A snapshot is several reads and not one, and
a residual read while calls are in progress must never read positive for a call that is
balancing correctly, or the shutdown check would fire falsely. ~~Each caller-side cell is written
by one thread, in program order, so the argument is per write sequence and needs no assumption
about where a thread switch can fall. The snapshot reads, in this order: `in_span_lost`; then, in
each cell, the hand-over, the span credits, the orphan credits, the orphan gauge and the orphan
exits; then `orphan_lost`; then the worker reference and the worker's terms, the drain's end
first, its leaving counts next, and the queue's taken-in count and `overflow_lost` last.~~ —
revised during the build (2026-11-15). Read cell by cell, the argument assumed a hand-over debits
only its own cell's credits. It does not: `asyncio.to_thread` and any `copy_context()` thread
share a span with its owner, so an in-span log on the executor thread is credited in that
thread's cell while the close's hand-over is credited in the owner's. A snapshot that read the
executor's cell, then saw the log and the close, then read the owner's, read `open_spans` at
minus 1 and `unaccounted == 1`; `tests/test_event_ledger_threads.py`'s
`test_a_span_shared_across_threads_never_reads_open_spans_below_zero` reddens with the
hand-overs read after the credits. The order built, as the plan review checked it: the worker
reference; the drain's end, its taking count, its held events and its exits; `in_span_lost`;
**every cell's hand-over in one pass**; the queue's taken-in count and `overflow_lost`; **every
cell's span credits in a second pass**, from a fresh snapshot of the cells so a cell registered
meanwhile is read too; the orphan pass, cell by cell, credit, gauge and exits; `orphan_lost`. This
is SPEC-091 FR-004's two-pass refinement built here, with one difference from that spec's
sentence "the worker's terms keep SPEC-088's order after the two passes": the worker's exits now
come **before** the passes, so that every exit and loss of the worker path is read before the
credits its events carry, which is what gives FR-001's per-snapshot "an `accepted` no less than
its exits and losses" (with them read after the credits, the eight-thread reader test read
`accepted` below them). The worker's `queued` and the meeting residual keep their properties,
because the taken-in count is still read after the taking count and after the hand-overs.

- The orphan path writes its gauge up, then its credit, then exactly one exit, then its gauge
  down. If the read counts a credit, the gauge was raised before it, so the later gauge read sees
  the call either still in progress or finished; if finished, its exit was written before the
  gauge came down and is read later still. So no call adds to the residual, and one whose credit
  was not read can only subtract. The orphan residual reads zero or negative while calls run, and
  zero at rest.
- The hand-over is written after the queue's count or `overflow_lost`, both read later. So the
  meeting residual reads zero or negative while hand-overs run; negative also when a caller
  outside the library submits to the worker directly, as tests do.
- A remainder gauge is read leaving before entering, which keeps it from reading below zero:
  `in_span_lost` and the hand-over before the span credits, ~~and the worker's exits before what
  it took and took in~~ and the drain's taking count before the queue's taken-in count. The
  worker's `in_flight` is not a remainder (below).
- On the orphan path an exit is read after its credit, so a call that credits and fails between
  the two reads makes `accepted` read below the exits and losses by the calls in progress: the
  negative orphan residual this paragraph already admits. The per-snapshot bound of FR-001's
  eight-thread criterion is the worker path's.

So FR-003 counts only a **positive** orphan or meeting residual: a positive one is a missed debit,
or an event the queue never took in, and nothing in progress produces one. A negative orphan
residual at rest, a debit made twice, is left to the fixture (FR-006), which reads at rest.

**Twin paths debit the same terms.** A settled engine run debits `delivered`, `skipped` and
`sink_lost` from its `Delivery` counts on the worker path and on the orphan path alike. A failed
run, or an `emit` that raised through its retries, debits the whole batch to its catcher's term,
which is `batch_lost` on the worker path and `orphan_lost` on the orphan path, as invariant 2
already assigns the batch. An `UNKNOWN` outcome can only occur in a run that settles
(`sinks/_engine.py:1873`), so it is in `sink_lost` and in nothing else. A sink swap moves no
term: the terms belong to the process and the worker, not to the sink.

**Every loss the library announces today has one term.** Against the core frame's loss-site
table (`docs/audits/2026-11-14-frames/core-pipeline.md`, section 2):

| Site | Today | Term | Event count the site lacks today |
|---|---|---|---|
| Span open fails | announced, uncounted | `in_span_lost` | `decorator.py:631` |
| Span close fails | `in_span_lost`, one short | `in_span_lost` | `decorator.py:687` |
| In-span build fails | `in_span_lost` | `in_span_lost` | none |
| Orphan emit fails | `orphan_lost` | `orphan_lost` | none |
| Queue full | `dropped`, in submissions | `overflow_lost` | `worker.py:676` |
| Submitted after `shutdown()` | `submitted_after_shutdown` | `stranded` | none: read from the queue, so `worker.py:703` needs no count |
| Batch abandoned or retry declined | `failed_batches`, in batches | `batch_lost` | `worker.py:1943`, `:1968`, `:1978` |
| Sink partial, dropped or unknown | `health().sink` | `sink_lost` | `worker.py:1940`, from `Delivery` |
| Shutdown expiry, batch in flight | a promise on stderr | `in_flight` | none: `_in_flight` holds it, privately (`worker.py:1440`) |
| Drain thread dies holding a batch | announced in lists | `drain_lost` | `worker.py:1679` |
| Append racing a close or sweep | nothing | stays in `open_spans` | none; recorded, not closed (`architecture.md` §13) |
| Swap | `incomplete_swaps` | no term moves | none |
| Fork | no loss; the child's counters restart | the child's ledger restarts (FR-005) | none |
| Lambda freeze with no `flush()` | cannot be counted | the pending gauges, until the reap | none |
| `SIGKILL`, `SIGTERM`, `os._exit` | cannot be counted | none: no process is left to hold a term | none |

The frame asked for an event count at `worker.py:703`. This spec does not add one: a retired
submission that was queued is `stranded` only if the final drain did not carry it, which the
submitting thread cannot know, so the term is read from the queue once the drain has ended and
has no over-count.

#### Acceptance Criteria:

- [ ] A traced call that returns moves `accepted` by 2 and a level call by 1, on the sync and the
      async wrapper alike, and a call whose event cannot be built moves it by the same amount
      (invariants 2 and 6).
- [ ] `p01_overflow.py`'s scenario ends with `accepted == 36000`, `delivered == 30030`,
      `overflow_lost == 5970`, which is three times `dropped`, and every other term zero
      (invariant 2).
- [ ] `p05_raise_nth.py`'s first scenario ends with `delivered == 600` and `batch_lost == 300`,
      the sum of its ten "lost 30 event(s)" lines, and its orphan scenario moves `orphan_lost`
      by 1 and `delivered` by 1 (invariants 2 and 11).
- [ ] One event sent with each of the six `Outcome` values, a raising `deliver` and an unreadable
      verdict moves the same single term on the worker path and on the orphan path, except a
      failed run, which moves `batch_lost` on one and `orphan_lost` on the other
      (invariants 6 and 7).
- [ ] A batch of events a `deliver` sink answers `UNKNOWN` moves `sink_lost` by the batch's size
      and moves no other loss term and not `delivered` (invariant 2).
- [ ] A `MultiSink` whose child raises while a sibling accepts moves `delivered` by the batch's
      size and no loss term, and `health().sink.failed` moves as it does today (invariant 7).
- [ ] Across a `configure(sink=...)` with spans in flight, with the old sink healthy and with it
      stalled past the swap budget, `delivered` ends equal to the events the two sinks received
      together, with `pending == 0` and `unaccounted == 0` once a `flush()` returns
      (invariant 2).
- [ ] A `flush()` from inside an open span leaves `open_spans == 0` and moves `delivered` by the
      events buffered so far; the span's close then moves `delivered` by 1 (invariant 2).
- [ ] `queued + stranded` equals the events counted in the worker's queue under its mutex, with
      the drain thread alive and stalled, and after it has ended (invariant 2).
- [ ] With eight threads tracing and a ninth reading `health()` in a loop, every snapshot has
      `unaccounted == 0` and an `accepted` no less than its exits and losses, and once the eight
      have joined and a `flush()` has returned, `pending == 0` and `accepted` is three times the
      calls made (invariant 2).
- [ ] A snapshot taken while a dying drain thread is held, by an injected hook, after it has set
      `_drain_finished` and before its terminal debit reads `unaccounted == 0`, and so does one
      taken after the debit and before the thread ends (invariant 2).
- [ ] `docs/invariants.md` states invariant 2's observable as this identity, in events, and keeps
      the recorded exception that `dropped` counts submissions (invariant 2).

### FR-002: Six accounting defects are closed, each held to its reproduction

#### Description:

Each defect is a place where an event leaves without a term, or with two. Four are the audit's.
Two were found by running this spec's prototype against the suite and one more case of the sixth
by its review (Evidence); a further defect found the same way, a forked child's inherited
counters, is FR-005's. All are in `docs/architecture.md` §12, each with its probe.

1. **Overflow after `shutdown()` is counted twice** (finding 16). `Worker.submit` counts
   `submitted_after_shutdown` before the put and `dropped` again when the put meets a full queue
   (`worker.py:669-680`), against its own docstring (`:656-658`). A retired submission is counted
   in `submitted_after_shutdown` only once its put has succeeded. In the ledger an overflowed
   event is in `overflow_lost` and a queued one in `stranded`, so the pair cannot overlap. A
   caller can see one consequence: `submitted_after_shutdown` now reads 0 when every late
   submission overflowed, since none of them was queued.
2. **A span that fails to open loses two events uncounted; a failed close undercounts by one and
   leaves its buffer where a later flush can deliver it** (finding 17). The open guard debits both
   boundary events to `in_span_lost`, and only when the span was not built: a span that was built
   but could not be pushed runs its call traced incompletely, and its close debits or delivers
   both boundary events as any close does. The close guard takes the span's buffer under
   `_sweep_lock` and marks the span closed, then debits what it took, and one more when `span.end`
   was not appended. Taking the buffer is what keeps the debited events from a second term: today
   a context still holding the span can `flush()` them after the guard has counted them lost. The
   guard learns whether `span.end` was appended from `Span.ended`, a new field defaulting to
   `False` that `_close_span` sets after the append; `_close_span`'s signature does not change.
   This supersedes SPEC-050 FR-003's fourth criterion, "one that fails before it counts only what
   the span held", which
   `tests/test_decorator_sync.py::test_a_span_failing_before_its_end_event_counts_only_what_it_held`
   pins.
3. **A drain thread that dies leaves the events it held in no counter** (finding 7).
   `Worker._terminal_failure` debits every event the thread had taken and not finished with to
   `drain_lost`, and its line names that count. The worker is still not restarted, and what is
   queued behind it is `stranded`.
4. **An expired `shutdown()` promises the batch in flight a count that never comes** (the
   accounting half of finding 9). The batch is in `in_flight` at that instant, and the line says
   so in place of "counted when the emit returns". If the emit returns, the drain thread debits
   the batch as it would any other. If the process exits first, no counter survives, which is the
   loss the Out of Scope list names.
5. **A `span.end` built while another thread sweeps the span lands on a buffer already handed
   over** (found here). `decorator.py:721` reads `span.events` before it calls `end_event`, so a
   `flush()` on a thread or task sharing the span can detach the buffer in between. The event is
   delivered late if the drain has not yet flattened that submission and is lost uncounted if it
   has. The close builds the event first and reads the buffer after, as `api._log` does. This is
   not the recorded window of `architecture.md` §13, which is `api._log`'s append and stays open.
6. **The events of a call a `BaseException` cuts short are in no counter** (found here). The four
   guards catch `Exception` and let a `KeyboardInterrupt` or `SystemExit` through (invariant 1),
   which leaves what that call had credited with no term. Each guard counts on a `BaseException`
   ~~exactly what it counts on an `Exception`~~ what its call can no longer deliver (revised
   during the build: on an `Exception` the open guard also credits and debits the `span.end` the
   call will never ask for, and an interrupted call asks for nothing more, which `p16`'s
   `accepted == 5` settles), then re-raises it unchanged and writes nothing to
   stderr: the exception reaching the caller is the announcement. The orphan guard (`api.py:165`)
   debits the orphan event, the in-span guard (`api.py:138`) the event it was building, the open
   guard (`decorator.py:630`) the `span.start` of a span that was not built, and of one that was
   built but interrupted before it could be pushed the buffer it takes, as the close guard takes
   one (added during the build: that span is on no context, so its `span.start` was otherwise a
   silent loss), and the close guard
   (`decorator.py:686`) the buffer it takes and the `span.end` if it was not appended, as in item
   2. Leaving the buffer on a span only that context can reach would be a silent loss whenever the
   context is not used again, which in the suite is every time, and invariant 2 forbids a silent
   loss where a counted one is possible.

**A log made where the guard gave up ships on the span's own ids.** The close guard also marks
the span abandoned (`Span.abandoned`, a new field defaulting to `False`). The span stays current
in its context, because popping it is out of scope, so a later level call in that context, from
the caller's `except` handler or from a task created inside the span, finds a closed span and
takes the orphan path. For an abandoned span that orphan keeps the span's trace id, span id,
parent id, name and defaults, so the log stays correlated with the call it belongs to, and it is
counted once, on the orphan path. A span that closed normally is untouched: a log from a task
that outlives it still gets a fresh trace, as SPEC-036 FR-004 settled, since nothing marks that
span abandoned. Measured at `ae0b7674`, such a log is appended to the dead span's buffer and
ships only if that context flushes again, without `span.end`; in a context that is not used
again it was lost with every counter at zero (Evidence).

**What a caller can see** from items 2 and 6 and the paragraph above, each in the release notes:

- An interrupted close's buffer is counted lost in `in_span_lost`, where today a later `flush()`
  in that context delivers it without `span.end` (invariant 2: counted, where it could be lost
  silently).
- A failed close's buffer is counted lost and is no longer delivered by another thread or task
  that shares the span and flushes later, where today it is both counted and delivered
  (invariant 2: one term, not two).
- An in-span log that another thread sharing the span buffered before the guard ran is counted
  lost with that buffer; in the review's unforced race today delivered 3 events and the revision
  0, with `in_span_lost=4` (invariant 2).
- A level call made after the guard, in a context that still holds the span, ships at once as an
  orphan on the span's own trace and span ids, where today it waits in the dead span's buffer
  for a flush that may not come (invariants 2 and 9).

#### Acceptance Criteria:

- [ ] After `shutdown()`, with no drain, submitting `N` spans to a queue that holds `C < N`
      leaves `dropped == N - C`, `submitted_after_shutdown == C`, `overflow_lost` twice the
      first and `stranded` twice the second; today the same scenario reads
      `submitted_after_shutdown == N` (invariant 2).
- [ ] `p03_shutdown_race.py 2000` prints a `submitted_after_shutdown` below `queued` plus
      `dropped`; today it prints one above (11,979 and 11,992 in two of this spec's runs, against
      10,013 to 10,094 on the prototype) (invariants 2 and 11).
- [ ] `p11_assembly.py` prints `in_span_lost=2` for (a), where today it prints 0; "moved by 3"
      for (b), where today it prints 2; and `unaccounted=0` for (c), where today it prints 6
      (invariant 2).
- [ ] A close whose `span.end` cannot be built, followed by a `flush()` from a context that still
      holds the span, ends with `in_span_lost == 3`, `delivered == 0` and `unaccounted == 0`; the
      spec as first drafted ended it with `delivered == 2` and `unaccounted == 2` (invariant 2).
- [ ] A span that is built but whose `push_span` raises still delivers both boundary events and
      moves no loss term (invariant 2).
- [ ] `p08_worker_death.py` ends with `accepted == 100`, `delivered == 20`, `drain_lost == 20`
      and `stranded == 60`, and its stderr line names 20 events; today the 20 are in no counter
      (invariants 2 and 11).
- [ ] `p02_shutdown_stalled.py` reads `queued == 980` and `in_flight == 20` when the expired
      `shutdown()` returns, and `delivered == 1000` with both gauges zero once the sink recovers
      (invariant 2).
- [ ] The expiry line of `p04_exit.py 0.02 10000` names the events in flight and
      `health().events.in_flight`, and does not contain "counted when the emit returns"
      (invariant 11).
- [ ] `p14_end_races_sweep.py` prints `span.end` in the sink's list on both rows; today the
      second row prints `['span.start', 'in-span']` with every counter zero (invariant 2).
- [ ] `p16_interrupt_through_a_guard.py` prints `KeyboardInterrupt` as what the caller got on all
      three rows and ends with `accepted == 5`, `orphan_lost == 1`, `in_span_lost == 4`,
      `delivered == 0` and `pending == 0`, with nothing on stderr; today it ends with both
      counters at 0 and two of its five events delivered (invariants 1 and 2).
- [ ] An in-span level call whose build raises `KeyboardInterrupt` reaches the caller unchanged
      and moves `in_span_lost` by 1, where today the event is in no term (invariants 1 and 2).
- [ ] `p17_log_after_an_interrupted_close.py` prints its handler row and its task row delivered
      with the span's own trace id and span id, and its control row on a fresh trace; at
      `ae0b7674` the first two are delivered nowhere (invariants 2 and 9).
- [ ] Each test that pins a number or a line this FR changes is rewritten in place, with the
      superseded claim struck and not deleted (invariant 2).

### FR-003: The balance is checked when `shutdown()` ends, and a failure is reported, never raised

#### Description:

Invariant 1 forbids an imbalance from raising into the application, so the check reports. It
runs at the end of every `shutdown()` call, after the drain has been joined or the wait has
expired, and it computes one number, `unaccounted`: the events the ledger cannot place.

- Events in the worker's `in_flight` when its drain thread has ended (FR-001's definition). A
  drain that ended has nothing in its hands, so these were taken and never debited.
- A positive drain residual: events the drain thread took that are neither held nor in an exit
  (added during the build, with the worker's `in_flight` measured, FR-001).
- A positive orphan residual: orphan events that were credited, are not in progress and are in
  no exit and no loss term. Every orphan call debits before it lowers its gauge, so this is a
  debit that was missed. Only the positive side counts, by FR-001's read-order argument: a
  negative residual is what a call in progress reads as, and it is ordinary while orphan logs are
  failing.
- A positive meeting residual: events handed over that the process worker's queue did not take
  in and that did not overflow. A hand-over is credited after `submit` returns, so this is a
  submission `submit` lost without a count, the class of finding 16.
- The amount by which `open_spans`, `queued` plus `stranded`, or `in_flight` falls below zero.
  A stage that reads below zero was debited more often than it was credited.

All of these are reads of counters already summed for a snapshot, so the same number is computed
for every `health()` call and is a field of the ledger (FR-004). A gauge is published as zero when
it would read below zero, and the deficit is in `unaccounted`, so a reader never meets a negative
count. `unaccounted` is not a term of the identity; it measures how far the terms are from
balancing, and it is zero in a correct library.

The check does not assert that `open_spans` is zero. A `shutdown()` may be called from inside a
traced function, and a daemon thread may be inside one when the interpreter exits, so a positive
remainder is a legitimate state in production. The fixture asserts it, because a test knows what
its own context holds (FR-006). The events the recorded append window strands on a closed span
(`architecture.md` §13) therefore stay in `open_spans`, visible for the first time and not
announced.

When the check finds `unaccounted` above zero it writes one line through `_diag`, naming counts
only, and it writes again only when the number differs from the last one it announced, since a
warm container may call `shutdown()` on every invocation.

**A `shutdown()` from a signal handler.** The check reads the caller-side cells under the ledger's
registration lock, a plain `threading.Lock`. A handler that runs `shutdown()` on a thread already
inside that lock, which a `health()` call holds for its read, waits on its own thread: forced by
raising the signal inside the read, it hung. This is one more instance of the open §12 item for
finding 3, which already records the same wait on `Runtime._lock`, the close registry's lock,
`Worker._lock` and the queue's mutex, and is closed by the lifecycle track's one command path.
This spec makes no new promise about a handler and adds that lock to the item's list.

**Re-entry on the registration lock** (added during the build, from the plan review). The hazard
above was worse for this lock than the paragraph says: a handler or a `__del__` that *logs* on a
thread with no cell yet, run while that thread was inside the lock (its own first registration,
or a `health()` or `shutdown()` reading), registered a cell and so took the same lock again, and
with a plain `Lock` waited on its own thread (`premise_3b` of the plan review). The lock is an
`RLock`, the registry is one immutable pair replaced compare-and-set so a re-entrant registration
made mid-fold is never overwritten, and every walk is over that pair. `tests/`
`test_event_ledger_threads.py::test_a_log_from_a_collection_inside_the_registration_lock_returns`
forces the collection inside the fold and reddens with a plain `Lock`. A handler on a thread
inside any of the item's other locks still waits.

#### Acceptance Criteria:

- [ ] With a worker whose drain thread has ended and whose ledger is made to hold 3 events in
      flight, `shutdown()` returns normally, `health().events.unaccounted == 3`, and stderr
      carries one line that names 3 events (invariants 1 and 11).
- [ ] With the orphan guard's `BaseException` debit removed, an orphan log the sink interrupts
      leaves `unaccounted == 1` and `shutdown()` writes the line; the spec as first drafted read
      that state as `in_flight == 1` with `unaccounted == 0` and wrote nothing (invariants 2
      and 11).
- [ ] With `Worker.submit` made to return without queueing or counting a 7-event submission (the
      review's `meet_gap.py`), a traced call with five logs leaves `unaccounted == 7` and
      `shutdown()` writes the line; the spec as revised once read `unaccounted == 0` and wrote
      nothing (invariants 2 and 11).
- [ ] 1,000 `shutdown()` calls, made while six threads log orphans that the sink refuses, write no
      "does not balance" line, and neither do 1,000 snapshots read meanwhile; the same holds with
      a no-op function call inserted between each read of the cell walk, and with the sink
      accepting (the review's `orphan_race.py`) (invariants 2 and 11).
- [ ] A second `shutdown()` with the same imbalance writes no second line, and one with a
      different imbalance writes one (invariant 11).
- [ ] A ledger made to read `open_spans` at minus 2 publishes `open_spans == 0` and
      `unaccounted == 2` (invariant 2).
- [ ] A fault raised inside the check is absorbed and announced by type, and `shutdown()` still
      closes the sink and returns (invariant 1).
- [ ] An expired `shutdown()` with a batch still inside the sink reports `unaccounted == 0`
      (invariant 2).
- [ ] A `shutdown()` called from inside a traced function, with that span's events still
      buffered, reports `unaccounted == 0` and writes no line (invariant 2).
- [ ] Every reproduction FR-002 names ends with `unaccounted == 0` (invariant 2).

### FR-004: `health()` carries the ledger in one new field, and the next spec is given a snapshot

#### Description:

`Health` gains one field, `events`, appended and defaulted to an all-zero ledger, so that a
caller who builds a `Health` by hand is not broken and a reader never has to test it for `None`.
It holds an `EventLedger`, a new public frozen keyword-only dataclass with one `int` per term of
FR-001, `unaccounted`, and two read-only properties, `pending` and `lost`, which are the sums the
identity's first and third lines name. Every field defaults to zero. The type is defined beside
`Health` in `log_foundry.worker` and exported from `log_foundry`, and both paths are inside the
1.x promise. A process with no worker reports a ledger too: the worker's terms are zero and the
caller's are its own.

The two terms that already exist as `Health` fields, `orphan_lost` and `in_span_lost`, appear in
the ledger with the same values, read once for both. `Health.queued` and `events.queued` differ
and both docstrings say how: the first counts submissions and markers, the second counts events.
The `in_span_lost` docstring's "The two are told apart by the count, which is one event for the
first and the span's whole buffer for the second" (`worker.py:232-234`) stops being true, since a
failed open now counts two events and an interrupted build one, and it is struck in place.

**What the arc's next spec is given.** One private function, `_lifecycle._ledger_snapshot()`,
returns an `EventLedger`. It may be called from any thread, the drain thread included. It
acquires neither `Runtime._lock`, `Worker._lock`, the queue's mutex nor `decorator._loss_lock`:
each worker term and each of the two loss counters is read as one attribute load. The one lock it
takes is the ledger's registration lock, ~~which it holds while it sums the caller-side cells~~
once per pass, to fold the dead cells and read one immutable snapshot of the cells, which it then
sums outside the lock (revised during the build); a caller thread takes it once, on its first
credit. So it cannot wait behind a `submit`, a
drain or a lifecycle call. Every loss term is non-decreasing in a process, so the events lost by
term between two points are the fieldwise difference of two snapshots. Its cost grows with the
cells of live threads that have logged: about 3 µs with one, 5 to 6 µs with nine and 15 to 17 µs
with fifty, on this machine (Evidence). Nothing else is offered, and `flush()` does not call it in
this spec.

**This is not the in-library metrics `CLAUDE.md` rules out.** The non-goal's words are "derive
them downstream". A count of events that never reached the sink cannot be derived downstream of
the sink; the terms are returned by `health()` and shipped nowhere; and `Health` has carried the
library's account of its own delivery since SPEC-017. The §12 item for finding 18 made this
argument by calling metrics "measurements of the application", which the non-goal does not say,
and it is corrected there in place.

#### Acceptance Criteria:

- [ ] `Health(queued=0, dropped=0, failed_batches=0)` still constructs, and its `events` equals
      `EventLedger()`; so does `health().events` in a process that has never logged
      (invariant 10).
- [ ] `EventLedger` is frozen and keyword-only, every field is an `int` defaulting to zero, and
      `log_foundry.EventLedger is log_foundry.worker.EventLedger` (invariant 10).
- [ ] `tests/typed_consumer/accepts.py` reads `health().events.lost` and builds an `EventLedger`
      by keyword under `mypy --strict`, and `rejects.py` shows a positional build refused
      (invariant 10).
- [ ] `health().events.in_span_lost == health().in_span_lost` and the same for `orphan_lost`, in
      a process that has lost one of each (invariant 2).
- [ ] `_ledger_snapshot()` returns while another thread holds `Runtime._lock`, the worker's
      `_lock`, the queue's mutex and `decorator._loss_lock` (invariant 3).
- [ ] The delivery doc records one snapshot's cost on 3.12 and 3.13 by the Evidence method with
      one, nine and fifty live cells, and with nine it is under 12 µs (invariant 3).
- [ ] The `Health` docstring, `health()`'s Returns block and the README's `Health` section name
      `events` and its terms, and the `in_span_lost` docstring's superseded sentence is struck;
      no handler idiom in the README changes (invariant 10).
- [ ] The pending release notes carry one entry naming the new field and type, and every change
      of FR-002 and FR-005 a caller can see: the three corrected counters, that
      `submitted_after_shutdown` can now read 0 when every late submission overflowed, the two
      changed stderr lines, the `span.end` now delivered, a forked child's counters, and the four
      changes FR-002's "What a caller can see" lists (invariant 10).
- [ ] The §12 items for findings 7, 16, 17 and 18, and the three filed with this spec that name
      it as their closure, are struck as closed by it; the item for finding 9 is narrowed to its
      capacity half and the item for finding 3 names the ledger's registration lock, in place
      (invariant 2). The fourth item that names this spec as its closure, *`NATSSink` leaves the
      events past a round's deadline unattempted*, filed from SPEC-082's PR, is struck only if
      `p18_nats_deadline_leaves_events_unattempted.py`, run with the `nats` extra, ends with
      every event of its run in one term; otherwise the delivery doc says why it stays open
      (added during the build).

### FR-005: The counters are exact and add no lock to the accept path, and a forked child starts balanced

#### Description:

**One rule keeps every counter exact.** A ledger counter is written by one thread only, or under
a lock its site already holds, and in no other way. The drain thread is the one writer of the
terms it debits and of its taking count. A caller thread writes credits, hand-overs and its orphan
gauge into a cell that only it writes, and a reader sums the cells. The queue's entering count is
written inside `_put`, under the mutex `put_nowait` already holds; `overflow_lost` under
`Worker._lock`, which that path takes today; and the two existing loss counters stay under
`decorator._loss_lock`. The only new lock is the registration lock, taken once per thread, when
it registers its cell, and by a reader.

**A cell is retired when its thread ends.** Registration stores, in the thread's local storage
beside the cell, an object nothing else holds, with a `weakref.finalize` that marks the cell dead.
A thread's local storage is freed when the thread ends, whether it was started by `threading` or
by `_thread`, so the mark is set then. The finalizer takes no lock, so it is safe on any thread at
any point. The next registration or read folds a dead cell's counts into a base and drops it.
Keyed on `Thread.is_alive()` alone, a cell of a thread started through `_thread` is never
retired, because `threading` sees such a thread as a `_DummyThread` that reports itself alive
(Evidence).

A bare `counter += 1` shared between threads was measured and is rejected. It lost no update in
48 million increments on each of CPython 3.12.12, 3.13.1 and 3.14.6, because those builds switch
threads only at a call or a backward jump. That is the interpreter's accident and not a
guarantee: `docs/best-practices/python/python.md` §13 forbids relying on it,
`requires-python` has no upper bound, and `architecture.md` §13 already declined the same
accident for the span detach. A lock around the credit was measured too, at 78 to 95 ns.

**No lock is added to the accept path.** The path already takes `decorator._sweep_lock` at every
close and the queue's mutex at every submit, and keeps doing so; a traced call does not return
while another thread holds `_sweep_lock`, on `ae0b7674` as on the prototype. The ledger adds only
the registration lock, which a thread takes on its first credit and never again, and writes under
`Worker._lock` and `decorator._loss_lock` only on the loss paths that take them today.

**On a free-threaded build**, which the library does not support, the counters stay exact,
because nothing above depends on the GIL. What still depends on it is the fix of FR-002 item 5
and the recorded append window: there a `span.end` could again land on a buffer already handed
over, and the ledger would report it in `unaccounted` where today it is silent.

**Cost.** The accept path runs on the caller's thread inside every traced call. Measured in
isolation (Evidence), one credit through the thread's cell costs 30 to 32 ns and the counting
`_put` adds 18 to 21 ns to a plain one, so a traced call with no log adds two credits, one
hand-over and one counting put, 107 to 117 ns, against about 19 µs for the call on the same
loaded machine. An end-to-end run could not resolve the change: two runs of the same tree
differed by more than it. The bounds are on the isolated measurements for that reason, and leave
room above them because the machine was loaded and the build's shape will differ from the
prototype's.

**A forked child.** Today a child's worker counters are zeroed and its queue replaced
(`worker.py:584-592`), while `orphan_lost` and `in_span_lost` keep the parent's counts
(`p15_fork_child_counters.py`: the child reads `orphan_lost=1` for a loss the parent took). The
child's ledger starts as what it holds: `accepted` and `open_spans` both equal the parent's
`open_spans` at the fork, and every other term is zero, the two existing loss counters included.
The parent's ledger is untouched. Events buffered on spans that were open on other threads of
the parent are in the child's memory and nothing in the child will close them, so they stay in
its `open_spans`; that over-read is recorded in `architecture.md` §13. The child's copy of the
forking thread's open spans is a second acceptance of the same events, which is the duplication
`p07_fork.py` measures and this spec does not change.

#### Acceptance Criteria:

- [ ] On a thread that has logged before, a traced call with one in-span log returns while
      another thread holds the ledger's registration lock, and likewise while it holds
      `decorator._loss_lock` or the worker's `_lock`; `decorator._sweep_lock`, which every close
      already takes, is not in that set (invariant 4).
- [ ] Eight threads making 50,000 traced calls each, with one log apiece, under a switch
      interval of 1e-6, end with `accepted == 1,200,000`, `pending == 0` and
      `unaccounted == 0` after a `flush()` (invariant 2).
- [ ] No ledger counter is written from two threads outside a lock: a test derives the write
      sites from the tree and fails on one that is neither drain-only, cell-owned, inside the
      queue's `_put` nor inside a `with` on a lock (invariant 2).
- [ ] The delivery doc records, by the Evidence method on 3.12 and 3.13, the isolated cost of one
      credit and of the counting put; each is under 50 ns, and a traced call's ledger writes sum
      to under 200 ns (invariant 4).
- [ ] After 2,000 threads started through `_thread.start_new_thread` have each logged and ended,
      the ledger holds no cell of theirs, and `accepted` still counts every event they asked for
      (invariant 2).
- [ ] `p15_fork_child_counters.py` prints, for the child at the fork, `accepted == 2`,
      `open_spans == 2`, `orphan_lost == 0` and every other term zero; after its span closes,
      `accepted == 3` and `delivered == 3`; and the parent's `orphan_lost` stays 1
      (invariants 2 and 12).
- [ ] A child forked while another thread of the parent holds the registration lock reads
      `health()` and logs without waiting (invariant 12).
- [ ] A retired parent forks a child whose later submissions are in `stranded`
      (invariants 6 and 12).

### FR-006: An autouse fixture fails the test whose process state does not balance

#### Description:

The fixture is what turns the next accounting defect into a failed test. It runs after each
test's body and before the suite's own reset, and it stops nothing and waits for nothing. It
fails the test when any of these holds:

- `unaccounted` is not zero.
- The orphan residual is not zero, with either sign. The shutdown check counts only a positive
  one, because a call in progress reads negative (FR-001); the fixture reads at rest, after the
  test's body, so a negative one there is a debit made twice.
- `open_spans` differs from the events buffered on the spans still open in the fixture's own
  context, which is zero for a test that closed what it opened.
- For any `Worker` the test built, `queued + stranded` differs from the events in its queue, or
  its own terms place events the ledger cannot: events in flight on an ended drain, or a gauge
  below zero.

A test that kills the drain thread balances through `drain_lost` and `stranded`. A test that
leaves a sink stalled balances through `in_flight` and `queued`, because the drain has not
ended. Those are states the identity admits, so such tests need nothing. The `lf` conftest
fixture replaces `decorator._flush` with a direct `emit`. It credits the hand-over as `_flush`
does and keeps its own count of the events it emitted directly, which no queue takes in, and the
fixture expects the meeting residual to be at most that count. Both are a few lines in
`tests/conftest.py`.

**Measured against the suite** at `ae0b7674`, over 9,896 test teardowns, with this fixture as
written (Evidence). With every fix of FR-002 and FR-005 and the `lf` hand-over, 7 tests fail the
balance, none of them an `lf` user, and each works below the accept sites, so each carries the
marker with that reason:

| Test | Reads | Why it cannot balance |
|---|---|---|
| `test_api.py::test_an_in_span_log_is_unaffected_by_a_broken_sink` | `open_spans` 1 | logs into a hand-built span pushed in a context it discards |
| `test_decorator_sync.py::test_a_raising_traced_call_leaves_no_reference_cycle` | `open_spans` 100 | replaces `decorator._flush` with a no-op |
| `test_delivery_paths.py::test_the_workers_emit_loop_survives_a_huge_max_retries` | `unaccounted` 1 | calls `Worker._emit` with a batch no queue took in |
| `test_span_sweep.py::test_two_threads_sweeping_one_shared_span_deliver_each_event_once` | `unaccounted` 8 | sweeps a hand-built span's events, which nothing credited |
| `test_synchronous_loss.py::test_the_increment_happens_under_the_lock` | `unaccounted` 1 | calls `_note_orphan_loss` directly |
| `test_synchronous_loss.py::test_the_lock_is_a_module_global_the_fork_walk_can_reach` | `unaccounted` 2 | calls `_note_orphan_loss` directly |
| `test_task_outliving_span.py::test_the_span_is_left_a_fresh_list_rather_than_a_cleared_one` | `unaccounted` 1 | calls `_flush` on a hand-built span |

The marker is `ledger_exempt`, takes the reason as its argument, and is strict: a marked test
that balances fails, so an exemption that is no longer needed cannot stay.

#### Acceptance Criteria:

- [ ] The fixture is autouse in `tests/conftest.py`, and the whole suite passes with it
      (invariant 2).
- [ ] Exactly the seven tests in the table above carry `ledger_exempt`, each with its reason, or
      the delivery doc names each difference with its reason (invariant 2).
- [ ] With FR-002's debit removed from `Worker._terminal_failure`, at least one existing test
      fails in the fixture, naming `unaccounted`; with the open guard's debit removed, at least
      one fails naming `open_spans`; with the orphan guard's `BaseException` debit removed,
      `test_api.py::test_a_keyboardinterrupt_from_the_sink_still_propagates` fails naming
      `unaccounted`. Each is restored and the suite is green (invariant 2).
- [ ] A test that leaves a traced call's span open on its own context fails in the fixture, and
      the same test passes once the span has closed (invariant 2).
- [ ] A marked test whose process balances fails (invariant 2).
- [ ] No test that uses the `lf` fixture carries the marker (invariant 2).
- [ ] The fixture adds no wait: a test whose sink is stalled when it ends finishes as fast with
      the fixture as without it, and the fixture reports it as balanced (invariants 2 and 3).

---

## Data Model

```python
# src/log_foundry/worker.py, beside Health; exported from log_foundry


@dataclass(frozen=True, kw_only=True)
class EventLedger:
    accepted: int = 0
    open_spans: int = 0  # gauge
    queued: int = 0  # gauge
    in_flight: int = 0  # gauge
    delivered: int = 0
    skipped: int = 0
    in_span_lost: int = 0
    orphan_lost: int = 0
    overflow_lost: int = 0
    batch_lost: int = 0
    sink_lost: int = 0
    drain_lost: int = 0
    stranded: int = 0
    unaccounted: int = 0  # not a term: zero when the terms balance

    @property
    def pending(self) -> int: ...  # open_spans + queued + in_flight

    @property
    def lost(self) -> int: ...  # the six *_lost terms + stranded


@dataclass(frozen=True, kw_only=True)
class Health:
    ...  # the twelve existing fields, unchanged
    events: EventLedger = EventLedger()  # appended


# src/log_foundry/model.py, private
class Span:
    ...  # unchanged
    ended: bool = False  # appended; set by _close_span once span.end is appended
    abandoned: bool = False  # appended; set by the close guard when it takes the buffer


# src/log_foundry/worker.py, private
class _CountingQueue(queue.Queue[object]):
    entered: int  # events of the submissions put, written in _put under the queue's mutex
```

---

## API / Interface Contract

```python
h = log_foundry.health()
e = h.events
assert e.unaccounted == 0                        # in a correct library, always
assert e.accepted == e.pending + e.delivered + e.skipped + e.lost   # by construction

# private, for the arc's next spec; any thread; no Runtime, Worker, queue or loss lock
before = _lifecycle._ledger_snapshot()
...
after = _lifecycle._ledger_snapshot()
lost_in_window = after.lost - before.lost
```

The two stderr lines FR-002 changes, with the changed clause only. The expiry clause was first
written "{n} event(s) in flight, in health().events.in_flight until the emit returns"; built, that
pushed the line past `_diag`'s 200-character detail bound at six-digit counts, which
`test_the_expiry_line_fits_the_detail_bound` holds, so it names the gauge in parentheses instead
(revised during the build):

```
...; {n} event(s) in flight (health().events.in_flight)
...; worker thread stopped; lost {n} event(s) in {k} undrained event-list(s) it held and {q}
queued item(s) undelivered, nothing further will be delivered
```

## File & Folder Structure

```
src/log_foundry/
├── _ledger.py          # new leaf helper: the caller-side cells, their retirement and reader
├── worker.py           # EventLedger, Health.events, _CountingQueue, the worker's terms
├── model.py            # Span.ended
├── decorator.py        # credits and debits at _begin, _end, _close_span, _flush, the sweep
├── api.py              # credits, debits and the orphan gauge in _log
├── _lifecycle.py       # _ledger_snapshot, the check in _shutdown_worker, the child's start
├── _fork.py            # the ledger's registration lock on the roster
└── __init__.py         # exports EventLedger
tests/
├── conftest.py         # the autouse fixture, the ledger_exempt marker, the lf fixture's hand-over
├── test_event_ledger.py            # FR-001, FR-003, FR-004
├── test_event_ledger_defects.py    # FR-002, one scenario per reproduction
├── test_event_ledger_threads.py    # FR-005
└── typed_consumer/     # accepts.py and rejects.py grow
docs/
├── invariants.md, architecture.md, component-inventory.md, README.md (the Health section)
└── release-notes/      # the pending notes
```

## Implementation Phases

### Phase 1: The counters and the snapshot, private (FR-001, FR-005)

- `_ledger.py`, the credits and debits at every site of FR-001's tables, `_CountingQueue`, the
  worker's terms, the cell retirement, and `_ledger_snapshot()` returning an `EventLedger` with
  `unaccounted`. The type is defined here, in `worker.py`, but not exported and not yet a field of
  `Health`, so Phase 1's tests can read it. No public field, no existing counter and no stderr
  line changes. The `drain_lost` debit at `_terminal_failure` lands here, since it changes no
  existing counter or line; its line's rewording stays in Phase 3 (revised during the build). The only existing tests that change are the rosters a new
  module, lock or worker method is entered in (Evidence).
- The tests of FR-001 and of FR-005's first five criteria, read through the private snapshot,
  with the isolated cost measured.

### Phase 2: The surface and the check (FR-003, FR-004)

- `Health.events`, the export of `EventLedger`, and the check at the end of `shutdown()` with its
  line, and the forked child's ledger restart, moved here from Phase 3 so the check reads a
  child's own ledger (revised during the build).
- The suite's per-test ledger reset, in the autouse fixture Phase 4 completes: without it, earlier
  tests' ledger state fired the shutdown line in later tests that pin stderr.
- The typed consumer, the docstrings and the README's `Health` section.

### Phase 3: The defects and the record (FR-002, the rest of FR-005)

- The ordering in `Worker.submit`, the open, close, in-span and orphan guards, `Span.ended`,
  `Span.abandoned` and the orphan on an abandoned span's ids,
  `_terminal_failure` and its line, the expiry line, which now names `health().events.in_flight`,
  the `span.end` build order, and the forked child's start.
- One test per reproduction, and the tests that pinned the old numbers and lines rewritten in
  place.
- `docs/invariants.md`, the §12 and §13 edits and the pending release notes.

### Phase 4: The fixture goes autouse (FR-006)

- The `lf` fixture hands over through the ledger; the `ledger_exempt` marker is registered and
  applied; the fixture is switched on for the whole suite.
- The mutations of FR-006's third criterion, each run and restored.

---

## Evidence

Everything here was measured on 2026-11-15 at `ae0b7674` on macOS, with CPython 3.13.1 unless a
row says otherwise, while other sessions loaded the machine (load average near 60 on 10 cores).
No prototype is committed. The first was the author's; the spec review built the spec as written
from its own reading; the revision took the review's build and changed it as FR-001 to FR-006 now
say. The build re-measures each number in its delivery doc.

**The probes today, re-run for this spec.** `p01` to `p05`, `p08` and `p11` reproduced the core
frame's recorded output; `p03` and `p04` are timing-dependent and moved as the frame says they
do. `p03 2000` printed `dropped=1191` inside `submitted_after_shutdown=11193` (the frame: 1,994
inside 11,994), and in two later runs `submitted_after_shutdown` 11,979 and 11,992. `p04` printed
"19980 event(s) in flight, counted when the emit returns" and lost 8,330 of 20,000.

**The reproductions on the revised prototype.**

| Reproduction | As first drafted (the review's build) | Revised |
|---|---|---|
| `p01` | — | `accepted=36000 delivered=30030 overflow_lost=5970` |
| `p02` | — | at expiry `queued=980 in_flight=20`; after recovery `delivered=1000` |
| `p03 2000` | — | `submitted_after_shutdown` 10,013 to 10,094 against `queued + dropped` 11,830 to 11,959 |
| `p05` | — | first scenario `delivered=600 batch_lost=300`; orphan scenario `orphan_lost=1` |
| `p08` | — | `accepted=100 delivered=20 drain_lost=20 stranded=60` |
| `p11` | — | (a) `in_span_lost=2`; (b) moved by 3; (c) `unaccounted=0` |
| `p14` | — | both rows deliver `span.end`; `accepted=6 delivered=6` |
| `p15` | — | child at the fork `accepted=2 open_spans=2 orphan_lost=0`; after its close `delivered=3` |
| `p16` | `accepted=5 delivered=2 in_span_lost=2 orphan_lost=1` | `accepted=5 delivered=0 in_span_lost=4 orphan_lost=1` |
| A failed close, then a `flush()` from a context holding the span | `in_span_lost=3 delivered=2 unaccounted=2`, and the shutdown line | `in_span_lost=3 delivered=0 unaccounted=0` |
| An in-span build interrupted | `open_spans=1` left over | `in_span_lost=1` |
| An orphan log interrupted, its debit removed | `in_flight=1 unaccounted=0`, no line | `unaccounted=1`, and the shutdown line |

`p11` (c) settles what the core frame left open. Of the four spans that did not deliver, each
costs two events: `in_span_lost` moves by 8 and nothing is unaccounted, in 12 of 12 runs on 3.12
and 3.13.

**The second review's three cases**, run against the second revision's prototype, which is the
second review's build changed as FR-001, FR-002, FR-003 and FR-006 now say.

| Case | Second review's build | Second revision |
|---|---|---|
| `submit` returns without queueing or counting a 7-event submission (`meet_gap.py`) | `accepted=7`, every other term 0, `unaccounted=0`, no line | `unaccounted=7`, and the shutdown line names 7 events |
| 1,000 `shutdown()` calls while six threads log orphans the sink refuses (`orphan_race.py`) | 54 false lines, naming up to 51 events | no line, and no snapshot of 1,000 read `unaccounted` |
| The same, with a no-op call between each read of the cell walk | — | no line, and no snapshot read it |
| The same with the sink accepting, with and without the no-op | 0 lines; 39 of 1,000 snapshots non-zero with the no-op | no line, and no snapshot read it |
| The orphan guard's `BaseException` debit removed (`orphan_mut.py`) | `unaccounted=1`, line written | `unaccounted=1`, line written |
| `p17_log_after_an_interrupted_close.py`, the handler and the task rows | delivered nowhere at `ae0b7674`; a fresh trace in the first revision | delivered on the span's own trace and span ids |
| `p17`, the control row (a task outliving a normal close) | a fresh trace | a fresh trace |

The read order is what decides the stress, not the sign rule alone. Counting the orphan residual
with both signs in the revised order read `-6` in all 1,000 snapshots under the refusing sink, a
call in progress for each of the six threads and never a positive one; the first revision's
order read 304 of 1,000 non-zero, from `-2` to `+6`. With the meeting check added, the suite's
balance failures stayed exactly FR-006's 7 tests, with the readings its table gives, and its own
failures were the 23 the second review counted. The first attempt at the meeting check flagged two
more tests, each of which replaces the process worker by hand after hand-overs to the one before
it; recording the hand-over count when the worker is built, and computing nothing while no
process worker exists, removed both.

**When the drain has ended.** A worker whose sink raises `SystemExit` on its first batch, read in
a loop until its thread ends, 60 trials a run. Reading `is_alive()` after the worker's counts saw
a transient `unaccounted` in 6 of 300 trials, and 3 to 5 of 60 in earlier runs; the review saw 1
of 60 reading `_drain_finished` after them. Reading `is_alive()` before them saw none in 300, and
so did `_drain_finished` read first, by luck of timing: with the dying thread held by a hook
after `_run` sets that event and before the terminal debit, a snapshot reading `_drain_finished`
read `in_flight=2` as `unaccounted=2`, and one reading `is_alive()` read `in_flight=2` with
`unaccounted=0`; after the debit both read `drain_lost=2` and `unaccounted=0`.

**The locks a traced call meets.** On a thread that had logged once, a traced call with one
in-span log was asked to run while another thread held one lock. On `ae0b7674` and on the
prototype it returned while `decorator._loss_lock` or the worker's `_lock` was held, and not
while `decorator._sweep_lock` was. On the prototype it also returned while the registration lock
was held.

**Cell retirement.** 2,000 threads started through `_thread.start_new_thread`, each making a
traced call and an orphan log: the review's build, which folds a cell when `is_alive()` is false,
kept 2,000 cells; the revised one kept none, and both counted all 8,000 events. Eight threads
making 20,000 traced calls each under a 1e-6 switch interval, with a ninth reading `health()`
48,386 times, ended exact with no reading out of balance; the review ran 1,200,000 events
against 116,304 readings, and 40 forks under six crediting threads, with the same result.

**The fixture against the suite.** The review built FR-006's fixture literally; it neither stops
the worker nor polls, and its verdicts were identical over three runs. The revision used it
unchanged. The author's first prototype recorded a different fixture, which stopped the process
worker and polled for up to 100 ms, and its counts are kept below as measured.

Each test was put in one class by reading it; a test the first prototype's run put under the
`lf` fixture is in its real class in the later runs, which hand the `lf` fixture's events over.

| Class | First prototype, left in | First prototype, fixed | Review's build, fixed | Revised, left in | Revised, fixed |
|---|---|---|---|---|---|
| Uses `lf`, with no hand-over in that run | 114 | 114 | — | — | — |
| Kills the drain thread (finding 7) | 19 | 0 | 0 | 19 | 0 |
| Fails a span's open or close (finding 17) | 13 | 0 | 0 | 12 | 0 |
| A `BaseException` through a guard (FR-002 item 6) | 3 | 3 | 4 | 7 | 0 |
| A sweep racing a close, unforced | 1 | 0 | 0 | 0 | 0 |
| Works below the accept sites (FR-006's table) | 8 | 7 | 7 | 7 | 7 |
| **Fails the balance** | **158** | **124** | **11** | **45** | **7** |
| of which use `lf` | 114 | 114 | 3 | 7 | 0 |

"Left in" switches off every fix of FR-002 and the orphan, in-span and close guards' new debits,
and keeps the ledger and the `lf` hand-over; the review measured 156 against the first
prototype's 158 in the same way. In every run with the ledger, the suite's own failures were the
tests that pin `Health`'s field list, a roster of modules, locks or predicates, SPEC-050 FR-003's
close count, or a stderr line FR-002 rewords: 24 in the revised run with the fixes, the same 24
tests as in the review's, and 9,838 passed against 9,857 before the ledger.

**Atomicity of an unlocked increment.** Eight threads, 2,000,000 increments each, switch
interval 1e-6, three rounds, on 3.12.12, 3.13.1 and 3.14.6: no update lost on a module global,
a slot or an instance attribute, while a control that splits the increment with a call lost
about 70%. The statement compiles with no call and no backward jump between its load and its
store.

**Cost, in isolation.** One statement in a loop of 100,000 to 200,000, the minimum of 200 to 300
runs with the candidates interleaved, timed on the thread's CPU clock, with the empty loop
subtracted. Nothing is allocated or grows inside the timed loop: the counting put is measured as
a `put_nowait` and a `get_nowait` against the same pair on a plain `queue.Queue`. The minimum is
taken because the machine was loaded.

| Write | 3.12.12 | 3.13.1 |
|---|---|---|
| Bare `+=` on a global | 15 ns | 14 ns |
| One credit through `_ledger.cell()` | 32 ns | 30 ns |
| The counting `_put`, over a plain one | 21 ns | 18 ns |
| A traced call's writes: two credits, one hand-over, one counting put | 117 ns | 107 ns |
| `with lock:` around the increment | 95 ns | 78 ns |
| One snapshot, one live cell | 3.3 µs | 3.0 µs |
| One snapshot, nine live cells | 6.0 µs | 5.0 µs |
| One snapshot, fifty live cells | 17.1 µs | 14.6 µs |

The first draft's 2.2 µs snapshot was measured with one live cell and without the orphan gauge;
the review measured 4.1 and 4.4 µs with nine. The bound of 12 µs at nine cells is about twice
the largest of these, because the cost grows with live threads and the machine was loaded.

A traced call measured 19 to 22 µs on the caller's thread and one with a log 24 to 29 µs. Two
runs of the unchanged tree differed by 2.5 µs and the prototype fell between them, so the
end-to-end figure says nothing about a change of this size.

---

## Revision history

- **2026-11-15, revised in place during the build** on `spec-088/impl`, each change struck in its
  section: the snapshot's read order is SPEC-091's two passes with the worker's terms first
  (FR-001), after a span shared through a copied context read below zero cell by cell; the
  worker's `in_flight` is measured, with a drain residual in `unaccounted` (FR-001, FR-003), for
  SPEC-090; the registration lock is re-entrant (FR-003), after the plan review's `premise_3b`
  deadlocked a plain one; FR-002 item 6's open-guard sentence is corrected and covers a built span
  whose push is interrupted; the expiry clause is shortened to fit `_diag`'s detail bound; the
  `NATSSink` §12 item is named in FR-004; the `drain_lost` debit and the child's restart move to
  earlier phases.

- **2026-11-15, revised after the second review**, which built the first revision and found three
  blocking defects, all fixed here. ~~"This sum holds by construction"~~: it held only where the
  caller's hand-over meets the queue, which nothing checked, so a submission lost inside `submit`
  read as balanced; the meeting residual now counts in `unaccounted`, with the hand-over credited
  after `submit`. ~~A two-sided orphan residual~~: it fired 54 false lines under refusing sinks;
  the read order now makes a call in progress read only negative, `unaccounted` counts the
  positive side, and the fixture, which reads at rest, checks both. ~~A later log ships as an
  orphan on a fresh trace~~: a log in a context that still holds an abandoned span keeps that
  span's ids, and the four changes a caller can see are listed for the release notes. Phase 1 now
  defines `EventLedger` privately, since its tests read it.

- **2026-11-15, revised after the spec review**, which built the first draft and found five
  blocking defects and nine smaller ones, each with a reproduction, all fixed here.
  ~~A close guard that debits the buffer and leaves it on the span~~: an event could land in
  `in_span_lost` and `delivered` both, so the guard now takes the buffer and closes the span.
  ~~An orphan call in progress read as a remainder of `in_flight`~~: a missing debit was invisible
  to the check and the fixture, so the gauge is measured. ~~Returns while every lock in `_fork`'s
  roster is held~~: a close already takes `_sweep_lock`, so the criterion names the locks the
  ledger adds or writes under. ~~Item 6 named three guards and left an interrupted close's buffer
  on the span~~: it names four and counts the buffer, which removed four of the review's eleven
  imbalanced tests. ~~Supersedes SPEC-036 FR-003's count~~: the count is SPEC-050 FR-003's fourth
  criterion. ~~"the identity holds"~~: it holds by construction, so the criteria assert
  `pending == 0` and `unaccounted == 0`. ~~One snapshot about 2.2 µs; under 5 µs with eight
  threads~~: that was one live cell; the bound is 12 µs at nine. ~~Under 150 ns per traced
  call~~: the entering count's write was missing from the sum; the bound is 200 ns. The drain's
  end, the entering count's shape, `Span.ended`, the open guard's case of a built span, the
  snapshot's locks, cell retirement and the signal-handler wait are now stated, and the expiry
  line moved to the phase that has the field it names.
