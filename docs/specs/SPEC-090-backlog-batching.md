---
type: Spec
id: SPEC-090
title: "Under a Backlog the Worker Sends Larger Batches, and the Exit Drain Sends Its Backlog in Batches"
description: "Lets the worker drain a backlog in batches sized to what its sink delivers in half a flush interval, capped at 1,000 events and with no new setting, and has the exit drain send its backlog by the same rule and stop starting batches it cannot finish, so a burst loses fewer events and an exit counts what it could not send."
spec_status: Completed
depends_on: [SPEC-004, SPEC-021, SPEC-039, SPEC-054, SPEC-062, SPEC-068, SPEC-072, SPEC-088]
arc: audit-2026-11-14
arc_order: 6
---

## Overview

The worker sends a batch every ten finished spans or every second, whichever comes first, and
that never changes, however much is waiting. A burst therefore drains at ten spans per sink call:
against a sink that costs 20 ms a call, 30,000 spans in under half a second lost about 19,800 of
them to a full queue, and the rest took 26 seconds to leave (finding 10 of the 2026-11-14 audit).
At exit the opposite happens: the worker hands its whole backlog to the sink in one call, so a
`shutdown()` that runs out of time leaves nearly 20,000 events inside one call whose outcome
nobody will learn (the capacity half of finding 9). Andrew decided on 2026-11-15 that capacity is
not a setting: asked "For burst capacity, should the worker send larger batches under backlog, or
expose settings?", he answered "Larger batches, no setting". This spec has the worker grow a batch
while a backlog is waiting, up to what its sink delivers in half a flush interval and never past
1,000 events, and has the exit drain send its backlog by the same rule. Because the exit drain
then sends many batches, it also stops starting a batch it predicts cannot finish before the
`shutdown()` deadline, counts what it did not send and says so in one line. Nothing changes when
nothing is waiting, and no argument is added anywhere.

## Scope

### In Scope

- One batching rule for the steady drain: a batch grows past ten submissions only while more are
  waiting, up to a size target measured from the sink's own rate, with the cap of 1,000 events a
  hard ceiling except for a batch today's rule would also send; with nothing waiting the worker
  batches exactly as it does today (FR-001).
- The exit drain continues the batch the steady drain was building and sends the backlog by the
  same rule, answering each flush marker in queue order through the steady drain's own helper;
  the expiry line's in-flight figure counts every event the drain thread holds (FR-002).
- The exit drain's budget: once it has declined a retry, or predicts from the longest attempt it
  has seen that a batch would end past the deadline, it declines every later batch unattempted,
  counts each, and writes one line for them; at exit its abandon lines are aggregated too
  (FR-003).
- Every shipped sink is shown to take a capped batch whole, and a sink whose cost is per event is
  shown to keep its emits shorter than the flush interval (FR-004).
- No setting, and the record: the decision Andrew's answer keeps standing, the exit-drain decision
  this spec refines, the §12 items it closes, the prose, the derived rosters and the release notes
  owed (FR-005).

### Out of Scope

- **Any `configure()` argument, `Worker` constructor argument, environment variable or other
  public tunable for the batch size, the cap, the queue size or the flush interval.** Andrew's
  answer of 2026-11-15, and the public-api decision *A frozen surface is keyword-first, and says
  what it will not grow*, which it keeps standing (invariant 10).
- **The queue's capacity.** It stays 10,000 submissions. It is the library's memory bound, and
  Andrew's answer named batches, not the queue (FR-001 says what a full queue costs in memory).
- **Shipping an open span's events before the span closes** (finding 8). Andrew answered it on
  2026-11-15, "ship on the worker's ~1 s interval", and it is a later spec of this arc, written
  after this one. FR-001 says why this spec's rule will not conflict with a worker that also
  drains open spans on its interval.
- **`FlushResult`'s loss counts** (SPEC-089). SPEC-089 is ahead of this spec in the PR queue and
  lands on `main` first; FR-002 says how the two specs' changes to the marker answerers meet.
- **Acknowledgement fixes in sinks**, and any change to a sink's code. FR-004 changes no sink; the
  one sink-side residual it measures is filed in `docs/architecture.md` §12, not fixed here.
- **Splitting a submission.** A span's events stay in one batch, so a single submission of more
  than 1,000 events, and `batch_size` submissions whose events add up past it, are sent as
  today's rule sends them.
- **The orphan path**, which sends one event per call and has no batch.
- **A loss no process survives to count.** When a single attempt outlasts the join at interpreter
  exit, what the drain thread holds and what is still queued are lost with the process, as
  SPEC-088's Out of Scope records; this spec makes what the drain holds at most one batch and one
  submission, and FR-003 makes that case rarer.

---

## Functional Requirements

### FR-001: A batch grows while a backlog is waiting, to half an interval of the sink's rate, with 1,000 events as a ceiling

#### Description:

**Today.** `Worker.__init__` sets `batch_size=10`, `flush_interval=1.0` and `max_queue=10_000`
(`worker.py:361-363`), and `_lifecycle.py:3796` builds the process worker with those defaults.
`_drain` takes one queue item per `get` (`worker.py:1723`) and emits the pending batch when it
holds `batch_size` submissions or `flush_interval` has passed since the last emit
(`worker.py:1741`). A submission is one finished span's events, so under a backlog every batch is
ten spans, whatever the sink could take. `p13_burst.py` at `3e27a0a6` (Evidence): 1,014 to 1,214
emit calls of exactly 20 events each, a backlog that took 24.9 to 27.7 s to drain after the burst,
and 17,861 to 19,860 of 30,000 submissions dropped, the most when the burst was fastest.

**The rule.** Two methods decide a batch, `_take` and `_complete`, and both drains call them
(FR-002).

1. **With nothing waiting, nothing changes.** When the batch holds `batch_size` submissions and
   the queue holds no item, it is emitted, as today. The time trigger and a flush marker emit the
   pending batch whatever its size, as today. The shutdown sentinel and a `_stop` seen at the top
   of the loop emit nothing, also as today: the sentinel breaks out (`worker.py:1736-1737`) and
   the loop condition exits (`worker.py:1720`), leaving `pending` partly filled, and `_drain`
   hands `pending` and `_pending_events` to the exit drain, which continues that batch
   (`worker.py:1744`; FR-002).
2. **A backlog is an item waiting in the queue when the count trigger would fire.** Then the batch
   grows by whole submissions while they fit within the **size target** `T`. After `_take`
   takes a submission `s` into a batch of `p` submissions and `e` events: when `p >= batch_size`
   and `e + len(s) > T`, the batch is emitted without `s`, and `s` begins the next batch (the
   **carry**); otherwise `s` joins the batch. `_complete` then reports the batch complete when it
   holds at least `batch_size` submissions and either nothing is waiting or its events have
   reached `T`. The waiting check is the queue's own `empty()`, which takes the mutex `get`
   already takes.
3. **The ceiling.** Since a batch holds more than `batch_size` submissions only while its events
   stay within `T`, and `T` never exceeds the cap, **no batch exceeds 1,000 events unless it holds
   at most `batch_size` submissions**, which is a batch today's rule would also send: ten spans of
   200 events, or one span of 5,000. The example the spec review gave, 499 two-event spans then a
   600-event span, is sent as 998 events and then a batch that begins with the 600.
4. **The size target** is set after each batch the drain emits, from that batch alone, and read
   by the next:
   - `0` when that batch was counted in `failed_batches`, when the sink generation (rule 5)
     changed while it was being emitted, or when it held no events;
   - the cap when its elapsed time was zero or negative;
   - otherwise `min(1_000, events / elapsed × flush_interval / 2)`.

   `elapsed` is `time.monotonic()` read before and after `_emit_pending`'s call of `_emit`, so its
   retries and the engine's waits lower the rate. `T` is `0`, which is today's batch, for a new
   worker, for a forked child (rule 6) and while the sink generation differs from the one `T` was
   measured under. A batch the exit drain declines without an attempt (FR-003) was not emitted
   and leaves `T` as it was.
5. **The sink generation.** `_sink_generation` is an `int` that `retarget` increments where it
   reassigns `self.sink` (`worker.py:1123`), under `Worker._lock`. `_emit` reads `self.sink` for
   the engine (`worker.py:1934`) and again on every 1.x attempt (`worker.py:1960`), so a batch can
   straddle a swap. The drain reads the generation before and after the emit and discards a
   measurement that straddled one. An `int` rather than the sink object, so nothing pins a sink a
   swap replaced: `Worker._FORK_SKIP` and `tests/test_child_sink_slots.py`'s slot table
   (`:54-76`) do not change.
6. **The fields and a fork.** The drain thread alone writes `_pending_events`, `_target` and
   `_target_generation`; `_sink_generation` is written under `_lock` by `retarget`. FR-003 adds
   the exit drain's fields. `_reinit_after_fork` resets every one of them except
   `_sink_generation`, which the child inherits harmlessly, as it inherits `sink`: `_pending_events`
   to `0`, since the child's drain thread starts with an empty `pending`, and `_target` to `0`, so
   a child's first batch is today's. Each new attribute gets a row with its child column in
   `tests/test_lifecycle_inventory.py`'s Worker table (`:1051-1082`), which fails on a `self.X` it
   does not list.
7. **The cap is 1,000 events**, a private module constant `_BACKLOG_BATCH_CAP` of `worker.py`,
   read at each use, as `_JOIN_MARGIN` is, so a test can patch it. It is not a `Worker` argument.

**Why the target is half an interval of the sink's own rate.** It keeps an emit shorter than the
time trigger's interval, so the wait a later event, a flush marker, a swap's fence or an exit's
join sees behind a batch in flight stays within what the time trigger already allows. And it stops
growth where growth stops paying: a sink whose cost is per call (`p13`'s, or the HTTP family up to
its request size) reaches the cap at once, while one whose cost is per event (SQS at ten entries a
request, `NATSSink` through JetStream at one ack per event) settles near half an interval's worth,
because a bigger batch only makes its call longer. **Half, not a whole interval**, because the time
trigger's clock is read before the emit (`worker.py:1740-1743`): with a whole interval an emit
lasted as long as the interval, the trigger fired on the first submission after it, and against a
sink costing 20 ms an event 22 of 65 batches held a single span (Evidence). A fixed cap with no
rate measured it the other way: a JetStream `NATSSink` behind a 20 ms ack lost 976 of 2,000 events
to its own 10 s `publish_timeout`, where today it loses none (FR-004, Evidence).

**Why elapsed time of zero gives the cap.** An emit the clock cannot see took less than the
clock's resolution, which is the limit of an unbounded rate, and the cap is what bounds that
limit. Keeping the previous target instead would let a frozen clock pin whatever it last
measured. The case is real: `tests/test_sinks_file.py:162` patches `time.monotonic` to a constant,
under which every emit reads zero, and an exception on the drain thread is terminal, because
`_run` catches it, records it and ends the thread (`worker.py:1527-1538`).

**Why the cap is 1,000.** It is the largest default per-request count of any shipped sink: the
HTTP family's `DEFAULT_MAX_BATCH_COUNT` (`sinks/http.py:52`) and the 1,000 its subclasses set,
and the `chunk_size` of `ClickHouseSink` and `PostgresSink` (`sinks/clickhouse.py:87`,
`sinks/postgres.py:135`). Past it, a chunking sink makes more requests per call and saves no
call. What a larger cap would add is loss granularity: a sink that fails after running fast
abandons its batch whole, so the cap bounds that at 1,000 events. A fixed cap of 2,000 on `p13`
dropped about 5,830 submissions against about 13,000 at 1,000 (Evidence), because `p13`'s sink
costs the same per call however large the batch, which is true of no shipped sink above 1,000
events.

**The queue is unchanged.** It holds 10,000 submissions, and that is the library's memory bound:
10,000 queued two-event spans held 15.8 MB, 1,584 bytes each (Evidence), so a queue ten times
larger would hold more than a 128 MB Lambda has. A batch holds references to events the queue
already held, so growing it moves no memory bound.

**What the caller's threads pay.** Nothing on their own path: `Worker.submit` is unchanged and
the rule runs on the drain thread. But a drain that keeps pace with a burst does Python work for
every event it takes, and under the GIL that time comes from the application's threads. Today's
tree already pays it whenever its sink is fast: 30,000 traced calls into an instant sink took
0.61 to 1.24 s, against 0.38 to 0.65 s into the 20 ms sink in the quieter `p13` runs, a sink that
drops two thirds of the burst instead. A burst the drain now keeps pace with costs what a fast
sink's burst costs today. The runs could not bound it more tightly than that (Evidence), so no
criterion states a ratio.

**A later spec drains open spans on the worker's interval** (Andrew, 2026-11-15, "ship on the
worker's ~1 s interval"). The rule is stated over submissions and events, never over spans, and
reads no span state, so a submission holding part of an open span's buffer is batched like any
other. It keeps the time trigger as the one interval, so a worker that also sweeps open spans on
that interval has one clock to follow, and the size target is measured from emits, which such a
sweep feeds like any other submission.

**What SPEC-088's ledger shows.** Under a burst `overflow_lost` falls (`p13`: from about 39,700
events to about 24,500 to 26,600). A batch abandoned while the sink was fast moves `batch_lost` by
up to 1,000 at once, where today it moves by ten spans' events; the next batch is then today's
size, by rule 4. `in_flight` holds the batch being emitted, the batch being built and at most one
carried submission.

**Each take is counted where it is taken** (added 2026-11-16): `_take` calls `_hold` straight
after `get` returns, before the join-or-carry decision and before any emit, so a carried
submission is in `in_flight` from its take, as the sentence above has it, because one counted only
after the previous batch emitted would leave the window from take to `_hold` open across a sink
call, over-reading `health().events.queued` for that long and failing the suite's balance
fixture whenever the call outlasts the 2 s it re-reads a drain outside `get`
(`tests/ledger_check.py`), while a shorter call would be hidden from it.

**A carry emit restarts the interval** (added 2026-11-16): when `_take` emits the batch ahead of
the submission it took, `_drain` sets `last_flush` to the clock read just before that take, as
the count trigger's emit sets it to the clock read before its emit. Otherwise a carry emit lasting
about half an interval left the time trigger to fire on the take after the next one, and a sink
whose cost is per event was handed single-span batches under a backlog.

#### Acceptance Criteria:

- [ ] `p13_burst.py` on 3.12 and 3.13 sends no batch above 1,000 events and drains its backlog in
      under 2 s after the burst, where today it takes 24.9 to 27.7 s (invariant 2).
- [ ] `spec090_burst_gated.py` on 3.12 and 3.13 reads `dropped == 19,990` on today's tree and the
      build alike, and the build carries the released backlog in at most 30 emit calls of at most
      1,000 events, where today it takes 1,000 calls of 20 (invariant 2).
- [ ] `Worker.submit` is unchanged, and a test that records the thread each call of `_take`,
      `_complete` and the target's measurement runs on finds only the drain thread (invariant 4).
- [ ] One span every 30 ms for 100 spans is sent in ten batches of 20 events, as today, and a
      batch whose tenth submission leaves nothing queued is emitted at ten submissions whatever
      the size target (invariant 2).
- [ ] With the target set to the cap, 499 two-event spans then a 600-event span then 10 more
      two-event spans, all queued at once, are sent as batches of 998 and 620 events (invariant 2).
- [ ] A sink costing 2 ms per event, sent 1,000 spans at once, receives all 2,000 events and is
      never held in one emit for longer than 1 s (invariant 3).
- [ ] With `time.monotonic` patched to a constant, a backlog of 2,000 spans is delivered whole, no
      batch exceeds 1,000 events, and `health().stopped_reason` stays `None` (invariants 1 and 2).
- [ ] After a batch that is abandoned, after `configure(sink=...)` installs a sink, after a batch
      whose emit straddled a `retarget`, and in a forked child, the next batch under a backlog
      holds exactly `batch_size` submissions (invariants 2, 6 and 12).
- [ ] A `flush()` made behind a backlog is answered once the batches carrying the events ahead of
      its marker are emitted, and no batch holds events from both sides of a marker
      (invariants 2 and 3).
- [ ] With the cap patched to 50, a backlog of two-event spans into an instant sink is sent, after
      its first batch, in batches of exactly 50 events (invariant 2).
- [ ] `tests/test_lifecycle_inventory.py` lists every attribute this spec adds to `Worker`, each
      with its child column, and `_reinit_after_fork` resets each one the table marks rebuilt
      (invariant 12).

### FR-002: The exit drain continues the steady drain's batch and sends the backlog by the same rule

#### Description:

**Today.** `_final_drain` takes everything queued into one list (`worker.py:1804-1815`), appended
to the `pending` `_drain` left it, and emits it in one call (`worker.py:1817`), answering every
flush marker it met only after that call. `p04_exit.py 0.02 10000` at `3e27a0a6` (Evidence): the
sink was handed 19,980 events in one call, had taken 10,540 to 11,800 of the 20,000 when the
process ended 30.3 to 33.1 s later, `close()` never ran, and the expiry line named 19,980 events
in flight, more than 10,500 of which the sink had already taken.

**The rule.** The exit drain continues the batch `_drain` handed it, with its `_pending_events`,
and takes items without waiting, passing each submission through `_take` and emitting whenever
`_complete` reports the batch complete, with the queue counted as waiting; the remainder goes when
the exit drain ends. It takes at most the entries the queue held when it began, so a caller that
keeps logging after `shutdown()` cannot keep it running; what arrives later stays queued, in
`stranded` and `submitted_after_shutdown`, as it does today when it arrives after the one sweep.

**A marker is answered where it is met, through one helper.** The exit drain answers a flush
marker the moment it meets it, after emitting the batch ahead of it, through one helper,
`_answer_marker`, which `_drain`'s marker branch (`worker.py:1726-1736`) calls too, so both drains
take the marker, compute `delivered` and set its event in one place. SPEC-089 lands on `main` before this spec and rewrites
both of today's answerers, `_final_drain`'s `finally` included, to set `_FlushMarker.unwinding`
and take its ledger snapshot. Whichever spec lands second moves the other's change into the one
helper: this spec's build carries SPEC-089's `unwinding` and snapshot into it, and deletes the
second copy in `_final_drain`. A marker no longer waits for the batches behind it.

**The expiry line counts what the drain thread holds.** Its in-flight figure is today the batch
inside `_emit` (`_in_flight`, read at `worker.py:1440`, written at `:1931`). Under this rule the
drain thread can hold more: the batch being built and one carried submission. The figure becomes
SPEC-088's worker `in_flight`, every event the drain thread has taken and not finished with, so
the line and the ledger agree. It is one read of a counter only the drain thread writes, so the
stopper reads one value and never adds two.

**What an expiry at exit now costs.** An expiry needs a single attempt that outlasts the join,
because FR-003 declines a batch predicted to. Then the drain thread holds at most one batch and
one submission, and the rest is still queued. The line names both, and of what it names only the
events the thread holds have an unknown fate; today that was the whole backlog.

**Twin paths.** `_drain` and `_final_drain` are invariant 6's named twins. Both call `_take` and
`_complete`, and both answer a marker through one helper, so a batch boundary or a marker's verdict
cannot be decided one way on one path and another way on the other.

#### Acceptance Criteria:

- [ ] `p04_exit.py 0.02 10000` on 3.12 and 3.13 hands the sink no batch above 1,000 events, where
      today it hands it 19,980 in one call (invariant 2).
- [ ] When a sink blocks past the deadline inside the third exit batch, the expiry line's in-flight
      figure equals `health().events.in_flight`, it is at most one batch and one submission, and
      the events the sink received plus the queued submissions' events plus that figure equal
      the events accepted (invariants 2 and 11).
- [ ] A worker built with `batch_size=1` and a 60 s interval, its cap patched to 10, that has
      measured one batch to its current sink so its target is at or above the cap, with 30
      two-event submissions queued at exit and a flush marker after the fifth, sends six batches
      of 10 events and answers the marker truthy after the first and before the second; today it
      sends the 60 events in one call and answers the marker after it (invariants 2 and 6).
- [ ] A thread that keeps submitting after `shutdown()` begins does not keep the exit drain
      running: the drain takes no entry queued after it began, and each such submission is counted
      in `submitted_after_shutdown` (invariants 2 and 3).
- [ ] A `BaseException` raised from the second exit batch leaves the markers answered before it
      answered, and every later marker is released by the drain thread's exit within its
      `flush()` timeout; with SPEC-089 on `main`, a marker answered while that exception unwinds
      has `unwinding` set, exactly as `_drain`'s branch sets it (invariants 1, 3 and 6).
- [ ] A test asserts that `_final_drain` and `_drain` answer a marker only by calling
      `_answer_marker`, and that neither sets a marker's event itself (invariant 6).
- [ ] For one queue and one target, the exit drain continuing a batch `_drain` left with three
      submissions makes the batch boundaries the steady drain makes for the same submissions
      queued behind those three (invariant 6).

### FR-003: The exit drain stops starting batches it cannot finish, and its loss lines stay few

#### Description:

**What batching the exit drain would cost without this.** SPEC-062 settled that every batch's
first attempt is made and only a retry is declined past the deadline. With one exit batch that
cost at most one extra attempt. With many it does not. The spec review's probe, adapted as
`spec090_exit_budget.py`: a sink whose first emit waits on a gate and whose every later emit
sleeps 0.5 s and raises, 30 submissions queued, `shutdown(timeout=1.2)`. Today: 1.05 to 1.06 s, 2
attempts, the join completed and `close()` ran once, which is the measured outcome the
sink-lifecycle decision *The exit drain declines only a retry predicted to end past the shutdown
deadline* records. A per-batch exit drain that makes every batch's first attempt: 1.20 s, 3
attempts, the join expired and `close()` never ran (Evidence).

**The rule.** An **exit batch** is a batch whose first attempt would begin after `claim_stop`
recorded the deadline. The first exit batch's first attempt is always made. From then on the
drain keeps the longest attempt any exit batch has made, and whether one has declined a retry.
Before a later exit batch's first attempt, it declines that batch, unattempted, when either:

- an exit batch has declined a retry, through `_retry_declined` or an engine run that reports
  `declined`; or
- `_retry_declined(longest)` predicts the attempt would end past the deadline, `longest` being the
  longest attempt any exit batch has made. An engine run is timed whole and counts as one attempt,
  which overstates it and so declines no later than the true attempts would.

Once one batch is declined, every later exit batch is declined too. A deadline of `None` declines
nothing, as today. A declined batch moves `failed_batches` by one and its events go to SPEC-088's
`batch_lost`, as a declined retry's do; it does not move the size target (FR-001 rule 4), so the
batches it is cut into are the rule's, not ten spans each.

**What this refines.** "The first attempt of every batch is always made" becomes "the first
attempt of the first exit batch is always made". The decision's reason holds as it was written: a
batch predicted to end after the deadline is not work the process gets, since the join expires,
the sink is left open and the process exits or is frozen. Under the rule the decision's measured
outcome holds with three exit batches: `spec090_exit_budget.py` on the prototype took 1.06 s,
made 2 attempts, completed the join and ran `close()` once, and counted the 40 events of the two
batches it declined (Evidence). At interpreter exit with a healthy but slow sink, `p04_exit.py`
now ends inside its budget with `close()` run, and its one line counts the 7,828 to 7,836 events
the sink was not handed, which today were lost with the process uncounted. In a process that goes
on running after an expired `shutdown()`, those batches are counted lost where today the drain
thread would have delivered them late; the release notes say so.

**`NATSSink`'s exit arithmetic still holds.** `sinks/nats.py:44-58` bounds the exit drain by the
`shutdown()` budget plus up to two `publish_timeout`s, "the emit in flight when the budget runs
out, and the first attempt of a tail batch, which is never declined". Under this rule that stays
true with "a tail batch" meaning the exit drain's first batch, the only one whose first attempt is
never declined: a later batch's attempt is made only when the longest attempt seen predicts it
fits, and one that outlasts that prediction is itself the emit in flight when the budget runs
out. FR-005 rewords the sentence to say so, and `sinks/nats.py:313-315`, whose "a batch's first
attempt is always made" is no longer true of a later exit batch.

**The lines at exit.** A per-batch exit drain writes a line per batch it loses: a dead sink that
raises at once costs about 1,000 abandon lines for a full queue, where today it costs one
(Evidence). So at exit:

- The first exit batch abandoned writes today's line.
- Every later abandoned exit batch, and every declined one, is added to one of two tallies, the
  events and the batches, kept under `Worker._lock` beside `failed_batches`, which moves first, by
  `_diag`'s record-first rule.
- A tally is written as one line per kind, abandoned and declined, by whichever takes it first:
  the exit drain when it ends, in a `finally`, or `stop()` when its join expires, before its
  expiry line. Taking it zeroes it under the lock, so no batch is announced twice. A drain thread
  that goes on after an expired join writes what it adds after that at its own end.

So a dead sink at exit writes two lines where a per-batch drain would write about a thousand, and
the declined batches of `p04` are one line. A retry declined inside an exit batch still writes
SPEC-062's line, once, since every batch after it is declined.

#### Acceptance Criteria:

- [ ] `spec090_exit_budget.py tree` against the build on 3.12 and 3.13, with three exit batches,
      returns within the 1.2 s budget with the join completed, 2 attempts made, `close()` run once
      and `failed_batches == 3`; `spec090_exit_budget.py batched` on today's tree reads the join
      expired, 3 attempts and `close()` never run (invariants 2, 3 and 5).
- [ ] `p04_exit.py 0.02 10000` on 3.12 and 3.13 ends with `close()` run and one line counting the
      declined events, and the events the sink received plus the events that line counts equal
      20,000; today `close()` never runs and no line counts the 8,200 to 9,460 events lost
      (invariants 2, 3 and 11).
- [ ] `spec090_dead_sink_exit_lines.py` writes at most three `log-foundry:` lines, and the events
      they name add up to every event accepted (invariants 2 and 11).
- [ ] With a deadline of `None` and a sink that always raises, every exit batch makes
      `max_retries + 1` attempts and none is declined (invariant 3).
- [ ] With the deadline already past when the exit drain begins, the first exit batch's first
      attempt is still made and every later batch is declined unattempted (invariant 3).
- [ ] When the join expires after two exit batches were abandoned, the abandon tally's line is
      written once, before the expiry line, and the drain thread writes no second line for those
      batches when it ends (invariant 11).
- [ ] An engine sink whose run declines a retry at exit makes every later exit batch declined
      unattempted, as a 1.x sink's declined retry does (invariant 6).

### FR-004: Every shipped sink takes a capped batch whole, and a per-event sink keeps its emits short

#### Description:

**No sink's largest batch grows.** Today's count trigger counts submissions of any size, and the
exit drain hands over the whole queue, 19,980 events in `p04`; SPEC-038 measured 5,980 for one
exit backlog (`sinks/nats.py:37`). Under FR-001's ceiling a batch above 1,000 events holds at most
`batch_size` submissions, which today's rule also sends. So this spec makes the largest batches
smaller at exit and makes batches up to 1,000 events frequent under a backlog, but sends no sink
a batch larger than one it already receives. What changes for a sink is how often it sees a large
batch, and how long that call takes.

**How each shipped sink takes a batch** (at `3e27a0a6`):

| Sinks | One batch reaches the destination as |
|---|---|
| `HTTPSink` and its subclasses: Elasticsearch, OpenSearch, Datadog, Honeycomb, New Relic, Loki, Splunk HEC, OTLP; `LogstashSink` in HTTP mode | requests of at most 1,000 events and the class's byte limit (`sinks/http.py:52`, `:60`) |
| `KinesisSink`, `FirehoseSink` | requests of at most 500 records (`sinks/kinesis.py:64`, `sinks/firehose.py:64`) |
| `SQSSink`, `SNSSink` | requests of at most 10 entries (`sinks/sqs.py:185`, `sinks/sns.py:69`) |
| `ClickHouseSink`, `PostgresSink` | inserts of at most `chunk_size`, 1,000 by default |
| `AzureEventHubsSink` | as many `EventDataBatch` packs as the hub's size limit needs (`sinks/eventhubs.py:346`) |
| `MongoDBSink`, `SQLiteSink`, `RedisStreamsSink`, `RedisListSink` | one `insert_many`, `executemany` or pipeline per round (`sinks/mongodb.py:428`, `sinks/sqlite.py:410`, `sinks/redis.py:283`) |
| `KafkaSink`, `RabbitMQSink`, `GooglePubSubSink`, `SentrySink`, `NATSSink` | one hand-off or publish per event |
| `FileSink`, `RotatingFileSink`, `StdoutSink`, `StderrSink`, `SyslogSink`, `LogstashSink` in socket mode, `LoggingSink` | one line, record or frame per event |
| `MemorySink`, `NullSink`, `CallbackSink` | the list as given |
| `MultiSink`, `FilteringSink`, `TransformSink` | whatever the sinks they wrap do |

**Two sinks are sensitive to a call's length, not its size.** `NATSSink` bounds a whole call by
one `publish_timeout`, 10 s by default (`sinks/nats.py:31`), and a JetStream publish waits for its
ack, so a batch whose acks outlast the budget leaves its tail unattempted and, when something
landed, `REJECTED` (`sinks/nats.py:240-276`). `GooglePubSubSink` waits on its oldest futures once
more than `max_pending`, 1,000 by default (`sinks/pubsub.py:50`), are outstanding, so a batch at
the cap never exceeds it alone. FR-001's size target keeps a per-event sink's call near half an
interval, a twentieth of `NATSSink`'s default budget. Measured (Evidence): a JetStream `NATSSink`
behind a 20 ms ack, sent 1,000 spans, lost none today, lost 976 events with a fixed 1,000-event
cap, and loses none under the rule, whose longest call was 0.504 to 0.506 s.

**The residual is filed, not fixed.** A `NATSSink` whose `publish_timeout` is shorter than half
the flush interval, or whose ack time grows more than twentyfold between one batch and the next,
can still be handed a batch it cannot finish within its budget, and loses that batch's tail,
counted and announced. The completion ritual files this in `docs/architecture.md` §12 with its
probe and its closure, which is `NATSSink` answering an event the deadline left unattempted as
`RETRYABLE`, since it never reached the driver and a resend cannot duplicate it. That is a change
to a sink's verdict, which this spec's Out of Scope leaves to the sinks track.

#### Acceptance Criteria:

- [ ] Every sink in `tests/sink_rigs.py`'s `RIGS`, handed one batch of 1,000 events, delivers all
      1,000 through its double and moves none of its loss counters (invariants 2 and 7).
- [ ] `HTTPSink` and each shipped subclass, handed 1,000 events through an opener double, sends
      the number of requests its count and byte limits predict and loses none (invariant 7).
- [ ] A test derives every public sink class under `src/log_foundry/sinks/` from the tree and fails
      on one missing from a table that names how it takes a batch, as the table above does
      (invariant 2).
- [ ] `spec090_nats_ack_backlog.py 0.002 1000 10`, a JetStream `NATSSink` behind a 2 ms ack with
      the default `publish_timeout` sent 1,000 spans through the worker, acks all 2,000 events
      with `health().sink.failed == 0`, and no `deliver` call lasts longer than 1 s
      (invariants 2 and 3).
- [ ] `docs/architecture.md` §12 carries the `NATSSink` residual with its probe, its measured
      numbers and its closure, and names this spec (invariant 2).

### FR-005: No setting is added, and the record says why

#### Description:

Andrew's answer keeps the public-api decision *A frozen surface is keyword-first, and says what it
will not grow* standing: the worker's tunables stay unreachable from `configure()`, which invariant
10 states. This spec adds no public name, so a typed consumer written against today's surface sees
no change. The completion ritual writes the record:

- **A new decision**, `docs/decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md`,
  quoting the question and the answer, with the rule of FR-001 as its fence and its History dated
  2026-11-15 for the decision and the build's date for the mechanism. The public-api decision's
  History gains a dated entry saying the answer kept it, and pointing to the new file.
- **A refined decision.** `docs/decisions/sink-lifecycle/the-exit-drain-declines-only-a-retry-predicted-to-end-past-the-shutdown-deadline.md`
  is rewritten in place for FR-003, title and fence included: the first attempt of the first exit
  batch is always made, and a later exit batch is declined once one is predicted to overrun. Its
  History gains a dated entry, and its measured outcome is restated with three exit batches.
- **`docs/architecture.md` §12.** The item for finding 10 ("The queue holds 10,000 submissions and
  a batch is 10 submissions…") is struck as closed by this spec, with `p13`'s numbers before and
  after. The item for finding 9, as SPEC-088 leaves it, narrowed to its capacity half, is struck as
  closed, with `p04`'s. FR-004's residual is filed.
- **The prose, rewritten in place or marked superseded.** Every hit of
  `grep -rn 'single emit\|exit backlog\|whole backlog\|one final batch\|as one batch\|as a single batch' src/ README.md`
  at `3e27a0a6`, each with what the build does with it:

  | Hit | Says | The build |
  |---|---|---|
  | `worker.py:1787` | `_final_drain` "emits the tail as one final batch" | rewrites |
  | `sinks/nats.py:37` | the exit backlog goes "as a single batch" | rewrites |
  | `sinks/nats.py:40` | "`_final_drain`'s single emit runs on that thread" | rewrites |
  | `sinks/nats.py:307` | in a superseded blockquote, "hands the exit backlog over as one batch" | adds a superseded marker |
  | `sinks/nats.py:1601` | `_final_drain` "hands the exit backlog over as one batch" | rewrites |
  | `sinks/http.py:78`, `:130`, `:2061` | SPEC-038's measured 5,980-event exit backlog, in the past tense | keeps: history |
  | `README.md:236` | "sweeps anything still queued into one final batch" | rewrites, with the "By count" bullet above it |
  | `README.md:967` | in a superseded blockquote, "backlog over as one batch" | adds a superseded marker |

  The grep does not reach three more, which the build rewrites too: `README.md:743-744` ("one
  large `emit` — the whole pending backlog at process exit, for instance —"), the arithmetic at
  `sinks/nats.py:44-58` (FR-003), and `sinks/nats.py:313-315` ("a batch's first attempt is always
  made"). And the `batch_size` docstring at `worker.py:407`.
- **The release notes owed.** No pending file exists under `docs/release-notes/`, and
  `docs/process/completion-ritual.md:37-38` tops up the pending notes only "when a tag is being
  prepared and that file exists". So, as SPEC-082 did (its spec `:704-706`, its delivery doc
  `:119-120`), the delivery doc carries a "Release notes owed" heading, or the pending file if one
  exists on `main` when this spec completes. The changes it lists: a sink sees batches of up to
  1,000 events under a backlog where it saw ten spans, and several batches at exit where it saw
  one; a flush marker met at exit is answered in queue order; the expiry line's in-flight figure
  counts every event the drain thread holds; and an exit drain predicted to overrun now declines
  its later batches, counted and announced in one line, so `close()` runs, where a process that
  goes on running after an expired `shutdown()` used to have them delivered late.

#### Acceptance Criteria:

- [ ] `inspect.signature` of `configure`, `Worker.__init__`, `flush`, `shutdown` and `health` is
      unchanged, `log_foundry.__all__` is unchanged, and `tests/typed_consumer/` passes unchanged
      (invariant 10).
- [ ] The new decision file exists in `docs/decisions/pipeline/`, quotes the question and the answer
      word for word, and the public-api decision's History carries the dated entry that points to
      it (invariant 10).
- [ ] The exit-drain decision in `docs/decisions/sink-lifecycle/` states FR-003's rule in its title
      and fence, and its History carries a dated entry naming this spec (invariant 3).
- [ ] The §12 items for findings 10 and 9 are struck as closed by this spec, each with its probe's
      numbers before and after (invariant 2).
- [ ] Each hit in the table above is rewritten, marked superseded or kept as the table says, and so
      are the three sites the grep does not reach and `worker.py:407` (invariant 2).
- [ ] The release-notes entry exists, in the pending file under `docs/release-notes/` if one
      exists on `main` when this spec completes and otherwise under a "Release notes owed" heading
      in the delivery doc, and names each change listed above (invariant 10).

---

## Data Model

```python
# src/log_foundry/worker.py, private

_BACKLOG_BATCH_CAP: int = 1_000
"""Events. Read at each use so a test can patch it, as _JOIN_MARGIN is."""


class Worker:
    ...  # constructor unchanged: batch_size=10, flush_interval=1.0, max_queue=10_000
    # FR-001; drain thread only unless said; all but _sink_generation reset by _reinit_after_fork
    _pending_events: int  # events in the batch being built
    _target: float  # the size target in events; 0 means today's batch
    _target_generation: int  # the sink generation _target was measured under
    _sink_generation: int  # bumped by retarget under _lock; inherited by a forked child
    # FR-003; drain thread only, except the tallies, which are under _lock
    _exit_batches: int  # exit batches whose first attempt was made
    _exit_longest: float  # the longest attempt any exit batch has made
    _exit_declining: bool  # a retry was declined, or a batch was; every later one is declined
    _exit_abandoned_seen: bool  # the first abandoned exit batch has written its own line
    _abandoned_tally: tuple[int, int]  # (events, batches) not yet announced
    _declined_tally: tuple[int, int]  # (events, batches) not yet announced

    def _take(self, pending: list[list[dict[str, object]]], item: list[dict[str, object]]) -> None:
        ...  # FR-001 rule 2: joins item to the batch, or emits the batch and carries item
    def _complete(self, pending: list[list[dict[str, object]]], *, waiting: bool) -> bool:
        ...  # FR-001 rule 2: batch_size submissions, and nothing waiting or the target reached
    def _answer_marker(self, pending: list[list[dict[str, object]]], marker: _FlushMarker) -> None:
        ...  # FR-002: the one answerer, called by both drains
    def _write_exit_tallies(self) -> None:
        ...  # FR-003: takes both tallies under _lock and writes at most one line for each
```

---

## API / Interface Contract

No public interface changes. The one observable a caller has is the batches its sink is handed,
and the exit drain's lines:

```python
# p13_burst.py on 3.13.1: 30,000 spans in 0.40 s into a sink costing 20 ms a call
emit calls by batch size (events): {20: 1, 378: 1, 1000: 35}   # today: {20: 1017}

# p04_exit.py 0.02 10000 on 3.13.1: the process exits in budget and close() runs
log-foundry: lost 7828 event(s); 40 exit batch(es) declined unattempted; each was predicted to end
after the shutdown() deadline
                     # today: the join expires, close() never runs, and 19980 are "in flight"
```

The two new lines' exact wording is the build's; each names its events and batches, and the
declined line says the batches were never attempted.

## File & Folder Structure

```
src/log_foundry/
└── worker.py           # _BACKLOG_BATCH_CAP, _take, _complete, _answer_marker, the measurement,
                        # the exit budget and its tallies, both drains, the expiry line's figure
tests/
├── test_backlog_batching.py     # FR-001
├── test_exit_drain_batches.py   # FR-002, FR-003
├── test_batch_bound_roster.py   # FR-004: the derived sink table, RIGS, the HTTP family, NATS
└── test_lifecycle_inventory.py  # the new Worker rows
docs/
├── decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md
├── decisions/public-api/a-frozen-surface-is-keyword-first-and-says-what-it-will-not-grow.md
├── decisions/sink-lifecycle/the-exit-drain-declines-only-a-retry-predicted-to-end-past-the-shutdown-deadline.md
├── architecture.md, README.md
└── spec-delivery/SPEC-090-*.md  # with "Release notes owed", unless a pending file exists
```

## Implementation Phases

### Phase 1: The rule in the steady drain (FR-001)

- `_BACKLOG_BATCH_CAP`, `_take` with the carry, `_complete`, the measurement around
  `_emit_pending` with its zero-elapsed and zero-event cases, the sink generation in `retarget`,
  and the resets in `_reinit_after_fork`, with the new rows in `tests/test_lifecycle_inventory.py`.
- The tests of FR-001, and `p13_burst.py` and `spec090_burst_gated.py` run against the build on
  3.12 and 3.13.

### Phase 2: The exit drain and its budget (FR-002, FR-003)

- `_final_drain` continuing `_drain`'s batch through `_take` and `_complete`, bounded by the
  entries queued when it began, answering each marker through `_answer_marker`, with SPEC-089's
  `unwinding` and snapshot carried into the helper.
- The expiry line's in-flight figure from SPEC-088's worker `in_flight`.
- The exit budget, the two tallies and their writers, in `_final_drain`'s `finally` and in
  `stop()`'s expiry branch.
- The tests of FR-002 and FR-003, and `p04_exit.py`, `spec090_exit_budget.py`,
  `spec090_exit_marker.py` and `spec090_dead_sink_exit_lines.py` run on 3.12 and 3.13. The
  existing exit-budget and expiry tests passed against the whole-interval prototype unchanged
  (Evidence); one the build finds pinning the single exit batch or the first-attempt rule is
  rewritten in place, its old claim struck.

### Phase 3: The sinks (FR-004)

- The derived sink table, the `RIGS` and HTTP-family batches of 1,000, and the JetStream
  `NATSSink` case.

### Phase 4: The record (FR-005)

- The signature checks, the three decision files, §12, the prose table, and the release notes
  owed.

---

## Evidence

Measured on 2026-11-15 at `3e27a0a6` on macOS (Darwin, 10 cores), with CPython 3.12.12 and
3.13.1, while other sessions loaded the machine; the load average is given where it moved a
number. No change to `src/` is committed. Each prototype ran against a scratch copy of `src/`,
through `PYTHONPATH`, with the worktree's `src/` untouched.

**The prototypes.** The **fixed-cap** prototype grew every batch under a backlog to a fixed number
of events. The **whole-interval** prototype is the first draft's rule, with `R × flush_interval`
as its target and the exit drain making every batch's first attempt. The **rule** prototype is
FR-001, FR-002 and FR-003 as written, with the cap at 1,000, except that it does not reset its
fields in a forked child, which no probe here forks, and that it emulates SPEC-088's `in_flight`
with a counter of its own. It is committed as
`docs/audits/2026-11-14-probes/core-pipeline/spec090_prototype.py`, which rewrites a scratch copy
of `worker.py` at `3e27a0a6`; its header says how. An earlier cut of it reset the target after a
declined batch, which split `p04`'s declined events into 389 and 390 batches of 20 where the rule
gives 37 and 40; `p04` and `spec090_exit_budget.py` were re-run on the committed one, and no
other probe here declines a batch, so its other numbers stand for both.

**The probes.** `p13_burst.py` and `p04_exit.py` are the audit's. The rest were written for this
spec and are committed beside them under `docs/audits/2026-11-14-probes/core-pipeline/`, each
runnable against the tree with `PYTHONPATH=src` and each headed by what it shows:
`spec090_burst_gated.py`, `spec090_burst_cost.py`, `spec090_per_event_sink.py`,
`spec090_nats_ack_backlog.py`, `spec090_light_latency.py`, `spec090_queue_memory.py`,
`spec090_exit_marker.py`, `spec090_boundaries.py`, `spec090_exit_budget.py` (the spec review's
probe, adapted to run through `shutdown()` so that `close()` is counted),
`spec090_frozen_clock.py` and `spec090_dead_sink_exit_lines.py`.

**`p13_burst.py`**, 30,000 traced calls into a sink costing 20 ms a call. The burst's own
duration moves with the machine's load, and `dropped` moves with it, so the rows give it.

| Tree | Interpreter | Burst | Emit calls | Events per call | Backlog drained in | `dropped` |
|---|---|---|---|---|---|---|
| today | 3.12.12 | 0.38 to 0.44 s | 1,014 to 1,017 | 20, every call | 25.8 to 27.7 s | 19,830 to 19,860 |
| today | 3.13.1 | 0.40 to 0.65 s | 1,016 to 1,025 | 20, every call | 25.3 to 27.6 s | 19,750 to 19,840 |
| today, loaded | both | 0.92 to 8.47 s | 1,036 to 1,214 | 20, every call | 24.9 to 26.7 s | 17,861 to 19,640 |
| whole interval | 3.12.12 | 0.41 s | 36 | mean 933, largest 1,000 | 0.6 s | 13,201 |
| whole interval | 3.13.1 | 0.40 s | 35 to 36 | mean 952 to 954, largest 1,000 | 0.5 s | 12,860 to 13,297 |
| whole interval, slower burst | both | 0.85 to 2.49 s | 50 to 86 | mean 698 to 947, largest 1,000 | 0.0 to 0.6 s | 0 to 6,318 |
| rule | 3.12.12 | 0.41 s | 37 | `{20: 1, 438: 1, 1000: 35}` | 0.5 s | 12,271 |
| rule | 3.13.1 | 0.40 s | 37 | `{20: 1, 378: 1, 1000: 35}` | 0.5 s | 12,301 |

The whole-interval rows are the half-interval target before the carry and the exit budget were
added; neither changes a `p13` batch, which never crosses the cap and has no exit backlog. A
slower burst leaves less backlog, so its batches are smaller and nothing may be dropped. The
fixed-cap prototype on 3.13.1, bursts of 0.39 to 0.42 s: a cap of 500 dropped 16,489 and 16,490
and drained in 1.1 s; 1,000 dropped 12,990 twice and drained in 0.6 s; 2,000 dropped 5,828 and
5,837 and drained in 0.3 s.

**`spec090_burst_gated.py`**, `p13` with the sink's first emit held by a gate, so the queue fills
whatever the machine's speed. On both interpreters, `dropped=19990` and 10,000 queued at the
release, on both trees. Today the backlog then went in 1,000 calls of 20 events, in 23.23 and
23.64 s. Under the rule it went in 22 calls, largest 1,000 events (`{22: 1, 466: 1, 1000: 19,
512: 1}` and `{22: 1, 476: 1, 1000: 19, 502: 1}`), in 0.56 s.

**`p04_exit.py 0.02 10000`**, interpreter exit with 20,000 events behind a sink costing 20 ms per
ten events, under the default 30 s `shutdown()` budget.

| Tree | Interpreter | Largest batch | Emit calls | Sink received | Ends | Lines |
|---|---|---|---|---|---|---|
| today | 3.12.12 | 19,980 | 2 | 10,540 to 11,580 | join expired, `close()` not run | 19,980 in flight |
| today | 3.13.1 | 19,980 | 2 | 11,520 to 11,800 | join expired, `close()` not run | 19,980 in flight |
| whole interval | 3.12.12 | 398 | 32 | 10,544 | join expired, `close()` not run | 378 in flight, 4,724 submissions queued |
| whole interval | 3.13.1 | 406 | 32 | 10,696 | join expired, `close()` not run | 352 in flight, 4,576 submissions queued |
| rule | 3.12.12 | 218 | 61 | 12,164 | process exited in 30.3 s, `close()` run | 7,836 events in 37 batches declined |
| rule | 3.13.1 | 224 | 61 | 12,172 | process exited in 29.8 s, `close()` run | 7,828 events in 40 batches declined |

Under the rule the received and declined events add to 20,000 exactly in both runs. On the
whole-interval prototype the events received plus two per queued submission fell short of 20,000
by less than the events in flight, 8 to 152, which is the identity FR-002's second criterion
holds for an expiry. One more whole-interval run on 3.12.12 printed nothing to compare: the
probe's child truncates its output file before each write, the process ended between the two,
and the parent found the file empty. It was run again.

**`spec090_exit_budget.py`**, a gated first batch then 30 queued submissions into a sink that
sleeps 0.5 s and raises, `shutdown(timeout=1.2)`.

| Tree and mode | Interpreter | `shutdown()` took | Join | Attempts | `close()` | `failed_batches` |
|---|---|---|---|---|---|---|
| today, `tree` | both | 1.05 to 1.06 s | completed | 2 | once | 1 |
| today, `batched` | both | 1.20 s | expired | 3 | never | 1 |
| rule, `tree` | both | 1.06 s | completed | 2 | once | 3 |

The rule's run wrote SPEC-062's "retry declined after 2 emit attempt(s)" line for 20 events and
one line for the other 40: "2 exit batch(es) declined unattempted".

**`spec090_dead_sink_exit_lines.py`**, 10,000 spans queued behind a sink that raises at once. On
both interpreters and both trees, 2 lines naming all 20,002 events. Today the second line was one
batch of 20,000 abandoned; under the rule it was "20000 event(s); 1000 more exit batch(es)
abandoned", with `failed_batches` 1,001 where today it is 2.

**`spec090_frozen_clock.py`**, `time.monotonic` patched to a constant under a backlog of 2,000
spans. Both trees delivered all 4,020 events with `stopped_reason=None`: today in 201 emits of 20,
under the rule in 5 emits, largest 1,000, the zero-elapsed case giving the cap.

**A JetStream `NATSSink`**, an injected client, the default 10 s `publish_timeout`
(`spec090_nats_ack_backlog.py`).

| Ack, spans | Tree | Acked | `health().sink.failed` | `deliver` calls | Largest | Longest call | Took |
|---|---|---|---|---|---|---|---|
| 20 ms, 1,000 | today | 2,000 | 0 | not traced | not traced | not traced | 43.3 to 49.7 s |
| 20 ms, 1,000 | fixed cap of 1,000 (3.13.1) | 1,026 | 976 | not traced | not traced | its 10 s budget expired | 30.1 s |
| 20 ms, 1,000 | rule | 2,000 | 0 | 119 | 24 | 0.504 to 0.506 s | 42.2 to 42.3 s |
| 2 ms, 200 | today | 400 | 0 | 20 | 20 | 0.050 to 0.052 s | 0.9 to 1.0 s |
| 2 ms, 200 | rule | 400 | 0 | 3 | 192 to 210 | 0.454 to 0.494 s | 0.9 to 1.0 s |

The fixed-cap run also wrote "NATSSink unconfirmed, JetStream publish abandoned" and its
`flush()` returned `reason='sink-flush'`. The today and fixed-cap rows at 20 ms ran before the
probe timed `deliver`.

**A sink costing a fixed time per event** (`spec090_per_event_sink.py`), on 3.12.12 and 3.13.1
alike.

| Cost, spans | Tree | Emit calls | Batches | Longest emit | Drained in |
|---|---|---|---|---|---|
| 20 ms, 1,000 | today | 100 | 20 events, every call | 0.41 s | 40.6 s |
| 20 ms, 1,000 | whole-interval target (3.13.1) | 65 | 50 events 22 times, 2 events 22 times | 1.01 s | 40.5 s |
| 20 ms, 1,000 | half-interval target | 78 | 26 events, 76 times | 0.53 s | 40.4 to 40.5 s |
| 2 ms, 200 | today | 20 | 20 events, every call | 0.05 s | 0.9 s |
| 2 ms, 200 | rule | 3 | 20, then 158 to 160, then 220 to 222 | 0.44 to 0.45 s | 0.8 s |

**No backlog** (`spec090_light_latency.py`), today and under the rule, on both interpreters: one
span every 30 ms for 100 spans was sent in 10 batches of 20 events, with the delay from a span's
close to its emit at a median of 151 to 164 ms and a maximum of 305 to 343 ms on both trees. One
every 1 ms for 2,000 spans into a sink costing 5 ms a call was sent in 200 batches of 20 events,
median 12 to 14 ms, maximum 19 to 29 ms, on today's tree and the half-interval prototype; it is
evidence and not a criterion, because a 1 ms gap is short enough for a loaded machine to queue
two spans behind a count trigger.

**Batch boundaries.** `spec090_exit_marker.py`, on both interpreters: today the 60 events went in
one call with the marker answered after it; under the rule six batches of 10 followed the
measured one, and the marker was answered after the first of them. `spec090_boundaries.py`:
under a backlog, the batch after one the sink failed held 20 events where the failed one held
1,000, and the first batch handed to a sink a `retarget` installed held 20 events, the next 1,000.

**The caller's burst** (`spec090_burst_cost.py`). A drain that keeps pace with a burst takes CPU
from the application's threads under the GIL. Today's tree already pays it whenever its sink is
fast: the same 30,000 calls into an instant sink took 0.61 to 1.24 s on today's tree, against
0.38 to 0.65 s into the 20 ms sink in the quieter `p13` runs above, a sink that drops two thirds of
the burst instead. Under the half-interval prototype a burst into the 20 ms sink took 0.40 to
1.96 s and into the instant sink 0.66 to 1.71 s, against 0.40 to 1.84 s and 0.61 to 1.24 s on
today's tree in runs interleaved with them, five a cell on each interpreter. The machine's load
moved from about 10 to above 300 across this spec's runs, and a burst varied more than fourfold
between neighbouring runs of one tree, so no bound tighter than "the cost a drain keeping pace
already has" can be read from them. The rule adds nothing to `Worker.submit`.

**A full queue's memory** (`spec090_queue_memory.py`, 3.13.1): 10,000 two-event spans queued
behind a gated sink held 15.8 MB, 1,584 bytes each.

**The suite.** `tests/test_worker.py`, `tests/test_worker_exit_budget.py` and
`tests/test_shutdown_expiry_bookkeeping.py` passed against the whole-interval prototype and
against the rule prototype, 221 of 221 on 3.13.1 each time. The whole suite, run against the
half-interval prototype before the carry and the exit budget were added, on 3.13.1 with `-n 8`,
reported 10,592 passed, 35 skipped and 21 xfailed, with none failed; that run's exit code was not
kept, and the build's own run is the one that counts.

---

## Revision history

- **2026-11-16, after the first diff review: two criteria's 200 spans could not fail.** FR-001's
  sixth criterion and FR-004's fourth sent 200 spans, 400 events at 2 ms each, which is 0.8 s of
  sink time in all, so no call could last a second, and a fixed cap with no measured target
  passed both. They now send 1,000 spans, 2,000 events: a batch at the cap would hold the sink
  for 2 s, and the rule's half interval about 0.5 s. The suite runs each on a clock that only the
  sink advances, so the worker measures the cost exactly and the test does not spend the 4 s.
  Measured against the build on 3.12.12 and 3.13.1: `spec090_nats_ack_backlog.py 0.002 1000 10`
  acked 2,000 with `sink.failed=0` in 12 `deliver` calls, largest 188 to 192 events, longest
  0.505 to 0.513 s; `spec090_per_event_sink.py 0.002 1000` received 2,000 in 10 emits, largest
  248, longest 0.50 to 0.51 s.
- **2026-11-16, at completion: the record reached further than FR-005's table.** The build found
  more sites stating a claim FR-001 or FR-003 made false, and rewrote or marked each: in
  `worker.py`, the `max_retries` Args text, `_emit`'s "the first attempt of a batch is never
  declined", `Health.failed_batches`, `EventLedger.batch_lost`, and the `_drain`,
  `_retry_declined` and `_JOIN_MARGIN` docstrings; `tests/test_sinks_http.py`'s present-tense
  claim about the exit backlog; the decisions *A shutdown shortens a wait; it must never skip
  work*, *An expired shutdown is final, and a join the drain thread owes is handed over once* and
  *A sink's wait is bounded, interruptible, and never taken on a destination's word*, which quote
  the exit-drain decision's old title or its one-batch exit; invariant 3's "a shutdown shortens a
  wait; it never skips work"; and in `architecture.md` §12, SPEC-062's closed `NATSSink` item, the
  after-shutdown line item ("the last read" became the bound), and the sentinel-stranding item,
  whose closure by re-reading the queue after the stop FR-002's bound rules out, struck, its
  window recorded as wider by the old pull loop's duration. The README's `failed_batches` and
  `batch_lost` rows now cover a batch declined unattempted. Two premises had moved: FR-002's
  expiry-line figure was already SPEC-088's `in_flight`, so it needed a test and no code; and a
  pending `docs/release-notes/v1.2.1.md` existed, so the release notes went there, as FR-005
  allows. The `NATSSink` residual, filed in §12, was measured directly at a 0.2 s
  `publish_timeout`: 193 of 400 events acked, where the tree before this spec acked all 400.
- **2026-11-16, a paragraph added to FR-001**: a carry emit restarts the time trigger's interval.
  Found by the plan review, which measured `spec090_per_event_sink.py 0.005 1000` on a prototype
  without it: 7 of 29 batches held a single span, and none with it.
- **2026-11-16, one sentence added to FR-001**: each take is counted by `_hold` where it is
  taken, before any carry or emit decision. Found by the second diff review of the fix that lets
  SPEC-088's balance fixture wait for a live drain to settle, whose reading holds only if the
  window from take to `_hold` never spans a sink call.
- **2026-11-15, revised after the spec review**, which found twelve defects, all fixed here.
  ~~The first attempt of every exit batch is made~~: with a slow-failing sink it turned a
  completed join into an expired one with the sink left open, so FR-003 declines a later exit
  batch predicted to overrun, counts it, and aggregates the exit drain's lines. ~~A target of
  `events / elapsed`~~ with no case for zero elapsed: a frozen clock would divide by zero on the
  drain thread; zero gives the cap. ~~The marker criterion on a fresh worker~~: its target is
  zero, so the criterion states one measured batch. ~~The sentinel emits the pending batch, as
  today~~: today it emits nothing and the exit drain continues that batch. ~~At least the
  target~~: a batch could overshoot by a submission; the carry makes the cap a ceiling. ~~The
  sink the target was measured on~~: replaced by a swap generation, which pins no sink. The
  derived rosters, the in-flight figure in the accumulation window, the prose list, the release
  notes owed, the coupling with SPEC-089 and three criteria that could flake are now stated, and
  the probes are committed.
- **2026-11-15, first draft.** Written from Andrew's answer of that day and the §12 items for
  findings 9 and 10. ~~A fixed cap of 1,000 events under every backlog~~: prototyped first, and set
  aside for the measured target after it cost a JetStream `NATSSink` 976 of 2,000 events (FR-004).
  ~~A target of one whole interval of the sink's rate~~: prototyped second, and halved after a sink
  costing 20 ms an event was handed a single span in 22 of 65 batches (FR-001).
