---
type: Spec
id: SPEC-090
title: "Under a Backlog the Worker Sends Larger Batches, and the Exit Drain Sends Its Backlog in Batches"
description: "Lets the worker drain a backlog in batches sized to what its sink delivers in half a flush interval, capped at 1,000 events and with no new setting, and has the exit drain send its backlog by the same rule, so a burst loses fewer events and an expired shutdown leaves at most one batch whose fate is unknown."
spec_status: Draft
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
nobody will learn (the capacity half of finding 9). Andrew decided on 2026-11-15 that capacity is not a
setting: asked "For burst capacity, should the worker send larger batches under backlog, or expose
settings?", he answered "Larger batches, no setting". This spec has the worker grow a batch while
a backlog is waiting, up to what its sink delivers in half a flush interval and never past 1,000
events, and has the exit drain send its backlog by the same rule. Nothing changes when nothing is
waiting, and no argument is added anywhere.

## Scope

### In Scope

- One batching rule: a batch grows past ten submissions only while more are waiting, up to a size
  target measured from the sink's own rate and capped at 1,000 events; with nothing waiting the
  worker batches exactly as it does today (FR-001).
- The exit drain sends its backlog by the same rule, in batches, answering each flush marker in
  queue order, so an expiry leaves one batch in flight (FR-002).
- Every shipped sink is shown to take a capped batch whole, and a sink whose cost is per event is
  shown to keep its emits shorter than the flush interval (FR-003).
- No setting, and the record: the decision Andrew's answer keeps standing, the §12 items it
  closes, the README, the docstrings and the release notes (FR-004).

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
- **`FlushResult`'s loss counts** (SPEC-089). A flush marker is answered by the same code with the
  same verdict; only its place in the exit drain changes (FR-002).
- **Acknowledgement fixes in sinks**, and any change to a sink's code. FR-003 changes no sink; the
  one sink-side residual it measures is filed in `docs/architecture.md` §12, not fixed here.
- **Declining the first attempt of a later exit batch.** SPEC-062's rule that a batch's first
  attempt is always made, and only its retries are declined past the `shutdown()` deadline, holds
  for every exit batch. So a `shutdown()` whose join expires in a process that goes on running
  still delivers the rest of what its exit drain took, as today.
- **Splitting a submission.** A span's events stay in one batch, so a single submission of more
  than 1,000 events, and ten submissions whose events add up past it, are sent as today's rule
  sends them.
- **The orphan path**, which sends one event per call and has no batch.
- **A loss no process survives to count.** At interpreter exit the batch in flight and the
  submissions still queued when the join expires are lost with the process, as SPEC-088's Out of
  Scope records; this spec makes the first of them at most one batch.

---

## Functional Requirements

### FR-001: A batch grows while a backlog is waiting, up to half an interval of the sink's own rate and never past 1,000 events

#### Description:

**Today.** `Worker.__init__` sets `batch_size=10`, `flush_interval=1.0` and `max_queue=10_000`
(`worker.py:361-363`), and `_lifecycle.py:3796` builds the process worker with those defaults.
`_drain` takes one queue item per `get` (`worker.py:1723`) and emits the pending batch when it
holds `batch_size` submissions or `flush_interval` has passed since the last emit
(`worker.py:1741`). A submission is one finished span's events, so under a backlog every batch is
ten spans, whatever the sink could take. `p13_burst.py` at `3e27a0a6` (Evidence): 1,014 to 1,214
emit calls of exactly 20 events each, a backlog that took 24.9 to 27.7 s to drain after the burst,
and 17,861 to 19,860 of 30,000 submissions dropped, the most when the burst was fastest.

**The rule.** One method decides when the pending batch is complete, and both drains call it
(FR-002).

1. **With nothing waiting, nothing changes.** When the batch holds `batch_size` submissions and
   the queue holds no item, it is emitted, as today. The time trigger, a flush marker and the
   shutdown sentinel emit the pending batch whatever its size, as today.
2. **A backlog is an item waiting in the queue when the count trigger would fire.** Then the drain
   keeps taking whole submissions into the same batch until it holds at least the **size target**
   in events, and emits it then; the time trigger, a marker or the sentinel still end it sooner.
   The check is the queue's own `empty()`, which takes the mutex `get` already takes.
3. **The size target** is `min(1_000, R × flush_interval / 2)`, where `R` is the events per second
   of the last batch this worker emitted **to the sink it holds now** and did not count in
   `failed_batches`, timed around the whole `_emit` call, so its retries and the engine's waits
   lower `R`. Where no such batch exists the target is zero, which is today's batch: a new worker,
   the first batch after a `retarget` installs a sink, a forked child's first batch, and the batch
   after one that was abandoned or declined.
4. **The cap is 1,000 events**, a private module constant of `worker.py` that is read at each use,
   as `_JOIN_MARGIN` is, so a test can patch it. It is not a `Worker` argument.

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
rate measured the other way: a JetStream `NATSSink` behind a 20 ms ack lost 976 of 2,000 events
to its own 10 s `publish_timeout`, where today it loses none (FR-003, Evidence).

**Why the cap is 1,000.** It is the largest default per-request count of any shipped sink: the
HTTP family's `DEFAULT_MAX_BATCH_COUNT` (`sinks/http.py:52`) and the 1,000 its subclasses set,
and the `chunk_size` of `ClickHouseSink` and `PostgresSink` (`sinks/clickhouse.py:87`,
`sinks/postgres.py:135`). Past it, a chunking sink makes more requests per call and saves no
call. What a larger cap would add is loss granularity: a sink that fails after running fast
abandons its batch whole, and an expired exit leaves the batch in flight unknown, so the cap
bounds both at 1,000 events. A fixed cap of 2,000 on `p13` dropped about 5,830 submissions against
about 13,000 at 1,000 (Evidence), because `p13`'s sink costs the same per call however large the
batch, which is true of no shipped sink above 1,000 events.

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

**What it does not touch.** The orphan path, which sends one event per call. A flush marker's
place in the FIFO and its verdict. The time trigger's interval. A submission is never split, so a
batch above 1,000 events is still possible where today's rule makes one: ten spans of 200 events,
or one span of 5,000.

**A later spec drains open spans on the worker's interval** (Andrew, 2026-11-15, "ship on the
worker's ~1 s interval"). The rule is stated over submissions and events, never over spans, and
reads no span state, so a submission holding part of an open span's buffer is batched like any
other. It keeps the time trigger as the one interval, so a worker that also sweeps open spans on
that interval has one clock to follow, and the size target is measured from emits, which such a
sweep feeds like any other submission.

**What SPEC-088's ledger shows.** Under a burst `overflow_lost` falls (`p13`: from about 39,700
events to about 26,000 at the core frame's burst speed). A batch abandoned while the sink was fast
moves `batch_lost` by up to 1,000 at once, where today it moves by ten spans' events; the next
batch is then today's size, by rule 3. `in_flight` holds the batch being emitted, at most one
batch, plus what the drain has taken into the next.

#### Acceptance Criteria:

- [ ] `p13_burst.py` on 3.12 and 3.13 sends no batch above 1,000 events and drains its backlog in
      under 2 s after the burst, where today it takes 24.9 to 27.7 s (invariant 2).
- [ ] In a `p13_burst.py` run whose burst takes 0.35 to 0.5 s, the emit calls carry a mean of at
      least 900 events, where today every call carries 20, and `dropped` is at most 15,000
      submissions, where today it is 19,790 to 19,860 (invariant 2).
- [ ] `Worker.submit` is unchanged, and a test that records the thread each call of `_complete`
      and of the rate measurement runs on finds only the drain thread (invariant 4).
- [ ] One span every 30 ms for 100 spans is sent in ten batches of 20 events, as today, and a
      batch whose tenth submission leaves nothing queued is emitted at ten submissions whatever
      the size target (invariant 2).
- [ ] A sink costing 20 ms per event, sent 1,000 spans at once, receives all 2,000 events and is
      never held in one emit for longer than 1 s, where the target with a whole interval held it
      for 1.01 s (invariant 3).
- [ ] After a batch that is abandoned, after `configure(sink=...)` installs a sink, and in a forked
      child, the next batch under a backlog holds exactly `batch_size` submissions
      (invariants 2, 6 and 12).
- [ ] A `flush()` made behind a backlog is answered once the batches carrying the events ahead of
      its marker are emitted, and no batch holds events from both sides of a marker
      (invariants 2 and 3).
- [ ] With the cap patched to 50, a backlog of two-event spans into an instant sink is sent, after
      its first batch, in batches of exactly 50 events (invariant 2).

### FR-002: The exit drain sends its backlog in batches by the same rule

#### Description:

**Today.** `_final_drain` takes everything queued into one list (`worker.py:1804-1815`) and emits
it in one call (`worker.py:1817`), answering every flush marker it met only after that call.
`p04_exit.py 0.02 10000` at `3e27a0a6` (Evidence): the sink was handed 19,980 events in one call,
had taken 10,540 to 11,800 of the 20,000 when the process ended 30.3 to 33.1 s later, `close()`
never ran, and the expiry line named 19,980 events in flight, more than 10,500 of which the sink
had already taken.

**The rule.** The exit drain takes items without waiting and passes each submission through
FR-001's method, which emits a batch whenever it is complete; with no waiting, "the queue holds an
item" is true until the last one, so the exit drain sends full batches and then the remainder.
A flush marker emits the pending batch and is answered there, with the verdict `_drain` gives it
(`worker.py:1726-1736`), so a marker no longer waits for batches behind it. The exit drain takes
at most the entries the queue held when it began, so a caller that keeps logging after
`shutdown()` cannot keep it running; what arrives later stays queued, in `stranded` and
`submitted_after_shutdown`, as it does today when it arrives after the one sweep. SPEC-062's rule
holds per batch: each batch's first attempt is made and its retries are declined when predicted to
end past the deadline.

**What an expiry at exit now costs.** The drain thread is inside one batch, at most the size
target or today's batch, and the rest is still queued. The line names both: the batch in
`in_flight`, by SPEC-088's wording, and the queued submissions. Of what the line names, only the
batch in flight has an unknown fate; today that was the whole backlog. At interpreter exit both
are lost with the process, which no counter survives. The events delivered by the deadline do not
change: `p04`'s sink costs 20 ms per ten events, so the batch size does not change its rate.

**Twin paths.** `_drain` and `_final_drain` are invariant 6's named twins. Both call the one
method of FR-001, so a batch boundary cannot be decided one way on one path and another way on the
other; and both answer a marker the same way.

#### Acceptance Criteria:

- [ ] `p04_exit.py 0.02 10000` on 3.12 and 3.13 hands the sink no batch above 1,000 events, where
      today it hands it 19,980 in one call (invariant 2).
- [ ] In the same run the expiry line names at most 1,000 events in flight, and the events the sink
      received plus two per queued submission the line names is at most 20,000 and at least
      20,000 less the events in flight (invariants 2 and 11).
- [ ] In the same run the sink receives at least 9,500 of the 20,000 events, where today it
      receives 10,540 to 11,800 (invariant 3).
- [ ] A worker built with `batch_size=1`, its cap patched to 10 and its sink fast enough for the
      cap, with 30 two-event submissions queued at exit and a flush marker after the fifth, sends
      six batches of 10 events and answers the marker truthy after the first and before the
      second; today it sends the 60 events in one call and answers the marker after it
      (invariants 2 and 6).
- [ ] A thread that keeps submitting after `shutdown()` begins does not keep the exit drain
      running: the drain takes no entry queued after it began, and each such submission is counted
      in `submitted_after_shutdown` (invariants 2 and 3).
- [ ] An exit drain against a sink that always raises makes each batch's first attempt, declines
      its retries past the deadline, and moves `failed_batches` once per batch (invariants 2
      and 3).
- [ ] A `BaseException` raised from the second exit batch leaves the markers answered before it
      answered, and every later marker is released by the drain thread's exit within its
      `flush()` timeout (invariants 1 and 3).
- [ ] For one queue and one measured target, the exit drain's batch boundaries are the steady
      drain's, through the one method (invariant 6).

### FR-003: Every shipped sink takes a capped batch whole, and a per-event sink keeps its emits short

#### Description:

**No sink's largest batch grows.** Today's count trigger counts submissions of any size, and the
exit drain hands over the whole queue, 19,980 events in `p04`; SPEC-038 measured 5,980 for one
exit backlog (`sinks/nats.py:37`). So every shipped sink already receives batches far above
1,000 events, and this spec makes such batches smaller at exit and frequent, but no larger, under a
backlog. What changes for a sink is how often it sees a large batch, and how long that call takes.

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
the cap never exceeds it alone. FR-001's size target keeps a per-event sink's call near half
an interval, a twentieth of `NATSSink`'s default budget. Measured (Evidence): a JetStream
`NATSSink` behind a 20 ms ack, sent 1,000 spans, lost none today, lost 976 events with a fixed
1,000-event cap, and loses none under the rule.

**The residual is filed, not fixed.** A `NATSSink` whose `publish_timeout` is shorter than half
the flush interval, or whose ack time grows more than twentyfold between one batch and the
next, can still be handed a batch it cannot finish within its budget, and loses that batch's
tail, counted and announced. The completion ritual files this in `docs/architecture.md` §12 with its probe and
its closure, which is `NATSSink` answering an event the deadline left unattempted as `RETRYABLE`,
since it never reached the driver and a resend cannot duplicate it. That is a change to a sink's
verdict, which this spec's Out of Scope leaves to the sinks track.

#### Acceptance Criteria:

- [ ] Every sink in `tests/sink_rigs.py`'s `RIGS`, handed one batch of 1,000 events, delivers all
      1,000 through its double and moves none of its loss counters (invariants 2 and 7).
- [ ] `HTTPSink` and each shipped subclass, handed 1,000 events through an opener double, sends
      the number of requests its count and byte limits predict and loses none (invariant 7).
- [ ] A test derives every public sink class under `src/log_foundry/sinks/` from the tree and fails
      on one missing from a table that names how it takes a batch, as the table above does
      (invariant 2).
- [ ] The JetStream `NATSSink` of the Evidence, behind a 20 ms ack with the default
      `publish_timeout`, sent 1,000 spans through the worker, acks all 2,000 events with
      `health().sink.failed == 0`, and no emit lasts longer than 1 s (invariants 2 and 3).
- [ ] `docs/architecture.md` §12 carries the `NATSSink` residual with its probe, its measured
      numbers and its closure, and names this spec (invariant 2).

### FR-004: No setting is added, and the record says why

#### Description:

Andrew's answer keeps the public-api decision *A frozen surface is keyword-first, and says what it
will not grow* standing: the worker's tunables stay unreachable from `configure()`, which invariant
10 states. This spec adds no public name, so a typed consumer written against today's surface sees
no change. The completion ritual writes the record:

- **A new decision**, `docs/decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md`,
  quoting the question and the answer, with the rule of FR-001 as its fence and its History dated
  2026-11-15 for the decision and the build's date for the mechanism. The public-api decision's
  History gains a dated entry saying the answer kept it, and pointing to the new file.
- **`docs/architecture.md` §12.** The item for finding 10 ("The queue holds 10,000 submissions and
  a batch is 10 submissions…") is struck as closed by this spec, with `p13`'s numbers before and
  after. The item for finding 9, as SPEC-088 leaves it, narrowed to its capacity half, is struck as
  closed, with `p04`'s. FR-003's residual is filed.
- **The prose that says the exit drain sends one batch**, rewritten in place: `README.md:236`
  ("sweeps anything still queued into one final batch") and the "By count" bullet above it, the
  backlog example at `README.md:743-744`, a superseded marker on the blockquote at
  `README.md:962-967`, and the docstrings at `worker.py:1787` (`_final_drain`), `worker.py:407`
  (`batch_size`), `sinks/nats.py:37`, `:307` and `:1601`. The sentences in `sinks/http.py` that
  cite SPEC-038's measured exit backlog are history and stay.
- **The release notes.** A sink sees batches of up to 1,000 events under a backlog where it saw
  ten spans, and several batches at exit where it saw one; a flush marker met at exit is answered
  in queue order; the expiry line's in-flight count is one batch.

#### Acceptance Criteria:

- [ ] `inspect.signature` of `configure`, `Worker.__init__`, `flush`, `shutdown` and `health` is
      unchanged, `log_foundry.__all__` is unchanged, and `tests/typed_consumer/` passes unchanged
      (invariant 10).
- [ ] The new decision file exists in `docs/decisions/pipeline/`, quotes the question and the answer
      word for word, and the public-api decision's History carries the dated entry that points to
      it (invariant 10).
- [ ] The §12 items for findings 10 and 9 are struck as closed by this spec, each with its probe's
      numbers before and after (invariant 2).
- [ ] `grep -rn 'one final batch\|as one batch\|as a single batch' src/ README.md` finds no
      sentence stating today's exit drain as current behaviour (invariant 2).
- [ ] The pending release notes for the next tag under `docs/release-notes/`, which the completion
      ritual creates if no earlier spec has, carry one entry naming the larger batches under a
      backlog, the batched exit drain, the marker answered in queue order and the one-batch
      expiry count (invariant 10).

---

## Data Model

```python
# src/log_foundry/worker.py, private

_BACKLOG_BATCH_CAP: int = 1_000
"""Events. Read at each use so a test can patch it, as _JOIN_MARGIN is."""


class Worker:
    ...  # constructor unchanged: batch_size=10, flush_interval=1.0, max_queue=10_000
    _pending_events: int  # events in the batch being built; drain thread only
    _target: float  # the size target in events; 0 means today's batch; drain thread only
    _target_sink: Sink | None  # the sink _target was measured on; the target applies only to it
    _FORK_SKIP = ("_held", "_taken_markers", "_queue", "_thread", "_target_sink")
    # _target_sink can pin a sink a swap replaced, the shape _FORK_SKIP's docstring gives for _held

    def _complete(self, pending: list[list[dict[str, object]]], *, waiting: bool) -> bool: ...
    # FR-001's one method: True when the batch holds batch_size submissions and either nothing
    # is waiting or it holds at least the size target in events. Both drains call it.
```

---

## API / Interface Contract

No public interface changes. The one observable a caller has is the batches its sink is handed:

```python
# p13_burst.py on 3.13.1: 30,000 spans in 0.40 s into a sink costing 20 ms a call
emit calls by batch size (events): {20: 1, 334: 1, 1000: 33, 926: 1}   # today: {20: 1017}

# p04_exit.py 0.02 10000 on 3.13.1
sink received 11442 of 20000 events in 62 emit call(s), largest batch 218 events
                                                    # today: 2 emit call(s), largest batch 19980
```

## File & Folder Structure

```
src/log_foundry/
└── worker.py           # _BACKLOG_BATCH_CAP, _complete, the measurement, both drains
tests/
├── test_backlog_batching.py     # FR-001
├── test_exit_drain_batches.py   # FR-002
└── test_batch_bound_roster.py   # FR-003: the derived sink table, RIGS, the HTTP family, NATS
docs/
├── decisions/pipeline/a-backlog-drains-in-larger-batches-and-capacity-is-not-a-setting.md
├── decisions/public-api/a-frozen-surface-is-keyword-first-and-says-what-it-will-not-grow.md
├── architecture.md, README.md
└── release-notes/      # the pending notes
```

## Implementation Phases

### Phase 1: The rule in the steady drain (FR-001)

- `_BACKLOG_BATCH_CAP`, `_complete`, the measurement around `_emit_pending`, the target that
  applies only to the sink it was measured on, and its reset in `_reinit_after_fork`.
  `_target_sink` joins `_FORK_SKIP`: after a swap it can hold the sink the swap replaced until the
  next batch is measured, which is the shape that tuple's docstring gives `_held`, and the fork
  repair walk must not reach that sink through it.
- The tests of FR-001, and `p13_burst.py` run against the build on 3.12 and 3.13.

### Phase 2: The exit drain (FR-002)

- `_final_drain` through `_complete`, bounded by the entries queued when it began, answering each
  marker where it meets it.
- The tests of FR-002, and `p04_exit.py` run on 3.12 and 3.13. The existing exit-budget and
  expiry tests passed against the prototype unchanged (Evidence); one the build finds pinning the
  single exit batch is rewritten in place, its old claim struck.

### Phase 3: The sinks (FR-003)

- The derived sink table, the `RIGS` and HTTP-family batches of 1,000, and the JetStream
  `NATSSink` case.

### Phase 4: The record (FR-004)

- The signature checks, the decision file and the public-api History entry, §12, the README and
  docstrings, and the release notes.

---

## Evidence

Measured on 2026-11-15 at `3e27a0a6` on macOS (Darwin, 10 cores), with CPython 3.12.12 and
3.13.1, while other sessions loaded the machine; the load average is given where it moved a
number. No prototype is committed. Each ran against a scratch copy of `src/`, through
`PYTHONPATH`, with the worktree's `src/` untouched. The **fixed-cap** prototype grew every batch
under a backlog to a fixed number of events. The **rule** prototype is FR-001 and FR-002 as
written, with the cap at 1,000, except that it did not reset the target in a forked child, which
no probe here forks. The **whole-interval** prototype is the rule with `R × flush_interval` as its
target, before it was halved.

`p13_burst.py` and `p04_exit.py` are the audit's (`docs/audits/2026-11-14-probes/core-pipeline/`).
The others were written for this spec and are not committed, so each is described where it is
used: `nats_ack_backlog.py` drives a real `NATSSink` through an injected client whose JetStream
`publish` sleeps for the ack time; `per_event_sink.py` sends spans at once into a sink that sleeps
for the per-event cost times the batch; `light_latency.py` closes one span per gap into a sink
that records each `span.end`'s delay to its emit; `burst_cost.py` is `p13` with the sink's
per-call cost as an argument; `queue_memory.py` reads `tracemalloc` across 10,000 spans queued
behind a gated sink; and `exit_marker.py` and `structural.py` build a `Worker` directly over a
recording sink.

**`p13_burst.py`**, 30,000 traced calls into a sink costing 20 ms a call. The burst's own
duration moves with the machine's load, and `dropped` moves with it, so the rows give it.

| Tree | Interpreter | Burst | Emit calls | Events per call | Backlog drained in | `dropped` |
|---|---|---|---|---|---|---|
| today | 3.12.12 | 0.38 to 0.44 s | 1,014 to 1,017 | 20, every call | 25.8 to 27.7 s | 19,830 to 19,860 |
| today | 3.13.1 | 0.40 to 0.65 s | 1,016 to 1,025 | 20, every call | 25.3 to 27.6 s | 19,750 to 19,840 |
| today, loaded | both | 0.92 to 8.47 s | 1,036 to 1,214 | 20, every call | 24.9 to 26.7 s | 17,861 to 19,640 |
| rule | 3.12.12 | 0.41 s | 36 | mean 933, largest 1,000 | 0.6 s | 13,201 |
| rule | 3.13.1 | 0.40 s | 35 to 36 | mean 952 to 954, largest 1,000 | 0.5 s | 12,860 to 13,297 |
| rule, slower burst | both | 0.85 to 2.49 s | 50 to 86 | mean 698 to 947, largest 1,000 | 0.0 to 0.6 s | 0 to 6,318 |

A slower burst leaves less backlog, so its batches are smaller and nothing may be dropped. The
fixed-cap prototype on 3.13.1, bursts of 0.39 to 0.42 s: a cap of 500 dropped 16,489 and 16,490
and drained in 1.1 s; 1,000 dropped 12,990 twice and drained in 0.6 s; 2,000 dropped 5,828 and
5,837 and drained in 0.3 s.

**`p04_exit.py 0.02 10000`**, interpreter exit with 20,000 events behind a sink costing 20 ms per
ten events, under the default 30 s `shutdown()` budget.

| Tree | Interpreter | Largest batch | Emit calls | Sink received | Expiry line |
|---|---|---|---|---|---|
| today | 3.12.12 | 19,980 | 2 | 10,540 to 11,580 | 19,980 in flight |
| today | 3.13.1 | 19,980 | 2 | 11,520 to 11,800 | 19,980 in flight |
| rule, whole interval | 3.12.12 | 398 | 32 | 10,544 | 378 in flight, 4,724 submissions queued |
| rule, whole interval | 3.13.1 | 406 | 32 | 10,696 | 352 in flight, 4,576 submissions queued |
| rule | 3.12.12 | 222 | 62 | 12,384 | 206 in flight, 3,765 submissions queued |
| rule | 3.13.1 | 218 | 62 | 11,442 | 198 in flight, 4,215 submissions queued |

`close()` ran in no run. In every rule run the events received plus two per queued submission
fell short of 20,000 by less than the events in flight: 8 to 152. One more rule run on 3.12.12
printed nothing to compare: the probe's child truncates its output file before each write, the
process ended between the two, and the parent found the file empty. It was run again, and that is
the row above.

**A JetStream `NATSSink` behind a 20 ms ack**, an injected client, the default 10 s
`publish_timeout`, 1,000 spans sent at once (`nats_ack_backlog.py 0.02 1000 10`).

| Tree | Acked | `health().sink.failed` | Batches | Longest emit | Took |
|---|---|---|---|---|---|
| today | 2,000 | 0 | not traced | not traced | 43.3 to 49.7 s |
| fixed cap of 1,000 (3.13.1) | 1,026 | 976 | not traced | its 10 s budget expired | 30.1 s |
| rule | 2,000 | 0 | 24 events in 78 of 84 (3.13.1) | 0.557 s (3.12.12), 0.585 s (3.13.1) | 43.4 to 43.5 s |

The fixed-cap run also wrote "NATSSink unconfirmed, JetStream publish abandoned" and its
`flush()` returned `reason='sink-flush'`.

**A sink costing 20 ms per event**, 1,000 spans sent at once (`per_event_sink.py 0.02 1000`), on
3.12.12 and 3.13.1 alike.

| Tree | Emit calls | Batches | Longest emit | Drained in |
|---|---|---|---|---|
| today | 100 | 20 events, every call | 0.41 s | 40.6 s |
| rule, whole interval (3.13.1) | 65 | 50 events 22 times, 2 events 22 times | 1.01 s | 40.5 s |
| rule | 78 | 26 events, 76 times | 0.53 s | 40.4 to 40.5 s |

**No backlog** (`light_latency.py`), today and under the rule, on both interpreters: one span
every 30 ms for 100 spans was sent in 10 batches of 20 events, with the delay from a span's close
to its emit at a median of 152 to 164 ms and a maximum of 320 to 343 ms on both trees; one every
1 ms for 2,000 spans into a sink costing 5 ms a call was sent in 200 batches of 20 events, median
12 to 14 ms, maximum 19 to 29 ms on both.

**The caller's burst.** A drain that keeps pace with a burst takes CPU from the application's
threads under the GIL. Today's tree already pays it whenever its sink is fast: the same 30,000
calls into an instant sink took 0.61 to 1.24 s on today's tree, against 0.38 to 0.65 s into the
20 ms sink in the quieter `p13` runs above, a sink that drops two thirds of the burst instead.
Under the rule a burst into the 20 ms sink took 0.40 to 1.96 s and into the instant sink 0.66 to
1.71 s, against 0.40 to 1.84 s and 0.61 to 1.24 s on today's tree in runs interleaved with them,
five a cell on each interpreter (`burst_cost.py`). The machine's load moved from about 10 to above
300 across this spec's runs, and a burst varied more than fourfold between neighbouring runs of
one tree, so no bound tighter than "the cost a drain keeping pace already has" can be
read from them. The rule adds nothing to `Worker.submit`.

**Batch boundaries** (`exit_marker.py` and `structural.py`, 3.13.1). A worker with
`batch_size=1`, the cap set to 10 and a 60 s interval, holding 30 two-event submissions with a
flush marker after the fifth at exit: under the rule it sent batches of 10 events six times and
answered the marker truthy after the first of them; today it sent the 60 events in one call and
answered the marker after it. Under a backlog, the batch after one the sink failed held 20 events
where the failed one held 1,000, and the first batch handed to a sink a `retarget` installed held
20 events, the next 1,000.

**A full queue's memory** (`queue_memory.py`, 3.13.1): 10,000 two-event spans queued behind a
gated sink held 15.8 MB, 1,584 bytes each.

**The suite.** `tests/test_worker.py`, `tests/test_worker_exit_budget.py` and
`tests/test_shutdown_expiry_bookkeeping.py` passed against the whole-interval prototype, 221 of
221 on 3.13.1. The whole suite, run against the rule prototype on 3.13.1 with `-n 8`, reported
10,592 passed, 35 skipped and 21 xfailed, with none failed, so no existing test pins the batch
size under a backlog or the single exit batch. The build's own run is the one that counts: the
prototype is not the build, and that run's exit code was not kept.

---

## Revision history

- **2026-11-15, first draft.** Written from Andrew's answer of that day and the §12 items for
  findings 9 and 10. ~~A fixed cap of 1,000 events under every backlog~~: prototyped first, and set
  aside for the measured target after it cost a JetStream `NATSSink` 976 of 2,000 events (FR-003).
  ~~A target of one whole interval of the sink's rate~~: prototyped second, and halved after a sink
  costing 20 ms an event was handed a single span in 22 of 65 batches (FR-001).
