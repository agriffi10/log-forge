---
type: Decision
title: "A backlog drains in larger batches, and capacity is not a setting"
description: "with an item waiting when the count trigger fires, a batch grows by whole submissions up to half a flush interval of the sink's measured rate, never past 1,000 events unless it holds at most `batch_size` submissions; with nothing waiting it is the count trigger's batch, and no argument, setting or environment variable sizes the batch, the cap, the queue or the interval. (SPEC-090)"
---

**Andrew's answer.** Asked on 2026-11-15 "For burst capacity, should the worker send larger
batches under backlog, or expose settings?", Andrew answered "Larger batches, no setting". The
burst loss it answered was finding 10 of the 2026-11-14 audit: a batch was ten submissions
whatever was waiting, so 30,000 spans into a sink costing 20 ms a call lost about two thirds of
them to a full queue and left the rest in calls of 20 events over 25 seconds or more. The answer
keeps *A frozen surface is keyword-first, and says what it will not grow*
(`docs/decisions/public-api/`) standing: the worker's tunables stay unreachable from
`configure()` (invariant 10).

**The rule** (`Worker._take`, `_complete` and `_set_target` in `worker.py`, called by both drains,
so the steady drain and the exit drain decide a batch boundary the same way, invariant 6):

- **Nothing waiting, nothing changes.** A batch of `batch_size` submissions with no item queued is
  emitted, and the time trigger and a flush marker emit whatever is pending, as before.
- **A backlog is an item waiting when the count trigger would fire.** Then the batch grows by
  whole submissions while its events stay within the **size target**. A submission that would
  take a batch of at least `batch_size` submissions past it is carried into the next batch, and a
  carry emit restarts the time trigger's interval. A span's events are never split.
- **The size target** is set from the batch just emitted, alone: `min(1_000, events / elapsed ×
  flush_interval / 2)`; the cap when `elapsed` is zero or negative (a clock that cannot see the
  emit, or one a test froze); zero, which is the count trigger's batch, after a batch counted in
  `failed_batches`, after one that held no events, after one whose emit straddled a
  `retarget`, for a new worker and in a forked child. A batch the exit drain declines, or
  abandons, leaves it as it was: at exit nothing waits for a failing sink to recover, and a reset
  cut a dead sink's backlog into `batch_size` submissions, each paying the sink's own calls and
  lines. The trade is a sink that fails because a batch is too large: at exit it is now handed
  the target every time rather than ten spans after its first failure. It is no regression
  against the release, which handed it the whole backlog in one call: with a sink refusing
  batches over 500 events, the build delivered 20 of 4,020 events at exit, the build that reset
  the target 80, and the tree before SPEC-090 none (the second diff review's probe).
- **The cap is 1,000 events**, the private constant `_BACKLOG_BATCH_CAP`, a ceiling except for a
  batch of at most `batch_size` submissions, which the count trigger sent before this rule too.

**A dead sink at exit keeps today's batches, and its repeated lines are folded** (SPEC-103).
Andrew's answer, asked on 2026-11-17 whether to keep growing a dead sink's exit batches or to keep
them and fold its lines, was "Fold lines, keep batches (Recommended)". Take a sink that died while
the process ran, so the size target reads zero at the stop. It is still handed the exit backlog in
`batch_size` submissions, and a refusing `HTTPSink` with 10,000 spans at exit still makes 16,016
requests. Its stderr no longer follows those calls. From the first abandoned exit batch on, the
lines a sink or the engine writes inside each later exit batch's call are folded into one line per
distinct text, written at the end of the exit (*One module writes every diagnostic…*,
`docs/decisions/failure-paths/`). That probe went from 1,004 lines to 6.

**Why growth at exit was rejected.** Six designs grew a dead sink's exit batches behind a zero
target, and every one broke the `shutdown()` budget for some sink. A grown batch is sized from what
the last batch cost. A sink whose cost changes between batches is handed a batch grown while it was
quick just as it turns slow, and that first slow attempt is longer by the factor the batch grew.
**Any growth makes the first attempt after a slowdown proportionally longer, and no cost bound
survives that.** Each bound added closed the instance before it, and a cost shape it had not
assumed then broke it: linear, then a step, then quadratic, then a slowdown. The last design
(`7f400f51`) grew only after quick failures and fell back to the count trigger's batch after a
slow one. It never reached `main`. The two slowdown probes, run against `7f400f51`, still broke
it:

- `docs/audits/2026-11-14-probes/core-pipeline/spec103_sqs_slowdown_exit.py <slow_at> 0.5` is a
  black-holed `SQSSink` or `SNSSink` that turns slow mid-exit. It ended the drain 1.60, 2.81 and
  6.42 s past a 0.5 s deadline, for `slow_at` 0.004, 0.01 and 0.02 s.
- `spec103_http_slowdown_exit.py 1.0` is a refusing `HTTPSink` whose requests turn into 0.3 s
  timeouts. It returned `ShutdownTimeout` with `close()` never run, and the drain ran on 18.3 to
  21.2 s after `shutdown()` returned.

With the batches kept and the lines folded (`f28f17d4`, on 3.13.1 and 3.12.12 alike), the first
probe's drain ends 0.18 to 0.20 s before the deadline and the second closes the sink once, as on
the tree before.

**Why half an interval of the sink's own rate.** Each batch to a per-call sink multiplies its
target by `flush_interval / (2 × cost per call)`, so one costing well under half an interval
reaches the cap at once, one costing near half an interval takes hundreds of batches, and one
costing more than half an interval never grows past the count trigger's batch
(`spec090_slow_per_call.py`: at 0.3 s a call the batches went 20, 24, 38 and so on to 1,000 in
about ten batches; at 0.4 s they reached 204 events after 14; at 0.45 s, 44 after 14; at 0.6 s
every batch was 20);
a per-event sink (SQS at ten entries a request, a JetStream `NATSSink` at one ack per event) settles
near half an interval's worth, so an emit stays shorter than the time trigger's interval and a
later event, a flush marker, a swap's fence or an exit's join waits behind it no longer than the
interval already allowed. Prototyped at a whole interval, a sink costing 20 ms an event was handed
a single span in 22 of 65 batches; prototyped as a fixed cap with no rate, a JetStream `NATSSink`
behind a 20 ms ack lost 976 of 2,000 events to its own 10 s `publish_timeout`, where under the rule
it loses none (SPEC-090 Evidence).

**Why 1,000.** It is the largest default per-request count of any shipped sink: the HTTP family's
`DEFAULT_MAX_BATCH_COUNT` and the `chunk_size` of `ClickHouseSink` and `PostgresSink`. Past it a
chunking sink makes more requests per call and saves no call, and a sink that fails after running
fast abandons its batch whole, so the cap bounds that loss.

**Rejected:**

- **A setting** — a `configure()` argument, a `Worker` constructor argument or an environment
  variable for the batch size, the cap, the queue or the interval. Andrew's answer, and the
  public-api decision above: the worker is built lazily, so a setting applied before the first
  span would take and one after would silently not.
- **A larger queue.** It stays 10,000 submissions, the library's memory bound: 10,000 queued
  two-event spans held 15.8 MB, so a queue ten times larger would hold more than a 128 MB Lambda
  has. A batch holds references the queue already held, so growing it moves no memory bound.
- **A fixed cap with no measured rate**, and **a target of a whole interval**: each measured
  against a per-event sink, above.
- **A cap of 2,000.** It dropped fewer submissions on `p13`, whose sink costs the same per call at
  any size, which no shipped sink above 1,000 events does.
- **At exit, reading a zero target as the cap** (SPEC-103's A). A sink that recovers by exit was
  handed 1,000 events in its first exit batch, which the budget cannot decline, and its 1.2 s join
  expired with the sink left open.
- **At exit, declining every batch after the first abandoned one** (B). One batch refused for its
  content declined every deliverable batch behind it, under a line blaming the deadline: 68 of
  4,002 events delivered, against 3,002.
- **An abandoned exit batch setting the target from its own rate** (D), and the same only while
  the target reads zero (D2). One quick failure lifted the target past a sink's 400-event limit:
  290 and 0 of 4,002 delivered, against 3,754 and 3,962.
- **Doubling a dead sink's exit batches with no rate bound** (E). A slow-failing sink's batches
  grew to 320 events, longer than any attempt the exit budget had seen.
- **Doubling, bounded by the rate** (E′). With `max_retries=0` the join expired in 4 to 5 runs of
  10 at a 1.28 to 1.32 s budget, where the tree before expired none.
- **E′ bounded by the longest exit attempt** (E3), and its predictor fix at `1fa9ea15`. A step cost,
  a black-holed `SQSSink` at 0.3 s a request, grew from 10 messages to 13, taking 0.6 s against a
  predicted 0.4 s. Its join expired with `close()` never run in 3 of 3 runs, and a quadratic cost
  broke it too.
- **Growth only after quick failures, back to the count trigger's batch after a slow one** (E6, at
  `7f400f51`). A sink that turned slow mid-exit broke it, by the slowdown probes above.

**Do NOT build** a public tunable for any of the four, or split a submission across batches, or
grow a dead sink's exit batches behind a zero target. The
exit drain's budget for the batches this sends at exit is *The exit drain declines a retry, or a
later batch, predicted to end past the shutdown deadline* (`docs/decisions/sink-lifecycle/`).

## History

### 2026-11-17

- **Refined** by SPEC-103: a dead sink at exit keeps today's batches, and its repeated lines are
  folded, by Andrew's answer "Fold lines, keep batches (Recommended)". `spec090_dead_http_exit.py
  zero 10000 3` wrote 1,004 lines at `4599a0c5` and 6 at `f28f17d4`, with its 16,016 opener calls
  and `failed_batches` 1,001 unchanged. The growth designs A, B, D, D2, E, E′, E3 and E6 are
  rejected above.
- **Withdrawn**: a trade the growth design needed, which never reached `main`. For that design,
  Andrew accepted on 2026-11-17 that a sink refusing batches over 400 events, failing five quick
  exit batches in a row and then recovering, would be handed batches past its limit and deliver 0
  of 4,002 events (`spec103_size_limited_exit.py 2 400 1-5 2000 zero`, against 3,882 on the tree
  before it). It was withdrawn the same day, because nothing grows now.

### 2026-11-16

- **Refined** by SPEC-090's second diff review: an abandoned exit batch leaves the size target as
  it was, as a declined one does. A refusing `HTTPSink` with 10,000 spans at exit made 15,217
  requests and wrote 953 lines with the reset, 321 and 22 without it, against 321 and 21 before
  SPEC-090. "A per-call sink reaches the cap at once" was narrowed to the cost at which it does.
- **Built** by SPEC-090: `_take`, `_complete`, `_set_target`, the sink generation `retarget`
  bumps, and the exit drain sending its backlog by the same rule. Measured on the build
  (`be686953`), `p13_burst.py` drained its backlog in 0.5 s on 3.13.1 and 0.6 s on 3.12.12, no
  batch above 1,000 events, against 27.1 s and 25.8 s in calls of 20 on the tree before it
  (`75e7bf1e`); the delivery doc has the runs.

### 2026-11-15

- **Settled** by Andrew's answer, "Larger batches, no setting".
