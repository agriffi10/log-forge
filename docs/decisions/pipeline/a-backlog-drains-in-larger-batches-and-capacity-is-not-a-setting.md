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
  `retarget`, for a new worker and in a forked child. A batch the exit drain declines leaves it as
  it was.
- **The cap is 1,000 events**, the private constant `_BACKLOG_BATCH_CAP`, a ceiling except for a
  batch of at most `batch_size` submissions, which the count trigger sent before this rule too.

**Why half an interval of the sink's own rate.** A per-call sink reaches the cap at once; a
per-event sink (SQS at ten entries a request, a JetStream `NATSSink` at one ack per event) settles
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

**Do NOT build** a public tunable for any of the four, or split a submission across batches. The
exit drain's budget for the batches this sends at exit is *The exit drain declines a retry, or a
later batch, predicted to end past the shutdown deadline* (`docs/decisions/sink-lifecycle/`).

## History

### 2026-11-16

- **Built** by SPEC-090: `_take`, `_complete`, `_set_target`, the sink generation `retarget`
  bumps, and the exit drain sending its backlog by the same rule. Measured on the build
  (`be686953`), `p13_burst.py` drained its backlog in 0.5 s on 3.13.1 and 0.6 s on 3.12.12, no
  batch above 1,000 events, against 27.1 s and 25.8 s in calls of 20 on the tree before it
  (`75e7bf1e`); the delivery doc has the runs.

### 2026-11-15

- **Settled** by Andrew's answer, "Larger batches, no setting".
