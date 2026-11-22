---
type: Decision
title: "A client-buffered sink's round waits for its delivery reports, and a truthy `flush()` means every event was acknowledged or counted"
description: "`KafkaSink` and `GooglePubSubSink` are delivery-engine sinks whose round hands every event to the client and then waits, within the sink's own timeout and until the stop signal once a 50 ms first slice has passed, for the reports on that round's messages; a success is `ACCEPTED`, a definitive refusal `REJECTED`, anything else `UNKNOWN`, counted and never resent; nothing is counted after the verdict. A truthy `flush()` means every event was acknowledged or counted. (SPEC-094)"
---

**Andrew's two answers, in the order they were given.**

- **2026-11-15, decision B.** Asked whether a client-buffered sink's verdict may come before the
  client's report, he answered **"Wait for the report (Recommended)"**: each batch's `emit` waits for
  the client's delivery reports within the sink's existing timeout, as core NATS confirms.
- **2026-11-16, what a truthy `flush()` then means.** The question set decision B's gloss, ~~"a truthy
  `flush()` means the broker acknowledged"~~ (superseded by the answer below), beside the earlier decision on `flush()`'s truthiness,
  "Keep truthy, add counts", which was recorded first, at `3e27a0a6` (SPEC-088) and `84386551`
  (SPEC-089), before decision B was recorded at `33582484` (SPEC-092). He answered **"Truthy, counted
  (Recommended)"**: an indefinite report leaves `flush()` truthy, its events counted `UNKNOWN` in
  `failed` with a line and never resent, and a definite rejection still makes `flush()` falsy.

So decision B reads: **a truthy `flush()` means every event was acknowledged or counted.** That
reconciliation rests on the 2026-11-16 answer, not on which decision came later. The old gloss is
struck where SPEC-092 stated it.

**Why both sinks moved onto the engine.** Before, each gave its verdict when the hand-off returned,
and counted a failed report afterwards, at a later batch or at `close()`: `probe_misc.py` case 3, 25
events whose every report is an error, read `flush()=True`, `failed_batches=0`, `failed=25` and 25
lines (2026-11-14 audit, finding 12). An emit-only wait was tried first and failed the corpus: an
`emit` can report a batch only as landed less what `losses()` moved, so an event counted in `failed`
that the broker kept reads as lost and held. Only a verdict can say `UNKNOWN`. Each sink gains
`deliver`, `log_foundry_counters` and the inline adapter as its `emit`; `KafkaSink` gains
`log_foundry_stop_signal`, which `GooglePubSubSink` already had; `failed` and `rejected` become
assignable properties over `failed_events` and `dropped_events`.

**The shared rule.** A round hands every remaining event to the client, then waits for the reports on
the messages it handed over. The wait ends when every one has a report, when the sink's own timeout
(`flush_timeout`, `overflow_timeout`) has passed, or when the stop signal is set once the first
slice has passed. The first slice is a deadline, the wait's start plus 50 ms, so a round under a set
stop still takes every report that arrives inside it. A pause between two asks goes through
`_retry.wait` and is never cut short by a signal already set when it began (`_retry.pause_on`), so a
round under a set stop does not ask in a tight loop.

| The report | Outcome | Counted | Line |
|---|---|---|---|
| success | `ACCEPTED` | nothing | none |
| a refusal no attempt could have written | `REJECTED` | `failed` | one per message, 1.x's text |
| any other error, none by the deadline, or none when the stop cut the wait | `UNKNOWN` | `failed` | one per round, `may have landed, not retried` |

Which refusals are definitive is *A delivery report is `REJECTED` only when no attempt could have
written the event*. A run with nothing `ACCEPTED`, something `REJECTED` and nothing `UNKNOWN` fails by
the engine's rule, so the worker counts one failed batch and `flush()` reads false; a run with any
`UNKNOWN` event settles. A report that arrives after its round settled changes no count and writes no
line, so nothing about a batch is counted after its verdict.

**`KafkaSink`'s wait is sliced.** One blocking `flush(flush_timeout)` waits on the producer's whole
backlog, so one message stuck on a leaderless partition held every later round for the full timeout.
Each pass calls `producer.flush(slice)`, at most 50 ms, which serves reports, returns early once the
queue is empty and, while it runs, holds librdkafka's flushing mark, under which queued messages go
without waiting out `linger.ms`; between passes the round reads its own report table. The wait also
ends when `flush(slice)` returns the `int` 0, and a message with no report is then `ACCEPTED`, which
is how a producer with `delivery.report.only.error` settles. A stuck partition costs each later round
one slice. The `flush()` hook calls `producer.flush(0)` and never raises for a remainder, since every
message a remainder holds was counted by its round; `close()` still flushes within `flush_timeout`,
so an `UNKNOWN` message may still land, and counts nothing.

**`GooglePubSubSink`'s wait is per future**, under one `overflow_timeout` deadline, in 50 ms slices
through `result(timeout=slice)`. A `TimeoutError` from a future whose `done()` is true is its outcome,
`UNKNOWN` at once; a `result` that takes no `timeout` is `UNKNOWN` at once. No verdict is outstanding
between emits, so the pending list, its reap, its overflow wait and `close()`'s counting are gone;
`max_pending` is still accepted and validated and bounds nothing. **`close()` waits for the
unconfirmed** (Andrew, 2026-11-22, "Wait at close (Recommended)"): a round keeps the futures it
left `UNKNOWN`, under `_futures_lock`, and `close()` takes them there and waits on them under one
`overflow_timeout` deadline, **not** cut short by the stop signal, counting nothing and writing no
line, since each round already gave and counted their verdicts. A future whose `result` takes no
`timeout`, or whose `done()` raises, is not waited on; a second `close()` waits on nothing, and
neither does a forked child's, since a list kept under another pid is dropped. A close landing mid-round makes the
later publishes `UNKNOWN` without raising, row 3 of *The sinks keep four declared exceptions to their
shared rules*.

**On the exit drain** the stop is set for the whole drain, so each round waits one slice and reads
the rest `UNKNOWN`; `close()`'s own bounded flush still gives Kafka's a chance to land, and
`GooglePubSubSink.close()`'s wait gives Pub/Sub's, whose sending thread is a daemon that a
process exiting on the verdict would end: measured with a stand-in at 0.2 s latency, a close that
only set the flag landed none of seven, and the waiting close lands all seven. The stop is what
keeps the drain itself from waiting out `GooglePubSubSink`'s 30 s default, which equals
`DEFAULT_SHUTDOWN_TIMEOUT`; the close's wait is the one bounded wait an exit then pays, at most
`overflow_timeout`, against a server that never answers. Since SPEC-098 the signal is the sink's
own and stays set after a `shutdown()` until a `configure()` installs the sink again, so a round
delivered in that window waits one slice too, and a sink installed again waits its full timeout.

**What it costs.** A broker round trip on every batch; against a broker that never answers, each
batch holds the drain thread for the sink's timeout, and an orphan log holds the caller's thread for
it (invariant 4's recorded exception). `docs/architecture.md` §13 carries the measured figures.

**Do NOT** give either sink a verdict before its round has read its reports; do NOT count a report
that arrives after its round settled; do NOT resend an `UNKNOWN` message; do NOT end the wait on a
stop signal before the first slice has passed, or skip a pause because the signal was already set;
do NOT make the `flush()` hook wait or raise for a remainder.

## History

### 2026-11-22

- **Refined** by SPEC-094 FR-002, on Andrew's answer "Wait at close (Recommended)": after the
  second diff review measured a flag-only `GooglePubSubSink.close()` losing every publish still
  in flight at a normal exit, `close()` waits up to `overflow_timeout` on the futures its rounds
  left `UNKNOWN`, counting nothing. `tests/test_pubsub_round_wait.py` holds the exit delivery and
  the close's bound.
- **Kept** through the rebase onto SPEC-098, which made the stop signal the sink's own and left it
  set after a `shutdown()` until a `configure()` installs the sink again; the rounds read it as
  before. `tests/test_stable_stop_signal.py`'s Pub/Sub test now confirms inside the round's wait,
  since a confirm outside it is `UNKNOWN` whatever the signal says.

### 2026-11-20

- **Settled** by SPEC-094 FR-001 and FR-002, on Andrew's answers of 2026-11-15 and 2026-11-16.
  `tests/test_kafka_report_wait.py` and `tests/test_pubsub_round_wait.py` hold it, and
  `tests/integration/test_kafka.py` and `tests/integration/test_pubsub.py` against real services.
