# Completed Spec — SPEC-074: The Delivery Engine for the SDK and Driver Sinks

## What was completed?

- Thirteen engine sinks: `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`, `ClickHouseSink`,
  `MongoDBSink`, `PostgresSink`, `_RedisSink` (both Redis sinks), `RabbitMQSink`,
  `AzureEventHubsSink`, `NATSSink`, `SocketTransport` and `SyslogSink`. Each defines `deliver`, one
  attempt per chunk, message or frame per round, and holds `log_foundry_counters`. Every sink but
  `SocketTransport` has an `emit` that is the one adapter call. Their eleven retry loops, backoff
  constants and hand-built `SinkLosses` are gone. `tests/test_engine_sinks_roster.py` derives the
  population, floor 13.
- Engine additions in `sinks/_engine.py`, all private. The run context gains the refusal marker
  (`refuse`), `final_rejections`, `sink_counted`, and `When.FAILED` with `announce(failed=)`. A
  sink's sub-counts commit with its `SinkCounters` totals under one lock, all or nothing
  (`Deferral.count_for`), and `_rewind` restores the marks along with the counts.
- Shared helpers: `_batch.adjudicate_keyed` (SQS and SNS `Failed`, MongoDB `writeErrors`),
  `_encode.encode_round` (each event is encoded once per run), and `sinks/_rounds.py` (send counts,
  loss lines and the keyed and positional settling).
- An SQS FIFO hold, decided per chunk at send time (`sqs._hold`).
- `max_retries` is read-only on `RabbitMQSink`, `SyslogSink` and `SocketTransport`, through
  `_args.ReadOnlyCount`, a descriptor that refuses a set.
- `SocketTransport.deliver` is new and `send_all` is deleted. `SyslogSink.deliver` forwards to it,
  and socket-mode `LogstashSink.emit` runs the adapter over it.

## What changed from earlier specs?

- The caller-visible changes are in the spec's *What a caller sees change* and in
  `docs/release-notes/v1.2.0.md`. `.failed` counts once per batch, and only what failed is resent.
  A non-retryable exception costs one attempt in total. An unreadable `Failed` or `writeErrors`
  list is counted and never resent. The total-failure message is now the adapter's. A `close()`
  during a retry wait returns at once.
- Decisions: two new ones, *A sink round resends only what failed, and an SQS FIFO group keeps its
  order* and *No shipped sink declares itself idempotent*. Several existing decisions were
  rewritten in place, each with a 2026-11-09 History entry. They are the five the spec lists, plus
  *A sink declares what it retries*, *A FIFO message group is a trace* and *An event a sink cannot
  encode* (the latter three at completion). `docs/invariants.md` entries 2 and 3,
  `architecture.md` §8 (a superseded marker), the README and the inventory were updated too.

## Verification

- All eight checks on PR 316 passed at head `082d5874`, including `test (py3.12)` and the
  integration job "sinks against real services". `main` was green on the merge commit `f087f312`.

## Open and accepted edges

- **Open, pending Andrew's call (Q4):** an SQS FIFO event still held behind a failing earlier
  event of its group when the `emit` budget runs out. Reading (a) is built: the event is counted
  `failed` with the line `SQSSink, 0 attempt(s), held behind an earlier message of its FIFO
  group`. Reading (b) would send held events in the run's last round. That needs the run context
  to tell the sink which round is last, so it would be a small follow-up confined to `sqs._hold`.
- Accepted: NATS's `_unlocked` still builds a `SinkLosses`, so that a forked child reads its
  counters without a lock (fork safety, invariant 12).
- Accepted: `LogstashSink`'s worker path keeps 1.x per-attempt counting until SPEC-075 gives it a
  `deliver`.
- Accepted: an `emit` override on a converted sink is counted once per `emit` call, as in 1.x.
- Accepted: the total-failure exception message changed, following SPEC-073's precedent.
