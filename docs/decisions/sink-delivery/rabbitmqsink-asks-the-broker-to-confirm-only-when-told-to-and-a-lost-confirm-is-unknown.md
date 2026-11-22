---
type: Decision
title: "`RabbitMQSink` asks the broker to confirm only when told to, and a lost confirm is unknown"
description: "publisher confirms are opt-in, `RabbitMQSink(confirm=True)`, for an owned and a borrowed connection alike: each channel the sink opens is put in confirm mode and each publish carries `mandatory`; a returned publish is `ACCEPTED`, `UnroutableError` or `NackError` `REJECTED`, a connection lost before the confirm `UNKNOWN`, never resent. An owned connection is probed before each round; a borrowed one is not. The default publishes unconfirmed, a recorded departure. (SPEC-093)"
---

**Andrew's choice, 2026-11-15.** Asked whether publisher confirms should be on by default, he
answered **"Off, opt in"**. A confirm costs a broker round trip per message, because a blocking
channel cannot pipeline confirms: `spec093_rabbitmq_confirm_cost.py` measured one `emit` of 1,000
events at 0.25 s with a 0.2 ms round trip, 1.12 s with a 1 ms one and 5.42 s with a 5 ms one, against
2 to 14 ms without, so at 5 ms one 1,000-event batch outlasts `log_foundry.flush()`'s 5 s default.
His 2026-11-16 answer on the TCP acknowledgement wait, "On, opt out", goes the other way, and the cost
is why (*A TCP socket round counts what it wrote to a connection its peer closed*).

**`confirm=False`, the default, is 1.x exactly**: no channel in confirm mode, `basic_publish` as
before, no probe. A publish that returned is all the sink can know, so a message routed nowhere or
written to a connection that died while idle reads as delivered. That is the corpus's
`publish-unconfirmed` departure, whose record is the class docstring.

**`confirm=True`.** `_active_channel` calls `confirm_delivery()` on each channel it opens, before the
first publish on it, probed by name as `SentrySink` probes `flush`, so a stand-in channel without it
publishes unconfirmed as in 1.x; a `confirm_delivery()` that raises closes the channel just opened
before the predicate reads the failure, so no channel leaks. On a confirming channel each publish
passes `mandatory=True`. pika 1.4.4's `basic_publish` then returns only once the broker acked:

- it returns: `ACCEPTED`;
- `UnroutableError` (returned, then acked: no queue took it) or `NackError`: `REJECTED`, since the
  broker answered and a resend earns the same answer; the channel stays open;
- `StreamLostError`, `ConnectionClosedByBroker`, `AMQPHeartbeatTimeout` or
  `ConnectionBlockedTimeout`: `UNKNOWN`, because the frame may have reached the broker; the channel
  is dropped, and an owned connection with it, so the next message reconnects, and each round writes
  one `ALWAYS` line, `RabbitMQSink, no confirm before the connection
  was lost (<Type>); may have landed, not retried`;
- anything else, `ChannelWrongStateError` and `ConnectionWrongStateError` among them: the 1.x
  predicate decides.

**The liveness probe, and the borrowed connection.** Before a round's first publish an owned
connection is asked `process_data_events(time_limit=0)`, probed by name; when it raises, the round
resets the connection, so a connection that died while idle is found before any frame is written and
its events stay `RETRYABLE` rather than reading as a lost confirm. A borrowed connection is not probed,
because `process_data_events` would run the caller's own consumer callbacks on the worker's thread.
So on a borrowed connection that died while idle the first publish reads as a lost confirm,
`UNKNOWN`, although nothing reached the broker: the corpus's `confirm-borrowed-unprobed` departure.
Confirms apply to both alike, because the sink never borrows a channel: it opens its own on the
borrowed connection, `confirm.select` is scoped to that channel, and the wait dispatches no other
channel's callbacks.

**The wait is bounded only by the connection.** pika's confirm wait has no timeout of its own; it
ends with the confirm or when the connection closes, on its heartbeat, `blocked_connection_timeout`
(30 s by default for an owned connection) or a socket error. That is invariant 3's sink blocked
inside a network call, recorded in `docs/architecture.md` §13.

**Validation.** `confirm` is a `bool` or a `TypeError` naming it and the type, before anything is
built; it is never read for its truth, since the argument is new and `confirm="false"` would turn
confirms on (*A bad argument is floored if it works and refused if it is already broken; a new one is
refused*).

**Do NOT** turn confirms on by default; do NOT probe a borrowed connection; do NOT resend a publish
whose confirm was lost; do NOT read a confirming publish's other exceptions as `UNKNOWN`, which an
earlier draft did and which read a stand-in's `TypeError` as a lost confirm.

## History

### 2026-11-20

- **Settled** by SPEC-093 FR-004, with Andrew's answer of 2026-11-15. `tests/test_rabbitmq_confirm.py`
  holds it, and `tests/integration/test_rabbitmq.py` against a real broker; the unroutable publish's
  re-plant without `mandatory` went red against the compose broker
  (`docs/audits/2026-11-14-probes/sinks/spec093_094_build_results.txt`).
