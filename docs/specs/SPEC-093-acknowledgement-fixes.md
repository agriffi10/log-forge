---
type: Spec
id: SPEC-093
title: "Acknowledgement Fixes: a Write the Sink Cannot Confirm Is Counted, Not Believed"
description: "Makes the TCP socket sinks, SentrySink's SDK backend, the AWS batch sinks and the _bulk sinks stop reporting as delivered what their backend never confirmed, and gives RabbitMQSink opt-in publisher confirms, so the rows SPEC-092's conformance corpus pins for findings 1, 11 and 14 turn green, RabbitMQ's default unconfirmed publish becomes a recorded departure, and no other row moves."
spec_status: Draft
depends_on: [SPEC-070, SPEC-072, SPEC-073, SPEC-074, SPEC-076, SPEC-092]
arc: audit-2026-11-14
arc_order: 9
---

## Overview

Five sinks tell the caller an event was delivered when nothing confirmed it. A TCP `SyslogSink` or
`LogstashSink` writes frames into a connection its peer has already closed and reports them taken.
`SentrySink` counts an event as sent when the Sentry SDK discarded it. The AWS batch sinks read a reply
that names no event as full delivery, and the `_bulk` sinks do the same with an empty object.
`RabbitMQSink` publishes without asking the broker to confirm anything. The 2026-11-14 audit found
each one, and SPEC-092's conformance corpus pins each one red. This spec makes the first four report
what they can actually know. An event is landed only when something confirmed it. It is lost when the
backend refused it. It is ambiguous, counted and announced as one that may have landed and never sent
again, when the sink cannot tell. `RabbitMQSink` gains publisher confirms as an option, off by default
as Andrew decided, and its default's unconfirmed publish becomes a recorded departure. Andrew decided
four of these rules on 2026-11-15, and this spec builds three of them. The fourth, a client-buffered
sink waiting for its delivery report, is SPEC-094, the arc's next spec, because it moves `KafkaSink`
and `GooglePubSubSink` onto the delivery engine.

## Scope

### In Scope

- A TCP socket transport that checks its held connection before each round and after it, and calls
  `UNKNOWN` every frame written into a connection that a later write or a reset shows was closed
  (FR-001).
- `SentrySink`'s SDK backend reading what `capture_event` returns, so a discarded event is counted
  as dropped rather than as sent (FR-002).
- Andrew's decision A for the AWS batch replies that name no event, and the same reading for a `_bulk`
  object that carries neither `errors` nor `items` (FR-003).
- `RabbitMQSink` publisher confirms, opt-in through `confirm=True` for an owned and a borrowed
  connection alike (FR-004).
- The conformance corpus's classes, variants, doubles, pins and departures for these sinks (FR-005).
- The record: `architecture.md` §12 and §13, the decisions, the invariants page, the README and the
  pending release notes (FR-006).

### Out of Scope

- **`KafkaSink` and `GooglePubSubSink` waiting for their delivery reports** (finding 12, Andrew's
  decision B). That is SPEC-094, the arc's next spec, which moves both sinks onto the delivery engine.
- **Idempotency keys and a count of the sends that may have duplicated** (finding 5). That is a later
  spec of this arc. The default `RabbitMQSink` keeps its finding-5 rows pinned. The opt-in confirmed
  variants read a lost confirm as `UNKNOWN`, which is not sent again, so their rows of that shape are
  green, and this adds no key and no counter.
- **Confirms on by default.** Andrew chose "Off, opt in" on 2026-11-15 (*Revision history*).
- **`SQSSink.MAX_BYTES`** against the service's current limit (`docs/architecture.md` §12).
- **The lifecycle command path**, and anything else under `_lifecycle.py`, `config.py` or `_fork.py`.
- **UDP.** A datagram is fire-and-forget. SPEC-092's §13 bullet and its `udp-fire-and-forget`
  departure stay as they are.
- **The rest of the `ElasticsearchSink` §12 item.** That is an object with `items` but no truthy
  `errors`, which still reads as success, unverified, until the Astra `_bulk` profile spec settles it.
  FR-003 takes only the object that carries neither key.
- **A `0` failed count with no records array** from Kinesis or Firehose. SPEC-070 settled that it
  reads as delivered, and decision A does not name it, so it stays delivered.
- **MongoDB's `writeErrors`.** It is a keyed reply, but MongoDB has no success list. Its unread
  reply is already `UNKNOWN`, and its row is green.
- **A JetStream ack that is lost, and an injected autocommit connection** (finding 15). Their pins stay.
- **Any change to the delivery engine, to `Outcome`, or to a counter's unit.** No shipped sink
  declares itself idempotent (`docs/decisions/sink-delivery/`), so an `UNKNOWN` event is never sent
  again.
- **A real-broker or real-service measurement of each fix.** That is owed to `integration.yml`
  (FR-004).

---

## Functional Requirements

Every premise below was first read at `33582484`, and the prototype under *Evidence* was measured
there. Two branches that touched `src/log_foundry/sinks/` were open while this spec was written,
`claude/driver-error-rework-c373af` (`sinks/_driver_error.py` and the sinks that call
`delivery_error`) and `claude/isinstance-guard-9eeb88` (an AST sweep of every type test). Both
landed, as `8bd0b54a` and `bb6656ff`, and SPEC-086 then landed as `9d70f782`. None of the three
changes a line this spec cites, and every `file:line` here was re-read at `9d70f782`. The prototype
was rebuilt on `9d70f782`, and the numbers under *Evidence* are from that tree unless they say
otherwise. **The build re-verifies every `file:line` against `main` as it stands.** No fix here calls
`delivery_error`, so this spec assumes no definition of it, current or earlier. Every type test the
fixes add is `_diag.is_a`, as *A type test reads the real type, never the value's __class__*
(`docs/decisions/failure-paths/`) requires, and `tests/test_class_check_roster.py` holds that.

**Andrew's decision A, made on 2026-11-15**, binds FR-003. He was asked whether an AWS batch reply
that names no failure should read as delivered. The examples were `{}`, a `Successful` list with no
`Failed`, and a Kinesis or Firehose reply with neither a count nor records. He answered
**"Unadjudicated (Recommended)"**: every event the reply does not name is `UNKNOWN`, counted in
`failed` and `dropped_unadjudicated`, announced once as one that may have landed, and never resent.
That matches the existing rule for an unreadable `2xx` on `_bulk`, Honeycomb and OTLP. Two more of
his answers that day bind FR-002 and FR-004, and *Revision history* quotes them.

### FR-001: A TCP socket transport never calls a frame taken by a connection it can see is closed

#### Description:

**Today.** `SocketTransport._round` marks a frame `ACCEPTED` as soon as `sendall` returns
(`sinks/_socket.py:404-408`, the `sendall` at `:517`). The class documents `failed` as "Messages not
confirmed" (`:157`), and nothing is ever confirmed. After the peer closes, the kernel takes the next
frames anyway and they go nowhere. `probe_socket.py` at `33582484` lost 4 of 50 from `SyslogSink` and
8 of 50 from `LogstashSink`, with `failed=0` and no line. The audit recorded 2 to 25.

**What the socket can know.** `spec093_socket_options.py` measures six options with a frame loop
shaped like `_round`, against a loopback listener (*Evidence*). A check between batches finds every
close the peer made between them. A check before every frame costs about 0.4 µs against a 4 to 10 µs
`sendall`, and it still let up to 10 of 50 frames vanish when the peer closed mid-batch. Calling the
frames written since the last check `UNKNOWN` when a later write raises catches a mid-batch close, but
only when a later write does raise. A check after the round's last frame catches the rest, but a FIN
alone cannot tell a close from a legal half-close: a peer that shuts down only its write side keeps
reading every frame. Counting on a FIN at that point reported all 50 frames of a half-closing peer as
may-have-landed, every one of which arrived. Counting only on a reset there reported none of them, and
still caught the close that a FIN-counting check caught. So the end-of-round check reads a reset and
not a FIN.

**The rule, for `transport="tcp"` only.**

1. **Before a round's first frame**, a held connection is checked by `_peer_state(sock)`, a
   module-level seam beside `_make_tcp` that answers `"open"`, `"fin"` or `"reset"`. The check is a
   zero-timeout readiness test, `select.select([sock], [], [], 0)`, falling back to `select.poll`
   where `select.select` refuses the descriptor with `ValueError` (one above `FD_SETSIZE`). A fresh
   `select.poll` object per check costs 21 to 41 µs, against 0.4 to 0.6 µs for `select.select`. When
   the socket is readable, the check calls `recv(1, socket.MSG_PEEK)`. Readable with nothing to read
   is `"fin"`, and an `OSError` from either call is `"reset"`. Either one makes the round call
   `_reset()` before it writes, so its frames go on a new connection and nothing is counted. A
   half-closed peer therefore costs a new connection per round and loses nothing. The check does not
   use `MSG_DONTWAIT`, because on a socket with a timeout, which `create_connection(timeout=…)` gives,
   CPython waits the whole timeout before that call returns. The probe measured 1.00 s of a 1.0 s
   timeout. A check that raises anything other than `OSError` answers `"open"`, so an object that is
   not a real socket keeps 1.x's behaviour.
2. **The frames written on the current connection since its last check that answered open** are
   unconfirmed. A connection opened inside the round counts as checked when it opened. When a later
   write on that connection raises, the unconfirmed frames become `UNKNOWN`. The raising frame keeps
   today's outcome, `RETRYABLE` or `REJECTED` as `_retryable_error` answers, and so it is still sent
   again on a new connection. That resend is finding 5, which is out of scope.
3. **After the round's last frame**, the connection is checked again. When the check answers
   `"reset"`, the round's unconfirmed frames become `UNKNOWN` and the connection is reset. A `"fin"`
   changes nothing in this round, and the next round's first check replaces the connection.
4. Each group of `UNKNOWN` frames is announced once, `ALWAYS`, with the line `lost N message(s);
   SocketTransport, written to a connection its peer closed; may have landed, not retried`. The
   engine counts them in `failed_events`, and they are never sent again.

**What stays unknowable.** Two windows remain. A peer can close with frames the kernel took and it
never read, and its reset can arrive after the end-of-round check. And a peer can close with frames it
read, then send only a FIN, so the check cannot tell it from a half-close. In both cases the frames
were reported landed, and the next round's check replaces the connection. FR-006 records this in §13.

#### Acceptance Criteria:

- [ ] `SyslogSink[tcp]-peer-closed` and `LogstashSink[tcp]-peer-closed` of
      `tests/test_conformance.py` read green (invariants 2 and 7).
- [ ] With `_make_tcp` returning ends of `socket.socketpair()`, a peer closed between two rounds
      leaves every frame of the second round on the new connection, `failed` unchanged and no
      `lost` line (invariants 2 and 7).
- [ ] With the peer closed mid-round and a later write raising `BrokenPipeError`, the frames written
      on that connection after its check are `UNKNOWN`, counted once in `failed`, named on one line,
      and never written again. The raising frame is written again on the next connection
      (invariants 2, 7 and 11).
- [ ] With the peer's end of a `socketpair` set to `SO_LINGER` zero and closed after the round's
      writes, so that the end-of-round check sees a reset, every frame of that round written on the
      connection is `UNKNOWN`, counted and named on one line (invariants 2 and 7).
- [ ] With a peer that calls `shutdown(SHUT_WR)` and keeps reading, `halfclose.py` (the reviewer's
      reproduction, re-run as a test) reads every frame received, `failed=0` and no line
      (invariant 2).
- [ ] A held socket whose check raises `TypeError` answers `"open"`, and its frames are `ACCEPTED`
      as in 1.x (invariant 1).
- [ ] `probe_socket.py` against the branch reports `batch2 received=50/50`, `failed=0` and no line
      for both sinks under FIN and RST, in five runs out of five (invariant 2).
- [ ] UDP rounds call no check, and `SyslogSink[udp]-peer-closed` and `LogstashSink[udp]-peer-closed`
      keep the `udp-fire-and-forget` departure (invariant 2).

### FR-002: An event the Sentry SDK discards at capture is counted as dropped, never as sent

#### Description:

**Today.** `SentrySink._capture` calls `self.client.capture_event(...)` and drops its return
(`sinks/sentry.py:448`). So `emit` adds every event the call did not raise on to `sent`
(`sinks/sentry.py:245`), and the class documents `sent` as "Events captured or sent to Sentry"
(`:84`). The audit's `probe_misc.py` case 5 used an SDK stand-in that kept 1 event in 5. At
`33582484` it still reads `sent=25` and `losses()` reads nothing lost, while 5 were kept.

**What the SDK reports.** sentry-sdk 2.74.0 is the version `poetry.lock` pins, and it documents
`Client.capture_event` as returning "An event ID. May be `None` if there is no DSN set or of if the
SDK decided to discard the event for other reasons" (`sentry_sdk/client.py:1166`). It returns `None`
when an error is ignored or a recursion guard is set (`_should_capture`, `:1047-1066`), when
`before_send` or an event processor drops the event (`:1180-1182`), when the error sample rate drops
it (`:1192-1198`), and when no transport holds it (`:1260-1268`). `Scope.capture_event` passes the id
through, or returns `None` under `disable_capture_event` (`sentry_sdk/scope.py:1462-1497`). What the
SDK loses later, after it returned an id, it never reports. A full transport queue drops the envelope
in `capture_envelope` (`sentry_sdk/transport.py:704-716`), and a rate limit acts on the worker
thread. That stays SPEC-092's `sdk-transport-drop` departure.

**The rule, as Andrew chose it on 2026-11-15** (*Revision history*). On the SDK branch, `_capture`
returns a third answer, *discarded*, when `capture_event` returns `None`, and the event moves no other
counter.

- **Count.** A new attribute, `discarded`, counts these events, and `losses()` reports it as
  `dropped`. The count is made through the `emit`'s own `Deferral`, so it lands only when `emit`
  returns.
- **Announce.** Each `emit` that discarded anything writes one line: `lost N event(s); SentrySink,
  discarded by the Sentry SDK at capture`.
- **Raise.** `emit` raises only when no qualifying event was sent and none was discarded, which is
  1.x's total failure. A batch with at least one discard returns, whatever else happened to it. That
  includes the mixed batch of discards and failures with nothing sent, because a resend would roll
  the sampler again and hand `before_send` an event it had already dropped. In that batch the
  failures are counted at capture in `transport_errors` (or the HTTP fallback's `failed`), as in 1.x,
  and the discards in `discarded` when `emit` returns. A batch of nothing but failures raises as
  before, with its `transport_errors` counted at capture as in 1.x.

Any value other than `None` reads as captured, as in 1.x. The HTTP fallback is unchanged.

**A caller sees** a stand-in `client=` whose `capture_event` returns nothing read as discarding every
event: `sent` stays 0, `losses().dropped` counts them, and one line is written per batch. A caller who
samples errors, or drops them in `before_send`, sees those events in `dropped`, with a line. The
library cannot tell sampling from an SDK that cannot deliver, because both return `None`.

#### Acceptance Criteria:

- [ ] `SentrySink[sdk]-never-acknowledged` reads green, and `SentrySink[sdk]-report-indefinite` keeps
      the `sdk-transport-drop` departure (invariants 2 and 7).
- [ ] `probe_misc.py` case 5 against the branch reads `sink.sent=5`, `losses: failed=0 dropped=20` and
      one line (invariants 2 and 11).
- [ ] A batch of nothing but discarded events returns, moves `discarded` by its size, and is emitted
      once through the worker (invariant 7).
- [ ] A batch of two discarded events and one that raises, with none sent, returns, moves
      `discarded` by 2 and `transport_errors` by 1, and is emitted once through the worker
      (invariants 2 and 7).
- [ ] A batch whose every qualifying event raises raises `SinkDeliveryError`, leaves `discarded`
      unchanged and moves `transport_errors` by its size, as in 1.x (invariants 2 and 7).
- [ ] `tests/test_deferral.py`'s `DEFERRING_SITES` lists `SentrySink.emit`, and its derived check that
      a `dropped` counter is written only by a commit passes over `discarded` (invariant 2).
- [ ] `capture_event` returning `""`, `0` or an object counts the event in `sent` (invariants 2 and
      7).

### FR-003: A batch reply that names no event adjudicates none of it

#### Description:

**Today.** `_rounds.read_keyed` reads a `Failed` list and calls every entry it does not name
`ACCEPTED`, and it never reads `Successful` (`sinks/_rounds.py:262`, `:271`). So `{}` reads as full
delivery. `_batch._adjudicate_counted` reads a Kinesis or Firehose reply with no failed count and no
coded record as delivered (`sinks/_batch.py:218-219`), which covers `{}` too. The `_bulk` reader reads
any object without a truthy `errors` as success (`sinks/elasticsearch.py:193`). At `33582484`,
`probe_sqs.py` G1 lost 6 of 25 uncounted and G2 lost 25 of 25. `probe_aws_more.py` cases 2 and 3 lost
25 of 25 Kinesis records and 6 of 25 SNS messages. Every one of those runs counted nothing and wrote no
line.

**The keyed rule, `SQSSink` and `SNSSink`.**

- A reply with neither `Failed` nor `Successful` is unread, so every entry of the call is `UNKNOWN`.
- A reply that carries `Successful` names entries in it, read with `adjudicate_keyed`'s discipline:
  a list of mappings, each with an `Id` that names one entry sent, no `Id` twice. An entry named in
  `Successful` is `ACCEPTED`. One named in `Failed` takes that entry's outcome as today. One named
  in neither is `UNKNOWN`. A `Successful` list the discipline refuses, or an entry named in both
  lists, makes the whole call unread.
- A reply that carries `Failed` and no `Successful` is read as in 1.x: an entry it does not name is
  `ACCEPTED`. Decision A names the empty object and `Successful` without `Failed`. A reply with only a
  `Failed` list states its failures in the field the 1.x reader trusts, and decision A does not name
  it. So this spec leaves the `Failed`-only reply alone, and test stand-ins that return
  `{"Failed": []}` keep working.
- Every `UNKNOWN` entry is counted in `failed` and in a new attribute, `dropped_unadjudicated`, on
  `SQSSink` and `SNSSink`. On these two sinks `dropped_unadjudicated` is a sub-count of `failed`,
  because `failed` is the engine's `failed_events`, which already holds every `UNKNOWN` event. On
  `KinesisSink`, `FirehoseSink` and the HTTP family it stays outside `failed`, as 1.x kept it, and
  FR-006 says so to callers. A call that was entirely unread writes 1.x's line with decision A's
  ending: `lost N message(s); <Class> could not adjudicate a <call> reply (N entry(ies) sent); may
  have landed, not retried`, where 1.x ended `; not retried`. An entry that was not named gets the
  line `lost N message(s); <Class>, a <call> reply named neither success nor failure for N
  entry(ies); may have landed, not retried`. Both are `ALWAYS`, and neither is ever sent again. The
  replies that were already unread at `33582484`, one that is not a mapping and a `Failed` list the
  discipline refuses, now move `dropped_unadjudicated` as well.

**The positional rule, `KinesisSink` and `FirehoseSink`.** `adjudicate_positional` gains a keyword,
`absent: bool = False`, meaning that the reply carried no per-record array at all. `settle_positional`
passes `absent=raw is None`. A reply with no failed count and no array is unadjudicated: its chunk is
`UNKNOWN`, counted on `dropped_unadjudicated` with 1.x's line. Every other combination reads as before.
A `0` count with no array stays delivered (SPEC-070), and a direct caller that does not pass `absent`
gets SPEC-070's reading unchanged.

**The `_bulk` rule, `ElasticsearchSink` and `OpenSearchSink`, decided in session.** Decision A's
principle is that a reply which names no verdict is never counted as delivered, and this rule applies
it to `_bulk`. In `_outcomes`, a `2xx` object that carries neither `errors` nor `items` cannot have
adjudicated any item, so it goes through `HTTPSink._unadjudicated`. That is the same path an
undecodable body takes (`elasticsearch.py:191`): `dropped_unadjudicated`, a may-have-landed line, and
no resend. `{}` and an error document such as `{"error": {...}, "status": 500}` take this path. An
object with `errors` absent or false beside `items`, and `{"errors": false}`, still read as success,
unverified. That keeps the §12 item's reason: an endpoint that omits only `errors` must not be
charged with every success. The rule refines the last sentence of the fence *A 2xx _bulk response with
no readable verdict is counted, never retried*, and FR-006 writes that refinement.

**A caller sees** counts and a line where 1.x showed silence, for replies only a proxy, an emulator or
a broken stand-in returns. A test stand-in that returns `{}` for SQS, SNS, Kinesis, Firehose or `_bulk`
now reads as unadjudicated. Code matching the text of an unread SQS or SNS reply's line sees its new
ending.

#### Acceptance Criteria:

- [ ] The `no-verdict-shape` rows of `SQSSink[standard]`, `SQSSink[fifo]`, `SNSSink`, `KinesisSink`,
      `FirehoseSink`, `ElasticsearchSink` and `OpenSearchSink` read green (invariants 2 and 7).
- [ ] `probe_sqs.py` against the branch reads `failed=6`, `uncounted_missing=0` for G1 and `failed=25`,
      `uncounted_missing=0` for G2, and G2's lines end `may have landed, not retried`.
      `probe_aws_more.py` reads `failed=25` with one line for case 2, and `failed=6` for case 3
      (invariants 2 and 11).
- [ ] An SQS or SNS reply of `{"Failed": []}`, of `Successful` naming every entry, or of both lists
      naming every entry, reads every entry it does not fail as `ACCEPTED` and moves no counter
      (invariant 7).
- [ ] A reply naming one entry in both lists, or a `Successful` list naming an unknown `Id`, makes
      every entry of the call `UNKNOWN`, and the client is called the same number of times as with a
      readable reply (invariants 2 and 7).
- [ ] `adjudicate_positional([a, b], [], failed_count=ABSENT, absent=True)` returns
      `Adjudication([], 2)`. With `absent` left out it returns `Adjudication([], 0)`. With
      `failed_count=0, absent=True` it returns `Adjudication([], 0)` (invariant 2).
- [ ] A `_bulk` `2xx` of `{}` or of `{"error": {"type": "auth"}, "status": 500}` moves
      `dropped_unadjudicated` by the chunk's size and is sent once, while `{"errors": false}` and
      `{"items": [...]}` without `errors` move nothing (invariants 2 and 7).
- [ ] A new `SQSSink` and a new `SNSSink` each read `dropped_unadjudicated == 0`, and
      `losses().failed` is unchanged by the sub-count (invariant 10).

### FR-004: `RabbitMQSink` can ask the broker to confirm each publish, and does when told to

#### Description:

**Today.** The value `basic_publish` returns is the verdict (`sinks/rabbitmq.py:431-436`). The sink
asks for neither publisher confirms nor `mandatory`, and the class docstring says that "written" is
not "acknowledged" (`:60-63`). `_retryable_error` leaves `NackError` and `UnroutableError`
unclassified because neither can be raised (`:451-453`).

**What pika gives.** In pika 1.4.4, `BlockingChannel.confirm_delivery()` turns on confirm mode for
that channel only (`pika/adapters/blocking_connection.py:2448-2472`). In confirm mode `basic_publish`
writes the frame and then waits in `_flush_output` for the broker's `Basic.Ack` or `Basic.Nack`
before it returns (`:2330-2368`). It raises `NackError` on a nack. With `mandatory=True` it raises
`UnroutableError` for a message that was returned and then acked, which means no queue took it. A
publish on a closed channel or connection raises `ChannelWrongStateError` or
`ConnectionWrongStateError` before anything is written. A connection lost while the publish waits
raises from `_flush_output`. The frame may or may not have reached the broker at that point. The
wait has no timeout of its own. It ends with the confirm, or when the connection closes: on its
heartbeat, on `blocked_connection_timeout` (30 s by default for an owned connection, SPEC-049
FR-005), or on a socket error. `BlockingConnection.process_data_events(time_limit=0)` reads the
socket once and then dispatches every channel's pending consumer callbacks (`:874-906`).

**The rule.**

- **The constructor.** `RabbitMQSink.__init__` gains a keyword-only `confirm: bool = False`, after
  `stack_timeout`. Andrew chose the default on 2026-11-15 (*Revision history*).
- **`confirm=False`** is 1.x exactly: no channel is put in confirm mode, `basic_publish` is called as
  today, no liveness probe runs, and the class docstring says what a returned publish proves.
- **`confirm=True`, the channel.** `_active_channel` calls `confirm_delivery()` on each channel it
  opens, before the first publish on it. The method is probed by name, as `SentrySink` probes
  `flush`, so a stand-in channel that lacks it publishes unconfirmed, as in 1.x. When
  `confirm_delivery()` raises, the channel just opened is closed through `_safe_close` before the
  exception goes to the predicate as a failure to get a channel, so no channel leaks. On a channel in
  confirm mode, `basic_publish` passes `mandatory=True`.
- **`confirm=True`, the liveness probe.** Before a round's first publish, an owned connection is
  asked `process_data_events(time_limit=0)`, probed by name. When it raises, the round calls
  `_reset()`, which closes the owned connection, so the first publish reconnects. A connection that
  died while idle is found here, before any frame is written, and its events stay `RETRYABLE`, as in
  1.x, rather than reading as a lost confirm. A borrowed connection is not probed, because
  `process_data_events` would run the caller's own consumer callbacks on the worker's thread
  (`blocking_connection.py:902-906`). So on a borrowed connection that died while idle, the first
  publish's `StreamLostError` reads as a lost confirm, `UNKNOWN`, although nothing reached the broker.
  FR-005 records that as a departure.
- **What a publish on a confirming channel means.**
  - It returns: `ACCEPTED`, because the broker acked it.
  - It raises `UnroutableError` or `NackError`: `REJECTED`. The broker answered and kept nothing, so a
    resend earns the same answer. The channel stays open, and the line is the arc's
    `RabbitMQSink, N attempt(s), <Type>`.
  - It raises `StreamLostError`, `ConnectionClosedByBroker`, `AMQPHeartbeatTimeout` or
    `ConnectionBlockedTimeout`: `UNKNOWN`. The connection is reset. Each round writes one line,
    `lost N message(s); RabbitMQSink, no confirm before the connection was lost (<Type>); may have
    landed, not retried`, `ALWAYS`.
  - It raises anything else, including `ChannelWrongStateError`, `ConnectionWrongStateError` and
    `ChannelClosedByBroker`: today's predicate decides, as before.

**Owned and borrowed alike.** The sink never borrows a channel. It borrows a connection, and it opens
its own channel on that connection (`rabbitmq.py:500`). AMQP's `confirm.select` is scoped to one
channel, so the caller's own channels are untouched. `_flush_output` dispatches no other channel's
consumer callbacks while it waits (`blocking_connection.py:512-556`). So confirms apply to both, and
the only difference is the liveness probe above.

**The cost** is a broker round trip per message, because a blocking channel cannot pipeline
confirms. `spec093_rabbitmq_confirm_cost.py` measured one `emit` of 1,000 events at 0.25 s with a
0.2 ms round trip, 1.12 s with a 1 ms one, and 5.42 s with a 5 ms one, against 2 to 14 ms without
confirms. At 5 ms, one 1,000-event batch outlasts `log_foundry.flush()`'s 5 s default timeout
(`__init__.py:42`). That is about 4,000, 900 and 180 events per second (*Evidence*), and it is why
confirms are opt-in.

#### Acceptance Criteria:

- [ ] `RabbitMQSink[confirmed]` reads green on every row the corpus runs for it, and
      `RabbitMQSink[confirmed-borrowed]` on every row but `peer-closed`, which reads
      `ambiguous-when-knowable` under the `confirm-borrowed-unprobed` departure (invariants 2 and 7).
- [ ] `RabbitMQSink[owned]` and `RabbitMQSink[borrowed]`, the default, read exactly as they read at
      `9d70f782`, and their `peer-closed` rows sit under the `publish-unconfirmed` departure
      (invariants 2 and 7).
- [ ] With a stand-in connection and channel that record calls, a `confirm=True` sink calls
      `confirm_delivery()` once per channel it opens, passes `mandatory=True` on every publish, and
      calls `process_data_events(time_limit=0)` once per round on an owned connection and never on
      a borrowed one. A default sink calls none of the three (invariant 10).
- [ ] With an owned connection whose `process_data_events` raises `StreamLostError`, a
      `confirm=True` round reconnects before its first publish, and every event is `ACCEPTED` with
      no `lost` line (invariants 2 and 7).
- [ ] `UnroutableError` and `NackError` each make the message `REJECTED`, publish it once, and keep
      the channel. `StreamLostError` raised by a confirming publish makes it `UNKNOWN`, publishes it
      once, and reconnects for the next message (invariants 2 and 7).
- [ ] A `confirm_delivery()` that raises leaves the channel it was called on closed, and the round's
      events `RETRYABLE` or `REJECTED` as the predicate answers (invariants 2 and 7).
- [ ] A stand-in channel without `confirm_delivery` publishes and is `ACCEPTED`, as in 1.x (invariant
      10).
- [ ] `inspect.signature(RabbitMQSink)` equals the `9d70f782` signature plus a keyword-only `confirm`
      with default `False`, as the last parameter (invariant 10).
- [ ] **Owed until a green run:** in `integration.yml`, against its real broker, a `confirm=True`
      sink delivers a batch, and a publish with `mandatory` to a routing key no queue is bound to is
      `REJECTED` and counted in `failed` with its line. The same test, with `mandatory=True` removed
      from `_publish` in a re-plant on the PR's branch, goes red, which shows it reads `mandatory`
      (invariants 2 and 7).

### FR-005: The conformance corpus holds what these sinks can now know

#### Description:

These changes are to SPEC-092's files under `tests/`. This spec builds after SPEC-092, so they apply
to the corpus as its build delivers it, and the tables in SPEC-092's FR-001, FR-002 and FR-004 are the
baseline.

- **Variants.** `RabbitMQSink` `owned` and `borrowed` stay in `socket-stream`, because the default
  is unconfirmed. Two new variants, `RabbitMQSink[confirmed]` (owned) and
  `RabbitMQSink[confirmed-borrowed]`, are in `per-event-ack`, as SPEC-092 FR-001 foresaw, each built
  with `picks={"confirm": True}`. `PARAMETERS` gains `confirm` for `RabbitMQSink`. That gives 55
  variants.
- **`Variant.adds`.** `Variant` gains `adds: Mapping[Fault, str]`, which lists the faults a variant
  runs beyond its class's, each with a reason. A variant's faults are, in order, `only` when it is set
  or else its class's faults, then `adds`, and then every fault in `skips` is removed. Both confirmed
  variants add `taken-then-raised` ("a confirmed publish writes its frame before it waits") and
  `peer-closed` ("a connection is held between batches"). A new pure function in
  `tests/conformance_classes.py`, `adds_faults(variants) -> list[str]`, returns one failure line, naming
  the variant and the fault, for each of these: an `adds` fault its class already runs, one on a
  variant that sets `only`, one that `skips` also names, and one that is not a `Fault`. A test applies
  it to `VARIANTS` and expects no line. That makes 268 rows: the 256, and 12 for the two confirmed
  variants.
- **The RabbitMQ double.** `RabbitChannel` gains `confirm_delivery()` and a `mandatory` keyword, and
  `RabbitConnection` gains `process_data_events(time_limit=0)`. That call raises `StreamLostError`
  on a broken connection, and on a generation-0 connection once the peer has closed, which marks it
  broken. On a confirming channel:
  - a refusal raises `ChannelWrongStateError` before anything is taken, which is what pika raises for
    a channel whose connection is gone;
  - `taken-then-raised` and `reply-lost` take the frame and then raise `StreamLostError`;
  - a write on a generation-0 connection after `peer-closed` raises `StreamLostError` without taking
    anything, and the call is marked knowable, because the connection was dead before the frame was
    written.

  An unconfirmed channel behaves as SPEC-092's double does. `DRIVERS["pika.exceptions"]` gains
  `UnroutableError`, `NackError` and `ChannelClosedByBroker`.
- **Pins.** These pins are removed:
  - `reply-without-verdict`, `bulk-without-errors`, `peer-closed-tcp` and `sdk-discard-counted-sent`,
    because every row in them reads green;
  - `publish-without-confirms`, because its item is closed (FR-006) and its two rows are now the
    default's departure.

  `reply-resent` (50 rows) and `reply-counted-lost` (4) are unchanged, because the default
  `RabbitMQSink` rows in them are unchanged.
- **Departures.** Two are new.
  - `publish-unconfirmed` holds `RabbitMQSink[owned]-peer-closed` and
    `RabbitMQSink[borrowed]-peer-closed` at `silent-loss`. Its record is
    `src/log_foundry/sinks/rabbitmq.py` and the phrase "With `confirm=False`, the default, a publish
    that returned is all this sink can know", which the class docstring carries.
  - `confirm-borrowed-unprobed` holds `RabbitMQSink[confirmed-borrowed]-peer-closed` at
    `ambiguous-when-knowable`. Its record is the same file and the phrase "A borrowed connection is
    not probed before a round, so a publish on one that died while idle reads as a lost confirm".
- **Totals.** 268 rows: 62 pinned under seven pins, 15 departure rows under nine departures, and 191
  that must pass. SPEC-092's FR-004 asserts its totals as literals, and those literals change to
  these numbers.

#### Acceptance Criteria:

- [ ] The corpus generates 268 rows over 55 variants, and its literals read 268 rows, 62 pinned, 15
      departure rows and 191 that must pass (invariants 2 and 7).
- [ ] Against SPEC-092's delivered corpus at the branch, only the rows the criteria of FR-001 to
      FR-004 name change verdict, and every other row reads as it read before this spec's `src/`
      change (invariants 2 and 7).
- [ ] `adds_faults` has standing fixtures in `tests/test_conformance_gate.py`, one per failure it
      names, each asserting its line names the variant and the fault, and one well-formed table that
      yields no line. Applied to `VARIANTS`, it yields none (invariants 2 and 7).
- [ ] With FR-001's second rule removed, so that a later raise leaves the frames before it
      `ACCEPTED`, in an export of the branch head, `SyslogSink[tcp]-peer-closed` and
      `LogstashSink[tcp]-peer-closed` read `silent-loss` again. The corpus's fake socket cannot be
      polled, so its check answers `"open"` and the corpus reaches only that rule. The two checks are
      held by FR-001's socketpair and half-close criteria (invariant 2).
- [ ] With the liveness probe removed, `RabbitMQSink[confirmed]-peer-closed` reads
      `ambiguous-when-knowable`. With `confirm_delivery` never called, `RabbitMQSink[confirmed]` reads
      `duplicate` and `RabbitMQSink[confirmed-borrowed]` reads `lost-but-held` on
      `taken-then-raised`. Each is measured in an export of the branch head (invariant 2).

### FR-006: The record says what each sink can know

#### Description:

These are the completion ritual's edits (`docs/process/completion-ritual.md`), made on the branch
that is pushed. They act on the §12 text that SPEC-092's build leaves, and SPEC-092's FR-004 lists
that text.

- **§12, struck as closed by SPEC-093.** Each struck item carries its probe's numbers before and
  after.
  - The finding-1 item ("A TCP `SyslogSink` or `LogstashSink` counts a frame as accepted when
    `sendall` returns…").
  - The finding-11 item ("`SentrySink`'s SDK backend counts an event the SDK discarded as sent").
  - The finding-14 item ("An AWS batch reply that names no failure is read as full delivery").
  - The RabbitMQ item ("`RabbitMQSink` publishes without confirms…"). Its closure quotes Andrew's
    "Off, opt in", names the `publish-unconfirmed` departure, and names the integration run that
    FR-004 owes.
- **§12, rewritten in place and dated.**
  - The `ElasticsearchSink` item ("`ElasticsearchSink` reads a `2xx` object without a truthy
    `errors` as success, unverified.") keeps only the object with `items` and no truthy `errors`,
    and says that `{}` and an error document are counted since SPEC-093.
  - The finding-5 item gains one sentence: a `confirm=True` `RabbitMQSink` publish whose confirm was
    lost is `UNKNOWN` and is not resent, and the default keeps the resend.
- **§13 gains four bullets.**
  - "**A TCP frame the kernel took is unconfirmed until the transport's next check.**" A peer whose
    reset arrives after the end-of-round check, or that closes with only a FIN, loses those frames
    while they read as delivered.
  - "**A FIN alone is not read as a loss, because it is also a legal half-close.**" The bullet
    carries the reproduction (`halfclose.py`, all 60 frames received with `failed=60` under a
    FIN-counting check) and the reason. A false count is the safe direction for a may-have-landed
    line, but a half-closing collector would see one on every batch, and a reset is what proves the
    peer discarded data.
  - "**A confirmed `RabbitMQSink` publish costs a broker round trip.**" The bullet carries the
    measured figures at 0.2, 1 and 5 ms, and says that at 5 ms one 1,000-event batch outlasts
    `flush()`'s 5 s default.
  - "**A confirm wait is bounded only by the connection.**" The wait ends on the heartbeat,
    `blocked_connection_timeout` or a socket error. It is an instance of invariant 3's sink blocked
    inside a network call.
- **Decisions in `docs/decisions/sink-delivery/`, each one file with its fence, entry and dated
  History.**
  - A new decision for FR-001's rule, with the half-close reasoning.
  - A new decision for FR-002's rule. It quotes Andrew's "Dropped, with a line (Recommended)" with
    the date, and the counter-precedent he was shown.
  - A new decision for decision A, which quotes Andrew's question and answer with the date, together
    with the `Failed`-only reading.
  - A new decision for FR-004's rule. It quotes Andrew's "Off, opt in" with the date, and records the
    liveness probe and the borrowed-connection departure.
- **Two settled decisions refined in place, each with a dated History entry naming SPEC-093.**
  - *A positional response adjudicates all of a chunk or none of it*: a reply that carries none of
    its verdict fields adjudicates none of its chunk, and in a keyed reply that lists `Successful`,
    an entry named in neither list is `UNKNOWN`.
  - *A 2xx _bulk response with no readable verdict is counted, never retried*: its last sentence
    becomes "An object with `errors` absent or false beside `items` still reads as success,
    unverified; one with neither key is counted (SPEC-093)".
- **`docs/invariants.md`.** Invariant 7's recorded exceptions gain a `SentrySink` batch with at least
  one discarded event and none sent, which returns, counted. The *Guarded* lines of invariants 2 and
  7 name the new tests.
- **README.**
  - The `SyslogSink` and `LogstashSink` rows (`README.md:783-784`) gain the TCP checks and the
    may-have-landed line.
  - The `SentrySink` paragraph (`:842-845`) says that a discarded event is in `losses().dropped`,
    including one sampled out or dropped by `before_send`.
  - The AWS paragraphs (`:870-880`) state decision A's reading, and that `dropped_unadjudicated` is
    a sub-count of `failed` on `SQSSink` and `SNSSink` but outside `failed` on `KinesisSink` and
    `FirehoseSink`.
  - The `ElasticsearchSink` row (`:779`) states FR-003's `_bulk` reading.
  - The queue-and-stream paragraph (`:956-958`) and the `RabbitMQSink` row (`:1001`) state
    `confirm=True`, the probe, and the cost.
- **The release notes.** In the pending `docs/release-notes/v1.2.1.md` if it is on `main` when this
  spec completes, and under a "Release notes owed" heading in the delivery doc otherwise, as SPEC-090
  words it.
  - **The file's title and opening paragraphs are rewritten**, so that no sentence there becomes
    false. Today's title is about the file and stream sinks only, and the opener says "No constructor
    signature or default changed", which `RabbitMQSink(confirm=)` makes false. The file keeps its
    name. Whether the release is 1.2.1 or 1.3.0 is chosen when it is tagged, since a new keyword
    argument is additive. The opener says so, and no sentence depends on the number.
  - The entry lists:
    - the TCP line and its `UNKNOWN` counts, and a half-closing peer's connection replaced each round;
    - `SentrySink.discarded` and its line, that sampling or `before_send` now moves `dropped` and
      prints a line, and the stand-in returning `None`;
    - decision A's counts and lines, the new ending of the unread-reply line, the stand-in returning
      `{}`, and `dropped_unadjudicated` on `SQSSink` and `SNSSink`, a sub-count of `failed` there and
      outside it on the other sinks that carry it;
    - the `_bulk` reading of an object with neither key;
    - `RabbitMQSink(confirm=)`, off by default; with it on, `mandatory`, `UnroutableError` and
      `NackError` as rejections, the `UNKNOWN` lost confirm, the probe, and the per-message round
      trip.

    Its compatibility list names each of these for a `>=1,<2` pin, as the file's opening list does.
- **`docs/component-inventory.md`.** No row is owed, because no module is added.

#### Acceptance Criteria:

- [ ] `docs/architecture.md` §12 strikes the four items and rewrites the two named above, each dated
      and naming SPEC-093, and §13 carries the four bullets.
- [ ] The four new decision files and the two dated History entries exist. The decision-A file quotes
      "Unadjudicated (Recommended)", the Sentry file "Dropped, with a line (Recommended)" and the
      RabbitMQ file "Off, opt in", each with the date 2026-11-15.
- [ ] `docs/invariants.md`'s recorded exceptions for a sink that delivered nothing name a
      `SentrySink` batch with a discard and nothing sent.
- [ ] Each README site above states the new behaviour. Nothing in the README says a TCP frame, a
      Sentry capture, an AWS reply or a RabbitMQ publish is delivered on less than this spec requires,
      and the README states where `dropped_unadjudicated` sits for each sink.
- [ ] The release-notes file's title and opener hold for this release, and its entry names each
      change listed.
- [ ] This FR serves no invariant: it is the record of FR-001 to FR-005, and the promises it
      describes are theirs.

---

## Data Model

```python
# src/log_foundry/sinks/_socket.py
def _peer_state(sock: socket.socket) -> Literal["open", "fin", "reset"]: ...   # seam beside _make_tcp

# src/log_foundry/sinks/sentry.py
class SentrySink:
    discarded: int          # new: events capture_event returned None for; losses().dropped

# src/log_foundry/sinks/sqs.py, sns.py
class SQSSink:  dropped_unadjudicated: int   # new: a sub-count of failed
class SNSSink:  dropped_unadjudicated: int   # new: a sub-count of failed

# src/log_foundry/sinks/_batch.py
def adjudicate_positional[T](
    records: list[T], results: list[dict[str, Any]], *, error_key: str = "ErrorCode",
    failed_count: object = _NOT_PASSED, rejected: bool = False,
    absent: bool = False,                     # new: the reply carried no per-record array
) -> Adjudication[T]: ...

# src/log_foundry/sinks/rabbitmq.py
class RabbitMQSink:
    def __init__(self, *, exchange: str, routing_key: str, connection: Any = None,
                 url: str | None = None, max_retries: int = 3,
                 blocked_connection_timeout: float | None = None,
                 socket_timeout: float | None = None, stack_timeout: float | None = None,
                 confirm: bool = False) -> None: ...   # new keyword, last

# tests/conformance_classes.py (SPEC-092)
@dataclass(frozen=True, kw_only=True)
class Variant:
    ...
    adds: Mapping[Fault, str] = MappingProxyType({})   # new: faults beyond the class's

def adds_faults(variants: Iterable[Variant]) -> list[str]: ...   # new: one line per misuse of adds
```

## File & Folder Structure

```
src/log_foundry/sinks/
├── _socket.py        # FR-001: _peer_state, the round's checks and UNKNOWN frames
├── sentry.py         # FR-002: discarded, the Deferral in emit
├── _rounds.py        # FR-003: read_keyed reads Successful; settle_keyed counts the unnamed
├── _batch.py         # FR-003: adjudicate_positional(absent=)
├── sqs.py, sns.py    # FR-003: dropped_unadjudicated
├── elasticsearch.py  # FR-003: an object with neither errors nor items
└── rabbitmq.py       # FR-004: confirm=, the probe, confirm_delivery, mandatory, the outcomes
tests/
├── conformance_classes.py, conformance_doubles.py, conformance_pins.py   # FR-005
└── test_*.py         # the new tests, and the ones that change (Evidence)
docs/                 # FR-006
```

## Implementation Phases

### Phase 1: The reply readers (FR-003)

- `read_keyed`, `settle_keyed`, `adjudicate_positional(absent=)`, `settle_positional`, the `_bulk`
  reader, and the two new attributes, with their tests.

### Phase 2: The writers (FR-001, FR-002, FR-004)

- The socket checks and `UNKNOWN` frames, `SentrySink.discarded`, and `RabbitMQSink`'s opt-in
  confirms and probe, with their tests and the integration run.

### Phase 3: The corpus and the record (FR-005, FR-006)

- SPEC-092's variants, `adds`, doubles, pins and departures, and the record.

## Evidence

The prototype is `docs/audits/2026-11-14-probes/sinks/spec093_prototype.patch`. It applies with
`patch -p1` to `git archive 9d70f782 src tests docs/audits/2026-11-14-probes`, and it carries this
spec's `src/` change and SPEC-092's prototype corpus edited as FR-005 says. SPEC-092 is not built, so
its prototype (`spec092_run.py`) stands in for its corpus. The corpus's `ledger` check does not run,
and the build re-measures against the delivered corpus. Numbers were taken on macOS with Python 3.13.1
and no extra installed, at `9d70f782` unless they say otherwise. Other sessions shared the machine,
and the one-minute load is given where it was read.

**The rows.** Against the prototype corpus at `9d70f782`, `src/` unchanged, 256 rows read 170 green
and 86 red, which is SPEC-092's record. With this patch alone applied, and no SPEC-094 change, 268 rows
read 191 green and 77 red. That is the same in three runs on Python 3.13.1 and one on Python 3.12.12,
each exiting 0, and from the committed patch applied to a fresh export. Exactly these ten rows move,
each from `silent-loss` to green:

- the `no-verdict-shape` rows of `SQSSink[standard]`, `SQSSink[fifo]`, `SNSSink`, `KinesisSink`,
  `FirehoseSink`, `ElasticsearchSink` and `OpenSearchSink`;
- `SyslogSink[tcp]-peer-closed` and `LogstashSink[tcp]-peer-closed`;
- `SentrySink[sdk]-never-acknowledged`.

The twelve new rows read green, except `RabbitMQSink[confirmed-borrowed]-peer-closed`, which reads
`ambiguous-when-knowable`. The default `RabbitMQSink` rows read as at `9d70f782`. During authoring the
changes were also measured one at a time, each on top of the ones before it, and each step moved only
the rows of the sinks it changed. The socket change alone moved exactly the two TCP rows.

**The mutants.** Each was run on this patch's corpus.

- With FR-001's second rule removed, both TCP `peer-closed` rows read `silent-loss`.
- With the end-of-round check counting a FIN as well as a reset, `halfclose.py` read `failed=60` for
  60 frames that all arrived. With it counting only a reset, it read `failed=0`. At `9d70f782`,
  `src/` unchanged, it read `failed=0`.
- With the liveness probe removed, `RabbitMQSink[confirmed]-peer-closed` read
  `ambiguous-when-knowable`.
- With `confirm_delivery` never called, `RabbitMQSink[confirmed]-taken-then-raised` read `duplicate`,
  `RabbitMQSink[confirmed-borrowed]-taken-then-raised` read `lost-but-held`, and
  `RabbitMQSink[confirmed-borrowed]-peer-closed` read `silent-loss`.

**The suite.** The full suite was run against the prototype in a scratch export with no `docs/`. 43
tests fail there with or without the prototype, because they read files the export lacks. The
prototype fails 25 more. They are the same 25 at `33582484`, at `bb6656ff` and, after this spec's
revision, at `9d70f782`, so the revision's changes (the opt-in default, the probe, the reset-only end
check, the mixed Sentry batch and the line's new ending) fail no test the earlier draft did not. Each
is accounted for:

- FR-002, 5: `test_sentry_backend.py::test_a_usable_client_is_preferred_over_the_fallback`,
  `::test_the_default_prefers_a_usable_sdk`, `test_sink_losses.py::test_sentry_transport_errors_are_isolated_per_event`
  and `test_sinks_saas.py::test_sentry_sdk_path_with_level_gating`, whose stand-in `capture_event`
  returns `None`; and `test_deferral.py::test_every_deferring_site_takes_the_one_helper`, whose
  `DEFERRING_SITES` lacks `SentrySink.emit`.
- FR-003 keyed and positional, 5: `test_classify.py::test_consistent_replies_read_as_before_and_odd_counts_are_unadjudicated`
  `[absent-no-array-KinesisSink]` and `[absent-no-array-FirehoseSink]`, which pin `{}` as delivered;
  `test_sinks_sqs.py::test_a_client_failure_costs_its_chunk_not_the_batch` and
  `test_sinks_sns.py::test_a_client_failure_costs_its_chunk_not_the_batch`, whose stand-in returns
  `{}` for success; and `test_sinks_encode.py::test_the_public_surface_grows_only_by_five_losses`,
  which meets `dropped_unadjudicated` on `SNSSink` and `SQSSink`.
- FR-003 `_bulk`, 14: `test_sinks_elasticsearch.py::test_an_object_without_truthy_errors_is_read_as_success_unverified`
  `[empty-object]` and `[error-document]`, which pin the old reading; and twelve in
  `test_http_rounds.py`, whose scripted `_bulk` server answers success with `{}`.
  - `test_emit_and_deliver_leave_equal_ledgers_bodies_and_lines`, six:
    `[ElasticsearchSink-200]`, `[ElasticsearchSink-503-then-200]`, `[ElasticsearchSink-oversized]`,
    and the same three for `OpenSearchSink`.
  - `test_worker_and_orphan_paths_leave_equal_ledgers`, four: `-200` and `-503-then-200` for each
    sink.
  - `test_each_shim_returns_its_1x_bool[ElasticsearchSink-no-errors-key]`.
  - `test_an_idempotent_reader_counts_an_unknown_once_per_run[recovers]`.
- FR-004, 1: `test_classify.py::test_every_driver_classes_call_is_literal_and_the_scan_finds_all_seven_drivers`,
  whose stand-in table lacks `UnroutableError`, `NackError` and `ConnectionWrongStateError`.

No existing test covers FR-002's mixed batch, so its criterion is a new test. An earlier draft's
prototype called `confirm_delivery` unconditionally and read every exception from a
confirming publish as `UNKNOWN`. It failed 41 RabbitMQ tests, whose doubles have no
`confirm_delivery`, and it read a stand-in's `TypeError` as a lost confirm. That is why FR-004 probes
the method by name and names the four classes. An earlier FR-003 read every SQS entry that
`Successful` did not name as `UNKNOWN`. It failed 9 more tests whose stand-ins return
`{"Failed": []}`, so FR-003 keeps the `Failed`-only reply. An earlier `_bulk` rule, "no `items`",
failed 4 more: the `[errors-false]`, `[errors-null]` and `[errors-zero]` rows of
`test_an_object_without_truthy_errors_is_read_as_success_unverified`, and
`test_sink_concurrency.py::test_elasticsearch_dropped_unadjudicated_is_counted_under_the_lock`. So
FR-003 takes only the object with neither key.

**The socket options.** `spec093_socket_options.py 5` was run once, at a one-minute load near 100. The
table gives silent loss out of the second batch, meaning frames reported accepted that the listener
did not receive, over the five runs. The bracketed figures are frames counted as may-have-landed that
did arrive.

| Scenario | today | check per round | check per frame | unknown on a raise | round + raise | round + raise + end | round + raise + end on reset |
|---|---|---|---|---|---|---|---|
| peer closes between batches, FIN | 6 to 22 | 0 | 0 | 0 | 0 | 0 | 0 |
| peer closes between batches, RST | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| peer closes after 10 of 50, mid-batch | 1 to 25 | 1 to 12 | 0 to 10 | 0 | 0 | 0 | 0 |
| peer closes after 2 of 3, 1 ms between frames | 1 | 1 | 0 | 1 | 1 | 0 | 0 |
| peer half-closes and keeps reading | 0 | 0 | 0 | 0 | 0 | 0 (50 counted) | 0 |

Earlier sessions of the probe, at three and five runs and lower loads, read 1 to 49 for *today* and
0 for *round + raise + end* in every scenario; the half-close row and the last column are new in this
session. The cost lines, three runs at a load near 21: `select.select` 0.41 to 0.59 µs per check, a
fresh `select.poll` object 21 to 41 µs, `sendall` of a 156-byte frame 4.1 to 9.8 µs, and
`recv(MSG_PEEK | MSG_DONTWAIT)` on an idle socket with a 1.0 s timeout raised `TimeoutError` after
1.00 s each time. `probe_socket.py` against the prototype read `batch2 received=50/50`, `failed=0` and
no line for both sinks under FIN and RST, in three runs out of three.

**The reply probes.** These were measured against the prototype, and the `33582484` figures are in
each FR's Description. `probe_sqs.py` read G1 `failed=6`, two lines, `uncounted_missing=0`, and G2
`failed=25`, three lines, `uncounted_missing=0`. Every other case read as at `33582484`.
`probe_aws_more.py` read case 2 `failed=25`, one line, and case 3 `failed=6`, two lines.
`probe_misc.py` case 5 read `sink.sent=5 … dropped=20`, one line.

**The confirm cost.** `spec093_rabbitmq_confirm_cost.py` measured the median `emit`, with a fake
channel that sleeps one round trip per confirmed publish, at a load near 21:

| Round trip | batch 10 | batch 100 | batch 1,000 |
|---|---|---|---|
| none (`9d70f782`) | 0.05 to 0.09 ms | 0.24 to 0.53 ms | 2.3 to 14.3 ms |
| 0.2 ms | 2.7 ms | 25.1 ms | 250 ms |
| 1.0 ms | 11.7 ms | 112 ms | 1,125 ms |
| 5.0 ms | 52.7 ms | 551 ms | 5,420 ms |

macOS sleeps overshoot short intervals, so the confirmed rows are an upper bound on the round trip
alone. SPEC-090's batches of up to 1,000 events pay the last column.

**What the prototype could not settle.** The real broker's confirm and return behaviour, which the
integration run owes (FR-004). What a real proxy or emulator returns for SQS, SNS, Kinesis or `_bulk`,
and whether a real SNS reply can omit `Successful`, which the `Failed`-only reading would then read as
1.x did. Whether the check sees a FIN and a reset the same way on Linux and Windows as on macOS, where
it was measured: CI runs on Ubuntu only (§12, finding 23), and FR-001's socketpair tests are what it
runs. And how often a real network delivers a reset after the end-of-round check, which loopback
cannot show.

## Revision history

- Drafted on 2026-11-15, and split from SPEC-094 at the delivery engine: SPEC-094 moves `KafkaSink`
  and `GooglePubSubSink` onto it, and this spec changes no sink's path through the engine.
- Revised the same day, after the fresh-context spec review and three answers.
  - **Andrew, 2026-11-15, on RabbitMQ's default: "Off, opt in".** `confirm=False` is the default,
    the default variants keep 1.x's rows, their `peer-closed` rows are now the `publish-unconfirmed`
    departure, and the confirmed variants are new. Rows went from 263 to 268, pinned from 61 to 62,
    departure rows from 13 to 15, and must-pass from 189 to 191.
  - **Andrew, 2026-11-15, on Sentry: "Dropped, with a line (Recommended)".** FR-002's counting is his
    choice. Beside it stands the precedent the other way, which he was shown. `SentrySink.losses()`
    says that "reporting a configured filter as loss would make the alert idiom fire on every INFO
    log" (`sentry.py:394-397`), and `FilteringSink` counts the events it declines as `SKIPPED`,
    counted nowhere. He was told that deliberate sampling and `before_send` drops would show up as
    counted loss, with a line, and chose it.
  - **Decided in session, not Andrew's call.** The `_bulk` extension follows from decision A's
    principle, that a reply which names no verdict is never counted as delivered, and refines that
    fence's last sentence (FR-006). The release's number, 1.2.1 or 1.3.0, is chosen when it is
    tagged, and FR-006 rewrites the pending file's title and opener so that nothing in it depends on
    the number.
  - **The review's findings, each fixed.**
    - An idle-dropped confirmed connection read as `UNKNOWN`. Fixed by FR-004's liveness probe on an
      owned connection, with the borrowed one recorded as a departure, since probing it would run the
      caller's callbacks.
    - The integration test for `mandatory` is now owed, with its re-plant check (FR-004).
    - A FIN counted at the end of a round reported a half-closing peer's delivered frames as
      may-have-landed. Fixed by reading only a reset there (FR-001), after the probe showed that the
      reset catches what the FIN caught; both residual windows are in §13 (FR-006).
    - The release notes name where `dropped_unadjudicated` sits and that Sentry sampling moves
      `dropped`, the README states the former, and the file's title and opener are rewritten.
    - The confirm cost at 5 ms is measured, including the batch that outlasts `flush()`.
    - SPEC-094 depends on this spec, and SPEC-092's sentences that named "the acknowledgement-fixes
      spec" name SPEC-093 or SPEC-094, recorded in SPEC-092's Revision history.
    - The Evidence no longer reads as if SPEC-094's sinks were in this spec's measured stack.
    - FR-002 states the mixed batch, which returns, and no longer says nothing of a raised batch is
      counted, since `transport_errors` is.
    - The poll and select costs and the `MSG_DONTWAIT` wait are measured by the probe itself.
    - FR-006 serves no invariant, and FR-005's gate criterion no longer leans on a SPEC-092 fixture.
    - `Variant.adds` combines with `only` and `skips` as FR-005 states, and `adds_faults` reports its
      misuse.
    - The unread-reply line carries decision A's ending.
    - A `confirm_delivery()` that raises no longer leaks its channel.
