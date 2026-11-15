---
type: Spec
id: SPEC-093
title: "Acknowledgement Fixes: a Write the Sink Cannot Confirm Is Counted, Not Believed"
description: "Makes the TCP socket sinks, SentrySink's SDK backend, the AWS batch sinks, the _bulk sinks and RabbitMQSink stop reporting as delivered what their backend never confirmed, each counting and announcing it as lost or as one that may have landed, so the rows SPEC-092's conformance corpus pins for findings 1, 11 and 14 and for RabbitMQ's missing confirms turn green and no other row moves."
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
each one, and SPEC-092's conformance corpus pins each one red. This spec makes each sink report what
it can actually know. An event is landed only when something confirmed it. It is lost when the
backend refused it. It is ambiguous, counted and announced as one that may have landed and never sent
again, when the sink cannot tell. Andrew decided two of the rules on 2026-11-15, and this spec builds
one of them. The other, a client-buffered sink waiting for its delivery report, is SPEC-094, the
arc's next spec, because it moves `KafkaSink` and `GooglePubSubSink` onto the delivery engine.

## Scope

### In Scope

- A TCP socket transport that checks its held connection before each round and after it, and calls
  every frame written into a connection later found closed `UNKNOWN` (FR-001).
- `SentrySink`'s SDK backend reading what `capture_event` returns, so a discarded event is counted
  as dropped rather than as sent (FR-002).
- Andrew's decision A for the AWS batch replies that name no event, and the same reading for a `_bulk`
  object that carries neither `errors` nor `items` (FR-003).
- `RabbitMQSink` publisher confirms, on by default for an owned and a borrowed connection alike, with
  a `confirm=False` opt-out (FR-004).
- The conformance corpus's classes, doubles, pins and departures for these sinks (FR-005).
- The record: `architecture.md` §12 and §13, the decisions, the invariants page, the README and the
  pending release notes (FR-006).

### Out of Scope

- **`KafkaSink` and `GooglePubSubSink` waiting for their delivery reports** (finding 12, Andrew's
  decision B). That is SPEC-094, the arc's next spec, which moves both sinks onto the delivery engine.
- **Idempotency keys and a count of the sends that may have duplicated** (finding 5). That is a later
  spec of this arc. FR-004 turns two finding-5 rows green as a side effect, because a confirmed publish
  whose confirm was lost is `UNKNOWN` and is not sent again. It adds no key and no counter.
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
  (FR-004, FR-006).

---

## Functional Requirements

Every premise below was read at `33582484`, and every measurement was taken there by the prototype
under *Evidence*. Two branches that touched `src/log_foundry/sinks/` were open while this spec was
written: `claude/driver-error-rework-c373af` (`sinks/_driver_error.py` and the sinks that call
`delivery_error`) and `claude/isinstance-guard-9eeb88` (an AST sweep of every type test). Both
landed before it was committed, as `8bd0b54a` and `bb6656ff`. They change no line this spec cites,
and every `file:line` here was re-read at `bb6656ff`. The prototype was rebased onto `bb6656ff`, and
there its corpus verdicts are identical. **The build re-verifies every `file:line` against `main` as
it stands.** No fix here calls `delivery_error`, so this spec assumes no definition of it, current or
earlier. Every type test the fixes add is `_diag.is_a`, as *A type test reads the real type, never
the value's __class__* (`docs/decisions/failure-paths/`) requires, and `tests/test_class_check_roster.py`
holds that.

**Andrew's decision A, made on 2026-11-15**, binds FR-003. He was asked whether an AWS batch reply
that names no failure should read as delivered. The examples were `{}`, a `Successful` list with no
`Failed`, and a Kinesis or Firehose reply with neither a count nor records. He answered
**"Unadjudicated (Recommended)"**: every event the reply does not name is `UNKNOWN`, counted in
`failed` and `dropped_unadjudicated`, announced once as one that may have landed, and never resent.
That matches the existing rule for an unreadable `2xx` on `_bulk`, Honeycomb and OTLP.

### FR-001: A TCP socket transport never calls a frame taken by a connection it can see is closed

#### Description:

**Today.** `SocketTransport._round` marks a frame `ACCEPTED` as soon as `sendall` returns
(`sinks/_socket.py:404-408`, the `sendall` at `:517`). The class documents `failed` as "Messages not
confirmed" (`:157`), and nothing is ever confirmed. After the peer closes, the kernel takes the next
frames anyway and they go nowhere. `probe_socket.py` at `33582484` lost 4 of 50 from `SyslogSink` and
8 of 50 from `LogstashSink`, with `failed=0` and no line. The audit recorded 2 to 25.

**What the socket can know.** `spec093_socket_options.py` measures five options with a frame loop
shaped like `_round`, against a loopback listener (*Evidence*). A check between batches finds
every close the peer made between them. A check before every frame costs about 0.4 µs against a
5 µs `sendall`, and it still let up to 33 of 50 frames vanish when the peer closed mid-batch. Calling
the frames written since the last check `UNKNOWN` when a later write raises catches a mid-batch
close, but only when a later write does raise. Only the combination below reported no frame accepted that the listener did
not receive, in every scenario and every run.

**The rule, for `transport="tcp"` only.**

1. **Before a round's first frame**, a held connection is checked by `_closed_by_peer(sock)`, a
   module-level seam beside `_make_tcp`. The check is a zero-timeout readiness test,
   `select.select([sock], [], [], 0)`, falling back to `select.poll` where `select.select` refuses
   the descriptor with `ValueError` (one above `FD_SETSIZE`). A fresh `select.poll` object per check
   was measured at 21 to 22 µs against 0.4 to 0.5 µs for `select.select`, under the same load. When
   the socket is readable, the check then calls `recv(1, socket.MSG_PEEK)`. Readable with nothing to
   read is a FIN, and an `OSError` from either call is a reset. Either way the check answers closed, and the round calls `_reset()` before
   it writes, so its frames go on a new connection and nothing is counted. The check does not use
   `MSG_DONTWAIT`. Measured on a socket with a timeout, which `create_connection(timeout=5)` gives,
   that call waited the whole timeout (5.00 s) before it raised. A check that raises anything other
   than `OSError` answers open, so an object that is not a real socket keeps 1.x's behaviour.
2. **The frames written on the current connection since its last check that answered open** are
   unconfirmed. A connection opened inside the round counts as checked when it opened. When a later
   write on that connection raises, the unconfirmed frames become `UNKNOWN`. The raising frame keeps
   today's outcome, `RETRYABLE` or `REJECTED` as `_retryable_error` answers, and so it is still sent
   again on a new connection. That resend is finding 5, which is out of scope.
3. **After the round's last frame**, the connection is checked again. When the check answers closed,
   the round's unconfirmed frames become `UNKNOWN` and the connection is reset.
4. Each group of `UNKNOWN` frames is announced once, `ALWAYS`, with the line `lost N message(s);
   SocketTransport, written to a connection its peer closed; may have landed, not retried`. The
   engine counts them in `failed_events`, and they are never sent again.

**What stays unknowable.** A peer can close with frames the kernel took and it never read, and if it
does so after the end-of-round check, the next round's check finds the close only after those frames
were reported. No socket call can see that window. FR-006 records it in §13.

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
- [ ] With the close seen only by the end-of-round check, every frame of that round written on the
      connection is `UNKNOWN`, counted and named on one line, and the next round opens a new
      connection (invariants 2 and 7).
- [ ] A held socket whose check raises `TypeError` answers open, and its frames are `ACCEPTED` as
      in 1.x (invariant 1).
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

**The rule.** On the SDK branch, `_capture` returns a third answer, *discarded*, when `capture_event`
returns `None`, and the event moves no other counter.

- **Count.** A new attribute, `discarded`, counts these events, and `losses()` reports it as
  `dropped`. The count is made through the `emit`'s own `Deferral`, so it lands only when `emit`
  returns.
- **Announce.** Each `emit` that discarded anything writes one line: `lost N event(s); SentrySink,
  discarded by the Sentry SDK at capture`.
- **Raise.** A discarded event is not one the batch failed to deliver. A batch whose every qualifying
  event was discarded returns. It is not raised and sent again, because a resend would roll the
  sampler again and would hand `before_send` an event it had already dropped. A batch with no event
  sent, no event discarded and at least one failure raises as before. When a batch with discards
  raises, nothing of it is counted, and the catcher owns the batch.

Any value other than `None` reads as captured, as in 1.x. The HTTP fallback is unchanged.

**A caller sees** a stand-in `client=` whose `capture_event` returns nothing read as discarding every
event: `sent` stays 0, `losses().dropped` counts them, and one line is written per batch. A caller who
samples errors sees the sampled-out events in `dropped`, with a line. The library cannot tell sampling
from an SDK that cannot deliver, because both return `None`.

#### Acceptance Criteria:

- [ ] `SentrySink[sdk]-never-acknowledged` reads green, and `SentrySink[sdk]-report-indefinite` keeps
      the `sdk-transport-drop` departure (invariants 2 and 7).
- [ ] `probe_misc.py` case 5 against the branch reads `sink.sent=5`, `losses: failed=0 dropped=20` and
      one line (invariants 2 and 11).
- [ ] A batch of nothing but discarded events returns, moves `discarded` by its size, and is emitted
      once through the worker (invariant 7).
- [ ] A batch whose events are discarded or raise, with none sent, raises `SinkDeliveryError` and
      leaves `discarded` unchanged (invariant 2).
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
  because `failed` is the engine's `failed_events`, which already holds every `UNKNOWN` event. A call
  that was entirely unread already wrote its line. An entry that was not named gets the line `lost N
  message(s); <Class>, a <call> reply named neither success nor failure for N entry(ies); may have
  landed, not retried`. Both are `ALWAYS`, and neither is ever sent again. The replies that were
  already unread at `33582484`, one that is not a mapping and a `Failed` list the discipline refuses,
  now move `dropped_unadjudicated` as well.

**The positional rule, `KinesisSink` and `FirehoseSink`.** `adjudicate_positional` gains a keyword,
`absent: bool = False`, meaning that the reply carried no per-record array at all. `settle_positional`
passes `absent=raw is None`. A reply with no failed count and no array is unadjudicated: its chunk is
`UNKNOWN`, counted on `dropped_unadjudicated` with 1.x's line. Every other combination reads as before.
A `0` count with no array stays delivered (SPEC-070), and a direct caller that does not pass `absent`
gets SPEC-070's reading unchanged.

**The `_bulk` rule, `ElasticsearchSink` and `OpenSearchSink`.** In `_outcomes`, a `2xx` object that
carries neither `errors` nor `items` cannot have adjudicated any item, so it goes through
`HTTPSink._unadjudicated`. That is the same path an undecodable body takes (`elasticsearch.py:191`):
`dropped_unadjudicated`, a may-have-landed line, and no resend. `{}` and an error document such as
`{"error": {...}, "status": 500}` take this path. An object with `errors` absent or false beside
`items`, and `{"errors": false}`, still read as success, unverified. That keeps the §12 item's reason:
an endpoint that omits only `errors` must not be charged with every success. Andrew's answer covered
AWS. This extension is his call (see *Revision history*), and it is specified here as recommended.

**A caller sees** counts and a line where 1.x showed silence, for replies only a proxy, an emulator or
a broken stand-in returns. A test stand-in that returns `{}` for SQS, SNS, Kinesis, Firehose or `_bulk`
now reads as unadjudicated.

#### Acceptance Criteria:

- [ ] The `no-verdict-shape` rows of `SQSSink[standard]`, `SQSSink[fifo]`, `SNSSink`, `KinesisSink`,
      `FirehoseSink`, `ElasticsearchSink` and `OpenSearchSink` read green (invariants 2 and 7).
- [ ] `probe_sqs.py` against the branch reads `failed=6`, `uncounted_missing=0` for G1 and `failed=25`,
      `uncounted_missing=0` for G2. `probe_aws_more.py` reads `failed=25` with one line for case 2,
      and `failed=6` for case 3 (invariants 2 and 11).
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

### FR-004: `RabbitMQSink` asks the broker to confirm each publish, by default

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
FR-005), or on a socket error.

**The rule.**

- **The constructor.** `RabbitMQSink.__init__` gains a keyword-only `confirm: bool = True`, after
  `stack_timeout`.
- **The channel.** When `confirm` is true, `_active_channel` calls `confirm_delivery()` on each
  channel it opens, before the first publish on it. The method is probed by name, as `SentrySink`
  probes `flush`, so a stand-in channel that lacks it publishes unconfirmed, as in 1.x. A
  `confirm_delivery()` that raises is a failure to get a channel, judged by the predicate as today.
  On a channel in confirm mode, `basic_publish` passes `mandatory=True`.
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
- **`confirm=False`** is 1.x exactly, and its class docstring says what a returned publish proves.

**Owned and borrowed alike.** The brief for this spec asked whether confirms on a borrowed channel
would change the caller's channel state. The sink never borrows a channel. It borrows a connection,
and it opens its own channel on that connection (`rabbitmq.py:500`). AMQP's `confirm.select` is
scoped to one channel, so the caller's own channels are untouched. `_flush_output` dispatches no other
channel's consumer callbacks while it waits (`blocking_connection.py:512-556`). So confirms apply to
both variants, and no departure is needed.

**The cost** is a broker round trip per message, because a blocking channel cannot pipeline
confirms. `spec093_rabbitmq_confirm_cost.py` measured one `emit` of 1,000 events at 0.31 s with a
0.2 ms round trip and 1.09 s with a 1 ms one. Without confirms it took 2.1 to 2.3 ms. That is about
3,200 and 900 events per second, against about 450,000 (*Evidence*). It is the reason `confirm=False`
exists, and whether confirms are the default is Andrew's call (see *Revision history*).

#### Acceptance Criteria:

- [ ] `RabbitMQSink[owned]` and `RabbitMQSink[borrowed]` read green on every row the corpus runs for
      them, `taken-then-raised` and `peer-closed` included (invariants 2 and 7).
- [ ] `RabbitMQSink[unconfirmed]` reads `duplicate` on `taken-then-raised`, under the `reply-resent`
      pin, and `silent-loss` on `peer-closed`, under the `publish-unconfirmed` departure (invariants
      2 and 7).
- [ ] With a stand-in channel that records calls, a confirmed sink calls `confirm_delivery()` once per
      channel it opens and passes `mandatory=True` on every publish. A `confirm=False` sink calls
      neither (invariant 10).
- [ ] `UnroutableError` and `NackError` each make the message `REJECTED`, publish it once, and keep
      the channel. `StreamLostError` raised by a confirming publish makes it `UNKNOWN`, publishes it
      once, and reconnects for the next message (invariants 2 and 7).
- [ ] A stand-in channel without `confirm_delivery` publishes and is `ACCEPTED`, as in 1.x (invariant
      10).
- [ ] `inspect.signature(RabbitMQSink)` equals the `33582484` signature plus a keyword-only `confirm`
      with default `True`, as the last parameter (invariant 10).
- [ ] **Owed until a green run:** `integration.yml`'s RabbitMQ test passes on the PR's branch with
      confirms on, against its real broker (invariants 2 and 7).

### FR-005: The conformance corpus holds what these sinks can now know

#### Description:

These changes are to SPEC-092's files under `tests/`, as its build delivers them. The tables in
SPEC-092's FR-001, FR-002 and FR-004 are the baseline.

- **Classes.** `RabbitMQSink` `owned` and `borrowed` move from `socket-stream` to `per-event-ack`, as
  SPEC-092 FR-001 foresaw. `Variant` gains `adds: Mapping[Fault, str]`, which lists the faults a
  variant runs beyond its class's, each with a reason. Both RabbitMQ variants add `taken-then-raised`
  ("a confirmed publish writes its frame before it waits") and `peer-closed` ("a connection is held
  between batches"). A new variant, `RabbitMQSink[unconfirmed]`, is in `socket-stream`. It is built
  owned with `picks={"confirm": False}`, and `PARAMETERS` gains `confirm` for `RabbitMQSink`. That
  gives 54 variants and 263 rows: the 256, plus `reply-lost` for the two confirmed variants, plus the
  unconfirmed variant's five rows.
- **The RabbitMQ double.** `RabbitChannel` gains `confirm_delivery()` and a `mandatory` keyword.
  On a confirming channel:
  - a refusal raises `ChannelWrongStateError` before anything is taken, which is what pika raises for
    a channel whose connection is gone;
  - `taken-then-raised` and `reply-lost` take the frame and then raise `StreamLostError`;
  - the second batch's first write after `peer-closed` raises `StreamLostError` without taking
    anything, as an unknowable call.

  An unconfirmed channel behaves as SPEC-092's double does. `DRIVERS["pika.exceptions"]` gains
  `UnroutableError`, `NackError` and `ChannelClosedByBroker`.
- **Pins.** These pins are removed because every row in them reads green: `reply-without-verdict`,
  `bulk-without-errors`, `peer-closed-tcp`, `publish-without-confirms` and `sdk-discard-counted-sent`.
  `reply-resent` loses `RabbitMQSink[owned]-taken-then-raised` and gains
  `RabbitMQSink[unconfirmed]-taken-then-raised`, which leaves it at 50 rows. `reply-counted-lost` loses
  `RabbitMQSink[borrowed]-taken-then-raised`, which leaves it at 3.
- **Departure.** A new one, `publish-unconfirmed`, holds `RabbitMQSink[unconfirmed]-peer-closed` at
  `silent-loss`. Its record is `src/log_foundry/sinks/rabbitmq.py` and the phrase "With
  `confirm=False` a publish that returned is all this sink can know", which the class docstring
  carries.
- **Totals.** 263 rows: 61 pinned under seven pins, 13 departure rows under eight departures, and
  189 that must pass. FR-004 of SPEC-092 asserts its totals as literals, and those literals change to
  these numbers.

#### Acceptance Criteria:

- [ ] The corpus generates 263 rows over 54 variants, and its literals read 263 rows, 61 pinned, 13
      departure rows and 189 that must pass (invariants 2 and 7).
- [ ] Against this spec's verdicts, run on SPEC-092's delivered corpus at the branch, only the rows
      the criteria of FR-001 to FR-004 name change verdict. Every other row reads as it read before
      this spec's `src/` change (invariants 2 and 7).
- [ ] `register_faults` returns no line, and SPEC-092 FR-004's standing fixture for a misspelt pin
      item still reports it. A new standing fixture in `tests/test_conformance_gate.py`, a variant
      whose `adds` names a fault its class already runs, fails naming that variant and that fault
      (invariants 2 and 7).
- [ ] With FR-001's second rule removed, so that a later raise leaves the frames before it
      `ACCEPTED`, in an export of the branch head, `SyslogSink[tcp]-peer-closed` and
      `LogstashSink[tcp]-peer-closed` read `silent-loss` again. The corpus's fake socket cannot be
      polled, so its check answers open and the corpus reaches only that rule. The two checks are
      held by FR-001's socketpair criteria, and with `_closed_by_peer` answering `False` the corpus
      rows stay green (invariant 2).
- [ ] With `confirm_delivery` never called, in an export of the branch head, both confirmed
      variants' `peer-closed` rows read `silent-loss` again, and their `taken-then-raised` rows read
      `duplicate` for `owned` and `lost-but-held` for `borrowed` (invariant 2).

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
  - The RabbitMQ item ("`RabbitMQSink` publishes without confirms…"). Its closure names the
    integration run that FR-004 owes.
- **§12, rewritten in place and dated.**
  - The `ElasticsearchSink` item ("`ElasticsearchSink` reads a `2xx` object without a truthy
    `errors` as success, unverified.") keeps only the object with `items` and no truthy `errors`,
    and says that `{}` and an error document are counted since SPEC-093.
  - The finding-5 item gains one sentence: a confirmed `RabbitMQSink` publish whose confirm was lost
    is `UNKNOWN` and is not resent, and `confirm=False` keeps the resend.
- **§13 gains three bullets.**
  - "**A TCP frame the kernel took is unconfirmed until the transport's next check.**" A peer that
    closes after the end-of-round check, with frames it never read, loses them as delivered.
  - "**A confirmed `RabbitMQSink` publish costs a broker round trip.**" The bullet carries the
    measured figures.
  - "**A confirm wait is bounded only by the connection.**" The wait ends on the heartbeat,
    `blocked_connection_timeout` or a socket error. It is an instance of invariant 3's sink blocked
    inside a network call.
- **Decisions in `docs/decisions/sink-delivery/`, each one file with its fence, entry and dated
  History.**
  - A new decision for FR-001's rule.
  - A new decision for FR-002's rule.
  - A new decision for decision A, which quotes Andrew's question and answer with the date, together
    with FR-003's `_bulk` extension and the `Failed`-only reading.
  - A new decision for FR-004's rule, with the default and the opt-out.
- **Two settled decisions refined in place, each with a dated History entry naming SPEC-093.**
  - *A positional response adjudicates all of a chunk or none of it*: a reply that carries none of
    its verdict fields adjudicates none of its chunk, and in a keyed reply that lists `Successful`,
    an entry named in neither list is `UNKNOWN`.
  - *A 2xx _bulk response with no readable verdict is counted, never retried*: an object with
    neither `errors` nor `items` is counted.
- **`docs/invariants.md`.** Invariant 7's recorded exceptions gain a `SentrySink` batch of nothing
  but discarded events, which returns, counted. The *Guarded* lines of invariants 2 and 7 name the
  new tests.
- **README.**
  - The `SyslogSink` and `LogstashSink` rows (`README.md:783-784`) gain the TCP check and the
    may-have-landed line.
  - The `SentrySink` paragraph (`:842-845`) says a discarded event is in `losses().dropped`.
  - The AWS paragraphs (`:870-880`) state decision A's reading and the new `dropped_unadjudicated` on
    `SQSSink` and `SNSSink`.
  - The `ElasticsearchSink` row (`:779`) states FR-003's `_bulk` reading.
  - The queue-and-stream paragraph (`:956-958`) and the `RabbitMQSink` row (`:1001`) state confirms,
    `confirm=False` and the cost.
- **The release notes.** In the pending `docs/release-notes/v1.2.1.md` if it is on `main` when this
  spec completes, and under a "Release notes owed" heading in the delivery doc otherwise, as SPEC-090
  words it. The entry lists:
  - the TCP line and its `UNKNOWN` counts;
  - `SentrySink.discarded`, its line, and the stand-in returning `None`;
  - decision A's counts and lines, the stand-in returning `{}`, and the new attributes;
  - the `_bulk` reading of an object with neither key;
  - `RabbitMQSink(confirm=)`, `mandatory`, `UnroutableError` and `NackError` as rejections, the
    `UNKNOWN` lost confirm, and the per-message round trip.

  Its compatibility list names each of these for a `>=1,<2` pin, as the file's opening list does.
- **`docs/component-inventory.md`.** No row is owed, because no module is added.

#### Acceptance Criteria:

- [ ] `docs/architecture.md` §12 strikes the four items and rewrites the two named above, each dated
      and naming SPEC-093, and §13 carries the three bullets (invariant 2).
- [ ] The four new decision files and the two dated History entries exist, and the decision-A file
      quotes "Unadjudicated (Recommended)" with the date 2026-11-15 (invariant 7).
- [ ] Invariant 7's recorded exceptions name a `SentrySink` batch of nothing but discarded events
      (invariant 7).
- [ ] Each README site above states the new behaviour, and nothing in the README says a TCP frame,
      a Sentry capture, an AWS reply or a RabbitMQ publish is delivered on less than this spec
      requires (invariant 2).
- [ ] The release-notes entry exists where the description says, and names each change listed
      (invariant 10).

---

## Data Model

```python
# src/log_foundry/sinks/_socket.py
def _closed_by_peer(sock: socket.socket) -> bool: ...   # seam beside _make_tcp; True on FIN/reset

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
                 confirm: bool = True) -> None: ...   # new keyword, last

# tests/conformance_classes.py (SPEC-092)
@dataclass(frozen=True, kw_only=True)
class Variant:
    ...
    adds: Mapping[Fault, str] = MappingProxyType({})   # new: faults beyond the class's
```

## File & Folder Structure

```
src/log_foundry/sinks/
├── _socket.py        # FR-001: _closed_by_peer, the round's checks and UNKNOWN frames
├── sentry.py         # FR-002: discarded, the Deferral in emit
├── _rounds.py        # FR-003: read_keyed reads Successful; settle_keyed counts the unnamed
├── _batch.py         # FR-003: adjudicate_positional(absent=)
├── sqs.py, sns.py    # FR-003: dropped_unadjudicated
├── elasticsearch.py  # FR-003: an object with neither errors nor items
└── rabbitmq.py       # FR-004: confirm=, confirm_delivery, mandatory, the outcome table
tests/
├── conformance_classes.py, conformance_doubles.py, conformance_pins.py   # FR-005
└── test_*.py         # the new tests, and the 25 that change (Evidence)
docs/                 # FR-006
```

## Implementation Phases

### Phase 1: The reply readers (FR-003)

- `read_keyed`, `settle_keyed`, `adjudicate_positional(absent=)`, `settle_positional`, the `_bulk`
  reader, and the two new attributes, with their tests.

### Phase 2: The writers (FR-001, FR-002, FR-004)

- The socket checks and `UNKNOWN` frames, `SentrySink.discarded`, and `RabbitMQSink` confirms, with
  their tests and the integration run.

### Phase 3: The corpus and the record (FR-005, FR-006)

- SPEC-092's classes, doubles, pins and departure, and the record.

## Evidence

Every number was taken at `33582484` on macOS with Python 3.13.1 and no extra installed, at a
one-minute load between 6 and 8, unless it says otherwise. The prototype is
`docs/audits/2026-11-14-probes/sinks/spec093_prototype.patch`. It applies with `patch -p1` to
`git archive bb6656ff src tests docs/audits/2026-11-14-probes`, and it carries this spec's `src/`
change and SPEC-092's prototype corpus edited as FR-005 says. SPEC-092 is not built at `33582484`, so
its prototype (`spec092_run.py`) stands in for its corpus. The corpus's `ledger` check does not run,
and the build re-measures against the delivered corpus.

**The rows.** Run against the prototype corpus at `33582484`, `src/` unchanged, 256 rows read 170
green and 86 red, which is SPEC-092's record. With the prototype's `src/`, 263 rows read 189 green and
74 red. That is the same in three runs on Python 3.13.1 and one on Python 3.12.12, each exiting 0,
from the committed patch applied to a fresh export. The rows that move are exactly these:

- the `no-verdict-shape` rows of `SQSSink[standard]`, `SQSSink[fifo]`, `SNSSink`, `KinesisSink`,
  `FirehoseSink`, `ElasticsearchSink` and `OpenSearchSink`, from `silent-loss` to green;
- `SyslogSink[tcp]-peer-closed` and `LogstashSink[tcp]-peer-closed`, from `silent-loss` to green;
- `SentrySink[sdk]-never-acknowledged`, from `silent-loss` to green;
- `RabbitMQSink[owned]` and `RabbitMQSink[borrowed]` `peer-closed`, from `silent-loss` to green;
- `RabbitMQSink[owned]-taken-then-raised`, from `duplicate` to green, and
  `RabbitMQSink[borrowed]-taken-then-raised`, from `lost-but-held` to green.

The new rows read: `reply-lost` of both confirmed variants green, and of `RabbitMQSink[unconfirmed]`
`clean`, `refused-once` and `refused-always` green, `taken-then-raised` `duplicate` and `peer-closed`
`silent-loss`. Each change was added on top of the ones before it, in this order: the socket change,
`SentrySink`, SPEC-094's two sinks, `RabbitMQSink` with its corpus edits, and the reply readers. Each
step moved only the rows of the sinks it changed, and the socket change alone moved exactly the two
TCP rows. Under the new table at `33582484`, the two confirmed `reply-lost` rows read green. That is truthful and proves nothing:
without confirms, no reply is lost. So the `taken-then-raised` rows of those variants are the ones that
show what confirms change.

**The suite.** The full suite was run against the prototype in a scratch export with no `docs/`. 43
tests fail there with or without the prototype, because they read files the export lacks. The
prototype fails 25 more, the same 25 at `33582484` and, rebased, at `bb6656ff`. Each is accounted
for:

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

An earlier prototype called `confirm_delivery` unconditionally and read every exception from a
confirming publish as `UNKNOWN`. It failed 41 RabbitMQ tests, whose doubles have no
`confirm_delivery`, and it read a stand-in's `TypeError` as a lost confirm. That is why FR-004 probes
the method by name and names the four classes. An earlier FR-003 read every SQS entry that
`Successful` did not name as `UNKNOWN`. It failed 9 more tests whose stand-ins return
`{"Failed": []}`, so FR-003 keeps the `Failed`-only reply. An earlier `_bulk` rule, "no `items`",
failed 4 more: the `[errors-false]`, `[errors-null]` and `[errors-zero]` rows of
`test_an_object_without_truthy_errors_is_read_as_success_unverified`, and
`test_sink_concurrency.py::test_elasticsearch_dropped_unadjudicated_is_counted_under_the_lock`. So
FR-003 takes only the object with neither key.

**The mutants.** Each was run on the rebased prototype's corpus.

- With FR-001's second rule removed, both TCP `peer-closed` rows read `silent-loss`.
- With `_closed_by_peer` answering `False`, they stay green, because the fake socket's check
  already answers open. The checks are held by FR-001's socketpair criteria, not by the corpus.
- With `confirm_delivery` never called, the confirmed variants' `peer-closed` rows read
  `silent-loss`, and their `taken-then-raised` rows read `duplicate` (`owned`) and `lost-but-held`
  (`borrowed`), as at `33582484`.

**The socket options.** `spec093_socket_options.py 5` was run once, at a one-minute load near 22.
The table gives silent loss out of the second batch, meaning frames reported accepted that the
listener did not receive, over the five runs:

| Scenario | today | check per round | check per frame | unknown on a raise | round + raise | round + raise + end |
|---|---|---|---|---|---|---|
| peer closes between batches, FIN | 5 to 7 | 0 | 0 | 0 (8 to 12 unknown) | 0 | 0 |
| peer closes between batches, RST | 0 | 0 | 0 | 0 | 0 | 0 |
| peer closes after 10 of 50, mid-batch | 1 to 7 | 1 to 5 | 0 to 33 | 0 | 0 | 0 |
| peer closes after 2 of 3, 1 ms between frames | 1 | 1 | 0 | 1 | 1 | 0 (3 unknown) |

Two earlier sessions of the same probe, at three runs each and a lower load, read 15 to 49 for
*today* in the first row and 1 to 39 for *check per round* in the third. The last column read 0 in
every scenario of every session. `select.select([sock], [], [], 0)` cost 0.38 to 0.49 µs per call and `sendall` of a
232-byte frame 4.8 to 5.6 µs, each in three runs. A per-frame check would add about 8% to a frame.
`probe_socket.py` against the prototype read `batch2 received=50/50`, `failed=0` and no line for
both sinks under FIN and RST, in five runs out of five.

**The reply probes.** These were measured against the prototype; the `33582484` figures are in each
FR's Description. `probe_sqs.py` read G1 `failed=6`, two lines, `uncounted_missing=0`, and G2
`failed=25`, three lines, `uncounted_missing=0`. Every other case read as at `33582484`.
`probe_aws_more.py` read case 2 `failed=25`, one line, and case 3 `failed=6`, two lines.
`probe_misc.py` case 5 read `sink.sent=5 … dropped=20`, one line.

**The confirm cost.** `spec093_rabbitmq_confirm_cost.py` measured the median `emit` of five, with a
fake channel that sleeps one round trip per confirmed publish:

| Round trip | batch 10 | batch 100 | batch 1,000 |
|---|---|---|---|
| none (`33582484`) | 0.04 to 0.05 ms | 0.23 ms | 2.1 to 2.3 ms |
| 0.2 ms | 3.1 ms | 30.8 ms | 309 ms |
| 1.0 ms | 15.0 ms | 149 ms | 1,090 ms |

macOS sleeps overshoot short intervals, so the confirmed rows are an upper bound on the round trip
alone. SPEC-090's batches of up to 1,000 events pay the 1,000 column.

**What the prototype could not settle.** The real broker's confirm and return behaviour, which the
integration run owes (FR-004). What a real proxy or emulator returns for SQS, SNS, Kinesis or `_bulk`,
and whether a real SNS reply can omit `Successful`, which the `Failed`-only reading would then read as
1.x did. And whether the check sees a FIN the same way on Linux and Windows as on macOS, where it was
measured: CI runs on Ubuntu only (§12, finding 23), and FR-001's socketpair tests are what it runs.

## Revision history

- Drafted on 2026-11-15. Three decisions here are Andrew's to make, and each is specified on its
  recommended side.
  - Whether confirms are `RabbitMQSink`'s default. Recommended and specified: on, with
    `confirm=False`. The alternatives are on with no opt-out, or off with opt-in.
  - Whether decision A's reading extends to a `_bulk` object with neither `errors` nor `items`.
    Recommended and specified: yes, for that object only.
  - Whether a `SentrySink` discard is counted as `dropped` with a line. Recommended and specified:
    yes. The alternative is a non-loss counter, which leaves its corpus row red as a departure.

  The work was split from SPEC-094 at the delivery engine: SPEC-094 moves `KafkaSink` and
  `GooglePubSubSink` onto it, and this spec changes no sink's path through the engine.
