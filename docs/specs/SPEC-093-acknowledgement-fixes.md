---
type: Spec
id: SPEC-093
title: "Acknowledgement Fixes: a Write the Sink Cannot Confirm Is Counted, Not Believed"
description: "Makes the TCP socket sinks, SentrySink's SDK backend, the AWS batch sinks and the _bulk sinks stop reporting as delivered what their backend never confirmed, and gives RabbitMQSink opt-in publisher confirms, so the rows SPEC-092's conformance corpus pins for findings 1, 11 and 14 turn green, RabbitMQ's default unconfirmed publish becomes a recorded departure, and no other row moves."
spec_status: Completed
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
as Andrew decided, and its default's unconfirmed publish becomes a recorded departure. The TCP
sinks' wait for the peer's acknowledgement is the other way round, on by default with an opt-out,
as he decided on 2026-11-16. Andrew decided four of these rules on 2026-11-15, and this spec builds
three of them. The fourth, a client-buffered
sink waiting for its delivery report, is SPEC-094, the arc's next spec, because it moves `KafkaSink`
and `GooglePubSubSink` onto the delivery engine.

## Scope

### In Scope

- A TCP socket transport that checks its held connection before each round and after it, waiting
  at the end for the peer's acknowledgement, and calls `UNKNOWN` every frame written into a
  connection that a later write or a reset shows was closed; and `ack_wait=False` on `SyslogSink`
  and `LogstashSink`, which turns the wait off (FR-001).
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
landed, as `8bd0b54a` and `bb6656ff`, and SPEC-086 and SPEC-088 then landed, up to `97a03f45`. None
of them changes a line this spec cites, and every `file:line` here was re-read at `97a03f45`. The
prototype was rebuilt on `97a03f45`, and the numbers under *Evidence* are from that tree unless they
say otherwise. **The build re-verifies every `file:line` against `main` as it stands.** No fix here calls
`delivery_error`, so this spec assumes no definition of it, current or earlier. Every type test the
fixes add is `_diag.is_a`, as *A type test reads the real type, never the value's __class__*
(`docs/decisions/failure-paths/`) requires, and `tests/test_class_check_roster.py` holds that.

**Andrew's decision A, made on 2026-11-15**, binds FR-003. He was asked whether an AWS batch reply
that names no failure should read as delivered. The examples were `{}`, a `Successful` list with no
`Failed`, and a Kinesis or Firehose reply with neither a count nor records. He answered
**"Unadjudicated (Recommended)"**: every event the reply does not name is `UNKNOWN`, counted in
`failed` and `dropped_unadjudicated`, announced once as one that may have landed, and never resent.
That matches the existing rule for an unreadable `2xx` on `_bulk`, Honeycomb and OTLP. Two more of
his answers that day bind FR-002 and FR-004, and a third, on 2026-11-16, binds FR-001's default;
*Revision history* quotes them.

### FR-001: A TCP socket transport never calls a frame taken by a connection it can see is closed

#### Description:

**Today.** `SocketTransport._round` marks a frame `ACCEPTED` as soon as `sendall` returns
(`sinks/_socket.py:404-408`, the `sendall` at `:517`). The class documents `failed` as "Messages not
confirmed" (`:157`), and nothing is ever confirmed. After the peer closes, the kernel takes the next
frames anyway and they go nowhere. `probe_socket.py` at `33582484` lost 4 of 50 from `SyslogSink` and
8 of 50 from `LogstashSink`, with `failed=0` and no line. The audit recorded 2 to 25. In Docker
`python:3.12-slim` at `97a03f45` it lost 1 of 50 from each.

**How the close rule was chosen: by a corpus driven through the real transport, under delay.** Three
earlier rules were chosen by reasoning or by a corpus that slept before each check, and each was
wrong where it had not been run. Counting on a FIN reported a half-closing peer's delivered frames
as may-have-landed. Counting only on a peek that raises fell silent on Linux, where a FIN followed by
the RST our later write draws still peeks as a FIN. Reading `SO_ERROR` at once after the round was
right on idle Linux loopback, where the RST our frames draw is usually back before the check runs,
and wrong everywhere else, a loaded Linux runner included (*Revision history*, 2026-11-22): the execution-frame review measured it right in 15 of 20 runs on macOS loopback,
and silent under 1 ms of `tc netem` delay, because the reset landed after the round's check and the
next round's check discarded it. So the rule is now the one that
`spec093_close_corpus.py` shows right when it drives `LogstashSink` in TCP mode, through the real
`SocketTransport`, with no sleep before any check, on macOS and in Docker `python:3.12-slim` on
loopback and under 1 ms and 10 ms of added delay each way (*Evidence*). It arranges each scenario
through the transport's two seams, `_peer_state` before the round and `_end_state` after it:

| Scenario | True outcome | Expected verdict |
|---|---|---|
| clean: the peer reads everything and stays open | all received | silent |
| half-close: the peer shuts down its write side and reads everything, 3 rounds | all received | silent |
| the peer closes (FIN) after the pre-check, before 1 or 5 frames reach it | the frames lost | counted |
| the peer resets, frames unread, just before the end check | the frames lost | counted |
| the peer closes (FIN) after reading everything, just before the end check | all received | silent |
| the peer resets after the end check, 2 rounds | the first round lost | first round uncounted, second right |
| the peer closes or resets mid-round, after 2,000 bytes of 1,000 frames | some lost | no loss uncounted |

**Why the end check waits for the acknowledgement.** A peer that closed before our frames reached it
answers them with a reset, never with an acknowledgement. After a FIN and the RST our 6-byte frame
drew, the send queue read 6 bytes unacknowledged in every read for the half second the probe
watched, on Linux and on macOS, while `SO_ERROR` read the reset once and 0 after; on macOS that
once came 55 ms after the write (*Evidence*). A peer that is open or half-closed
acknowledges them. So waiting until every byte written is acknowledged or a reset arrives decides the
round on the peer kernel's answer to the round's own bytes, whenever that answer lands, and a check
that does not wait decides it on whatever has landed by then. Waiting only once a FIN is visible, the
review's candidate, was measured too: under delay the FIN itself had not landed by the end check, so
it waited for nothing and stayed silent in every run.

**The rule, for `transport="tcp"` only.**

1. **`_peer_state(sock)`**, a module-level seam beside `_make_tcp`, answers `"open"`, `"fin"` or
   `"reset"`:
   - `getsockopt(SOL_SOCKET, SO_ERROR)` holding `ECONNRESET`, `EPIPE`, `ECONNABORTED` or `ETIMEDOUT`
     (`_RESET_ERRNOS`) is `"reset"`. Any other pending error is not: Linux reports a soft ICMP error,
     such as `EHOSTUNREACH`, there on a connection that is still alive, and it reads as `"open"`.
     Reading `SO_ERROR` clears it, so this function is its only reader in the transport, and a reset
     it reads is acted on by the caller that read it;
   - otherwise a zero-timeout readiness test, `select.select([sock], [], [], 0)`, falling back to
     `select.poll` where `select.select` refuses the descriptor with `ValueError` (one above
     `FD_SETSIZE`), then, when readable, `recv(1, socket.MSG_PEEK)`: empty is `"fin"`, data is
     `"open"`;
   - an `OSError` from any of these is `"reset"`, and any other exception is `"open"`, so an
     object that is not a real socket keeps 1.x's behaviour.

   It never uses `MSG_DONTWAIT`: on a socket with a timeout, which `create_connection(timeout=…)`
   gives, CPython waits the whole timeout before that call returns, measured at 1.00 s of a 1.0 s
   timeout. A fresh `select.poll` object costs 21 to 41 µs, against 0.4 to 0.6 µs for
   `select.select`.
2. **`_unacked(sock)`**, a second seam, answers the bytes the peer's kernel has not acknowledged:
   `ioctl(SIOCOUTQ)` (`termios.TIOCOUTQ`) on Linux and `getsockopt(SOL_SOCKET, SO_NWRITE)` (`0x1024`)
   on macOS. Any exception, or a platform with neither, answers `None`.
3. **Before a round's first frame**, a held connection whose state is not `"open"` is replaced
   (`_reset()`), so the round's frames go on a new connection and nothing is counted. A
   half-closing peer therefore costs a new connection per round and loses nothing.
4. **The frames written on the current connection since its last check that answered `"open"`**
   are unconfirmed. A connection opened inside the round counts as checked when it opened. When a
   later write on that connection raises, the unconfirmed frames become `UNKNOWN`. The raising
   frame keeps today's outcome, `RETRYABLE` or `REJECTED` as `_retryable_error` answers, and so it
   is still sent again on a new connection. That resend is finding 5, which is out of scope.
5. **After the round's last frame, `_end_state(sock)`** reads `_peer_state`, and returns at once on
   `"reset"`. Otherwise it reads again every `_RESET_STEP` (1 ms) until `_unacked` answers 0 or
   `None`, or `_RESET_WAIT` (0.25 s) has passed since it began, and returns the last state read. The
   pause between reads is `_retry.wait(_RESET_STEP)`, so the two gates that hold SPEC-074 FR-006's
   rule, that only the engine waits, see it: `_socket` joins `tests/test_sink_retry.py`'s
   `WAIT_EXCLUSIONS` with the reason "it waits for the peer's acknowledgement of its own frames,
   which is not a retry backoff", and `tests/test_engine_sinks_roster.py`'s finding that an engine
   sink's module calls `wait` skips a module `WAIT_EXCLUSIONS` names, so the two gates read one
   list. A pause on a private `threading.Event` would have passed both gates unseen, which is why
   the pause goes through the helper they watch. `"reset"` makes the round's unconfirmed frames
   `UNKNOWN` and replaces the connection. `"fin"` and `"open"` change nothing in this round, and a
   `"fin"` is replaced by the next round's first check.
6. Each group of `UNKNOWN` frames is announced once, `ALWAYS`, with the line `lost N message(s);
   SocketTransport, written to a connection its peer closed; may have landed, not retried`. The
   engine counts them in `failed_events`, and they are never sent again.

**The opt-out, `ack_wait`.** Andrew answered on 2026-11-16, choosing **"On, opt out
(Recommended)"**: the wait is on by default, and a keyword turns it off.

- **The keyword.** `SyslogSink.__init__` and `LogstashSink.__init__` each gain a keyword-only
  `ack_wait: bool | None = None`. On `SyslogSink` it is appended after `max_datagram_bytes`, its
  last parameter. On `LogstashSink` it follows `max_datagram_bytes`, its last named parameter, and
  precedes `**http_kwargs`, which Python requires to stay last; a keyword-only parameter has no
  position a caller can depend on, so invariant 10's rule, that the surface grows by appending, holds
  either way. `SocketTransport` gains `ack_wait: bool = True`, appended last, which the two sinks
  pass and nothing else sets.
- **The name.** `ack_wait` names what it controls, the wait for the peer's TCP acknowledgement, and
  says that turning it off turns a wait off. `confirm`, `RabbitMQSink`'s keyword, was not reused: it
  turns on a broker's application-level confirmation, and this acknowledgement is the peer kernel's,
  which says the bytes arrived and nothing about whether the collector kept them.
- **`None`, the default, and `True`** wait, as the rule above says.
- **`False`** is the check the previous revision specified: `_end_state` reads `_peer_state` once,
  `SO_ERROR` and the peek, and waits for nothing. It is not 1.x: the pre-round check, the raise rule
  and the end check all still run, so a reset that has landed by the end of the round is still
  counted. The corpus measured what it gives up (*Evidence*): silent loss on the FIN-after-the-
  pre-check, RST-before-the-end-check and reset-after-the-end-check rows under 10 ms of delay, and on
  macOS loopback.
- **Validation**, in a helper beside `SocketTransport`, `_socket.require_ack_wait`, which both sinks
  call before anything is built:
  - a value that is neither `None` nor a `bool`, judged with `_diag.is_a(value, bool)`, is refused
    with `TypeError` naming `ack_wait` and the type. It is not read for its truth, because a new
    keyword has no 1.x caller to keep working and `"false"` would otherwise turn the wait on. That is
    *A bad argument is floored if it works and refused if it is already broken; a new one is
    refused* (`docs/decisions/sink-delivery/`): the argument is new, so nothing is floored;
  - a `bool` with the UDP transport is refused with `ValueError` naming `ack_wait`, as an argument
    the chosen backend cannot use: a datagram has no acknowledgement to wait for. On `LogstashSink`, a
    `bool` beside `url=` is refused by the existing socket-only rule, which gains `ack_wait`.
    `max_datagram_bytes` is accepted and ignored over TCP, and that is not a precedent here: its
    default on `SyslogSink` is an `int`, so a value given cannot be told from the default, while
    `ack_wait`'s default is `None` on both sinks, so it can, and invariant 13 says no argument is
    silently ignored because another was given;
  - a transport that is neither is left to `SocketTransport`, which refuses it as today.

**The bound is decided in session.** `_RESET_WAIT` stays 0.25 s: the coordinator kept it on
2026-11-16, and it is not Andrew's call. The reasoning, which the constant's docstring carries: it
covers a 50 ms round trip under Linux's longest delayed acknowledgement, 200 ms (`TCP_DELACK_MAX`),
the longest a peer that never writes back may hold its answer; a longer bound would close more of
the window a late reset leaves, and would cost every round against a peer that neither acknowledges
nor resets that much more.

**What the wait costs.** One round trip, plus however long the peer holds its acknowledgement, on
every TCP round, because the check returns once the last byte is acknowledged. Logstash and a syslog
collector never write back, so the peer acknowledges on its delayed-acknowledgement timer, which
Linux starts at 40 ms (`TCP_DELACK_MIN`) and lets grow to 200 ms (`TCP_DELACK_MAX`). The corpus's
clean round, median of 20, took 0.5 to 0.6 ms on loopback on both platforms, 7.9 ms with 1 ms of
delay each way, and 44.2 ms with 10 ms each way (*Evidence*). `_RESET_WAIT`
bounds it at 0.25 s a round, which covers a 50 ms round trip under Linux's longest delayed
acknowledgement. Other platforms' timers were not read. A peer that never acknowledges nor
resets, such as one whose host vanished, costs each round the whole 0.25 s.

**What stays unknowable, and why it is not counted later.**

- **A reset that reaches us after the end check.** The peer's kernel acknowledged the frames, and
  the peer then reset, closing with them unread, after the end check read the acknowledgement. The
  frames read as delivered. Under delay this includes a reset the peer sent just before the check,
  still in flight when it read. The next round's pre-check reads the reset and replaces the
  connection, and it does not count the previous round's frames: that round's run already settled
  them `ACCEPTED`, and SPEC-088 FR-001 has every event leave through exactly one term, so a later
  count would put them in `delivered` and in `sink_lost` both. The window's size is what the peer's
  kernel acknowledged and its reader had not taken, which is at most the peer's receive buffer:
  measured on an idle loopback connection at default sizes, 128,282 bytes (817 frames of 157 bytes)
  on Linux and 191,540 to 192,639 bytes (1,220 to 1,227 frames, two runs) on macOS, and receive
  autotuning lets it grow to `tcp_rmem`'s maximum, 6,291,456 bytes on Linux, and `autorcvbufmax`,
  4,194,304 bytes on macOS.
- **A round trip longer than `_RESET_WAIT`.** The check returns before the answer, and a reset that
  arrives later is the window above.
- **A platform where `_unacked` answers `None`**, Windows among them. The check does not wait, and
  the window is the one the review measured, a reset in flight at the check, which the corpus
  reproduces under delay with the wait removed.
- **A close mid-round.** A peer that closes with frames still arriving resets, which discards what
  it had not read, and nothing the sender sees says how much that was. Frames it had read before
  the close can be counted `UNKNOWN`. That over-count is the safe direction, and the corpus prints
  it as `unknown-but-read`.

FR-006 records the first two windows and the cost in §13.

#### Acceptance Criteria:

- [ ] `SyslogSink[tcp]-peer-closed` and `LogstashSink[tcp]-peer-closed` of
      `tests/test_conformance.py` read green (invariants 2 and 7).
- [ ] A new test, `tests/test_socket_close_corpus.py`, runs the table's scenarios against
      `SocketTransport` over loopback TCP in the ordinary `CI / test` job, arranging each through
      the `_peer_state` and `_end_state` seams and never sleeping for a packet to land, except for
      the close a scenario's peer sent: a row whose peer closes before the end check waits there,
      up to 2 s, until that close has landed on the connection it closed, read only through the
      connection's TCP state and its send queue so that no pending reset is consumed. Every such
      row waits with `ack_wait=False`, which leaves a reset in flight at its one read uncounted by
      design, and the two mid-round rows wait in both modes, since a peer's kernel acknowledges
      frames its reader has not taken, and a reset of acknowledged frames that lands after the
      check is FR-006's first window in both. The other `ack_wait=True` rows do not wait, so they
      prove the sink's own wait. It asserts:
      no `UNKNOWN` and no `lost` line for the clean, half-close and FIN-after-reading rows; every
      frame the peer lost counted once in `failed`, no frame it received counted, and the count
      named on one line, for the FIN-after-the-pre-check rows, at 1 and 5 frames, and the
      reset-before-the-end-check row. (On macOS every frame of the round is written to the dead
      connection and counted; on Linux at 5 frames the reset makes the second write raise, so the
      first frame is `UNKNOWN` by rule 4 and the rest go out again on a new connection and
      arrive.) For
      the reset-after-the-end-check row, the first round uncounted and the second round's frames
      either received or counted; and for the two mid-round rows, no more frames lost than counted
      (invariants 2, 7 and 11).
- [ ] A seam test drives `_end_state` with `_peer_state` and `_unacked` stubbed, on any platform:
      with `_unacked` answering 6 three times and then 0, it answers `"open"` after the fourth read;
      with `_peer_state` answering `"reset"` on its second read while `_unacked` answers 6, it
      answers `"reset"` after the second read; with `_unacked` answering 6 forever and
      `_RESET_WAIT=0.1`, it answers the last state in no less than 0.1 s and within 1.1 s; and with
      `_unacked` answering `None`, it answers after one read (invariants 2 and 3).
- [ ] Each re-plant reddens a named check, measured in an export of the branch head. With the end
      check counting `"fin"` as well as `"reset"`, the corpus test's half-close row reads a false
      count, on both platforms. With `_unacked` answering 0 always, so that the check does not
      wait, the seam test's first three cases fail on any platform, and on macOS the corpus test's
      FIN-after-the-pre-check row at 1 frame reads silent loss. With `_peer_state` reading no
      `SO_ERROR`, the corpus test's FIN-after-the-pre-check row at 1 frame reads silent loss on
      Linux, where CI runs it. With `_RESET_ERRNOS` emptied, so that a pending reset is read and
      cleared but not believed, the FIN-after-the-pre-check row at 1 frame and the
      RST-before-the-end-check row read silent loss on Linux, and the second on macOS
      (invariant 2).
- [ ] `SyslogSink(transport="tcp")` and `LogstashSink(host=, port=)` built with no `ack_wait`, or
      with `ack_wait=True`, wait at the end check: under the seam test's first case each reads
      `_unacked` four times. With `ack_wait=False` each reads `_peer_state` once and `_unacked`
      never, and the corpus test run with `ack_wait=False` reads every row as the rule does on
      Linux loopback once the close has landed, on Linux, where it runs (invariants 2 and 10).
- [ ] `ack_wait="false"`, `ack_wait=0` and `ack_wait=1.0` are refused with `TypeError` naming
      `ack_wait` and the type, on both sinks, before any socket is built. `ack_wait=True` or
      `False` with `transport="udp"` is refused with `ValueError` naming `ack_wait`, and on
      `LogstashSink` beside `url=` by the socket-only rule; `ack_wait=None` is accepted in every mode
      (invariant 13).
- [ ] `tests/test_public_surface.py` and `tests/typed_consumer/` accept the new keyword as an
      addition, and a 1.x call to either constructor builds the same sink as before with the wait
      on (invariant 10).
- [ ] With `SO_ERROR` holding `EHOSTUNREACH` on a connection whose peer is open, `_peer_state`
      answers `"open"` and the round's frames are `ACCEPTED` (invariant 2).
- [ ] A peer closed between two rounds leaves every frame of the second round on the new
      connection, `failed` unchanged and no `lost` line (invariants 2 and 7).
- [ ] With the peer closed mid-round and a later write raising `BrokenPipeError`, the frames written
      on that connection after its check are `UNKNOWN`, counted once in `failed`, named on one line,
      and never written again. The raising frame is written again on the next connection
      (invariants 2, 7 and 11).
- [ ] A held socket whose check raises `TypeError` answers `"open"`, and its frames are `ACCEPTED`
      as in 1.x (invariant 1).
- [ ] `probe_socket.py` against the branch reports `batch2 received=50/50`, `failed=0` and no line
      for both sinks under FIN and RST, in Docker `python:3.12-slim` and on macOS (invariant 2).
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
  `stack_timeout`. Andrew chose the default on 2026-11-15 (*Revision history*). A value that is not
  a `bool`, judged with `_diag.is_a(value, bool)`, is refused with `TypeError` naming `confirm` and
  the type, before anything is built. It is not read for its truth, for the reason `ack_wait` is
  not (FR-001): the argument is new, so no 1.x caller depends on it, and `confirm="false"` would
  otherwise turn confirms on. That is *A bad argument is floored if it works and refused if it is
  already broken; a new one is refused* (`docs/decisions/sink-delivery/`).
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
      `97a03f45`, and their `peer-closed` rows sit under the `publish-unconfirmed` departure
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
- [ ] `inspect.signature(RabbitMQSink)` equals the `97a03f45` signature plus a keyword-only `confirm`
      with default `False`, as the last parameter (invariant 10).
- [ ] `confirm="false"`, `confirm=0` and `confirm=1.0` are refused with `TypeError` naming
      `confirm` and the type, before any connection is made, for an owned and a borrowed
      connection alike; `confirm=True` and `confirm=False` are accepted (invariant 13).
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
- **No variant for `ack_wait=False`, and so no departure.** RabbitMQ's opt-in earned variants
  because the corpus's double can tell a confirmed publish from an unconfirmed one. Its fake TCP
  socket cannot tell a waited end check from an unwaited one: it cannot be polled and has no send
  queue to read, so `_peer_state` answers `"open"` and `_unacked` answers `None`, and both settings
  read it once. Measured: with the default flipped to `False` in an export of the prototype, all 268
  rows read as with it on. A variant would add rows that only repeat the TCP rows, so the opt-out's
  cost is held where it shows, in `spec093_close_corpus.py` and `tests/test_socket_close_corpus.py`
  (FR-001), and recorded in §13 (FR-006). The totals do not move. The corpus's gate still asks
  that every constructor parameter be a variant's pick or neutral, so `ack_wait` is listed in
  `NEUTRAL` for `SyslogSink` and `LogstashSink`, with that reason beside it: it changes what the
  sink can know, and the corpus cannot see the difference.
- **Totals.** SPEC-095 landed first (#414), and this spec lands with SPEC-094 on top of it. On the
  merged tree: 268 rows, 6 pinned under four pins, 34 departure rows under fourteen departures,
  and 228 that must pass. SPEC-092's FR-004 asserts its totals as literals, and those literals
  change to these numbers. Before SPEC-095 landed, this spec's own settlement read 268 rows, 62
  pinned under seven pins, 15 departure rows under nine departures and 191 that must pass,
  measured at `a17a28a4` on base `c67931cd`, a local commit of the build that the squash merge
  leaves off the remote.

#### Acceptance Criteria:

- [ ] The corpus generates 268 rows over 55 variants, and on the merged tree its literals read 268
      rows, 6 pinned, 34 departure rows and 228 that must pass (invariants 2 and 7).
- [ ] Against the corpus and `src` of the `origin/main` the build lands on (`295ba803`, with
      SPEC-095 and SPEC-098), only the rows the criteria of FR-001 to FR-004 name and SPEC-094
      FR-003's four change verdict, the twelve confirmed `RabbitMQSink` rows are new, and every
      other row reads as it read before (invariants 2 and 7).
- [ ] `adds_faults` has standing fixtures in `tests/test_conformance_gate.py`, one per failure it
      names, each asserting its line names the variant and the fault, and one well-formed table that
      yields no line. Applied to `VARIANTS`, it yields none (invariants 2 and 7).
- [ ] With FR-001's fourth rule removed, so that a later raise leaves the frames before it
      `ACCEPTED`, in an export of the branch head, `SyslogSink[tcp]-peer-closed` and
      `LogstashSink[tcp]-peer-closed` read `silent-loss` again. The corpus's fake socket cannot be
      polled, so its check answers `"open"` and the corpus reaches only that rule. The two checks are
      held by `tests/test_socket_close_corpus.py` (FR-001) (invariant 2).
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
- **§13 gains five bullets.**
  - "**A TCP peer that acknowledges a round's frames and then resets, after the end-of-round check,
    leaves them reported delivered.**" It names the corpus's reset-after-the-end-check row, whose
    first round went uncounted in every run on both platforms at every delay, and a round trip
    longer than `_RESET_WAIT`. It gives
    the window's measured size, the peer's receive buffer: 817 frames of 157 bytes on Linux and 1,220
    to 1,227 on macOS at default sizes, and up to 6 MB and 4 MB under autotuning. It says why the next
    round's check, which sees the reset, does not count them: SPEC-088's one-term ledger.
  - "**Every TCP round waits for the peer to acknowledge its last frame, up to 0.25 s.**" It carries
    the corpus's clean-round medians on loopback and under 1 ms and 10 ms of delay, the delayed
    acknowledgement behind them, and the 0.25 s a round that a peer which neither acknowledges nor
    resets costs. It says that where `_unacked` cannot read the send queue, Windows among them, the
    check does not wait and a reset in flight at the check goes uncounted, and that `ack_wait=False`
    makes every platform behave so, with the rows the corpus measured silent under delay. It says
    the 0.25 s bound was decided in session, and why.
  - "**A FIN alone is not read as a loss, because it is also a legal half-close.**" The bullet
    carries the reproduction (`halfclose.py`, all 60 frames received with `failed=60` under a
    FIN-counting check) and the corpus's rows in which every frame arrived after a FIN. A false
    count is the safe direction for a may-have-landed line, but a half-closing collector would see
    one on every batch. A reset, read through `SO_ERROR` or a peek that raises, is what shows the
    peer discarded data.
  - "**A confirmed `RabbitMQSink` publish costs a broker round trip.**" The bullet carries the
    measured figures at 0.2, 1 and 5 ms, and says that at 5 ms one 1,000-event batch outlasts
    `flush()`'s 5 s default.
  - "**A confirm wait is bounded only by the connection.**" The wait ends on the heartbeat,
    `blocked_connection_timeout` or a socket error. It is an instance of invariant 3's sink blocked
    inside a network call.
- **Decisions in `docs/decisions/sink-delivery/`, each one file with its fence, entry and dated
  History.**
  - A new decision for FR-001's rule, with the half-close reasoning, the acknowledgement wait and
    its cost, and why a reset the next round sees is not counted. It quotes Andrew's "On, opt out
    (Recommended)" with the date 2026-11-16, beside the RabbitMQ decision's "Off, opt in" as the
    precedent the other way, and says why the two differ: a confirm costs a broker round trip per
    message, and the acknowledgement wait one round trip per round, bounded. It records `ack_wait`'s
    name and validation, and the 0.25 s bound as decided in session.
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
  - The `SyslogSink` and `LogstashSink` rows (`README.md:783-784`) gain the TCP checks, the wait
    for the peer's acknowledgement at the end of each round, `ack_wait=False` to turn it off, and
    the may-have-landed line.
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
    signature or default changed", which `RabbitMQSink(confirm=)` and the two `ack_wait=` keywords
    make false. So does the same sentence under *What did not change* (`v1.2.1.md:240` at
    `97a03f45`), which is rewritten too. The file keeps its name. Whether the release is 1.2.1 or 1.3.0 is chosen when it is tagged, since a new keyword
    argument is additive. The opener says so, and no sentence depends on the number.
  - The entry lists:
    - the TCP line and its `UNKNOWN` counts, the end-of-round wait for the peer's acknowledgement, a
      round trip plus the peer's delayed acknowledgement and at most 0.25 s, and a half-closing
      peer's connection replaced each round;
    - `SyslogSink(ack_wait=)` and `LogstashSink(ack_wait=)`, on by default; `False` keeps the
      checks without the wait, and what that gives up; a non-`bool` refused with `TypeError`, and a
      value for UDP or beside `url=` with `ValueError`;
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
      and naming SPEC-093, and §13 carries the five bullets.
- [ ] The four new decision files and the two dated History entries exist. The decision-A file quotes
      "Unadjudicated (Recommended)", the Sentry file "Dropped, with a line (Recommended)" and the
      RabbitMQ file "Off, opt in", each with the date 2026-11-15, and the FR-001 file "On, opt out
      (Recommended)" with the date 2026-11-16.
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
_RESET_ERRNOS: frozenset[int]   # ECONNRESET, EPIPE, ECONNABORTED, ETIMEDOUT
_RESET_WAIT: float = 0.25       # the end check's bound
_RESET_STEP: float = 0.001      # its pause between reads
def _peer_state(sock: socket.socket) -> Literal["open", "fin", "reset"]: ...   # seam beside _make_tcp
def _unacked(sock: socket.socket) -> int | None: ...                          # seam: SIOCOUTQ / SO_NWRITE
def _end_state(sock: socket.socket, wait_for_ack: bool = True) -> Literal["open", "fin", "reset"]: ...
def require_ack_wait(value: object, owner: str, transport: object) -> bool: ...  # judges ack_wait
class SocketTransport:
    def __init__(self, host, port, *, ..., sink_name: str = "SocketTransport",
                 ack_wait: bool = True) -> None: ...                  # new keyword, last

# src/log_foundry/sinks/syslog.py, logstash.py
class SyslogSink:
    def __init__(self, host, port=514, *, ..., max_datagram_bytes: int = ...,
                 ack_wait: bool | None = None) -> None: ...           # new keyword, last
class LogstashSink:
    def __init__(self, url=None, *, ..., max_datagram_bytes: int | None = None,
                 ack_wait: bool | None = None,                        # new keyword
                 **http_kwargs: Unpack[HTTPAuthKwargs]) -> None: ...

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
├── _socket.py        # FR-001: _peer_state, _unacked, _end_state, require_ack_wait, the checks
├── syslog.py, logstash.py  # FR-001: ack_wait=
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
`patch -p1` to `git archive 97a03f45 src tests docs/audits/2026-11-14-probes`, and it carries this
spec's `src/` change and SPEC-092's prototype corpus edited as FR-005 says. SPEC-092 is not built, so
its prototype (`spec092_run.py`) stands in for its corpus. The corpus's `ledger` check does not run,
and the build re-measures against the delivered corpus. Numbers were taken on macOS with Python 3.13.1
and no extra installed, at `97a03f45` unless they say otherwise. Other sessions shared the machine,
and the one-minute load is given where it was read.

**The rows.** Against the prototype corpus at `97a03f45`, `src/` unchanged, 256 rows read 170 green
and 86 red, which is SPEC-092's record. With this patch alone applied, and no SPEC-094 change, 268 rows
read 191 green and 77 red. That is the same in three runs on Python 3.13.1 and one on Python 3.12.12,
each exiting 0, and from the committed patch applied to a fresh export, and row for row the same
as the second revision's prototype, so the third revision's close rule moves no row. Exactly these
ten rows move,
each from `silent-loss` to green:

- the `no-verdict-shape` rows of `SQSSink[standard]`, `SQSSink[fifo]`, `SNSSink`, `KinesisSink`,
  `FirehoseSink`, `ElasticsearchSink` and `OpenSearchSink`;
- `SyslogSink[tcp]-peer-closed` and `LogstashSink[tcp]-peer-closed`;
- `SentrySink[sdk]-never-acknowledged`.

The twelve new rows read green, except `RabbitMQSink[confirmed-borrowed]-peer-closed`, which reads
`ambiguous-when-knowable`. The default `RabbitMQSink` rows read as at `97a03f45`. During authoring the
changes were also measured one at a time, each on top of the ones before it, and each step moved only
the rows of the sinks it changed. The socket change alone moved exactly the two TCP rows.

**The mutants.** Each was run on this patch's corpus.

- With FR-001's fourth rule removed, so that a raise leaves the frames before it `ACCEPTED`, both
  TCP `peer-closed` rows read `silent-loss`, re-measured on the third revision's patch.
- With the end-of-round check counting a FIN as well as a reset, `halfclose.py` read `failed=60` for
  60 frames that all arrived. With the rule as specified, it read `failed=0` in Docker
  `python:3.12-slim` and on macOS. At `97a03f45`, `src/` unchanged, it read `failed=0`.
- With `_peer_state` reading no `SO_ERROR`, the reviewer's `fin_then_rst.py` read `fin` after the
  FIN and the RST on Linux. With it, `reset` on Linux and on macOS.
- With the liveness probe removed, `RabbitMQSink[confirmed]-peer-closed` read
  `ambiguous-when-knowable`.
- With `confirm_delivery` never called, `RabbitMQSink[confirmed]-taken-then-raised` read `duplicate`,
  `RabbitMQSink[confirmed-borrowed]-taken-then-raised` read `lost-but-held`, and
  `RabbitMQSink[confirmed-borrowed]-peer-closed` read `silent-loss`.

**The suite.** The full suite was run against the prototype in a scratch export with no `docs/`.
Some tests fail there with or without the prototype, because they read files the export lacks: 43
at `9d70f782` and 50 at `97a03f45`. The prototype fails 27 more. 25 of them are the same 25 at
`33582484`, at `bb6656ff`, at `9d70f782` after the first revision, and at `97a03f45` after the
second and third, so no revision's changes fail a test the first draft did not, except the
third's end-of-round wait, which fails the two gates FR-001 names. The fourth revision's
`ack_wait` keyword fails none: the same 27, and SPEC-094's 52 on top, as before it. Each is
accounted for:

- FR-001, 2: `test_sink_retry.py::test_only_the_engine_waits_or_backs_off` and
  `test_engine_sinks_roster.py::test_the_engine_sinks_are_the_sixteen`, which read `_socket`'s
  `_retry.wait` as an engine sink waiting; FR-001 adds `_socket` to `WAIT_EXCLUSIONS` and has the
  roster skip what that list names.

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

**The close corpus.** `spec093_close_corpus.py 20` drove `LogstashSink` in TCP mode through the
prototype's `SocketTransport`, with no sleep before any check, 20 runs a scenario, on macOS (Python
3.13.1) and in Docker `python:3.12-slim` (Linux 6.12, Python 3.12.16) on loopback and under `tc
qdisc add dev lo root netem delay 1ms` and `10ms`, which delay each direction. A run is right when
no frame was lost that the round did not count and, outside the two mid-round rows, nothing was
counted that arrived. Each cell gives the runs that were right, for the rule as specified:

| Scenario | macOS | Linux | Linux, 1 ms | Linux, 10 ms |
|---|---|---|---|---|
| clean | 20/20 | 20/20 | 20/20 | 20/20 |
| half-close, 3 rounds | 20/20 | 20/20 | 20/20 | 20/20 |
| FIN after the pre-check, 1 frame | 20/20 | 20/20 | 20/20 | 20/20 |
| FIN after the pre-check, 5 frames | 20/20 | 20/20 | 20/20 | 20/20 |
| RST before the end check, frames unread | 20/20 | 20/20 | 20/20 | 20/20 |
| FIN before the end check, all read | 20/20 | 20/20 | 20/20 | 20/20 |
| RST after the end check: first round uncounted (residual), second right | 20/20 | 20/20 | 20/20 | 20/20 |
| peer FINs mid-round: no loss uncounted (frames counted that arrived) | 20/20 (20) | 20/20 (20) | 20/20 (1) | 20/20 (0) |
| peer RSTs mid-round: no loss uncounted (frames counted that arrived) | 20/20 (20) | 20/20 (20) | 20/20 (1) | 20/20 (0) |

The bracketed figure is the runs in which frames the peer had read were counted `UNKNOWN`, the
over-count FR-001 describes; across them 108 to 1,000 frames were counted for 3 to 340 lost. On
the reset-after-the-end-check row, every run's first round went uncounted, which is the residual
window. On loopback its second round went out on a new connection, the reset having reached the
pre-check, and arrived; under delay the reset had not arrived by the pre-check, so the second round
went out on the reset connection, was lost, and was counted.

The clean round's median, which is the end check's cost on a healthy connection, was 0.5 ms on
macOS and 0.6 ms on Linux loopback, 7.9 ms at 1 ms of delay and 44.2 ms at 10 ms. A round trip
is 2 ms and 20 ms there, and the rest is the peer's delayed acknowledgement.

The rules that came before, run through the same corpus at 10 runs a scenario. A cell names the
rows that went wrong and in how many runs:

| Rule | macOS | Linux | Linux, 1 ms | Linux, 10 ms |
|---|---|---|---|---|
| read `SO_ERROR` and peek once, at once after the round (the previous revision) | FIN-after-the-pre-check, 1 frame 8 silent, 5 frames 4; RST-before-the-end-check 8 | none | both FIN-after-the-pre-check rows, RST-before-the-end-check and the reset-after-the-end-check row's second round 10 silent; mid-round FIN 2 | the same rows 10 silent; mid-round 5 and 4 |
| the same, waiting up to 0.1 s once a FIN is seen (the review's candidate) | not run | none, but every half-closed round paid the 0.1 s (102.7 ms median) | the same rows as the previous revision, 10 silent; mid-round RST 2 | the same rows 10 silent; mid-round 5 and 3 |

The candidate's wait never began under delay, because the FIN itself had not landed by the end
check, so it did no better than the previous revision there.

`ack_wait=False`, the opt-out, run through the same corpus with `--no-ack-wait` at 5 runs a
scenario, reads as the previous revision's rule, which it is: every row right on Linux loopback in
those runs, though not on a loaded runner, where the mid-round rows were silent in some runs, as
they were with the wait on, through FR-006's first window (*Revision history*, 2026-11-22); on
macOS, FIN-after-the-pre-check at 1 frame silent in 5 runs, at 5 frames in 1, and
RST-before-the-end-check in 1; under 10 ms of delay on Linux, both FIN-after-the-pre-check rows,
RST-before-the-end-check and the reset-after-the-end-check row's second round silent in every run,
and the mid-round rows in 4 and 2. Its clean round's median was 0.1 ms on macOS and 0.4 ms on Linux
under 10 ms of delay, against 1.3 ms and 43.0 ms with the wait on in the same runs, every row of
which read as the table above says.

Each re-plant, run through the same corpus at 10 runs a scenario. A cell names the rows that went
wrong and in how many runs; every other row read as the rule's did:

| Re-plant | macOS | Linux | Linux, 1 ms | Linux, 10 ms |
|---|---|---|---|---|
| the end check counts `"fin"` as well as `"reset"` | half-close 10 false counts; FIN-after-reading 4 | half-close 10 | half-close 10 | half-close 10 |
| `_unacked` answers 0, so the check does not wait | FIN-after-the-pre-check, 1 frame 10 silent; 5 frames 3; RST-before-the-end-check 6 | none | both FIN-after-the-pre-check rows, RST-before-the-end-check and the reset-after-the-end-check row's second round 10 silent; mid-round 2 and 1 | the same rows 10 silent; mid-round 3 and 6 |
| `_peer_state` reads no `SO_ERROR` | none: the peek raises after a reset there | FIN-after-the-pre-check, 1 frame 10 silent | both FIN-after-the-pre-check rows 10 silent | the same |
| `_RESET_ERRNOS` emptied | FIN-after-the-pre-check, 1 frame 6 silent, 5 frames 8; RST-before-the-end-check 10 | FIN-after-the-pre-check, 1 frame 10 silent; RST-before-the-end-check 10 | not run | not run |

So no single platform's loopback catches every re-plant. Linux loopback, where CI runs the corpus
test, catches the FIN-counting, `SO_ERROR` and errno-set re-plants, and the seam test catches the
missing wait on any platform, which is why FR-001's criteria pair the two.

**Why the end check reads the send queue.** `spec093_fin_after_rst.py` reads the socket every 50 ms
for half a second after the peer closed and our next 6-byte frame drew its reset. On Linux, in
Docker, `SO_ERROR` read `EPIPE` (32) at once and 0 on every read after. On macOS it read 0 at once,
because the reset had not arrived, `ECONNRESET` (54) at 55 ms, and 0 after. On both, the send queue
read 6 bytes unacknowledged on every read, and the peek read an empty FIN once the socket was
readable. So a reset is visible once, to the first reader of `SO_ERROR`, it can land well after
the write on macOS loopback, and the frame it answered is never acknowledged.
`spec093_unread_buffer.py` measured the residual window's size: a peer that stopped reading had
acknowledged 128,282 bytes on Linux and 191,540 to 192,639 bytes on macOS, over two runs, when the
sender's buffer filled, against a peer `SO_RCVBUF` of 131,072 and 197,176 bytes.

**The socket options.** `spec093_socket_options.py 5`, run once at a one-minute load near 100 on
macOS, gives silent loss out of the second batch, meaning frames reported accepted that the listener
did not receive, over five runs. The bracketed figure is frames counted as may-have-landed that did
arrive. Its last column, an end check that reads only a reset, is the peek-only rule that the corpus
shows wrong on Linux; the specified rule replaces it.

| Scenario | today | check per round | check per frame | unknown on a raise | round + raise | round + raise + end | round + raise + end on reset |
|---|---|---|---|---|---|---|---|
| peer closes between batches, FIN | 6 to 22 | 0 | 0 | 0 | 0 | 0 | 0 |
| peer closes between batches, RST | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| peer closes after 10 of 50, mid-batch | 1 to 25 | 1 to 12 | 0 to 10 | 0 | 0 | 0 | 0 |
| peer closes after 2 of 3, 1 ms between frames | 1 | 1 | 0 | 1 | 1 | 0 | 0 |
| peer half-closes and keeps reading | 0 | 0 | 0 | 0 | 0 | 0 (50 counted) | 0 |

The cost lines, three runs at a load near 21: `select.select` 0.41 to 0.59 µs per check, a fresh
`select.poll` object 21 to 41 µs, `sendall` of a 156-byte frame 4.1 to 9.8 µs, and
`recv(MSG_PEEK | MSG_DONTWAIT)` on an idle socket with a 1.0 s timeout raised `TimeoutError` after
1.00 s each time. `probe_socket.py` against the third revision's prototype read `batch2
received=50/50`, `failed=0` and no line for both sinks under FIN and RST, in Docker
`python:3.12-slim` and on macOS. At `97a03f45`
it read 49 of 50 with `failed=0` in Docker.

**The reply probes.** These were measured against the prototype, and the `33582484` figures are in
each FR's Description. `probe_sqs.py` read G1 `failed=6`, two lines, `uncounted_missing=0`, and G2
`failed=25`, three lines, `uncounted_missing=0`. Every other case read as at `33582484`.
`probe_aws_more.py` read case 2 `failed=25`, one line, and case 3 `failed=6`, two lines.
`probe_misc.py` case 5 read `sink.sent=5 … dropped=20`, one line.

**The confirm cost.** `spec093_rabbitmq_confirm_cost.py` measured the median `emit`, with a fake
channel that sleeps one round trip per confirmed publish, at a load near 21:

| Round trip | batch 10 | batch 100 | batch 1,000 |
|---|---|---|---|
| none (`9d70f782`, before SPEC-088 landed) | 0.05 to 0.09 ms | 0.24 to 0.53 ms | 2.3 to 14.3 ms |
| 0.2 ms | 2.7 ms | 25.1 ms | 250 ms |
| 1.0 ms | 11.7 ms | 112 ms | 1,125 ms |
| 5.0 ms | 52.7 ms | 551 ms | 5,420 ms |

macOS sleeps overshoot short intervals, so the confirmed rows are an upper bound on the round trip
alone. SPEC-090's batches of up to 1,000 events pay the last column.

**What the prototype could not settle.** The real broker's confirm and return behaviour, which the
integration run owes (FR-004). What a real proxy or emulator returns for SQS, SNS, Kinesis or `_bulk`,
and whether a real SNS reply can omit `Successful`, which the `Failed`-only reading would then read as
1.x did. Windows, where the corpus was not run and `_unacked` reads nothing, so the end check does
not wait there. A real network's loss, reordering and a peer's delayed-acknowledgement settings,
which `netem`'s fixed delay on loopback does not model, and so how often a real peer resets after
acknowledging. And other platforms' delayed-acknowledgement timers, which bound how long the end
check waits on a healthy peer.

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
- Revised again on 2026-11-16, after SPEC-094's review found the second close rule wrong on Linux.
  - **The method changed, not only the rule.** The FIN rule had produced false loss on a
    half-close, and the peek-only rule produced silent loss on Linux, where a FIN followed by the RST
    our later frame draws still peeks as a FIN. FR-001's rule now comes from a committed scenario
    corpus, `spec093_close_corpus.py`, run in Docker `python:3.12-slim` and on macOS over six
    scenarios, including the ones that must stay silent. Of the three rules it measures, only the one
    that reads `SO_ERROR` before the peek is right on every row on both platforms, and FR-001's
    criteria now assert each row's verdict on Linux in CI.
  - §13 keeps one TCP window, a reset that lands after the end-of-round check, which is the corpus's
    one unknowable row on both platforms. The earlier bullet's claim that a FIN-only close after
    reading loses frames is withdrawn, because the corpus shows those frames received.
  - The base moved to `97a03f45`, where SPEC-088 landed. No line this spec cites moved, the corpus
    totals did not move, and the release-notes sentence FR-006 must rewrite now also stands under
    *What did not change*.
- Revised a third time on 2026-11-16, after the execution-frame review, which drove the real
  transport with no settle and found the `SO_ERROR` rule silent off loopback.
  - **S1, the instrument first.** `spec093_close_corpus.py` is rebuilt to drive `LogstashSink`
    through the real `SocketTransport`, arranging each scenario through the two seams and never
    sleeping for a packet, and it was run on macOS and on Linux on loopback and under 1 ms and
    10 ms of `netem` delay. The previous revision's corpus slept 50 ms before each check, which let
    every reset land first.
  - **The rule is derived from it.** The end check now waits until the peer's kernel has
    acknowledged every byte or a reset arrives, bounded at 0.25 s, since a peer that closed before
    the frames reached it answers them with a reset and never an acknowledgement. Of the rules run,
    it alone is right on every row on both platforms at every delay. The review's candidate, waiting
    for a reset only once a FIN is seen, stayed silent under delay because the FIN had not landed by
    the check either. The cost, a round trip plus the peer's delayed acknowledgement on every
    round, is measured and goes to §13.
  - **A reset the next round sees is not counted.** The previous round's run already settled its
    frames `ACCEPTED`, and SPEC-088 FR-001 lets an event leave through one term only. It is the
    residual window, recorded in §13 with its measured size, the peer's receive buffer.
  - **The criteria arrange each scenario through the seams**, and a seam test with `_peer_state`
    and `_unacked` stubbed holds the wait on any platform, since CI cannot add delay. Each re-plant
    names the rows it reddens.
  - **S5.** Only `ECONNRESET`, `EPIPE`, `ECONNABORTED` and `ETIMEDOUT` in `SO_ERROR` read as a reset,
    so a soft ICMP error on a live Linux connection does not count frames that arrived.
  - The end check's pause is `_retry.wait`, so `_socket` joins `WAIT_EXCLUSIONS` rather than
    waiting where that gate cannot see it.
- Revised a fourth time on 2026-11-16, after Andrew's answer on the wait's default.
  - **Andrew, 2026-11-16, on the TCP acknowledgement wait: "On, opt out (Recommended)".** The wait
    stays on by default, and `SyslogSink` and `LogstashSink` gain `ack_wait=`, whose `False` keeps
    the previous revision's immediate check. It sits beside his 2026-11-15 "Off, opt in" for
    `RabbitMQSink(confirm=)`, the precedent the other way: a confirm costs a broker round trip per
    message, and this wait one round trip per round, bounded at 0.25 s. FR-001 specifies the
    keyword, its name and its validation, and FR-006 records it in the decision, the README and the
    release notes.
  - **Decided in session, not Andrew's call.** `_RESET_WAIT` stays 0.25 s, kept by the coordinator:
    it covers a 50 ms round trip under Linux's 200 ms longest delayed acknowledgement.
  - The corpus gains no variant and no departure, because its fake socket reads alike either way,
    measured with the default flipped. The totals do not move.
- Corrected on 2026-11-19, during the build, by the orchestrator's ruling on the plan.
  - **`confirm=` is validated.** FR-004 specified no check on `confirm`, while FR-001 refuses a
    non-`bool` `ack_wait` on the ground that a new keyword has no 1.x caller and `"false"` would
    turn its behaviour on. The same ground holds for `confirm`, and the settled fence *A bad
    argument is floored if it works and refused if it is already broken; a new one is refused*
    requires it. FR-004 now refuses a non-`bool` with `TypeError` and gains the criterion.
  - **`ack_wait` is neutral in the corpus.** FR-005 gave `ack_wait` no variant; the corpus's gate
    requires every parameter to be a pick or neutral, so FR-005 now says it is listed in
    `NEUTRAL`, and why.
- Corrected on 2026-11-20, at completion, from the build.
  - **FR-001's close-corpus criterion held on macOS only as written.** It asked that every frame of
    the round be `UNKNOWN` on the FIN-after-the-pre-check rows at 1 and 5 frames. On Linux at 5
    frames the peer's reset reaches the sink before the round's second write, which raises, so
    rule 4 makes only the first frame `UNKNOWN` and the raising frame and those after it go out
    again on a new connection and arrive. That is the rule working, and the corpus judges the run
    right. The criterion now asks what the corpus asks: every frame the peer lost counted once, no
    frame it received counted, and one line. `tests/test_socket_close_corpus.py` asserts that.
- Corrected on 2026-11-22, when the build was rebased onto SPEC-095 and SPEC-098.
  - **FR-005's totals are the merged tree's.** SPEC-095 landed first and removed its
    `reply-resent` pin's 50 rows, moved two more to green and added five departures over 19 rows,
    so the literals read 268 rows, 6
    pinned under four pins, 34 departure rows under fourteen departures and 228 that must pass.
    The settlement this spec measured before then, 268, 62, 15 and 191, is kept as measured at
    `a17a28a4` on base `c67931cd`, a local commit that is not on the remote after the squash
    merge. Criterion 2 is read against `295ba803`: fourteen shared rows move, this spec's ten
    and SPEC-094's four, and the twelve confirmed rows are new.
- Revised on 2026-11-22, after `main` went red at the merge commit `62733aa0`. The py3.13 run
  failed `test_a_close_mid_round_loses_no_frame_it_does_not_count[fin-no-ack-wait]` with 850
  frames lost and none counted: every frame was written and acknowledged by the peer's kernel,
  and the reset its `close()` then sent was still in flight at the end check. That is FR-006's
  first window, and it is open in both modes, since the wait stops once every byte is
  acknowledged; the system-frame diff review reproduced it on `fin-ack-wait` too, in 1 of 250
  loaded runs. A row whose peer closes before the end check now waits there, up to 2 s, until the
  close has landed on the connection it closed, read through the connection's TCP state and its
  send queue and never through `SO_ERROR` or a peek, so no pending reset is consumed: every such
  row with `ack_wait=False`, and the two mid-round rows in both modes. FR-001's second and fifth
  criteria say so; the rule FR-001 specifies is unchanged. The Evidence, the architecture's §13
  bullet, the decision and the release notes now record the mid-round rows as silent in some runs
  on a loaded Linux runner, in both modes. A first attempt, which waited only until every byte
  was acknowledged, was the review's finding: an acknowledgement is not the answer to a close.
  Under load in Docker, the mid-round rows failed 3 of 2,000 executions with that attempt and none
  of 4,000 with this one, across `python:3.12-slim` and `python:3.13-slim`. The re-plants of a
  `_peer_state` that reads no `SO_ERROR`, an empty `_RESET_ERRNOS` and an `ack_wait=False` end
  check that never reads still turn the FIN-after-the-pre-check or reset-before-the-end-check
  rows red under the wait.
