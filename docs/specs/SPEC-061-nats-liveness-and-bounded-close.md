---
type: Spec
id: SPEC-061
title: "`NATSSink` Liveness and a Bounded Close"
description: "Gives NATSSink a loop that runs for its whole life, a server round trip before every publish and after every core batch, and a close the sink itself bounds."
spec_status: In Progress
depends_on: [SPEC-047]
---

## Overview

`NATSSink` reports success for events that never left the process, and its `close()` takes ten
seconds it cannot be told to shorten. Both defects are recorded as open in `architecture.md`
section 12 (the `is_connected` window and the unreachable `close()` bound), and measuring them
found a third that is worse: after any outage the sink never publishes again. All three have one
root cause. The sink drives its private event loop only inside `run_until_complete`, so `nats-py`'s
reader, ping and reconnect tasks receive CPU only while a call is in progress. A stalled server is
therefore never noticed, a reconnect never runs, and the sink's only liveness evidence is a flag
the driver cannot update between calls.

This spec runs the loop on a thread the sink owns for its whole life, asks the server rather than
the driver's flag whether it is there before it publishes, confirms a core batch with a second
round trip or counts it, and closes the client under a deadline the sink sets instead of calling
the driver's `drain()`. Recovery after an outage is bounded by the driver's own reconnect budget
(`max_reconnect_attempts`, SPEC-047 FR-002): once it is spent the client is closed for good and the
sink refuses every call, reported rather than silent. Rebuilding a client is not part of this spec.

Measured at `71e42dd` against `nats-py` 2.16.0 (the latest release on PyPI), Python 3.13.1, and
the repo's pinned `nats` image under Docker Desktop. M1 to M10 are the labels the rest of the spec
cites; FR-003 AC-8 re-asserts M6 against the server in the integration suite.

- **M1, the liveness window.** Under `docker pause` (a stalled server: no FIN, no RST), **75 of 75**
  one-event emits were accepted over 148 s with `is_connected` true throughout, and nothing raised
  or counted. The driver's ping task (`ping_interval` 120 s, `max_outstanding_pings` 2) declares
  the connection stale at an interval check after more than two pings are outstanding, so in the
  interval **(240, 360] s of loop time**, and the loop runs only during emits. SPEC-047's 40 s
  figure is a lower bound of this. Under `docker stop` and `docker kill` the **first** emit
  afterwards was accepted and its event lost uncounted, because the end-of-stream was processed
  during that emit's own loop run; the second was refused.
- **M2, no reconnect.** With `reconnect_time_wait=0.5`, after `docker stop` and `docker start` the
  sink was **still refusing 40 s after the restart**, the client in status RECONNECTING with its
  reconnect task pending forever. `emit` refuses on `not is_connected` without running the loop,
  so the task that would reconnect never runs. Every batch after any outage is lost until the
  process restarts.
- **M3, the driver hazard any timed-out round trip walks into.** `await client.flush(timeout=0.5)`
  against a paused server raises `FlushTimeoutError`, and the driver's `wait_for` **cancels** the
  PONG future, which stays in `client._pongs`. After `docker unpause` the late PONG reaches
  `_process_pong`, which calls `set_result(True)` on the cancelled future. The resulting
  `InvalidStateError` escapes into `_read_loop`'s generic handler, which logs "nats: encountered
  error" and **ends the reader task while the status stays CONNECTED**. From then on `flush()`
  takes its "internal loops are dead" fallback and **returns normally against a paused server**,
  a false confirmation, and the end-of-stream is never noticed. `Client.rtt()` has the same
  pattern, and today's `NATSSink.flush()` (the driver's 10 s default) can already trigger it. The
  consequence for any design: **a PONG wait the server may still answer must never be cancelled
  by the sink.**
- **M4, a prototype** (loop on a background thread, a PING round trip before and after the batch,
  `publish_timeout=1.0`): 1-event emit median of 50 **0.47 ms** (today 0.04 ms); 500-event emit
  **1.26 ms** (today 0.90 ms); the first emit after `docker stop` refused in 0.0000 s with `failed`
  at 0; `stop` then `start` resumed **0.50 s** after the start; each emit under `docker pause`
  refused in 1.00 s with `failed` at 0; `close()` against a paused server returned in **2.0 s**,
  where today it takes **10.0 s** and raises `FlushTimeoutError`. The prototype's pause-then-
  unpause "resume" was the M3 false confirmation, because it cancelled its PONG waits; it is not
  evidence of anything. The prototype's `close()` ran `drain()` under `wait_for`, which M7 shows
  does not hold in JetStream mode.
- **M5, driver facts** (`nats/aio/client.py`, 2.16.0). `drain()` waits for its subscriptions and
  then, in a `finally`, awaits `self.flush()` with `DEFAULT_FLUSH_TIMEOUT = 10`, which is not a
  connect option; it raises `ConnectionReconnectingError` while reconnecting, and it **swallows
  `CancelledError`** (L913) before that `finally`. `flush(timeout)` sends a PING and `wait_for`s
  its future, cancelling it on timeout, and returns without a PONG when the reader or flusher task
  is done. `_process_pong` pops `_pongs[0]` and calls `set_result(True)` with no `done()` check.
  `_pongs` is reset on every (re)connect, and `stats["reconnects"]` increments on each reconnect.
  No attribute records when the server was last heard from. `is_connected` flips only when the
  reader sees an end-of-stream or an error, an `OSError` in the flusher, or the ping task declares
  the connection stale.
- **M6, unconfirmed events can land.** Five core publishes written to a paused server's socket,
  then `docker unpause`: all **5 of 5** reached a JetStream stream bound to the subject. A batch
  whose confirmation went unanswered is therefore not evidence of non-delivery, and re-sending it
  would duplicate. FR-003 AC-8 re-asserts this against the server in the integration suite.
- **M7, `drain()` cannot be bounded from outside.** In JetStream mode a sink-built client always
  holds a subscription, the JetStream response inbox: 0 subscriptions before the first JetStream
  publish, 1 after. `wait_for(client.drain(), 2.0)` then took **12.00 s and raised
  `FlushTimeoutError`**, against 2.00 s with no subscription, because the swallowed cancellation
  (M5) lets the `finally`'s 10 s flush run to its own timeout.
- **M8, `client.close()` is unbounded.** Against a peer that stopped reading, with 20 MB pending
  (20,000 publishes of 1,000 bytes), `close()` was still running at 8 s. While the client was
  reconnecting with data pending it raised `TypeError: 'NoneType' object is not callable`.
- **M9, a round trip while reconnecting.** `flush(timeout=600)` against a client in RECONNECTING
  returned in **0.000 s** with no PONG, by the reader-or-flusher-done fallback (M5). A `flush()`
  that returns is therefore not evidence that the server answered.
- **M10, a client on another loop.** A client connected on one (idle) loop and flushed from a
  second loop returned only at its own timeout, **3.001 s in three of three** runs. The sink cannot
  drive a client it did not connect on its own loop.

## Scope

### In Scope

- A private event loop that runs on one daemon thread owned by the sink from construction to
  `close()`, with `emit`, `flush` and `close` submitting work to it under a bound (FR-001).
- A server round trip, through the driver's public `flush`, before an `emit` publishes, with a
  probe that is never cancelled while the server may still answer it, and a latch that refuses
  immediately while a probe is known to be unanswered (FR-002).
- Confirming a core batch with a second round trip, and counting rather than raising when it goes
  unanswered (FR-003).
- A `close()` that drives its own round trip and `client.close()` under one deadline, and a
  `flush()` the sink bounds (FR-004).
- The documentation, three decision-file edits, the invariant edits, three new open items and one
  known constraint in `architecture.md`, and the integration tests that go with them (FR-005).

The probe, the second round trip and the latch apply **only to a client the sink connected itself**
(M10). An injected `client=` keeps today's `is_connected` guard and no probe.

### Out of Scope

- `NATSSink`'s exit drain exceeding `shutdown()`'s join. That is the open item owned by SPEC-062,
  which changes `Worker._emit`; this spec does not touch `worker.py`.
- Re-acquiring a connection in a forked child (`reacquire_after_fork`). FR-001 makes the child
  refuse; FR-005 records the re-acquisition as a new section 12 open item, because the hook runs at
  fork time and a connect blocks.
- A sink that rebuilds its client after the driver's reconnect budget is spent; FR-005 records it
  as a section 12 open item.
- Detecting a driver reader task that dies while the status stays CONNECTED. The public API cannot
  see it; FR-002 states the residual and FR-005 records it.
- A `Nats-Msg-Id` header for JetStream deduplication, which stays its own open item.
- Any new constructor argument. Every bound below is a module constant or an existing argument.
- A finalizer that stops the thread of a sink nobody closed. A sink the library owns is closed
  under invariant 5; one the caller built and never closed leaves one parked daemon thread, as any
  connection-holding sink leaves its connection.
- Probing, confirming or draining an injected client, which the sink cannot drive (M10).
- Release notes. Whether this ships in the next patch or a later release is not settled here;
  FR-005 records that an entry is owed.

---

## Functional Requirements

Unless an acceptance criterion says "injected", a unit test reaches the sink through a fake `nats`
module whose `connect` returns the fake client (SPEC-047 FR-002 AC-5's idiom), because the probe
exists only for a client the sink connected. A fake models the real driver's `drain`, `close` and
`flush` faithfully: its `drain` swallows `CancelledError` and awaits a `flush` in a `finally`
(M5), its `flush` returns without a PONG when its reader is dead or it is disconnected (M5, M9),
and its `_pongs` is the list M5 describes.

### FR-001: The loop runs on a sink-owned thread for the sink's life

#### Description:

`__init__` starts one daemon thread, named `log-foundry-nats-loop`, that runs the sink's event loop
until `close()`. When the sink builds its own client, `nats.connect` runs as a coroutine on that
thread, so the client is bound to the loop that will drive it, and construction waits for it with
no bound of the sink's own: the driver's `connect_timeout` and `max_reconnect_attempts` (SPEC-047
FR-002) govern it, as they do today. `emit`, `flush` and `close` each submit one coroutine with
`asyncio.run_coroutine_threadsafe` and wait on the returned future, **holding `self._lock` across
the submit and the wait**, so calls stay serialized exactly as SPEC-028 requires. Because the loop
now runs between calls, the driver's reader, ping and reconnect tasks run too, which is what ends
M1's accepted-and-lost first emit and M2's permanent refusal.

A caller never waits on that future without a limit. The wait is `publish_timeout` plus
`CALL_SLACK` for `emit` and `flush`, and the close deadline plus `CALL_SLACK` for `close`. Each
coroutine bounds itself well inside that, so the slack is a backstop against a loop a driver has
wedged. **On expiry the loop is treated as wedged and is not recovered:** the sink marks itself
closed, calls `loop.call_soon_threadsafe(loop.stop)` and joins the thread under
`LOOP_JOIN_TIMEOUT`, leaving the loop unclosed if the thread is still alive, and releases the lock.
`emit` and `flush` then raise `SinkDeliveryError`; `close()` raises `TimeoutError`, the type its
own deadline's expiry raises. A later `emit` or `flush` raises `SinkDeliveryError` naming the
closed sink, and a later `close()` returns without touching anything. The backstop raise can follow
a publish that already left; it is accepted because the path is reachable only through a driver
call that ignores the deadline the coroutine gave it, and a duplicate is the safer error beside a
hung application thread.

**A core publish is bounded inside the coroutine, so a stalled peer cannot reach the backstop
through it.** The driver's core `publish()` writes into its pending buffer and, once that buffer
passes 2 MiB, force-flushes it under a flush timeout that defaults to `None`, so against a peer
that has stopped reading the await never returns. `_publish_all` therefore runs each core publish
as a task waited with `asyncio.wait` under what remains of the deadline and **never cancels it**:
the driver swallows the cancellation and the bytes already in its buffer may yet land (M3, M6). A
publish that outlives the deadline is abandoned with a reference kept until `close()`, counted as
published, and the events after it as unattempted, exactly as budget expiry counts them; FR-003's
confirmation then runs on what remains of the deadline, which is normally nothing, so the batch is
counted. A JetStream publish is not wrapped: its ack wait is bounded by the driver's own timeout,
and its send can only block once a single payload exceeds the driver's pending size.

`close()` is FR-004's sequence; its tail stops the loop and joins the thread under
`LOOP_JOIN_TIMEOUT`. Before stopping it cancels every task still pending on the loop and lets the
cancellations run, so nothing is "destroyed but pending" (today's `close()` emits that warning). A
thread still alive when the join expires is abandoned as a daemon, and `close()` returns without
raising for it and leaves the loop unclosed, since a running loop cannot be closed.

**A closer is not starved by in-flight callers.** `threading.Lock` is not FIFO, so a `close()`
queued behind a stream of emits could wait indefinitely. `close()` therefore sets `self._closing`
**before** taking the lock, and `emit` and `flush` read it before taking the lock and again after
acquiring it, raising `SinkDeliveryError` naming a closing sink when it is set. A call already
inside its coroutine finishes under its own bound.

**In a forked child there is no thread.** The sink records its construction pid, and every public
member compares it with `os.getpid()` **before taking `self._lock`**, since a lock a caller thread
held at fork time is repaired only by `_fork`'s child handler, and only for a sink reachable from
`log_foundry`'s modules. In a child, `emit` and `flush` raise `SinkDeliveryError` without running
anything on the inherited loop, client or socket, and `close()` returns without touching the
client, the loop or the thread, because the child acquired none of them (invariants 5 and 12).
Re-acquiring a connection in the child is Out of Scope.

Module constants, each with a docstring stating its reason: `CALL_SLACK = 1.0`, the margin the
backstop adds to a bound a coroutine already enforces on itself, which only has to cover loop
scheduling; `LOOP_JOIN_TIMEOUT = 1.0`, enough for cancellation callbacks to run and no more,
because a join that waits longer defeats FR-004's bound. `DEFAULT_CLOSE_TIMEOUT` is FR-004's.

#### Acceptance Criteria:

- [ ] AC-1: After constructing a sink from a fake `nats` module, exactly one live thread named
      `log-foundry-nats-loop` exists, it is a daemon, and the fake `connect` coroutine recorded
      that thread as the one it ran on.
- [ ] AC-2: With a fake client that records `threading.current_thread()` in `publish`, `flush` and
      `close`, every recorded thread is the loop thread and none is the caller's.
- [ ] AC-3: A construction refused with `ValueError` starts no thread and builds no loop, and a
      `connect` that raises leaves no live loop thread, a closed loop, and re-raises the driver's
      exception unchanged.
- [ ] AC-4: With a fake whose background reconnect is a task that flips `is_connected` after 0.3 s
      of loop time, and **no call into the sink during that time**, an `emit` made after 0.5 s is
      accepted. Against today's sink it is refused, because the task never ran.
- [ ] AC-5: Through `run_concurrently` with a preemption point injected after the submit and before
      the wait, eight threads calling `emit` while one calls `flush` and another calls `close` all
      return or raise only `SinkDeliveryError`, every thread joins within a stated deadline, no two
      coroutines overlap (the fake counts concurrent entries), and the loop thread is gone
      afterwards.
- [ ] AC-6: With a fake whose `publish` never completes and `publish_timeout=0.2`, `emit` raises
      `SinkDeliveryError` in at most `0.2 + CALL_SLACK` plus a stated test margin, the sink is
      then closed (a following `emit` raises naming the closed sink), and its loop thread has been
      joined or abandoned within `LOOP_JOIN_TIMEOUT`.
- [ ] AC-7: `close()` leaves no live loop thread, a closed loop, and no pending task, and the
      sink writes nothing containing "Task was destroyed but it is pending" to stderr or the
      `asyncio` logger when collected.
- [ ] AC-8: With a loop thread wedged in a blocking call, `close()` raises `TimeoutError` within
      the close deadline plus `CALL_SLACK` plus `LOOP_JOIN_TIMEOUT` and a stated margin, the sink is
      marked closed, the loop is left unclosed and the thread is a daemon; a second `close()`
      returns without raising and without calling the client.
- [ ] AC-9: After `close()`, `emit` and `flush` raise `SinkDeliveryError` naming the closed sink
      and move no `losses()` counter, and a second `close()` returns without calling the client
      again.
- [ ] AC-10: In a child created by `os.fork` (bounded by `signal.alarm` and a parent-side kill,
      as `tests/test_fork_lifecycle.py` does) while a caller thread is mid-`emit` and holds
      `self._lock`, `emit` and `flush` raise `SinkDeliveryError` and the fake client records no
      call, `close()` returns and the fake records no `close`, the loop is not stopped and the
      thread object is not joined; and the parent's next `emit` is accepted. The test's sink is
      unreachable from `log_foundry`'s modules (not configured, not owned by the lifecycle), so
      `_fork._reinit_primitives` does not replace its lock, and a mutant that moves the pid check
      after the lock acquisition hangs the child and is killed.
- [ ] AC-11: `test_nats_sink_does_not_reenter_its_managed_event_loop` in
      `tests/test_sink_concurrency.py` is rewritten for the new mechanism and still fails when
      `self._lock` is removed.
- [ ] AC-12: With one `emit` blocked inside its coroutine and several more queued on the lock, a
      `close()` from another thread completes within its bounds after the blocked call ends, and
      the queued calls raise `SinkDeliveryError` naming a closing sink with the fake's `publish`
      count unchanged since `close()` began; a mutant that drops the post-acquire `_closing` read
      lets them publish.
- [ ] AC-13: Serves invariants 3, 5, 6 and 12; invariant 6 because `emit`, `flush` and `close` all
      reach the loop through one submit-and-wait helper, asserted by the thread records in AC-2
      covering all three.

### FR-002: An emit publishes only after the server answers a round trip

#### Description:

Before publishing, `emit` of a sink-connected client sends a PING and waits for the PONG, in
**both** core and JetStream modes, inside the emit's one `publish_timeout` deadline (SPEC-047
FR-001's budget, no new argument). No answer within what remains means `SinkDeliveryError`, nothing
published and no counter moved (SPEC-032's refusal rule: a failure reported to the worker is not
also counted). The existing `_is_connected` check stays in front of the probe as the cheap refusal,
with the same reason text, and is **checked again** after the probe is answered, immediately before
the first publish.

**The mechanism has a hard constraint from M3: the sink never cancels a PONG wait the server may
still answer.** The probe is a task running the driver's public `Client.flush(timeout=
PROBE_FLUSH_TIMEOUT)`, and the sink waits on it without cancelling it when its own deadline
expires (`asyncio.wait`, or `asyncio.shield`). The rules:

1. At most one probe is outstanding.
2. An unanswered probe is **reused** by the next call, which waits on the same task. A second PING
   is never stacked behind it.
3. **A probe is answered only if all three hold:** it completed without raising; `_is_connected()`
   is true when checked; and `stats["reconnects"]` still equals the value recorded when it was
   sent. The second and third conditions exist because `flush()` returns without a PONG when the
   reader or flusher is done, which is the case while reconnecting (M9). A probe that finished
   before the call began is not evidence about now: the call retrieves its outcome, discards it and
   sends a fresh PING. Only a probe that completes during the call's own wait can answer it.
4. A probe is discarded while still pending only when the driver has reset its PONG queue, meaning
   `client.stats["reconnects"]` differs from the value recorded when the probe was sent, or at
   `close()`. A discarded probe's task is left to finish on its own inner timeout, which is harmless
   because its future is no longer in `_pongs`, and every probe task's outcome is retrieved (a done
   callback), so no "Task exception was never retrieved" is ever logged.
5. The probe uses only public driver API (`Client.flush`, `stats`, `is_connected`), never
   `_send_ping` or `_pongs`. An unreadable `stats` is treated as "no reconnect".
6. A client with no `flush` attribute skips the probe, and FR-003 skips its confirmation, for the
   reason `_is_connected` gives: the sink does not invent a refusal for a client that never
   claimed the capability. An injected `client=` is never probed either (M10).

**The known-unanswered latch.** Every probe records when it was sent. While the outstanding probe
has gone unanswered for at least `publish_timeout` and `stats["reconnects"]` still equals its
recorded value, `emit` and `flush` raise `SinkDeliveryError` **at once**, before taking
`self._lock` and without waiting, from a state the loop thread publishes and the caller reads
without blocking. Otherwise a call reusing the probe waits at most `min(remaining deadline,
publish_timeout - probe age)`. The effect: a stall costs one caller one `publish_timeout`, and every
later caller is refused immediately, instead of each queueing on the lock for its own
`publish_timeout` (which is the application's thread on the orphan path, invariant 4), until the
probe is answered, which resumes delivery, or a reconnect discards it. The latch clears when the
probe task finishes for any reason, because the next call then sends a fresh PING and waits on
that.

`PROBE_FLUSH_TIMEOUT = 600.0` is the driver's own `wait_for` bound inside the probe and must
**exceed** `ping_interval x (max_outstanding_pings + 1)`, the loop time by which the driver's ping
task has declared a silent connection stale and ended it (defaults 120 s and 2, so 360 s, M1). A
PONG can only arrive on the connection its PING left on, and that connection ends no later than
360 s into a stall, so by the time the inner `wait_for` could cancel the future there is no PONG
left that could arrive for it. The constant's docstring states this argument, the driver defaults
it rests on, and that an injected client built with a longer ping interval falls outside it.

**A residual the public API cannot close.** The driver still cancels PONG futures of its own
(`drain()` at L917, `Subscription._drain`, and after a reconnect at L1774), and a reader task that
dies while the status stays CONNECTED makes `flush()` return without a PONG while `is_connected`
is true. Condition 3 cannot see that, and nothing the sink may call can. It is recorded as a
section 12 open item (FR-005).

#### Acceptance Criteria:

- [ ] AC-1: On a healthy fake, `emit` calls the fake's `flush` once before its first publish, in
      both core and JetStream mode; the fake's call log shows the order.
- [ ] AC-2: On a fake that never answers, with `publish_timeout=0.3`, `emit` raises
      `SinkDeliveryError` in at most `0.3 + CALL_SLACK` plus a stated margin, and in at least 0.3
      s; the fake's `publish` is never called; `losses()` is unchanged; no `_diag.lost` line is
      written. Parametrized over core and JetStream.
- [ ] AC-3: Across five consecutive refused emits against one stall, the fake's `flush` is called
      once in total and its `_pongs` never holds more than one future.
- [ ] AC-4 (the M3 regression): with a fake modelling `_pongs` exactly as M5 describes, where a
      timed-out driver `flush` cancels its future and `deliver_pong()` calls `set_result(True)` on
      `_pongs[0]` unguarded, so a cancelled future raises `InvalidStateError` and marks the fake's
      reader dead: after a stall that outlasts `publish_timeout` and then ends with a delivered
      PONG, the fake's reader is still alive, the next emit is accepted, and **a second stall is
      still refused** (a dead reader would let the fake's `flush` return without a PONG and the
      second stall would be accepted). The test is run against the same sink with the probe
      replaced by `await client.flush(timeout=publish_timeout)` and must fail there.
- [ ] AC-5: The same regression holds against the real server: after a `docker pause` longer than
      the emit's `publish_timeout`, then `docker unpause`, emits are accepted again, and a second
      `docker pause` is refused. Recorded in FR-005's integration list.
- [ ] AC-6: A probe whose PONG arrives *between* calls is not credited to the next call: the next
      emit sends a second PING (the fake's `flush` count is 2), and on a fake that stops answering
      after the first PONG that emit is refused.
- [ ] AC-7: After the fake performs a reconnect (resets `_pongs`, increments `stats["reconnects"]`),
      the next emit sends a fresh PING rather than waiting on the orphaned one, and the orphaned
      probe ending in `FlushTimeoutError` later produces no "never retrieved" log.
- [ ] AC-8: Every call the sink makes to the fake's `flush` carries `timeout=PROBE_FLUSH_TIMEOUT`,
      and the module source names neither `_send_ping` nor `_pongs`.
- [ ] AC-9: `PROBE_FLUSH_TIMEOUT` is greater than the product `DEFAULT_PING_INTERVAL x
      (DEFAULT_MAX_OUTSTANDING_PINGS + 1)` read from `nats.aio.client` (the idiom of the existing
      ack-ceiling mirror test), and the constant's docstring names `ping_interval` and
      `max_outstanding_pings`.
- [ ] AC-10: A sink-connected client with no `flush` attribute is published to with no probe, no
      second round trip and no refusal; a client whose `is_connected` is false is refused without
      any PING being sent; an empty batch sends no PING.
- [ ] AC-11: Against a paused real server, `emit` with `publish_timeout=1.0` raises
      `SinkDeliveryError` in about 1.0 s with `losses()` unchanged (M1: 75 of 75 accepted today).
- [ ] AC-12: With a fake whose `flush` returns at once while the fake reports itself disconnected
      (M9), `emit` raises `SinkDeliveryError` and publishes nothing; and with a fake that answers
      the probe and then flips to disconnected, `emit` raises before its first publish. A fake whose
      `flush` returns at once after `stats["reconnects"]` moved is not an answer either.
- [ ] AC-13 (the latch, first half): against a never-answering fake, once one `emit` has been
      refused after `publish_timeout`, a second `emit` and a `flush` from other threads raise
      `SinkDeliveryError` in under 0.05 s each, with the fake's `flush` count still 1, **while a
      third thread holds `self._lock`** (the latch is read before the lock).
- [ ] AC-14 (the latch, second half): with that stall ended by a delivered PONG, the next call is
      accepted and the one after is accepted without delay; and after a fake reconnect the next
      call is not refused by the latch and sends a fresh PING. Before the latch trips, a second
      caller's wait on the reused probe does not exceed `publish_timeout` minus the probe's age,
      asserted by timing it from the first caller's start.
- [ ] AC-15: An injected `client=` whose fake records every call is never sent a `flush` by
      `emit` or by FR-003, and keeps today's `is_connected` refusal.
- [ ] AC-16: Serves invariants 2, 3, 4, 6 and 7: nothing is counted on a refusal (2), the wait is
      bounded by `publish_timeout` and the latch bounds it for later callers (3), the orphan
      path's caller thread carries a stall once rather than once per caller (4), `emit` and
      `flush` share the probe (6), and a sink that delivered nothing raises (7).

### FR-003: A core batch is confirmed, or counted

#### Description:

After a core (non-JetStream) batch's publishes, `emit` of a sink-connected client waits for a second
round trip through the FR-002 mechanism, **sent after the last publish** and within what remains of
the deadline. `_is_connected()` is checked again immediately before it is sent. A single connection
is processed in order, so an answered PONG (FR-002 rule 3) means the server processed every publish
before it. No answer, including when the deadline is already spent or the client has become
disconnected (the probe is still sent where it can be, so the next call can reuse it), means every
event that was published and not otherwise counted is added to `failed` and announced once through
`_diag.lost` with a reason naming the unconfirmed round trip, and **`emit` returns**. It does not
raise, because the bytes are in flight to a server that may yet process them, and a worker retry
could then deliver them twice (SPEC-018's rule, the reason an unadjudicable batch response is
counted and not retried). If every publish raised, nothing was sent, `emit` raises as today, and no
second round trip is made.

**This is not an exception to "a sink that delivered nothing raises" in spirit, and the argument
is stated.** That fence exists so a sink the worker believes cannot hide a total failure. An
unconfirmed batch is not known to be a total failure, and it is not hidden: it is counted and
announced, the loss is visible in `health().sink`, and a sustained stall is still reported as
failed batches, because the next call is refused with a raise, by FR-002's probe or its latch. Only
the one batch that straddles the onset of a stall is counted rather than raised. JetStream mode has
no such confirmation, since each publish already awaited its ack; an injected client has none
either.

The healthy-path cost is two round trips per core `emit`, run on whichever thread calls `emit`,
which on the orphan path is the application's own (invariant 4). M4's prototype measured 0.47 ms
against 0.04 ms for one event and 1.26 ms against 0.90 ms for 500. The cost is accepted and
FR-005 records it as an exception under invariant 4.

A core publish the deadline abandons (FR-001) is one of the published events this step counts
when the confirmation goes unanswered, which it normally does, since an abandoned publish has
spent the deadline. `failed` is a sink's own upper-bound counter, so an unconfirmed batch that
later turns out to have landed stays counted. It is outside SPEC-059's `Deferral` rule, which governs `dropped`: the count
is made on a path that returns, and nothing after it raises, so no retry can count it again.

#### Acceptance Criteria:

- [ ] AC-1: On a healthy core fake, `emit` of three events calls `publish` three times and then
      `flush` once more, so the call log reads probe, publish, publish, publish, probe.
- [ ] AC-2: When the second round trip goes unanswered, `emit` returns `None`, `failed` rose by the
      number of events published, and exactly one `_diag.lost` line was written whose reason
      contains "unconfirmed" and "PONG".
- [ ] AC-3: When the deadline is already spent after the publishes, the same count and line occur
      with no wait, and the probe task exists for the next call to reuse.
- [ ] AC-4: With a batch in which one event's publish raises and two succeed but are unconfirmed,
      `failed` rises by exactly three in total (one for the raise, two unconfirmed), not four.
- [ ] AC-5: When every publish raises, `emit` raises `SinkDeliveryError`, the fake's `flush` was
      called once only (the FR-002 probe), and `failed` equals the batch size as today.
- [ ] AC-6: In JetStream mode, `emit` calls the fake's `flush` exactly once.
- [ ] AC-7: After an unconfirmed batch against a continuing stall, a second `emit` raises
      `SinkDeliveryError` (FR-002), so the sustained stall is reported, not absorbed.
- [ ] AC-8: Against a real server, the integration test pauses it after the first publish of a core
      batch (a hook wrapped around the client's `publish` runs `docker compose pause nats`), `emit`
      returns with `failed` equal to the batch size, and after `docker compose unpause nats` all
      the events appear at the stream. That assertion is the evidence behind returning rather than
      raising, and it is made against the server, not argued.
- [ ] AC-9: `tests/test_deferral.py` passes unmodified, and the `failed` stores this FR writes
      need no allowlist entry beyond the existing `failed` rule.
- [ ] AC-10: With a fake that answers the first probe and then reports itself disconnected after
      the publishes, `emit` returns having counted the published events and sent no second PING
      wait; and a second-round-trip `flush` that returns at once while disconnected is not an
      answer.
- [ ] AC-11: An injected `client=` core batch is published and `emit` returns with no second round
      trip and no count.
- [ ] AC-12: Serves invariants 2, 4, 6 and 7; invariant 2 because the unconfirmed events are
      counted and announced, invariant 4 because FR-005 records the two-round-trip cost on the
      orphan path, invariant 6 because core and JetStream are twins and differ only here, and
      invariant 7 because FR-005 records this case as an exception.
- [ ] AC-13 (FR-001's core publish bound, seen from here): in core mode with `publish_timeout=1.0`,
      a batch over 2 MiB, and a fake whose `publish` blocks after the pre-publish probe answered
      (the server pausing just then), `emit` returns rather than raises in about 1.0 s plus a
      stated margin, `failed` equals the batch size (the blocked publish abandoned and counted as
      unconfirmed, the rest unattempted), one `_diag.lost` line names each, the loop thread is
      still alive and the sink still open (the backstop did not fire), and the abandoned task
      completes once the fake's publish is released.

### FR-004: `close()` and `flush()` are bounded by the sink, not by the driver's drain

#### Description:

**`close()` stops calling the driver's `drain()`.** M7 shows no deadline set from outside can
bound it, because it swallows the cancellation and its `finally` flush runs to its own 10 s. This is
the closure §12 named for the item: drive the steps itself rather than the driver's combined one.
`close()` runs its own sequence under one deadline `D`, `drain_timeout` when the caller passed one,
else `DEFAULT_CLOSE_TIMEOUT = 1.0`:

1. For a sink-connected client, one probe round trip through the FR-002 mechanism (skipped when a
   probe is latched as unanswered, when the client reports itself disconnected, and when the
   client has no `flush`), to push and confirm the outbound buffer. It is waited for at most `D`.
2. `client.close()`, as a task under what remains of `D`, waited with `asyncio.wait` and
   **cancelled on expiry**. Any failure from it, including the `TypeError` M8 measured while
   reconnecting, is absorbed through `_diag.absorbed`. A client without a `close` attribute skips
   the step.
3. Cancel the tasks still pending (the probe among them, now that no PONG can arrive), stop the
   loop and join the thread (FR-001).

Neither an expired nor a failed step 2 skips step 3. Afterwards `close()` raises: `TimeoutError`
when step 1 **ran** and went unanswered by the FR-002 rule (expiry, a client that turned
disconnected during it, a moved reconnect counter), the probe's own exception if it raised,
otherwise nothing. A **skipped** step 1 raises nothing: a client that is disconnected or latched at
close time has nothing to push that is not already counted, because emits were refused throughout. This keeps today's contract,
"whatever the flush raises; the loop is closed regardless", and nothing uncounted rides on the
confirmation, since FR-003 left every core batch confirmed or counted.

The consequence for an **injected** client is stated: the sink does not drain it. `close()` skips
step 1, because the sink cannot drive its loop (M10), and runs step 2, so its subscriptions are
closed rather than drained and whatever its `close()` flushes is all that is flushed.

`drain_timeout` is forwarded to `nats.connect` exactly as SPEC-047 FR-002 AC-1 and AC-3 require,
raw and absent when `None`. A value that bounds nothing (zero, negative, `inf` or `NaN`) falls back
to `DEFAULT_CLOSE_TIMEOUT` through `usable_timeout` for the sink's own deadline. An injected
`client=` cannot be given `drain_timeout`, so it always gets the default. The driver option no
longer bounds anything the sink calls, since the sink does not call `drain()`; its docstring says so.

`DEFAULT_CLOSE_TIMEOUT` is `1.0` so that the close deadline plus `LOOP_JOIN_TIMEOUT` (1.0) is
`_lifecycle.DEFAULT_CLOSER_GRACE` (2.0): a close that fits the grace is not cut by the exit, and a
longer one would be. The constant's docstring argues that sum, and that FR-003 leaves nothing
uncounted riding on close's confirmation. A caller who passes a larger `drain_timeout` accepts a
close the exit may cut.

**`flush()`** (SPEC-036 FR-002) of a sink-connected client goes through the FR-002 probe, bounded by
`publish_timeout` and by the latch, and raises `SinkDeliveryError` when it is unanswered or the
client reports itself disconnected. The driver's 10 s `FlushTimeoutError` can no longer escape from
it, and the sink's own `flush()` never cancels a PONG wait. The driver still cancels futures of its
own (FR-002, the residual), so this is narrower than the claim that the M3 hazard is gone. It counts
nothing. An injected client, or one with no `flush` attribute, keeps today's behaviour.

#### Acceptance Criteria:

- [ ] AC-1: With a fake modelling the real `drain` and `close` (M5), the sink's `close()` never
      calls `drain`; with a JetStream publish done first (so the fake holds one subscription) and
      a stalled fake server, `close()` returns within `D + LOOP_JOIN_TIMEOUT` plus a stated margin,
      where `wait_for(drain(), D)` on the same fake takes about 10 s more (M7).
- [ ] AC-2: With a fake whose `flush` never answers and no `drain_timeout`, `close()` raises
      `TimeoutError` in at most `DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT` plus a stated margin,
      the fake's `close` was called, and no loop thread is alive.
- [ ] AC-3: With a fake whose `close` never returns, `close()` cancels it at `D`, returns within
      `D + LOOP_JOIN_TIMEOUT` plus a margin, and stops and joins the loop thread (M8).
- [ ] AC-4: With a fake whose `close` raises `TypeError`, `close()` still stops and joins the loop
      thread, writes one `_diag.absorbed` line naming `TypeError` and no message text, and raises
      nothing when the probe was answered.
- [ ] AC-5: With `drain_timeout=0.3` and a fake that never answers, `close()` raises in about 0.3
      s, and the fake `nats.connect` received `drain_timeout=0.3`; with `drain_timeout=None` that
      keyword is absent from its kwargs. With `drain_timeout` zero, negative, `inf` and `NaN`, the
      deadline is `DEFAULT_CLOSE_TIMEOUT` and the raw value is still forwarded.
- [ ] AC-6: A test asserts `DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT <=
      _lifecycle.DEFAULT_CLOSER_GRACE`.
- [ ] AC-7: Against a paused real server `close()` returns in at most the deadline plus
      `LOOP_JOIN_TIMEOUT` plus a stated margin (M4: 2.0 s; today 10.0 s and `FlushTimeoutError`),
      **in core mode and again in JetStream mode after at least one JetStream publish** (M7).
- [ ] AC-8: `flush()` on a healthy fake returns after the PONG; on a fake that never answers, with
      `publish_timeout=0.3`, it raises `SinkDeliveryError` in at most `0.3 + CALL_SLACK` plus a
      margin and moves no counter; on a disconnected fake it raises without sending a PING.
- [ ] AC-9: After the stall in AC-8 ends with a delivered PONG, the fake's reader is alive and a
      second stalled `flush()` is refused (the M3 regression for `flush`).
- [ ] AC-10: An injected client's `close()` calls its `close` under `D` with no `flush` and no
      `drain`, and a client with no `close` attribute does not make `close()` raise; an injected
      client's `flush()` keeps today's behaviour.
- [ ] AC-11: Serves invariants 3, 5, 6 and 7; invariant 5 because a close in flight is waited for
      under the lock and the connection is released exactly once, and invariant 6 because `emit`
      and `flush` share the probe.
- [ ] AC-12: `close()` on a sink-connected fake that reports itself disconnected sends no PING,
      calls the fake's `close`, stops and joins the loop, and returns without raising; so does one
      whose probe is latched as unanswered. A `close()` whose probe ran and expired raises
      `TimeoutError` (AC-1), so the two outcomes are told apart by the test, not by the type alone.

### FR-005: Docs, decision records and integration evidence

#### Description:

`architecture.md` section 12: the two items "`NATSSink`'s `is_connected` guard has a wide,
unmeasured window" and "`NATSSink.close()`'s real bound is the driver's, and it is not reachable"
move to *Resolved* in the form that section already uses (struck title, spec named, "closed by
SPEC-061 FRn"). Three new open items, each naming what would close it: a **forked child** (the
sink refuses in a child because it acquired no connection, and a `reacquire_after_fork` would
connect during the fork hook, where a connect blocks); a **driver reader that dies while the
status stays CONNECTED** (the residual FR-002 states); and a **sink that rebuilds its client**
after the driver's reconnect budget is spent. The item about the exit drain exceeding
`shutdown()`'s join is left exactly as it is. Section 13's *Known constraints* gains one: the sink
cannot drive a client bound to another loop (M10), so an injected client's liveness is the caller's.

`NATSSink`'s docstrings are corrected, using the repo's strike-and-supersede style where a claim
is reversed and naming SPEC-061: the class docstring (the private loop "inside `run_until_complete`"
description, the "40 s lower bound" paragraph, and the retry paragraph's claim that a core publish
returns after the hand-off with nothing waited on), `__init__`'s `drain_timeout` entry ("is **not**
what bounds `close()`"), and the `emit`, `flush`, `close` and `_is_connected` docstrings. The README's
NATS paragraph is corrected where it states either claim.

Three decision files change, following `docs/process/knowledge-layout.md`:

- **New**, in `docs/decisions/sink-lifecycle/`: a one-line fence stating that **a driver's liveness
  flag is not evidence: a sink that cannot see the wire asks the server, and never cancels a wait
  the server may still answer.** The entry records the reasoning (M1 to M3, M7 to M9), the rejected
  alternatives (the driver's flag, `rtt()`, a private-attribute probe, a per-call cancel-and-retry,
  `wait_for(drain())`), the do-NOT lines, and ends in a dated `## History`.
- **Refined**, in `docs/decisions/sink-delivery/`: "A sink that delivered nothing raises; one that
  delivered something reports" gains the unconfirmed core batch as counted rather than raised, with
  the reason that the next call is refused (the probe or its latch), so the stall is still
  reported, and a dated History entry.

`sh scripts/gen-index.sh` regenerates the index.

`docs/invariants.md` gains four recorded exceptions. Invariant 7: the unconfirmed core batch
(`emit` returns having counted a batch whose delivery it could not confirm, announced once, with
the next call refused). Invariant 2: the same case in its list of losses counted but never retried.
Invariant 4: a core `NATSSink.emit` costs two server round trips on the calling thread, which is
the application's on the orphan path (M4's figures). Invariant 12: a forked child's `NATSSink`
refuses, because it acquired no connection and `reacquire_after_fork` is not implemented.

The integration suite (`tests/integration/test_nats.py`) gains tests using the idiom the existing
outage test uses, `docker compose -f tests/integration/docker-compose.yml pause|unpause|stop|start
nats`, each restoring the server in a `finally`. Unit tests with fakes cover every FR.

The release-notes entry for this change is owed in `docs/release-notes/<next tag>.md`, in the
completion ritual's step 6, once the tag that carries it is settled; this spec writes none.

#### Acceptance Criteria:

- [ ] AC-1: Both items sit under *Resolved* in the section's existing form naming SPEC-061, the three
      new items sit under *Open* each with its closure condition, the SPEC-062 item is
      byte-identical to before, and §13 *Known constraints* carries the injected-client entry.
- [ ] AC-2: Each docstring named above has its reversed claim struck in place with SPEC-061, and no
      remaining sentence in `nats.py` or the README says `close()` is bounded by the driver's
      10 s, that `is_connected` alone guards a publish, or that the loop runs only inside a call.
      `poetry run python scripts/docstring-lint.py` is green.
- [ ] AC-3: The new decision file's fence is a single line stating the claim above; the file has a
      `## History` with a dated first entry; `sh scripts/docs-lint.sh` is green with the index
      regenerated.
- [ ] AC-4: The sink-delivery decision file names the unconfirmed core batch and the reason, and its
      `## History` has a dated entry for it.
- [ ] AC-5: Invariants 2, 4, 7 and 12 each carry the recorded exception above, naming SPEC-061.
- [ ] AC-6: The integration suite has tests for stall refusal (FR-002 AC-11), resume after
      `stop` and `start` within a stated multiple of `reconnect_time_wait` (M2: never today; M4: 0.50
      s), reader survival after a late PONG (FR-002 AC-5), the close bound under a stall in core and
      in JetStream mode (FR-004 AC-7), and unconfirmed events landing after `unpause` (FR-003 AC-8).
- [ ] AC-7: `git diff` shows no file under `docs/release-notes/`, and the PR body names the owed
      entry.
- [ ] AC-8: Serves invariants 2, 4, 7 and 12 (AC-5) and records the evidence for invariants 3 and 5.

---

## Data Model

```python
# src/log_foundry/sinks/nats.py -- module constants added
DEFAULT_CLOSE_TIMEOUT: float = 1.0     # + LOOP_JOIN_TIMEOUT == _lifecycle.DEFAULT_CLOSER_GRACE
PROBE_FLUSH_TIMEOUT: float = 600.0     # > ping_interval * (max_outstanding_pings + 1) = 360
CALL_SLACK: float = 1.0                # backstop margin on a bound the coroutine enforces itself
LOOP_JOIN_TIMEOUT: float = 1.0         # join bound for the loop thread at close

# NATSSink private state added
_pid: int                              # os.getpid() at construction; compared before _lock
_thread: threading.Thread              # the loop thread, daemon
_closing: bool                         # set by close() before it takes _lock; read around _lock
_closed: bool                          # set under _lock by the first close(), or by the backstop
_owns_client: bool                     # True when the sink connected the client itself
_probe: tuple[asyncio.Task[None], float, int] | None   # (task, monotonic() when sent,
                                       #  stats["reconnects"] when sent); written by the loop
                                       #  thread as one tuple, read lock-free by callers: two
                                       #  fields could tear (a new task with an old send time
                                       #  would trip the latch on a probe just sent), one tuple
                                       #  cannot
_abandoned: set[asyncio.Task[None]]    # core publishes that outlived their deadline, kept until close
_drain_timeout: float                  # the sink's own close deadline D
```

## API / Interface Contract

No signature changes. `NATSSink(subject, *, client, jetstream, servers, publish_timeout,
connect_timeout, max_reconnect_attempts, reconnect_time_wait, drain_timeout)` is unchanged, and
the README table row is unchanged.

```
emit(batch)   raises SinkDeliveryError: closed or closing, forked child, disconnected, probe
                      unanswered or latched, nothing published, or the backstop expired.
              returns None, having counted the batch in `failed`, when a core batch is unconfirmed.
flush()       raises SinkDeliveryError: closed or closing, forked child, disconnected, probe
                      unanswered or latched, or the backstop expired.
close()       raises TimeoutError: the round trip went unanswered within the deadline, or the
                      backstop expired (the loop is wedged: the sink marks itself closed, stops
                      the loop with call_soon_threadsafe and joins under LOOP_JOIN_TIMEOUT); or the
                      probe's own exception. A failure of client.close() is absorbed, not raised.
              returns None in a forked child, and on any call after a first close or a backstop.
```

## Configuration / Environment

No new configuration. `drain_timeout` additionally sets the sink's own `close()` deadline.

## File & Folder Structure

```
src/log_foundry/sinks/nats.py                       # the loop thread, the probe, close and flush
tests/test_sinks_nats.py                            # existing tests updated for the thread and close
tests/test_sinks_nats_liveness.py                   # new: fakes for FR-001 to FR-004, the fork child
tests/test_sink_concurrency.py                      # the NATS re-entry test rewritten
tests/integration/test_nats.py                      # the integration tests
docs/decisions/sink-lifecycle/<slug>.md             # the new decision (index regenerated)
docs/decisions/sink-delivery/a-sink-that-delivered-nothing-raises-one-that-delivered-something-reports.md
docs/architecture.md  docs/invariants.md  README.md
```

## Implementation Phases

The complexity rule in `docs/process/model-routing.md` triggers on a new thread (concurrency and
locking), the close lifecycle and fork behaviour, and on the execution harness below. The
implementation plan is written on Fable, reviewed on Fable, and the build runs on Fable.

### Phase 1: The loop thread and the bounded wait (FR-001)

- Build the thread, the submit-and-wait helper, the pid guard, the `_closing` and `_closed` flags,
  the backstop and the close tail with a stub sequence; update `tests/test_sinks_nats.py` for the
  thread and for `close()` no longer draining.
- Build the execution harness the spec requires: `tests/conftest.py::run_concurrently` with
  preemption points injected as `tests/test_lifecycle_races.py` does, for concurrent `emit`,
  `flush` and `close`, and the real-`os.fork` child. A review that only reads this phase does not
  satisfy it.

### Phase 2: The probe, the latch and the refusal (FR-002)

- Build the non-cancelling, reused, single-outstanding probe with the three-condition answer and
  the latch, and the fake client modelling `_pongs`, `drain`, `close` and `flush` faithfully; the
  M3 regression, run once against the cancelling variant to prove it fails there.

### Phase 3: Confirming a core batch (FR-003)

- Build the second round trip, the count and the announcement; the paused-server landing test.

### Phase 4: Bounded close and flush (FR-004)

- Build `close()`'s own sequence under its deadline and `flush()` through the probe.

### Phase 5: Docs, decisions, integration (FR-005)

- Section 12, section 13, docstrings, README, the new and refined decision files, the four
  invariant edits, the integration tests (close bound in both modes), `sh scripts/gen-index.sh`,
  then the completion ritual.
- Mutation checks are owed on every new guard, each broken once and watched to redden before
  reverting: the pid guard and its position before the lock, the lock across the wait, the
  `_closing` reads before and after the lock, the backstop, the non-cancelling wait, probe reuse,
  the finished-probe discard, the reconnect discard, the three answer conditions, the latch and its
  position before the lock, the unconfirmed count, the deadline-spent branch, `client.close()`'s
  cancellation and absorption, the join bound, the JetStream no-second-probe branch, and the
  injected-client no-probe branch. A review frame that walks the future edits (adding a sink
  method, changing a constant) is advisable for the probe's rules.
