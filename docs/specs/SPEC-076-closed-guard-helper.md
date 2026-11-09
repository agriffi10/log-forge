---
type: Spec
id: SPEC-076
title: "The Closed-Guard Helper"
description: "Gives the shipped sinks that release a transport one private helper, in two modes, for the transport lock, the idempotent close and the refusal after close, and defines what a close does when it overlaps a call still inside the sink and what a closed sink's deliver answers."
spec_status: In Progress
depends_on: [SPEC-028, SPEC-032, SPEC-061, SPEC-069, SPEC-072, SPEC-074]
arc: stability-redesign
arc_order: 8
---

## Overview

Every sink that holds a connection, a file or a producer has to answer three questions: which calls
may run at once, what a second `close()` does, and what a call made after `close()` does. Thirteen
shipped sink classes keep closed state of their own, and eleven of them answer with their own copy
of the same few lines: a flag and a lock in `__init__`, an `if self._closed: raise` at the top of
`emit` and an `if self._closed: return` at the top of `close`. The copies have started to
disagree. `SocketTransport.close` and `GooglePubSubSink.close` are unguarded: a second call repeats
the release, harmless today only because what it repeats happens to do nothing the second time.
`KafkaSink` checks its flag outside any lock, so an `emit` that passed the check before `close()`
began can still produce after `close()` has flushed the producer, and that message is never
flushed, counted or announced. And no copy says what happens when a `close()` overlaps a call that
is still inside the sink.

The plan's stability arc moves that logic into one small private helper, `ClosedGuard`, once
SPEC-072 and SPEC-074 have settled what a lock must span. The engine holds no lock across `deliver`
and waits between rounds with no lock held. So a converted sink's transport lock spans **one round**,
never a backoff. It cannot be one base class, and the helper has **two modes** for that reason:

- `SERIAL` serialises the I/O. One lock spans every guarded section and the close, as SPEC-028
  requires for a transport that cannot be shared.
- `CLOSE_ONLY` guards only the close. Sections run concurrently, as SPEC-028 kept for the
  thread-safe `pymongo`, `redis-py` and `confluent-kafka` clients.

The helper is a context manager rather than a base class. That is what lets `SQLiteSink` keep its
encoding outside the lock and wrap only its insert. Each site whose shape fits adopts it. Each site
that does not fit stays local, with its reason named.

The spec also settles two things the arc left here. First, what a closed sink's `deliver` answers:
SPEC-072 assigns that to this spec, and SPEC-074 already gives the answer for its engine sinks: a
`REJECTED` verdict with `RunContext.refuse(error)`, so a refused run that fails moves no sink
counter. This spec keeps that answer, moves it behind the guard's refused branch, and adds the
guard's `CLOSING` state to what it covers. Second, what a close does when it overlaps a call that is still running, which is the plan's risk "an emit
that times out is still running". This spec builds on SPEC-061 (`NATSSink`'s liveness and bounded
close), which has landed, in 1.1.0, and is in `depends_on`. Its closing flag is the rule this spec generalises to every `SERIAL` sink, and nothing
SPEC-061 specifies is re-specified here.

**Figures**, each re-measured at `fda134a` with its instrument named:

- **Eight locked modules, nine classes.** `grep -rln 'self._lock = threading.Lock()'
  src/log_foundry/sinks` finds `_socket`, `clickhouse`, `eventhubs`, `file` (`FileSink` and
  `RotatingFileSink`), `nats`, `postgres`, `rabbitmq` and `sqlite`. Five of them hold that lock
  across a backoff: a `wait(` call sits in the locked path of `_socket`, `clickhouse`, `eventhubs`,
  `postgres` and `rabbitmq` (`grep -n 'wait('` over the eight). Four hold it across I/O only:
  `NATSSink`, `FileSink`, `RotatingFileSink` and `SQLiteSink`. `SQLiteSink` builds its rows before it
  takes the lock (`sqlite.py:108`, lock at `:113`). The plan said six across I/O and backoff.
  SPEC-074 then takes every backoff out of the lock.
- **Three lock only the close.** `grep -rln '_close_lock = threading.Lock()' src/log_foundry/sinks`
  finds `kafka`, `mongodb` and `redis`. `GooglePubSubSink` is a fourth shape: its `_futures_lock`
  covers the close and each publish's hand-off, not the publish.
- **Sixteen refusal-after-close sites.** `grep -rn 'is closed"' src/log_foundry/sinks` finds 13 in an
  `emit` or `SocketTransport.send_all` and 3 in a `flush` (`kafka`, `nats`, `pubsub`). The plan
  counted 12.
- **Eleven idempotent closes, in ten modules.** An AST scan finds 11 `close` methods whose first
  statement, or the first statement inside their first `with`, is `if <closed>: return`. They are
  `ClickHouseSink`, `AzureEventHubsSink`, `FileSink`, `RotatingFileSink`, `KafkaSink`,
  `MongoDBSink`, `NATSSink` (`self._loop.is_closed()`), `PostgresSink`, `RabbitMQSink`, `_RedisSink`
  and `SQLiteSink`. `SocketTransport.close` and `GooglePubSubSink.close` are unguarded: a second
  call repeats the release, which is observably harmless only because `_reset` does nothing once
  `_sock` is `None` (`_socket.py:420`) and Pub/Sub's second drain finds an empty list
  (`pubsub.py:671-674`). The plan counted 10.
- **The Kafka window.** A probe at `fda134a` used a fake producer whose `produce` parks. It ran
  `KafkaSink.emit` on one thread, waited until that call was inside `produce`, called `close()` on
  the main thread and then released `produce`. `close()` returned in 0.000 s, its flush found
  nothing, and the message stayed queued in the producer after `close()` returned, with
  `losses() == SinkLosses(dropped=0, failed=0)` and nothing on stderr. `KafkaSink`'s own docstring
  names this outcome: a message produced after the flush "dies with the process — accepted,
  uncounted".

## Scope

### In Scope

- `ClosedGuard`, its two modes, its three states and its one release (FR-001).
- `SERIAL` mode: what the lock spans, the closing rule that stops a close waiting behind a queue,
  and what a close that overlaps a call still inside the sink does, with its bound (FR-002).
- `CLOSE_ONLY` mode: concurrent sections, a close that waits for the sections inside for at most
  `CLOSE_OVERLAP_WAIT`, and the count for a section the release overtook, which closes the Kafka
  window (FR-003).
- What a closed or closing sink answers. A shipped `deliver` returns a verdict. An emit-only sink
  raises today's `SinkDeliveryError` (FR-004).
- The adoption by the eleven classes, the two that stay local and why, and the lints in
  `tests/test_sink_concurrency.py` that derive from the code this moves, re-derived so they cannot
  shrink (FR-005).
- The guard on the fork repair (FR-006).
- Prose and records at completion:
  - each adopting sink's docstrings. `MongoDBSink.close`'s "The lock does **not** make a close wait
    for an in-flight `insert_many`" becomes false and is rewritten;
  - `docs/invariants.md` entries 3, 5, 7 and 12;
  - `docs/architecture.md` §13's *`shutdown()`'s timeout bounds the drain, not the sink's
    `close()`* and *An orphan log can wait on the sink lock*;
  - `docs/component-inventory.md`, a row for `sinks/_guard.py`.
- The decision records the completion ritual writes:
  - one new decision in `docs/decisions/sink-lifecycle/` that records FR-002's and FR-003's overlap
    rule;
  - a dated History entry in `a-sink-tolerates-concurrent-callers-the-library-cannot-serialize-them-for-it`.
    Its fence is unchanged; its body gains that the lock now lives in the guard, and that a
    `CLOSE_ONLY` close waits for a bounded time;
  - `a-sink-that-released-its-transport-refuses-one-that-released-nothing-keeps-accepting` is
    **not** rewritten. Its "Refusing moves no `losses()` counter" stays true for every adopter on
    a run that **fails**, engine sinks included (FR-004), as do `README.md`'s `sink.failed`
    (attempted delivery) and SPEC-061's post-close criteria. On a run that settles, SPEC-074's rule
    counts a refused round's events once in `failed_events`, because no catcher sees a settled run.
- Six FRs, inside the 3–6 aim. The arc's step 7 needed no merge or split.

### Out of Scope

- **Bounding the release itself.** `client.close()`, `producer.flush()` or a file's `close()` stays
  as unbounded as `architecture.md` §13 records. The helper bounds only the wait for a call that is
  already inside. An interruptible close would change the sink contract.
- **`NATSSink`'s closed and closing state.** Its pid check before the lock, its `_closing` flag, its
  wedged-loop backstop that marks the sink closed from the waiting thread, and a `close()` that
  raises `TimeoutError` all stay SPEC-061's. Only the verdict its closed `deliver` returns is this
  spec's (FR-004).
- **`GooglePubSubSink`'s close.** It stays local (FR-005), with its orphaned-future count unchanged.
- **The sinks that release nothing.** The HTTP family, `SQSSink`, `SNSSink`, `KinesisSink`,
  `FirehoseSink`, the stdout, null, memory, logging and callback sinks, and the wrappers that
  forward a close keep accepting after `close()` (SPEC-032). None takes the guard.
- **Third-party sinks.** The guard is private. A third-party `deliver` that raises after its own
  close is SPEC-072's escaped exception, unchanged.
- **The library's closes.** Which close the runtime starts, when, and on which thread is
  `_lifecycle`'s, and SPEC-078's after this spec. It is not changed here.
- **The fork walk itself** (SPEC-079). FR-006 requires the guard to be reachable by it and pins that
  with a test, and adds no reach of its own.
- **`threading.RLock`.** A section that re-enters its own `SERIAL` guard deadlocks, which SPEC-028
  chose on purpose: a sink re-entering its own `emit` is a bug that an `RLock` would hide.
- **A diagnostic line for a refusal**, and a sink counter for any refusal (SPEC-032 and SPEC-069
  keep it uncounted).
- **`RunContext.refuse` itself.** It is SPEC-074's, and this spec only calls it.
- **Any public name**, or any change to `emit`'s contract, `SinkLosses`, `configure()` or `Config`.
- **Deleting a test.** No test is deleted. Tests that read a sink's `_lock` keep working through
  FR-005's read-only aliases: eight in `tests/test_fork_lifecycle.py`, and
  `tests/test_sinks_oversize_deferral.py:284` and `tests/test_socket_oversize_deferral.py:186`.
  The one that assigns it, `tests/test_fork_lifecycle.py:898` (`sink._lock = inherited`), is
  re-pointed at `sink._guard._lock`. The lints change by re-deriving their population (FR-005),
  never by deleting a case.

### Twin paths (invariant 6)

- **Worker and orphan paths** reach one sink object, and through one guard. FR-002 and FR-004 drive
  both and compare ledgers.
- **`deliver` and `emit`** of an engine sink: a refused `deliver` answers with a verdict, and the
  inherited adapter turns a failed run into the raise a 1.x `emit` caller expects (FR-004).
- **`emit` and `flush`** of `KafkaSink`: both are guarded sections with one refusal rule.
- **The two modes**: the same refusal, idempotence and single release, asserted by one parametrised
  test over both (FR-001).
- **`FileSink` and `RotatingFileSink`**, **the two Redis sinks** (one `_RedisSink` guard), and
  **`SyslogSink` and socket-mode `LogstashSink`** (one `SocketTransport` guard). Each criterion
  that names one also names its twin.

---

## Functional Requirements

### FR-001: One guard, two modes, three states, one release

#### Description:

`ClosedGuard` lives in `src/log_foundry/sinks/_guard.py`, a private leaf module that imports nothing
from the package. It is built with a `GuardMode` and holds a state: `OPEN`, then `CLOSING` from the
moment a `close()` begins, then `CLOSED` from the moment its release begins.

- **`enter()`** is a context manager that yields a `Section`. The section is **admitted** when the
  state is `OPEN`, and **refused** otherwise. A refused section holds no lock and registers nothing.
  The caller answers it before doing anything else, as FR-004 says. `enter()` itself never raises
  and never answers, because what a refusal means is the sink's (a verdict or a raise).
- **`close(release)`** runs `release` exactly once in the guard's life.
  - It sets `CLOSED` **before** `release` runs. That is the order `Sink.close`'s docstring states,
    and the order every sink above already keeps.
  - A second `close()` that finds a close running waits for that close's release to return, then
    returns without calling `release`. It never returns through a close still in flight. This is
    SPEC-050's rule applied inside one sink, and the rule `KafkaSink.close`'s docstring requires,
    so that a caller never believes a producer was flushed when it was not.
  - A `release` that raises leaves the guard `CLOSED` and propagates by identity. A later `close()`
    returns without calling it, as each sink's flag set before its release does today.
  - Idempotence is keyed on `CLOSED`, never on `CLOSING`. A closer interrupted before its release
    began (a `KeyboardInterrupt` while it waits for the lock or for `_idle`) leaves the guard
    `CLOSING`, so every `enter()` is still refused, and the **next** `close()` runs the release.
- **`closed`** reads true once a close has begun in this process, so in `CLOSING` and in `CLOSED`.
  It is what `FileSink.reacquire_after_fork` reads where it reads `self._closed` today.

The guard holds `threading.Lock`s, never an `RLock` (SPEC-028), and one `threading.Event`. It holds no
`threading.Condition`, which the fork repair does not replace (FR-006). Its state lock is a leaf. The
order is the transport lock (`_lock`), then the state lock (`_state_lock`). No counter lock is
taken while the state lock is held, so SPEC-028's order, transport then counter, is unchanged.

#### Acceptance Criteria:

- [ ] For both modes, an `OPEN` guard admits a section. After `close(release)`, every `enter()`
      yields a refused section and `closed` is true. While a close is `CLOSING`, `enter()` yields a
      refused section and `closed` is true.
- [ ] For both modes, eight threads calling `close(release)` at once call `release` exactly once.
      Each of the eight returns only after `release` has returned, which the test checks with a
      timestamp `release` records.
- [ ] A `release` that raises `RuntimeError` propagates it by identity and leaves `closed` true. A
      second `close()` returns without calling `release`. A `release` that raises
      `KeyboardInterrupt` propagates the same way.
- [ ] `enter()` raises nothing in any state. Inside a section of a `SERIAL` guard refused at the
      first read, `guard._lock.acquire(blocking=False)` succeeds. A caller queued on `_lock` and
      refused at the read after acquiring it holds no lock in its refused block: a `close()`
      blocked on the lock completes its release while that block is still running (the block waits
      for the release on an event, with a 2 s limit). A mutant that yields the refusal before
      releasing the lock times out and is killed.
- [ ] A `close()` interrupted by `KeyboardInterrupt` while it waits leaves `enter()` refusing and
      `release` uncalled. The next `close()` calls `release` once.
- [ ] An AST scan of `sinks/_guard.py` finds no import from `log_foundry`, no `RLock` and no
      `Condition`.
- [ ] Serves invariants 5, 7 and 1: the release happens once and a close in flight is waited for,
      refusal follows the release, and the guard raises nothing of its own into a caller.

### FR-002: `SERIAL` — the lock spans each section and the close; a close refuses at once and waits for the one inside

#### Description:

**What the lock spans.** In `SERIAL` mode `_lock` is held for the whole of an admitted section and
for the whole of `close`. Each adopter decides what its section covers, which is why the helper is a
context manager and not a base class:

- an engine sink (`ClickHouseSink`, `PostgresSink`, `AzureEventHubsSink`, `RabbitMQSink`,
  `SocketTransport`): one round, inside `deliver`. SPEC-074 already keeps every backoff out of the
  lock, so the lock is never held across an engine wait;
- `FileSink` and `RotatingFileSink`: a batch's writes, rotation and flush;
- `SQLiteSink`: the insert and its commit. The rows are still built outside the lock.

**The closing rule** is SPEC-061's for `NATSSink`, generalised. `threading.Lock` makes no promise of
FIFO order, so without the rule a `close()` queued behind callers takes its turn whenever the lock
favours it. So `close()` sets `CLOSING` **before** it takes `_lock`, and `enter()` reads the state
twice: before it takes `_lock`, and again after acquiring it. A caller that finds the state other
than `OPEN` at either read is refused without entering. So once a close has begun, the only section
it can wait for is the one holding `_lock` at that moment. Every later caller is refused, including
one already queued on the lock.

**What a close that overlaps a call still inside does: it waits for that section to end.** It never
releases the transport under the call. The library itself does not start a close while a drain
thread of this process may be inside the sink, because `_lifecycle`'s `held` answers from thread
liveness through `Worker.may_be_inside` (SPEC-054), and SPEC-078 restates that on every take site
later in this arc. So the calls a `SERIAL` close can overlap are the ones the runtime does not
track: an orphan log on an application thread, a `flush()` draining an owed sink, an orphan log
that reaches a sink whose detached close outlived the grace, a wrapper closing a child directly, and
the application calling `close()` itself. In each case the close waits, for three reasons:

- releasing an unshareable transport under a live call is undefined in the driver. Unlocked,
  `SQLiteSink` was measured killing the interpreter with a bus error (the SPEC-028 decision);
- a close killed partway is the partial write `architecture.md` §13 ranks worse than a leaked handle.
  Measured there, a daemon close killed inside `SQLiteSink`'s `commit()`;
- handing the release to the call still inside would let `close()` return before the release, which
  is the "returned through" shape FR-001 forbids.

**The bound.** A `SERIAL` close waits for at most **one** section (the one holding the lock when it
began), then runs its release. For an engine sink that section is one round, and the round is bounded
by the driver call's own timeout where the driver has one. For a local sink it is one batch's write.
Where the driver call has no timeout, the wait has none either. That is invariant 3's recorded
exception for `Sink.close()`, unchanged, and what bounds the exit is still `DEFAULT_CLOSER_GRACE`
for a detached close. Today a close could wait behind any number of queued callers. Now it waits
behind one.

**Orphan contention** (invariant 4, SPEC-028). An orphan caller and the drain thread enter the same
guard. The orphan caller can wait for at most the round the drain thread is inside, never for its
backoff.

#### Acceptance Criteria:

- [ ] One section is parked inside a `SERIAL` guard and five more callers are queued on its lock.
      A `close()` from another thread then returns after the parked section ends, all five callers
      are refused, and the driver double records no call from any of them. A mutant that drops the
      read after acquiring the lock lets them reach the driver and is killed.
- [ ] A caller that arrives while a `close()` waits on the lock is refused within 0.05 s, without
      blocking on the lock.
- [ ] A `close()` with no section inside returns within 0.05 s and calls `release` once.
- [ ] For each of the eight `SERIAL` adopters, a driver double counting concurrent entries never
      reads above 1, across eight threads each sending 25 batches mixed between a worker and
      orphan `info()` calls.
- [ ] `RabbitMQSink` and `ClickHouseSink` (and the same for `SocketTransport` through `SyslogSink`):
      - while the drain thread is in an engine backoff, an orphan `info()` runs its round without
        waiting on the guard, and finishes before the drain thread's next round;
      - while the drain thread is parked inside a round, the orphan call waits until that round
        ends and no longer.
- [ ] `test_close_waits_for_an_in_flight_emit_on_every_locked_sink`,
      `test_close_during_an_in_flight_emit_does_not_strand_the_writer` and
      `test_the_orphan_path_and_the_worker_emit_into_one_sink_concurrently` in
      `tests/test_sink_concurrency.py` pass unedited.
- [ ] Serves invariants 3, 4 and 5: a close waits for at most one section and never releases under
      one, an orphan caller waits for at most one round of the drain thread, and the transport is
      released once.

### FR-003: `CLOSE_ONLY` — sections run concurrently; a close refuses at once, waits a bounded time, then releases

#### Description:

`MongoDBSink`, the two Redis sinks (through `_RedisSink`) and `KafkaSink` lock only the close on
purpose. Their clients are thread-safe, and a lock across I/O would remove the parallelism that
SPEC-028 kept. Today their close does not wait at all. That is how the Kafka window in the Overview
loses a message.

In `CLOSE_ONLY` mode an admitted section takes no lock across its body. It is counted in `_inside`
under the state lock, and the `_idle` event is cleared while any section is inside. `close()` takes
`_lock` for its whole run, so a second closer waits as FR-001 says. Then:

1. It sets `CLOSING` under the state lock. From then on `enter()` refuses.
2. If a section is inside, it waits on `_idle` for at most `CLOSE_OVERLAP_WAIT = 0.5` seconds. The
   wait is an event wait, so it is idle, and it ends early when the last section leaves.
3. It sets `CLOSED` and runs `release`, whether or not a section is still inside.

**The bound** is `CLOSE_OVERLAP_WAIT` plus the release. For Kafka 0.5 s is measured headroom:
`produce()` is a local hand-off, measured in this sink's docstring at 0.0001 s for three messages.
For MongoDB and Redis it is a chosen bound, not a measured one. A round against a reachable server
returns in the client's round trip, well inside it. A round that outlives it is the call the bound
exists to stop waiting for, and that call's outcome is still counted (below). 0.5 s is a quarter of
`DEFAULT_CLOSER_GRACE`. That leaves room for the release inside a full grace, but it is no
guarantee: a detached close's grace is `min(DEFAULT_CLOSER_GRACE, deadline − now)`
(`_lifecycle.py:1244`), so with less than 0.5 s of budget left the overlap wait alone can outlast
it. The constant is private, a module constant of `_guard.py`, with that reason in its docstring.

**A section still inside when the release runs is told so.** After its `with` block exits,
`Section.released_during` is true when the guard reached `CLOSED` while the section was admitted. A
`SERIAL` section always reads false. What each adopter does with that:

- **`KafkaSink`** counts the messages that call handed to the producer in `failed` and writes one
  `_diag.lost` line naming the count and "produced while the sink was closing". The count covers
  every message that call produced, including those produced before the release began, which the
  release's flush may have delivered. It is therefore the upper bound invariant 2 records for
  `failed`, not an exact loss, and nothing that call accepted is uncounted. The store is immediate, beside `_on_delivery`'s, and
  `tests/test_deferral.py`'s `IMMEDIATE` names it for `("KafkaSink", "emit")`.
- **`MongoDBSink` and the Redis sinks** need no flag. A call still inside meets a client the release
  has closed. What the client then raises is classified by SPEC-070's `_retryable_error` and counted
  by the engine like any other round. The next round's `enter()` is refused (FR-004), so no round
  after the release reaches the client. The two differ in what is counted:
  - `pymongo` raises `InvalidOperation` on use after close. That is not a `ConnectionFailure` and
    carries no `RetryableWriteError` label, so SPEC-070 makes it `REJECTED`. It is an attempted
    delivery, counted in `.failed`.
  - `redis-py` raises `redis.exceptions.ConnectionError`, which SPEC-070 makes `RETRYABLE`. The next
    round is refused, and a failed run commits nothing for a refused round (SPEC-074), so `.failed`
    stays 0 and the catcher counts the batch. For Redis, a call that outlives the bound can reopen a
  connection that nothing reaps, which is the leak the SPEC-032 decision records for a post-close
  emit. That cost is accepted against waiting without a bound on a call the client gives no
  timeout.

#### Acceptance Criteria:

- [ ] With one section parked inside a `CLOSE_ONLY` guard, `close()` refuses a new `enter()` at
      once. If the section leaves 0.2 s later, `close()` runs `release` after it leaves, in under
      0.3 s. If it never leaves, `release` runs between 0.45 s and 0.75 s after `close()` began.
      Each wait uses under 0.05 s of CPU, measured through `tests/test_worker_exit_budget.py`'s
      helper. A mutant that skips the wait runs `release` at once and is killed.
- [ ] Two sections run their bodies at the same time inside one `CLOSE_ONLY` guard, and a third
      thread's `close()` waits for both to leave.
- [ ] The Overview's Kafka probe against the adopted sink, in two variants:
      - the probe releases the parked `produce` 0.1 s after `close()` began: `close()` returns
        in under 0.3 s, the producer has flushed the message, `losses().failed == 0`, and nothing
        is written to stderr;
      - the probe releases `produce` only after `close()` returns, which is its order at
        `fda134a`: `close()` takes between 0.45 s and 0.75 s, then `losses().failed == 1` and one
        stderr line names the count. A mutant that ignores `released_during` leaves `failed == 0`
        and is killed.
- [ ] `KafkaSink.flush()` is a section. Called while a close waits, it is refused with today's
      message, and a `close()` called while a `flush()` is inside waits for it up to the bound.
- [ ] Through a `Worker`, a round is parked past the bound, then fails on the released client.
      Nothing raises into the calling thread, and no later round makes a client call:
      - `MongoDBSink`, the double raising `pymongo.errors.InvalidOperation`: `.failed == n` and
        `failed_batches == 1`;
      - `RedisListSink`, the double raising `redis.exceptions.ConnectionError`: `.failed == 0` and
        `failed_batches == 1`.
- [ ] `tests/test_deferral.py` passes with `("KafkaSink", "emit")` added to `IMMEDIATE` and no
      floor lowered, and `tests/test_diag.py`'s roster covers the new line.
- [ ] Serves invariants 2, 3 and 5: an event a call handed over before the release is delivered or
      counted, a close waits idle for at most `CLOSE_OVERLAP_WAIT` plus its release, and the release
      runs once.

### FR-004: A closed or closing sink answers without reaching its transport

#### Description:

Every guarded member opens with the same three lines: `with self._guard.enter() as section:`, then
`if not section:`, then the sink's answer. No other statement comes before the answer, so a refused
call does no I/O.

**An engine sink's `deliver` keeps SPEC-074's answer.** SPEC-074 already answers a closed or closing
engine sink with a `REJECTED` verdict and `RunContext.refuse(error)`, `NATSSink` included. This spec
does not change that answer. It moves the answer behind the guard's refused branch, so it also
covers the guard's `CLOSING` state, from the moment a `close()` begins. On a refused section the
sink calls `run_context().refuse(error)`, where `error` is the `SinkDeliveryError` its 1.x
`emit` raised for a closed sink, with the same message (SPEC-061's closed error for `NATSSink`). It
then returns `Verdict.uniform(Outcome.REJECTED, len(batch))`, because no resend can change a closed
sink, and makes no transport call. This covers the five `SERIAL` engine adopters, `MongoDBSink`,
`_RedisSink`, and `NATSSink` from its own closed or closing state, which stays SPEC-061's and
local. Nothing escapes `deliver`, so SPEC-072's rule for an escaped exception is untouched. A closed sink then:

- costs one `deliver` call and is never offered again within the run, so it spends none of
  SPEC-072's worker-path budget of `(s + 1) × (w + 1) − 1` further rounds;
- writes no `_diag.absorbed` line naming `SinkDeliveryError` for a refused round;
- fails the run when nothing was accepted, so `flush()` is falsy, the worker moves
  `failed_batches`, the orphan path moves `orphan_lost`, and the inherited adapter raises `error`
  **by identity**, so a 1.x `emit` caller reads today's message;
- on that failed run moves **no sink counter**. `failed_events`, `.failed` and `losses()` stay as
  they were, so SPEC-032's rule (a refusal reported to the catcher is not also counted) and
  `README.md`'s `sink.failed`, which counts attempted delivery, stay true. The catcher's term is
  the refused events' one count.

A round refused after an earlier round of the same run landed events leaves a run that **settles**.
No catcher sees a settled run, so SPEC-074 counts the refused round's events once in
`failed_events` with one line, and invariant 2 keeps them from going uncounted.

**An emit-only adopter raises its 1.x refusal.** `FileSink`, `RotatingFileSink`, `SQLiteSink` and
`KafkaSink` raise a plain `SinkDeliveryError` with today's message from `emit`, and `KafkaSink` raises
its own from `flush`. None counts it, which is SPEC-032's rule, and SPEC-069's for the local sinks.

**An empty batch** returns before the guard is entered, closed or not (SPEC-032).

#### Acceptance Criteria:

- [ ] For each of the eight engine sinks above, `deliver` after `close()`, and again while a
      `close()` is `CLOSING`, calls `RunContext.refuse` with a `SinkDeliveryError` carrying the
      sink's fda134a closed message and returns `Verdict.uniform(Outcome.REJECTED, n)`, and the
      driver double records no call.
- [ ] For each of those eight, after `close()`:
      - through a `Worker` the batch costs exactly one `deliver` call, `failed_batches == 1` and
        `flush()` is falsy;
      - an orphan `info()` gives `orphan_lost == 1`;
      - `emit` raises, by identity, the error `deliver` passed to `refuse`, with its fda134a
        message. `SocketTransport` has no `emit`, so for it the call is `SyslogSink.emit`, and
        socket-mode `LogstashSink.emit` gives the same;
      - in every case `failed_events`, `.failed` and `losses()` are unchanged, and stderr contains
        no `SinkDeliveryError`. A mutant that drops the `refuse` call moves `.failed` and is
        killed.
- [ ] For each of the four emit-only adopters, `emit` after `close()` raises an exception whose
      type is exactly `SinkDeliveryError`, with its fda134a message unchanged. `losses()` is
      unchanged and the driver records no call. `KafkaSink.flush()` after `close()` raises
      `KafkaSink cannot flush: the sink is closed`.
- [ ] An engine sink closed between two rounds of a run that accepted events in round one makes no
      client call in round two and offers no event twice. The run settles, `flush()` is truthy,
      and the refused round's events are counted once in `failed_events` with one line.
- [ ] `emit([])` returns `None` on every adopter, open and closed.
- [ ] `test_a_closed_sink_refuses_a_batch_instead_of_reaching_its_driver` and
      `test_a_closed_sink_still_treats_an_empty_batch_as_a_no_op` pass for every key of
      `POST_CLOSE_BUILDERS` as SPEC-074 leaves them, with no edit by this spec.
- [ ] Twin paths: the worker path and the orphan path leave the same `SinkLosses` for one event
      sent to each closed engine sink.
- [ ] Serves invariants 7, 2 and 6: a sink that released its transport refuses, every refused event
      is counted once (by its catcher on a failed run, never also by the sink, and by the engine
      only on a settled run), and `deliver` and `emit`, worker and orphan, keep one ledger.

### FR-005: Eleven classes adopt the guard, two stay local, and the lints that derive from this code are re-derived

#### Description:

**Adopting, each at the one attribute `self._guard`:**

| Mode | Classes |
|---|---|
| `SERIAL` | `ClickHouseSink`, `PostgresSink`, `AzureEventHubsSink`, `RabbitMQSink`, `SocketTransport`, `FileSink`, `RotatingFileSink`, `SQLiteSink` |
| `CLOSE_ONLY` | `MongoDBSink`, `_RedisSink`, `KafkaSink` |

Each adopter deletes its `_closed` flag, its `_lock` or `_close_lock`, and its closed checks and
returns. `SocketTransport.close` is guarded rather than relying on `_reset` doing nothing the
second time. That accounts for twelve of the Overview's sixteen refusal sites and ten of its
eleven idempotent closes.

**The 1.x names stay readable** (orchestrator's decision). `README.md` documents subclassing a
shipped sink, and a 1.x subclass that reads `self._closed` or takes `with self._lock:` would
otherwise get `AttributeError`. So every adopter keeps read-only properties under its fda134a
names:

- `_closed` returns `self._guard.closed`;
- `_lock` on the eight `SERIAL` adopters, and `_close_lock` on the three `CLOSE_ONLY` adopters (the
  name each used at `fda134a`), return the guard's `_lock`.

A subclass that takes that lock around its own work serialises with the guard's sections in
`SERIAL` mode, as it did with the 1.x lock. An assignment to either name raises `AttributeError`.
The aliases are properties, never stored attributes, so the stores the lints look for stay absent.
Taking the guard's `_lock` for the property also keeps the tests that read `sink._lock` working
(*Out of Scope*, *Deleting a test*). `SyslogSink` and socket-mode
`LogstashSink` refuse through the `SocketTransport` they hold, as they do today.

**Staying local, each with its reason in its class docstring:**

- **`NATSSink`.** Its closed state is read from the event loop and from SPEC-061's flags, and its pid
  check runs **before** its lock, because its lock may be one the fork repair never reached. Its
  wedged-loop backstop marks it closed from a waiting caller, and its `close()` raises
  `TimeoutError`. None of that is a section or a release, and SPEC-061 owns it. It keeps its two
  refusal sites and its idempotent close, and answers a closed `deliver` as FR-004 says.
- **`GooglePubSubSink`.** It is not an engine sink, and its close is deliberately absorbed
  mid-batch. A publish whose hand-off finds the sink closed is counted under `_futures_lock` as an
  orphaned future, and `close()` drains what is pending. Its overlap rule is that count, which the
  plan records as a deliberate, documented difference. It keeps its two refusal sites.

**The lints are re-derived, never thinned.** `tests/test_sink_concurrency.py` derives two
judgements from the code this spec moves:

- `_takes_the_transport_lock` looks for `with self._lock`;
- `_may_not_claim_it_accepts` looks for a stored `self._closed`.

Both would go blind to all eleven adopters, which is the silent roster shrink SPEC-071 pins floors
against. So:

- `_takes_the_transport_lock`, which SPEC-074 re-aims at `deliver` with a floor of the six locked
  engine classes, also accepts `with self._guard.enter()` when the class's `__init__` builds
  `self._guard` with `GuardMode.SERIAL`. It still reads `emit` for a class without `deliver`, so
  `FileSink`, `RotatingFileSink` and `SQLiteSink` are found through it. SPEC-074's six stay found:
  five through their guard, and `NATSSink` through its own `self._lock`.
- A `CLOSE_ONLY` adopter keeps the docstring claim `**no** transport lock` that it makes today.
- `_may_not_claim_it_accepts` also bars a class that builds a `ClosedGuard`.
- A new AST check holds the opening rule of FR-004.

#### Acceptance Criteria:

- [ ] An AST scan of `src/log_foundry/sinks` finds exactly the eleven classes above building a
      `ClosedGuard` at `self._guard`, eight `SERIAL` and three `CLOSE_ONLY`. It finds no store to
      `_closed` or `_closing` in any class but `NATSSink` and `GooglePubSubSink`, and both name
      their reason in their docstring.
- [ ] No adopter assigns `self._lock` or `self._close_lock`. Within the eleven adopters,
      `grep -n 'is closed"'` finds each of their twelve fda134a closed messages exactly once, inside
      a refused branch: the five emit-only raises (four `emit`s and `KafkaSink.flush`) and the seven
      errors passed to `RunContext.refuse`.
- [ ] `test_every_driver_backed_sink_records_a_concurrency_decision` passes, and SPEC-074's floor
      of six locked engine classes holds, plus the three local `SERIAL` sinks. A fixture `SERIAL`
      adopter whose `enter()` is deleted from its `deliver` is reported. A fixture `CLOSE_ONLY`
      adopter without the `**no** transport lock` claim is reported.
- [ ] `test_every_sink_class_records_a_post_close_decision` passes. A fixture class that builds a
      `ClosedGuard` and claims `**adds no post-close guard**` is reported.
- [ ] An AST check finds `if not section:` as the first statement inside every
      `with self._guard.enter() as section:`. Its body is either one `raise`, or one
      `run_context().refuse(...)` call followed by one `return`, as the contract writes it. A fixture with a driver call before the
      check, or inside its body, is reported.
- [ ] The floors are kept:
      - `test_the_sink_roster_is_not_empty_and_has_not_collapsed` keeps its floor of 34;
      - `POST_CLOSE_BUILDERS` keeps its 15 keys;
      - `_EXPECTED_CLOSERS` in `tests/test_sink_release_roster.py` is unchanged;
      - no floor in `tests/test_deferral.py` or `tests/test_worker_predicate_roster.py` is lowered.
- [ ] For each adopter, a subclass reading `self._closed` sees `False` before `close()` and `True`
      after it. For a `SERIAL` adopter, a subclass method holding `with self._lock:` keeps a
      concurrent section waiting until it leaves. For a `CLOSE_ONLY` adopter, `self._close_lock` is
      the guard's `_lock`. Assigning `_closed`, `_lock` or `_close_lock` raises `AttributeError`.
      `tests/test_fork_lifecycle.py`, `tests/test_sinks_oversize_deferral.py` and
      `tests/test_socket_oversize_deferral.py` pass with only the line-898 edit.
- [ ] Every sink that releases nothing (the Out of Scope list) still delivers a batch after
      `close()`, and none builds a `ClosedGuard`.
- [ ] Serves invariants 7, 5 and 6: which sinks refuse is still enforced from the code, each
      adopter's close is once-only, and twins share one guard.

### FR-006: The guard is repaired in a forked child

#### Description:

A guard's lock left held by a parent thread would hang the child on its first log call, or its first
close. Its state would also tell the child about callers that do not exist there. The repair has two
halves.

**Its primitives are on the walk.** `_lock`, `_state_lock` and `_idle` are plain `threading.Lock`
and `threading.Event` attributes of the guard. The guard is a package type held directly at
`self._guard` by a shipped sink. So `_fork._reinit_primitives` enters it, as it enters every
package-owned holder it reaches today, and replaces each primitive, and an `Event` keeps its set
state. No new reach is needed. That is unlike SPEC-072's `log_foundry_counters`, which needed a named
reach because a third-party sink is not entered. This FR's fork test is driven through `configure()`,
so it reddens if a later walk from the runtime's roots (SPEC-079) stops at the sink rather than
entering its `_guard`.

**Its state is adopted lazily, by pid.** The guard records `os.getpid()` when it is built. Every
member, `closed` included, first compares it with the current pid under the repaired state lock.
When they differ, the guard adopts the child's state:

- `_inside` becomes 0 and `_idle` is set, because no parent thread is inside in the child;
- `CLOSING` becomes `OPEN`, because no parent closer runs in the child, and a close that had not yet
  set `CLOSED` had not begun its release. That is exactly what the child saw in 1.x, where the flag
  was set only inside the lock;
- `CLOSED` stays `CLOSED`, so a sink whose release had begun in the parent refuses in the child, as
  in 1.x.

The adoption runs before `FileSink.reacquire_after_fork` reads `closed`, because the handlers run
after the locks (invariant 12's order).

#### Acceptance Criteria:

- [ ] A sink is configured through `configure()`, and a child is forked under `signal.alarm` and a
      parent-side kill, as `tests/test_fork_lifecycle.py` does, while a parent thread holds a
      `SERIAL` adopter's guard lock inside a parked section. The child's `info()` delivers within
      5 s, its `close()` completes, and its `_guard._lock` is a different object from the parent's.
      The same holds with the parent thread holding `_state_lock`.
- [ ] A child forked while a parent section is inside a `CLOSE_ONLY` guard closes it in under
      0.25 s. A mutant without the pid adoption waits `CLOSE_OVERLAP_WAIT` and is killed.
- [ ] A child forked while a parent `close()` is `CLOSING` (waiting on the lock) admits sections. A
      child forked after the parent's close set `CLOSED` refuses them.
- [ ] A `FileSink` closed before the fork re-acquires nothing in the child, and the parent's next
      `emit` after the fork is accepted.
- [ ] Serves invariant 12: a forked child never hangs on a guard lock its parent held, and adopts no
      caller or closer that exists only in the parent.

---

## Data Model

```
// src/log_foundry/sinks/_guard.py  -- private; imports nothing from log_foundry
class GuardMode(enum.Enum):
    SERIAL = "serial"            # one lock spans every admitted section and the close
    CLOSE_ONLY = "close-only"    # sections run concurrently; the lock spans the close only

CLOSE_OVERLAP_WAIT: float = 0.5  # CLOSE_ONLY: the longest a close waits for sections inside

class _State(enum.Enum):
    OPEN = "open"
    CLOSING = "closing"          # a close has begun; every enter() is refused
    CLOSED = "closed"            # the release has begun or finished

class Section:
    admitted: bool               # __bool__ returns it
    released_during: bool        # read after the with-block: CLOSED was reached while admitted;
                                 # always False in SERIAL

class ClosedGuard:
    _mode: GuardMode
    _lock: threading.Lock        # SERIAL: each admitted section and the close; CLOSE_ONLY: the close
    _state_lock: threading.Lock  # leaf: _state, _inside, _pid
    _idle: threading.Event       # set while no CLOSE_ONLY section is inside
    _state: _State
    _inside: int
    _pid: int                    # os.getpid() at construction; a mismatch adopts the child (FR-006)

    def __init__(self, mode: GuardMode) -> None: ...
    @property
    def closed(self) -> bool: ...                      # CLOSING or CLOSED, after adoption
    @contextlib.contextmanager
    def enter(self) -> Iterator[Section]: ...          # never raises
    def close(self, release: Callable[[], None]) -> None: ...   # raises only what release raises
```

## API / Interface Contract

The shapes an adopter writes:

```python
# an engine sink (SERIAL here; CLOSE_ONLY is the same text)
def deliver(self, batch: list[dict[str, object]]) -> Verdict:
    with self._guard.enter() as section:
        if not section:
            run_context().refuse(SinkDeliveryError(
                f"ClickHouseSink inserted none of {len(batch)} event(s): the sink is closed"
            ))
            return Verdict.uniform(Outcome.REJECTED, len(batch))
        ...  # one round of I/O

def close(self) -> None:
    self._guard.close(self._release)

# an emit-only sink
def emit(self, batch: list[dict[str, object]]) -> None:
    if not batch:
        return
    rows = self._rows(batch)                   # SQLiteSink: built outside the lock
    with self._guard.enter() as section:
        if not section:
            raise SinkDeliveryError(
                f"SQLiteSink inserted none of {len(batch)} event(s): the sink is closed"
            )
        self._insert(rows)

# KafkaSink.emit: the section the release overtook
with self._guard.enter() as section:
    if not section:
        raise SinkDeliveryError(...)
    accepted = self._produce_all(batch, pending)
if section.released_during and accepted:
    ...  # count `accepted` in failed, one _diag.lost line
```

## Configuration / Environment

None. `CLOSE_OVERLAP_WAIT` is a module constant of `_guard.py`, unreachable from `configure()`.

## File & Folder Structure

```
src/log_foundry/sinks/
├── _guard.py           # GuardMode, CLOSE_OVERLAP_WAIT, Section, ClosedGuard (new, private)
├── clickhouse.py       # SERIAL
├── postgres.py         # SERIAL
├── eventhubs.py        # SERIAL
├── rabbitmq.py         # SERIAL
├── _socket.py          # SERIAL; close() becomes idempotent
├── file.py             # SERIAL, FileSink and RotatingFileSink
├── sqlite.py           # SERIAL, rows built outside the section
├── mongodb.py          # CLOSE_ONLY
├── redis.py            # CLOSE_ONLY, on _RedisSink
├── kafka.py            # CLOSE_ONLY; emit and flush are sections; the overtaken-section count
└── nats.py             # local state kept; a closed deliver answers through RunContext.refuse

tests/
├── test_closed_guard.py         # FR-001 to FR-003 against doubles, both modes parametrised
├── test_sink_concurrency.py     # FR-002's adopter cases; FR-005's re-derived lints and fixtures
├── test_sink_post_close.py      # FR-004 across the adopters, worker and orphan
├── test_fork_lifecycle.py       # + FR-006; :898 re-pointed at sink._guard._lock
├── test_sinks_kafka.py          # + FR-003's window
└── test_deferral.py             # + ("KafkaSink", "emit") in IMMEDIATE
```

## Implementation Phases

### Phase 1: The helper

- `sinks/_guard.py` with both modes, the pid adoption and `CLOSE_OVERLAP_WAIT`.
- `tests/test_closed_guard.py`: FR-001, FR-002's and FR-003's guard-level criteria, against
  doubles.

### Phase 2: The `SERIAL` adopters

- The eight `SERIAL` classes, each `deliver` or `emit` opening with FR-004's three lines, and each
  close through `self._guard.close`.
- FR-002's adopter criteria, FR-004's for these sinks, and `NATSSink`'s closed `deliver` verdict.

### Phase 3: The `CLOSE_ONLY` adopters

- `MongoDBSink`, `_RedisSink` and `KafkaSink`, with Kafka's `flush` as a section and the
  overtaken-section count.
- FR-003's adopter criteria and the rest of FR-004.

### Phase 4: Lints, fork, prose and records

- FR-005's lint changes with their fixtures, and the floor checks.
- FR-006's fork cases.
- The docstrings, the `invariants.md` entries, the `architecture.md` §13 paragraphs, the inventory
  row, and the decision records named under *In Scope*.

## Revision history

- Drafted against `fda134a` from the arc plan's Change 3 row "Transport lock, idempotent close,
  refusal after close", the paragraph after that table, and the Risks bullet "An emit that times out
  is still running".
- Departures from the plan, each re-measured:
  - the counts (sixteen refusal sites, not 12; eleven idempotent closes, not 10; five locks held
    across a backoff and four across I/O only, not six);
  - `SQLiteSink` fits the helper after all, because the helper is a context manager and not a base
    class;
  - `NATSSink` and `GooglePubSubSink` stay local;
  - the overlap the plan foresaw resolves differently by mode: a `SERIAL` close waits for the one
    section inside, and a `CLOSE_ONLY` close waits at most `CLOSE_OVERLAP_WAIT`;
  - the Kafka window was found while drafting and is closed by that wait, with the overtaken
    section counted.
- Revised on the orchestrator's decision before review: a refusal must not move `.failed`. A closed
  engine sink's `deliver` answered `Verdict.uniform(Outcome.REJECTED, n)` in the first draft, which
  the engine counts in `failed_events`. It now answers through SPEC-074's `RunContext.refuse(error)`,
  so a failed run commits no sink counter and the adapter raises the 1.x closed error by identity,
  and the SPEC-032 decision is no longer rewritten. Re-read
  against SPEC-072's 1.x-total budget and SPEC-078 FR-005; neither changes this spec's rules.
- Spec review, first round (seven should-fix findings and the nits, each fixed):
  - FR-003's Kafka criterion now names when the probe releases `produce` in each variant;
  - the tests that read or assign a sink's `_lock` are named, with read-only `_closed`, `_lock` and
    `_close_lock` aliases added on the orchestrator's decision for 1.x subclasses;
  - "Refusing moves no `losses()` counter" is limited to failed runs;
  - `SocketTransport.close` and `GooglePubSubSink.close` are restated as unguarded, not
    non-idempotent, and the `_reset` call-count criterion was dropped;
  - FR-004 says it keeps SPEC-074's answer and adds `CLOSING`;
  - the Redis and MongoDB criteria name the exception and the counter;
  - nits: the post-acquire refusal path is tested, an interrupted closer is defined, the grace
    claim is narrowed and the 0.5 s bound justified, Kafka's count is stated as an upper bound,
    and the AST check reads `run_context().refuse`.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
