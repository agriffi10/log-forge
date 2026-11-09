---
type: Decision
title: "A closing sink refuses at once; a SERIAL close waits for the one call inside, a CLOSE_ONLY close waits a bounded time"
description: "a close sets `CLOSING` before it takes the transport lock, so every later caller is refused; a `SERIAL` close then waits for the one section holding the lock and never releases under it, and a `CLOSE_ONLY` close waits at most `CLOSE_OVERLAP_WAIT` (0.5 s) and releases, telling the section it overtook (`released_during`), which `KafkaSink` counts. Never an `RLock`; never a release handed to the call inside. (SPEC-076)"
---

**What a close does when it overlaps a call still inside the sink**, for the eleven sinks that
share `sinks/_guard.py`'s `ClosedGuard`. The guard is `OPEN`, then `CLOSING` from the moment a
`close()` begins, then `CLOSED` from the moment its one release begins. A close sets `CLOSING`
under the state lock **before** it takes the transport lock, and every `enter()` outside `OPEN` is
refused holding no lock. `threading.Lock` promises no FIFO order, so a close queued behind callers
used to take its turn whenever the lock favoured it. Now a `SERIAL` `enter()` reads the state before
taking the lock and again after acquiring it, and a caller already queued on the lock is refused
too. This is SPEC-061's closing flag for `NATSSink`, generalised. Idempotence is keyed on `CLOSED`,
never `CLOSING`: a closer interrupted before its release began leaves every section refused, and
the next close runs the release.

**`SERIAL` waits for the one section inside, and never releases under it.** The library does not
start a close while its own drain thread may be inside the sink (`Worker.may_be_inside`, SPEC-054),
so the calls a close can overlap are the ones the runtime does not track: an orphan log, a
`flush()` draining an owed sink, a wrapper closing a child, the application's own `close()`. The
close waits for them because the alternatives are worse. Releasing an unshareable transport under a
live call is undefined in the driver, and unlocked `SQLiteSink` was measured killing the interpreter
with a bus error (*A sink tolerates concurrent callers*). A close killed partway is the partial
write `architecture.md` §13 ranks below a leaked handle. The wait is bounded by **one** section: one
engine round, bounded by the driver call's own timeout where it has one, or one batch's write.
Where the driver call has no timeout the wait has none either, which is invariant 3's recorded
exception for `Sink.close()`, still bounded for the exit by `DEFAULT_CLOSER_GRACE`.

**`CLOSE_ONLY` waits at most `CLOSE_OVERLAP_WAIT`, then releases.** `MongoDBSink`, the Redis sinks
and `KafkaSink` keep their clients' parallelism (SPEC-028), so a section takes no lock across its
body and is counted under the state lock instead. A close waits on an event for the sections
inside, idle, for at most 0.5 s, then sets `CLOSED` and releases whether or not one is still
inside. For Kafka the bound is measured headroom over a local hand-off. For MongoDB and Redis it is
chosen: a round against a reachable server finishes well inside it, and a round that outlives it is
the call the bound exists to stop waiting for. 0.5 s is a quarter of `DEFAULT_CLOSER_GRACE`, which
leaves room for the release inside a full grace and promises nothing once less remains. A section
the release overtook reads `released_during` after its `with` block. `KafkaSink` counts every
message that `emit` handed to the producer in `failed`, with the line `lost N message(s);
KafkaSink, produced while the sink was closing`. That is invariant 2's upper bound, since the
release's flush may have delivered some. MongoDB and Redis need no flag: the overtaken call meets
a closed client, the engine classifies what it raises like any other round, and the next round is
refused. `pymongo`'s `InvalidOperation` is `REJECTED` and counted in `.failed`. `redis-py`'s
`ConnectionError` is `RETRYABLE`, so the catcher counts the batch.

**Rejected:**

- **A `threading.RLock`.** A section that re-enters its own `SERIAL` guard deadlocks, as SPEC-028
  chose: a sink re-entering its own `emit` is a bug an `RLock` would hide.
- **Releasing under the call inside, for `SERIAL`.** That is the bus error and the partial write
  above.
- **Handing the release to the call inside**, so the close returns at once and the last section
  out releases. `close()` would then return before the release, which is the "returned through"
  shape the guard forbids: a caller would believe a producer was flushed when it was not.
- **An unbounded `CLOSE_ONLY` wait.** A `redis-py` call is bounded only by the client's own socket timeout,
  which the library neither sets nor sees, so a close could wait as long as that call, and a detached close would always spend its whole grace. The
  accepted cost is that a Redis call past the bound can reopen a connection nothing reaps, the leak
  the SPEC-032 decision records.

**Do NOT** set `CLOSING` after taking the transport lock; do NOT key idempotence on `CLOSING`; do
NOT let a refused section hold a lock or reach the transport; do NOT bound the release itself here
(an interruptible close would change the sink contract); do NOT give the guard an `emit`, which
`_lifecycle` would then record as a sink. `NATSSink` and `GooglePubSubSink` keep their own rules,
SPEC-061's and the orphaned-future count. (SPEC-076 FR-001 to FR-003)

## History

### 2026-11-09

- **Settled** by SPEC-076 FR-001 to FR-003: the closing rule for both modes, the `SERIAL` one-section
  wait, the `CLOSE_ONLY` bounded wait and the count for a section the release overtook, which closes
  the Kafka window the spec's probe found.
