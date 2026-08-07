# Spec: The Sink Concurrency Contract

**ID:** SPEC-028  
**Status:** Draft  
**Last Updated:** 2026-08-05  
**Depends On:** SPEC-002, SPEC-004, SPEC-008

## Overview

Sinks were designed for one caller. `sinks/file.py:9-10` says so outright — "a single-process,
single-worker-thread writer is assumed (arch §9)" — and arch §9's single drain thread is what makes
that assumption look safe.

It is not true today. The orphan path emits on the **caller's** thread (`api.py:62`), against the
same sink object the worker thread is draining into, and the caller's thread may be any of the
application's. An audit probe with two application threads calling `log_foundry.info()` alongside
traced calls observed `emit` entered concurrently by `app-1`, `app-2` and `log-foundry-worker`, with
overlapping calls.

What that breaks is not theoretical. `RotatingFileSink._rotate` (`file.py:152-175`) rebinds
`self._stream` and `self._size` with no lock, so a concurrent write can go to a closed file or to
the pre-rotation one. `SocketTransport` shares one TCP socket, and two interleaved `sendall` calls
corrupt octet-counted syslog framing into a stream the receiver cannot resynchronize. `PostgresSink`
shares one psycopg connection across a `cursor` / `commit` / `rollback` sequence that assumes it owns
the transaction. `SQLiteSink` passes `check_same_thread=False` and then relies on `with self._conn`.
Every sink's `self.failed += 1` is a non-atomic read-modify-write, so the counters SPEC-026 surfaced
can undercount.

**Amended by evidence during the build:** `SQLiteSink` is worse than "loses rows". The concurrency
test written for FR-004 kills the interpreter outright against the unlocked sink — a reproducible
`Fatal Python error: Bus error` from concurrent use of one `sqlite3` connection, not an exception
the caller could catch. That puts this defect in breach of the library's first promise (logging
never breaks the application), not merely its loss-reporting one, and is recorded as a constraint
in `architecture.md` §13 where the lock's cost is weighed against it.

`sinks/base.py` — the entire interface definition — states no concurrency contract at all, so a
third-party sink author has nothing to write against either.

This spec decides the contract and makes the shipped sinks satisfy it.

## Scope

### In Scope

- Stating the concurrency contract in `sinks/base.py`: `emit` and `close` may be called from more
  than one thread and must tolerate it.
- Serializing the shipped sinks that hold mutable transport state.
- Making the loss counters safe to increment and read concurrently.
- Correcting `file.py`'s single-writer claim and `architecture.md` §9.
- Tests that actually run concurrent emitters, which the suite has none of.

### Out of Scope

- **Removing the orphan path's synchronous emit.** Settled in `architecture.md` §12 Resolved. This
  spec takes the concurrency as given and makes the sinks correct under it, rather than reopening
  the decision that produced it.
- **Parallelism as a feature.** The contract is "correct under concurrent calls", not "faster under
  them". A single lock per sink is the intended shape; per-connection pools are a different design
  and are not built here.
- **Process-level concurrency.** Two processes appending to one file, or writing one SQLite
  database, stays out of scope exactly as `file.py` and `sqlite.py` already say.
- **Reordering guarantees.** Concurrent callers may interleave batches; nothing here promises an
  order across threads. Within one `emit` call, order is preserved as it is today, and SPEC-016's
  FIFO group ordering is unaffected.
- **`MemorySink`, `NullSink`, `StdoutSink`.** Their operations are already effectively atomic for
  the shapes they use (a `list.extend`, an `int` add, a buffered stream write); they gain the
  counter fix from FR-003 and nothing else. Stated so an implementer does not lock them
  unnecessarily.

---

## Functional Requirements

### FR-001: The contract is stated

#### Description:

`sinks/base.py` documents that a `Sink` must tolerate `emit` being called concurrently from
multiple threads, and `close` being called while an `emit` is in flight. This is the interface
document a third-party sink is written against, and the reason this defect was invisible is that it
said nothing.

The contract is stated as a requirement on implementations, not a promise the library makes to
serialize on their behalf — the library cannot, because the orphan path runs on a thread it does not
own.

#### Acceptance Criteria:

- [ ] `Sink.emit`'s docstring states that concurrent calls from multiple threads are possible and
      must be tolerated, and names the orphan path as the reason.
- [ ] `Sink.close`'s docstring states it may be called while an `emit` is in flight, and that it
      must not leave a half-released resource that a concurrent `emit` will use.
- [ ] `architecture.md` §9's single-drain-thread statement is qualified with the orphan path.
- [ ] `file.py`'s "a single-process, single-worker-thread writer is assumed" is corrected to the
      delivered behaviour.
- [ ] The README's sink-author guidance states the requirement.

### FR-002: Sinks holding mutable transport state serialize their use of it

#### Description:

Each shipped sink that mutates transport state during `emit` — a stream it may rebind, a socket it
reuses, a connection with transaction scope — guards that state with a lock held for the duration of
the operation that assumes exclusivity.

The lock is per sink instance and is not re-entrant across sinks: `MultiSink` must not hold a lock
while calling into children, or a fan-out would serialize on the slowest destination beyond what its
children already require.

#### Acceptance Criteria:

- [ ] `RotatingFileSink` performs a rotation without a concurrent writer observing a closed or
      pre-rotation stream; concurrent emits from several threads produce a file whose lines are all
      intact and individually well-formed JSON.
- [ ] `FileSink` likewise.
- [ ] `SocketTransport` sends each framed message without interleaving another thread's bytes; a
      concurrent syslog test yields frames a receiver can parse in full.
- [ ] `PostgresSink` completes a `cursor`/`commit`/`rollback` sequence without another thread's emit
      interleaving on the same connection.
- [ ] `SQLiteSink`, `MongoSink` and `ClickHouseSink` likewise for their driver's stated threading
      requirements, with a docstring line stating which requirement each is satisfying.
- [ ] `close()` on each of these does not race an in-flight `emit`: it either waits or is a no-op,
      never a release under an active writer.
- [ ] `MultiSink` does not hold a lock across a child's `emit`.
- [ ] Throughput of the single-threaded path is not materially changed (an uncontended lock).

### FR-003: Loss counters are safe under concurrency

#### Description:

Every `self.failed += 1` and `self.dropped_oversized += 1` in `sinks/` is a read-modify-write that
loses increments under concurrent emitters. SPEC-026 made these the numbers an operator alerts on,
so an undercount is a missed alert. The inventory is wider than those two names: 29 increment sites
across 20 modules, including `dropped_unadjudicated` (SPEC-018) and `NullSink.dropped`. All of them
are in scope — an operator who cannot tell which counters are trustworthy has none that are.

They are made safe under a **dedicated counter lock, separate from the transport lock FR-002
introduces**, held only across an increment or the two-field snapshot. Reusing the transport lock
was the original intent here and is wrong: SPEC-026 states in both `base.py` and the README that
`losses()` must be safe to call *while* `emit` is running, and `health()` is the call an operator
makes when things are already going wrong. Under one lock, `health()` would block behind an
in-flight insert and its retry backoff — seconds — which also contradicts this FR's own last
criterion. Lock order is fixed transport → counter and never the reverse, so the pair cannot
deadlock.

The counters stay public, writable, plain `int` attributes. They are documented public surface and
the suite assigns to one directly, so converting them to read-only properties would be a breaking
change bought for nothing: a single-attribute read is already atomic in CPython, and only the
*write* and the *pair read* need guarding.

#### Acceptance Criteria:

- [ ] Concurrent emitters against a permanently-failing sink produce a `failed` count exactly equal
      to the number of failures, with no lost increments, over a test that reliably reproduces the
      race today.
- [ ] The same for `dropped_oversized` and `dropped_unadjudicated`.
- [ ] `Sink.losses()` (SPEC-026 FR-002) reads a coherent snapshot — both fields from the same
      instant, never a half-updated pair.
- [ ] `MultiSink.failed` is safe while children are emitting concurrently.
- [ ] Reading a counter never blocks an emit for longer than the read itself.

### FR-004: The concurrency is covered by tests

#### Description:

The suite has no concurrent-emitter test, which is why none of this surfaced. Each guarantee above
gets a test that fails against the current code.

#### Acceptance Criteria:

- [ ] A shared test helper runs N threads emitting against one sink and joins them, usable by each
      sink's module.
- [ ] Each FR-002 guarantee has a test that fails before the fix and passes after.
- [ ] The counter test in FR-003 fails before the fix (demonstrably, not merely by luck) and passes
      after.
- [ ] A test asserts the orphan path and the worker can emit into one sink concurrently without
      corruption — the scenario that motivates the spec.
- [ ] The tests are deterministic enough for CI: no sleeps used as synchronization, and no
      dependence on thread scheduling for the assertion to hold.

---

## Data Model

No new public types. Per-sink internal state only:

```python
# On each sink guarding transport state:
self._lock = threading.Lock()          # transport state; held for the whole operation
self._counter_lock = threading.Lock()  # counters only; never held across I/O

# The shape, in outline:
def emit(self, batch):
    with self._lock:
        ...  # rotation check, socket send, transaction — everything assuming exclusivity
        with self._counter_lock:       # transport → counter, never the reverse
            self.failed += len(batch)

def losses(self):
    with self._counter_lock:
        return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)
```

A sink holding no transport state worth guarding — a thread-safe boto3 client, a driver that
documents its own locking — carries the counter lock alone.

`threading.Lock`, not `RLock`: a sink whose `emit` re-enters its own `emit` is a bug, and an `RLock`
would hide it.

Locking `emit` for its full duration has one caller-visible consequence, accepted deliberately: an
application thread on the orphan path can now *block* behind an in-flight emit where today it
corrupts shared state instead. It is bounded by SPEC-027 — every wait inside is interruptible and
`shutdown()` takes a timeout — and it is recorded in `architecture.md` §13 rather than left for a
reader to discover.

---

## API / Interface Contract

```python
# sinks/base.py — the contract, in the docstring an implementer reads:

class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None:
        """Ship a batch of serialized event dicts.

        **May be called concurrently from multiple threads.** The background worker drains on its
        own thread, and a level call made with no active span emits synchronously on the *caller's*
        thread (architecture §12), so an implementation holding mutable transport state — a stream
        it rebinds, a reused socket, a connection with transaction scope — must serialize access to
        it. Implementations that hold no such state need do nothing.
        """
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/sinks/
├── base.py         # modified — the documented contract
├── file.py         # modified — lock; corrected single-writer claim
├── _socket.py      # modified — lock around framed sends
├── sqlite.py       # modified — lock
├── postgres.py     # modified — lock around the transaction sequence
├── mongodb.py      # modified — lock
├── clickhouse.py   # modified — lock
├── multi.py        # modified — counter safety only, no lock across children
└── (sinks with counters)  # modified — counter safety

tests/
├── conftest.py     # modified — concurrent-emitter helper
└── (per-sink test modules)  # modified — concurrency cases

docs/architecture.md  # modified — §9 qualification
README.md             # modified — sink-author requirement
```

## Implementation Phases

### Phase 1: The contract and the test helper

- Document the contract in `sinks/base.py`; correct `file.py` and `architecture.md` §9.
- Add the concurrent-emitter helper to `conftest.py`.
- Add the failing counter test from FR-003.

### Phase 2: File and socket sinks

- Lock `FileSink`, `RotatingFileSink`, `SQLiteSink`, `SocketTransport`.
- Tests: rotation under concurrent writers; syslog frame integrity; close-vs-emit.

### Phase 3: Database sinks and counters

- Lock `PostgresSink`, `MongoSink`, `ClickHouseSink`; make every counter and `losses()` safe.
- Tests: transaction isolation; exact counts; coherent `losses()` snapshots; `MultiSink`.

### Phase 4: Documentation

- README sink-author requirement; per-sink docstring lines naming the driver requirement satisfied.
