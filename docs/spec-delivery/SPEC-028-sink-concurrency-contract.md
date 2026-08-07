# Completed Spec — SPEC-028: The Sink Concurrency Contract

## What was completed?

`sinks/file.py` and `sinks/sqlite.py` both claimed "a single-process, single-worker-thread writer is
assumed (arch §9)", and `sinks/base.py` — the interface a third-party sink is written against —
stated no concurrency contract at all. Neither has been true since SPEC-002: the orphan path emits
synchronously on the **caller's** thread, which is any thread of the application, against the same
sink object the worker is draining into.

- **The contract is stated** (FR-001). `Sink.emit` documents that concurrent calls from several
  threads are possible and must be tolerated, naming the orphan path as the reason; `Sink.close`
  documents that it may be called while an `emit` is in flight and must not leave a half-released
  resource. It is written as a requirement on implementations rather than a promise the library
  serializes on their behalf, because the library does not own the calling thread. `architecture.md`
  §9's single-drain-thread bullet is qualified, both sinks' false claims are corrected, and the
  README's sink-author section leads with the requirement and a worked example.
- **Four sinks holding transport state are serialized** (FR-002). `FileSink`, `RotatingFileSink`,
  `SQLiteSink` and `SocketTransport` take a per-instance `threading.Lock` — not an `RLock`, so a
  sink re-entering its own `emit` stays a visible bug. `close()` takes the same lock, so it waits
  for an in-flight emit rather than releasing under it. `SocketTransport._reset` is deliberately the
  one unlocked caller, since `_send_one` reaches it with the lock already held.
- **Three database sinks, decided per driver** (FR-002). `PostgresSink` locks because a `psycopg`
  connection carries one transaction and this sink's unit of work is a `cursor`/`commit`/`rollback`
  sequence that assumes it owns it. `ClickHouseSink` locks because the client holds per-session
  state and is not published as shareable. **`MongoDBSink` deliberately does not**: `pymongo`'s
  client is thread-safe and owns a pool, and FR-002 asks for correctness under concurrent calls, not
  for parallelism to be removed. Each says which requirement it is satisfying, including Mongo's
  reason for having no lock — otherwise it reads as an omission.
- **Every loss counter is guarded** (FR-003). 30 increment sites across 19 modules, plus 14
  `losses()` reads. The HTTP family inherits one lock from `HTTPSink`.
- **The suite gained its first concurrent-emitter tests** (FR-004): a `run_concurrently` helper in
  `conftest.py` whose threads rendezvous on a `Barrier`, and ten tests including the motivating
  scenario — application threads on the orphan path and `log-foundry-worker` inside one sink at
  once, asserted through an observed overlap count rather than through the events arriving.

## Notes for the next spec

**The counter lock is a second lock, not FR-003's original one.** The FR said counters go under the
transport lock where one exists. That breaks a SPEC-026 guarantee stated in both `base.py` and the
README — `losses()` must be safe to call *while* `emit` is running — because `health()` would then
block behind an in-flight insert *and its retry backoff*, which is exactly when an operator calls
it. Two locks, ordered transport → counter and never the reverse. The FR was amended in place.

**Three of the ten tests were vacuous on the first attempt, and the third one is the interesting
case.** Mutation-testing against a stashed `src/` (the discipline that has caught this six reviews
running) found:

1. A `FileSink` test asserting *line integrity* passed against the bug. `TextIOWrapper.write` holds
   its own lock, so a line cannot splice even unlocked. It was replaced by one asserting the
   guarantee the sink lock actually adds: the write loop over a batch is indivisible.
2. Two counter tests asserting exact counts passed against the bug — see below.
3. The `losses()`-does-not-block test passed against the bug, and correctly so: its baseline is the
   **one-lock design**, not pre-SPEC-028 code. Unguarded counters trivially do not block. It was
   verified by mutating `losses()` to take the emit lock, where it fails. Recorded in the criterion,
   because a future reader running the usual stash-and-rerun check would otherwise call it vacuous.

**The counter race is not reproducible on the interpreters CI runs, and FR-003 was amended to say
so.** A bare `+=` on an instance attribute lost **nothing** across 1.6M concurrent increments on
CPython 3.13 — 8 to 32 threads, switch interval down to 1 ns, with and without work between
increments. Python promises no atomicity here and the free-threaded build removes the GIL that is
currently covering for it, so the guard is correct and cheap; but "a test that reliably reproduces
the race today" asks for something that cannot be written. The tests instead assert deterministically
that the increment happens *inside* the critical section, via a `CountingLock` whose acquisitions
must equal the number of counted events. That property is what survives the GIL going away, and it
fails against the unguarded code (no `_counter_lock` attribute at all).

**`SQLiteSink` was worse than the spec assumed, which changed the cost/benefit in §13.** Concurrent
use of one `sqlite3` connection with `check_same_thread=False` does not lose rows — it kills the
interpreter, reproducibly, with `Fatal Python error: Bus error`. Not an exception a caller could
catch. That moves the defect from breaching the loss-reporting rule to breaching the library's first
promise, and it is the reason the §13 constraint concludes that blocking an orphan log on a lock is
the better trade.

**One accepted cost, recorded rather than hidden** (`architecture.md` §13). Locking `emit` for its
full duration means a level call made with no active span can now *block* behind an in-flight emit
including its backoff. It is bounded by SPEC-027 (interruptible waits, `shutdown()` timeout), it
does not touch the traced path, and the alternative is the bus error above.

**Not done, and deliberately.** `MemorySink`, `NullSink` and `StdoutSink` got the counter fix and no
lock, per the spec's Out of Scope. `MultiSink` holds no lock across a child's `emit`, and its
`losses()` needs no counter lock because it sums children rather than reading its own `failed`.
Process-level concurrency — two processes on one file or one SQLite database — remains out of scope
exactly as before.
