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

**The counter race needs an injected preemption point, and my first amendment over-claimed.** A
bare `+=` on an instance attribute lost **nothing** across 1.6M concurrent increments on CPython
3.13 — 8 to 32 threads, switch interval down to 1 ns, with and without work between increments.
I concluded from that that a reproducing test "cannot be written". Review disproved it: a property
on the counter's *storage* — the same class of injection `CountingLock` already uses — reproduces
genuine lost increments in the unmodified pre-SPEC-028 `MultiSink` (6400 expected, 6350 observed;
zero after the fix). The accurate claim is that CPython's eval breaker does not fall inside a bare
attribute read-modify-write, so the race is unobservable *without injection* — not unobservable.
The shipped tests still assert that the increment happens inside the critical section, since both
candidate tests inject and that assertion is the one that survives free-threading.

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

**Three sinks the FR-002 checklist missed, found by review after the merge.** FR-002's Description
covers "each shipped sink that mutates transport state during `emit`", but its acceptance criteria
enumerated seven sinks by name and the implementation followed the list rather than the code.
`NATSSink` drives a private `asyncio` loop, which is single-entry: measured unlocked, threads raised
`RuntimeError: This event loop is already running`, most events never published, and **one thread
never returned from `emit` at all** — an application thread hung forever on the orphan path, which
is strictly worse than anything this spec set out to fix. `RabbitMQSink` shares and rebinds one
`pika` channel (223 of 240 publishes overlapped) and had no `_closed` flag, so a concurrent emit
could reopen a connection after `shutdown()` closed it. `AzureEventHubsSink` shares a producer and
builds an `EventDataBatch` across its loop. All three now lock, and `GooglePubSubSink` had its
futures list swapped under a lock rather than iterated-then-cleared, which was dropping unconfirmed
publishes that `losses()` then never reported.

**Not done, and deliberately.** `MemorySink`, `NullSink` and `StdoutSink` got the counter fix and no
lock, per the spec's Out of Scope. `MultiSink` holds no lock across a child's `emit`, and its
`losses()` needs no counter lock because it sums children rather than reading its own `failed`.
Process-level concurrency — two processes on one file or one SQLite database — remains out of scope
exactly as before.


## Verification

- 959 tests pass; `ruff`, `mypy --strict` and `spec-lint` clean on 3.12 and 3.13.
- Every new test was mutation-checked against the specific mutant it claims to catch, one at a
  time. Nine kill their mutant on the assertion they advertise. Three deserve a caveat: unlocked
  `SQLiteSink` and unlocked `NATSSink` kill theirs by **crashing or hanging the interpreter**
  rather than by asserting (a bus error and a permanently wedged thread respectively), so a
  regression in either presents in CI as a native crash or a slow failure rather than a clean red
  test; and `test_losses_does_not_block_behind_an_in_flight_emit` is measured against the
  **one-lock design**, because unguarded counters trivially do not block.
- Three lock sites that survived the first mutation pass — `ClickHouseSink.emit`, the `close()`
  half of the contract, and `dropped_unadjudicated` — now have tests, and each was confirmed
  failing against a targeted mutant.
- Vendor thread-safety claims were checked against the vendors' own documentation rather than
  asserted: boto3 ("clients are generally thread-safe", with the caveat about calling
  `boto3.client()` concurrently, which this code avoids by building in `__init__`), redis-py
  ("client instances can safely be shared between threads", with its `Pipeline` exception, which
  this code avoids by keeping the pipeline a per-call local), confluent-kafka, and pymongo.

## What changed from earlier specs?

- **SPEC-026's `losses()` contract is now a concurrency contract.** `base.py` said `losses()` must
  be "safe to call during an emit"; this spec says what that costs — a dedicated counter lock,
  never the transport lock — and the README's sink-author example was rewritten to show both.
- **SPEC-027's `shutdown(timeout=...)` was silently weakened and then restored.** Making `close()`
  take the emit lock meant an application thread on the orphan path could block the close
  indefinitely, so the bound covered the join and not the call. `_close_if_owed` now runs the close
  on a daemon thread joined for the remaining budget, and reports through the existing
  `"ShutdownTimeout"` vocabulary. A `BaseException` is carried back across that thread boundary,
  because SPEC-025 FR-004 requires a `KeyboardInterrupt` from `close()` to reach the caller and a
  bare `Thread` would have discarded it — the suite caught that regression.
- **SPEC-027's roster lesson is now enforced rather than remembered.** That spec found three sinks
  missed by a hand-written list and derived its roster from the AST. This spec's FR-002 enumerated
  seven sinks and missed three more, so
  `test_every_driver_backed_sink_records_a_concurrency_decision` fails any driver-backed sink that
  neither locks nor records why it need not.
