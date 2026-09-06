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

**Two limits of the lint, recorded rather than hidden.** It proves a lock is *entered on the
path*, not that it covers the body — an empty `with self._lock: pass` satisfies it, which is why
every locked sink also has a behavioural test. That limit stands. ~~And its *scope* gate still
admits a module by a lazy driver import or a hardcoded handle token, so a lock added to
`HTTPSink.emit` would be invisible to both it and the post-close roster. No present instance; the
honest fix is to widen scope to every class in `sinks/` with an `emit`, which belongs with
SPEC-031's residue.~~ — **closed by SPEC-032**, which widened the gate to exactly that and moved
the item off SPEC-031: the post-close roster derives from the same gate and would have inherited
the gap.

~~**Post-close loss outside this spec's scope.** `GooglePubSubSink.emit` after `close()` still
appends futures nothing will resolve, and `KafkaSink` accepts produces past close — the silent-loss
shape SPEC-026 exists to end, reached one call later. Neither sink takes a transport lock, so
neither is in the roster this spec enforces. SPEC-030 owns what the library should *signal* after a
completed shutdown; Pub/Sub's case is loss rather than signalling and should be named there
explicitly.~~ — **closed by SPEC-032.** SPEC-030 confirmed it was loss rather than signalling and
handed it on; SPEC-032 fixed both sinks and a third the survey found (`_RedisSink`, which
*succeeded* after close by reconnecting).

**Not done, and deliberately.** `MemorySink`, `NullSink` and `StdoutSink` got the counter fix and no
lock, per the spec's Out of Scope. `MultiSink` holds no lock across a child's `emit`, and its
`losses()` needs no counter lock because it sums children rather than reading its own `failed`.
Process-level concurrency — two processes on one file or one SQLite database — remains out of scope
exactly as before.


## Verification

- 982 tests pass; `ruff`, `mypy --strict` and `spec-lint` clean on 3.12 and 3.13.
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
- **SPEC-027's `shutdown(timeout=...)` is weakened by this spec, and that is recorded rather than
  fixed.** Making `close()` take the emit lock means an application thread on the orphan path can
  hold it inside a driver call and delay the close past `shutdown`'s budget — the bound covers the
  join, not the call. A joinable daemon-thread close was written to restore the bound and then
  **reverted after review**: at interpreter exit the daemon is killed wherever it has reached,
  which for `SQLiteSink` is between `commit()` and `close()`, converting the leaked handle
  SPEC-027 FR-004 accepts into the partial write it was avoiding. It also could not distinguish a
  slow-but-successful close from a stuck one, so it latched SPEC-019's `stopped_reason` alert term
  and wrote "the sink is left open" for closes that had completed. A wrong signal is worse than a
  slow one. The residual delay is now a constraint in `architecture.md` §13.
- **SPEC-027's roster lesson is now enforced rather than remembered.** That spec found three sinks
  missed by a hand-written list and derived its roster from the AST. This spec's FR-002 enumerated
  seven sinks and missed three more, so
  `test_every_driver_backed_sink_records_a_concurrency_decision` fails any driver-backed sink that
  neither locks nor records why it need not.


## Second review round (pre-merge, PR #116)

The first two PRs merged on green CI without an independent review; that was the process error
behind everything above, and `CLAUDE.md` now states that green CI is not a review.

> **Superseded, 2026-08-30:** this spec's finding originally moved the review gate to *before the
> merge*. The gate has since moved earlier still — the diff is reviewed **before the push**, so a
> branch reaches the remote already reviewed rather than collecting fixes in public. The reason is
> unchanged and this spec is still the evidence for it; only the point it binds has moved. See
> `docs/process/reviewer-contract.md` → *The reviewer contract*.

A third review, run *before* merging the corrections, returned DO NOT MERGE with
13 findings. The ones that changed code:

- **The daemon-thread close was a net regression** (above). Reverted.
- **`RabbitMQSink`'s new `_closed` flag created a permanent connection leak.** `close()` began
  returning early on the flag, but `emit` never consulted it and `_active_channel` reopens whenever
  it finds no connection — so one `log_foundry.info()` after `shutdown()` opened an AMQP connection
  nothing would ever reap. `emit` now refuses when closed, as `SQLiteSink` and `MongoDBSink` do.
- **The lint was insensitive to the regression it exists to prevent.** Its first form scanned each
  *module* for the substring `with self._lock`, and passed with the locks stripped from three
  sinks' `emit` methods because each module's `close` still contained one. Its second form accepted
  any class docstring citing the spec, so a sink documenting *why it locks* stayed exempt after its
  lock was deleted. It is now per class, walks `emit` and the helpers `emit` calls via the AST, and
  the only exemption is the specific claim `**no** transport lock`. Verified by stripping each of
  the five locked sinks' `emit` locks in turn.
- **`MongoDBSink._close_lock` did not do what its docstring said** — it serializes concurrent
  closes, not a close against an in-flight insert. The docstring now says so, and `emit` gained the
  `_closed` check that actually covers the overlap.
- **Two test defects.** The RabbitMQ test survived its mutant about one run in five (one publish
  per emit is too short a critical section); it now emits batches and killed the mutant 20/20. The
  `dropped_unadjudicated` test never failed on the value it advertised, so a new test uses the
  reviewer's own storage-injection technique — a descriptor that yields inside the counter's
  read-modify-write — to observe **real lost increments** against the unguarded code. That is also
  the concrete demonstration that the corrected FR-003 amendment was right and the first one wrong.

**A near-miss worth recording.** The scripted edit that rewrote the lint spliced the file by index
and silently deleted four tests — nats, rabbitmq, clickhouse and `dropped_unadjudicated`. The suite
stayed green, because deleted tests do not fail, and the PR would have claimed coverage that no
longer existed. Caught by comparing the collected test count against the expected one. A scripted
edit to a test file needs `pytest --collect-only` before and after, not just a green run.


## Third review round (pre-merge, PR #116)

Returned DO NOT MERGE with the code judged correct but under-tested — and it caught this branch
repeating the defect it had just documented. The `_closed` checks added for `MongoDBSink` and
`RabbitMQSink` *in response to the second review* could both be deleted with all 960 tests still
green: fixes asserted rather than demonstrated.

- **Every locked sink now has an emit-after-close test**, parametrized across all six, asserting
  both that `SinkDeliveryError` comes back *and that the driver was not touched*. The second half
  is load-bearing: `RabbitMQSink`'s `_active_channel` reopens whenever it finds no connection, so
  a test asserting only the exception would have passed against the leak. Writing them exposed a
  further trap — the first doubles failed after close for unrelated reasons (Mongo's fake lacked
  `insert_many`, RabbitMQ's reconnect hit a missing `pika`), so the tests passed with the guards
  deleted. Both doubles now keep working after close, so an emit that slips past a guard *lands*.
- **`AzureEventHubsSink` gained the behavioural test it never had.** It was the one sink locked by
  the corrections with only a docstring behind it, and its lock survived being narrowed to `with
  self._lock: pass` with the full suite green. The new test reports overlap across the whole
  `create_batch` → `add` → `send_batch` sequence, so narrowing is caught, not just removal.
- **Post-close behaviour is now one rule across every locked sink** rather than three answers.
  That sentence read "all six locked sinks" for one round and was false three ways — nine classes
  take a transport lock, one of the six named does not, and three unnamed ones still gave the old
  answer. See the fourth round below. An empty batch stays a no-op whether the sink is closed
  or not.
- **`architecture.md` §13 said the opposite of what this branch claimed it said.** The worker
  docstring and this doc both stated the residual `shutdown()` delay was "recorded in §13"; §13
  asserted the delay was *bounded* because `shutdown()` takes a timeout — true before the revert,
  false after. §13 now records the real constraint: the timeout bounds the drain join, and
  bounding the close properly would need an interruptible `close()`, a sink-contract change.

Known and accepted: the lint proves a lock is entered on the path, not that it covers the body
(an empty `with self._lock: pass` satisfies it) — narrowing is the behavioural tests' job, which
is why every locked sink now has one. And a regression in `NATSSink`'s lock presents in CI as a
job timeout rather than a red test, because the wedged threads are non-daemon; stated in the
test's own docstring.


## Fourth review round (pre-merge, PR #116)

The third review confirmed every earlier blocker fixed and demonstrably tested, and then found the
roster miss **again** — the fourth on this PR, and this time inside the fix for the third.
`CLOSED_SINKS` was written by hand next to a rule it was supposed to enforce, naming six sinks when
nine classes take a transport lock. The gap was not cosmetic: `SocketTransport` has no closed flag
at all, `close()` is only `_reset()`, and `_socket()` reopens on demand — so one
`log_foundry.info()` after `shutdown()` opened and leaked a live TCP connection, measured through
the public API. That is verbatim the `RabbitMQSink` defect the previous round had just fixed and
written up, in the more commonly deployed sink (`SyslogSink` and `LogstashSink` are core; `amqp` is
an extra). `FileSink`, `RotatingFileSink` and `PostgresSink` carried the same shape more mildly:
a flag `close()` honoured and `emit` ignored.

- **All ten post-close guards now exist and are individually mutation-checked.** Removing any one
  fails at least one test.
- **The roster is derived, not written.** `test_every_locked_sink_has_a_post_close_case` computes
  the locked classes from the same AST walk the contract lint uses and asserts the double map
  covers them; locking a new sink's `emit` fails it until someone supplies a double. Verified by
  dropping an entry and by locking `SNSSink` — both fail. Writing this also caught the parametrized
  builder falling through to `NATSSink` for unrecognized names, which would have run four more NATS
  cases while claiming to cover four new sinks.
- **`_closed` is now written before the resources it guards disappear**, in all of them, matching
  SPEC-025's recorded rule that a failed close is announced rather than retried.

The pattern across four rounds is worth stating: the code has been broadly sound and the *claims
about it* have not — an unbacked "recorded in §13", a fix with no test, a docstring describing a
guarantee its lock did not provide, and three hand-written rosters. Every one was caught by a fresh
context and none by CI, which is why `CLAUDE.md` now says green CI is not a review.
