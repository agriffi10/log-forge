# Pipeline: buffer, worker, drain — decisions

The settled decisions for the buffer, the flush worker and the two drains, including what a forked
child repairs. Read the fences; pull an entry only when you need the reasoning.

## Contents

- [Fences](#fences)
- [Buffer-then-flush, background, non-blocking](#buffer-then-flush-background-non-blocking)
- [Two drains, deliberately distinct](#two-drains-deliberately-distinct)
- [`flush()` reports delivery, and answers from the drain that carried the events](#flush-reports-delivery-and-answers-from-the-drain-that-carried-the-events)
- [The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded](#the-close-is-once-only-across-both-delivery-paths-the-atexit-registration-is-not-the-thing-being-guarded)
- [A worker guard asks one of three questions, and the set is enforced rather than remembered](#a-worker-guard-asks-one-of-three-questions-and-the-set-is-enforced-rather-than-remembered)
- [One lifecycle owner: the bookkeeping around the two emit paths exists once](#one-lifecycle-owner-the-bookkeeping-around-the-two-emit-paths-exists-once)
- [Only the forked *child* is repaired, and its order of work is the contract](#only-the-forked-child-is-repaired-and-its-order-of-work-is-the-contract)
- [A value the child inherits is stranded, never merely detached](#a-value-the-child-inherits-is-stranded-never-merely-detached)

## Fences

- **Buffer-then-flush, background, non-blocking** — the app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)
- **Two drains, deliberately distinct** — `shutdown()` is terminal; `flush()` drains on demand and leaves everything running. A frozen-not-exited process needs the second, and `atexit` never runs there. (SPEC-013)
- **`flush()` reports delivery, and answers from the drain that carried the events** — a marker that finds nothing pending inherits the outcome of the emit ahead of it in the FIFO — otherwise a second concurrent flush reports success for events the first one just abandoned. **Narrowed: a `shutdown()` that expires answers the outstanding markers pessimistically, so `abandoned` no longer implies the drain adjudicated — `ok=True` is untouched.** (SPEC-021, SPEC-036, SPEC-050)
- **The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded** — the arming fires **after** the emit returns, because what must be recorded is that an event *reached* the sink; keying on a configured sink is the phrasing that misses it. **A close in flight is waited for, not returned through** — both paths empty their record before an unbounded `close()` runs, so `atexit` returned through a close still delivering; the wait takes `join_closers`' grace. (SPEC-004, SPEC-030, SPEC-031, SPEC-050)
- **A worker guard asks one of three questions, and the set is enforced rather than remembered** — existence, liveness, and the **moment**, which carries two predicates because an abandoned drain answers them oppositely: `in_flight` reads `draining` for the signal refresh, `held` reads thread liveness for the close. Ownership stopped being a question when one record replaced four. **None of the three takes the lifecycle lock.** The entry also carries the owed-close record (one record, armed by delivery and discharged at the close), the per-module roster floor, and when joining beats detaching. (SPEC-035, SPEC-040, SPEC-044, SPEC-045, SPEC-046, SPEC-054; arch §9.2)
- **One lifecycle owner: the bookkeeping around the two emit paths exists once** — the *emit* paths stay two (arch §9, SPEC-028); what existed twice was the bookkeeping around them, and thirteen twin pairs are now one retirement **count**, one stop event, one owed-close record, one closer and one `Health` assembly. A count rather than a latch, because a latch makes a worker built after a `shutdown()` returned count every event it delivers as stranded. Retiring the closed-sink latch raises three close counts from one to two, each following the delivery it discharges. (SPEC-054; arch §9.2, §12)
- **Only the forked *child* is repaired, and its order of work is the contract** — locks and events, then inherited buffers, then the registered handlers — a lock re-initialised after a handler that takes it is a handler that hangs. `before` does not run for a C-level fork at all, and the repair is never wrapped in `gc.disable()` — that hides a macOS `os_log` crash rather than fixing one. (SPEC-039; arch §13)
- **A value the child inherits is stranded, never merely detached** — `dup2` to `/dev/null` *and* reopen in **append** mode: rebinding alone leaves the old object flushing to the real file, and `"w"` truncates a log shared with the parent. The hook is **per-sink** on purpose: `StdoutSink` has the same window and must *not* be fixed. (SPEC-039)

---

### Buffer-then-flush, background, non-blocking

**Buffer-then-flush, background, non-blocking** — span queue flushed at span end by a worker thread; app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)


### Two drains, deliberately distinct

**Two drains, deliberately distinct** — `shutdown()` is terminal (stops the worker, closes the sink); `flush()` drains on demand and leaves everything running. A frozen-not-exited process (serverless) needs the second, and `atexit` never runs there. (SPEC-013)


### `flush()` reports delivery, and answers from the drain that carried the events

**`flush()` reports delivery, and answers from the drain that carried the events** — the marker brings the emit's *outcome* back, so `True` means the sink took them, not merely that a drain ran. A marker that finds nothing pending inherits the outcome of the emit that carried what was ahead of it in the FIFO — otherwise a second concurrent flush reports success for events the first one just abandoned. It is not a verdict on every batch ever sent; `health().failed_batches` is the cumulative record. (SPEC-021) **What it reports *over* is three places, not one** (SPEC-036): the queue, the buffers on spans still **open** in the calling context, and the sink's own **client** buffer. Each was a place `flush()` returned success while events sat undelivered — the in-span case zero of two events on the README's own recipe. A span is swept and left open, its boundary events backfilled first (so they carry the baggage as of the flush, not the close) and its buffer detached by swap, never cleared; a swept span then makes `continue_trace()` refuse, or one span carries two trace ids. The bound is the calling context, because `contextvars` cannot enumerate another thread's. A sink that buffers in a driver implements the optional `flush()`, probed by `flush_sink` — which **propagates** where `read_losses` swallows, since a swallowed flush failure is a sink the worker believes. **Narrowed 2026-09-02 (SPEC-050 FR-001):** a `shutdown()` that *expires* answers the outstanding markers pessimistically rather than leaving them to a drain it has just given up on — including one the drain thread is holding, one taken after that sweep, which answers itself, one whose `flush()` enqueued it after the sweep had run, and one that never reached a full queue at all — the last two caught by `flush` itself, before and after its put, consulting the same flag. **Four** producers of `abandoned`, each found by running a probe rather than reading the fix, and three of them by someone re-running the previous fix's own probe. So a `False`/`abandoned` no longer implies the drain adjudicated it. The converse is untouched and is what the entry is for: `delivered` starts `False` and is written only by the owning drain, so no added path can produce `ok=True` over lost events. The alternative was a `flush(timeout=None)` — documented as supported — waiting forever, measured still alive after `shutdown()` returned.


### The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded

**The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded** — a process that only ever logs outside a span builds no worker, so nothing owned its sink's close and nothing performed it. The arming lives in `api._log`'s orphan branch and fires **after** the emit returns, because what has to be recorded is that an event *reached* the sink: keying on a configured sink is the obvious phrasing and is wrong, since `configure()` runs `_ensure_sink()` unconditionally and would close a `StdoutSink` nothing was ever written to. One `atexit` handler covers both paths — `_shutdown_worker` drains a worker if there is one and closes the orphan sink otherwise — which is what makes a single registration under the existing flag correct. Two handlers double-close (`atexit` runs LIFO) and reusing the flag for a worker-only handler costs a mixed process its exit drain (SPEC-004 FR-005); **both traps are green against every in-process test**, and trap A is caught by exactly one test in the suite — the orphan→span subprocess case. A live worker still owns the close, which is what makes a mixed process one `close()` in either order, and it inherits the worker's reasons for *not* closing (an expired shutdown leaves the sink open). No worker is created to answer any of this: `health().retired` is synthesized from a module flag, the same refusal `_swap_sink` and `_flush_worker` already make. `submitted_after_shutdown` is deliberately **not** incremented here — SPEC-030 defines it as a submission queued where nothing will drain it, and a later orphan log is refused at a closed sink and announced, which is not the same claim; `retired` alone is what stops being vacuous. (SPEC-031 FR-006, arch §13) **A close in flight is waited for, not returned through** (SPEC-050 FR-002). Both paths empty their record — `_sink_closed`, `_orphan_owed` — before an unbounded `close()` runs, so a second caller found nothing owed and returned instantly; where the first is a background thread and the second is `atexit`, the interpreter exits through a running close and kills it. Measured with a close-is-delivery sink: twelve events dead in its buffer at 0.31 s, nothing on stderr. The in-flight close is an `Event` in a **slot** and not a flag beside a permanent event — the orphan close is not once-only, so a second window would be unwaitable behind an event still set from the first, and a child inherits `None` rather than a set-or-clear question `_fork._fresh_primitive` would answer wrong. The wait is `min(DEFAULT_CLOSER_GRACE, remaining)` on **both** paths, the bound `join_closers` already uses: taking the whole budget would make a *stuck* close cost thirty seconds at an exit where it costs none, and a flat cap on one path alone made the same `shutdown(timeout=0)` return in under half a second through the worker and take the whole two seconds through the orphan record.


### A worker guard asks one of three questions, and the set is enforced rather than remembered

> **Superseded by SPEC-054 FR-002/FR-004 (2026-09-06).** It read *four questions* — existence,
> liveness, ownership, and ownership ∧ moment — while the library kept **four** records of which
> sinks were owed a close. One record replaced them, and "who owns this sink's close" then had one
> answer and stopped being a question a call site could get wrong. What survives of the conjunction
> is the moment on its own, carrying **two** predicates. The reasoning below is kept in full: it is
> what the merge had to preserve, and the four-question set is why it could be preserved.

**A worker guard asks one of three questions, and the set is enforced rather than remembered** — existence, liveness, and the moment (`architecture.md` §9.2). The fourth is new and is the one site where the arc's own "ownership, not liveness" slogan is wrong: bare ownership skips the stop-signal offer for a worker whose shutdown has *finished*, leaving a live sink holding a set event that can never clear, and liveness alone un-skips for the whole drain and hands the drain thread an event nobody will set. Both measured. Three reviewers had each named a different site, each was fixed, and a fourth shipped — so the fix is not a fifth correction but `tests/test_worker_predicate_roster.py`, which derives every site from `decorator.py`'s AST and fails on one that declares no category. Its subject vocabulary is derived too, to a fixpoint: a function whose return value names the worker is itself a worker name, or `worker = _snapshot()` rebinds a guard's category behind a neutral name with everything green. A roster that hand-lists anything — sites or tokens — rots. (SPEC-035, arch §9.2) **SPEC-040 made each question a method on one owner** (`_lifecycle._state`), so a call site *selects* a question instead of composing one, and the seven globals it read became that object's state. **None of the four takes the lock** — four guards ask with it already held, so a non-reentrant acquire inside a question deadlocks, and `_get_worker`'s outer check is unlocked on the `@trace` hot path. The roster now walks **both** modules, and its floor is **per module**: one total is cleared by one module alone while every site in the other stops being checked (measured, 37 against a floor of 36, passing). **A refactor shrinks a derived roster as silently as a deletion does** — two of this spec's own lint rules went dead in the commit that renamed their subject, with the file still green, and widening the scope immediately filed two SPEC-042 sites that had gone unfiled for two specs. (SPEC-040, arch §9.2) **SPEC-044 then locked the reads that must stay consistent.** A question is a single atomic load, but *acting* on one is not, and five races lived in that gap: a `shutdown()` that read "no worker" and returned having stopped a worker built one instruction later (`health()` reading `retired=True` over a live drain thread), a `_get_worker` that discarded an unclosed sink's close record, a log call that cancelled the stop signal a close was waiting on (8.01 s of an 8 s backoff, both paths), a swap that closed a sink without recording it, and a forked child hooking a superseded one. The fences: a shutdown-in-progress **depth counter** — a boolean is not nestable and two concurrent `shutdown()` calls are documented as normal, so the first to finish lowered it while the second still ran, reproducing the defect verbatim; a rule that **no transition clears the orphan close record without deciding who performs that close**; and an in-flight-close registry keyed on the **moment**, not on retirement, because retirement would reverse SPEC-033 FR-004's requirement that a sink adopted after `shutdown()` still backs off. The fence is deliberately not permanent — a worker built after `shutdown()` **returned** still delivers, which `_worker_health` had already settled and a permanent retirement fence would have superseded silently. What stays open and is recorded rather than half-fixed: `shutdown(timeout=)` does not bound the live sink's `close()` on either path (both bounding mechanisms were built and reverted; the third needs an interruptible `Sink.close`), and ~~two concurrent `configure(sink=…)` threads still double-close a sink at the same rate as before~~ — **SPEC-045 found that item was the wrong way round.** (SPEC-044, arch §13) **The record of which sinks the orphan path owes a close for is a set, not a slot**, and the defect was never a sink closed twice: it was the **live** sink closed by nobody. Arming a second sink discarded the first, so an ordinary `info()` that resolved a sink and was preempted armed a superseded sink over the live one — measured `C.closes == 0` with every `configure()` call sequential on one thread, which is what rules out a lock around a call documented as not thread-safe. A sink written to *after* its close has something new to flush, so a second close is owed rather than spurious: refusing it was built and measured stranding 2 of 3 events on a wrapper graph and losing on 31 of 80 fuzz seeds against 0 before, and two narrower variants each still lost. The set removes the trade instead of choosing a side. `_orphan_closed_sink` stays separate and unchanged — it names a sink a swap left *open*, which is a different claim from one that was closed. **Every site that consumes the record needs its own test**: three of the four consuming loops were found by mutation, not review, and the swap's worker branch survived a truncating mutant even after its no-worker sibling had one. (SPEC-045, arch §13) **The owed closes then run concurrently and are all joined** (SPEC-046), because draining a set in sequence cost one slow close *times* the number owed — 8.02 s for four 2-second closes against a 1.0 s budget, and 11.4 s at 200 sinks. Reusing SPEC-030's detached closer is the obvious move and loses data twice over: the grace is what remains of the budget (completed 1 of 4) and caps at `DEFAULT_CLOSER_GRACE` regardless (a 3 s close delivered nothing). **Joining beats detaching wherever there is no budget left to protect** — it is strictly better than the sequential drain on both axes, cost falls and loss stays zero — which is the opposite of `_start_closer`'s refusal to fall back inline, and the difference is whose budget is at stake. The join is in a `finally`: without it a Ctrl-C abandoned every close *mid-write* where the sequential drain abandoned one and had not started the rest, trading a leaked resource for a corrupt one. A single `Sink.close` is still unbounded. (SPEC-046, arch §12, §13)


### One lifecycle owner: the bookkeeping around the two emit paths exists once

**One lifecycle owner: the bookkeeping around the two emit paths exists once** — the library delivers events two ways and that stays (arch §9, SPEC-028): a `@trace`d
call hands its buffer to a worker, a level call with no active span emits on the caller's thread.
What existed **twice** was the bookkeeping — who owes a sink a close, which stop event a sink
holds, whether the process is retired, whether a close is in flight — once on `Worker` and once in
`_lifecycle`, with a set of predicates whose only job was to let each side ask the other what it
held. Measured at `98c7e78` by reading both modules end to end: **thirteen twin pairs**, and across
the seven specs that fixed lifecycle defects **13 of their 36 FRs** edited a mechanism on both
sides or added a guard on one that read the other's state. `docs/invariants.md` invariant 6 names
the consequence, and a probe found a live instance on its first run — `health().sink` read
`SinkLosses(dropped=0, failed=3)` inside a span and `None` outside one against the same sink,
because the no-worker branch synthesized eight fields and not that one. SPEC-040 is excluded from
that count because it *moved* rather than fixed: it put the orphan side's seven globals on one
object and left the worker's on `Worker`, so "one owner" had been the name of a file since
2026-08-11 and not yet a fact. (SPEC-054)

**Retirement is a count, not a latch, and that is what makes one latch possible.** A worker built
after a `shutdown()` **returned** still delivers (SPEC-044 FR-001, preserved), and against a
latched boolean every event it delivered would be counted as queued where nothing will drain it —
`Health`'s definition of `submitted_after_shutdown` (SPEC-030 FR-001). Against the count, that
worker records the count at its build as its `_epoch` and reads as live until the *next*
`shutdown()` moves it, which is exactly when its submissions start being stranded. The count moves
a **second** time before a late worker is stopped, or that worker reads as live after being
stopped: delivering nothing, its next submission uncounted, and a later swap waiting a whole budget
for a fence it cannot confirm. `worker.py` binds the owner once at import rather than reaching
through the module per read — measured over 5M iterations in one process, best of 7:
`self._shutdown_done` 3.80 ns, `_lifecycle._state.retirements > self._epoch` 14.86 ns, bound
7.55 ns; the whole-`submit` harness cannot resolve any of that, spreading ~20 ns across runs of one
tree. (SPEC-054 FR-001)

**One owed-close record, armed by delivery and discharged at the close, retires two mechanisms
that contradicted it.** A sink enters the record when an orphan emit lands on it, when a worker is
built on it, and when a swap installs it in a worker; it leaves at the moment a close of it
*starts*, under the lock, in the same critical section that decides who performs it and registers
that close — so there is no instant at which a sink is neither owed nor in flight, which a
preempted orphan emit re-arms and a racing `shutdown()` then closes alongside. The **closed-sink
latch** refused re-arming for the most recently closed sink only, and what it protected against — a
close performed against a sink the drain thread may still be inside — is answered at close time by
`held`, not by refusing the arming. **`Worker(sink_released=)`** made a late worker inherit a
discharged close; under one record its build arms its sink and the closer's second pass closes it
after that worker's drain. Both retirements raise a close count from 1 to 2, and each second close
follows the delivery it discharges — SPEC-045 FR-002's one close per write-epoch, asserted by the
sink's event count at each close rather than by the count alone. One case is a genuine redundancy
and is the accepted trade: a swap the worker **declined** arms the new sink anyway, because
SPEC-035 FR-003 guarantees a declined sink is owned by somebody, and not arming it was built and
measured costing that guarantee outright. (SPEC-054 FR-002)

**One closer for every exit, and a caller waits on the registry rather than on the record.** Four
functions closed sinks on one path each; one takes every owed sink nothing `held` says something
may be inside, closes the live one inline and the rest on threads joined in a `finally`, and grants
the closer grace **once**, at the end, on every path. A caller waits on every close registered by
somebody else for `closer_grace(deadline)` — keyed on the registry and not on the record, because
by the time a bystander arrives the caller inside the close has already discharged that sink, so
the record is empty and a record-keyed wait never fires (measured: the second caller returned
through a 0.6 s close at `closed == 0`). A sink held only by a *drain thread* costs no grace, since
nothing will release it inside one. A registration the closer did not hand off is discharged in a
`finally`, because an entry nobody sets is permanent: that sink is skipped by the signal refresh
forever, never taken by a later closer, and every later bystander waits out the whole grace on it.
The registration is held on a small object rather than as a bare `Event` in the registry dict,
because `_fork`'s repair walk assigns a replacement back **by name** and cannot reach a primitive
that is only a dict value. (SPEC-054 FR-003)

**`health()` is assembled once, and every field has one authority** — the worker's counters where a
worker exists, `retired` from the count, `sink` from the **configured** sink, `closing_sinks` from
the closer registry, `inherited_sink` from the config, the loss counters from `decorator`. Two
branches assembling eight fields apiece is what let `sink` be filled on one path only. Reading the
config rather than `worker.sink` is an observable change on the worker path — the two differ
permanently after a declined swap and after any `configure()` on a retired worker — taken because
`architecture.md` §12 already named the config the authority for "installed", and it closes that
section's `inherited_sink` open item in the same edit. (SPEC-054 FR-005)

### Only the forked *child* is repaired, and its order of work is the contract

**Only the forked *child* is repaired, and its order of work is the contract** — locks and events, then inherited buffers, then the registered handlers, because a lock re-initialised after a handler that takes it is a handler that hangs on the child's only thread. `before` does not run for a C-level fork at all (uWSGI calls `PyOS_AfterFork_Child` only), so the child handler must be sufficient regardless. The repaired roster is **derived, never listed** — a walk over this package's own objects, with an AST lint forbidding a primitive built where the walk cannot write it back, and an identity memo so a sink's `log_foundry_stop_signal` stays the worker's `_stop`. The worker is rebuilt **in place** with a replaced queue *object*; a retired parent forks a retired child. What the walk cannot reach is the caller's and is recorded, not half-fixed — including the shared sink, which both processes also *close*. **The repair is never wrapped in `gc.disable()`**, and that was built rather than assumed: it hides a macOS crash instead of fixing one — a forked child that finalizes an inherited, unclosed `sqlite3.Connection` faults inside Apple's `os_log`, and suppressing collection during the repair only greens a child that exits immediately, while re-enabling the collector in a child whose parent had disabled it. arch §13 carries the mechanism. (SPEC-039, arch §9, §13)


### A value the child inherits is stranded, never merely detached

**A value the child inherits is stranded, never merely detached** — `dup2` to `/dev/null` *and* reopen in **append** mode. Rebinding the stream alone leaves the old object flushing to the real file when the GC reaches it; reopening in `"w"` truncates a log shared with the parent, which passed 1626 tests because every test forked with the file empty on disk, making truncate and append the same program. The hook is per-sink precisely because `StdoutSink` has the same window and must **not** be fixed. (SPEC-039 FR-004)


