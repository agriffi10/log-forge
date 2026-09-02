# Key Decisions — full register

The settled architectural decisions, in full. **Don't re-litigate these.** `CLAUDE.md` carries a
one-line digest of each; this file carries the reasoning, the constraints, what was rejected, and any
explicit "do NOT build" fences. Pull the entry you need — you rarely need them all.

Rules that keep this file useful:

- **Entry first, line second.** Write the full entry here *before* adding the one-line digest to
  `CLAUDE.md`. The digest line must never be the only home of a fact — a digest that outgrows its
  register inverts the whole model, which is exactly what this repo did until 2026-09-02: it had no
  register, so `CLAUDE.md` was the register, and it reached 89,340 bytes.
- **One `###` heading per entry**, listed in the Contents below, and grouped by AREA rather than by
  spec number. Ordering by spec turns a register into a changelog, and a shape that reads as
  disposable gets treated as disposable.
- **The heading matches the bold label of its `CLAUDE.md` digest line**, so the line greps straight to
  its entry. `scripts/docs-lint.sh` checks that correspondence in both directions — run it locally before every push.
- **When a decision reverses an earlier one,** update the entry in place and add a superseded marker
  at every *other* doc site still stating the old claim.
- **Date-stamp user decisions** (YYYY-MM-DD) so "settled" has a when.

## Contents


**The trace model and its context**

- [Unit of work = a decorated call](#unit-of-work--a-decorated-call)
- [IDs are W3C Trace Context compatible](#ids-are-w3c-trace-context-compatible)
- [Context via `contextvars`](#context-via-contextvars)
- [Cross-process traces are adopted explicitly, never auto-instrumented](#cross-process-traces-are-adopted-explicitly-never-auto-instrumented)
- [Boundary events take the span's *final* baggage; mid-span events keep the moment's](#boundary-events-take-the-spans-final-baggage-mid-span-events-keep-the-moments)
- [Per-request context is released at the root span — baggage restored, an adopted trace context cleared](#per-request-context-is-released-at-the-root-span--baggage-restored-an-adopted-trace-context-cleared)

**The pipeline: buffer, worker, drain**

- [Buffer-then-flush, background, non-blocking](#buffer-then-flush-background-non-blocking)
- [Two drains, deliberately distinct](#two-drains-deliberately-distinct)
- [`flush()` reports delivery, and answers from the drain that carried the events](#flush-reports-delivery-and-answers-from-the-drain-that-carried-the-events)
- [The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded](#the-close-is-once-only-across-both-delivery-paths-the-atexit-registration-is-not-the-thing-being-guarded)
- [A worker guard asks one of four questions, and the set is enforced rather than remembered](#a-worker-guard-asks-one-of-four-questions-and-the-set-is-enforced-rather-than-remembered)
- [Only the forked *child* is repaired, and its order of work is the contract](#only-the-forked-child-is-repaired-and-its-order-of-work-is-the-contract)
- [A value the child inherits is stranded, never merely detached](#a-value-the-child-inherits-is-stranded-never-merely-detached)

**Event assembly: safety and bounds**

- [A reserved word needs exactly one route through, including its own name](#a-reserved-word-needs-exactly-one-route-through-including-its-own-name)
- [An event is safe by construction — coerced and bounded once at assembly, not per sink](#an-event-is-safe-by-construction--coerced-and-bounded-once-at-assembly-not-per-sink)
- [A value too large to *render* is replaced, never clipped](#a-value-too-large-to-render-is-replaced-never-clipped)

**The sink contract: delivery and its verdict**

- [The sink is a durable buffer, not the final store](#the-sink-is-a-durable-buffer-not-the-final-store)
- [A FIFO message group is a trace, not the process](#a-fifo-message-group-is-a-trace-not-the-process)
- [A positional response adjudicates all of a chunk or none of it](#a-positional-response-adjudicates-all-of-a-chunk-or-none-of-it)
- [A sink that delivered nothing raises; one that delivered something reports](#a-sink-that-delivered-nothing-raises-one-that-delivered-something-reports)
- [A redirect is a delivery failure, not a route to follow](#a-redirect-is-a-delivery-failure-not-a-route-to-follow)
- [A client exception costs its chunk, and is provable non-delivery for it](#a-client-exception-costs-its-chunk-and-is-provable-non-delivery-for-it)
- [A sink that released its transport refuses; one that released nothing keeps accepting](#a-sink-that-released-its-transport-refuses-one-that-released-nothing-keeps-accepting)
- [A destination's limit is found by halving the *budget*, not the chunk](#a-destinations-limit-is-found-by-halving-the-budget-not-the-chunk)

**The sink contract: waiting, concurrency and shutdown**

- [A value on the wire is measured on the clock that cannot move](#a-value-on-the-wire-is-measured-on-the-clock-that-cannot-move)
- [A sink's wait is bounded, interruptible, and never taken on a destination's word](#a-sinks-wait-is-bounded-interruptible-and-never-taken-on-a-destinations-word)
- [A sink tolerates concurrent callers; the library cannot serialize them for it](#a-sink-tolerates-concurrent-callers-the-library-cannot-serialize-them-for-it)
- [A terminal `shutdown()` and a captured sink are both reported, not prevented](#a-terminal-shutdown-and-a-captured-sink-are-both-reported-not-prevented)
- [A sink handoff is owned by whoever is delivering, and "a worker exists" is not "a worker owns this sink"](#a-sink-handoff-is-owned-by-whoever-is-delivering-and-a-worker-exists-is-not-a-worker-owns-this-sink)
- [A shutdown shortens a *wait*; it must never skip *work*](#a-shutdown-shortens-a-wait-it-must-never-skip-work)
- [A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable](#a-process-releases-only-a-transport-it-acquired-here-and-unrecorded-must-be-unclaimable-rather-than-merely-unreleasable)

**Failure paths and diagnostics**

- [A dead worker is reported, not restarted — and as a *reason*, not a liveness flag](#a-dead-worker-is-reported-not-restarted--and-as-a-reason-not-a-liveness-flag)
- [Every path the caller stands on is total, and a swallowed fault is announced by *type*](#every-path-the-caller-stands-on-is-total-and-a-swallowed-fault-is-announced-by-type)
- [One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times](#one-module-writes-every-diagnostic-so-the-rules-are-applied-once-rather-than-remembered-twenty-eight-times)

**The public API surface**

- [Logs-only, send everything for now](#logs-only-send-everything-for-now)
- [An extra's floor is a published contract — moved deliberately, never by a bot](#an-extras-floor-is-a-published-contract--moved-deliberately-never-by-a-bot)
- [A public accessor hands out a copy; the library reads the live object](#a-public-accessor-hands-out-a-copy-the-library-reads-the-live-object)
- [A result that can grow a reason must stop being a `bool` before 1.0, not after](#a-result-that-can-grow-a-reason-must-stop-being-a-bool-before-10-not-after)
- [A protocol that is exported is a protocol that will be inherited](#a-protocol-that-is-exported-is-a-protocol-that-will-be-inherited)

**Release, supply chain and naming**

- [Version comes from Git tags, published to PyPI as `log-foundry`](#version-comes-from-git-tags-published-to-pypi-as-log-foundry)
- [Every action is pinned to a commit SHA, and the pins are maintained, not frozen](#every-action-is-pinned-to-a-commit-sha-and-the-pins-are-maintained-not-frozen)
- [A scanner that exits zero has not said "clean"](#a-scanner-that-exits-zero-has-not-said-clean)
- [An SBOM describes the published artifact, and is generated from it](#an-sbom-describes-the-published-artifact-and-is-generated-from-it)
- [Release assets are attached to a draft, never to a published release](#release-assets-are-attached-to-a-draft-never-to-a-published-release)
- [`pip-audit` gates, and audits the extras or it audits nothing](#pip-audit-gates-and-audits-the-extras-or-it-audits-nothing)
- [One name everywhere: `log-foundry` / `log_foundry`](#one-name-everywhere-log-foundry--log_foundry)

**Working rules: findings, rosters and testing bounds**

- [A subclass that inherits a method is still in the roster](#a-subclass-that-inherits-a-method-is-still-in-the-roster)
- [A bound is only a bound if it is measured where it binds](#a-bound-is-only-a-bound-if-it-is-measured-where-it-binds)
- [An open item is closed by being fixed, settled, or recorded as a constraint — never deleted](#an-open-item-is-closed-by-being-fixed-settled-or-recorded-as-a-constraint--never-deleted)
- [A read-only finding is not closed until it has been run, and the job that runs it needs a floor rather than an exit code](#a-read-only-finding-is-not-closed-until-it-has-been-run-and-the-job-that-runs-it-needs-a-floor-rather-than-an-exit-code)

---


## The trace model and its context


### Unit of work = a decorated call

**Unit of work = a decorated call** — (`@log_foundry.trace`); outermost call starts a trace, every call is a span within it. (arch §4)


### IDs are W3C Trace Context compatible

**IDs are W3C Trace Context compatible** — `trace_id` 16B/32hex, `span_id` 8B/16hex, `log_id` UUID; makes future trace adoption cheap. (arch §3.1)


### Context via `contextvars`

**Context via `contextvars`**, not thread-locals — correct under threads and asyncio; holds a span stack + baggage. (arch §5)


### Cross-process traces are adopted explicitly, never auto-instrumented

**Cross-process traces are adopted explicitly, never auto-instrumented** — `continue_trace()` takes a W3C `traceparent`/baggage the *caller* moved; no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014, arch §12)


### Boundary events take the span's *final* baggage; mid-span events keep the moment's

**Boundary events take the span's *final* baggage; mid-span events keep the moment's** — one backfill at close completes `span.start`/`span.end` (which describe the whole span and carry the outcome), while an `info` is left exactly as it was emitted. Backfilling everything would also invert `build_event`'s precedence by letting baggage beat a per-call field. (SPEC-015)


### Per-request context is released at the root span — baggage restored, an adopted trace context cleared

**Per-request context is released at the root span — baggage restored, an adopted trace context cleared** — the asymmetry is deliberate. Baggage set before any span is a process-level default, so it is restored *to*; an inbound context is a one-shot handoff to the trace it names, and restoring it would put back an adoption made *before* the span, leaving a warm container joining the first caller's trace forever. Consequences: one `continue_trace()` serves one root span (a batch needs one per record, or one `@trace` entry point), and the release lands in the context the span's `finally` runs in — so adopting outside a span and dispatching into an `asyncio.Task` needs `reset_context()`, recorded as a constraint in arch §13. Nested spans never reset: "at or below" is where baggage starts, the root span's close is where it stops. (SPEC-024, arch §5.1)


## The pipeline: buffer, worker, drain


### Buffer-then-flush, background, non-blocking

**Buffer-then-flush, background, non-blocking** — span queue flushed at span end by a worker thread; app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)


### Two drains, deliberately distinct

**Two drains, deliberately distinct** — `shutdown()` is terminal (stops the worker, closes the sink); `flush()` drains on demand and leaves everything running. A frozen-not-exited process (serverless) needs the second, and `atexit` never runs there. (SPEC-013)


### `flush()` reports delivery, and answers from the drain that carried the events

**`flush()` reports delivery, and answers from the drain that carried the events** — the marker brings the emit's *outcome* back, so `True` means the sink took them, not merely that a drain ran. A marker that finds nothing pending inherits the outcome of the emit that carried what was ahead of it in the FIFO — otherwise a second concurrent flush reports success for events the first one just abandoned. It is not a verdict on every batch ever sent; `health().failed_batches` is the cumulative record. (SPEC-021) **What it reports *over* is three places, not one** (SPEC-036): the queue, the buffers on spans still **open** in the calling context, and the sink's own **client** buffer. Each was a place `flush()` returned success while events sat undelivered — the in-span case zero of two events on the README's own recipe. A span is swept and left open, its boundary events backfilled first (so they carry the baggage as of the flush, not the close) and its buffer detached by swap, never cleared; a swept span then makes `continue_trace()` refuse, or one span carries two trace ids. The bound is the calling context, because `contextvars` cannot enumerate another thread's. A sink that buffers in a driver implements the optional `flush()`, probed by `flush_sink` — which **propagates** where `read_losses` swallows, since a swallowed flush failure is a sink the worker believes.


### The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded

**The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded** — a process that only ever logs outside a span builds no worker, so nothing owned its sink's close and nothing performed it. The arming lives in `api._log`'s orphan branch and fires **after** the emit returns, because what has to be recorded is that an event *reached* the sink: keying on a configured sink is the obvious phrasing and is wrong, since `configure()` runs `_ensure_sink()` unconditionally and would close a `StdoutSink` nothing was ever written to. One `atexit` handler covers both paths — `_shutdown_worker` drains a worker if there is one and closes the orphan sink otherwise — which is what makes a single registration under the existing flag correct. Two handlers double-close (`atexit` runs LIFO) and reusing the flag for a worker-only handler costs a mixed process its exit drain (SPEC-004 FR-005); **both traps are green against every in-process test**, and trap A is caught by exactly one test in the suite — the orphan→span subprocess case. A live worker still owns the close, which is what makes a mixed process one `close()` in either order, and it inherits the worker's reasons for *not* closing (an expired shutdown leaves the sink open). No worker is created to answer any of this: `health().retired` is synthesized from a module flag, the same refusal `_swap_sink` and `_flush_worker` already make. `submitted_after_shutdown` is deliberately **not** incremented here — SPEC-030 defines it as a submission queued where nothing will drain it, and a later orphan log is refused at a closed sink and announced, which is not the same claim; `retired` alone is what stops being vacuous. (SPEC-031 FR-006, arch §13)


### A worker guard asks one of four questions, and the set is enforced rather than remembered

**A worker guard asks one of four questions, and the set is enforced rather than remembered** — existence, liveness, ownership, and ownership ∧ moment (`architecture.md` §9.2). The fourth is new and is the one site where the arc's own "ownership, not liveness" slogan is wrong: bare ownership skips the stop-signal offer for a worker whose shutdown has *finished*, leaving a live sink holding a set event that can never clear, and liveness alone un-skips for the whole drain and hands the drain thread an event nobody will set. Both measured. Three reviewers had each named a different site, each was fixed, and a fourth shipped — so the fix is not a fifth correction but `tests/test_worker_predicate_roster.py`, which derives every site from `decorator.py`'s AST and fails on one that declares no category. Its subject vocabulary is derived too, to a fixpoint: a function whose return value names the worker is itself a worker name, or `worker = _snapshot()` rebinds a guard's category behind a neutral name with everything green. A roster that hand-lists anything — sites or tokens — rots. (SPEC-035, arch §9.2) **SPEC-040 made each question a method on one owner** (`_lifecycle._state`), so a call site *selects* a question instead of composing one, and the seven globals it read became that object's state. **None of the four takes the lock** — four guards ask with it already held, so a non-reentrant acquire inside a question deadlocks, and `_get_worker`'s outer check is unlocked on the `@trace` hot path. The roster now walks **both** modules, and its floor is **per module**: one total is cleared by one module alone while every site in the other stops being checked (measured, 37 against a floor of 36, passing). **A refactor shrinks a derived roster as silently as a deletion does** — two of this spec's own lint rules went dead in the commit that renamed their subject, with the file still green, and widening the scope immediately filed two SPEC-042 sites that had gone unfiled for two specs. (SPEC-040, arch §9.2) **SPEC-044 then locked the reads that must stay consistent.** A question is a single atomic load, but *acting* on one is not, and five races lived in that gap: a `shutdown()` that read "no worker" and returned having stopped a worker built one instruction later (`health()` reading `retired=True` over a live drain thread), a `_get_worker` that discarded an unclosed sink's close record, a log call that cancelled the stop signal a close was waiting on (8.01 s of an 8 s backoff, both paths), a swap that closed a sink without recording it, and a forked child hooking a superseded one. The fences: a shutdown-in-progress **depth counter** — a boolean is not nestable and two concurrent `shutdown()` calls are documented as normal, so the first to finish lowered it while the second still ran, reproducing the defect verbatim; a rule that **no transition clears the orphan close record without deciding who performs that close**; and an in-flight-close registry keyed on the **moment**, not on retirement, because retirement would reverse SPEC-033 FR-004's requirement that a sink adopted after `shutdown()` still backs off. The fence is deliberately not permanent — a worker built after `shutdown()` **returned** still delivers, which `_worker_health` had already settled and a permanent retirement fence would have superseded silently. What stays open and is recorded rather than half-fixed: `shutdown(timeout=)` does not bound the live sink's `close()` on either path (both bounding mechanisms were built and reverted; the third needs an interruptible `Sink.close`), and ~~two concurrent `configure(sink=…)` threads still double-close a sink at the same rate as before~~ — **SPEC-045 found that item was the wrong way round.** (SPEC-044, arch §13) **The record of which sinks the orphan path owes a close for is a set, not a slot**, and the defect was never a sink closed twice: it was the **live** sink closed by nobody. Arming a second sink discarded the first, so an ordinary `info()` that resolved a sink and was preempted armed a superseded sink over the live one — measured `C.closes == 0` with every `configure()` call sequential on one thread, which is what rules out a lock around a call documented as not thread-safe. A sink written to *after* its close has something new to flush, so a second close is owed rather than spurious: refusing it was built and measured stranding 2 of 3 events on a wrapper graph and losing on 31 of 80 fuzz seeds against 0 before, and two narrower variants each still lost. The set removes the trade instead of choosing a side. `_orphan_closed_sink` stays separate and unchanged — it names a sink a swap left *open*, which is a different claim from one that was closed. **Every site that consumes the record needs its own test**: three of the four consuming loops were found by mutation, not review, and the swap's worker branch survived a truncating mutant even after its no-worker sibling had one. (SPEC-045, arch §13) **The owed closes then run concurrently and are all joined** (SPEC-046), because draining a set in sequence cost one slow close *times* the number owed — 8.02 s for four 2-second closes against a 1.0 s budget, and 11.4 s at 200 sinks. Reusing SPEC-030's detached closer is the obvious move and loses data twice over: the grace is what remains of the budget (completed 1 of 4) and caps at `DEFAULT_CLOSER_GRACE` regardless (a 3 s close delivered nothing). **Joining beats detaching wherever there is no budget left to protect** — it is strictly better than the sequential drain on both axes, cost falls and loss stays zero — which is the opposite of `_start_closer`'s refusal to fall back inline, and the difference is whose budget is at stake. The join is in a `finally`: without it a Ctrl-C abandoned every close *mid-write* where the sequential drain abandoned one and had not started the rest, trading a leaked resource for a corrupt one. A single `Sink.close` is still unbounded. (SPEC-046, arch §12, §13)


### Only the forked *child* is repaired, and its order of work is the contract

**Only the forked *child* is repaired, and its order of work is the contract** — locks and events, then inherited buffers, then the registered handlers, because a lock re-initialised after a handler that takes it is a handler that hangs on the child's only thread. `before` does not run for a C-level fork at all (uWSGI calls `PyOS_AfterFork_Child` only), so the child handler must be sufficient regardless. The repaired roster is **derived, never listed** — a walk over this package's own objects, with an AST lint forbidding a primitive built where the walk cannot write it back, and an identity memo so a sink's `log_foundry_stop_signal` stays the worker's `_stop`. The worker is rebuilt **in place** with a replaced queue *object*; a retired parent forks a retired child. What the walk cannot reach is the caller's and is recorded, not half-fixed — including the shared sink, which both processes also *close*. (SPEC-039, arch §9, §13)


### A value the child inherits is stranded, never merely detached

**A value the child inherits is stranded, never merely detached** — `dup2` to `/dev/null` *and* reopen in **append** mode. Rebinding the stream alone leaves the old object flushing to the real file when the GC reaches it; reopening in `"w"` truncates a log shared with the parent, which passed 1626 tests because every test forked with the file empty on disk, making truncate and append the same program. The hook is per-sink precisely because `StdoutSink` has the same window and must **not** be fixed. (SPEC-039 FR-004)


## Event assembly: safety and bounds


### A reserved word needs exactly one route through, including its own name

**A reserved word needs exactly one route through, including its own name** — `echo` and `message` were parameters stealing ordinary words from the field namespace, and `fields=` is the escape hatch, so `fields` becomes the third reserved word and `fields={"fields": …}` must work. The keyword form wins a collision (`{**base, **overrides}`), and the merge **absorbs** a non-mapping rather than raising: it runs in the emitter, outside `api._log`'s orphan guard, so an unguarded merge broke SPEC-025's promise on all four paths. (SPEC-034 FR-004)


### An event is safe by construction — coerced and bounded once at assembly, not per sink

**An event is safe by construction — coerced and bounded once at assembly, not per sink** — `build_event` runs every value through `sanitize.py`, so all 40+ bare `json.dumps` calls in `sinks/` are correct by consequence, it costs one pass per event rather than one per destination (`MultiSink`), and the guarantee reaches the non-JSON sinks too. The unserializable fallback is a type-name placeholder, never `repr()`, so the fix cannot widen the PII exposure arch §6 prevents. Ceilings bound per *value*, not per event. (SPEC-017)


### A value too large to *render* is replaced, never clipped

**A value too large to *render* is replaced, never clipped** — `int` is the one type with no natural ceiling, and CPython refuses to render one past `sys.get_int_max_str_digits()`, so an over-long integer becomes `<int: ~N digits>`. Truncating digits would silently change the number, and a wrong number is worse than a visibly elided one. Detection is `bit_length()`, never `len(str(v))` — the obvious check raises the very error being prevented — with the ratio rounded so it errs toward replacing. (SPEC-020)


## The sink contract: delivery and its verdict


### The sink is a durable buffer, not the final store

**The sink is a durable buffer, not the final store** — ship to SQS (absorbs spikes/outages), a separate consumer indexes into ELK. `StdoutSink` is the zero-dep default. (arch §8, §9.1)


### A FIFO message group is a trace, not the process

**A FIFO message group is a trace, not the process** — `MessageGroupId` defaults to the event's `trace_id`: SQS orders *within* a group, and a trace is the unit whose events must stay ordered, while per-trace groups keep traces parallel instead of serializing everything behind one group. Overridable with a constant or a callable. Ordering is best-effort across a retry boundary, and sender faults are abandoned rather than re-sent byte-identical. (SPEC-016)


### A positional response adjudicates all of a chunk or none of it

**A positional response adjudicates all of a chunk or none of it** — an id-less per-record array must prove it describes the records sent (same length, right shape) before entry *i* may be read as record *i*; a mismatch is evidence of misalignment, so even the overlapping prefix is refused. What it cannot adjudicate is **abandoned and counted** (`dropped_unadjudicated`), never retried — the API reported a failure count, so some of the chunk landed and re-sending would duplicate downstream forever, while an abandoned record is a loss counted here and now. Id-keyed responses (`SQSSink`, `SNSSink`) select by `Id`, cannot mis-pair, and are deliberately not unified with this. (SPEC-018)


### A sink that delivered nothing raises; one that delivered something reports

**A sink that delivered nothing raises; one that delivered something reports** — the worker's retry, `failed_batches` and `flush()`'s verdict all run on an exception, so a sink that absorbs a total failure is a sink the worker *believes*: retry never engages, counters stay at zero, and `flush()` returns `True` while everything is lost. Raising is safe exactly when nothing landed, because there is nothing downstream to duplicate — which is also why partial failure must **not** raise (the worker retries whole batches). Absorbed loss goes to an optional `losses()`, aggregated into `Health.sink` and kept *nested*: `dropped` at the queue is backpressure, `dropped` at the sink is an event that never reached the wire, and one number would hide which fix applies. `losses()` is probed by name rather than declared on the Protocol, so a pre-SPEC-026 sink still satisfies `Sink`. Three cases stay silent by prior decision — an unadjudicable response (SPEC-018: cannot prove nothing landed, so a retry may duplicate), an SQS sender fault (SPEC-016: provably rejected, a byte-identical re-send can only fail again), and an oversized event (nothing to retry). The first is suppressed batch-wide and the second only when nothing *recoverable* was also lost: "unknown" and "rejected" are not the same claim. `losses().failed` is an upper bound on loss, not a count of it. (SPEC-026, arch §8, §9) **The same test decides where a loss with no worker behind it is counted** (SPEC-036 FR-003): `Health.orphan_lost` and `in_span_lost` are two fields because the orphan path can fail at the destination *or* the data while the in-span path can only fail at the data — and every other field describes a worker, so a process logging only outside a span read all zeros over total loss. A `MultiSink` child that reports nothing is charged to the fan-out in **events**, the unit `SinkLosses.failed` has, which is why `MultiSink.failed` — child *calls* — still stays out of the sum. **And a backend is chosen on whether it can *deliver*, not on whether it imports** (SPEC-043). `SentrySink` read an installed `sentry-sdk` as a working one, so an uninitialised process got a `NonRecordingClient` whose no-op `capture_event` reported success — the same sink-the-worker- believes shape, arriving through backend *selection* rather than through an absorbed failure. The capability check runs **per `emit`**, because `sentry_sdk.init()` legitimately follows the sink's construction and a once-at-construction answer would pin the wrong backend forever; it reads **two** client members, since `is_active()` is a class discriminator and two of the three undeliverable states report themselves active. `transport` is the member that binds, and **no acceptance criterion can tell the pair from `transport` alone** — recorded in the spec so a green AC is not read as evidence for `is_active()`. A client publishing neither member is *usable*, so the check cannot break an injected double. Selection is otherwise the caller's: `backend=` names it, an explicit `"sdk"` that cannot deliver is a refusal rather than a silent diversion to HTTP — substituting a backend quietly is this defect in a new place — and an argument no selectable backend can consume is a `ValueError`, not an ignore. That last rule covers the two *injection* arguments only, since an explicit `max_retries=3` is indistinguishable from the default. (SPEC-043)


### A redirect is a delivery failure, not a route to follow

**A redirect is a delivery failure, not a route to follow** — `urlopen`'s default opener follows `301`/`302`/`303` on a `POST` by rewriting the method to `GET` and **dropping the body while keeping every header**, so an `http://` collector behind a load balancer redirecting to `https://` lost every batch, forwarded the `Authorization` header to a host the caller never configured, and read the redirect target's `200` as delivery — `losses()` at zero, forever. Every sink in the family now opens through a private `build_opener` whose `redirect_request` returns `None`, so a `3xx` arrives as an `HTTPError` and takes the existing counted-and-announced `_abandon` path. **Refused, not followed correctly**: re-issuing the `POST` against the `Location` would make the library decide that an unverified host may have the batch and the credential, which is the caller's decision and not a logging library's. One site sets the opener and one calls it, so six subclasses, `LogstashSink`'s HTTP backend and `SentrySink`'s fallback are covered without a line of their own; an injected `opener=` is the caller's object and is used as given. **`307` and `308` were already refused** for a `POST` by the stdlib — the audit and the spec's first draft both said all five were followed, and the two extra parameters in the test are labelled regression pins rather than evidence, because an acceptance criterion that passes against the unfixed code proves nothing. The opener is built **per sink at construction**, not once at import: `python.md` forbids a module doing real work at import time, and `build_opener` snapshots `ProxyHandler`'s environment, so an import-time opener would pin whatever `http_proxy` said when `log_foundry` was first imported and silently ignore a proxy the application set afterwards — where `urlopen`'s lazily-built global opener would not. A sink is built once, so the cost is one object per sink and never per request. (SPEC-048 FR-001)


### A client exception costs its chunk, and is provable non-delivery for it

**A client exception costs its chunk, and is provable non-delivery for it** — the four AWS batch sinks called their client unguarded inside a chunk loop, so a `ClientError` on chunk N escaped `emit` after chunks 1..N-1 had landed, and the worker retries *whole batches*: measured `duplicates=10` for a 25-event SQS batch and `duplicates=500` for 1,000 Kinesis records, `losses()` reading `(0, 0)` throughout. The exit drain is one large batch by construction, so it is exactly this shape. **The guard goes inside `_send`, never around it in `emit`**: by the time `_send` raises it may hold a non-zero `accepted` from an earlier attempt and have narrowed its entries to the retryable subset, so an outer guard charges the whole chunk and reports a partial success as "nothing delivered" — provoking the very retry it was added to remove. **Treated as provable non-delivery**, which feeds `SQSSink`'s `recoverable_loss` term so a wholly-failed batch still raises; without that the guard converts a total failure into a silent one, which is SPEC-026's defect reintroduced by its own fix. The cost is real and recorded rather than argued away: a read timeout means the request went out and the *reply* was lost, so a re-send may duplicate — SPEC-018's "cannot prove nothing landed". It is taken on expected cost, because an unreachable endpoint is the common client exception and is precisely what the worker's retry exists for, while suppressing the raise for it would lose every event of every batch for a whole outage; `boto3`'s own `max_attempts` already carries the duplication property. `KinesisSink`/`FirehoseSink`'s `unknown` term is **not** set by the guard: SPEC-018's "unadjudicable" describes a *response*, and an exception is not a response. The roster that keeps this true is derived on **shape** — every `_send` calling `self.client.<method>(...)` — with a floor, because a hard-coded four-module list leaves a fifth AWS-shaped sink green. (SPEC-048 FR-002)

### A sink that released its transport refuses; one that released nothing keeps accepting

**A sink that released its transport refuses; one that released nothing keeps accepting** — the SPEC-026 rule applied to the sink's own lifecycle, where an absorbed batch is a batch the worker believes just the same. Both halves bind: three shipped sinks lost every post-`close()` event (and Redis *succeeded*, leaking a reconnect nothing reaps), while making the stateless sinks refuse would invent loss where a batch would have delivered — so which applies is a property of the sink, recorded per class and enforced. Refusing moves no `losses()` counter: it is a failure **reported** to the worker, not one absorbed, and counting both would report one loss twice. A close landing *mid*-batch does not raise even when it catches everything — `publish()` already happened, so the total-failure test is on **refusals**, not on successes (SPEC-018's rule that only provable non-delivery may be retried). The guard is keyed on the *sink* being released, never on client ownership, or every injected-client sink would keep accepting after `shutdown()`. The lint's scope gate stopped guessing at the same time — every class in `sinks/` with an `emit`, because a roster whose completeness is the point cannot rest on a heuristic, which is exactly how two of the three sinks stayed invisible for four specs. (SPEC-032, arch §8, §13)


### A destination's limit is found by halving the *budget*, not the chunk

**A destination's limit is found by halving the *budget*, not the chunk** — recursive chunk-halving is `2N-1` requests (11,954 measured for one exit backlog), because each accepted size is rediscovered in every branch; halving the budget re-chunks the remainder once per reduction and converges in `log2(ratio)` (8 for a 5 MB default against a 20 kB endpoint). Capping the recursion *depth* instead is the trap: a 250× ratio needs ~8 halvings, so a cap of 4 delivered 2 events of 2,000. Each reduction halves **what was refused**, not the nominal budget. A `413` is never retried — it is a verdict on the bytes — and a size already refused is not asked about again, **except under `gzip`**, where the refusal is uncompressed and the destination judges the wire. (SPEC-038 FR-001)


## The sink contract: waiting, concurrency and shutdown


### A value on the wire is measured on the clock that cannot move

**A value on the wire is measured on the clock that cannot move** — `RotatingFileSink`'s rotation deadline is `time.monotonic()`, as `Span.start_ts` already was, because a wall-clock deadline is defeated by any step larger than the interval. The *label* stays wall-clock wherever one exists; here none does, since backups are numbered rather than timestamped. (SPEC-031 FR-001)


### A sink's wait is bounded, interruptible, and never taken on a destination's word

**A sink's wait is bounded, interruptible, and never taken on a destination's word** — one drain thread means a sink's backoff pauses *all* delivery, and it spans `shutdown()`, so `time.sleep` is the wrong primitive: every sink waits on the worker's stop event, pushed onto it by the worker (`hasattr` probe, as with `losses()`) so `sinks` still never imports `worker`. A wrapper sink forwards it to whatever actually holds the retry loop — set on a wrapper the signal reaches nothing, which moves the defect rather than fixing it. `Retry-After` is advice, not an instruction: clamped to `max_retry_after`, and rejected outright when non-positive or non-finite (the test is `not (value > 0)`, because `value <= 0` reads `False` for `NaN`). Zero is rejected too — a rate-limiting destination saying "wait zero seconds" is far more likely truncated than meant. `shutdown()` is bounded because a sink blocked *in* a call still holds the thread, and an expired one leaves the sink **open**: the drain thread may still be inside `emit`, and a leaked resource in an exiting process beats a corrupt write. It reports through `stopped_reason` (`"ShutdownTimeout"`) rather than a new field, extending SPEC-019's vocabulary as that spec intended. (SPEC-027, arch §9) **A bound belongs to the BATCH, and whose bound it is decides whether to add one or expose it** (SPEC-047). `NATSSink` awaited a JetStream ack per *event* over an unbounded batch — measured 25.01 s for five against a stalled server, with `_final_drain` handing the exit backlog over as one batch — which is SPEC-038's "a bound applied per item is `n × timeout`, not a bound" in a sink that spec did not reach. One `publish_timeout` now covers the call, each publish taking `min(DEFAULT_ACK_TIMEOUT, remaining)`; the bare remainder would give the *first* event a longer ack wait than the driver's own default. It reads no stop signal and declares no `log_foundry_stop_signal`: that await is **work**, not an inter-attempt wait. `KafkaSink` gets **no** retry and no new bound — `produce()` is a 0.0001 s local hand-off and librdkafka's own retry is real (measured: the delivery callback at 300.18 s), so a second loop could only re-send messages the producer already owns, duplicating downstream against SPEC-018 and multiplying the worst case rather than bounding it. What was missing there was **reachability**, so `producer_config=` exposes librdkafka's bound and the five-minute default stands, since lowering it would drop what a process surviving a short outage delivers today. An argument the receiving backend cannot consume is a `ValueError`, never an ignore (SPEC-043) — but `publish_timeout` is outside that set, being the sink's own bound rather than a connect-time request. The exit drain is still `(max_retries + 1) × publish_timeout` and can exceed `shutdown()`'s join; recorded in §12 rather than closed, because bounding the worker's retry of an already-bounded batch is every sink's question. (SPEC-047, arch §12, §13)


### A sink tolerates concurrent callers; the library cannot serialize them for it

**A sink tolerates concurrent callers; the library cannot serialize them for it** — the worker drains on one thread, but a level call with no active span emits on the *caller's* thread, so `emit`/`close` are called concurrently against one sink object and `sinks/base.py` states that as a requirement on implementations. It cannot be a promise: the library does not own that thread. The lock is held for the **whole** operation assuming exclusivity, `threading.Lock` not `RLock` (a sink re-entering its own `emit` is a bug an `RLock` would hide), and `close()` takes it too, so it waits rather than releasing under a writer. Per driver, not per family: Postgres locks (one connection, one transaction), ClickHouse locks (per-session state, not published as shareable), **Mongo does not** (`pymongo` is thread-safe with its own pool, and the goal is correctness under concurrency, not the removal of parallelism) — each says which requirement it satisfies, Mongo included, or its bare emit reads as an oversight. Counters take a **second, dedicated** lock, ordered transport → counter: sharing one would make `health()` block behind an in-flight insert and its backoff, contradicting SPEC-026's "safe to call during an emit". The accepted cost is that an orphan log can now wait on the lock (arch §13) — bounded by SPEC-027, and the alternative is measured: unlocked `SQLiteSink` does not lose rows, it kills the interpreter with a bus error. The counter race is **not reproducible without injecting a preemption point** — a bare `+=` lost zero across 1.6M concurrent increments, though a property on the counter's storage does reproduce real loss — so the tests assert the increment happens *inside* the lock, the property that survives free-threading. **Which sinks lock is decided per driver and recorded in each sink's docstring, enforced by a lint**, because the first pass worked from the spec's hand-written file list and missed three sinks — `NATSSink` re-entering its own event loop could hang an application thread permanently. That is SPEC-027's roster lesson repeated; a roster in prose is not a roster the tests check. (SPEC-028, arch §9, §13)


### A terminal `shutdown()` and a captured sink are both reported, not prevented

**A terminal `shutdown()` and a captured sink are both reported, not prevented** — the two lifecycle mistakes the library documented and then stayed silent about. Logging after `shutdown()` is still *accepted*: refusing it would hide the mistake and restarting the worker would fight a process trying to exit, so `Health` reports the **pair** `retired` + `submitted_after_shutdown` — a pair because `retired` alone is correct usage, and a new pair because `stopped_reason` is `None` after a clean shutdown and must stay that way (SPEC-019). The check in `submit` is one unlocked read of a write-once flag, which is not the *liveness* check SPEC-019 excluded from the hot path. A late `configure(sink=...)` swaps the live target — drain to the old sink, reassign (never rebuild: the queue, thread, counters and `atexit` registration survive), **fence with a second drain**, then close — because the first drain only proves the *pre-swap* events landed, and closing while the drain thread is inside `emit` is what SPEC-028 exists to prevent. A drain that cannot be confirmed does not cancel the swap (the caller asked for that sink) but leaves the old one **open** and counts `incomplete_swaps`, on SPEC-027 FR-004's reasoning that a leaked resource beats a close raced against a write. **One deadline covers all four steps**, the close included: `Sink.close` takes no timeout, so it runs on a **daemon** thread joined for the remainder. The wrong-signal objection SPEC-028 reverted for is **dissolved by deriving no signal from an expired join** — no counter, no line, so a slow close can never latch a loss on a healthy swap — and the live fact is published instead, as `Health.closing_sinks`, a gauge that falls as well as rises and is deliberately *not* a term in the alert idiom. **Neither thread flag is sufficient alone and both were built:** non-daemon stopped `atexit` from ever running (CPython joins non-daemon threads first), losing the *live* sink; daemon alone kills a slow-but-succeeding close, losing the buffer of a sink whose `close()` *is* its delivery. So the flag is not the mechanism — **the capped grace is**: `shutdown()` closes the live sink, then joins any outstanding closer for `DEFAULT_CLOSER_GRACE`, carved from its own budget so it neither extends shutdown nor lets a stuck close hold the exit for the full 30 s, and granted on the idempotent path too (an expired first call returns before reaching it). Running *after* the live sink's close is defence in depth, not the guarantee — both orders measure identically, since the cap returns control first — but it is the right order and is pinned by a test. What SPEC-028 refused to abandon was the sink still being delivered to; this one is fenced out by two confirmed drains — but its interpreter-exit objection *does* reach here once the close outlives `configure()`, so §13 records that an abandoned close can land inside a `commit()`. `shutdown()`'s own close stays inline. (SPEC-030, arch §7, §9, §13)


### A sink handoff is owned by whoever is delivering, and "a worker exists" is not "a worker owns this sink"

**A sink handoff is owned by whoever is delivering, and "a worker exists" is not "a worker owns this sink"** — `_swap_sink` returned early on a null worker, so a late `configure(sink=...)` in a process that only logs outside a span left the previous sink open with `incomplete_swaps` at zero. The orphan path now records the sink **object** an emit reached, because `configure()` assigns `_config.sink` *before* the swap runs and a boolean cannot say which sink is owed; the record is **re-pointed** at the new sink rather than cleared, since clearing leaks the new one in a process that swaps and exits without logging again — a case the boolean got right. No drain and no fence: orphan emits are synchronous and have returned, and the one writer a fence could not exclude is the one `Worker._close_swapped_out` already documents itself as not covering. `incomplete_swaps` stays **worker-only** — it means an unconfirmed *drain*, and widening it would stop telling an operator whether events were misrouted or a close was merely slow. Two guards key on **ownership** (`_worker.sink is X`), because `Worker.swap_sink` returns early once `_shutdown_done`, so a retired worker keeps its old sink forever while events go to a newly configured one — the identity form still declines on an *expired* shutdown, which is what the original guard existed for. Review of the spec found two more instances of the same boolean: the once-only close was per **process**, so a sink configured after `shutdown()` was closed by nothing (measured losing a buffering sink's whole batch while `health()` read `retired=True`, `submitted_after_shutdown=0` — SPEC-030's pair needs a worker to count a submission), and `Worker._offer_stop_signal` was SPEC-027's only caller, so this path's backoffs were never interruptible. The stop event is **replaced, never cleared**: an `Event` cannot be un-set and `_retry.wait` returns instantly on a set one, so a live sink holding the shutdown's event backs off not at all. The closer machinery moved to `_lifecycle.py` because the state must be process-global — a closer started before any worker existed must still be counted by `closing_sinks` and still granted the exit grace. (SPEC-033, arch §7, §9, §13)


### A shutdown shortens a *wait*; it must never skip *work*

**A shutdown shortens a *wait*; it must never skip *work*** — `Worker.shutdown` sets the stop event **before** joining the drain thread, so it is set for the whole of `_final_drain`. Any sink consulting it to do *less* is therefore degrading itself on the exit drain, which is the one path a serverless process has. Measured four times in one spec: `HTTPSink` ending its 413 search delivered **nothing** of a 2,000-event backlog that had been going out in 30 requests; `KafkaSink` cutting its flush to zero delivered **0 of 11**, since `produce()` is a local hand-off and `flush()` is the only thing that drains the producer. `_retry.wait` is the one place the signal belongs — it shortens a backoff and cancels no attempt. (SPEC-038 FR-001 AC-4a, FR-006 AC-3)


### A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable

**A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable** — the record is stamped when the library is *handed* a sink (`configure()`, the lazy default) over the whole reachable graph, and every close consults it, the five shipped wrappers included. Write-once alone was not enough: it defends a record that already exists, so where the parent's bounded walk saw nothing a child could `configure()` its way into genuine ownership and destroy the parent's transport legitimately — measured on a socket. A fork handler marks everything inherited `_FOREIGN` **before any other handler runs**, which is what gives "no record" a terminal state. The one default that is *not* refusal is a sink no wrapper the library holds is releasing: that is the caller's own object, and refusing there turned `FilteringSink(inner).close()` into a silent no-op. Descent is bounded on a measurement (1,109 ms → 2 ms on a 100k-event `MemorySink`) and every gap it leaves fails toward a leak. `reacquire_after_fork()` is renamed from the discard: returning from it *is* a claim of ownership, which is why a subclass adding a transport must override it. The residual — a sink the parent held only in application state, first handed over by the child — is undecidable inside the rule and recorded in §13, not asserted away. (SPEC-042, arch §9, §13)


## Failure paths and diagnostics


### A dead worker is reported, not restarted — and as a *reason*, not a liveness flag

**A dead worker is reported, not restarted — and as a *reason*, not a liveness flag** — the drain loop is guarded end to end and records the exception type that ended it (`Health.stopped_reason`), because `dropped` climbing already means backpressure and must not double as "the thread is gone". A reason string is `None` for a live worker, a never-created one, **and** a cleanly shut-down one, so it extends the alert idiom by a term; an `alive` flag would read `False` on every process that has not logged yet. No auto-restart: a thread that resurrects itself fights a process trying to exit. Type name only, never the exception message (arch §6). (SPEC-019)


### Every path the caller stands on is total, and a swallowed fault is announced by *type*

**Every path the caller stands on is total, and a swallowed fault is announced by *type*** — the decorator (setup, body, close, teardown), the orphan emitter and its echo, and `shutdown()` with its `atexit` drain all absorb an `Exception` and report one `_diag.absorbed` line rather than raising. Never `BaseException`: a `KeyboardInterrupt` or `SystemExit` is the operator's or the runtime's intent and must reach the caller — the same line SPEC-019 drew in the opposite direction for the worker thread, where the *absence* of a handler was the defect. A pre-body fault degrades to an **untraced call**, never a failed one; a failed close is announced, not retried (the once-only flag stays ahead of it, because a second `close()` on a partially released sink is worse than an unclosed one). Only the type is written, never the message (arch §6). `_diag` must import nothing from its own package. (SPEC-025)


### One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times

**One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times** — which is exactly how twelve sites came to print `repr(exception)` while the other eight printed a type name, and how two came to be unguarded. `_diag` owns `absorbed`/`lost`/`rejected`: an exception is named by `type(exc).__name__`, and where that is not diagnosable (an `OSError` is not "refused" vs "host unknown") the caller passes a detail built from values the *library* controls — an `errno`, an HTTP status, an attempt count — never from the exception's text. Any detail is escaped **then** bounded, so the bound governs what is written, and `isprintable()` is the escape test rather than a C0 table: `splitlines()` breaks on three separators such a table misses, so a newline count would call a forged line safe. The one bounded `repr` is `rejected`, whose input is an inbound *header* rather than an exception — and it is escaped afterwards anyway, because `repr` escaping newlines is a property of the built-ins, not of `repr`. A test forbids any other module writing to stderr; it is a lint on the idiom (`stderr.write`, `print(file=…)`, `traceback.print_*`), not a sandbox. (SPEC-029, arch §6)


## The public API surface


### Logs-only, send everything for now

**Logs-only, send everything for now** — no metrics/OTel-native traces; sampling is deferred and **unbuilt** — no `should_send` exists in code, and the per-span flush makes the pipeline span-outcome-ready, *not* tail-sampling-ready. (arch §10, §13)


### An extra's floor is a published contract — moved deliberately, never by a bot

Dependabot's first `pip` PR raised `boto3`/`sentry-sdk`/`pika` past floors that already admitted the new release. Those raises were **kept** (staying near-current on boto3 is worth the narrowing) but `versioning-strategy: increase-if-necessary` stays, so the floors now move only when a human decides they should. A floor raise is a contract change: it cuts a release **minor**, not patch. (`v0.9.0`)


### A public accessor hands out a copy; the library reads the live object

**A public accessor hands out a copy; the library reads the live object** — `get_config()` and `get_baggage()` copy, because a public getter documented "do not mutate" is a promise the caller's slip breaks silently, while `config._live_config()` and `context._live_baggage()` are the per-event reads, since `build_event` runs one to three config reads and one baggage read **per event** and a copy there allocates per event. Both copies are **one level**: deep-copying arbitrary caller objects inside an accessor that must never raise trades a narrow sharing bound for a wide new failure, so the bound is stated and pinned rather than closed. Freezing `Config` also turned every write into a read-modify-write, and one writer (`_ensure_sink`) runs on the orphan logging path — measured, one concurrent `info()` permanently reverted `configure()` in 268 of 2000 trials — so `_config_lock` serializes the writers while reads stay lock-free. (SPEC-034 FR-003, FR-005)


### A result that can grow a reason must stop being a `bool` before 1.0, not after

**A result that can grow a reason must stop being a `bool` before 1.0, not after** — `flush()` answered five outcomes with one bit and `continue_trace()` two. A `NamedTuple` cannot be retrofitted (a non-empty tuple is always truthy, so every `if flush():` would silently keep passing), so `FlushResult`/`ContinueResult` carry `__bool__` plus a `reason`, and grow by new reason values only. `Worker.flush` carries the type too: the five outcomes are distinguishable only there. For the same reason `Health` and `SinkLosses` became frozen dataclasses — six specs had each argued their appended field left the indices undisturbed, and with a dataclass there are no indices. (SPEC-034 FR-007, FR-008)


### A protocol that is exported is a protocol that will be inherited

**A protocol that is exported is a protocol that will be inherited** — `Sink`'s members were empty-bodied and not `@abstractmethod`, so a subclass with one typo instantiated happily and its inherited `emit` returned `None`: three events gone, `flush()` truthy, every counter zero. `mypy` refused it and only the runtime did not. Structural satisfaction is untouched, which matters because no shipped sink inherits it. (SPEC-034 FR-005)


## Release, supply chain and naming


### Version comes from Git tags, published to PyPI as `log-foundry`

**Version comes from Git tags, published to PyPI as `log-foundry`** — tags cut releases, merges to `main` publish `.devN` pre-releases. (SPEC-012)


### Every action is pinned to a commit SHA, and the pins are maintained, not frozen

**Every action is pinned to a commit SHA, and the pins are maintained, not frozen** — a mutable tag on a workflow holding `id-token: write` against PyPI is a silent path from a third-party repository to every consumer's `pip install`, so `pypa/gh-action-pypi-publish` is pinned away from the `release/v1` branch PyPA itself recommends: a lagging pin fails loudly at release time, a compromised action fails forever. Dependabot's `github-actions` ecosystem moves the pins, which is what makes pinning affordable — the version comment must stay exactly `# vX.Y.Z` or it silently stops rewriting it. Pin to the tip of the major in use; a major bump is its own reviewable PR. (SPEC-022)


### A scanner that exits zero has not said "clean"

**A scanner that exits zero has not said "clean"** — zizmor in SARIF mode and CodeQL both report to code scanning and pass the job regardless of findings, deliberately: Advanced Security owns triage, and blocking belongs in a ruleset. Only `dependency-review` fails a build. So the alert count is the verdict, never the check mark — and a green audit is not evidence a *setting* is present (zizmor's `dependabot-cooldown` stops at the first passing entry). State the setting. (SPEC-022)


### An SBOM describes the published artifact, and is generated from it

**An SBOM describes the published artifact, and is generated from it** — `make-sbom.py` installs the built wheel with every extra into a throwaway venv and describes *that*, because runtime dependencies are empty by design and the extras are the whole dependency surface. The generator runs from a *second* venv or it lists itself and its ~30 dependencies as the library's (measured: 98 components vs 43). `cyclonedx-py`'s `poetry` mode cannot read this project at all — it wants `[tool.poetry].name`, and PEP 621 puts the name in `[project]`, the same misreading `dependabot.yml` documents. An empty SBOM, one versioned `0.0.0`, or one carrying build tooling fails the job: an inaccurate SBOM is worse than none, because it looks authoritative. (SPEC-023)


### Release assets are attached to a draft, never to a published release

**Release assets are attached to a draft, never to a published release** — this repository has immutable releases enabled, so assets freeze at publish: create-as-draft → upload → publish. And deleting an immutable release does **not** free its tag name, so a botched release is repaired only by a new version tag, never by recreating the old one. Both were learned by shipping `v0.10.0` without its SBOM and then making it unrepairable. The job is deliberately *not* idempotent — a re-run that claimed to replace an asset it cannot touch would be lying. (SPEC-023)


### `pip-audit` gates, and audits the extras or it audits nothing

**`pip-audit` gates, and audits the extras or it audits nothing** — `dependency-review` only sees a PR's dependency *diff*, so an advisory against an already-pinned dependency is invisible to it; the weekly re-examination is the point. `--no-root` is load-bearing (Poetry installs the project editable, and `--strict` refuses an editable distribution), and `--strict` is on because a silently skipped package is an unaudited one. Suppressions are per-advisory with written reasons in `.github/pip-audit-ignores.txt`, never by severity or package. (SPEC-023)


### One name everywhere: `log-foundry` / `log_foundry`

**One name everywhere: `log-foundry` / `log_foundry`** — the import package was renamed from `log_forge` in `v0.2.0` so it matches the distribution name. Breaking for `0.1.x` users; no compatibility shim was shipped. Historical `log-forge` mentions survive only where they name the PyPI-rejected original.


## Working rules: findings, rosters and testing bounds


### A subclass that inherits a method is still in the roster

**A subclass that inherits a method is still in the roster** — scope keyed on *defining* one makes membership a function of where code happens to sit, and moving five `emit`s into a base dropped those classes out of two lints in a single commit, 34 → 29, with the suite green. Only the sibling roster noticed, and only because it had a floor guard. Both rosters now scope on defines-or-inherits, both carry floors, a class overriding neither `emit` nor `close` may answer from the ancestor whose code it runs, and **a test asserts the two cover the same classes** — they had already drifted twice on trigger and on base spelling. (SPEC-038 FR-001 AC-1a/AC-1b)


### A bound is only a bound if it is measured where it binds

**A bound is only a bound if it is measured where it binds** — the recurring shape behind five of this spec's seven blocking defects. A wall-clock assertion cannot see a busy-spin (a slice loop bounded at 1.00 s wall burned 3.5 M calls and a pegged core, so the test asserts **CPU** time); a timeout applied per *item* is `n × timeout`, not a bound; and a chunk size is a floor division, so a byte-charging test only bites at sizes where the division tips — the same test was vacuous at 1,012 bytes (the count limit binds first) and again at 9,000, and now asserts its own sensitivity as a precondition. (SPEC-038 FR-004, FR-005)


### An open item is closed by being fixed, settled, or recorded as a constraint — never deleted

**An open item is closed by being fixed, settled, or recorded as a constraint — never deleted** — a note that is merely removed takes its reasoning with it, and a reader cannot tell a live defect from a decision that reads like one. Superseded notes are struck through in place and marked with the spec that closed them. ~~`architecture.md` §12 carries no open items and §13 states the constraints.~~ — **superseded 2026-09-01.** The rule is one-way by construction: §13 grew from 68 lines to 640 over seventeen specs with nothing pruning it, and unfinished work accreted there among the constraints while §12 read "None". **§12 is what is unfinished** — a defect nobody scheduled or a question left open, each entry naming what would close it — and **§13 is what the design will not do**, a limit it accepts and keeps accepting. A closed item is struck in place with the spec that closed it and its reasoning is *not* repeated in §13: that lives here in Key Decisions and in the delivery doc, and a third copy is a fork with no merge. (SPEC-021, revised 2026-09-01)


### A read-only finding is not closed until it has been run, and the job that runs it needs a floor rather than an exit code

**A read-only finding is not closed until it has been run, and the job that runs it needs a floor rather than an exit code** — fourteen sink modules reach a third party through one of eleven optional extras and none was ever executed, so the sinks most likely to be in production were the ones least verified. All three of SPEC-041's inherited "read-only" findings held up, and each was *reproduced before being fixed*: one `pg_terminate_backend` ended `PostgresSink` delivery for the process, a stock Logstash turned a batch of three into **one** event with every field as text in `message`, and NATS delivered **one of six** with every counter at zero. The guard on such a job is the repo's roster idiom, not `pytest`'s exit 5: exit 5 catches only a forgotten gate variable, while a fixture that skips on an absent service exits **0** — measured, a dropped module reads `13 passed` and a skip reads `15 passed, 1 skipped`. So an absent service **fails**, and a per-module floor makes a silent shrink loud. What stays unverified is a **derived** roster (`tests/test_sink_integration_roster.py`), and its population is the lazy-import modules ∪ those importing *or defining* `HTTPSink`/`SocketTransport`, because the obvious single-marker derivation silently omits `logstash` — the spec's own named minimum. **The job earned itself on its first run**, failing three tests no local run could: a service that has bound its port has not necessarily begun serving, so readiness is a real request, never a connect. (SPEC-041, arch §8)

