# Invariants — what the library promises, as things you can observe

One page, numbered, loaded before a spec is written and before a diff is reviewed. Each entry is a
promise stated as an **observable** — something a probe can measure and a test can assert — with
where the exceptions to it are recorded and where it is guarded today. It is not the decisions
register (`docs/decisions/` holds the *fences*: what was built, what was rejected and why) and it is
not the constraints list (`architecture.md` §13 holds what the design accepts it cannot do). A
fence explains a mechanism; an invariant is what every mechanism, present and future, must keep.

**How it is used.** A spec's acceptance criteria name the invariant(s) each FR serves, by number
— `scripts/spec-lint.sh` fails a Draft or In Progress spec whose FR names none, or names a number
this page lacks; an FR that keeps no invariant says so with the exact phrase `serves no invariant`,
and the template gives the spellings.
The second diff reviewer — the one that starts from the system rather than the change
(`docs/process/reviewer-contract.md`) — starts from this page: for each invariant the diff touches, it asks whether
the change keeps it on **every twin path** (invariant 6), not only the one the spec names. An
audit's "not covered" list is a list of invariants nobody has measured lately. A *behavioural*
finding that breaks no invariant here is a candidate for a new one; prose, lint and hygiene
findings are judged by their own rules (`docs/process/completion-ritual.md`).

**Where an open defect against an invariant lives:** `architecture.md` §12, never here. This page
states what is true when the library is correct; it does not carry a changelog.

---

## 1. The library never fails the caller

Every public call returns normally on a fault of the library's own. A `@trace`d function's own
exception is re-raised unchanged, with the span recorded as an error. The guards catch
`Exception`, never `BaseException`, so a `KeyboardInterrupt`, `SystemExit` or `CancelledError`
raised inside a library fault still reaches the caller, because those are the operator's or the
runtime's intent (SPEC-025).

*Observable:* once a call has been accepted, no exception whose innermost frame is in
`log_foundry` reaches application code, on any input, on any thread, in any lifecycle state.
*Recorded exceptions:* a constructor, `configure()` or `@trace` may refuse an unusable argument
at the call that supplies it — that is invariant 13, and it is the *opposite* promise for the
moment of construction. So may `flush()` and `shutdown()`, for their `timeout` alone: a NaN is a
`ValueError` and a value that is not an `int`, a `float` or `None` a `TypeError`, raised at entry
before anything is drained, claimed or counted, so the call was never accepted and a retry with a
good value works (SPEC-066).
*Guarded:* `tests/test_api.py`, `tests/test_decorator_sync.py`, `tests/test_sanitize.py`,
`tests/test_assembly_budget.py` (a hostile argument returns on both delivery paths before a
deadline), `tests/test_baggage_header.py` (a hostile baggage value, on either intake route, leaves
`current_baggage_header()` returning before a deadline, SPEC-064), `tests/test_inbound_totality.py`
(every hostile probe, in each of `continue_trace`'s arguments, returns before a deadline and runs
none of its own code, SPEC-067), `tests/test_numeric_doors.py` (a refused `flush()` or `shutdown()`
timeout moves no counter and does not retire, SPEC-066);
`tests/test_invariants_model.py` asserts it over random interleavings.

## 2. Every accepted event is delivered, or counted and announced — exactly once

Once a level call or a span close has returned, the event has one of three futures: it reaches
the sink, or it is counted in exactly one loss term and announced on stderr (throttled where it
can repeat, invariant 11), or it is still queued where a `shutdown()` left it — in which case
`retired` is set and `submitted_after_shutdown` counts it (SPEC-030). Nothing is lost silently
and nothing is counted twice.

*Observable:* the ledger balances. For spans, `span.end` delivered + `queued` + `dropped` equals
spans started; for orphan logs, delivered + `orphan_lost` equals calls made; an event that could
not be *built* is in `in_span_lost` or `orphan_lost`; loss a sink absorbed is in `health().sink`.
*Recorded exceptions:* `dropped` counts submissions, not events, and a sink's `failed` is an upper
bound (`health()` docstring); `queued` also counts a `flush()` marker stranded by a racing
`shutdown()`, for the life of the process (`Health.queued` docstring, SPEC-050 FR-001); ~~three
losses are counted but never retried, because a retry would duplicate — an unadjudicable batch
response, an SQS sender fault, an oversized event (SPEC-018, SPEC-016, SPEC-017)~~ — superseded
by SPEC-058: five losses are counted but never retried by the sink — an unadjudicable batch
response (since SPEC-070, a Kinesis or Firehose failed count its coded records contradict among
them), an SQS or SNS sender fault (SNS since SPEC-070), an oversized event (SPEC-018, SPEC-016,
SPEC-017), a wrapper's or
an SQS id callable's per-event failure when its siblings were delivered, and an event a sink
cannot encode — first an unrenderable HTTP event (SPEC-058), and since SPEC-069 an event any
record-building sink's encode step refuses — because a retry would re-deliver what landed beside
them, or fail the same way.
A sixth joins them with SPEC-061 FR-003: a core `NATSSink` batch whose confirming round trip went
unanswered is counted in `failed` and announced once, never retried, because the bytes are in
flight to a server that may yet process them (five of five did after a `docker unpause`) and a
retry would duplicate them; the next call is refused instead, so the stall is still reported.
So is a `NATSSink` publish the driver never returned from within the budget, on every path where
no confirmation will judge it — JetStream, and an injected client of either mode — counted as
unconfirmed where it is abandoned, with every later call refused while it is still pending
(SPEC-061 FR-001). That leaves a JetStream asymmetry, recorded rather than removed: an ack wait
that expires raises in the driver, is counted as a raised publish, and a batch in which nothing
else landed goes back to the worker, which retries it (SPEC-047's behaviour) — and every ack wait
the budget caps does expire, because the sink hands the driver a timeout that ends inside its own
wait (`DRIVER_DEADLINE_MARGIN`; before that margin the two tied and every such wait read as
abandoned, SPEC-061's diff review frame 3); a publish the driver never returns from — a force-flush
into a peer that stopped reading, or a driver ignoring its own timeout — is abandoned, counted as
unconfirmed, and never retried. Both mean the same thing — no ack, and the event may have been
persisted — so the first can duplicate while the second can lose. The closure is `architecture.md` §12's `Nats-Msg-Id` deduplication item, which
makes a retry safe.
At every shipped site that decides a per-event drop, a `dropped` count is made only by an `emit`
that returned; one that raised counts none of them, and whoever caught it — the worker, the
orphan path or `MultiSink` — owns the batch. The sites are the drops `TransformSink`,
`FilteringSink`, `SQSSink._chunks` and `HTTPSink._items` decide and `MultiSink`'s absorbed count
(SPEC-058), and the oversize drops of the record-building sinks and the socket transport, the
hand-offs `KafkaSink` and `GooglePubSubSink` see refused, and `HTTPSink._drop_refused`'s `413`
(SPEC-059), and the encode step every record-building sink runs before its first bulk call,
`sinks/_encode.encode_events`, whose count lands in `_unencodable` (or `rejected` for `KafkaSink`
and `GooglePubSubSink`, and on `SocketTransport` for `SyslogSink` and socket-mode `LogstashSink`)
(SPEC-069). A counter some `losses()` reports as `dropped` is written only by that commit: the
counters are derived from the tree and the rule checked over it (`tests/test_deferral.py`). The
exception is a sink's own `failed` count, the upper bound above: it can land before a raise and
again each time the worker retries that batch, the SQS and SNS sender faults included
(`SQSSink._send` and, since SPEC-070, `SNSSink._send` count one before `emit` raises for a
recoverable loss beside it), a failure the sink's retry predicate does not accept, counted at its
first attempt before `emit` raises when nothing landed (SPEC-070), and the batch size a
`SQLiteSink`, `FileSink` or `RotatingFileSink` adds to `failed` when its driver raises, before it
raises in turn (SPEC-069 FR-004). A third-party sink that counts
a drop before raising is outside what the library can hold to this (`MultiSink.emit` names the
cost).
A sink that offers `deliver` (SPEC-072) reports instead of counting: the delivery engine is the one
writer of its `SinkCounters`, committing once at the end of a run through one `Deferral`.
`dropped_events` is committed only by a run that settles, and is exact; a run that fails commits
only `failed_events` and hands the batch to its catcher — the worker's abandon or declined line and
`failed_batches`, the orphan path's `orphan_lost`, or the inline adapter's raise — so a catcher's
retry of an adapter's raise counts no drop twice. `failed_events` takes each event at most once per
run, never once per round, and is the same upper bound as every `failed`: an event of a failed run
is also in its catcher's term. The engine is a fourth catcher, for a `deliver` that raised. The
contract gives the never-retried losses above their outcomes — `UNKNOWN` for an unadjudicable
response and both NATS cases, `REJECTED` for a sender fault, `DROPPED` for an oversized,
unencodable or id-callable failure, and `SKIPPED`, counted in the wrapper's own `_dropped`, for a
wrapper's per-event failure (SPEC-072 FR-003) — and the engine resends none of them unless
the sink declares itself idempotent, which no shipped sink does. No shipped sink defines `deliver`
yet; the sinks that adopt it in later specs return those outcomes.
*Guarded:* `tests/test_worker.py`, `tests/test_sink_losses.py`, `tests/test_deferral.py`,
`tests/test_encode.py` and `tests/test_sinks_encode.py` (the encode step over a roster derived from
the tree, SPEC-069), `tests/test_classify.py` (every retry loop counts once at the attempt that
failed, the sender faults and the contradictory counts, SPEC-070), `tests/test_delivery_engine.py`
(the engine's counting table and run-failed rule) and `tests/test_delivery_paths.py` (each catcher
of a failed run, SPEC-072); `tests/test_invariants_model.py` is the identity itself, for an `emit`
sink and, holding each term to its exact share, for a `deliver` sink.

## 3. A wait is bounded by the caller's timeout, and is idle while it waits

`flush(timeout)`, `shutdown(timeout)` and `configure(sink=…)` return within their budget, and a
bound is measured in CPU as well as wall clock, so a busy-spin cannot pass as a wait (SPEC-038).
Every backoff waits on the worker's stop event, never `time.sleep`, so a shutdown cuts it short
(SPEC-027). A shutdown shortens a wait; it never skips work (SPEC-038). The delivery engine's waits
between rounds are the same (SPEC-072): each goes through `sinks._retry.wait`, capped at `MAX_WAIT`,
on the worker's `_stop` or, on the orphan path and in the inline adapter, on the sink's
`log_foundry_stop_signal`; a server's hint is bounded by the sink's `max_retry_after`; and SPEC-062's
prediction is asked before every further round, so on the exit drain an engine run declines a round
predicted to end past `shutdown()`'s deadline, as the 1.x `emit` loop does.

*Observable:* elapsed wall time ≤ the timeout plus the sink's one in-flight call; CPU time ≈ 0.
*Recorded exceptions:* `Sink.close()` takes no timeout and is unbounded on both delivery paths;
what bounds the exit is `DEFAULT_CLOSER_GRACE`, carved from the shutdown budget
(`architecture.md` §13). A sink blocked *inside* a network call holds the thread for that call.
An engine run whose stop event's `wait` raises announces it once and sleeps the rest of its waits,
each still capped, rather than letting the exception out or skipping a wait; and an engine sink that
declares no `log_foundry_stop_signal` sleeps its waits on the orphan path and in the adapter, as a
1.x sink without the attribute backs off uninterruptibly (SPEC-072).
*Guarded:* the wall-clock half by `tests/test_worker.py` (against a wedged sink),
`tests/test_sink_retry.py`, `tests/test_sinks_nats_liveness.py` (every `NATSSink` wait, the
backstop and the latch, SPEC-061), `tests/test_sinks_nats_matrix.py` (those waits and both latches
as a mutation-gated corpus, including the next and the queued caller's latency), `tests/test_worker_exit_budget.py` (the exit drain's
retries fit the `shutdown()` budget, SPEC-062) and `tests/test_delivery_paths.py` (the same cases
through `deliver`, SPEC-072); the CPU half by `tests/test_sinks_pubsub.py`,
`tests/test_concurrent_owed_closes.py` (a `shutdown()` with no close in flight),
`tests/test_worker_exit_budget.py` (each of its D/T cases' `shutdown()` over the exit drain,
through one measuring helper), and through that helper `tests/test_delivery_engine.py` (a stop cuts
a hinted engine wait short, idly; the 1.x schedule, the hint's bounds and a raising stop event,
SPEC-072) and `tests/test_delivery_paths.py` — `flush()` has no CPU-time assertion, listed so the
gap is visible.

## 4. Inside a span, the caller's thread never carries sink I/O; outside one, it does

A `@trace`d function hands its buffer to the worker and returns; the sink is reached on the drain
thread. A level call with no active span emits synchronously on the caller's thread, against the
same sink object the worker drains into — which is why a sink must tolerate concurrent callers
(SPEC-028) and why the orphan path can block on a sink's lock or backoff (`architecture.md` §13).

*Observable:* inside a span, `info()` latency is independent of the sink; outside one, it is the
sink's.
*Recorded exceptions:* a core `NATSSink.emit` costs two server round trips on the calling thread,
one before its first publish and one after its last, which on the orphan path is the application's
own (SPEC-061 FR-003); measured at `71e42dd` against the prototype, a one-event emit went from
0.04 ms to 0.47 ms and a 500-event emit from 0.90 ms to 1.26 ms, the price of a publish that is
confirmed rather than believed.
*Guarded:* `tests/test_api.py` (which path each call takes), `tests/test_sink_concurrency.py`.
Not measured as a latency bound by any test — listed so the gap is visible.

## 5. A sink the library wrote to is closed, once, by one owner, and a close in flight is waited for

Whoever is delivering owns the close: the worker for its sink, the orphan path for a sink it
reached with no worker, the swap for the sink it replaced. Ownership is recorded when the library
is *handed* a sink and consulted by every close, so a process releases only a transport it
acquired here (SPEC-042). The record is a set, so arming a second sink never discards the first
(SPEC-045); closes run concurrently and are all joined (SPEC-046); a second caller finding a
close already running waits for it rather than returning through it (SPEC-050).

*Observable:* after `shutdown()`, every sink that received an event has `close()` called on it,
and no sink is closed by a process that did not open it.
*Recorded exceptions:* a swap whose drain cannot be confirmed leaves the old sink open until a
later `shutdown()` finds the drain thread ended (SPEC-050); an expired `shutdown()` leaves the sink
open, and only a later call that finds the drain thread ended closes it — so in an `atexit` exit
after an expired call, a sink still inside an emit is not closed at all (SPEC-027 FR-004, decided
final by SPEC-068); a sink written to after its close is owed a second close, which is correct
rather than a double (SPEC-045).
*Guarded:* `tests/test_owed_closes.py`, `tests/test_shutdown_lifecycle.py`,
`tests/test_sink_ownership.py`; `tests/test_invariants_model.py` asserts the first observable;
`tests/test_sinks_nats_liveness.py` that a `NATSSink` close in flight is waited for under its
lock, its connection released once, and a forked child releases nothing (SPEC-061).

## 6. Twin paths keep the same promises

The library has pairs of code paths that must behave identically under invariants 1–5, 7–8 and
13: the **worker** path and the **orphan** path, ~~and the bookkeeping around them~~; the **sync**
and **async** decorator bodies; a sink's `emit` and its `flush`; the `_drain` loop and
`_final_drain`; `flush()` and `shutdown()` as the two answerers of a flush marker. A fix applied to
one twin is a recurring shape in this repo's history — SPEC-033's two same-day regressions, the
SPEC-035 guard sites, SPEC-050's daemon-thread close on both paths. This invariant is a review
obligation rather than a mechanism: a spec names the twins its FRs touch, and the system-frame
reviewer checks each.

**The two delivery paths are still twins for the *emit*; their bookkeeping is not** (SPEC-054).
Thirteen twin pairs — the retirement latch, the stop event, the owed-close record, the closer, the
in-flight gate, the grace arithmetic, the swap, the `Health` assembly and the rest — were one
mechanism implemented once per path, and they are one mechanism now. That is the strongest form
this invariant takes: not a review obligation but a shape in which the fix cannot land on one side.
What remains a twin is what the two paths genuinely do differently, which is where the event goes
and on whose thread.

**A sink's `deliver` and its `emit` are a twin pair too** (SPEC-072), and both delivery paths run
the same engine over `deliver`. The worker and the orphan path each call `run_delivery` first, so
one probe and one engine serve both, and fall back to `emit` unchanged for a sink that does not
offer it; the worker spends the engine's budget with no whole-batch resend around it, and the
orphan path runs the sink's own `max_retries` on the caller's thread. A sink's `emit` that is
`emit_via_deliver` runs the same engine inline, so a wrapper calling `inner.emit(batch)` and the
worker calling `deliver` keep one ledger in the sink's `SinkCounters`.

*Observable:* a probe run against one twin and its sibling produces the same ledger.
*Guarded:* `tests/test_worker_predicate_roster.py` derives the guard sites from the AST rather
than a hand list; `tests/test_invariants_model.py` drives both delivery paths in one run and
catches the races reached at an unforced rate — a window a few instructions wide needs an
injected preemption point, which is `tests/test_lifecycle_races.py`'s job, one race at a time;
`tests/test_delivery_paths.py` sends each of six outcomes, a raising `deliver` and a malformed
verdict as one event through the worker and through the orphan path and holds the two to the same
`SinkLosses`, and drives `emit` and `deliver` of one sink through a real `Worker` (SPEC-072).

## 7. A sink that delivered nothing raises; one that delivered something reports

Total failure raises `SinkDeliveryError`, so the worker's retry engages and `failed_batches`
moves. Partial failure returns and reports through `losses()`; a client exception costs its own
chunk and nothing else (SPEC-026, SPEC-043, SPEC-048). A redirect is a delivery failure, not a
route to follow (SPEC-048). A sink that released its transport refuses after `close()`; one that
released nothing keeps accepting (SPEC-032).

**A sink that offers `deliver` states the same promise per event** (SPEC-072): it returns a
`Verdict` rather than raising, and the engine applies the rule to the run. A run **fails** when no
event was `ACCEPTED` (`SKIPPED` is not), at least one ended `REJECTED`, still `RETRYABLE` when the
rounds ran out or were declined, or in a round whose `deliver` raised, and none ended `UNKNOWN` or
in a round whose verdict could not be read; otherwise it settles. A failed run is a failed batch:
the worker moves `failed_batches` once with no whole-batch resend, the orphan path moves
`orphan_lost`, and the inline adapter raises `SinkDeliveryError`, so a wrapper calling
`inner.emit(batch)` sees what a 1.x `emit` showed it. A settled run moves no failed-batch counter
and its losses are counted in `SinkCounters`; across the rounds the engine offers again only the
events still to send, so no event a verdict called `ACCEPTED` is sent again.

*Observable:* with a destination that fails every call, `flush()` is falsy and a counter moved;
with one that fails some calls, nothing raised, `losses()` is non-zero, and no event landed twice.
*Recorded exceptions:* an `emit` may return normally having delivered nothing, because what it did
not deliver was counted — an all-oversized batch, a batch of nothing but events the sink cannot
encode (an HTTP batch it cannot render, SPEC-058, and since SPEC-069 the same batch on every
record-building sink, each event counted `dropped`; `NATSSink` and `SentrySink`, the declared
exceptions, still count such an event `failed` and raise for a batch of nothing else), a wrapper
batch of one failure and otherwise deliberate drops, an SQS batch of an id failure and an oversized
or unencodable event (SPEC-058, SPEC-069), and an SQS or SNS batch rejected wholly as sender faults
(SPEC-016 for SQS, SPEC-070 FR-003 for SNS). Nothing in such a batch is helped by a retry, which is
the test this page's rule turns on. So a `_bulk` server answering every POST with a `2xx` the sink
cannot adjudicate — a body it cannot read or that is not an object, or a truthy `errors` with a
short `items` array — leaves `flush()` truthy and `failed_batches` at zero, with the loss in
`health().sink`: the events may have landed, so a retry could duplicate them. A Honeycomb batch
`200` the sink cannot adjudicate — a body it cannot read, not an array of objects, the wrong length,
or an entry whose `status` is not an integer — leaves the same three signs. An HTTP `2xx` is a
verdict however its body ends: a body cut short, stalled or reset after the status line settles its
chunk — delivered for a sink that ignores the body, unadjudicated for the `_bulk` sinks,
`HoneycombSink` and `OTLPSink`, whose verdict is in the body (SPEC-070 FR-004) — and is never
re-sent; before any status line, a protocol error is retried and counted like a connection error.
An OTLP server answering every POST with a `2xx` whose `partialSuccess` is present but cannot be
read, or whose body cannot be read, or with a `2xx` over the 4 MiB response bound, leaves the same three signs for the same
reason (SPEC-056); only a `2xx` rejecting every record of every chunk raises there, because nothing
landed — and the same holds for a Honeycomb `200` rejecting every event of every chunk. A core
`NATSSink` batch whose confirming round trip went unanswered leaves them too (SPEC-061 FR-003):
`emit` returns having counted the batch in `failed` and announced it once, because its bytes are in
flight to a server that may yet process them, and the next call is refused by the probe or its
latch, so a sustained stall is still reported as failed batches and only the one batch that
straddles its onset is counted rather than raised.
The recorded exception extends to `deliver` (SPEC-072 FR-004): a run whose verdict marks an event
`UNKNOWN`, or whose verdict cannot be read — not a `Verdict`, outcomes that are not exact `Outcome`
members, the wrong length, or a field that raises when read — settles even when nothing else was
accepted, its events counted in `failed_events` and resent only by a sink that declares
`log_foundry_idempotent = True`, because `deliver` returned and the events may have landed. So does
a run of nothing but `DROPPED` events, counted in `dropped_events`, and a run of nothing but
`SKIPPED` events, a deliberate non-send the engine counts nowhere. An exception escaping `deliver`
is not this exception: no response is in hand, so its round's events fail the run.
*Guarded:* `tests/test_sink_losses.py` (a roster over the remote transports), the per-sink tests,
`tests/test_sinks_encode.py` (an all-unencodable batch settles on every record-building sink,
SPEC-069), `tests/test_classify.py` and `tests/test_sinks_sns.py` (a batch lost only to sender
faults returns, one with a recoverable loss beside it raises, SPEC-070), `tests/test_driver_error.py` (a SQLite or file driver error raises with `failed` moved,
SPEC-069), `tests/test_sinks_http_body_read.py` (real sockets, every HTTP subclass),
`tests/integration/` against real services (SPEC-041), `tests/test_verdict.py` (a malformed
`Verdict` refused where it is built), `tests/test_delivery_engine.py` (the run-failed rule, the
unknown outcomes and the unreadable verdict) and `tests/test_delivery_paths.py` (a failed run on
the worker, the orphan path and the adapter, a settled one on each, SPEC-072).

## 8. An event is safe by construction, once, at assembly

`build_event` coerces and bounds every value, so assembly's promise holds, and a sink encodes under
a guard (SPEC-069: an event a sink still cannot encode costs only itself); the ceilings bound
each *value*, and each coercion pass is bounded as a whole at 131,072 values visited and 16 MiB
of text charged, which bounds the work and not the event's size at a destination; an
unserializable value becomes a type name, never a `repr`; a number too large to render is
replaced, never clipped; a reserved word has exactly one route through (SPEC-017, SPEC-020,
SPEC-025, SPEC-034, SPEC-057). A route that returns the caller's object, or lets its method decide
a measurement, admits it only by exact type or through an unbound builtin accessor, and builtin
text, bytes and containers are routed by their real type, ahead of every branch that renders through
`str()`: an `Enum`, `UUID` or `Decimal` claim through `__class__` cannot send one there, though an
`int` or `float` claim makes it the type-name placeholder, since those branches refuse what they
cannot convert; a container
mapping key is coerced as a value and written as the JSON of the result, not as its `repr`
(SPEC-063). The premise holds at assembly **and at a
transform's coercion**: a `TransformSink` runs its function after assembly, so it checks each
result with the observable's JSON check — `json.dumps(result, allow_nan=False)` and a strict UTF-8
encode — and replaces one that fails with a coercion by assembly's per-type rules (SPEC-058).

*Observable:* for any value, the call returns, every string in the event encodes as UTF-8 and
`json.dumps(event, allow_nan=False)` succeeds, every leaf of the event is of type exactly `str`,
`int`, `float`, `bool` or `NoneType` and every container exactly `dict` or `list`, every configured
ceiling holds exactly, `truncated` marks every cut and every non-finite
substitution (a type-name placeholder is visible on its own and does not set it), and no
top-level key was overwritten. A lone surrogate is replaced by U+FFFD and marked; a key that
cannot be rendered becomes a marked key placeholder and costs no sibling (SPEC-055). A pass
visits at most 131,072 values (`sanitize._MAX_NODES`) and charges at most 16 MiB of text
(`sanitize._MAX_RENDERED_BYTES`); the value that crosses either budget is a `<node limit>` or
`<size limit>` that sets `truncated`, and nothing after it is visited (SPEC-057). A key that is a
`tuple`, `list`, `dict`, `set` or `frozenset` by its real type (an `Enum` member excepted) spends
one node and its members' nodes and bytes as a value would, and is written as the JSON text of its
coerced form, so no member's `repr` is written inside it; a `bytes` or `bytearray` key is priced
before it is rendered (SPEC-063).
*Recorded exceptions:* a destination can refuse what assembly allows, and two sinks repair it
after assembly, on a marked copy, never mutating the batch (SPEC-069 FR-003): `PostgresSink`
replaces every U+0000 in a string value or key, the config stamps included, with U+FFFD, because
JSONB and its text columns refuse it; `MongoDBSink` replaces an integer outside the signed 64-bit
range with assembly's `<int: ~N digits>` placeholder (`sanitize.int_placeholder`) and a NUL in a
key with U+FFFD, because `bson.encode` refuses them. Each repaired event carries `truncated`, as
assembly marks the same substitutions; a repair is not a loss and is neither counted nor announced.
The three config stamps `service`, `version` and `env` are copied from
the config without a ceiling — `configure()` refuses one that is not a `str` or cannot encode
(invariant 13), but an over-long one is stamped unbounded (SPEC-055, Out of Scope). A boundary
event takes two passes, `build_event`'s and `backfill_baggage`'s, so `span.start` and `span.end`
can each hold up to twice each budget (SPEC-057). A key that is an arbitrary object, or any
`Enum` member, keeps its own `str()`, which for a default object is its `repr`: the no-`repr` rule
holds for a value and inside a container key, not for such a key, whose `__str__` is the caller's
code (SPEC-063). An `Enum` member whose real type is a builtin container or bytes is the exception
to that exception: its text, as a value and as a key, is computed as `Type.NAME`, never by
`str()`, which for a `ReprEnum` or an mro-forged member is the builtin's `repr` (SPEC-063). An
object that is not a builtin and claims `UUID`, `Decimal`, `datetime` or `Enum` through `__class__`
renders through that claim, by its own `str()`, `isoformat()` or `.value`: that is the transparent
proxy route, kept on purpose, so the no-`repr` rule does not cover it, and such an object's
`__str__` can write what its `repr` would (SPEC-063). A transform's result that passes the JSON
check is delivered as the transform returned it, so what a transform adds may hold a builtin
subclass: the exact-type clause of the observable holds at assembly and for a transform's
coercion, not for a passing transform result (SPEC-058, SPEC-063). The
ceilings bound the values of an assembled event and do not bound what a transform adds: a
transform's coercion is for type only, under no configured ceiling, outside the two pass budgets,
and with one fixed depth bound of 100, marked as assembly marks a depth cut, because what a transform adds is the caller's code
choosing the size (SPEC-058).
*Guarded:* `tests/test_sanitize.py`, `tests/test_model.py`, the reserved-word route-through
by `tests/test_public_surface.py`, `tests/test_sanitize_corpus.py`, which holds every
hostile value to the whole observable on both delivery paths, and `tests/test_assembly_budget.py`,
which holds both budgets exactly and the reads to the ceiling;
`tests/test_assembly_keys_and_scalars.py`, which holds the container-key rule, the exact-type
routes and the real-type routing (SPEC-063); a transform's output by
`tests/test_transform_coercion.py` (SPEC-058); the sink-side repairs by `tests/test_encode.py`,
`tests/test_sinks_encode.py`, `tests/integration/test_postgres.py` and `test_mongo.py`
(SPEC-069).

## 9. Context is scoped, and an adopted context confers nothing

Baggage and an adopted trace context are released when the root span closes — baggage restored,
the adopted context cleared, because an inbound context is a one-shot handoff (SPEC-024). A
malformed `traceparent` never reaches the event stream; adopting one grants no authority
(SPEC-014). Threads and tasks see their own span and never a sibling's (arch §5).

*Observable:* the next root span after a released one starts a fresh trace with the prior
baggage; a task's events carry its own `span_id`.
*Guarded:* `tests/test_context.py`, `tests/test_trace_continuation.py`,
`tests/test_decorator_async.py`.

## 10. The public surface only grows, and hands out nothing the library reads

Every public dataclass is keyword-only and grows by appending; results grow by new reason values;
`Mapping` in, copies out; `Sink`'s members are abstract so a typo cannot instantiate; the worker's
tunables are unreachable from `configure()` (SPEC-034, SPEC-051).

*Observable:* a typed consumer written against the frozen surface (SPEC-034) type-checks and
runs against every later release of the same major; mutating anything a getter returned changes
no later event.
*Guarded:* `tests/test_public_surface.py`, `tests/typed_consumer/` (SPEC-051).

## 11. A diagnostic names a type, never a value, says what happened, and one module writes them all

Every stderr line the library writes about itself goes through `_diag`, is throttled where it can
repeat, and names an exception by `type(exc).__name__` — never `repr`, which reprints a psycopg
statement with its bound parameters (SPEC-029). What the line says happened — how many, after how
many attempts, on which path — is true, or the line is a defect.

*Observable:* `grep` of the library's stderr output for any caller value, header, token, URL or
statement finds nothing; every count a line carries reproduces from the ledger.
*Guarded:* `tests/test_diag.py` (a roster over every site) for the first half; the second is
asserted line by line in the tests of the site that writes it, and the throttle by
`tests/test_worker.py` and `tests/test_console_echo.py` (SPEC-055).
`tests/test_inbound_totality.py` holds `continue_trace`'s `"rejected"` to a written line on every
hostile probe (SPEC-067).

## 12. A forked child is repaired; the parent is untouched; the child releases only what it acquired

Locks and events first, inherited buffers second, registered handlers last; `before` never runs
for a C-level fork; a value the child inherits is stranded, not detached (SPEC-039, SPEC-042).

*Observable:* a child forked mid-drain delivers its own events, never hangs on a parent's lock,
and closes no transport the parent opened.
*Recorded exceptions:* a forked child's `NATSSink` refuses every `emit` and `flush` and its
`close()` returns without touching anything, because the child acquired no thread, loop or
connection and `reacquire_after_fork` is not implemented (SPEC-061 FR-001; `architecture.md` §12
names what would close it). The pid is compared before the sink's lock, since a lock a caller
held at fork time stays held in the child. The walk enters only this package's objects, so a sink
of your own is repaired only if it subclasses `Sink` or a shipped sink — with one reach past that
boundary (SPEC-072 FR-003): a value the walk would not enter, such as a structurally typed sink the
worker holds, is read for the one attribute `log_foundry_counters`, statically, and an exact
`SinkCounters` there has its lock replaced, so a child does not hang on a reached engine sink's
counters. The reach is one level deep: an engine sink held inside a wrapper of your own, or a sink
that is itself a container subclass of your own, is not reached, and nothing else of a foreign
namespace is read. **An unreached shape whose counters' lock a parent thread held at the fork
hangs the child's first run that commits into it:** an `info()` on the orphan path blocks the
caller, and on the worker path the drain thread blocks, so `flush()` times out. A registry of live
`SinkCounters` the walk reads, rather than finds, is SPEC-079's follow-up.
*Guarded:* `tests/test_fork_lifecycle.py` (including an engine sink's counters, SPEC-072),
`tests/test_sink_ownership.py`,
`tests/test_sinks_nats_liveness.py` (the `NATSSink` child, under a real `os.fork`).

## 13. A bad argument is refused where it is written, never later on the drain thread

A constructor, `configure()` or `@trace` that is handed an argument no backend can use raises at
that call — `TypeError` or `ValueError`, with the argument named — rather than accepting it and
failing on every `emit` for the life of the process, or silently delivering nothing (SPEC-043 at
run time; SPEC-051 for the static half, where a mistyped forwarded keyword is a `mypy` error;
SPEC-049 extended the run-time rule to timeouts, chunk sizes, the rotation interval, headers, URLs,
the Sentry DSN and an argument the selected backend cannot consume, and floored what still works;
SPEC-066 refused a non-`int` at every `int` floor and range check in a sink constructor and the
worker, and a `max_retry_after` that is not an `int` or `float`, judged before any client is
built, and gave `flush()` and `shutdown()` a verdict for their `timeout` — `inf` waits unbounded,
a NaN or non-number is refused at the call).

*Observable:* no argument a constructor accepted later raises from `emit` or makes `emit` return
having delivered nothing; no argument is silently ignored because another was given.
*Recorded exceptions:* a required argument that a constructor documents as ignored beside an
injected connection is accepted, because refusing it would break every such caller —
`SQLiteSink.database`, the one such argument, pinned by name in
`tests/test_injected_client_roster.py` (SPEC-071 FR-003).
*Guarded:* `tests/test_sentry_backend.py`, `tests/test_sinks_nats.py`, the per-sink construction
tests; `tests/test_config.py` for the stamps and, since SPEC-065, the four payload ceilings (a
non-`int` refused, an `int` subclass stored exact); `tests/test_decorator_sync.py` for what
`@trace` refuses (SPEC-055), and for a value lying about its type being refused where it is
written or traced, never absorbed on every call (SPEC-080); the wider population is SPEC-049's —
`tests/test_socket_transport.py`,
the per-sink refusal tests it added, and the floor-side pins in `tests/test_sink_retry.py`, which
also holds SPEC-065's spoofed-subclass pins for the three validators, and
`tests/test_sinks_pubsub.py` — whose spec Overview was the
measured list of what still constructs and should not; `tests/test_numeric_doors.py` for the
`int` doors and the `flush()`/`shutdown()` `timeout`, and `tests/test_numeric_door_roster.py`,
which derives the `int` doors from the AST and fails naming one left unjudged (SPEC-066);
`tests/test_type_doors.py` for a value lying about its type at every argument door, the HTTP
sinks' `auth` and the seven callable arguments, and `tests/test_type_door_roster.py`, which derives
the doors from the AST and fails naming one that asks the value rather than its type;
`tests/test_sinks_args.py` for the build options refused beside an injected client, and
`tests/test_injected_client_roster.py`, which derives each constructor's build options and fails
naming one ignored beside it; `tests/test_extras_sites.py` for a missing extra refused at the
constructor that needs it, and `tests/test_extras_roster.py`, which fails naming an extra imported
other than through `import_extra` (SPEC-071).
