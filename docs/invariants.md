# Invariants — what the library promises, as things you can observe

One page, numbered, loaded before a spec is written and before a diff is reviewed. Each entry is a
promise stated as an **observable** — something a probe can measure and a test can assert — with
where the exceptions to it are recorded and where it is guarded today. It is not the decisions
register (`decisions.md` holds the *fences*: what was built, what was rejected and why) and it is
not the constraints list (`architecture.md` §13 holds what the design accepts it cannot do). A
fence explains a mechanism; an invariant is what every mechanism, present and future, must keep.

**How it is used.** A spec's acceptance criteria name the invariant(s) each FR serves, by number.
The second diff reviewer — the one that starts from the system rather than the change
(`process.md` §3) — starts from this page: for each invariant the diff touches, it asks whether
the change keeps it on **every twin path** (invariant 6), not only the one the spec names. An
audit's "not covered" list is a list of invariants nobody has measured lately. A *behavioural*
finding that breaks no invariant here is a candidate for a new one; prose, lint and hygiene
findings are judged by their own rules (`process.md` §5).

**Where an open defect against an invariant lives:** `architecture.md` §12, never here. This page
states what is true when the library is correct; it does not carry a changelog.

---

## 1. The library never fails the caller

Every public call returns normally on a fault of the library's own. A `@trace`d function's own
exception is re-raised unchanged, with the span recorded as an error. The guards catch
`Exception`, never `BaseException`, so a `KeyboardInterrupt`, `SystemExit` or `CancelledError`
raised inside a library fault still reaches the caller, because those are the operator's or the
runtime's intent (SPEC-025).

*Observable:* no exception whose innermost frame is in `log_foundry` reaches application code, on
any input, on any thread, in any lifecycle state.
*Guarded:* `tests/test_api.py`, `tests/test_decorator_sync.py`, `tests/test_sanitize.py`;
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
bound (`health()` docstring); three losses are counted but never retried, because a retry would
duplicate — an unadjudicable batch response, an SQS sender fault, an oversized event (SPEC-018,
SPEC-016, SPEC-017).
*Guarded:* `tests/test_worker.py`, `tests/test_sink_losses.py`; `tests/test_invariants_model.py`
is the identity itself.

## 3. A wait is bounded by the caller's timeout, and is idle while it waits

`flush(timeout)`, `shutdown(timeout)` and `configure(sink=…)` return within their budget, and a
bound is measured in CPU as well as wall clock, so a busy-spin cannot pass as a wait (SPEC-038).
Every backoff waits on the worker's stop event, never `time.sleep`, so a shutdown cuts it short
(SPEC-027). A shutdown shortens a wait; it never skips work (SPEC-038).

*Observable:* elapsed wall time ≤ the timeout plus the sink's one in-flight call; CPU time ≈ 0.
*Recorded exceptions:* `Sink.close()` takes no timeout and is unbounded on both delivery paths;
what bounds the exit is `DEFAULT_CLOSER_GRACE`, carved from the shutdown budget
(`architecture.md` §13). A sink blocked *inside* a network call holds the thread for that call.
*Guarded:* the wall-clock half by `tests/test_worker.py` (against a wedged sink) and
`tests/test_sink_retry.py`; the CPU half only by `tests/test_sinks_pubsub.py` — `flush()` and
`shutdown()` have no CPU-time assertion, listed so the gap is visible.

## 4. Inside a span, the caller's thread never carries sink I/O; outside one, it does

A `@trace`d function hands its buffer to the worker and returns; the sink is reached on the drain
thread. A level call with no active span emits synchronously on the caller's thread, against the
same sink object the worker drains into — which is why a sink must tolerate concurrent callers
(SPEC-028) and why the orphan path can block on a sink's lock or backoff (`architecture.md` §13).

*Observable:* inside a span, `info()` latency is independent of the sink; outside one, it is the
sink's.
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
later `shutdown()` finds the drain thread ended (SPEC-050); a sink written to after its close is
owed a second close, which is correct rather than a double (SPEC-045).
*Guarded:* `tests/test_owed_closes.py`, `tests/test_shutdown_lifecycle.py`,
`tests/test_sink_ownership.py`; `tests/test_invariants_model.py` asserts the first observable.

## 6. Twin paths keep the same promises

The library has pairs of code paths that must behave identically under invariants 1–5 and 7–8:
the **worker** path and the **orphan** path; the **sync** and **async** decorator bodies; a sink's
`emit` and its `flush`; the `_drain` loop and `_final_drain`. A fix applied to one twin is a
recurring shape in this repo's history — SPEC-033's two same-day regressions, the SPEC-035 guard
sites, SPEC-050's daemon-thread close on both paths. This invariant is a review obligation rather
than a mechanism: a spec names the twins its FRs touch, and the system-frame reviewer checks each.

*Observable:* a probe run against one twin and its sibling produces the same ledger.
*Guarded:* `tests/test_worker_predicate_roster.py` derives the guard sites from the AST rather
than a hand list; `tests/test_invariants_model.py` drives both delivery paths in one run.

## 7. A sink that delivered nothing raises; one that delivered something reports

Total failure raises `SinkDeliveryError`, so the worker's retry engages and `failed_batches`
moves. Partial failure returns and reports through `losses()`; a client exception costs its own
chunk and nothing else (SPEC-026, SPEC-043, SPEC-048). A redirect is a delivery failure, not a
route to follow (SPEC-048). A sink that released its transport refuses after `close()`; one that
released nothing keeps accepting (SPEC-032).

*Observable:* with a destination that fails every call, `flush()` is falsy and a counter moved;
with one that fails some calls, nothing raised, `losses()` is non-zero, and no event landed twice.
*Guarded:* `tests/test_sink_losses.py` (a roster over the remote transports), the per-sink tests,
`tests/integration/` against real services (SPEC-041).

## 8. An event is safe by construction, once, at assembly

`build_event` coerces and bounds every value, so a sink may `json.dumps` bare; the ceilings bound
each *value*, not the event; an unserializable value becomes a type name, never a `repr`; a
number too large to render is replaced, never clipped; a reserved word has exactly one route
through (SPEC-017, SPEC-020, SPEC-025, SPEC-034).

*Observable:* for any value, the call returns, `json.dumps(event, allow_nan=False)` succeeds,
every configured ceiling holds exactly, `truncated` marks every cut and every non-finite
substitution (a type-name placeholder is visible on its own and does not set it), and no
top-level key was overwritten.
*Guarded:* `tests/test_sanitize.py`, `tests/test_model.py`, and the reserved-word route-through
by `tests/test_public_surface.py`.

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

## 11. A diagnostic names a type, never a value, and one module writes them all

Every stderr line the library writes about itself goes through `_diag`, is throttled where it can
repeat, and names an exception by `type(exc).__name__` — never `repr`, which reprints a psycopg
statement with its bound parameters (SPEC-029).

*Observable:* `grep` of the library's stderr output for any caller value, header, token, URL or
statement finds nothing.
*Guarded:* `tests/test_diag.py` (a roster over every site).

## 12. A forked child is repaired; the parent is untouched; the child releases only what it acquired

Locks and events first, inherited buffers second, registered handlers last; `before` never runs
for a C-level fork; a value the child inherits is stranded, not detached (SPEC-039, SPEC-042).

*Observable:* a child forked mid-drain delivers its own events, never hangs on a parent's lock,
and closes no transport the parent opened.
*Guarded:* `tests/test_fork_lifecycle.py`, `tests/test_sink_ownership.py`.
