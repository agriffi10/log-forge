---
type: Spec
id: SPEC-073
title: "The HTTP Family on the Delivery Engine"
description: "Puts HTTPSink and its eight shipped subclasses on SPEC-072's delivery engine: each offers deliver, one request per chunk per round, its emit runs the same engine inline, the three per-record readers return per-event outcomes through one shared unreadable-response method, every documented counter keeps its 1.x name and unit through a family-neutral run context, and a third-party subclass that overrides emit or _handle_response keeps its 1.x path."
spec_status: Draft
depends_on: [SPEC-018, SPEC-027, SPEC-038, SPEC-056, SPEC-058, SPEC-059, SPEC-062, SPEC-066, SPEC-070, SPEC-072]
arc: stability-redesign
arc_order: 5
---

## Overview

SPEC-072 adds the contract and the engine and leaves every shipped sink where it was. This spec puts
the first family on it: `HTTPSink` and the classes that inherit it. Today the family answers "did it
land, and may it be sent again?" in three layers:

- `HTTPSink._send` retries one request up to `max_retries` with its own backoff.
- `HTTPSink.emit` re-chunks, searches for a size the destination accepts after a `413`, and decides
  whether the batch as a whole failed.
- The worker then resends the **whole** batch whenever `emit` raised. So a chunk that already landed
  is sent again, and a permanently refused one is re-POSTed once per worker attempt.

Three subclasses read a per-record verdict out of a `2xx` and count it by hand, each with its own
copy of "give up on a response I cannot read".

After this spec a family sink offers `deliver(batch)`. One call is one **round**: every event is
rendered and chunked, each chunk goes in one request, and each event's fate is returned by index.
The engine resends only the events a round called retryable, within SPEC-072's budget. On the
worker path that budget is the 1.x total of `(sink + 1) × (worker + 1)` attempts, so a batch
survives the same outage it survives today. What each resend carries is only the retryable subset:
a chunk that landed or was refused for good is never sent again. `emit` runs the same engine
inline, so a caller of `emit` sees the 1.x contract.

What this spec keeps exactly is what the family's callers can see:

- the `413` halving of the budget (SPEC-038);
- the `Retry-After` clamp and `max_retry_after` (SPEC-027);
- the per-event drops and the rule that a failed batch counts none (SPEC-058, SPEC-059);
- OTLP's narrower retryable statuses (SPEC-056);
- every documented counter's name and unit. `HTTPSink.failed` still counts abandoned requests.

The counts that move are listed in FR-002, each with its reason. Every one of them is in the
release notes.

It builds on SPEC-066 (every `max_retries` an exact, floored `int`; `HTTPSink.max_batch_count`'s
validation), which has landed, in 1.1.0, and is in `depends_on`. It consumes SPEC-070's `HTTPSink._retryable_error`, its read-failed flag (`_UnreadBody`,
`_read_failed`) and `_send`'s `count_unexpected` keyword. It does not touch #291's base-URL refusals,
which landed.

Figures, each with its instrument and the commit it was measured at:

- **The family** (`fda134a`). An AST scan of `src/log_foundry/sinks/*.py` for classes whose bases
  reach `HTTPSink` finds `HTTPSink` and eight subclasses: `DatadogSink`, `ElasticsearchSink`,
  `OpenSearchSink`, `HoneycombSink`, `LokiSink`, `NewRelicSink`, `OTLPSink`, `SplunkHECSink`.
  - Two classes hold an `HTTPSink` rather than inherit one: `LogstashSink` (HTTP mode, through
    `emit`) and `SentrySink` (its fallback, through `HTTPSink._send`).
  - The same scan finds `emit` defined on `HTTPSink` alone.
- **Per-record readers** (`fda134a`). `_handle_response` is overridden by `ElasticsearchSink`,
  `HoneycombSink` and `OTLPSink`, and `OpenSearchSink` inherits it. `_abandon_unadjudicated` is
  defined three times, once on each of those three, with three different line texts.
- **One retry loop and one backoff constant** (`fda134a`). An AST walk of `http.py` finds one
  `for attempt in range(...)` loop, in `HTTPSink._send`, and one `_BACKOFF_BASE = 0.1` assignment.
- **Tests that drive a family sink through a real `Worker`** (`fda134a`). Thirteen test files name
  both `Worker(` and a family class, `SentrySink` or `LogstashSink`. The instrument is
  `grep -l 'Worker('` over `tests/*.py`, filtered by `grep -l` for the eleven class names. Which of
  them pin the removed whole-batch resend is FR-005's list.
- **Probe D** (`tests/test_sinks_http_refused.py`). It made 8 requests through
  `Worker(max_retries=3)` at `fda134a`: four whole-batch attempts of a `413` and an opener
  `RuntimeError`. SPEC-070 changed that shape: `HTTPSink._send` now catches an opener's
  `RuntimeError`, counts it in `failed` at its one attempt and lets nothing escape `emit`, so once
  the `413` drop has settled chunk 1, `emit` returns, the worker sees success and the whole run is
  2 requests with the drop counted once
  (`test_an_opener_exception_after_a_drop_is_counted_and_the_drop_settles`). SPEC-070 rebuilt
  Probe D's tests on an instance-level `_handle_response` that raises on chunk 2's `200`,
  outside `_send`'s guard, so the raise still escapes `emit` and the probe still makes 8 requests
  on SPEC-070's tree.

## Scope

### In Scope

- `HTTPSink.deliver`, the round, its status-to-outcome rules, and `emit` and `_send` running through
  the engine. `_BACKOFF_BASE`, `_sleep_backoff`, `_deliver` and the retry loop are deleted (FR-001).
- One private, **family-neutral** addition to SPEC-072's `sinks/_engine.py`, the run context, and the
  HTTP family's counters on it (FR-002). The run context is how every shipped sink keeps its 1.x
  counter names, units and lines, and SPEC-074 extends it for the SDK and driver sinks.
- `SentrySink`'s HTTP backend: the `HTTPSink._send` it calls runs on the engine (FR-001).
  - `SentrySink` gets **no** `deliver` in 1.2.0. Its SDK backend hands each event to
    `capture_event`, which reports no per-batch outcome a verdict could carry, and one sink cannot
    offer `deliver` for one backend and not the other.
  - It keeps its own `emit` and its 1.x `losses()`.
- `ElasticsearchSink`, `HoneycombSink` and `OTLPSink` returning per-event outcomes, and the one
  `HTTPSink` method that gives up on a response the sink cannot read (FR-003).
- How the base detects a third-party override of `emit` or `_handle_response` and keeps its 1.x path
  (FR-004).
- Every subclass on both paths and both forms, the rosters this moves, the tests that pin a removed
  mechanism, and the decision, invariant, README and inventory prose (FR-005).
- Five FRs. This is the second half of the arc's plan step 4, split from SPEC-072 where the inert
  engine goes live. It takes plan step 5's unreadable-response method because that is an
  `HTTPSink` method.

### Out of Scope

- **A `deliver` on `SentrySink`**, in this spec or any other of the arc (above). The `HTTPSink._send`
  it calls keeps its signature, return value, counter, line and raise (FR-001).
- **`LogstashSink`, `FilteringSink`, `TransformSink` passing a verdict through** (SPEC-075).
  `LogstashSink`'s HTTP mode reaches the engine here only through its inner `HTTPSink.emit`.
- **Retrying a per-record rejection.** An Elasticsearch item error, a Honeycomb per-event `4xx` and an
  OTLP rejected record stay `REJECTED`, whatever status the item carries: counted and never resent by
  the sink, as in 1.x.
- **A new retryable status or a changed one.** `_retryable_status` and OTLP's `429`, `502`, `503`,
  `504` are unchanged. A `413` is never `RETRYABLE`; it drives the budget search (FR-001).
- **An adaptive budget across batches.** The `413` search is kept per run, as it was per `emit`. Its
  reduced budget is never carried into a later batch, for the ratchet `HTTPSink._chunk_budget`'s
  docstring records.
- **Declaring the shipped family idempotent.** No shipped HTTP sink sets `log_foundry_idempotent`,
  so an `UNKNOWN` outcome of theirs is never resent (#283's rule). A third-party subclass may declare
  it, and FR-003 counts that case once.
- **A compatibility promise for private methods other than the documented hooks.**
  - `_render`, `_body`, `_handle_response`, `_retryable_status`, `_response_limit`, `_item_size`,
    `_body_reserve`, `_prepare`, `_apply_auth`, `_attempt` and `_send` keep their signatures and
    meaning.
  - `_deliver` and `_sleep_backoff` are removed, and so are `_abandon`'s and `_abandon_unreduced`'s
    immediate stores. A third-party override of one of them is no longer called.
  - The chunk path no longer calls `_send`, so an override of `_send` still serves `SentrySink`
    and direct callers but no longer sees the chunks.
- **Changing `_handle_response`'s signature**, `SinkLosses`, `configure()`, `Config`, or any
  constructor. No public name is added beyond the `deliver` member SPEC-072 defines.

### Twin paths (invariant 6)

- **Worker and orphan.** A family sink is driven through both, for every outcome kind (FR-005).
- **`deliver` and `emit`.** One round function serves both. Their ledgers are equal for the same
  responses wherever the two budgets coincide (FR-001, FR-005).
- **The round and `_send`.** One classification of a response serves both, so `SentrySink`'s
  fallback and the chunk path cannot drift (FR-001).
- **The nine classes.** Each is swept over real sockets by the existing rosters (FR-005).
- **An eligible and an ineligible subclass.** The same responses through a shipped subclass and
  through a third-party override each leave the ledger their path owes (FR-004).

---

## Functional Requirements

### FR-001: An HTTP sink delivers in rounds of one request per chunk, and the engine owns the retries

#### Description:

`HTTPSink.deliver(batch)` runs one **round**:

- `_items` renders every event and drops what cannot be sent alone.
- The remaining items are chunked by `_take` as today.
- Each chunk is sent in **one** request, built by `_body`, `_prepare` and `_apply_auth` and made by
  `_attempt`, exactly as 1.x builds and makes it.

Nothing in the round waits or retries. The engine (SPEC-072) resends the `RETRYABLE` events in later
rounds, with the sink's own `max_retry_after`, its stop signal and, on the worker path, SPEC-062's
prediction. Its budget is SPEC-072's, and it keeps the 1.x total:

| Path | Further rounds | Where it comes from |
|---|---|---|
| worker path (`run_delivery(..., worker_retries=w)`) | `(s + 1) × (w + 1) − 1`, 15 at the defaults of 3 and 3 | 1.x attempts per `emit`, times the worker's attempts |
| orphan path (`worker_retries=None`) | `s` | 1.x had no resend around an orphan `emit` |
| `emit` itself (`emit_via_deliver`) | `s` | the catcher's own retry multiplies above it, as in 1.x |

Here `s` is the sink's `max_retries` and `w` the worker's. `HTTPSink.emit(batch)` is
`emit_via_deliver(self, batch, deliver=self._round)`. `deliver=` is the keyword SPEC-072's adapter
takes for a round function its static lookup would not find (FR-004). So `emit` raises
`SinkDeliveryError` exactly when the run fails, and returns otherwise.

**What a request's answer means.** One classification serves the round and `_send`:

| The request | Outcome of its events | Counted (FR-002) |
|---|---|---|
| a `2xx` | what `_outcomes(payload, items)` returns: all `ACCEPTED` for the base class | the reader's own (FR-003) |
| an exception before a status line that `_retryable_error` accepts (SPEC-070) | `RETRYABLE` | `failed`, if this is the run's last round |
| a status `_retryable_status` accepts | `RETRYABLE`, with its `Retry-After` | `failed`, if this is the run's last round |
| a `413`, in the round | the budget search below | |
| a non-`2xx` longer than `_response_limit()` | `REJECTED` | `failed` |
| any other status, or an exception `_retryable_error` refuses | `REJECTED` | `failed` |

**The `413` search runs per run, not per round.** It works as SPEC-038 does:

- A `413` halves the budget to the refused body's size and re-chunks the round's remaining items.
- A lone refused item is `DROPPED`, and so is one `_known_refused` proves no smaller than one
  already refused.
- A multi-item chunk still refused after `MAX_BUDGET_REDUCTIONS` is `REJECTED`.

The budget, the refused size and the reduction count live in the run's notes (FR-002). A later
round's resend therefore starts from the reduced budget instead of rediscovering it, and a run
searches no more than one 1.x `emit` did.

**The round's hint** is the largest finite, positive `Retry-After` among its `RETRYABLE` responses,
or `None` when none has one. The engine clamps it against `max_retry_after` by
`clamp_server_delay`'s ceiling rule. That is the clamp `_sleep_backoff` applies today, and SPEC-072
adopts it: an unusable ceiling (`0`, negative, `NaN`) means backoff, and an infinite one honours the
hint up to `MAX_WAIT`. One round has one wait, so a batch whose several chunks failed retryably
waits once per round, where 1.x waited once per chunk.

**A hook fault costs its chunk.** The round calls each hook a subclass can override (`_body`,
`_outcomes` and `_handle_response`) under a guard catching `Exception`, never `BaseException`:

- If `_body` raises, the chunk's events are `DROPPED` and counted on `_unrenderable`, as a render
  fault is (SPEC-058). Nothing was sent, and the same bytes would fail the same way.
- If the `2xx` reader raises, the request succeeded and its events may have landed. The chunk goes
  through the unreadable-response method (FR-003) and its events are `UNKNOWN`.

A `deliver` of this family therefore never raises an `Exception`. Under SPEC-072 an escaped
exception fails the run, which would report total failure for a batch whose other chunks landed.

**`_send` keeps its 1.x contract** for its callers outside the chunk path: `SentrySink`'s fallback,
and a third-party subclass that builds its own body.

- It takes the same arguments, including SPEC-070's `count_unexpected: bool = True`.
- It returns the `2xx` payload.
- When it gives up it counts one `failed`, writes one `lost 1 request(s)` line naming the attempts
  actually made, and raises `SinkDeliveryError`. A `413` raises `_PayloadTooLarge` instead, uncounted,
  when `splittable`.
- With `count_unexpected=False`, an exception `_retryable_error` refuses is attempted once,
  counted by neither `_send` nor its run's line, and **re-raised unchanged**, the same object with
  its own traceback. `SentrySink` then counts it on `.transport_errors` as today. The engine never
  sees it: `_send`'s one-request round catches it and returns `REJECTED`, and `_send` raises the
  stored exception after the run ends.

`_send` runs its attempts as rounds of one request, through SPEC-072's `_run`. Its budget is
`max_retries`, its stop event is `log_foundry_stop_signal`, and it has no decline predicate. So
`http.py` holds no retry loop and no wait of its own.

**A run inside a run is independent.** SPEC-072 lets a third-party `deliver` call another sink's
`emit`, and every engine sink's `emit` is a run. Nesting is therefore reachable through any engine
sink, not only through `_send`, and no rule here can forbid it. What holds instead:

- Each run has its own context, its own budget and its own commit.
- The run context is a stack, so the inner run's `round`, notes and ledger are its own, and the
  outer run reads none of them.
- So `_send` called from a third-party wrapper's round, or from a hook inside its own sink's round,
  makes `max_retries + 1` attempts and counts as 1.x did.
- Its waits run inside the enclosing round. The worker's SPEC-062 prediction measures that round
  as one longer round.

**Deleted:** `_BACKOFF_BASE`, `_sleep_backoff`, `_deliver`, `_send`'s `for attempt` loop, and
`_abandon`'s and `_abandon_unreduced`'s direct stores to `failed` (FR-002 replaces them).

#### Acceptance Criteria:

- [ ] `http.py` holds no `for` loop over `range(...)` naming `retries`, no `_BACKOFF_BASE`, and no
      call to `wait`, `time.sleep` or `Event.wait`, and neither does any module of the family (an
      AST check over every class the family scan finds).
- [ ] Worker budget: an `HTTPSink` at its defaults under a default `Worker`, answered nine `503`s
      then a `200`, is delivered on the 10th request with `failed_batches == 0`, as at `fda134a`.
- [ ] Worker budget: `HTTPSink(max_retries=0)` under `Worker(max_retries=3)`, answered `503` then
      `200`, is delivered on the 2nd request. Answered `503` throughout, it makes 4 requests and
      `failed_batches == 1`.
- [ ] Orphan budget: an orphan `info()` against `HTTPSink(max_retries=2)` answered `503` throughout
      makes 3 requests on the caller's thread, and `orphan_lost == 1`.
- [ ] A two-chunk batch answered `200` and `400` makes 2 requests and `failed` rises by 1. Through a
      real `Worker` with `max_retries=3` it is still 2 requests (where `fda134a` made 2), `flush()` is
      truthy and `failed_batches == 0`.
- [ ] A one-chunk batch answered `400` throughout, through a real `Worker` with `max_retries=3`, makes
      1 request where `fda134a` made 4, and `failed_batches == 1`.
- [ ] Through `emit` with `max_retries=2`, a one-chunk batch answered `503` then `200` makes 2
      requests, waits `0.1` s once and leaves `failed` at 0. Answered `503` throughout it makes 3
      requests, and `failed` is 1, with one line reading `3 attempt(s), HTTP 503`.
- [ ] Two chunks answered `503` and `200`, then `200`, make 3 requests in all, and the second round's
      request carries exactly the first chunk's events.
- [ ] An opener raising `URLError` is `RETRYABLE`. One raising `RuntimeError` makes 1 request,
      counts `failed == 1` and fails a one-chunk run, also through a real `Worker`.
- [ ] Retry-After:
  - `Retry-After: 2.5` with `max_retry_after=1.0` waits `1.0`.
  - Two retryable chunks with hints `1` and `4` wait `4`, once.
  - With `max_retry_after` of `0`, `-1` or `nan` the hint is ignored and the wait is the backoff,
    as at `fda134a`.
  - With `inf`, a hint of `100` waits `100`.
- [ ] A stop signal set during a hinted wait returns `emit` within 0.5 s of wall time and under
      0.05 s of CPU, measured through `tests/test_worker_exit_budget.py`'s helper.
- [ ] A destination refusing any body over 300 bytes is sent 40 small events under a 5,000,000-byte
      budget. It gets the same requests in the same order as at `fda134a`. When one of those
      requests is answered `503` and resent in round two, round two sends nothing over 300 bytes.
- [ ] The existing `413` tests in `tests/test_sinks_http.py` and `tests/test_sinks_http_refused.py`
      pass unedited, apart from the Probe D tests FR-005 names.
- [ ] A subclass whose `_body` raises `ValueError` for one chunk of three has that chunk's events
      counted on `losses().dropped` once, with a line naming `ValueError`. The other two chunks are
      sent, `deliver` raises nothing, and `emit` does not raise.
- [ ] `_send` keeps 1.x behaviour:
  - A `400` raises `SinkDeliveryError` after 1 request with `failed == 1`.
  - A `503` throughout makes `max_retries + 1` requests.
  - A `413` with the default `splittable` is the counted abandonment, and with `splittable=True` it
    raises `_PayloadTooLarge` uncounted.
  - With `count_unexpected=False`, an opener's `RuntimeError` is raised from `_send` by identity
    after 1 request, `failed` unmoved and no line written.
  - `tests/test_sinks_http.py`'s two `_send` tests and every `SentrySink` HTTP-fallback test pass
    unedited.
- [ ] Nested runs: a third-party test sink with `deliver` wraps a `SentrySink` on the HTTP fallback,
      and its `deliver` calls `inner.emit(batch)` against `503` then `200`. The inner `_send` makes
      2 requests and waits once, and the Sentry event is sent. Inside the inner run
      `run_context().round` reads 1, then 2. After it returns, the wrapper's context still reads
      its own round 1 with its own notes.
- [ ] Nested runs, inside the sink's own round: a third-party `_outcomes` hook, on its chunk's `2xx`,
      calls `self._send` against `503` then `200`. That `_send` makes 2 requests and returns the
      payload, and the chunk's outcome is what the hook returns. The outer run still has 1 round.
- [ ] `read_deliver` is `None` for `SentrySink` on either backend.
- [ ] Serves invariants 2, 3, 4 and 7:
  - each event is offered until it has an outcome, and never after;
  - every wait is the engine's: bounded, idle and cut by the stop event;
  - an orphan call still retries on the caller's thread;
  - a run that delivered nothing raises from `emit`, and a partial one reports.

### FR-002: The family's counters keep their 1.x names and units through a family-neutral run context

#### Description:

SPEC-072's engine counts events into `SinkCounters` at the end of a run, and this family's
documented counters count other things:

- `failed` counts **requests** abandoned;
- a per-event drop is counted only when the batch did not fail (SPEC-058, SPEC-059);
- a non-retryable abandonment announces the attempts it made.

No count may be reclassified in a minor release. So this spec adds one private piece to
`sinks/_engine.py`, the **run context**, and keeps every family counter's name and unit. The run
context is **family-neutral**: it names no HTTP type, and it is the mechanism by which every
shipped sink keeps its 1.x counts and lines. SPEC-074 extends it for the SDK and driver sinks, and
changes nothing defined here. Its extensions are one commit under one lock where the owner's lock is
its counters' lock, final rejections, and a private refusal marker.

`run_context()` returns the context of the innermost engine run on the calling context. Contexts
form a stack in a `ContextVar`, pushed and popped in a `finally`, so the drain thread, every orphan
caller and every nested run (FR-001) each see only their own. A context exposes:

- `round`: the round's 1-based number. A line that names attempts uses it, because the events of
  round *n* have each been offered *n* times.
- `notes(owner)`: a per-run, per-owner `dict` the owner keeps across rounds. FR-001 keeps the `413`
  search state there.
- `count(owner, name, n, when=...)` and `announce(line, when=...)`: an increment of `owner.name` and a
  stderr line, both committed when the run ends, as `when` says:
  - `ALWAYS`: whatever the run's outcome.
  - `SETTLED`: only when the run settles.
  - `LAST`: only when the round is the run's last.
- At the end of the run the engine commits each owner's counts through a `Deferral` on that owner,
  under the owner's `_counter_lock`, read at commit time. It then writes every line outside the lock,
  in registration order. A `BaseException` commits nothing, as SPEC-072 states.

The context holds no sink after its run ends, so it adds no global that holds a sink (invariant 12).
A round called outside any run (a direct `sink.deliver(batch)`) commits its own ledger when it
returns. It counts as a one-round run that is its own last and settles unless its verdict fails. So
`losses()` still moves for a caller that bypasses the engine. Each count names its owner, so a
delegate that forwards `deliver` (SPEC-075's `LogstashSink`) commits into the `HTTPSink` that made
the count, never into itself.

**Where each 1.x count lands:**

| Count | Unit | `when` |
|---|---|---|
| `dropped_oversized` (per-event ceiling, a `413` lone item) and `_unrenderable` (render or `_body` fault) | events | `SETTLED` |
| `failed` for a `REJECTED` request (status, response bound, refused exception, unreduced `413`) | requests | `ALWAYS` |
| `failed` for a request whose events were `RETRYABLE` | requests | `LAST` |
| `item_errors`, `rejected_events`, `rejected_records` | events | `ALWAYS` |
| `dropped_unadjudicated` | events | `ALWAYS`, or `LAST` for a sink declaring `log_foundry_idempotent` (FR-003) |

Each line keeps its 1.x text, and the attempt count it names is the round number. A run in which the
sink registered any announcement is announced by those lines alone, so a family loss keeps the
line it has in 1.x and the engine writes no settled line of its own: that is SPEC-072 FR-003's
refinement of the engine's settled line, which this spec relies on rather than adds. A failed run's catcher still
writes its line: the worker's abandoned or declined line, the orphan path's, or the adapter's raise.

The engine's event-unit totals still land in `log_foundry_counters` as SPEC-072 defines them.
`HTTPSink.losses()` and each subclass's `losses()` keep their 1.x formula over the 1.x attributes, so
`read_losses(sink)`, `health().sink` and `.failed` read as they did. `dropped_unadjudicated` joins the
base class, starting at 0, and `HTTPSink.losses().failed` adds it.

**Every count that moves, and why.** Each loss still lands in a documented term. Each move below is
deliberate, and each is a line in the release notes:

1. **A total failure is counted once per batch, not once per worker attempt.** The worker no longer
   resends a whole batch around an engine run (SPEC-072), so a request rejected for good is sent and
   counted once. That covers a non-retryable status, the response bound and a refused exception, and
   it covers a per-record rejection on `item_errors`, `rejected_events` and `rejected_records` too. A
   retryable outage counts `failed` once per abandoned request of the last round, so 16 attempts at
   the defaults count 1 where 1.x counted 4, and the line names `16 attempt(s)`. `failed` was always
   documented as an upper bound, and it is now a tighter one.
2. **A lone `413` refusal beside a batch that otherwise failed** moves from `dropped_oversized` to the
   catcher's failed term (`failed_batches`, `orphan_lost` or the adapter's raise). In 1.x the drop
   settled its chunk, so `emit` returned and counted it. Under SPEC-072's batch-failed rule a
   `DROPPED` event does not keep a run from failing, so the drop is not counted and the event is
   counted with its batch. This is how 1.x already counted a per-event-ceiling drop beside a total
   failure.
3. **A `_body` that raises** moves from the catcher's failed term to `dropped` (`_unrenderable`). In
   1.x the exception escaped `emit` and the worker resent the batch, chunks that had landed included.
   Now the chunk is dropped, the rest are sent, and a batch whose every `_body` raises settles with
   `flush()` truthy and `failed_batches` at 0. That is invariant 7's recorded exception for a batch
   of nothing but events the sink cannot encode, as SPEC-069 words it, which FR-005 extends to name
   it.
4. **A `2xx` reader that raises** moves from the catcher's failed term to `dropped_unadjudicated`
   (into `failed`). The events may have landed, so the run never fails on them and they are never
   resent.

**What lands at a different moment.** Commits happen when the run ends, so three things change:

- **When a count lands.** In 1.x `failed` was already counted when the next chunk's request went
  out. Now it lands when the run ends, and so do `item_errors`, `rejected_events`,
  `rejected_records` and `dropped_unadjudicated`.
- **A `BaseException` raised mid-run.** It now discards counts that 1.x had already made, with their
  lines.
- **Line order on stderr.** The family's lines for a run are written together after its commit, in
  registration order, where 1.x interleaved them with the requests.

The drops (`SETTLED`) land at the same moment as in 1.x, when the run settles, and that is the moment
`emit` returns.

#### Acceptance Criteria:

- [ ] Run context basics:
  - `run_context()` inside a round reports `round` 1, 2 and 3 for three rounds of one run, and
    `notes(owner)` returns the same `dict` in each.
  - Two runs on two threads against one sink never see each other's notes or ledger.
  - A run nested in a round sees a fresh context, and the outer context is restored when it ends.
- [ ] Commit rules:
  - `when=SETTLED` counts commit for a settled run and not for a failed one.
  - `ALWAYS` commits for both.
  - `LAST` commits only the final round's entries, including in a run ended by the worker's decline.
  - A `BaseException` raised from round two commits nothing, and writes none of the run's lines.
- [ ] `failed` counts:
  - Through `emit`, a one-chunk `503` throughout at `max_retries=2` leaves `failed == 1`, as at
    `fda134a`.
  - Through a real `Worker` with `max_retries=3`, the same batch makes 12 requests (as at `fda134a`)
    and leaves `failed == 1` where `fda134a` left 4.
  - Three chunks answered `503` throughout through `emit` leave `failed == 3`, with three lines each
    naming `3 attempt(s)`.
- [ ] A `503`, `503`, `400` sequence leaves `failed == 1`, with one line naming `3 attempt(s), HTTP 400`.
- [ ] Oversized events:
  - An oversized event beside a delivered chunk is counted on `dropped_oversized` once, with its 1.x
    line.
  - Beside a chunk answered `400`, the run fails and `dropped_oversized` stays 0.
- [ ] A lone `413` refusal:
  - Beside a chunk answered `400`, the run fails, `dropped_oversized` stays 0 and through a real
    `Worker` `failed_batches == 1`.
  - Beside a delivered chunk, it is counted on `dropped_oversized` once.
- [ ] A batch of three chunks whose every `_body` raises makes no request, counts
      `losses().dropped == n` with one line, settles, and leaves `flush()` truthy.
- [ ] A settled family run with a loss writes exactly its 1.x line or lines and no engine line.
      `tests/test_diag.py`'s roster covers every new call site, and no line carries a value.
- [ ] `log_foundry.health().sink` for each family class, after each FR-001 case, equals
      `sink.losses()` computed by its 1.x formula.
- [ ] A forked child that finds the counters' lock held by a parent thread completes a family round
      within 5 s, and its counts add to the inherited totals.
- [ ] Serves invariants 2, 11 and 12:
  - every event is in one counted term exactly once, and a failed run counts no drop;
  - every line's attempt count is true;
  - the run context gives a forked child nothing to hang on.

### FR-003: The three readers return per-event outcomes through one unreadable-response method

#### Description:

`HTTPSink._outcomes(payload, items) -> tuple[Outcome, ...]` is the hook the round reads a `2xx`
through. The base implementation asks `self._handle_response(payload, items)`: `True` makes every
event `ACCEPTED` and `False` makes every event `REJECTED`. That keeps the meaning a 1.x override's
bool had in a total failure. `ElasticsearchSink`, `HoneycombSink` and `OTLPSink` override
`_outcomes` and stop counting by hand:

- **`ElasticsearchSink`** (and so `OpenSearchSink`): an aligned `items` array marks each item with an
  error `REJECTED` and every other item `ACCEPTED`, counting the errors on `item_errors`. A body with
  no truthy `errors` is all `ACCEPTED`.
- **`HoneycombSink`**: an aligned status array marks a `2xx` entry `ACCEPTED` and any other integer
  `REJECTED`, counted on `rejected_events`.
- **`OTLPSink`**: a `partialSuccess` rejecting *r* of *n* records (clamped as today) is counted on
  `rejected_records`. The response gives a count, not an identity, so the chunk's first *r* events
  are `REJECTED` and the rest `ACCEPTED`. The positions carry no claim about which record the server
  refused, and the sink's docstring says so. A body OTLP reads as full success stays all `ACCEPTED`.

**One method gives up on a response.** `HTTPSink._unadjudicated(items, detail, noun="event")`
replaces the three `_abandon_unadjudicated` copies. It:

- counts the chunk on `dropped_unadjudicated`;
- writes `lost <n> <noun>(s); <class> <detail>; they may have landed; not retried`;
- returns every event `UNKNOWN`.

Each reader passes its own `detail`, so each line is byte-identical to its 1.x text. Every reader
calls it on every route the 1.x copy covered (undecodable, non-object, misaligned,
self-contradicting, over the response bound). It calls it first of all for a body SPEC-070's
`_read_failed` flags. The round also calls it for a reader that raised (FR-001).

An `UNKNOWN` event never fails a run (SPEC-072), as a 1.x unadjudicated chunk settled. A shipped
family sink is not idempotent, so its `UNKNOWN` is final at once, and `_unadjudicated` counts and
announces it `ALWAYS`. A third-party subclass that declares `log_foundry_idempotent = True` has its
`UNKNOWN` chunk resent. For it `_unadjudicated` counts and announces `LAST`, so an event is counted
once per run, and only when it ended the run unadjudicated. A later round that accepts it counts
nothing.

Each reader keeps `_handle_response(payload, items) -> bool` for a direct 1.x caller. It runs
`_outcomes` in a context outside any run, commits that context at once as FR-002 describes, and
returns `False` exactly when every outcome is `REJECTED`. So `tests/test_public_surface.py`'s calls
and OTLP's hostile-bytes test read what they read at `fda134a`.

#### Acceptance Criteria:

- [ ] `_abandon_unadjudicated` is defined nowhere in `src/`. `HTTPSink._unadjudicated` is the only
      definition that writes `dropped_unadjudicated`, and an AST check holds that for every family
      class.
- [ ] For each reader, every unadjudicated case in its existing tests behaves as at `fda134a`: the
      same line text, the same `dropped_unadjudicated`, one request, nothing resent, and `flush()`
      truthy.
- [ ] `ElasticsearchSink` with items `[ok, error, ok]` returns `[ACCEPTED, REJECTED, ACCEPTED]` and
      `item_errors == 1`. `[error, error]` fails a one-chunk run, and `emit` raises.
- [ ] `HoneycombSink` statuses `[202, 400, 202]` return `[ACCEPTED, REJECTED, ACCEPTED]` and
      `rejected_events == 1`. Statuses `[400, 400, 400]` fail a one-chunk run.
- [ ] `OTLPSink`:
  - `rejectedLogRecords` of 2 of 5 returns two `REJECTED` and three `ACCEPTED`, with
    `rejected_records == 2`.
  - 5 of 5 fails the run.
  - `"9" * 30` is `UNKNOWN`, through `_unadjudicated`.
- [ ] A body a reader cannot read is `UNKNOWN` for all four readers, one request each. The cases are
      `tests/test_sinks_http_body_read.py`'s three `_READ_ERRORS`, a truncated `Content-Length` body
      and a truncated chunked one. An empty `2xx` body is `ACCEPTED` for `OTLPSink` and `UNKNOWN` for
      the other three, as SPEC-070 leaves them.
- [ ] A third-party `ElasticsearchSink` subclass declaring `log_foundry_idempotent = True` gets an
      unreadable `2xx` and then a `200`. It makes 2 requests, ends with `dropped_unadjudicated == 0`,
      and writes no unadjudicated line. Unreadable throughout at `max_retries=2`, it makes 3 requests
      and counts `dropped_unadjudicated == n` once, with one line.
- [ ] Each reader's `_handle_response`, called directly, returns the bool it returned at `fda134a`
      for every payload in that reader's existing tests, and moves the same counters.
- [ ] Serves invariants 2, 7 and 11:
  - each event is counted once, even across an idempotent resend;
  - a chunk wholly rejected still fails, while a partial one reports;
  - each line keeps its type-only, count-true text.

### FR-004: A third-party subclass that overrides `emit` or `_handle_response` keeps its 1.x path

#### Description:

A subclass written against 1.x extends the family through its documented hooks. Two of its
overrides would change meaning if the worker called `deliver` around them, and the base keeps each
one on its 1.x path.

**An override of `emit`.**

- SPEC-072 offers `deliver` only when no class below the one that defines it overrides `emit`, and
  it looks both up on the type. Such a subclass is therefore delivered to through its own `emit`.
- Its `super().emit(batch)` reaches `HTTPSink.emit`, which runs the round through the engine inline
  with `deliver=self._round`.
- So it still gets the chunking, the sink's own retries, the raise on total failure and the
  deferred drops, and the worker resends its raise as in 1.x.

**An override of `_handle_response`.**

- A 1.x override returns a bool, and its `False` meant "not settled". When no chunk settled the
  batch raised, and the worker resent it. Offered `deliver`, the same override would make its batch
  fail without a resend.
- So such a class is **ineligible**:
  - The worker and the orphan path call its `emit`, which runs the engine inline as above.
  - Its round reads a `2xx` through `HTTPSink._outcomes`, which is the override's bool, even when an
    intermediate shipped class (for example `ElasticsearchSink`) defines `_outcomes`.
  - Its `emit` raises on total failure and returns on partial success. The worker's 1.x resend is
    what a total failure gets.

**The base decides per class, in `HTTPSink.__init_subclass__`.** A class whose own body defines
`deliver` is left alone. For every other subclass it computes eligibility from the new class's MRO
down to `HTTPSink`:

- **ineligible** when a class above `HTTPSink` in the MRO defines `emit`, or when the first class
  defining `_handle_response` comes before the first defining `_outcomes`;
- **eligible** otherwise.

It then assigns `deliver` on the class only where the inherited value is wrong:

- An ineligible class gets `deliver = None`. SPEC-072's static lookup offers `deliver` only when it
  is a plain function, so `None` is not offered.
- An eligible class whose inherited `deliver` is `None` gets `deliver = HTTPSink.deliver` back. That
  happens when it defines `_outcomes` below an ancestor's `_handle_response`, and its MRO holds no
  `emit` override. Because an `emit` override makes a class ineligible, restoring `deliver` can never
  bypass one.

The shipped classes are eligible, and so are a third-party subclass that overrides only other hooks
and one that overrides both `_handle_response` and `_outcomes`. The detection is static, by class and
never by instance. A test that assigns `sink._handle_response` on an instance is spying, not
subclassing, and FR-005 moves those spies to `_outcomes`.

**The departures** are FR-002's moves 3 and 4. A third-party hook that raised escaped `emit` in 1.x,
and the worker resent chunks that had landed. Now the raise costs its chunk:

- a `_body` fault is counted on `_unrenderable`, inside `dropped`;
- a `2xx`-reader fault is counted on `dropped_unadjudicated`, inside `failed`.

#### Acceptance Criteria:

- [ ] `read_deliver` returns the bound `deliver` for an instance of each of the nine shipped classes.
      An AST-derived roster over the family asserts that no shipped class defines `emit`, `deliver`
      or `deliver = None`, and that each is eligible.
- [ ] A third-party `HTTPSink` subclass whose `emit` calls `super().emit(batch)` is not offered
      `deliver`.
  - Through a real `Worker` with `max_retries=2`, against an endpoint answering `400` throughout,
    its `emit` is called 3 times with 3 requests in all, and `failed_batches == 1`.
  - Against `503` then `200`, its events land once.
- [ ] A third-party subclass of `HTTPSink`, and one of `ElasticsearchSink`, whose
      `_handle_response(self, payload, items)` returns `False` are not offered `deliver`.
  - Through a real `Worker` with `max_retries=2` and a one-chunk batch, `emit` raises each time and
    the worker makes 3 attempts.
  - Beside a second chunk whose override returns `True`, `emit` returns and neither chunk is resent.
- [ ] The `ElasticsearchSink` subclass above has its override called on every `2xx`, and
      `ElasticsearchSink._outcomes` is never called for it.
- [ ] Eligibility of other third-party subclasses:
  - A subclass overriding only `_render` or `_body` is offered `deliver`.
  - One defining both `_handle_response` and `_outcomes` is offered it.
  - `B(A)`, where `A` overrides `_handle_response` and `B` adds `_outcomes`, is offered it, with
    `A` still not offered.
  - `B(A)`, where `A` overrides `emit` and `B` adds `_outcomes`, is not offered it.
- [ ] A third-party `_handle_response` that raises `ValueError` for one chunk of two leaves the other
      chunk delivered and `emit` returning. The raising chunk is counted on `dropped_unadjudicated`
      once, with a line naming `ValueError`.
- [ ] `tests/test_public_surface.py`'s existing pins on the family's hooks pass unedited, and
      `_handle_response`'s signature is `(self, payload: bytes, items: list[_Item]) -> bool`.
- [ ] Serves invariants 6, 7 and 10:
  - an eligible and an ineligible subclass each keep the ledger their path owes;
  - total failure raises from every `emit` a 1.x caller reaches;
  - the surface only grows.

### FR-005: Every class, both paths and both forms, held by the rosters that already hold the family

#### Description:

The proof is the existing real-socket rosters run against the new mechanism, plus the twins the
change creates.

- `tests/test_sink_losses.py` and `tests/test_sinks_http_body_read.py` keep passing.
  `tests/test_sinks_http_body_read.py` holds `_BODY_READERS`, every subclass over real sockets.
- `_BODY_READERS` is re-derived as the classes defining `_outcomes`, beside the classes defining
  `_handle_response`, and the two sets are asserted equal.
- The model test (SPEC-072's `deliver` recorder) gains an `HTTPSink` behind a scripted opener.

**Which tests still break, and how each is re-expressed.** The worker-path budget keeps the 1.x
total for retryable events. So a test that relies on the worker's resend of a **retryable** failure
passes unedited. That covers a sink with `max_retries=0` under `Worker(max_retries=3)` answered `500`
or `503` then `200`, and these named tests in particular pass unedited:

- `tests/test_sinks_http_render.py::test_a_retried_unrenderable_event_is_counted_once_or_never` (both
  ids);
- `tests/test_sinks_http_render.py::test_probe_c_rerun_the_oversized_event_is_counted_once_or_never`
  (both ids);
- `tests/test_sinks_otlp.py::test_the_oversize_drop_is_retry_safe` (both ids).

The `down` ids of those tests assert the worker's abandoned line by its prefix,
`lost N event(s); batch abandoned`, and nothing after it. SPEC-072's abandoned wording for an engine
run keeps that prefix.

What breaks is a test that relies on the worker resending a **non-retryable** outcome. No round
resends one, because an outcome rejected for good is final. Each such test is rewritten to what the
engine guarantees:

- `tests/test_sinks_honeycomb.py::test_through_a_worker_every_event_rejected_moves_failed_batches`
  asserts 1 POST and `rejected_events == 3`, where `fda134a` gave 4 and 12.
- `tests/test_sinks_otlp.py::test_every_record_rejected_raises_and_the_worker_recounts_per_attempt`
  asserts 1 POST and `rejected_records == 5`, where `fda134a` gave 4 and 20. It is renamed to say it
  counts once.
- **Probe D** (`tests/test_sinks_http_refused.py`): the opener's `RuntimeError` is refused by
  SPEC-070's predicate, so it is `REJECTED` at its first attempt and never resent. Four test
  functions are rewritten, 13 test ids in all:
  - `test_probe_d_down_counts_no_413_drop` keeps the `RuntimeError` and asserts one round: 2 calls
    where `fda134a` made 8, `dropped_oversized == 0`, no `413` line, `failed_batches == 1`, and the
    worker's abandoned line under SPEC-072's wording. It still pins invariant 2's rule that a failed
    run counts no drop.
  - `test_every_http_subclass_counts_no_413_drop_when_down` (9 ids) is rewritten the same way, with 2
    calls each.
  - `test_the_never_offered_route_defers_too[down]` is rewritten the same way, with 2 calls.
  - `test_probe_d_once_counts_the_413_drop_once` cannot keep a non-retryable fault, because no run
    resends it, so its first fault becomes a retryable `URLError`. It then asserts round one's `413`
    drop and the `URLError`, round two's single request answered `200` (3 calls where `fda134a` made
    4), and the drop counted once with one line. It still pins the rule that a resend never recounts
    a drop.
  - `test_the_never_offered_route_defers_too[once]` changes the same way: 3 calls,
    `dropped_oversized == 2` and two lines.

**Tests that pin a removed mechanism are rewritten:**

- `tests/test_sinks_http_url.py`'s `sink._sleep_backoff = ...` assignment patches a deleted method.
  The run's stop signal, set before `emit`, replaces it.
- `tests/test_sink_retry.py`'s interruptible-wait roster loses its `_sleep_backoff` branch.
  - Its derived module set is pinned to the set at the build's base minus `http`, so `http` leaving
    the set is asserted rather than silent.
  - The engine's round loop joins the check that a retry loop waits through `wait`.
- SPEC-070's loop roster (`_retryable_error` over every `for` loop on `range(...)` naming `retries`)
  goes from a floor of 12 to 11. It gains a check that `HTTPSink.deliver`, `emit` and `_send` reach
  `_attempt` only through `_engine`.
- `tests/test_deferral.py`: `HTTPSink`'s `DEFERRING_SITES` entry and its two `IMMEDIATE` entries move
  to the run context.
  - The gate counts a `count(..., when=SETTLED)` as a deferred write. So `dropped_oversized` and
    `_unrenderable` are still derived as dropped counters, written only by a commit.
  - No floor in that file or in `tests/test_worker_predicate_roster.py` is lowered.
- The OTLP truncated-`2xx` test that replaces `sink._handle_response` on the instance spies on
  `_outcomes` instead.

The build runs the suite before editing any test and reports every failing id in the PR body. Each
id is either one named above or a test the build edits, and the PR body gives each edited test's
removed mechanism. A test that pinned a behaviour named in `docs/invariants.md` is not edited unless
it is named above, with its invariant still asserted.

**Prose and records, at completion.** The two decisions below are rewritten in place, each with a
dated History entry, at the sentences named, as SPEC-070 leaves them. The completion ritual records
one new decision in `docs/decisions/sink-delivery/`. Its fence states:

- the family delivers one request per chunk per round, and the engine owns its retries within the
  1.x total;
- the family-neutral run context keeps its counters' units;
- each counter move of FR-002;
- a 1.x override keeps its path.

The rewrites and markers:

- `a-2xx-this-sink-cannot-read-is-counted-and-the-bulk-reading-is-bulks-alone.md`:
  - The fence's "counts the chunk on `dropped_unadjudicated` (an upper bound, into `failed`) and
    settles it" says the chunk is counted there through `HTTPSink._unadjudicated`, and its events are
    `UNKNOWN`: never resent by a shipped sink, and never a failed run.
  - The body's "The chunk is settled; `losses()` sums the counter into `failed`." says the same.
  - "`HTTPSink`'s default `_handle_response` still reads any `2xx` as delivered" becomes
    "`HTTPSink`'s default `_outcomes`, through `_handle_response`, still reads any `2xx` as
    `ACCEPTED`".
  - The closing "Both settle the chunk without a re-send." becomes "Both are `UNKNOWN`; neither is
    re-sent."
- `after-a-status-line-the-status-decides-a-2xx-whose-body-cannot-be-read-is-never-re-sent.md`:
  - The fence's "are retried, then counted in `failed`" becomes "are `RETRYABLE`, resent by the
    engine within its budget, and counted in `failed` for the last round's request".
  - The fence's "a `2xx` settles its chunk (`_bulk` counts it unadjudicated), a `503` retries, a
    `413` splits, a `400` is abandoned" becomes FR-001's outcome table.
  - The body's "anything escaping escapes `emit`, and the worker re-sends a batch the destination
    may have accepted" becomes "anything escaping a round would fail the engine's run, reporting as
    undelivered a batch the destination may have accepted, so the round guards every hook".
  - In *Rejected: `bytes | None`*, "lets `json.loads(None)`'s `TypeError` escape `emit`, so the batch
    is re-sent" becomes "makes the override raise on every unreadable body, which the round's guard
    then counts unadjudicated even where the override meant success".
- Superseded markers, each at the sentence still stating the old mechanism:
  - *A client exception costs its chunk*: "guarded *inside* `_send`" becomes "inside the round's
    request".
  - *A protocol's retry set is a private predicate hook*: "read at `_send`'s one site" becomes "read
    at the one classification FR-001 describes".
  - *A `dropped` count is written only by a `Deferral` commit*: `HTTPSink`'s sites now commit
    through the run context.
- The search state's scope is stated in the docstrings of `MAX_BUDGET_REDUCTIONS` ("one `emit` may
  make") and `_chunk_budget`, which become "one run". *A destination's limit is found by halving the
  budget* states no scope and is not edited.
- `docs/invariants.md`:
  - Entry 7's "only a `2xx` rejecting every record of every chunk raises there" gains "or, through
    `deliver`, fails the run".
  - Entry 7's recorded exceptions gain a batch whose every `_body` raises, beside SPEC-069's "a
    batch of nothing but events the sink cannot encode", which replaced the all-unrenderable HTTP
    batch.
  - Entry 2's list of deferring sites names the run context for `HTTPSink`.
- The README's HTTP-family retry paragraph, the docstrings of `HTTPSink`, `_send` and the three
  readers, and `docs/component-inventory.md`'s `http` row describe the round.

#### Acceptance Criteria:

- [ ] `tests/test_sink_losses.py` and `tests/test_sinks_http_body_read.py` pass, and every test in
      them not named above is unedited (`git diff` on each file touches only the named tests).
- [ ] The named "pass unedited" tests in `tests/test_sinks_http_render.py` and
      `tests/test_sinks_otlp.py` pass with their files unedited at those tests.
- [ ] Each rewritten test above asserts the figure stated for it, and fails at the build's base.
- [ ] Twin paths, for each of the nine shipped classes: the same scripted responses through a real
      `Worker` and through an orphan `info()` outside a span leave equal `losses()` after each, and
      equal request counts. The responses are `200`; `400`; `503` then `200`; an unreadable `2xx`;
      and an oversized event.
- [ ] Twin forms, for each of the nine: `emit(batch)` and an orphan run through `deliver`, which
      have the same budget `s`, leave equal `losses()`, equal request bodies in the same order and
      equal stderr lines over the same responses.
- [ ] The model test runs every seed with a family sink behind a scripted opener. No event is
      accepted twice, and every event is accepted or inside one counted term.
- [ ] The two pinned rosters each fail when mutated once. Deleting `http` from the expected
      retry-module set reddens `tests/test_sink_retry.py`. Restoring a `for attempt` loop in `_send`
      reddens SPEC-070's roster.
- [ ] Each decision named above carries its rewrite and a dated History entry. The build runs a
      `grep` and reports it: no file under `docs/` or in `README.md` still says `_send` retries the
      chunk path, or that the worker resends an HTTP batch that delivered nothing.
- [ ] Serves invariants 2, 6 and 7:
  - the ledger balances for every class on both delivery paths and both forms;
  - a run that delivered nothing raises or fails, while a partial one reports.

---

## Data Model

```
# src/log_foundry/sinks/_engine.py   -- private; added by this spec to SPEC-072's module.
# Family-neutral: nothing below names an HTTP type. It is the counting mechanism for every
# shipped sink; SPEC-074 extends RunContext (one-lock commit, final rejections, a refusal marker).
class When(enum.Enum):
    ALWAYS = "always"      # committed when the run ends, whatever its outcome
    SETTLED = "settled"    # committed only if the run settles (a per-event drop)
    LAST = "last"          # committed only if this round is the run's last

class RunContext:
    round: int                                             # 1-based, the round in progress
    def notes(self, owner: object) -> dict[str, object]: ...   # per run, per owner, across rounds
    def count(self, owner: _Counted, name: str, n: int, *, when: When) -> None: ...
    def announce(self, line: Callable[[], None], *, when: When) -> None: ...

def run_context() -> RunContext: ...   # the innermost run on the calling context (a ContextVar
                                       # stack), or a detached one-round run outside any run

# used from SPEC-072, not added here: run_delivery(..., worker_retries=w | None); the budget table
# of FR-001; emit_via_deliver(sink, batch, *, deliver=None); _run(sink, deliver, counters, batch,
# *, sink_retries, worker_retries, stop, decline); the static lookup that offers only a plain function.


# src/log_foundry/sinks/http.py
class HTTPSink:
    log_foundry_counters: SinkCounters                     # SPEC-072's event-unit totals
    failed: int                                            # requests, as in 1.x
    dropped_oversized: int                                 # events, SETTLED, as in 1.x
    dropped_unadjudicated: int                             # new on the base; 0 unless a reader gives up

    def __init_subclass__(cls, **kwargs: object) -> None: ...   # FR-004's eligibility
    def deliver(self, batch: list[dict[str, object]]) -> Verdict: ...   # one round
                                                           # class attribute None when ineligible
    def emit(self, batch: list[dict[str, object]]) -> None: ...         # the engine inline
    def _round(self, batch: list[dict[str, object]]) -> Verdict: ...
    def _outcomes(self, payload: bytes, items: list[_Item]) -> tuple[Outcome, ...]: ...
    def _unadjudicated(self, items: list[_Item], detail: str, noun: str = "event"
                       ) -> tuple[Outcome, ...]: ...       # the one unreadable-response method
    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool: ...   # unchanged
    def _send(self, body: bytes, *, content_type: str,
              extra_headers: dict[str, str] | None = None,
              splittable: bool = False,
              count_unexpected: bool = True) -> bytes: ... # 1.x contract (SPEC-070's keyword);
                                                           # its rounds run through _engine._run
    # deleted: _BACKOFF_BASE, _sleep_backoff, _deliver, _send's for-attempt loop
```

## API / Interface Contract

No public name is added. `deliver` is the optional member SPEC-072 defines, and it now appears on the
nine shipped classes. A third-party subclass that overrides one of the two 1.x hooks keeps its
1.x path:

```python
class Stamped(HTTPSink):                 # overrides emit: keeps its 1.x path (FR-004)
    def emit(self, batch):
        super().emit([{**e, "stamp": 1} for e in batch])

class Strict(ElasticsearchSink):         # overrides _handle_response: ineligible, 1.x path
    def _handle_response(self, payload, items):
        return b'"errors":false' in payload

read_deliver(Stamped("http://h"))        # None: the worker calls Stamped.emit
read_deliver(Strict("http://h", index="i"))                    # None
read_deliver(ElasticsearchSink("http://h", index="i"))         # the bound deliver
```

## File & Folder Structure

```
src/log_foundry/sinks/
├── _engine.py          # + When, RunContext, run_context()
├── http.py             # deliver/_round/_outcomes/_unadjudicated/__init_subclass__; emit and _send on the engine
├── elasticsearch.py    # _outcomes; _handle_response as the 1.x shim; no _abandon_unadjudicated
├── honeycomb.py        # likewise
└── otlp.py             # likewise

tests/
├── test_http_rounds.py            # new: FR-001, FR-002 and FR-004 against scripted openers
├── test_sinks_http_body_read.py   # _BODY_READERS beside the derived _outcomes set
├── test_sink_retry.py             # pinned module set; _sleep_backoff branch removed
├── test_deferral.py               # HTTPSink's sites moved to the run context
├── test_classify.py               # SPEC-070's loop roster: floor 11, the engine-only check
├── test_sinks_{honeycomb,otlp,http_refused,http_url}.py   # the named rewrites
└── test_invariants_model.py       # + an HTTPSink behind a scripted opener
```

## Implementation Phases

### Phase 1: The run context

- Add `When`, `RunContext` and `run_context()` to `_engine.py`, and test them against scripted test
  sinks: rounds, notes, the three `when` values, a detached context, a nested run, `BaseException`,
  two threads and a fork.

### Phase 2: The round

- Write `HTTPSink._round`, `deliver`, the classification table, the `413` search in the run's notes,
  the hint and the hook guards.
- Move `emit` and `_send` onto the engine, keeping `count_unexpected`, and delete `_BACKOFF_BASE`,
  `_sleep_backoff`, `_deliver` and the loop.
- Run the suite and record every failing id, then rewrite the retry and deferral rosters and the
  named tests in the same commit as the mechanism they pin.

### Phase 3: The readers

- Add `_outcomes` and `_unadjudicated`.
- Move the three readers onto them, keep each `_handle_response` as the shim, and derive
  `_BODY_READERS` beside `_outcomes`.

### Phase 4: Overrides and both paths

- Add `__init_subclass__`'s eligibility and the third-party fixtures.
- Add the worker, orphan and `emit` twin sweeps over the nine classes, and the model-test recorder.

### Phase 5: Prose and records

- The decision rewrites and markers, the new decision, invariants 2 and 7, the README, the
  docstrings, the inventory row and the release-notes entry, which lists every counter move.

## Revision history

- Drafted against `fda134a` from the arc plan's Change 2 items 5 to 7 and Change 3's rows "Retry
  loop", "Backoff constant" and "Giving up on an unreadable response". Decided while drafting, where
  the plan was silent:
  - the `413` search state is per run, not per round;
  - a round's hint is the largest of its retryable responses';
  - an OTLP partial success is positional by count only;
  - a raising `_body` drops its chunk on `_unrenderable`;
  - `_send` stays, on the engine;
  - a subclass whose `_handle_response` sits below its `_outcomes` is not offered `deliver`.
- Revised on the orchestrator's decisions:
  - The run context is the counting mechanism for every shipped sink, family-neutral, and SPEC-074
    extends it.
  - `deliver = None`, `emit_via_deliver`'s `deliver=` and `clamp_server_delay`'s ceiling rule are
    SPEC-072's.
  - The lone-`413` move is stated as deliberate.
  - `SentrySink` is in scope with no `deliver`.
- Revised on the spec review:
  - The worker-path budget is SPEC-072's 1.x total, `(s + 1) × (w + 1) − 1` further rounds (Andrew's
    decision, 2026-11-04). Its two cases are criteria, and the broken-test list is re-derived from
    it.
  - `in_run()` and `_send`'s one-attempt special case are withdrawn. A nested run is independent
    instead, because SPEC-072 makes nesting reachable through any engine sink.
  - `count_unexpected` is in `_send`'s contract.
  - Every counter move and every changed moment is enumerated.
  - `dropped_unadjudicated` counts `LAST` for an idempotent sink.
  - Eligibility is restored for an eligible subclass of an ineligible one.
  - The `_send` override bypass and the request-building hooks are named, and the halving and
    `_chunk_budget` citations are corrected.
- Cross-arc review: the invariant 7 edit and FR-002's item 3 point at SPEC-069's wording of the
  recorded exception, and the Data Model names `_run`'s `sink_retries, worker_retries` as SPEC-072
  does.
- Build-order re-read: FR-002's "no engine settled line" for a run with a registered announcement
  cites SPEC-072 FR-003, which now carries that refinement itself.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
- 2026-11-07: SPEC-070 changed the Probe D premise. An opener `RuntimeError` after a drop is now 2
  requests and `emit` returns, and Probe D was rebuilt on a raising `_handle_response`, which still
  makes 8 requests; the Probe D paragraph under the measured figures is revised to say so.
