---
type: Spec
id: SPEC-073
title: "The HTTP Family on the Delivery Engine"
description: "Puts HTTPSink and its eight shipped subclasses on SPEC-072's delivery engine: each offers deliver, one request per chunk per round, its emit runs the same engine inline, the three per-record readers return per-event outcomes through one shared unreadable-response method, every documented counter keeps its 1.x name and unit through a family-neutral run context, and a third-party subclass that overrides emit or _handle_response keeps its 1.x path."
spec_status: Completed
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
- **Tests that drive a family sink through a real `Worker`** (`fda134a`, re-measured at
  `c4e51fbd`). Thirteen test files at `fda134a`, and fifteen at `c4e51fbd`, name both `Worker(`
  and a family class, `SentrySink` or `LogstashSink`. The instrument is `grep -l 'Worker('` over
  `tests/*.py`, filtered by `grep -l` for the eleven class names. It counts co-occurrence, so it is
  an upper bound: the two files added since, `tests/test_classify.py` (SPEC-070) and
  `tests/test_numeric_doors.py` (SPEC-066), build their `Worker` around a Redis, SQS, SNS or stub
  sink and drive no family sink through one. Which files pin the removed whole-batch resend is
  FR-005's list.
- **Probe D** (`tests/test_sinks_http_refused.py`). It made 8 requests through
  `Worker(max_retries=3)` at `fda134a`: four whole-batch attempts of a `413` and an opener
  `RuntimeError`. SPEC-070 changed that shape: `HTTPSink._send` now catches an opener's
  `RuntimeError`, counts it in `failed` at its one attempt and lets nothing escape `emit`, so once
  the `413` drop has settled chunk 1, `emit` returns, the worker sees success and the whole run is
  2 requests with the drop counted once
  (`test_an_opener_exception_after_a_drop_is_counted_and_the_drop_settles`). SPEC-070 rebuilt
  Probe D's tests on an instance-level `_handle_response`, set by the file's `_arm` helper, that
  raises on chunk 2's `200` outside `_send`'s guard, so the raise still escapes `emit` and the
  probe still makes 8 requests at `c4e51fbd`. Under this spec neither fault keeps its meaning. The
  raising reader makes its chunk `UNKNOWN` (FR-001, FR-003), and an `UNKNOWN` settles the run, so
  the drop would be counted. The opener's `RuntimeError` beside the drop now fails the run, which
  is FR-002's move 2. FR-005 rebuilds each Probe D test on a fault that keeps the rule it pins.

## Scope

### In Scope

- `HTTPSink.deliver`, the round, its status-to-outcome rules, and `emit` and `_send` running through
  the engine. `_BACKOFF_BASE`, `_sleep_backoff`, `_deliver`, `_abandon` and the retry loop are
  deleted (FR-001).
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
  `504` are unchanged. A `413` is never `RETRYABLE` for a shipped class; it drives the budget
  search (FR-001). A third-party `_retryable_status` that accepts `413` keeps 1.x's order, in which
  the retry test runs first, so its `413` is `RETRYABLE`.
- **An adaptive budget across batches.** The `413` search is kept per run, as it was per `emit`. Its
  reduced budget is never carried into a later batch, for the ratchet `HTTPSink._chunk_budget`'s
  docstring records.
- **Declaring the shipped family idempotent.** No shipped HTTP sink sets `log_foundry_idempotent`,
  so an `UNKNOWN` outcome of theirs is never resent (#283's rule). A third-party subclass may declare
  it, and FR-003 counts that case once.
- **A compatibility promise for private methods other than the documented hooks.**
  - `_render`, `_body`, `_handle_response`, `_retryable_status`, `_response_limit`, `_item_size`,
    `_body_reserve`, `_prepare`, `_apply_auth`, `_attempt` and `_send` keep the signatures and
    meaning they have at the build's base. For `_prepare` and `_apply_auth` that meaning is the
    one PR 311 (`c4e51fbd`) gave them, which this spec does not change.
  - `_deliver`, `_sleep_backoff` and `_abandon` are removed, and a third-party override of one of
    them is no longer called. `_abandon_unreduced` stays, without its immediate store, and keeps
    its 1.x signature `(chunk)`: it reads the run's context through `run_context()`, so a
    third-party override written against 1.x is still called on the unreduced-`413` path.
  - `_items(batch, pending)` and `_drop_refused(chunk, pending)` keep their 1.x signatures. Their
    `pending` is now typed by a private Protocol that both a `Deferral` and the run context's
    `drops` view satisfy.
  - The chunk path no longer calls `_send`, so an override of `_send` still serves `SentrySink`
    and direct callers but no longer sees the chunks.
- **Changing `_handle_response`'s signature**, `SinkLosses`, `configure()`, `Config`, or any
  constructor. This spec adds no public name of its own beyond the `deliver` member SPEC-072
  defines. The adapter `HTTPSink.emit` runs, `emit_via_deliver`, is public since PR 312
  (`log_foundry.emit_via_deliver` and `log_foundry.sinks.verdict.emit_via_deliver`, a wrapper over
  the unchanged private `_engine.emit_via_deliver`); `HTTPSink.emit` calls the private one.

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
- Each chunk is sent in **one** request, exactly as `_send` builds and makes one at the build's
  base. `_body` builds the body. `_prepare` builds the headers and the optionally compressed
  bytes, and it calls `_apply_auth`, which writes the `Authorization` value built once at
  construction (PR 311). `_attempt` makes the request.

**Each item maps back to its event.** A first pass walks the batch forward by identity, which
is exact for every shipped sink, since `_items` builds each item from the batch's own event object
in order. An item that walk cannot place (an `_items` override that returns items built from other
objects, such as a subclass enriching each event first) takes, in order, the next batch index no
item claimed, so an `_items` override that returns one item per event, in order, built from other
objects, maps exactly by position. An event no request carries is `DROPPED` only when the number
of such events equals what `_items` dropped and counted, which the round learns by counting the
drops `_items` records; otherwise it is `REJECTED`, an over-report that can fail a run and is never
hidden. A placed event starts `REJECTED` until its chunk's outcome replaces it, and every drop path
settles its chunk `DROPPED` explicitly, so no event the sink did not drop is ever `DROPPED` or
`ACCEPTED` by default.

The round adds no header of its own. PR 311's header guards read this path: the `extra_headers`
scan in `tests/test_sinks_http_auth.py` reads every `_prepare` call's third argument and every
`extra_headers=` keyword in `src/`, and it reports any value other than a `dict` literal or the
forwarded parameter `extra_headers`. So the round passes one of those two, never `None`.

Nothing in the round waits or retries. The engine (SPEC-072) resends the `RETRYABLE` events in later
rounds, with the sink's own `max_retry_after`, its stop signal and, on the worker path, SPEC-062's
prediction. Its budget is SPEC-072's, and it keeps the 1.x total:

| Path | Further rounds | Where it comes from |
|---|---|---|
| worker path (`run_delivery(..., worker_retries=w)`) | `(s + 1) × (w + 1) − 1`, 15 at the defaults of 3 and 3 | 1.x attempts per `emit`, times the worker's attempts |
| orphan path (`worker_retries=None`) | `s` | 1.x had no resend around an orphan `emit` |
| `emit` itself (`emit_via_deliver`) | `s` | the catcher's own retry multiplies above it, as in 1.x |

Here `s` is the sink's `max_retries` and `w` the worker's. `HTTPSink.emit(batch)` is
`_engine.emit_via_deliver(self, batch, deliver=self._round)`, the private adapter behind PR 312's
public `log_foundry.emit_via_deliver`. `deliver=` is the keyword SPEC-072's adapter
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

The table is one private method, `HTTPSink._exchange`, which makes the request once and registers
its count and line on the run context. Its checks run in 1.x `_send`'s order: the exception, the
`2xx`, the response bound, `_retryable_status`, then the `413`. So a third-party
`_retryable_status` that accepts `413` still retries it, as in 1.x. Outside the round (`_send`
called with `splittable=False`) a `413` is the counted `REJECTED` row.

**The `413` search runs per run, not per round.** It works as SPEC-038 does:

- A `413` halves the budget to the refused body's size and re-chunks the round's remaining items.
- A lone refused item is `DROPPED`, and so is one `_known_refused` proves no smaller than one
  already refused.
- A multi-item chunk still refused after `MAX_BUDGET_REDUCTIONS` is `REJECTED`.

The budget, the smallest refused size and the reduction count live in the run's notes (FR-002).
A later round's resend therefore starts from the reduced budget instead of rediscovering it, drops
without a request a lone event no smaller than one already refused, and gets no fresh
`MAX_BUDGET_REDUCTIONS`, so a run searches no more than one 1.x `emit` did.

**The round's hint** is the largest finite, positive `Retry-After` among its `RETRYABLE` responses,
or `None` when none has one. The engine clamps it against `max_retry_after` by
`clamp_server_delay`'s ceiling rule. That is the clamp `_sleep_backoff` applies today, and SPEC-072
adopts it: an unusable ceiling (`0`, negative, `NaN`) means backoff, and an infinite one honours the
hint up to `MAX_WAIT`. One round has one wait, so a batch whose several chunks failed retryably
waits once per round, where 1.x waited once per chunk.

**A hook fault costs its chunk.** The round runs exactly these under a guard catching
`Exception`, never `BaseException`: what builds a chunk's request (`_known_refused`, `_body`,
`_prepare` with the `_apply_auth` it calls, and the construction of the
`urllib.request.Request`), and the `2xx` readers (`_outcomes` and `_handle_response`):

- If anything raises before the request is handed to `_attempt` (`_known_refused`, `_body`,
  `_prepare`, `_apply_auth` or building the `Request`), the chunk's events are `DROPPED` and counted on
  `_unrenderable` `SETTLED`, as a render fault is (SPEC-058). Nothing was sent, and the same
  inputs would fail the same way. The faults of one round aggregate as 1.x `_items`' render faults
  do: one count summing their events, and one line registered at the end of the round, naming the
  first exception's type, `lost <n> event(s); <class>, rendering raised <Type>`.
- If the `2xx` reader raises, the request succeeded and its events may have landed. The chunk goes
  through the unreadable-response method (FR-003) and its events are `UNKNOWN`, with the line
  `lost <n> event(s); <class> reading a 2xx response raised <Type>; they may have landed; not
  retried`. Only a reader that raises gets that line. A result that is not one `Outcome` per
  event (not a sequence, the wrong length, or another element type) makes the chunk `UNKNOWN`
  the same way, with the line `reading a 2xx response gave no outcome per event`. The result is
  checked by wrapping it in a `Verdict` and reading it through SPEC-072's `outcomes_of`, and a
  raise from that check reads as the same malformed result.

So no fault from a request-building hook, a `2xx` reader, `_attempt` or the opener escapes a round.
A fault from `_attempt` or the opener is classified by FR-001's table, and a fault building the
request or reading a `2xx` is under the guard. Under SPEC-072 an escaped exception fails the run,
which would report total failure for a batch whose other chunks landed. The guard covers those
hooks and no others. A raising third-party override of `_retryable_status`, `_items`, `_take`,
`_event_ceiling`, `_chunk_budget`, `_drop_refused` or `_abandon_unreduced` escapes the round, as
these escaped 1.x `emit`, and the engine then fails that round's events and announces the
exception's type. (`_retryable_status` is asked only of a non-`2xx` status, so it escapes only
then.) `_response_limit` is the one listed hook that does not escape: `_attempt` reads it first,
inside the attempt `_exchange` classifies, so a raising override is that attempt's refused
exception, `REJECTED` and counted in `failed` with a `connection error, <Type>` line, whatever
status the response carried, a `2xx` included. 1.x read it once in `_send` before the first
request, so the raise escaped `emit` raw with nothing sent.

> **Superseded** (fix/http-round-guards, after SPEC-073): the guard now covers every hook the
> round reaches, a fault costing only its chunk, `DROPPED` before its request is sent and `UNKNOWN`
> after; and an event `_items` leaves out uncounted is announced by the round itself. The decision
> lives in `docs/decisions/sink-delivery/the-http-family-delivers-in-rounds-and-a-run-context-keeps-its-counters-units.md`.

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
  stored exception after the run ends. The one exception to this is a `SinkDeliveryError`
  (`_PayloadTooLarge` included), which every caller reads as already counted, so `_send` counts it
  once in `failed` and writes its line, as SPEC-070 leaves it.

`_send` runs its attempts as rounds of one request, through SPEC-072's `_run`. Its budget is
`max_retries`, its stop event is `log_foundry_stop_signal`, and it has no decline predicate. So
`http.py` holds no retry loop and no wait of its own. `_prepare` runs once, before the run, as in
1.x. Each round builds a fresh `Request`. A raise from building a request, or from a hook
`_send`'s classification calls after the response (`_retryable_status`), is re-raised from
`_send` unchanged, the same object, after the run ends, counted nowhere, as 1.x let it escape:
never as a `SinkDeliveryError`, which every caller, `SentrySink` above all, reads as already
counted. A raising `_response_limit` is read inside the attempt, so it is classified and counted
as that attempt's refused exception: one request, `failed` moved, and `SinkDeliveryError` naming
`connection error, <Type>` (with `count_unexpected=False`, the exception itself, uncounted); in
1.x it escaped raw. The run's engine totals go to a throwaway `SinkCounters`, never the
sink's `log_foundry_counters`, because a request is not an event: a `SentrySink` envelope or a
nested `_send` never appears in the sink's event totals.

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

**Deleted:** `_BACKOFF_BASE`, `_sleep_backoff`, `_deliver`, `_abandon`, `_send`'s `for attempt`
loop, and `_abandon_unreduced`'s direct store to `failed` (FR-002 replaces them).

#### Acceptance Criteria:

- [x] `http.py` holds no `for` loop over `range(...)` naming `retries`, no `_BACKOFF_BASE`, and no
      call to `wait`, `time.sleep` or `Event.wait`, and neither does any module of the family (an
      AST check over every class the family scan finds: 8 modules, `http.py` and the seven its
      subclasses live in, `OpenSearchSink` sharing `elasticsearch.py`).
- [x] Worker budget: an `HTTPSink` at its defaults under a default `Worker`, answered nine `503`s
      then a `200`, is delivered on the 10th request with `failed_batches == 0`, as at `fda134a`.
- [x] Worker budget: `HTTPSink(max_retries=0)` under `Worker(max_retries=3)`, answered `503` then
      `200`, is delivered on the 2nd request. Answered `503` throughout, it makes 4 requests and
      `failed_batches == 1`.
- [x] Orphan budget: an orphan `info()` against `HTTPSink(max_retries=2)` answered `503` throughout
      makes 3 requests on the caller's thread, and `orphan_lost == 1`.
- [x] A two-chunk batch answered `200` and `400` makes 2 requests and `failed` rises by 1. Through a
      real `Worker` with `max_retries=3` it is still 2 requests (where `fda134a` made 2), `flush()` is
      truthy and `failed_batches == 0`.
- [x] A one-chunk batch answered `400` throughout, through a real `Worker` with `max_retries=3`, makes
      1 request where `fda134a` made 4, and `failed_batches == 1`.
- [x] Through `emit` with `max_retries=2`, a one-chunk batch answered `503` then `200` makes 2
      requests, waits `0.1` s once and leaves `failed` at 0. Answered `503` throughout it makes 3
      requests, and `failed` is 1, with one line reading `3 attempt(s), HTTP 503`.
- [x] Two chunks answered `503` and `200`, then `200`, make 3 requests in all, and the second round's
      request carries exactly the first chunk's events.
- [x] An opener raising `URLError` is `RETRYABLE`. One raising `RuntimeError` makes 1 request,
      counts `failed == 1` and fails a one-chunk run, also through a real `Worker`.
- [x] Retry-After:
  - `Retry-After: 2.5` with `max_retry_after=1.0` waits `1.0`.
  - Two retryable chunks with hints `1` and `4` wait `4`, once.
  - With `max_retry_after` of `0`, `-1` or `nan` the hint is ignored and the wait is the backoff,
    as at `fda134a`.
  - With `inf`, a hint of `100` waits `100`.
- [x] A stop signal set during a hinted wait returns `emit` within 0.5 s of wall time and under
      0.05 s of CPU, measured through `tests/test_worker_exit_budget.py`'s helper.
- [x] A destination refusing any body over 300 bytes is sent 40 small events under a 5,000,000-byte
      budget. It gets the same requests in the same order as at `fda134a`. When two adjacent
      requests of round one (the 4th and 5th, events 0–4 and 5–9) are answered `503`, round two
      resends them as exactly two requests, events 0–4 then 5–9, each 300 bytes or less: round two
      makes no `413`, because it starts from the budget round one found.
- [x] A lone event whose body is no smaller than one round one refused is dropped in round two
      without a request: two events, one per chunk, answered `503` then `413`, make 2 requests in
      all, and `dropped_oversized == 2`.
- [x] A resend gets no fresh `MAX_BUDGET_REDUCTIONS`: with every item charged nothing, two
      four-event chunks answered `503` then `413` throughout make 27 requests, the second chunk's
      25 and one resend of the first, which is `REJECTED` after one request, and `failed == 2`, the
      round-one `503` never counted.
- [x] A multi-item chunk still `413` after `MAX_BUDGET_REDUCTIONS` makes
      `MAX_BUDGET_REDUCTIONS + 1` requests, is `REJECTED` and counts `failed == 1` with its 1.x
      `still 413 after 24 reduction(s)` line, `dropped_oversized` unmoved. A third-party
      `_abandon_unreduced` override written to the 1.x signature `(chunk)` is still called.
- [x] A third-party `_retryable_status` that accepts `413` is retried before the search: answered
      `413` then `200` at `max_retries=1`, it makes 2 requests and drops nothing.
- [x] Mapping: an `_items` override that rebuilds every event (`{**event, "host": "h1"}`), through a
      real `Worker` with `max_retries=3` and the sink at `max_retries=0`: answered `400` the run
      fails with `failed_batches == 1` after 1 request; answered `200`, or `503` then `200`, it
      delivers both events with nothing dropped and no drop line. An override that returns no item
      for one of two events, and counts no drop, gets `REJECTED` for that event, never `DROPPED`.
- [x] The request-building line names the round's first exception: three chunks whose `_body`
      raises `ValueError`, then `KeyError`, then nothing make 1 request and one line, `lost 2
      event(s); <class>, rendering raised ValueError`.
- [x] A reader result that is not one `Outcome` per event (`None`, a `set`, a list of `str`, or one
      outcome for two events) makes both events `UNKNOWN`, with the line `lost 2 event(s); <class>
      reading a 2xx response gave no outcome per event; they may have landed; not retried`.
- [x] The existing `413` tests in `tests/test_sinks_http.py` and `tests/test_sinks_http_refused.py`
      pass with their bodies unedited, apart from the Probe D tests FR-005 names. Their files
      change only as FR-005 says: the `_no_sleep` fixture, the Probe D tests and their helpers,
      and the `match=` of `test_every_chunk_failing_is_the_total_failure_the_worker_retries`, a
      `500` test, not a `413` one.
- [x] A subclass whose `_body` raises `ValueError` for one chunk of three has that chunk's events
      counted on `losses().dropped` once, with a line naming `ValueError`. The other two chunks are
      sent, `deliver` raises nothing, and `emit` does not raise.
- [x] A subclass whose `_prepare` raises `ValueError` for one chunk of three has that chunk's
      events counted on `losses().dropped` once, with a line naming `ValueError`. The other two
      chunks are sent, and neither `deliver` nor `emit` raises.
- [x] `_send` keeps 1.x behaviour:
  - A `400` raises `SinkDeliveryError` after 1 request with `failed == 1`.
  - A `503` throughout makes `max_retries + 1` requests.
  - A `413` with the default `splittable` is the counted abandonment, and with `splittable=True` it
    raises `_PayloadTooLarge` uncounted.
  - With `count_unexpected=False`, an opener's `RuntimeError` is raised from `_send` by identity
    after 1 request, `failed` unmoved and no line written.
  - A raise building its `Request` is re-raised from `_send` unchanged, by identity, with no
    request made and `failed == 0`.
  - A `_retryable_status` that raises on a `500` is re-raised from `_send` by identity after 1
    request, with `failed == 0` and nothing on stderr; a `_response_limit` that raises is counted
    as the attempt's refused exception, 1 request, `failed == 1`, and `SinkDeliveryError` reading
    `<class>, 1 attempt(s), connection error, RuntimeError`
    (`test_send_reraises_a_classifying_hooks_fault_unchanged`, both ids).
  - `tests/test_sinks_http.py`'s two `_send` tests and every `SentrySink` HTTP-fallback test pass
    with their bodies unedited. The other edits in their files are the ones FR-005 names: the
    wait patches (`tests/test_sinks_http.py`'s `_no_sleep` fixture and `tests/test_classify.py`'s
    `_sentry` helper), `tests/test_classify.py`'s loop roster, and the `match=` FR-005 names in
    `tests/test_sinks_http.py`.
- [x] PR 311's header guards pass with their test bodies unedited:
      `tests/test_sinks_http_auth.py`, `tests/test_sinks_http_header_collisions.py`,
      `tests/test_sinks_http_header_names.py`, `tests/test_sinks_http_header_merge.py` and
      `tests/test_sinks_http_header_roster.py`. The only edits in those files are their
      `_no_sleep` fixtures and one corpus row of `tests/test_sinks_http_header_collisions.py`
      whose planted method was the deleted `_sleep_backoff`, now `_round`. No family class gains
      an override of `_prepare`, `_apply_auth` or `_send`.
- [x] Nested runs: a third-party test sink with `deliver` wraps a `SentrySink` on the HTTP fallback,
      and its `deliver` calls `inner.emit(batch)` against `503` then `200`. The inner `_send` makes
      2 requests and waits once, and the Sentry event is sent. Inside the inner run
      `run_context().round` reads 1, then 2. After it returns, the wrapper's context still reads
      its own round 1 with its own notes.
- [x] Nested runs, inside the sink's own round: a third-party `_outcomes` hook, on its chunk's `2xx`,
      calls `self._send` against `503` then `200`. That `_send` makes 2 requests and returns the
      payload, and the chunk's outcome is what the hook returns. The outer run still has 1 round.
- [x] `read_deliver` is `None` for `SentrySink` on either backend.
- [x] Serves invariants 2, 3, 4, 7 and 13:
  - each event is offered until it has an outcome, and never after;
  - every wait is the engine's: bounded, idle and cut by the stop event;
  - an orphan call still retries on the caller's thread;
  - a run that delivered nothing raises from `emit`, and a partial one reports;
  - the round sends the headers and credentials construction checked, so no argument PR 311
    refuses or freezes there is read differently on the chunk path.

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
  in registration order. A `BaseException` commits nothing, as SPEC-072 states. A commit that
  raises for one owner is announced through the engine's absorbed line and does not stop the
  others' commits or any line.
- `drops(owner)`: a `Deferral`-shaped view, `count(counter, n=1)` and `announce(line)`, that
  registers every count and line `SETTLED` against `owner`. It is what `_items` and `_drop_refused`
  receive as their 1.x `pending` argument, so both keep their signatures.

Owners and notes are keyed by identity (`id`), so no third-party `__hash__` or `__eq__` runs. The
context is cleared when its run ends, so it holds no sink afterwards and adds no global that holds
a sink (invariant 12).

A round called outside any run (a direct `sink.deliver(batch)`) commits its own ledger when it
returns. It counts as a one-round run that is its own last and settles unless its verdict fails. So
`losses()` still moves for a caller that bypasses the engine. `run_context()` called outside any
run, as a direct call of a reader's `_outcomes` makes, returns an *immediate* context, in which each
count and line commits at once as a one-round run that is its own last and settles. Each count
names its owner, so a delegate that forwards `deliver` (SPEC-075's `LogstashSink`) commits into the
`HTTPSink` that made the count, never into itself.

**Where each 1.x count lands:**

| Count | Unit | `when` |
|---|---|---|
| `dropped_oversized` (per-event ceiling, a `413` lone item) and `_unrenderable` (render fault, or a fault building a chunk's request) | events | `SETTLED` |
| `failed` for a `REJECTED` request (status, response bound, refused exception, unreduced `413`) | requests | `ALWAYS` |
| `failed` for a request whose events were `RETRYABLE` | requests | `LAST` |
| `item_errors`, `rejected_events`, `rejected_records` | events | `ALWAYS` |
| `dropped_unadjudicated` | events | `ALWAYS`, or `LAST` for a sink declaring `log_foundry_idempotent` (FR-003) |

Each line keeps its 1.x text, and the attempt count it names is the round number. A settled run in
which the sink registered a line that will commit is announced by those lines alone, so a family
loss keeps the line it has in 1.x and the engine writes no settled line of its own. That is SPEC-072
FR-003's refinement of the engine's settled line. SPEC-072's `_run` did not carry it, since it had
no context to read, so **this spec adds it**, keyed on lines that will commit rather than lines
registered: a `LAST` line from an earlier round, which will not commit, does not silence the
engine's line, so a settled loss is never left unannounced. A third-party bool
`_handle_response` whose `False` makes its chunk `REJECTED` registers no line of its own, so a
settled run beside it gets the engine's settled line, which 1.x never wrote. A failed run's catcher
still writes its line: the worker's abandoned or declined line, the orphan path's, or the adapter's
raise.

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
   it. The move covers a raise from `_prepare`, from `_apply_auth` or from building the `Request`
   as well, each of which escaped `emit` through `_send` in 1.x and is now a `_body` fault's drop.
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

**One message changes with them.** `emit` is SPEC-072's adapter, so its total-failure
`SinkDeliveryError` is the adapter's, `delivered none of <n> event(s): rejected <r>, unresolved
<u>, raised <x> after <k> delivery round(s) to <class>`, where 1.x `HTTPSink.emit` raised
`<class> delivered none of <n> chunk(s)`. It counts events, not chunks, and names counts and the
class, never a value.

#### Acceptance Criteria:

- [x] Run context basics:
  - `run_context()` inside a round reports `round` 1, 2 and 3 for three rounds of one run, and
    `notes(owner)` returns the same `dict` in each.
  - Two runs on two threads against one sink never see each other's notes or ledger.
  - A run nested in a round sees a fresh context, and the outer context is restored when it ends.
- [x] Commit rules:
  - `when=SETTLED` counts commit for a settled run and not for a failed one.
  - `ALWAYS` commits for both.
  - `LAST` commits only the final round's entries, including in a run ended by the worker's decline.
  - A `BaseException` raised from round two commits nothing, and writes none of the run's lines.
- [x] `failed` counts:
  - Through `emit`, a one-chunk `503` throughout at `max_retries=2` leaves `failed == 1`, as at
    `fda134a`.
  - Through a real `Worker` with `max_retries=3`, the same batch makes 12 requests (as at `fda134a`)
    and leaves `failed == 1` where `fda134a` left 4.
  - Three chunks answered `503` throughout through `emit` leave `failed == 3`, with three lines each
    naming `3 attempt(s)`.
- [x] A `503`, `503`, `400` sequence leaves `failed == 1`, with one line naming `3 attempt(s), HTTP 400`.
- [x] Oversized events:
  - An oversized event beside a delivered chunk is counted on `dropped_oversized` once, with its 1.x
    line.
  - Beside a chunk answered `400`, the run fails and `dropped_oversized` stays 0.
- [x] A lone `413` refusal:
  - Beside a chunk answered `400`, the run fails, `dropped_oversized` stays 0 and through a real
    `Worker` `failed_batches == 1`.
  - Beside a delivered chunk, it is counted on `dropped_oversized` once.
- [x] A batch of three chunks whose every `_body` raises makes no request, counts
      `losses().dropped == n` with one line, settles, and leaves `flush()` truthy.
- [x] An unreduced chunk's `failed` counts `ALWAYS`: beside a chunk answered `503` and landed in
      round two, a four-event chunk still `413` after `MAX_BUDGET_REDUCTIONS` keeps `failed == 1`
      and its line, in 27 requests.
- [x] A settled family run with a loss writes exactly its 1.x line or lines and no engine line.
      `tests/test_diag.py`'s roster covers every new call site, and no line carries a value.
- [x] `log_foundry.health().sink` for each family class equals `sink.losses()` computed by its
      1.x formula, after each of FR-005's five twin-path responses (`200`; `400`; `503` then `200`;
      an unreadable `2xx`; an oversized event) on the orphan path.
- [x] A forked child that finds the counters' lock held by a parent thread completes a family round
      within 5 s, and its counts add to the inherited totals.
- [x] Serves invariants 2, 11 and 12:
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
`_read_failed` flags. The round also calls it for a reader that raised, and for one whose result
is not one `Outcome` per event (FR-001), with the details `reading a 2xx response raised <Type>`
and `reading a 2xx response gave no outcome per event`. It registers its line even for an empty
chunk, as each 1.x copy wrote it.

An `UNKNOWN` event never fails a run (SPEC-072), as a 1.x unadjudicated chunk settled. A shipped
family sink is not idempotent, so its `UNKNOWN` is final at once, and `_unadjudicated` counts and
announces it `ALWAYS`. A third-party subclass that declares `log_foundry_idempotent = True` has its
`UNKNOWN` chunk resent. For it `_unadjudicated` counts and announces `LAST`, so an event is counted
once per run, and only when it ended the run unadjudicated. A later round that accepts it counts
nothing.

Each reader keeps `_handle_response(payload, items) -> bool` for a direct 1.x caller. It runs
`_outcomes` in a one-round context of its own, pushed whatever else is running, and commits that
context when `_outcomes` returns, as FR-002 describes for a round outside any run. It returns
`False` exactly when the chunk is non-empty and every outcome is `REJECTED`. The non-empty clause
is what keeps `tests/test_sinks_otlp.py::test_handle_response_never_raises_on_hostile_bytes`,
which calls it with an empty chunk and expects `True` for six payloads, reading what it read: "every
outcome `REJECTED`" holds vacuously for no outcomes. One bool differs from 1.x as a result, and no
existing test covers it: 1.x `OTLPSink` returned `False` for an empty chunk beside a positive
`rejectedLogRecords`, and the shim returns `True`. So `tests/test_public_surface.py`'s calls and
OTLP's hostile-bytes test read what they read at `fda134a`.

#### Acceptance Criteria:

- [x] `_abandon_unadjudicated` is defined nowhere in `src/`. `HTTPSink._unadjudicated` is the only
      definition that writes `dropped_unadjudicated`, beside `HTTPSink.__init__`'s zero, and an AST
      check holds that for every family class.
- [x] For each reader, every unadjudicated case in its existing tests behaves as at `fda134a`: the
      same line text, the same `dropped_unadjudicated`, one request, nothing resent, and `flush()`
      truthy.
- [x] `ElasticsearchSink` with items `[ok, error, ok]` returns `[ACCEPTED, REJECTED, ACCEPTED]` and
      `item_errors == 1`. `[error, error]` fails a one-chunk run, and `emit` raises.
- [x] `HoneycombSink` statuses `[202, 400, 202]` return `[ACCEPTED, REJECTED, ACCEPTED]` and
      `rejected_events == 1`. Statuses `[400, 400, 400]` fail a one-chunk run.
- [x] `OTLPSink`:
  - `rejectedLogRecords` of 2 of 5 returns two `REJECTED` and three `ACCEPTED`, with
    `rejected_records == 2`.
  - 5 of 5 fails the run.
  - `"9" * 30` is `UNKNOWN`, through `_unadjudicated`.
- [x] A body a reader cannot read is `UNKNOWN` for all four readers, one request each. The cases are
      `tests/test_sinks_http_body_read.py`'s three `_READ_ERRORS`, a truncated `Content-Length` body
      and a truncated chunked one. An empty `2xx` body is `ACCEPTED` for `OTLPSink` and `UNKNOWN` for
      the other three, as SPEC-070 leaves them.
- [x] A third-party `ElasticsearchSink` subclass declaring `log_foundry_idempotent = True` gets an
      unreadable `2xx` and then a `200`. It makes 2 requests, ends with `dropped_unadjudicated == 0`,
      and writes no unadjudicated line. Unreadable throughout at `max_retries=2`, it makes 3 requests
      and counts `dropped_unadjudicated == n` once, with one line.
- [x] A reader's `_handle_response` shim called inside a round, by a third-party bool override of
      `ElasticsearchSink`, commits its counts when it returns, not when the outer run ends.
- [x] Each reader's `_handle_response`, called directly, returns the bool its 1.x code returned for
      every payload in that reader's existing tests, and moves the same counters. The expectations
      were recorded from the readers at the Phase 2 commit (`8663f868` after the rebase), still the build base's
      1.x code, so the test is a differential oracle, never recomputed from the shims.
- [x] Serves invariants 2, 7 and 11:
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

**The base decides per class, in `HTTPSink.__init_subclass__`.** It reads every member through
`sinks.base`'s static accessors (`_class_member`, `_MRO`, `_NAMESPACE`), comparing positions in the
MRO and finding `HTTPSink`'s own position by identity, so no metaclass hook runs and no class is
compared with `==`.

- It writes a private class flag, `_READS_BOOL`, on **every** subclass, `False` included: true when
  the first class defining `_handle_response` comes before the first defining `_outcomes`. The
  round reads a class with the flag through `HTTPSink._outcomes`, which is the override's bool.
- A class whose own body defines `deliver` is then left alone.
- For every other subclass the eligibility slice runs from the new class down to the **nearest
  ancestor whose own body defines a non-`None` `deliver`**: `HTTPSink`, or a third party's
  engine-native subclass. So a plain child of a third party's engine-native subclass keeps that
  parent's `deliver`.
- The class is **ineligible** when a class in that slice defines `emit`, or when its
  `_handle_response` comes before its `_outcomes` and inside the slice; **eligible** otherwise.

It then assigns `deliver` on the class only where the inherited value is wrong:

- An ineligible class gets `deliver = None`, and a second private class flag,
  `_DELIVER_WITHDRAWN = True`, marks that `None` as the base's own. SPEC-072's static lookup offers
  `deliver` only when it is a plain function, so `None` is not offered.
- An eligible class gets its `deliver` back **only over a `None` the base itself assigned**: when
  the `deliver` it inherits is `None` and the class holding that `None` carries
  `_DELIVER_WITHDRAWN`. It gets the nearest non-`None` `deliver` above it, which is
  `HTTPSink.deliver` unless a third party's engine-native ancestor defines its own. That happens
  when it defines `_outcomes` below an ancestor's `_handle_response`, and its slice holds no `emit`
  override. Because an `emit` override in the slice makes a class ineligible, restoring `deliver`
  can never bypass one.
- A third party's own `deliver = None` is an opt-out. It carries no `_DELIVER_WITHDRAWN`, so
  neither that class nor any plain child of it is ever restored.

The shipped classes are eligible, and so are a third-party subclass that overrides only other hooks
and one that overrides both `_handle_response` and `_outcomes`. The detection is static, by class and
never by instance. A test that assigns `sink._handle_response` on an instance is spying, not
subclassing. FR-005 moves the one such spy that keeps a purpose, in OTLP's truncated-`2xx` test, to
`_outcomes`, and replaces Probe D's `_arm` spy with a fault in the opener's script.

**The departures** are FR-002's moves 3 and 4. A third-party hook that raised escaped `emit` in 1.x,
and the worker resent chunks that had landed. Now the raise costs its chunk:

- a `_body`, `_prepare` or `_apply_auth` fault is counted on `_unrenderable`, inside `dropped`;
- a `2xx`-reader fault is counted on `dropped_unadjudicated`, inside `failed`.

#### Acceptance Criteria:

- [x] `read_deliver` returns the bound `deliver` for an instance of each of the nine shipped classes.
      An AST-derived roster over the family asserts that no shipped class defines `emit`, `deliver`
      or `deliver = None`, and that each is eligible.
- [x] A third-party `HTTPSink` subclass whose `emit` calls `super().emit(batch)` is not offered
      `deliver`.
  - Through a real `Worker` with `max_retries=2`, against an endpoint answering `400` throughout,
    its `emit` is called 3 times with 3 requests in all, and `failed_batches == 1`.
  - Against `503` then `200`, its events land once.
  - On the orphan path, `info()` outside a span against `503` throughout makes `s + 1` requests
    through its `emit`, and `orphan_lost == 1`.
- [x] A third-party subclass of `HTTPSink`, and one of `ElasticsearchSink`, whose
      `_handle_response(self, payload, items)` returns `False` are not offered `deliver`.
  - Through a real `Worker` with `max_retries=2` and a one-chunk batch, `emit` raises each time and
    the worker makes 3 attempts.
  - Beside a second chunk whose override returns `True`, `emit` returns and neither chunk is resent.
  - On the orphan path, `info()` outside a span against `503` throughout makes `s + 1` requests
    through its `emit`, and `orphan_lost == 1`.
- [x] The `ElasticsearchSink` subclass above has its override called on every `2xx`, and
      `ElasticsearchSink._outcomes` is never called for it.
- [x] Eligibility of other third-party subclasses:
  - A subclass overriding only `_render` or `_body` is offered `deliver`.
  - One defining both `_handle_response` and `_outcomes` is offered it.
  - `B(A)`, where `A` overrides `_handle_response` and `B` adds `_outcomes`, is offered it, with
    `A` still not offered.
  - `B(A)`, where `A` overrides `emit` and `B` adds `_outcomes`, is not offered it, and neither is
    `B(A)` where `A` overrides both `emit` and `_handle_response` and `B` adds `_outcomes`.
  - A third-party class setting `deliver = None` is not offered it, and neither is its child that
    adds `_outcomes`.
  - A plain child of a third-party subclass that defines its own `deliver` is offered that
    parent's `deliver`, and so is a plain child of one that defines both `deliver` and its own
    `emit`: the slice ends at that parent, so its `emit` is not an override inside it.
- [x] A permanent third-party bool reader whose `False` makes one of two chunks `REJECTED` settles,
      leaves its 1.x counters at zero, and is announced by the engine's settled line alone,
      `lost 1 event(s); rejected 1, unresolved 0, raised 0, unknown 0, dropped 0 after 1 delivery
      round(s) to <class>`.
- [x] A third-party `_handle_response` that raises `ValueError` for one chunk of two leaves the other
      chunk delivered and `emit` returning. The raising chunk is counted on `dropped_unadjudicated`
      once, with a line naming `ValueError`.
- [x] `tests/test_public_surface.py`'s existing pins on the family's hooks pass unedited, and
      `_handle_response`'s signature is `(self, payload: bytes, items: list[_Item]) -> bool`.
- [x] Serves invariants 6, 7 and 10:
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
- **Probe D** (`tests/test_sinks_http_refused.py`). Its tests inject their fault through `_arm`, an
  instance-level `_handle_response` that raises on chunk 2's `200`. Under this spec that chunk is
  `UNKNOWN`, which settles the run, so a "down" test would count the drop it exists to forbid.
  Each test gets a fault in the opener's script instead, and the spy has no use left. The "down"
  tests pin invariant 2's rule that a failed run counts no drop, so their fault must fail the run:
  a `400`, which is `REJECTED` for every family class. The "once" tests pin that a resend never
  recounts a drop, so their fault must be resent: a `URLError`, which is `RETRYABLE`. Six test
  functions are rewritten, 14 test ids in all. Each id fails at `c4e51fbd`, where every one of
  these scripts settles its batch after 2 requests and counts the drop.
  - `test_probe_d_down_counts_no_413_drop` answers `413` then `400`, and asserts one round: 2 calls
    where `fda134a` and `c4e51fbd` made 8, `dropped_oversized == 0`, no `413` line, `failed == 1`
    with the `lost 1 request(s); HTTPSink, 1 attempt(s), HTTP 400` line, `failed_batches == 1`, and
    the worker's abandoned line under SPEC-072's wording, `batch abandoned after 1 emit attempts`.
  - `test_every_http_subclass_counts_no_413_drop_when_down` (8 ids, one per shipped subclass) is
    rewritten the same way: 2 calls each, `losses().dropped == 0` and `failed_batches == 1`.
  - `test_the_never_offered_route_defers_too[down]` is rewritten the same way: 2 calls for its three
    events, `dropped_oversized == 0` and no line.
  - `test_probe_d_once_counts_the_413_drop_once` answers `413`, then a `URLError`, then `200`. It
    asserts round one's `413` drop and `RETRYABLE` event, round two's single request answered `200`
    (3 calls where `fda134a` and `c4e51fbd` made 4), `failed == 0`, and the drop counted once with
    one line.
  - `test_the_never_offered_route_defers_too[once]` changes the same way: 3 calls,
    `dropped_oversized == 2` and two lines.
  - `test_a_fanout_absorbs_the_probe_d_batch` answers its child `413` then `400`. The child's run
    fails, so its `emit` raises, the child's `losses().dropped` is 0 and the fan-out counts both
    events, as its existing assertion on `multi.losses()` says. It still pins that a raise
    discards the child's drop.
  - `test_an_opener_exception_after_a_drop_is_counted_and_the_drop_settles` keeps its opener
    `RuntimeError`, which is now FR-002's move 2. It asserts 2 calls as before, `failed_batches ==
    1`, `losses() == SinkLosses(dropped=0, failed=1)`, no `413` line, the `lost 1 request(s)` line
    unchanged and the worker's abandoned line. It is renamed to say the drop is not counted. With
    the `400` tests it pins the same rule on the other `REJECTED` route of FR-001's table.
  - `test_an_all_413_batch_settles_and_counts_all` and `test_an_orphan_413_settles_inside_the_sink`
    pass with their bodies unedited: an all-`DROPPED` run settles, as SPEC-072's rule says.

**Tests that pin a removed mechanism are rewritten:**

- **The wait patches.** `http.py` loses its only call to `wait` with `_sleep_backoff`, and so its
  import. Sixteen sites patch `log_foundry.sinks.http.wait` at the branch's rebased base,
  `458d79e6`, and a `monkeypatch` of a missing attribute raises. Fifteen are in the files the
  instrument finds (`grep -rln 'log_foundry.sinks.http.wait\|http_module, "wait"' tests/`):
  twelve at `c4e51fbd`, and three PR 312 added, the `_no_sleep` fixtures of
  `tests/test_sinks_http_body_hook.py`, `test_sinks_http_credentials.py` and
  `test_sinks_http_header_values.py`. The sixteenth is `tests/test_sink_losses.py`'s `_no_sleep`,
  which builds the path with an f-string over a tuple of module names holding `"http"`, so the
  grep cannot see it. Each is re-pointed at the engine's wait, `log_foundry.sinks._retry.wait`,
  which `_engine._wait` calls through the module:
  - the autouse `_no_sleep` fixtures of `tests/test_sinks_http.py`, `test_sinks_http_render.py`,
    `test_sinks_honeycomb.py`, `test_sinks_otlp.py`, `test_sinks_http_body_read.py`,
    `test_wrapper_failures.py`, `test_sinks_http_auth.py`, `test_sinks_http_header_collisions.py`,
    `test_sinks_http_header_names.py`, `test_sinks_http_header_merge.py`,
    `test_sinks_http_header_roster.py`, `test_sinks_http_body_hook.py`,
    `test_sinks_http_credentials.py` and `test_sinks_http_header_values.py`, with no test body
    edited;
  - `tests/test_sink_losses.py`'s `_no_sleep`, whose tuple loses `"http"` and which patches
    `_retry.wait` beside the per-module patches it keeps;
  - `tests/test_sinks_otlp.py::test_a_persistent_503_makes_three_requests_and_waits_on_the_stop_signal`
    spies on the engine's wait, still asserting 3 requests and two waits on the sink's signal;
  - `tests/test_sinks_http_body_read.py::test_retry_after_survives_a_failed_body_read` records the
    engine's waits, still asserting `[2.0]`;
  - `tests/test_numeric_doors.py`'s `_retry_after_waits` helper records the engine's waits, and the
    nine ids of `test_a_float_subclass_max_retry_after_is_judged_by_the_number_it_holds` and
    `test_a_numeric_max_retry_after_is_still_judged_floor_side` read the backoff from
    `_engine.BACKOFF_BASE`, which equals the deleted `_BACKOFF_BASE` (both `0.1`).
- `tests/test_sinks_http_url.py`'s two `sink._sleep_backoff = ...` assignments patch a deleted
  method. In its `_counted_at_one_attempt` helper the assignment records waits for the "takes no
  backoff" assertion, which would pass vacuously, so the helper records the engine's waits
  instead, through a `threading.Event` subclass set as the sink's stop signal that records each
  `wait`. In `test_an_invalid_url_failed_every_request_without_a_socket` the run's stop signal,
  set before `emit`, replaces it.
- `tests/test_sink_retry.py`'s interruptible-wait roster loses its `_sleep_backoff` branch.
  - Its derived module set is pinned to the set at the build's base minus `http`, so `http` leaving
    the set is asserted rather than silent.
  - The engine's round loop joins the check that a retry loop waits through `wait`.
- SPEC-070's loop roster in `tests/test_classify.py` (`_retryable_error` over every `for` loop on
  `range(...)` naming `retries`) goes from a floor of 12 to 11 (`_LOOP_FLOOR`). It gains a check
  that `HTTPSink.deliver`, `emit` and `_send` reach `_attempt` only through `_engine`. In the same
  file:
  - the floor's corpus test, `test_the_loop_roster_fails_eleven_conforming_loops_on_its_floor`,
    moves its two ids from 11 and 12 loops to 10 and 11, with its expected message;
  - `_LOOPS` loses its `HTTPSink` entry, whose `_http` builder records waits through an instance
    `_sleep_backoff`, so `test_the_sweep_covers_every_retry_loop_the_roster_finds` holds the
    eleven owners. The `[HTTPSink]` id of each of the four sweeps over `sorted(_LOOPS)` goes with
    it: `test_a_type_error_is_one_attempt_counted_as_exhaustion_counts`,
    `test_an_accepted_exception_is_attempted_max_retries_plus_one`,
    `test_a_keyboard_interrupt_propagates_and_moves_no_counter` and
    `test_a_predicate_that_raises_fails_at_once_naming_the_original`. Each of the four HTTP cases
    is re-expressed against `HTTPSink.emit` in `tests/test_http_rounds.py`, with its figures and
    the engine's waits;
  - the `_sentry` helper's `monkeypatch.setattr(sink._http, "_sleep_backoff", ...)` raises on a
    deleted method, so it patches the engine's wait instead. The four `SentrySink` tests that use it
    pass with their bodies unedited.
- `tests/test_deferral.py`: `HTTPSink`'s `DEFERRING_SITES` entry and its two `IMMEDIATE` entries move
  to the run context.
  - `DEFERRING_SITES` lists `_engine._commit` beside `_run`, the one commit of every run context.
  - A new shape check holds a run-context `count` of a dropped counter to `when=When.SETTLED`. It
    recognises the call by shape: an attribute call `count` that passes a `when=`, `owner=` or
    `name=` keyword, or three or more positional arguments. It takes the counter from `name=`,
    else the second positional, and fails closed on a name that is missing or not a string
    literal. Only a `SETTLED` spelt on `When` (bare, or through a module, `_engine.When`) is
    settled; a `.SETTLED` on any other base is a finding. A fixture corpus asserts each finding's
    text and the silence of a `Deferral.count` and of a forwarding ledger's two-argument count.
    So `dropped_oversized` and `_unrenderable` are still derived as dropped counters, committed
    only by a settled run.
  - No floor in that file or in `tests/test_worker_predicate_roster.py` is lowered.
- The OTLP truncated-`2xx` test that replaces `sink._handle_response` on the instance spies on
  `_outcomes` instead.
- **`emit`'s total-failure message** (FR-002) is the adapter's, so two `match=` arguments that
  quoted 1.x `HTTPSink.emit`'s own raise, `delivered none of <n> chunk`, now quote the adapter's
  `delivered none of <n> event`, with every other assertion unedited:
  `tests/test_sinks_http.py::test_every_chunk_failing_is_the_total_failure_the_worker_retries`
  (3 events) and
  `tests/test_sinks_elasticsearch.py::test_every_chunk_rejecting_every_item_is_a_total_failure`
  (4 events).
- `tests/test_encode.py`'s `derive`, the gate deriving which classes reach `json.dumps` from
  `emit`, follows a callable passed as a `deliver=` keyword as a call, since `HTTPSink.emit`
  reaches its round only as `emit_via_deliver(..., deliver=self._round)`. Without that the nine
  family classes drop out of its roster. One corpus case, an engine sink whose round reaches
  `json.dumps` unguarded, is added; the roster's floor is not lowered.
- `tests/test_declared_exceptions.py`'s `_shipped_readers` reads a reader of `_outcomes` as well
  as one of `_handle_response`, so row 4 keeps finding the four readers.
- One corpus row of `tests/test_sinks_http_header_collisions.py` planted its header write in the
  deleted `_sleep_backoff`; it plants it in `_round`, and its id becomes `in-round`.

The build runs the suite before editing any test and reports every failing id in the PR body. Each
id is either one named above or a test the build edits, and the PR body gives each edited test's
removed mechanism. A test that pinned a behaviour named in `docs/invariants.md` is not edited unless
it is named above, with its invariant still asserted.

**Prose and records, at completion.** The decisions below are rewritten in place, each with a
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
  - The fence's "a `2xx` settles its chunk (the body readers count it unadjudicated), a `503`
    retries, a `413` splits, a `400` is abandoned" becomes FR-001's outcome table.
  - The body's "anything escaping escapes `emit`, and the worker re-sends a batch the destination
    may have accepted" becomes "anything escaping a round would fail the engine's run, reporting as
    undelivered a batch the destination may have accepted, so the round guards every hook".
  - In *Rejected: `bytes | None`*, "lets `json.loads(None)`'s `TypeError` escape `emit`, so the batch
    is re-sent" becomes "makes the override raise on every unreadable body, which the round's guard
    then counts unadjudicated even where the override meant success".
  - In *Rejected: a keyword parameter `read_failed`*, the `TypeError` "out of `emit`" that "the
    worker re-sends" becomes a `TypeError` the round's guard counts unadjudicated on every
    unreadable body.
- Rewritten in place too, each with a dated History entry, at the sentence still stating the old
  mechanism:
  - *A protocol's retry set is a private predicate hook*: "read at `_send`'s one site" becomes "read
    at the one classification FR-001 describes", and its rejected per-sink retry set on the worker
    records that SPEC-073 made a `REJECTED` HTTP request final.
  - *A `dropped` count is written only by a `Deferral` commit*: `HTTPSink`'s sites now commit
    through the run context.
  - The completion's grep found five more decisions stating the old mechanism, each rewritten the
    same way: *A sink declares what it retries* (the twelve loops, `HTTPSink._send`'s among them,
    the floor of twelve, and "the worker still re-sends a raised batch"); *A populated partial
    success is counted, never retried* (a fully rejected chunk resent by the worker, counted once
    per attempt, immediately); *A Honeycomb batch response is a per-event verdict* (the same);
    *The sinks keep four declared exceptions* (row 2's inherited loop); and *The response is read
    under a bound* (`_send` abandoning an oversized non-`2xx`). *A sink may report per event*
    gains a History entry saying the HTTP family adopted it.
  - *A client exception costs its chunk* is **not** edited. Its "guarded *inside* `_send`" is the
    four AWS sinks' `_send`, not `HTTPSink._send`, and the decision names no HTTP sink, so this
    spec's earlier instruction to mark it was mis-aimed.
- `docs/architecture.md`'s statement that a `dropped` count is made only by an `emit` that
  returned gains a superseded marker for the HTTP family, and its open item on a `timeout` too large
  for a socket names `_exchange` where it named `HTTPSink._send`.
- The search state's scope is stated in the docstrings of `MAX_BUDGET_REDUCTIONS` ("one `emit` may
  make") and `_chunk_budget`, which become "one run". *A destination's limit is found by halving the
  budget* states no scope and is not edited.
- `docs/invariants.md`:
  - Entry 7's "only a `2xx` rejecting every record of every chunk raises there" gains "or, through
    `deliver`, fails the run".
  - Entry 7's recorded exceptions gain a batch whose every chunk's request cannot be built (a
    `_body`, `_prepare` or `_apply_auth` that raises, or a `Request` that cannot be built), beside
    SPEC-069's "a batch of nothing but events the sink cannot encode", which replaced the
    all-unrenderable HTTP batch.
  - Entry 2's list of deferring sites names the run context for `HTTPSink`.
- The README's HTTP-family retry paragraph, the docstrings of `HTTPSink`, `_send` and the three
  readers, and `docs/component-inventory.md`'s `http` row describe the round. The README's OTLP
  paragraph, which said a wholly rejected chunk engages the worker's retry, and its general
  statement that a sink that delivered nothing raises so the worker's retry engages, say what the
  engine does instead. The inventory's delivery-engine row names the run context.

#### Acceptance Criteria:

- [x] `tests/test_sink_losses.py` and `tests/test_sinks_http_body_read.py` pass, and every test in
      them not named above is unedited (`git diff` on each file touches only the named tests and
      the wait patches named above).
- [x] The named "pass unedited" tests in `tests/test_sinks_http_render.py` and
      `tests/test_sinks_otlp.py` pass with their files unedited at those tests.
- [x] Each rewritten test above asserts the figure stated for it. Each one whose figure changes
      fails at the build's base. A test whose only edit re-points a wait patch keeps its figures
      and need not.
- [x] Twin paths, for each of the nine shipped classes: the same scripted responses through a real
      `Worker` and through an orphan `info()` outside a span leave equal `losses()` after each, and
      equal request counts. The responses are `200`; `400`; `503` then `200`; an unreadable `2xx`;
      and an oversized event.
- [x] Twin forms, for each of the nine: `emit(batch)` and an orphan run through `deliver`, which
      have the same budget `s`, leave equal `losses()`, equal request bodies in the same order and
      equal stderr lines over the same responses.
- [x] The model test runs every seed with a family sink behind a scripted opener. No event is
      accepted twice, and every event is accepted or inside one counted term.
- [x] The two pinned rosters each fail when mutated once. Expecting `http` back in the
      retry-module set (dropping the pin's `- {"http"}`) reddens `tests/test_sink_retry.py`.
      Restoring a `for attempt` loop in `_send` reddens SPEC-070's roster.
- [x] Each decision named above carries its rewrite and a dated History entry. The build runs a
      `grep` and reports it: no file under `docs/` or in `README.md` still says `_send` retries the
      chunk path, or that the worker resends an HTTP batch that delivered nothing.
- [x] Serves invariants 2, 6 and 7:
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
    round: int                                             # read-only, 1-based, the round in progress
    def notes(self, owner: object) -> dict[str, object]: ...   # per run, per owner, across rounds
    def count(self, owner: _Counted, name: str, n: int, *, when: When) -> None: ...  # a zero is not kept
    def announce(self, line: Callable[[], None], *, when: When) -> None: ...
    def drops(self, owner: _Counted) -> _Drops: ...        # a Deferral-shaped view, all SETTLED
    # private: _begin(n) records the round; _committed(settled=, final=) selects what commits;
    # _close() clears it when its run ends; an immediate context commits each entry at once

class _Drops:                                              # count(counter, n=1), announce(line)
    ...

_RUNS: ContextVar[tuple[RunContext, ...]]                  # the runs on the calling context, innermost last

def run_context() -> RunContext: ...   # the innermost run on the calling context, or, outside any
                                       # run, an immediate context whose entries commit at once
def detached(call, failed) -> T: ...   # call as a one-round run of its own, pushed whatever else is
                                       # running, committed when it returns (settled unless failed)
def one_round(round_fn, batch) -> Verdict: ...   # a sink's deliver: inside a run, part of it;
                                                 # outside one, detached
def _commit(owners, lines) -> None: ...          # one Deferral per owner, then every line; listed
                                                 # in tests/test_deferral.py's DEFERRING_SITES
def _uncounted(fault, n, what) -> None: ...      # the absorbed line for a commit that raised

# _run pushes a RunContext for the whole run, records each round on it, commits it after the
# engine's own Deferral, and writes its settled line only when no sink line will commit.
# used from SPEC-072, not added here: run_delivery(..., worker_retries=w | None); the budget table
# of FR-001; emit_via_deliver(sink, batch, *, deliver=None) (public since PR 312 as
# log_foundry.emit_via_deliver, a wrapper over this one); _run(sink, deliver, counters, batch,
# *, sink_retries, worker_retries, stop, decline); outcomes_of; the static lookup that offers only
# a plain function.


# src/log_foundry/sinks/http.py
class _Pending(Protocol):                                  # what _items and _drop_refused take as
    def count(self, counter: str, n: int = 1) -> None: ...  # pending: a Deferral or RunContext.drops
    def announce(self, line: Callable[[], None]) -> None: ...

@dataclass(frozen=True)
class _Answer:                                             # _exchange's classification of one request
    outcome: Outcome | None                                # None: a 413 the caller may split
    payload: bytes; retry_after: float | None; detail: str; error: Exception | None

@dataclass
class _Sent:                                               # what _send's one-request rounds leave
    answer: _Answer | None; unbuilt: Exception | None

class HTTPSink:
    log_foundry_counters: SinkCounters                     # SPEC-072's event-unit totals
    failed: int                                            # requests, as in 1.x
    dropped_oversized: int                                 # events, SETTLED, as in 1.x
    dropped_unadjudicated: int                             # new on the base; 0 unless a reader gives up
    _READS_BOOL: ClassVar[bool]                            # written on every subclass: read 2xx via the bool
    _DELIVER_WITHDRAWN: ClassVar[bool]                     # this class's deliver = None is the base's

    def __init_subclass__(cls, **kwargs: Any) -> None: ...      # FR-004's eligibility
    def deliver(self, batch: list[dict[str, object]]) -> Verdict: ...   # _engine.one_round(self._round, ...)
                                                           # class attribute None when ineligible
    def emit(self, batch: list[dict[str, object]]) -> None: ...         # the engine inline
    def _round(self, batch: list[dict[str, object]]) -> Verdict: ...
    def _exchange(self, request, *, splittable: bool, count_unexpected: bool) -> _Answer: ...
                                                           # FR-001's one classification
    def _give_up(self, context: RunContext, reason: str, when: When) -> str: ...  # failed + its line
    def _read(self, payload: bytes, chunk: list[_Item]) -> tuple[Outcome, ...]: ...  # the reader guard
    def _outcomes(self, payload: bytes, items: list[_Item]) -> tuple[Outcome, ...]: ...
    def _unadjudicated(self, items: list[_Item], detail: str, noun: str = "event"
                       ) -> tuple[Outcome, ...]: ...       # the one unreadable-response method
    def _abandon_unreduced(self, chunk: list[_Item]) -> None: ...  # unchanged; reads run_context()
    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool: ...   # unchanged
    def _send(self, body: bytes, *, content_type: str,
              extra_headers: dict[str, str] | None = None,
              splittable: bool = False,
              count_unexpected: bool = True) -> bytes: ... # 1.x contract (SPEC-070's keyword);
                                                           # its rounds run through _engine._run
    # deleted: _BACKOFF_BASE, _sleep_backoff, _deliver, _abandon, _send's for-attempt loop

# module helpers: _wholly_rejected(outcomes) and _read_as_1x(reader), the readers' bool shim;
# _positions(batch, items, dropped), the item-to-index mapping: a forward-only identity walk, then
# unclaimed indices in order, each unplaced event DROPPED or REJECTED by the drop count; _Tally, a
# _Pending that forwards every drop _items records and counts them; _settle.
```

## API / Interface Contract

This spec adds no public name. `deliver` is the optional member SPEC-072 defines, and it now
appears on the nine shipped classes. The adapter it runs through, `emit_via_deliver`, became public
in PR 312, not here; `HTTPSink.emit` calls the private `_engine.emit_via_deliver`. A third-party subclass that overrides one of the two 1.x hooks keeps its
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
├── _engine.py          # + When, RunContext, _Drops, run_context(), detached(), one_round(), _commit()
├── http.py             # deliver/_round/_exchange/_outcomes/_unadjudicated/__init_subclass__; emit and _send on the engine
├── elasticsearch.py    # _outcomes; _handle_response as the 1.x shim; no _abandon_unadjudicated
├── honeycomb.py        # likewise
├── otlp.py             # likewise
└── sentry.py           # one docstring: _send, not the deleted _abandon, counts what it raises

tests/
├── test_http_rounds.py            # new: FR-001 to FR-004 and FR-005's twins against scripted openers
├── test_run_context.py            # new: the run context against scripted test sinks (FR-002)
├── test_sinks_http_body_read.py   # _BODY_READERS beside the derived _outcomes set
├── test_sink_retry.py             # pinned module set; _sleep_backoff branch removed
├── test_deferral.py               # HTTPSink's sites moved to the run context; the SETTLED shape check
├── test_classify.py               # SPEC-070's loop roster: floor 11, the engine-only check,
│                                  #   no HTTPSink in _LOOPS, _sentry on the engine's wait
├── test_sinks_{honeycomb,otlp,http_refused,http_url}.py   # the named rewrites
├── test_sinks_{http,elasticsearch}.py   # one match= each: the adapter's total-failure message
├── test_encode.py                 # derive follows a deliver= callable; one corpus case
├── test_declared_exceptions.py    # row 4's readers include the _outcomes definers
├── test_sinks_http_header_collisions.py   # one corpus row planted in _round
├── test_numeric_doors.py          # the Retry-After waits read from the engine
├── (eleven files' _no_sleep fixtures, and test_sink_losses.py's)   # the engine's wait (FR-005)
└── test_invariants_model.py       # + an HTTPSink behind a scripted opener
```

## Implementation Phases

### Phase 1: The run context

- Add `When`, `RunContext` and `run_context()` to `_engine.py`, and test them against scripted test
  sinks: rounds, notes, the three `when` values, a detached context, a nested run, `BaseException`,
  two threads and a fork.

### Phase 2: The round

- Write `HTTPSink._round`, `deliver`, the classification table, the `413` search in the run's notes,
  the hint and the hook guards. `__init_subclass__`'s eligibility and the base `_outcomes` and
  `_unadjudicated` land here, beside the first definition of `deliver`, so no commit offers
  `deliver` to a bool reader; the three shipped readers stay ineligible until Phase 3.
- Move `emit` and `_send` onto the engine, keeping `count_unexpected`, and delete `_BACKOFF_BASE`,
  `_sleep_backoff`, `_deliver` and the loop.
- Run the suite and record every failing id, then rewrite the retry and deferral rosters and the
  named tests in the same commit as the mechanism they pin.

### Phase 3: The readers

- Move the three readers onto `_outcomes` and `_unadjudicated`, keep each `_handle_response` as the shim, and derive
  `_BODY_READERS` beside `_outcomes`.

### Phase 4: Overrides and both paths

- Add the third-party fixtures that hold `__init_subclass__`'s eligibility.
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
- 2026-11-08: re-derived against `c4e51fbd`. Every PR from #293 to #311 has landed since
  `fda134a`, SPEC-066, SPEC-069 to SPEC-072 and PR 311 among them.
  - FR-005's Probe D list rested on the opener `RuntimeError` SPEC-070 removed. It is rebuilt on
    faults in the opener's script: a `400` for the "down" tests, so the run still fails, and a
    `URLError` for the "once" tests, so it is still resent. Under this design SPEC-070's raising
    reader would make its chunk `UNKNOWN` and settle the run. The list now also names
    `test_a_fanout_absorbs_the_probe_d_batch` and the opener-exception test, which this design
    breaks too. The subclass sweep has 8 ids, not 9; it holds the shipped subclasses only and had
    8 at `fda134a` as well. The count is 14 ids in six functions, collected with
    `pytest --collect-only`.
  - The Worker-test figure moved from 13 to 15 files: SPEC-066 and SPEC-070 each added one that
    names a family class beside a `Worker` it builds around another sink.
  - FR-005 gains the test sites that patch what this spec deletes and were not named: twelve files
    patching `log_foundry.sinks.http.wait` (six of them at `fda134a`, six added since by SPEC-066
    and PR 311), `tests/test_numeric_doors.py`'s two readers of `_BACKOFF_BASE`, a second
    `_sleep_backoff` site in `tests/test_sinks_http_url.py`, and SPEC-070's `tests/test_classify.py`:
    the `HTTPSink` entry of `_LOOPS`, the floor's corpus test and the `_sentry` helper. FR-001's
    "pass unedited" criteria now say "bodies unedited" and name those helper edits.
  - PR 311 moved request building: `_apply_auth` writes the `Authorization` value built once at
    construction. FR-001 now says each chunk's request is built as `_send` builds it at the
    build's base, names PR 311's `extra_headers` scan, which reads every `_prepare` call, and
    holds PR 311's header guards to passing with their bodies unedited, under invariant 13. Out of
    Scope's list of kept private methods means their meaning at the build's base.
  - FR-001's `count_unexpected=False` paragraph gains SPEC-070's exception for a
    `SinkDeliveryError`. FR-005 quotes the second decision's fence as
    SPEC-070 left it. FR-004 says which instance spy moves and which goes. FR-005's "fails at the
    build's base" criterion now applies to the rewrites whose figures change, since a re-pointed
    wait patch keeps its figures.
  - Decided by the orchestrator: the round's guard extends to `_prepare`, `_apply_auth` and
    building the `Request`, and a raise from any of them is a `_body` fault, `DROPPED` on
    `_unrenderable` with the same line, since nothing was sent. "A `deliver` of this family never
    raises an `Exception`" covers those sites too. FR-001 gains the `_prepare` criterion, FR-002's
    move 3 and FR-004's departures name the new hooks, and the `dropped` table row says so.
- 2026-11-08: revised to match the build (`391fdd04` to `1ed802b0`, as rebased onto `458d79e6`), at
  completion. Each item is
  what the build decided or found, and the text above now says it:
  - S1: SPEC-072's `_run` did not carry FR-003's "no engine settled line beside a sink line"
    refinement, since it had no context to read. This spec adds it, keyed on lines that will
    commit rather than lines registered; FR-002 said it relied on it.
  - S2: a reader's `_handle_response` shim returns `False` exactly when the chunk is non-empty and
    every outcome is `REJECTED`, and it runs `_outcomes` in a context of its own, pushed whatever
    else is running.
  - S3 and the line texts: a request-building fault aggregates per round into one `_unrenderable`
    count and one line, `<class>, rendering raised <Type>`, naming the first type; a raising reader
    writes `<class> reading a 2xx response raised <Type>; they may have landed; not retried`; a
    malformed reader result writes `reading a 2xx response gave no outcome per event`.
  - S6: `run_context()` outside any run returns an immediate context that commits each entry at
    once. S9: `_send`'s runs count into a throwaway `SinkCounters`. S10: a third-party
    `_retryable_status` accepting `413` keeps 1.x's order, so the retry wins. S11 and S12: the
    Data Model gains the private additions, `RunContext.drops(owner)`, the `_Pending` Protocol,
    `_READS_BOOL`, `_DELIVER_WITHDRAWN`, `_exchange`, `_give_up`, `_read`, `_Answer`, `_Sent`,
    `detached`, `one_round` and `_commit`.
  - FR-004's eligibility, narrowed by the plan review: `__init_subclass__` restores `deliver` only
    over a `None` the base itself assigned, so a third-party `deliver = None` opt-out and its plain
    children stay not offered; and the slice ends at the nearest ancestor whose own body defines a
    non-`None` `deliver`, so a plain child of a third-party engine-native subclass keeps it.
    Criteria 4.2 and 4.3 gain an orphan-path case each, and 4.5 the opt-out, the engine-native
    child and an `emit` override beside a bool reader.
  - N1 and N2: `emit`'s total-failure message is the adapter's `delivered none of <n> event(s):
    ...`, and two `match=` arguments were edited for it; FR-005 names them, and criteria 1.13 and
    1.16 account for them. N3: `tests/test_encode.py`'s `derive` follows a callable passed as
    `deliver=`. FR-005 also names the `tests/test_declared_exceptions.py` reader set and the
    `tests/test_sinks_http_header_collisions.py` corpus row the build edited.
  - FR-005's wait-patch list gains its thirteenth site, `tests/test_sink_losses.py`'s `_no_sleep`,
    an f-string the grep instrument could not see; every patch targets `log_foundry.sinks._retry.wait`.
  - FR-001's family scan finds 8 modules, `OpenSearchSink` sharing `elasticsearch.py`; criterion
    1.1 says so. `tests/test_run_context.py` is a new file beside `tests/test_http_rounds.py`.
  - Found at completion, each stated where it applies rather than left implied:
    `_abandon_unreduced` stays with its 1.x signature and reads the run's context itself, so a
    third-party override written against 1.x is still called; the hook guard covers the request-building hooks
    and the `2xx` readers only, so a raising third-party override of another private method
    (`_retryable_status`, `_items`, `_take` and the rest FR-001 lists) escapes the round as it escaped 1.x `emit`; and *A client exception costs its chunk*, which FR-005 told
    the completion to mark, is about the AWS sinks' `_send` and is left as it stands.
  - Criterion 2.9 is narrowed to the five twin-path responses the build measures it over, where it
    said "after each FR-001 case".
  - FR-004's mechanism and the base `_outcomes` and `_unadjudicated` were built in Phase 2, beside
    the first `deliver`, rather than in Phases 3 and 4, so that no commit offered `deliver` to a
    bool reader. The Phase 2 commit was knowingly red on four ids of
    `test_every_http_subclass_counts_no_413_drop_when_down`, the `ElasticsearchSink`,
    `OpenSearchSink`, `HoneycombSink` and `OTLPSink` ids, whose readers were still ineligible, and
    green from Phase 3.
- 2026-11-08: revised on the frame-1 diff review (`d73378c4`, rebased as `9892c6a6`), and on
  PR 312, which landed on `main` as `458d79e6` while this spec was in review:
  - The round maps items to events by identity first and then by unclaimed position, so an `_items`
    override returning rebuilt events maps exactly; an event no request carries is `DROPPED` only
    when their number equals what `_items` dropped and counted, else `REJECTED`. FR-001 says so
    and gains a criterion for an enriching override through a real `Worker` and one that omits an
    event.
  - Only a raising reader gets the `raised <Type>` line; a result that is not one `Outcome` per
    event (not a sequence, the wrong length, or another element type) gets `gave no outcome per
    event`. FR-001 says so and gains its criterion.
  - Criterion 1.12's second clause is the test that pins the per-run budget (two adjacent `503`s,
    resent as two requests with no `413`). New criteria pin the smallest refused size and the
    reductions already spent (27 requests, `failed == 2`), the unreduced chunk on both the base and
    a 1.x-signature override, a third-party `_retryable_status` accepting `413`, `_send`
    re-raising a raise building its `Request`, the request-building line naming the round's first
    exception, an unreduced chunk counted `ALWAYS` beside a later-landing one, a shim committing at
    once inside a round, a plain child of an engine-native parent that has its own `emit`, and a
    permanent bool reader announced by the engine's line. Every one is ticked against its test in
    `tests/test_http_rounds.py`. The FR count stays five.
  - PR 312 made `emit_via_deliver` public as a wrapper over the private `_engine` one. This spec
    still adds no public name; the text that said "no public name is added" now says that, and
    that `HTTPSink.emit` calls the private adapter. FR-005's wait-patch count is sixteen: PR 312's
    three new files patched `log_foundry.sinks.http.wait` too.
- 2026-11-08: revised on the frame-2 diff review (`3ab93aa3`):
  - `_send` caught only the `Request` build, so a raising `_retryable_status` escaped into the
    engine as a raised round and `_send` then raised `SinkDeliveryError("<class> made no
    attempt")`, which `SentrySink` reads as already counted, though nothing was. It now catches the
    classification too and re-raises the same object after the run, uncounted, as 1.x let it
    escape. FR-001's `_send` paragraph says so and gains its criterion,
    `test_send_reraises_a_classifying_hooks_fault_unchanged`, ticked.
  - A raising `_response_limit` is recorded as an edge rather than changed: `_attempt` reads it
    first, so it is the attempt's refused exception, counted in `failed`, in the round and in
    `_send` alike, even for a response that was a `2xx`. In 1.x `_send` read it once before the
    first request and the raise escaped `emit` raw. FR-001's guard paragraph lists exactly the
    guarded hooks and the escaping ones, and names `_response_limit` as the one that is classified
    rather than escaping, which earlier text here listed among the escaping ones.
  - The deferral gate recognised a run-context `count` only by three positionals, so a keyword
    form passed it. FR-005 describes the widened shape.
