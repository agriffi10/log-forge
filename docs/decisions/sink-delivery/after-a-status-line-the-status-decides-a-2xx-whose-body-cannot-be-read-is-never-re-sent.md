---
type: Decision
title: "After a status line the status decides; a 2xx whose body cannot be read is never re-sent"
description: "before a status line, `URLError`, `OSError` and `http.client.HTTPException` are `RETRYABLE`, resent by the engine within its budget, and counted in `failed` for the last round's request; after one, a body read that fails is an `_UnreadBody`, equal to `b\"\"`, and the status decides — a `2xx` is what its reader returns (`UNKNOWN` for a body reader that cannot read it), a retryable status is `RETRYABLE`, a `413` drives the search, any other is `REJECTED`. `_handle_response` keeps `bytes`; `_read_failed` tells an unreadable body from an empty one. (SPEC-073)"
---

**After a status line the status decides; a 2xx whose body cannot be read is never re-sent** —
`HTTPSink._attempt` used to read the body with a bare `response.read()` once the status was known,
while `_send` caught only `(URLError, OSError)`. Measured against a real socket with
`max_batch_count=2` and four events: a `2xx` whose `Content-Length` body arrived short raised
`http.client.IncompleteRead` out of `emit`, so chunk 2 was never sent, and after a delivered chunk
the worker's retry re-sent it (through a real `Worker`, `failed_batches=1` and the first chunk
POSTed again on every retry); a body that stalled past `timeout` or was reset mid-read landed in
`_send`'s connection-error retry, so `max_retries=2` re-POSTed each accepted chunk three times and
then counted it in `failed`; and a garbage status line raised `BadStatusLine` raw, an
`HTTPException` that is not an `OSError`. A `2xx` status line is not provable non-delivery, which is
the only thing SPEC-018 lets a sink re-send, and invariant 2 forbids the chunk the raise skipped.

**The status line is the dividing line.** Before it, nothing the server said can be relied on, so a
connection error, a refused or garbled status line and an unparseable header block are retried and
then abandoned with the existing `connection error, <Type>` line, counted in `failed`; any other
exception there fails at its first attempt through the same line (*A sink declares what it retries,
and anything else fails at once*, SPEC-070). After it, the body is read under its own guard
catching `Exception` (never `BaseException`), for the reason `ElasticsearchSink`'s JSON guard
gives: anything escaping a round would fail the engine's run, reporting as undelivered a batch the
destination may have accepted, so the round guards every hook. A failed read yields an `_UnreadBody`, a private `bytes` subclass equal to `b""`, and the
status and its `Retry-After` decide exactly as they would have with a body. So the change reaches non-`2xx` statuses
too: a `503` whose body is cut short is retried and reported as `HTTP 503`, a `413` still drives the
budget search, and a `400` is abandoned once, where each used to raise out of `emit` on a short body
and be retried as a connection error on a stall or reset.

**`b""`, not `None`.** `_handle_response` is a documented extension hook — the `1.0.0` release
notes tell a subclass to extend through `_render`, `_body` and `_handle_response`, and the public
surface test calls the three unrestricted — so its signature is held under `1.x` like a public one,
and this decision keeps it `bytes`. The premise this corrects was that the hook was private and its
type could be widened freely.

**An override can tell an empty `2xx` body from an unreadable one, by asking** (SPEC-070 FR-004).
The unreadable one is an `_UnreadBody`, and `_read_failed(payload)` answers whether a payload is
one; an honestly empty body stays plain `b""`. The flag travels in the value the hook already
receives, so `_handle_response`'s signature is unchanged. The three shipped readers ask it first and
count an unreadable `2xx` body unadjudicated: `ElasticsearchSink` (and `OpenSearchSink`) and
`HoneycombSink`, whose reading no longer rests on `json.loads(b"")` raising, and `OTLPSink`, which
reads an **empty** body as success, since an OTLP success needs no body, and until SPEC-070 settled
an unreadable one silently the same way. **The residue** is a third-party override that does not ask:
it sees `b""` for both, as under `1.x`, and reads them alike. The base class ignores the body and
settles both as delivered, because no body is its verdict.

**`read()` with no size, on purpose.** CPython's `read()` raises `IncompleteRead` on a short
`Content-Length` body and on a truncated chunked one, but `read(amt)` returns a short
`Content-Length` body silently with `response.length` still above zero (measured on 3.12.12 and
3.13.1); a body with neither length nor chunking is read to EOF, where truncation is undetectable. A
bounded read must check `response.length` itself, or a short body reads as a complete one.

**A header block that fails to parse is the edge of the rule.** `LineTooLong` and "got more than 100
headers" are raised after a status line `urllib` never hands back, which may have been a `2xx`. They
are retried as pre-status errors, on the expected-cost reasoning *A client exception costs its
chunk, and is provable non-delivery for it* records for a read timeout: the status cannot be seen,
and a destination sending malformed headers on a success is pathological.

**Rejected: retrying after a `2xx`.** That is the defect: a chunk the destination accepted is sent
again, once per retry. **Rejected: `bytes | None`.** Passing `None` for an unreadable body breaks a
third-party override at type-check time, and at runtime an override whose JSON guard catches only
`ValueError` makes the override raise on every unreadable body, which the round's guard then counts
unadjudicated even where the override meant success. **Rejected: a separate `_handle_unreadable` hook.** It
would be one more extension surface held under `1.x` that cannot be removed; the flag in the payload
gives the readers that need the difference what they need without one. **Rejected: a keyword
parameter `read_failed: bool = False` on `_handle_response`** (SPEC-070). A third-party override
with the two-argument signature, or a `*args, **kwargs` wrapper forwarding to one, would raise
`TypeError` exactly when a body fails to read, which the round's guard counts unadjudicated on
every unreadable body even where the override meant success (before SPEC-073 the raise escaped
`emit` and the worker re-sent the batch, the defect this decision exists to prevent). **Rejected: `read(amt)`.** It hides a
short body (above).

**Do NOT build** a retry on a failed body read after a `2xx`, or a body read outside the guard.
**`SentrySink`** reads no body, so an unreadable `2xx` envelope counts as sent, and a pre-status
protocol error is now `HTTPSink`'s counted abandonment rather than one of its `transport_errors`.

## History

### 2026-11-08

- **Refined** by SPEC-073: the fence's outcomes are FR-001's table. A pre-status transport error
  is `RETRYABLE`, resent by the engine and counted in `failed` only for the last round's request;
  a `2xx` is what its reader's `_outcomes` returns, `UNKNOWN` for one it cannot read. A hook that
  raises no longer escapes `emit` to a worker resend: the round guards it, so the two rejected
  alternatives' cost is now a chunk counted unadjudicated rather than a batch re-sent.
  *Never re-sent* is unchanged.

### 2026-11-07

- **Applied to `deliver`** by SPEC-072 FR-004, fence unchanged: the engine makes this decision general. A verdict marking an event `UNKNOWN`, or a verdict the engine cannot read (`outcomes_of` refuses it: not a `Verdict`, outcomes not exact `Outcome` members, the wrong length, or a field that raises), means `deliver` returned and the events may have landed, so the run settles with them counted in `failed_events`, `flush()` stays true, and they are resent only by a sink declaring `log_foundry_idempotent = True` — which no shipped sink does.
- **Refined** by SPEC-070 FR-004: an unreadable `2xx` body is an `_UnreadBody`, equal to `b""`, and
  `_read_failed` tells it from an empty one, so the residue shrinks to third-party overrides that do
  not ask; `OTLPSink` now counts an unreadable body. The keyword-parameter alternative is recorded
  as rejected. Before a status line, an exception outside the three classes is now counted at its
  first attempt rather than escaping `emit`. *Never re-send* is unchanged.

### 2026-11-03

- **Refined.** `HoneycombSink` now reads its per-event array and counts `b""`, closing the §12 item
  this entry named as open.
- **Settled.** A short, stalled or reset `2xx` body raised out of `emit` or was re-sent, and a
  garbage status line escaped raw; the status line now divides retry from settle.
