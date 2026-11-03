---
type: Decision
title: "After a status line the status decides; a 2xx whose body cannot be read is never re-sent"
description: "before a status line, `URLError`, `OSError` and `http.client.HTTPException` are retried, then counted in `failed`; after one, a body read that fails is `b\"\"` and the status decides as it always did — a `2xx` settles its chunk (`_bulk` counts it unadjudicated), a `503` retries, a `413` splits, a `400` is abandoned. `_handle_response` keeps `bytes`, so it cannot tell an empty body from an unreadable one."
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
then abandoned with the existing `connection error, <Type>` line, counted in `failed`. After it, the
body is read under its own guard catching `Exception` (never `BaseException`), for the reason
`ElasticsearchSink`'s JSON guard gives: anything escaping escapes `emit`, and the worker re-sends a
batch the destination may have accepted. A failed read yields `b""`, and the status and its
`Retry-After` decide exactly as they would have with a body. So the change reaches non-`2xx` statuses
too: a `503` whose body is cut short is retried and reported as `HTTP 503`, a `413` still drives the
budget search, and a `400` is abandoned once, where each used to raise out of `emit` on a short body
and be retried as a connection error on a stall or reset.

**`b""`, not `None`.** `_handle_response` is a documented extension hook — the `1.0.0` release
notes tell a subclass to extend through `_render`, `_body` and `_handle_response`, and the public
surface test calls the three unrestricted — so its signature is held under `1.x` like a public one,
and this decision keeps it `bytes`. The premise this corrects was that the hook was private and its
type could be widened freely.

**The residue: an override cannot tell an empty `2xx` body from an unreadable one.** It is handed
`b""` for both, so the way a subclass reads an empty body is the way it reads an unreadable one. A
subclass that reads an empty body as success settles an unreadable one silently. That is what the base
class does, since it ignores the body, and it is correct for OTLP's reading of an empty body as
success, since the status there is the verdict. It is not a claim that every body-ignoring subclass
is right to ignore the body: `HoneycombSink`'s `2xx` carries a per-event array it never reads, which
`docs/architecture.md` §12 records as open. It is also why `ElasticsearchSink` reading an empty `2xx`
as unadjudicated is the right reading for `_bulk`: `json.loads(b"")` raises, so an unreadable body
takes *A 2xx _bulk response with no readable verdict is counted, never retried* with no branch of its
own.

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
`ValueError` lets `json.loads(None)`'s `TypeError` escape `emit`, so the batch is re-sent — the
defect again, moved into the caller's code. **Rejected: a separate `_handle_unreadable` hook.** It
would be one more extension surface held under `1.x` that cannot be removed, and nothing needs to
treat an unreadable body differently from an empty one today. **Rejected: `read(amt)`.** It hides a
short body (above).

**Do NOT build** a retry on a failed body read after a `2xx`, or a body read outside the guard.
**`SentrySink`** reads no body, so an unreadable `2xx` envelope counts as sent, and a pre-status
protocol error is now `HTTPSink`'s counted abandonment rather than one of its `transport_errors`.

## History

### 2026-11-03

- **Settled.** A short, stalled or reset `2xx` body raised out of `emit` or was re-sent, and a
  garbage status line escaped raw; the status line now divides retry from settle.
