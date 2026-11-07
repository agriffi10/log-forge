---
type: Decision
title: "A protocol's retry set is a private predicate hook, not a second loop"
description: "`HTTPSink._retryable_status(status)` is read at `_send`'s one site and defaults to `429`/`5xx`; `OTLPSink` narrows it to `429`, `502`, `503`, `504`, so a `500` is abandoned on the first request and every other sink is unchanged by construction. A `413` still halves the budget and re-sends: nothing landed. (SPEC-056)"
---

**A protocol's retry set is a private predicate hook, not a second loop** — OTLP names exactly
`429`, `502`, `503` and `504` retryable and forbids retrying anything else, where the HTTP family
retries `429` and every `5xx`. The family's condition moved out of `_send`'s literal expression into
`HTTPSink._retryable_status(status) -> bool`, read at that one site, so `OTLPSink` overrides four
statuses and inherits the loop, the backoff, `Retry-After`, the counting and the raise untouched; a
`500` is abandoned on the first request, counted, announced and raised like a `400`. An AST roster
holds `OTLPSink` as the only subclass defining the hook, and a sibling sweep holds every other sink
to retrying a `500` and a `501`. **Rejected: a per-sink retry set on the worker.** `Worker._emit`
re-sends a batch whenever `emit` raises, whatever the status, so a `400` is still offered
`max_retries + 1` times by the pipeline above the sink; teaching the worker a per-sink "do not retry"
is a change to every sink's contract, not to this one, and stays out of scope. **The exception hook
is the same shape** (SPEC-070): every retry loop in the sinks, `HTTPSink._send`'s included, asks a
private `_retryable_error(err)` beside this status hook, through `is_retryable` so a predicate that
raises reads as "not retryable" (*A sink declares what it retries, and anything else fails at
once*). `OTLPSink` overrides only the status hook; the roster of *The sinks keep four declared
exceptions to their shared rules* holds it as the one class that does. **Kept on purpose: the
`413` search.** OTLP's rule says nothing outside the set is retried; the base class halves the byte
budget on a `413` and re-sends the same records in smaller requests (*A destination's limit is found
by halving the budget*). It is kept because nothing landed — a `413` precedes ingestion — and because
OTLP's own server rule for an oversized request is to answer `413`, which is exactly the answer the
search exists to act on. (SPEC-056 FR-003)

## History

### 2026-11-07

- **Refined** by SPEC-070: `_retryable_error` joins `_retryable_status` as the exception half of the
  same private-predicate shape, and the `_retryable_status` roster becomes row 2 of the declared
  exceptions table.

### 2026-11-03

- **Settled.** Shipped with `OTLPSink`; the hook's default is the literal expression it replaced.
