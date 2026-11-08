---
type: Decision
title: "A populated partial success is counted, never retried; every record rejected raises"
description: "a `2xx` whose `partialSuccess.rejectedLogRecords` is in `1 … 2**63-1` counts on `rejected_records`, clamped to the chunk, and the chunk is not re-sent, since a retry would re-send what landed; a chunk whose every record was rejected is `REJECTED`, so a run with no accepted record fails — `emit` raises, `failed_batches` moves, and since SPEC-073 nothing is re-sent, so OTLP's no-retry rule holds too. The line never carries `errorMessage`. (SPEC-056, SPEC-073)"
---

**A populated partial success is counted, never retried; every record rejected raises** — OTLP
answers `200` for full success and for partial success alike, the difference being a
`partialSuccess` object in the body with `rejectedLogRecords` (an `int64`, written by OTLP/JSON as
a decimal string and accepted as a number) and an `errorMessage`. `OTLPSink._outcomes` (its
`_handle_response` since SPEC-073 a 1.x shim over it) sorts a `2xx` body into three outcomes and
never raises on any bytes; a raise would cost its chunk through the round's guard, counted
unadjudicated, where before SPEC-073 it escaped `emit` and the worker re-sent chunks that already
landed. **Full success**: a body
that is empty, not JSON, not an object, or without a `partialSuccess` — what a server must send on
success — and a count of `0`, absent or `null`, which OTLP uses to carry a warning. Nothing is
counted or written. **Rejected**: a count in `1 … 2**63 - 1`, as a digit string or a bare number,
added to `rejected_records` clamped to the chunk's size so a confused server cannot inflate `failed`
past what was sent, announced once as `lost N log record(s)` naming the class, and **not retried**:
the records that landed would be re-sent. **Unadjudicated** (its own decision, *A `2xx` this sink
cannot read is counted*). The response gives a count, not an identity, so the chunk's first *r*
events are `REJECTED` and the rest `ACCEPTED`; the positions carry no claim about which record the
server refused. A chunk with some records accepted delivered something; one whose every record was
rejected is wholly `REJECTED`, so a run none of whose records was accepted fails: `emit` raises
`SinkDeliveryError`, and on the worker `failed_batches` moves, which is how an operator sees a
permanent rejection at all (invariant 7). **Since SPEC-073 that failure is not re-sent**: the
worker calls the sink's `deliver`, and a `REJECTED` record is final, so OTLP's no-retry rule now
holds as well, and `rejected_records` counts the chunk once. Before it, the worker's retry
re-sent the refused records — invariant 7 outranked OTLP's rule then — and each worker attempt
added the chunk's size again. **The
line never carries `errorMessage`**: it is the server's text, free to repeat an attribute the caller
logged, and invariant 11 names a type and never a value. **The body is parsed with
`parse_int=str`.** A bare 5,000-digit count makes `json.loads` raise `ValueError` ("Exceeds the
limit (4300 digits)"), which would read as not-JSON and settle as full success while its string twin
is unadjudicated; handing every bare integer over as its digit string gives both spellings one rule.
Both counters are registered `ALWAYS` on the engine's run context and committed when the run ends,
under `_counter_lock`, and feed `failed` only — never `dropped`, which the derived gate in
`tests/test_deferral.py` reserves for a settled commit. (SPEC-056 FR-004)

## History

### 2026-11-08

- **Refined** by SPEC-073: the reader is `OTLPSink._outcomes`, positional by count, and a chunk
  whose every record was rejected is `REJECTED`, so a run with none accepted fails and is not
  re-sent: `rejected_records` counts it once, where each worker attempt used to add it again, and
  OTLP's no-retry rule no longer has to yield to invariant 7. The counters commit when the run
  ends rather than at once.

### 2026-11-03

- **Settled.** Shipped with `OTLPSink`.
