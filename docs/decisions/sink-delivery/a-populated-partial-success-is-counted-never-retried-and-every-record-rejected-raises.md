---
type: Decision
title: "A populated partial success is counted, never retried; every record rejected raises"
description: "a `2xx` whose `partialSuccess.rejectedLogRecords` is in `1 … 2**63-1` counts on `rejected_records`, clamped to the chunk, and the chunk is not re-sent, since a retry would re-send what landed; a chunk whose every record was rejected is not delivered, so an `emit` with no delivered chunk raises — invariant 7 outranks OTLP's no-retry rule, and `failed_batches` is how permanent rejection is seen. The line never carries `errorMessage`. (SPEC-056)"
---

**A populated partial success is counted, never retried; every record rejected raises** — OTLP
answers `200` for full success and for partial success alike, the difference being a
`partialSuccess` object in the body with `rejectedLogRecords` (an `int64`, written by OTLP/JSON as
a decimal string and accepted as a number) and an `errorMessage`. `OTLPSink._handle_response` sorts
a `2xx` body into three outcomes and never raises on any bytes, because an exception escaping it
escapes `emit` and the worker then re-sends chunks that already landed. **Full success**: a body
that is empty, not JSON, not an object, or without a `partialSuccess` — what a server must send on
success — and a count of `0`, absent or `null`, which OTLP uses to carry a warning. Nothing is
counted or written. **Rejected**: a count in `1 … 2**63 - 1`, as a digit string or a bare number,
added to `rejected_records` clamped to the chunk's size so a confused server cannot inflate `failed`
past what was sent, announced once as `lost N log record(s)` naming the class, and **not retried**:
the records that landed would be re-sent. **Unadjudicated** (its own decision, *A `2xx` this sink
cannot read is counted*). A chunk with some records accepted returns delivered; one whose every
record was rejected returns not-delivered, so an `emit` none of whose chunks was delivered raises
`SinkDeliveryError` and the worker's retry engages. That retry re-sends records the server already
refused, which OTLP's rule does not permit; it cannot duplicate anything, because nothing landed,
it is bounded by the worker's `max_retries`, and `failed_batches` moving is how an operator sees a
permanent rejection at all — **invariant 7 outranks OTLP's no-retry rule here.** The consequence is
the one invariant 2 records for `failed`: each worker attempt adds the chunk's size again. **The
line never carries `errorMessage`**: it is the server's text, free to repeat an attribute the caller
logged, and invariant 11 names a type and never a value. **The body is parsed with
`parse_int=str`.** A bare 5,000-digit count makes `json.loads` raise `ValueError` ("Exceeds the
limit (4300 digits)"), which would read as not-JSON and settle as full success while its string twin
is unadjudicated; handing every bare integer over as its digit string gives both spellings one rule.
Both counters are immediate under `_counter_lock` and feed `failed` only — never `dropped`, which
the derived gate in `tests/test_deferral.py` reserves for a `Deferral` commit. (SPEC-056 FR-004)

## History

### 2026-11-03

- **Settled.** Shipped with `OTLPSink`.
