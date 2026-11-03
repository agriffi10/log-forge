---
type: Decision
title: "A 2xx _bulk response with no readable verdict is counted, never retried"
description: "an undecodable or non-object 2xx `_bulk` body settles its chunk: no raise, no retry, but `sent` goes to `dropped_unadjudicated` with a may-have-landed line. An object without a truthy `errors` still reads as success, unverified."
---

**A 2xx `_bulk` response with no readable verdict is counted, never retried** — when
`ElasticsearchSink` (and `OpenSearchSink` through it) gets a 2xx whose body cannot be decoded for
any reason — syntax, encoding, nesting past the decoder's recursion guard, memory — or decodes to
something other than an object, the chunk is settled: `emit` does not raise and the worker does not
re-send it, because a 2xx is not provable non-delivery and a re-send would duplicate whatever
landed. It is also **counted**: the chunk's `sent` goes to `dropped_unadjudicated`, with one
`_diag.lost` line saying the events may have landed. This extends *A positional response
adjudicates all of a chunk or none of it* from a misaligned array to a body with no array at all,
for the same reason: the per-item verdict lives in the body, so without one the server has not
confirmed these events, and invariant 2 forbids losing them silently.

**Rejected: settling silently.** Before this decision an undecodable body returned with every counter
at zero, and a too-deep one raised `RecursionError` out of `emit`, so the worker re-sent a batch the
server had indexed and never sent the chunks after it. The first fix settled the too-deep body
silently, and a system-frame review reproduced that as a clean `flush()` with zero losses while the
destination answered every POST with an unreadable 200. Counting changes no retry behaviour: the
counted branch returns the same `False` the silent one did, and `failed` is a documented upper
bound, so counting a body that did land is allowed.

**Not decided here: an object without a truthy `errors` key.** It is read as success, unverified,
and the sink does not check that it is `_bulk`-shaped, so `{"error": {...}, "status": 500}` and an
`items` array of errors with no `errors` flag both count nothing. Routing an absent key to
unadjudicated would charge every success from an endpoint that omits it, which Astra's
OpenSearch-compatible `_bulk` appears to do by its source. It belongs with the Astra profile;
`docs/architecture.md` §12 records the exposure.

**OTLP reads the same shape differently, on purpose** — SPEC-056 FR-004 settles an unreadable 2xx
OTLP body without a count, because a protobuf response is legal there. Do NOT copy either reading
into the other sink.

## History

### 2026-11-03

- **Settled.** A too-deep body raised out of `emit` and was retried; the fix routes every unreadable
  or non-object 2xx body to the unadjudicated count.
