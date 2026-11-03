---
type: Decision
title: "The envelope is charged per record, and the wrapper withheld"
description: "`OTLPSink._item_size` charges every record its own resource group's framing — `_otel.framing(key, version)`, the difference in UTF-8 bytes between a request holding one empty record and an empty one — plus its separator, and `_body_reserve` withholds the `{\"resourceLogs\":[]}` wrapper; `LokiSink`'s method, over-counting when events share a resource, which is the safe direction. (SPEC-056)"
---

**The envelope is charged per record, and the wrapper withheld** — an OTLP chunk's body is not the
sum of its records: each distinct `(service, version, env)` adds a `resource`, a `scopeLogs` wrapper
and two schema URLs, and a `configure()` mid-run can put several resources in one chunk. So
`OTLPSink._item_size` charges each record `_otel.framing(key, version)` — what its own resource's
group costs around its records, computed as the difference in UTF-8 bytes between
`request_body([(key, "")], v)` and `request_body([], v)` so it can never disagree with the body
builder, and in bytes rather than characters because `ensure_ascii=False` makes a stamp such as
`café` longer in bytes than in characters — plus the one-byte separator that joins it to its
neighbour; `_body_reserve` withholds the wrapper, `len(request_body([], v))`, nineteen bytes. This
is `LokiSink`'s method (*a stream header charged per item*, SPEC-038): it over-counts whenever
events share a resource, which is the safe direction — more requests than strictly needed, never
one the destination rejects — and the cost is stated: a chunk of one resource is charged an envelope
per record, so it carries fewer records than the budget alone would allow. The criterion that
catches an uncharged envelope is the three-resource half of the budget test, where every request
spans all three; the one-resource half cannot see it, because the slack greedy filling leaves can
absorb a single uncharged envelope, and a planted `_item_size` without the framing term is a
standing test. `max_batch_bytes` is measured on the uncompressed body; the inherited generic
defaults (1,000 and 5,000,000) stand, since OTLP publishes no record count and its 64 MiB server
recommendation is far above. (SPEC-056 FR-003)

## History

### 2026-11-03

- **Settled.** Shipped with `OTLPSink`.
