---
type: Decision
title: "A 2xx this sink cannot read is counted, and the `_bulk` reading is `_bulk`'s alone"
description: "an OTLP `2xx` with a `partialSuccess` that is not an object, a count that is not a usable integer, or a body over the response bound counts the chunk on `dropped_unadjudicated` (an upper bound, into `failed`) and settles it; a body that is empty, not JSON or not an object is full success, because OTLP needs no `partialSuccess` to succeed. `_bulk`'s counted reading of an unreadable 2xx is `_bulk`'s, not the family's. (SPEC-056)"
---

**A 2xx this sink cannot read is counted, and the `_bulk` reading is `_bulk`'s alone** — an OTLP
`2xx` whose verdict is present but unreadable — a `partialSuccess` that is `5`, `[]` or `"x"`, a
count that is negative, a `bool`, a float, a list, non-ASCII digits, over-long or beyond int64, or a
body longer than the 4 MiB response bound — is counted on `dropped_unadjudicated`, the chunk's size
as the upper bound, with one `lost N log record(s)` line naming the class and never the body, and is
**not retried**: the request succeeded, so re-sending could duplicate what landed (*A positional
response adjudicates all of a chunk or none of it*, SPEC-018). The chunk is settled; `losses()` sums
the counter into `failed`. **What is not counted is as deliberate as what is.** A body that is
empty, not JSON, not an object or without a `partialSuccess` is full success here, where
`ElasticsearchSink` counts an undecodable or non-object `_bulk` body unadjudicated (*A 2xx `_bulk`
response with no readable verdict is counted, never retried*, #276). The two readings differ because
the protocols do: a `_bulk` verdict can only come from the body, so no body is no verdict, while an
OTLP success needs no `partialSuccess` at all and a proxy's page or a protobuf body is not this
sink's to adjudicate. **`_bulk`'s counted reading is `_bulk`'s, not the family's**: `HTTPSink`'s
default `_handle_response` still reads any `2xx` as delivered, and a new subclass copies neither
reading without arguing for it. **The bounded read, and what it changes.** `OTLPSink` is the first
sink to bound the response it reads (`_response_limit`, 4 MiB; `_attempt` reads one byte past it),
and CPython's `HTTPResponse.read(amt)` on a Content-Length body that arrives short returns the short
bytes rather than raising `IncompleteRead` — only the unbounded `read()` goes through `_safe_read` —
so on this sink a truncated `2xx` lands in the full-success bucket as "not JSON", where every other
sink's unbounded read raises out of `emit`; a chunked body still raises either way. Measured at
`7641de1` against a local server (`probe_truncated.py` in the build's scratchpad): a `200` with
`Content-Length: 100` and 20 bytes on the wire gives `read()` → `IncompleteRead(20 bytes)` and
`read(4194305)` → the 20 bytes; one chunk then a close gives `IncompleteRead` from both reads. So
`OTLPSink` settles a Content-Length-truncated `2xx` as full success, silently, where the `_bulk`
sinks count it and every other sink raises. Accepted and recorded rather than guarded, because
the base class's own behaviour on a failed `2xx` read — an `IncompleteRead` escaping `emit`, an
`OSError` mid-body re-POSTed by the loop — is a pre-existing defect filed separately, and this spec
neither widens nor fixes it: the read stays at its one statement under no new guard, and the probe-D
sweep in `tests/test_sinks_http_refused.py` holds every subclass, `OTLPSink` included, to the escape.
(SPEC-056 FR-003, FR-004)

## History

### 2026-11-03

- **Settled.** Shipped with `OTLPSink`; the `_bulk` contrast was drawn after #276 landed.
