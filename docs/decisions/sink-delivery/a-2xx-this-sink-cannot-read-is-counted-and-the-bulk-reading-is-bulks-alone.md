---
type: Decision
title: "A 2xx this sink cannot read is counted, and the `_bulk` reading is argued, never inherited"
description: "an OTLP `2xx` with a `partialSuccess` that is not an object, a count that is not a usable integer, or a body over the response bound counts the chunk on `dropped_unadjudicated` (an upper bound, into `failed`) and settles it; a body that is empty, not JSON or not an object is full success, because OTLP needs no `partialSuccess` to succeed. `_bulk`'s counted reading of an unreadable 2xx is not the family's: a sink takes it only on its own argument, as `HoneycombSink` does. (SPEC-056)"
---

**A 2xx this sink cannot read is counted, and the `_bulk` reading is argued, never inherited** —
an OTLP `2xx` whose verdict is present but unreadable — a `partialSuccess` that is `5`, `[]` or `"x"`, a
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
sink's to adjudicate. **`_bulk`'s counted reading is not the family's**: `HTTPSink`'s default
`_handle_response` still reads any `2xx` as delivered, and a new subclass copies neither reading
without arguing for it — `HoneycombSink` takes the counted one on its own argument, that its
per-event verdict too is only in the body (*A Honeycomb batch response is a per-event verdict, read
all of a chunk or none of it*). **A body that cannot be read at all is `b""`, and `b""` is full
success here.** *After a status line the status decides; a 2xx whose body cannot be read is never
re-sent* (#283) reads the body under a guard once the status is in hand and hands
`_handle_response` `b""` for a read that failed, so an override cannot tell an empty body from an
unreadable one and must read both the same way. OTLP's reading is the one that decision names as
correct for it: a `2xx` is the verdict, an empty body is a success with nothing to say, so a body
cut short, stalled or reset after the status line settles its chunk as delivered, uncounted — where
the `_bulk` sinks and `HoneycombSink` count the same `b""` unadjudicated, because their only verdict
is in the body. The
bounded read keeps that rule rather than escaping it: CPython's `read(amt)` returns a short
`Content-Length` body silently with `response.length` still above zero, where the bare `read()`
raises `IncompleteRead`, so `HTTPSink._attempt` checks `response.length` after a bounded read and
turns fewer bytes than it asked for, with bytes still owed, into the same `b""` a raise gives; a
truncated chunked body raises from `read(amt)` and takes the guard. Proved through a real socket
on every gate run (`tests/test_sinks_otlp.py`, the truncated-`2xx` cases): a `200` declaring
`Content-Length: 100` that delivers a complete `partialSuccess` rejecting every record and then
closes reaches `OTLPSink` as `b""` and is full success, one POST, nothing counted, nothing raised —
which is also what makes the `response.length` check a mutation target, since without it the same
bytes parse and raise; the chunked twin raises inside `read(amt)` and arrives the same way. A `2xx`
over the bound is a different case: the read returns one byte past it and `_handle_response` counts
the chunk unadjudicated. "Over the bound" is measured on the bytes read, not on the length declared:
a body declared at 10 MiB that is cut after 5 MiB is over the bound and counted, while one cut after
2 MiB returns fewer bytes than asked with bytes still owed, so it is truncated, reaches the sink as
`b""` and is full success. Both settle the chunk without a re-send. (SPEC-056 FR-003, FR-004; #283)

## History

### 2026-11-03

- **Refined.** `HoneycombSink` now counts `b""` too, so "`_bulk`'s alone" in the title became
  "argued, never inherited": the counted reading is still no subclass's default.
- **Settled.** Shipped with `OTLPSink`; the `_bulk` contrast was drawn after #276 landed, and the
  unreadable-body paragraph was rewritten onto #283's guard when the branch was rebased over it —
  before #283, a Content-Length-truncated `2xx` reached this sink as short bytes.
