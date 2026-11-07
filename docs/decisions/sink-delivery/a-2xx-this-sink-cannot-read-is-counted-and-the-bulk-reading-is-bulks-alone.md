---
type: Decision
title: "A 2xx this sink cannot read is counted, and the `_bulk` reading is argued, never inherited"
description: "an OTLP `2xx` with a `partialSuccess` that is not an object, a count that is not a usable integer, a body over the response bound, or a body that could not be read counts the chunk on `dropped_unadjudicated` (an upper bound, into `failed`) and settles it; a body that is empty, not JSON or not an object is full success, because OTLP needs no `partialSuccess` to succeed. `_bulk`'s counted reading of an empty or undecodable 2xx is not the family's: a sink takes it only on its own argument, as `HoneycombSink` does. (SPEC-056, SPEC-070)"
---

**A 2xx this sink cannot read is counted, and the `_bulk` reading is argued, never inherited** —
an OTLP `2xx` whose verdict is present but unreadable — a `partialSuccess` that is `5`, `[]` or `"x"`, a
count that is negative, a `bool`, a float, a list, non-ASCII digits, over-long or beyond int64, or a
body longer than the 4 MiB response bound — is counted on `dropped_unadjudicated`, the chunk's size
as the upper bound, with one `lost N log record(s)` line naming the class and never the body, and
is **not retried**: the request succeeded, so re-sending could duplicate what landed (*A positional
response adjudicates all of a chunk or none of it*, SPEC-018). Since SPEC-070 a `2xx` whose body
could not be read at all is counted the same way (below). The chunk is settled; `losses()` sums
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
all of a chunk or none of it*). **A body that cannot be read at all is counted, and an empty one
is full success** (SPEC-070 FR-004). *After a status line the status decides; a 2xx whose body
cannot be read is never re-sent* (#283) reads the body under a guard once the status is in hand and
hands `_handle_response` an `_UnreadBody`, equal to `b""`, for a read that failed; `_read_failed`
tells it from an honestly empty body. This sink asks first, before the response bound: an unreadable
body may have been an all-rejected `partialSuccess` cut short, so settling it as delivered would
lose its rejections silently, and it is counted on `dropped_unadjudicated` with the line written for
an unreadable `partialSuccess`, settled and never re-sent. An empty body is still a success with
nothing to say — the one place this sink reads a body differently from the `_bulk` sinks and
`HoneycombSink`, which count an empty body too because their only verdict is in it (*The sinks keep
four declared exceptions to their shared rules*, row 4). Before SPEC-070 the hook could not tell the
two apart, and a body cut short, stalled or reset after the status line settled here as delivered,
uncounted. The
bounded read keeps that rule rather than escaping it: CPython's `read(amt)` returns a short
`Content-Length` body silently with `response.length` still above zero, where the bare `read()`
raises `IncompleteRead`, so `HTTPSink._attempt` checks `response.length` after a bounded read and
turns fewer bytes than it asked for, with bytes still owed, into the same `_UnreadBody` a raise
gives; a truncated chunked body raises from `read(amt)` and takes the guard. Proved through a real socket
on every gate run (`tests/test_sinks_otlp.py`, the truncated-`2xx` cases): a `200` declaring
`Content-Length: 100` that delivers a complete `partialSuccess` rejecting every record and then
closes reaches `OTLPSink` unread, equal to `b""`, and is one POST with all five records counted on
`dropped_unadjudicated`, nothing raised and nothing re-sent — which is also what makes the
`response.length` check a mutation target, since without it the same bytes parse and raise; the chunked twin raises inside `read(amt)` and arrives the same way. A `2xx`
over the bound is a different case: the read returns one byte past it and `_handle_response` counts
the chunk unadjudicated. "Over the bound" is measured on the bytes read, not on the length declared:
a body declared at 10 MiB that is cut after 5 MiB is over the bound and counted, while one cut after
2 MiB returns fewer bytes than asked with bytes still owed, so it is truncated, reaches the sink
unread and is counted unadjudicated too. Both settle the chunk without a re-send. (SPEC-056 FR-003, FR-004; #283)

## History

### 2026-11-07

- **Refined** by SPEC-070 FR-004: a `2xx` whose body could not be read is counted on
  `dropped_unadjudicated` and settled, where it was full success because the hook could not tell it
  from an empty body; an empty body is still full success. The truncated-`2xx` proof now asserts the
  count.

### 2026-11-03

- **Refined.** `HoneycombSink` now counts `b""` too, so "`_bulk`'s alone" in the title became
  "argued, never inherited": the counted reading is still no subclass's default.
- **Settled.** Shipped with `OTLPSink`; the `_bulk` contrast was drawn after #276 landed, and the
  unreadable-body paragraph was rewritten onto #283's guard when the branch was rebased over it —
  before #283, a Content-Length-truncated `2xx` reached this sink as short bytes.
