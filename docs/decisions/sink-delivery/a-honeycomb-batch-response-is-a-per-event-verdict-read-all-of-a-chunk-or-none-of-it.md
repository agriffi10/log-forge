---
type: Decision
title: "A Honeycomb batch response is a per-event verdict, read all of a chunk or none of it"
description: "`HoneycombSink` reads `/1/batch`'s `200` array positionally: an entry whose integer `status` is outside `2xx` counts on `rejected_events`, never re-sent; a body that is empty or unreadable (`b\"\"`), not an array of objects, the wrong length, or holding a status that is not an `int` (a `bool` and a float are not) counts the whole chunk on `dropped_unadjudicated`, never re-sent; a chunk whose every event was rejected is `REJECTED`, so a run with no accepted event fails (`emit` raises) and, since SPEC-073, is not re-sent. The server's `error` text is never written."
---

**A Honeycomb batch response is a per-event verdict, read all of a chunk or none of it** —
Honeycomb's `POST /1/batch/<dataset>` answers `200` "Enqueued for processing" whatever happened to
the events, and the body is an array of `{"status": <number>, "error": <string>}`, its documented
example `[{"status": 202}, {"status": 400, "error": "..."}]`
(docs.honeycomb.io/api/events/createevents, read 2026-11-03). `HoneycombSink` inherited `HTTPSink`'s
default `_handle_response`, which ignores the body, so an event rejected inside the `200` was
counted nowhere and announced nowhere, against invariant 2. It now reads the array. **The array
names no event**, so it is positional, and the docs do not promise either its order or its length —
that inference rests on the example, on Refinery's batch handler (`route/route.go`), which appends
one entry per event in request order, and on `libhoney-go`, which pairs entry *i* with the *i*-th
event sent; the length gate is what makes acting on it safe. So the rule of *A positional response
adjudicates all of a chunk or none of it* (SPEC-018) applies whole: the array must have the chunk's
length and every entry must be an object whose `status` is an `int` before entry *i* is read as
event *i*. **One unreadable entry leaves the whole chunk unadjudicated**, which is stricter than
`ElasticsearchSink`, where an item without a readable error counts as indexed. The difference is the
protocol's: `_bulk` signals failure with a key, so its absence can be read as success, while
Honeycomb signals success with the status itself, so a missing status is neither an acceptance nor a
rejection. A `bool` is refused, though Python's `True` is an `int`, and so is `400.0`, since a
server writing either is not describing the event in the form Honeycomb documents.

**A status in `200`–`299` is an acceptance and any other integer a rejection.** The docs show only
`202` and `400`; Refinery also writes a per-event `429` when its queue is full. Reading the whole
`2xx` range as acceptance keeps a proxy or a later API version answering `200` from charging every
event, and reading every other integer as a rejection keeps a `429` or `500` inside the array from
being read as delivery. Refinery's `429` is counted and not re-sent like any other rejection — a
re-send would duplicate the events that landed beside it. Rejected events count on
`rejected_events`, one `lost N event(s)` line names the class, and they are **not re-sent**,
because the chunk's other events landed. Since SPEC-073 `HoneycombSink._outcomes` returns each
event's outcome, `ACCEPTED` or `REJECTED`, and `_handle_response` is a 1.x shim over it. **A chunk
whose every event was rejected is wholly `REJECTED`**, so a run with no accepted event fails —
`emit` raises `SinkDeliveryError` and `failed_batches` moves, which is how a permanent rejection is
seen (invariant 7) — and it is sent once and counted once, since no round resends an outcome
rejected for good. Before SPEC-073 the worker re-sent such a batch and each attempt added the
chunk's size to `rejected_events` again, the cost *A populated partial success is counted, never
retried; every record rejected raises* recorded for OTLP.

**A body that is no verdict is counted, never read as success.** That includes `b""`: since *After a
status line the status decides; a 2xx whose body cannot be read is never re-sent* (#283), an empty
body and one that could not be read after the status line both reach the hook equal to `b""` (since
SPEC-070 the second as an `_UnreadBody`, which the hook asks `_read_failed` about first), and
Honeycomb's only per-event verdict is in the body, so reading `b""` as delivery would settle
silently an all-rejected response whose tail was cut off. This is the `_bulk` sinks' reading,
reached here on this destination's own argument rather than copied, as *A 2xx this sink cannot read
is counted, and the `_bulk` reading is argued, never inherited* asks; ~~`OTLPSink` reads `b""` the
other way because an OTLP success needs no body~~. The whole chunk goes to
`dropped_unadjudicated` with a may-have-landed line, through `HTTPSink._unadjudicated`,
`losses().failed` sums it with `rejected_events` and the abandoned requests, and its events are
`UNKNOWN`, never re-sent and never a failed run. **The known cost**: a proxy that speaks Honeycomb's batch
protocol but answers with something other than an array — none is known to; Refinery answers with
the array, and `libhoney-go` itself fails a whole batch whose `200` body does not decode — would
have every chunk charged as an upper bound, one stderr line per chunk, since `_diag.lost` is not
throttled. That is loud, not silent, which is the side invariant 2 lets it err on.

> **Superseded in part by SPEC-070 FR-004:** `OTLPSink` now counts an unreadable `2xx` body as this
> sink does, and reads only an **empty** one as success, because an OTLP success needs no body — row
> 4 of *The sinks keep four declared exceptions to their shared rules*.

**Rejected: reading an empty or unreadable `2xx` as success.** It settles silently the very response
the gap was about. **Rejected: reading the overlapping prefix of a misaligned array.** A length
disagreement is evidence the entries do not line up with the events, so counting the prefix could
charge accepted events as rejected and report a total failure as partial. **Rejected: a per-event
retry of the rejected events.** A `400` here is about the event — too many columns, a malformed body
— and a byte-identical re-send fails the same way; a re-send of the chunk would duplicate what
landed. **Rejected: writing the server's `error` text.** It is the server's, free to echo a value
the caller logged, and invariant 11 names a type and never a value. **Not done: a response bound.**
`HoneycombSink` reads its `2xx` whole, as `ElasticsearchSink` does; only `OTLPSink` bounds its
response, and a bound would also change how an oversized non-`2xx` is retried.

## History

### 2026-11-08

- **Refined** by SPEC-073: the reader is `HoneycombSink._outcomes`, giving up through
  `HTTPSink._unadjudicated`. A wholly rejected chunk is `REJECTED`, so a run with nothing accepted
  fails and is sent and counted once, where the worker used to re-send it and recount
  `rejected_events` on each attempt.

### 2026-11-07

- **Marked.** SPEC-070 FR-004: the unreadable body arrives as an `_UnreadBody`, and `OTLPSink`'s
  contrasting reading now covers only an empty body; the sentence carries a superseded marker.

### 2026-11-03

- **Settled.** Closes `docs/architecture.md` §12's open item, found by the system-frame review of
  #283. Guarded by `tests/test_sinks_honeycomb.py` and the body-reader roster in
  `tests/test_sinks_http_body_read.py`.
