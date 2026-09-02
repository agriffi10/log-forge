# Spec: Sink Delivery — Loss and Duplication

**ID:** SPEC-048
**Status:** Draft
**Last Updated:** 2026-09-02
**Depends On:** SPEC-016, SPEC-018, SPEC-026, SPEC-027, SPEC-032, SPEC-036, SPEC-038

## Overview

Six sink defects put events on the wire wrongly, twice, or not at all, and every one of them
reports success. The 2026-09-02 pre-1.0 audit found two it would not tag without: an HTTP
collector that answers `301` takes the batch's bytes and never sees them again, because the
default `urlopen` opener follows the redirect as a body-less `GET` — carrying the bearer token to
whatever host the redirect names — and the sink reads the redirect target's `200` as delivery. And
the four AWS batch sinks raise when the client fails part-way through a batch, so the worker's
retry re-sends the chunks that already landed; the exit drain, which is one large batch by
construction, is exactly that shape. The rest of this spec is the same failure in smaller places:
a Pub/Sub `close()` whose wait is unbounded while its own `flush()` is bounded, a Sentry `close()`
that leaves captured events in the SDK's background queue, a Kinesis size check that under-counts
every record by its partition key, and three residues that corrupt a frame or duplicate a write.

What unites them is the counter: in every case `health()` and `losses()` read zero while events
are lost or doubled, which is the condition SPEC-026 exists to end.

## Scope

### In Scope

- `HTTPSink` and every sink built on it refusing a `3xx` rather than following it.
- Per-chunk isolation of the client call in `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`.
- The Kinesis per-record ceiling charging the partition key, as its request budget already does.
- `GooglePubSubSink.close()` running its own `flush()`'s bounded deadline loop.
- `SentrySink.close()` pushing the SDK transport before forwarding to the HTTP fallback.
- Three wire-level residues: a caller's `Content-Encoding` overwritten by `gzip=True`, a
  `RotatingFileSink` rotation failure duplicating the events already written in that batch, and
  whitespace in `SyslogSink(app_name=)` / `ElasticsearchSink(index=)` corrupting the frame.

### Out of Scope

- Construction-time validation of sink arguments, and the driver-default waits — SPEC-049,
  built after this one.
- `LogstashSink` silently ignoring `host`/`port`/`transport`, and `SentrySink` accepting an
  unparseable DSN — both are construction-time refusals and belong to SPEC-049 with the rest.
- The `SocketTransport` abandonment line's attempt count, and the `LoggingSink` docstring that
  overstates what it can report: they are diagnostic and prose truth, not delivery, and go to
  SPEC-049's docstring FR.
- Following a redirect *correctly* — re-issuing the `POST` against the `Location`. A redirect is
  treated as a delivery failure, not as a route to be followed; see FR-001.
- Any change to `worker.py`, `_lifecycle.py` or the public top-level API.

---

## Functional Requirements

### FR-001: A redirect is a counted delivery failure, never a silent re-`POST`-as-`GET`

#### Description:

`HTTPSink` passes requests to `urllib.request.urlopen`, whose default opener follows `301`, `302`,
`303`, `307` and `308`. For the first three the stdlib rewrites the method to `GET` and drops the
body while keeping every header, including `Authorization`. The batch is never delivered, the
credential reaches a host the caller did not configure, and the redirect target's `200` is read as
success. The sink must use a private opener that refuses to redirect, so a `3xx` arrives at
`_attempt` as an `HTTPError` and takes the existing `_abandon` path — counted in `failed`,
announced through `_diag`, and raised to the worker as the total failure it is.

This reaches every subclass and `SentrySink`'s HTTP fallback, because all of them send through
`HTTPSink._request`. An injected `opener=` is untouched: it is the caller's object and the
library does not reshape it.

#### Acceptance Criteria:

- [ ] A collector answering `302` with a `Location` on a second origin causes `emit` to raise
      `SinkDeliveryError`; the second origin receives **no** request at all.
- [ ] After that emit, `losses()` reports `failed >= 1` and a `_diag` line names `HTTP 302`.
- [ ] The `Authorization` header is sent to the configured URL only; a test asserting on the
      redirect target's received headers sees no request to assert on.
- [ ] `307` and `308` are refused on the same path, so no status in `300..399` is followed.
- [ ] A `200` from the configured URL is unaffected: the existing success tests still pass, and a
      caller-injected `opener=` is called exactly as before.

### FR-002: A partial batch never raises — the client call is isolated per chunk

#### Description:

`SQSSink`, `SNSSink`, `KinesisSink` and `FirehoseSink` each loop over chunks calling `self._send`,
which documents `Raises: Exception: Whatever the client raises`. Nothing catches it, so a
`ClientError` or `EndpointConnectionError` on chunk N propagates out of `emit` after chunks
`1..N-1` were delivered. The worker reads that as total failure and re-sends the whole batch.
`base.py` already states the rule the four break: do not raise on partial failure.

Each sink must guard its own client call the way `ClickHouseSink._insert` already does — count the
chunk's entries into `failed`, announce the failure by exception type through `_diag`, and let the
chunk loop continue. `emit` then raises only when nothing was delivered, which is the existing
total-failure test in each of the four, unchanged.

#### Acceptance Criteria:

- [ ] With a client that raises on its second call, a 25-event batch through `SQSSink` (3 chunks)
      returns without raising, and a second `emit` of the same batch — the worker's retry — is
      never provoked, so the destination receives each event exactly once.
- [ ] After that emit, `losses().failed` equals the number of entries in the failed chunk, and a
      `_diag` line names the exception type.
- [ ] A client raising on **every** call still makes `emit` raise `SinkDeliveryError`, so the
      worker's retry engages for a genuinely total failure.
- [ ] The same three criteria hold for `SNSSink`, `KinesisSink` and `FirehoseSink`.
- [ ] `KeyboardInterrupt` and `SystemExit` are **not** absorbed by the guard (SPEC-025).

### FR-003: The Kinesis per-record ceiling charges the partition key

#### Description:

`KinesisSink._records` compares `len(data)` against `MAX_RECORD_BYTES`, but `PutRecords` charges
the partition key against the per-record limit as well as against the request budget —
`_record_size` already charges it for the second. A record at exactly 1 MiB of data with a
256-byte key passes the sink's check and is rejected by the service, which fails the whole
`PutRecords` call; with FR-002 in place that is a counted chunk failure rather than a duplication,
but the record can never be delivered and should be dropped before the request is built.

#### Acceptance Criteria:

- [ ] An event whose serialized data is `MAX_RECORD_BYTES` with a non-empty partition key is
      dropped before any client call, `losses().dropped` moves by one, and a `_diag` line names
      the byte total including the key.
- [ ] An event whose data plus key is exactly `MAX_RECORD_BYTES` is **sent**, not dropped.
- [ ] The dropped-record message reports the total charged, not the data length alone.

### FR-004: `GooglePubSubSink.close()` is bounded, and counts what it abandons

#### Description:

`close()` calls `_resolve(future)` with no timeout for every pending future. `_resolve`'s own
docstring records that this is deliberate — "wait indefinitely, which is what `close` does" — but
`Worker.shutdown` closes the live sink **inline**, and the client's publish deadline is 600 s, so
one unreachable destination holds process exit for that long per future. The sink's `flush()`
already has the right shape: one deadline over the whole list, `_Unboundable` caught, the futures
lock never held across a `result()`. `close()` must run that same loop, and then count whatever is
still in flight into `failed` and announce it once with a count — `KafkaSink._flush_bounded`'s
rule, for its reason.

`close()` must stay total (`Raises: None`), which is the FR-011 isolation boundary this sink
already documents.

#### Acceptance Criteria:

- [ ] With futures that never settle, `close()` returns within `overflow_timeout` plus a small
      margin rather than running to the client's own deadline.
- [ ] The futures still in flight when it returns are counted into `losses().failed`, and one
      `_diag` line reports the count.
- [ ] `close()` raises nothing, including when a future's `result()` itself raises.
- [ ] Futures that settle normally are still resolved and still counted exactly as today, so the
      existing `close()` tests pass unchanged.
- [ ] The bound is asserted on **CPU** time as well as wall clock, so a busy-spin cannot pass it.

### FR-005: `SentrySink.close()` pushes the SDK transport

#### Description:

`capture_event` hands to the Sentry SDK's background transport and returns. `SentrySink.flush()`
pushes that queue; `close()` forwards only to the HTTP fallback, which holds nothing. So
`shutdown()` on its own — the whole of the frozen-Lambda path, where the SDK's own timer never
fires again — strands every captured event in the SDK's worker. `close()` must call `self.flush()`
first, absorbing whatever it raises, because `close()` is an isolation boundary and a failing
flush must not stop the forward that follows it.

#### Acceptance Criteria:

- [ ] After `emit` then `close()` on an injected client, the client's `flush` has been called
      exactly once.
- [ ] A client whose `flush` raises does not make `close()` raise, and the HTTP-fallback forward
      still happens.
- [ ] A sink with no client (`backend="http"`) still calls `close()` with no SDK flush attempted,
      unchanged.
- [ ] `close()` stays idempotent: a second call performs no second SDK flush.

### FR-006: Three residues that corrupt a frame or duplicate a write

#### Description:

Three small defects with the same consequence as the large ones.

`HTTPSink._prepare` applies `gzip` **after** merging the caller's headers, so `gzip=True`
overwrites a `Content-Encoding` the caller set. The caller's header must win, as every other
header of theirs already does — and when it does, the body must not be gzipped, or the header and
the bytes disagree.

`RotatingFileSink.emit` writes events one at a time inside the batch lock, rotating between them.
An `OSError` from `_rotate` mid-batch leaves the events already written on disk and propagates, so
the worker re-sends the whole batch and the pre-rotation events are written twice. The rotation
failure must be absorbed into the same counted-loss shape the rest of the sink family uses, so a
failed rotation costs the events it could not write rather than duplicating the ones it could.

`SyslogSink(app_name=)` goes into an RFC 5424 header where a space ends the field, and
`ElasticsearchSink(index=)` goes into a `_bulk` action line as JSON. Whitespace in either
corrupts the frame for every event the sink ever sends. Both must be rejected at construction.

#### Acceptance Criteria:

- [ ] `HTTPSink(gzip=True, headers={"Content-Encoding": "identity"})` sends
      `Content-Encoding: identity` and an **uncompressed** body.
- [ ] `HTTPSink(gzip=True)` with no caller header still sends `Content-Encoding: gzip` and a
      gzipped body.
- [ ] A `RotatingFileSink` whose `_rotate` raises part-way through a batch does not raise out of
      `emit` when some events were written; the events it could not write are counted in
      `losses().failed` and announced once.
- [ ] Re-emitting that batch is not required to avoid duplication — the criterion above is what
      removes it — but a batch where **nothing** was written still raises, so the worker retries.
- [ ] `SyslogSink(app_name="my app")` and `ElasticsearchSink(index="my index")` each raise
      `ValueError` at construction naming the offending argument; a tab and a newline are refused
      on the same path.

---

## Data Model

No new types. The changes are to behaviour and to existing counters:

```python
# sinks/http.py — a module-level opener shared by every HTTPSink, built once.
_NO_REDIRECT_OPENER: urllib.request.OpenerDirector

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl) -> None: ...
```

Counters that move where they did not before, all already public through `losses()`:

| Sink | Counter | New reason it moves |
|---|---|---|
| `HTTPSink` | `failed` | a `3xx` response |
| `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink` | `failed` | a client exception on one chunk |
| `KinesisSink` | `dropped_oversized` | data + key over the per-record ceiling |
| `GooglePubSubSink` | `failed` | futures still in flight when `close()`'s bound expired |
| `RotatingFileSink` | `failed` (new attribute) | events a failed rotation could not write |

## API / Interface Contract

No public signature changes. Two constructors gain a refusal they did not have:

```python
SyslogSink(host, port, *, app_name="log-foundry", ...)   # ValueError on whitespace in app_name
ElasticsearchSink(url, *, index, ...)                    # ValueError on whitespace in index
```

`RotatingFileSink` gains a `losses()` method and a `failed` counter, which the sink protocol
already defines as optional and `read_losses` already discovers.

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/sinks/
├── http.py            # FR-001 opener, FR-006 gzip header precedence
├── sqs.py             # FR-002
├── sns.py             # FR-002
├── kinesis.py         # FR-002, FR-003
├── firehose.py        # FR-002
├── pubsub.py          # FR-004
├── sentry.py          # FR-005
├── file.py            # FR-006 rotation failure
├── syslog.py          # FR-006 app_name
└── elasticsearch.py   # FR-006 index
tests/
├── test_sinks_http.py, test_sinks_sqs.py, test_sinks_kinesis.py, test_sinks_firehose.py,
├── test_sinks_sns.py, test_sinks_pubsub.py, test_sinks_sentry.py, test_sinks_file.py,
└── test_sinks_syslog.py, test_sinks_elasticsearch.py
```

## Implementation Phases

### Phase 1: The two blocking defects (FR-001, FR-002)

- Build the no-redirect opener and route `_attempt` through it; assert the refusal against a real
  two-origin HTTP server, not a fake opener, since the defect is in the stdlib's opener.
- Add the per-chunk guard to the four AWS sinks, with a shared shape rather than four spellings.
- A roster test over the four asserting each one's chunk loop is guarded, so a fifth AWS-shaped
  sink cannot be added unguarded.

### Phase 2: The bounded and stranded closes (FR-004, FR-005)

- Factor Pub/Sub's deadline loop out of `flush()` and call it from `close()`, counting the
  remainder.
- Call `self.flush()` from `SentrySink.close()` inside an absorbing guard.

### Phase 3: The residues (FR-003, FR-006)

- Kinesis per-record charge including the key.
- gzip header precedence, rotation-failure accounting, and the two whitespace refusals.

### Phase 4: Mutation-verify the guards whose failure would be silent

- Re-plant each defect and confirm the new test reddens: the followed redirect, the unguarded
  chunk call, the unbounded close, the missing SDK flush. Assert the **reason** in every case —
  a count or an exit code passes against the neighbouring code path.
