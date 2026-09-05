# Spec: Sink Delivery — Loss and Duplication

**ID:** SPEC-048
**Status:** Completed
**Last Updated:** 2026-09-02
**Depends On:** SPEC-016, SPEC-018, SPEC-026, SPEC-027, SPEC-032, SPEC-036, SPEC-038

## Overview

Seven sink defects put events on the wire wrongly, twice, or not at all. The 2026-09-02 pre-1.0
audit found two it would not tag without: an HTTP collector that answers `301` takes the batch's
bytes and never sees them again, because the default `urlopen` opener follows the redirect as a
body-less `GET` — carrying the bearer token to whatever host the redirect names — and the sink
reads the redirect target's `200` as delivery. And the four AWS batch sinks let a client exception
escape mid-batch, so the worker's retry re-sends the chunks that already landed; the exit drain,
which is one large batch by construction, is exactly that shape. The rest is the same failure in
smaller places: a Pub/Sub `close()` whose wait is unbounded while its own `flush()` is bounded, a
Sentry `close()` that leaves captured events in the SDK's background queue, a Kinesis size check
that under-counts every record by its partition key, a file rotation whose failure both duplicates
and permanently breaks the sink, and a `gzip` flag that overwrites a header the caller set.

What unites them is the **counter**: in every case `losses()` reads zero for events that were lost
or doubled, which is the condition SPEC-026 exists to end. The *verdict* differs — three of the
seven do raise today — but a raise with no counter behind it tells the operator nothing about what
happened, and in four of the seven it provokes the duplication.

## Scope

### In Scope

- `HTTPSink` and every sink built on it refusing a `3xx` rather than following it.
- Per-chunk isolation of the client call in `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`.
- The Kinesis per-record ceiling charging the partition key, in **UTF-8 bytes**, as its request
  budget should already have done.
- `GooglePubSubSink.close()` running its own `flush()`'s bounded deadline loop, and the one
  residual unbounded `_resolve` on the flush-races-close path.
- `SentrySink.close()` pushing the SDK transport before forwarding to the HTTP fallback.
- A `RotatingFileSink` rotation failure costing neither a duplicate nor the sink itself.
- A caller's `Content-Encoding` surviving `gzip=True`.

### Out of Scope

- **Every construction-time refusal**, without exception — including `SyslogSink(app_name=)` and
  `ElasticsearchSink(index=)`, which the first draft of this spec kept and its Out of Scope
  simultaneously forbade. They are in SPEC-049, named there explicitly. The seam is *when the
  library refuses*, not *what the consequence is*: a spec that validates arguments in two places
  cannot be reviewed as one rule. SPEC-049 FR-004 carries both.
- `LogstashSink` silently ignoring `host`/`port`/`transport`, and `SentrySink` accepting an
  unparseable DSN — the same seam, also SPEC-049 FR-004.
- The `SocketTransport` abandonment line's attempt count, and the `LoggingSink` docstring that
  overstates what it can report — diagnostic and prose truth, SPEC-049 FR-006.
- Following a redirect *correctly* — re-issuing the `POST` against the `Location`. A redirect is
  treated as a delivery failure, not as a route to be followed; see FR-001.
- Any change to `worker.py`, `_lifecycle.py`, `config.py`, `results.py` or the top-level API.

---

## Functional Requirements

### FR-001: A redirect is a counted delivery failure, never a silent re-`POST`-as-`GET`

#### Description:

`HTTPSink` passes requests to `urllib.request.urlopen`, whose default opener follows `301`, `302`
and `303` **on a POST**, rewriting the method to `GET` and dropping the body while keeping every
header, including `Authorization`. The batch is never delivered, the credential reaches a host the
caller did not configure, and the redirect target's `200` is read as success.

`307` and `308` are **already** refused for a POST: `HTTPRedirectHandler.redirect_request` raises
`HTTPError` unless the method is `GET`/`HEAD`, or the status is one of the first three and the
method is `POST`. The audit and this spec's first draft both said all five were followed; only three
are. They stay in the parametrized test as a **regression pin** rather than as evidence the fix
works — an acceptance criterion that passes against the unfixed sink proves nothing, and this one
is labelled so it is not read as proof.

The sink must use a private opener that refuses to redirect, so a `3xx` arrives at `_attempt` as an
`HTTPError` — which `_attempt` already unifies into a status — and takes the existing `_abandon`
path: counted in `failed`, announced through `_diag`, and raised as `SinkDeliveryError` for the
chunk. `HTTPSink.emit` then decides the batch's verdict as it does for any other abandoned chunk,
so a multi-chunk batch whose other chunks landed still does not raise.

One site sets the opener (`http.py:277`) and one calls it (`http.py:867`), so the change reaches
all six `HTTPSink` subclasses, `LogstashSink`'s HTTP backend (`logstash.py:128`) and `SentrySink`'s
fallback (`sentry.py:166`) without touching any of them. An injected `opener=` is untouched: it is
the caller's object and the library does not reshape it.

#### Acceptance Criteria:

- [ ] Against a real two-origin `http.server`, a collector answering `302` with a `Location` on the
      second origin makes a single-chunk `emit` raise `SinkDeliveryError`; the second origin
      receives **no** request at all.
- [ ] After that emit, `losses().failed` is 1 and a `_diag` line names `HTTP 302`.
- [ ] The test asserts on the second origin's received-request list being empty, which is where the
      `Authorization` header would otherwise appear.
- [ ] `301` and `303` are refused on the same path, asserted identically to `302`.
- [ ] `307` and `308` are refused too — a **regression pin**, not a demonstration: the stdlib
      already refuses both on a POST, so these two parameters pass against the unfixed sink and the
      test says so in a comment.
- [ ] A `200` from the configured URL is unaffected, and a caller-injected `opener=` is called
      exactly as before — the existing `HTTPSink` suite, which injects an opener in almost every
      test, passes untouched.

### FR-002: A client exception costs its chunk, never the batch

#### Description:

`SQSSink`, `SNSSink`, `KinesisSink` and `FirehoseSink` each loop over chunks calling `self._send`,
whose docstring says `Raises: Exception: Whatever the client raises`. Nothing catches it, so a
`ClientError` or `EndpointConnectionError` on chunk N propagates out of `emit` after chunks
`1..N-1` were delivered, and the worker re-sends the whole batch. `base.py` already states the rule
the four break, and SPEC-026's register entry states it as the reason: partial failure must not
raise, because the worker retries whole batches.

**The guard goes inside `_send`, around the client call, within `_send`'s own attempt loop** — not
around `self._send(chunk)` in `emit`. The placement is load-bearing: by the time `_send` raises it
may already hold a non-zero `accepted` from an earlier attempt (`sqs.py:364`) and may have narrowed
`entries` to the retryable subset (`sqs.py:381`), so an `emit`-level guard would charge the whole
chunk to `failed` and report `delivered` as zero for entries that are already in the queue —
turning a partial success into "nothing delivered" and provoking the very retry this FR removes.
Inside `_send` the accounting is exact: entries accepted stay accepted, and only the entries
outstanding at the failing attempt are charged.

Within `_send`'s loop the raising call is **retried** on the remaining budget, as
`ClickHouseSink._insert` retries its own, since nothing was adjudicated and re-sending the same
entries is what recovers a transient fault. On the final attempt the outstanding entries are
counted into `failed`, announced by `type(err).__name__` through `_diag`, and `_send` returns
normally.

**A client exception is treated as provable non-delivery for its chunk**, which is the decision
this FR takes rather than leaves open. It is not free: a read timeout means the request went out
and the *response* was lost, so records may have landed, and re-sending duplicates them — the
"cannot prove nothing landed" case SPEC-018 abandons rather than retries. It is taken anyway on
expected cost. An unreachable or refused endpoint is the common client exception and is exactly
what the worker's retry exists for; suppressing the raise for it would lose every event of every
batch for the whole outage, silently. The response-lost case is rarer, and boto3's own
`max_attempts` retry already carries that property, so this sink does not introduce it. Concretely:
`SQSSink`'s `recoverable_loss` term is **set** by the guard, and `KinesisSink`/`FirehoseSink`'s
`unknown` term is **not** — `unknown` stays what SPEC-018 defined it as, a *response* that could
not be adjudicated, and an exception is not a response.

#### Acceptance Criteria:

- [ ] With a client that raises on its second call, a 25-event batch through `SQSSink` (3 chunks of
      10/10/5) returns without raising, and the destination receives each event exactly once —
      asserted by re-emitting the batch as the worker would and finding the retry was not provoked.
- [ ] After that emit, `losses().failed` is 10 — the failed chunk's entries — and a `_diag` line
      names the exception type.
- [ ] A client raising on **every** call makes `emit` raise `SinkDeliveryError` for all four sinks.
      For `SQSSink` this requires the guard to set `recoverable_loss`; a test asserts the raise,
      because with the guard added and that term left `False` the sink returns normally having lost
      the whole batch.
- [ ] A client that accepts part of a chunk and then raises on the retry charges only the entries
      still outstanding: `losses().failed` is less than the chunk size, and the accepted entries
      count toward `delivered`.
- [ ] `KeyboardInterrupt` and `SystemExit` reach the caller unabsorbed from all four (SPEC-025).
- [ ] The first three criteria hold for `SNSSink`, `KinesisSink` and `FirehoseSink`.
- [ ] A roster test asserts each `_send` guards its client call, and is **derived on shape** — every
      `_send` in `sinks/` whose body calls `self.client.<method>(...)` — with a floor of four, as
      this repo's other rosters carry one. A hard-coded four-module list would leave a fifth
      AWS-shaped sink green, which is the criterion's whole point.

### FR-003: The Kinesis per-record ceiling charges the partition key, in bytes

#### Description:

`KinesisSink._records` compares `len(data)` against `MAX_RECORD_BYTES`, but `PutRecords` charges
the partition key against the per-record limit as well as against the request budget. A record at
exactly 1 MiB of data with a 200-byte key passes the sink's check and is rejected by the service,
which fails the whole `PutRecords` call.

The key must be charged in **UTF-8 bytes**, and in two places: the new per-record check, and
`_record_size` (`kinesis.py:271`), which today adds `len(record["PartitionKey"])` — a character
count, so a multi-byte key under-charges the request budget too. The `[:256]` truncation at
`kinesis.py:193` is likewise characters and must become a byte bound, or a 256-character key of
multi-byte characters exceeds ~~the service's own 256-byte key limit~~ — corrected by the
2026-09-04 audit's N12: the API reference states the limit in **characters**, so the byte bound
shipped is stricter than the service's and safe; only a non-ASCII key between 256 bytes and 256
characters lands on a different shard than an unbounded one would.

**Every encode here passes `errors="replace"`.** ~~`sanitize.coerce` passes a lone surrogate through
unchanged~~ — corrected by SPEC-055 FR-001, which replaces it at assembly; the guard here stays
because the key is derived from a batch a `TransformSink` may have rewritten after assembly — so
a bare `.encode("utf-8")` on a caller's `trace_id` raises `UnicodeEncodeError` — a raw exception
out of `emit`, which is the failure this whole spec exists to remove, introduced by its
own fix. `sanitize.truncate_str` is the inventoried byte-bounded clipper and is deliberately **not**
reused: it appends a truncation marker, which in a partition key changes the shard a record lands
on.

#### Acceptance Criteria:

- [ ] An event whose serialized data is `MAX_RECORD_BYTES` with a non-empty partition key is
      dropped before any client call; `losses().dropped` moves by one, and the `_diag` line reports
      the total charged including the key, not the data length alone.
- [ ] An event whose data plus key is exactly `MAX_RECORD_BYTES` is **sent**, not dropped.
- [ ] A partition key of non-ASCII characters is charged its UTF-8 byte length in both the
      per-record check and `_record_size`, asserted with a key whose byte length exceeds its
      character length.
- [ ] A partition key longer than 256 **bytes** is truncated to at most 256 bytes and the result is
      still valid UTF-8, including when the cut falls mid-character.
- [ ] An event whose partition-key field holds a lone surrogate does not raise out of `emit`.

### FR-004: `GooglePubSubSink.close()` is bounded, and counts what it abandons

#### Description:

`close()` calls `_resolve(future)` with no timeout for every pending future. `_resolve`'s docstring
records that as deliberate — "wait indefinitely, which is what `close` does" — but `Worker.shutdown`
closes the live sink **inline**, and the client's publish deadline is 600 s, so one unreachable
destination holds process exit for that long. Measured with a stalled client: `flush()` raised at
0.51 s and `close()` ran to the client's deadline with the worker's stop signal already set.

`flush()`'s deadline loop is factored out and shared, but **`close()` cannot run it unmodified**,
and the two reasons are the design rather than details of it. Both were found by building the naive
version and running the suite, where each broke an existing test.

**The stop signal must not shorten a close.** The loop's guard is `_out_of_time`, which returns
`True` the moment `log_foundry_stop_signal` is set — and `Worker.shutdown` sets that event *before*
closing the sink inline, so a shared loop would abandon every pending future on every ordinary
shutdown. That is SPEC-038's settled rule reversed: *a shutdown shortens a wait; it must never skip
work*, and the exit drain is the one path a serverless process has. `close()`'s bound is therefore
`time.monotonic()` against `overflow_timeout` alone.

**An unboundable future is resolved unbounded, never counted.** A future whose `result()` takes no
`timeout` raises `_Unboundable`; SPEC-036 measured that counting it invents loss on publishes that
were going to succeed, and `test_a_future_whose_result_takes_no_timeout_is_not_counted_as_lost`
forbids it. `close()` resolves those with `timeout=None` as today — which is what "unbounded" is
still correct for — and bounds only the futures that *can* be waited on within a timeout.

So the shared loop returns two lists, expired and unboundable, and the close-shaped tail resolves
the second unbounded and counts the first into `failed` with one announcement —
`KafkaSink._flush_bounded`'s rule, for its reason.

**All three close-race tails take that tail**, not just `close()`: `flush()`'s
(`pubsub.py:480-482`) and `_await_overflow`'s (`pubsub.py:343-345`) each resolve leftover futures
with `timeout=None` when a close lands mid-pass. The third runs on whichever thread called `emit`,
which on the orphan path is an application thread — SPEC-028's exact hazard. Widening to the third
site is the "fix the rule, not the cited line" discipline: it is the same three lines, and
documenting why one instance was left would cost more than fixing it.

`_resolve`'s docstring becomes false the moment `close` is bounded; it is corrected here rather
than left for SPEC-049, because the sentence describes the behaviour this FR changes.

`close()` must stay total (`Raises: None`), which is the isolation boundary this sink documents.

#### Acceptance Criteria:

- [ ] With futures that never settle and `overflow_timeout=0.3`, `close()` returns in under 3 s
      against a client deadline of 30 s. The margin is deliberately generous: the assertion is
      carried by the **gap** to the client's deadline, not by a tight budget, which is what makes it
      survive a loaded machine.
- [ ] The futures still in flight when it returns are counted into `losses().failed`, and exactly
      one `_diag` line reports the count.
- [ ] CPU time burned across that `close()` is under 0.1 s, so a busy-spin cannot pass it.
- [ ] `close()` raises nothing, including when a future's `result()` itself raises.
- [ ] With the worker's **stop signal set** — a shutdown in progress — `close()` waits exactly as
      long as it does without it, and counts the same. This replaces
      `test_the_stop_signal_is_not_consulted_by_close`'s current assertion that close waits
      `[None]`: that pinned the *mechanism* (unboundedness), and this FR changes the mechanism while
      keeping the rule it was there for. Asserting the rule directly is the stronger test.
- [ ] `test_a_future_whose_result_takes_no_timeout_is_not_counted_as_lost` passes **unchanged**:
      `failed` stays 0 and each future is resolved exactly once.
- [ ] Both other close-race tails — `flush()`'s and `_await_overflow`'s — are bounded on the same
      tail, so no `timeout=None` call is reachable for a future that *can* be bounded, and each
      counts what it abandoned.
- [ ] Futures that settle normally are still resolved and counted exactly as today. Every existing
      `close()`/`flush()` test passes unchanged **except** the one named above, whose assertion this
      FR deliberately replaces — the first draft's blanket "all existing tests pass unchanged" was
      false, and would have forced the design that reverses SPEC-038.

### FR-005: `SentrySink.close()` pushes the SDK transport

#### Description:

`capture_event` hands to the Sentry SDK's background transport and returns. `SentrySink.flush()`
pushes that queue; `close()` forwards only to the HTTP fallback, which holds nothing. So
`shutdown()` on its own — the whole of the frozen-Lambda path, where the SDK's own timer never
fires again — strands every captured event in the SDK's worker. Measured: 25 events captured, zero
`flush` calls seen by the client during `close()`.

`close()` calls `self.flush()` first, absorbing whatever it raises, because `close()` is an
isolation boundary and a failing flush must not stop the forward that follows it.

The flush is **not** suppressed on a repeat close. This sink deliberately adds no post-close guard
(SPEC-032 FR-003), so events can legitimately be captured between two closes, and a flag
suppressing the second flush would strand exactly what this FR exists to un-strand. A repeat flush
of a drained queue is a no-op.

#### Acceptance Criteria:

- [ ] After `emit` then `close()` on an injected client, the client's `flush` has been called
      exactly once.
- [ ] A client whose `flush` raises does not make `close()` raise, and the HTTP-fallback forward
      still happens — asserted on the fallback, not on the absence of an exception alone.
- [ ] A sink with no SDK client (`backend="http"`) closes with no SDK flush attempted, unchanged.
- [ ] `emit` → `close()` → `emit` → `close()` calls the client's `flush` **twice**, and a comment in
      the test names the post-close-capture case as the reason.

### FR-006: A rotation failure costs neither a duplicate nor the sink

#### Description:

`RotatingFileSink.emit` writes events one at a time inside the batch lock, rotating between them.
`_rotate` closes the active stream **first**, then shifts backups, then reopens. An `OSError` from
a rename therefore leaves the sink holding a **closed** stream and propagates out of `emit`.
Measured: an 8-event batch failed after 3 events were written; the worker re-sent the batch and 11
lines reached disk, 3 of them duplicates. With a persistent failure the raw `PermissionError`
escapes `emit` on every subsequent batch — a non-`SinkDeliveryError` the worker books as
`failed_batches` with no `losses()` behind it, since the sink has none.

**The batch is flushed before the rotation is attempted.** `_rotate`'s first statement is
`self._stream.close()`, which flushes, while `emit` flushes once at the end of the batch — so under
the canonical trigger for a rotation failure, a full or read-only filesystem, it is that flush that
raises and the batch's buffered lines are gone before any rename is tried. Absorbing the error is
then not enough: the events are already lost and `os.path.getsize` reports a file that never
received them. Flushing first makes the "every event on disk exactly once" criterion hold at every
one of `_rotate`'s raise sites rather than only at the renames.

A failed rotation must then be absorbed: reopen the active file, re-seed `_size` from it, **re-arm
`_next_rollover`** — `_rotate` sets it on its last line, so an absorbed failure otherwise leaves a
deadline permanently in the past and every later event retries the rotation — announce the failure
once through `_diag`, and **continue the batch on the un-rotated file**. Nothing is lost
and nothing is duplicated; the only cost is the active file exceeding `max_bytes` until a rotation
succeeds, which is what happens anyway when rotation is impossible. That is the same trade
SPEC-027 FR-004 already took — a leaked resource beats a corrupt write.

`file.py:207-212` currently argues that no counter belongs on this class. That paragraph is about
events *dropped*, and it stays true: this FR drops nothing, so no `losses()` and no counter is
added. The rotation failure is announced, not counted.

#### Acceptance Criteria:

- [ ] A `RotatingFileSink` whose `_rotate` raises part-way through a batch does not raise out of
      `emit`, and every event of that batch is on disk exactly once.
- [ ] Re-emitting the same batch afterwards, as the worker would on a raise, is shown not to be
      provoked: the first `emit` returned, so the duplication path is closed at its source.
- [ ] Exactly one `_diag` line per failed rotation names the exception type; a batch that rotates
      successfully writes none.
- [ ] After a failed rotation the sink's stream is **open**: a subsequent `emit` with the failure
      still in place writes its events rather than raising, and the file has grown past
      `max_bytes`.
- [ ] A rotation failing at the **flush** rather than at a rename — the full-disk shape — also
      leaves every event of the batch on disk exactly once, which is the criterion the first draft
      could not meet because its only injection point was `os.replace`, downstream of the flush.
- [ ] After an absorbed failure the time trigger is re-armed, so a persistent failure writes one
      `_diag` line per rotation *attempt* at the configured interval rather than one per event.
- [ ] Once the underlying failure clears, the next triggering event rotates normally.
- [ ] `RotatingFileSink` gains no `losses()` and no counter, and the existing rotation suite passes
      unchanged.

### FR-007: `gzip=True` does not overwrite a header the caller set

#### Description:

`HTTPSink._prepare` merges the caller's headers and then applies `gzip`, so `gzip=True` overwrites
a `Content-Encoding` the caller supplied — the one header of theirs that does not win. When the
caller's header does win, the body must not be gzipped, or the header and the bytes disagree and
the destination decodes garbage.

#### Acceptance Criteria:

- [ ] `HTTPSink(gzip=True, headers={"Content-Encoding": "identity"})` sends
      `Content-Encoding: identity` and a body that is **not** gzipped, asserted by parsing the body
      as JSON rather than by its length.
- [ ] `HTTPSink(gzip=True)` with no caller header still sends `Content-Encoding: gzip` and a body
      `gzip.decompress` reads.
- [ ] A per-request `extra_headers` `Content-Encoding` — beneath the caller's own — does **not**
      suppress gzip, since the precedence order `extra_headers` < caller headers is unchanged.

---

## Data Model

No new types. One new module-level object and one new class in `http.py`:

```python
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl) -> None: ...

def _no_redirect_opener() -> Callable[..., Any]: ...   # built per sink, not at import
```

A first draft of this section put the opener in a module-level constant. It is built per
construction instead: `python.md` forbids import-time work, and `build_opener` snapshots the proxy
environment, so an import-time opener ignores a proxy the application sets after import.

Counters that move where they did not before, all already public through `losses()`:

| Sink | Counter | New reason it moves |
|---|---|---|
| `HTTPSink` | `failed` | a `3xx` response |
| `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink` | `failed` | a client exception on one chunk's final attempt |
| `KinesisSink` | `dropped_oversized` | data + key bytes over the per-record ceiling |
| `GooglePubSubSink` | `failed` | futures still in flight when `close()`'s bound expired |

`RotatingFileSink` deliberately gains none (FR-006).

## API / Interface Contract

**No public signature changes and no new constructor refusals** — every construction-time refusal
is SPEC-049's. The observable changes are to what reaches the wire and to which counters move.

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/sinks/
├── http.py            # FR-001 opener, FR-007 gzip header precedence
├── sqs.py             # FR-002
├── sns.py             # FR-002
├── kinesis.py         # FR-002, FR-003
├── firehose.py        # FR-002
├── pubsub.py          # FR-004
├── sentry.py          # FR-005
└── file.py            # FR-006
tests/
├── test_sinks_http.py, test_sinks_sqs.py, test_sinks_sns.py, test_sinks_kinesis.py,
├── test_sinks_firehose.py, test_sinks_pubsub.py, test_sinks_sentry.py, test_sinks_file.py
└── test_sink_losses.py     # the four-sink guard roster (FR-002)
```

## Implementation Phases

### Phase 1: The two blocking defects (FR-001, FR-002)

- The no-redirect opener, asserted against a real two-origin HTTP server — every existing HTTP
  test injects a fake opener and would not exercise the fix at all.
- The per-chunk guard inside each `_send`, with `SQSSink`'s `recoverable_loss` fed by it, and the
  roster test over the four.

### Phase 2: The bounded and stranded closes (FR-004, FR-005)

- Factor Pub/Sub's deadline loop out of `flush()`, call it from `close()` and from the
  flush-races-close branch, count the remainder, correct `_resolve`'s docstring.
- `SentrySink.close()` calls `self.flush()` inside an absorbing guard.

### Phase 3: The wire and file residues (FR-003, FR-006, FR-007)

- Kinesis byte-charging in all three places.
- Rotation-failure absorption and stream reopen.
- gzip header precedence.

### Phase 4: Mutation-verify the guards whose failure would be silent

- Re-plant each defect and confirm the new test reddens: the followed redirect, the unguarded
  chunk call, `SQSSink`'s unfed `recoverable_loss`, the unbounded close, the missing SDK flush, the
  propagating rotation failure. Assert the **reason** in every case — a count or an exit code
  passes against the neighbouring code path.

## Revision history

The first draft was revised, not replaced, after its spec review. Three blocking findings: its
*Out of Scope* deferred construction-time validation to SPEC-049 while FR-006 required two such
refusals, and SPEC-049 did not name them (now both are SPEC-049 FR-004, and this spec's Out of
Scope says so explicitly); FR-002 claimed each of the four sinks' total-failure test was
"unchanged", which is false for `SQSSink`, whose `recoverable_loss` term the guard would have left
`False` — silently losing a wholly-failed batch; and FR-002 left the Kinesis/Firehose `unknown`
term undecided, which is an Open Question in declarative clothes. Also corrected: the Elasticsearch
half of the audit's R15 claim, which does not reproduce — `json.dumps` escapes the `_bulk` action
line correctly and an index name the server rejects is already counted per item, so the mechanism
is not frame corruption and the residue moved to SPEC-049; a reference to a method `HTTPSink._request`
that does not exist; a Kinesis key charged in characters rather than UTF-8 bytes; and a Sentry
idempotence criterion that would have re-created the defect it was written to close.
