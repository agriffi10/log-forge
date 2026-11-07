# Completed Spec — SPEC-070: Retry Classification — What a Sink Retries, Per Exception, Per Entry and for an Unreadable 2xx

## What was completed?

Five FRs, built in `a05fb91b`..`042ddc23`, the second spec of the `stability-redesign` arc. The
principle: **a sink retries only what its destination documents as transient, and everything else
is counted once, at the attempt that made it.**

- **One predicate shape, one home** (FR-001): `sinks/_classify.py`, a leaf module, defines
  `ErrorPredicate`, `EntryPredicate`, `TRANSIENT_BUILTINS`, `driver_classes`, `is_retryable`,
  `aws_retryable` and `sender_fault`. Each of the twelve retry loop classes declares
  `_retryable_error`, and each loop catches `Exception`, runs its cleanup and retries only when
  `is_retryable(self._retryable_error, err)` agrees; anything else takes the exhaustion exit at its
  first attempt with no backoff, the line carrying `attempt + 1`. `HTTPSink` and `SocketTransport`
  now count what used to escape `emit` raw; `HTTPSink._send` gains a private
  `count_unexpected=True`, which `SentrySink` passes as False to keep its `.transport_errors`.
- **Vendor classes** (FR-002): each driver sink's predicate resolves its driver's transient classes
  per call from `sys.modules`, imports nothing, and accepts the builtin `ConnectionError` and
  `TimeoutError`. `PostgresSink` reads the SQLSTATE class and recognises libpq's three connect-time
  refusals by text; `_reconnect_if_broken` now re-raises the connect failure so the predicate sees
  it (a build-time finding, recorded in the spec's revision history).
- **Entries and counts** (FR-003): SQS and SNS read `SenderFault` only through `sender_fault`;
  `SNSSink` counts a sender fault once, never retries it, and returns `(accepted, recoverable loss)`
  from `_send` as `SQSSink` does. Kinesis and Firehose declare `_retryable_entry` (always True) and
  pass their failed count to `adjudicate_positional(failed_count=)`, whose three-way rule counts a
  contradictory reply unadjudicated; `_batch.ABSENT` and `describe_count` are new.
- **Unreadable body** (FR-004): `HTTPSink._attempt` returns a private `_UnreadBody(bytes)`, equal to
  `b""`, for a `2xx` whose body could not be read; `_read_failed` asks. The three readers branch on
  it first; `OTLPSink` now counts an unreadable body on `dropped_unadjudicated`.
- **Declared exceptions** (FR-005): four rows, held by `tests/test_declared_exceptions.py`.

Guarded by `tests/test_classify.py` (the loop roster with a floor of twelve, the twelve-loop sweep,
the vendor tables over `tests/driver_standins.py`, the sender-fault roster, the contradictory
counts, the extras-leg class-existence check), `tests/test_declared_exceptions.py`,
`tests/test_sinks_http_body_read.py`, `tests/test_sinks_sns.py` and the `VARCHAR(5)` case in
`tests/integration/test_postgres.py`.

Decisions: `docs/decisions/sink-delivery/a-sink-declares-what-it-retries-and-anything-else-fails-at-once.md`
and `docs/decisions/sink-delivery/the-sinks-keep-four-declared-exceptions-to-their-shared-rules.md`;
eight more rewritten in place or marked (History dated 2026-11-07). Invariants 2 and 7 widened.

## Measured base figures

From the spec, at `fda134a`: `RedisListSink` made 4 calls for one `TypeError` at `max_retries=3`;
an `HTTPSink` opener raising `RuntimeError` escaped `emit` with `failed=0`; a `VARCHAR(5)`
truncation was sent twice to PostgreSQL 17.11 at `max_retries=1`; three contradictory Kinesis and
Firehose replies each counted nothing.

## What changed from earlier specs?

- **SPEC-016**: `SNSSink` takes `SQSSink`'s sender-fault rule and its `_send` return shape.
- **SPEC-018**: a Kinesis or Firehose failed count its records contradict is unadjudicated.
- **SPEC-038**: the socket transport's permanent-errno check moves into its predicate.
- **SPEC-041**: a failed PostgreSQL reconnect is no longer absorbed; it fails the attempt.
- **SPEC-048**: the AWS `_send` guard retries only a transient exception.
- **SPEC-056 / #283**: `OTLPSink` counts an unreadable `2xx` body; an empty one is still success.
- **SPEC-059**: Probe D's tests raise from `_handle_response`, since an opener exception no longer
  escapes `emit`.
- `tests/test_sink_retry.py`'s `usable_timeout` test is renamed for the four calls it now covers.

## What changed for a caller

In `docs/release-notes/v1.2.0.md`: two *Fixed* entries (OTLP, Kinesis and Firehose) and two
*Changed* entries (retry classification, shared with SPEC-074; the SNS sender fault), with the
changed diagnostic strings. No public name, argument or default changes.

## Follow-ups

- **Owed until the PR's CI is green:** FR-002's `VARCHAR(5)` integration case and the extras-leg
  class-existence check run in `integration.yml`.
- **SPEC-072 onward:** the worker still re-sends a batch that raised for a non-retryable failure,
  one sink attempt per worker attempt; SPEC-072's verdict is where that ends.

## Verification

The tests named above; the gate results are recorded in the PR.
