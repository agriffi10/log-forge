---
type: Spec
id: SPEC-070
title: "Retry Classification: What a Sink Retries, Per Exception, Per Entry and for an Unreadable 2xx"
description: "Makes every retry loop in the sinks ask a declared predicate whether an exception is worth another attempt, classifies a rejected queue entry per service from the vendor's documented codes, counts a 2xx whose body could not be read in every sink that reads a body, and records the deliberate exceptions in one table."
spec_status: Draft
depends_on: [SPEC-061, SPEC-066, SPEC-068]
arc: stability-redesign
arc_order: 2
---

## Overview

Every sink retries by its own rule, and the rules disagree on the question "is this failure worth
another attempt?". Ten SDK and driver loops retry **any** exception, so a `TypeError` raised by the
caller's own code, a validation error the server will give again and a constraint the data breaks
each cost `max_retries + 1` attempts and their backoff before they are counted; the HTTP family and the
socket transport retry transport errors only, and let any other exception escape `emit` raw and
uncounted. Among the queue services, `SQSSink` treats a sender fault as permanent while `SNSSink`
retries it, though the vendor's reply carries the same required field in both. And a `2xx` whose
body could not be read is counted by two sinks and reported as success by a third, because the hook
that receives the body cannot tell an unreadable one from an empty one.

This spec gives each question one answer, on today's loops. A retry loop asks a predicate its sink
declares, as `HTTPSink._retryable_status` already does for statuses; an exception the predicate does
not name fails at once, counted and announced on the first attempt. A rejected entry is classified
once per service, from the vendor's documented error codes. A body that failed to read is counted
everywhere a body carries the verdict. The three deliberate departures the plan names, and one that
survives this spec, are recorded in a single table with a roster that fails when a fifth appears.
The predicate is a plain callable with a defined shape and a defined home, so that SPEC-072's engine
can take it as an argument when it moves the loops; this spec does not move them.

Every figure is anchored to `fda134a` (`origin/main`, the tree this spec was drafted on):

- **Twelve retry loops.** An AST scan of `src/log_foundry/sinks/*.py` for a `for` loop over
  `range(...)` whose argument names `retries`, listing each loop's `except` types, finds twelve:
  `SocketTransport._send_one` (`OSError`), `HTTPSink._send` (`URLError`, `OSError`,
  `http.client.HTTPException`) and ten that catch `Exception` — `ClickHouseSink._insert`,
  `AzureEventHubsSink._send`, `FirehoseSink._send`, `KinesisSink._send`, `MongoDBSink.emit`,
  `PostgresSink._insert_batch`, `RabbitMQSink._publish`, `_RedisSink.emit` (both Redis sinks),
  `SNSSink._send` and `SQSSink._send`. `KafkaSink`, `NATSSink`, `GooglePubSubSink`, `SentrySink`,
  `SQLiteSink` and `FileSink` have no such loop.
- **Any exception is retried.** `RedisListSink` over a client whose `pipeline().execute()` raises
  `TypeError` makes **4** client calls at `max_retries=3` and counts `failed=1`, stderr reading
  `RedisListSink, 4 attempts, TypeError`. `HTTPSink` over an opener raising `RuntimeError` raises
  `RuntimeError` out of `emit` with `losses()` at `failed=0`.
- **A permanent failure is retried against a real server.** Against a throwaway container of the
  repo's postgres image (PostgreSQL 17.11, psycopg 3.3.8), a caller-owned table whose `level`
  column is `VARCHAR(5)`, and `PostgresSink(table=..., create_table=False, max_retries=1).emit` of
  two events at levels `info` and `warning`: the insert raised `StringDataRightTruncation`
  (SQLSTATE `22001`, a `DataError`) and was retried, stderr `lost 2 event(s); PostgresSink, 2
  attempts, StringDataRightTruncation`, `failed=2`, `SinkDeliveryError` raised; a one-event `info`
  batch landed (positive control). The retry fails the same way. This is a failure every
  assembled event can reach: the event is valid, the caller's schema refuses it.
- **One reply field, two answers.** The vendors' API references, read on 2026-11-04: SQS
  `BatchResultErrorEntry` (`https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_BatchResultErrorEntry.html`)
  and SNS `BatchResultErrorEntry` (`https://docs.aws.amazon.com/sns/latest/api/API_BatchResultErrorEntry.html`)
  both carry `SenderFault`, a required `Boolean` "specifying whether the error happened due to the
  caller of the batch API action". `SQSSink._send` reads it; `SNSSink._send` retries every entry
  named in `Failed`. Kinesis `PutRecords`
  (`https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html`) states a record's
  `ErrorCode` "can be one of the following values: `ProvisionedThroughputExceededException` or
  `InternalFailure`", and Firehose `PutRecordBatch`
  (`https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html`) that it "is
  one of the following values: `ServiceUnavailableException` or `InternalFailure`" and, for an
  internal error or a timeout, that "the write might have completed or it might have failed" and to
  "retry the request, resending only those records that might have failed processing". The two
  documented sets are each transient. Request-level failures that are the caller's fault
  (`InvalidArgumentException`, `ResourceNotFoundException`, the KMS access errors; HTTP 400 on every
  one) are separate whole-request errors on those pages, not per-record codes.
- **A failed count the entries contradict is read as success.** A probe of `KinesisSink` and
  `FirehoseSink` (`max_retries=1`, a two-event batch) with three responses — a failed count of `0`
  whose entries both carry `ErrorCode: InternalFailure`, the same entries with the count absent,
  and a count of `2` with no entry coded — returned from `emit` after one call every time, with
  `losses()` at `(0, 0)` and nothing on stderr. The first two never reach `adjudicate_positional`:
  `if not response.get("FailedRecordCount"): return sent` (`kinesis.py`, and `FailedPutCount` at
  its `firehose.py` twin) returns first; the third reaches it with a full-length array and no coded
  entry, so nothing is retried and the chunk counts as sent. Invariant 2's "nothing is lost
  silently" fails on all three.
- **Four readers of a body.** The `HTTPSink` subclasses that define `_handle_response` are
  `ElasticsearchSink` (`OpenSearchSink` inherits it), `HoneycombSink` and `OTLPSink`
  (`tests/test_sinks_http_body_read.py`'s `_BODY_READERS`). The first two count a `b""` as
  unadjudicated; `OTLPSink` returns success for it. `HTTPSink._attempt` hands all three `b""` for an
  empty body and for one that failed to read.

This spec builds on SPEC-061 (NATS liveness), SPEC-066 (the numeric doors, which route every
`max_retries` floor through `floor_count`) and SPEC-068, which have landed, in 1.1.0, and are in
`depends_on`. SPEC-061 adds no retry loop to `NATSSink`
and raises `SinkDeliveryError` where it raised before, so NATS declares no predicate here. SPEC-066
changes how `max_retries` is judged and nothing at the catch. SPEC-069, which lands first in the
arc, moves encoding ahead of each bulk call; this spec is correct with or without it, because the
predicate sits at the catch and sees whatever the call raises.

**What the change does not do.** `emit`'s contract is unchanged: a sink that delivered nothing still
raises, and the worker still re-sends a raised batch up to its own `max_retries`. A non-retryable
exception therefore ends the *sink's* loop at the first attempt and is counted once per `emit`
call; whether the worker should re-send it at all is the verdict SPEC-072 introduces.

## Scope

### In Scope

- `src/log_foundry/sinks/_classify.py`, a leaf module: the predicate shapes, the helpers and the
  AWS predicate and entry classifications the sinks share (FR-001, FR-003).
- A `_retryable_error` predicate on each of the twelve loop classes, the loops rewritten to ask it,
  and a derived roster that holds the twelve to it (FR-001, FR-002).
- The vendor exception classes each driver sink retries, each cited to the driver's source or
  documentation (FR-002).
- Entry classification for SQS, SNS, Kinesis and Firehose, including what an undocumented
  per-record code means, and a Kinesis or Firehose response whose failed count contradicts its
  entries counted unadjudicated rather than read as success (FR-003).
- A read-failed flag the three body readers consult, and OTLP counting an unreadable `2xx`
  (FR-004).
- The table of declared exceptions and its roster, the decision files, invariant, README and
  release-notes edits (FR-005).
- Five FRs, inside the 3-6 band; the arc split puts this spec's subject, classification, ahead of
  the engine that consumes it.

### Out of Scope

- **Moving the loops, the verdict, a `deliver` method or retry-after handling.** SPEC-072 and
  SPEC-073 build the engine; the loops stay where they are and keep their own backoff.
- **Teaching the worker not to re-send a batch a sink raised for a permanent reason.** A
  `SinkDeliveryError` from a non-retryable failure is still re-sent by the worker; the decision
  *A protocol's retry set is a private predicate hook, not a second loop* rejected that change as
  a contract change for every sink, and the verdict SPEC-072 adds is where it lands.
- **The unreadable-response method shared by the three readers.** SPEC-073 builds it on this
  spec's flag; this spec adds the flag and changes OTLP's reading, and leaves the three copies of
  the counting in place.
- **Id-keyed batch parsing and a malformed `Failed` entry.** SQS and SNS keep their hand-written
  parsing (SPEC-074); a `Failed` item that is not a mapping still escapes `emit` as the SNS
  `emit` docstring says. (A Kinesis or Firehose response whose failed count disagrees with its
  coded entries is **in** scope, FR-003.)
- **Encoding errors, SQLite and `FileSink` errors, the syslog pid** (SPEC-069), and the shared
  validators and chunkers (SPEC-071).
- **Retry loops that do not exist.** Kafka, NATS, Pub/Sub, Sentry's SDK backend, SQLite and
  `FileSink` gain no loop and no predicate.
- **Publishing `_retryable_error`, the flag or `_classify` as public names.** All are private under
  `1.x`; a third-party subclass is unaffected (FR-004).
- **Changing a floor, a refusal, a retry-status set or a close rule.** FR-005 records four
  deliberate divergences and changes none of them.

### Twin paths (invariant 6)

- **Worker and orphan paths.** Both call `emit`, and the change sits below it. One non-retryable
  failure is driven through a real `Worker` and through an orphan call and the ledgers compared
  (FR-001).
- **The twelve loops.** One roster derives them from the AST and requires each to ask its predicate,
  so a thirteenth cannot keep the old catch (FR-001).
- **SQS and SNS.** One `sender_fault` function serves both, and a roster asserts neither reads the
  field itself (FR-003).
- **Kinesis and Firehose.** Same classification, same tests, one parametrisation (FR-003).
- **The four body readers and the base.** Every reader is swept for an unreadable body; the base
  class and a reader-less subclass are swept for none (FR-004).

---

## Functional Requirements

### FR-001: A retry loop asks its sink's predicate, and what the predicate does not name fails at once

#### Description:

`_classify.py` defines `ErrorPredicate = Callable[[Exception], bool]` (True: another attempt may
succeed), `EntryPredicate = Callable[[Mapping[str, object]], bool]`, and the helpers in the Data
Model. Each of the twelve loop classes defines `_retryable_error(self, err: Exception) -> bool`, and
its loop catches `Exception`, runs the cleanup it runs today (`RabbitMQSink._reset`,
`PostgresSink._rollback`, `SocketTransport._reset`), and retries only when the attempt is not the
last **and** the predicate says so. Otherwise it takes the exit it takes today when attempts run
out: the same counter, the same `_diag` line, the same return or `SinkDeliveryError`, with the
attempt count the truth (`attempt + 1`, where ten loops wrote `max_retries + 1`, which would read
`4 attempts` for a failure that made one — invariant 11).

`HTTPSink._retryable_error` is the three-class tuple its `except` names today; `SocketTransport`'s is
`OSError` whose `errno` is outside `_PERMANENT_ERRNOS`, folding that set, which the socket loop
checks inline today, into the predicate. Both loops now catch `Exception`, so a non-transport
exception that escaped `emit` raw and uncounted is counted and abandoned at once (measured above:
`RuntimeError` through `HTTPSink`, `failed=0`).

`HTTPSink._send` has two callers, `HTTPSink._deliver` and `SentrySink._post_envelope`
(`sentry.py`, through its held `HTTPSink`), and the widened catch reaches both. Sentry counts an
exception escaping `_send` on its own `.transport_errors` today, and moving that count to the held
sink's `.failed` would reclassify a documented counter in a minor release. So `_send` gains a
private keyword-only `count_unexpected: bool = True`; Sentry passes `False`, and then an exception
the predicate rejects is re-raised unchanged after its one attempt, uncounted by `_send`, for
Sentry's own handler to count on `.transport_errors` exactly as now. A transport error is retried
on both callers as today.

The predicate is **total**: it never raises, whatever the exception's attributes hold. `is_retryable`
calls a predicate and answers `False` when it raises, so a malformed `response` attribute degrades to
"fails at once", which is counted and announced with the original exception's type, never silent.
`KeyboardInterrupt`, `SystemExit` and `CancelledError` are not `Exception` and still reach the caller.
A failing-at-once exception takes no backoff wait (invariant 3).

A derived roster holds the loops to the rule, in the manner of `tests/test_error_path_rules.py`: it
walks `src/log_foundry/sinks/*.py` for every `for` over `range(...)` naming `retries`, requires each
enclosing class to define `_retryable_error` and each loop's handler to call
`is_retryable(self._retryable_error, <the caught exception>)`, and pins a floor of twelve, the count
measured above, so a refactor cannot shrink the population silently. A direct
`self._retryable_error(err)` does not satisfy it: that call bypasses the wrapper that makes the
predicate total, so a predicate that raised would escape the handler. Its failure names the module,
class and line.

#### Acceptance Criteria:

- [ ] `_classify.py` exists, imports nothing from `log_foundry` at runtime (an AST test, like the
      other leaf helpers), and defines `ErrorPredicate`, `EntryPredicate`, `TRANSIENT_BUILTINS`,
      `driver_classes` and `is_retryable`.
- [ ] For each of the twelve loop classes, a client or transport call that raises `TypeError` is
      made **once**: no backoff wait is taken, the counter that exhaustion moves rises by the same
      amount, exactly one `_diag` line names `TypeError` and says `1 attempt`, and the sink returns
      or raises as it does on exhaustion. (Redis: 1 call where the same probe made 4 at
      `fda134a`.)
- [ ] For each of the twelve, an exception its predicate accepts (FR-002 names them) is attempted
      `max_retries + 1` times and the line reports that count.
- [ ] `HTTPSink` over an opener raising `RuntimeError` makes one request, counts `failed=1`,
      announces it and raises `SinkDeliveryError` for a one-chunk batch; `SocketTransport` over a
      socket raising `ValueError` makes one send and counts `failed=1`. `OSError(errno.EMSGSIZE)` is
      still one send and `OSError(errno.ECONNREFUSED)` is still `max_retries + 1`.
- [ ] `SentrySink` on its HTTP backend, over an opener raising `RuntimeError`, makes one request
      and moves `.transport_errors` by 1 and the held sink's `.failed` by 0, with the same
      `SentrySink, RuntimeError` line as at `fda134a`; over one raising `URLError` it makes
      `max_retries + 1` requests and moves `.failed` by 1, as today.
- [ ] `RabbitMQSink._reset`, `PostgresSink._rollback` and `SocketTransport._reset` are each called
      exactly once for a `TypeError`, before the sink gives up.
- [ ] A predicate that raises is treated as not retryable: `is_retryable` returns `False`, the sink
      makes one attempt, and the counted line names the **original** exception's type, not the
      predicate's.
- [ ] A `KeyboardInterrupt` raised by each of the twelve loops' calls propagates unchanged and moves
      no counter, as the existing `KeyboardInterrupt` tests for the four AWS sinks assert.
- [ ] Through a real `Worker` with `max_retries=2`, a Redis client raising `TypeError` is called
      3 times (one per worker attempt, not 3 x 4), and an orphan `info()` outside a span makes 1
      call; both ledgers show `failed` moved once per call and no event delivered.
- [ ] The roster fails, naming the class, on a fixture loop that catches `Exception` and never asks
      the predicate, and on one that calls `self._retryable_error(err)` directly (the mutant planted
      against the shipped tree, too: one loop rewritten to the direct call reddens the roster); it
      stays silent on one calling `is_retryable(self._retryable_error, err)`; its floor is 12 and a
      fixture tree with 11 loops fails on the floor.
- [ ] Serves invariants 1, 2, 3, 6, 7 and 11: the failure is counted and announced once at the
      attempt that made it (2, 11), no wait is taken (3), `emit` still raises when nothing was
      delivered (7), a predicate cannot make the library fail the caller (1), and both delivery
      paths and all twelve loops answer the same way (6).

### FR-002: Each SDK and driver sink retries the vendor's transient exception classes and nothing else

#### Description:

Each driver sink's predicate names the classes its driver documents or, where the documentation is
silent, defines as transient in its own source. `driver_classes(module, *names)` resolves them from
`sys.modules` without importing the driver: an exception instance of a driver class proves its
module is loaded, so a sink given an injected client and no installed extra never imports one, and a
name the installed version lacks is skipped rather than raising. **Resolution is per call**, inside
the predicate, never cached at import or in `__init__`: the drivers are imported lazily (for
example `PostgresSink._connect` imports `psycopg` on first connect), so a set resolved earlier would
be `()` and stay `()` after the driver loaded, and every transient error would fail at once. Every predicate also accepts
`TRANSIENT_BUILTINS = (ConnectionError, TimeoutError)`, the leaf builtins an injected client or a
stand-in raises for a dropped connection or a timeout, and redis-py's own default `Retry` lists
`socket.timeout`, the same class.

Sources read at botocore 1.43.123, pymongo 4.19.0, redis-py 8.1.0, pika 1.4.4, azure-eventhub
5.15.1, clickhouse-connect 1.12.0 and psycopg 3.3.8, the latest releases when drafted; the extras
leg of CI runs the installed versions (the class-existence criterion below).

| Sink | Retried (besides `TRANSIENT_BUILTINS`) | Fails at once, by example | Source |
|---|---|---|---|
| SQS, SNS, Kinesis, Firehose | `botocore.exceptions.ConnectionError` and `HTTPClientError`; a `ClientError` whose HTTP status is `429`, `500`, `502`, `503` or `504`, or whose `Error.Code` is in `RequestTimeout`, `RequestTimeoutException`, `PriorRequestNotComplete`, botocore's throttled codes, or `KmsThrottled`, `KMSThrottling`, `KMSThrottlingException` | `ParamValidationError`, `NoCredentialsError`, a `ClientError` of `InvalidArgumentException`, `AccessDenied`, `ResourceNotFoundException`, any non-botocore exception | `botocore/retries/standard.py` (`TransientRetryableChecker`, `ThrottledRetryableChecker`), and the throttling codes each service's API reference names for the request |
| ClickHouse | `clickhouse_connect.driver.exceptions.OperationalError` | `DatabaseError` itself, `DataError`, `ProgrammingError`, `InterfaceError` | `driver/exceptions.py` (`OperationalError`: "not necessarily under the control of the programmer, e.g. an unexpected disconnect"); `driver/_backend/httpcommon.py` (`retryable_http_statuses = (429, 503, 504)`; `build_http_error` raises `OperationalError` once the driver itself retried); `http_sync.py` wraps a transport `HTTPError` in `OperationalError` |
| MongoDB | `pymongo.errors.ConnectionFailure` (so `AutoReconnect`, `NetworkTimeout`, `NotPrimaryError`, `ServerSelectionTimeoutError`, `WaitQueueTimeoutError`); any `PyMongoError` carrying the label `RetryableWriteError` | `OperationFailure`, `WriteError`, `DuplicateKeyError`, `WTimeoutError` (the write was applied), `InvalidDocument`, `DocumentTooLarge`, `InvalidOperation`, `ConfigurationError` | `pymongo/errors.py` (`AutoReconnect`: "the operation ... has not necessarily succeeded"); `mongo_client_shared.py` (labels a connection failure and every server `errorLabels`) |
| Redis (both sinks) | `redis.exceptions.ConnectionError` except its `AuthenticationError`, `AuthorizationError` and `ExternalAuthProviderError` subclasses (present in 8.1.0; skipped by `driver_classes` in a version that lacks it); `TimeoutError`; `TryAgainError`; `ClusterDownError` | `ResponseError` and the rest of its subclasses, `DataError`, the three credential subclasses | `redis/retry.py` (the default `supported_errors` are `ConnectionError`, `TimeoutError`, `socket.timeout`); `redis/cluster.py` retries `TryAgainError` and `ClusterDownError`; the three credential subclasses are narrowed out because a resend cannot fix a credential |
| RabbitMQ | `pika.exceptions.AMQPConnectionError` except `AuthenticationError`, `ProbableAuthenticationError`, `ProbableAccessDeniedError`, `IncompatibleProtocolError`, `ConnectionClosedByClient` and `ConnectionOpenAborted`; `ChannelWrongStateError` | `ChannelClosedByBroker`, `ProtocolSyntaxError` and its subclasses. (`NackError` and `UnroutableError` are not classified: `RabbitMQSink` enables neither `confirm_delivery` nor `mandatory`, so pika cannot raise them here, and a broker nack is a retryable outcome in RabbitMQ's publisher-confirms guide, not a permanent one.) | `pika/exceptions.py`; `ChannelWrongStateError` is what a publish on the channel a dropped connection left behind raises, and the loop's `_reset` then reconnects |
| Azure Event Hubs | `azure.eventhub.exceptions.ConnectionLostError`, `ConnectError` except `AuthenticationError`, `OperationTimeoutError` | `EventDataError`, `EventDataSendError`, `ClientClosedError`, a bare `EventHubError` | `azure/eventhub/exceptions.py` (`ConnectionLostError`: "in most cases the client will automatically retry"); `_transport/_pyamqp_transport.py` `_create_eventhub_exception` maps AMQP link and connection errors to those classes and everything unrecognised to a bare `EventHubError`, which `_handle_exception` raises for an AMQP message rejection as `EventDataSendError` |
| PostgreSQL | a `psycopg.errors.OperationalError` whose `sqlstate` is `None` (psycopg's own client-side failures: a failed connect, `ConnectionTimeout`, "the connection is closed"), **except** a connect failure whose message carries one of the three refusals below, or whose SQLSTATE class is `08` (connection exception), `40` (transaction rollback), `53` (insufficient resources) or `57` (operator intervention), plus `55P03` `LockNotAvailable` and `55006` `ObjectInUse` | a connect-time refusal (below); every other `OperationalError` — class `28` (`InvalidPassword`, `InvalidAuthorizationSpecification`), `54` (`ProgramLimitExceeded`), `27`, `2F`, `38`, `39`, `3B`, the rest of `55`, `58`, `F0`, `HV` — and `DataError` (`StringDataRightTruncation` included), `IntegrityError`, `ProgrammingError`, `InternalError`, `InterfaceError`, `NotSupportedError` | `psycopg/errors.py`. psycopg files every one of those classes under `OperationalError` (a walk of its subclasses by SQLSTATE class at 3.3.8), so the class alone does not separate a transient failure from a permanent one; the SQLSTATE classes retried are the ones PostgreSQL's *Appendix A* defines as connection, rollback, resource and operator conditions |

**A connect-time refusal is recognised by its text, because nothing structured carries it.**
`PostgresSink._insert_batch` reconnects inside its loop (`_reconnect_if_broken`), and psycopg
raises every failed connect as a plain `OperationalError("connection failed: …")`
(`psycopg/generators.py`, the `POLL_FAILED` branch). Measured at `fda134a` (psycopg 3.3.8 binary,
a throwaway container of the repo's postgres image, PostgreSQL 17.11): a wrong password, an
unknown user, an unknown database and a refused port each raised that class with `sqlstate=None`
and `diag.sqlstate=None`, and only the message differed — `FATAL:  password authentication failed`
for the first two, `FATAL:  database "nope" does not exist` for the third, `could not receive data
from server` for the refused port. So a connect-time `OperationalError` with `sqlstate is None`
whose message contains libpq's `FATAL:  password authentication failed`, `FATAL:  role "` followed
later by `" does not exist`, or `FATAL:  database "` followed later by `" does not exist` **fails at
once**: the server answered and refused, and a resend asks the same question of the same
configuration. Every other connect failure stays retryable, as today.

The limits are stated rather than hidden. The text follows the server's `lc_messages`, so a server
reporting in another language is not matched and falls back to today's retry: a false negative
costs only today's behaviour, never a lost event that a retry would have saved. A transient `FATAL`
— `the database system is starting up`, `sorry, too many clients already` — is deliberately not
matched. The match reads the message inside the predicate only; no diagnostic prints it, and the
stderr line still names `OperationalError` and the attempt count (invariant 11). A wrong password
at **construction** is not this path: `__init__` connects and raises to the caller, as today.

A non-retryable exception ends the loop at once, exactly as FR-001 says. The MongoDB loop keeps its
`writeErrors` adjudication ahead of the predicate, so a `BulkWriteError` that names failed documents
is still counted from `details`.

#### Acceptance Criteria:

- [ ] For each row, an exception of **each** listed retried class (raised from a stand-in module
      installed in `sys.modules`, since the unit suite runs without the extras) is attempted
      `max_retries + 1` times, and an exception of **each** listed fails-at-once class is attempted
      once. A class named as an exception to a retried class (`AuthenticationError`,
      `ExternalAuthProviderError`, `ConnectionClosedByClient`, and for PostgreSQL an
      `OperationalError` with `sqlstate="28P01"` or `"54000"`) is attempted once although its parent
      is retried; a PostgreSQL `OperationalError` with `sqlstate=None` whose text is none of the three refusals and
      one with `"40001"` are attempted `max_retries + 1` times.
- [ ] A `PostgresSink` whose held connection is broken, so that `emit` reconnects, over a stand-in
      `_connect` raising `OperationalError` (`sqlstate=None`) with each of the three measured texts
      — `… FATAL:  password authentication failed for user "postgres"`, `… FATAL:  role "nobody"
      does not exist`, `… FATAL:  database "nope" does not exist` — makes one connect, counts
      `failed` once for the batch, and writes one line naming `OperationalError` and `1 attempt`
      that contains none of the message text.
- [ ] The same stand-in with the refused-port text (`… could not receive data from server: …`),
      with `… FATAL:  the database system is starting up`, and with a German rendering of the
      password message (`… FATAL:  Passwort-Authentifizierung für Benutzer »postgres«
      fehlgeschlagen`) makes `max_retries + 1` connects.
- [ ] Each predicate resolves its classes per call: a predicate first called while the stand-in
      driver module is absent, then called again after it is installed in `sys.modules`, retries the
      driver's transient class on the second call.
- [ ] For each AWS sink, a `ClientError` is classified on its status and code: status `503` retried
      with any code; status `400` with `Code="ThrottlingException"` retried; status `400` with
      `Code="InvalidArgumentException"` attempted once; a `ClientError` whose `response` is `None`,
      has no `Error`, has a `Code` that is a `list`, or a status that is a `str` is attempted once and
      raises nothing out of the predicate.
- [ ] `PostgresSink` against the repo's postgres image, with `create_table=False` over a table whose
      `level` column is `VARCHAR(5)`, emitting `info` and `warning` events
      (`tests/integration/test_postgres.py`), makes one attempt, counts `failed=2` once, raises
      `SinkDeliveryError`, and stderr reads `1 attempt` and `StringDataRightTruncation` where
      `fda134a` read `2 attempts`. Run once with the service up; the criterion settles at that run.
- [ ] With `LOG_FOUNDRY_EXTRAS=1` (the integration workflow's extras leg), every class name a
      predicate resolves through `driver_classes` exists in the installed driver, so a rename cannot
      empty a set unnoticed; without the extras the test is skipped, as the existing
      `_EXTRAS_EXPECTED` tests are.
- [ ] `driver_classes` returns `()` for a module absent from `sys.modules` and for one mapped to
      `None`, skips a name the module lacks, and imports nothing: `sys.modules` has the same keys
      before and after.
- [ ] No test fake relies on a non-transient exception to reach a retry. The private
      `_Boom(Exception)` of `tests/test_sinks_{sqs,sns,kinesis,firehose}.py` is rebased on
      `ConnectionError`, so its name and the assertions that name it (`test_sinks_sqs.py` at the
      two `"_Boom" in` checks, `test_sinks_sns.py`, `test_sinks_kinesis.py`,
      `test_sinks_firehose.py`) are unchanged. A bare `RuntimeError` or `ValueError` fake in
      `tests/test_sinks_{clickhouse,eventhubs,mongodb,postgres,rabbitmq,redis}.py` or
      `tests/test_sink_losses.py` that drives a retry becomes `ConnectionError`, and an assertion
      naming the old type changes with it; one that tests the counted line rather than the retry —
      `test_sinks_postgres.py`'s "the diagnostic names the *insert's* failure" — keeps its
      `RuntimeError`, which FR-001 counts at one attempt, and changes only an attempts count it
      asserts. The build lists every assertion it edited in the PR body.
- [ ] Serves invariants 1, 2 and 7: a predicate cannot raise into the caller (1), a failure is
      counted once at the attempt that made it (2), and the raise-or-report shape of `emit` is
      unchanged (7).

### FR-003: A rejected queue entry is classified once per service, and a contradictory failed count is counted

#### Description:

`_classify.py` defines `sender_fault(entry)` as `bool(entry.get("SenderFault"))`, the truthiness
test `SQSSink._send` makes today, so `SQSSink` is unchanged byte for byte: absent, `None`, `False`,
`0` and `""` are retried, and `True`, `1`, `"true"` and `"false"` are permanent. A strict `is True`
was considered and rejected: botocore parses the documented `Boolean` shape to a Python `bool`, so
the two tests agree on every reply the real client returns and differ only on a fake's, and the
difference would be a behaviour change to `SQSSink` with nothing to buy. `SQSSink._retryable_entry`
is `not sender_fault`, now through the shared function, and **`SNSSink._retryable_entry` is the
same**: SNS's `BatchResultErrorEntry` is the same shape with the same required field, so a sender
fault there is a rejection a byte-identical re-send fails again. `SNSSink._send` therefore counts a
sender-fault entry on `failed` when the response arrives, announces it as `SQSSink` does (`rejected
as invalid (first code: <code>); not retried`), drops it from the next round and resends only the
rest, and `SNSSink.emit` takes `SQSSink`'s return shape from `_send`, `(delivered, recoverable
loss)`, so a chunk lost only to sender faults does not raise and one with a retryable loss beside
it still does.

`KinesisSink._retryable_entry` and `FirehoseSink._retryable_entry` return True for every record the
response marks failed, **documented or not**. The documented per-record codes are, for Kinesis,
`ProvisionedThroughputExceededException` and `InternalFailure`, and for Firehose
`ServiceUnavailableException` and `InternalFailure`; each is transient, so the vendor's whole
per-record list is retryable, and today's behaviour is the classification. For a code the pages do
not list, the rule is **retry, bounded by `max_retries`**, for three reasons: the permanent kinds of
failure on these services are reported as whole-request errors (`InvalidArgumentException` and the
KMS errors, HTTP 400), so the per-record channel carries what the service failed to do rather than
what the caller did wrong; the only remedy either page gives for a failed record is to resend it;
and the two mistakes are not symmetric — an undocumented permanent code costs at most `max_retries`
bounded, idle waits before it is counted failed, as every retried failure does, whereas an
undocumented transient code classed permanent loses records the service would have taken, counted
but unrecoverable. The same direction as `sender_fault`'s: what the vendor has not said is
permanent is not treated as permanent.

A record is failed exactly as today: its result carries a truthy `ErrorCode`. A non-string code is
a failed record with an unknown code, and is retried like any other.

**A failed count the entries contradict is unadjudicated, never success.** Each response carries
both a failed count (`FailedRecordCount`, `FailedPutCount`) and a per-record array, and the sinks
read the count first and stop when it is falsy (measured above: three contradictory shapes counted
nothing). `adjudicate_positional` gains a keyword-only `failed_count`, both sinks pass the
response's count and drop their short-circuit, and the rule is:

1. The chunk is **delivered** only when the count is the `int` `0` or absent **and** no usable
   result carries an `ErrorCode` (a `0` with no array at all stays delivered, as today).
2. It is **retried as today**, the coded records resent, only when the count is a positive `int`
   (a `bool` is not one), the array has one entry per record, and exactly `count` entries are coded.
3. **Every other combination is unadjudicated**: SPEC-018's path, the whole chunk counted on
   `dropped_unadjudicated`, one may-have-landed line, settled and not retried. That includes a count
   of `0` or none beside coded entries, a positive count beside fewer or more coded entries than it
   states, a count that is not an `int`, and today's length mismatch.

A count of `0` with coded entries is unadjudicated rather than retried because the two halves of
one reply disagree about which records landed, and a resend of the coded ones could duplicate
records the count says were taken — the reasoning *A positional response adjudicates all of a chunk
or none of it* records for a length mismatch.

The SNS sender fault becomes a further case beside those that
`docs/decisions/sink-delivery/a-sink-that-delivered-nothing-raises-one-that-delivered-something-reports.md`
records as staying silent ("an SQS sender fault"), a list SPEC-061 and SPEC-069 also extend; the build refines that entry in place with a
dated History entry, and invariant 2's list of losses counted but never retried and invariant 7's
list of batches that return having delivered nothing each name the SNS sender fault beside the SQS
one.

#### Acceptance Criteria:

- [ ] A SNS `Failed` entry with `SenderFault: True` is sent once: one `publish_batch` call, `failed`
      up by the entry count, one line naming the first `Code` and `not retried`, no backoff wait. A
      `Failed` list of one sender-fault and one `SenderFault: False` entry resends only the second.
- [ ] For both `SQSSink` and `SNSSink`, `SenderFault` absent, `None`, `False` and `0` are retried
      (`max_retries + 1` calls) and `True` and `1` are permanent (one call); `SQSSink`'s existing
      sender-fault tests pass unmodified.
- [ ] A `SNSSink` batch whose every chunk is rejected wholly as sender faults returns, with `failed`
      equal to the entry count and `flush()` truthy; a batch of one sender-fault chunk and one chunk
      lost to `SenderFault: False` past the retry bound raises `SinkDeliveryError`, as `SQSSink`'s
      twin test does.
- [ ] Through a real `Worker`, the sender-fault SNS batch is sent once per worker attempt and the
      `SQSSink` batch is sent once per worker attempt, with equal ledgers (the twin pair, invariant
      6).
- [ ] A roster derived from the AST asserts `SQSSink` and `SNSSink` read `SenderFault` only through
      `sender_fault`, and reports a fixture class that calls `.get("SenderFault")` itself.
- [ ] For Kinesis and Firehose, a failed record with each documented code is attempted
      `max_retries + 1` times; one with `"ValidationException"` (undocumented), one whose `ErrorCode`
      is `5`, and one whose code is a `list` are attempted `max_retries + 1` times too, and are
      counted `failed` once when the bound is spent.
- [ ] A Kinesis response entry with no `ErrorCode` is not failed and is not resent, as today.
- [ ] For both Kinesis and Firehose, two events, `max_retries=1`, one call each: a count of `0`
      with both entries coded `InternalFailure` is counted `dropped_unadjudicated=2`, announced once,
      not resent, and `emit` returns.
- [ ] The same with the count absent and both entries coded: counted `dropped_unadjudicated=2`,
      announced once, not resent.
- [ ] The same with a count of `2` and no entry coded: counted `dropped_unadjudicated=2`,
      announced once, not resent.
- [ ] Delivered and retried shapes are unchanged: a count of `0` (or absent) with no coded entry
      counts nothing; a count of `0` with no `Records` / `RequestResponses` array counts nothing; a
      count of `1` with exactly one coded entry resends that one record; a count of `True` or `"1"`
      is unadjudicated.
- [ ] Invariant 2's list of losses counted but never retried and invariant 7's list of batches that
      return having delivered nothing each name the SNS sender fault; the decision *A sink that
      delivered nothing raises; one that delivered something reports* names it as a further case,
      with a dated History entry; `docs/architecture.md`'s struck list and the README's two
      sender-fault paragraphs name `SNSSink` beside `SQSSink`.
- [ ] Serves invariants 2, 6 and 7: a sender fault is counted once and never retried, and a
      contradictory count is counted rather than read as success, so nothing is lost silently or
      delivered twice (2); the SQS and SNS twins answer alike (6); and a batch lost only to a
      permanent rejection returns reported while one with a recoverable loss still raises (7).

### FR-004: A 2xx whose body could not be read is counted in every sink that reads a body

#### Description:

`HTTPSink._attempt` returns, for a `2xx` whose body failed to read (the raise path and the
`response.length` path of its bounded read), an instance of `_UnreadBody(bytes)`, a private
`bytes` subclass that equals `b""`, and `_read_failed(payload)` answers whether a payload is one.
`_handle_response`'s **signature is unchanged**, so the flag travels in the value the hook already
receives. A keyword parameter `read_failed: bool = False` was the alternative; it is rejected
because a third-party override with the two-argument signature, or a `*args, **kwargs` wrapper
that forwards to one, raises `TypeError` out of `emit` exactly when a body fails to read, and
anything escaping `emit` is re-sent by the worker, which is the defect the decision *After a status
line the status decides* exists to prevent. An unchanged override sees `b""`, as under `1.x`; the
three shipped overrides ask `_read_failed`. `_UnreadBody()` has no content, and every ordinary
operation on it (`json.loads`, `len`, slicing) behaves as on `b""`.

`ElasticsearchSink` (and so `OpenSearchSink`) and `HoneycombSink` branch on the flag first and count
the chunk unadjudicated exactly as they count an empty or undecodable body today, so their reading
of an unreadable body no longer rests on `json.loads(b"")` raising. **`OTLPSink` counts it too**:
`_abandon_unadjudicated(sent)`, the same count and line it writes for an unreadable
`partialSuccess`, and returns True, so the chunk is settled and never re-sent. A body that is
**empty** is untouched: OTLP reads `b""` as success, because an OTLP `2xx` needs no
`partialSuccess`, and `ElasticsearchSink` and `HoneycombSink` count it, because their only verdict
is in the body. That difference is recorded as the fourth row of FR-005's table. The base class, `SentrySink`
and every other sink that ignores a response body keep reading a `2xx` as delivered: no body is
their verdict.

Non-`2xx` statuses are unaffected: a `503` whose body cannot be read is still retried by status,
a `413` still splits, a `400` is abandoned once. The status still decides before the body is asked.

**Recorded decisions this revises**, each rewritten in place by the build with a dated History
entry, and invariant 7 edited (the completion ritual's step for a reversal):

- `docs/decisions/sink-delivery/after-a-status-line-the-status-decides-a-2xx-whose-body-cannot-be-read-is-never-re-sent.md`:
  the last sentence of its fence ("`_handle_response` keeps `bytes`, so it cannot tell an empty body
  from an unreadable one"), the paragraph **"The residue: an override cannot tell an empty `2xx`
  body from an unreadable one"** (the residue shrinks to third-party overrides that do not ask the
  flag), and the sentence in the *Rejected* paragraph that "nothing needs to treat an unreadable
  body differently from an empty one today"; the rejected keyword parameter is added there.
  *Never re-send* is untouched.
- `docs/decisions/sink-delivery/a-2xx-this-sink-cannot-read-is-counted-and-the-bulk-reading-is-bulks-alone.md`:
  the fence's sentence that a body "that is empty, not JSON or not an object is full success"
  (the unreadable body is added as counted), the bold sentence **"A body that cannot be read at all
  is `b""`, and `b""` is full success here"** and the sentences that follow it down to "count the
  same `b""` unadjudicated", the "Proved through a real socket" sentences that end "full success,
  one POST, nothing counted", and the closing clause for a body cut after 2 MiB ("reaches the sink
  as `b""` and is full success").
- **Invariant 7's recorded exceptions**, the sentence beginning "An HTTP `2xx` is a verdict however
  its body ends": "delivered for a sink that ignores the body or, like OTLP, reads an empty one as
  success, unadjudicated for the `_bulk` sinks and `HoneycombSink`" becomes "delivered for a sink
  that ignores the body, unadjudicated for the `_bulk` sinks, `HoneycombSink` and `OTLPSink`, whose
  verdict is in the body", and the OTLP sentence after it gains "or whose body cannot be read".
- Superseded markers at the other sites still stating the old reading: the OTLP paragraph of
  `a-2xx-bulk-response-with-no-readable-verdict-is-counted-never-retried.md`, the sentence in
  `a-honeycomb-batch-response-is-a-per-event-verdict-read-all-of-a-chunk-or-none-of-it.md` that
  "`OTLPSink` reads `b""` the other way", and the docstrings of `HTTPSink._handle_response` and
  `_attempt` and of the three overrides. The README's `OTLPSink` paragraph never stated the old
  reading, so it gains a sentence instead: a `2xx` whose body could not be read is counted on
  `.dropped_unadjudicated` and never retried; an empty one is success.

#### Acceptance Criteria:

- [ ] For each of `ElasticsearchSink`, `OpenSearchSink`, `HoneycombSink` and `OTLPSink`, a `2xx`
      whose body fails to read (the three `_READ_ERRORS` of `tests/test_sinks_http_body_read.py`,
      a truncated `Content-Length` body and a truncated chunked one) is one request per chunk,
      counted unadjudicated (`failed` equals the events sent), announced, settled and never
      re-sent; `_COUNTS_UNREADABLE` equals `_BODY_READERS` in that test.
- [ ] A `2xx` with an empty body is success for `OTLPSink` (nothing counted, nothing announced) and
      unadjudicated for the other three, unchanged.
- [ ] A `HTTPSink` subclass fixture whose `_handle_response(self, payload, items)` has the
      two-argument signature, and one that is `def f(*args, **kwargs)` forwarding to it, receive
      `b""` for an unreadable body, are called without a `TypeError` and settle the chunk as
      delivered, as under `1.x`.
- [ ] `_UnreadBody() == b""`, `json.loads(_UnreadBody())` raises as `json.loads(b"")` does, and
      `_read_failed(b"")` is `False`, so an empty body and an unreadable one are told apart
      only where the flag is asked.
- [ ] A `503`, `413` and `400` each with an unreadable body behave as the existing cases in
      `tests/test_sinks_http_body_read.py` assert: retried by status, split, abandoned once.
- [ ] `tests/test_public_surface.py`'s calls to `_handle_response` pass unchanged, and `OTLPSink`'s
      test of `_handle_response` on hostile bytes (`test_handle_response_never_raises_on_hostile_bytes`)
      still passes: the hook never raises.
- [ ] Invariant 7's exceptions paragraph and both decision entries carry the rewrite above, and the
      README's `OTLPSink` paragraph carries the added sentence; no file under `docs/` or `README.md` still says OTLP reads an unreadable body
      as success (a `grep` the build runs and reports).
- [ ] Serves invariants 2 and 7: an unreadable `2xx` is counted exactly once and never re-sent, so
      nothing is lost silently and nothing is delivered twice (2), and it settles rather than raises
      (7).

### FR-005: The deliberate exceptions to the shared rules are one table, held by a roster

#### Description:

The shared rules have four deliberate departures. The table is a decision file,
`docs/decisions/sink-delivery/the-sinks-keep-four-declared-exceptions-to-their-shared-rules.md`,
and a test, `tests/test_declared_exceptions.py`, holds the same rows as data and checks each against
the tree. A row holds: the shared rule it departs from, the classes that depart, how they depart,
why, the decision that records it and a derivation or a named test that proves the departure.

| Rule | Departing classes | Departure | Recorded in | Derived by |
|---|---|---|---|---|
| A timeout that is already broken is refused, one that works is floored | `KafkaSink` (`flush_timeout`), `NATSSink` (`publish_timeout`, and `drain_timeout` when one is passed, since SPEC-061) and `GooglePubSubSink` (`overflow_timeout`) floor it through `usable_timeout`; `PostgresSink` floors `connect_timeout` at 2 (`max(connect_timeout, _MIN_CONNECT_TIMEOUT)` at `fda134a`, `floor_count` since SPEC-066 landed in 1.1.0); every other timeout is refused through `require_timeout` | floor, not refuse, for 1.x | *A bad argument is floored if it works and refused if it is already broken; a new one is refused* | per sink class, each `__init__` parameter whose name ends in `timeout`, classed by the call that judges it (below) |
| A status is retried if it is `429` or `5xx` | `OTLPSink` retries `429`, `502`, `503`, `504` | a `500` is abandoned on the first request | *A protocol's retry set is a private predicate hook, not a second loop* | the classes defining `_retryable_status`; the existing roster in `tests/test_sinks_otlp.py` |
| A sink that released its transport refuses; a close mid-batch raises nothing by itself | `GooglePubSubSink` absorbs a close that lands mid-batch | the rest of the batch is counted unconfirmed, not raised | *A sink that released its transport refuses; one that released nothing keeps accepting* | `test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise` in `tests/test_sinks_pubsub.py`, which the roster requires to exist |
| A body that cannot carry the verdict is counted | `OTLPSink` reads an **empty** `2xx` body as success; the `_bulk` sinks and `HoneycombSink` count it | an OTLP success needs no `partialSuccess` | *A 2xx this sink cannot read is counted, and the `_bulk` reading is argued, never inherited* | `_handle_response` of each of the four readers on `b""` |

**How row 1 is derived.** For each class in `sinks/*.py` with an `__init__`, the scan takes every
parameter whose name ends in `timeout` and every call that receives it as a positional argument. A
call to `usable_timeout`, `max` or `floor_count` puts the class in the row. A call to a
**module-level function of the same module** is resolved one level: the scan reads that function's
body for the same three names, which is how `KafkaSink`'s `flush_timeout` reaches `usable_timeout`
through `kafka._usable_timeout`. Anything else — `require_timeout`, a keyword forwarded to another
constructor (`SyslogSink`, `LogstashSink`), a value handed to the driver unjudged (`NATSSink`'s
`connect_timeout`), a dict comprehension (`RabbitMQSink`) — keeps the class out
of the row. Measured with that scan at `fda134a`, the row is exactly the four classes named. The
stated bound: a floor reached through two helpers, or through a keyword, is not seen; the floor of
four in the AC below is what makes that bound fail loudly if a refactor moves one of the four out
of sight, rather than silently.

The fourth row is not in the plan's list of three. It is the one divergence that survives FR-004,
so a table that omitted it would be incomplete the day it was written. The decision's body is the
table with the reasoning of each row, and it ends in its dated History. The rows change no behaviour.

The roster asserts, per row: the derived population equals the declared classes; the named decision
file exists and its body names every declared class; and where a named test is the proof, that the
test is collected. It carries a fixture corpus: a fixture sink that overrides `_retryable_status`, a
fixture module calling `usable_timeout`, and a fixture with a deleted proof test are each reported
by the row they break, and the shipped tree is reported clean.

#### Acceptance Criteria:

- [ ] The decision file exists in `docs/decisions/sink-delivery/` with a fence of one line, the four
      rows and a dated History; `sh scripts/gen-index.sh` regenerates `docs/decisions/index.yaml`
      to list it, and `sh scripts/docs-lint.sh` passes.
- [ ] `tests/test_declared_exceptions.py` derives each row's population and finds
      `{KafkaSink, NATSSink, GooglePubSubSink, PostgresSink}`, `{OTLPSink}`, the Pub/Sub test and
      the four readers; none is a hand list that can shrink silently. `KafkaSink` is found through
      `kafka._usable_timeout`; row 1's population has a floor of four, so a refactor that hides one
      of them behind a second helper fails on the floor.
- [ ] Adding a fifth floor-side timeout, a second `_retryable_status` override or a fifth
      body reader that reads `b""` differently fails the roster naming the row; each of the roster's
      checks is mutation-tested once (the derived population emptied, the decision file renamed, the
      Pub/Sub test deleted) and reddens on its own reason.
- [ ] `tests/test_sink_retry.py::test_the_two_shipped_usable_timeout_callers_stay_on_the_floor_side`
      still passes; its name says two where `grep -nE '(^|[^_])usable_timeout\(' src/log_foundry/sinks/*.py`,
      less its `def` line, finds four calls (`NATSSink`'s `publish_timeout` and `drain_timeout`,
      `kafka._usable_timeout` and `GooglePubSubSink`'s `overflow_timeout`), four calls from three
      classes, and the build renames it to
      `test_the_four_shipped_usable_timeout_calls_stay_on_the_floor_side`.
- [ ] No behaviour changes: the full suite's tests of the three plan-named departures pass unmodified.
- [ ] Serves no invariant: it records and pins departures that FR-001 to FR-004's invariants already
      hold, and changes no behaviour a probe could observe.

---

## Data Model

```
# src/log_foundry/sinks/_classify.py            (leaf: imports nothing from log_foundry at runtime)
# The arc's one definition of the retry predicate: SPEC-072 and later specs use these two
# types and the two method names below as written here, and define none of their own.
type ErrorPredicate = Callable[[Exception], bool]
    # True: another attempt may succeed. Total: never raises.
type EntryPredicate = Callable[[Mapping[str, object]], bool]
    # True: a resend of this rejected entry may succeed.

TRANSIENT_BUILTINS: Final[tuple[type[Exception], ...]] = (ConnectionError, TimeoutError)

def driver_classes(module: str, *names: str) -> tuple[type[BaseException], ...]
    # The named classes of a driver module already in sys.modules; () for an absent or None
    # module; a missing name is skipped. Imports nothing.

def is_retryable(predicate: ErrorPredicate, err: Exception) -> bool
    # predicate(err), or False if the predicate raised.

def aws_retryable(err: Exception) -> bool          # FR-002: the four AWS sinks' predicate
def sender_fault(entry: Mapping[str, object]) -> bool   # FR-003: bool(entry.get("SenderFault"))

# on each of the twelve loop classes (private)
def _retryable_error(self, err: Exception) -> bool
# on SQSSink, SNSSink, KinesisSink, FirehoseSink (private)
def _retryable_entry(self, entry: Mapping[str, object]) -> bool

# src/log_foundry/sinks/http.py
class _UnreadBody(bytes): ...                      # equals b""; the flag FR-004 describes
def _read_failed(payload: bytes) -> bool           # isinstance(payload, _UnreadBody)
HTTPSink._send(..., *, count_unexpected: bool = True)   # FR-001; SentrySink passes False

# src/log_foundry/sinks/_batch.py
def adjudicate_positional[T](records, results, *, error_key="ErrorCode",
                             failed_count: object = _ABSENT) -> Adjudication[T]
    # FR-003's three-way rule when failed_count is passed; today's rule when it is not
```

## API / Interface Contract

No public name is added, changed or removed (invariant 10). SPEC-072's engine receives the exception
predicate as `sink._retryable_error` and the entry predicate as `sink._retryable_entry`, both plain
callables; a sink that defines neither has nothing retryable.

## File & Folder Structure

```
src/log_foundry/sinks/_classify.py            # new
src/log_foundry/sinks/{_socket,_batch,http,sentry,otlp,elasticsearch,honeycomb,sqs,sns,kinesis,
                       firehose,clickhouse,mongodb,redis,rabbitmq,eventhubs,postgres}.py   # edited
tests/test_classify.py                        # new: helpers, predicates, entries, the loop roster
tests/test_declared_exceptions.py             # new: FR-005's roster and corpus
tests/test_sinks_http_body_read.py            # edited: _COUNTS_UNREADABLE, _BODY_READERS docstring
tests/test_sinks_otlp.py                      # edited: the truncated-2xx cases now count
tests/integration/test_postgres.py            # edited: the VARCHAR(5) truncation case
docs/decisions/sink-delivery/the-sinks-keep-four-declared-exceptions-to-their-shared-rules.md   # new
docs/decisions/sink-delivery/a-sink-declares-what-it-retries-and-anything-else-fails-at-once.md # new
```

## Implementation Phases

### Phase 1: The shared module and the twelve loops (FR-001)

- Write `_classify.py` and its tests, then the loop roster with its fixture corpus, run first
  against `fda134a` to watch it redden on all twelve.
- Add `_retryable_error` to `HTTPSink` and `SocketTransport`, rewrite their two loops, and add the
  `TypeError`, cleanup, `KeyboardInterrupt` and worker-and-orphan cases, and pass
  `count_unexpected=False` from `SentrySink._post_envelope` with its pin. Re-run
  `tests/test_deferral.py`, `tests/test_worker_predicate_roster.py` and
  `tests/test_error_path_rules.py` and confirm each roster's population is unchanged.

### Phase 2: The vendor predicates (FR-002)

- Add the ten driver predicates and rewrite the ten loops, one sink at a time, changing each failing
  fake to a retried class in the same commit. Add the stand-in-module tests, the extras-leg class
  existence test and the PostgreSQL truncation integration case, and run the integration case against the
  repo's image.

### Phase 3: Entry classification (FR-003)

- Add `sender_fault`, move `SQSSink` onto it, give `SNSSink` the sender-fault rule and `SQSSink`'s
  `_send` return shape, and add the Kinesis and Firehose entry predicates and their parametrised
  tests. Add `failed_count` to `adjudicate_positional`, drop both sinks' falsy-count short-circuit,
  and add the three contradictory-count cases, run first against `fda134a` to watch them redden
  (all three counted nothing there). Refine *A sink that delivered nothing raises; one that
  delivered something reports* with a dated History entry. Update `README.md` (the sender-fault paragraphs and the excepted-cases paragraph),
  invariants 2 and 7 (an SNS sender fault joins the SQS one in both lists) and `architecture.md`'s
  struck list.

### Phase 4: The read-failed flag (FR-004)

- Add `_UnreadBody` and `_read_failed`, return it from `_attempt`, branch on it in the three
  overrides, and rewrite the tests that pinned OTLP's old reading (the truncated-`2xx` cases in
  `tests/test_sinks_otlp.py` and `_COUNTS_UNREADABLE`). Rewrite the two decisions, invariant 7, the
  README paragraph and the docstrings.

### Phase 5: The table, the decisions and the release notes (FR-005)

- Write the declared-exceptions decision and its roster with the corpus, mutation-test each roster
  check once, and write `a-sink-declares-what-it-retries-and-anything-else-fails-at-once.md` (the
  predicate rule, the vendor sets, the undocumented-code rule and the worker caveat). Refine *A
  client exception costs its chunk, and is provable non-delivery for it* and *A protocol's retry
  set is a private predicate hook, not a second loop* with dated History entries. Add the
  release-notes entries.

## Revision history

- Drafted against `fda134a`. The plan (`stability plan, Change 3`) named the three divergences and
  left open how the 2xx flag travels, what an undocumented per-record code means and where the table
  lives; this draft settles them as a payload-carried flag, retry-bounded, and a decision file plus a
  roster, and adds the fourth row.
- Revised after the spec review: the Kinesis/Firehose contradictory failed count came into scope
  (FR-003) on a probe showing three shapes counted nothing; the PostgreSQL predicate became an
  SQLSTATE-class allow-list after a walk of psycopg's `OperationalError` subclasses found classes
  `27`, `28`, `2F`, `38`, `39`, `3B`, `55`, `58`, `F0` and `HV` under it, with the connect-time
  authentication case stated as indistinguishable; `sender_fault` kept `SQSSink`'s truthiness
  rather than `is True`, which would have changed `SQSSink`; row 1 of FR-005 gained `PostgresSink`
  and a one-level helper resolution; the loop roster requires `is_retryable`; the predicate types
  were made the arc's one definition; `driver_classes` resolves per call; `SentrySink`'s call to
  `_send` was named and kept on its own counter; Redis's `ExternalAuthProviderError` was narrowed
  out and pika's `NackError` and `UnroutableError` dropped from the table.
- The PostgreSQL example changed from a NUL in `fields` (`UntranslatableCharacter`) to a caller's
  `VARCHAR(5)` column (`StringDataRightTruncation`), measured at `fda134a`, because SPEC-069 repairs
  every U+0000 before encoding, so the NUL never reaches the server once it lands.
- Cross-arc review: the SNS sender fault is "a further case" in the silent-cases list, with no ordinal, since SPEC-061 and SPEC-069 extend the same list.
- Andrew asked on 2026-11-04 that the PostgreSQL connect-time authentication case be folded in. It
  was stated as indistinguishable and retried; it now fails at once on libpq's three refusal texts,
  on the orchestrator's measurement at `fda134a`, with the localisation limit stated and the
  transient `FATAL`s left retryable.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
- 2026-11-05, drift review: row 1 adds `NATSSink`'s `drain_timeout`, which SPEC-061 floors through `usable_timeout`, and drops it from the unjudged examples; the `usable_timeout` call count is re-measured as four on `93113e1b`.
- 2026-11-05, execution-frame review: the renamed test says "four calls", from three classes, not four callers.
