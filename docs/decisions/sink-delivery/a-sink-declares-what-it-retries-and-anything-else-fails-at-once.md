---
type: Decision
title: "A sink declares what it retries, and anything else fails at once"
description: "every sink retry loop asks `is_retryable(self._retryable_error, err)` and retries only what its sink's predicate names: the driver's documented transient classes, resolved per call and never imported, plus builtin `ConnectionError` and `TimeoutError`. Anything else, a predicate that raises included, is counted and announced at its first attempt with no backoff; `emit` still raises when nothing landed, so the worker still re-sends it. (SPEC-070)"
---

**A sink declares what it retries, and anything else fails at once** — before SPEC-070 ten SDK and
driver loops caught `Exception` and retried it, so a `TypeError` from the caller's own client, a
validation error and a constraint the data breaks each cost `max_retries + 1` attempts and their
backoff before they were counted: `RedisListSink` made 4 calls for one `TypeError`, and
`PostgresSink(max_retries=1)` sent a `VARCHAR(5)` truncation (`StringDataRightTruncation`) to a real
PostgreSQL 17.11 twice, at `fda134a`. The HTTP family and the socket transport erred the other way:
they caught transport errors only and let any other exception escape `emit` raw and uncounted, so
an opener raising `RuntimeError` left `failed=0`.

**The rule.** Each of the twelve retry loops — `SocketTransport._send_one`, `HTTPSink._send`, and
the loops of `ClickHouseSink`, `AzureEventHubsSink`, `FirehoseSink`, `KinesisSink`, `MongoDBSink`,
`PostgresSink`, `RabbitMQSink`, `_RedisSink` (both Redis sinks), `SNSSink` and `SQSSink` — catches
`Exception`, runs the cleanup it always ran (`_reset`, `_rollback`), and retries only when another
attempt remains **and** `is_retryable(self._retryable_error, err)` says so. Otherwise it takes the
exit it takes when attempts run out: the same counter, the same `_diag` line, the same return or
`SinkDeliveryError`, with the attempt count the truth, `attempt + 1`. The six loops that wrote a
bare `N attempts` now write `N attempt(s)` like the rest. A failure that fails at once takes no
backoff wait (invariant 3). `KeyboardInterrupt`, `SystemExit` and `CancelledError` are not
`Exception` and still reach the caller.

**The predicate is a plain callable with one definition.** `sinks/_classify.py`, a leaf module,
defines `ErrorPredicate` (`Callable[[Exception], bool]`, True when another attempt may succeed),
`EntryPredicate` for a rejected batch entry, `TRANSIENT_BUILTINS`, `driver_classes`, `is_retryable`,
`aws_retryable` and `sender_fault`. SPEC-072's engine takes `sink._retryable_error` and
`sink._retryable_entry` as arguments and defines no predicate of its own. **It is total**:
`is_retryable` answers False when the predicate raises or its answer cannot be read as a truth
value, so a malformed attribute on an exception degrades to "fails at once", which is counted and
announced with the original exception's type. A direct `self._retryable_error(err)` would bypass
that wrapper, which is why the loop roster refuses it.

**Driver classes are resolved per call, from `sys.modules`, never imported.** `driver_classes(module,
*names)` returns the named classes of a module already loaded, `()` for one absent or mapped to
`None`, and skips a name the installed version lacks. An exception instance of a driver class proves
its module is loaded, so a sink given an injected client with no extra installed imports nothing.
The drivers are imported lazily — `PostgresSink._connect` imports `psycopg` on first connect — so a
set resolved at import or in `__init__` would be `()` and stay `()`, failing every transient error
at once. Every predicate also accepts the builtin `ConnectionError` and `TimeoutError`, what an
injected client or a stand-in raises for a dropped connection or a timeout.

**What each sink retries**, read at the locked driver versions:

| Sink | Retried (besides the builtins) | Fails at once, by example | Source |
|---|---|---|---|
| `HTTPSink` and subclasses | `URLError`, `OSError`, `http.client.HTTPException` | a `RuntimeError` or an `OverflowError` an opener raises | the three classes its `except` named before |
| `SocketTransport` | an `OSError` whose errno is not in `_PERMANENT_ERRNOS` | `EMSGSIZE`, a `ValueError` from a closed socket object | SPEC-038's permanent-errno set (`EMSGSIZE`), now inside the predicate |
| SQS, SNS, Kinesis, Firehose (`aws_retryable`) | botocore's `ConnectionError` and `HTTPClientError`; a `ClientError` whose status is `429`, `500`, `502`, `503` or `504`, or whose `Error.Code` is a transient or throttling code | `ParamValidationError`, `NoCredentialsError`, a `ClientError` of `InvalidArgumentException`, `AccessDenied` or `ResourceNotFoundException`, a malformed `ClientError` | `botocore/retries/standard.py` at 1.43.119 (`TransientRetryableChecker`, `ThrottledRetryableChecker`), copied as a cited literal because the source is a private attribute; the three KMS throttling codes from the services' API references |
| `ClickHouseSink` | `OperationalError` | `DatabaseError` itself, `DataError`, `ProgrammingError`, `InterfaceError` | clickhouse-connect 1.4.2 `driver/exceptions.py`; `driver/httpclient.py` raises `OperationalError` for a transport error and for a `429`, `503` or `504` it has already retried |
| `MongoDBSink` | `ConnectionFailure` and its subclasses; any `PyMongoError` labelled `RetryableWriteError` | `OperationFailure`, `WriteError`, `DuplicateKeyError`, `WTimeoutError`, `InvalidDocument`, `DocumentTooLarge`, `InvalidOperation`, `ConfigurationError` | pymongo 4.19.0 `errors.py`, `mongo_client_shared.py`; the `writeErrors` adjudication still runs ahead of the predicate |
| Redis (both) | `ConnectionError` except its `AuthenticationError`, `AuthorizationError` and `ExternalAuthProviderError` subclasses; `TimeoutError`; `TryAgainError`; `ClusterDownError` | `ResponseError` and its other subclasses, `DataError`, the three credential subclasses | redis-py 8.1.0 `retry.py` (the default `supported_errors`), `cluster.py` |
| `RabbitMQSink` | `AMQPConnectionError` except `AuthenticationError`, `ProbableAuthenticationError`, `ProbableAccessDeniedError`, `IncompatibleProtocolError`, `ConnectionClosedByClient` and `ConnectionOpenAborted`; `ChannelWrongStateError` | `ChannelClosedByBroker`, `ProtocolSyntaxError` | pika 1.4.4 `exceptions.py`; `NackError` and `UnroutableError` are unreachable, since the sink enables neither `confirm_delivery` nor `mandatory` |
| `AzureEventHubsSink` | `ConnectionLostError`, `ConnectError` except `AuthenticationError`, `OperationTimeoutError` | `EventDataError`, `EventDataSendError`, `ClientClosedError`, a bare `EventHubError` | azure-eventhub 5.15.1 `exceptions.py`, `_transport/_pyamqp_transport.py` |
| `PostgresSink` | an `OperationalError` whose SQLSTATE class is `08`, `40`, `53` or `57`, or which is `55P03` or `55006`; one with no SQLSTATE that is not a connect-time refusal | class `28`, `54`, the rest of `55` and every other `OperationalError`; `DataError`, `IntegrityError`, `ProgrammingError`, `InternalError`, `InterfaceError`, `NotSupportedError` | psycopg 3.3.6 `errors.py`, which files every server condition under `OperationalError`, so the SQLSTATE decides; PostgreSQL's *Appendix A* for the classes |

**PostgreSQL's connect-time refusal is recognised by its text**, because nothing structured carries
it: psycopg raises every failed connect as a plain `OperationalError` with `sqlstate=None`, and at
`fda134a` a wrong password, an unknown user, an unknown database and a refused port differed only in
their messages. A connect failure whose message carries libpq's `FATAL:  password authentication
failed`, `FATAL:  role "…" does not exist` or `FATAL:  database "…" does not exist` fails at once;
every other connect failure is retried. The limits are stated, not hidden: the text follows the
server's `lc_messages`, so a server reporting in another language is not matched and keeps the
retry, which costs only the old behaviour; a transient `FATAL` (`the database system is starting
up`, `sorry, too many clients already`) is deliberately not matched; the message is read inside the
predicate only and never printed (invariant 11). For the predicate to see it, `_reconnect_if_broken`
now **re-raises** the connect failure after its once-per-outage line, which gains `; the attempt
fails with it and is counted`. It used to absorb it, and the attempt then failed on the dead
handle's `the connection is closed`, which hid a refusal from any predicate.

**Rejected entries.** `sender_fault(entry)` is `bool(entry.get("SenderFault"))`, the truthiness
`SQSSink` always tested, and SQS and SNS both read the field only through it: SNS's
`BatchResultErrorEntry` carries the same required `SenderFault`, so an SNS sender fault is now
counted when the reply arrives, announced `SNSSink, rejected as invalid (first code: <Code>); not
retried`, and dropped from the next round, and a batch lost only to sender faults returns. A strict
`is True` was rejected: botocore parses the documented `Boolean` to a `bool`, so the two tests
differ only on a fake's reply, and the difference would have changed `SQSSink` for nothing. Kinesis
and Firehose retry **every** failed record, documented code or not, bounded by `max_retries`: each
vendor's documented per-record codes are transient, the permanent kinds of failure on those services
arrive as whole-request errors (`InvalidArgumentException`, the KMS errors), the only remedy either
page gives is to resend, and the two mistakes are not symmetric — an undocumented permanent code
costs at most `max_retries` bounded waits, while an undocumented transient code read as permanent
loses records the service would have taken. Their `_retryable_entry` answers True and is declared
for SPEC-072's engine; the loop does not ask it, since a branch for False would be dead code.

**`SentrySink` keeps its own counter.** `HTTPSink._send` serves `SentrySink._post_envelope` too, and
Sentry counts an exception escaping it on `.transport_errors`, a documented counter. So `_send` takes
a private keyword-only `count_unexpected=True`; Sentry passes False, and then an exception the
predicate rejects is re-raised unchanged after its one attempt, uncounted by `_send`. A transport
error is retried on both callers as before. The HTTP abandonment line keeps its `connection error,
<Type>` wording for any exception before a status line, a `RuntimeError` included, because the rule
is the same exit with the same line.

**The worker still re-sends a raised batch.** `emit`'s contract is unchanged: a sink that delivered
nothing still raises, and the worker re-sends a raised batch up to its own `max_retries`, so a
non-retryable failure costs one sink attempt per worker attempt (3 calls through
`Worker(max_retries=2)`, where it cost 3 × 4) and is counted once per call. Teaching the worker not
to re-send it is a contract change for every sink, rejected in *A protocol's retry set is a private
predicate hook, not a second loop*; SPEC-072's verdict is where it lands.

**Rejected: naming the transient classes in each `except` clause.** It cannot import a driver the
caller has not installed, it gives SPEC-072's engine nothing to take as an argument, and it would
have kept twelve copies of the rule rather than one shape. **Rejected: resolving the classes once.**
See above: a lazily imported driver would leave the set empty for good.

**Do NOT build** a retry loop that does not ask `is_retryable(self._retryable_error, err)`, a
predicate that imports its driver, or a cached class set. `tests/test_classify.py` holds the loops
to the rule with a roster derived from the AST (every `for` over `range(...)` naming `retries`, a
floor of twelve, a fixture corpus), holds SQS and SNS to reading `SenderFault` only through
`sender_fault`, and — on the integration workflow's extras leg — checks every class name a
predicate resolves exists in the installed driver, so a rename cannot empty a set unnoticed.

## History

### 2026-11-07

- **Settled** by SPEC-070 FR-001 to FR-003. The PostgreSQL reconnect's re-raise was found at build
  time: the spec assumed the connect failure reached the loop's handler, and it was absorbed.
