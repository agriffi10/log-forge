# Spec: Sink Construction Validation and Driver Bounds

**ID:** SPEC-049
**Status:** Draft
**Last Updated:** 2026-09-02
**Depends On:** SPEC-026, SPEC-027, SPEC-041, SPEC-043, SPEC-047, SPEC-048

## Overview

A sink built with an unusable argument is accepted today and fails later, on a background thread,
in a way nothing counts. `HTTPSink(timeout=-1)` constructs happily and then raises a raw
`ValueError` out of every `emit` for the life of the process, moving no counter but
`health().failed_batches`. `ClickHouseSink(chunk_size=-5)` is worse: `emit` returns **normally**
having inserted nothing, because a negative chunk size makes the chunker yield no chunks at all
and the "nothing landed" test never fires. `RotatingFileSink(when="S", interval=0)` rotates on
every single event, and fifteen written events leave two on disk. In each case the caller is
standing right there at `configure()` when the mistake is made and hears nothing about it.

Beside that sits a second, quieter class: waits this library never asked the driver to bound. The
argument passes straight through to `pymongo`, `pika` or `clickhouse-connect`, whose own defaults
are `None`, `None` and `300` seconds — and every one of those waits happens on the worker's single
drain thread, which is the thread every other sink's delivery is queued behind. SPEC-041 bounded
`psycopg` for exactly this reason and SPEC-047 bounded `confluent-kafka` and `nats-py`; these three
were never reached.

This spec makes a bad sink argument fail where it is written, and gives the three remaining drivers
the treatment their siblings already have.

## Scope

### In Scope

- Construction-time refusal of degenerate timeouts, chunk sizes and rotation bounds across
  `HTTPSink`, `SocketTransport`, `ClickHouseSink`, `PostgresSink`, `GooglePubSubSink` and
  `RotatingFileSink`.
- Rejection of CR/LF in a caller's header name or value, and of a URL whose scheme is not
  `http`/`https`.
- Extending SPEC-043's rule — an argument no backend can use is an error, not a silent ignore —
  to `LogstashSink` and `SentrySink`, the two places it is still violated.
- Refusing whitespace in `SyslogSink(app_name=)` and `ElasticsearchSink(index=)`. These arrive
  here because **every** construction-time refusal is this spec's, whatever its consequence:
  SPEC-048's first draft kept them for their wire effect and its own Out of Scope forbade them,
  which is what a seam drawn on consequence rather than on timing produces.
- Bounding `pymongo`'s socket timeout and `pika`'s blocked-connection timeout, whose driver
  defaults are infinite; exposing `clickhouse-connect`'s `send_receive_timeout`, whose default is
  finite; and recording all three in `architecture.md` §13.
- The `sinks/` docstring and diagnostic residue the audit's standards sweep left: a missing
  section, a missing module docstring, a `@staticmethod`, four measured counts anchored to a spec
  rather than a commit, one abandonment line that overstates its attempt count, and one docstring
  that claims a failure it cannot observe.

### Out of Scope

- Anything that changes what reaches the wire when the configuration is valid — SPEC-048, built
  before this one.
- Normalising the sink family's argument *names* (`url`/`dsn`/`uri`, `chunk_size`/`max_pending`).
  The audit records that drift as frozen; this spec adds no new spelling and renames nothing.
- `**http_kwargs: object` on the seven platform sinks. Making those `Unpack[TypedDict]` is
  additive in 1.x and is not attempted here.
- Bounding `pymongo`'s `serverSelectionTimeoutMS` or `pika`'s `socket_timeout`/`stack_timeout`.
  Those defaults are finite (30 s, 10 s, 15 s); they are exposed and recorded, not overridden.
- The `tests/` tree's own scaffolding and its wall-clock budgets outside `test_sinks_*.py`.
- Validating an Elasticsearch index name beyond whitespace. The audit recorded whitespace there as
  frame corruption; it is not — `json.dumps` escapes the `_bulk` action line correctly and a name
  the cluster rejects already arrives as a counted per-item error. The refusal is kept because it
  turns a sink that fails every batch forever into a startup error, not because anything is silent,
  and the full index-name grammar is the cluster's to enforce.

---

## Functional Requirements

### FR-001: The HTTP family refuses an unusable timeout, a CRLF header and a non-`http` URL

#### Description:

`HTTPSink.__init__` stores `timeout`, `headers` and `url` verbatim. A `timeout` of `-1`, `nan` or
`inf` reaches `urlopen` and raises `ValueError` from inside `emit` on every batch, forever; CR or
LF in a header name or value does the same from `http.client`; and a URL whose scheme is not
`http`/`https` sends the batch somewhere `urlopen` will read a file from or an FTP server will
reject — `file:///etc/passwd` raises a raw `TypeError`, `ftp://` burns the full retry budget on
every batch. All three must be refused at construction with a `ValueError` naming the argument.

`timeout` is checked with the existing `_retry.usable_timeout` test — `not (0 < value < inf)`,
which is `NaN`-safe — but raises rather than falling back, because a sink's own network timeout is
not a value the library may silently substitute.

#### Acceptance Criteria:

- [ ] `HTTPSink(url, timeout=t)` raises `ValueError` for each of `-1`, `0`, `nan` and `inf`, and
      the message names `timeout`.
- [ ] `HTTPSink(url, headers={"X": "a\r\nInjected: 1"})` and `HTTPSink(url, headers={"X\r\nY": "1"})`
      each raise `ValueError` naming the header; a bare `\n` and a bare `\r` are refused too.
- [ ] `HTTPSink("file:///etc/passwd")` and `HTTPSink("ftp://h/x")` raise `ValueError` naming the
      scheme; `http://` and `https://` construct.
- [ ] Every sink built on `HTTPSink` inherits the refusal — a roster test over its subclasses
      asserts each one rejects `timeout=-1`, so a new subclass cannot opt out by accident.
- [ ] A valid construction is unchanged: the existing `HTTPSink` suite passes untouched.

### FR-002: Degenerate bounds are refused where they are written

#### Description:

Four more sinks accept a value that cannot work.

`SocketTransport(timeout=-1|nan)` raises `ValueError` from `socket.settimeout` inside `send_all`.
`ClickHouseSink(chunk_size=0)` raises from `range()`; `chunk_size=-5` is the silent one — the
chunker yields nothing, so `chunks` is `0`, the `if chunks and not inserted` test is `False`, and
a whole batch is discarded with `losses()` at zero. `PostgresSink(chunk_size=0)` spends four
attempts and three backoffs per batch failing the same way. `GooglePubSubSink(overflow_timeout=nan)`
makes `_out_of_time` compare against `nan`, which is `False` forever, so the deadline loop that
bounds `flush()` and `_await_overflow` has no bound at all.

Each is refused at construction with a `ValueError` naming the argument.

#### Acceptance Criteria:

- [ ] `SocketTransport(host, port, timeout=t)` raises `ValueError` for `-1`, `0`, `nan`, `inf`.
- [ ] `ClickHouseSink(chunk_size=n)` and `PostgresSink(chunk_size=n)` raise `ValueError` for `0`
      and for any negative `n`; a positive value constructs.
- [ ] A test asserts the pre-fix silent-loss shape is gone: constructing with `chunk_size=-5` is
      impossible, so no `emit` can return having discarded a batch with `losses()` at zero.
- [ ] `GooglePubSubSink(overflow_timeout=t)` raises `ValueError` for `nan`, `inf`, `0` and
      negatives.
- [ ] Every message names the offending argument and the value received.

### FR-003: `RotatingFileSink` refuses a rotation bound that destroys data

#### Description:

`RotatingFileSink` accepts `interval<=0`, `max_bytes<0` and `backup_count<0`. `interval=0` and
negatives put the rollover deadline permanently in the past, so `_should_rotate` fires on every
event: measured, three emits of five events left **two** lines on disk out of fifteen. A negative
`max_bytes` disables the size trigger by accident rather than by the documented `0`, and a
negative `backup_count` makes the retention loop a no-op while reading as "keep some".

All three raise `ValueError` at construction. `max_bytes=0` and `backup_count=0` stay valid: both
are documented switches, not mistakes.

#### Acceptance Criteria:

- [ ] `RotatingFileSink(path, when="S", interval=n)` raises `ValueError` for `0` and negatives;
      `interval=1` constructs.
- [ ] `RotatingFileSink(path, max_bytes=-1)` and `(path, backup_count=-1)` each raise
      `ValueError`; `max_bytes=0` and `backup_count=0` construct and behave exactly as documented.
- [ ] `interval` is validated even when `when=None`, so a caller who passes a bad interval without
      a unit is still told.
- [ ] The existing rotation suite passes unchanged.

### FR-004: An argument no backend can use is an error, not a silent ignore

#### Description:

SPEC-043 settled this and `NATSSink` implements it: passing a connect-time argument alongside an
already-connected `client=` raises rather than being dropped. Two sinks still violate it.

`LogstashSink(url=..., host=..., port=...)` builds the HTTP backend and discards `host`, `port`
and `transport` in silence, so a caller who meant the socket backend gets HTTP and no word about
it. It must raise when a socket-only argument accompanies `url=`.

Two more arguments are accepted that no backend can use. `SyslogSink(app_name=)` is interpolated
raw into a space-delimited RFC 5424 header (`syslog.py:229`), so whitespace shifts PROCID, MSGID and
STRUCTURED-DATA for every message — corruption no counter sees. `ElasticsearchSink(index=)` is
different and the difference is stated because the audit got it wrong: the action line is built with
`json.dumps` (`elasticsearch.py:88`), so the NDJSON frame stays intact and a name the cluster rejects
is counted per item. It is refused anyway, because an index name the cluster can never accept makes
every batch fail for the life of the process, and a startup error is the honest place to say so.

`SentrySink._parse_dsn` documents `Raises: ValueError: If the DSN cannot be parsed as a URL`, but
`urlparse` almost never raises: a DSN with no scheme, no host or no key yields an ingest URL of
`None://None/api//envelope/` and an auth header reading `sentry_key=None`, and the sink then posts
every event to a URL that cannot exist. The DSN must be validated — scheme, host and public key
all present — with a `ValueError` at construction, making the existing docstring true.

#### Acceptance Criteria:

- [ ] `LogstashSink(url="http://h/", host="h")` raises `ValueError` naming the ignored arguments;
      the same for `port=` and for a non-default `transport=`.
- [ ] `LogstashSink(url=...)` alone and `LogstashSink(host=..., port=...)` alone both construct.
- [ ] `SentrySink(dsn="not-a-dsn")` raises `ValueError`; so do a DSN with no host and one with no
      public key.
- [ ] A well-formed DSN produces exactly the ingest URL and auth header it produces today — an
      existing-behaviour test pins both strings.
- [ ] The refusal is at construction, not at first emit, for both sinks.
- [ ] `SyslogSink(app_name="my app")` raises `ValueError`; a tab and a newline are refused on the
      same path. Measured before the fix: the RFC 5424 header became
      `<14>1 <ts> <host> my app 22113 - - {json}`, so a receiver reads `app` as PROCID and `22113`
      as MSGID — every field after APP-NAME shifts, for every message the sink ever sends, silently.
- [ ] `ElasticsearchSink(index="my index")` raises `ValueError` on the same whitespace test.

### FR-005: The three remaining drivers' waits are bounded or recorded

#### Description:

Measured in an isolated environment against a peer that accepts TCP and never replies:
`pymongo`'s resolved `socket_timeout` is `None`; `pika`'s `blocked_connection_timeout` default is
`None`; `clickhouse-connect`'s `send_receive_timeout` default is `300`, and one call waited
**300.01 s** against **3.00 s** with an explicit bound. `ClickHouseSink._insert` makes
`max_retries + 1` attempts, so that is four such waits on the drain thread per batch.

The two whose driver default is *infinite* are bounded by this library, as `psycopg`'s was:
`MongoDBSink` gains `socket_timeout` and `RabbitMQSink` gains `blocked_connection_timeout`, each
with a bounded module-level default applied when the sink builds its own client. The one whose
default is *finite* is exposed rather than overridden, as `NATSSink`'s connect arguments were:
`ClickHouseSink` gains `send_receive_timeout`, defaulting to `None`, passed to
`clickhouse_connect.get_client` only when given. `pymongo`'s `serverSelectionTimeoutMS` and
`pika`'s `socket_timeout`/`stack_timeout` are finite and are exposed on the same `None`-default
terms.

Every one of them follows SPEC-043's rule: passing one alongside `client=` or `connection=` raises,
because an already-connected client cannot consume it. All three driver defaults are recorded in
`architecture.md` §13 beside `DEFAULT_CONNECT_TIMEOUT`, so the numbers are a decision rather than
an omission — including the one this spec could not execute.

#### Acceptance Criteria:

- [ ] `MongoDBSink(uri=...)` with no client passes a bounded `socketTimeoutMS` to `MongoClient`,
      asserted through an injected client factory rather than a live server.
- [ ] `RabbitMQSink(url=...)` with no connection passes a bounded `blocked_connection_timeout`
      to `pika.ConnectionParameters`/`URLParameters`, asserted the same way.
- [ ] `ClickHouseSink(send_receive_timeout=30)` forwards `send_receive_timeout=30`;
      `ClickHouseSink()` with no value forwards **no** `send_receive_timeout` key at all.
- [ ] Passing any of the new arguments alongside `client=`/`connection=` raises `ValueError`
      naming them, matching `NATSSink`'s existing message shape.
- [ ] Each new argument is refused when unusable, on FR-001's `usable_timeout` test.
- [ ] `architecture.md` §13 carries one row per driver naming the default, whether this library
      overrides it, and — for `pika`'s blocked-connection path — that the behaviour is recorded
      from the driver's documented default and was **not** executed here.

### FR-006: Docstring and diagnostic truth in `sinks/`

#### Description:

Six literal violations of rules this repo already states, all inside `sinks/`.
`postgres.py::_reconnect_if_broken` carries no `Args:`/`Returns:`/`Raises:` — the only such miss in
the package. `sinks/__init__.py` has no module docstring. `file.py::_rollover_seconds` is a
`@staticmethod`, which `python.md` §9 forbids. Four docstrings anchor a measured count to a spec
number rather than to a commit a reader can re-measure from (`nats.py`, `http.py`, `pubsub.py`,
`loki.py`), which is the global rule against volatile numbers in standing prose.

Two are diagnostic rather than cosmetic. `SocketTransport._send_one` writes
`{max_retries + 1} attempt(s)` unconditionally, so a message abandoned after **one** send on a
permanent errno is reported as four attempts — `HTTPSink` already reports the true count.
`LoggingSink.emit`'s `Raises:` claims it surfaces whatever the handlers raise, but
`logging.Handler.handleError` absorbs a handler's own failure by default, so the sink cannot
observe it and the docstring promises a report it will never make.

#### Acceptance Criteria:

- [ ] `_reconnect_if_broken` has all three sections; `sinks/__init__.py` has a one-line module
      docstring; `_rollover_seconds` is a module-level function.
- [ ] No docstring in `sinks/` anchors a measured count to a spec number alone: each of the four
      either drops the number and states the principle, or cites a commit.
- [ ] `SocketTransport` abandoning on a permanent errno after one send writes `1 attempt(s)`;
      abandoning past the retry bound still writes `max_retries + 1`.
- [ ] `LoggingSink.emit`'s docstring states what it can and cannot observe, and a test asserts the
      unobservable case — a handler whose `emit` raises and whose `handleError` absorbs it — leaves
      the sink reporting success.
- [ ] `ruff`, `mypy --strict` and `docs-lint` stay green.

---

## Data Model

New module-level constants and one shared validator. No new public types.

```python
# sinks/_retry.py — raises where usable_timeout falls back.
def require_timeout(value: float, name: str, owner: str) -> float: ...
    # ValueError unless 0 < value < inf   (NaN-safe, the usable_timeout test)

def require_positive(value: int, name: str, owner: str) -> int: ...
    # ValueError unless value > 0

# sinks/mongodb.py
DEFAULT_SOCKET_TIMEOUT = 30.0        # seconds; pymongo's own default is None

# sinks/rabbitmq.py
DEFAULT_BLOCKED_CONNECTION_TIMEOUT = 30.0   # seconds; pika's own default is None
```

## API / Interface Contract

Three constructors gain arguments; all are keyword-only and additive.

```python
MongoDBSink(*, client=None, uri=None, database, collection, max_retries=3,
            socket_timeout: float | None = None,           # None -> DEFAULT_SOCKET_TIMEOUT
            server_selection_timeout: float | None = None) # None -> pass nothing

RabbitMQSink(..., connection=None,
             blocked_connection_timeout: float | None = None,  # None -> the module default
             socket_timeout: float | None = None,              # None -> pass nothing
             stack_timeout: float | None = None)

ClickHouseSink(table, *, client=None, dsn=None, create_table=False, chunk_size=1000,
               max_retries=3,
               send_receive_timeout: float | None = None)      # None -> pass nothing
```

Every other change is a refusal of a value the constructor accepts today. `ValueError` is raised
in every case, matching `SocketTransport`'s transport check and `_chunk.valid_identifier`.

## Configuration / Environment

None. The two new defaults are module constants, not environment reads.

## File & Folder Structure

```
src/log_foundry/sinks/
├── _retry.py          # FR-001/FR-002 shared validators
├── __init__.py        # FR-006 module docstring
├── http.py            # FR-001
├── _socket.py         # FR-002, FR-006 attempt count
├── clickhouse.py      # FR-002, FR-005
├── postgres.py        # FR-002, FR-006 docstring sections
├── pubsub.py          # FR-002, FR-006
├── file.py            # FR-003, FR-006 staticmethod
├── logstash.py        # FR-004
├── sentry.py          # FR-004
├── mongodb.py         # FR-005
├── rabbitmq.py        # FR-005
├── syslog.py          # FR-004
├── elasticsearch.py   # FR-004
├── logging_sink.py    # FR-006
├── nats.py, loki.py   # FR-006
docs/architecture.md   # FR-005 §13 rows
```

## Implementation Phases

### Phase 1: The shared validators and the HTTP family (FR-001)

- `require_timeout` / `require_positive` in `_retry.py`, with their own tests, including the
  `NaN` case the obvious two-comparison form gets wrong.
- Apply to `HTTPSink`; add the subclass roster test.

### Phase 2: The remaining refusals (FR-002, FR-003, FR-004)

- `SocketTransport`, `ClickHouseSink`, `PostgresSink`, `GooglePubSubSink`, `RotatingFileSink`.
- `LogstashSink`'s ignored-argument error, `SentrySink`'s DSN validation, and the two
  whitespace refusals SPEC-048's seam handed over.

### Phase 3: The driver bounds (FR-005)

- The three constructors, their `client=` conflict errors, and the `architecture.md` §13 rows.
- Assert the forwarded kwargs through a factory seam rather than a live server, since CI installs
  no extras.

### Phase 4: Docstring and diagnostic truth (FR-006)

- The six literal fixes, and the `SocketTransport` attempt count with a test that distinguishes
  the permanent-errno path from the retry-exhausted one.

### Phase 5: Mutation-verify the refusals

- Each new `ValueError` is a guard whose failure is silent — the sink simply constructs again.
  Remove each check in turn, confirm its test reddens, and assert the **message**, not the
  exception type: `ValueError` is what several neighbouring checks already raise.

## Revision history

Widened at authoring time, before its own spec review, by SPEC-048's review: that spec's Out of
Scope deferred construction-time validation here while one of its FRs required two such refusals,
and this spec did not name them. The seam is now stated as *when* the library refuses rather than
*what the consequence is*, and FR-004 carries `SyslogSink(app_name=)` and
`ElasticsearchSink(index=)` alongside the two SPEC-043 violations it already had.
