# Spec: Sink Construction Validation and Driver Bounds

**ID:** SPEC-049
**Status:** Draft
**Last Updated:** 2026-09-05
**Depends On:** SPEC-021, SPEC-026, SPEC-027, SPEC-041, SPEC-043, SPEC-047, SPEC-048

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
drain thread, which every other sink's delivery is queued behind. SPEC-041 bounded `psycopg` for
exactly this reason and SPEC-047 bounded `confluent-kafka` and `nats-py`; these three were never
reached.

This spec makes a bad sink argument fail where it is written, gives the three remaining drivers the
treatment their siblings already have, and closes the two construction-time open items SPEC-047
recorded and deferred to "a major version that can refuse it" — which 1.0 is.

## Scope

### In Scope

- Construction-time refusal of degenerate timeouts, chunk sizes and the one rotation bound that
  destroys data, across `HTTPSink`, `SocketTransport`, `ClickHouseSink`, `PostgresSink`,
  `GooglePubSubSink` and `RotatingFileSink`, under the floor-vs-refuse rule FR-001 states once
  for all of them — which also sorts two `RotatingFileSink` bounds onto the *floor* side (FR-003).
- Rejection of CR/LF in a caller's header name, header value **or bearer token**, and of a URL
  whose scheme is not `http`/`https`.
- Extending SPEC-043's rule — an argument no backend can use is an error, not a silent ignore — to
  every remaining violation: `LogstashSink`, `SentrySink`'s DSN, `SyslogSink(app_name=)`,
  `ElasticsearchSink(index=)`, and the two `NATSSink` items `architecture.md` §12 records as open.
- Bounding `pymongo`'s socket timeout and `pika`'s blocked-connection timeout, whose driver
  defaults are infinite; exposing `clickhouse-connect`'s `send_receive_timeout`, whose default is
  finite; and recording all three in `architecture.md` §12.
- The `SocketTransport` abandonment line's attempt count, which overstates by up to four.
- Two literal prose defects in `sinks/`: a `@staticmethod`, and a docstring paragraph dedented to
  column 0.

**Above the 3–6 aim at seven FRs, deliberately.** Six are one rule — *the library refuses at
construction what it cannot make work* — applied to six populations that share a test corpus and a
single new helper; splitting them would put the same helper's fixture corpus in two specs. The
seventh (FR-007) is prose only and rides along because it touches the same files. The reviewer may
reject this.

### Out of Scope

- Any change to how a **valid** batch is chunked, framed, retried or adjudicated — SPEC-048, built
  before this one. (The earlier wording, "anything that changes what reaches the wire when the
  configuration is valid", excluded FR-005: a 30 s `socketTimeoutMS` where `pymongo` had `None`
  does change what reaches the wire under a valid configuration.)
- Normalising the sink family's argument *names* (`url`/`dsn`/`uri`, `chunk_size`/`max_pending`).
  The audit records that drift as frozen; this spec adds no new spelling and renames nothing.
- ~~`**http_kwargs: object` on the seven platform sinks. `Unpack[TypedDict]` is additive in
  1.x.~~ — already shipped by SPEC-051, one `Unpack[…]` TypedDict per sink (`HTTPPlatformKwargs`,
  `HTTPRetryKwargs`, `HTTPAuthKwargs`, `HTTPKwargs` in `http.py`); struck by the 2026-09-04
  audit's N9.
- Bounding `pymongo`'s `serverSelectionTimeoutMS` or `pika`'s `socket_timeout`/`stack_timeout`.
  Those defaults are finite (30 s, 10 s, 15 s); they are exposed and recorded, not overridden.
- Changing where the two existing `usable_timeout` callers sit. `KafkaSink(flush_timeout=-1)` and
  `NATSSink(publish_timeout=-1)` keep falling back to their module defaults, because that
  configuration **works today** and FR-001's rule protects it. This is the answer to "two rules
  for one question": there is one rule, stated in FR-001, and those two are on the floor side of
  it.
- Validating an Elasticsearch index name beyond whitespace. The audit recorded whitespace there as
  frame corruption; it is not — `json.dumps` escapes the `_bulk` action line correctly and a name
  the cluster rejects already arrives as a counted per-item error. The refusal is kept because it
  turns a sink that fails every batch forever into a startup error, not because anything is
  silent, and the full index-name grammar is the cluster's to enforce.
- The measured counts in `src/` docstrings. The audit's C6 called them "anchored to a spec rather
  than a commit" and named four files in `sinks/`; the population is **nineteen sites across nine
  modules** there, and SPEC-052's sweep names three more outside it (`config.py`'s
  `_config_lock` docstring, `worker.py::Health`, `_lifecycle.py::_close_orphan_sink`) that the
  audit's split gave to neither session. They are
  design rationale rather than standing rules — the global rule against volatile numbers governs
  rules, and stripping the evidence from a docstring would remove the reasoning that rule wants
  kept. **The spec ID is the anchor, one hop short of a commit:** that rule's alternative to
  deleting a number is to anchor both ends to a commit a reader can re-measure from, and a spec ID
  reaches one through its delivery doc, which names the release that carried it. So these are
  anchored, not unanchored. Recorded here rather than deleted, per SPEC-021.

---

## Functional Requirements

### FR-001: The HTTP family refuses an unusable timeout, a CRLF header or token, and a non-`http` URL

#### Description:

**The rule, stated once for this whole spec.** The library already *floors* several degenerate
arguments — `max_retries` at zero in six sinks, `max_batch_count` at one, `PostgresSink`'s
`connect_timeout` at libpq's minimum of two, and `KafkaSink`/`NATSSink`'s timeouts back to their
module defaults through `_retry.usable_timeout`. That is not a second rule competing with this
spec's refusals; it is the same rule's other half:

> **The library floors a value that works today and refuses one that is already broken. A newly
> added argument is refused, because it has no working configuration to protect.**

Every floor listed above lands on a value that delivers events, so flooring is a no-op for anyone
sane and a silent rescue for anyone else. Every refusal this spec adds is on a value that today
either destroys data silently or raises an uncounted exception on every batch forever — there is
no working configuration to break. That is why `KafkaSink(flush_timeout=-1)` keeps becoming `10.0`
while `RabbitMQSink(blocked_connection_timeout=-1)` will raise: the first works, the second is new.

Applied here: `HTTPSink.__init__` stores `timeout`, `headers`, `auth` and `url` verbatim. A
`timeout` of `-1` or `nan` reaches `urlopen` and raises a raw `ValueError` from inside `emit` on
every batch, `inf` raises a raw `OverflowError` from the same place, and `0` fails every connection
before it opens — four values, none of which delivers, so none is a floor candidate; CR or LF in a
header name or value raises a raw `ValueError` from `http.client`; a bearer token
containing CR/LF is written straight into `Authorization` in `http.py::HTTPSink._apply_auth`, which
is the same
header injection in the argument with the highest consequence; and a URL whose scheme is not
`http`/`https` sends the batch somewhere else entirely — `file:///etc/passwd` raises a raw
`TypeError`, `ftp://` burns the full retry budget on every batch.

The check is `_retry.require_timeout`, a raising sibling of `usable_timeout` using the identical
`not (0 < value < inf)` test, which is the `NaN`-safe form (`NaN` compares `False` to everything,
so the obvious pair of comparisons lets it through).

#### Acceptance Criteria:

- [ ] `HTTPSink(url, timeout=t)` raises `ValueError` for each of `-1`, `0`, `nan` and `inf`, and the
      message names `timeout` and the value.
- [ ] `HTTPSink(url, headers={"X": "a\r\nInjected: 1"})`, `HTTPSink(url, headers={"X\r\nY": "1"})`
      and `HTTPSink(url, auth="tok\r\nInjected: 1")` each raise `ValueError` naming the argument; a
      bare `\n` and a bare `\r` are refused on the same path.
- [ ] A `(user, password)` `auth` pair containing CR/LF does **not** raise, because it is
      base64-encoded before it reaches the header — a test pins that, so the asymmetry is
      deliberate rather than an oversight.
- [ ] `HTTPSink("file:///etc/passwd")` and `HTTPSink("ftp://h/x")` raise `ValueError` naming the
      scheme; `http://` and `https://` construct.
- [ ] Every sink built on `HTTPSink` inherits the refusal — a roster test derived from the module
      list asserts each subclass rejects `timeout=-1`, so a new subclass cannot opt out.
- [ ] `KafkaSink(flush_timeout=-1)` and `NATSSink(publish_timeout=-1)` still construct and still
      fall back to their module defaults, pinning the floor side of the rule against a later
      well-meaning "consistency" change.
- [ ] `require_timeout` has its own tests including `nan`, and `usable_timeout` is unchanged.
- [ ] Serves invariant 13: each refusal is at the constructor, and the two floor-side pins keep
      `usable_timeout`'s callers on the working side of the rule the page records.

### FR-002: Degenerate bounds are refused where they are written, and the shape they exploited is closed

#### Description:

Four more sinks accept a value that cannot work. `SocketTransport(timeout=-1|nan)` reaches
`create_connection` and raises there — for `transport="udp"` the timeout is never used at all, so
this refusal is new behaviour for a UDP caller passing a junk value, which is intended and is
noted here rather than discovered. `ClickHouseSink(chunk_size=0)` raises from `range()`;
`chunk_size=-5` is the silent one. `PostgresSink(chunk_size=0)` spends four attempts and three
backoffs per batch failing the same way. `GooglePubSubSink(overflow_timeout=nan)` makes
`_out_of_time` compare against `nan`, which is `False` forever, so the deadline loop that bounds
`flush()` and `_await_overflow` has no bound at all.

**Refusing the argument closes the route, not the shape.** `chunk_list` yields nothing for a
negative size, so `ClickHouseSink.emit` reaches `if chunks and not inserted` with `chunks == 0`,
returns normally, and a whole batch is gone with `losses()` at zero. Refusing `chunk_size <= 0`
removes the only known way in; the *branch* is still unguarded, and a future chunker change walks
back into it. So `emit` additionally raises when a non-empty batch produced no chunks — the guard
that makes the criterion below a test rather than a restatement of the refusal. `_chunk.chunk_list`
gains the `ValueError` its own docstring already claims it raises.

#### Acceptance Criteria:

- [ ] `SocketTransport(host, port, timeout=t)` raises `ValueError` for `-1`, `0`, `nan` and `inf`,
      for both `transport="tcp"` and `transport="udp"`.
- [ ] `ClickHouseSink(chunk_size=n)` and `PostgresSink(chunk_size=n)` raise `ValueError` for `0`
      and for any negative `n`; a positive value constructs.
- [ ] With `clickhouse.chunk_list` patched to yield nothing, `ClickHouseSink.emit` of a non-empty
      batch raises `SinkDeliveryError` and the message says no chunk was produced. ~~With
      `chunk_size` monkeypatched to `-5` after construction~~ — struck, see Revision history: that
      route now raises a raw `ValueError` out of `chunk_list` before the guard is reached.
- [ ] `chunk_list(items, 0)` and `chunk_list(items, -1)` raise `ValueError`, making the existing
      docstring true.
- [ ] `GooglePubSubSink(overflow_timeout=t)` raises `ValueError` for `nan`, `inf`, `0` and
      negatives.
- [ ] Every message names the offending argument and the value received.
- [ ] Serves invariants 13 and 7: the refusals are at construction, and the `chunks == 0` guard
      makes a batch that produced no chunks raise rather than return having delivered nothing.

### FR-003: `RotatingFileSink` refuses a rotation bound that destroys data

#### Description:

`RotatingFileSink` accepts `interval<=0`, `max_bytes<0` and `backup_count<0`, and **FR-001's rule
sorts them into two groups, not one.** `interval=0` and negatives put the rollover deadline
permanently in the past, so `_should_rotate` fires on every event: measured, three emits of five
events left **two** lines on disk out of fifteen. That destroys data, so it is refused.

`max_bytes<0` and `backup_count<0` are **floored to `0`, not refused.** Both *work* today:
`_should_rotate` tests `self._max_bytes > 0` and `_rotate` tests `self._backup_count > 0`, so a
negative behaves identically to the documented `0` — measured, a sink built with `-1` and one built
with `0` leave the same files at the same sizes. Under FR-001's rule they are on the floor side, and
refusing a configuration that delivers would be the breaking change at 1.0 that rule exists to
prevent; flooring is a behavioural no-op that makes the stored value legible.

`max_bytes=0` and `backup_count=0` stay valid: both are documented switches, not mistakes.

`interval` is validated even when `when=None`, where it is inert today — a new refusal of a
currently harmless call, taken because an interval that means nothing is a caller who believes a
time trigger is armed, and named here so it is a decision rather than a side effect.

#### Acceptance Criteria:

- [ ] `RotatingFileSink(path, when="S", interval=n)` raises `ValueError` for `0` and negatives;
      `interval=1` constructs.
- [ ] `RotatingFileSink(path, when=None, interval=0)` also raises, and the message says the
      interval is invalid regardless of whether a unit was given.
- [ ] `RotatingFileSink(path, max_bytes=-1)` and `(path, backup_count=-1)` **construct**, floored to
      `0`, and behave exactly as `0` does — asserted on the files left on disk, not on the attribute
      alone. `max_bytes=0` and `backup_count=0` are unchanged.
- [ ] The existing rotation suite passes unchanged.
- [ ] Serves invariants 13 and 2: the rotation bound that would delete written events is refused
      where it is written, so no accepted event is lost to it, and the two that work are floored
      rather than refused, keeping invariant 13's rule to arguments *no backend can use*.

### FR-004: An argument no backend can use is an error, not a silent ignore

#### Description:

SPEC-043 settled this and `NATSSink` partly implements it. Six violations remain.

`LogstashSink(url=…)` builds the HTTP backend and silently discards `host`, `port`, `transport` and
`max_datagram_bytes`, so a caller who meant the socket backend gets HTTP and no word about it.
`transport` and `max_datagram_bytes` have non-`None` defaults, which is SPEC-043's own recorded
limit — an explicit value is indistinguishable from the default — so both become `str | None` and
`int | None`, as `host` and `port` already are, which is what makes the rule total rather than
partial.

`SentrySink._parse_dsn` documents `Raises: ValueError: If the DSN cannot be parsed as a URL`, but
`urlparse` almost never raises: `"https:///project"` yields no host and `"https://host/1"` yields
no public key, and the sink then posts every event to a URL that cannot exist. Scheme, host, key
and project id must all be present. The DSN is validated **where it is parsed**, which is every
construction that builds the HTTP fallback — and not on `backend="sdk"`, where `_parse_dsn` is
never called and `_dsn` is never read (the envelope header that carries it belongs to the fallback
that backend does not build). Refusing a value that has no effect would invent a failure rather
than report one, the opposite of this spec's rule.

`SyslogSink(app_name=)` is interpolated raw into a space-delimited RFC 5424 header
(`syslog.py::SyslogSink._frame`), so whitespace shifts PROCID, MSGID and STRUCTURED-DATA for every
message — corruption no counter sees. An **empty** `app_name` shifts them identically and is refused on the
same path. `ElasticsearchSink(index=)` is different, and the difference is stated because the audit
got it wrong: the action line is built with `json.dumps`
(`elasticsearch.py::ElasticsearchSink._render`), so the NDJSON
frame stays intact and a name the cluster rejects is counted per item. It is refused anyway,
because an index name the cluster can never accept makes every batch fail for the life of the
process.

**Two `architecture.md` §12 open items are closed here rather than left contradicting this FR.**
`NATSSink(client=X, servers=…)` ignores `servers` silently — the same defect as `LogstashSink`'s,
and §12 records it as "Closed by a major version that can refuse it". 1.0 is that version, and a
spec claiming every construction-time refusal cannot leave the identical case open two sections
away. `NATSSink(max_reconnect_attempts=0)` makes the connect loop unbounded, and §12 names
refusing it as the closure. Both entries are struck in place and marked with this spec, per
SPEC-021.

#### Acceptance Criteria:

- [ ] `LogstashSink(url=…, host=…)` raises `ValueError` naming the ignored argument; the same for
      `port=`, `transport=` and `max_datagram_bytes=`, each passed explicitly.
- [ ] `LogstashSink(url=…, transport="tcp")` — the *default* value, passed explicitly — raises,
      which is the case the `str | None` change exists to make reachable.
- [ ] `LogstashSink(url=…)` alone and `LogstashSink(host=…, port=…)` alone both construct, and the
      socket backend still receives the transport and datagram bound it did before.
- [ ] `SentrySink(dsn="https:///project")` and `SentrySink(dsn="https://host/1")` raise
      `ValueError` whose message names the DSN. `SentrySink(dsn="not-a-dsn")` also raises, but the
      test asserts the **DSN** message rather than merely that something raised — FR-001's scheme
      check would refuse that one anyway, and an AC that cannot tell them apart tests FR-001.
- [ ] A well-formed DSN produces exactly the ingest URL and auth header it produces today, pinned
      as two literal strings.
- [ ] `SyslogSink(app_name="my app")`, `("my\tapp")`, `("a\nb")` and `("")` each raise `ValueError`.
- [ ] `ElasticsearchSink(index="my index")` raises on the same whitespace test.
- [ ] `NATSSink(client=X, servers=…)` raises, joining the four connect arguments already refused
      there. `servers` is checked from a structure separate from the forwarded `supplied` dict,
      because `nats.connect(servers or …, **supplied)` would otherwise receive it twice.
- [ ] `NATSSink(max_reconnect_attempts=n)` raises for `0` and negatives. This **supersedes**
      SPEC-047, which forwarded a falsy value deliberately and measured why: its
      `test_a_falsy_connect_bound_is_still_forwarded` and the constructor docstring's "passed
      through rather than corrected here" paragraph are struck in place per SPEC-021, never
      deleted — a newly false docstring shipped in the same diff as FR-007 would be the defect
      that FR exists to remove.
- [ ] `architecture.md` §12's two entries are struck through in place and marked closed by
      SPEC-049, rather than deleted.
- [ ] Every refusal is at construction, not at first emit.
- [ ] Serves invariant 13, whose second observable — no argument silently ignored because another
      was given — is this FR's whole subject.

### FR-005: The three remaining drivers' waits are bounded or recorded

#### Description:

Measured in an isolated environment against a peer that accepts TCP and never replies:
`pymongo`'s resolved `socket_timeout` is `None`, and `MongoDBSink.emit` took **60.66 s for two
attempts** against 3.12 s with an explicit `serverSelectionTimeoutMS=1500`; `pika`'s
`blocked_connection_timeout` default is `None`; `clickhouse-connect`'s `send_receive_timeout`
default is `300`, and one call waited **300.01 s** against **3.00 s** with an explicit bound.
`ClickHouseSink._insert` makes `max_retries + 1` attempts **per chunk**, so a batch costs
`ceil(len(batch) / chunk_size) × 4` such waits on the drain thread.

The two whose driver default is *infinite* are bounded by this library, as `psycopg`'s was:
`MongoDBSink` gains `socket_timeout` and `RabbitMQSink` gains `blocked_connection_timeout`, each
with a bounded module-level default applied when the sink builds its own client. The one whose
default is *finite* is exposed rather than overridden, as `NATSSink`'s connect arguments were:
`ClickHouseSink` gains `send_receive_timeout`, defaulting to `None` and forwarded only when given.
`pymongo`'s `serverSelectionTimeoutMS` and `pika`'s `socket_timeout`/`stack_timeout` are finite and
exposed on the same `None`-default terms.

`pika.URLParameters` takes only a URL — the option is an attribute, and pika parses
`?blocked_connection_timeout=` out of the URL query into it. So the library's default applies only
when the URL named nothing, rather than overriding a value the caller wrote. That is deliberately
*not* `PostgresSink`'s behaviour, which documents that it overrides a DSN's `connect_timeout`: pika
lets the two be told apart and libpq did not.

Every new argument follows SPEC-043: passing one alongside `client=`/`connection=` raises, because
an already-connected client cannot consume it. The `None` default is what makes that decidable —
SPEC-043's caveat is about `max_retries=3`, where the default is a legal value; here `None` is not.
It also means a caller cannot ask for `pymongo`'s unbounded original; they inject their own client
instead, which the docstring says.

#### Acceptance Criteria:

- [ ] `MongoDBSink(uri=…)` with no client passes `socketTimeoutMS` derived from
      `DEFAULT_SOCKET_TIMEOUT` to `MongoClient`, asserted through an injected client factory rather
      than a live server.
- [ ] `RabbitMQSink(url=…)` with no connection sets `blocked_connection_timeout` on the parameters
      object; a URL carrying `?blocked_connection_timeout=60` keeps `60`, asserted through an
      injected `pika` stand-in.
- [ ] `ClickHouseSink(send_receive_timeout=30)` forwards `send_receive_timeout=30`;
      `ClickHouseSink()` with no value forwards **no** `send_receive_timeout` key at all.
- [ ] Passing any new argument alongside `client=`/`connection=` raises `ValueError` naming them,
      matching `NATSSink`'s existing message shape; passing none alongside an injected client
      constructs.
- [ ] Each new argument is refused when unusable, on FR-001's `require_timeout` — they are new, so
      the rule's third clause applies.
- [ ] `architecture.md` **§12** carries one entry per driver naming the default, whether this
      library overrides it, and a *Closed by* clause, matching how SPEC-047 recorded its own —
      **not §13**, which is Non-goals and says of itself that it is not a backlog.
- [ ] The `pika` entry states that the blocking behaviour is recorded from the driver's documented
      default and was **not executed** by SPEC-049, and cites SPEC-049, so a later measurement
      supersedes it in place rather than reading as permanently settled.
- [ ] Serves invariants 3 and 13: the three drivers' waits on the drain thread become bounded,
      and each new argument is refused, or refused beside `client=`, at construction.

### FR-006: An abandonment line reports the attempts actually made

#### Description:

`SocketTransport._send_one` writes `{self._max_retries + 1} attempt(s)` unconditionally in its
abandonment line, so a message abandoned after **one** send on a permanent errno — the
`_PERMANENT_ERRNOS` branch, which skips the remaining attempts by design — is reported as four.
`HTTPSink._request` already passes `attempt + 1` and reports the truth. A diagnostic that
overstates is one an operator uses to reach the wrong conclusion, which is the whole reason
SPEC-029 put the rules in one module.

#### Acceptance Criteria:

- [ ] A UDP send failing with `EMSGSIZE` on the first attempt writes `1 attempt(s)`.
- [ ] A send failing with a retryable errno on every attempt still writes `max_retries + 1`.
- [ ] The two cases are asserted in one test file with the same `max_retries`, so a change that
      collapses them fails.
- [ ] Serves invariant 11: the attempt count the line carries reproduces from what was tried.

### FR-007: Three literal prose defects in `sinks/`

#### Description:

`file.py::RotatingFileSink._rollover_seconds` is a `@staticmethod`, which `python.md` §9 forbids; it
is the only
one in the package. And `postgres.py::_reconnect_if_broken`'s docstring has a paragraph dedented to
column 0 mid-docstring, which renders wrongly in every tool that reads it.

`sinks/__init__.py`'s missing module docstring — the audit's C6 assigned it here — is **handed to
SPEC-052** rather than fixed here. It is the only module in the tree without one, so it is the sole
finding of that spec's new docstring gate, and a gate shipped with an exemption for its own only
finding is not a gate. SPEC-048 and SPEC-049 touch the file nowhere else.

`LoggingSink.emit` documents `Raises: Exception: Whatever the logger's handlers raise`. That is
overstated rather than false: `logging.Handler`'s own `emit` implementations route their failures
to `handleError`, which absorbs by default, so a handler failing in the ordinary way is invisible
here — but a custom handler that lets an exception out of `emit`, or overrides `handleError` to
re-raise, does propagate. The docstring must say which it can observe rather than claiming either
extreme.

#### Acceptance Criteria:

- [ ] `_rollover_seconds` is a module-level function, and no `@staticmethod` remains in
      `src/log_foundry/`, asserted by an AST test rather than by grep.
- [ ] `_reconnect_if_broken`'s docstring has no line at column 0 after the opening quotes;
      an AST test over `src/` asserts that of every docstring, so the class is closed rather than
      the instance.
- [ ] `LoggingSink.emit`'s docstring names the default-`handleError` case as unobservable and the
      re-raising case as observable, and a test exercises both: a handler absorbing its own failure
      leaves `emit` returning, and one re-raising propagates.
- [ ] `ruff`, `mypy --strict` and `docs-lint` stay green.
- [ ] Serves no invariant: the `@staticmethod`, the dedent and the `Raises:` line are prose and
      layout, which `docs/invariants.md` leaves to `process.md` §5's own rules, and the handler
      test pins behaviour that already holds rather than changing what a caller observes.

---

## Data Model

```python
# sinks/_retry.py — a raising sibling of usable_timeout, same NaN-safe test.
def require_timeout(value: float, name: str, owner: str) -> float: ...
def require_positive(value: int, name: str, owner: str) -> int: ...

# sinks/mongodb.py
DEFAULT_SOCKET_TIMEOUT = 30.0                # pymongo's own default is None

# sinks/rabbitmq.py
DEFAULT_BLOCKED_CONNECTION_TIMEOUT = 30.0    # pika's own default is None
```

## API / Interface Contract

Three constructors gain keyword-only arguments; two change a default from a value to `None` so an
explicit pass becomes distinguishable.

```python
MongoDBSink(*, client=None, uri=None, database, collection, max_retries=3,
            socket_timeout: float | None = None,            # None -> DEFAULT_SOCKET_TIMEOUT
            server_selection_timeout: float | None = None)  # None -> pass nothing

RabbitMQSink(..., connection=None,
             blocked_connection_timeout: float | None = None,   # None -> module default
             socket_timeout: float | None = None,               # None -> pass nothing
             stack_timeout: float | None = None)

ClickHouseSink(table, *, client=None, dsn=None, create_table=False, chunk_size=1000,
               max_retries=3, send_receive_timeout: float | None = None)

LogstashSink(..., transport: str | None = None,        # was "tcp"
             max_datagram_bytes: int | None = None)    # was DEFAULT_MAX_DATAGRAM_BYTES
```

Everything else is a refusal of a value the constructor accepts today, always `ValueError`, matching
`SocketTransport`'s transport check and `_chunk.valid_identifier`.

## Configuration / Environment

None. The two new defaults are module constants, not environment reads.

## File & Folder Structure

```
src/log_foundry/sinks/
├── _retry.py          # FR-001 validators
├── _chunk.py          # FR-002 chunk_list raise
├── http.py            # FR-001
├── _socket.py         # FR-002, FR-006
├── clickhouse.py      # FR-002, FR-005
├── postgres.py        # FR-002, FR-007
├── pubsub.py          # FR-002
├── file.py            # FR-003, FR-007
├── logstash.py        # FR-004
├── sentry.py          # FR-004
├── syslog.py          # FR-004
├── elasticsearch.py   # FR-004
├── nats.py            # FR-004
├── mongodb.py         # FR-005
├── rabbitmq.py        # FR-005
└── logging_sink.py    # FR-007
docs/architecture.md   # FR-004 strikes in §12, FR-005 rows in §12
```

## Implementation Phases

### Phase 1: The validators and the HTTP family (FR-001)

- `require_timeout` / `require_positive` in `_retry.py`, with tests including `NaN`, and the
  floor-vs-refuse rule written into `require_timeout`'s docstring beside `usable_timeout`'s.
- Apply to `HTTPSink` (timeout, headers, auth, URL scheme); add the subclass roster test and the
  two floor-side pins. The roster is **named, not floored**, and reuses
  `tests/test_public_surface.py::_http_sink_subclasses()`, which walks inheritance transitively:
  `OpenSearchSink` inherits through `ElasticsearchSink` and is invisible to a direct scan of
  `class X(HTTPSink)`, so a scan-derived roster would silently be one short.

### Phase 2: The remaining refusals (FR-002, FR-003, FR-004)

- `SocketTransport`, `ClickHouseSink` (plus the `chunks == 0` guard), `PostgresSink`,
  `GooglePubSubSink`, `chunk_list`, `RotatingFileSink` (`interval` refused; `max_bytes` and
  `backup_count` floored).
- `LogstashSink` (four arguments, two default changes), `SentrySink`'s DSN, `SyslogSink`,
  `ElasticsearchSink`, and the two `NATSSink` items with their §12 strikes.

### Phase 3: The driver bounds (FR-005)

- The three constructors, their `client=` conflict errors, and the `architecture.md` §12 entries.
- Assert the forwarded arguments through a factory seam — CI installs no extras, so a live driver
  is unavailable and a stand-in is the only honest assertion.

### Phase 4: Diagnostic and prose truth (FR-006, FR-007)

- The `SocketTransport` attempt count with a test that distinguishes the permanent-errno path from
  the retry-exhausted one.
- The three literal fixes and the `LoggingSink` docstring, each with the AST test that closes its
  class rather than its instance.

### Phase 5: Mutation-verify the refusals

Each new `ValueError` is a guard whose failure is silent — the sink simply constructs again. Remove
each check in turn, confirm its test reddens, and assert the **message**, not the exception type:
`ValueError` is what several neighbouring checks already raise, so a type-only assertion passes
against the wrong guard firing. Restore each mutant by copying from the scratchpad, never with
`git checkout --`.

## Revision history

**Revised 2026-09-05, before its build, on `main` at `212fd16`.** Every construction the Overview
and the FRs name was re-probed there and still constructs; `ClickHouseSink(chunk_size=-5)` still
returns from `emit` with zero inserts and `losses()` at zero; `interval=0` still leaves two of
fifteen lines; and the three drivers' defaults were re-read from the installed packages
(`pymongo` `socketTimeoutMS` `None`, `pika` `blocked_connection_timeout` `None` and parsed from the
URL query, `clickhouse-connect` `send_receive_timeout` `300`). Four corrections were folded in,
each originating in the plan review of an earlier, unpushed build attempt (2026-09-02) and each
re-verified here rather than carried: **FR-003** now floors `max_bytes<0` and `backup_count<0`
instead of refusing them, because a sink built with `-1` is measurably indistinguishable from one
built with `0` and FR-001's own rule puts a working value on the floor side — the first draft
refused two values that work in the same breath as it asserted "floor what works". **FR-002**'s
criterion that monkeypatched `chunk_size` to `-5` after construction was unsatisfiable: once
`chunk_list` refuses a non-positive size, that route raises a raw `ValueError` out of the
generator's first `next()`, inside the emit lock, before `emit` reaches the `chunks == 0` guard —
the two changes in the FR cancelled each other, so the test patches `chunk_list` instead.
**FR-004** now says the Sentry DSN is validated where it is parsed rather than on every backend
(`backend="sdk"` never parses or reads it), and says out loud that the NATS refusal supersedes a
shipped SPEC-047 test and docstring, which the earlier text did not. **FR-001**'s description
said all four degenerate timeouts raise `ValueError`; measured, `inf` raises `OverflowError` and
`0` fails the connection instead, so the sentence now says what each does. The two remaining line
anchors (`syslog.py:229`, `_socket.py:338`) became symbol anchors, finishing what the 2026-09-04
audit's N9 started.

Revised, not replaced, after its spec review; four blocking findings and nine should-fix, all
accepted.

The most useful was that **FR-006's first defect did not exist**: it claimed
`postgres.py::_reconnect_if_broken` lacked `Args:`/`Returns:`/`Raises:` and was "the only such miss
in 449 functions". It has all three. The claim came from the source audit and was written down
without being checked; an AST sweep of `src/log_foundry/` finds the only functions missing a
section are `decorator.py`'s two `@overload` stubs, and nothing in `sinks/` is missing one. A real
defect at the same site — a paragraph dedented to column 0 — replaces it. The neighbouring C6 claim
about measured counts anchored to a spec went the same way: nineteen sites across nine modules, not
four, and they are design rationale rather than standing rules, so the item is recorded in Out of
Scope rather than swept.

Also settled: the spec had **two rules for one question** — `usable_timeout` floors while
`require_timeout` would raise, with no stated discriminator, so `KafkaSink(flush_timeout=-1)` would
have silently become `10.0` while a structurally identical new argument raised. FR-001 now states
the one rule (floor what works, refuse what is broken, refuse what is new) and Out of Scope pins
the two floor-side callers. The spec also claimed every construction-time refusal while
`architecture.md` §12 held two SPEC-047 open items of exactly that kind, one of them deferring the
*same* defect as FR-004's `LogstashSink` case to "a major version"; both are now closed here and
struck in place. And FR-002's third criterion restated its own premise rather than testing
anything, while its conclusion was false — refusing `chunk_size <= 0` closes the route into
`ClickHouseSink`'s unguarded `chunks == 0` branch but not the branch, which is now guarded.

`sinks/__init__.py`'s module docstring was handed to SPEC-052 after this revision, at that
session's request: it is the only module in the tree lacking one and therefore the only finding of
that spec's new docstring gate, which cannot go green around it. That session independently
re-measured the false `_reconnect_if_broken` claim above and reached the same result.
