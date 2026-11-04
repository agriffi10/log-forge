---
type: Spec
id: SPEC-066
title: "Numeric Doors Refuse What Is Not a Number: Sink Counts, the Socket Port and the Public Waits"
description: "Makes every public int-typed sink constructor argument refuse a non-int with a named TypeError instead of flooring it, and gives flush() and shutdown() a defined verdict for inf, NaN and a non-number."
spec_status: Completed
depends_on: [SPEC-049, SPEC-065]
---

## Overview

SPEC-065 settled one rule and applied it to `configure()`'s four payload ceilings and to the
validators the sinks share: a value outside the declared type is refused with a `TypeError` naming
the argument and the type, never floored, even where it happened to work; an `int` instance, `bool`
included, is of the declared type and is normalised with `int.__index__`. This spec applies the same
rule to the doors SPEC-065 left, which `docs/architecture.md` §12 lists as three Open items
(invariant 13), and adds the guard that stops the class recurring — it has now been found in
SPEC-049, in SPEC-065 and here, each time by a sweep and never by a test.

Every figure below is anchored to `7dc881f`, the tree this spec was drafted on, except the
`flush`/`shutdown` measurements marked `ce2e1fe` (SPEC-065's parent; it differs from `7dc881f` in
`config.py`, `sinks/_retry.py`, `_chunk.py` and `file.py`, none of which `flush` or `shutdown`
reads). A finding was read by constructing the object and
running `emit` or the call, one process per value.

- **Nineteen constructor arguments are `int`-typed, are not judged, and accept a float.** An AST
  scan of every `__init__` in `src/log_foundry/sinks/*.py` and `worker.py` for a parameter
  annotated `int` or `int | None` that is passed to `max(...)` or compared and never given to a
  validator reports exactly these nineteen. A float constructs on every one (`2.5`; `nan` and `inf`
  too, except at `SocketTransport.port`, whose range check refuses them with the wrong message,
  below). A `str` already raised at construction on the floored arguments — a raw `TypeError: '>'
  not supported between instances of 'str' and 'int'` out of the comparison, naming neither the
  argument nor the type — and so did `None` for `max_retries`; for those the change is the message,
  not the verdict:
  - `max_retries` on twelve constructors (thirteen concrete classes, `_RedisSink` serving both Redis
    sinks) — `SocketTransport`, `ClickHouseSink`, `AzureEventHubsSink`,
    `FirehoseSink`, `HTTPSink`, `KinesisSink`, `MongoDBSink`, `PostgresSink`, `RabbitMQSink`,
    `_RedisSink` (so both Redis sinks), `SNSSink` and `SQSSink` — floored by `max(max_retries, 0)`.
    `max()` passes a float through, so `HTTPSink(url, max_retries=2.5).emit([event])` raises
    `TypeError: 'float' object cannot be interpreted as an integer` out of `range(retries + 1)` on
    every batch, with `failed` at zero: invariant 13's observable broken. `nan` and `inf` do the
    same, because `max(nan, 0)` returns `nan` (the value is the first argument). `SyslogSink`,
    `LogstashSink` and `SentrySink` inherit it through the `SocketTransport` or `HTTPSink` they
    construct, and `Worker.__init__` stores its `max_retries` unjudged (the floor is in `_emit`).
  - `RotatingFileSink.backup_count`, floored by `max(backup_count, 0)`: `2.5` or `inf` raises from
    `range(...)` on the first rollover *after* `_rotate` has closed the stream, so every later
    `emit` and `close()` raises `ValueError: I/O operation on closed file` and the sink is dead for
    the life of the process; `nan` silently disables backups. `max_bytes`, its sibling, does not
    crash — `nan` disables the cap and `inf` never rotates — but it is outside `int` all the same.
  - `HTTPSink.max_batch_count` (`int | None`, floor 1): `2.5` chunks by three, `nan` by one and
    `inf` without a limit, silently. `PostgresSink.connect_timeout` (`int`, floor 2) is forwarded
    to libpq, which wants a decimal integer. `GooglePubSubSink.max_pending` (`int | None`, floor 1):
    `nan` raises `TypeError` at every reap, `2.5` only once more than two futures are outstanding
    (`excess = max(len(outstanding) - self.max_pending, 0)` at `pubsub.py:325`, a float slice bound
    from the third future on), and `inf` is accepted and means no bound, because
    `max(len(outstanding) - inf, 0)` is never positive. `None` is not an unbounded
    spelling there — it is `DEFAULT_MAX_PENDING`.
  - `SocketTransport.port` and `NATSSink.max_reconnect_attempts`, whose checks are comparisons
    only. `port=2.5` passes `0 <= port <= 65535` and raises a raw `TypeError` at the first
    `sendto` or `create_connection`; `nan` and `inf` are refused, by the range, with the wrong
    message. `max_reconnect_attempts=2.5`, `nan` and `inf` pass `<= 0` and are forwarded to
    `nats-py` as given.
- **`flush(timeout=)` and `shutdown(timeout=)` have no verdict for `inf`, `nan` or a non-number**
  (`ce2e1fe`; the values marked `7dc881f` below were re-run there). With a worker draining a 0.4 s sink:
  `flush(math.inf)` returns `FlushResult(ok=False, reason='thread-died')` at 0.00 s while the thread
  is alive, having delivered nothing; `flush(math.nan)` returns `timed-out` at 0.00 s;
  `flush(1e300)` returns `thread-died`; `flush("5")` returns `thread-died`. `shutdown(math.inf)` and
  `shutdown(1e300)` raise `OverflowError: timestamp out of range for platform time_t`,
  `shutdown(math.nan)` raises `ValueError`, and `shutdown("5")` raises a raw `TypeError`, all into
  the caller, against `Raises: None`. Edges the sweep did not list, read at `7dc881f`:
  `shutdown(-10**400)` raises `OverflowError: int too large to convert to float`, because the
  deadline is `time.monotonic() + timeout`, while `flush(-10**400)` returns `thread-died` where
  `flush(-1)` returns `timed-out`; and `threading.TIMEOUT_MAX` itself works on both calls
  (a drained 0.3 s sink, `flush` returning `ok=True` at 0.31 s, `shutdown` closing the sink at
  0.30 s), as do `-math.inf`, `-1e300` and `-9223372037.0` (all "do not wait" on both calls).
- **The `int` subclass case, at the `max()` floor.** `max(x, 0)` returns `x` itself when `x` is not
  less than 0, so an `int` subclass is stored as itself, and one whose comparisons lie keeps a
  negative: `class Lie(int)` with `__lt__` returning `False`, `max(Lie(-3), 0)` returns the `Lie`,
  not `0`. That is SPEC-065's defect at the floor side of these doors.
- **Found by the second diff review, each reproduced at `7dc881f`**, before this spec's build
  touched the three sites:
  - `HTTPSink.max_retry_after` (`float`) is stored as given and first compared inside
    `_retry.clamp_server_delay`, on the first `Retry-After` header. With an opener answering 429
    with `Retry-After: 1` and `max_retries=1`, `"30"` and `None` construct and `emit` raises a raw
    `TypeError` after one attempt, with `failed` at zero (invariant 13). A `float` subclass whose
    comparisons claim it is positive while it holds `-1.0` passes both `min()` calls and pauses
    for `_retry.MAX_WAIT`, a day. A number — `nan`, `inf`, `2.5`, `0` — is already answered
    floor-side by `clamp_server_delay`, which falls back to exponential backoff for an unusable
    ceiling.
  - A refusal that names the value it refuses renders it with `{value!r}`, and CPython refuses to
    convert an `int` past `sys.get_int_max_str_digits()` digits (4,300 by default):
    `SocketTransport("h", 10**5000)` raises `ValueError: Exceeds the limit (4300 digits) for
    integer string conversion` out of the port check's own message, naming neither `port` nor the
    class, and so do `_retry.require_positive` and `require_timeout` for `-10**5000`,
    `NATSSink`'s `max_reconnect_attempts` check and `config._require_positive`.
  - `RotatingFileSink(when="S", interval=10**400)` raises a raw `OverflowError: int too large to
    convert to float` out of `time.monotonic() + period` in `_schedule_next`, *after* the file has
    been opened, so the refusal leaves the file on disk and its stream unclosed.

The rule is the settled one in
`docs/decisions/sink-delivery/a-bad-argument-is-floored-if-it-works-and-refused-if-it-is-already-broken-a-new-one-is-refused.md`,
as SPEC-065 refined it, and nothing here reverses it. **The reach of a type refusal:** it is asked
of every `int` argument the constructor's own body floors or range-checks, whether or not a later
branch reads it. `PostgresSink(connection=c, connect_timeout=2.5)` is refused although an injected
connection never reads `connect_timeout`, because the declared type is already violated (a `mypy`
error) and the decision's "do NOT validate a value the selected backend never reads" clause governs
*value* validation, such as parsing a DSN that `backend="sdk"` never uses. A delegating sink
(`SentrySink`, `SyslogSink`, `LogstashSink`) refuses through its delegate only, so
`SentrySink(backend="sdk", max_retries=2.5)` stays accepted (Out of Scope). Every float above is outside `int` (or `int |
None`), so under invariant 10's typed-consumer observable it is already a `mypy` error and no
type-checked caller breaks; what the rule protects — `True`, an `int` subclass, a negative `int`
floored to its floor — keeps working. `max_retry_after`'s refusals are the same case one type up:
a `str`, `None`, `Fraction` or `Decimal` is outside its declared `float`, which admits an `int`,
so it too was already a `mypy` error. The two public waits are a different case, because their
declared type is `float | None` and `inf` and `nan` are floats: for them the verdict is not the
rule's but the owner's, below.

**The owner's decision on the two waits (2026-11-03).** `None` stays unbounded. `inf`, and any
finite value above `threading.TIMEOUT_MAX`, is the same as `None`: an unbounded wait, which is what
the caller evidently meant, not an error. `nan` is refused with `ValueError` naming `timeout`. A
non-number is refused with `TypeError` naming `timeout`. A `bool`, `int` or `float` subclass is
normalised to the exact builtin it holds. Negatives and zero keep their meaning, *do not wait*.

## Scope

### In Scope

- A raising helper pair in `sinks/_retry.py` — `exact_int`, which refuses a non-`int` and returns
  the exact `int`, and `floor_count`, which floors that — and every `max(<int argument>, N)` floor
  on `max_retries` routed through it, `Worker.__init__`'s included (FR-001).
- The other `int` floors of the same shape, `RotatingFileSink`'s `max_bytes` and `backup_count`,
  `HTTPSink.max_batch_count`, `PostgresSink.connect_timeout` and `GooglePubSubSink.max_pending`;
  the type of `HTTPSink.max_retry_after`, a `float` door judged floor-side for its value; and a
  `RotatingFileSink` rollover period too large for a `float` (FR-002).
- The two comparison-only `int` doors, `SocketTransport.port` and
  `NATSSink.max_reconnect_attempts`, and every refusal in this spec's helpers and doors and their
  twins that names an `int` value, rendered without converting one too long to print (FR-003).
- The public `flush(timeout=)` and `shutdown(timeout=)` verdicts above, at the call, before
  anything is claimed or counted (FR-004).
- A structural test that derives the sites from the AST and fails naming one, with its own
  fixture corpus and a derived mutation sweep (FR-005).
- The docs: the three §12 items resolved and one new item opened, the decision file, invariants 1
  and 13, the v1.1.0 notes (FR-006).

### Out of Scope

- **Private ints the library supplies itself**: `chunk_items`, `_diag.lost`, `Deferral.count` and
  `truncate_*`. No caller can hand them a float, so there is no door.
- **`Worker.batch_size`, `max_queue` and `flush_interval`.** `Worker` is not public and its tunables
  are unreachable from `configure()` (invariant 10). `Worker.max_retries` is in only because §12
  named it and the edit is one line in `__init__`; **`Worker._emit` is not touched** — its own
  `max(self.max_retries, 0)` stays, since `_emit` is being rewritten in the exit-drain-budget work
  running in another session (branch `claude/062-exit-drain-budget-c74d8d`), and after FR-001 it is a
  no-op on an already-floored int. **Coordination:** FR-001's `Args:` update on `Worker.__init__`
  rewrites the `max_retries:` line that branch also rewrites, a guaranteed textual conflict if both
  land; this spec rebases onto SPEC-062 if it lands first and keeps both edits.
- **A delegating sink's unread argument.** `SentrySink(backend="sdk", max_retries=2.5)` stays
  accepted: the SDK backend builds no `HTTPSink`, so no door sees the value, and the sink's own body
  neither floors nor range-checks it (its docstring already puts `max_retries` outside the
  unused-argument rule). `SyslogSink` and `LogstashSink` always build the delegate, so they refuse.
- **SPEC-062 has landed** (`0aafcc1`, on `main` beneath this spec's branch), so the two notes above
  and below that name its branch are history: no rebase is owed, and their fences stand — `_emit`,
  `Worker.stop`, `claim_stop` and `_shutdown_worker` are not edited, and `Worker.__init__`'s
  `max_retries:` entry keeps SPEC-062's sentences.
- **`Worker.stop`, `claim_stop` and `_lifecycle._shutdown_worker`.** That session owns them. The door
  here is the public `flush` and `shutdown` functions: the internal code receives `None` or a finite
  number and never `inf` or `nan`, and the `atexit` path binds `_shutdown_worker` with its constant
  default, so it needs no door.
- **`usable_timeout` callers** (`KafkaSink.flush_timeout`, `NATSSink.publish_timeout`,
  `GooglePubSubSink.overflow_timeout`). The decision settles them floor-side, and SPEC-065 already
  made the helper judge the exact builtin.
- **NATS's float timeouts** — `connect_timeout`, `reconnect_time_wait` and `drain_timeout` — are
  found, not fixed: they are unchecked floats, not int doors, and `nats.py` is being rewritten in
  another session. FR-006 records them as an Open item in §12. This spec's NATS change is the one
  door, `max_reconnect_attempts`, so as not to widen the conflict.
- **`retarget`, `closer_grace` and `join_closers`**, which take monotonic instants and are internal.
- **Float-annotated parameters.** `max(timeout, 0.0)` on a `float` argument is a different door with
  different rules (`require_timeout`, `usable_timeout`); the guard in FR-005 does not examine one.
  `HTTPSink.max_retry_after` is the one `float` door this spec touches (FR-002), and its tests,
  not the guard, hold it.
- **A `float` timeout too large for the socket.** `require_timeout` accepts any positive finite
  number, and `socket.settimeout` raises `OverflowError` above about `9.2e9` seconds, so
  `HTTPSink(timeout=1e10)` constructs and raises out of `emit` uncounted. Found while answering
  the second diff review; it is SPEC-049's `float` door, not an `int` door, so FR-006 records it
  as an Open item in §12 rather than this spec fixing it.
- **Constructors other than `__init__`.** The guard starts from `__init__` only, following one call
  level from it (FR-005); a classmethod factory forwarding an `int` to one is judged there.
- **An `int` passed through unchecked** to a client (no `max`, no comparison). It is not an
  instance of the shape this spec closes; the guard does not report it.
- **Flooring a float** by `math.ceil` or `int()`. SPEC-065's Overview carries the reason.
- **A maximum on a count.** `max_retries=10**9` is an `int` and stays accepted.

### Twin paths (invariant 6)

- **Twelve `max_retries` constructors and the worker.** One helper serves all thirteen sites, and the
  AST guard (FR-005) derives twelve of them, so a fourteenth cannot be left on a bare `max`. The
  roster in FR-001's test is checked against the guard's judged set; the guard cannot see a bare
  `self.max_retries = max_retries` (no `max`, no comparison), so the roster is what holds `Worker`.
- **Delegating sinks.** `SyslogSink` and `LogstashSink` (both branches) reach `SocketTransport` or
  `HTTPSink`, and `SentrySink`'s HTTP backend reaches `HTTPSink`; each is probed through its own
  constructor, not only through the class that does the judging.
- **`flush()` and `shutdown()`** are the two answerers of a flush marker. They share one
  normalisation, so a value cannot be an unbounded wait for one and an error for the other.
- **`sinks/_retry` and `config`.** `exact_int` and `config._require_positive` normalise with the same
  `int.__index__`; `exact_int` shares its message shape (`<owner> <name> must be an integer, not
  <type>`) with `_retry.require_positive`, not with `config`, which says `<name> must be an int, got
  <type>` (SPEC-065). Both `_require_positive`s render a refused value through one
  `sanitize._int_text` (FR-003), so an `int` too long to print is named the same way at both.

---

## Functional Requirements

### FR-001: `max_retries` is refused when it is not an int, and floored when it is

#### Description:

`sinks/_retry.py` gains two functions beside `require_positive`. `exact_int(value, name, owner)`
raises `TypeError(f"{owner} {name} must be an integer, not {type(value).__name__}")` for a value that
is not an `int` instance and otherwise returns `int.__index__(value)`. `floor_count(value, floor,
name, owner)` returns `max(exact_int(value, name, owner), floor)`. Each of the twelve
`max(max_retries, 0)` sites listed in the Overview, and `Worker.__init__`'s store of `max_retries`,
becomes `floor_count(max_retries, 0, "max_retries", <owner>)`. Behaviour for any `int` is unchanged:
a negative still floors to 0 and the sink delivers. Each constructor's `Args:`/`Raises:` say so, and
so do those of `SyslogSink`, `LogstashSink` and `SentrySink`, which forward `max_retries` (and
`SyslogSink` and `LogstashSink` `port`, FR-003) and whose callers read the class they constructed,
not the delegate; `Worker._emit` is not edited.

**Judge first, build second — this holds for every argument in this spec.** A constructor judges
each argument it floors or range-checks before it builds any owned client, socket, connection or
file. Replacing `max(max_retries, 0)` where it stands is not enough: `MongoDBSink` builds its
`MongoClient` (which starts monitor threads) before the floor, `AzureEventHubsSink` its producer,
the Redis and the four boto3 sinks their clients, so a refusal raised after the build would leave an
unclosed client behind. Each of those floors moves above the build. A constructor that is handed a
client builds nothing, so its refusal is the same; the criterion below is about the sinks that build
their own. An exact result is `type(x) is int`: `max(True, 1)` returns `True` and `True == 1`, so a
criterion saying "stores `1`" would pass against the bare floor.

**What the roster is compared with.** The scan (FR-005) judges `_RedisSink.__init__` and
`Worker.__init__`, not the public Redis classes, whose `__init__` forwards through `super()`. The
roster names concrete classes and resolves each through its MRO to the first class that the scan's
judged set names for `max_retries`: `RedisStreamsSink` and `RedisListSink` resolve to `_RedisSink`,
`Worker` is in the roster and resolves to itself. The comparison fails naming a roster class that
resolves to nothing, and a judged class that no roster class resolves to. `Worker` is in the roster
because the scan cannot see a bare `self.max_retries = max_retries` (no `max`, no comparison).

#### Acceptance Criteria:

- [ ] For each of the thirteen classes — the twelve constructors, both Redis sinks named
      individually — and each of `2.5`, `float("nan")`, `float("inf")`, `"2"` and `None`,
      construction raises `TypeError`
      whose message contains `max_retries` and the type name (`float`, `str`, `NoneType`); at
      `7dc881f` the floats constructed and `"2"` and `None` raised a raw `TypeError` from the
      comparison naming neither. The sinks are built as their existing construction tests build
      them (an injected client, opener or transport).
- [ ] `HTTPSink("http://x", max_retries=2.5)` raises `TypeError` naming `max_retries` and `float`.
- [ ] `SyslogSink`, `LogstashSink` (the `host`/`port` branch and the `url` branch) and `SentrySink`
      with its HTTP backend each raise `TypeError` naming `max_retries` for `2.5`, and each one's
      `__init__` docstring names `TypeError` under `Raises:`.
- [ ] Judged before built: with the client factory patched to count calls, `MongoDBSink(uri=...,
      max_retries=2.5)` leaves the `MongoClient` count at 0, and so do `AzureEventHubsSink`, both
      Redis sinks built from a URL and the four boto3 sinks built without an injected client, for
      their own `max_retries`; `RotatingFileSink` leaves no file (FR-002).
- [ ] `Worker(sink, max_retries=2.5)` raises `TypeError` naming `max_retries` and `float`, and
      `Worker(sink, max_retries=-1).max_retries` is the exact `int` `0`.
- [ ] `max_retries=-1` still floors to 0 and delivers: `HTTPSink("http://x", max_retries=-1)` posts
      the batch once against a healthy opener, `sink.max_retries == 0` and
      `type(sink.max_retries) is int`; the existing `-1` pins in `tests/test_sink_losses.py` stay
      green unchanged.
- [ ] `max_retries=True` stores the exact `int` `1` (`type(x) is int`), and `max_retries=False` the
      exact `0`, on every class in the first criterion.
- [ ] An `int` subclass is judged by the number it holds: `Lie(-3)` (a `class Lie(int)` whose `__lt__`
      and `__gt__` return `False`) stores the exact `int` `0`, and `Lie(3)` stores the exact `3`,
      on every class; at `7dc881f` the first stored the `Lie` itself.
- [ ] The roster (the thirteen classes and `Worker`, fourteen in all, the Redis pair resolved
      through the MRO to `_RedisSink`, so thirteen judged sites) is compared with the FR-005 scan's
      judged `max_retries` set, and the test fails naming a class that is in one and not the other.
- [ ] `tests/test_public_surface.py` and `tests/typed_consumer/` are green unchanged, and no
      signature or default of a shipped constructor differs.
- [ ] Serves invariant 13 (a bad argument is refused where it is written, not on every `emit`),
      invariant 6 (thirteen sites, one judgement) and invariant 10 (no signature moves).

### FR-002: The other int floors refuse a non-int at the door

#### Description:

The remaining `max(<int argument>, N)` floors go through `floor_count`, owner and argument named.
`RotatingFileSink.max_bytes` and `backup_count` (floor 0) are judged before the file is opened, so a
refused construction leaves nothing on disk. `HTTPSink.max_batch_count` (`int | None`, floor 1) and
`GooglePubSubSink.max_pending` (`int | None`, floor 1) judge a supplied value and take their class
default for `None`, as today. `PostgresSink.connect_timeout` (`int`, floor 2) is judged, whether or
not an injected connection will ever read it (the reach of a type refusal, in the Overview). A
float, `nan`, `inf` or `str` is refused with `TypeError`; an `int` is floored exactly as before.
`max_pending=inf`, which meant no bound, is refused; `max_pending=None` is `DEFAULT_MAX_PENDING`, as
it was. Every judgement here precedes any owned resource (FR-001), and a "stores `N`" below means
the exact `int` (`type(x) is int`).

**`HTTPSink.max_retry_after`** is a `float` door, so its value stays floor-side: a ceiling that is
zero, negative or `NaN` still reaches `clamp_server_delay`, which falls back to exponential
backoff, exactly as before. Its *type* is judged at construction, beside `max_retries` and before
the default opener is built, by a new `_retry.exact_seconds(value, name, owner)`, the type half
of `require_timeout`: a value that is not an `int` or `float` instance raises
`TypeError(f"{owner} max_retry_after must be a number of seconds, not {type(value).__name__}")`,
`require_timeout`'s message shape, and an accepted value is stored as the exact builtin through
`_exact_number`, so a lying `float` subclass is judged by the number it holds. `require_timeout`
takes its own type check from `exact_seconds`, so the two cannot drift. The seven `HTTPSink`
subclasses forward the keyword through their `**http_kwargs`, and `LogstashSink` does in HTTP
mode, so each one's `Raises:` names it, as `HTTPSink`'s does.

**The rollover period.** `_rollover_seconds` converts `unit * interval` to a `float` before the
file is opened, and a period that overflows is refused with `ValueError` naming `interval`. Every
period that converts is still accepted: a very long one is a time trigger that never fires, which
works.

#### Acceptance Criteria:

- [ ] `RotatingFileSink(path, backup_count=2.5)` raises `TypeError` naming `backup_count` and
      `float` at construction, and `path` does not exist afterwards; the same for `inf`, `nan` and
      `"2"`, and for `max_bytes` with each of `2.5`, `nan`, `inf` and `"2"`. At `7dc881f` the
      floats constructed and `"2"` raised a raw `TypeError` from the comparison.
- [ ] A refused `backup_count` cannot kill the sink: a `RotatingFileSink` built with
      `backup_count=2` over a size rollover keeps rotating and `close()` returns, and the
      `2.5` construction that used to rotate once and then fail every `emit` raises at the call.
- [ ] `RotatingFileSink(path, max_bytes=-1)` and `(path, backup_count=-1)` still behave as `0` and
      store the exact int `0`; `True` stores the exact int `1`.
- [ ] `HTTPSink(url, max_batch_count=...)` raises `TypeError` naming `max_batch_count` for `2.5`,
      `nan`, `inf` and `"3"`; `-3` and `0` floor to the exact `1` and the sink delivers one event per
      request; `True` stores the exact `1`; `None` stores the class's `MAX_BATCH_COUNT`.
- [ ] `GooglePubSubSink(topic, client=c, max_pending=...)` raises `TypeError` naming `max_pending`
      for `2.5`, `nan`, `inf` and `"3"`; `0` and `-3` floor to the exact `1`; `True` stores the exact `1`;
      `None` stores `DEFAULT_MAX_PENDING`; a large `int` such as `sys.maxsize` is accepted.
- [ ] `PostgresSink` with `connect_timeout` of `2.5`, `nan`, `inf` or `"5"` raises `TypeError` naming
      `connect_timeout`, with an injected `connection=` as well as without; `0` and `1` floor to the
      exact `2`; `True` stores the exact `2` (a pin, not a fail-first case: `max(True, 2)` was
      already the exact `2` at `7dc881f`); `30` is stored as `30`.
- [ ] A `Lie` subclass (as in FR-001) is stored as the exact `int` it holds at all five arguments.
- [ ] `HTTPSink(url, max_retry_after=...)` raises `TypeError` for `"30"`, `None`, `b"30"` and
      `fractions.Fraction(30)`, whose message is exactly `HTTPSink max_retry_after must be a number
      of seconds, not <type>`; at `7dc881f` each constructed, and `"30"` and `None` then raised a
      raw `TypeError` out of `emit` on a 429 with `failed` at zero.
- [ ] Each of `DatadogSink`, `ElasticsearchSink`, `HoneycombSink`, `LokiSink`, `NewRelicSink`,
      `OTLPSink` and `SplunkHECSink` refuses `max_retry_after="30"` with that message carrying its
      own class name, and `LogstashSink` in HTTP mode refuses it naming `HTTPSink`; the
      `TypeError:` entry of each one's `Raises:`, and of `HTTPSink`'s, names `max_retry_after`.
- [ ] A `float` subclass whose `<` and `>` return `True` and which holds `-1.0` is stored as the
      exact `float` `-1.0`, and a 429 with `Retry-After: 1` then waits the exponential backoff
      (`_BACKOFF_BASE`), not the server's delay; at `7dc881f` the subclass itself was stored.
- [ ] Controls, the floor side unchanged: with an opener answering 429 with `Retry-After: 1` once
      and then 200, and `wait` patched to record, a ceiling of `nan`, `0` or `-1.0` waits the
      backoff, `inf`, `2.5`, `True` and `10**400` wait `1.0`, `0.5` waits `0.5`, and every one
      delivers in two requests with `failed` at zero.
- [ ] `RotatingFileSink(path, when="S", interval=10**400)`, `(when="D", interval=10**305)` and
      `(when="s", interval=10**5000)` raise exactly `ValueError` (`type(e) is ValueError`) whose
      message starts `RotatingFileSink interval is too large: `, and `path` does not exist
      afterwards; at `7dc881f` each raised `OverflowError` with the file already created. A
      control: `when="S", interval=10**300` constructs, writes and closes, and never rotates.
- [ ] Serves invariant 13 and invariant 6, whose twin doors — these constructors and FR-001's —
      must give one verdict on the same value.

### FR-003: The port and NATS reconnect attempts are judged for type

#### Description:

`SocketTransport.port` is passed to `exact_int` and the exact `int` is stored, and the existing
`0`–`65535` `ValueError` then runs on that value and reports it. **Required shape:** the result is
rebound to the parameter name — `port = exact_int(port, "port", "SocketTransport")`, and likewise
`max_reconnect_attempts = exact_int(...)` — so the comparison reads the parameter itself. That is
what lets the FR-005 sweep see the door go: a mutant `port = port` followed by a comparison on the
parameter is reported, where `self._port = exact_int(port, ...)` followed by a comparison on
`self._port` would be mutated into a comparison the scan never sees. `NATSSink.max_reconnect_attempts`,
when not `None`, is passed to `exact_int`, the existing `<= 0` refusal runs on the exact value with
its existing message (the explanation of why `nats-py` needs a positive bound is kept), and the
exact `int` is what reaches the client's keyword arguments. The NATS edit is that one door.

**Naming an `int` too long to print.** Both doors' range refusals name the value, so each renders
it through a new `sanitize._int_text(value)`, the free-function form of the convention in
`docs/decisions/event-assembly/a-value-too-large-to-render-is-replaced-never-clipped.md`: the
digits through the unbound `int.__repr__`, or `<int: ~N digits>` when the `bit_length()` estimate
(the one `_Coercer._bounded_int` uses) exceeds the interpreter's digit limit, capped at the
limit's default, 4,300, even when the limit is lifted or `0`, so a refusal never renders millions
of digits. The value is never converted to a string to measure it. The other refusals in this
spec's helpers and their twins that name an `int` value use it too — `_retry.require_positive`,
`require_timeout` (when the exact value is an `int`), `config._require_positive` and
`RotatingFileSink`'s new period refusal (FR-002) — found by enumerating, by AST, every
interpolation inside a `raise` in `src/`; the rest name strings, types, library-computed counts,
or a `usable_timeout` result (`KafkaSink.flush_timeout`, `NATSSink.publish_timeout`), a `float`
door this spec leaves alone (Out of Scope). `sanitize` imports nothing from the package at runtime, so `_retry`, `config`
and the sinks can import it without a cycle.

#### Acceptance Criteria:

- [ ] `SocketTransport("h", 2.5)` raises `TypeError` naming `port` and `float`, for UDP and TCP, and
      for `"514"`, `nan` and `inf`; at `7dc881f` `2.5` constructed and raised out of every `emit`,
      and `nan` and `inf` were refused by the range check, naming the value but not the type.
- [ ] `SocketTransport("h", True)` stores the exact int `1`; `0` and `65535` are accepted and `-1`
      and `65536` still raise `ValueError` naming `port` and the value.
- [ ] A `Lie(70000)` port whose comparisons return `True` raises `ValueError` reporting `70000`, and
      a `Lie(514)` whose `__le__` returns `False` is accepted and stored as the exact `514`.
- [ ] `SyslogSink` and `LogstashSink` with `port=2.5` raise `TypeError` naming `port`, and their
      `Raises:` name it. The result of `exact_int` is rebound to `port` and to
      `max_reconnect_attempts` (the FR-005 sweep reddens when either call is removed).
- [ ] `NATSSink(max_reconnect_attempts=2.5)` raises `TypeError` naming `max_reconnect_attempts` and
      `float`, for `nan`, `inf` and `"3"` as well; `0` and `-1` still raise `ValueError` matching
      `max_reconnect_attempts must be positive` (the existing pin); `None` is unchanged; `3` and
      `True` reach the client keyword arguments as the exact ints `3` and `1`.
- [ ] Each of `SocketTransport("h", 10**5000)`, `SocketTransport("h", -10**5000)`,
      `HTTPSink(url, max_batch_bytes=-10**5000)` (`require_positive`), `HTTPSink(url,
      timeout=-10**5000)` (`require_timeout`), `RotatingFileSink(path, interval=-10**5000)`,
      `NATSSink(subject, max_reconnect_attempts=-10**5000)` and
      `configure(max_value_bytes=-10**5000)` raises exactly `ValueError` whose message begins
      with the door's own wording, naming the argument (and the class, for a sink), with the
      value rendered as `<int: ~5001 digits>`; at `7dc881f` each raised CPython's `Exceeds the
      limit (4300 digits)` out of the message itself.
- [ ] `_int_text` returns `70000` and `-5` as digits, a 4,001-digit `int` as its digits, an `int`
      subclass with a lying `__repr__` as its digits, and `<int: ~6001 digits>` for `10**6000` with
      the interpreter limit set to `0`, and honours a limit lowered to `1000`.
- [ ] Serves invariant 13 and invariant 6: the port is the door `SocketTransport`'s three delegating
      sinks share, and the rendering is one function for every refusal that names an `int`.

### FR-004: `flush()` and `shutdown()` give inf, NaN and a non-number a verdict at the call

#### Description:

`flush(timeout=)` and `shutdown(timeout=)` in `src/log_foundry/__init__.py` normalise `timeout`
through one private function, `_retry.wait_budget(value, name)`, before anything else, and pass its
result on. The verdict: `None` is `None`. A value that is not an `int` or `float` instance raises
`TypeError(f"{name} must be an int or float number of seconds, or None, not {type(value).__name__}")`;
that is the settled rule, and it refuses what only worked by duck typing (`flush(fractions.Fraction(2))`
returned `ok=True` at `7dc881f`), so the message says which types are accepted rather than
describing a `Fraction` as not a number. An `int` or
`float` instance is read as the exact builtin (`_exact_number`). `nan` raises `ValueError(f"{name}
must not be NaN")`. A value above `threading.TIMEOUT_MAX` — `inf` included — is `None`. A value
below `-threading.TIMEOUT_MAX` is `0`, so that no negative can overflow the deadline sum. Everything
else, negatives and zero included, is passed as it is today. The refusal is raised at entry, before
a sweep, a claim or a retirement count, so a retry with a good value works and `health().retired`
does not move on a refused `shutdown`. Both public docstrings' `Raises:` — today `None.` — name exactly the two refusals, and `flush`'s
"Call it in a `finally`" guidance gains one sentence saying a refusal is raised at entry for a bad
`timeout` argument and nothing else. The never-the-reason-a-caller-fails sentence is
`_lifecycle._flush_worker`'s own `Raises:`, which this spec does not edit and which stays true,
because the refusal happens in `flush` before `_flush_worker` is reached. FR-004 edits nothing in
`worker.py` or `_lifecycle.py`.

#### Acceptance Criteria:

- [ ] With a worker draining a sink that takes about 0.3 s per batch, `flush(timeout=math.inf)`
      returns a truthy `FlushResult` after the sink has received every event, having waited rather
      than returned at once; at `ce2e1fe` it returned `thread-died` at 0.00 s.
- [ ] The same setup: `shutdown(timeout=math.inf)` and `shutdown(timeout=1e300)` each return `None`
      having closed the sink, and `health().retired` is true; at `ce2e1fe` both raised
      `OverflowError`. `shutdown(timeout=10**400)` and `flush(timeout=10**400)` behave the same.
- [ ] `flush(timeout=threading.TIMEOUT_MAX)` and `shutdown(timeout=threading.TIMEOUT_MAX)` behave
      as `None` does and do not raise, and the value passed on is not rewritten.
- [ ] `flush(math.nan)` and `shutdown(math.nan)` raise `ValueError` whose message contains `timeout`;
      afterwards `health().retired` is false and `shutdown(2.0)` still drains and closes the sink,
      and `flush(2.0)` after a refused `flush(nan)` returns truthy.
- [ ] `flush("5")`, `flush(b"5")`, `shutdown("5")` and `shutdown(object())` raise `TypeError` whose
      message contains `timeout` and the type name, and neither moves a counter nor retires.
- [ ] `flush(fractions.Fraction(2))` and `shutdown(decimal.Decimal(2))` raise `TypeError` whose
      message contains `Fraction` or `Decimal` and `int or float`; at `7dc881f` the first returned
      `ok=True`.
- [ ] `True` is read as `1`: `flush(True)` and `shutdown(True)` behave as `1` (this already held
      at `7dc881f`; the recorded-argument criterion below is what discriminates), and a `float` subclass
      whose comparisons say it is positive but which holds `0.0` is treated as `0.0` (this too already
      behaved so at `7dc881f`, measured at `fda134a`; the recorded-argument criterion below, which
      sees the exact builtin `float`, is what discriminates).
- [ ] `flush(0)`, `flush(-1)`, `shutdown(0)` and `shutdown(-1)` are unchanged: `flush` reports
      `timed-out`, `shutdown` returns and records `ShutdownTimeout` with the sink left open; so is
      `flush(None)` against a fast sink. `shutdown(-math.inf)` and `shutdown(-10**400)` do the
      same as `shutdown(-1)` and neither raises; at `7dc881f` the second raised `OverflowError`.
      `flush(-10**400)` reports `timed-out` like `flush(-1)`; at `7dc881f` it reported
      `thread-died`.
- [ ] A refused call writes nothing to stderr, and the module-level default of both functions is
      unchanged (`5.0` and `DEFAULT_SHUTDOWN_TIMEOUT`).
- [ ] The value that reaches `_flush_worker` and `_shutdown_worker` is never `inf` or `nan`: a test
      patches each to record its argument, calls with `inf`, `1e300`, `2.0`, `True` and the `float`
      subclass holding `0.0`, and sees `None`, `None`, `2.0`, the exact `int` `1` (`type(x) is int`,
      not `True`) and the exact `float` `0.0` (`type(x) is float`).
- [ ] Serves invariant 1 (a call accepted returns; a refusal is the recorded exception, at the
      call), invariant 3 (the bound is the caller's, and `inf` is the caller's choice of none) and
      invariant 13, and invariant 6, since `flush()` and `shutdown()` answer a marker from one
      normalisation.

### FR-005: A structural test derives the sites and fails naming one

#### Description:

`tests/test_numeric_door_roster.py` walks the AST of every `__init__` in every module under
`src/log_foundry/sinks/`, subpackages included (`rglob`, so a sink added in a subpackage is not
outside the population), and `worker.py`. It has two outputs. **Findings:** for each parameter
annotated `int`, or `int` unioned only with `None` however it is spelled (`int | None`,
`Optional[int]`, `Union[int, None]`, a string annotation), a site `<file>:<Class>.<param>: <shape>`
when the parameter appears inside a `max(...)` or `min(...)` call or a comparison other than
`is`/`is not` and does not also appear as an argument of `exact_int`, `floor_count` or
`require_positive` in that constructor. `min` is a shape for the reason `max` is: `min(nan, 10)` is
`nan`, so a cap passes a float through exactly as a floor does. Only those three judges silence it:
`isinstance`, `require_timeout` and `usable_timeout` refuse nothing for an `int` parameter (a
constructor with `self._b = isinstance(p, bool); self.p = max(p, 0)` accepts `2.5`). **Judged
set:** `<file>:<Class>.<param>` for every such parameter that does appear as an argument of one of
those three, the name FR-001's roster is compared with.

**One call level is followed.** When `__init__` passes the parameter itself, a bare name, as a
positional or keyword argument to a function defined in the same module, or to a method of the same
class called as `self.<name>(...)` (a `staticmethod` keeps its first parameter), the callee's
matching parameter is read in the callee's body by the same rules, and what is found is reported
against the `__init__` parameter, the shape naming the callee (`max in _floor`). So a judgement
moved into such a helper keeps the parameter judged, and a bare `max` moved into one is still a
finding. That follow adds one site to the judged set on the existing tree,
`RotatingFileSink.interval`, judged by `require_positive` inside `_rollover_seconds`, and no
finding: after the build the set is the nineteen sites this spec closes, `Worker.max_retries`, the
seven `require_positive` sites that exist at `7dc881f` and `interval`, twenty-eight. Its limits
are stated in the module docstring beside the pure-type limit: a bare `max` two calls down, in a
helper imported from another module, or on an expression such as `p + 1` passed to a helper is not
seen, and a door built that way is a review finding, not an exemption. The scan takes source text,
so a fixture corpus can feed it strings; the tree test feeds it the files. It is silent on float
arithmetic such as `max(0.0, deadline - time.monotonic())` and on a `float`-annotated parameter.

**The sweep's population** is the `floor_count` and `exact_int` calls that judge an examined
`__init__`'s parameter, in `__init__` itself or in a callee followed from it, found by the same
walk. `floor_count`'s own body (`max(exact_int(...), floor)` in `sinks/_retry.py`) is not in it,
because no `__init__` passes it a parameter of its own; that is the population's edge, not an
exemption. Two floors keep a vacuous scan from passing: the judged set holds at least nineteen, and
the sweep finds at least sixteen helper calls. The first is the number of sites this spec closes,
so it can fall only if one of them leaves the scan, with nine to spare for an unrelated
constructor to go (twenty-eight at the build); the second sits four below the twenty calls this
spec writes (eighteen `floor_count`, two `exact_int`), for the same reason. A call-finder that
matches nothing fails on its own count instead of mutating nothing.

**Every failure names its remedy**, because a guard read months later is read by someone who did
not write it: an unjudged door says to route the value through `floor_count`, `exact_int` or
`require_positive` before anything is built, and that a renamed judge belongs in `JUDGES`; a
population floor says what a rename or a moved module needs; the sweep says what to do when a
removed call goes unreported; a judged `max_retries` site no roster class claims says to add the
class to `MAX_RETRIES_DOORS` in `tests/test_numeric_doors.py`; a roster class nothing resolves to
says how far the follow reaches; and the roster count says that a class added to or removed from
`MAX_RETRIES_DOORS` changes it, here and in FR-001.

#### Acceptance Criteria:

- [ ] On the tree after FR-001 to FR-003 the scan reports nothing. Run once over a checkout of
      `7dc881f`'s sources, it reports exactly the nineteen sites of the Overview, each named by
      file, class and parameter (a one-off check, since a standing test cannot read history); the
      `min` shape, the union spellings and the follow add none there.
- [ ] The scan examines the real population: the file list includes `http.py`, `file.py`,
      `_socket.py` and `worker.py`, the judged set holds at least nineteen, and the sweep's call
      population is at least sixteen, so a glob, a rename or a call-finder that matches nothing
      fails instead of passing. A module in a subpackage of `sinks/` is in the list, and a bare
      `max` in one is reported naming its path (a fixture tree); with `glob` in place of `rglob`
      that fixture reddens.
- [ ] Fixtures assert the reason: it fires, naming the site and `max`, on `self.x = max(p, 0)`
      and on `max(p if p is not None else D, 1)`; naming `min` on `self.x = min(p, 10)`; naming
      `compare` on `if p <= 0: raise ...` for `p: int | None`; naming `max` for `p:
      Union[int, None]`, `p: typing.Union[None, int]` and `p: "int | None"`; and, through one call
      level, naming `max in _floor` for a method `self._floor(p)` (and a `staticmethod` of that
      name), `min in _cap` for a module function `_cap(p)`, and `compare in _check` for
      `self._check(value=p)`. It is silent when the same code also passes `p` to `floor_count`,
      `exact_int` or `require_positive`, or to a helper method that does, and it is not silenced
      by `isinstance(p, bool)` on `p` with `self.p = max(p, 0)`.
- [ ] Fixtures assert silence: `max(0.0, deadline - time.monotonic())` in a constructor, a
      `float`-annotated `max(timeout, 0.0)`, `p: int | float` and `p: Optional[str]` with a bare
      `max`, `p is None`, an `int` parameter only forwarded to a client, directly or through a
      helper, a helper that bounds an argument other than the one `p` was passed as, and a helper
      with a bare `max` that `__init__` calls without `p`.
- [ ] Guard mutants, each planted in a scratch copy of the test file and never in a tracked file,
      redden a named fixture: `min` dropped from the shapes, `glob` for `rglob`, the follow
      disabled, a `staticmethod` given a `self`, keyword arguments not mapped, a callee's judge
      ignored, a callee's helper calls left out of the sweep, the annotation read by fixed
      spelling, and the annotation's `int`-or-`None` subset test dropped.
- [ ] A derived mutation sweep: for every call in the sweep's population, the test rewrites that
      call in memory — `floor_count(x, N, ...)` to `max(x, N)`, `exact_int(x, ...)` to `x` — and
      asserts the scan then reports that site, the `port` and `max_reconnect_attempts` doors among
      them; deleting one helper call in a source file therefore reddens the test, and a fixture
      shows a call inside a followed helper is in the population and its removal reported as
      `max in <helper>`. No file is mutated.
- [ ] Every assertion in the module that reads the source tree, the in-memory mutator's included,
      carries a message naming its remedy, the `len(roster) == 14` check among them; the fixture
      tests, which read only strings, compare whole results instead.
- [ ] There is no exemption list: a legitimate unjudged site must be a finding on the helper or the
      spec, not an entry.
- [ ] Serves invariant 13 and invariant 6, the recurring shape a hand list of sites could not hold.

### FR-006: The docs record the closure

#### Description:

The three §12 Open items — the float floor on `max_retries` and `backup_count`, `flush`/`shutdown`
and `inf`/`nan`, and `SocketTransport(port=2.5)` — move to `### Resolved`, struck and marked
"→ closed by SPEC-066", in the format SPEC-065's resolved item uses. Two new Open items record
NATS's unchecked float timeouts and the `float` timeout too large for a socket (Out of Scope). The
sink-delivery decision's fence and body change: the clause "the
`max(x, 0)` floors on `max_retries` and `backup_count` are not yet held to it (arch §12)" becomes the
closure, with a dated History entry, and its body names the one `float` door this spec judges
for type, `HTTPSink.max_retry_after`. Invariant 1's *Recorded exceptions* and invariant 13's text
name `flush()` and `shutdown()` refusing a timeout at the call. `docs/release-notes/v1.1.0.md` gains
a `### ... (SPEC-066)` subsection at the end of `## Fixed`, and its lead paragraph's "without a code
change unless the code passes a `float` payload ceiling to `configure()`" sentence gains this spec's
caller-visible breaks. `CHANGELOG.md`, `RELEASES.md` and the notes title are not touched.

#### Acceptance Criteria:

- [ ] `grep` for the three item headings finds each only under `### Resolved`, each with
      "closed by SPEC-066", and `### Open` holds two new items: one naming `connect_timeout`,
      `reconnect_time_wait` and `drain_timeout` on `NATSSink`, and one naming
      `require_timeout` and `socket.settimeout`'s `OverflowError`.
- [ ] The decision file's `description` and body no longer say the `max(x, 0)` floors are not
      yet held, its body names `max_retry_after` as refused for type and judged floor-side for
      value, its History carries a SPEC-066 entry dated the day the build lands, and `sh
      scripts/gen-index.sh` leaves `docs/decisions/index.yaml` carrying the new fence.
- [ ] In `docs/invariants.md`, the *Recorded exceptions* of the first entry and the text of the
      thirteenth each contain `flush()` and `shutdown()` and the word `timeout`.
- [ ] The v1.1.0 subsection is the last in `## Fixed`; names a float or `inf` to an int sink
      argument and a `nan` or non-number timeout as the breaks; names the timeouts that worked and
      are now refused (a `Fraction` for `flush` only, a numpy integer scalar on both calls), says a
      `Decimal`, a numpy `float32` or a `str` timeout already failed (`flush` reported
      `thread-died`, `shutdown` raised a bare `TypeError`) and now fails with a named `TypeError`
      at entry, says a numpy `float64` is a `float` and keeps working, and names a numpy integer at
      an int argument as refused; says a negative
      `max_retries` still floors, `inf` as a timeout now waits, and
      `GooglePubSubSink(max_pending=inf)` is now refused (pass a large `int`); says a `str` or
      `None` `HTTPSink` `max_retry_after`, which raised out of `emit` on the first `Retry-After`,
      is now refused at construction; says a `Fraction`, `Decimal` or numpy `float32` there
      delivered only while the server asked for no more than the ceiling and raised out of `emit`
      once it asked for more, and that a numpy integer worked, all now refused; says an accepted
      ceiling is stored as the exact builtin; says a `RotatingFileSink` period too large for a
      `float` is now a `ValueError` naming `interval` before the file is opened, where it was an
      `OverflowError` after; and says a refusal naming an `int` too long to print now names it
      `<int: ~N digits>`; the lead paragraph names the same breaks.
- [ ] `sh scripts/docs-lint.sh`, `sh scripts/spec-lint.sh` and `sh scripts/gen-index.sh` pass with
      no diff left over from the index.
- [ ] The docstrings that changed — each constructor's `Args:`/`Raises:`, `flush`, `shutdown`, and
      the new helpers — pass `poetry run python scripts/docstring-lint.py`.
- [ ] Serves no invariant: this FR is prose and register edits, which their own rules judge; the
      page entries it edits only record what FR-004 changes.

---

## Data Model

```
// src/log_foundry/sinks/_retry.py
exact_int(value, name, owner) -> int
    // not an int instance -> raise TypeError(f"{owner} {name} must be an integer, not {type(value).__name__}")
    // else                -> int.__index__(value)            (type(result) is int)
floor_count(value, floor, name, owner) -> int
    // max(exact_int(value, name, owner), floor)
exact_seconds(value, name, owner) -> float                  // the type half of require_timeout
    // not an int/float instance -> raise TypeError(f"{owner} {name} must be a number of seconds, not {type(value).__name__}")
    // else                      -> _exact_number(value)     (the exact int or float)
wait_budget(value, name) -> float | None                     // the public waits' normalisation
    // None                      -> None
    // not an int/float instance -> raise TypeError(f"{name} must be an int or float number of seconds, or None, not {type(value).__name__}")
    // exact = _exact_number(value)
    // exact != exact (NaN)      -> raise ValueError(f"{name} must not be NaN")
    // exact > threading.TIMEOUT_MAX (inf too) -> None
    // exact < -threading.TIMEOUT_MAX          -> 0.0
    // else                      -> exact

// src/log_foundry/sanitize.py
_int_text(value) -> str                                      // an int named in a refusal
    // digits = int.bit_length(value) * 30103 // 100000 + 1
    // ceiling = sys.int_info.default_max_str_digits if the limit is 0, else min(limit, default)
    // digits > ceiling -> f"<int: ~{digits} digits>"
    // else             -> int.__repr__(value)
```

`flush(timeout: float | None = 5.0)` and `shutdown(timeout: float | None = DEFAULT_SHUTDOWN_TIMEOUT)`
keep their signatures and defaults; no public name moves (invariant 10).

## API / Interface Contract

```
HTTPSink("http://x", max_retries=2.5)        # TypeError: HTTPSink max_retries must be an integer, not float
HTTPSink("http://x", max_retries=-1)         # floors to 0, delivers
SocketTransport("h", 2.5)                    # TypeError: SocketTransport port must be an integer, not float
RotatingFileSink("app.log", backup_count=2.5)  # TypeError, no file created
HTTPSink("http://x", max_retry_after="30")   # TypeError: HTTPSink max_retry_after must be a number of seconds, not str
SocketTransport("h", 10**5000)               # ValueError: SocketTransport port must be 0-65535, not <int: ~5001 digits>
RotatingFileSink("app.log", when="S", interval=10**400)  # ValueError: RotatingFileSink interval is too large: ..., no file created
log_foundry.flush(timeout=math.inf)          # drains, truthy result (an unbounded wait, as None)
log_foundry.shutdown(timeout=math.nan)       # ValueError: timeout must not be NaN; nothing retired
log_foundry.flush("5")                       # TypeError: timeout must be an int or float number of seconds, or None, not str
```

## File & Folder Structure

```
src/log_foundry/sinks/_retry.py     # exact_int, floor_count, exact_seconds, wait_budget
src/log_foundry/sanitize.py         # _int_text, for a refusal naming an int
src/log_foundry/config.py           # _require_positive's message renders through _int_text
src/log_foundry/sinks/{datadog,elasticsearch,honeycomb,logstash,loki,newrelic,otlp,
                       splunk}.py   # Raises: names a forwarded max_retry_after
src/log_foundry/sinks/{_socket,clickhouse,eventhubs,file,firehose,http,kinesis,mongodb,nats,
                       postgres,pubsub,rabbitmq,redis,sns,sqs}.py   # the door at each argument
src/log_foundry/worker.py           # Worker.__init__ only
src/log_foundry/__init__.py         # flush and shutdown normalise at entry
tests/test_sink_retry.py            # the three helpers
tests/test_numeric_doors.py         # FR-001 to FR-004 matrices and the public waits
tests/test_numeric_door_roster.py   # FR-005
docs/architecture.md, docs/invariants.md, docs/release-notes/v1.1.0.md,
docs/decisions/sink-delivery/a-bad-argument-is-floored-….md
```

## Implementation Phases

### Phase 1: The helpers and the constructor doors

- Add `exact_int` and `floor_count` to `sinks/_retry.py` and unit-test them beside the existing
  validators, including the `Lie` subclass, run first against a bare `max` to watch it redden.
- Route the twelve `max_retries` sites, `Worker.__init__`, the five other floors, `port` and
  `NATSSink.max_reconnect_attempts` through them; keep NATS to that one door. Update each
  constructor's `Args:` and `Raises:` (the three delegating sinks' too), each judgement above any
  client, socket, connection or file the constructor builds. Rebase onto SPEC-062 first if it has
  landed.
- Add the construction matrices of FR-001 to FR-003, each run first against `7dc881f` to watch the
  defect reproduce.

### Phase 2: The public waits

- Add `wait_budget` and call it first in `flush` and `shutdown`; rewrite both public `Raises:` sections and
  add the one sentence to `flush`'s `finally` guidance. Do not edit `worker.py` or
  `_lifecycle.py`.
- Add the FR-004 tests with a draining sink that takes a fraction of a second, a generous budget on
  anything asserted to be bounded, and a patched `_flush_worker`/`_shutdown_worker` to see what
  reaches them.

### Phase 3: The guard

- Write `tests/test_numeric_door_roster.py` with the scan taking source text, the fixture corpus, the
  tree test, the population floors and the derived mutation sweep; run the sweep once to see every
  site redden.

### Phase 4: Documentation

- Resolve the three §12 items and open the NATS one; update the decision file, invariants 1 and 13,
  and the v1.1.0 notes as FR-006 states; run `sh scripts/gen-index.sh` and the lints.

---

## Revision history

- **Spec review round 1 answered.** Majors: the sweep population is helper calls inside an
  `__init__` the scan examines, with floors on the scan's own counts (M1); the scan's two outputs
  and the MRO-resolved roster, `Worker` included, are specified (M2); every argument is judged
  before any owned client is built, with a no-client criterion (M3). Minors: wording of what
  constructed at `7dc881f` (1); exact-`int` criteria and the `True` recorded-argument check (2);
  silencing set cut to three helpers (3); `exact_int` result rebound to the parameter (4); reach of a
  type refusal and the delegating-sink Out of Scope (5); `Fraction`/`Decimal`/numpy and the message
  (6); twin-paths message shape (7); the `Raises:` sentence actually meant (8); SPEC-062 conflict
  note (9); "judged" defined with its headroom (10); History date "the day the build lands" (11);
  `flush(-10**400)` (12); `max_pending` wording (13); delegating sinks' `Raises:` (14).
- **Plan review answered.** The numpy wording of FR-006's notes criterion is narrowed (a numpy
  `float64` is a `float` and keeps working as a timeout); the twelve `max_retries` sites are thirteen
  classes and the roster fourteen; the `float`-subclass and `connect_timeout=True` cases are named as
  pins, the recorded-argument criterion carrying the float subclass; SPEC-062's landing is noted in
  Out of Scope.
- **Second diff review answered.** `HTTPSink.max_retry_after` refuses a non-number at construction
  through `exact_seconds` and stores the exact builtin, its value still judged floor-side, and every
  forwarding class's `Raises:` says so (FR-002); a `RotatingFileSink` period too large for a `float`
  is refused naming `interval` before the file is opened (FR-002); a refusal naming an `int` too
  long to print renders it through `sanitize._int_text` at every site found by enumerating the
  interpolations in `raise` statements (FR-003); the guard reports `min`, reads every union
  spelling of `int | None`, scans `sinks/` recursively, follows one call level into a same-module
  helper, and names a remedy in every tree-reading failure (FR-005), the follow adding
  `RotatingFileSink.interval` to the judged set (twenty-eight). A `float` timeout too large for a
  socket was found on the way and recorded as Open rather than fixed (Out of Scope, FR-006).
