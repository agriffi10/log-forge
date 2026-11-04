# Completed Spec — SPEC-066: Numeric Doors Refuse What Is Not a Number: Sink Counts, the Socket Port and the Public Waits

## What was completed?

Six FRs, built as `5c3e0612..bbcaf883`. SPEC-065's rule, that a value outside the declared type is
refused with a named `TypeError` and never floored, now holds at the doors SPEC-065 left, the three
`architecture.md` §12 items its review opened.

- **The `int` floors** (FR-001, FR-002): `max_retries` on twelve sink constructors and
  `Worker.__init__`, `RotatingFileSink`'s `max_bytes` and `backup_count`, `HTTPSink.max_batch_count`,
  `PostgresSink.connect_timeout` and `GooglePubSubSink.max_pending` go through the new
  `_retry.floor_count`, which refuses a non-`int` through `_retry.exact_int` and floors the exact
  `int`. Every judgement precedes the client, connection or file the constructor builds.
  `max_pending=inf` is now refused. The delegating sinks (`SyslogSink`, `LogstashSink`, `SentrySink`'s
  HTTP backend) refuse through their delegate.
- **`HTTPSink.max_retry_after`** (FR-002) is judged for type by the new `_retry.exact_seconds`,
  which `require_timeout` now uses for its own type check. Its value stays floor-side in
  `clamp_server_delay`.
- **The rollover period** (FR-002): a `RotatingFileSink` period too large for a `float` is a
  `ValueError` naming `interval`, raised before the file is opened.
- **The comparison-only doors** (FR-003): `SocketTransport.port` and
  `NATSSink.max_reconnect_attempts` are rebound through `exact_int` before their range checks.
  Every refusal that names an `int` renders it through the new `sanitize._int_text`, which gives
  `<int: ~N digits>` past the digit limit.
- **The public waits** (FR-004): `flush()` and `shutdown()` pass `timeout` through the new
  `_retry.wait_budget` at entry. `inf`, or anything above `threading.TIMEOUT_MAX`, waits unbounded,
  NaN is a `ValueError`, a non-number is a `TypeError`, and a value below `-TIMEOUT_MAX` is `0.0`.
  A refused `shutdown()` retires nothing.
- **The guard** (FR-005): `tests/test_numeric_door_roster.py` derives the `int` doors from the AST
  of every `__init__` under `sinks/` and in `worker.py`, follows one call level into a same-module
  helper, and fails naming an unjudged site and its remedy. It carries a fixture corpus, population
  floors and a derived mutation sweep over every helper call.
- **The record** (FR-006): the three §12 items are Resolved, and two new Open items cover NATS's
  unchecked float timeouts and a `float` timeout too large for a socket. The decision, invariants 1
  and 13, the README's `flush()` paragraph and the v1.1.0 notes are updated to match.

Deviation, one line: `_int_text` counts a negative's sign toward the limit, which the spec's Data
Model did not. It now replaces the same integers as `_Coercer._bounded_int` and `_otel._int_digits`.

## What changed from earlier specs?

- The decision `a-bad-argument-is-floored-if-it-works-and-refused-if-it-is-already-broken-a-new-one-is-refused`
  is refined, not reversed. Its fence no longer says the `max(x, 0)` floors are not yet held to the
  rule, and its body now records the reach of a type refusal and the owner's verdict on the two
  waits. Both are dated in its History.
- SPEC-013 and SPEC-025's "`flush()`/`shutdown()` never raise" now has one recorded exception, a
  bad `timeout` refused at entry (invariant 1). SPEC-065's Out of Scope line saying the two calls
  validate nothing carries a superseded marker.
- SPEC-049's `require_positive` and `require_timeout`, and SPEC-065's `config._require_positive`,
  keep their verdicts. Only their messages changed, now rendering an `int` through `_int_text`.
- Caller-visible refusals are listed in `docs/release-notes/v1.1.0.md` under *Fixed*, and the
  notes' lead names them.

## Verification

`tests/test_numeric_doors.py` (the construction matrices and the public waits),
`tests/test_sink_retry.py` (the four helpers) and `tests/test_numeric_door_roster.py` (the guard,
its corpus and sweep). The fresh-context diff reviews ran before the push, and the gate results
are recorded in the PR.
