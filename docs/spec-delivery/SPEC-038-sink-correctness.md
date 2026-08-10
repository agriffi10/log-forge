# Completed Spec — SPEC-038: Sink Correctness

Ten FRs across three PRs (#143, #144, #145). Four more moved to SPEC-041 before the build.

## What was completed?

- **FR-001** — `HTTPSink.emit` owns a chunk loop; subclasses extend through `_render` / `_body` /
  `_handle_response` and a lint forbids overriding `emit`. `Worker._final_drain` hands this
  family the whole exit backlog (5,980 events measured) and only `NewRelicSink` inherited the
  base `emit`, so the other five sent it as one request the destination rejected whole.
- **FR-002** — `PostgresSink._rollback` guards the cleanup call. Unguarded, a rollback that
  raised took attempts from 4 to 1, left `failed=0` after a totally lost batch, wrote no
  diagnostic, and handed the worker a raw driver exception.
- **FR-004** — `GooglePubSubSink` reaps settled futures per emit and bounds the pending list.
  It was append-only for the process lifetime, and `result()` ran only at `close()`, so `failed`
  stayed 0 and `health()` read clean through an entire outage.
- **FR-005 / FR-009** — Firehose delimits its records (`{"a":1}{"b":2}` was unparseable by
  Athena, Glue and OpenSearch ingest) and uses the documented 1,024,000-byte per-record ceiling
  rather than `1024*1024`; Kinesis charges the `PartitionKey` its request budget actually pays.
- **FR-006** — `KafkaSink.close()` bounds its flush and counts the remainder `flush()` returns.
- **FR-007** — an oversized UDP datagram is dropped before the send, with `EMSGSIZE` the only
  errno treated as permanent.
- **FR-008** — `maxlen` on both Redis sinks, defaulting to `None`.
- **FR-012** — `RotatingFileSink(backup_count=1)`; the old default destroyed a generation at
  every rollover.
- **FR-013** — `MemorySink`, `NullSink` and `StderrSink` leave `sinks.util`, which is deleted
  with no alias.

Suite 1301 → 1406.

**Deviations, all amended in the spec in place:** FR-001 AC-5 (Loki's and Splunk's limits are
this library's choice, not vendor figures — neither publishes one); FR-006 AC-3 (**inverted** —
see below); FR-009 AC-3 (per-record boundaries delivered, per-request pairs not).

## What changed that a later spec should know?

- **`HTTPSink` is a template method now.** A new platform sink overrides `_render`/`_body`, never
  `emit`, and `tests/test_public_surface.py` enforces it — resolving `ast.Attribute` bases too,
  since `class X(http.HTTPSink)` was invisible to the first version of that lint.
- **Both sink rosters scope on *defines or inherits*.** Moving five `emit`s into a base dropped
  those classes out of the concurrency and post-close lints — 34 → 29, silently, because that
  file had no floor guard while its sibling did. Both now have floors, a class overriding neither
  `emit` nor `close` may answer from the ancestor whose code it runs, and a new test asserts the
  two rosters cover the same classes. This composed correctly across PRs: FR-013's moved
  `StderrSink` entered scope automatically under FR-001's widened rule.
- **`sinks.util` is gone with no alias.** Import `MemorySink` from `log_foundry.sinks.memory`.
- **`tests/test_error_path_rules.py` is a new package-wide lint** — no cleanup call reached
  through `self.<attr>.<method>()` may be unguarded inside an `except`. Its handler roster is
  derived and complete; its cleanup *predicate* is a stated vocabulary, and four evasions are
  named in its docstring rather than implied away.

## Anything deliberately left open?

- **`dropped_oversized` re-counts across a worker retry** when a batch holds an oversized frame
  *and* its sendable remainder then fails totally. `dropped` is an exact count everywhere else,
  so the departure is recorded on FR-007 AC-1 and at the drop site. The worker owns the retry and
  re-hands the original events, so no sink can fix it; `FirehoseSink._records` and
  `KinesisSink._records` have the same pre-existing shape. **A fix belongs one level up.**
- **`GooglePubSubSink`'s pending list grows against a client that resolves nothing.** "Never
  invent loss", "never block delivery indefinitely" and "bound memory" cannot all hold once the
  destination stops answering; the first two are kept and the third is documented.
- **The 413 refusal memory is a single low-water mark**, so strictly decreasing event sizes cost
  O(N) requests, and under `gzip` the shortcut is off entirely.

## Evidence

Twelve fresh-context review rounds across the three PRs. **Seven blocking defects; five were
introduced by a previous round's fix**, which is the number worth carrying forward.

Four were one mistake repeated — bounding or skipping the wrong thing on a shutdown path:

| where | measured cost |
|---|---|
| `HTTPSink` ended the 413 search while stopping | a 2,000-event exit backlog delivering in 30 requests delivered **nothing** |
| `HTTPSink` capped the 413 search by recursion depth | **2 events of 2,000** against a 20 kB endpoint under the 5 MB default |
| `GooglePubSubSink` bounded the overflow wait per *future* | `excess × 30 s` per emit, a shutdown ignored for 5.04 s |
| `KafkaSink` cut its flush timeout to zero while stopping | **11 buffered, 0 delivered**, all booked as `failed` |

The root cause is one fact: **`Worker.shutdown` sets the stop event *before* joining the drain
thread**, so it is set for the whole of `_final_drain`. A shutdown may shorten a *wait*; it must
never skip *work*. FR-001 AC-4a records it, and FR-006 then repeated it in a sibling FR.

Two further classes worth naming:

- **A wall-clock bound cannot see a busy-spin.** The Pub/Sub slice loop was correctly bounded at
  1.00 s wall while burning 3,496,237 `result()` calls and a pegged core. The test asserts on
  CPU time.
- **Two tests were written vacuous, then "fixed" into a second vacuous form.** The Firehose
  per-request test passed with the delimiter removed at 1,012-byte records (the *count* limit
  binds first) and again at 9,000 (a chunk size is a floor division: `4194304 // 9012 ==
  4194304 // 9011`). It now sits at 8,989 and **asserts that sensitivity as a precondition**.

Every blocking defect was green in CI, mutation-tested by its author, and found only by a
reviewer *measuring* rather than reading.
