# Completed Spec — SPEC-026: Sink Loss Visibility

## What was completed?

Four specs built a loss-reporting apparatus — `failed_batches` (SPEC-017), `dropped_unadjudicated`
(SPEC-018), `stopped_reason` (SPEC-019), `flush()`'s delivery verdict (SPEC-021) — and against every
shipped remote transport, none of it fired. Each sink caught its own failures, counted them on a
private attribute with no accessor, wrote a stderr line, and returned normally. A sink that reports
success is a sink the worker believes: the retry never engages, `failed_batches` stays at zero, and
`flush()` returns `True`. With a dead syslog socket the measured reading was
`flush() == True` and `health() == (0, 0, 0, None)` while every message was lost.

- **The contract, stated once** (FR-001, FR-004). `sinks/base.py` gains `SinkLosses(dropped, failed)`,
  `SinkDeliveryError`, `read_losses(sink)`, and the two rules on `Sink.emit`'s docstring where a sink
  author will read them: *raise when you delivered none of the batch* (the worker's retry cannot
  duplicate what never landed), *never raise when you delivered some* (it would re-deliver what did).
- **Every shipped sink honours it** (FR-001). `SocketTransport` (so `SyslogSink` and socket-mode
  `LogstashSink` inherit it), `HTTPSink._abandon` (so all seven platform subclasses inherit it
  without a line of their own), `SentrySink`, `MultiSink`, and the twelve queue/stream/database
  sinks. `health().failed_batches` becomes non-zero and `flush()` returns `False` against a
  permanently-failing sink — the two assertions that failed before this spec.
- **Absorbed loss is readable** (FR-002, FR-003). An optional `losses()` on each sink, aggregated
  into `Health.sink` — appended exactly as SPEC-019 appended `stopped_reason`, so every earlier
  field keeps its index. Nested rather than flattened: `dropped` on the worker is backpressure at
  the queue, `dropped` on the sink is an event that never reached the wire, and one number would
  make the remedies indistinguishable. `read_losses` is total, because `health()` is
  documented "Never raises" and is the call an operator makes when things are already wrong.
- **Documented where each audience reads** (FR-004): `architecture.md` §8 (the sink obligations)
  and §9 (what "retries with backoff" is conditional on), the `health()` docstring, and a new
  README "Writing your own sink" section with a worked example. `SinkLosses` and
  `SinkDeliveryError` are re-exported from the package root, since `health().sink` returns one.

**Three cases where nothing landed and the sink still must not raise**, each settled by an earlier
spec and each newly load-bearing here:

| Case | Why no raise | Reported as |
|---|---|---|
| An **unadjudicable** batch response (Kinesis, Firehose, Elasticsearch `_bulk`) | The sink cannot prove nothing landed, so the worker's retry risks duplicating (SPEC-018) | `losses().failed` |
| An **SQS sender fault** | Provably rejected; a byte-identical re-send can only fail the same way (SPEC-016 FR-006) | `losses().failed` |
| An **oversized** event | It can never fit, so there is nothing to retry | `losses().dropped` |

The two suppressions differ in scope, deliberately. SQS's is **conditional**: if any chunk was lost
for a retryable reason the raise stands, because those events are recoverable and nothing landed for
a retry to duplicate. Kinesis/Firehose's is **batch-wide**: once any chunk is unadjudicable the emit
can no longer prove nothing was delivered.

**Deliberate deviations from the spec as written.** (1) `MultiSink`, `FilteringSink` and
`TransformSink` return `SinkLosses | None` rather than always a value — FR-003 separates "reports
nothing" from "reports no loss", and a wrapper that flattened them would claim a clean bill of
health for a sink that never gave one. (2) `NullSink` deliberately exposes no `losses()`: discarding
is what it is *for*, and reporting it would fire the alert idiom on every batch. (3) `SentrySink`
sends one envelope per event, so it isolates per-event failures and raises only when nothing landed.
(4) `MultiSink.failed` counts child *calls*, not events, and is excluded from the aggregate.

## What changed from earlier specs?

- **`Health` gained a fifth field.** Unpacking it whole was already broken by SPEC-019's fourth;
  positional and attribute access to every earlier field are unchanged.
- **`HTTPSink._send` returns `bytes`, not `bytes | None`.** Every caller spelled `None` as "nothing
  to parse" and the worker read it as a successful emit. `_abandon` is now `NoReturn`.
- **Sinks raise where they used to return.** Eighteen existing per-sink tests that asserted
  "abandoned and returned" now assert the raise. Anything catching only its driver's exceptions
  around an `emit` should add `SinkDeliveryError`.
- **`max_retries` is floored at zero in all twelve sink retry loops**, as `Worker._emit` already
  floors its own (SPEC-021). A negative value made `range(max_retries + 1)` empty, so the sink
  attempted nothing at all — and then reported whatever its loop fell through to: a bare `return`
  in most of them, indistinguishable from success. No counter moved and nothing reached stderr,
  which is the shape this whole arc exists to remove. Reachable only by misconfiguration, but
  reachable.
- **`ElasticsearchSink` gained `dropped_unadjudicated`**, and adjudicates its `_bulk` response
  through `_batch.usable_results` — the SPEC-018 helper, now used outside the two sinks it was
  written for.
- **Unblocks SPEC-030**, which appends to the same `Health`.

## Verification

Local: 875 tests pass (64 new), `ruff` and `mypy --strict` clean over 49 source files, `spec-lint`
clean. CI green on 3.12 and 3.13 across PRs
[#108](https://github.com/agriffi10/log-forge/pull/108) and
[#109](https://github.com/agriffi10/log-forge/pull/109).

Every raise and every `losses()` accessor was mutation-tested individually — removed in turn with
the suite re-run — and each is killed by at least one test. Six fresh-context reviews found nine
real defects, five of them the *same shape as the bug being fixed*, which is the finding worth
recording: a change that makes a previously-advisory value load-bearing introduces failure modes
wherever that value was approximate.

- `SentrySink` guarded only its HTTP branch, so an SDK `capture_event` raising on event 3 of 10
  propagated mid-batch and the worker re-sent the two Sentry had already accepted. Fixed, then
  found again: the HTTP branch caught only `SinkDeliveryError`, and `HTTPSink._send` catches
  `(URLError, OSError)`, which does **not** cover `http.client.HTTPException` — an `IncompleteRead`
  off `response.read()` came straight through. One guard over both branches, catching `Exception`.
- The `_bulk` response was adjudicated positionally with no length check, then with a length check
  but no shape check. Both directions were reachable: a longer `items` array made a partial success
  raise; a shorter or unreadable one reported a total failure as success.
- `AzureEventHubsSink` lost its empty-batch guard when `_send` gained a return value. The Azure SDK
  short-circuits an empty `EventDataBatch` and returns — a phantom success that counted as delivery
  and suppressed the raise for everything else in the emit. It survived the suite because the
  existing fake did not model the short-circuit.
- SQS applied FR-006's suppression batch-wide, so a sender-fault chunk silenced the raise for a
  different chunk lost to a throttle. With `MAX_BATCH` 10 and the worker's default `batch_size` 10,
  multi-chunk emits are the normal case, not an edge.

Two testing traps are worth recording: mutating a *copied* checkout does nothing, because Poetry's editable install resolves
`log_foundry` back to the original tree by absolute path and every mutant "passes"; and `git
checkout -- <file>` restores the old mtime, so stale `__pycache__` produces a phantom failure on the
clean run afterwards. The revert step also discards unstaged work — `git add -A` before mutating in
place.
