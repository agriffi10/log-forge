# Completed Spec — SPEC-075: Wrappers Map Verdicts; MultiSink Keeps Calling Each Child's emit

## What was completed?

- `FilteringSink` and `TransformSink` define `deliver`, offered exactly when the inner sink offers
  it (SPEC-072's probe, taken once at construction). When offered, `log_foundry_counters` is the
  inner sink's own `SinkCounters` object, so a run through the wrapper counts where the bare sink's
  run would. Each round applies the 1.x per-event step, forwards the rest through
  `RunContext.delegate` and maps the inner verdict back to the wrapper's own indices. A declined
  event is `SKIPPED` and counted nowhere, except in a round the engine fails as raising (the inner
  `deliver` raised, or its marks did not fit): there SPEC-072's raising-round upper bound counts
  every event of the round, `SKIPPED` ones included, in the shared `failed_events`. A failed event
  is `SKIPPED` and counted in the wrapper's `_dropped` through the run context, `SETTLED`. A call
  whose every event failed is `REJECTED` with the refusal marker. A malformed inner verdict turns
  the forwarded events `UNKNOWN`. Inside an engine run it also sets aside what the inner call
  registered, as the engine does for a bare sink since PR 319. A direct `deliver()` keeps those
  registrations, as the bare call does.
- Caller code runs once per event per engine run (Andrew's calls). `TransformSink` keeps each
  event's forwarded result in the run context's notes, so later rounds forward the same object. A
  copy per round broke a FIFO `SQSSink`'s order and its attempt-count line. `FilteringSink` keeps
  each forwarded event's decision the same way, so a resend is not put to the predicate or the
  level floor again. Asked again, a stateful predicate (dedupe, rate limit, sampling) could
  decline its own resend and lose the event silently. Caller code runs outside any engine run, as
  on the `emit` path; a `deliver()` it calls is its own detached run (`_engine.outside_runs`, from
  the frame-3 review).
- `LogstashSink` is a pure delegate in both modes. In HTTP mode it offers when the `HTTPSink`
  does. In socket mode it offers on the `SocketTransport`'s counters and calls the transport's
  `deliver` directly, as `SyslogSink` does. Its verdict is the backend's, unmapped.
- All three copy the inner sink's `max_retries` and `max_retry_after` at construction, as the
  engine reads them, and expose them as new public read-only names (Andrew's call). A copy that is
  absent or that the engine reads as declaring none is stored as `None`. Their stop signal now
  sits where the engine's static reader finds it (`_wrapper.ForwardedStopSignal`) and is still
  forwarded. They declare no idempotence.
- New: `sinks/_wrapper.py`, `_args.ReadOnlySeconds`, a reason-carrying `ReadOnlyCount`,
  `_engine.declared_count` and `RunContext.delegate`. `_fork._assign` writes a set-only
  descriptor's value into the instance `__dict__` without running its `__set__`.
- `MultiSink` offers no `deliver` and calls each child's `emit`, unchanged. Its `emit` docstring
  names an engine child under the double-count cost it already accepted.
- The base `Sink.deliver` docstring states the detached-call contract (SPEC-074 handover item 1a,
  Andrew's call: documentation only).

## What changed from earlier specs?

- On the worker path a wrapped engine sink now runs one engine loop with its own budget. Before,
  the worker's whole-batch loop ran around the inner sink's inline engine.
- When the caller's code fails on every event, the batch now costs one attempt with no worker
  retry, and no sink counter moves; `emit` still re-raises by identity. Socket-mode `LogstashSink`
  counts each lost event once per batch, where SPEC-074 counted it once per worker attempt.
- A caller-code failure on the `deliver` path writes an `absorbed a failure` line per call. It
  replaces the `lost` line the `emit` path writes.
- A third-party subclass of the three classes that assigns `max_retries` or `max_retry_after` now
  raises `AttributeError` (Andrew's call).
- SPEC-074's engine-sink roster (13) and its restated loop roster (11) name the three classes as
  exclusions.
- Decisions: one new one, *A wrapper offers `deliver` exactly when what it wraps does, and counts
  only its own decisions*. Three existing ones gained a dated 2026-11-09 entry: *Caller code that
  fails for one event*, *A raising `emit` counts nothing* and *A sink round resends only what
  failed*. `docs/invariants.md` 2, 6 and 7, `architecture.md` §8, the README, the inventory and
  the 1.2.0 release notes were also updated.

## Verification

- The claims above are pinned by `tests/test_wrapper_verdicts.py` (FR-001 to FR-003, the
  predicate and transform caches, the set-aside and the twins), `tests/test_multi_engine_children.py`
  (FR-004) and `tests/test_engine_sinks_roster.py` (the exclusions). The PR's CI run is the record
  of the full suite.

## Open and accepted edges

- Closed by the predicate cache: caller code never runs in a later round. A later round resends
  only events the inner sink answered `RETRYABLE`, and only a forwarded event gets an inner
  outcome. Both wrappers forward such an event again from the run's notes without running the
  predicate or the function. So the all-failed refusal, and any caller-code failure, can occur
  only in the round that first sees an event. The frame-2 review's case (a predicate raising on a
  resend, charged to the inner `failed_events` as "refused … after others landed") no longer
  arises (`test_a_predicate_cannot_fail_a_forwarded_event_on_its_resend`).
- Accepted: on the worker path, the engine's own lines through a wrapper name the wrapper (or
  `LogstashSink`), not the inner sink or transport. Lines the inner sink registers itself still
  name the inner sink.
- Accepted: a third-party inner sink whose budget is a property or a `bool` reads as declaring no
  budget, through the wrapper as it does bare.
- Accepted (Andrew's call, documentation only): a direct `deliver()` outside an engine run is one
  detached round with no memory. A caller looping it sees a held or giving-up line on every call,
  and an `SQSSink` FIFO hold is never released. `emit_via_deliver` is the retrying path.
- Accepted: under `MultiSink`, a failed engine child's events are counted in its own `failed` and
  again in the fan-out's absorbed count. This is invariant 2's recorded upper bound.
