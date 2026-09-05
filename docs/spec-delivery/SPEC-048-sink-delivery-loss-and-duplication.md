# Completed Spec — SPEC-048: Sink Delivery — Loss and Duplication

## What was completed?

- **A `3xx` is a counted delivery failure, never a route to follow** (FR-001). `sinks/http.py`
  gains `_NoRedirect` and `_no_redirect_opener()`, called per construction and the default
  for every sink in the family; one
  site set the opener and one calls it, so the change reaches six subclasses, `LogstashSink`'s
  HTTP backend and `SentrySink`'s fallback untouched. An injected `opener=` is used as given.
- **A client exception costs its chunk, never the batch** (FR-002). The guard sits *inside* each
  `_send`'s attempt loop in `sqs`/`sns`/`kinesis`/`firehose`, not around `_send` in `emit`, because
  `_send` may already hold a non-zero `accepted` and a narrowed entry list. `SQSSink`'s
  `recoverable_loss` term is fed by the guard, without which a wholly-failed batch returns
  normally with everything lost. `KinesisSink`/`FirehoseSink`'s `unknown` term is deliberately
  untouched: SPEC-018's "unadjudicable" describes a *response*, and an exception is not one.
- **Kinesis charges the partition key in UTF-8 bytes** (FR-003), in the per-record ceiling and in
  `_record_size`. New `_partition_key` / `MAX_PARTITION_KEY_BYTES`; both encodes carry
  `errors="replace"`, since ~~`sanitize` passes a lone surrogate through~~ (corrected by SPEC-055
  FR-001, which replaces it at assembly; the guard stays for a batch rewritten after assembly)
  and a bare encode would raise out of `emit`. `sanitize.truncate_str` is rejected in writing — its marker changes the
  shard.
- **`GooglePubSubSink.close()` is bounded** (FR-004). New `_resolve_within` / `_past` /
  `_drain_pending`; `_out_of_time` is gone. All three close-race tails share the bound, including
  `_await_overflow`'s, which runs on an application thread on the orphan path.
- **`SentrySink.close()` pushes the SDK transport** (FR-005), absorbed, and deliberately not
  suppressed on a repeat close — this sink has no post-close guard, so events can be captured
  between two closes.
- **A rotation failure costs neither a duplicate nor the sink** (FR-006). `emit` flushes *before*
  attempting a rotation; new `_rotate_or_continue` reopens, re-seeds `_size`, re-arms
  `_next_rollover` and announces once. No counter and no `losses()` — nothing is dropped.
- **`gzip=True` no longer overwrites a caller's `Content-Encoding`** (FR-007), and when the
  caller's wins the body is not compressed.

**Deviation from the spec as written:** its FR-001 claimed `urlopen` follows all five 3xx
statuses. It follows three — CPython refuses `307`/`308` on a POST already — so those two
parameters are labelled regression pins, not evidence. The spec was corrected in place before the
build.

## What changed from earlier specs?

- **SPEC-036's `_resolve` contract**: `timeout=None` now describes what an *unboundable* future
  gets, not what `close` does. Its docstring is corrected in place.
- **`test_a_shutdown_defers_the_overflow_wait_to_close_rather_than_blocking_the_drain`** (SPEC-038)
  had its assertion replaced: `waits == [None]` pinned unboundedness as the *mechanism*, and this
  spec changes the mechanism while keeping the rule. It now compares a close with the stop signal
  set against one without, which fails for any close that shortens itself during a shutdown —
  strictly stronger, and it is what stops the obvious implementation from reversing SPEC-038.
- No public signature changed, and no construction-time refusal was added; both belong to SPEC-049.

## Verification

Five gates green by exit code: `ruff` 0, `mypy` 0, `pytest` 0 (1958 passed, 8 skipped — 43 new),
`spec-lint` 0, `docs-lint` 0. Collected test names diffed against the parent commit: **0 removed**.
Twenty-three mutants planted and all twenty-three killed, one per guard whose failure would be
silent, each asserting a reason rather than an exit code. FR-001 is exercised against two real `http.server`
origins because every existing HTTP test injects a fake opener and would not touch the fix; the
FR-002 roster was demonstrated by adding a hypothetical unguarded fifth AWS sink and watching it
be named. The second diff review built eight programs driving these sinks through the worker and
found no loss or duplication end to end; two residuals it measured are recorded in
`architecture.md` §12 rather than fixed, with a **Closed by** clause each.

Nothing was deferred to CI.
