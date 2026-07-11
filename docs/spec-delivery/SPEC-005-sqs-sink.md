# Completed Spec — SPEC-005: SQSSink and Optional `sqs` Extra

## What was completed?

The headline production sink: events ship to an SQS queue that buffers them durably in front of
the (out-of-scope) ELK indexer. `boto3` stays optional — stdout-only users remain
dependency-free.

- **`sinks.sqs`** (new) — `SQSSink(queue_url, client=None, *, max_retries=3)` implementing the
  SPEC-001 `Sink` protocol (`emit`/`close`), `isinstance`-checkable.
  - **Lazy boto3** — `import boto3` happens inside the constructor only when no client is
    injected, never at module top, so importing the module (and the library) needs no `boto3`
    (FR-001).
  - **Chunking** — `_chunks` splits each batch into sends of ≤ `MAX_BATCH` (10) messages **and**
    ≤ `MAX_BYTES` (256 KB), each body a single `json.dumps`, full coverage with no loss/dup
    (FR-002).
  - **Partial failure** — `_send` inspects the response `Failed` list and retries only those
    entries (bounded); successfully-sent entries are never re-sent; entries still failing past
    the bound are counted (`failed`) and logged (FR-003).
  - **Oversized** — an event whose serialized size alone exceeds `MAX_BYTES` is dropped with a
    counted (`dropped_oversized`) warning; the rest of the batch still sends (FR-004).
  - **`close`** — no-op; the sink buffers nothing (FR-005).

No changes to the worker, the batching contract, or any earlier module. The extra
(`boto3>=1.34`) was already declared in `pyproject.toml` — originally named `sqs`, **renamed to
`aws`** in SPEC-010 when `SNSSink`/`KinesisSink`/`FirehoseSink` joined `SQSSink` on boto3.

## What changed from earlier specs?

Nothing — SPEC-005 is purely additive (one new module + its tests). `SQSSink` is a drop-in
`configure(sink=SQSSink(...))` with no change elsewhere in the pipeline.

## Verification

Local gates green — ruff clean, `mypy --strict` clean (13 src files; the lazy `boto3` import
carries a scoped `type: ignore[import-not-found]` since the extra isn't installed in CI),
`pytest` **73 passed / 0 skipped**. `test_sinks_sqs.py` injects a fake SQS client (no boto3 /
AWS / network) and covers protocol conformance, boto3-created vs injected client, count/byte
chunking, full-coverage no-loss/dup, `json.dumps` bodies, retry-only-failed, persistent-failure
counting without discarding successes, and oversized drop-with-rest-sent. Fresh-context code
review run before merge.
