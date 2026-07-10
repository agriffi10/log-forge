# Completed Spec — SPEC-004: Background Flush Worker and Graceful Shutdown

## What was completed?

Flushing moved off the hot path. A finished span is now handed to a background worker instead
of blocking the decorated call on `sink.emit`.

- **`worker`** (new) — `Worker(sink, *, batch_size=10, flush_interval=1.0, max_queue=10_000,
  max_retries=3)`. Owns a bounded `queue.Queue` and a daemon thread:
  - **`submit(events)`** — `put_nowait` handoff; non-blocking (FR-001).
  - **Batching** — the drain loop emits when `batch_size` event-lists accumulate **or**
    `flush_interval` elapses, flattening queued per-span lists into one `sink.emit` (FR-002).
  - **Retry** — a failing `emit` is retried with capped backoff; past `max_retries` the batch
    is abandoned with a counted (`failed_batches`) stderr warning and draining continues, so a
    broken sink never kills the thread (FR-003).
  - **Backpressure** — a full queue drops the newest submission and counts it (`dropped`),
    never blocking the app (FR-004).
  - **`shutdown()`** — signals stop, drains the queue, emits the tail, then `sink.close()`;
    idempotent, woken promptly via a sentinel (FR-005).
- **`decorator`** — `_flush(span)` now calls `_get_worker().submit(span.events)`; the worker is
  created lazily, one per process, from `_ensure_sink()`, and registers the drain via `atexit`
  on first creation (FR-006).
- **Façade** — `log_forge.shutdown()` drains + closes; also `atexit`-registered.

The span lifecycle from SPEC-001/003 is untouched — only *where a finished span goes* changed.

## What changed from earlier specs?

`decorator._flush` swapped from a direct `sink.emit` to `worker.submit`. Because delivery is now
async, the shared test fixtures were updated: `conftest.lf` keeps flushing synchronous for the
pipeline tests (the "synchronous-flush test mode" the fixture always anticipated) and resets the
process worker between tests; the SPEC-001 `test_decorator_sync.py` tests now call
`log_forge.shutdown()` to drain before asserting (real end-to-end worker coverage). No public
API from earlier specs changed.

Concurrency hardening (from fresh-context review): the idle drain loop now advances its
flush-window even when the queue is empty, so `get()` blocks a full interval instead of
busy-spinning a core on a `0s` timeout; the `dropped` counter increments under a lock;
`shutdown()`'s once-only flag is set under that lock; and `atexit` registers the drain exactly
once per process rather than per worker creation.

## Verification

Local gates green — ruff clean, `mypy --strict` clean (12 src files), `pytest` **62 passed / 0
skipped**, and `test_worker.py` (14 tests) stable across repeated runs. `test_worker.py` covers
non-blocking submit, count/time batching, order/no-loss, retry survival + abandon-count,
drop-newest backpressure with an observable counter, an idle-not-busy-spin regression, drain-
and-close, idempotent shutdown, and the lazy single per-process worker. Fresh-context code
review run before merge; the one blocking finding (idle busy-spin) was fixed and re-verified.
