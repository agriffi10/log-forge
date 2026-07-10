# Completed Spec — SPEC-003: Async `@trace` Support

## What was completed?

`@trace` now works on `async def` functions with identical span semantics to the sync path —
an execution mode, not new vocabulary.

- **`decorator`** — `trace()`'s `decorate` branches on `asyncio.iscoroutinefunction(fn)` at
  decoration time: async functions get a coroutine-function wrapper that `await`s the body;
  sync functions keep the SPEC-001 wrapper. The two are deliberate near-duplicates (the only
  difference is `await fn(...)`) — no shared clever helper across the sync/async boundary.
- **Lifecycle** — span opens before the body is awaited, closes after it completes/raises;
  `status` `ok`/`error`, non-swallowing re-raise, `pop_span` in `finally`. `BaseException`
  covers `asyncio.CancelledError`, so a cancelled coroutine records an error end event rather
  than leaking an unclosed span.
- **Context** — no new machinery: `contextvars` already propagates the span stack + baggage
  across `await` points and concurrent tasks. Children awaited under one parent share its
  `trace_id` and set `parent_span_id` to the parent's `span_id` with distinct `span_id`s;
  sibling baggage does not leak.

No changes to `_open_span`/`_close_span`/`_flush`, the `Span` model, or the event schema.

## What changed from earlier specs?

Nothing in production code beyond the added branch. The `test_decorator_async.py` `skipif`
guard added in SPEC-002 lifted automatically once `iscoroutinefunction(trace(async_fn))`
became `True`; the file grew from 2 placeholder tests to 8 covering FR-001..003.

## Verification

Local gates green — ruff clean, `mypy --strict` clean (11 src files), `pytest` **47 passed / 0
skipped** (the async suite now runs). New/expanded tests in `test_decorator_async.py`: dispatch
by coroutine-function, parameterized async `@trace`, await-chain nesting, concurrent
`asyncio.gather` (shared trace / distinct spans / parent linkage), sibling baggage isolation,
and cancellation recording an error end event. Fresh-context code review run before merge.
