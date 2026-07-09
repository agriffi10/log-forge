# Spec: Async `@trace` Support

**ID:** SPEC-003
**Status:** Draft
**Last Updated:** 2026-07-09
**Depends On:** SPEC-001, SPEC-002

## Overview

The concurrency decision is threads **and** asyncio (architecture §5). SPEC-001's `@trace`
covers synchronous functions; this spec makes the same decorator work on `async def`
functions, so the span opens when the coroutine starts and closes when it actually finishes
(awaits complete), not when the coroutine object is merely created. `contextvars` already
propagates the span stack and baggage correctly across tasks, so the only new machinery is an
async-aware wrapper selected at decoration time. Span identity, hierarchy, non-swallowing
re-raise, and the event schema are unchanged from SPEC-001 — this spec adds an execution mode,
not new semantics.

## Scope

### In Scope

- Detecting coroutine functions at decoration time (`asyncio.iscoroutinefunction`) and
  returning an `async` wrapper that `await`s the wrapped function.
- Identical span lifecycle to the sync path: open on enter, `end_event` with `status`
  `ok`/`error`, re-raise unchanged, pop in `finally`.
- Correct context behavior across `await` boundaries and concurrent tasks (each task sees its
  own current span / baggage; concurrent sibling tasks under one parent share the parent's
  `trace_id` and set `parent_span_id` to the parent's `span_id`).
- Parameterized use (`@trace(name=..., defaults=...)`) on async functions.

### Out of Scope

- Any change to the synchronous wrapper or the shared `_open_span` / `_close_span` / `_flush`
  helpers beyond dispatching to the async wrapper.
- The background worker — SPEC-004. Async spans flush via the same path as sync spans in the
  current phase.
- Structured concurrency helpers, task-group instrumentation, or auto-tracing of un-decorated
  coroutines — not in scope.

---

## Functional Requirements

### FR-001: Async-aware decoration

#### Description:

`@trace` applied to an `async def` returns a coroutine function that traces the call, rather
than tracing creation of the coroutine object.

#### Acceptance Criteria:

- [ ] `asyncio.iscoroutinefunction(traced_async_fn)` is `True` after decoration (the wrapper is
      itself a coroutine function).
- [ ] Awaiting the decorated coroutine runs the original body and returns its result unchanged.
- [ ] `@trace` and `@trace(name=..., defaults=...)` both work on async functions.
- [ ] Applying `@trace` to a synchronous function still returns the SPEC-001 sync wrapper
      (dispatch is by `iscoroutinefunction` at decoration time).

### FR-002: Async span lifecycle

#### Description:

The span opens before the body is awaited and closes after it completes or raises.

#### Acceptance Criteria:

- [ ] The span is opened and pushed before `await fn(...)`, and closed (end event appended,
      flushed) after the await completes.
- [ ] On success the end event has `status="ok"`; on exception `status="error"` with
      `error.type`/`error.stack`, and the exception propagates out of the awaited call
      unchanged.
- [ ] `BaseException` (including `asyncio.CancelledError`) is recorded as an error end event and
      re-raised; the span is never left unclosed.
- [ ] The span is popped from the context stack in a `finally` block regardless of outcome.

### FR-003: Context correctness under asyncio

#### Description:

Nested and concurrent async spans maintain correct trace/hierarchy via `contextvars`.

#### Acceptance Criteria:

- [ ] An async function decorated with `@trace` that `await`s another decorated async function
      produces a child span whose `trace_id` matches the parent and whose `parent_span_id`
      equals the parent's `span_id`.
- [ ] Multiple child coroutines awaited concurrently (e.g. via `asyncio.gather`) under one
      decorated parent each share the parent's `trace_id` and set `parent_span_id` to the
      parent's `span_id`, with distinct `span_id`s.
- [ ] Baggage set in a parent async span is visible to child async spans it awaits; baggage set
      inside one child does not leak into a sibling child.

---

## Data Model

No new types. Reuses SPEC-001 `Span` and the `LogEvent` schema unchanged.

---

## API / Interface Contract

```python
# decorator.py — trace() gains an async branch inside decorate(fn)
#   if asyncio.iscoroutinefunction(fn): return <async wrapper>
#   else:                               return <sync wrapper from SPEC-001>

# Public signature is unchanged:
def trace(func=None, *, name=None, defaults=None): ...

# Example
@log_forge.trace
async def fetch(user_id: int) -> dict:
    log_forge.info("fetching", user_id=user_id)
    return await load(user_id)

@log_forge.trace
async def load(user_id: int) -> dict:
    ...

await fetch(4127)   # one trace_id; load's parent_span_id == fetch's span_id
```

## Configuration / Environment

No new configuration, environment variables, or dependencies. `pytest-asyncio`
(`asyncio_mode=auto`) is already declared in `pyproject.toml` for testing.

## File & Folder Structure

```
src/log_forge/
└── decorator.py       # add the async wrapper branch to trace()
tests/
└── test_decorator_async.py   # async lifecycle, nesting, concurrent gather, baggage   (new)
```

## Implementation Phases

### Phase 1: Async wrapper

- Add the `iscoroutinefunction` branch to `trace()`'s `decorate`, mirroring the sync wrapper
  with `async`/`await` and reusing `_open_span`/`_close_span`/`_flush` (FR-001, FR-002).
- Keep the sync and async wrappers as deliberate near-duplicates (no shared clever helper — the
  sync/async split is a hard boundary).

### Phase 2: Concurrency tests

- Test async nesting (parent→child trace/hierarchy), concurrent `asyncio.gather` children, and
  baggage isolation between siblings (FR-003).
- Test cancellation records an error end event and re-raises (FR-002).
