# Spec: Context Lifetime — Scoping Baggage and Adopted Trace Context

**ID:** SPEC-024  
**Status:** Draft  
**Last Updated:** 2026-08-05  
**Depends On:** SPEC-014, SPEC-015

## Overview

Two pieces of per-request state — baggage and the adopted inbound trace context — are written into
`contextvars` and never taken back out. `set_baggage` calls `.set()` with no token; `continue_trace`
records an adopted `(trace_id, parent_span_id)` that nothing in `src/` ever clears. Both therefore
outlive the request that set them, and in a process that handles requests sequentially on one thread
— the main thread, a pooled worker thread, a warm Lambda container — they bleed into the next one.

The consequences are the two worst kinds this library can produce, because both put *wrong data* in
the log stream rather than merely losing it. A `user_id` set as baggage while serving Alice appears
on every event of the unrelated request that follows. A handler that adopted a `traceparent` on one
invocation keeps joining that caller's trace on every later invocation that supplied no header at
all, parented to a span in a process that has long since exited — so two unrelated requests are
indistinguishable downstream from one distributed call.

Both are also documented as not happening. `context.py` calls it "the current trace's baggage";
architecture §5 files it under "Baggage — trace-scoped dynamic context" and promises events are
stamped "at or below the point they were set". Neither statement is true today: the actual scope is
the whole `contextvars` context, forever. This spec gives both values the lifetime they are already
documented to have, and adds the explicit reset that a caller not using `@trace` needs.

## Scope

### In Scope

- Restoring baggage to its prior value when the **root** span that ran under it closes.
- The same lifetime for the adopted inbound trace context.
- A public function to clear both explicitly, for callers who never open a span.
- Correcting the docstrings and `architecture.md` §5 to describe the delivered behaviour.
- Tests for sequential reuse on one thread — the case the current suite does not cover, and the
  reason the leak survived to a public release.

### Out of Scope

- **Changing what baggage *is*.** It stays a flat `dict[str, object]` merged into events with the
  precedence `build_event` already implements (arch §5.1). Only its lifetime changes.
- **Auto-clearing at *every* span boundary.** Baggage set in a nested call must stay visible to its
  parent and to the siblings that follow it inside the same trace — that is the "at or below" rule
  and it is the whole point of the feature. The reset belongs at the root, where the trace ends.
- **Isolating concurrent tasks or threads.** `contextvars` already does this correctly, and SPEC-003
  covers it. The defect is strictly sequential reuse of one context; nothing here changes the
  concurrent behaviour, and the existing sibling-isolation tests must keep passing unchanged.
- **Clearing baggage set outside any span.** A `set_baggage` call made before any `@trace` call is a
  deliberate process-level default and is restored to, not erased. (`configure(defaults=...)` remains
  the better tool for that, and the docs should keep saying so.)
- **A `traceparent` on the outbound side.** `current_traceparent()` and friends are unaffected.
- **Any new config key or constructor argument.**

---

## Functional Requirements

### FR-001: Baggage is restored when the root span closes

#### Description:

`@trace` already brackets the span stack with a `contextvars` token (`push_span`/`pop_span`,
`context.py:64-71`). Baggage gets the same treatment, but only at the **root** — the span opened
when no other span was active. On entry the current baggage token is captured; on exit it is reset,
restoring whatever was in effect before the trace began.

Nested spans do not reset. Baggage set three calls deep must remain visible to the events of its
parent and of every later sibling in the same trace, which is what "trace-scoped" means and what
SPEC-015's boundary backfill relies on.

The reset happens in the `finally` alongside `pop_span`, so it covers the success path, the error
path, and the async wrapper identically — the same three paths `_close_span` already unifies.

#### Acceptance Criteria:

- [ ] Baggage set inside a root-span call is absent from the events of a later, unrelated root-span
      call on the same thread.
- [ ] Baggage set inside a *nested* call is present on the parent span's `span.end` event and on the
      events of a sibling call made after it within the same trace.
- [ ] Baggage set *before* any span is opened is visible inside spans and still present after they
      close — the reset restores it rather than clearing it.
- [ ] The restore happens when the root span raises, exactly as when it returns.
- [ ] The same holds for an `async` root span, and concurrent `asyncio` tasks remain isolated from
      each other (the existing SPEC-003 sibling tests pass unchanged).
- [ ] SPEC-015's backfill still sees the span's final baggage: `span.start` and `span.end` carry
      baggage set during the body, because the backfill runs in `_close_span`, before the reset.

### FR-002: The adopted trace context has the same lifetime

#### Description:

`context._adopted` is released at the same point as baggage — the root span's exit — but it is
**cleared**, not restored. A handler that adopted a context on one invocation must not still be
joining that trace on the next invocation that adopted nothing.

The asymmetry with FR-001 is deliberate, and restoring would defeat the requirement. The documented
call site is the first line of the decorated entry point, but adopting *before* the span opens is
equally legitimate (a framework middleware that dispatches, and the caller-side example in this
spec's API contract). A token restore puts back whatever was current at root-span **entry**, so an
adoption made before the span survives it — leaving invocation 2 joined to invocation 1's trace,
exactly the defect this spec exists to close. Baggage set outside a span is a process-level default
and is restored to (Out of Scope); an adopted context is a one-shot handoff to the trace it names,
consumed by it. A caller who opens no span clears it with FR-003's `reset_context()`.

Within a single trace the adopted context keeps its current meaning exactly: it is consulted only
when no span is open (`decorator.py:72-73`), so a nested call still inherits from its in-process
parent. SPEC-014's re-parenting of an already-open root span is likewise untouched.

#### Acceptance Criteria:

- [ ] After a root span that ran under an adopted context closes, a subsequent root span opened with
      no new `continue_trace` call starts a **fresh** trace with `parent_span_id=None`.
- [ ] Two sequential invocations that each call `continue_trace` with a *different* `traceparent`
      land in their own respective traces.
- [ ] Adopting a context still re-parents the currently-open root span and its buffered events
      (SPEC-014 FR-001 behaviour is unchanged, including the "not a root → leave alone" rule).
- [ ] `continue_trace` called with nothing valid remains a silent no-op and does not clear a context
      adopted earlier in the *same* trace.
- [ ] The existing `tests/test_trace_continuation.py` fixture no longer needs to reset the private
      `context._adopted` by hand; the reset is removed and the suite still passes.

### FR-003: A public reset for callers who open no span

#### Description:

A caller using the emitters without `@trace` — the orphan path — has no root span to hang the reset
on, and today has no way to clear either value at all. `log_foundry.reset_context()` clears baggage
and the adopted context together, restoring the process to the state it had before either was set.

One function rather than two: the two values have the same lifetime and the same failure mode, and a
caller who wants one almost always wants the other. It is exported from `log_foundry` and documented
beside `continue_trace` and `set_baggage`.

#### Acceptance Criteria:

- [ ] `log_foundry.reset_context()` clears baggage: a subsequent emitter call carries none of the
      previously-set keys.
- [ ] It clears the adopted context: the next root span starts a fresh trace.
- [ ] It is safe to call when nothing was ever set, when no span is open, and inside an open span.
- [ ] It never raises, for the same reason every other entry point on this path does not
      (architecture §4).
- [ ] It appears in `log_foundry.__all__` and in the README, with one line stating that `@trace`
      users do not need it.

### FR-004: The documentation describes the delivered scope

#### Description:

The claims that made this defect invisible are corrected in place, and the newly-true ones stated
precisely — including the boundary FR-001 draws between a root and a nested span, which is the part a
reader is most likely to get wrong.

#### Acceptance Criteria:

- [ ] `context.py`'s module docstring states the lifetime of each of the three context variables.
- [ ] `set_baggage`'s and `get_baggage`'s docstrings say when baggage is discarded.
- [ ] `architecture.md` §5 "Baggage — trace-scoped dynamic context" states the root-span boundary
      explicitly, rather than only "at or below the point they were set".
- [ ] `continue_trace`'s docstring states that the adopted context does not survive the trace.
- [ ] The README documents `reset_context()` and notes that a long-lived process reusing one thread
      is the case it exists for.

---

## Data Model

```python
# src/log_foundry/context.py — new token accessors, mirroring push_span/pop_span

def push_baggage_scope() -> contextvars.Token[dict[str, object]]:
    """Capture the current baggage token, for restoration at root-span exit."""


def pop_baggage_scope(token: contextvars.Token[dict[str, object]]) -> None:
    """Restore baggage to its pre-scope value; clear the adopted context. Never raises."""


def reset_context() -> None:
    """Clear baggage and any adopted context outright. Public, re-exported from `log_foundry`."""
```

Only baggage needs a token: FR-002's adopted context is cleared rather than restored, so there is
nothing to capture for it.

`pop_baggage_scope` must tolerate a token created in a different context — `contextvars.Token`
raises `ValueError` when reset from a context other than the one that made it, which is reachable if
a caller pushes work onto another thread mid-span. It catches and falls back to setting the value
directly, because a decorated function must not fail on the way out (architecture §4).

---

## API / Interface Contract

```python
# The decorator, in outline — one capture on entry, one restore in the finally:

def wrapper(*args, **kwargs):
    is_root = context.current_span() is None
    span = _open_span(name or fn.__qualname__, defaults)
    token = context.push_span(span)
    scope = context.push_baggage_scope() if is_root else None
    try:
        ...
    finally:
        context.pop_span(token)
        if scope is not None:
            context.pop_baggage_scope(scope)   # restores baggage, clears the adopted context


# Caller side — the case this spec fixes:
log_foundry.continue_trace(event.get("traceparent"))   # invocation 1
handle()                                                # joins the caller's trace
# ... container frozen and thawed; invocation 2 supplies no header ...
handle()                                                # now a fresh trace, not invocation 1's
```

`is_root` is computed **before** `_open_span`, since that call is what makes the span current.

## Configuration / Environment

None. No new config keys, environment variables, or constructor arguments.

## File & Folder Structure

```
src/log_foundry/
├── context.py         # modified — scope tokens, reset_context, docstrings
├── decorator.py       # modified — bracket the root span; no change to _open_span's rules
└── __init__.py        # modified — export reset_context

tests/
├── test_context.py            # modified — scope + reset unit tests
├── test_decorator.py          # modified — sequential reuse on one thread
└── test_trace_continuation.py # modified — drop the manual _adopted reset; add reuse cases

docs/architecture.md   # modified — §5 baggage scope
README.md              # modified — reset_context, sequential-reuse note
```

## Implementation Phases

### Phase 1: Scope the two context variables

- Add `push_baggage_scope` / `pop_baggage_scope` to `context.py`, with the cross-context fallback.
- Bracket the root span in both decorator wrappers.
- Tests: sequential root spans on one thread for baggage and for the adopted context; nested and
  sibling visibility preserved; error path; async; concurrent-task isolation unchanged; SPEC-015
  backfill still sees final baggage.

### Phase 2: The explicit reset

- Add `context.reset_context()`, re-export from `log_foundry`, add to `__all__`.
- Tests: clears both; safe in every position; never raises.
- Remove the manual `_adopted` reset from the `test_trace_continuation.py` fixture.

### Phase 3: Documentation

- `context.py` module + function docstrings; `continue_trace`'s docstring.
- `architecture.md` §5; README section for `reset_context` and the sequential-reuse case.
