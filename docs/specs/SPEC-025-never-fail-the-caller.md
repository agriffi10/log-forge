# Spec: The Library Must Not Fail the Caller

**ID:** SPEC-025  
**Status:** Draft  
**Last Updated:** 2026-08-05  
**Depends On:** SPEC-004, SPEC-017

## Overview

Architecture §4 makes one promise above the rest: logging never breaks the application. The
decorator records a failure and re-raises the caller's exception unchanged; a broken destination
degrades logging and nothing more. SPEC-017 was written to close the places that promise leaked, and
closed the ones it found.

Three remain, and each is worse than the ones already fixed, because in each the exception the caller
receives is one the library invented.

`_close_span` sits *inside* the decorator's `try`. When it raises — a sink that fails to construct,
a `Worker()` that cannot start a thread on an exhausted process — a decorated function that already
computed and returned its result raises instead. The `except BaseException` handler then re-enters
`_close_span`, so the span emits both a `status=ok` and a `status=error` end event for the same call:
the log stream now contradicts itself about a call that succeeded.

The orphan path in `api.py` calls `_ensure_sink().emit([event])` with no guard, so a bare
`log_foundry.info(...)` outside any span raises whatever the sink raises straight into the caller.
SPEC-017's delivery doc records "the orphan-path crash is gone" — true of the *sanitize* crash it
fixed, not of the sink crash on the very next line.

`Worker.shutdown()` marks itself done and then calls `self.sink.close()` unguarded, so a sink whose
close fails raises out of `shutdown()` — and out of the `atexit` handler — while the once-only flag
means the retry is a no-op and the sink is never closed at all.

This spec closes all three. It is the same promise, the same failure shape, and the same remedy as
SPEC-017; these are the instances that audit did not reach.

## Scope

### In Scope

- Guarding `_close_span` so a decorated function's own outcome is never replaced by a logging fault.
- Removing the duplicate end event that the current double-call produces.
- Guarding the orphan-path emit in `api.py`.
- Making `shutdown()` total, and ordering it so a failed close is not silently permanent.
- One stderr line per new absorbed failure, matching the existing convention.
- Tests for all three paths, none of which is covered today.

### Out of Scope

- **Making the orphan path asynchronous.** That it emits synchronously on the caller's thread is a
  settled decision recorded in `architecture.md` §12 Resolved, and is the stated reason `sanitize`
  must be total. Only the missing guard is a defect here. Routing orphans through the worker is a
  separate question and is not reopened by this spec.
- **Reporting these absorbed failures through `health()`.** They happen on the caller's thread,
  outside the worker's accounting, and inventing a second counter channel here would collide with
  SPEC-026, which owns loss reporting. A stderr line is the signal for now.
- **Retrying a failed `_close_span` or orphan emit.** The event is lost; the point is that the
  *caller* survives. Retry policy belongs to the worker and to SPEC-027.
- ~~**`_open_span`.** It runs before the body and its failure modes (id generation, dataclass
  construction) are not reachable in practice. Left alone deliberately rather than wrapped for
  symmetry.~~ **Brought into scope during the build** (FR-001), by decision: "not reachable in
  practice" is the same argument that left the three defects above unguarded, and a fault *before*
  the body is strictly worse than one after it — the library would stop the application doing its
  work at all, rather than merely losing the log. The pre-body setup (`_open_span`, `push_span`,
  `push_baggage_scope`) is now guarded as one unit, and SPEC-024's deferred note about
  `push_baggage_scope` sitting outside the `try` is closed by the same change.
- **`flush()`.** Already guarded and already documented "Never raises" (`decorator.py:230-235`).

---

## Functional Requirements

### FR-001: A successful call stays successful

#### Description:

`_close_span` is made total: any exception it raises is absorbed, recorded on stderr, and not
propagated. A decorated function that returned normally returns its value; a decorated function that
raised re-raises *its own* exception, unchanged and with its `__context__` untouched.

The guard catches `Exception`, not `BaseException`. A `KeyboardInterrupt` or `SystemExit` arriving
while closing a span is the operator's or the runtime's intent and must still reach the caller —
this is the same line SPEC-019 drew in the opposite direction for the worker thread, where the
absence of any handler was the defect.

**The pre-body setup is guarded on the same terms** (see Out of Scope, amended during the build).
`_open_span`, `push_span` and `push_baggage_scope` run *before* the caller's function, so a fault
there does not merely lose the log — it stops the application doing its work. The three are opened
as one unit; any fault gives back "no span" and the call proceeds **untraced**, with its own result
or exception reaching the caller unchanged. Partial setup is kept rather than unwound: a span that
was created but never pushed still closes and flushes, which preserves more than discarding it.
`pop_span` becomes total in the same way `pop_baggage_scope` already is (SPEC-024), since both run
in the `finally` where a raise would replace the caller's own exception.

#### Acceptance Criteria:

- [ ] With a sink whose construction raises, a decorated function that returns `42` returns `42`,
      and nothing propagates to its caller.
- [ ] With the same sink, a decorated function that raises `ValueError` propagates that `ValueError`
      — not the sink's exception, and not with the sink's exception as `__cause__`.
- [ ] The same holds for the `async` wrapper, including for a cancelled coroutine
      (`asyncio.CancelledError` still propagates unchanged).
- [ ] A `KeyboardInterrupt` raised inside `_close_span` still reaches the caller.
- [ ] One stderr line, prefixed `log-foundry:`, names the exception **type** that was absorbed and
      states that the span's events were lost. The message is not written (architecture §6, and the
      rule SPEC-019 FR-001 applies to `stopped_reason`).
- [ ] A normal close writes nothing to stderr and is unmeasurably affected.
- [ ] With `_open_span` raising, a decorated function still runs and still returns `42`; one stderr
      line names the type and says the call ran untraced.
- [ ] With `_open_span` raising, a decorated function that raises still propagates its own
      exception.
- [ ] After a failed close the span stack is left clean — `current_span()` is `None` again — so a
      later call in the same context is not parented to a span that never closed.
- [ ] An untraced **nested** call leaves its parent current, so the parent's trace is not split.
- [ ] A root call whose span could not be opened still releases its baggage scope, so baggage set
      inside it does not reach the next call (SPEC-024's guarantee survives the failure path).
- [ ] A raising traced call retains no reference cycle: repeated calls do not accumulate objects
      that only `gc.collect()` can free.

### FR-002: One span produces one end event

#### Description:

The double `span.end` is a consequence of the unguarded call, but it must not be able to return by
another route. The wrappers are restructured so `_close_span` is invoked exactly once per span, on
every path.

The clean shape is to close in a `finally` with the outcome determined by whether an exception was
seen, rather than closing once in the `try` and again in the `except`. That also removes the current
ordering subtlety where a success close runs *before* `return result` and an error close runs before
the re-raise.

#### Acceptance Criteria:

- [ ] A span that succeeds emits exactly one `span.end`, with `status="ok"` and no `error` field.
- [ ] A span that raises emits exactly one `span.end`, with `status="error"` and the `error`
      sub-document.
- [ ] No span emits two `span.end` events under any failure of the close path.
- [ ] `duration_ms` still measures to the end of the function body, not to the end of the flush.
- [ ] The events of a successful call are still flushed before the wrapper returns, so a caller that
      calls `flush()` immediately afterwards observes them.
- [ ] The `finally`-based restructure preserves `pop_span` ordering: the span is popped after it is
      closed, so SPEC-015's backfill still reads the baggage that was live inside the span.

### FR-003: The orphan path cannot raise into the caller

#### Description:

`api._log`'s direct `emit` is guarded. A level call made with no active span records what it can and
returns; it never propagates a sink failure to the application that called `log_foundry.info(...)`.

The echo path is guarded on the same terms and for the same reason — `ConsoleWriter.write` touches a
stream that may be closed, and an echo is a diagnostic convenience that must not outrank the caller.

#### Acceptance Criteria:

- [ ] With a sink whose `emit` raises, a bare `log_foundry.info("x")` outside any span returns
      normally.
- [ ] The same holds for `debug`, `warning`, `error` and `critical`.
- [ ] The same call *inside* a span is unaffected — it still appends to the span's buffer and does
      not touch the sink (no behaviour change on the in-span path).
- [ ] `echo=True` with a broken console stream does not raise, and the event still reaches the sink.
- [ ] One stderr line names the absorbed exception's type; if stderr itself is broken, the call still
      returns normally.
- [ ] A `KeyboardInterrupt` from the sink still propagates, as in FR-001.

### FR-004: `shutdown()` is total, and a failed close is not silently permanent

#### Description:

`Worker.shutdown()` currently sets `_shutdown_done = True` before joining and closing
(`worker.py:248-258`), so a failing `sink.close()` both escapes to the caller and leaves the sink
permanently unclosed behind an idempotent no-op.

Two changes. The close is guarded, so `shutdown()` — and the `atexit` handler registered at
`decorator.py:207` — cannot raise. And the failure is recorded, so a caller can tell a clean
shutdown from one that drained but could not close.

The once-only flag stays where it is. Re-running a drain is not safe, and a second `shutdown()`
retrying a close that already failed would call `close()` twice on a sink that may have partially
released its resources. Idempotence is preserved; what changes is that the failure is visible rather
than swallowed by the flag.

#### Acceptance Criteria:

- [ ] `log_foundry.shutdown()` with a sink whose `close()` raises returns normally.
- [ ] The queued events are still drained and emitted before the close is attempted.
- [ ] A second `shutdown()` remains a no-op and also does not raise.
- [ ] The failure is written to stderr as one line naming the exception type.
- [ ] The `atexit`-driven shutdown of a process using such a sink exits with the process's own
      status; the failure does not become an unhandled exception at interpreter shutdown.
- [ ] `health()` stays readable afterwards and its existing fields keep their meanings.

---

## Data Model

No new types. One new internal helper, used by FR-001, FR-003 and FR-004 so the three sites report
identically:

```python
# src/log_foundry/_diag.py  (new module — see SPEC-029, which takes ownership of it)

def absorbed(where: str, exc: BaseException, detail: str = "") -> None:
    """Announce a failure the library absorbed rather than propagated. Type name only; never raises."""
```

If SPEC-029 has not landed when this spec is built, the helper ships here in its final shape and
SPEC-029 adopts it; the two must not each invent one.

---

## API / Interface Contract

```python
# The sync wrapper, in outline — one close, one flush, on every path. `_begin` and `_end` hold
# the guards so the two near-duplicate wrappers cannot drift apart:

def wrapper(*args, **kwargs):
    span, token, scope = _begin(name or fn.__qualname__, defaults)   # never raises
    status, error = "ok", None
    try:
        result = fn(*args, **kwargs)
    except BaseException as exc:
        status, error = "error", exc
        raise                       # unchanged, per architecture §4
    finally:
        try:
            _end(span, token, scope, status, error)                  # never raises
        finally:
            del status, error       # break the frame↔exception cycle — see below
    return result


def _end(span, token, scope, status, error):
    if span is not None:
        try:
            _close_span(span, status, error)
        except Exception as exc:    # never BaseException — see FR-001
            _diag.absorbed("closing a span", exc, "the span's events were lost")
    if token is not None:
        context.pop_span(token)          # total (SPEC-025)
    if scope is not None:
        context.pop_baggage_scope(scope)  # total (SPEC-024)
```

`status`/`error` are set in the `except` and read in the `finally`, so the close runs exactly once
and knows the outcome. The bare `raise` keeps the caller's exception identity and traceback.

`_begin` keeps whatever succeeded and `_end` releases exactly that much, which is what lets a
degraded call proceed instead of failing. The **order of its three steps** is what keeps every
partial state coherent: the baggage scope is taken *first*, so a root call that loses its span
still has a scope to release, and the state "span kept, scope lost" — a traced call that leaks its
baggage into the next request, i.e. the SPEC-024 defect reappearing through a failure path — is
unreachable. A span that exists but was never pushed is still closed and flushed; it is simply not
current, so nothing nests under it.

`_end`'s `is not None` checks are load-bearing rather than defensive: `pop_span(None)` would fall
through that function's own guard to `set(())` and wipe the whole stack, detaching the parent of an
untraced *nested* call.

One ordering constraint inside `_end`, and only one: the baggage scope is released **after** the
close, so SPEC-015's backfill still reads the baggage that was live inside the span. The stack pop
is order-independent — `_close_span` reads the span it is handed and the current baggage, never the
stack.

The outcome must be **deleted after `_end`**. `except ... as exc` auto-deletes its target precisely
because a local holding an exception whose traceback holds this frame is a reference cycle; keeping
it in `error` past the handler rebuilds that cycle and pins the caller's own frames and locals until
a generational collection. Measured at ~13 objects per raising call.

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── decorator.py       # modified — guarded `_begin`/`_end`, single-call close in both wrappers
├── context.py         # modified — `pop_span` made total, as `pop_baggage_scope` already is
├── api.py             # modified — guarded orphan emit and echo
├── worker.py          # modified — guarded close in shutdown()
└── _diag.py           # new — the shared absorbed-failure reporter

tests/
├── test_decorator_sync.py   # modified — close-failure and single-end-event cases
├── test_decorator_async.py  # modified — the same for the async wrapper
├── test_api.py              # modified — orphan and echo failure cases
└── test_worker.py           # modified — shutdown with a failing close
```

## Implementation Phases

### Phase 1: The decorator

- Restructure both wrappers to the single-close `finally` shape; guard the close.
- Add `_diag.absorbed`.
- Guard the pre-body setup in `_begin`; make `pop_span` total.
- Tests: success survives a broken close; the caller's own exception is preserved; exactly one
  `span.end` on both paths; `KeyboardInterrupt` still propagates; `duration_ms` and flush ordering
  unchanged; async and cancellation; a call whose span cannot be opened still runs.

### Phase 2: The orphan path

- Guard the direct emit and the echo in `api._log`.
- Tests: all five level functions with a raising sink; a broken console stream; in-span behaviour
  unchanged; a broken stderr on top of a broken sink.

### Phase 3: `shutdown()`

- Guard `sink.close()`; record and announce the failure.
- Tests: shutdown returns normally, drains first, stays idempotent, and does not raise from
  `atexit`.
