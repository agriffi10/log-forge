# Spec: AWS Lambda Compatibility — Python 3.12 Support and a Repeatable `flush()`

**ID:** SPEC-013
**Status:** Draft
**Last Updated:** 2026-07-22
**Depends On:** SPEC-004 (background worker + `shutdown`), SPEC-012 (release pipeline)

## Overview

`log-foundry` cannot currently be used from an AWS Lambda function. Two unrelated-looking
properties block it, and they are one spec because they have one cause: **the library assumes a
process that starts, runs, and exits**, and Lambda gives it a process that is created once, frozen
and thawed repeatedly, and killed without warning.

**Nothing can install it.** `requires-python = ">=3.13"` excludes `python3.12`, which is the
newest runtime a large class of deployed Lambda fleets is on. `pip install` refuses outright.
Nothing in the library actually needs 3.13 — there is no PEP 695 syntax, no `TypeIs`, no
`copy.replace`, no 3.13 stdlib call anywhere in `src/`.

**And nothing can flush it safely.** `shutdown()` is terminal by design: `Worker.shutdown()` sets
`_shutdown_done`, joins the thread, and closes the sink, and `decorator._shutdown_worker()` never
resets the module-level `_worker`. In a normal process that is exactly right — it runs once, at
exit. In Lambda it is a trap. The handler must flush before returning, because Lambda **freezes
the execution environment** the instant the handler returns: the worker's `flush_interval` loop
stops mid-interval, and whatever is still queued is lost when the container is eventually reaped.
But a handler that calls `shutdown()` to force that flush silently logs **nothing for the rest of
that warm container's life** — the first invocation appears and no subsequent one does. The
symptom is "works locally, broken in production," and the cause is three call-frames deep in a
library the user is not reading.

`atexit` does not save it either: Lambda kills a frozen environment without running exit handlers.
There is no point at which the library's existing drain path is guaranteed to run.

The missing primitive is a **drain that does not retire the worker**. This spec adds it, and
lowers the floor to 3.12 so the library can be installed where it is needed.

## Scope

### In Scope

- `requires-python = ">=3.12"`, with CI proving it on 3.12 **and** 3.13.
- A public `lf.flush(timeout=...)` that drains the queue through the sink without closing the sink,
  retiring the worker thread, or consuming the once-only shutdown flag.
- Preserving `shutdown()`'s current terminal semantics exactly, including its `atexit` registration.
- Documenting the Lambda usage pattern in the README, since a correct API used at the wrong point
  in the lifecycle is still a silent data-loss bug.

### Out of Scope

- **Changing `shutdown()`.** Making it re-entrant (resetting `_worker` so the next `@trace` builds
  a fresh one) was the considered alternative. Rejected: it would pay thread creation on every
  Lambda invocation, and it overloads one name with two lifecycles — "drain" and "drain and stop"
  are different operations and should have different names.
- **A synchronous / worker-less mode.** Emitting inline on span close would remove the flush
  problem entirely, but it puts an SQS round-trip inside every decorated call — the exact
  back-pressure SPEC-004's worker exists to prevent.
- **`continue_trace()` / adopting an inbound trace context — now SPEC-014.** A separate, genuinely distributed
  concern; a consumer wanting one trace across several Lambdas correlates on a baggage field until
  it lands. Recorded as the follow-up in FR-006, not built here.
- **A Lambda extension, layer, or any AWS packaging artifact.** Consumers package the library
  themselves. This spec ships a library change and nothing else.
- **Lowering the floor below 3.12.** 3.11 and earlier are not requested and are not tested.
- Any change to sinks, event schema, `trace`, or the emitters.

---

## Functional Requirements

### FR-001: The supported floor is Python 3.12, and CI proves it

#### Description

The claim "3.12 works" is worth exactly as much as the test run behind it. A `requires-python`
edit alone changes what pip *permits*, not what is known to work — and the failure it would hide
(a 3.13-only call on a rarely-exercised path) surfaces as an `AttributeError` in a consumer's
production Lambda, not here.

#### Acceptance Criteria:

- [ ] `pyproject.toml` declares `requires-python = ">=3.12"`.
- [ ] `[tool.mypy] python_version` is lowered to `"3.12"`. A comment records why: mypy must check
      against the **lowest** supported version, or it will happily accept a 3.13-only API on a
      floor that does not have it.
- [ ] `[tool.ruff]` sets **no** explicit `target-version`, so ruff continues to infer it from
      `requires-python` and cannot drift from the declared floor. If a `target-version` is ever
      added, it must match.
- [ ] `ci.yml` runs the full gate (ruff + mypy + pytest) as a **matrix over 3.12 and 3.13**, not
      3.13 alone. Both must be green for the workflow to pass.
- [ ] `release.yml` continues to build on a single version — a pure-Python wheel is
      version-independent, and building on the floor while testing both is the correct split. A
      comment records that.
- [ ] The CLAUDE.md tech-stack row and README installation section are updated from `>= 3.13` to
      `>= 3.12`.
- [ ] The suite is confirmed green on a real 3.12 interpreter **before** the floor is lowered, not
      after. If any incompatibility is found, it is fixed in this spec or the floor stays at 3.13 —
      the declaration must never be aspirational.

### FR-002: `flush()` drains without retiring the worker

#### Description

The caller's guarantee must be precise, because a vaguer one is useless: **every event submitted
before `flush()` was called has been passed to `sink.emit` by the time it returns.** Events
submitted concurrently by another thread may or may not be included, and that is fine — the caller
cannot have meant those.

The mechanism follows from the queue being FIFO. Enqueue a marker; everything submitted earlier is
necessarily ahead of it, so by the time the worker reaches the marker, those events are either
already emitted or sitting in `pending`. The worker emits `pending` and signals. No lock, no
inspection of queue internals, and no coordination with the batching triggers.

#### Acceptance Criteria:

- [ ] `Worker.flush(timeout: float | None = 5.0) -> bool` enqueues a flush marker carrying a
      `threading.Event`, waits for it, and returns `True` when signalled or `False` on timeout.
- [ ] On dequeuing the marker the worker emits `pending` **immediately** — ignoring the
      `batch_size` and `flush_interval` triggers — then sets the event, then continues its loop.
      The `_stop` event is **not** set, the thread is **not** joined, and `sink.close()` is **not**
      called. A test asserts the sink is still usable and the worker still running after a flush.
- [ ] **The marker is excluded from `pending` in `_run`'s append.** The existing guard is
      `if item is not None and item is not _SHUTDOWN` — a marker falling through it would be
      appended as if it were a list of events and handed to `sink.emit`, crashing the worker
      thread. A test asserts a flush does not corrupt the emitted batch: the batch contains
      exactly the submitted events and no marker.
- [ ] The same exclusion is applied in `_final_drain`, which re-drains the queue on stop and has
      its own copy of that guard. A test covers `shutdown()` racing a pending marker.
- [ ] Ordering holds: events submitted before `flush()` are emitted before it returns. A test
      submits N spans, flushes, and asserts all N reached the sink with no wait on
      `flush_interval` — i.e. the test must fail if `flush()` merely sleeps.
- [ ] Enqueuing the marker uses a **blocking put with the same timeout**, not `put_nowait`. A
      comment records why: `put_nowait` on a full queue would skip the flush and return as if it
      had succeeded, which is the one outcome a flush must never produce silently. If the put
      times out, `flush()` returns `False`.
- [ ] `flush()` is safe to call repeatedly and concurrently — each call gets its own marker and
      event. A test asserts two flushes on one process both return `True` and both deliver.
- [ ] A test asserts `flush()` **does not** consume `_shutdown_done`, by flushing twice and then
      calling `shutdown()` and confirming it still drains and closes.

### FR-003: `flush()` cannot hang, and cannot resurrect a dead worker

#### Description

The consumer this exists for runs with a hard execution timeout, often on a path where logging is
the least important thing happening. A flush that blocks forever converts "some logs were lost"
into "the invocation timed out," which is strictly worse than the bug it was added to fix.

There are two ways to block forever, and both must be closed: the worker thread is gone (post-
`shutdown()`, nothing will ever consume the marker), or the sink is wedged (`emit` blocking
indefinitely inside the worker thread).

#### Acceptance Criteria:

- [ ] `lf.flush()` **returns immediately with `True`** when no worker has ever been created. It
      must **not** call `_get_worker()`: building a worker and registering `atexit` in order to
      drain nothing is pure cost, and in a process that never logged it starts a thread that
      otherwise would not exist.
- [ ] `flush()` returns `False` — promptly, without waiting out the timeout — when the worker has
      already been shut down. A test calls `shutdown()` then `flush()` and asserts it returns
      quickly and does not block for `timeout` seconds.
- [ ] The timeout is honoured on a wedged sink: a test with a sink whose `emit` blocks asserts
      `flush(timeout=0.1)` returns `False` in well under a second and **does not raise**.
- [ ] `flush()` **never raises**, for any sink failure or internal state. Failures are reported by
      the return value and, where the worker is involved, the existing `failed_batches` counter and
      stderr warning path. A comment records the rule: the library must never be the reason a
      caller's function fails, and a flush is the call most likely to be made in a `finally`.
- [ ] `timeout=None` means wait indefinitely, and the docstring states plainly that this is unsafe
      in any environment with an execution deadline.

### FR-004: `shutdown()` is unchanged

#### Acceptance Criteria:

- [ ] `shutdown()` keeps its exact current semantics: idempotent, sets `_stop`, joins the thread,
      drains, calls `sink.close()`, and stays registered via `atexit` on first worker creation.
- [ ] No caller is required to change. A consumer on the previous version who upgrades and calls
      only `shutdown()` observes no behavioural difference. The existing SPEC-004 tests pass
      unmodified — if one needs editing, the change has gone further than intended.
- [ ] The `shutdown()` docstring gains one sentence pointing at `flush()` for callers that need to
      drain and keep logging, naming the warm-container failure mode explicitly rather than saying
      "see also."

### FR-005: The public surface and the pattern that makes it correct

#### Description

`flush()` is easy to export and easy to use at the wrong moment. Documenting *where* it goes is
part of shipping it: a flush written as the last line of a handler body is precisely the line that
does not run when the handler raises — and the invocation whose logs are most worth having is the
one that failed.

#### Acceptance Criteria:

- [ ] `flush` is exported from `log_foundry.__init__` and added to `__all__`, alongside `shutdown`.
- [ ] It is fully typed and passes `mypy --strict`; the signature is
      `def flush(timeout: float | None = 5.0) -> bool`.
- [ ] The README gains a short **"Serverless / short-lived processes"** subsection under
      *Shutdown and Flushing*, stating: call `flush()` before the handler returns, call it in a
      `finally`, and never call `shutdown()` per-invocation — with the reason (the worker does not
      come back, so only the first invocation on a warm container would log).
- [ ] The README example shows the `finally` placement, not a trailing call.
- [ ] `docs/component-inventory.md` gains a row for `flush` so it is discoverable as a reusable
      primitive.
- [ ] The docstring on `flush()` states the guarantee in FR-002's words — *events submitted before
      this call have been passed to the sink when it returns* — rather than "flushes the queue,"
      which does not tell a caller what they can rely on.

### FR-006: What this does not solve, recorded where it will be read

#### Description

A consumer instrumenting several Lambdas in one pipeline will get one trace per invocation and
reasonably conclude the tracing is broken. It is not — a per-process trace context is doing
precisely what it can across separate processes. Saying so costs a paragraph and saves the
investigation.

#### Acceptance Criteria:

- [ ] `architecture.md`'s **Known Constraints / Non-goals** section records that trace context does
      not cross a process boundary: N invocations produce N `trace_id`s, and `parent_span_id` is
      never set across them.
- [ ] The same entry records the shape of the fix, so it does not have to be rederived: a
      `continue_trace(trace_id, parent_span_id)` entry point plus the caller threading those two
      values through whatever payload already crosses the boundary.
- [ ] The Known Constraints entry names **SPEC-014 — Cross-Process Trace Continuation** as the
      follow-up that closes it, rather than leaving a code TODO. That spec is written and indexed,
      so this criterion is satisfied by pointing at it, not by inventing a placeholder row.
- [ ] The entry also records that `atexit` does not run when a serverless environment is reaped,
      so `flush()` is the only guaranteed drain there — this is why FR-002 exists and is the first
      thing someone debugging missing tail events needs to know.

---

## API / Interface Contract

```python
def flush(timeout: float | None = 5.0) -> bool:
    """Drain buffered events through the sink without closing it.

    Every event submitted before this call has been passed to ``sink.emit`` when it returns
    ``True``. Unlike ``shutdown()`` the worker stays alive and the sink stays open, so logging
    continues normally afterwards.

    Returns ``False`` if the drain did not complete within ``timeout`` (or the worker has
    already been shut down). Never raises.
    """
```

```python
# AWS Lambda — the whole pattern.
import log_foundry as lf
from log_foundry.sinks.sqs import SQSSink

lf.configure(service="billing-api", env="prod", sink=SQSSink(queue_url=QUEUE_URL))

@lf.trace
def handler(event, context):
    lf.info("received", records=len(event["Records"]))
    try:
        return do_work(event)
    finally:
        lf.flush()      # in `finally`: the failed invocation is the one worth logging.
                        # NEVER shutdown() here — the worker does not come back, and every
                        # later invocation on this warm container would log nothing.
```

## File & Folder Structure

```
src/log_foundry/worker.py        # Worker.flush + the flush marker; marker exclusion in
                                 # _run and _final_drain
src/log_foundry/decorator.py     # module-level flush entry point (no-op when no worker)
src/log_foundry/__init__.py      # export `flush`
pyproject.toml                   # requires-python >=3.12; mypy python_version 3.12
.github/workflows/ci.yml         # 3.12 + 3.13 matrix
README.md                        # installation floor + the serverless subsection
CLAUDE.md                        # tech-stack row
docs/architecture.md             # Known Constraints: cross-process traces, atexit
docs/component-inventory.md      # `flush` row
tests/test_worker.py             # FR-002/FR-003 coverage
```

## Implementation Phases

### Phase 1: Prove 3.12, then lower the floor

- Run the full gate (ruff, mypy, pytest) on a real 3.12 interpreter **first**. Fix anything it
  finds, or stop — the declaration follows the evidence, not the other way round.
- `requires-python`, mypy `python_version`, the CI matrix, README and CLAUDE.md.

### Phase 2: `flush()`

- The flush marker + `Worker.flush`, including the `pending` exclusion in both `_run` and
  `_final_drain`.
- The module-level entry point: no-op with no worker, prompt `False` after shutdown, never raises.
- Export and type it; tests for every FR-002 and FR-003 criterion — in particular the wedged-sink
  timeout and the post-`shutdown()` non-hang, which are the two that turn a logging bug into an
  invocation timeout.

### Phase 3: Document and release

- README serverless subsection, `shutdown()` docstring cross-reference, architecture.md Known
  Constraints entry, component-inventory row, the `continue_trace` follow-up row.
- `sh scripts/spec-lint.sh`, then the completion ritual.
- Tag `v0.3.0` — a **minor** bump: `flush` is additive and the floor is widened, so nothing that
  worked on 0.2.0 breaks.
