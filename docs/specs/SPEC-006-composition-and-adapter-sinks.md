# Spec: Composition and Adapter Sinks

**ID:** SPEC-006
**Status:** Completed
**Last Updated:** 2026-07-10
**Depends On:** SPEC-001

## Overview

Users want flexibility in *where* and *how* their logs are written without log-forge shipping a
bespoke transport for every destination. This spec adds four **zero-dependency** sinks that either
combine other sinks or adapt arbitrary user code: `CallbackSink` (ship anywhere via a plain
function), `MultiSink` (fan a batch out to several sinks at once), `FilteringSink` (drop events by
predicate or minimum level before forwarding), and `TransformSink` (reshape or redact events before
forwarding). Each implements the SPEC-001 `Sink` protocol (arch §8), stays dependency-free, and
nests arbitrarily with the other three and with any existing sink (`StdoutSink`, `SQSSink`, …). Four
small classes cover an enormous surface: with a callback and a fan-out, a user can reach almost any
destination and tee to several at once without writing a `Sink` subclass at all.

## Scope

### In Scope

- `CallbackSink(fn, *, on_close=None)` — `emit` delegates the batch to a user callable; the ultimate
  escape hatch for "send my logs somewhere log-forge doesn't natively support."
- `MultiSink(*sinks)` — fan the same batch out to every child sink, isolating a failing child so its
  siblings still receive the batch; `close()` closes every child.
- `FilteringSink(inner, *, predicate=None, min_level=None)` — forward to `inner` only the events that
  match a predicate and/or meet a minimum level; forward nothing (no `inner.emit`) when none match.
- `TransformSink(inner, fn)` — apply a per-event transform before forwarding; `fn` returning `None`
  drops that event; the incoming batch is never mutated in place.
- `close()` cascade semantics across nested compositions (each underlying sink closed exactly once).
- All four are `isinstance(sink, Sink)`-checkable and add **no** runtime dependency.

### Out of Scope

- The reserved tail-sampling `should_send` seam (arch §10). `FilteringSink` is a *static* predicate
  applied at emit time, not the sampling hook — that remains deferred and owns level/rate policy at
  span decision time.
- Parallel/async fan-out. `MultiSink` emits to children **sequentially** on the worker thread;
  concurrent emit across children is deferred.
- Any concrete network/disk transport — those are the other specs in this arc (SPEC-008..011).
- De-duplication or ordering guarantees across children beyond "same batch, same order, each child."

---

## Functional Requirements

### FR-001: CallbackSink

#### Description:

`CallbackSink` turns a plain callable into a `Sink`, so a user can direct events anywhere without
writing a class.

#### Acceptance Criteria:

- [ ] `CallbackSink(fn)` stores `fn`; `emit(batch)` calls `fn(batch)` exactly once with the batch
      list unchanged.
- [ ] An exception raised by `fn` propagates out of `emit` (so the worker's retry/backoff path
      handles it) — `CallbackSink` does not swallow it.
- [ ] `close()` is a no-op unless an `on_close` callable was supplied, in which case `close()` calls
      it exactly once.
- [ ] `isinstance(CallbackSink(lambda b: None), Sink)` is `True`.

### FR-002: MultiSink fan-out

#### Description:

`MultiSink` forwards each batch to several sinks so a process can, e.g., echo to stdout **and** ship
to SQS from one `configure(sink=...)`.

#### Acceptance Criteria:

- [ ] `MultiSink(a, b, c).emit(batch)` calls `emit(batch)` on `a`, then `b`, then `c`, in
      construction order, each with the same batch.
- [ ] If one child's `emit` raises, the remaining children still receive the batch; the failure is
      counted and logged to stderr, and does **not** propagate out of `MultiSink.emit` (so a single
      broken child never fails the whole fan-out or the worker retry).
- [ ] `close()` calls `close()` on every child even if an earlier child's `close()` raises; a child
      close failure is counted/logged, not propagated.
- [ ] `MultiSink()` with no children is valid: `emit` and `close` are no-ops.
- [ ] `isinstance(MultiSink(...), Sink)` is `True`.

### FR-003: FilteringSink

#### Description:

`FilteringSink` drops events that don't match a predicate or minimum level before handing the rest to
an inner sink.

#### Acceptance Criteria:

- [ ] `FilteringSink(inner, predicate=fn)` forwards to `inner.emit` only the events for which
      `fn(event)` is truthy, preserving their order.
- [ ] `FilteringSink(inner, min_level="WARNING")` forwards only events whose `level` is at or above
      the given level per standard severity ordering (`DEBUG < INFO < WARNING < ERROR < CRITICAL`);
      an event with an unknown/missing `level` is forwarded (fail-open) rather than dropped.
- [ ] Level comparison is **case-insensitive** on both the configured `min_level` and each event's
      `level` (e.g. `min_level="warning"` and an event `level` of `"Warning"` compare correctly) —
      consistent with SPEC-007's `LoggingSink`.
- [ ] An invalid `min_level` (not one of the five names, case-insensitive) raises `ValueError` at
      construction — fail fast rather than silently forwarding every event.
- [ ] When `predicate` and `min_level` are both given, an event must satisfy **both** to be forwarded.
- [ ] When no event passes, `inner.emit` is **not** called (no empty-batch emit).
- [ ] `close()` delegates to `inner.close()`; `isinstance(...)` is `True`.

### FR-004: TransformSink

#### Description:

`TransformSink` reshapes each event (e.g. redact a field, add host metadata, rename keys) before
forwarding to an inner sink.

#### Acceptance Criteria:

- [ ] `TransformSink(inner, fn).emit(batch)` builds a new list by applying `fn` to each event and
      forwards it to `inner.emit`.
- [ ] `fn` returning `None` for an event drops that event from the forwarded batch.
- [ ] The incoming `batch` and its event dicts are not mutated in place — `fn`'s result is what is
      forwarded, and dropping/keeping does not alter the caller's list.
- [ ] When every event is dropped, `inner.emit` is not called.
- [ ] `close()` delegates to `inner.close()`; `isinstance(...)` is `True`.

### FR-005: Arbitrary nesting and close-once cascade

#### Description:

The four sinks compose with each other and with existing sinks, and `close()` reaches every
underlying sink exactly once.

#### Acceptance Criteria:

- [ ] A nested composition such as
      `MultiSink(StdoutSink(), FilteringSink(TransformSink(SQSSink(...), redact), min_level="ERROR"))`
      constructs and emits without special-casing.
- [ ] Calling `close()` on the outer sink reaches every underlying sink: a leaf that appears once in
      the tree is closed exactly once. A sink shared at multiple positions is closed once **per
      position** (the simple `close()` protocol carries no cross-tree visited-set), so every sink's
      `close()` is expected to be idempotent — as all shipped sinks are.
- [ ] The wrappers hold no span/context types — they operate purely on the event-dict batch (arch §8).

---

## Data Model

```
# src/log_forge/sinks/callback.py
CallbackSink {
  fn: Callable[[list[dict]], None]
  on_close: Callable[[], None] | None = None
}

# src/log_forge/sinks/multi.py
MultiSink {
  sinks: tuple[Sink, ...]
  failed: int          # child emit/close failures (counted, logged, not propagated)
}

# src/log_forge/sinks/filtering.py
FilteringSink {
  inner: Sink
  predicate: Callable[[dict], bool] | None
  min_level: str | None      # compared via a DEBUG<INFO<WARNING<ERROR<CRITICAL rank
}

# src/log_forge/sinks/transform.py
TransformSink {
  inner: Sink
  fn: Callable[[dict], dict | None]   # None => drop the event
}
```

Events are the SPEC-001 `LogEvent` dicts; these sinks never look inside beyond `level` (FilteringSink)
and whatever the user's `predicate`/`fn` chooses to read.

---

## API / Interface Contract

```python
# sinks/callback.py
class CallbackSink:
    def __init__(self, fn, *, on_close=None) -> None: ...
    def emit(self, batch: list[dict]) -> None: ...
    def close(self) -> None: ...

# sinks/multi.py
class MultiSink:
    def __init__(self, *sinks: Sink) -> None: ...

# sinks/filtering.py
class FilteringSink:
    def __init__(self, inner: Sink, *, predicate=None, min_level=None) -> None: ...

# sinks/transform.py
class TransformSink:
    def __init__(self, inner: Sink, fn) -> None: ...

# Usage — tee to stdout for dev + SQS for prod, redacting a field and only shipping errors upstream
import log_forge
from log_forge.sinks.stdout import StdoutSink
from log_forge.sinks.sqs import SQSSink
from log_forge.sinks.multi import MultiSink
from log_forge.sinks.filtering import FilteringSink
from log_forge.sinks.transform import TransformSink

def redact(event: dict) -> dict:
    event = dict(event)                       # shallow copy of the top level, AND
    fields = dict(event.get("fields", {}))    # copy the nested dict before mutating it
    fields.pop("password", None)              # (a plain dict(event) would share `fields`)
    event["fields"] = fields
    return event

log_forge.configure(sink=MultiSink(
    StdoutSink(),
    FilteringSink(TransformSink(SQSSink(queue_url="..."), redact), min_level="ERROR"),
))
```

## Configuration / Environment

None. These sinks are stdlib-only and add no config keys, env vars, or dependencies (the core stays
dependency-free).

## File & Folder Structure

```
src/log_forge/sinks/
├── callback.py     # CallbackSink                                   (new)
├── multi.py        # MultiSink (fan-out, per-child failure isolation) (new)
├── filtering.py    # FilteringSink (predicate / min_level)          (new)
└── transform.py    # TransformSink (per-event reshape/redact)       (new)
tests/
└── test_sinks_compose.py   # all four + a nested-composition close-once test (new)
```

## Implementation Phases

### Phase 1: The "where" multipliers — CallbackSink + MultiSink

- Implement `CallbackSink` (delegate + optional `on_close`) and `MultiSink` (sequential fan-out with
  per-child failure isolation and close-all) (FR-001, FR-002).
- Test callback delegation/propagation, fan-out order, one-child-failure isolation, and empty
  `MultiSink()`.

### Phase 2: The "how" reshapers — FilteringSink + TransformSink + composition

- Implement `FilteringSink` (predicate + `min_level` rank, fail-open on unknown level, no empty emit)
  and `TransformSink` (per-event `fn`, `None` drops, no in-place mutation) (FR-003, FR-004).
- Add a nested-composition test asserting emit routing and that `close()` reaches each underlying
  sink exactly once (FR-005).
