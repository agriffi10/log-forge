# Spec: Flush and Buffer Visibility

**ID:** SPEC-036  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-013, SPEC-021, SPEC-026, SPEC-030, SPEC-035

## Overview

`flush()` exists so a process that will be frozen or killed — a Lambda, a job runner — can force
delivery before it stops. `README.md` calls it "the only guaranteed drain" in that environment.
The 2026-08-07 audit found three places where it reports success over undelivered events, and the
first of them breaks the recipe the README itself publishes.

**Measured on `734a9b2`**, the documented serverless recipe verbatim:

```
README guard: drained=True  failed_batches=0 dropped=0 queued=0 stopped_reason=None
delivered so far: []
after the handler returns, delivered = []
```

Zero of two events delivered, every surface clean, nothing on stderr. An in-span event lives on
`span.events` until the span **closes**; `Worker.flush` drains the *queue*; so `flush()` called
inside a `@trace`d function — which is where the recipe puts it — has by construction nothing to
drain. `queued` reads `0` afterwards too, because the drain thread has already taken the
submission into its private `pending`.

This generalises well beyond Lambda. Any long-lived span — a consumer loop, a server `main`
wrapped in `@trace` — buffers indefinitely with every surface reading clean.

The other two are the same shape one layer down: a sink that buffers in its *client*
(`KafkaSink`, `GooglePubSubSink` both flush only in `close()`) is unreachable through `flush()`
because `Sink` declares no flush hook; and the orphan path's own loss reports nowhere, which is
SPEC-034 FR-004 and moves here where it belongs.

## Scope

### In Scope

- `flush()` reaching events buffered in an **open span**.
- A flush hook on the `Sink` protocol, so a client-buffering sink can be drained without closing.
- The orphan path's loss counter (moved from SPEC-034 FR-004).
- An `asyncio` task appending to a span buffer that has already been emitted.
- A permanently dead `MultiSink` child being invisible to `health()`.

### Out of Scope

- **Changing what `flush()` returns.** SPEC-021 settled its window — it reports the drain that
  carried the events, not everything ever lost — and the audit's own review confirmed a latching
  flag would make one transient failure print "undelivered" forever on a warm container.
  Cumulative loss is `health()`'s job, which is why FR-003 adds a counter rather than a return
  value.
- **Making `flush()` a richer type than `bool`.** Real (audit P5) and it belongs to the API
  freeze, SPEC-034.
- **Auto-closing long-lived spans, or bounding how long a span may stay open.** A span's lifetime
  is the caller's; FR-001 makes its buffer *reachable*, not shorter.
- **Ordering guarantees for events emitted from a task after its parent span closed.** FR-004
  makes that case visible and lossless; it does not promise the event lands inside the span it
  was logically part of, which `contextvars` cannot deliver once the span is gone.

---

## Functional Requirements

### FR-001: `flush()` drains open spans

#### Description:

`flush()` must reach every event the caller has emitted, including those still buffered on spans
that have not closed. Anything less makes the serverless contract SPEC-013 built unenforceable,
and makes the README's own recipe wrong.

**The design decision is what "drain an open span" means**, because a span is not finished and
its `span.end` has not happened. Two candidates:

- *Emit the buffered events and leave the span open*, clearing its buffer. The events reach the
  sink; the eventual `span.end` arrives later in its own batch. Events from one span then span
  two batches, which the event schema already tolerates — every event carries its own
  `trace_id`/`span_id` and the README already warns that line order is not delivery order.
- *Close and reopen the span.* Rejected: it would emit a `span.end` the function did not reach,
  with a `duration_ms` and `status` that are fabrications.

The first is the design. `flush()` gains the ability to sweep the span stack in the calling
context, hand each span's buffered events to the worker, and clear the buffer — then drain as it
does today.

**A sweep reaches only the calling context's spans**, which is the honest bound: `contextvars`
gives no way to enumerate other threads' or tasks' contexts. That must be stated rather than
implied, since a `flush()` in a handler that fanned out to tasks will not reach them.

#### Acceptance Criteria:

- [ ] AC-1: The README's serverless recipe, run verbatim, delivers every event before the handler
      returns. This is the headline case and is a test, not an example.
- [ ] AC-2: `flush()` inside a nested span drains **every** open span in the stack, not only the
      innermost.
- [ ] AC-3: The span stays open and usable: events emitted after the flush still land, and the
      eventual `span.end` carries the real `duration_ms` and `status`.
- [ ] AC-4: No event is delivered twice — a flush followed by a normal span close delivers each
      event exactly once. A test counts, and is the one most likely to catch a wrong
      implementation.
- [ ] AC-5: `flush()` with no span open behaves exactly as today.
- [ ] AC-6: `flush()` returns `True` only if the swept events were actually delivered, preserving
      SPEC-021's meaning — the sweep is inside the window the call reports on.
- [ ] AC-7: The bound is documented: a sweep covers the calling context, and events buffered in
      another thread's or task's open span are not reached. `README.md` and `architecture.md` §9
      say so, and a test pins it so the limit cannot silently widen or narrow.
- [ ] AC-8: `tests/test_promises.py`'s loss-visibility cells for `traced` and `async` still pass,
      and any cell this closes has its `xfail` marker removed — `strict=True` makes that
      mandatory rather than optional.

### FR-002: `Sink` gains a flush hook, and `flush()` uses it

#### Description:

`KafkaSink.emit` hands to librdkafka's local buffer and calls `producer.flush()` only in
`close()`. `GooglePubSubSink.emit` appends an unresolved future and resolves them only in
`close()`. So for those sinks `log_foundry.flush()` structurally cannot reach the data — measured
against a stand-in with that shape: `flush() → True`, on the wire 0, in the client buffer 3,
`health()` all zeros.

`Sink` declares `emit` and `close`. It gains an **optional** `flush()`, probed by name exactly as
`losses()` and `stop_signal` are (SPEC-026, SPEC-027), so every existing third-party sink still
satisfies the protocol.

#### Acceptance Criteria:

- [ ] AC-1: `log_foundry.flush()` calls `sink.flush()` when the sink has one, after draining the
      queue — the order matters, since the queue's events must reach the client buffer before it
      is flushed.
- [ ] AC-2: A sink without the method is unaffected, and a pre-SPEC-036 sink still satisfies
      `Sink`. Asserted with a bare `emit`/`close` class.
- [ ] AC-3: `KafkaSink` and `GooglePubSubSink` implement it; `KafkaSink.flush` passes a timeout
      and **counts the remainder it returns** (which is the exact number still queued), and
      `GooglePubSubSink.flush` resolves its outstanding futures and counts failures.
- [ ] AC-4: A `sink.flush()` that fails follows the SPEC-026 rule — total failure raises so
      `log_foundry.flush()` reports `False`; absorbed partial loss goes to `losses()`.
- [ ] AC-5: `sinks/base.py` documents it beside `losses()`, including that it must be safe to
      call concurrently with `emit` (SPEC-028) and that it is *not* a close.
- [ ] AC-6: A lint asserts every sink that buffers in a client implements it, derived from the
      sink roster rather than a hand-written list.

### FR-003: The synchronous path reports its loss

#### Description:

Moved verbatim from SPEC-034 FR-004, which was reviewed three times and is unchanged in
substance. A level call with no active span emits on the caller's thread; SPEC-025 guards it so a
broken destination cannot fail the caller; nothing records that the event was lost, because
`health()` describes a worker and there is none.

`Health` gains an appended field `orphan_lost`. Not `failed_batches` — that means *batches* a
worker abandoned after spending a retry budget, and delivery continues after it; this has no
batch, no retry and no worker. Not `SinkLosses` — the sink did not absorb anything, it raised,
which is what SPEC-026 requires of it. `flush()` is deliberately untouched (see Out of Scope).

#### Acceptance Criteria:

Carried over from SPEC-034 FR-004 AC-1..AC-15, with two changes:

- [ ] AC-1: The superseded criterion is SPEC-033 FR-006 AC-5 ("No field is added to `Health`"),
      struck through in place and marked with **this** spec rather than SPEC-034.
- [ ] AC-2: `tests/test_promises.py`'s `orphan` and `post_shutdown` loss-visibility cells lose
      their `xfail` markers, which `strict=True` forces.

### FR-004: An event from a task outliving its span is not lost

#### Description:

`contextvars` copies the *same* `Span` object into every task created inside a span, and
`submit(span.events)` hands the live list to the queue without detaching it. A fire-and-forget
`create_task` that logs after its parent span closed appends to a list that has already been
emitted.

Measured, same code twice: under the worker's 1 s `flush_interval` the event is delivered but
ordered *after* `span.end` inside its own span; over the interval, silently lost. A pure race on
a timer.

The fix is to **detach at submit** — hand the worker a copy and leave the span with a fresh list —
so the outcome stops depending on timing. That makes a late append land in a buffer nothing will
emit, which is *also* loss, so the second half is to notice it: a span whose buffer is non-empty
after it closed has had a late write, and that is reportable.

#### Acceptance Criteria:

- [ ] AC-1: The outcome no longer depends on the flush interval. The same test at a 0.01 s and a
      10 s interval produces the same result.
- [ ] AC-2: A late append is not silently dropped — it is either delivered or counted, and the
      test asserts which.
- [ ] AC-3: An event emitted from a task **inside** the span's lifetime is unaffected and still
      lands in that span.
- [ ] AC-4: No event is duplicated by the detach.
- [ ] AC-5: `architecture.md` §5 states what a task outliving its parent span gets, since
      `contextvars` makes this reachable by design rather than by mistake.

### FR-005: A dead `MultiSink` child is visible

#### Description:

`MultiSink.emit` isolates a failing child and returns normally unless *every* child failed — the
right behaviour, and SPEC-026 settled it. But `losses()` sums only children that implement
`losses()`, so a child without one contributes nothing, and a destination that has delivered
nothing since the process started is invisible.

Measured: `MultiSink(StdoutSink, DeadRemote)`, `flush() → True`, `health()` all zeros, 9 events on
the good child and 0 on the dead one, permanently. Stderr carries a line per batch — the one
channel this arc has repeatedly shown is not a monitoring surface.

`MultiSink` already counts its own isolated failures in `self.failed` and deliberately excludes
it from `losses()`. That exclusion is the defect: it was to avoid double-counting a child that
reports its own, but the result is under-counting every child that does not.

#### Acceptance Criteria:

- [ ] AC-1: A `MultiSink` with one permanently failing child reports non-zero loss through
      `health().sink`.
- [ ] AC-2: A child that implements `losses()` is **not** double-counted — the test uses one
      reporting child and one silent child and asserts the exact total.
- [ ] AC-3: Which child is failing is discoverable. If the aggregate cannot say, the stderr line
      must name the child's class, and the docstring must say the aggregate is a total.
- [ ] AC-4: A healthy `MultiSink` still reports zero.

---

## Data Model

```python
# src/log_foundry/worker.py
class Health(NamedTuple):
    ...                       # nine existing fields, positions unchanged
    orphan_lost: int = 0      # appended (FR-003)

# src/log_foundry/sinks/base.py — the optional protocol grows one member
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None: ...
    def close(self) -> None: ...
    # optional, probed by name: losses(), stop_signal, and now flush()
```

## Implementation Phases

### Phase 1: FR-003 — the orphan counter

Carried from SPEC-034, already reviewed; landing it first clears two `xfail` cells and proves the
harness's `strict=True` mechanism works end to end.

### Phase 2: FR-001 — the span sweep

The headline. AC-4 (no double delivery) and AC-2 (nested spans) first.

### Phase 3: FR-002 — the `Sink` flush hook

### Phase 4: FR-004 and FR-005
