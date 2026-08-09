# Spec: Flush and Buffer Visibility

**ID:** SPEC-036  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-013, SPEC-021, SPEC-026, SPEC-030  
**Sequenced after:** SPEC-035 — ordering only. Nothing here needs any of 035's FRs; the one real
coupling runs the other way, since 035 FR-005's fork lock roster must pick up the counter lock
FR-003 adds, which is why that roster is derived rather than listed.

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
- **A `buffered` gauge in `health()`.** Both A1 and L2 note that `health().queued` reads `0` over
  events that exist — buffered on an open span, or held in the drain thread's private `pending`.
  FR-001 makes those events *reachable*, which is the defect; making them *countable* is a
  separate feature with its own design surface (what does a gauge over another thread's
  `contextvars` even mean?) and it is additive, so it costs nothing to defer past the freeze.
  Recorded here so the omission is a decision rather than an oversight.
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

**There may be no worker to hand them to, and that is the case AC-1 names.** Measured on this
branch: inside the *first* `@trace`d call `decorator._worker is None` — the worker is created when
a span *closes* — and `_flush_worker` returns `True` without building one, by documented design
(SPEC-013: "a process that never logged has nothing to drain"). A cold-start Lambda whose handler
flushes before returning is exactly that state, so a sweep that submits into a worker that does
not exist delivers nothing and still reports `True`.

So the sweep must **create the worker when it has events to submit**. That narrows
`_flush_worker`'s refusal rather than contradicting it: the refusal exists so an empty flush does
not stand up a thread, and a sweep that found buffered events is not an empty flush. The refusal
stands unchanged when the sweep finds nothing.

**A sweep must not cost a boundary event its baggage backfill.** SPEC-015 completes
`span.start`/`span.end` at *close*, by iterating `span.events` — so a sweep that has emptied the
buffer leaves `span.start` shipping with `fields={}`. Measured:

```
control (no sweep):          span.start fields={'user_id': 'u42'}
with an FR-001-style sweep:  span.start fields=None
```

That is the defect SPEC-015 exists to fix, recreated by any in-span `flush()`. The design must
therefore either backfill a boundary event **before** submitting it in the sweep — accepting that
it carries the baggage as of the flush rather than as of the close, which is a real semantic
change and must be stated — or keep a reference to already-swept boundary events so the close-time
merge still finds them. FR-001 picks the **first**: the second means an event is mutated after the
worker owns it, which SPEC-028's concurrency contract forbids.

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
- [ ] AC-4: No event is delivered twice **and none is destroyed** — a flush followed by a normal
      span close delivers each event exactly once, counted against the sink. This is the AC most
      likely to catch a wrong implementation: `span.events.clear()` empties the *same list object*
      `Worker.submit` was handed, so the natural reading of "clear the buffer" destroys the swept
      events while `flush()` still returns `True`. Measured on a nested span: 4 of 6 events gone,
      with AC-6 and the old AC-4 both passing. **FR-004's detach-at-submit must land first**, and
      the phases below are ordered accordingly.
- [ ] AC-5: A swept `span.start` carries the same baggage it would have carried had the span
      closed normally at that moment. A test asserts the backfill survives the sweep, since this
      is where SPEC-015 regresses.
- [ ] AC-6: `flush()` with no span open behaves exactly as today, **and a flush that sweeps
      nothing still creates no worker** — SPEC-013's refusal is narrowed, not removed. A test
      asserts `decorator._worker is None` after a flush in a process that has never logged.
- [ ] AC-7: A sweep that finds buffered events in a process with no worker creates one and
      delivers them. This is the cold-start path and the one AC-1 exercises; without it AC-1
      cannot pass.
- [ ] AC-8: The swept events reach the **sink**, asserted by counting what the sink received —
      not by asserting `flush()` returned `True`. A draft of this AC said the latter and no
      implementation could fail it: `Worker.flush` answers its marker from `_nothing_lost_since`,
      an empty batch short-circuits in `_emit`, so zero swept events still reports delivered.
- [ ] AC-9: The bound is documented: a sweep covers the calling context, and events buffered in
      another thread's or task's open span are not reached. `README.md` and `architecture.md` §9
      say so, and a test pins it so the limit cannot silently widen or narrow.
- [ ] AC-10: Two tasks sweeping the same shared `Span` concurrently deliver each event once and
      destroy none — `contextvars` copies the same object into both, so this is reachable without
      the caller doing anything unusual.
- [ ] AC-11: `tests/test_promises.py`'s loss-visibility cells for `traced` and `async` still pass,
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
- [ ] AC-7: **A process with no worker still calls `sink.flush()`.** `_flush_worker` returns
      `True` on a null worker without touching the sink, so an orphan-only process with a
      `KafkaSink` would never reach its client buffer — the same shape SPEC-031 FR-006 and
      SPEC-033 each found on the close path.

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

- [ ] AC-1: Five `info()` calls with no span against a sink whose `emit` raises leave
      `health().orphan_lost == 5`.
- [ ] AC-2: `flush()` is unchanged — it still returns `True` in that process, and a test pins
      that, with a comment naming SPEC-021 so a later reader does not "fix" it.
- [ ] AC-3: The new field is **appended** to `Health`, so every existing field keeps its index;
      `Health._fields[:9]` is unchanged.
- [ ] AC-4: A mixed process reports orphan losses in `orphan_lost` and worker losses in
      `failed_batches`, separately — a test asserts both, and that neither absorbs the other.
- [ ] AC-5: The counter is incremented under its **own** lock, not `_worker_lock` — SPEC-028's
      dedicated-counter-lock rule, because the orphan path runs on arbitrary application threads.
      The reason is *not* that `_worker_lock` is held across a blocking close: it is released
      before `owed.close()` and `close_detached` only starts a thread. The blocking hold that
      matters is `_get_worker` across `Worker(_ensure_sink())`. A dedicated lock also cannot
      deadlock here: the increment sits in `api._log`'s `except`, where `_note_orphan_emit` has
      already released `_worker_lock` and the propagating exception has released any sink lock.
- [ ] AC-6: A loss anywhere inside the orphan guard counts — a sink that fails to *construct* or
      an event that fails to build, not only a failing `emit` — since `api._log` wraps all three
      and the event is lost either way. A test covers the construction failure specifically,
      because an increment placed after `sink.emit` would pass AC-1 and fail this.
- [ ] AC-7: `health()` still creates no worker, and the field reads correctly with `_worker`
      unset **and** with a worker present (SPEC-031 FR-006, SPEC-033).
- [ ] AC-8: A successful orphan emit moves nothing.
- [ ] AC-9: The stderr line SPEC-025 already writes is unchanged; this adds a counter, not a
      second announcement.
- [ ] AC-10: **SPEC-033 FR-006 AC-5 ("No field is added to `Health`") is superseded**, struck
      through in place and marked with this spec per SPEC-021's rule, in the spec, in
      `architecture.md` §13, and in `tests/test_orphan_sink_handoff.py::test_health_gains_no_field`
      — which is **rewritten** to pin the tenth field and the unchanged first nine indices, not
      deleted. `tests/test_worker.py`'s `len(h) == 9` assertion moves with it.
- [ ] AC-11: `Health`'s own `Attributes:` block documents the field, as every appended field
      before it does.
- [ ] AC-12: Every surface that currently states this loss is counted nowhere is corrected:
      `README.md`'s alert idiom, its `Health` table, **its serverless recipe — which hand-writes
      its own condition and is the motivation this FR cites** — the post-`shutdown()` paragraph at
      `README.md:830-836`, `architecture.md` §13, `decorator.py`'s `_worker_health` docstring, and
      `__init__.py`'s `health()` docstring.
- [ ] AC-13: `tests/conftest.py`'s reset fixture clears the counter alongside the SPEC-031/033
      flags. Its docstring already names the failure this prevents: state leaking into every later
      test.
- [ ] AC-14: **The README PR (`docs/readme-1.0`) merges before this spec is built.** All the README
      criteria above target text that PR rewrites, and one of them ("silence is not success
      anywhere") exists only there.
- [ ] AC-15: Each assertion is mutation-tested.
- [ ] AC-16: `tests/test_promises.py`'s `orphan` and `post_shutdown` loss-visibility cells lose
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
emit, which is *also* loss, so the second half is to notice it.

**The detection must happen at append time, not after the fact.** A first draft of this FR said "a
span whose buffer is non-empty after it closed is reportable", which has no observer: nothing in
the library looks at a span again after `_close_span` submits and returns. `api._log` is the only
code that appends, so it is the only place that can notice — the span carries a closed flag, and
an append to a closed span takes the orphan route instead (a fresh one-event span, which is
already what a level call with no span does) or is counted.

#### Acceptance Criteria:

- [ ] AC-1: The outcome no longer depends on the flush interval. The same test at a 0.01 s and a
      10 s interval produces the same result.
- [ ] AC-2: A late append is not silently dropped — it is either delivered or counted, decided in
      `api._log` at append time against a closed-span flag, and the test asserts which. A
      post-hoc check of the buffer cannot satisfy this: nothing reads a span after it closes.
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
it from `losses()`. The exclusion has a real reason — the units differ, `MultiSink.failed` counts
child *calls* that raised while `SinkLosses.failed` counts *events* — and a child that implements
`losses()` increments both, so simply folding it in would double-count exactly the case AC-2
forbids.

So the fix is neither to fold it in nor to leave it out, and `self.failed` as it stands cannot
support either: it is a single counter with no per-child breakdown, so there is no way to add
"events from the silent children only". `MultiSink` must count **per child** — for each child that
`read_losses` returns `None` for, add `len(batch)` on each failing call — and leave a reporting
child to report itself.

#### Acceptance Criteria:

- [ ] AC-1: A `MultiSink` with one permanently failing child reports non-zero loss through
      `health().sink`.
- [ ] AC-2: A child that implements `losses()` is **not** double-counted — the test uses one
      reporting child and one silent child and asserts the exact total. The aggregate adds
      `len(batch)` per failing **silent** child, matching `SinkLosses.failed`'s unit. This needs
      per-child accounting that `MultiSink` does not have today; the existing `self.failed` counts
      child *calls* across all children and stays out of the sum.
- [ ] AC-5: Which children are silent is decided by `read_losses(child) is None`, the same probe
      the aggregate already uses, so a child that gains a `losses()` later moves categories
      automatically.
- [ ] AC-6: Which child is failing is discoverable. If the aggregate cannot say, the stderr line
      must name the child's class, and the docstring must say the aggregate is a total.
- [ ] AC-7: A healthy `MultiSink` still reports zero.

---

## Data Model

```python
# src/log_foundry/worker.py
class Health(NamedTuple):     # still a NamedTuple *here*; SPEC-034 FR-008 converts it later,
    ...                       #   which is why SPEC-034 declares a dependency on this spec
    orphan_lost: int = 0      # appended (FR-003)

# src/log_foundry/sinks/base.py — the optional protocol grows one member
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None: ...
    def close(self) -> None: ...
    # optional, probed by name: losses(), stop_signal, and now flush()
```

## Implementation Phases

### Phase 1: FR-004's detach-at-submit, then FR-003

The detach comes **first in the whole spec**, not fourth: FR-001's sweep is unsafe without it,
because clearing a buffer the worker was handed by reference destroys the events. FR-003 (the
orphan counter, carried from SPEC-034 and already reviewed) rides with it and clears two `xfail`
cells, proving the harness's `strict=True` mechanism end to end.

### Phase 2: FR-001 — the span sweep

The headline. AC-4 (nothing destroyed), AC-5 (the backfill survives) and AC-2 (nested spans)
first — those are the three a wrong implementation passes the rest of the ACs without.

### Phase 3: FR-002 — the `Sink` flush hook

### Phase 4: FR-005
