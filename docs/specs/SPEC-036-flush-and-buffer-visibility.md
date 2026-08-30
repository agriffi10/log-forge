# Spec: Flush and Buffer Visibility

**ID:** SPEC-036  
**Status:** In Progress  
**Last Updated:** 2026-08-30  
**Depends On:** SPEC-013, SPEC-021, SPEC-026, SPEC-030, SPEC-034, SPEC-037

Every spec this one depends on has shipped. From SPEC-034 it takes `Health` as a frozen dataclass —
so FR-003's two counters are plain appends with no index to prove — and `FlushResult`, so a new
failure reason has somewhere to go. From SPEC-037 it inherits `in_span_lost`, the counter that spec
deferred here so the pair is designed once, in the spec that invents `orphan_lost`. Sequencing
history is under **Revision history**.

## Overview

`flush()` exists so a process that will be frozen or killed — a Lambda, a job runner — can force
delivery before it stops. `README.md` calls it "the only guaranteed drain" in that environment. The
2026-08-07 audit found three places where it reports success over undelivered events, and the first
of them breaks the recipe the README itself publishes.

**Re-measured on `690d2a5`** (originally on `734a9b2`), the documented serverless recipe verbatim:

```
README guard: drained=True reason=None failed_batches=0 dropped=0 queued=0 stopped_reason=None
delivered so far: []
after the handler returns, delivered = []
```

Zero of two events delivered, every surface clean, nothing on stderr. An in-span event lives on
`span.events` until the span **closes**; `Worker.flush` drains the *queue*; so `flush()` called
inside a `@trace`d function — which is where the recipe puts it — has by construction nothing to
drain. `queued` reads `0` afterwards too, because the drain thread has already taken the submission
into its private `pending`. SPEC-034 has since made the return value a `FlushResult` that exists to
say *why*, and it says `reason=None` — success.

This generalises well beyond Lambda. Any long-lived span — a consumer loop, a server `main` wrapped
in `@trace` — buffers indefinitely with every surface reading clean.

The other two findings are the same shape one layer down: a sink that buffers in its *client*
(`KafkaSink`, `GooglePubSubSink` both flush only in `close()`) is unreachable through `flush()`
because `Sink` declares no flush hook; and the orphan path's own loss reports nowhere, which was an
FR of SPEC-034's earlier draft and moves here where it belongs (FR-003).

## Scope

### In Scope

- `flush()` reaching events buffered in an **open span**.
- A flush hook on the `Sink` protocol, so a client-buffering sink can be drained without closing.
- The orphan path's loss counter (moved from SPEC-034's earlier draft; FR-003 here).
- An `asyncio` task appending to a span buffer that has already been emitted.
- A permanently dead `MultiSink` child being invisible to `health()`.

### Out of Scope

- **Changing what `flush()` reports about.** SPEC-021 settled its window — it reports the drain that
  carried the events, not everything ever lost — and the audit's own review confirmed a latching
  flag would make one transient failure print "undelivered" forever on a warm container. Cumulative
  loss is `health()`'s job, which is why FR-003 adds a counter rather than a return value. FR-002
  adds a new `reason` token, which is additive and is what `FlushResult` was built for; it does not
  widen the window.
- ~~**Making `flush()` a richer type than `bool`** — real (audit P5) and it belongs to the API
  freeze, SPEC-034.~~ — **shipped**: SPEC-034 FR-007 landed `FlushResult`/`ContinueResult`, so this
  spec inherits the type rather than deferring it.
- **Auto-closing long-lived spans, or bounding how long a span may stay open.** A span's lifetime is
  the caller's; FR-001 makes its buffer *reachable*, not shorter.
- **A `buffered` gauge in `health()`.** Both audit A1 and L2 note that `health().queued` reads `0`
  over events that exist — buffered on an open span, or held in the drain thread's private
  `pending`. FR-001 makes those events *reachable*, which is the defect; making them *countable* is
  a separate feature with its own design surface (what does a gauge over another thread's
  `contextvars` even mean?) and it is additive, so it costs nothing to defer. Recorded here so the
  omission is a decision rather than an oversight.
- **Ordering guarantees for events emitted from a task after its parent span closed.** FR-004 makes
  that case visible and lossless; it does not promise the event lands inside the span it was
  logically part of, which `contextvars` cannot deliver once the span is gone. The event takes the
  orphan route, so it gets a **fresh `trace_id`** and not merely a different span — it leaves its
  trace. Stated because that is the SPEC-024 wrong-data category this spec invokes elsewhere, and it
  is accepted only because the alternative on this path is losing the event outright.

---

## Functional Requirements

### FR-001: `flush()` drains open spans

#### Description:

`flush()` must reach every event the caller has emitted, including those still buffered on spans
that have not closed. Anything less makes the serverless contract SPEC-013 built unenforceable, and
makes the README's own recipe wrong.

**What "drain an open span" means.** A span is not finished and its `span.end` has not happened. Two
candidates: *emit the buffered events and leave the span open*, clearing its buffer — the events
reach the sink and the eventual `span.end` arrives later in its own batch, so one span's events span
two batches, which the schema already tolerates (every event carries its own `trace_id`/`span_id`,
and the README already warns that line order is not delivery order); or *close and reopen the span*,
rejected because it emits a `span.end` the function did not reach, with a `duration_ms` and `status`
that are fabrications. The first is the design: `flush()` sweeps the span stack in the calling
context, hands each span's buffered events to the worker, clears the buffer, then drains as today.

**The README recipe is separable from the defect, and the two are not traded for each other.**
`flush()` sits inside the `@trace`d handler because that is where the README puts it; moving it
*outside* the span closes the published recipe in documentation with no runtime change:

```python
def handler(event, context):          # not decorated
    try:
        return _handler(event, context)   # @lf.trace — the span closes here
    finally:
        drained = lf.flush()
```

That is worth doing and is **not** a substitute for the sweep, on two grounds: it fixes one
published example while every other long-lived span keeps buffering with every surface reading
clean, which is the general defect; and it makes the library's correctness depend on the caller
having read a paragraph, the standard SPEC-025 and SPEC-030 both refused. So AC-1a takes the doc
change first, because it is free and stops the wrong shape being copied while the rest is built, and
AC-1 still requires the in-span form to work. After AC-1a, AC-1 tests a recipe the README no longer
publishes — which AC-1 records, so a later reader does not delete it as dead. **There are two such
sites**, the `continue_trace()` example and the serverless recipe, not the one the first draft named.

**There may be no worker to hand the events to.** Measured: inside the *first* `@trace`d call
`decorator._worker is None` — the worker is created when a span *closes* — and `_flush_worker`
returns success without building one, by documented design (SPEC-013: "a process that never logged
has nothing to drain"). A cold-start Lambda whose handler flushes before returning is exactly that
state. So the sweep must **create the worker when it has events to submit**, which narrows
SPEC-013's refusal rather than contradicting it: the refusal exists so an *empty* flush does not
stand up a thread, and a sweep that found buffered events is not an empty flush. The refusal stands
unchanged when the sweep finds nothing (AC-6, AC-7).

**A sweep must not cost a boundary event its baggage backfill.** SPEC-015 completes
`span.start`/`span.end` at *close*, by iterating `span.events` — so a sweep that emptied the buffer
leaves `span.start` shipping with `fields={}`. Measured:

```
control (no sweep):          span.start fields={'user_id': 'u42'}
with an FR-001-style sweep:  span.start fields=None
```

That is the defect SPEC-015 exists to fix, recreated by any in-span `flush()`. The design backfills
a boundary event **before** submitting it in the sweep, accepting that it carries the baggage as of
the flush rather than as of the close — a real semantic change, stated here and pinned by AC-5. The
alternative, keeping a reference to already-swept boundary events so the close-time merge still
finds them, means mutating an event after the worker owns it, which SPEC-028's concurrency contract
forbids.

**A sweep destroys events unless the submit detaches.** `span.events.clear()` empties the *same list
object* `Worker.submit` was handed, so the natural reading of "clear the buffer" destroys the swept
events while `flush()` still reports success. Measured on a nested span: 4 of 6 events gone, with an
earlier draft's AC-4 and AC-6 both passing. FR-004's detach-at-submit must land first, and the
phases are ordered accordingly. AC-4 is the criterion most likely to catch a wrong implementation.

**A sweep makes a later `continue_trace()` unable to re-parent what it swept.**
`decorator._reparent_current_span` re-parents the events still *buffered* on the open root span, by
iterating `span.events` — SPEC-014's mechanism for adopting a context on the entry point's first
line. Swept events have left the buffer, so they keep the pre-adoption trace while everything after
joins the inbound one. Measured with the detach in place:

```
control: span.start 4bf92f35 · before-adopt 4bf92f35 · after-adopt 4bf92f35 · span.end 4bf92f35
swept:   span.start 47726b1a · before-adopt 47726b1a · after-adopt 4bf92f35 · span.end 4bf92f35
```

One span, two trace ids — what `_reparent_current_span`'s docstring calls "worse than no
continuation at all because it looks like data rather than a bug", and the SPEC-024 category of
wrong data rather than lost data. Reproduced independently against `5ad6699` with the sweep modelled
as FR-001 + FR-004 specify it. The local trace id differs run to run, which is the point: what
reproduces is the **shape**, two distinct ids across one span, not the ids above.

**What a swept span refuses is the trace context, not the call.** `continue_trace()` does two
separable things and `decorator.py` separates them deliberately: it adopts a trace context, and it
merges a `baggage` header, the second "independently of the trace context, because losing
correlating fields is bad and losing the trace join because one field was malformed is worse".
Refusing the whole call would drop a baggage merge SPEC-014 decided must survive a trace-context
failure — trading wrong data for lost data. So the refusal covers `set_adopted_context` and the
re-parent; the baggage merge runs as today (AC-11a). AC-11 exists as its own criterion because
nothing in AC-1..AC-10 needs it: an implementation satisfying all of them still leaves
`continue_trace()` no way to tell a swept span from an untouched one, which is how this handoff
would arrive with no catcher. The refusal reports through `_diag.rejected`, the channel SPEC-014's
other refusals already use.

**Which span carries the record is `context.current_span()`** — what `_reparent_current_span` itself
reads, and it already returns early unless that span is a root (`span.parent_span_id is not None`).
So the refusal bites exactly where an adoption would otherwise have done something, and
`continue_trace()`'s documented placement on the entry point's first line is unaffected: nothing has
been swept yet. An earlier draft of this paragraph said "the root span" while arguing the case for
the innermost, which are different behaviours whenever a child span is opened *after* a flush.

**A third design was considered and not taken: splitting `Span.events` into a pending buffer and a
full record.** Three of this FR's landmines exist because one list is simultaneously the queue's
payload, SPEC-015's backfill target and SPEC-014's re-parent target, and a `pending`/`record` split
would dissolve AC-4's outright. It is recorded rather than adopted because it does not reach the
other two: both the backfill and the re-parent *mutate* events, and once an event has been submitted
SPEC-028's contract forbids touching it, whichever list it is also in. So AC-5 and AC-11a survive
the split intact, and the change would buy one trap for a second copy of every event on the hot
path. Worth re-opening only if a fourth consumer of `span.events` appears.

**AC-10's concurrency must be forced, not raced, and it is about threads.** Measured during this
spec's implementer review: a naive detach shows zero duplicates in 400 unforced trials and 40 of 40
when the window is held open, so an unforced
test ticks the criterion against CPython's current bytecode granularity — the property SPEC-028
says a test must not rest on. `tests/conftest.py::run_concurrently` and injected preemption points
exist for this. It names *threads* because two asyncio tasks cannot interleave inside a synchronous
`flush()` at all, which would make the literal scenario unfalsifiable — at the cost of the old
criterion's reachability argument, which was true of tasks (`contextvars` copies the same `Span`
object into both) and is not true of a bare thread, which starts with a fresh context. The shared
span is still reached without the caller doing anything unusual, via `asyncio.to_thread` or
`copy_context().run`.

**AC-8 counts what the sink received, and the obvious phrasing is vacuous.** A draft asserted that
`flush()` returned `True` instead, which no implementation could fail: `Worker.flush` answers its
marker from `_nothing_lost_since`, and an empty batch short-circuits in `_emit`, so zero swept events
still reports delivered.

**A sweep reaches only the calling context's spans**, which is the honest bound: `contextvars` gives
no way to enumerate another thread's or task's context. AC-9 states it rather than leaving it
implied, since a `flush()` in a handler that fanned out to tasks will not reach them.

#### Acceptance Criteria:

- [ ] AC-1: The README's serverless recipe in its **in-span** form, run verbatim, delivers every
      event before the handler returns. It is a test, not an example, and it survives AC-1a.
- [ ] AC-1a: Both README sites move `flush()` outside the traced span, as their own commit, first.
- [ ] AC-2: `flush()` inside a nested span drains **every** open span in the stack, not only the
      innermost.
- [ ] AC-3: The span stays open and usable — events emitted after the flush still land, and the
      eventual `span.end` carries the real `duration_ms` and `status`.
- [ ] AC-4: A flush followed by a normal span close delivers each event exactly once against the
      sink: none duplicated, **and none destroyed**.
- [ ] AC-5: A swept `span.start` carries the same baggage it would have carried had the span closed
      normally at that moment.
- [ ] AC-6: `flush()` with no span open behaves exactly as today, and a flush that sweeps nothing
      creates no worker — asserted as `decorator._worker is None` after a flush in a process that
      has never logged.
- [ ] AC-7: A sweep that finds buffered events in a process with no worker creates one and delivers
      them.
- [ ] AC-8: Delivery is asserted by counting what the **sink** received, never by asserting
      `flush()` was truthy.
- [ ] AC-9: `README.md` and `architecture.md` §9 state that a sweep covers the calling context only,
      and a test pins the bound so it cannot silently widen or narrow.
- [ ] AC-10: Two **threads** sweeping the same shared `Span` concurrently deliver each event once
      and destroy none, asserted with the window forced open rather than by racing it.
- [ ] AC-11: The sweep records that it swept, on the `Span`, and that record is what AC-11a keys on.
- [ ] AC-11a: `continue_trace()` on a swept span refuses the **trace context** — neither adopting it
      nor re-parenting what is left — reports through `_diag.rejected`, and returns a falsy
      `ContinueResult`. The `baggage` merge still runs, and the record is read from the span
      `_reparent_current_span` acts on — `context.current_span()`.
- [ ] AC-11b: Tests assert one trace id across the whole span in both orders, and that a `baggage=`
      passed to the refused call still reaches the events that follow it.
- [ ] AC-12: `tests/test_promises.py`'s `traced` and `async` loss-visibility cells still pass.

### FR-002: `Sink` gains a flush hook, and `flush()` uses it

#### Description:

`KafkaSink.emit` hands to librdkafka's local buffer and calls `producer.flush()` only in `close()`.
`GooglePubSubSink.emit` appends an unresolved future and resolves them only in `close()`. So for
those sinks `log_foundry.flush()` structurally cannot reach the data — measured against a stand-in
with that shape: `flush() → True`, on the wire 0, in the client buffer 3, `health()` all zeros.

`Sink` declares `emit` and `close`. It gains an **optional** `flush()`, probed by name exactly as
`losses()`, `log_foundry_stop_signal` and `reacquire_after_fork()` are (SPEC-026, SPEC-027,
SPEC-039 FR-004, its contract restated by SPEC-042 FR-005), so every existing third-party sink still satisfies the protocol.

**The roster is five, and SPEC-042 measured it.** That spec read the shipped `close()` bodies to
find what a *refused* close costs a forked child: `KafkaSink` and `GooglePubSubSink` do not deliver
their buffer, `NATSSink` does not drain its loop, and `SQLiteSink` and `PostgresSink` do not
**commit**. Since a forked child now refuses to close a sink it inherited, those five have no route
out for pending work until this hook exists — the last two leaving inserts uncommitted on a
connection the parent also holds, which is the safer outcome but not a good one. Which of the five
implement the hook is this spec's decision (AC-3a); what SPEC-042 handed over is the roster, so the
choice is not made against a list of two.

**AC-1's ordering lands this FR on SPEC-035's worker-predicate roster, and the category is
`LIVENESS`.** Sequencing the sink flush *after* the drain means binding the drain's result in
`_flush_worker`, which today returns `worker.flush(timeout)` directly; a binding that names the
worker is a roster site and must declare a category before it can pass. `LIVENESS` is defined as
"who *performs* an action, and a retired worker performs nothing", which is exactly what the bound
value carries — `Worker.flush` returns falsy immediately once `_shutdown_done`, as that site's
existing roster reason already records. The precedent is `("_worker_health", "worker.health()", 0)`,
a method-call binding on the worker filed under the same category.

No fifth category is needed, and an earlier draft of this paragraph said otherwise on two counts,
both false against the file it cited: that none of the four describes *performing* a drain (`LIVENESS`
is defined in those words), and that the roster's docstring refuses a fifth (the refusal is one
`ROSTER` reason rejecting a specific proposed category, `not-a-worker-question`, as an unbounded
escape hatch, while `_linted_nodes` says a fifth would be picked up automatically). Recorded because
a negative claim written into a spec has nothing downstream to catch it.

This is the **worker-predicate** roster, not the fork-lock roster FR-003 AC-5a names. The Revision
history's note that "the roster moved to SPEC-039" is about the latter, and conflating the two is
what left this unnoticed.

**A failed sink flush needs a `FlushResult` reason, and it is the only one this spec invents.** An
earlier header claimed FR-001 and FR-002 each invent one; FR-001 does not — a sweep that cannot
deliver already lands on `"abandoned"`, `"queue-full"` or `"thread-died"`, the tokens SPEC-034
shipped. FR-002 adds `"sink-flush"`, for a queue that drained cleanly and a client buffer that did
not (AC-8). Adding a token is what `FlushResult` was built for and is additive by construction.

#### Acceptance Criteria:

- [ ] AC-1: `log_foundry.flush()` calls `sink.flush()` when the sink has one, **after** draining the
      queue — the order matters, since the queue's events must reach the client buffer before it is
      flushed.
- [ ] AC-2: A sink without the method is unaffected and still satisfies `Sink`, asserted with a bare
      `emit`/`close` class.
- [ ] AC-3: `KafkaSink` and `GooglePubSubSink` **implement it** — the FR's own evidence is that both
      buffer in a client. `KafkaSink.flush` passes a timeout and counts the remainder it returns (the
      exact number still queued); `GooglePubSubSink.flush` resolves its futures and counts failures.
- [ ] AC-3a: The decision is taken against SPEC-042's measured five — `KafkaSink`,
      `GooglePubSubSink`, `NATSSink`, `SQLiteSink`, `PostgresSink` — and which of them implement the
      hook is recorded with its reason.
- [ ] AC-4: A `sink.flush()` that fails follows the SPEC-026 rule — total failure raises so
      `log_foundry.flush()` reports falsy; absorbed partial loss goes to `losses()`.
- [ ] AC-5: `sinks/base.py` documents it beside `losses()`, including that it must be safe to call
      concurrently with `emit` (SPEC-028) and that it is *not* a close.
- [ ] AC-6: A lint asserts every sink that buffers in a client implements it, derived from the sink
      roster rather than a hand-written list.
- [ ] AC-7: A process with no worker whose orphan path has reached a sink still calls
      `sink.flush()`, gated on `decorator._orphan_sink` — the sink an orphan emit actually reached,
      not "a sink has been resolved". A `flush()` in a process that has never logged touches no sink
      and materialises none.
- [ ] AC-8: A `flush()` whose queue drained but whose `sink.flush()` failed returns
      `FlushResult(ok=False, reason="sink-flush")`, and the token is documented on `FlushResult`.
- [ ] AC-9: The drain result bound in `_flush_worker` is filed on SPEC-035's worker-predicate
      roster (`tests/test_worker_predicate_roster.py`) as **`LIVENESS`**, with a reason citing the
      `worker.health()` precedent. No fifth category is added.
- [ ] AC-10: `flush()` on a **closed** sink follows SPEC-032 — a sink that released its transport
      refuses rather than touching it, and the post-close roster gains a `flush` arm.
- [ ] AC-10a: The roster's exemption gate resolves its strongest fact from `emit`/`send_all`, so a
      sink whose `flush` holds transport state while its `emit` does not could still claim
      `ACCEPTS_AFTER_CLOSE`. The gate learns `flush`.

### FR-003: The synchronous path reports its loss

#### Description:

A level call with no active span emits on the caller's thread; SPEC-025 guards it so a broken
destination cannot fail the caller; nothing records that the event was lost, because `health()`
describes a worker and there is none.

`Health` gains `orphan_lost`. Not `failed_batches` — that means *batches* a worker abandoned after
spending a retry budget, and delivery continues after it; this has no batch, no retry and no worker.
Not `SinkLosses` — the sink did not absorb anything, it raised, which is what SPEC-026 requires of
it.

**It gains a second field with it: `in_span_lost`, inherited from SPEC-037 AC-5c.** That spec
shipped the *guard* — an unguarded `build_event` inside a span can fail the caller — while deferring
the counting here, so the two are designed as a pair in the spec that invents the vocabulary rather
than one per spec with a name negotiated across two drafts. The distinction they preserve is
SPEC-026's test, *would one number hide which fix applies*: `orphan_lost` covers everything inside
the orphan guard, including a failing `sink.emit`, so it climbing means **the destination or the
data**; the in-span path cannot lose an event at `emit` (that is `failed_batches`), so `in_span_lost`
climbing means **the data**, always. Different remediation, two fields.

**The counters are the eleventh and twelfth `Health` fields**, not the tenth and eleventh an earlier
draft assumed — SPEC-042 has since appended `inherited_sink`. Under SPEC-034's frozen dataclass
there is no index to prove either way, and `test_health_gains_no_field` already pins the set **by
name**, so AC-10 adds two names to a list rather than rewriting a test.

**The increment takes its own lock, not `_worker_lock`** (SPEC-028's dedicated-counter-lock rule),
because the orphan path runs on arbitrary application threads. The reason is *not* that
`_worker_lock` is held across a blocking close — it is released before `owed.close()`, and
`close_detached` only starts a thread; the blocking hold that matters is `_get_worker` across
`Worker(_ensure_sink())`. A dedicated lock cannot deadlock here either: the increment sits in
`api._log`'s `except`, where `_note_orphan_emit` has already released `_worker_lock` and the
propagating exception has released any sink lock.

#### Acceptance Criteria:

- [ ] AC-1: Five `info()` calls with no span against a sink whose `emit` raises leave
      `health().orphan_lost == 5`.
- [ ] AC-1a: Five `info(ValueError(…))` calls **inside** a span leave `health().in_span_lost == 5`,
      asserted separately from `orphan_lost` with neither absorbing the other. Deliberately not a
      criterion on their sum.
- [ ] AC-2: `flush()` is unchanged — it still reports success in that process, pinned by a test
      naming SPEC-021 so a later reader does not "fix" it.
- [ ] AC-3: Both fields are appended to `Health` with path-scoped names. ~~so every existing field
      keeps its index~~ — struck (Revision history): SPEC-034 made `Health` a dataclass.
- [ ] AC-4: A mixed process reports orphan losses in `orphan_lost` and worker losses in
      `failed_batches`, separately, with neither absorbing the other.
- [ ] AC-5: The counter is incremented under its **own** lock, not `_worker_lock`.
- [ ] AC-5a: SPEC-039 FR-003's derived fork roster picks the new lock up with no edit there, and a
      test proves it. SPEC-039 has shipped, so this AC is the side that must be checked.
- [ ] AC-6: A loss anywhere inside the orphan guard counts — a sink that fails to *construct* or an
      event that fails to build, not only a failing `emit`. A test covers the construction failure
      specifically, since an increment placed after `sink.emit` would pass AC-1 and fail this.
- [ ] AC-7: `health()` still creates no worker, and both fields read correctly with `_worker` unset
      **and** with a worker present.
- [ ] AC-8: A successful orphan emit moves nothing.
- [ ] AC-9: The stderr line SPEC-025 already writes is unchanged — this adds a counter, not a second
      announcement.
- [ ] AC-10: **SPEC-033 FR-006 AC-5 ("No field is added to `Health`") is superseded**, struck through
      in place and marked with this spec per SPEC-021's rule, at `SPEC-033-orphan-path-sink-handoff.md:461`.
- [ ] AC-10a: The **second** home is marked too: `test_health_gains_no_field` gains the two names, and
      its name and docstring still assert the superseded claim over a twelve-name list, so both get the
      marker. ~~and in `architecture.md` §13~~ — struck: §13 states nothing about `Health` (the word
      occurs once in that file, in §6), so it was never a third home.
- [ ] AC-11: `Health`'s own `Attributes:` block documents both fields.
- [ ] AC-12: Every surface that currently states this loss is counted nowhere is corrected:
      `README.md`'s alert idiom, its `Health` table, its serverless recipe, its post-`shutdown()`
      paragraph, `architecture.md` §13, `decorator.py`'s `_worker_health` docstring, and
      `__init__.py`'s `health()` docstring.
- [ ] AC-13: `tests/conftest.py`'s reset fixture clears both counters alongside the SPEC-031/033
      flags.
- [ ] AC-14: The README criteria above are satisfied against **whatever `main`'s README says at
      build time**. ~~The README PR merges before this spec is built.~~ — struck (Revision history).
- [ ] AC-15: The assertions guarding the two increment sites are mutation-tested — the narrow rule,
      not every assertion in the FR.
- [ ] AC-16: `tests/test_promises.py`'s `orphan` and `post_shutdown` loss-visibility cells lose their
      `xfail` markers, which `strict=True` forces — **and the assertion those cells run learns the two
      new fields**. Removing the markers alone leaves both cells red: the disjunction names the
      worker's three counters and a sink term, none of which a visible `orphan_lost` satisfies.

### FR-004: An event from a task outliving its span is not lost

#### Description:

`contextvars` copies the *same* `Span` object into every task created inside a span, and
`submit(span.events)` hands the live list to the queue without detaching it. A fire-and-forget
`create_task` that logs after its parent span closed appends to a list that has already been
emitted.

Measured, same code twice: under the worker's 1 s `flush_interval` the event is delivered but
ordered *after* `span.end` inside its own span; over the interval, silently lost. A pure race on a
timer.

The fix is to **detach at submit** — hand the worker a copy and leave the span with a fresh list —
so the outcome stops depending on timing. That makes a late append land in a buffer nothing will
emit, which is *also* loss, so the second half is to notice it.

**The detection must happen at append time, not after the fact.** A first draft said "a span whose
buffer is non-empty after it closed is reportable", which has no observer: nothing in the library
looks at a span again after `_close_span` submits and returns. `api._log` is the only code that
appends, so it is the only place that can notice — the span carries a closed flag, and an append to
a closed span takes the orphan route instead (a fresh one-event span, which is already what a level
call with no span does).

**It adds no `Health` field.** A span that has closed is not a span, so the append takes the orphan
route and inherits its accounting: delivered on success, and on failure counted in FR-003's
`orphan_lost` by the guard that already wraps it. Stated because the alternative — a third counter —
would make `Health` thirteen fields, and because leaving the destination unnamed is exactly what
SPEC-037 FR-001 AC-5 had to be re-opened to fix.

#### Acceptance Criteria:

- [ ] AC-1: The outcome no longer depends on the flush interval — the same test at a 0.01 s and a
      10 s interval produces the same result.
- [ ] AC-2: A late append is not silently dropped. It is decided in `api._log` at append time
      against a closed-span flag, takes the orphan route, and the test asserts which outcome it got.
- [ ] AC-3: An event emitted from a task **inside** the span's lifetime is unaffected and still
      lands in that span.
- [ ] AC-4: No event is duplicated by the detach.
- [ ] AC-5: `architecture.md` §5 states what a task outliving its parent span gets.

### FR-005: A dead `MultiSink` child is visible

#### Description:

`MultiSink.emit` isolates a failing child and returns normally unless *every* child failed — the
right behaviour, and SPEC-026 settled it. But `losses()` sums only children that implement
`losses()`, so a child without one contributes nothing, and a destination that has delivered nothing
since the process started is invisible.

Measured: `MultiSink(StdoutSink, DeadRemote)`, `flush() → True`, `health()` all zeros, 9 events on
the good child and 0 on the dead one, permanently. Stderr carries a line per batch — the one channel
this arc has repeatedly shown is not a monitoring surface.

`MultiSink` already counts its own isolated failures in `self.failed` and deliberately excludes it
from `losses()`. The exclusion has a real reason — the units differ, `MultiSink.failed` counts child
*calls* that raised while `SinkLosses.failed` counts *events* — and a child that implements
`losses()` increments both, so folding it in would double-count exactly the case AC-2 forbids. So
the fix is neither to fold it in nor to leave it out, and `self.failed` as it stands cannot support
either: it is a single counter with no per-child breakdown, so there is no way to add "events from
the silent children only". `MultiSink` must count **per child** — for each child that `read_losses`
returns `None` for, add `len(batch)` on each failing call — and leave a reporting child to report
itself.

#### Acceptance Criteria:

- [ ] AC-1: A `MultiSink` with one permanently failing child reports non-zero loss through
      `health().sink`.
- [ ] AC-2: A child that implements `losses()` is **not** double-counted — one reporting child and
      one silent child, asserting the exact total. The aggregate adds `len(batch)` per failing
      **silent** child, matching `SinkLosses.failed`'s unit; the existing `self.failed` stays out of
      the sum.
- [ ] AC-3: Which children are silent is decided by `read_losses(child) is None`, so a child that
      gains a `losses()` later moves categories automatically. The consequence is recorded: the
      aggregate can **fall** when a child starts reporting, against `SinkLosses`'s "cumulative for its
      lifetime", and the None-until-something idiom the shipped wrappers use makes that reachable.
- [ ] AC-4: `tests/test_sink_losses.py::test_multisink_excludes_its_own_call_counter` is **rewritten,
      not deleted** — it asserts the superseded behaviour (`failed=6` where this FR gives `failed=7`).
- [ ] AC-4a: The superseded reasoning is struck through in place and marked with this spec in **both**
      homes — that test's docstring and `MultiSink.losses()`'s docstring — since striking only the one
      AC-4 quotes leaves the other reading as live design.
- [ ] AC-5: Which child is failing is discoverable. If the aggregate cannot say, the stderr line
      names the child's class and the docstring says the aggregate is a total.
- [ ] AC-6: A healthy `MultiSink` still reports zero.

---

## Data Model

```python
# src/log_foundry/context.py — the sweep needs the whole stack, and `current_span()` returns only
# the innermost, so the private `_span_stack` gains a read accessor (FR-001 AC-2)

# src/log_foundry/decorator.py — the detach lives in `_flush`, not `Worker.submit`: FR-004 needs the
# span left with a fresh list, and `Worker.submit` has no span to leave one with

# src/log_foundry/model.py — Span gains two flags, both read on the append/adopt hot paths
@dataclass
class Span:
    ...
    swept: bool = False       # FR-001 AC-11: set by the sweep, read by AC-11a's refusal
    closed: bool = False      # FR-004 AC-2: set at close, read by api._log at append time

# src/log_foundry/worker.py — a frozen dataclass since SPEC-034 FR-008, so both are plain
# appends: the eleventh and twelfth fields, after SPEC-042's `inherited_sink`
@dataclass(frozen=True)
class Health:
    ...
    orphan_lost: int = 0      # FR-003
    in_span_lost: int = 0     # FR-003, inherited from SPEC-037 AC-5c

# src/log_foundry/results.py — one new reason token, additive by construction
#   FlushResult.reason is `str | None`, so "sink-flush" is a documented token, not a type
#   change (FR-002 AC-8)

# src/log_foundry/sinks/base.py — the optional protocol grows one member
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None: ...   # @abstractmethod, SPEC-034 FR-005
    def close(self) -> None: ...                                  # @abstractmethod, SPEC-034 FR-005
    # optional, probed by name: losses(), log_foundry_stop_signal, reacquire_after_fork(),
    # and now flush()
```

## Implementation Phases

### Phase 0: FR-005 and FR-001 AC-1a — the two that need nothing else

FR-005 depends on no other FR here and closes real, permanent, total loss to one destination. It was
scheduled last because it is small, which is the wrong reason: a destination that has delivered
nothing since the process started, with `flush()` reporting success and `health()` all zeros, does
not become less urgent for being cheap to fix. AC-1a rides with it for the same reason — a
documentation commit that stops the wrong recipe being copied.

### Phase 1: FR-003, then FR-004 **entire**

The detach comes **first of the sweep work**, not fourth. ~~FR-001's sweep is unsafe without it,
because clearing a buffer the worker was handed by reference destroys the events.~~ — corrected
while building Phase 2: that overstates a hazard as a dependency. The sweep detaches at **its own**
submit site (`events, span.events = span.events, []`), so it needs nothing from FR-004; what is
true is that writing it as `submit(span.events)` then `.clear()` destroys the events, which is a
hazard of one phrasing rather than a missing prerequisite. The order still holds for a different
reason: FR-004 settles who may take a span's buffer and when, and landing it first means the
sweep's detach is written against a settled rule rather than inventing a second one in the same
diff. FR-003 rides with
it — the orphan counter carried from SPEC-034 and already reviewed, together with SPEC-037's
`in_span_lost`, designed as a pair here — and clears the last two `xfail` cells, proving the
harness's `strict=True` mechanism end to end.

**FR-004's closed-span routing ships with its detach, in this phase, and that is a correction.** An
earlier plan split the FR across Phases 1 and 4, which measurably regresses `main` for the three
phases between: today a late append is a race on the flush interval — delivered and correctly
correlated under it, lost over it — while the detach alone loses it at **every** interval, because
the buffer it lands in is one nothing will ever emit. Deferring the second half turns a race into a
certainty and calls the tree shippable. The two halves are one change, and FR-004's
`architecture.md` §5 entry (AC-5) lands with them rather than two phases later — the code and the
sentence describing it ship together.

FR-003 is listed first because FR-004 depends on it: a late append takes the orphan route and is
counted in `orphan_lost`, which FR-003 is what adds.

Note for the builder: `tests/conftest.py`'s `lf` fixture monkeypatches `decorator._flush`, so a
test written against that fixture does not exercise the detach at all.

### Phase 2: FR-001 — the span sweep

The headline, and **all** of FR-001, its `README.md` and `architecture.md` §9 bound (AC-9) included.
AC-4 (nothing destroyed), AC-5 (the backfill survives) and AC-2 (nested spans) come first — those
are the three a wrong implementation passes the rest of the ACs without. AC-1a is Phase 0's.

### Phase 3: FR-002 — the `Sink` flush hook

~~### Phase 4: FR-004's closed-span detection~~ — **dissolved.** Its two halves moved to the phases
that build the code they describe: FR-004 AC-5 to Phase 1, FR-001 AC-9 to Phase 2. A phase holding
only documentation for code that shipped two phases earlier means `main` deliberately carries
undocumented behaviour in between, and it left both criteria claimed by two phases at once.

Every FR and criterion is now claimed by exactly one phase, which is the property the split above
broke and the reason it is recorded here rather than silently re-ordered.

---

## Revision history

Provenance for decisions that were re-opened, superseded or re-scheduled. Kept out of the
requirements above so the builder meets each criterion once (process.md §4).

- **2026-08-30 — acceptance criteria right-sized, and the spec reconciled with a `main` that moved
  under it.** Authored 2026-08-09, it carried 51 criteria averaging 48 words, which `process.md` §4
  now names as its own cautionary example. The criteria are unchanged in substance; the reasoning
  they carried moved into each FR's Description. Reconciled in the same pass, since SPEC-034, 037,
  038, 039 and 042 have all shipped since: the headline defect was **re-measured on `690d2a5`** and
  reproduces identically; `flush()` already returns `FlushResult`, so an Out of Scope bullet
  deferring that to SPEC-034 is struck as shipped; `Health` has gained `inherited_sink`, so the two
  new counters are the **eleventh and twelfth** fields, not the tenth and eleventh; SPEC-034 already
  rewrote `test_health_gains_no_field` to pin names rather than positions, so FR-003 AC-10 adds two
  names instead of rewriting a test; the README has **two** in-span `flush()` sites, not the one the
  draft named; and FR-002's roster of five is now a shipped handoff from SPEC-042 rather than a
  pending one. Seven criteria are new — three splits of criteria that were two tests wearing one
  checkbox (FR-001 AC-11b, FR-003 AC-10a, FR-005 AC-4a) and four new requirements (FR-002 AC-8,
  AC-9, AC-10, AC-10a, below). FR-003 AC-15 was narrowed from "each assertion is
  mutation-tested" to the two increment sites, and two criteria were **strengthened**: FR-003 AC-16
  with the assertion edit that removing the `xfail` markers actually requires, and FR-001 AC-10 with
  a forced concurrency window.
- **2026-08-30 — the header's "two new `flush()` reasons" claim was settled, not inherited.** It
  promised a decision without making one, which §3 calls an Open Question in declarative clothes.
  FR-001 invents none — a sweep that cannot deliver lands on SPEC-034's existing `"abandoned"`,
  `"queue-full"` or `"thread-died"`. FR-002 invents `"sink-flush"` (AC-8).
- **2026-08-30 — three defects found by an implementer asked to build the spec rather than read it,
  all predating this edit.** The phase plan split FR-004 across two phases, which measurably
  regresses `main` in between — a race on the flush interval becomes certain loss — so FR-004 now
  ships entire in Phase 1 and Phase 4 is dissolved, every criterion being claimed by exactly one
  phase. FR-002 AC-1's ordering forces a binding onto SPEC-035's worker-predicate roster, unnoticed
  because this spec had struck SPEC-035 on the grounds "the roster moved to SPEC-039" — true of the
  fork-lock roster, false of the worker-predicate one; it is filed `LIVENESS` (AC-9). And `flush()`
  had no post-close rule at all (AC-10).
- **2026-08-30 — a false negative claim was written into this spec and then removed.** A draft of
  FR-002's Description asserted that none of the roster's four categories describes *performing* a
  drain, and that its docstring refuses a fifth. Both are false against the file cited: `LIVENESS`
  is defined in exactly those words, and the refusal is of one specific proposed category. Kept as a
  record because a wrong claim in a spec has no test to catch it later — the asymmetry `process.md`
  §3 names when it says to spot-check a negative claim before writing it down.
- **Sequencing.** ~~Sequenced after SPEC-035 — ordering only… 035 FR-005's fork lock roster must
  pick up the counter lock FR-003 adds~~ — the roster moved to SPEC-039 with the fork FR; it is
  still derived, and still for this reason (FR-003 AC-5a).
- **FR-003 was SPEC-034's FR-004 in an earlier draft**, reviewed three times there and unchanged in
  substance. That label no longer resolves — 034's FR-004 became "`echo` and `message` stop being
  reserved words" — which is why the FR describes its origin rather than citing it.
- **FR-003 AC-14 was once a hard gate on an unmerged README PR** (`docs/readme-1.0`), written when
  that PR was days old. It has been a draft since 2026-08-07 and its own description records a
  fact-check that found three false claims in it, so it is not a merge waiting to happen. A hard
  gate on an unmerged draft is a spec that cannot start; the criterion now applies to whatever
  `main`'s README says at build time.
- **SPEC-034 FR-008's ordering debt.** Under the original build order this spec would have appended
  to a `NamedTuple`, forcing criteria about preserved indices. Struck with the reversal (SPEC-021's
  rule that a superseded decision is marked, never deleted): 034 built first, so there are no
  indices to prove.
