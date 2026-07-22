# Spec: Baggage on Boundary Events

**ID:** SPEC-015
**Status:** Completed
**Last Updated:** 2026-07-22
**Depends On:** SPEC-002 (`set_baggage`), SPEC-014 (`_reparent_current_span`, the backfill precedent)

## Overview

Baggage is the library's correlation mechanism: a caller sets `request_id` once and every record
from that point on carries it, so one filter returns one unit of work. Today that promise holds for
`info`/`warning`/`error` events and **fails for the two events that matter most** — `span.start` and
`span.end` are built with `baggage={}` hardcoded, so they carry none of it.

`span.end` is the record that carries `duration_ms`, `status` and `error`. So the questions
baggage exists to answer — "how long did this unit of work take", "did it fail" — are exactly the
questions a baggage filter cannot answer. A consumer filtering `fields.request_id == X` gets the
narration of a span and loses its outcome.

This spec makes both boundary events carry baggage, with the documented field precedence intact.

## Scope

### In Scope

- **One mechanism**: both boundary events are completed by a single backfill at span close, so
  each carries the span's **final** baggage.
- The backfill lives in `model.py` as a pure function and is **driven by `decorator.py`**, which
  passes the baggage in. `model.py` must not learn to read context — it "deliberately imports
  neither `context` nor `decorator`" (arch §6), and this spec does not spend that constraint.
- The same treatment on both the sync and async `@trace` paths, and on the error path.
- Tests that pin the precedence order and the failure this spec fixes.

### Out of Scope

- **Changing `build_event`'s precedence rule** (`cfg.defaults` → `span.defaults` → `baggage` →
  per-call `fields`). This spec makes boundary events *obey* it; it does not redefine it.
- **Backfilling mid-span events.** An `info` emitted before `set_baggage` legitimately did not
  have it — that event is a record of a moment, and rewriting it would be a lie. Only the two
  boundary events, which describe the span *as a whole*, are backfilled. FR-003 pins this.
- **Making baggage inheritable across sibling spans** or any change to `contextvars` scoping.
- **A new public API.** No new exported name; `set_baggage` is unchanged.

---

## Functional Requirements

### FR-001: Both boundary events carry the span's final baggage

#### Description:

`@trace` buffers `span.start` before the body runs, so baggage set on the handler's first line does
not exist yet; `span.end` is built at close, when it does. Rather than two mechanisms, one
backfill at close completes both — the span's events are flushed as **one batch at close**, so
this happens before anything is emitted. Nothing is rewritten from a consumer's point of view.

This mirrors `_reparent_current_span` (SPEC-014), which backfills `trace_id` into buffered events
for the same structural reason: `build_event` snapshots span-level values into each event dict, so
a value that changes after an event is buffered must be written back.

#### Acceptance Criteria:

- [ ] A span that calls `set_baggage(request_id="r1")` in its body emits a `span.end` whose
      `fields` contains `request_id="r1"`, alongside `duration_ms` and `status`.
- [ ] The buffered `span.start` event carries it too, merged **before** the batch reaches the
      worker.
- [ ] This holds on the **error** path: a span whose body raises emits a `span.end` with
      `status="error"`, the `error` block, **and** the baggage.
- [ ] Baggage set *after* an inner span closes does not retroactively appear on that inner span's
      events — each span backfills at *its own* close, from the baggage live at that moment.

### FR-002: The backfill is precise about which events and which keys

#### Acceptance Criteria:

- [ ] The backfill is keyed on the boundary-event message constants, not on a positional index
      into `span.events` — a comment records that the position is an implementation detail.
- [ ] Baggage **wins** over `cfg.defaults` and `span.defaults` on a key conflict, matching
      `build_event`'s documented precedence. A test asserts a `span.defaults` key overridden by
      baggage reads as the baggage value on `span.start`.
- [ ] The backfill runs on the sync path, the async path, and the error path — all three route
      through `_close_span`, and a test covers each.
- [ ] A span that sets no baggage emits boundary events whose `fields` are unchanged — no empty
      key, no `None` value, byte-identical to today's output. The backfill returns early on
      empty baggage.
- [ ] `model.py` gains **no import of `context`**: the baggage is a parameter. A test or comment
      records the arch §6 constraint being respected.

### FR-003: Mid-span events are not rewritten

#### Description:

The line this spec draws: boundary events describe the span, so they take its final correlation
context. `info`/`warning`/`error` events describe a moment, so they keep what was true then.

#### Acceptance Criteria:

- [ ] An `info` emitted **before** `set_baggage` does **not** gain the key at close.
- [ ] An `info` emitted **after** `set_baggage` carries it (unchanged behaviour).
- [ ] A per-call field still beats baggage on a key conflict for that event (unchanged
      precedence) — a test pins it, because a naive backfill over all events is exactly the
      change that would break it.
- [ ] A comment at the backfill records why mid-span events are excluded.

### FR-004: The regression cannot come back

#### Acceptance Criteria:

- [ ] A test asserts the **full span batch** for a body that sets baggage and logs once: all three
      events (`span.start`, the `info`, `span.end`) carry `request_id`, in one assertion, so a
      future refactor that fixes one and drops another fails.
- [ ] Existing tests covering the `span.start` / `span.end` shape still pass unmodified, or their
      diff is limited to the added key — the delivery doc records which changed and why.
- [ ] `SPEC-014`'s reparenting tests still pass: backfilling `fields` must not disturb
      `trace_id` / `span_id` / `parent_span_id` on a reparented span.

---

## Data Model

No schema change. The `fields` object on the two boundary events becomes populated where it was
previously empty:

```jsonc
// before — baggage discarded
{"message": "span.end", "fields": {},                        "duration_ms": 412, "status": "ok"}

// after — baggage carried
{"message": "span.end", "fields": {"request_id": "abc-123"}, "duration_ms": 412, "status": "ok"}
```

## API / Interface Contract

```python
# model.py — new internal helper, not exported from the package façade. Pure: the baggage is a
# parameter, so `model` still imports neither `context` nor `decorator` (arch §6).
backfill_baggage(span: Span, baggage: dict[str, object]) -> None

# decorator.py — the one call site; `_close_span` already owns context access.
def _close_span(span: Span, status: str, exc: BaseException | None) -> None:
    span.events.append(end_event(span, status, exc))
    backfill_baggage(span, context.get_baggage())
    _flush(span)
```

`start_event` and `end_event` keep their current signatures.

No public API change: `configure`, `trace`, `set_baggage`, `flush`, `shutdown` are untouched.

## File & Folder Structure

```
src/log_foundry/model.py          # start_event/end_event take live baggage; backfill_baggage()
src/log_foundry/decorator.py      # _close_span backfills before _flush
tests/test_baggage_boundary.py    # new — FR-001..FR-004
```

## Implementation Phases

### Phase 1: Backfill

- `backfill_baggage()` in `model.py`, called from `_close_span` before `_flush` (FR-001, FR-002).
- Confirm the sync, async and error paths all route through `_close_span`.

### Phase 2: Pin the behaviour

- `tests/test_baggage_boundary.py` covering FR-001..FR-004, including the precedence assertions
  and the whole-batch regression test.
- Full gate: `pytest`, `ruff check .`, `mypy`.
- Completion ritual + release `v0.4.0`.
</content>
</invoke>
