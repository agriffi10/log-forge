# Completed Spec — SPEC-036: Flush and Buffer Visibility

## What was completed?

`flush()` now reaches every event the caller has emitted, and loss on the paths that had no
counter is visible. Shipped in three PRs, grouped by review frame rather than by phase.

- **FR-001 — `flush()` drains open spans.** `decorator._sweep_open_spans` hands each open span's
  buffer to the worker and leaves the span open; `context._live_span_stack` and `Span.swept` are
  new. The README's own serverless recipe delivered **zero of two events** with every counter
  clean before this. A swept root span makes `continue_trace()` refuse the trace context, or one
  span carries two trace ids.
- **FR-002 — the `Sink` flush hook.** Optional `flush()`, probed by `sinks.base.flush_sink`, with
  the **opposite** failure rule to `read_losses` (it propagates). Implemented by `KafkaSink`,
  `GooglePubSubSink`, `NATSSink`, `SentrySink`, `LoggingSink` and the three wrappers; every other
  sink class records why it needs none, enforced by a new lint. New `FlushResult` reason
  `"sink-flush"`.
- **FR-003 — the synchronous path reports its loss.** `Health.orphan_lost` and
  `Health.in_span_lost`, incremented under a dedicated `decorator._loss_lock`.
- **FR-004 — a task outliving its span.** `_flush` detaches the buffer by swap; `Span.closed` is
  read by `api._log` at append time so a late append takes the orphan route.
- **FR-005 — a dead `MultiSink` child is visible.** Per-child accrual for children that report
  nothing, in events rather than calls.

**Deviations, each recorded in place.** FR-002 AC-3's "count the remainder" is not counted as
loss: those messages are still queued, so booking them would report a loss that has not happened.
`SQLiteSink`/`PostgresSink` implement no hook — both commit inside `emit`, which qualifies what
SPEC-042 measured from the close bodies. FR-002 AC-10a is **owed**: SPEC-032's post-close gate
resolves from `emit`/`send_all` and cannot yet judge a `flush` arm, so the refusals are asserted
per class instead.

## What changed from earlier specs?

- **SPEC-033 FR-006 AC-5** ("No field is added to `Health`") is superseded, struck in place there
  and in `test_health_gains_no_field`, whose *name* is the superseded claim and is kept.
- **SPEC-015's backfill** now also runs at a sweep, so a swept boundary event carries the baggage
  as of the **flush** rather than the close.
- **SPEC-013's refusal** is narrowed: a sweep that finds buffered events builds the worker. An
  empty flush still builds nothing.
- `tests/test_promises.py` has **zero `xfail` cells** — the 2026-08-07 audit harness is clean.

## Verification

Four gates green by exit code on every PR (`ruff`, `mypy`, `pytest` 1760 passed, `spec-lint`), and
`main` watched green after each merge. Every guard was mutation-tested by re-planting; **eight
tests were found vacuous and fixed**, including FR-001 AC-4 — which the spec calls the criterion
most likely to catch a wrong implementation — and which passed against a sweep that did nothing.
Residuals recorded rather than closed, in `architecture.md` §13: the `api._log` build-then-append
window, and `Span.swept`'s single-threaded guarantee.
