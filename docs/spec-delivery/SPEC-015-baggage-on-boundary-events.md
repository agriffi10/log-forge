# Completed Spec — SPEC-015: Baggage on Boundary Events

## What was completed?

- **Baggage now reaches `span.start` and `span.end`.** Both were built with `baggage={}`
  hardcoded, so the two events carrying `duration_ms`, `status` and `error` carried none of the
  correlation context — a consumer filtering on a baggage key got a span's narration and lost its
  outcome.
- `model.backfill_baggage(span, baggage)` — merges the final baggage into the buffered **boundary**
  events, matched on the message constants rather than a position in `span.events`.
- `decorator._close_span` calls it after appending the end event and before `_flush`. All three
  paths (sync, async, error) close through `_close_span`, so one call site covers them.
- The baggage is a **parameter**, not read from `context`: `model` still imports neither `context`
  nor `decorator` (arch §6). `tests/test_baggage_boundary.py` pins that constraint.
- Deliberate deviation from the spec as first drafted: rather than `end_event` taking live baggage
  *and* a separate start-event backfill, one backfill at close covers both. Fewer moving parts, and
  it is what the arch §6 constraint allows.

## What changed from earlier specs?

- **SPEC-001/002 event shape:** boundary events' `fields` are now populated where a span sets
  baggage. Additive — a span that sets none emits byte-identical output, and the backfill returns
  early on empty baggage.
- **SPEC-014 reparenting is untouched.** `backfill_baggage` writes only `fields`; `trace_id`,
  `span_id` and `parent_span_id` are not read or written. The existing continuation tests pass
  unmodified.
- No public API change. `configure`, `trace`, `set_baggage`, `flush`, `shutdown` are unchanged.

## Verification

Full gate green locally: `pytest` 351 passed, `ruff check .` clean, `mypy` clean over 46 files,
`spec-lint` 0 failures.

Both halves of the fix were **mutation-tested rather than trusted green**: deleting the
`backfill_baggage` call fails 7 of the 11 new tests (the 4 that still pass are the
unchanged-behaviour guards, which is correct); inverting the merge to `{**baggage, **fields}` fails
exactly `test_baggage_beats_span_defaults_on_a_boundary_event` and nothing else.
</content>
