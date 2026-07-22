# Completed Spec — SPEC-014: Cross-Process Trace Continuation

## What was completed?

Traces now cross a process boundary when the caller carries the context. Shipped in both
directions — a context nobody can read is a context nobody can propagate.

- **`continue_trace(traceparent=None, *, trace_id=None, parent_span_id=None, baggage=None)`**
  (`decorator.py`) — adopts an inbound context into a `contextvars.ContextVar`, and re-parents the
  current span *in place* when that span is a root. Returns `True`/`False`, never raises.
  `traceparent` wins over explicit ids (with a warning); `parent_span_id` may be omitted to join
  the trace as another root.
- **The buffered-event rewrite** (`_reparent_current_span`) — the trap FR-003 called out.
  `build_event` snapshots the ids into each event dict, so re-parenting only the `Span` dataclass
  leaves the already-buffered `span.start` on the old trace: one span emitting its start on trace
  A and its end on trace B. `span_id` is never overwritten.
- **`traceparent` codec** (`ids.py`) — `parse_traceparent` / `format_traceparent` +
  `is_valid_trace_id` / `is_valid_span_id`. Strict: 32/16 *lowercase* hex, all-zero rejected,
  version `ff` rejected, version `00` exactly four fields — but a **higher** version parses its
  first three fields and ignores the rest, per the W3C forward-compatibility rule. Outbound flags
  are hard-coded `01`; an inbound flags byte is parsed and then deliberately not round-tripped,
  because honouring another system's sampling decision would mean dropping spans.
- **Producer side + baggage codec** (`context.py`) — `current_traceparent()`,
  `current_trace_context()`, `current_baggage_header()`, and the W3C `baggage` parse/format pair:
  percent-encoded (so `,` `=` and non-ASCII round-trip), `str()` for non-string values, 8192-byte
  cap. Baggage succeeds or fails independently of the trace context.
- `_open_span` consults the adopted context **only** when no span is open, so a nested call still
  inherits from its in-process parent.

Deviations, both deliberate: `continue_trace` lives in `decorator.py` rather than `api.py` — it is
the other half of `_open_span`'s hierarchy rules — and a *silently absent* `traceparent` (`None`
with no explicit ids) is a no-op rather than a warned rejection, since `event.get("traceparent")`
legitimately yields `None` on every uninstrumented call and warning there would write a line per
invocation. Malformed values still warn.

## What changed from earlier specs?

`_open_span` (SPEC-001) gained the adopted-context branch; behaviour is byte-for-byte unchanged
when `continue_trace()` is never called. `context.py` (SPEC-001) gained a third ContextVar and the
baggage codec; `ids.py` (SPEC-001) gained the codec its own docstring had promised. The README
note added by SPEC-013 — "trace context does not cross a process boundary today" — and
`architecture.md`'s matching Known Constraints entry are amended: what remains is that propagation
is **manual** by design.

## Verification

Full gate green (ruff, mypy `--strict`, pytest — 340 tests, 51 of them new in
`tests/test_trace_continuation.py`). The FR-003 rewrite was mutation-tested: re-parenting only the
dataclass makes `test_reparenting_rewrites_events_already_buffered` fail on a split trace, which
is what that test exists to catch. Not verified here: real multi-process propagation over a live
SQS/HTTP hop — the codec round-trip (`current_traceparent()` → `continue_trace()`) is asserted
in-process instead.
