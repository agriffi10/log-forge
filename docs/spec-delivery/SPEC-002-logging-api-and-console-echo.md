# Completed Spec — SPEC-002: Logging API and Console Echo

## What was completed?

User code can now emit its own structured events *inside* a decorated call, surface a
human-readable line immediately, and attach trace-scoped context — all riding the SPEC-001
pipeline unchanged.

- **`api`** — `debug`/`info`/`warning`/`error`/`critical`, signature
  `(message, *, echo=False, **fields)`; each appends one `build_event` record (stamped with
  current baggage) to the active span. `_log` helper centralizes routing; `set_baggage`
  re-exported here.
- **`console`** — `ConsoleWriter` (default `sys.stderr`): renders `LEVEL   message` lines,
  synchronous, independent of the async `Sink`.
- **Echo** — `echo=True` writes the console line *and* still appends to the span queue
  (additive; never redirects).
- **Orphan handling** — a level call with no active span emits a standalone one-event span
  (fresh `trace_id`, `parent_span_id=None`) flushed directly, so nothing is dropped.
- **Façade** — `__init__` now exports the five emitters + `set_baggage`.

Deviations (all in-session, reversible): the orphan path resolves its sink via `_ensure_sink()`
(not raw `get_config().sink`) so a zero-config orphan log falls back to `StdoutSink` instead of
crashing on `None.emit` — same pattern the decorator uses.

## What changed from earlier specs?

No production code from SPEC-001 changed. Two pre-written tests, previously *skipped* while the
`lf` fixture gated on `info`, began running once `info` shipped and were corrected to match the
authored specs: (1) `test_baggage_flows_to_descendant_logs` narrowed to assert baggage on the
user *log* event, not on SPEC-001 span-boundary events (SPEC-002 leaves those unchanged);
(2) `test_decorator_async.py` gained a self-lifting `skipif` (probes
`iscoroutinefunction(trace(async_fn))`) so the async suite skips until SPEC-003 rather than
failing.

## Verification

Local gates green — ruff clean, `mypy --strict` clean (11 src files), `pytest` 39 passed / 2
skipped (the two skips are the SPEC-003 async tests). New tests: `test_api.py` (level append,
uppercase level, log-id uniqueness, precedence, baggage flow, orphan) and `test_console_echo.py`
(format, echo additivity, default-off, orphan echo). Fresh-context code review run before merge.
