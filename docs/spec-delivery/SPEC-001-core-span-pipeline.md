# Completed Spec — SPEC-001: Core Span Pipeline

## What was completed?

The first end-to-end slice: decorating a function now emits structured JSON span-start/end
events that correlate by shared IDs and flush to a pluggable sink.

- **`config`** — `configure()` / `get_config()`; `_ensure_sink()` applies the lazy `StdoutSink`
  default (used by both `configure` and the flush path).
- **`ids`** — `new_trace_id` (32hex) / `new_span_id` (16hex) / `new_log_id` (uuid4).
- **`model`** — `Span`; `build_event` / `start_event` / `end_event`; ISO-8601-ms `Z` timestamps,
  monotonic `duration_ms`, precedence merge (config → span → baggage → call fields).
- **`context`** — `contextvars` span stack + baggage (`current_span`/`push_span`/`pop_span`/
  `get_baggage`/`set_baggage`), token/`reset`.
- **`sinks.base` + `sinks.stdout`** — `Sink` Protocol + JSON-lines `StdoutSink`.
- **`decorator`** — sync `@trace` (bare or parameterized); non-swallowing (records `error`,
  re-raises unchanged); direct flush.
- **Façade** — `log_forge.configure` / `log_forge.trace`.

Deviations (all in-session, reversible): zero-config `@trace` falls back to `StdoutSink`
rather than erroring (arch §8); `set_baggage` exists in `context` but is **not** yet on the
façade (deferred to SPEC-002); `test_model.py` / `test_decorator.py` stay skipped because the
pre-written `lf` fixture gates on `info` (SPEC-002) — added `test_model_unit.py` and
`test_decorator_sync.py` for direct coverage now.

## What changed from earlier specs?

None — SPEC-001 is the first build. It supersedes the setup-phase `core.py` (`LogForge`) and
`modules/v1` scaffolding, both deleted. Added CI (`.github/workflows/ci.yml`: ruff/mypy/pytest)
and `pytest pythonpath=["src"]` (the editable `.pth` wasn't honored under pytest — without it
CI would skip every test yet report green).

## Verification

Local gates green — ruff clean, `mypy --strict` clean (9 src files), `pytest` 23 passed / 8
skipped (skips are `info`/async, scheduled for SPEC-002/003). The arch §11 nested demo was run
through a real `StdoutSink`: one shared `trace_id`, child `parent_span_id` == parent `span_id`,
both `status=ok`. Shipped across two PRs (#4 pure units + CI, #6 `@trace` + façade), each
fresh-context reviewed and merged on green CI.
