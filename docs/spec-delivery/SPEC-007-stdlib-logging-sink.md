# Completed Spec — SPEC-007: Stdlib Logging Bridge Sink

## What was completed?

A zero-dependency `LoggingSink` that bridges log-forge into Python's standard `logging`: each event
dict becomes one `logging.LogRecord` dispatched through a configurable logger, handing users their
whole existing `logging` setup (handlers, `logging.config`, third-party handlers) for free.

- **`sinks.logging_sink`** (new) — `LoggingSink(logger=None, *, default_level="INFO")` implementing
  the SPEC-001 `Sink` protocol, `isinstance`-checkable. Module named `logging_sink` so it never
  shadows the stdlib `logging` module.
  - **Dispatch** — one `LogRecord` per event via `logger.handle(record)`, in batch order; default
    target `logging.getLogger("log_forge")` (FR-001).
  - **Level mapping** — name → numeric, case-insensitive; unknown/missing → `default_level`;
    `levelno`/`levelname` reflect the mapping (FR-002).
  - **Verbatim message** — `msg=message`, `args=()`, so a literal `%`/`%s`/`%(name)s` never
    interpolates (FR-004).
  - **Structured fields (reserved-attr safe)** — identity keys + each `fields` entry attach as flat
    record attributes, skipping any key that collides with a reserved `LogRecord` attr, an identity
    key, or `"fields"` itself; the full `fields` dict is always attached as `record.fields` so a
    skipped collision is never lost (flat + nested). Attaching never raises (FR-003).
  - **`close`** — no-op; never shuts down or reconfigures the user's logging (FR-005).

**Deviation from the Draft:** FR-003 was hardened during build to the explicit *flat + nested*
design (was "namespaced or skipped"); a fresh-context review then caught that a field literally
named `"fields"` clobbered `record.fields`, fixed by skipping `"fields"` in the flat loop (+ a
regression test).

## What changed from earlier specs?

Nothing — purely additive (one new module + its tests). No change to the `Sink` protocol, the
worker, the batching contract, or any earlier module; no new dependency or extra.

## Verification

Local gates green — `ruff check` + `ruff format` clean, `mypy --strict` clean (18 src files),
`pytest` **113 passed**. `test_sinks_logging.py` uses a capturing handler on isolated loggers (no
network): dispatch order, level mapping (incl. unknown→default and case-insensitivity), literal-`%`
message, close-noop, flat+nested field attach, reserved- and `"fields"`-collision safety, and a
`logging.Formatter` read-back. Fresh-context code review run before merge (one FR-003 losslessness
defect found and fixed).
