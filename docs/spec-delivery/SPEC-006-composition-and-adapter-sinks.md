# Completed Spec — SPEC-006: Composition and Adapter Sinks

## What was completed?

Four **zero-dependency** sinks that combine other sinks or adapt user code, each implementing the
SPEC-001 `Sink` protocol (`emit`/`close`), `isinstance`-checkable, and nesting arbitrarily with each
other and any existing sink (`StdoutSink`, `SQSSink`, …). Together a callback + fan-out reach almost
any destination and tee to several at once without writing a `Sink` subclass.

- **`sinks.callback`** — `CallbackSink(fn, *, on_close=None)`: `emit` hands the batch to `fn`
  unchanged; exceptions propagate (worker handles them); `close` calls `on_close` once if given
  (FR-001).
- **`sinks.multi`** — `MultiSink(*sinks)`: sequential fan-out in construction order; a child whose
  `emit`/`close` raises is isolated — counted on `failed`, logged to stderr, never propagated (the
  §7-sanctioned isolation boundary catching `Exception`, so `KeyboardInterrupt`/`SystemExit` still
  propagate); empty `MultiSink()` is a genuine no-op (FR-002).
- **`sinks.filtering`** — `FilteringSink(inner, *, predicate=None, min_level=None)`: forward only the
  events clearing the predicate **and** `min_level`; unknown/missing level fails open; no empty-batch
  emit; `close` delegates to `inner` (FR-003).
- **`sinks.transform`** — `TransformSink(inner, fn)`: forward a new list of `fn(event)` results,
  `None` drops an event, the caller's batch/dicts are never mutated in place; no empty-batch emit
  (FR-004).
- Arbitrary nesting + close cascade verified (FR-005).

**Hardening added during build** (beyond the original Draft): `FilteringSink` level comparison is
case-insensitive and an invalid `min_level` raises `ValueError` at construction (fail fast); FR-005's
close-once guarantee was scoped to "each leaf appearing once is closed once; a shared leaf is closed
once per position, so `close()` must be idempotent."

## What changed from earlier specs?

Nothing — purely additive (four new modules + `tests/test_sinks_compose.py`). No change to the
`Sink` protocol, the worker, the batching contract, or any earlier module. No new dependency or extra
(core stays dependency-free). Uses the per-module import convention (no `sinks/__init__` re-exports).

## Verification

Local gates green — `ruff check` clean, `ruff format` clean on changed files, `mypy --strict` clean
(17 src files), `pytest` **100 passed**. Tests use pure in-process doubles (no I/O): delegation/
propagation/`on_close`, ordered fan-out, child-failure isolation + stderr log + close-all, predicate/
rank/case-insensitivity/fail-open/both-required/no-empty-emit, transform drop + non-mutation, nested
routing, and the close-once cascade. Fresh-context code review run before merge (no correctness bugs;
review nits addressed).
