# Completed Spec — SPEC-034: The Public API Freeze

## What was completed?

- **FR-001/002** — `SQSSink(queue_url, *, client)`; `SentrySink(client=)` with no alias.
- **FR-003** — `Config` is frozen, `configure()` rebinds the module global, `get_config()`
  returns a copy. `config._live_config()` is the internal read.
- **FR-004** — `fields=` escape hatch on all five emitters, merged under `**kv`.
- **FR-005** — `Sink`, `Config`, `read_losses`, `get_baggage` exported. `Sink.emit`/`close` are
  `@abstractmethod`.
- **FR-006** — `stop_signal` → `log_foundry_stop_signal`, documented in `sinks/base.py`.
- **FR-007** — `FlushResult` / `ContinueResult` in new `src/log_foundry/results.py`.
- **FR-008** — `Health` and `SinkLosses` are frozen dataclasses.
- `tests/test_public_surface.py` — 60 tests. Suite 1191 → 1265, none removed (verified by
  diffing `--collect-only` names across the 62-site conversion, not by counting passes).

## What changed that a later spec should know?

- **`Health` and `SinkLosses` no longer support `len()`, indexing or unpacking.** Appending a
  field is now a plain append — no index proof, no `len()` migration. This is what takes
  SPEC-036 FR-003 and SPEC-037 AC-5c off the critical path to 1.0.
- **`flush()` and `continue_trace()` return objects, not bools.** `if flush():` is unchanged;
  `flush() is True` cannot hold. **`Worker.flush` carries the type too** — the five outcomes are
  distinguishable only there. A new `reason` is additive by design; SPEC-036 adds two and
  SPEC-036 FR-001 AC-11a a third.
- **`_config_lock` is new.** Ordering is one-way: `decorator._worker_lock` → `_config_lock`,
  never the reverse. A test pins what may run under it.
- **`config._live_config()` and `context._live_baggage()` are the internal reads.** Public
  accessors copy; the per-event path must not. Both are pinned structurally.

## Anything deliberately left open?

`get_config().defaults` and `get_baggage()` are **one-level** copies; nested values are shared.
Deep-copying arbitrary caller objects inside an accessor that must not raise trades a narrow
sharing bound for a wide new failure. Stated in both docstrings and pinned by tests.

## Evidence

Four review frames, each finding what the previous could not: diff-scoped (2 ACs ticked but not
built), library-first (**an incomplete `Sink` subclass silently swallowed every event**),
concurrency (**freezing `Config` let a concurrent `info()` permanently revert `configure()` —
268/2000 trials**), and caller-experience (**`_merge` raised into the caller on all four paths**).
Three of the four blocking findings were regressions this spec introduced, none visible in its
own diff.

Six harnesses of mine passed against the defect they were built to catch, each exposed only by
mutating the code under them: a race harness reporting 0 reversions with the defect present, an
8-thread test whose fast path hid it, an emitter roster that searched instead of classifying, an
echo assertion `capsys` could not see, a README parser defeated by an inline comment, and a
`fixpoint` test whose snippet ordering made one pass sufficient.
