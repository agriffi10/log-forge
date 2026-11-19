# Completed Spec — SPEC-097: Lifecycle Commands Take Their Turn, and a Call Made From Inside One Never Waits on It

## What was completed?

- **A sink change is in flight from before its config write** (FR-001): `open_command_swap` and
  `close_command_swap` in `_lifecycle.py`, called from `config._change_sink`; `_Operation` gains
  `installing`, `withheld` and `detach`; `_close_owed` marks a change withheld and `_swap_sink`
  skips its arm on the no-worker branch and after a `declined` retarget.
- **Merge and defer** (FR-002): `_merged_or_deferred` at `_shutdown_worker`'s entry and
  `run_deferred_stops`, run by `_change_sink` on every exit once it releases `config._sink_lock`,
  its closes detached. A stop defers only to `Runtime._deferred_taker`, the change
  `open_command_swap` registers when no running one is, and only while `_on_stack` finds that
  change's frame on its thread's stack (`_live_taker`); the deadlines live on the change's
  `_Operation.deferred`, and `end_command_swap` takes them, found by the frame, in one section of
  `_deferred_lock`, which joined the ownership oracle. `tests/test_change_interrupts.py` raises
  `KeyboardInterrupt` at every line event of the eight functions a change and its deferral run,
  the rows derived at run time.
- **The drain-thread refusal** (FR-003): `_flush_worker` returns `FlushResult(ok=False,
  reason="re-entrant")` on the live worker's drain thread, compared by identity.
- **The hold** (FR-004): six locks became `RLock`s read through `_library_locks()` and
  `in_section()`; `hold`, `_run_held` and the `ParamSpec`-typed `runs_held` on `configure`,
  `flush`, `health`, `shutdown` and `continue_trace`, with level and traced calls running held calls
  inline, on every path out, the exceptional one through `_run_held_unwinding`, which never lets a
  held call's failure replace the exception unwinding; `Runtime._calls_here` (`_CallsHere`) is the
  per-thread state.
- **Re-entry** (FR-005): `spec097_reentry_sweep.py` extended to three workloads;
  `tests/_reentry.py`, the recording lock, installed by `tests/conftest.py` on
  `LOG_FOUNDRY_REENTRY_RECORD`; `tests/test_lock_oracle.py`, the AST roster of every lock.
- **The child** (FR-006): `_clear_after_fork` drops the deferred taker, the thread state and
  `config._sink_holder`. New tests: `test_deferred_stop_bound.py`, `test_reentrant_flush.py`,
  `test_signal_handler_calls.py`, `test_lock_reentry.py`, `test_lock_oracle.py`,
  `test_change_interrupts.py`.
- **Deviations**, each recorded in the spec where it applies: six locks converted, not seven
  (`_sweep_lock` already was one); the ledger lock, `_owned_lock`, `ConsoleWriter._lock` and the
  factory's `_build_lock` stay out of the oracle with reasons; a stop's last close pass was built
  and removed as unreachable; FR-004's stop-row criterion reads `other_closes=0` and FR-003's
  ceiling 0.5 s; each pin left in the phase that turned its cells green.

## What changed from earlier specs?

- **SPEC-054's trade is reversed** for a declined swap a stop overtook (Andrew, 2026-11-16).
- **A `flush()` inside a SPEC-091 sweep hold** returns `"re-entrant"`, not `"abandoned"`.
- **A re-entrant `shutdown()` landing mid-change** no longer drains under it: it is merged,
  deferred or held, and invariant 5's recorded exception for it is struck in place.
- **Pins:** `drain-flush`, `nested-shutdown` and `installed-closed-twice` are removed from
  `tests/model_pins.py`, so the three pins that invariant 3's *Guarded* paragraph excused for these
  items are gone; that paragraph stays in force for the pins left.
- **The record:** five decisions rewritten in place, dated 2026-11-18. Superseded markers were
  added to two more decisions, the SPEC-054 and SPEC-091 delivery docs, and `architecture.md` §13.
  §12 moves five items to *Resolved* and opens ten, six of them residuals the diff reviews found.

**The decision search's other files**, untouched as rewrites (FR-007):

- `pipeline/an-open-spans-events-ship-on-the-workers-interval.md` — amended by the build (the
  `"re-entrant"` line, with its History), and the finding-3 reason marked here.
- `pipeline/every-accepted-event-is-in-exactly-one-term-…` — the ledger's `RLock`, which stays out
  of the oracle as before.
- `sink-delivery/a-file-write-reports-each-event-…` — a signal handler between a write and its
  count's assignment, a file sink's concern.
- `sink-lifecycle/a-closing-sink-refuses-at-once-…` — rejects an `RLock` for a sink's transport
  lock, out of scope.
- `sink-lifecycle/a-process-releases-only-a-transport-…` — `_owned_lock` is re-entrant, unchanged.
- `sink-lifecycle/a-sink-tolerates-concurrent-callers-…` — a sink's own lock, not an `RLock`.
- `sink-lifecycle/a-hand-the-worker-build-declines-…` — the stop-end hand runs after the stop
  leaves `_stops_running`, so a setter's `shutdown()` there is not merged (read from the code).
- `sink-lifecycle/a-sink-factory-runs-under-no-lock-…` — marked: a factory's drain-thread
  `flush()` is now refused rather than waiting on itself.
- `sink-lifecycle/a-stop-never-takes-while-the-current-signal-…` — a measured mutant, history.

Outside the search, `pipeline/an-expired-shutdown-is-final-…` is marked the same way.

## Verification

This ritual ran `spec-lint.sh`, `docs-lint.sh` and the tests that read `architecture.md` §12; the
six gates run on the branch before its push. The evidence re-measured on the build over
`c9068a46` is in the spec's revision history.
