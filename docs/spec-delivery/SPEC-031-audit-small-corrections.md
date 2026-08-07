# Completed Spec — SPEC-031: Audit Small Corrections

Shipped in two PRs, as the spec's Phase 4 asks: FR-001..FR-005 (#121), FR-006 alone (#122).

## What was completed?

- **FR-001 — `RotatingFileSink` rotates on a monotonic clock.** `_schedule_next` and
  `_should_rotate` both read `time.monotonic()`; a backward wall-clock step no longer defers
  rotation. AC-4 (preserve wall-clock filename labels) is **vacuous here** and recorded as such:
  backups are numbered, never timestamped, so no filename derives from a clock.
- **FR-002 — UDP reaches IPv6.** `_make_udp` gained a `host` parameter and resolves the address
  family via `getaddrinfo`, once per socket creation. 14 test seam sites updated; no default was
  given to the parameter, which would have hidden it from every real reader.
- **FR-003 — three false doc claims corrected.** `architecture.md` §12 and §6.1 both said console
  echo goes to stdout (it has been stderr since SPEC-002); the construction-time stream binding is
  now documented on `ConsoleWriter` and `StdoutSink`. **A fourth was corrected alongside them**:
  §6.1's "Configurable" bullet described a global echo destination/format/`echo_level` surface that
  was never built and that §12 had already declined. Same false-claim shape, same paragraph. The
  `api.py:53` comment the spec names was **already gone** — SPEC-025 rewrote that docstring — so the
  settled decision is now stated there with a pointer to §12 instead of a comment being removed.
- **FR-004 — two micro-inconsistencies.** `_Coercer` caches the integer ceiling in `__init__`
  (`__slots__` gained `_int_ceiling`); `model.py`'s function-local `get_config`/`new_log_id` moved
  to module scope in `build_event`, `end_event` and `backfill_baggage` — there was no cycle to dodge.
- **FR-005 — recorded, not changed.** `Worker._release_waiters`'s use of `queue.Queue`'s private
  `.mutex`/`.queue` is now an `architecture.md` §13 constraint, with a test against a mixed queue so
  a CPython change fails loudly instead of silently timing out flush waiters.
- **FR-006 — a process that never created a worker closes its sink.** New in `decorator.py`:
  `_note_orphan_emit` (armed from `api._log`'s orphan branch, **after** the emit returns),
  `_close_orphan_sink`, `_register_exit_handler`, and three module flags. `health().retired` is
  synthesized without a worker. `conftest._reset_worker` clears the flags.

**The design choice worth knowing:** the spec's Data Model suggested avoiding `_atexit_registered`.
What ships instead **unifies the handler** — `_shutdown_worker` covers both paths, so one
registration under the existing flag is correct. That dodges both traps the FR names at once: no
LIFO double-close, and a mixed process keeps its worker drain. The once-only property lives on the
*close*, as the FR requires, not on the registration. A live worker still owns the close.

**Out of scope, and still open:** `configure(sink=A)` → `info()` → `configure(sink=B)` → `info()`
leaves **A** unclosed with `incomplete_swaps` at zero, because `_swap_sink` returns early on a null
worker. Recorded in `architecture.md` §13 beside the struck-through FR-006 entry. It needs its own
spec.

## What changed from earlier specs?

- `_make_udp()` → `_make_udp(host)`. Module-private test seam; both callers are in this repo.
- `Health.retired` is no longer purely a `Worker` field — `decorator._worker_health` synthesizes it
  when there is no worker (SPEC-030 FR-001's meaning is unchanged; only its reach widened).
  `submitted_after_shutdown` is deliberately **not** synthesized: SPEC-030 defines it as queued
  where nothing drains, and this path is refused-and-announced.
- `architecture.md` §13's "a process that only ever used the orphan path never closes its sink"
  is struck through and marked closed (SPEC-021's rule).
- `api` now imports `decorator`. New edge, no cycle — `decorator` does not import `api`.

## Verification

1082 tests pass on 3.12 and 3.13; `ruff`, `mypy --strict`, `spec-lint` clean. 33 tests added, none
removed (verified by diffing `pytest --collect-only` names, not pass counts).

Every new assertion was mutation-checked against reverted source. FR-006's battery ran four
mutants: the full revert (kills 8), **trap A verbatim** — arming via `_atexit_registered` so
`_get_worker` skips registration — which the whole 1082-test suite catches in exactly **one** test
(`test_a_mixed_process_at_interpreter_exit_closes_once_and_still_drains[orphan-then-span]`,
the criterion the spec said the rest of the set does not imply), **trap B** (a second `atexit`
handler, caught only by the subprocess tests that assert the close *count*), and arming from
`configure()` instead of a landed emit (caught only by the AC-8 test). Two FR-002 tests survive
their mutation by design — they are AC-2/AC-3 regression guards, not proof of the fix.

The IPv6 delivery tests bind a real `::1` loopback receiver and skip where the host has no IPv6
stack. Interpreter-exit behaviour is exercised in subprocesses, since `atexit` does not run under
pytest and `atexit._run_exitfuncs()` would fire the whole registry.
