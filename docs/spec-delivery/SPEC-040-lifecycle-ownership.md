# Completed Spec — SPEC-040: Lifecycle Ownership

## What was completed?

The process's delivery lifecycle has one owner. `_lifecycle.py` gains `class _Lifecycle` and the
module-global `_state` holding the seven fields `decorator.py` carried (`_worker`, the lock,
`_atexit_registered`, `_orphan_sink`, `_orphan_closed_sink`, `_orphan_stop`, `_orphan_retired`),
plus the fourteen functions that read them — `_get_worker`, `_register_exit_handler`,
`_note_orphan_emit`, `_rebuild_worker_after_fork`, `_offer_orphan_signal`, `_close_orphan_sink`,
`_shutdown_worker`, `_swap_sink`, `_adopt_declined_swap`, `_flush_live_sink`, `_flush_worker`,
`_delivering_to_an_inherited_sink`, `_worker_health`.

`architecture.md` §9.2's four questions are now four methods — `worker_exists`, `live_worker`,
`worker_owns`, `worker_owns_now` — and a call site *selects* one instead of composing a predicate.
**None takes the lock**: four guards ask with it already held, so a non-reentrant acquire inside a
question would deadlock, and `_get_worker`'s outer check is unlocked on the `@trace` hot path.
The added call costs ~6.6 ns against an ~18.6 µs span.

`decorator.py` falls **1390 → 729 lines** and keeps the decorator, the span machinery and the two
loss counters, reaching the worker only through `_lifecycle._get_worker()`.

Deliberate deviations from the spec:

- **The methods carry a `worker` token** (`worker_exists`, not `exists`). Sentinel derivation in
  the roster is per-tree, so a call site in `decorator.py` naming a question defined in
  `_lifecycle.py` drops out of the roster otherwise — measured 3 sites → 1 filed.
- **`DEFAULT_SHUTDOWN_TIMEOUT` / `DEFAULT_SWAP_TIMEOUT` moved too**, re-exported from `worker.py`.
  Two moved functions bind them as **def-time** defaults, which a function-local import cannot
  supply, and `_lifecycle`'s module-level imports are pinned to three by a test.
- **The loss counters stayed.** Not among the seven, and neither a worker nor an orphan-sink
  global, so FR-001 AC-1 does not reach them; moving them would be scope the spec forbids itself.

## What changed from earlier specs?

- **SPEC-035 FR-002's roster** now walks `_lifecycle.py` **and** `decorator.py`, with a site floor
  (36) and a scope-name collision test that makes its module-less key sound. It derives **38**
  sites, up from 36 — widening the scope immediately filed two sites in SPEC-042's
  `_inheritance_roots`, which had composed the existence question on `decorator._worker` from one
  module away and gone unfiled for two specs.
- **SPEC-042's `_inheritance_roots`** now asks the owner rather than reading another module's
  global. Behaviour unchanged.
- **SPEC-039's fork registration** for the worker rebuild moved into `_lifecycle.py`, where the
  ordering against `_mark_inherited` is now two adjacent lines rather than an import-order
  accident. Pinned by a new test.
- **The sibling release roster** (SPEC-042) needed its receiver resolver extended to follow a
  module-global instance into its class, and three hand-copied call-shape checks unified — both
  lifecycle closers now call `release(...)` unqualified, which two of those checks did not match.
- Internal import sources moved in `__init__.py`, `config.py` and `api.py`. No public signature
  changed.

## What it found and did not fix

The execution review built harnesses rather than reading, and turned up **six lifecycle races**.
Every one reproduces byte-identically on the pre-SPEC-040 tree, so none is this spec's doing —
and its Out of Scope forbids fixing behaviour, precisely so a refactor cannot smuggle one in. All
six are recorded in `architecture.md` §13 with the harness that reproduces each, and **they want
their own spec**. The worst is a `shutdown()` racing a first `@trace`: it reads the existence
question unlocked, takes the no-worker branch, and returns having stopped no drain thread and
closed no sink, while `health()` reports `retired=True` and later logs still deliver with
`submitted_after_shutdown=0`. `atexit` recovers it at process exit — but a frozen serverless
container never exits, which is the deployment `flush()`/`shutdown()` exist for. Confirmed
independently, not taken on the reviewer's word.

## Verification

All four gates green locally (`ruff`, `mypy --strict`, 1821 passed / 5 skipped exit 0,
`spec-lint`). Behaviour preservation was proved mechanically rather than argued: a normalised AST
comparison of each moved function against its pre-move original found **12 of 13 identical**, the
one difference being the deliberate `refresh_stop_signal()` extraction the fork shape lint
required, and **all four questions identical** as methods. `pytest --collect-only` names differ
from the pre-spec baseline by **+3 / −0** (two FR-004 guards, one handler-order guard). The public
surface was dumped before and after: every `__all__` identical, the only delta being incidental
re-exports dropped from the internal `decorator` module. A five-mutant sweep in Phase 1 confirmed
each question is behaviourally distinguished; **one site is not** — `_flush_live_sink`, recorded
in its roster row per FR-003 AC-5.
