# Completed Spec — SPEC-035: Shutdown and Fork Lifecycle

## What was completed?

- **FR-001** — the stop signal is offered on `Worker.draining`, a new predicate: ownership
  conjoined with the moment. The spec's own AC-1 prescribed bare ownership and is struck in
  place; a shipped SPEC-033 test proves that would have re-broken the guarantee this FR cites.
  Neither category alone is right at that site.
- **FR-003** — `Worker.swap_sink` returns whether it adopted the sink; `_swap_sink` re-homes one
  it declined (`_adopt_declined_swap`). `False` means *declined only*, never "unconfirmed drain".
- **FR-004** — `shutdown()`'s idempotent path waits on `_drain_settled`, an event set both where
  the drain loop stops and where a shutdown abandons it, bounded by the caller's own budget.
- **FR-002** — `tests/test_worker_predicate_roster.py`: sixteen sites, four categories, derived
  from `decorator.py`'s AST. `architecture.md` §9.2 states the rule.
- **FR-006** — `architecture.md` §13 records audit C5, the `_diag` write under `_worker_lock`.

**Deviation:** FR-005 (`os.fork()`) is **not** in this spec. It moved to
[SPEC-039](../specs/SPEC-039-fork-lifecycle.md) once everything else had shipped — the largest
piece here, the only one needing a new module, and the only one that is not a shutdown path. It is
struck in place in the spec with the reasoning, and its four prepared measurements moved with it.

## What changed that a later spec should know?

- **`Worker.draining` is a fourth question, not a rephrasing of an existing one.** Ownership alone
  skips the signal offer for a worker whose shutdown has *finished*, leaving a live sink holding a
  set event that can never clear — SPEC-033 FR-004's tight retry loop. Liveness alone un-skips for
  the whole drain and hands the drain thread an unset event nobody will set. Both measured.
- **A new worker guard in `decorator.py` must be classified** or the roster fails. Its vocabulary
  is derived, not listed: a function whose return value names the worker is itself a worker name,
  to a fixpoint, so `worker = _snapshot()` cannot hide a rebinding behind a neutral name.
- **The roster's scope is `decorator.py` only**, and the orphan path's sink comparisons are
  deliberately outside it. SPEC-040 is where that boundary is revisited.
- **`_drain_settled`, not `_drain_finished`.** `flush()` and the sentinel gate read the latter as
  "the loop stopped reading the queue"; widening it hangs a waiter that committed before the first
  caller abandoned (measured 20.01 s against a 20 s budget, indefinite with `timeout=None`).

## Anything deliberately left open?

Audit C5 is recorded in §13 rather than fixed — an error path only, and the fix spreads one
diagnostic decision across two functions at three sites. `Worker.submit` is named there as the
counter-example.

## Verification

1,199 passed / 8 xfailed, `ruff`, `mypy --strict`, `spec-lint` green; CI green on 3.12 and 3.13.
FR-002 took eleven review rounds; the last four rotated frame (library-first, then attacker) and
each found something the six same-frame rounds before them could not. Three mutations pin the
accessor derivation, including the round-11 attack applied to `decorator.py` itself. The
fixpoint test asserts **both** source orders — with the accessor defined first, one pass already
reaches the wrapper, and the first version of that test passed the no-fixpoint mutant.
