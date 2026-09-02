# Completed Spec — SPEC-050: Lifecycle Residue

## What was completed?

Five findings from a pre-1.0 audit dated 2026-09-02, each reproduced by running it against
`eb80099` before the spec was written and re-run against the fix.

- **FR-001** — `Worker.shutdown`'s expiry branch now calls `_release_waiters()`. A
  `flush(timeout=None)` parked behind a stuck sink used to wait forever on a drain that call had
  just given up on; it now returns `FlushResult(ok=False, reason="abandoned")`.
- **FR-002** — an in-flight inline `close()` is recorded in a slot holding an `Event`, on the
  worker path (`Worker._closing`, drained in `_close_if_owed`) and the orphan path
  (`_lifecycle._orphan_closing`, in `_close_orphan_sink`). A caller that did not claim the close
  waits for it, capped at `DEFAULT_CLOSER_GRACE`. Before: `atexit` returned through a running
  close and process exit killed it — twelve buffered events lost at 0.31 s, nothing on stderr.
- **FR-003** — `decorator._flush` resolves the worker *before* detaching the span's buffer, so
  `_end` can count `len(span.events)` into `in_span_lost` when the close is absorbed. A process
  that cannot start a drain thread reported `in_span_lost=0` over total loss.
- **FR-004** — a sink stranded by an unconfirmed `configure(sink=...)` is recorded in
  `Worker._unclosed_swaps` and closed, detached, by the first `shutdown()` that finds the drain
  thread ended. New: `Worker._FORK_SKIP`, `Worker._discard_owed_swap`, `_lifecycle.discharge_owed`.
- **FR-005** — `Worker.submit` re-reads `_shutdown_done` after its put, so a submission that
  raced the final drain reaches `submitted_after_shutdown` and its stderr line.

**Deviations.** The spec listed three prune sites for the owed-swap record; the confirmed-swap one
survived every mutant because a recorded sink is never the live sink, so it was removed and the
invariant stated where it stood. The spec said the orphan re-arm route was guarded but not
asserted because no reachable sequence was found; building it showed it *is* reachable
(`A.closes == 2`), so the discharge is atomic with taking the record and the route is tested.

**Rejected, with the reasoning in `decisions.md`:** `health().retired` reading `True` for a worker
built after an orphan-only `shutdown()`. The field documents an action the caller took, the
alert idiom is the *pair*, and the fresh worker's events are not lost silently.

## What changed from earlier specs?

- **SPEC-030's "the previous sink is left open"** now means *for now*, not forever. Superseded in
  place at `architecture.md` §7, `docs/decisions.md`, and four `worker.py` docstrings including the
  public `Health.incomplete_swaps`. **Owed elsewhere:** `README.md:253` and `README.md:1009` carry
  the same claim and belong to the release-surface session.
- **SPEC-031's `_release_waiters`** gains a third caller.
  `test_an_expired_shutdown_leaves_the_sentinel_for_the_live_thread` asserted the opposite and is
  superseded in place: the events still reach the sink, so what the sweep trades is a verdict.
- **`Health.in_span_lost`** has two causes now, not one. Its "always means the data" clause is
  struck in place, here and in `decorator._note_in_span_loss`.
- `_close_if_owed` takes a `deadline` parameter; one test monkeypatched it with a zero-arg lambda.

## Verification

All five gates green locally at exit 0 — `ruff`, `mypy --strict`, `pytest`, `spec-lint`,
`docs-lint` — plus `docs-lint-test` (the linter was not touched, so this was a check, not a
requirement). Twenty-six new tests. Every guard whose failure would be silent was mutation-tested
one at a time, restored by copying from the scratchpad rather than `git checkout --`, with
`log_foundry.__file__` confirmed to resolve to this worktree: sixteen mutants planted, fifteen
killed, one documented as equivalent (reading `_orphan_closing` under the lock versus the value
captured under the same lock). Three of the fifteen were killed only after the test that named
them was rewritten to reach the guard.
