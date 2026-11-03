# Completed Spec — SPEC-062: The Exit Drain's Retries Fit the Caller's Budget

## What was completed?

- `Worker._emit` times each attempt and, before each retry, declines it when `now + the batch's
  longest attempt + _JOIN_MARGIN` (0.1 s) is past the `shutdown()` deadline. The first attempt of a
  batch is never declined, and `shutdown(timeout=None)` has no deadline to decline against.
- `Worker.claim_stop(deadline)` wins the once-only stop gate and records the deadline in one
  decision under `Worker._lock`; `_lifecycle._shutdown_worker` claims for a live worker before it
  sets `_state._stop`, and `Worker.stop` claims for itself on the direct and late-worker routes.
- A declined retry counts once in `failed_batches` and writes its own `lost` line. The
  `ShutdownTimeout` expiry line adds `N event(s) in flight, counted when the emit returns`, from
  `Worker._in_flight`.
- No public signature change and no knob; the budget is the caller's `timeout`.
- Measured with a 0.5 s failing sink under `shutdown(timeout=1.2)`. At `71e42dd`: 1.21 s, 3
  attempts, `close()` called 0 times, `lost 0 item(s)`. After this spec: 1.01 s, 2 attempts,
  `close()` called once. Pinned by `tests/test_worker_exit_budget.py`.
- Deviations the reviews forced: the closer-grace reserve was replaced by the join margin, because it
  declined retries that would have fitted; the deadline is recorded before the lifecycle's
  `_stop.set()`; gate and record became one decision, because a split gate left the sink open in 1
  run in 60 under concurrent stoppers. Reasoning: `docs/decisions/sink-lifecycle/`.
- Open follow-ups recorded in `architecture.md` §12: queued items announced twice, a second
  `shutdown()` after an expired one returning at once, and `_reinit_after_fork(resume=True)` not
  resetting `_stopped`.

## What changed from earlier specs?

- SPEC-027/SPEC-047: the `architecture.md` §12 item "`NATSSink`'s exit drain can still exceed
  `shutdown()`'s join" is Resolved. SPEC-038's fence is refined, not reversed.
- Invariant 3's *Guarded* line corrected; the predicate roster files `worker.claim_stop` as the
  moment (SPEC-035, SPEC-040).
- `failed_batches` now also counts a declined retry (README and `Health` docstring updated).

## Verification

Ruff, mypy, pytest, spec-lint, docs-lint and docstring-lint run locally before the push; two
fresh-context diff reviews ran before it.
