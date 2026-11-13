# Completed Spec — SPEC-083: Fork Mid-Hand: A Child Forked Inside a Stop-Signal Setter Has Its Backoff Cut Short and Never Re-Enters the Setter

## What was completed?

- **The offer record** (FR-001), in `_lifecycle.py`. `offer_stop_signal` pushes `(id(sink),
  event)` onto `Runtime._offering_here` before the probe and pops it in a `finally`. `_offers_here`
  reads it.
- **What a child keeps, and its shutdown** (FR-002, FR-003). `_clear_after_fork` copies the forking
  thread's offered events into `Runtime._stale_signals`. It keeps exact `threading.Event` objects
  only and calls `_at_fork_reinit()` on each. `_shutdown_worker` sets them where it sets
  `_state._stop`, so the wait is cut short on both of its branches. A sink that does not subclass
  `Sink` was the one affected, and its backoff in such a child now ends at the child's `shutdown()`.
- **No setter re-entry** (FR-004). `handing_here` also reads the offer record.
  `Worker._reinit_after_fork` skips a sink it reports. A new `_in_handing` (the `handing` mark
  alone) is what `_pay_marks_after_stop` asks.
- **A parent-process fix the spec did not name.** At `53b68203`, a setter that `Worker.retarget`
  ran and that logged was handed its signal again inside itself (calls=2, depth=2), so a setter
  holding its own lock deadlocked the process. Now it runs once.
- **The record** (FR-005). The docstrings for `_handing_here`, `_building_here` and the new code
  are updated. So are the inventory rows, `architecture.md` §13 (a new entry, with its two limits)
  and the worker rebuild's re-offer sentence in the fork section, and invariants 3 and 12. There is
  a new decision in `docs/decisions/sink-lifecycle/`. The setter and payment decisions are refined
  in place, and `v1.2.0.md` has a *Fixed* entry.
- **Departures from the plan.**
  - FR-001 AC2's test fills the factory wrapper's `_inner` directly, as
    `test_child_sink_slots._build_factory` does.
  - The test sink blocks on its own lock only in a child, and acquires it with a bound in the test
    process.
  - FR-002 AC5's assertion also requires the child's `|exited 0`.
  - One docstring in `tests/test_reentrant_stop_signal.py`
    (`test_a_paid_hand_never_runs_a_setter_inside_itself`) is reworded, with its assertions
    unchanged.
- Commits: `999c4844` (In Progress), `6bffa5de` (Phase 1), `599b3812` (Phase 2), `bc4bef0a`
  (FR-005 docstrings), then the completion commit.

## What changed from earlier specs?

- The resumed worker's fork-handler hand (SPEC-039, SPEC-068 FR-005) no longer reaches a sink the
  forking thread was offering.
- The `handing_here` decision test (fix after SPEC-079) is wider. `_will_hand`, `_get_worker`, the
  factory wrapper's build and the new skip take the wider reading. The stop-end payment keeps the
  mark alone, so its behaviour is unchanged.
- `tests/conftest.py` resets the two new `Runtime` fields. `tests/test_lifecycle_inventory.py`
  gains their rows and rewords the `_handing_here` and `_building_here` rows.

## Verification

- New suite: `tests/test_fork_mid_hand.py`, covering FR-001 to FR-004 and the parent-side
  `test_a_setter_a_retarget_runs_that_logs_is_not_handed_again`.
- Mutation testing was function-scoped, as the implementer reported it. M1 to M10 were each
  killed for the stated reason: the capture, the set, `_at_fork_reinit()`, the offer-record
  read, the resumed worker's skip, a reset of both marks, the exact type, replace rather than
  append, the pop in `finally`, and a resume that never offers. M11, the payment given the widened
  read, survived the full suite. It is equivalent, by the spec's argument that no reachable state
  tells the two readings apart.
- Gates as the implementer reported them, all exiting 0 at `bc4bef0a`: `pytest` (9800 passed, 34
  skipped on 3.13.1), the five fork-related files on 3.12.12 (703 passed), ruff, mypy, spec-lint,
  docs-lint and docstring-lint.
