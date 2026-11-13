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
- **The cross-thread view** (FR-004, found by the system-frame diff review). The mark and the
  record are per thread, so at `d3950848` another thread of an `os._exit` child could still hand
  the sink again: the child's first log from another thread, and the resumed drain thread through
  a factory wrapper's build. A lock-holding setter hung that thread. `Runtime._fork_offers` (the
  forking thread's live offer list) and `Runtime._fork_inherited` (its length at the fork) are
  rewritten by `_clear_after_fork`, and `offer_stop_signal`'s `finally` lowers the count as that
  thread unwinds. `handing_here` also reads `_fork_offers[:_fork_inherited]` from any thread.
  `_in_handing` is unchanged. Its docstring now argues all four offers that have no `handing`
  block, where the earlier argument named only the retarget.
- **The re-owed debt** (found by the execution review). Another thread of the child could pay the
  fork's signal debt while the forking thread was still inside the setter. It left that sink out
  and cleared the debt, so on the retarget path the sink kept the parent's event after the forking
  thread unwound. Where `offer_stop_signal`'s `finally` lowers `_fork_inherited`, it now also sets
  `_signal_owed`, in one `_state._lock` section. `tests/test_runtime_holder.py`'s lock-site roster
  gains `("offer_stop_signal", "_state._lock")`.
- **A third recorded limit.** A grandchild forked by another thread of a child that is still inside
  a lock-holding setter hangs on its first log. It is not closed, because the same hang occurs with
  no fork inside a hand at all. It is in `architecture.md` §13 and in the decision.
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
  - The cross-thread view and the re-owe are mechanisms added after the plan review, found by
    the diff reviews. The spec's revision history records both.

## What changed from earlier specs?

- The resumed worker's fork-handler hand (SPEC-039, SPEC-068 FR-005) no longer reaches a sink the
  forking thread was offering.
- The `handing_here` decision test (fix after SPEC-079) is wider. `_will_hand`, `_get_worker`, the
  factory wrapper's build and the new skip take the wider reading. The stop-end payment keeps the
  mark alone, so its behaviour is unchanged.
- `tests/conftest.py` resets the four new `Runtime` fields. `tests/test_lifecycle_inventory.py`
  gains their rows and rewords the `_handing_here` and `_building_here` rows.

## Verification

- New suite: `tests/test_fork_mid_hand.py`, covering FR-001 to FR-004 and the parent-side
  `test_a_setter_a_retarget_runs_that_logs_is_not_handed_again`. The cross-thread view adds:
  - the lock-holding re-entry test parametrized over the first log's thread, the forking one or
    another, on all three paths;
  - `test_a_resumed_drain_through_a_factory_wrapper_never_reenters_the_setter`;
  - a fresh-thread read in the grandchild test;
  - `test_an_offer_the_forking_thread_makes_after_the_fork_is_not_shared`;
  - `test_a_forking_thread_that_unwinds_in_the_child_shares_nothing_it_offers_later`.
- Mutation testing was function-scoped, as the implementer reported it. M1 to M10 were each
  killed for the stated reason: the capture, the set, `_at_fork_reinit()`, the offer-record
  read, the resumed worker's skip, a reset of both marks, the exact type, replace rather than
  append, the pop in `finally`, and a resume that never offers. M11, the payment given the widened
  read, survived the full suite. It is equivalent, by the spec's argument that no reachable state
  tells the two readings apart.
- The cross-thread view's three mutants, as the implementer reported them, were each killed:
  - removing the `_fork_offers` read turned six tests red, with a thread hung;
  - removing the shrink turned the unwinding test red, reading `S=False R=True`;
  - removing the `[:_fork_inherited]` slice turned two tests red.
- The re-owe's mutant (removed) turned only the `payer=another-thread` row of the unwinding
  retarget test red.
- **An execution review beyond the usual four.** The cross-thread view was a new mechanism added
  after both diff frames had run, so no review had examined it. One more round was spent on it, in
  the execution frame. That review planted four mutants of the new mechanism, all of which were
  killed: the shrink, `handing_here`'s third term, the slice, and a copied rather than live list. It
  ran the five new tests five times each, under `-n 8` and serially, with no flakes. It found the
  re-owe and the third limit.
- Gates as reported, all exiting 0:
  - at `ede27f5f`: `pytest` (9806 passed, 34 skipped on 3.13.1), and the five fork-related files on
    3.12.12 (704 passed);
  - at `f50b8648`: `pytest` (9807 passed, 34 skipped), with ruff, mypy, spec-lint, docs-lint and
    docstring-lint.
