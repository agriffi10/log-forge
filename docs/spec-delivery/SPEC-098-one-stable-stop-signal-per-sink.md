# Completed Spec — SPEC-098: One Stable Stop Signal per Sink, Handed Once per Installation

## What was completed?

- **One event per sink** (FR-001): `Runtime._signals`, keyed by `id(sink)` and holding the sink
  weakly where it can, its one `threading.Event` and whether a setter given that event has
  returned; `offer_stop_signal` assigns that event and marks the sink handed at the setter's
  return; `offer_once` is the one offer site per path, lock-free for a handed sink. Dead entries
  are pruned when a sink gets its first entry.
- **Set by every stop, cleared by a reinstall** (FR-002): `_set_every_signal` in
  `_shutdown_worker`'s entry section walks `Runtime._signal_events`; `_new_signal` makes an event
  set while a stop runs; `reinstall_signals`, called by `config._change_sink` after the stamp,
  clears the graph's events in place and unhands them, its check of `_stops_running` and its clear
  in one `_owned_lock` section.
- **The child** (FR-003): `_seed_signals_after_fork`, the first step of `_walk_after_fork`,
  re-initialises every live event in place and enters it in the walk's memo; both new fields are
  in `Runtime._FORK_SKIP`. No setter runs in the repair.
- **The protocol** (FR-004): the README's protocol section and its 1.0.0 note carry the four
  phrases; invariant 3, `architecture.md` and the inventory say a sink waits on its own event.
- **The plan's departures, each ruled before the build:** D-1, SPEC-097's leftovers
  (`_await_swap_decisions`, `_shutdown_running`, `_late_worker`, `_sink_lock`) stay for a later
  spec and `_take_declines` stays with its docstring rewritten; D-2, `Runtime.in_flight` lost its
  one reader and is deleted; D-3, `flush()` offers each pending sink, and `Worker.retarget`'s
  same-sink return offers too; D-4, the seed marks handed every sink the forking thread was being
  offered, keeping SPEC-083's "no setter re-entered in the child". The plan review's M-1 added
  `offer_once` on the drain thread before each batch.
- **Two in-scope changes the plan did not name.** The **owed-record snapshot**: `_close_owed`'s
  take and `_swap_sink`'s `in_use` and `joins` now iterate `list(...)` of `_state._closes.owed`,
  because once the deletion removed the earlier first entry of the close registry's section, the
  re-entry sweep's handler fired inside `held()`, armed a sink, and `configure()` raised
  "dictionary changed size during iteration" (pinned by
  `test_a_sink_armed_inside_held_does_not_break_a_configure`). **`tests/test_lock_held_discharges.py`
  is deleted whole**: every check in it held the shape that kept a discharge under `_state._lock`
  from owing a deferred hand, which no longer exists, and its `put-back-discarded` check would
  refuse the now-correct bare `put_back` call.

## What changed from earlier specs?

- **SPEC-027's hand:** a sink is handed its own event once per installation, not the worker's at
  every refresh. **SPEC-033 FR-004 AC-6**'s "the same sink logged to again" is struck in place: a
  delivery after `shutdown()` with no `configure()` finds the event set. **SPEC-083**'s offer record
  survives; its inherited-offer mechanism is replaced by the seed.
- **Deleted from `src/`:** `_hand_out`, `_settle_signal`, `_SETTLE_ROUNDS`,
  `Runtime._hands_running`, `_signal_owed`, `_Close.hand_owed` with `defer_hand`, `marked` and
  `unmark`, `_hand_after_close`, `_pay_marks_after_stop`, `_pay_a_mark_the_stop_missed`,
  `_hand_retargeted`, `_stale_signals`, `_fork_offers`, `_fork_inherited`, `_will_hand`,
  `_may_replace`, `_owed_candidates`, `_refreshes`, `_open_hand`, `handing_here`, `_in_handing`,
  `handing`, `_mid_hand`, `_arm_and_hand`, `Runtime._handing_here` with `payment_owed`,
  `Runtime.in_flight` (D-2) and the factory wrapper's settle loop. **Kept, with their readers:**
  `refresh_stop_signal` (the worker's own event, at `Worker.__init__` and the fork resume),
  `_take_declines` (a stop first stops a worker built since its entry), `rearm_if_released` (after
  a first hand, in `_note_orphan_emit` and the factory's `_hand`), `_CloseRegistry.in_flight`
  (read by `Runtime.held`) and `_offers_here` / `_offering_here` (the offer record, read by the
  seed).
- **The decision search** (FR-005's grep) returns 21 files at `463894aa`. Rewritten in place, each
  with a History line of 2026-11-20: in `sink-lifecycle/`, *A stop-signal setter runs under no
  lock…*, *A child forked inside a stop-signal setter…*, *A stop never takes while…* (D-1) and *A
  sink's wait is bounded…*; in `pipeline/`, *A worker guard asks one of three questions…* (D-2) and
  *Only the forked child is repaired…*. Refined with a marker: `pipeline/` *Lifecycle state has one
  holder…* and *One lifecycle owner…*, and, outside the search, *A swap keeps the sinks…*.
  Superseded with their reasoning kept: *A hand the worker build declines…* (its *Rejected:
  declining without paying*, for a late delivery) and *A hand while a stop has a take-ahead…*.
  Marked, one sentence: *A sink handoff is owned…*, whose "the stop event is replaced, never
  cleared" is superseded for a sink's own event, which a `configure()` clears in place (first
  diff review, m-5); the rest of it, the record holding the sink object, stands.
  Left, with why: *A shutdown waits for a swap in flight…* and `public-api/` *Concurrent sink
  changes…* (the command path is unchanged); both `sink-delivery/` files (the engine reads the
  sink's attribute, as before); *A driver's liveness flag…* (no stop signal read); *A shutdown
  shortens a wait…* and *The exit drain declines…* (the worker's own event); *A process releases
  only…* (ownership only); *A sink factory runs under no lock…* (its rejection is about the build wait); *A sink factory
  builds…* ("hands the stop signal to what it built" holds, as its own event).
- **The new decision:** `docs/decisions/sink-lifecycle/a-sink-holds-one-stop-signal-of-its-own-handed-once-per-installation-set-by-every-stop-and-cleared-by-a-configure-that-installs-it-with-no-stop-running.md`.
- **`architecture.md`:** §12 moves three items to *Resolved* and opens five, unfixed: five sink
  modules' and two `_fork.py` docstrings still name the worker's event; a reinstall's clear can cut
  the interruption of a close still running from an expired stop (reasoned, not measured); a child
  forked while a different parent thread is inside a sink's first setter, or between a
  `configure()`'s clear and its offer (measured), can run that setter in its repair; two
  `_swap_sink` comprehensions read the live owed record, with no call inside to reach; and a sink a
  `sink_factory` returns again after a `shutdown()` keeps the set event, a change from `c67931cd`
  (`set=True waited=[0.0]` against `set=False waited=[1.01]`), also a 1.2.1 Known gap. §13's
  SPEC-083 item carries a superseded marker.

**The tests the deletion moved.** Collected on `463894aa` and on the branch, 80 ids vanished and
66 were added, 45 of them in the new `tests/test_stable_stop_signal.py` (after both diff reviews). By what each asserted:

*A setter ran again, now one hand per installation:* the `orphan-shutdown` row of
`test_a_setter_reentering_the_library_returns` and the resumed-worker child test in
`test_fork_mid_hand.py` are rewritten; `test_a_paid_hand_never_runs_a_setter_inside_itself`, the
retarget-declined payment and the four build-declined payments (`…build declined…`, `…put back
pays…`, `…refused release pays…`, `…no stop running hands at once…`) are renamed to one-hand
outcomes.

*An internal of the deleted machinery, deleted with it:* in `test_reentrant_stop_signal.py`, the
hand-token, settle, stale-event, debt, deferral, mark-payment and wrapper-mid-build tests, and 20
rows of `test_a_setter_reentering_the_library_returns` whose hand sites are gone; in
`test_fork_mid_hand.py`, the six inherited-offer tests; `test_the_offer_does_not_consult_liveness`;
and `tests/test_lock_held_discharges.py` (20 ids).

*A roster or inventory reading the new state, re-derived:* `test_runtime_holder.py`,
`test_lifecycle_inventory.py`, `test_worker_predicate_roster.py` (21 rows removed, floor 90),
`test_lifecycle_races.py`, `test_lifecycle_transitions.py`, `test_lock_reentry.py`,
`test_sink_ownership.py`, `test_fork_lifecycle.py` (a declared seeded exemption and a test of it),
`test_fork_roster.py` (four new refusal rows) and one shifted id in `test_numeric_door_roster.py`.

*A sink holding the worker's event, now its own:* `test_config.py`, `test_sink_factory.py`,
`test_public_surface.py`, `test_sink_retry.py`, `test_shutdown_expiry_bookkeeping.py`,
`test_orphan_sink_handoff.py`, `test_shutdown_lifecycle.py`, `test_fork_child_state.py`,
`test_fork_lifecycle.py`, `test_fork_roster.py`, the offer-record and child tests of
`test_fork_mid_hand.py` (FR-003 AC-4's held-condition test rewritten, the lock-holding forker's
`calls=0` kept as D-4's proof) and, in `test_reentrant_stop_signal.py`, the take-ahead hand's
`orphan-late` row, four rows of the stop that never closes, the no-stop retarget and the worker
build's hand.

*A wait after a stop, now FR-002's outcome:* `test_a_sink_emitted_to_after_shutdown_still_backs_off`
in three rows (newly configured and reconfigured wait, logged again does not); the retired-worker,
retired-child and slow-setter swap tests; four `test_reentrant_stop_signal.py` tests renamed to the
sink's own set event, the stops-tail one asserting `waited_late=[0.0]` then
`waited_reconfigured=[0.3]`; and the six mark-payment rows the spec named, deleted with the seams
they were built from, their `[0.3]` outcome held by that stops-tail test.

**Readings, `463894aa` (the code of `c67931cd`) against the branch:**

- `spec097_stable_signal.py` `sequence`: `assignments=4 distinct_events=2 set_after_shutdown=False
  wait_after_shutdown=2.01s` against `assignments=2 distinct_events=1 set_after_shutdown=True
  wait_after_shutdown=0.00s`; `child-orphan` and `child-worker`: `setter_calls=1` and `2`,
  `same_event_object=False`, against `setter_calls=0 same_event_object=True`, each waiting 0.00 s
  with `parent_event_set_by_child=False`; `cut` and the three `swap-` rows equal.
- `p9_second_stop_in_fork_repair.py`: `two-stops-second-none` never left the repair in 6.0 s and
  `two-stops-second-finite` took 3.01 s, against all three child rows returning in 0.05 to 0.06 s
  with no setter call.
- **`spec097_pubsub_after_shutdown.py`, recorded and not pinned** (the test pins only
  `confirmed=7 failed=0`, because SPEC-093/094 deletes `GooglePubSubSink`'s flush machinery):
  identical on both trees, `confirmed=7 failed=0` at 1.0, 2.5, 3.0 and 3.9 s, `flush()`
  `(True, None)` at 1.0 s and `(False, 'sink-flush')` at the other three. Its
  `signal_set_after_reuse=True` on both: straight after `configure(P)` the branch's P still holds
  the wrapper's forwarded event, and P's first log hands it its own, cleared.
- **The model cells:** 640 on each tree, no verdict differs.
- **The re-entry sweep** on the branch: `sections=140` or `141`, the count varying by run because
  the sections a workload reaches depend on thread timing, and `failed=0
  ast_sections_not_reached=17` on each of seven runs after the second diff review made it fire on a
  section's identity (141, 141, 141, 140, 140, 141, 141 sections). Eight
  run only on the drain thread (`worker.py` `_decline_exit_batch`, `_emit` three times,
  `_nothing_lost_since`, `_release_marker`, `_take_marker`, `_terminal_failure`); eight only in a
  forked child's repair (`_ledger.restart_in_child`, `_adopt_factory_after_fork`,
  `_clear_after_fork` twice, `_record_seeds`, `_restart_ledger_after_fork`, `_walk_after_fork`,
  `reclaim`); and `_ledger.restart`, which only the test suite's balance check calls.

## Verification

This ritual ran `spec-lint.sh`, `docs-lint.sh` and `gen-index.sh`, and re-ran the probes, the
cells and the sweep above; the six gates run on the branch before its push. The build ran the
suite on 3.13.1 and on 3.12.12, `integration.yml`'s extras leg (`--all-extras`,
`LOG_FOUNDRY_EXTRAS=1`) at `94afef16` and the docker services leg at `94afef16` only: Docker was
down when the second diff review's fixes landed, and `sinks/_factory.py` changed since only in
docstrings, so that leg is owed to CI. The second review's frame ran on 3.12.12. The re-entry sweep was revised in that
review to fire on a section's identity rather than its index, and to exit 1 on a failure.
**Owed to CI:** a run on Linux, which nothing here measured.
