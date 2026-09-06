# Completed Spec — SPEC-054: One Lifecycle Owner for Both Delivery Paths

## What was completed?

The **bookkeeping** around the two emit paths exists once. The emit paths themselves are unchanged
(arch §9, SPEC-028): a `@trace`d call still hands its buffer to a worker, a level call with no
active span still emits on the caller's thread. What was thirteen twin pairs is now:

- **One retirement count and one stop event** on `_lifecycle._state` (FR-001). `Worker` lost
  `_shutdown_done`, `retired`, its own `threading.Event` and `_offer_stop_signal`; it is handed the
  owner's event at its build, records the count as `_epoch`, and reads `retirements > _epoch` where
  it read its own flag. A **count**, not a latch, because a latch makes a worker built after a
  `shutdown()` returned count every event it delivers as stranded (SPEC-030 FR-001's definition).
- **One owed-close record**, `_Lifecycle._owed`, replacing `_orphan_owed`, `_orphan_closed_sink`,
  `Worker._sink_closed` and `Worker._unclosed_swaps` (FR-002). Armed by an orphan emit that lands, a
  worker built on a sink, and a swap the worker adopted; discharged at the moment a close *starts*,
  under the lock, in the section that registers that close.
- **One closer**, `_lifecycle._close_owed`, replacing `Worker._close_if_owed`, `Worker._close_sink`,
  `_close_orphan_sink` and the per-sink `_close_owed` (FR-003). `_shutdown_worker` has one shape for
  both branches and grants the closer grace once, at the end. `_swap_sink` is one function over
  `Worker.retarget`, which reports `declined` / `fenced` / `unfenced` and hands back the sink it
  displaced.
- **Three questions, not four** (FR-004): existence, liveness, and the moment — which carries two
  predicates, `in_flight` (reads `draining`) and `held` (reads thread liveness), because an
  abandoned drain answers them oppositely.
- **One `Health` assembly** (FR-005), every field with one authority.
- **One fork residue handler** (FR-006): `_clear_after_fork` empties the close registry and nothing
  else, and `_owed` is declared in the owner's `_FORK_SKIP`.

Retired, each said out loud: the **closed-sink latch** — what it protected against is answered at
close time by `held`, not by refusing an arming — and **`Worker(sink_released=)`**, which has no
work to do once a late worker's build arms its own sink.

## What changed from earlier specs?

Observable, and none of it public API (`tests/test_public_surface.py` and `tests/typed_consumer/`
are unchanged):

- **`health().sink` is filled on both paths**, from the **configured** sink. It read `None` on the
  orphan path — the divergence probe 1 found — and `worker.sink` on the other. The two differ
  permanently after a declined swap (SPEC-035 FR-003) and after any `configure()` on a retired
  worker (SPEC-033 FR-002); arch §12 already named the config the authority for "installed".
- **`health().inherited_sink` answers from the config**, closing arch §12's open item. It read the
  owed record's last entry, which arming order does not make the installed sink.
- **Three close counts go 1 → 2**, all three the closed-sink latch's retirement, and each second
  close follows the delivery it discharges — SPEC-045 FR-002's one close per write-epoch, asserted
  by the sink's event count at each close rather than by the count alone. A **fourth** case is a
  genuine redundancy and the accepted trade: a swap the worker *declined* arms the new sink anyway,
  because SPEC-035 FR-003 guarantees a declined sink is owned by somebody. Not arming it was built
  and measured costing that guarantee outright.
- **A worker's build releases nothing.** It armed its sink and released, detached, any other owed
  sink (SPEC-044 FR-002); those now stay owed until the `configure()` that supersedes them, or exit.
- **A worker-path `shutdown()` no longer pays the grace for an orphan close** it never touched
  (SPEC-050 FR-002's process-wide gate, measured at 2.007 s wall for an unrelated sink).
- `Worker.shutdown` → `stop`, `Worker.swap_sink` → `retarget`. Internal; 169 worker-receiver
  `.shutdown(` call sites in `tests/` at `3a4d337`, counted over that tree.

## What the build found that the spec and the plan did not

- **The swap released a sink the worker's thread may still be inside.** Two records hid it — the
  stranded sink lived on the worker and the swap's loop only saw the orphan one — and under one
  record a later `configure()` closed it under a live writer, which is SPEC-033 FR-002's measured
  defect at a new site. Reproduced at `A.closes == 2`; the fix is the `held` guard the closer uses.
- **The bystander wait cannot be keyed on the record.** By the time a bystander arrives, the caller
  inside the close has discharged that sink, so the record is empty and the wait never fires —
  measured, the second caller returned through a 0.6 s close at `closed == 0`, which is the defect
  SPEC-050 FR-002 exists to prevent. It is keyed on the registry, excluding its own registrations.
- **A registration the closer did not hand off leaked on any non-local exit**, which is worse than
  the count it replaced: that sink is skipped by the signal refresh forever and never taken by a
  later closer. Discharged in a `finally`, with the `try` opening on the line after the take.
- **A `threading.Event` in a module dict is unreachable by the fork walk**, which assigns a
  replacement back *by name*. `_Closing` holds it as an attribute, so the shipped primitive-shape
  lint is satisfied as written rather than widened.
- **FR-003's "arm `new` whether or not the worker adopted it" contradicts FR-002's "never two for
  one write-epoch"** on a declined swap. Resolved in FR-003's favour, for the reason above.

## Verification

All six gates green locally on the branch: `ruff`, `mypy`, `pytest`, `spec-lint.sh`,
`docs-lint.sh`, `docstring-lint.py`. `tests/test_invariants_model.py` green over every seed after
each phase, and once over a widened `SEEDS = range(64)`: **65 passed**.

**Both probes re-run.** Probe 1 (FR-005 AC-1): outside a span, `health().sink` reads
`SinkLosses(dropped=0, failed=1)` against a `MultiSink` with one raising child, matching the sink's
own `losses()`, where it read `None` before. Probe 2 (the control) is byte-identical to the
baseline on both paths — the post-`shutdown()` difference between the paths is SPEC-030's fence and
is kept.

**`Worker.submit`, FR-001 AC-5.** The isolated read is what resolves this: over 5M iterations in
one process, best of 7, `self._shutdown_done` was 3.80 ns and the bound
`_lifecycle_state.retirements > self._epoch` is 7.55 ns, so **+7.5 ns** across `submit`'s two
reads — inside the 10 ns budget. Reached through the module rather than bound it is 14.86 ns, or
+22 ns, which is outside it. The whole-`submit` harness **cannot resolve** a delta this size: it
spreads ~20 ns across repeated runs of one tree, and an earlier version of it reported +14 ns for a
change that is +3 ns, which was a deque growing to a million entries inside the timer.

**The roster floor is re-derived, not lowered.** `log_foundry._lifecycle` 46 → 42, accounted for
per scope in `_SITE_FLOOR`'s docstring by deriving the site list from both trees and diffing it.
Categories: 22 existence, 14 liveness, 8 moment.

**Ten mutants planted and killed**, one at a time, in place: three roster categories (FR-004 AC-3),
a record rebind and an out-of-transition clear (FR-002 AC-6), the fork opt-out (FR-006 AC-1), a
ninth direct close (FR-003 AC-7), plus the three guards this build added — the bystander wait, the
swap's `held` term, and the registration discharge.

**Test names diffed from `--collect-only` on both refs, never pass counts**: 2563 → 2572, over the
whole change — 22 names added, 13 removed, **net 9**, and each of the 13 removals pairs with an
addition that restates the claim its old name made. The nine net-new tests are the ones the
criteria name and `tests/` did not have: `threading.get_ident()` inside `close()` on each path
(2), the four-sink fan-out on the worker path, the no-close-in-flight wall-and-CPU bound, the
`KeyboardInterrupt` during the fan-out, the stranded close past the grace, FR-002 AC-5's two
racing shutdowns, and FR-001 AC-2's second shutdown stranding the late worker's next submission.
FR-005 AC-3's worker-path `inherited_sink` test is the tenth addition; it nets out against
`test_the_worker_waits_for_the_swapped_out_close_it_started`, which became
`test_the_swap_waits_...` when the close moved to the owner.
