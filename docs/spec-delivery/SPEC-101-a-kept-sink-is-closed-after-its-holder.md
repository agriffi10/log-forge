# Completed Spec — SPEC-101: A Kept Sink Behind a Caller's Wrapper Is Closed After That Wrapper, Never Beside It

## What was completed?

- **Two records on the close registry** (FR-001): `_CloseRegistry._written`, each sink the
  registry armed, and `_CloseRegistry._after`, each caller's wrapper with its attached members,
  both under `_lock`; a sink or persistent member is held weakly where its type allows and pruned
  once it dies, and `_Member` marks a member persistent or ordered (an ordered one is strong). `_attach_written` attaches every written sink in the
  installed graph to its closest caller's wrapper at each `configure(sink=…)`, and
  `_attach_skipped` attaches or moves a member a keep spared (`_Keep.spares` now returns why, and
  `_Keep` takes the installed `root`).
- **The after-holder close** (FR-001, FR-003): `release()` runs `_release_after` on the thread that
  closed the holder, before its discharge; a member's `Exception` is absorbed with one `_diag`
  line naming a close after the wrapper that holds it. `_ships_close`, through the new
  `_fork._member_defined_here`, tells a caller's wrapper from a shipped one statically.
- **The take order** (FR-002): `_order_after` in `_close_owed`'s take and the swap's stale take,
  never behind a shipped holder. `_put_back_ordered` puts back transitively, and
  `_release_ordered_alone` discharges the members ordered behind a holder whose release closed
  nothing; `_clear_after_fork` clears both records.
- **The model test** (FR-004): `Leaves` and `Mine` beside the recorder in
  `tests/test_invariants_model.py`, installed over a written recorder by a second random stream;
  `Closes.by_wrapper` and `check_repeat`'s four rows in `tests/model_oracles.py`.
- **The plan review's measured findings, fixed in the build:** an ordered attachment sits beside
  the persistent one rather than replacing it (M2), and both put-backs are transitive (M3).

## What changed from earlier specs?

- **SPEC-087's fourth shape is closed** and its third reads two closes in sequence: its three
  tests pinning the old counts are rewritten, and the decision *A swap keeps the sinks the graph
  it installs holds, and one decision closes a sink once* (`docs/decisions/pipeline/`) gains
  Part 4, an extended fence and two "never" items. `architecture.md` §12's item moves to
  *Resolved*; §13's shapes are rewritten. Invariant 5, the README's *Writing your own sink* and
  `docs/release-notes/v1.2.1.md` (two compatibility entries, a *Fixed* section) say what a
  caller's wrapper now gets.
- **SPEC-098's two reinstall tests** in `tests/test_stable_stop_signal.py` reach "signal set, still
  open" through an expired `shutdown()` with the drain parked in the wrapper, since FR-001 now
  closes the member after a completed one.
- **Two residuals open in §12**, unfixed under the scope freeze: FR-002's overlap returns for a
  sink armed in the swap's unlocked graph-read window (measured), and `_written` and `_after`
  hold a sink whose type refuses a weak reference strongly until a fork, so both grow under
  repeated configures of such a wrapper (measured).

## Verification

Measured on the built branch, Python 3.13 on macOS:

- **SPEC-085's 637 cells** on `5d0e7f68` and on the build: 0 differ; three red on both (273 and
  329 `never-closed@1`, 624 `ChildBroken:two-sinks`).
- **Mutants**, each run once over the 24 default sequential seeds: no after-holder close, 18 of 24
  fail (14 on "every sink written to is closed", 4 on the close-count rule); attachments dropped
  after one close, 5 seeds `late-write@`; `Mine`'s mark removed, 14 seeds `repeat@`; FR-002's
  order removed, 0 seeds, while `test_an_unconfirmed_swap_into_mine_is_closed_in_sequence` and the
  Mine row of the FR-002 tests fail on an overlap; an ordered attach replacing the persistent one
  fails `test_a_member_ordered_once_stays_attached` (Leaves `[3]` for `[3, 4]`); the
  `_release_ordered_alone` call removed leaves `{'closed': 0, 'in_flight': 1}` in the forked child.
- **Install draws:** `Leaves` in 21 of 24 seeds, `Mine` in 16; every seed's step digest unchanged.
- **Probes:** `leaves_remaining_holder.py` prints `A.closes=1 A.closed_while_inside=0
  A.closes_while_parked=0 A.refused=0 A.events=5 B.closes=1`; `spec101_slow_holder.py` slow
  3.0 s, hung 20.0 s, multi 0.0 s; `spec101_interrupt.py` `A.at_entry=[2]`.

The six gates run on the branch before its push. **Owed to CI:** a run on Linux, which nothing here measured.
