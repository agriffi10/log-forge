# Completed Spec — SPEC-081: Ownership Record: Pin Only What the Library Holds, and Mark the Heap When a Child's Marking Trips

## What was completed?

- **A bounded record** (FR-001 to FR-003), in `_lifecycle.py`. Each `Runtime._owned` entry is an
  `_Entry` that holds its sink strongly only while the library still holds it, and weakly after a
  performed close or a supersession. `_repin` runs on every exit of `_swap_sink` and after every
  close `release` performs, and never raises. Before it demotes an entry, `_capture` records the
  sinks beneath it. `_entry_for` reads a dead entry as absent everywhere, and `_repin` prunes dead
  entries without any weak-reference callback. `Runtime._owned_lock` is an `RLock`, and
  `Runtime._owned_clock` is the epoch that guards a re-pin and a close's settle.
- **A child that pins and marks** (FR-004 to FR-006). `_pin_inherited`, registered with the new
  `_fork.register_pin`, runs first in the child. `_record_seeds` leaves out a closed sink of a
  shipped class exactly (`_fork._defined_here(type(sink))`). A tripped or inherited-failed
  marking runs `_mark_heap` over `_heap_objects()`, and a frozen heap counts as a failed scan. The
  last resort is `stamp(sink, *, built=False)`, with `built=True` at exactly four library-built
  roots. Three once-only lines guard the capture, the re-pin and the refused `sink=`.
- **The record** (FR-007). The ownership decision was revised with the fence kept byte-identical,
  and the order-of-work decision has the pin first. `architecture.md` changes §9 (the pin is now
  step 1), §12 (a superseded marker) and §13 (two items struck, items 7 and 8 rewritten, the eight
  residuals added). Invariant 12 has the pin first and loses the re-configure exception, and
  invariant 5 records the last resort's unclosed `sink=`. `v1.2.0.md` gains two *Fixed* entries.
- **Implementer decisions.** The spec's `_owned_epoch` was built as `_owned_clock`, because
  `tests/test_worker_predicate_roster.py` treats every `_epoch` site as a worker question. FR-001's
  lock criterion asserts "not held by the entering thread" through stand-ins that record their
  owner. The capture reads containers through `_read_members`, which announces nothing, so a parent
  fault is announced under the capture's own line. The `id`-reuse tests build live ballast first.
  The spec's revision history records where the code says more than the text.
- Commits: `b1ebbf0b` (In Progress), `ba36616a` (Phase 1: FR-001 to FR-003), `f8dc19a1`
  (Phase 2: FR-004 to FR-006), then the completion commit.

## What changed from earlier specs?

- SPEC-042's strong-for-life record is retired. Its two reasons are now met by the dead-entry rule
  and the child's pin. A tripped marking no longer leaves `_marking_failed` set: the heap scan
  clears it, and so does a grandchild's successful scan of a flag it inherited. The trip line is
  now a `pending` line, not a `lost` one.
- SPEC-079's order of work gains the pin as its first step. `_bounded_children` now wraps
  `_read_members`, and `_positional_sinks` takes the reader as a parameter.
- `_FactorySink._checked`, `config._ensure_sink`, `config._change_sink` (for a factory) and
  `_adopt_factory_after_fork` stamp with `built=True`.
- Re-recorded guards: `LOCK_SITES_AT_BASE` (six new sites), `tests/test_lifecycle_inventory.py`
  (the `RLock` and four `Runtime` fields, with `register_pin` among the registries), and
  `tests/test_worker_predicate_roster.py` (`_delivering_sinks`'s four liveness reads). The twelve test
  files FR-001 lists were re-expressed for the entry's shape.
  `test_a_runaway_container_trips_the_ceiling_and_refuses_everything` now reads the flag clear
  with the sink still refused. `tests/test_sink_factory.py`'s refusing child now needs a failing
  heap scan as well.

## Verification

- New suites: `tests/test_ownership_record.py` (FR-001 to FR-003) and
  `tests/test_fork_heap_marking.py` (FR-004 to FR-006, and the retired line read as joined literals
  across `src/`), plus a heap-scan test in `tests/test_fork_static_reads.py`.
- Mutation, function-scoped in the implementer's worktree: all 20 reddened. M1 the epoch comparison
  removed from `_settle_close`. M2 `_capture` removed from `_repin_record`. M3a the pin made a
  no-op, M3b the pin moved after the roster. M4 the closed-seed exclusion removed. M5
  `_defined_here(type(r))` changed to `_is_owned(r)`, M5b the class test removed. M6a to M6e the
  dead-entry rule removed in `stamp`, the capture write, the walk and heap write, `releasable`'s
  entry and `releasable`'s owner. M7 the `_mark_heap` call removed. M8 the freeze check removed. M9
  the inherited-flag scan removed. M10a `built` exempting every root, M10b `built` ignored. M11 the
  re-pin's absorb removed. M12 the demoted list not outliving the locks. M13 the `RLock` made a
  `Lock`.
- **Owed to CI:** the full suite on 3.12 and 3.13, whose fork tests run real `os.fork()`
  children on Linux.

## Open and accepted edges

- A heap scan's copy-on-write cost is reasoned from how CPython stores reference counts, not
  measured.
- The eight residuals are in `architecture.md` §13, under *The bounded ownership record leaves
  eight residuals*. The ones a caller can hit: a frozen heap sends a tripped child to the last
  resort, and a last-resort child's `sink=` is never closed (use `sink_factory=`).
