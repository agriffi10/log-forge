# Completed Spec — SPEC-044: Lifecycle Races

## What was completed?

Five of the six lifecycle races recorded at `architecture.md` §13 are closed and the sixth is
documented. All six were re-reproduced on `main` at `4fea8f3` before anything was changed, and each
is now pinned by a committed reproduction in `tests/test_lifecycle_races.py` rather than by the
scratch harness §13 named.

- **FR-001** — `shutdown()` racing a first `@trace` left a live drain thread and an unclosed sink
  while `health()` read `retired=True`. `_shutdown_worker` now latches retirement and reads the
  worker in one critical section, and `_get_worker` registers under the same lock any worker built
  while a shutdown runs, which that call then drains. New: `_Lifecycle._shutdown_running` (a **depth
  counter** — a boolean was measured reproducing the original defect under two concurrent
  shutdowns), `_late_worker`, and `Worker(sink_released=…)`.
- **FR-002** — `_get_worker` discarded an unclosed sink's close record when a blocked
  `configure(sink=B)` changed what `_ensure_sink()` returned. A transition now decides the close
  before clearing the record, at both sites that clear it.
- **FR-003** — a log call landing inside a close replaced the stop event that close was waiting on.
  `_closing_now` brackets `release()`; `_offer_orphan_signal` skips the refresh for a sink being
  released; `_clear_closing_after_fork` empties it in a child.
- **FR-004** — `_swap_sink`'s worker branch now latches the sink it hands over, keyed on
  `worker.sink` rather than on the orphan record, which is `None` in the reproduced case.
- **FR-005** — `_Lifecycle._FORK_SKIP`, a class attribute, because `_fork._skipped_names` reads the
  opt-out off the holder and the module-level tuple never reached an instance attribute.
- **FR-006** — documentation only, deliberately: both ways of bounding the live sink's `close()`
  were built and reverted, and the third needs an interruptible `Sink.close`. A test pins the limit
  on both delivery paths.

**Deviation:** the closer join's budget arithmetic in `_swap_sink` is flagged, not tested —
separating it from the raw timeout needs a load-sensitive 0.6-vs-1.0 second margin, and a flaky
bound is worse than an unpinned expression. Recorded in the test's own docstring.

## What changed from earlier specs?

- `Worker.__init__` gains keyword-only `sink_released`; `Worker.swap_sink` resets `_sink_closed`,
  because the flag describes one sink rather than the worker's lifetime.
- `tests/test_worker_predicate_roster.py`: eight rows, per-module floor 37 → 45.
- `tests/test_sink_release_roster.py`: a table for the one requester that deliberately does not join
  its detached closer, and FR-004 AC-2's latch-disposition roster over the same derived population.
- `tests/test_span_sweep.py` (SPEC-036) discarded a worker per trial without shutting it down,
  leaking thirty daemon threads to the end of the session; fixed at source.
- `docs/process.md` and `CLAUDE.md`: the PR grouping is no longer a blocking reviewer gate.
- `architecture.md` §13: races 1–5 struck in place and marked fixed (SPEC-021's rule), the
  double-close paragraph struck likewise, the timeout entry gains the orphan path, and the
  concurrent-`configure` double-close is recorded as measured-and-unfixed.

## Verification

Four gates green locally by **exit code**, and the suite green in three file orderings. Every new
guard was mutation-tested: fix reverted, named test observed to redden, fix restored. Four of the
first-draft tests passed against the defect they named and were rewritten; the reviews found three
of those four.
