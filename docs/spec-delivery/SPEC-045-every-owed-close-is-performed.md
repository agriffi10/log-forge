# Completed Spec — SPEC-045: Every Owed Close Is Performed

## What was completed?

The sink every event was going to could be closed by nobody. The orphan path's owed-close record
was a **single slot**, so arming a second sink discarded the first, and an ordinary `info()` that
resolved a sink and was preempted armed a superseded sink over the live one when it resumed.
Measured deterministically with every `configure()` call sequential on one thread: `C.closes == 0`
on the live sink, its buffer never delivered.

- `_Lifecycle._orphan_sink` → `_orphan_owed`, a `dict[int, Sink]` of every sink owed a close, plus
  `_Lifecycle.take_orphan_owed()` — the one transition that empties it, reading and clearing under
  the lifecycle lock in one step.
- The three consuming sites (`_close_orphan_sink`, `_swap_sink`'s two branches, `_get_worker`) act
  on every sink the record names; the three reading sites (`_flush_live_sink`,
  `_inheritance_roots`, `_delivering_to_an_inherited_sink`) follow it past the first.
- `tests/test_owed_closes.py`, and a lint in `tests/test_lifecycle_races.py` that fails any site
  rebinding the record or emptying it outside `take_orphan_owed`.

**The spec was rewritten mid-build, and both discarded designs are in its revision history rather
than deleted.** It was authored as *Release Once Per Acquisition*, on the reading that the defect
was a sink closed twice. The spec review established the single-threaded hand-back is **not** a
double close — its two closes are one per acquisition, the second flushing a sink that took a
further event. The second diff review then established that refusing a repeat close *loses data*:
2 of 3 events on a wrapper-graph shape, 31 of 80 lifecycle-fuzz seeds against 0 before it. Two
narrower variants were built and each still lost on an adversarial seed. Making the record a set
removes the trade rather than choosing a side.

**Deviation:** three limits are recorded in `architecture.md` §13 rather than repaired — a sink
released while still live inside the wrapper that replaced it (pre-existing; a criterion pins only
that it must not become a loss), the exit close running the owed set inline and in sequence, and
`health().inherited_sink` reading the record's last entry, which arming order does not make the
installed sink.

## What changed from earlier specs?

- `_Lifecycle._orphan_closed_sink` is **untouched**: SPEC-044 FR-004 sets it for a sink a swap
  leaves *open*, which is a different claim from one that was closed.
- `tests/test_worker_predicate_roster.py`: seven rows re-keyed, one new site, per-module floor
  45 → 46. `tests/test_lifecycle_races.py`: the disposition table went 6 → 5 sites as three clears
  collapsed into `take_orphan_owed`. `tests/test_sink_release_roster.py`: `_close_orphan_sink`'s
  release receiver needed a local annotation to keep resolving.
- `configure()`'s docstring no longer forbids handing the previous sink back, and `README.md`
  states the close guarantee where it states the thread-safety caveat.

## Verification

Four gates green locally by **exit code**. A lifecycle fuzz found **0 undelivered events** over
4×80 seeded runs plus 80 runs of the two seeds that caught the earlier designs, matching `main`'s
baseline of 0 over 10×80. **Three of the four consuming loops were found by mutation, not review** —
truncating each to one sink passed the entire suite, and the swap's worker branch still did so
after its no-worker sibling had a test. Five fresh-context reviews before the push; the fifth was
one past the session budget and is why the two remaining coverage gaps were caught.
