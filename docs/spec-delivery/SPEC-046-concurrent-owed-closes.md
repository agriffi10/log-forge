# Completed Spec — SPEC-046: Concurrent Owed Closes

## What was completed?

`shutdown()` cost one slow sink's `close()` multiplied by the number of sinks the orphan path
owed a close. Measured on `main` at `dcb07c3` against `shutdown(timeout=1.0)` with 2-second
closes: 1 owed sink 2.00 s, 2 owed 4.01 s, 4 owed 8.02 s — linear, because SPEC-045 made the
owed-close record a set without changing how the set is drained. This is the one item that spec
*introduced* rather than inherited.

- `_close_orphan_sink` closes one sink on the calling thread and the rest on threads of their own,
  joining every one in a `finally`. New: `_close_owed`, `_inline_close_choice`,
  `_live_config_sink`.
- The inline sink is the **configured** one where it is owed, else the most recently armed — so
  `shutdown()`'s own close stays inline (SPEC-030) and the single-owed-sink case never spawns a
  thread.
- A reviewer measured 200 owed sinks at 11.4 s → 0.06 s, and `main` timing out past 90 s at 2,000.

**The design was replaced by its own spec review, and the revision history records why.** Routing
the superseded closes through SPEC-030's `_start_closer`/`join_closers` is the obvious reuse of
shipped machinery and loses data two independent ways: the grace is what remains of the shutdown
budget (completed **1 of 4**) and it caps at `DEFAULT_CLOSER_GRACE` regardless (a 3-second close
delivered nothing where it delivers today). It also recharged SPEC-044's double grace at 4.02 s
and reached the §13 entry recording that a daemon close of this same sink was built and reverted.

**Deviation:** a single `Sink.close` is still unbounded. That is §13's constraint, unchanged and
deliberately not narrowed — one stuck sink still holds the exit.

## What changed from earlier specs?

- The `release()` call moved out of `_close_orphan_sink` into `_close_owed`, so
  `_EXPECTED_CLOSERS`, `_LATCH_DISPOSITIONS` and the release-receiver resolver in
  `tests/test_sink_release_roster.py` are re-keyed, and `tests/test_lifecycle_races.py`'s
  under-lock lint — which matches call names and cannot see through a rename — names the new
  helper. A widening, not an accommodation.
- `architecture.md` §12's exit-close entry moves to *Resolved*; §13's `shutdown()`-timeout entry
  says the cost no longer multiplies.

## Verification

Four gates green locally by **exit code**. Seven mutants planted and all killed. Four reviews
before the push, and each found something the previous could not: the spec review measured the
first design losing 3 of 4 closes; the plan review caught `configured in owed` being value
equality and an unguarded `Thread.start()`; the first diff review found the thread-body guard
untested (a `_close_owed` re-raising only on a fan-out thread passed the whole suite) and the
four-sink test unable to catch a dropped join; the second found a Ctrl-C abandoning every close
mid-write, a seventh mutant, and two false prose claims. Verified against `main` on `atexit`-only
exits, non-main-thread and concurrent `shutdown()`, `fork()`, callbacks from inside a close,
thread exhaustion, and a 60-seed × 2-tree fuzz.
