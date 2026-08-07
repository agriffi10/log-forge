# Completed Spec — SPEC-033: Orphan-Path Sink Handoff

## What was completed?

- A late `configure(sink=...)` now closes the previous sink in a process with **no worker** — one
  that only ever logs outside a span. `decorator._swap_sink` gained a second branch instead of
  returning early on a null worker.
- The orphan path records the sink **object** an emit reached (`_orphan_sink`) rather than a
  boolean, and the most recently closed one (`_orphan_closed_sink`) rather than a process-wide
  latch. `api._log` passes the identity it already resolves.
- New `src/log_foundry/_lifecycle.py`: `close_detached`, `join_closers`, `closing_count`,
  `offer_stop_signal`, `DEFAULT_CLOSER_GRACE` — process-global, moved off `Worker`, used by both
  paths.
- Two defects found by review of the spec and fixed here, not deferred: a sink configured **after**
  `shutdown()` was closed by nothing at all, and an orphan-only process never received a SPEC-027
  stop signal.
- `tests/test_orphan_sink_handoff.py` — 43 tests. Suite 1106 → 1151, none removed.

**Deviation from the spec:** none in substance. `close_detached` returns the thread rather than
joining (the spec's own final revision), so a caller can start it under `_worker_lock` and wait
after releasing.

## What changed that a later spec should know?

- **`incomplete_swaps` is worker-only, and now says so.** It records a *drain* that could not be
  confirmed; the orphan path has no drain and leaves it at zero deliberately. Do not widen it —
  `closing_sinks` is the field for a close that has not come back.
- **`DEFAULT_CLOSER_GRACE` moved to `_lifecycle` and is deliberately not re-exported from
  `worker`.** A stale `monkeypatch.setattr(worker_mod, ...)` would set an inert attribute and the
  test would pass against the real grace under an unchanged name. Without the re-export
  monkeypatch raises.
- **The closer roster is process-global.** `tests/conftest.py` clears it; a new test that leaves a
  hung closer would otherwise leak a non-zero `closing_sinks` into the next test.
- **Two questions, not one: who *performs* a swap, and who *owns* a sink's close.** The first is
  liveness — `Worker.swap_sink` returns early once `_shutdown_done`, so routing a swap to a
  retired worker loses the handoff entirely and leaks the adopted sink with every counter clean.
  The second is ownership — a worker that *holds* a sink has either closed it or deliberately
  left it open because its drain thread may still be inside that sink's `emit` (SPEC-027 FR-004).
  `_live_worker()` answers the first; `_worker.sink is X` answers the second, in **both**
  `_close_orphan_sink` and `_swap_sink`'s orphan branch. Answering the second with liveness
  double-closes on a clean shutdown and closes **under a live writer** on an expired one — both
  measured, and the second is what SPEC-028 exists to prevent. Two review rounds of this PR each
  found one half of this wrong; do not collapse them.
- **`_close_orphan_sink` leaves `_orphan_sink` set when a worker owned it.** That is why the swap
  needs its own ownership test rather than trusting the record: after any shutdown the record
  still names the worker's sink.
- **`Worker.shutdown` grants the closer grace; `_shutdown_worker` must not grant it again.**
  Doing both charges two full `DEFAULT_CLOSER_GRACE` against one exit (measured 4.01 s against a
  2 s grace). The orphan branch grants it where nothing else will.
- **`_orphan_stop` is replaced, never cleared.** An `Event` cannot be un-set and `_retry.wait`
  returns instantly on a set one, so a sink still holding the shutdown's event backs off not at
  all.

## Anything deliberately left open?

Recorded in `architecture.md` §13, not fixed: handing back an already-swapped-out sink closes it
twice — **the worker path behaves identically** (measured `A.closes=2`), `configure()` already
documents that the previous sink must not be handed back, and `Sink.close` is required idempotent.
Two further shapes need a concurrent emit during `configure()`, which that call's documented
"not thread-safe" covers.

`architecture.md` §13 carries no open item from this spec. The 2026-08-05 audit arc
(SPEC-024..033) is now fully shipped.

## Evidence

Measured on `f17edd4`, probe registered before the library's own `atexit` handler so it runs after
it:

| Sequence | Before | After |
|---|---|---|
| `configure(A)` → `info()` → `configure(B)` → `info()` → `shutdown()` | A unclosed, 1 event held, `incomplete_swaps=0` | both closed once, events delivered |
| `configure(A)` → `info()` → `shutdown()` → `configure(B)` → `info()` | `B.closed=0`, event lost, `retired=True submitted_after_shutdown=0` | `B.closed=1`, delivered |
| same, with a `@trace` (worker built then retired) | `B.closed=0`, event lost | `B.closed=1`, delivered |
| hanging `close()` on the swap | n/a (no close at all) | bounded by `DEFAULT_SWAP_TIMEOUT`, `closing_sinks=1` |
| `_retry.wait(5.0)` on a set event | 0.000 s (vs 0.405 s unset) | fresh event armed; post-shutdown sinks still back off |

Sixteen mutants, all caught. Six from the build: swap returns early (13 failures), clear instead of
re-point (5), `_close_orphan_sink` guards on existence (1), `_shutdown_worker` returns early (1),
never refresh a set event (2), skip the signal offer on existence (1). Six more added after the
PR review found them escaping: `_swap_sink` guards on existence (1), remove both record clears (2 —
green across the whole suite before), hold `_worker_lock` across the closer join (1), grant the
grace twice (1), route the live close through `close_detached` (1), signal offer on existence in a
mixed process (1). Four more after the review of those fixes: decide the swap's close by
liveness (2 — the double close and the close-under-a-live-writer), and two runtime-import forms
the cycle test missed (`import log_foundry.worker`, `from log_foundry import worker as _w`) —
the forms a regression would actually take, since that is the import style this package uses.

Three of those six escaped because the **test** was wrong, not the code: the mixed-process test
asserted before `shutdown()` so a close at exit was invisible; the lock test timed an emit that
takes the unlocked fast path and acquired nothing; and the inline-close test used elapsed time,
which the exit grace satisfies for a detached close just as well. Time and counters were the wrong
observables — the thread the close ran on is the right one.
