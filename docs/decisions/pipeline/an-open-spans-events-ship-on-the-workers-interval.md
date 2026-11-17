---
type: Decision
title: "An open span's events ship on the worker's interval"
description: "about once a flush interval the drain thread takes every open span's buffered events past its held `span.start`, in place, one submission per span, never once the span's close has begun, under a sweep hold no thread re-enters and the drain waits for at most `_SWEEP_WAIT`; `span.start` stays on the span until its close, and no setting changes the cadence. (SPEC-091)"
---

**Andrew's answer.** Asked on 2026-11-15 "Should an open span's log events ship before the span
closes? Today they wait for the close, so a process killed mid-span (SIGKILL, OOM, timeout) loses
every event in it, uncounted.", Andrew answered "Ship every ~1s (Recommended)". The loss it
answered was finding 8 of the 2026-11-14 audit: `p12_kill.py`'s `SIGKILL` 3.5 s into an open root
span delivered 0 of its 51 events, with nothing on stderr. On the build (`459d1592`) it delivers
50, the missing one being the held `span.start`.

**The rule** (`decorator._sweep_registered`, run by `Worker._run_sweep`; SPEC-091 FR-001, FR-002):

- **Every open span is registered** in `decorator._open_spans` from its push until its close
  begins: `_begin` registers it, `_close_span` and `_abandon` remove it first. Both are single
  dict operations with no lock, so the open and a level call take no lock.
- **The worker is built when the first span opens**, before the span is pushed, so the drain
  thread exists inside the process's first traced call. Once a worker exists the open reads one
  attribute and calls no `_lifecycle` function.
- **The take.** About once a flush interval, on its own clock (`last_sweep`), the drain thread
  reads each registered span's length first, skips it if it is no longer registered or is
  `closed`, and otherwise takes `span.events[held:n]` and deletes that slice **in place**. Each
  span's take is **its own submission**, so SPEC-090's cap stays meaningful. A sweep that handed
  anything in makes the time trigger due, so an event leaves within about one interval when no
  backlog is waiting; under a backlog the swept submission waits its turn like any other.
- **`span.start` is held** (`Span.held = 1`) so it ships at the close with the span's final
  baggage; an in-span `flush()` ships it and sets `held` to 0.
- **The sweep hold.** Every non-append write to a span's buffer, and `continue_trace`'s check and
  re-parent, happens inside `decorator._sweep_hold`, an `RLock` taken nowhere else. Each of the
  five hold sites first asks `_sweep_held_here()`: on a thread already inside a hold, which a
  collector callback or finalizer can be, a sweep takes nothing, a close or an abandon is deferred
  to the holder (drained before the release, so `span.end` still ships last), and
  `continue_trace()` is refused. Nothing is written to stderr inside a hold except by such a
  callback; `Worker.submit` is split into `_enqueue` and `_announce` so every library line is
  written after the release.
- **The drain alone waits a bounded time** for the hold, `worker._SWEEP_WAIT` (0.05 s), and skips
  the sweep on expiry; every other caller blocks. The drain is the thread the others wait on.
- **`flush()` reaches every open span**, the caller's whole and every other thread's and task's
  past its `span.start`; the exit drain makes one sweep before it fixes its bound.
- **A take whose hand-in raised is counted** in `in_span_lost` at once.

**What a kill still loses.** `SIGKILL`, `SIGTERM` without a `shutdown()` and `os._exit` lose up
to about one interval of an open span's events, every still-open span's `span.start`, and whatever
is queued or in a batch the sink has not accepted; no counter survives to show it. The sweep runs
between emits, so a kill during an emit or a retry backoff loses everything logged on open spans
since the last sweep.

**Kept, unchanged:** *Boundary events take the span's final baggage; mid-span events keep the
moment's* (`docs/decisions/trace-model/`), which is why `span.start` is held and a worker sweep
never takes a boundary event; and *Two drains, deliberately distinct* (this area): the sweep is
the worker's own work and part of `shutdown()`'s exit drain, not a third drain, and `flush()` stays
the repeatable one.

**Rejected** (each measured in SPEC-091's Evidence and FR-001, FR-002 unless said):

- **"Sweep at shutdown only"** and **"Keep it, document it"**, the two other options Andrew was
  offered: neither helps a `SIGKILL`, an out-of-memory kill or a Lambda timeout, where the audit
  saw the loss.
- **A hand-off on the owner's next append**: a handler that logs and then blocks never makes one.
- **A sweep tied to the time trigger**: while the count trigger kept firing it never ran, every
  sample at the 5 s cap. **A sweep that did not make the trigger due**: up to 2.006 s.
- **A take that rebinds the list**: an append that read the attribute before the rebind was lost,
  1 of 36,015 events in one of three runs at a switch interval of 1e-6 s; in place, none in ten.
- **No membership read before the take**: `span.end` left before its own logs in 25 and 29 spans
  a run.
- **One concatenated hand-in per sweep**: one submission past `_BACKLOG_BATCH_CAP` that SPEC-090's
  rule must send whole. It held the lock 157 to 159 µs for 1,000 spans against 428 to 433 µs per
  span, a cost taken for the cap.
- **A stderr write under the lock**: with stderr wedged on the overflow line, a traced call on
  another thread had not closed after 4.01 s; with the lines after the release it closed at 0.00 s.
- **Shipping `span.start` early**: it would leave without the span's final baggage.
- **A per-span lock**: the in-place take makes it unnecessary, and it would sit on every append.
- **A plain `Lock` for `_sweep_lock`**: a collector callback that closed a span inside the hold
  took the lock again on its own thread, and the sweeping thread did not return within 5 s.
- **An `RLock` alone, with no per-site check**: the nested close swapped the list the sweep was
  taking from, so `span.end` shipped ahead of three logs and the ledger read 3 events short at
  `shutdown()`.
- **An unbounded wait by the drain**: a `flush(3.0)` made while holding the lock returned
  `"timed-out"` after 3 s with the drain waiting on it, and `ok=True` with the bound (measured
  before the per-site check, which now makes that `flush()` report `"abandoned"` in under 1 s).
- **A `SIGTERM` handler that sweeps**: the default disposition skips `atexit`, `p12_kill.py`'s
  `SIGTERM` loss was of five **closed** spans already queued, and a handler calling `shutdown()`
  is §12's finding-3 deadlock.

**Do NOT build** a setting for the sweep's cadence, a sweep that takes `span.start` or rebinds
`span.events`, a write to a span's buffer outside the hold other than `append`, or a site that
enters the hold without asking `_sweep_held_here()` first; `tests/test_span_events_site_rule.py`
derives the last two from the AST. The residuals the build accepted are in `architecture.md` §13.

## History

### 2026-11-17

- **Built** by SPEC-091 at `459d1592`: the registry, `Span.held`, `_sweep_registered`,
  `_SweepHold` with the per-site check and the deferred closes, the drain's bounded wait, the
  open-time build, `flush()`'s registry sweep and the exit drain's sweep, and the child forgetting
  the registry. The plan review's two deadlocks on the lock gave the hold its re-entry guard and the
  drain its bound.

### 2026-11-15

- **Settled** by Andrew's answer, "Ship every ~1s (Recommended)".
