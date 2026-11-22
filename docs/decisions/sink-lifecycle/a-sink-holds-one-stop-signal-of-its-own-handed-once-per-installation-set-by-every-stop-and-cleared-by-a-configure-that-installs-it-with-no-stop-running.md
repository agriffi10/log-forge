---
type: Decision
title: "A sink holds one stop signal of its own, handed once per installation, set by every stop, and cleared by a configure() that installs it with no stop running"
description: "each sink the library delivers to holds one `threading.Event` of its own (`Runtime._signals`), assigned through `log_foundry_stop_signal` at its first delivery and again only after a `configure()` installs it anew; every stop sets every such event, one made during a stop is made set, and it stays set for a delivery after the stop until a `configure()` with no stop running clears it in place; a forked child keeps each event, re-initialised, and runs no setter. Never hand the worker's event to a sink, never mark a sink handed before its setter returns, never clear while a stop runs. (SPEC-098)"
---

**What was decided.** A sink's backoff waits on an event that is the sink's own, not on the
worker's stop event. `Runtime._signals` keys it by `id(sink)`, holding the sink weakly where it can
and strongly where it cannot, beside the event and whether a setter given that event has returned.
`offer_stop_signal` assigns that event and marks the sink handed only at the setter's return;
`offer_once` is the one offer site on each delivery path, the orphan log, the worker build, both
branches of a swap, the drain thread's batch, `flush()`, the fork resume and the factory wrapper,
and is lock-free for a sink already handed. A stop's entry section sets every event in
`Runtime._signal_events` under `_owned_lock`, inside `_state._lock`; an event made while a stop runs
is made set. `config._change_sink` calls `reinstall_signals`, which clears in place the event of the
sink it installs and of every sink its graph reaches, and marks each unhanded, so its next delivery
offers the same object again. The worker's own `_stop` still wakes the drain thread, and worker-path
rounds wait on it. Because the drain thread's batch offers the sink, a setter can run on the drain
thread; a setter raising a `BaseException` there ends the worker, exactly as an `emit` raising one
there does (measured on `c67931cd` for an `emit` raising `KeyboardInterrupt`: the worker stops with
`stopped_reason` `KeyboardInterrupt` and the line *absorbed a failure while draining the log queue*,
and on SPEC-098 identically for a setter), and a setter there that calls `flush()`, `shutdown()`, a
level call or `configure()` returns (`tests/test_stable_stop_signal.py`).

**Why.** The event a sink held used to change: a worker built after a stop had a fresh one, a
forked child had its own, and every delivery path handed whichever was current through the sink's
setter, which is the sink's own code. Six of the eleven lifecycle fixes after the 1.2.0 arc were
about which event a sink held at a given moment, and each added state: a debt to re-hand, a token
while a hand was open, a four-round settle loop, marks paid when a close finished, the events a
child forked inside a setter kept. The 2026-11-14 audit's lifecycle frame recommended one stable
event per sink, handed once, and Andrew took that direction the same day. With it the machinery
that chose which event a sink holds had nothing to do and was deleted; the list is in SPEC-098's
delivery doc. The README's protocol keeps its name, its type and its promise that the event is set
when a shutdown starts, so a 1.x sink keeps working and invariant 10 takes no exception.

**Why a `configure()` clears it: Andrew's option 2, 2026-11-16.** His first answer that day was
that the signal stays set after `shutdown()`. Executing it against `GooglePubSubSink` replaced it
the same day. In `spec097_pubsub_after_shutdown.py` a wrapper forwards the signal to P and leaves P
open, the `shutdown()` sets P's signal, P is configured again, five orphan logs publish, `flush()`
runs and the exit drain closes P, with `overflow_timeout` 2 s. With the signal kept set through the
`configure()`, the flush and overflow passes ended at once, the exit drain's close bound started
almost at once, and at confirm delays of 2.5, 3.0 and 3.9 s it read `confirmed=0 failed=7`, every
publish confirming between `overflow_timeout` and twice that counted lost, where `ead40798` read
`confirmed=7 failed=0`. With the clear it reads as `ead40798` at all four delays (SPEC-098's
Evidence). So the signal stays set for a delivery after a stop with no `configure()` between, and a
`configure()` that installs the sink again clears it.

**What makes the clear unable to race a stop's set.** The check of `Runtime._stops_running` and the
clear are one `_owned_lock` section, and a stop joins `_stops_running` before its own section sets
every event and leaves it only when it returns. A clear either finds the stop and clears nothing,
or runs wholly before the stop's set. SPEC-097's command gate does not carry this, because a stop's
wait for a change in flight is bounded and the two can still overlap.

**What it costs.** A delivery after `shutdown()` with no `configure()` between finds the event set:
the sink's own waits collapse and so do the delivery engine's waits on the orphan path, while every
attempt is still made (the spec reviewer's `engine_after.py` orphan row took 0.00 s against 0.31 s at
`ead40798`). A child forked after its parent's `shutdown()` inherits the set event until it
configures the sink again. `_note_orphan_emit` on an armed, handed sink costs about 32 ns more and
takes no lock (56.8 to 57.8 ns at `ead40798` and 88.3 to 90.1 ns on the prototype, SPEC-098's
Evidence).

**Rejected.**

- *Marking a sink handed before its setter returned.* A second thread's log reached the sink before
  it held any event: `test_another_thread_hands_the_signal_before_it_emits` read `held=[False]` on
  all three rows until the mark moved to the return.
- *Seeding a forked child from the sink table.* It dropped the event of a released wrapper whose
  member still held it through the wrapper's forward, so nothing in the child re-initialised or set
  it: `orphanwrap.py`'s child rows read 4.72 s and 4.71 s against 0.00 s. The seed walks the event
  set.
- *Re-offering a sink whenever its attribute is not its table event.* That reads the attribute,
  which can be sink code, on every delivery.
- *Keeping the signal set through a `configure()`.* The `GooglePubSubSink` losses above.
- *Setting a displaced sink's event at a swap.* A swap does not stop delivery, so it cuts no wait.

**Do not build:** a hand of the worker's event to a sink; a setter run in the fork repair; a handed
mark made before the setter returns; a clear while a stop is running.

**What is left** (`architecture.md` §12): a reinstall's clear can cut the interruption of a close
still running from an expired stop, reasoned and not measured; a child forked while a different
parent thread is inside a sink's first setter can run that setter in its repair; a late delivery,
with no `configure()`, to a `GooglePubSubSink` a `shutdown()` left open is unmeasured.

## History

### 2026-11-20

- **Noted**, from SPEC-098's first diff review: a setter can run on the drain thread, and a
  `BaseException` from it ends the worker as one from `emit` does.
- **Settled** by SPEC-098, building the direction Andrew took on 2026-11-14 with his option-2 call
  of 2026-11-16. It supersedes, for a late delivery only, SPEC-033 FR-004 AC-6's "the same sink
  logged to again" and the *Rejected: declining without paying* reason in *A hand the worker build
  declines for a close in flight is paid when that close finishes*.
