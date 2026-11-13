---
type: Decision
title: "A hand while a stop has a take ahead hands the current signal and never refreshes it"
description: "while a `shutdown()` still has a take of the owed record ahead of it (`Runtime._take_ahead`), the orphan log's locked branch and `flush()`'s section hand a sink the current stop signal as it is, set or not, and open a hand token only when they publish an arm, a cleared debt or a real refresh (`_refreshes`); the swap's no-worker branch returns before its hand while a take is ahead, and the factory wrapper only relays what it was handed. Never gate it on the whole stop (`_stops_running`), never pay a refusal through a mark or `_signal_owed`. (fix after SPEC-079)"
---

**The defect.** `_shutdown_worker` sets the process's stop signal in its entry section and takes
the owed sinks later: after the swaps it awaits, after `Worker.stop` on the worker branch, and for a
late worker's sink after that worker's stop, in the second pass. In between no close of the sink is
in flight, so `_will_hand` accepts it, and the deciding section called `refresh_stop_signal()`,
which replaces the set event with a fresh unset one, and handed that. The take's close then waited
on the unset event for the sink's whole backoff. Measured at `a8e91fac` in a forked child, with a
sink whose `close()` backs off 4 s on its signal and `shutdown(timeout=1)`: an `info()` before the
first take on the no-worker path took 4.01 s, one after `worker.stop` before the first take 4.00 s,
one after a late worker's stop before the second take 4.01 s, and a `flush()` while another
thread's hand token was open, before the first take, 4.00 s to 4.01 s; each close held an unset
event. `flush()` hands only under a token or a debt; a forked child's debt on a caller's own sink
class reaches no close, because the child refuses to close an inherited caller sink.

**The rule.** While any stop has a take ahead, those two sections hand `_state._stop` as it is and
never refresh it. `_refreshes()` is the one predicate both ask: `_stop.is_set() and not
_take_ahead()`. It is computed before the token decision, so the token condition reads it in place
of `_stop.is_set()` and a section opens a token only when it publishes an arm, a debt or a real
refresh. This is the rule `_settle_signal` and `_hand_after_close` already kept, carried to these
two sections, not a new guard. Measured with the fix: each of the four cases above took 0.00 s, the
close holding the set event. `test_a_hand_before_the_take_leaves_the_close_the_shutdowns_signal`
holds all four rows, asserting the event the close held.
`test_a_hand_that_keeps_the_stops_signal_opens_no_token` and its `flush()` twin hold the token
condition at each site: with `_stop.is_set()` left there, each section opened a token for a hand
that published nothing, and at the log's site a `flush()` meeting that token handed the sink the
set signal a second time.

**The sweep of hand sites.**

- The orphan log's locked branch (`_arm_and_hand`) and `flush()` (`_flush_live_sink`): changed here.
- The swap's no-worker branch (`_swap_sink`) also refreshes, but returns first while
  `_column() is DRAINING and _take_ahead()`; a take ahead implies a stop running, which is
  `DRAINING`, so it never hands while a take is ahead and is left alone. An edit there would be an
  equivalent mutant. With a live worker the swap goes through `Worker.retarget`.
- `Worker.retarget`'s hand: defers into a stop's close in flight
  (`docs/decisions/sink-lifecycle/a-hand-the-worker-build-declines-for-a-close-in-flight-is-paid-when-that-close-finishes.md`).
- The factory wrapper's build (`sinks/_factory.py`, `_FactorySink._hand` and
  `_forward_stop_signal`) is a relay, not a source: it forwards only the event the library stored
  on the wrapper, its settle re-forwards only what the wrapper now holds, and nothing there calls
  `refresh_stop_signal`. Every event that reaches a built sink went through one of the deciding
  sites here first.
- `_settle_signal`, `_hand_after_close` and `_pay_marks_after_stop` never refresh.

**The bound is `_take_ahead()`, not the whole stop.** After a stop's last take nothing in that
call takes a sink re-armed since, and a close it still has in flight is refused by `_will_hand`'s
in-flight term. Gating on `_state._stops_running` instead handed the stop's set event to an orphan
log re-arming a sink in the stop's tail, after its last take: measured,
`test_an_orphan_log_after_the_last_take_is_handed_a_fresh_signal` reddens under that gate, the sink
holding the set event rather than a fresh one, so its writer's backoffs collapse to nothing
(SPEC-033 FR-004) with no close to cut short. The worker build gates on the whole stop for a reason
this path does not have: a built worker delivers, and never passes through the orphan fast path's
repair.

**A refusal to refresh needs no payment.** The orphan fast path declines on a set signal, so the
next orphan log after the stop's last take takes the locked branch, refreshes and hands, as it does
after any `shutdown()`; `flush()` refreshes there too. No mark, and no `_signal_owed` debt, which is
paid by a refresh: that is the defect.

**What it costs.**

- **The take-ahead reaches past the closes it protects.** A no-worker stop that may still meet a
  late worker keeps its take ahead through its first pass's closes, so a sink no take of the call
  will close — configured mid-stop, while the first pass closes another — holds the set event for
  that window. Measured with the fix: three orphan logs to such a sink, each backing off 0.3 s,
  waited 0.00 s each during the first pass's close, against 0.31 s each at `a8e91fac`; after the
  `shutdown()` returned the next log refreshed it and its backoffs waited 0.30 s and 0.31 s again.
  That is inside the `shutdown()` call, where a shutdown shortening a wait is invariant 3's promise;
  the tight-loop concern of SPEC-033 FR-004 is about after it. `_take_ahead()` is kept because it is
  the existing "a take is ahead" predicate, already the swap's `DRAINING` cell, and is narrower than
  the whole stop.
- **The setter runs once per log in that window.** The fast path declines on a set signal, so each
  orphan log during a take-ahead takes the locked branch and hands the set event again: in the same
  measurement the sink's setter ran three times for the three logs, against once at `a8e91fac`.

**What is left.** The current signal is the stop's set event unless a late worker has been built
since. A late worker built during a no-worker stop's take-ahead publishes its own unset `_stop` as
`_state._stop`, which that stop sets only at the late worker's stop, after its first pass, so a hand
of the current signal before the first pass takes a sink delivers an unset event the close then
waits on. To reproduce: an orphan hand to R whose setter is still running as the stop enters,
then a `@trace` building the late worker on another sink before the first take, so the hand's
settle re-reads the late worker's unset signal; or a `flush()` while that build's hand token is
open. With a 4 s backoff in R's close and `shutdown(timeout=1)`, R's first-pass close waited
4.00 s to 4.01 s at `a8e91fac` and on this branch alike. It is recorded with the worker build's decision, which
it touches too, and left to a session of its own.

## History

### 2026-11-13

- **Settled**, as a fix after SPEC-079, with `Worker.retarget`'s deferral beside it.
