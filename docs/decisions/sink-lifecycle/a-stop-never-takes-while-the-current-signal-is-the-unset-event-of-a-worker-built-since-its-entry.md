---
type: Decision
title: "A stop never takes while the current signal is the unset event of a worker built since its entry"
description: "a `shutdown()`'s take of the owed record declines while `_state._stop` is the unset event of the worker built since the stop began (`_take_declines`), records it (`_Operation.publishing_worker`), and the stop then claims, retires if still live, and sets that worker's event in its one existing section, takes again at once, joins the worker and runs a final pass; no section marks the call drained while that worker is still to be stopped. Never forbid the build's publication, never hand each taken sink a set event at its close instead, never key the decline on the `_late_worker` slot. (fix after SPEC-079)"
---

**The defect.** A worker built after a stop's entry publishes its own unset `_stop` as the
current signal, and any hand of "the current signal" can deliver it to a sink the stop is about to
close: a settle whose setter is still running across the take, a `flush()` under the build's hand
token, a deferred payment. The close then waits on an event no stop has set. Measured at `864d71e5`
in forked children, a sink R whose `close()` backs off 4 s on its signal and `shutdown(timeout=1)`:

| Route | At `864d71e5` |
|---|---|
| late, settle: R's setter parked as a no-worker stop enters; `configure(sink=S2)` declines; a `@trace` builds the late worker on S2; the settle re-reads the signal and hands R that worker's event | 4.00 s, R's close held the unset event |
| late, flush: a `flush()` while the build's hand token is open hands owed R that event | 4.01 s, unset |
| tail: the first pass waits on a swap's displaced close; an orphan log re-arms R with its setter parked; `configure(sink=C)` declines; a `@trace` lands after the no-worker `finally` lowered `_shutdown_running` and before the second pass's take, so it builds a **live** worker T, registered nowhere; the settle hands R T's event | R's close waited 4.0 s on it, and T stayed live |
| two stops: two no-worker stops parked before their takes; the late build; R handed its event | 1.01 s each, R's close waiting until the other stop set the event at its own deadline |

At most one event is published after any stop's entry, because `_get_worker` builds only while no
worker exists and nothing but a fork resets the worker: the build's. That is the whole class.

**The rule: a stop's take never runs while the current signal is an unset event published by the
worker built since; it first stops that worker, which sets the event, and takes then.** Every
event a hand can deliver to a sink the stop takes is then set at the take, whatever route
delivered it and in whatever order setters finish: an event chosen before the stop's entry is the
object the entry set, and the one published after it is set by the worker's stop before the take.
It is a property of the close, not of the hand sites, so it covers every hand site at once,
including any added later.

**The mechanism.** `_close_owed` asks `_take_declines(op, worker)` in its take's critical section:
`op in _state._stops_running and not _state._stop.is_set() and worker is not None and
worker._stop is _state._stop`. When it is true the take takes nothing, records the worker as
`op.publishing_worker`, and leaves `op.drained` and `op.unsettled` alone, since no take was made; it
still snapshots and waits on the bystanders as before. `_shutdown_worker`, after either branch,
picks the worker to stop — the late worker it popped, or else the one its take declined for — and,
in the section that already retired a late worker, moves the retirement count only if that worker
is still live, claims its stop with the call's deadline, and sets its event, which is the current
signal. After a decline it then takes again at once, so the owed sinks close on that set event
**while the worker drains**; then it joins the worker, passing `op.budget` when it won the claim,
and the final pass closes the worker's own sink, as the second pass always did. The retake is the
call's first take that takes anything, so it **waits on closes somebody else has in flight, as a
first pass does** (SPEC-050 FR-002, invariant 5): a second stop entering between the decline and
the retake can take an owed sink first, and a retake that did not wait returned through that
close with the sink still open. Its wait ends at the call's one `grace_end`, so no second grace is
spent; the final pass after the join still never waits.

**At most two rounds and four `_close_owed` calls.** A round with a worker to stop sets that
worker's event, which is then `_state._stop`; nothing replaces a set signal while a take is ahead
and no second worker is built, so no later take of the call declines. A round with none, which
runs because the first pass waited, can meet a decline in its final pass, from a worker built
after the no-worker `finally` and before that take; the second round stops it. Counted on the
tail route: the first pass, a final pass that declines, the retake (waiting) and the final pass —
four calls in two rounds, one of them declining and taking nothing.

`op.drained` is never set while `op.publishing_worker` is, in the take's section and in the
no-worker `finally` alike, so the hands keep handing the set signal through the worker's join, as
they did through a late worker's
(`docs/decisions/sink-lifecycle/a-hand-while-a-stop-has-a-take-ahead-hands-the-current-signal-and-never-refreshes-it.md`).

No lock site and no `_move` site moved: the retire stays in `_shutdown_worker`'s one existing
section, made conditional and reached by a decline too, so the pinned counts in
`tests/test_runtime_state.py`, `tests/test_runtime_holder.py` and
`tests/test_lifecycle_transitions.py` are unchanged; a shutdown that meets no build never declines,
because its entry set the signal.

**Measured with the fix, against `864d71e5`.** Every row above closes R on a set event, held by
`test_a_stop_never_closes_on_the_unset_event_of_a_worker_built_since_its_entry` in
`tests/test_reentrant_stop_signal.py`, which asserts the event R's close held when it began
(`close:W1:set`, the worker's own event, set) and the time as a second check: settle and flush
0.00 s; tail, R's close 0.0 s and T retired and stopped; two stops 0.00 s and 0.31 s, the second
released 0.3 s later, with `retirements` at 3, as at `864d71e5`. At `864d71e5` every row reads
`close-long:W1:unset`. A probe with a 0.5 s close of R behind a late worker whose `emit` sleeps
3 s took `shutdown(timeout=1)` 1.01 s with R's close inside the budget, against 1.00 s to 1.01 s
at `864d71e5`.

Mutation, each against the whole test module, a survivor confirmed on the whole suite:

- no decline: all four rows red, `close-long:W1:unset`;
- decline on the slot (`_state._late_worker is worker`) instead of the signal: the tail row red,
  T never registered; the two-stops row stays green, since both stops decline before either pops
  the slot;
- an unconditional retire: the two-stops row red at `retirements=4`;
- no retake before the join, or no set in the section (so the retake declines again and the sink
  waits for `Worker.stop`'s set): `test_a_declined_take_closes_the_owed_sinks_while_the_worker_drains`
  red, the late worker's `emit` never seeing R closed within 0.5 s;
- `drained` set at the retake: `test_a_stop_keeps_its_take_ahead_through_the_join_of_a_worker_its_take_declined_for`
  red, a sink configured during the join closed on a refreshed unset event after 4 s;
- `drained` set in the no-worker `finally` of a stop whose own late worker another stop popped:
  `test_a_stop_that_declined_keeps_its_take_ahead_after_the_stop_that_popped_its_worker_ends` red,
  the same 4 s;
- the retake without its bystander wait (as at `1e559923`):
  `test_a_declined_stops_retake_waits_for_a_close_another_stop_started` red, the declined stop
  returning before another stop's 1 s close of R had finished;
- the token-set term or the identity term dropped: the whole suite green but for the predicate
  roster's re-keyed text. Both are guards, below.

**Rejected.**

- **Close-side: hand each taken sink a set event before its close.** A settle that passed
  `_may_replace` before the take can still be inside the setter when the take runs, and its offer
  lands after the closing thread's: the sink is closed on the unset event anyway, and the setter
  runs on two threads at once. It would also run a setter on every shutdown close.
- **A late build keeps a private event and does not publish it** while a take is ahead. That
  breaks the identity `live_worker()._stop is _state._stop` that `_may_replace`,
  `_pay_marks_after_stop` and `_hand_after_close` rest on, so the deferred payment would refuse to
  hand the worker's sink its own event, which the build-declines decision measured as required; and
  the tail worker is live, so its event would still need publishing once the take-ahead ends, by a
  new publication site.
- **Decline on the `_late_worker` slot.** Misses the tail worker, which is never registered:
  measured as a mutant, above, where a re-entrant `shutdown()` inside a close also hung
  (`test_a_setter_reentering_the_library_returns`, `close-end-shutdown`).
- **Extend the late window to the last take**, keeping `_shutdown_running` raised until the second
  pass's take. It makes the tail worker a late one, but needs a new lock site in an outer `finally`
  for interrupt safety and covers nothing the decline does not.
- **Join the worker, then take.** The plan's first prototype: a 0.5 s close of R behind a late
  worker whose `emit` sleeps 3 s took `shutdown(timeout=1)` to 1.51 s, R's close starting after the
  deadline. Claim, retire and set at once, take, then join is the worker branch's own order.
- **Mark the call drained at the retake.** The prototype did, since the retake is a pass with no
  bystander wait: during the worker's join a `configure(sink=Q)` went ahead and refreshed the
  signal, and the final pass closed Q on that unset event, 4.3 s against 0.3 s at `864d71e5`.

**What it costs.**

- **A sink's close moves into the worker's drain.** R's close used to run before the late worker
  was stopped; now it runs once the section has set that worker's event, while it drains, inside
  the same call's budget.
- **A `@trace` landing in the few bytecodes between a no-worker stop's first-pass `finally` and its
  final take builds a worker that stop now retires and stops**, where it used to stay live. A
  `@trace` a moment earlier, during the first pass, already got that outcome as the late worker
  (SPEC-044 FR-001), so the boundary of "a worker this stop retires" moves from the end of the
  registration window to the stop's last take. Every later event in that process is then counted
  in `health().submitted_after_shutdown` rather than delivered, which is the outcome `State`'s
  docstring reserves for a `shutdown()` that stopped a worker (SPEC-078 FR-004's `CLOSED`
  arrows); the release notes say it.
- **The retire is conditional.** Two stops can now stop one worker, so the count moves once per
  worker; a late worker an earlier call already retired is no longer counted a second time, which
  nothing read (`live_worker` compares with `>`).
- **A late worker's expiry line names the caller's timeout** (`op.budget`), as the worker
  branch's does, rather than what was left of it: `shutdown timed out after 1s` where it read
  `after 0.493832s`.
- **The roster grows ten sites**, `tests/test_worker_predicate_roster.py`; the attribute is
  `publishing_worker`, not `publisher`, so the walker sees every test of it.

**Excluded, named rather than claimed away.**

- **A stop continuing on a forked child's thread.** The child empties `_stops_running`, so its
  takes never decline, and the token-set term keeps a parent's stop from stopping the child's own
  worker. That continuation's closes may meet the child's unset signal. Contrived: the fork must
  happen on the stopping thread, inside the stop.
- **An interrupt raised inside the section between the claim and the set** leaves the event
  unset. The section is a few bytecodes under `_state._lock`.

**Guards that no test fires.** The identity term `worker._stop is _state._stop` is the loop's
termination guard: without it a take facing an unset signal some other writer published would
decline, set an event that is not the signal, and decline again forever. No such writer exists
today, since every refresh is gated off while a take is ahead. The token-set term is equivalent in
every parent-process route; its job is the fork exclusion above.

## History

### 2026-11-13

- **Refined** the same day, after the system-frame diff review: the retake after a decline waits
  on closes another caller has in flight, as a first pass does, since it is the call's first take
  that takes anything; and the loop is bounded at two rounds and four `_close_owed` calls, not
  one round and three takes, which the tail route exceeded.
- **Settled**, as a fix after SPEC-079, closing the late-build case both
  `a-hand-while-a-stop-has-a-take-ahead-hands-the-current-signal-and-never-refreshes-it.md` and
  `a-hand-the-worker-build-declines-for-a-close-in-flight-is-paid-when-that-close-finishes.md`
  recorded as left.
