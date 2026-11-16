---
type: Spec
id: SPEC-098
title: "One Stable Stop Signal per Sink, Handed Once per Installation"
description: "Gives every sink the library delivers to one threading.Event of its own, assigned through log_foundry_stop_signal at its first delivery and again only when a configure() installs it anew, set by every stop, cleared by such a configure() when no stop is running, and kept in a forked child by re-initialising it in place, so no fork repair runs sink code and the machinery that chose which event a sink holds is deleted, while a 1.x sink written to the README's protocol keeps working unchanged."
spec_status: Draft
depends_on: [SPEC-027, SPEC-033, SPEC-042, SPEC-054, SPEC-079, SPEC-081, SPEC-083, SPEC-085, SPEC-086, SPEC-097]
arc: audit-2026-11-14
arc_order: 14
---

## Overview

A sink that retries waits between attempts on an event the library gives it, so that a `shutdown()`
cuts the wait short. Today that event is the current worker's stop event, and it changes: a worker
built after a stop has a fresh one, a forked child has its own, and every place the library reaches
a sink hands it whichever is current, through the sink's `log_foundry_stop_signal` setter, which is
the sink's own code. Six of the eleven lifecycle fixes after the 1.2.0 arc were about which event a
sink held at a given moment, and each added state: a debt to re-hand, a token while a hand is open, a
four-round settle loop, marks paid when a close finishes, the events a child forked inside a setter
keeps. The 2026-11-14 audit's lifecycle frame recommended the change Andrew took that day: one stable
event per sink, handed once. This spec builds it. Each sink the library delivers to gets one event of
its own. Every stop sets every such event, and it stays set for whatever is delivered after the stop,
until a `configure()` installs that sink again with no stop running, which clears it and hands it
again. A forked child re-initialises the same object instead of handing a new one, and the
machinery that chose which event a sink holds is deleted. The README's protocol keeps its name, its
type and its promise that the event is set when a shutdown starts, so a sink written to it in 1.x
keeps working.

## Scope

### In Scope

- Each sink the library delivers to holds one `threading.Event` of its own, assigned through
  `log_foundry_stop_signal` at its first delivery; later deliveries assign nothing; no sink is reached
  before it holds it, on the orphan path, the worker build, either branch of a swap or a fork resume
  (FR-001).
- Every stop sets every event the library made; an event made while a stop runs is made set; it
  stays set for deliveries after the stop; a `configure()` that installs a sink again, with no stop
  running, clears the events of that sink and of every sink its graph reaches and hands them again
  (FR-002).
- A forked child keeps every event object the library made, re-initialised in place before the
  repair walk, whichever sink holds it; the repair runs no setter; the child's stop sets them, and the
  parent's are untouched (FR-003).
- The protocol stays the README's: name, type, "set when shutdown starts"; the README, the
  invariants page, `architecture.md` and the component inventory say what changed; a 1.x sink keeps
  working, and invariant 10 takes no recorded exception (FR-004).
- The machinery whose only job was to choose which event a sink holds is deleted, one offer replaces
  it at each delivery site, and the decisions that fence it, or state it in prose, are rewritten in
  place (FR-005).

### Out of Scope

- **When a lifecycle call may run.** That is SPEC-097's command path, which this spec builds on: its
  re-entry sweep is FR-005's check, and its command gate is what lets FR-002's clear run with no
  stop in flight.
- **The worker's own stop event.** `Worker._stop` still wakes the drain thread and is what the
  delivery engine waits on for the worker path (`worker.py:2405`). The engine's waits on the orphan
  path and in the inline adapter read the sink's attribute, as today, and so read the stable event.
- **Setting a displaced sink's event at a swap.** A swap does not stop delivery, so it cuts no wait,
  as today.
- **Clearing a sink's event for a delivery after a stop with no `configure()`.** It stays set, by
  Andrew's answer of 2026-11-16; FR-002 says what that costs.
- **A sink the library never delivers to directly.** A sink reached only through a caller's wrapper
  holds what that wrapper's setter forwards, as the README already says.

---

## Functional Requirements

### FR-001: A sink holds one event of its own, assigned at its first delivery, and is never reached without it

#### Description:

`offer_stop_signal(sink, stop)` is the one place the library runs a sink's setter. It now ignores
the event its caller passes. It finds the sink's own event in a table beside the ownership record,
`Runtime._signals`, keyed by `id(sink)` with a weak reference to tell a reused id from the sink (a
strong one for a sink that cannot be weakly referenced), and creates the event the first time. It
assigns that event, and records the sink as handed once a setter given that event has **returned**.
Two threads reaching a sink's first delivery at once may both assign, and assign the same object:
marking the sink handed before its setter returned let a second thread's log reach the sink before
it held any event (measured: `test_another_thread_hands_the_signal_before_it_emits` read
`held=[False]` on all three rows until the mark moved to the setter's return). A thread already
inside an offer of a sink does not offer it again, which is today's per-thread offer record.

**One offer site per delivery path, `offer_once(sink)`.** It reads the table lock-free, one
dictionary read and one weak-reference call, and returns at once for a sink already handed; only a
first hand takes `_owned_lock`. Its callers are every place a sink is first reached:

- the orphan path, `_note_orphan_emit`, which arms the sink under `_state._lock` only when the record
  does not owe it, then calls `offer_once`, so an orphan log on an armed, handed sink takes no lock
  for its signal;
- the worker build, after its section;
- the no-worker swap, after the section that arms the sink it installs, and `Worker.retarget` on the
  worker branch;
- the fork resume, which finds the sink already handed and assigns nothing;
- the factory wrapper, for the sink its factory built.

**Dead entries are pruned whenever a sink gets its first entry.** The spec reviewer's `twins.py`
`table-growth` row, 3,000 sinks configured and logged to in turn, read `entries=123 dead=122` before
the prune and `entries=2 dead=1` after it: the one dead entry is the last sink, released after the
final sink's first entry was made.

#### Acceptance Criteria:

- [ ] `spec097_stable_signal.py`'s `sequence` row, `configure(S)`, an orphan log, a traced call,
      `flush()`, `configure(T)`, `configure(S)`, an orphan log, `shutdown()` and an orphan log after
      it, reads `assignments=2 distinct_events=1`: one hand at S's first delivery and one when
      `configure(S)` installs it again (FR-002). It reads 4 and 2 at `ead40798` (invariant 3).
- [ ] Its `swap-orphan`, `swap-worker` and `swap-multi` rows read `held_when_configure_returned=` all
      `True`: every sink a swap installs, on either branch and inside a `MultiSink`, holds an event
      when `configure()` returns, before anything is delivered to it. With the no-worker swap's offer
      removed, `swap-orphan` and `swap-multi` read `False`; with `Worker.retarget`'s removed,
      `swap-worker` does (both mutants measured on the prototype) (invariants 3 and 6).
- [ ] `test_another_thread_hands_the_signal_before_it_emits` passes on its three rows (invariants 3
      and 6).
- [ ] A test asserts that a sink whose setter raises an `Exception` is offered once, the failure
      announced once, and that one whose setter raises `KeyboardInterrupt` is offered again at its
      next delivery (invariants 1 and 11).
- [ ] After 3,000 sinks are each configured, logged to and released, and one more is configured and
      logged to, the table holds at most one entry whose sink has been collected (invariant 5).
- [ ] `_note_orphan_emit` on a sink already armed and handed takes no lock: a test counting
      acquisitions of `_state._lock` and `_owned_lock` across 1,000 such calls reads 0 (invariant 4).

### FR-002: Every stop sets every event; a `configure()` that installs a sink again clears it, and a late delivery finds it set

#### Description:

`_shutdown_worker`'s entry section, which sets `_state._stop` today, also sets every event the
library has made (the weak set of FR-003), taking `_owned_lock` inside `_state._lock`, the order the
process already uses. An event made while a stop is running is made set, so a sink first reached
during a drain backs off on a set event, as the take-ahead rule makes it today.

**A `configure()` that installs a sink again clears its event, by Andrew's call of 2026-11-16.** At
the start of the sink change, after the ownership stamp, the change clears the event of the sink it
installs and of every sink the graph walk reaches from it that already has an entry, and marks each
unhanded, so its next delivery offers it the cleared event again. The clear runs only when no stop is
running. **What makes it unable to race a stop's set:** the check of `Runtime._stops_running` and the
clear are one `_owned_lock` section, and a stop adds itself to `_stops_running` before its own
`_owned_lock` section sets every event, and leaves it only when the stop returns. So a clear either
finds the stop and does nothing, or runs entirely before the stop's set, which sets the event again.
Under SPEC-097 a sink change is published before its config write and a stop meets it, but a change
and a stop can still overlap (a stop's wait for a change is bounded), so it is this section, not the
command gate, that the guarantee rests on.

The cases:

- **A sink a wrapper still holds.** Installing the sink again clears its own event and hands it again;
  an earlier wrapper's event, which the sink may still hold through a forward, is replaced by that
  hand. `twins.py`'s `multi-member-then-direct` reads `A_first_is_A_after=True`, as at `ead40798`.
- **A sink inside a `MultiSink`.** The graph walk reaches it, so `configure(MultiSink(P, Q))` clears
  P's and Q's events as well as the wrapper's; the wrapper's hand then forwards its own event onto
  them.
- **A `configure()` that runs a deferred stop (SPEC-097 FR-002).** The clear is at the change's start
  and the deferred stop at its end, so the stop the sink's own code asked for sets the event again.
- **A forked child that calls `configure()`.** The child's stops are cleared by its fork handler, so
  its `configure()` clears the child's copy of the event; the parent's is untouched. `twins.py`'s
  `shutdown-then-fork-reconfigure` reads `waited=2.00s signal_set=False`, as at `ead40798`.

**What stays set, and what that supersedes.** A delivery after a stop with no `configure()` between,
a late worker's or a log made after the shutdown, finds the event set. The sink's own waits collapse
and its attempts are still made, and so do the delivery engine's waits on the orphan path, which
read the sink's attribute (the reviewer's `engine_after.py` orphan row reads `took=0.00s`, against
0.31 s at `ead40798`). Worker-path rounds wait on `worker._stop` and are unchanged. A child forked
after its parent's `shutdown()` inherits the set event: `twins.py`'s `shutdown-then-fork` reads
`waited=0.00s`, against 2.00 s at `ead40798`, and `fork_after_shutdown.py` leaf/orphan 0.00 s against
2.00 s. Two shipped statements are superseded for that case only:

- SPEC-033 FR-004 AC-6, "a sink emitted to after `shutdown()` holds an unset event and still backs
  off", in its second case, "the same sink logged to again". Its first case, a newly configured sink,
  still holds, and so does the same sink configured again.
- `docs/decisions/sink-lifecycle/a-hand-the-worker-build-declines-for-a-close-in-flight-is-paid-when-that-close-finishes.md`,
  lines 156 to 157, which rejects declining without paying because "the late worker then delivers
  into a sink still on the shutdown's set signal".

On a client-buffered sink the late case costs more than time. SPEC-094's rounds for `KafkaSink` and
`GooglePubSubSink` wait for reports "until the stop signal", so a late delivery's rounds read the
rest `UNKNOWN` after one slice, as SPEC-094 already says of the exit drain; the shipped
`GooglePubSubSink` ends its flush and overflow passes on a set signal the same way. That is why a
`configure()` clears the event: before the clear, a `GooglePubSubSink` installed again after a
`shutdown()` counted publishes lost that confirm today (Evidence). SPEC-094 needs no line, since its
rule reads the sink's signal and this spec changes only when that signal is set.

#### Acceptance Criteria:

- [ ] `spec097_stable_signal.py`'s `cut` row reads `wait ended 0.00s after shutdown() was called`
      (invariant 3).
- [ ] `spec097_pubsub_after_shutdown.py` reads as at `ead40798` at each of its four confirm delays,
      1.0, 2.5, 3.0 and 3.9 s: `confirmed=7 failed=0`, with `flush()` truthy at 1.0 s and falsy
      `sink-flush` at the other three (invariants 2 and 7).
- [ ] `twins.py`'s `shutdown-then-fork-reconfigure` reads `waited=2.00s signal_set=False`, and its
      `shutdown-then-fork` reads `waited=0.00s signal_set=True`, and a test asserts each (invariants
      3 and 12).
- [ ] A test forces a stop's entry between a clear's check of `_stops_running` and its clear, by
      parking the clear inside a patched helper, and asserts the sink's event is set when the stop
      returns; with the check moved out of the `_owned_lock` section it fails (invariant 3).
- [ ] A test asserts that after `configure(MultiSink(P, Q))` installs P and Q again after a
      `shutdown()`, neither P's, Q's nor the wrapper's event is set before the next stop (invariant 3).
- [ ] Every `waiter` cell of SPEC-085 passes, in the parent and in a forked child (invariant 3).
- [ ] A test parks a first delivery between the creation of its event and the stop's entry, on each
      side of the lock, and asserts the event is set when the stop returns (invariant 3).
- [ ] SPEC-033's FR-004 AC-6 is struck in place for "the same sink logged to again", naming this
      spec, and `test_a_sink_emitted_to_after_shutdown_still_backs_off` is rewritten to assert the
      newly configured sink and the reconfigured sink back off and the late-logged sink does not;
      the decision's lines 156 to 157 are marked superseded for a late delivery, dated (invariant 3).

### FR-003: A forked child keeps every event the library made, re-initialised in place, and runs no setter

#### Description:

The repair walk replaces each lock and event it finds on a package object with a fresh one, through
a memo shared with the global roster (`_fork._fresh_primitive`), and today `_reinit_after_fork` and
the debt it leaves hand the child a fresh signal through each sink's setter, inside the fork repair
or at the child's first delivery. That is what SPEC-083 and §12's *A second `shutdown()` made from a
stop-signal setter during a forked child's fork repair* are about.

**Every event the library makes is in a weak set, `Runtime._signal_events`, and the seed walks that
set, not the sink table.** Before the walk, the child re-initialises each live event in the set in
place (`threading.Event._at_fork_reinit`, which keeps its flag) and enters it in the walk's memo as
its own replacement, so the walk leaves any attribute pointing at that same object. The stop's set
walks the same set. Seeding by the sink table, the first prototype dropped the event of a released
wrapper W while its member A still held it through W's forward: in a child, nothing re-initialised or
set it, and the reviewer's `orphanwrap.py` read 4.72 s and 4.71 s on its `multi-child` and
`filter-child` rows against 0.00 s at `ead40798`. Seeding by event reads 0.00 s on all four rows. The
alternative, re-offering a sink whenever its attribute is not its table event, would read the
attribute, which can be sink code, on every delivery. The set and the table are both in
`Runtime._FORK_SKIP`, since a walk into the table would reach superseded sinks and run their hooks.

No setter runs in the repair or at the child's first delivery, because the sink is already handed.
The child's own `shutdown()` sets the events, which are the child's copies; the parent's are not
touched.

#### Acceptance Criteria:

- [ ] `spec097_stable_signal.py`'s `child-orphan` and `child-worker` rows read `setter_calls=0
      same_event_object=True wait_ended=0.00s after its shutdown(); parent_event_set_by_child=False`
      (at `ead40798` the setter runs 1 and 2 times and the object differs) (invariants 3 and 12).
- [ ] `orphanwrap.py`'s `multi`, `multi-child`, `filter` and `filter-child` rows each read
      `shutdown_took=0.00s`, and a test asserts the two child rows (invariants 3 and 12).
- [ ] `lifecycle/p9_second_stop_in_fork_repair.py`'s three child rows read `repair returned` within
      0.1 s, with no setter call (invariants 3 and 12).
- [ ] `tests/test_fork_mid_hand.py::test_a_child_sets_an_offered_event_whose_condition_a_parent_thread_held`
      is rewritten, not deleted, to assert that a child forked while a parent thread holds the sink's
      event's condition returns from its `shutdown()` with that event set; the reviewer's
      `heldcond.py` reads `returned in 0.00s set=True same=True` (invariant 12).
- [ ] The `forker` cells and the `child-` `waiter` cells of SPEC-085 pass (invariant 12).
- [ ] `tests/test_fork_roster.py` holds the table and the set as declared state, and a fixture that
      drops either from `_FORK_SKIP` makes the roster gate fail for that reason (invariant 12).

### FR-004: The protocol keeps its name, type and promise, so a 1.x sink keeps working

#### Description:

The README documents the attribute (`README.md:1116-1156`, and the 1.0.0 note at `:80`): optional,
an attribute, a `threading.Event | None`, assigned by the library, set when shutdown starts, "may be
assigned more than once, from more than one thread at the same moment", and forwarded by a wrapper's
property. Every one of those stays true, so a sink written against the 1.x text works unchanged and
invariant 10's observable holds. Invariant 10 takes no recorded exception: Andrew's acceptance of
2026-11-14 allowed a break in this protocol, and this form uses none of it.

What a reader of the old text could have relied on and cannot now:

- that the event is the worker's, which nothing exposes;
- that a delivery after a shutdown with no `configure()` finds an unset event (FR-002);
- that a child's sink is assigned a new event (FR-003);
- that a sink which sets its own attribute to `None` is handed again by a later delivery. It is
  handed again only when a `configure()` installs it again: `twins.py`'s `reset-to-none-then-shutdown`
  reads `held_at_backoff=True shutdown_took=0.00s`, as at `ead40798`, because its sink is configured
  again; one reused by a late log alone keeps `None`.

The caller-visible changes, for `docs/release-notes/v1.2.1.md`:

- A sink's `log_foundry_stop_signal` setter runs at the sink's first delivery and again when a
  `configure()` installs it anew, with an event that is the sink's own, where it ran at every
  refresh with the worker's event; two threads making a first delivery at once may both run it, with
  the same event.
- After a `shutdown()`, a sink delivered to with no `configure()` between finds its event set. Its
  own backoff waits collapse, and so do the delivery engine's waits on the orphan path; its attempts
  are all still made, and worker-path rounds still wait on the worker's stop event. A sink that a
  `configure()` installs again after the shutdown finds its event cleared and backs off as before.
- A sink first reached while a shutdown is running is given an event already set.
- A forked child's sink keeps the same event object, re-initialised, and its setter does not run in
  the child; the child's `shutdown()` sets it and the parent's stays as it was. A child forked after
  its parent's `shutdown()` inherits the set event until it configures the sink again.

#### Acceptance Criteria:

- [ ] `tests/typed_consumer/` and `tests/test_public_surface.py` pass, the latter's stop-signal test
      rewritten to assert a sink is handed its own event (invariant 10).
- [ ] The reviewer's `readme_sinks.py`, the README's leaf sink and its wrapper example run unchanged
      on the orphan and the worker path, reads `shutdown took 0.00s` on all four rows, as at
      `ead40798`, and a test asserts it (invariants 3 and 10).
- [ ] `README.md`'s protocol section and its 1.0.0 note state "its own", "set when a shutdown starts",
      "stays set for a delivery after it" and "cleared when `configure()` installs it again", and
      `docs/release-notes/v1.2.1.md` carries the four changes above (invariant 10).
- [ ] `docs/invariants.md` invariant 3, whose lines 214 to 217 say every backoff waits on "the
      worker's stop event", says a sink's backoff waits on its own event, set by every stop and
      cleared by a `configure()` that installs it again; `architecture.md:335` and `:658` and
      `docs/component-inventory.md:42`, which say the same of the worker's event, are rewritten; a
      `grep` for "worker's shutdown event" and "worker's stop event" over those three files finds
      only sentences about the worker path's own waits (invariant 3).

### FR-005: The machinery that chose which event a sink holds is deleted, and its decisions rewritten

#### Description:

Once a sink is handed one event per installation, the mechanisms whose job was to choose, refresh,
defer or re-pay a hand have nothing to do, and the prototype deletes them: `_hand_out`,
`_settle_signal` and `_SETTLE_ROUNDS`, `Runtime._hands_running`, `_signal_owed`, the `hand_owed`
marks with `defer_hand`, `marked` and `unmark`, `_hand_after_close`, `_pay_marks_after_stop` and
`_pay_a_mark_the_stop_missed`, `_hand_retargeted`, `_stale_signals`, `_fork_offers` and
`_fork_inherited`, and with them `_will_hand`, `_may_replace`, `_owed_candidates`, `_refreshes`,
`_open_hand`, `handing_here`, `_in_handing`, `handing`, `_mid_hand`, `_arm_and_hand` and the factory
wrapper's settle loop. `refresh_stop_signal` and `_take_declines` stay: each is read for the worker's
own event. `rearm_if_released` stays, called after a first hand.

The decisions that fence these mechanisms, or state the old hand in prose, are found by one search,
run at build time and reproducible at `ead40798`:

```
grep -l '_hand_out\|_settle_signal\|_SETTLE_ROUNDS\|_hands_running\|_signal_owed\|hand_owed\|defer_hand\|_hand_after_close\|_pay_marks_after_stop\|_hand_retargeted\|_stale_signals\|_fork_offers\|_fork_inherited\|_offering_here\|handing_here\|refresh_stop_signal\|_refreshes\|_take_declines\|log_foundry_stop_signal\|stop signal\|stop-signal\|stop event' docs/decisions/*/*.md
```

At `ead40798` it returns 20 files. Each that states a behaviour this spec removes is rewritten in
place with a dated History line, or, where nothing of it remains, marked superseded by this spec
with its reasoning kept; each it leaves is listed in the delivery doc with why.

#### Acceptance Criteria:

- [ ] `grep -rn` for each deleted name over `src/` finds nothing, and the delivery doc lists every
      name above with "deleted" or the reader that keeps it; the criteria below hold the behaviour the
      deleted code kept (invariants 3 and 12).
- [ ] After the deletion, every SPEC-085 cell's verdict equals its verdict on SPEC-097's built tree,
      cell by cell, the passive model gate passes, and SPEC-097's `spec097_reentry_sweep.py` reads
      `failed=0` with every section it does not reach running only on the drain thread or in a
      forked child's repair (invariants 3, 5 and 12).
- [ ] The suite's failures on the deletion are rewritten or deleted by what each asserts, the
      delivery doc listing each: a setter run again becomes one hand per installation; an internal
      of the deleted machinery is deleted with it; a sink holding the worker's event becomes its own
      event; a wait after a stop is rewritten to FR-002's outcome, the late delivery collapsing and
      the reconfigured sink waiting (invariant 6).
- [ ] Each file the search returns is rewritten in place with a History line dated the day the build
      lands, or marked superseded, or listed with why it stays (invariant 6: a rewritten decision names
      both paths a hand ran on).
- [ ] `architecture.md` §12 moves *One stable stop signal per sink changes a documented public
      protocol*, *A kept sink a straddling orphan log put back in the record can be handed a stop
      signal on its own*, saying it was closed by construction and never reproduced, and *A second
      `shutdown()` made from a stop-signal setter during a forked child's fork repair* to
      *Resolved*, naming this spec (invariants 3 and 12).

---

## Data Model

```python
# src/log_foundry/_lifecycle.py
class Runtime:
    _FORK_SKIP = ("_owned", "_signals", "_signal_events")
    _signals: dict[int, tuple[weakref.ref[object] | object, threading.Event, bool]]
    #          id(sink) -> (the sink, weakly where it can be; its one event; handed: a setter given
    #          that event has returned since the last install). Guarded by _owned_lock.
    _signal_events: weakref.WeakSet[threading.Event]   # every event made; the stop and the seed walk it

def offer_stop_signal(sink: Sink, stop: threading.Event) -> None: ...   # `stop` is ignored
def offer_once(sink: object) -> bool: ...          # lock-free when handed; the one site per path
def reinstall_signals(sink: object) -> int: ...    # configure(): clear and unhand the graph, no stop running
```

## API / Interface Contract

The attribute is unchanged:

```python
class MySink:
    log_foundry_stop_signal: threading.Event | None = None   # this sink's own event

    def _backoff(self, delay: float) -> None:
        if self.log_foundry_stop_signal is not None:
            self.log_foundry_stop_signal.wait(delay)    # returns early once a shutdown has started
```

## File & Folder Structure

```
src/log_foundry/
├── _lifecycle.py      # the table, the event set, offer_once, the set at a stop, the clear at a
│                      # reinstall, the seed before the walk; the deletions
├── config.py          # the sink change calls reinstall_signals
├── worker.py          # the build, retarget and resume reduce to offer_once
└── sinks/_factory.py  # the wrapper's hand reduces to offer_once
tests/
├── test_stable_stop_signal.py   # FR-001 to FR-004 (new)
├── test_reentrant_stop_signal.py, test_fork_mid_hand.py, test_sink_retry.py, ...   # FR-005 rewrites
docs/audits/2026-11-14-probes/core-pipeline/spec097_stable_signal.py, spec097_pubsub_after_shutdown.py
```

## Implementation Phases

### Phase 1: The stable event (FR-001, FR-002, FR-003)

- The table, the event set, the hand that assigns once and marks at the setter's return, the prune,
  `offer_once` at every delivery site, the set at a stop's entry, the clear at a reinstall.
- The seed before the walk in a child, and `_FORK_SKIP`.

### Phase 2: The protocol and the deletion (FR-004, FR-005)

- The README, the invariants page, `architecture.md`, the inventory, the release notes.
- The deletions, one mechanism at a time, each followed by the cells and the suite; the rewritten
  tests; the decisions; §12.

---

## Evidence

Measured against `ead40798` and this spec's part of
`docs/audits/2026-11-14-probes/core-pipeline/spec097_prototype.patch` (SPEC-097's Evidence says how
it is applied). The patch now carries the deletion: it is built on SPEC-097's part, which it needs,
so this spec's part with the deletion does not apply alone. Its part without the deletion was also
measured alone. Python 3.13.1 on macOS. The spec reviewer's probes named here are in that review's
scratch tree and are not committed; their ports are this spec's tests.

**`spec097_stable_signal.py`:**

| Row | `ead40798` | prototype |
|---|---|---|
| `sequence` | `assignments=4 distinct_events=2 set_after_shutdown=False wait_after_shutdown=2.00s` | `assignments=2 distinct_events=1 set_after_shutdown=True wait_after_shutdown=0.00s` (the last log is late, with no `configure()`) |
| `cut` | wait ended 0.00 s after `shutdown()` | the same |
| `child-orphan` | `setter_calls=1 same_event_object=False wait_ended=0.00s` | `setter_calls=0 same_event_object=True wait_ended=0.00s` |
| `child-worker` | `setter_calls=2 same_event_object=False wait_ended=0.00s` | `setter_calls=0 same_event_object=True wait_ended=0.00s` |
| `swap-orphan`, `swap-worker`, `swap-multi` | every installed sink held | every installed sink held |

**The `GooglePubSubSink` losses that changed Andrew's answer.** `spec097_pubsub_after_shutdown.py`:
P is handed, held by a caller's wrapper that forwards the signal and leaves P open, the `shutdown()`
sets P's signal, then P is configured again, five orphan logs publish, `flush()` runs and the exit
drain closes P; a fake publisher's futures confirm after the given delay; `overflow_timeout` is 2 s.

| Confirm delay | `ead40798` | signal kept set (before option 2) | option 2 |
|---|---|---|---|
| 1.0 s | `confirmed=7 failed=0`, `flush()` truthy | `confirmed=7 failed=0`, `flush()` falsy `sink-flush` | as `ead40798` |
| 2.5 s, 3.0 s, 3.9 s | `confirmed=7 failed=0`, `flush()` falsy `sink-flush` | `confirmed=0 failed=7`, "7 publish(es) still in flight when the 2.0s close bound expired" | as `ead40798` |

With the signal kept set, the flush and overflow passes (`pubsub.py:510-528` and `:657`) ended at
once, the futures went back, and the exit drain's close bound started almost at once, so every
publish confirming between `overflow_timeout` and twice that went from confirmed to counted lost.

**The reviewer's rows**, on the prototype with the deletion and option 2:

| Probe and row | `ead40798` | prototype |
|---|---|---|
| `orphanwrap.py` `multi`, `multi-child`, `filter`, `filter-child` | 0.00 s each | 0.00 s each (4.72 s and 4.71 s on the child rows when seeded by the sink table) |
| `twins.py` `shutdown-then-fork` | `waited=2.00s signal_set=False` | `waited=0.00s signal_set=True` |
| `twins.py` `shutdown-then-fork-reconfigure` | `waited=2.00s` | `waited=2.00s` |
| `twins.py` `reset-to-none-then-shutdown` | `held_at_backoff=True shutdown_took=0.00s` | the same (4.71 s before option 2) |
| `twins.py` `table-growth` | no table | `entries=2 dead=1` (`entries=123 dead=122` before the prune) |
| `twins.py` `multi-member-then-direct` | `A_first_is_A_after=True` | `A_first_is_A_after=True` (`False` before option 2) |
| `readme_sinks.py`, four rows | 0.00 s | 0.00 s |
| `heldcond.py` | `returned in 0.00s set=False` | `returned in 0.00s set=True` |
| `engine_after.py` orphan | `took=0.31s` | `took=0.00s` |
| `fork_after_shutdown.py` leaf/orphan, engine/orphan | 2.00 s, 0.30 s | 0.00 s, 0.00 s |

`p9_second_stop_in_fork_repair.py`: at `ead40798`, `two-stops-second-none` never leaves the repair in
6 s and `two-stops-second-finite` takes 3.06 s; on the prototype all three child rows read `repair
returned` in 0.05 to 0.06 s with no setter call.

**The cells.** This spec's part without the deletion, alone, moves none of the 637 cells or the
reproduction: every verdict equals `ead40798`'s. With SPEC-097's part, the deletion and option 2,
every verdict equals SPEC-097's part alone, cell for cell.

**The re-entry sweep** on the prototype with the deletion: 29 sections reached, 0 failed, 22 not
reached, none of them in the hand machinery, which is gone.

**The suite** with the deletion and option 2: 157 of 11,520 failed, SPEC-097's 41 and 116 of this
spec's, sorted by what each test asserts, read from its source and the helpers it calls, with the six
the reviewer named that pin `waited=[0.3]` moved by hand from the deleted machinery to the waits:

| Class | Tests | Becomes |
|---|---|---|
| a setter is run again | 31 | one hand per installation |
| an internal of the deleted machinery | 22 | deleted with it |
| a roster or inventory reads the new state | 21 | re-derived |
| a sink holds the worker's event | 19 | the sink's own event |
| a wait after a stop | 15 | FR-002's outcome: a late delivery collapses, a reconfigured sink waits |
| other: the take-decline's four rows, whose close now runs on the sink's own set event; two lints of the deleted payment; two roster gates | 8 | rewritten or re-derived |

The fifteen waits are `test_a_sink_emitted_to_after_shutdown_still_backs_off`,
`test_a_sink_a_retired_worker_holds_still_gets_a_usable_signal`,
`test_an_orphan_log_during_the_drain_does_not_strand_the_backoff`,
`test_an_orphan_log_during_the_drain_leaves_the_stop_signal_alone`,
`test_a_late_hand_after_a_fresh_worker_still_leaves_the_current_signal`,
`test_a_worker_built_in_a_stops_tail_is_handed_its_signal_when_the_stop_ends`,
`test_a_hand_before_the_take_leaves_the_close_the_shutdowns_signal[orphan-late]`,
`test_a_resumed_drain_through_a_factory_wrapper_never_reenters_the_setter`,
`test_a_shutdown_does_not_wait_out_a_no_worker_swaps_slow_setter`, and the six:
`test_a_mark_made_after_the_stops_end_looked_is_still_paid[build]` and `[retarget]`,
`test_a_stop_end_payment_is_owed_only_to_a_hand_of_the_sink_it_pays[in-r]`,
`test_a_build_inside_a_setters_hand_still_marks_the_close` and
`test_a_stop_end_payment_inside_a_hand_is_paid_when_the_hand_ends[writer]` and `[stop]`. By its
source, each asserts a wait, or an unset event, for a delivery after a stop with no `configure()`
between, or for one whose `configure()` ran while the stop was still running, where no clear is
made. The sort is the author's, read from the sources, not a reviewer's.

**Costs.** `_note_orphan_emit` on an armed, handed sink, isolated by `spec097_cost.py orphan-note`:
56.8 to 57.8 ns at `ead40798` and 88.3 to 90.1 ns with the deletion, three runs each, about 32 ns
more and no lock. End to end, three interleaved runs: an orphan `info()` 7.84 to 8.01 µs at
`ead40798` and 8.28 to 8.76 µs on the prototype with both specs; a traced call with one log 23.7 to
24.5 µs and 25.5 to 26.0 µs, which includes SPEC-097's 0.4 µs. A stop's entry walks the event set
once under `_owned_lock`, and a reinstall walks the installed graph once.

**What is not proven.** The late-delivery cost on SPEC-094's built `KafkaSink` and
`GooglePubSubSink`, which do not exist yet. A late delivery to a `GooglePubSubSink` that a
`shutdown()` left open: the probe's reuse goes through `configure()`. That the sort above matches
each test's intent beyond what its source says. A run on Linux.

---

## Revision history

- 2026-11-16, authored with SPEC-097 as one split (SPEC-097's Revision history says why).
- ~~**Andrew's call, 2026-11-16:** a sink's stop signal stays set after `shutdown()`.~~ Replaced the
  same day.
- **Andrew's call, 2026-11-16 (option 2), replacing the answer above:** a sink's stop signal stays
  set for a delivery after a stop, but a `configure()` that installs the sink again with no stop
  running clears it. The reason is the measured `GooglePubSubSink` losses in the Evidence: with the
  signal kept set, 7 of 7 publishes confirming between 2.0 and 3.9 s were counted lost where
  `ead40798` confirmed them.
- Invariant 10 takes no recorded exception (FR-004): every property the README promises holds, and
  the compatible form uses none of the break Andrew's acceptance of 2026-11-14 allowed.
- 2026-11-16, revised after SPEC-097's spec review: FR-005 re-runs SPEC-097's re-entry sweep after
  the deletion.
- 2026-11-16, revised after this spec's review. MAJOR-1: the seed and the stop walk a weak set of
  every event made, not the sink table, so a member holding a released wrapper's event is still set
  and re-initialised. MAJOR-2: the superseded statements are named for the late case only, the
  release-note line says which waits collapse, the forked-child case is added, the `GooglePubSubSink`
  consequence was executed (above) and led to option 2. MAJOR-3: SPEC-097 is in `depends_on`.
  MAJOR-4: the deletion is built in the prototype and measured, `offer_once` is the one offer site on
  each path, and the swap criterion is new. MINOR-5: closed by option 2 for a sink configured again,
  and listed in FR-004 for a late log. MINOR-6: dead entries pruned at a sink's first entry, with a
  binary criterion. MINOR-7: the search is widened and reproducible (20 files), and the invariants
  page, `architecture.md` and the inventory have a criterion. MINOR-8: the README citation is
  `:1116-1156`, and §12's straddling entry says it was never reproduced.
- Rejected while prototyping: marking a sink handed before its setter returned, which let another
  thread's log reach the sink holding no event; seeding the fork by the sink table (`orphanwrap.py`'s
  child rows 4.7 s); keeping the signal set through a `configure()` (the `GooglePubSubSink` losses).
