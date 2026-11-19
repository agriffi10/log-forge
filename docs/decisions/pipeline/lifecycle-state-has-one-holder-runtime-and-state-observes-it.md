---
type: Decision
title: "Lifecycle state has one holder, Runtime, and the stored State decides the transitions"
description: "the lifecycle bookkeeping lives on `_lifecycle._state`, a `Runtime`, with no module-level alias; its settled `State` is stored in `_current`, written only by `_move` inside four existing critical sections and the fork handler's re-settle, and the transition table is total over six calls and five states, with one changed cell: a swap with no live worker during a drain with a take ahead declines. `CLOSED` keeps its arrows. Every owed and in-flight close lives in one registry. Three locks remain, all `RLock`s; a lifecycle call reads `_lock` and the registry's for ownership and never re-enters them. The token sets are discarded lock-free, and a sink change's added lock-free before its config write, a bounded exception to `python.md` §13. (SPEC-077, SPEC-078, SPEC-097; arch §9.2)"
---

**One holder.** SPEC-040 put the orphan side's seven globals on one object, `_lifecycle._state`,
and SPEC-054 merged the twin bookkeeping (*One lifecycle owner*). Seven more names still sat at
module scope in `_lifecycle.py`, each with its own lock where it had one. SPEC-077 renamed
`_Lifecycle` to `Runtime` and moved all seven onto it. No alias was left at module scope, because
a second home is the defect the move removes. `_state` stays the module global and the one
instance, so the `@trace` hot path and `worker.py`'s binding are untouched.
`tests/test_lifecycle_inventory.py` makes "nothing else holds lifecycle state" a gate rather than a
sentence: it derives every lock or event construction, every `Runtime`, `Worker` and
`_CloseRegistry` field and every module-level state binding, and fails on one with no declared
home.

**One close registry** (SPEC-078 FR-001). A close passed through three facts — owed, in flight,
and for a detached close its thread — held in four places (`_owed`, `_closing_now`, `_closers` and
`_close_owed`'s local thread list), with the moves between them written out at each site.
`_CloseRegistry`, held as `Runtime._closes`, owns all of them: `arm`, `take` (owed to in flight in
one call under `_lock`, so no instant has a sink taken to be closed neither owed nor in flight),
`put_back`, `cede` (out of the record with no close, for a sink the graph a swap installs still
holds, which is then closed by that graph's forward; a ceded sink is neither owed nor in flight by
design, SPEC-087), `register`, `finish`, and the reads. Each in-flight entry, a `_Close`, records its rule —
`INLINE`, `JOINED` (a shutdown's fan-out, joined in full, SPEC-046) or `GRACED` (detached, given
the call's one grace) — and its thread, and holds no sink, because the fork walk reaches the
registry. An entry counts its participants, so a forwarded release joining a detached close still
in flight does not discharge it while that thread is inside `close()`. The owed record's fork
opt-out moved with it: `_CloseRegistry._FORK_SKIP = ("owed",)`, and `Runtime._FORK_SKIP` is
`("_owned",)`.

**`State` is stored and decides** (SPEC-078 FR-003, FR-004). SPEC-077 introduced it as an
observer derived from the flags. Now `Runtime._current` stores the settled part — rules 3 to 6 of
SPEC-077's derivation **without** the configured-sink term, because the config is not the
runtime's and `configure()` must not take its lock — written only by `Runtime._move`, at the end of
the four critical sections that can change it (the build, the orphan arm, and the shutdown's entry
and late-worker sections), and — since SPEC-079 — in the forked child's `_clear_after_fork`, which
re-settles what a fork landing inside one of those sections half-wrote. No other section calls
it, because none can change the value. `state`
reads the token sets, then `_current`, and reports a stored `UNCONFIGURED` as `RUNNING` once a sink
has resolved; it stays lock-free and is the reporting view. The library reads `Runtime._column()`
— the token sets, then `_current` — under the lock it already holds.

**The transition table is total, and one cell changed.** Six calls (`BUILD`, `ARM`, `SWAP`,
`FLUSH`, `STOP`, `FORK`) by five states; every cell is today's behaviour except one: **a swap
during a drain with no live worker declines at entry, while the drain still has a take of the
owed record ahead of it** — no retarget, no close, no arm. After the drain's last take, which the
drain marks on its `_Operation` under the lock, the swap runs its no-worker branch: a sink re-armed
in the tail is taken by nothing left in that call, and declining there left it owed and open (found
by the first diff review). At
`fda134a` such a swap moved the drained sink's close onto a daemon that exit kills, and closed the
new sink, which nothing had been written to. A live worker in the drain (a late one, or one built
in the drain's tail) still swaps, because declining there loses the configure. Only `_swap_sink`
reads the column, because the other five rows give the same code in every column.

**`CLOSED` keeps its arrows, by decision.** After a `shutdown()` that built no worker, a `@trace`
builds a live worker and delivers (SPEC-044 FR-001); after one that stopped a worker, the same call
is counted in `submitted_after_shutdown` and the state stays `CLOSED` (SPEC-019, SPEC-030); an
orphan log after either is delivered and re-arms the sink (SPEC-032). Making `CLOSED` terminal
would reclassify the first case's delivered events in a minor release, and no invariant needs it.

**Three locks, not one.** `_lock` guards the worker, the owed record, the retirement count and the
stop event; the registry's leaf lock guards the closes in flight; `_owned_lock` guards the ownership
record. SPEC-078 merged the two former leaves (`_closing_now_lock`, `_closers_lock`) into the
registry's one, which changed no acquisition order, since they never nested. Folding the leaf into
`_lock` would deadlock `in_flight` and `held`, which are asked with `_lock` held, and closer
threads discharge it without `_lock`. The order: `_lock`, then `_config_lock`, then `_owned_lock`;
and `_lock` with `Worker._lock` and the registry's lock as leaves. Nothing acquires `_lock` beneath
any of them, with one exception SPEC-081 made possible: `_owned_lock` is re-entrant, because a
sink the record holds only weakly can be collected inside a record section and its finalizer can
forward a close back into the record on that thread, and a close reached that way whose entry
carries a declined stop-signal hand takes `_lock` under `_owned_lock` (`_hand_after_close`). Two
threads can deadlock there; it is recorded in arch §13 as a residual rather than closed.

**`_lock` and the registry's lock are `RLock`s read for ownership** (SPEC-097 FR-004). A signal
handler or finalizer that interrupted a thread inside one of them and called `shutdown()` waited
on its own thread for good (`lifecycle/p1b_run.py`: 5, 29 and 32 of 200 fresh processes at
`ead40798`). `_lifecycle._library_locks()` lists them with `Worker._lock`, the worker queue's
mutex, `config._config_lock`, `decorator._sweep_lock` and `decorator._loss_lock`, and a
`shutdown()` or `configure()` made on a thread owning any of them is held on that thread, a
`flush()` refused, so no lifecycle call re-enters one. A level or traced call made there does
re-enter, which FR-005's sweep found safe in every section a workload reaches. `_owned_lock` stays
out of that list, re-entrant by design. The order above is unchanged. `tests/test_lock_oracle.py`
holds every lock construction to the list or a recorded reason.

**The token sets are discarded without the lock, and that departs from `python.md` §13 on
purpose.** `_swap_sink` and `_shutdown_worker` each add their `_Operation` to `_swaps_running` or
`_stops_running` inside a critical section they already have, and discard it in a `finally` with
one `set.discard`. An `_Operation` hashes by identity and runs no Python code, so under the GIL the
discard is one operation. They are sets, not booleans or counters, because two concurrent
`configure(sink=…)` or `shutdown()` calls are documented as normal, and the worker branch of
`_shutdown_worker` has no critical section after `worker.stop()`. The `_state._lock` counts per call
are pinned in `tests/test_runtime_state.py`, and SPEC-078 moved none of them. **The exception now
carries a decision** — the one changed cell reads `DRAINING` off `_stops_running` — and it stays
sound: the token is added before anything the swap could decline over, inside the critical section
the swap reads it in, and a stale discard can only make a later read see `DRAINING` end late,
which costs that one swap a decline of a drain already over, never a close under a drain.
**Since SPEC-097 one add is lock-free too**: a non-re-entrant sink change adds its swap token
before its config write (`open_command_swap`), outside every lock but `config._sink_lock`, so a
`shutdown()` meets the change from the write and `configure()`'s pinned counts do not move. It is
one `set.add` of an identity-hashed object under the GIL, as the discard is; the swap reads the
same token in its sections as before.

**Rejected:**

- **One lock**, the stability plan's target. It deadlocks or reorders, for the reasons above.
- **A `Runtime.sink` field.** The live sink is held twice already, by `Worker.sink` and by the
  config, and the two differ on purpose after a declined swap.
- **Moving `Worker`'s drain state onto `Runtime`.** `_stopped`, `_join_owed` and the drain events
  belong to one worker, and there can be two (`_worker` and `_late_worker`).
- **Module-level aliases for the moved names.** An alias would be a second home.
- **A terminal `CLOSED`** (the stability plan's diagram): it reclassifies delivered events.
- **A `_column()` read at every entry**: five of six rows give one answer in every column, so the
  read would decide nothing.
- **Grace-capping the fan-out joins**: SPEC-046 measured it losing data.

**Do NOT** write `_current` outside `Runtime._move`, or call `_move` outside the four sections that
can change it and the fork handler `_clear_after_fork` — `tests/test_lifecycle_transitions.py` scans
both, by name. Do NOT let a library call decide
on `Runtime.state`; it reads `_column()` under its own lock. Do NOT add lifecycle state at module
scope in `_lifecycle.py`, or a field to `Runtime`, `Worker` or `_CloseRegistry`, without a row in
the inventory test. Do NOT merge the three locks without re-deriving the lock order. Do NOT touch
the owed record outside `_CloseRegistry.arm`, `take`, `put_back` and `cede`. (SPEC-077 FR-001 to
FR-003, SPEC-078 FR-001 to FR-004, SPEC-087 FR-001)

## History

### 2026-11-18

- **Refined by SPEC-097**: `_lock` and the registry's lock are `RLock`s that a lifecycle call
  reads for ownership and never re-enters, with four locks outside `Runtime`; a sink change adds
  its swap token lock-free before its config write, a second lock-free move beside the discard;
  and `Runtime` gains `_deferred_taker` and `_calls_here`, each with its inventory row and reset
  in a forked child. The fence says so, and drops a clause it carried twice to stay inside its
  length cap.

### 2026-11-15

- **Refined by SPEC-087**: the close registry gains `cede`, the one move out of the owed record that
  closes nothing, made by a swap for a sink the graph it installs still holds. The registry's list
  of moves and the Do-NOT line name it, and the claim that no instant has a sink neither owed nor in
  flight is narrowed to a sink taken to be closed, since a ceded sink is neither by design.

### 2026-11-13

- **Refined by SPEC-081**: `_owned_lock` is an `RLock`, and the lock order gains its one
  exception, a finalizer that reaches `_hand_after_close` with `_owned_lock` held.

### 2026-11-10

- **Refined by SPEC-079**: `_clear_after_fork` is a fifth `_move` site. A fork landing inside one of
  the four sections, between a field write and its `_move`, left a child whose stored state differed
  from its derived one, and a fork inside `_get_worker` between `_worker =` and the arm left a live
  worker's sink unowed; the handler re-settles both under the lock the roster replaced.

### 2026-11-09

- **Refined** by SPEC-078: `State` is stored (`_current`, one writer, `_move`) and decides, the
  transition table is total with one changed cell (a swap during a drain with no live worker
  declines while the drain has a take ahead), `CLOSED` keeps its arrows, every close lives in one registry (`Runtime._closes`),
  and the two leaf locks merged, leaving three. The title changed from "State observes it until
  transitions are checked"; the file name stays, as a decision's file name never changes.

- **Settled** by SPEC-077: one holder, `Runtime`; `State` as a lock-free observer; four locks kept;
  the token sets' lock-free discard as a bounded exception to `python.md` §13.
