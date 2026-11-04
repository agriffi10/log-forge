---
type: Spec
id: SPEC-078
title: "The Owed-Close Registry and Checked Lifecycle Transitions"
description: "Puts every owed and in-flight sink close in one registry that spends the closer grace once per shutdown, and makes the stored lifecycle State the authority: every lifecycle call checks it under the runtime's lock and has a defined answer in every state."
spec_status: Draft
depends_on: [SPEC-045, SPEC-046, SPEC-050, SPEC-054, SPEC-062, SPEC-077]
arc: stability-redesign
arc_order: 10
---

## Overview

SPEC-077 gives the lifecycle bookkeeping one holder, `Runtime`, and names the process's five states
in `State` — but as an **observer**, read off the flags and deciding nothing. This spec is the next
step of the arc: `State` becomes the **authority**. It is stored, written in one place under the
runtime's lock, and every lifecycle call — a sink swap, `flush()`, `shutdown()`, the exit drain, a
span needing a worker, an orphan log arming a sink, and a forked child's entry — reads it on entry
and takes the answer the transition table gives for that state. Beside it, the closes the library
owes stop living in four structures (`_owed`, `_closing_now`, `_closers` and a local list of
threads inside `_close_owed`) and live in **one registry**, which records how each close is run and
waited for, and owns the one grace a shutdown may spend on somebody else's close. SPEC-079 then
reworks the fork repair from `Runtime`'s roots; it is not built here.

Two caller-visible defects are closed by the restructuring, and both were measured at `fda134a`
before anything was designed on them, with the probes described in FR-002 and FR-004:

- **A shutdown spends the closer grace twice.** A swapped-out sink whose `close()` outlives the
  swap's budget is waited on once by `_close_owed`'s bystander wait and again by `join_closers`:
  `shutdown(timeout=30)` took **4.00 s** on the worker path and **4.01 s** on the orphan path
  against a 2.0 s `DEFAULT_CLOSER_GRACE`. The two tests that say the grace is granted once
  (`tests/test_shutdown_lifecycle.py::test_the_closer_grace_is_granted_exactly_once_per_call` and
  `tests/test_orphan_sink_handoff.py::test_the_worker_path_grants_the_closer_grace_exactly_once`)
  count calls to `join_closers`, and the second grace is not one.
- **A `configure(sink=…)` landing inside a shutdown's drain, once the worker is retired, moves the
  live sink's close onto a daemon.** With the swap injected between `Worker.stop` returning and
  `_close_owed`'s take, the
  worker's sink A was closed on a `log-foundry-sink-close` thread — joined only for the grace and
  killed at interpreter exit wherever it has reached, the SPEC-028 objection SPEC-030 and SPEC-046
  keep the live sink's close inline to avoid — and the new sink B, which nothing was ever written
  to, was closed inline in its place. Without the injection A closes on `MainThread`.

**`CLOSED` keeps today's arrows** (FR-004). The plan this arc follows draws no arrow out of `Closed`;
the code has three, and the recorded decisions require them: a span after a `shutdown()` that built
no worker builds a live one and delivers (SPEC-044 FR-001: "deliberately *not* a permanent
retirement fence … making every later worker born retired would … lose events that land today"),
and making it terminal would reclassify those delivered events as `submitted_after_shutdown`, which
the 1.2.0 additive rule forbids in a minor release.

**This spec builds on three peer specs that land first, in 1.1.0, and are not in `depends_on`
because none is in this directory yet.** SPEC-066 gives `flush(timeout=)` and `shutdown(timeout=)`
a verdict for `inf`, `nan` and a non-number at the public call, so every transition here receives
`None` or a finite number. SPEC-068 settles `architecture.md` §12's double announce on an expired
exit drain, a second `shutdown()` after an expired one, and a fork-resumed worker not resetting
`Worker._stopped`; the `STOP` row below takes those answers as SPEC-068 leaves them. SPEC-077 is in
`depends_on` and is the base this spec edits. **This spec changes four of SPEC-077's names,
each on purpose:**
- Its token sets `_swaps_running` and `_stops_running` hold `_Operation`s, not bare `object()`s
  (FR-003).
- `Runtime._FORK_SKIP` goes from `("_owed", "_owned")` to `("_owned",)`, because `owed` moves to
  the registry with its own opt-out (FR-001).
- Its four close names (`_closers`, `_closers_lock`, `_closing_now`, `_closing_now_lock`) and
  `_owed` are replaced by `Runtime._closes`.
- Its inventory floor of lock and event construction sites in `_lifecycle.py` and `worker.py`
  falls from eleven to ten, for the merged leaf lock (FR-006).

## Scope

### In Scope

- One close registry on `Runtime` holding the owed record and every close in flight, with the join
  rule of each, replacing `_owed`, `_closing_now`, `_closing_now_lock`, `_closers`, `_closers_lock`,
  `_Closing`, `join_closers`, `closing_count` and `_close_owed`'s local thread list (FR-001).
- The close rules kept exactly — inline, fan-out joined in full, detached and graced, a bystander
  waiting — and the grace spent once per call (FR-002).
- A stored `State` with one writer, SPEC-077's token sets carrying the operation that holds
  `SWAPPING` or `DRAINING`, and every lifecycle entry reading it under the lock with no new lock
  acquisition (FR-003).
- The transition table, total over six calls and five states, its one changed cell (a swap during a
  drain with no live worker declines), and the decision that `CLOSED` keeps its arrows (FR-004).
- Slow work as a tracked operation with a deadline the runtime owns, and the guarantee about an
  emit still running after its deadline, with what is left to SPEC-076 (FR-005).
- The model test driving the table, and the closed list of tests re-expressed because they pin a
  removed mechanism (FR-006).
- The FR count is six, within the aim of 3 to 6; the arc's step 9 needed no merge or split.

### Out of Scope

- **Grace-capping the fan-out joins.** The stability plan says the owed closes are "all joined
  under `closer_grace`"; at `fda134a` only the detached closes and the bystander wait are, and the
  shutdown's own fan-out is joined in full. SPEC-046 built the grace-capped version and measured it
  losing data: four owed 2-second closes against `shutdown(timeout=1.0)` completed **1 of 4**, and
  "a 3-second close delivered nothing where today it delivers" (SPEC-046, *Scope* and *Revision
  history*). FR-002 keeps SPEC-046's rule. A shutdown's own closes are joined in full, and only
  detached closes get the grace. That rule is the 1.2.0 fixed decision "owed closes stay concurrent
  (SPEC-046)", and the plan's wording is corrected here.
- **Bounding `Sink.close`.** The inline close stays unbounded on both paths (`architecture.md` §13).
- **Making `CLOSED` terminal**, for the reason the Overview gives and FR-004 records.
- **Serialising concurrent `configure(sink=…)` calls.** `configure()` is documented as not
  thread-safe, with every sink involved still closed (SPEC-045); a second swap runs alongside the
  first, as today.
- **Tracking an emit or a flush on an application thread.** The orphan path emits on the caller's
  thread and `flush()` drains each owed sink's own buffer on it; the library cannot serialise them
  against a close (SPEC-028), and what a sink does when one overlaps its close is SPEC-076's
  closed-guard helper's.
- **The late-worker gate, and `_shutdown_running` surviving a fork.** The second is the known
  defect SPEC-077 records and leaves to SPEC-079; this spec does not touch it. `_get_worker` keeps
  registering a late worker on `_shutdown_running > 0`, not on `DRAINING`: `_shutdown_worker`
  lowers that counter in the critical section that reads `_late_worker`, before its second pass and
  its grace, so the gate's window is narrower than the state's. Gating on `DRAINING` would register a worker built after that
  read where nothing stops it.
- **Fork.** The repair walk, the lock roster, `_mark_inherited` and `sink_factory=` are SPEC-079's.
  This spec touches the child only at its entry handler (FR-003) and moves the owed record's
  `_FORK_SKIP` entry to the registry (FR-001).
- **One lock.** Three remain on `Runtime` — its lock, the registry's leaf lock and `_owned_lock` —
  for SPEC-077 FR-001's reason: `held` and `in_flight` are asked with `Runtime._lock` held and read
  the in-flight half, and closer threads discharge it without that lock.
- **A `Runtime.sink` field**, for SPEC-077's reason.
- **SPEC-066's and SPEC-068's fixes**, named in the Overview.
- **`architecture.md` §12's first open item** (a sink released while live inside the graph that
  replaced it).
- **A public name.** Everything here is private to `log_foundry._lifecycle`.

---

## Functional Requirements

### FR-001: One registry holds every owed close and every close in flight

#### Description:

A close passes through three facts today, held in four places: **owed** (`Runtime._owed`, under
`Runtime._lock`), **in flight** (`_closing_now`, an `int`-keyed dict of `_Closing` events under the
leaf `_closing_now_lock`), and, for a detached close, **its thread** (`_closers`, under
`_closers_lock`) — plus the fan-out threads `_close_owed` keeps in a local list. The moves between
them are written out at each site. Measured at `fda134a` with
`grep -nE "_owed\[id\(.*\)\] = |del _state\._owed|_owed\.pop|_owed\.setdefault" src/log_foundry/_lifecycle.py`:
four sites arm the record (`_get_worker`, `_note_orphan_emit` and two in `_swap_sink`), three take
from it (`_close_owed` and two in `_swap_sink`) and one puts back (`_close_owed`'s `finally`). The
rule all of them must keep — no instant at which a sink is neither owed nor in flight (SPEC-054
FR-003) — is restated at each.

`_CloseRegistry`, held as `Runtime._closes`, owns all three facts and every move between them: `arm`,
`take`, `put_back`, `register` (a release with no take, which is how the five shipped wrappers'
forwarded `release(…, owner=self)` reaches it), `finish`, and the read `in_flight`. `take` removes a
sink from `owed` and records its entry in one call made under `Runtime._lock`, so the rule above has
one home. Each in-flight entry is a `_Close` carrying the bystander's event, the entry's `_Rule`
(FR-002) and, once started, its thread. A `_Close` holds **no sink**, as `_Closing` does not: the
registry is reached by the fork repair walk through `_state`, and an owned object holding a sink
would hand the walk superseded sinks to run fork hooks on. `owed` is skipped from that walk by
`_CloseRegistry._FORK_SKIP = ("owed",)`, which replaces the `_owed` entry SPEC-077 put on
`Runtime._FORK_SKIP`.

Two of SPEC-077's four locks merge: `_closing_now_lock` and `_closers_lock` are both leaves under
`Runtime._lock` that never nest with each other, so one leaf lock guards the whole in-flight half
and no acquisition order changes. `owed` stays under `Runtime._lock`, and `_note_orphan_emit`'s fast
path stays one unlocked membership test, now `id(sink) in _state._closes.owed`.

**What a forked child does with each new field**, in SPEC-077's inventory column:
- `_CloseRegistry._lock` and each `_Close.event`: `walk`.
- `_in_flight`: `reset: _clear_after_fork`.
- `owed`: `inherited`, as `_owed` is at `fda134a`.

That is one change to SPEC-077's rows: `_closers` read `inherited`, since its threads read as dead
in a child and are pruned. Its threads now live in `_in_flight` entries and are dropped with them.

`Health.closing_sinks` keeps its documented meaning — swapped-out sinks inside `close()` right now —
as the number of `GRACED` entries whose thread is alive, which is the population `_closers` held.

**Twin paths (invariant 6).** The worker and orphan paths already share the record; the registry
keeps them sharing it. The swap's closes and the shutdown's closes are the twins whose take is now
one method; the fork child's clear of the in-flight half is the third reader.

#### Acceptance Criteria:

- [ ] `_lifecycle.py` binds none of `_closing_now`, `_closing_now_lock`, `_closers`,
  `_closers_lock`, `_Closing`, `_register_closing`, `_finish_closing`, `_closing`, `join_closers` or
  `closing_count` at module level (`_worker_health` reads `_closes.graced_alive()`), `Runtime` has no `_owed` attribute, and an AST scan finds no subscript, `del`,
  `pop` or `setdefault` on `_closes.owed` outside `_CloseRegistry`'s methods.
- [ ] Every library close — the five wrappers' forwarded releases included — registers an entry
  before `close()` runs and discharges it on every exit, a refused release and a thread that never
  started included: after each existing ownership and owed-close test the registry's in-flight
  half is empty.
- [ ] A `_Close` holds no reference to a sink, and a child forked after `configure(A)`, `info()`,
  `configure(B)` runs no fork hook on `A` (the SPEC-044 FR-005 fork test, edited only for the
  path).
- [ ] With an interrupt injected between a take and its close, the sink is owed again and its entry
  discharged (the existing `_close_owed` interrupt test, edited only for the path).
- [ ] `health().closing_sinks` reads one while a swapped-out sink is inside a stuck `close()` and
  zero after it returns, and is not raised by a shutdown's fan-out closes (the existing tests pass).
- [ ] Invariants 5, 6 and 12: the existing owed-close, ownership and fork-lifecycle tests pass on
  both delivery paths, so every sink written to is closed once by one owner and a child releases
  only what it acquired.

### FR-002: Every owed close is performed by its rule, and a shutdown spends the grace once

#### Description:

The registry records, per entry, which of three rules runs and waits for it, and the rules are
today's, gathered rather than changed:

- **`INLINE`** — on the calling thread, chosen by `_inline_close_choice`: the worker's own sink,
  else the configured one, else the last armed (SPEC-054 FR-003, which SPEC-062 states). This is
  SPEC-030's decision that `shutdown()`'s own close stays inline, and the single-owed-sink case
  starts no thread (SPEC-046 FR-003).
- **`JOINED`** — every other sink a shutdown takes, each on its own thread, all joined **in full**
  in a `finally` (SPEC-046), so the cost is the slowest close and none is abandoned to a daemon
  that exit kills.
- **`GRACED`** — detached: the swap's closes of the sinks it supersedes and of the sink it fenced
  out, joined to the swap's budget by the swap; and a sink swapped out without a confirmed fence
  that a shutdown finds with the drain thread ended (SPEC-050 FR-004). A shutdown grants these the
  grace.

A caller also waits, as a **bystander**, on every in-flight entry it did not take, whatever its
rule (SPEC-050 FR-002: a close in flight is waited for, not returned through).

**The grace is spent once per call.** At `fda134a` two waits compute it independently —
`_close_owed`'s bystander wait and `join_closers` — and a `GRACED` entry the shutdown did not take
is waited on by both. The probe: `configure(sink=A)` with A's `close()` sleeping 10 s, one event
(through `@trace` and `flush()` for the worker path, through `info()` for the orphan path),
`_swap_sink(B, timeout=0.1)`, then `shutdown(timeout=30)`. It took 4.00 s and 4.01 s against a
2.0 s grace. The registry fixes one end instant per `shutdown()` call, `grace_end`, at the first
graced wait the call makes — after the inline close, so SPEC-030's order holds — and every graced
wait of that call, the bystander wait and the join of `GRACED` threads, ends at it. Two concurrent
shutdowns each have their own, as each is bounded by its own budget. A `GRACED` close that the
call's second pass detaches (an unfenced swapped-out sink whose drain thread ended during the call,
`_lifecycle.py:1742` at `fda134a`) therefore gets only what is left of a grace an earlier wait
started. That is the "once", and it is SPEC-050 FR-004's judgement: such a sink already had the
swap's whole budget. A call with nothing graced to wait on sets no `grace_end` and spends no grace,
as `tests/test_concurrent_owed_closes.py::test_a_worker_path_shutdown_with_no_close_in_flight_pays_no_grace`
holds today.

**Twin paths (invariant 6).** The worker-path and orphan-path shutdowns reach the same closer; the
probe covers both, and so must the test.

#### Acceptance Criteria:

- [ ] The probe above returns in under 1.5 × `DEFAULT_CLOSER_GRACE` on the worker path and on the
  orphan path, and in at least 0.9 × it, so the grace was waited and not skipped.
- [ ] A counting hook on the registry replaces the two `join_closers`-counting tests named in the
  Overview. It shows a `shutdown()` that has a bystander entry or a `GRACED` entry setting its
  `grace_end` exactly once, and a `shutdown()` with neither setting it zero times.
- [ ] `tests/test_concurrent_owed_closes.py::test_four_owed_closes_cost_the_slowest_not_their_sum`
  (one 0.2 s inline close and three 2 s closes, all completed and overlapping under
  `shutdown(timeout=1.0)`) and `::test_a_close_longer_than_the_closer_grace_still_delivers` pass
  with their assertions unedited.
- [ ] The worker's own sink closes on the calling thread on both paths, and a single owed sink
  starts no thread.
- [ ] A second `shutdown()` arriving inside the first one's inline close waits for it
  (`tests/test_worker.py::test_a_second_shutdown_waits_for_an_inline_close_it_did_not_claim`
  passes).
- [ ] A close that raises on the calling thread or on a `JOINED` thread is absorbed and announced
  on stderr, and the other closes still run and are joined.
- [ ] Invariants 3 and 5: every owed sink is closed exactly once across a shutdown on both delivery
  paths, and a shutdown's wait on closes it did not perform is at most one grace.

### FR-003: `State` is stored, written in one place, and checked by every lifecycle entry

#### Description:

SPEC-077's `Runtime.state` derives the state on every read, lock-free, from several fields that can
be mid-update. This spec stores the part of it the runtime owns. `Runtime._current: State` holds
SPEC-077's rules 3 to 6 **with rule 5's configured-sink term left out**: `RUNNING`, `CLOSED` or
`UNCONFIGURED`, from the worker, the retirement count and the owed record only. The config is not
the runtime's. `configure()` writes it without entering the runtime, and it must not take the
runtime's lock, so `_current` never reads the config. It is written by one method, `Runtime._move`.

**Where `_move` is called is decided by whether removing the call would be observable.** It is
called at the end of each critical section that can change the value:
- the build in `_get_worker`, which writes `_worker` and arms the owed record from empty;
- `_note_orphan_emit`'s locked branch, which arms it from empty;
- `_shutdown_worker`'s entry, which moves the retirement count;
- its late-worker section, which moves the count a second time.

All four sections already exist, so `_move` adds no acquisition. No other critical section calls it,
because none can change the value:
- `_close_owed`'s take and put-back run only after `_shutdown_worker` has moved the count. At
  `fda134a` its only callers are `_lifecycle.py:1867`, `:1870` and `:1881`. After that move, rule 3
  or rule 4 decides and the owed record is not read.
- `_swap_sink` arms a sink only when one is already owed.
- `_clear_after_fork` changes none of the fields the rules read; it only empties the token sets.

A call in any of those would be an equivalent mutant, so none is written.

The two transient states and the config keep SPEC-077's derivation at the read. `state` reads
`DRAINING` while `_stops_running` is non-empty and `SWAPPING` while `_swaps_running` is. Otherwise it
reads `_current`, and it reports a stored `UNCONFIGURED` as `RUNNING` once the config has resolved a
sink. It stays lock-free and total.

**One change to SPEC-077's token sets.** A token is an `_Operation` (FR-005), not a bare `object()`,
so the token that holds `SWAPPING` or `DRAINING` also carries that call's deadline. SPEC-077's names
and their discipline are unchanged: `set[_Operation]`, each token added in the call's first critical
section, and discarded in a `finally` by one `set.discard`, without the lock.

**Dispatch.** Each lifecycle entry reads, once, in the critical section it already takes,
`_stops_running`, then `_swaps_running`, then `_current`. The first non-empty set, or else
`_current`, is the table column FR-004 answers from. Dispatch never reads the config term, so a
`SWAP` or an `ARM` from cold is in the `UNCONFIGURED` column although `state` already reports
`RUNNING` there. The entries are `_get_worker`'s inner check (`BUILD`),
`_note_orphan_emit`'s locked branch (`ARM`), `_swap_sink` (`SWAP`), `_flush_worker` (`FLUSH`, which
moves nothing), `_shutdown_worker` for `shutdown()` and the exit drain (`STOP`), and
`_clear_after_fork` (`FORK`). The `FORK` entry empties the token sets (SPEC-077), and the child's
`_current` is inherited as the parent had it, which is exact. Its child-repair value is `inherited`.
`configure()` without `sink=` writes the config and is not a lifecycle call.

**The lock counts SPEC-077 FR-002 pins do not move.** Every write of `_current` sits in a critical
section the call already has, and the transient states come from the lock-free token discard. The
one changed answer, a swap that declines in `DRAINING` with no live worker, returns from its first
critical section. That is 1 acquisition, which is SPEC-077's figure for a `configure(sink=…)` with no live worker, the case
it is at `fda134a`. The pinned figures stay as SPEC-077 lists them:
- `configure(sink=…)`: 2 with a live worker; 1 with no live worker and something owed; 1 with
  nothing owed.
- `shutdown()`: 2 on the worker branch, 3 on the no-worker branch.
- The exit drain: 2.
- `flush()`: 1.
- Orphan `info()`: 1 the first time, 0 after.
- `@trace`: 1 for the build, 0 after.

**The hot path.** `_get_worker`'s outer `_state.worker_exists()` stays unlocked (`docs/decisions/pipeline/`
→ *A worker guard asks one of three questions*); the state is read only past it, under the lock the
build already takes.

**Twin paths (invariant 6).** `flush()` and `shutdown()` answer a marker from one worker and now read
one state; `BUILD` and `ARM` are the two delivery paths' entries and move the state by one method.

#### Acceptance Criteria:

- [ ] An AST scan finds `_current` assigned only inside `Runtime._move` and `Runtime.__init__`. It
  finds `_state._move()` called in exactly the four critical sections named above, and in each one
  after the last write of `_worker`, `retirements` or the owed record.
- [ ] After every step of FR-006's sequential driver, `_current`, read directly and not through
  `state`, equals rules 3 to 6 without the config term, evaluated over the runtime's fields.
- [ ] Each of the four `_move` calls is mutation-tested, and a named test kills it:
  - Deleting it from the build kills the sequential driver's span step from `UNCONFIGURED`.
  - Deleting it from `_note_orphan_emit` kills the driver's orphan `info()` from `UNCONFIGURED`.
  - Deleting it from the shutdown's entry kills the driver's `shutdown()` from `RUNNING`.
  - Deleting it from the late-worker section kills FR-004's directed `BUILD` × `DRAINING` test,
    in-window case, which reads `_current` as `CLOSED` once the shutdown returns. The sequential
    driver cannot reach a late worker.
- [ ] SPEC-077's token-set exit criteria pass unedited in their assertions with `_Operation` tokens.
- [ ] SPEC-077 FR-002's pinned acquisition counts pass unchanged, and a `configure(sink=…)` that
  declines in `DRAINING` takes `_state._lock` once.
- [ ] `state` returns while another thread holds `_state._lock`. A sampler reading it through a
  concurrent run of the model test only ever gets a `State`.
- [ ] A child forked with a swap and a shutdown in flight starts with both token sets empty, and
  with `_current` equal to rules 3 to 6 without the config term over its inherited fields.
- [ ] `flush()` leaves the state unchanged in every state. A worker its sweep builds moves it only by
  the `BUILD` row.
- [ ] Invariants 1, 3 and 6: no entry raises from the state check, no public call gains a lock
  acquisition, and `_get_worker`'s outer check is still a bare unlocked `_state.worker_exists()`.

### FR-004: Every lifecycle call has a defined answer in every state

#### Description:

The table is total: six calls by five states, thirty cells. A cell is either today's behaviour,
named, or the one changed answer.

**The column is the dispatch value FR-003 defines:** the token sets first, then `_current`. **"→" is
what `state` reads when the call returns.** In the `SWAPPING` and `DRAINING` columns, the call that
put the process there is still inside when this one returns. So "→" names that transient state
first, then what `state` reads once that call leaves.

A `SWAP` that has work reads `SWAPPING` while it runs, and every `STOP` reads `DRAINING`. A swap's
work is a live worker or something owed, the same test SPEC-077's oracle uses
(`_lifecycle.py:1969`). A drain's **tail** is the part of a no-worker `shutdown()` after its
`finally` has lowered `_shutdown_running` (`_lifecycle.py:1875` at `fda134a`): the late-worker
stop, the second pass and the grace.

| Call | `UNCONFIGURED` | `RUNNING` | `SWAPPING` | `DRAINING` | `CLOSED` |
|---|---|---|---|---|---|
| `BUILD`: a span needs a worker and none exists | builds, arms its sink → `RUNNING` | builds → `RUNNING` | builds; the swap continues → `SWAPPING`, then as the swap leaves it | inside the `_shutdown_running` window: the late worker this drain stops (SPEC-044 FR-001) → `DRAINING`, then `CLOSED`; in the tail: a live worker the drain does not stop → `DRAINING`, then `RUNNING` | builds a live worker → `RUNNING` |
| `ARM`: an orphan log reaches a sink not owed | arms → `RUNNING` | arms → `RUNNING` | arms → `SWAPPING`, then as the swap leaves it | arms; owed a further close (SPEC-045 FR-002) → `DRAINING`, then as the drain leaves it | arms → `CLOSED` |
| `SWAP`: `configure(sink=…)` | nothing to swap → `RUNNING`, since the config has resolved a sink; `_current` stays `UNCONFIGURED` | through `SWAPPING` when it has work, directly when not → `RUNNING` | runs alongside the swap in flight → `SWAPPING`, then the settled state when the last leaves | **no live worker: declines at entry**, with no retarget, no close and no arm; a live worker (a late one, or one built in the tail): swaps as in `RUNNING` → `DRAINING`, then as the drain leaves it | through `SWAPPING` only when something is owed: supersedes the owed sinks and arms the new one → `CLOSED` |
| `FLUSH`: `flush()` | touches no sink → unchanged | drains → unchanged | drains → unchanged | answers as at `fda134a` → unchanged | answers as at `fda134a` → unchanged |
| `STOP`: `shutdown()` or the exit drain | moves the count → `CLOSED` | drains and closes → `CLOSED`, or `RUNNING` when no worker existed and a span built one in the tail | drains and closes; the swap's retarget declines (SPEC-033 FR-002) → `SWAPPING` while the swap is inside, then `CLOSED` | joins as a second caller, as SPEC-068 leaves it → `DRAINING` while the first call is inside, then as the `RUNNING` cell | moves the count, closes what was armed since → `CLOSED`, with the `RUNNING` cell's tail exception |
| `FORK`: the child's entry | `UNCONFIGURED` | `RUNNING` | `_current`, inherited: `RUNNING` or `CLOSED` | `_current`, inherited | `CLOSED` |

**A worker built in a drain's tail is today's behaviour, recorded rather than fixed.** Measured at
`fda134a`: after `configure(sink=B)` and one orphan `info()`, `_lifecycle.join_closers` was wrapped
to run a `@trace` call and then `configure(sink=C)`, and `shutdown(timeout=5)` was called. The traced
call built a worker that was live and was not `_late_worker`, and the swap retargeted it to C. After
the shutdown returned, the worker was still live, and the next span reached C. Such a worker's epoch
equals the count the drain already moved, so it is the sequential case SPEC-044 FR-001 keeps: a
worker built after the drain's last move of the count delivers. The `SWAP` cell keeps retargeting
it, because a declined swap there would leave a live worker on B with no `incomplete_swaps` and no
line. That is the lost configure `_swap_sink`'s `Raises:` disowns.

**The changed cell: a swap during a drain with no live worker declines.** At `fda134a` a
`configure(sink=B)` that enters while a shutdown is draining, after the worker's stop has retired it,
finds no live worker, so it takes the no-worker branch: it releases detached every owed sink that is neither B nor held — which includes the
worker's sink once its drain thread has ended — and arms B. Measured at `fda134a` by wrapping
`_lifecycle._close_owed` so that its first call runs `configure(sink=B)` before delegating, after
`configure(sink=A)`, one traced call and `flush()`: A's `close()` ran on `log-foundry-sink-close`
and B's on `MainThread`; without the wrapper A's ran on `MainThread` and B was never closed. With no
live worker in `DRAINING`, the drain in progress owns every owed close, so the swap does nothing but
return: the
config already names B, and the first event that reaches B arms it as any orphan log does. A swap
that entered before the drain is past its entry and finishes by today's rules, where its retarget
declines.

**`CLOSED` keeps its arrows, and this is the decision.** Measured at `fda134a`: after a `shutdown()`
that built no worker, a `@trace` call built a live worker and its event was delivered; after a
`shutdown()` that stopped a worker, the same call was counted in `submitted_after_shutdown` and
announced, and the state stayed `CLOSED`; an orphan `info()` after either was delivered and armed
the sink again. All three stand. The first is SPEC-044 FR-001's recorded refusal of a permanent
retirement fence, and the count-not-latch reasoning of
`docs/decisions/pipeline/one-lifecycle-owner-the-bookkeeping-around-the-two-emit-paths-exists-once.md`;
the second is SPEC-019 and SPEC-030's "reported, not restarted"
(`docs/decisions/sink-lifecycle/a-terminal-shutdown-and-a-captured-sink-are-both-reported-not-prevented.md`);
the third is SPEC-032. Making `CLOSED` terminal would turn the first case's delivered events into
`submitted_after_shutdown`, which reclassifies a count in a minor release, and no invariant needs
it. What changes is the prose that says otherwise: `Runtime`'s docstring ("`shutdown()` is terminal
by design … nothing restarts") and the public `shutdown()` docstring ("Logging afterwards is
**accepted and undeliverable**") each state the two shapes.

#### Acceptance Criteria:

- [ ] Each of the thirty cells is driven by a test that asserts the cell's answer and every value
  its "→" names, read through `state`. A cell whose dispatch column is `SWAPPING` or `DRAINING` is
  reached through an injected preemption point that holds the other call inside. A cell with no test
  fails a totality check over `_Op × State`.
- [ ] The swap-during-drain probe above closes A on the calling thread and never closes B, on the
  worker path and the orphan path, and that `configure(sink=B)` returns at once.
- [ ] The tail probe above still retargets the tail-built worker to C, and C receives the next
  span. `BUILD` × `DRAINING` has a test per half: inside the window the worker is `_late_worker`
  and the shutdown stops it, and in the tail it is not and stays live.
- [ ] After a worker-less `shutdown()`, a `@trace` call is delivered and the state reads `RUNNING`;
  after a worker's `shutdown()`, it raises `submitted_after_shutdown` by one and the state reads
  `CLOSED`.
- [ ] SPEC-077's sequential table passes row for row unchanged, since none of its rows is a
  transient cell.
- [ ] `Runtime`'s docstring and `log_foundry.shutdown.__doc__` state both shapes of a later span,
  and no docstring in `src/` calls the state after `shutdown()` one from which nothing restarts.
- [ ] Invariants 1, 2 and 5: no cell raises into the caller, every event of every cell is delivered
  or counted once, and every sink written to in every cell is closed by the drain or the exit drain.

### FR-005: Slow work is one tracked operation, and no library close begins under a live drain thread

#### Description:

An `_Operation` is the slow half of a `SWAP` or a `STOP`, and the token SPEC-077's sets hold: added
under the lock in the call's first critical section, run outside it, and discarded without it. Its
`deadline` is computed once, when it is built, from the caller's timeout, and every slow
step reads what remains from it rather than from a local: the swap's `Worker.retarget` and its
closer joins; the shutdown's `Worker.claim_stop` (which records it for the exit drain's decline,
SPEC-062 FR-001), `Worker.stop`, the bystander wait and the graced join, through `grace_end`. The
emit on the drain thread is the tracked half of the drain: it is bounded by what SPEC-062 bounds it
by, the op's deadline as `claim_stop` recorded it. `BUILD`, `ARM` and `FLUSH` hold no state while
they run and create no operation.

**What happens to an emit still running when the deadline passes.** The operation ends at its
deadline: `Worker.stop` records `ShutdownTimeout`, the state settles to `CLOSED`, and the drain
thread is left inside `emit`. The guarantee this spec makes, on every take site — the shutdown's,
the swap's supersede and its fenced release, the exit drain's — is that **no close the library
starts begins while a drain thread of this process is alive and may be inside that sink**, however
long ago the deadline passed: each take asks `Runtime.held`, which answers from thread liveness
through `Worker.may_be_inside`, never from a deadline. So a close on a registry thread never overlaps
an emit on the drain thread for one sink.

**What it does not guarantee, and leaves to SPEC-076's closed-guard helper.** The guarantee covers
the drain thread only. A `close()` the library starts can still overlap an emit or a flush the
runtime does not track: an orphan log on an application thread (invariant 4's outside-a-span
half), `flush()` draining an owed sink's own buffer while a racing shutdown takes and closes it, a
`GRACED` close abandoned after the grace that a later orphan log reaches, and a third-party wrapper
closing its children directly. Each is SPEC-028's contract — a sink tolerates concurrent callers —
and what a sink does with an emit during or after its close (refuse after releasing its transport,
keep accepting when it released nothing, invariant 7) is that helper's.

#### Acceptance Criteria:

- [ ] Each `_Operation`'s `deadline` is set once, when it is built in the call's first critical
  section. In `_swap_sink` and `_shutdown_worker`, an AST scan finds the `timeout` parameter read
  only in that construction. Every call to `Worker.stop`, `Worker.retarget`, `Worker.claim_stop` or
  a thread join is passed `op.deadline` or a remainder computed from it, so today's
  `worker.stop(timeout, …)` (`_lifecycle.py:1866` at `fda134a`) fails the scan.
- [ ] A sink whose `emit` blocks on an event the test holds, past `shutdown(timeout=0.3)`: while the
  drain thread is inside it, a second `shutdown(timeout=0.3)`, a `configure(sink=B)` and the exit
  drain's function `_lifecycle._shutdown_worker(timeout=0.3)` each return without calling its
  `close()`.
- [ ] In that run, a recorder on the sink sees no `close()` overlap an `emit` on the drain thread.
- [ ] The SPEC-050 FR-004 tests (a sink left open by an unconfirmed swap is closed once the drain
  thread has ended) pass as SPEC-068 leaves them.
- [ ] `tests/test_worker_exit_budget.py` passes unedited, so the exit drain's retries still fit the
  `shutdown()` budget.
- [ ] Invariants 3 and 5: every wait in a `SWAP` or `STOP` ends by its operation's deadline or its
  `grace_end`, except the inline and `JOINED` closes §13 records as unbounded, and no sink a live
  drain thread may be inside is closed.

### FR-006: The model test drives the table, and the suites keep what they assert

#### Description:

SPEC-077's sequential driver and its oracle table stay, and the oracle stays **independent of the
code**: the test carries its own table, not `_current` or an import of anything in `_lifecycle`.
This spec adds a second file of directed tests, one per transient cell, each opening the window with
an injected preemption point in the style of `tests/test_lifecycle_races.py` (a hook in
`Worker.retarget` for `SWAPPING`, in `Worker.stop` or `_close_owed` for `DRAINING`), and the
totality check FR-004 names.

Tests that pin a removed mechanism are re-expressed, never deleted. Measured at `fda134a` with
`grep -rlw -- <name> tests/` over the ten removed names FR-001 lists and `_owed`, the files are `conftest.py`,
`test_api.py`, `test_concurrent_owed_closes.py`, `test_config.py`, `test_fork_lifecycle.py`,
`test_lifecycle_races.py`, `test_orphan_sink_handoff.py`, `test_owed_closes.py`,
`test_shutdown_lifecycle.py`, `test_sink_ownership.py`, `test_sink_release_roster.py`,
`test_worker.py` and `test_worker_predicate_roster.py`. Most edits are a path. The rest move a
monkeypatch from `join_closers` to the registry's graced join and keep the assertion.

Three derived guards are re-derived, never left to shrink. `tests/test_lifecycle_races.py`'s
under-lock lint keys on names (`_UNDER_LOCK_OFFENDERS = frozenset({"join_closers", "_close_owed"})`
at `fda134a`), so removing `join_closers` would empty half of it silently: the set names the
registry's join methods instead, and a mutant that calls one under `_state._lock` must fail it.
`tests/test_worker_predicate_roster.py`'s `_SITE_FLOOR` for `log_foundry._lifecycle` (43 at
`fda134a`) does not fall, and the state reads in the entries are filed with a category. SPEC-077's
lifecycle inventory test re-declares the moved fields, and its floor of lock and event construction
sites in `_lifecycle.py` and `worker.py` falls by exactly one, for the merged leaf, in the same
commit, with the merge named in the table's `reason`. The registry's rows carry the child values
FR-001 gives, and the token sets keep `reset: _clear_after_fork`.

#### Acceptance Criteria:

- [ ] The test names `pytest --collect-only -q` reports at the build's base are all present at the
  head; the only additions are this spec's.
- [ ] The directed tests and the totality check pass, and the sequential driver passes over its
  fixed seed range with the stored-versus-derived check of FR-003 after every step.
- [ ] A mutant calling the registry's graced join under `_state._lock` fails the under-lock lint,
  and the predicate roster's `_lifecycle` floor is at least 43.
- [ ] Each guard of a defect this spec closes is mutation-tested, and the named test goes red:
  - A swap that does not decline in `DRAINING` with no live worker: FR-004's swap-during-drain test.
  - A swap that declines in `DRAINING` with a live worker: FR-004's tail test.
  - A grace computed per wait instead of once per call: FR-002's elapsed test, on both paths.
  - `JOINED` threads joined under the grace:
    `test_concurrent_owed_closes.py::test_a_close_longer_than_the_closer_grace_still_delivers`.
  - The four `_move` mutants: the tests FR-003 names.
- [ ] `poetry run pytest`, `ruff`, `mypy`, `docstring-lint.py`, `spec-lint.sh` and `docs-lint.sh`
  pass.
- [ ] Invariants 1, 2, 3, 5, 6 and 12 are checked by the model test as SPEC-077 lists them, now
  against the stored state.

---

## Data Model

```
// src/log_foundry/_lifecycle.py

class State(enum.Enum): ...          # SPEC-077's five members, unchanged

class _Op(enum.Enum):                # the table's six rows
    BUILD = "build"; ARM = "arm"; SWAP = "swap"; FLUSH = "flush"; STOP = "stop"; FORK = "fork"

class _Operation:                    # the slow half of a SWAP or a STOP
    kind: _Op                        # SWAP or STOP
    deadline: float | None           # time.monotonic() + timeout, set once when built
    grace_end: float | None          # STOP only: set at the call's first graced wait, then reused

class _Rule(enum.Enum):
    INLINE = "inline"                # on the calling thread
    JOINED = "joined"                # a shutdown's fan-out thread, joined in full (SPEC-046)
    GRACED = "graced"                # detached; joined to the swap's budget, then to grace_end

class _Close:                        # one close in flight; holds no sink
    event: threading.Event           # set when the close ends, however it ended
    rule: _Rule
    thread: threading.Thread | None  # None for INLINE, and until started

class _CloseRegistry:
    owed: dict[int, Sink]            # was Runtime._owed; guarded by Runtime._lock
    _in_flight: dict[int, _Close]    # was _closing_now, _closers and _close_owed's local list
    _lock: threading.Lock            # leaf; was _closing_now_lock and _closers_lock
    _FORK_SKIP = ("owed",)

class Runtime:                       # SPEC-077's, with these changes
    _current: State                  # SPEC-077 rules 3-6 without the config term; written only by _move, under _lock
    _swaps_running: set[_Operation]  # SPEC-077's token set; tokens are now _Operations (was set[object])
    _stops_running: set[_Operation]  # likewise
    _closes: _CloseRegistry          # replaces _owed and SPEC-077's four close names
    _FORK_SKIP = ("_owned",)
    # removed: _owed, _closers, _closers_lock, _closing_now, _closing_now_lock
    # kept: _lock, _worker, _late_worker, _stop, retirements, _shutdown_running,
    #       _atexit_registered, _owned, _owned_lock, _marking_failed
```

Lock order after this spec: `Runtime._lock` → `_config_lock` → `_owned_lock`, and `Runtime._lock` →
`Worker._lock` and `_CloseRegistry._lock` as leaves, with nothing acquiring `Runtime._lock` beneath
any of them.

## API / Interface Contract

No public signature changes. The private contract:

```
_state._closes.arm(sink) -> None                  # caller holds _state._lock
_state._closes.take(sink, rule) -> _Close         # owed -> in flight in one call; caller holds _state._lock
_state._closes.put_back(sink, close) -> None      # caller holds _state._lock
_state._closes.register(sink, rule) -> _Close     # a release with no take
_state._closes.finish(sink, close) -> None        # sets the event outside the leaf lock
_state._closes.in_flight(sink) -> bool            # leaf lock only; held() and in_flight() ask it
_state._closes.join_joined(closes) -> None        # every JOINED thread, in full
_state._closes.wait_graced(op, mine) -> None      # bystander wait and GRACED joins, to op.grace_end
_state._closes.graced_alive() -> int              # Health.closing_sinks
_state._move() -> None                            # caller holds _state._lock; re-settles _current
_Operation(kind, timeout)                         # built and added to its token set in the call's first critical section;
                                                  # discarded in a finally with set.discard, no lock (SPEC-077)
_state.state -> State                             # lock-free; DRAINING / SWAPPING from the token sets, else _current
                                                  # (a stored UNCONFIGURED reported RUNNING once a sink resolved)
```

## File & Folder Structure

```
src/log_foundry/_lifecycle.py                 # _CloseRegistry, _Close, _Rule, _Op, _Operation; Runtime's stored state
src/log_foundry/worker.py                     # docstrings only (the moved names)
src/log_foundry/__init__.py                   # shutdown()'s docstring (FR-004)
tests/test_lifecycle_transitions.py           # the directed cell tests and the totality check
tests/test_invariants_model.py                # the stored-versus-derived check
tests/conftest.py and the twelve files in FR-006
docs/decisions/pipeline/                      # two rewritten in place, SPEC-077's rewritten
```

## Implementation Phases

### Phase 1: The registry (FR-001, FR-002)

- Build `_CloseRegistry` and move `_owed`, the in-flight records and the thread list onto it; route
  the three takes, the put-back and the five wrapper releases through its methods.
- Give each entry its rule; fix `grace_end` once per call; delete `join_closers`.
- Re-express the tests that pin the removed names; add the double-grace probe as a test on both
  paths; re-derive the under-lock lint.

### Phase 2: The stored state (FR-003, FR-004)

- Add `_current` and `_move`; make the token sets hold `_Operation`s; settle in the
  child handler.
- Read the state at each entry; add the declining swap; correct the two docstrings.
- Write the directed cell tests and the totality check.

### Phase 3: Operations and the model test (FR-005, FR-006)

- Move every slow step's deadline onto its operation; add the stuck-emit test.
- Add the stored-versus-derived check to the sequential driver; run the mutation set.
- Record the decisions and regenerate the indexes.

## Documentation

The build rewrites in place, each with a dated History entry:
`docs/decisions/pipeline/the-close-is-once-only-across-both-delivery-paths-the-atexit-registration-is-not-the-thing-being-guarded.md`,
whose fence ends "the wait takes `join_closers`' grace" and becomes the call's one grace from the
registry; and the pipeline decision SPEC-077 records ("`State` is an observer until a later spec
makes the transitions checked"), which becomes the stored authority, the transition table, the
declining swap, and `CLOSED` keeping its arrows. It adds a History line to
`docs/decisions/pipeline/a-worker-guard-asks-one-of-three-questions-and-the-set-is-enforced-rather-than-remembered.md`
(the owed-close record and the join rules now live in the registry) and to
`docs/decisions/sink-lifecycle/a-terminal-shutdown-and-a-captured-sink-are-both-reported-not-prevented.md`
(confirmed as the table's `CLOSED` column; no reversal). `architecture.md` §9.2 and the §13 entry on
`shutdown()`'s timeout are updated for the registry, and `docs/component-inventory.md`'s lifecycle
row for the registry. The release notes for 1.2.0 carry the two fixes.

## Revision history

- Drafted from the stability plan's Change 1, items 2 to 4, its five-state diagram and its step 9,
  on SPEC-077 as drafted. Every count and both defects were re-measured at `fda134a`; the plan's
  figures were taken at `ce2e1fe`.
- The plan's "all are joined under `closer_grace`, which SPEC-046 measured as necessary" is not
  what SPEC-046 measured: it measured concurrency as necessary and the grace-capped join as losing
  data. FR-002 keeps SPEC-046's rule and Out of Scope says why.
- The plan's diagram has no arrow out of `Closed`; FR-004 records the three the code and the
  decisions require.
- Aligned with SPEC-077 as revised after its review, before this spec's own review.
  - The `_ops` tuple this draft introduced is gone. SPEC-077's token sets stay, now holding
    `_Operation`s.
  - `_current` now stores only the settled state, written inside existing critical sections, so
    none of SPEC-077 FR-002's pinned lock counts moves. The first draft's lock-taking `_end` would
    have added one acquisition to every `SWAP` that has work and every `STOP`. That would have made
    `configure(sink=…)` 3 with a live worker and 2 with no live worker and something owed, and
    `shutdown()` 3 and 4 on its two branches.
  - The `SWAP` row follows SPEC-077's split on something being owed.
  - The registry's rows carry the child-repair column.
  - The four changes to SPEC-077's names are listed in the Overview.
  - The orchestrator confirmed SPEC-046's join rule as the fixed decision.
- After the spec review, five findings were addressed.
  - The declining swap is narrowed to "no live worker". The reviewer's tail probe showed a worker
    built in a no-worker drain's tail is live and unregistered, and that a decline there loses the
    configure. `BUILD` × `DRAINING` is split by the `_shutdown_running` window, and `STOP`'s "→"
    names the tail exception.
  - Dispatch and "→" are defined: dispatch reads the token sets, then `_current`; "→" reads `state`.
  - `_current` leaves out the config term. `_move` sits in the four sections where removing it is
    observable, each with the test that kills it. The equivalent `_move` calls in `_close_owed`,
    `_swap_sink` and the child handler are dropped.
  - The SPEC-046 citation is corrected, the `grace_end` criterion tightened, the FR-005 deadline
    scan made to discriminate, the FR-005 title narrowed, and `closing_count` added to the removed
    names.
