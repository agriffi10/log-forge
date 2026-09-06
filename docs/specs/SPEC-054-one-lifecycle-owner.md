# Spec: One Lifecycle Owner for Both Delivery Paths

**ID:** SPEC-054
**Status:** Completed
**Last Updated:** 2026-09-04
**Depends On:** SPEC-030, SPEC-033, SPEC-035, SPEC-040, SPEC-044, SPEC-045, SPEC-046, SPEC-050

## Overview

The library delivers events two ways: a `@trace`d function hands its buffer to a background
worker, and a level call with no active span emits on the caller's thread with no worker at all.
Those two *emit* paths are settled and stay (arch §9, SPEC-028). What this spec is about is the
bookkeeping around them — who owes a sink a close, which stop event a sink holds, whether the
process is retired, whether a close is in flight — which today exists **twice**: once as
attributes of `Worker`, once as attributes and module globals in `_lifecycle`, with a set of
predicates whose only job is to let each side ask the other what it holds. `docs/invariants.md`
invariant 6 names the consequence: a fix lands on one twin and the other regresses, and the page
lists three occasions. This spec measured the shape before proposing to change it.

**Measured at `98c7e78`, by reading both modules end to end.** Thirteen twin pairs, each a
mechanism implemented once per path, listed with `file:line` under *The twins* below. Across the
seven specs that fixed lifecycle defects (SPEC-030, 033, 035, 044, 045, 046, 050), **13 of their
36 functional requirements** edited a mechanism on both sides or added a guard on one side that
reads the other side's state; the per-spec table is under *The twins*, with the rule applied
strictly — an FR *measured* on both paths whose edit landed on one side does not count. SPEC-040 is excluded from
that count because it moved rather than fixed: it put the orphan side's seven globals onto one
object and left the worker's on `Worker`, so "one owner" has been the name of a file since
2026-08-11 and not yet a fact. In the two modules, docstrings run to roughly 2,600 of 4,041
lines, and a large share of that prose explains how the two sides stay in step — the four
questions, the cross-pruning between two records that can name one sink, the grace "granted
once, by whichever path owns this call".

**The probe found a live divergence on the first run.** `docs/invariants.md` §2 states, as an
observable, that *loss a sink absorbed is in `health().sink`*. Against a `MultiSink` whose first
child raises, one `info()` inside a span reports `health().sink == SinkLosses(dropped=0,
failed=N)`; the same call **outside** a span reports `health().sink is None` while the sink's own
`losses()` reads `failed=1`. `_worker_health`'s no-worker branch synthesizes eight fields and not
that one. This is not a bug anyone hid: `Health.sink`'s docstring says "`None` when there is no
worker", which is the two-owner shape describing itself as the contract. It is the exact
invariant-6 shape — SPEC-026 FR-003 wrote the field into `Worker.health()`, and the orphan
branch was written by other specs from a list of what a worker reports. A second probe, on the
post-`shutdown()` log call, found the twins differ **by design**: the orphan path delivers the
late event to a sink that still accepts and counts nothing, the worker path queues it forever and
counts `submitted_after_shutdown=1`. That difference is SPEC-030's fence and this spec keeps it.

**The conclusion is that the merge is worth making, and that it is four phases rather than one.**
The alternative — record the duplication as a constraint — was weighed against the numbers above
and against the cost of the merge, which is real: about 200 sites in 14 test files name the internals
that go away (the pattern is stated under *Risks*), three derived lints are keyed on them, and the guard roster loses two of its four
categories. The case for building it is that the invariant-6 review obligation is currently the
*only* thing keeping the twins in step, and the probe shows it has already missed one. After
this spec the two emit paths still exist and invariant 6 still applies to them; what stops
existing is the second copy of every lifecycle decision.

## Scope

### In Scope

- One owner — `_lifecycle._Lifecycle` — for the retirement latch, the stop signal, the
  owed-close record, the in-flight-close gate and the exit closer, on both delivery paths
  (FR-001 to FR-003).
- `Worker` reduced to a drain engine: a queue, a thread, the markers, the counters, and two
  answers about its own thread that only it can give. It is handed its sink and its stop event
  and asks the owner whether the process is retired.
- The guard roster re-derived for the questions that survive, and the derived lints re-keyed
  (FR-004).
- `health()` and `flush()` reading one record, which closes the divergence the probe found
  (FR-005).
- The forked child's repair covering the merged state in SPEC-039's order (FR-006).
- Six FRs, under the ceiling. The seam between FR-002 and FR-003 is real — a merged record can
  be read by two closers for one phase — but the phases below do not use it, because a record
  with two readers is the shape SPEC-050 FR-004 measured as `A.closes == 2`.

### Out of Scope

- **Any change to how an event is emitted.** The orphan path keeps all three of its settled
  properties: it emits on the caller's thread, it builds no worker in order to log, and it never
  waits behind the drain (arch §9, SPEC-028). The one owner is a passive object; `api._log`'s
  branch is unchanged except for the name of the function it calls to arm the close.
- **Any change to what a post-`shutdown()` log call does** (SPEC-030). The probe's second
  finding — delivered-or-refused on the orphan path, queued-and-counted on the worker path —
  is the consequence of the two emit paths and is kept. `Health.retired` and
  `submitted_after_shutdown` keep their meanings.
- **Restarting a worker** (SPEC-019). Retirement stays terminal; a worker built after a
  `shutdown()` returned still delivers, which `_worker_health` settled and SPEC-044 FR-001
  deliberately preserved.
- **Bounding `Sink.close()`.** It is unbounded on both paths (arch §13) and stays so. FR-003
  merges who waits for a close and for how long; it bounds no close.
- **The ownership record** (`_owned`, `stamp`, `releasable`, `reclaim`, `_mark_inherited`) and
  `release()` as the one path by which the library closes a sink (SPEC-042). Every close this spec
  moves still goes through `release()`.
- **`Worker._release_waiters`' read of `queue.Queue` internals**, the flush-marker machinery, and
  every counter `Health` carries. Only where the snapshot is *assembled* changes (FR-005).
- **The two open items about the record's last entry** — `health().inherited_sink` reading it,
  and the predicate roster's weight (arch §12). FR-005 answers `inherited_sink` from the config
  as §12 already prescribes, since the record it read from no longer exists; the roster question
  is narrowed by FR-004 and not settled.
- **`docs/invariants.md` invariant 6 itself.** The sync and async decorator bodies, `emit` and
  `flush`, `_drain` and `_final_drain`, and the two answerers of a flush marker are twins this
  spec does not touch. The invariant stays; the worker/orphan pair drops out of its first clause.

---

## The twins

Thirteen pairs at `98c7e78`. "Worker" is `src/log_foundry/worker.py`, "orphan" is
`src/log_foundry/_lifecycle.py`; a line number is the attribute's assignment or the `def`.

| # | Mechanism | Worker side | Orphan side | Bridge between them |
|---|---|---|---|---|
| 1 | Retirement latch | `_shutdown_done` :359, latched :1234 | `_state._orphan_retired` :158, set :1797 | `_worker_health` reads them with `or` :2209 |
| 2 | Stop event | `_stop` :356, set :1248, waited :1677 and :1824 | `_state._orphan_stop` :157, set :1798 | `refresh_stop_signal` :246 replaces one of the two |
| 3 | Stop-signal offer | `_offer_stop_signal` :488 | `_offer_orphan_signal` :1389 | `worker_owns_now` :298 exists only to arbitrate between the two events |
| 4 | Owed-close record | `_sink_closed` :360, `_unclosed_swaps` :363 | `_state._orphan_owed` :155, `_state._orphan_closed_sink` :156 | `_discard_owed_swap` :1063 and `discharge_owed` :1560 prune each other's record, because both can name one sink |
| 5 | The close | `_close_if_owed` :1311, `_close_sink` :1409 | `_close_orphan_sink` :1590, `_close_owed` :1449 | identical `_diag` text at :1427 and :1482 |
| 6 | In-flight-close gate for a bystander | `_closing` slot :361, :1393 | `_orphan_closing` :374 and `_orphan_idle` :413 | none — a worker `shutdown()` pays the orphan gate's grace (:407 records it) |
| 7 | Bystander grace arithmetic | `_closer_grace` :46 | `_bystander_grace` :1535 | `join_closers` :1138 is a third copy of the same `min(grace, remaining)` |
| 8 | The exit grace grant | `shutdown` → `_join_closers` :1268 | `_shutdown_worker` → `join_closers` :1816 | arranged by hand so it is granted once (:1738 docstring) |
| 9 | The sink swap | `swap_sink` :940 | `_swap_sink`'s no-worker branch :1933–1946 | `_adopt_declined_swap` :1959 re-homes a sink the worker refused mid-swap |
| 10 | Fork residue reset | `_reinit_after_fork` :371 clears `_closing`, `_unclosed_swaps` :467–469 | `_clear_closing_after_fork` :441 zeroes `_orphan_closing`, sets `_orphan_idle` | two handlers, both registered at :2215–2217 |
| 11 | `Health` assembly | `health()` :677 | `_worker_health`'s no-worker branch :2198–2207 | the probe's divergence lives here: `sink=` is assembled on one side only |
| 12 | Sink-buffer flush target | `[worker.sink]` | `_orphan_owed.values()` | one function, `_flush_live_sink` :2003, branching on `live_worker()` |
| 13 | Shutdown entry | `shutdown` :1154 | `_shutdown_worker` :1738 | the second is both the router and the orphan path's shutdown |

The four questions of arch §9.2 — `worker_exists` :184, `live_worker` :213, `worker_owns` :274,
`worker_owns_now` :298 — are the interface between the two owners: they let the orphan side ask
what the worker holds. `tests/test_worker_predicate_roster.py`'s table has 48 rows at `98c7e78`:
21 existence, 14 liveness, 10 ownership, 3 ownership ∧ moment.

**FRs that touched both twins**, by reading each FR's description and its delivery doc. The rule:
an FR counts when its delivered change edited a mechanism on both sides, or added a guard on one
side that reads the other side's state. Documentation-only FRs never count.

| Spec | FRs | Both twins | Which |
|---|---|---|---|
| SPEC-030 | 4 | 0 | — |
| SPEC-033 | 7 | 4 | FR-002 (swap keyed on `_worker.sink`), FR-003 (closer moved off `Worker` for both), FR-004 (orphan signal skipped on worker ownership), FR-005 (one module, both callers) |
| SPEC-035 | 5 | 3 | FR-001 (`Worker.draining` added for the orphan site), FR-002 (roster over both), FR-003 (`swap_sink` returns, `_swap_sink` re-homes) |
| SPEC-044 | 6 | 3 | FR-001 (depth counter plus `Worker(sink_released=)`), FR-002 (`_get_worker` decides the orphan record's close), FR-004 (worker branch latches the orphan slot). Not FR-003, whose edit is `_lifecycle` alone though it was measured on both paths, and not FR-006, which is documentation and a test |
| SPEC-045 | 5 | 1 | FR-001 (consumers include `_get_worker` and the swap's worker branch) |
| SPEC-046 | 4 | 0 | — |
| SPEC-050 | 5 | 2 | FR-002 (widened to both paths by its own spec review), FR-004 (`_unclosed_swaps` on the worker, `discharge_owed` on the orphan record) |
| **Total** | **36** | **13** | |

**The probe.** Two scripts, each running one scenario per delivery path in a fresh interpreter;
the implementer re-runs both against the tree the build starts from and again at the end.

1. `configure(sink=MultiSink(Failing(), MemorySink()))`, one `info()`, read `health().sink`.
   Inside a span: `SinkLosses(dropped=0, failed=3)` — three events, three child failures. Outside:
   `None`, with `sink.losses()` reading `failed=1` and `orphan_lost=0`. **The one-owner shape
   cannot produce the second line**, because FR-005 assembles the snapshot once from one record.
2. `shutdown()`, then one more log call. Outside a span the event is in the sink and
   `submitted_after_shutdown=0`; inside, `queued=1`, `submitted_after_shutdown=1`, one stderr
   line. **Both shapes produce this**, and the spec keeps it: it is a property of the emit paths.

The reverse question — something the two-owner shape does that one owner could not — was looked
for and not found. The orphan path's freedom from a thread, from an `atexit` registration until
an event lands, and from the drain, are properties of the emit and stay. What the two-owner shape
does that one owner cannot is disagree with itself, which probe 1 shows.

---

## Functional Requirements

### FR-001: One retirement latch and one stop signal, held by the lifecycle owner

#### Description:

`_Lifecycle` holds `retirements`, a count incremented under `_lock` at the top of
`_shutdown_worker` and **once more, under the same lock, immediately before that call stops a
late worker** (FR-003) — a late worker records the already-incremented count at its build, so
without the second move the call that stops it would leave it reading as live, delivering
nothing, with its next submission uncounted and a later swap waiting a whole budget for a fence
it cannot confirm, all three measured on a model built from this spec — and `_stop`, the one `threading.Event` every sink and the worker's
drain loop wait on. `health().retired` is `retirements > 0`. `Worker` loses `_shutdown_done`,
`retired`, `_stop` and `_offer_stop_signal`: it is handed its event at construction, holds that
object for the life of its thread, records the count at its build as `_epoch`, and where it read
its own flag — `submit`'s two reads, `flush`'s guard, `swap_sink`'s two re-checks — it reads
`_lifecycle._state.retirements > self._epoch`. **A count rather than a boolean, because a
boolean changes what `submitted_after_shutdown` means.** A worker built after a `shutdown()`
returned still delivers (SPEC-044 FR-001's preserved case), and against a latched boolean every
event it delivered would be counted as "queued where nothing will drain it" (`Health`'s
definition, SPEC-030 FR-001). Against the count, that worker's epoch equals the count until the
*next* `shutdown()` moves it, which is exactly when its submissions start being stranded — the
same instant today's per-worker flag latches, one call earlier in the same critical section.
`live_worker` is `_worker` while its epoch is current.

The refresh rule is unchanged (SPEC-033 FR-004): an event that is already set is replaced with a
fresh one before it is offered, and a worker built after a `shutdown()` returned is handed a
fresh one for the same reason a sink adopted then is — it delivers, and a set event would collapse
every backoff to zero. A worker that already holds the set event keeps it: a retired worker is
never restarted (SPEC-019), so the replacement can only ever reach a later deliverer. The skip
rule is unchanged too (SPEC-035 FR-001, SPEC-044 FR-003): the sink's signal is not replaced while
the worker is still *draining* into it or a close of it is running, which is `in_flight(sink)` in
FR-004 — the same predicate `worker_owns_now(sink) or _closing(sink)` computes today. It reads
`worker.draining`, not thread liveness, on purpose: an abandoned drain counts as not draining, so
a sink still written to after an expired `shutdown()` gets a fresh event rather than SPEC-033
FR-004's tight retry loop.

Measured cost of the read that moves: a `self` attribute read is 4.1 ns and a read through the
module global is 5.9 ns on this machine, so `submit` costs under 2 ns more on each of two reads.
SPEC-040 held the traced call to a ~6.6 ns addition; this is inside that and must be re-measured
on the build, not assumed.

#### Acceptance Criteria:

- [ ] `grep -n '_shutdown_done\|_orphan_retired\|_orphan_stop' src/log_foundry/` returns nothing,
      and `grep -n '_stop = threading.Event()' src/log_foundry/worker.py` returns nothing, so no
      stop event is built there. `Worker._stop` survives as
      the reference to the owner's event that the constructor was handed; `_lifecycle._state`
      holds the only latch — the count — and builds the only stop event, and `_worker_health` no
      longer computes `retired` with an `or`.
- [ ] After `shutdown()` on a process that only ever logged outside a span, and after one that
      only ever logged inside one, `health().retired` is `True` and comes from the same attribute.
      A worker built after that `shutdown()` returned delivers with `submitted_after_shutdown`
      staying at 0, and a second `shutdown()` then strands and counts its next submission — probe
      2's worker line, re-run after a prior orphan-only shutdown. Invariant 2.
- [ ] The tests under `tests/test_orphan_sink_handoff.py`'s FR-004 banner —
      `test_a_sink_emitted_to_after_shutdown_still_backs_off` above all — the tests under
      `tests/test_shutdown_lifecycle.py`'s FR-001 banner, and
      `tests/test_lifecycle_races.py::test_a_close_in_flight_keeps_the_stop_signal_it_was_given`
      on both of its parametrizations pass with their assertions unchanged. Invariants 3, 6.
- [ ] A worker built after an orphan-only `shutdown()` returned delivers its events, and a
      `shutdown()` after that stops it: pinned by the existing
      `test_a_worker_built_after_shutdown_returned_still_delivers` plus one assertion that the
      second shutdown's drain finished. Invariant 2.
- [ ] `Worker.submit` measured before and after over 1M calls on this branch's tree, the delta
      recorded in the delivery doc in nanoseconds, and under 10 ns per call. Invariant 4.
- [ ] `tests/test_invariants_model.py` green over every seed. Invariant 6.

### FR-002: One owed-close record, with one arming rule and one discharge rule

#### Description:

`_Lifecycle._owed: dict[int, Sink]` replaces four things: `_state._orphan_owed`,
`_state._orphan_closed_sink`, `Worker._sink_closed` and `Worker._unclosed_swaps`. It names every
sink the library owes a close. **Arming:** a sink enters the record when an orphan emit lands on
it (SPEC-031 FR-006's rule, kept: a configured sink nothing wrote to is never armed), when a
worker is built on it, and when a swap installs it in a worker — the worker path's rule that
`shutdown()` closes the live sink whether or not anything was emitted since the swap, kept.
**Discharge:** a sink leaves the record at the moment a close of it *starts*, under `_lock`, in
the same critical section that decides who performs it. That is `take_orphan_owed`'s rule
(SPEC-045 FR-003) applied to the whole record, and the cross-pruning pair — `_discard_owed_swap`
and `discharge_owed` — goes away because there is no second record to prune.

**A sink written to after its close is owed another** (SPEC-045 FR-002) becomes the only rule
about re-arming, and two mechanisms that contradict it are retired, said out loud:

- **The closed-sink latch** (`_orphan_closed_sink`, SPEC-033 FR-001, SPEC-044 FR-004) refused
  re-arming for the *most recently* closed sink only — SPEC-044 FR-004's own text records that
  "after closing A then B, A is forgettable and re-armable again" — and `docs/decisions/` describes
  the same slot as naming "a sink a swap left *open*", which is a different claim. Under one
  record an emit that lands re-arms, always; what the latch protected against — a close performed
  against a sink the drain thread may still be inside — is answered at close time by the moment
  question (FR-003), not by refusing the arming.
- **`Worker(sink_released=…)`** (SPEC-044 FR-001) made a worker built during a `shutdown()`
  inherit a discharged close for a sink that shutdown's orphan branch had just closed. Under one
  record the flag has no work to do: the late worker's sink is armed by its build, and the
  closer's second pass (FR-003) closes it after that worker's drain. Where the worker registered
  before the first pass took anything, that is the sink's only close; where it was built *during*
  the first pass's close — the ordering the flag existed for, which is possible because
  `_shutdown_running` spans the close, exactly as today — the sink is closed a second time, after
  the events that worker delivered into it, which SPEC-045 FR-002 says is one close per
  write-epoch and not a double.

The other observable this changes is a sink written to after a **completed** close: an explicit
`shutdown()`, then an `info()` outside a span to a sink that still accepts, then `atexit`. Today
the latch refuses the re-arm for that sink and its post-close batch is never flushed; after this
spec it is re-armed and closed again, which is SPEC-045 FR-002's rule and one close per
write-epoch rather than a double. **A sink re-armed while its close is still running is not
closed concurrently.** The closer takes only sinks that are not `held` — no close registered
against them and no drain thread that may be inside them (FR-004's second predicate, not the
refresh's) — so an emit that lands during a close leaves the sink in the record; the closer that
finds it held waits out the grace and then takes, once more, whatever that wait released. That
second pass never waits, and a sink re-armed during *it* stays for a later `shutdown()`, which is
the same tail SPEC-050 FR-002's bystander already accepts at `atexit`.

**A worker's build arms its sink and releases nothing.** Today `_get_worker` releases, detached,
any orphan-owed sink other than the one it built on (SPEC-044 FR-002); under one record that sink
simply stays owed, and is released by the `configure()` that superseded it — a swap always
follows the config write that made the build see a different sink — or at exit. A behaviour that
disappears, said so rather than left to be discovered.

`sinks/base.py` still asks an implementation to make `close()` idempotent and the library still
does not rely on it (SPEC-044): every close performed here is either the first close of a sink
armed by a build, a swap or a landed emit — a live target is closed at exit whether or not
anything was written since it was installed, the worker path's rule today — or a close that
follows a write since the previous one. Never two for one write-epoch, ~~without exception~~ —
corrected during the build: a swap the worker **declines** arms the new sink anyway (FR-003), so
a `shutdown()` that lands inside a declining swap can close that sink and the exit close it again
with nothing written between. The alternative is not arming it, which costs SPEC-035 FR-003's
guarantee that a declined sink is owned by somebody; three shipped tests hold that guarantee and
all three fail without the arming. The redundant close is therefore the accepted trade, and it is
the first clause of this sentence rather than a violation of the last: a live target the config
installed, closed at exit.

The record is declared in the owner's `_FORK_SKIP`, for the reason `_owned` and
`Worker._unclosed_swaps` are: it pins superseded sinks, and the repair walk would run their fork
hooks. A live target is still reached through the config and `worker.sink`, and
`_inheritance_roots` reads the record directly, as it reads `_orphan_closed_sink` today.

#### Acceptance Criteria:

- [ ] `grep -n '_orphan_owed\|_orphan_closed_sink\|_sink_closed\|_unclosed_swaps\|sink_released\|discharge_owed\|_discard_owed_swap' src/log_foundry/`
      returns nothing.
- [ ] Every scenario in `tests/test_owed_closes.py` and `tests/test_concurrent_owed_closes.py`
      passes with its close-count assertions unchanged, on both paths where it is parametrized.
      Invariant 5.
- [ ] The scenario SPEC-044 FR-004 reproduced with a preemption point at `_ensure_sink` — an
      orphan emit resolving A before a swap and emitting after it — ends with A closed **twice**,
      the second close *after* the late event landed, asserted by recording the sink's event count
      at each close. Not `A.closes == 1`, which is the assertion this supersedes. Invariant 5.
- [ ] SPEC-044 FR-001's three race tests in `tests/test_lifecycle_races.py`: the two where the
      worker registers before the close pass with `closes == 1` unchanged; the one where it is
      built *during* the close (`test_the_racing_shutdown_closes_the_sink_once_when_the_orphan_branch_closes_first`)
      is re-stated to `closes == 2` with the second close after that worker's events landed,
      asserted by the sink's event count at each close — and performed by the **first**
      `shutdown()`'s second pass, so the count is already 2 when that call returns.
      `assert not _drain_threads()` holds in all three: a worker built during the close is still
      registered and stopped. Invariant 5.
- [ ] Two `shutdown()` calls racing while an `info()` outside a span lands on the sink the first
      is closing, with a close the test can release: `close()` is never entered on two threads at
      once, asserted by a sink that counts concurrent entries. Released inside the grace, the
      **second caller** performs the sink's next close after its wait; held past the grace, the
      second caller returns with the sink still in the record and a **later** `shutdown()` closes
      it. Two tests, one per branch. Invariant 5.
- [ ] `tests/test_lifecycle_races.py`'s two lints on the record —
      `test_every_site_that_clears_the_orphan_record_declares_its_disposition` and
      `test_the_owed_close_record_is_only_ever_mutated_in_place` — are re-keyed to the new record
      and still redden when a site rebinds it or empties it outside the one transition. Proven by
      planting each mutant once.
- [ ] `tests/test_lifecycle_races.py::test_a_forked_child_does_not_hook_a_superseded_sink` passes
      against the merged record, and a child of `configure(A)` → `info()` → `configure(B)` runs
      `reacquire_after_fork()` on B only. Invariant 12.
- [ ] `tests/test_invariants_model.py` green over every seed, and over a widened `SEEDS = range(64)`
      run once locally with the count recorded in the delivery doc. Invariant 6.

### FR-003: One closer for every exit, with one in-flight gate and one grace

#### Description:

`_lifecycle._close_owed(deadline)` replaces `Worker._close_if_owed`, `Worker._close_sink`,
`_close_orphan_sink` and `_close_owed`. `_shutdown_worker` calls it on both paths after the drain
— `Worker.stop(timeout)`, which is what remains of `Worker.shutdown`: **set the event it holds**
— the owner set `_state._stop` at the top of the call, but a late worker's build refreshed it, so
a late worker and its sink hold an event nothing else will set; measured, a 3 s backoff bounded
the stop at 3.01 s against 0.06 s with the set in place — queue the sentinel, join, record
`ShutdownTimeout` and release waiters on expiry, wait on `_drain_settled` on the idempotent
path. The closer, under `_lock`:

1. takes every owed sink that is not `held(sink)` (FR-004): a release registered in
   `_closing_now`, or `worker.may_be_inside(sink)` — the worker's answer, true while its thread
   is **alive** for its live sink and for a swapped-out sink whose fence was not confirmed
   (SPEC-050 FR-004's `_unclosed_swaps`, now `Worker._held`, the sinks its own thread may be
   inside, pruned on a confirmed fence or a re-adoption). Thread liveness, not `draining`: after an
   expired `shutdown()` the thread is alive inside `emit` and the sink is left open (SPEC-027
   FR-004), which is the opposite answer from the one the signal refresh needs in that state.
   The worker asked is `_state._worker`; `_late_worker` is the same object when it is set, and is
   never the only handle consulted;
2. registers each in `_closing_now`, which becomes `dict[int, threading.Event]` so a bystander
   has something to wait on — the per-sink registration `release()` already brackets (SPEC-044
   FR-003), carrying an event instead of nothing. **The discharge and the registration are one
   critical section, performed by the thread that decides the close**, a detached one included:
   today a detached `release()` registers on the daemon thread it starts, after the caller has
   released `_lock`, and with the latch gone that gap is a sink neither owed nor in flight, which
   a preempted orphan emit re-arms and a racing `shutdown()` then closes alongside;
3. closes them the way SPEC-046 settled: one on the calling thread, the rest on threads that are
   joined in a `finally`. The inline one is the worker's live sink where a worker exists, else the
   configured sink where it is owed, else the most recently armed — so `shutdown()`'s own close
   stays inline on both paths (SPEC-030), which is the risk below.
4. A sink the worker held at the moment its thread ended — in `_held`, not `worker.sink`, with
   the thread no longer alive, which is the unconfirmed-swap case and is decided at the take —
   is released **detached** and granted only the grace, by `join_closers` at the end of the
   call, keeping SPEC-050 FR-004's decision: it already had the swap's whole budget, so it is far
   more likely stuck than slow, and joining it would let one stuck swapped-out sink hold the
   exit where today it costs the grace.

A caller whose first pass found a sink with a close **registered** waits on every event in
`_closing_now` for `closer_grace(deadline)`, the one arithmetic that replaces `_closer_grace`,
`_bystander_grace` and `join_closers`'s inline copy: `min(DEFAULT_CLOSER_GRACE, remaining)`, the
cap for `None`. No registered event, no wait: a sink held only by a drain thread — the expired
`shutdown()`, or a late worker mid-drain — costs no grace, since nothing will release it inside
one. **The second pass runs only when there is something for it to do** — the first pass found a
held sink, or a late worker was registered — and it never waits. Run unconditionally it defeats
FR-002's two-caller criterion: the first caller, just out of its own inline close, re-takes the
re-armed sink before the bystander returns from its wait, on every run of a model built from
this spec. `_shutdown_worker` stops the worker it found, runs the first pass, then — with
`_shutdown_running` raised across that pass on the no-worker branch, as it is across today's
close, so a worker built meanwhile is registered; the worker branch never raises it, because
`_get_worker` returns the existing retired worker there rather than building one — reads and
moves `retirements` once more under `_lock` and stops the late worker SPEC-044 FR-001
registered, and runs the second pass, which closes what
that worker's build armed and whatever the first pass's wait released. Nothing runs a third. The process-wide count and idle
event go, so a worker-path `shutdown()` no longer pays for an orphan close of a sink it never
touched (twin 6's recorded cost) — it waits only on closes in flight, which is what both
docstrings say the wait is for. `join_closers` is granted once, at the end of `_shutdown_worker`,
on every path.

`_swap_sink` becomes one function. With a live worker it calls `worker.retarget(new, deadline)` —
drain, reassign, fence — which reports *declined* (retired mid-swap) or *fenced*; the owner then
arms `new` whether or not the worker adopted it, which is `_adopt_declined_swap` with nothing to
adopt, and releases the old sink detached and joined to the budget where fenced, or leaves it in
the record where not, counting `incomplete_swaps` as today. Every other owed sink that is neither
the old nor the new one is released detached and joined to the budget, on both branches. With no
worker the drain step is skipped, and `new` is armed **only when something was owed** — today's
branch, kept, because a `configure(A)` then `configure(B)` with nothing ever written must arm
nothing (the arming rule in FR-002); a worker's adoption is what arms a sink on the other branch,
and there is no adoption here.

#### Acceptance Criteria:

- [ ] `grep -n '_close_if_owed\|_close_orphan_sink\|_close_swapped_out\|_join_closers\|_closer_grace\|_bystander_grace\|_orphan_closing\|_orphan_idle\|_adopt_declined_swap' src/log_foundry/`
      returns nothing.
- [ ] On both paths, the live sink's `close()` runs on the thread that called `shutdown()`,
      asserted by the sink recording `threading.get_ident()` inside `close()`. Invariant 5 and the
      SPEC-028 objection.
- [ ] Four owed sinks with 2-second closes on the worker path: `shutdown()` returns in under 4 s
      and all four closes complete — SPEC-046 FR-001's two criteria, which
      `tests/test_concurrent_owed_closes.py` tests only on the orphan path today. Invariant 3.
- [ ] A second `shutdown()` arriving while the first is inside an inline close waits for it up to
      `min(DEFAULT_CLOSER_GRACE, remaining)`, on both paths —
      `tests/test_worker.py::test_a_second_shutdown_waits_for_an_inline_close_it_did_not_claim` and
      `tests/test_shutdown_lifecycle.py::test_an_orphan_only_second_shutdown_waits_for_the_close_in_flight`
      unchanged — and a worker-path `shutdown()` with no close in flight against any sink returns without
      waiting the grace, asserted at under 0.2 s wall and ~0 CPU. Invariant 3.
- [ ] After an unconfirmed swap and a clean `shutdown()`, the stranded sink is closed once the
      drain thread has ended and its close is detached —
      `tests/test_worker.py::test_a_sink_stranded_by_an_unconfirmed_swap_is_closed_at_shutdown` and
      `test_an_expired_shutdown_leaves_a_stranded_sink_for_the_next_call` unchanged, plus an
      assertion that a stranded sink whose `close()` outlasts the grace does not hold `shutdown()`
      past `DEFAULT_CLOSER_GRACE`. Invariant 3.
- [ ] A `KeyboardInterrupt` delivered during the fan-out close reaches the caller with every
      started close joined first, on both paths. SPEC-046 measured the unjoined shape abandoning
      four closes mid-write and put the join in a `finally`; no test in `tests/` pins it today, so
      this is a new test, not an existing one re-run. Invariant 1.
- [ ] `tests/test_sink_release_roster.py`'s `_EXPECTED_CLOSERS`, `_EXPECTED_REQUESTERS` and
      `_LATCH_DISPOSITIONS` are re-keyed, and `test_a_ninth_direct_close_is_caught` still reddens.
- [ ] `tests/test_invariants_model.py` green over every seed. Invariant 6.

### FR-004: Three questions, not four, and the roster re-derived rather than lowered

#### Description:

With one record, "who owns this sink's close" has one answer and is no longer a question a call
site can get wrong. What remains is **existence** (`worker_exists`), **liveness**
(`live_worker`, now `_worker` while its epoch is current), and the **moment**, which is one category with
**two predicates**, because an abandoned drain answers them oppositely and that has to be said
at the category rather than rediscovered at a site. `in_flight(sink)` — the worker is
*draining* into the sink (`worker.draining`, unchanged) or a release is registered in
`_closing_now` — is asked by the signal refresh, where an abandoned drain must count as over so
the sink gets a fresh event (SPEC-033 FR-004). `held(sink)` — the worker's thread is *alive* and
the sink is one it may be inside, or a release is registered — is asked by the closer's take,
where an abandoned drain must count as still inside so the sink is left open (SPEC-027 FR-004).
Both were measured on the expired-shutdown state: SPEC-033 FR-002 closing under a live writer,
SPEC-035 FR-001 the fresh event never arriving. `worker_owns` and `worker_owns_now` are deleted. This supersedes the four-question set of SPEC-035 FR-002 and
SPEC-040 FR-002 and the "ownership, not liveness" slogan, and says so where those are recorded:
arch §9.2, `docs/decisions/`'s guard entry, and the roster test's own docstring.

The roster (`tests/test_worker_predicate_roster.py`) loses its 10 ownership rows and 3 ownership
∧ moment rows and gains the `in_flight` site. Its per-module floor is **re-derived from the
merged tree and stated with the count it was derived from**, never lowered to whatever passes:
`_SITE_FLOOR` at `98c7e78` reads `{"log_foundry._lifecycle": 46, "log_foundry.decorator": 2}`,
and a floor left at 46 fails on the first phase while a floor edited to fit is the shrink the
lint exists to catch. The three limitations the roster discloses about itself are unchanged.

#### Acceptance Criteria:

- [ ] `grep -n 'worker_owns' src/ tests/` returns nothing, and `_lifecycle._state` has exactly
      four question methods — `worker_exists`, `live_worker`, `in_flight`, `held` — over three
      categories, each with no lock (the class docstring's rule, unchanged).
- [ ] The roster passes with every remaining row filed under one of three categories, and its
      floor is re-derived: the delivery doc states the new counts per module and the commit they
      were measured at.
- [ ] Deleting any one question's use at any one site fails the roster with that site named.
      Proven by planting three mutants, one per category.
- [ ] arch §9.2's table has three rows; the entry *A worker guard asks one of four questions* in
      `docs/decisions/pipeline.md` carries a superseded marker pointing here, and its heading,
      Contents row and fence label move together (`docs-lint.sh` fails otherwise).
- [ ] `tests/test_lifecycle_races.py::test_no_close_or_drain_is_performed_under_the_lifecycle_lock`
      passes against the merged closer. Invariant 3.

### FR-005: `health()` and `flush()` read one record

#### Description:

`_worker_health` assembles the snapshot once: the worker's counters where a worker exists and
zeros where not, and — on **both** branches — `retired` from the owner, `sink` from
`read_losses` over the **configured** sink — the field is SPEC-026 FR-003's, and this is the same
authority FR-005 uses for `inherited_sink` — `closing_sinks` from the registry, `inherited_sink`
from the config (arch §12's prescribed answer, taken because the record entry it used to read no
longer exists), and the two loss counters from `decorator`. The worker path reads `worker.sink`
today, and the two differ in three states, not one: inside a swap, briefly; **permanently** after
a declined swap (SPEC-035 FR-003: the worker retired mid-swap and keeps A while B is delivered
to); and permanently after any `configure(sink=…)` on a retired worker (SPEC-033 FR-002). In the
last two `health().sink` today reports A's losses while every event goes to B; after this spec it
reports B's. That is an observable change on the worker path, taken because arch §12 already
names the config the authority for "installed" and B is the sink being delivered to. `_worker_health`'s docstring claim that
"`worker.py` imports nothing from this module" is false at `98c7e78` (`worker.py:11`) and is
struck in the same edit. `Worker.health()` returns its own
counters only and stops reaching into `_lifecycle`. `_flush_live_sink` drains the sinks the record
names, with no branch on which path armed them.

This closes probe 1. `Health.sink`'s docstring drops "when there is no worker" and keeps "when the
sink reports nothing"; `README.md`'s three sentences about `health().sink` need no change, since
they describe what the field means and not which path fills it.

#### Acceptance Criteria:

- [ ] Probe 1 re-run: outside a span, `health().sink == SinkLosses(dropped=0, failed=1)` against
      the `MultiSink` with one raising child, and `orphan_lost == 0`. Invariant 2.
- [ ] `_worker_health` has one `Health(...)` construction site, and
      `grep -n '_lifecycle\.' src/log_foundry/worker.py` shows no call from `Worker.health`.
- [ ] `health().inherited_sink` in a forked child that inherited its configured sink is `True`
      on both paths, and `False` after the child `configure()`s a sink of its own — arch §12's
      open item closed and struck in place. Invariant 12.
- [ ] `flush()` in an orphan-only process drains every sink the record names, pinned by
      `tests/test_owed_closes.py::test_flush_reaches_every_sink_the_orphan_path_still_owes`
      unchanged. Invariant 2.
- [ ] `tests/test_public_surface.py` and `tests/typed_consumer/` unchanged and green: no public
      field or type moves. Invariant 10.

### FR-006: The forked child repairs the merged state, in SPEC-039's order

#### Description:

The child's order of work is unchanged — primitives, then buffers, then handlers, with
`_mark_inherited` first — and the two residue handlers become one: `_clear_after_fork` empties
`_closing_now` (a registration whose `finally` no surviving thread will run) and nothing else,
because the count and the idle event it also reset are gone. `Worker._reinit_after_fork` keeps
its queue replacement, its counter zeroing and its drain-event settling, and stops clearing
fields it no longer has; its resume decision reads the owner's latch. The owner's `_FORK_SKIP`
names the record. The stop event the child's worker holds is the owner's, re-initialised by the
walk once rather than twice.

#### Acceptance Criteria:

- [ ] Every test in `tests/test_fork_lifecycle.py` passes, its four both-path parametrizations
      included, and
      `tests/test_lifecycle_races.py::test_the_fork_opt_out_is_declared_where_the_walk_reads_it`
      is re-keyed to the record and still reddens when the declaration is removed. Invariant 12.
- [ ] A child forked while the parent was inside an inline close has an empty `_closing_now`,
      and an orphan emit in that child hands its sink an unset event — SPEC-044 FR-003's fork test,
      unchanged. Invariant 12.
- [ ] A retired parent forks a retired child, read from the one latch: `health().retired` is
      `True` in the child on both paths, with no worker built to answer it. Invariant 2.
- [ ] `tests/test_fork_lifecycle.py::test_no_module_builds_a_primitive_the_walk_cannot_repair`
      passes over both modules with the one stop event assigned as an attribute of the owner, so
      the walk writes it back in the child (SPEC-039 FR-002's shape rule).

---

## Data Model

```
// src/log_foundry/_lifecycle.py
_Lifecycle {
  _worker: Worker | None
  _lock: threading.Lock
  _atexit_registered: bool
  retirements: int                       // FR-001: shutdown() entries, written under _lock; health().retired is > 0
  _stop: threading.Event                 // FR-001: the one signal; replaced when set, never cleared
  _owed: dict[int, Sink]                 // FR-002: every sink owed a close, by identity
  _shutdown_running: int                 // unchanged (SPEC-044 FR-001)
  _late_worker: Worker | None            // unchanged
  _FORK_SKIP = ("_owed",)                // FR-002, FR-006

  worker_exists() -> Worker | None       // existence
  live_worker() -> Worker | None         // liveness: _worker while its _epoch is current
  in_flight(sink) -> bool                // moment, for the refresh: worker.draining into sink, or a release registered
  held(sink) -> bool                     // moment, for the close: worker.may_be_inside(sink), or a release registered
  refresh_stop_signal() -> Event         // unchanged
}
_closing_now: dict[int, threading.Event]   // FR-003: registered with the discharge, under _lock; a bystander waits
closer_grace(deadline) -> float            // FR-003: the one arithmetic

// src/log_foundry/worker.py
Worker {
  sink: Sink
  _queue, _thread, _lock, counters, _taken_markers, _drain_finished, _drain_settled   // unchanged
  _stop: threading.Event                 // handed in; held for the thread's life
  _epoch: int                            // FR-001: _state.retirements at build; retired for this worker means it moved
  _held: list[Sink]                      // FR-003: sinks this thread may be inside (live + unfenced)

  submit(events)                         // reads _lifecycle._state.retirements > self._epoch
  flush(timeout) -> FlushResult          // unchanged verdicts
  stop(timeout) -> None                  // what remains of shutdown(): sentinel, join, expiry, waiters
  retarget(new, deadline) -> SwapOutcome // declined | fenced | unfenced
  may_be_inside(sink) -> bool            // thread alive and sink in _held
  draining -> bool                       // unchanged
  health() -> counters only
}
```

## API / Interface Contract

No public symbol changes. `log_foundry.shutdown`, `flush`, `health`, `configure`, `Health`,
`FlushResult` and `worker.DEFAULT_*` keep their names, types and documented meanings; SPEC-034's
frozen surface and SPEC-051's typed consumer are the gate. Internal names above are the plan's to
finalise; the FRs bind their *existence and count*, not their spelling.

## File & Folder Structure

```
src/log_foundry/
├── _lifecycle.py     the owner: latch, signal, record, closer, swap, shutdown, health, flush
└── worker.py         the drain engine: queue, thread, markers, counters, stop, retarget
tests/
├── test_worker_predicate_roster.py   three categories, floor re-derived
├── test_lifecycle_races.py           record lints re-keyed
├── test_sink_release_roster.py       closer tables re-keyed
└── test_invariants_model.py          unchanged; green after every phase
```

## Implementation Phases

Each phase leaves `main` green — the six gates and `tests/test_invariants_model.py` over every
seed — and re-derives the roster floor for the sites it moved. The plan decides the PR grouping.

### Phase 1: The latch and the signal (FR-001)

- Move the retirement latch and the stop event onto the owner; hand the worker its event; point
  `submit`, `flush` and `swap_sink` at the owner's latch; delete the worker's copies.
- Re-run probe 2 as a control: the post-shutdown behaviour of both paths is unchanged.
- Measure `submit` before and after.

### Phase 2: The record and the closer (FR-002, FR-003, FR-006, and FR-004's code)

- Merge the four records into `_owed` with the arming and discharge rules; add `Worker._held`
  and `may_be_inside`; retire the closed-sink latch and `sink_released`, updating the SPEC-044
  tests to the write-epoch assertion.
- Write the one closer with per-sink in-flight events and the one grace; reduce
  `Worker.shutdown` to `stop`; make `_swap_sink` one function over `retarget`.
- Declare the record in `_FORK_SKIP`; collapse the two residue handlers; run the fork suite.
- FR-004's code half: delete `worker_owns` and `worker_owns_now`, add `in_flight` and `held`, re-derive
  the roster floor from the merged tree and state the count it came from.
- Re-key the three derived lints; plant the mutants each FR names.

### Phase 3: The readers (FR-005)

- Assemble `Health` once; answer `inherited_sink` from the config and strike arch §12's item;
  make `_flush_live_sink` read the record; re-run probe 1.

### Phase 4: The record (FR-004's documentation half, and the ritual)

- arch §9.2 to three questions; a superseded marker on the guard entry in
  `docs/decisions/pipeline.md`, with heading, Contents row and fence label moved together;
  `docs/invariants.md` §6's
  first clause narrowed; §12's two open items updated (one closed, one narrowed).
- Delivery doc with the re-derived floor counts, the `submit` delta, the widened model-test run,
  and both probes' final output.

## Risks

- **The SPEC-028 objection, which is the one most likely to bite.** Interpreter exit kills a
  daemon wherever it has reached, and for `SQLiteSink` that can be inside `commit()`. Today the
  live sink's close is inline on both paths and every other exit close is either joined (orphan
  record) or already-budgeted-and-detached (stranded swap). A merged closer that picks the wrong
  sink for the calling thread — after a swap, when the config names B and the worker still holds
  A — would put the live close on a fan-out thread, joined but exposed to a bystander returning
  after the grace. FR-003's inline rule is ordered *worker's live sink, then configured, then
  last armed* for that reason, and its second criterion asserts the thread identity inside
  `close()` on both paths. The residual SPEC-050 FR-002 accepted — `atexit` as a bystander gives
  up after the grace and the interpreter exits through a background thread's close — is
  unchanged, and is the reason a bystander waits on *every* in-flight event rather than one.
- **A refactor silently shrinks a derived guard.** Three lints are keyed on names this spec
  deletes. Each phase re-keys them and plants a mutant; a green suite after a rename is not
  evidence (SPEC-040's own history, recorded in `docs/decisions/`).
- **Two closes that used to be one.** FR-002 changes two observable counts from 1 to 2: a late
  worker built during the first pass's close (SPEC-044 FR-001's third race test), and a sink
  written to after a completed close. Both are SPEC-045 FR-002's rule applied to the sites that
  still contradicted it; each criterion pins the second close *after* the delivery it
  discharges. `sinks/base.py` already requires the second to be safe.
- **The record in the fork walk.** A new global holding sinks needs `_FORK_SKIP` or the walk
  re-hooks superseded sinks (SPEC-044 FR-005's hazard, twice avoided in SPEC-050 by the same
  declaration). FR-006 names it; the fork suite's opt-out lint is the gate.
- **Test churn.** Measured at `98c7e78` with one `grep -o` over `tests/*.py` for the names FR-001
  to FR-003 delete — the four `_orphan_*` fields, `_orphan_closing`, `_orphan_idle`,
  `_shutdown_done`, `_sink_closed`, `_unclosed_swaps`, `._closing`, `worker_owns`,
  `take_orphan_owed`, `discharge_owed`, the two grace helpers, the two closers and
  `sink_released` — 200 sites in 14 files; `conftest.py` names nine more. Diff test *names*
  with `--collect-only` after each phase, never pass counts: a scripted rename can delete a test
  and leave the suite green.
- **Two lock orders meet.** `_close_if_owed` takes `_state._lock` then `worker._lock`;
  `_swap_sink`'s worker branch does the same. The merged closer keeps that order and takes
  `_closing_now_lock` last, as its docstring already requires; the under-lock lint in
  `tests/test_lifecycle_races.py` is the gate.
