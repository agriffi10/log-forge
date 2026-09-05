# Spec: One Lifecycle Owner for Both Delivery Paths

**ID:** SPEC-054
**Status:** Draft
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

`_Lifecycle` holds `retired`, latched under `_lock` on entry to `_shutdown_worker`, and `_stop`,
the one `threading.Event` every sink and the worker's drain loop wait on. `Worker` loses
`_shutdown_done`, `retired`, `_stop` and `_offer_stop_signal`: it is handed its event at
construction, holds that object for the life of its thread, and reads `_lifecycle._state.retired`
where it read its own flag — `submit`'s two reads, `flush`'s guard, `swap_sink`'s two re-checks.

The refresh rule is unchanged (SPEC-033 FR-004): an event that is already set is replaced with a
fresh one before it is offered, and a worker built after a `shutdown()` returned is handed a
fresh one for the same reason a sink adopted then is — it delivers, and a set event would collapse
every backoff to zero. A worker that already holds the set event keeps it: a retired worker is
never restarted (SPEC-019), so the replacement can only ever reach a later deliverer. The skip
rule is unchanged too (SPEC-035 FR-001, SPEC-044 FR-003): the sink's signal is not replaced while
a delivery or a close is in flight against it, which is `in_flight(sink)` in FR-004 — the same
predicate `worker_owns_now(sink) or _closing(sink)` computes today, asked once.

Measured cost of the read that moves: a `self` attribute read is 4.1 ns and a read through the
module global is 5.9 ns on this machine, so `submit` costs under 2 ns more on each of two reads.
SPEC-040 held the traced call to a ~6.6 ns addition; this is inside that and must be re-measured
on the build, not assumed.

#### Acceptance Criteria:

- [ ] `grep -n '_shutdown_done\|_orphan_retired\|_orphan_stop' src/log_foundry/` returns nothing,
      and `grep -c 'threading.Event()' src/log_foundry/worker.py` is exactly 3 — the two drain
      events and the flush marker's — so no stop event is built there. `Worker._stop` survives as
      the reference to the owner's event that the constructor was handed; `_lifecycle._state`
      holds the only latch and builds the only stop event, and `_worker_health` no longer computes
      `retired` with an `or`.
- [ ] After `shutdown()` on a process that only ever logged outside a span, and after one that
      only ever logged inside one, `health().retired` is `True` and comes from the same attribute.
      Invariant 2.
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
  "after closing A then B, A is forgettable and re-armable again" — and `decisions.md` describes
  the same slot as naming "a sink a swap left *open*", which is a different claim. Under one
  record an emit that lands re-arms, always; what the latch protected against — a close performed
  against a sink the drain thread may still be inside — is answered at close time by the moment
  question (FR-003), not by refusing the arming.
- **`Worker(sink_released=…)`** (SPEC-044 FR-001) made a worker built during a `shutdown()`
  inherit a discharged close for a sink that shutdown's orphan branch had just closed. The
  ordering that needed it no longer exists: the merged `_shutdown_worker` performs its one close
  **after every drain, the late worker's included** (FR-003), so a late worker's sink is armed by
  its build, drained, and closed once, in either registration order.

The one observable this changes is a sink written to after a **completed** close: an explicit
`shutdown()`, then an `info()` outside a span to a sink that still accepts, then `atexit`. Today
the latch refuses the re-arm for that sink and its post-close batch is never flushed; after this
spec it is re-armed and closed again, which is SPEC-045 FR-002's rule and one close per
write-epoch rather than a double. **A sink re-armed while its close is still running is not
closed concurrently.** The closer takes only sinks with no delivery or close in flight against
them — `in_flight(sink)` (FR-004), the same predicate the signal refresh asks — so an emit that
lands during a close leaves the sink in the record; the closer that finds it in flight waits out
the grace and then takes, once more, whatever that wait released. A sink re-armed during *that*
close stays for a later `shutdown()`, which is the same tail SPEC-050 FR-002's bystander already
accepts at `atexit`.

`sinks/base.py` still asks an implementation to make `close()` idempotent and the library still
does not rely on it (SPEC-044): every close performed here is against a sink that received
something since the last one.

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
- [ ] SPEC-044 FR-001's three race tests in `tests/test_lifecycle_races.py` pass with their
      "closed once" assertions **unchanged**, in both registration orders, because the one close
      now runs after the late worker's drain; and a fourth assertion in the orphan-closes-first
      test records that the close ran after that worker's events landed. Invariant 5.
- [ ] Two `shutdown()` calls racing while an `info()` outside a span lands on the sink the first
      is closing: `close()` is never entered on two threads at once, asserted by a sink that
      counts concurrent entries, and the sink is closed again after the emit landed — by the
      second caller after its wait, or by a later `shutdown()`. Invariant 5.
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
— `Worker.stop(timeout)`, which is what remains of `Worker.shutdown`: queue the sentinel, join,
record `ShutdownTimeout` and release waiters on expiry, wait on `_drain_settled` on the idempotent
path. The closer, under `_lock`:

1. takes every owed sink with nothing in flight against it — `in_flight(sink)` (FR-004): a
   release registered in `_closing_now`, or `worker.may_be_inside(sink)`, the worker's answer,
   true while its thread is alive for its live sink and for a swapped-out sink whose fence was
   not confirmed (SPEC-050 FR-004's `_unclosed_swaps`, now a list the worker keeps of sinks its
   own thread may hold, pruned on a confirmed fence or a re-adoption). The worker asked is
   `_state._worker`; `_late_worker` is the same object when it is set, and is never the only
   handle consulted;
2. registers each in `_closing_now`, which becomes `dict[int, threading.Event]` so a bystander
   has something to wait on — the per-sink registration `release()` already brackets (SPEC-044
   FR-003), carrying an event instead of nothing;
3. closes them the way SPEC-046 settled: one on the calling thread, the rest on threads that are
   joined in a `finally`. The inline one is the worker's live sink where a worker exists, else the
   configured sink where it is owed, else the most recently armed — so `shutdown()`'s own close
   stays inline on both paths (SPEC-030), which is the risk below.
4. A sink the worker held at the moment its thread ended — the unconfirmed-swap case — is released
   **detached** and granted only the grace, keeping SPEC-050 FR-004's decision: it already had the
   swap's whole budget, so it is far more likely stuck than slow, and joining it would let one
   stuck swapped-out sink hold the exit where today it costs the grace.

A caller that found a sink in flight waits on every event in `_closing_now` for
`closer_grace(deadline)`, the one arithmetic that replaces `_closer_grace`, `_bystander_grace`
and `join_closers`'s inline copy: `min(DEFAULT_CLOSER_GRACE, remaining)`, the cap for `None` —
and then takes once more whatever the wait released and is still owed (FR-002). The closer runs
**once per `shutdown()` call, after every drain**: `_shutdown_worker` stops the worker it found,
then the late worker SPEC-044 FR-001 registered, then closes — where today `_close_orphan_sink`
runs before the late worker's own shutdown and that shutdown closes its sink itself. The process-wide count and idle
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
worker the drain step is skipped and nothing else differs.

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
(`live_worker`, now `_worker` unless `retired`), and the **moment** — `in_flight(sink)`, true
while the worker is draining into the sink (`worker.draining`, unchanged) or a release of it is
registered in `_closing_now` — asked at the two sites that act on a sink, the signal refresh
and the closer's take. `worker_owns` and
`worker_owns_now` are deleted. This supersedes the four-question set of SPEC-035 FR-002 and
SPEC-040 FR-002 and the "ownership, not liveness" slogan, and says so where those are recorded:
arch §9.2, `decisions.md`'s guard entry, and the roster test's own docstring.

The roster (`tests/test_worker_predicate_roster.py`) loses its 10 ownership rows and 3 ownership
∧ moment rows and gains the `in_flight` site. Its per-module floor is **re-derived from the
merged tree and stated with the count it was derived from**, never lowered to whatever passes:
`_SITE_FLOOR` at `98c7e78` reads `{"log_foundry._lifecycle": 46, "log_foundry.decorator": 2}`,
and a floor left at 46 fails on the first phase while a floor edited to fit is the shrink the
lint exists to catch. The three limitations the roster discloses about itself are unchanged.

#### Acceptance Criteria:

- [ ] `grep -n 'worker_owns' src/ tests/` returns nothing, and `_lifecycle._state` has exactly
      three question methods, each with no lock (the class docstring's rule, unchanged).
- [ ] The roster passes with every remaining row filed under one of three categories, and its
      floor is re-derived: the delivery doc states the new counts per module and the commit they
      were measured at.
- [ ] Deleting any one question's use at any one site fails the roster with that site named.
      Proven by planting three mutants, one per category.
- [ ] arch §9.2's table has three rows; `decisions.md`'s entry *A worker guard asks one of four
      questions* carries a superseded marker pointing here and its heading, Contents row and
      digest label move together (`docs-lint.sh` fails otherwise).
- [ ] `tests/test_lifecycle_races.py::test_no_close_or_drain_is_performed_under_the_lifecycle_lock`
      passes against the merged closer. Invariant 3.

### FR-005: `health()` and `flush()` read one record

#### Description:

`_worker_health` assembles the snapshot once: the worker's counters where a worker exists and
zeros where not, and — on **both** branches — `retired` from the owner, `sink` from
`read_losses` over the **configured** sink — SPEC-030's definition of the field, and the same
authority FR-005 uses for `inherited_sink` — `closing_sinks` from the registry, `inherited_sink`
from the config (arch §12's prescribed answer, taken because the record entry it used to read no
longer exists), and the two loss counters from `decorator`. The worker path reads `worker.sink`
today, which differs from the config only inside a swap; the config wins there because arch §12
already names it the authority for "installed". `_worker_health`'s docstring claim that
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
  retired: bool                          // FR-001: the one latch, written under _lock
  _stop: threading.Event                 // FR-001: the one signal; replaced when set, never cleared
  _owed: dict[int, Sink]                 // FR-002: every sink owed a close, by identity
  _shutdown_running: int                 // unchanged (SPEC-044 FR-001)
  _late_worker: Worker | None            // unchanged
  _FORK_SKIP = ("_owed",)                // FR-002, FR-006

  worker_exists() -> Worker | None       // existence
  live_worker() -> Worker | None         // liveness: _worker unless retired
  in_flight(sink) -> bool                // moment: worker.draining into sink, or a release registered
  refresh_stop_signal() -> Event         // unchanged
}
_closing_now: dict[int, threading.Event]   // FR-003: release() registers; a bystander waits
closer_grace(deadline) -> float            // FR-003: the one arithmetic

// src/log_foundry/worker.py
Worker {
  sink: Sink
  _queue, _thread, _lock, counters, _taken_markers, _drain_finished, _drain_settled   // unchanged
  _stop: threading.Event                 // handed in; held for the thread's life
  _held: list[Sink]                      // FR-003: sinks this thread may be inside (live + unfenced)

  submit(events)                         // reads _lifecycle._state.retired
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
- FR-004's code half: delete `worker_owns` and `worker_owns_now`, add `in_flight`, re-derive
  the roster floor from the merged tree and state the count it came from.
- Re-key the three derived lints; plant the mutants each FR names.

### Phase 3: The readers (FR-005)

- Assemble `Health` once; answer `inherited_sink` from the config and strike arch §12's item;
  make `_flush_live_sink` read the record; re-run probe 1.

### Phase 4: The record (FR-004's documentation half, and the ritual)

- arch §9.2 to three questions; superseded markers in `decisions.md` (guard entry) and the
  digest line, with heading, Contents row and label moved together; `docs/invariants.md` §6's
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
  evidence (SPEC-040's own history, recorded in `decisions.md`).
- **The late-worker double close.** FR-002's second retirement changes an observable count from 1
  to 2 for a sink that accepts after `close()`. It is the rule SPEC-045 already settled, applied
  to the one site that still contradicted it; the criterion pins the second close *after* the
  delivery it discharges.
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
