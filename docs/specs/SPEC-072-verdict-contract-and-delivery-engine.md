---
type: Spec
id: SPEC-072
title: "Verdict Contract and Delivery Engine"
description: "Adds the optional per-event Verdict returned by deliver(batch), one delivery engine that resends only retryable events and writes a sink-owned counter object, and the worker, orphan and inline-emit paths that run it, proven against test sinks before any shipped sink adopts it."
spec_status: Draft
depends_on: [SPEC-027, SPEC-058, SPEC-059, SPEC-062, SPEC-070]
arc: stability-redesign
arc_order: 4
---

## Overview

A sink today answers one question, "did the batch land, and is it safe to send again?", in its own
way. `Sink.emit(batch)` returns `None` and raises when nothing was delivered; the worker then resends
the **whole** batch. A partial outcome reaches the worker only as a cumulative `losses()` count,
which feeds `health()` and never the retry decision. So every sink faces the same choice on every
vendor response: raise, and the worker resends events the destination already accepted, or return,
and the sink must count its own loss. Three fixes (#276, #283, #286) answered that one question in
three places, and twelve modules carry their own copy of the retry loop's backoff constant (measured
below).

This spec writes the answer once, as a contract and an engine, and changes no shipped sink. A sink
**may** add `deliver(batch) -> Verdict`, probed by name the way `losses()` and `flush()` are. A
`Verdict` says, per event and by index in the batch the sink was given, whether the event was
accepted, rejected for good, or is worth sending again, and may carry the server's retry-after hint.
One engine runs the rounds: it resends only the events a verdict called retryable, within the same
total of attempts 1.x gives a batch (the sink's `max_retries` times the worker's) and the sink's own
`max_retry_after`, waiting on the stop signal and keeping SPEC-062's
shutdown-deadline prediction for every round. It writes every outcome into a counter object **the
sink owns**, so `losses()`, `.failed` and `read_losses(inner)` stay true and the counts survive a
swap, a fork and a new worker. The worker and the orphan path call `deliver` when the sink offers it
and `emit` otherwise, and `emit` itself gains an inline adapter, so a third-party wrapper that calls
`inner.emit(batch)`, or a subclass that calls `super().emit()`, keeps its 1.x behaviour.

This is the central spec of the `stability-redesign` arc (release 1.2.0, additive: no count is
reclassified, no `emit` contract changes). It builds on SPEC-066 (every sink's `max_retries` is an
exact, floored `int`, so the engine reads it as one) and SPEC-068 (the worker's shutdown-expiry
accounting), both of which land first in 1.1.0 and are not in `depends_on` because they are not yet
in this directory. `Worker._in_flight`, which FR-005 sets around a run, is SPEC-062's, already in
`depends_on`. It consumes SPEC-070's private
exception predicate, `_retryable_error(self, err: Exception) -> bool`. The seam is inert until
SPEC-073: at this spec's build **no shipped sink defines `deliver`**, so every shipped sink runs
exactly the paths it runs today and the engine is proven against test sinks.

Figures below were re-measured at `fda134a` (the `main` this spec was drafted against). Each names
its instrument:

- Retry loops with a private backoff: twelve modules assign `_BACKOFF_BASE = 0.1` (`grep -rln
  '^_BACKOFF_BASE' src/log_foundry`: `_socket`, `clickhouse`, `eventhubs`, `firehose`, `http`,
  `kinesis`, `mongodb`, `postgres`, `rabbitmq`, `redis`, `sns`, `sqs`).
- `max_retries`: ten sink modules assign `self.max_retries` (`grep -rl 'self.max_retries *='
  src/log_foundry/sinks`); the worker's own default is 3 (`Worker.__init__`).
- `max_retry_after` exists in `sinks/http.py` alone (`grep -rl max_retry_after src/log_foundry`),
  default `DEFAULT_MAX_RETRY_AFTER = 30.0`.
- `losses()` is defined in 24 files (`grep -rl 'def losses' src/log_foundry`); Kafka, Pub/Sub and
  `MultiSink` are three of them and never get `deliver`.
- Call sites of `.emit(` that catch or delegate: the worker's `_emit`, the orphan call in `api.py`,
  `MultiSink`, and `FilteringSink`, `TransformSink` and `LogstashSink` delegating (`grep -rn
  '\.emit(' src/log_foundry`, comments and docstrings excluded).
- `log_foundry.__all__` holds 30 names (`len(log_foundry.__all__)`).
- `NullSink.dropped` is a `dropped` counter that no `losses()` reports (`grep -rn '\.dropped\b'
  src/log_foundry/sinks`), which constrains the counter names chosen in FR-003.
- The fork repair does not enter a sink that is not a package type: `_fork._reinit_primitives`
  skips the namespace of a holder `_is_owned` refuses (`continue` at `_fork.py:487`). Probed in this
  spec's review: a lock-holding stand-in for `SinkCounters` held by a structurally typed sink was
  not replaced in a child, and the same sink subclassing `log_foundry.Sink` was. FR-003 closes it.
- `clamp_server_delay` does not filter a non-number: `clamp_server_delay("3", 30.0)` raises
  `TypeError` and `clamp_server_delay(True, 30.0)` returns `True` (the same probe), so FR-002 checks
  the hint's type first.

## Scope

### In Scope

- `Outcome`, `Verdict` and `SinkCounters`, the optional `deliver` member and its probe
  `read_deliver`, and their public import path (FR-001).
- The engine: rounds that resend only retryable events, the 1.x wait schedule written once, the sink's
  `max_retries` and `max_retry_after`, the stop-signal wait and SPEC-062's per-round prediction
  (FR-002).
- The mapping from outcomes to counters, the run-failed rule, the `losses()` / `.failed` /
  `read_losses` truth through swap, fork and a new worker, and the fork walk's reach to a
  third-party sink's counters (FR-003).
- The two unknown outcomes, an exception escaping `deliver` and an unreadable verdict, and the
  sink's declared idempotence (FR-004).
- The worker path and the orphan path calling `deliver` when offered and `emit` otherwise, and the
  model test driving both forms (FR-005).
- The inline adapter by which an engine sink's `emit` runs the same engine (FR-006).
- Prose that states the contract: the `Sink` docstring, `docs/architecture.md` §8, the README's
  sink-authoring text, `docs/invariants.md` entries 2, 3, 6, 7 and 12, and
  `docs/component-inventory.md`. Invariant 7's *Recorded exceptions* sentence "an `emit` may return
  normally having delivered nothing, because what it did not deliver was counted" is extended to a
  run whose verdict marks an event `UNKNOWN` or cannot be read (FR-004).
- At completion, in `docs/decisions/sink-delivery/`: the decision
  `a-raising-emit-counts-nothing-whoever-catches-the-raise-owns-the-batchs-count` is rewritten in
  place as it applies to `deliver` (FR-003 says how), with a dated History entry; the decisions
  `after-a-status-line-the-status-decides-a-2xx-whose-body-cannot-be-read-is-never-re-sent` and
  `a-client-exception-costs-its-chunk-and-is-provable-non-delivery-for-it` each gain a dated History
  line saying how the engine applies them (FR-004), with their fences unchanged; and one new decision
  records the verdict contract.
- This is the first half of the arc's plan step 4, split from the HTTP family (SPEC-073) along the
  seam `authoring-a-spec.md` names, the point where something inert goes live. Six FRs; the spec is
  not above the ceiling.

### Out of Scope

- **Moving any shipped sink onto the engine.** The HTTP family is SPEC-073; the SDK and driver sinks
  are SPEC-074. The twelve `_BACKOFF_BASE` constants and the sinks' own retry loops stay until their
  sink adopts the engine and are deleted there. So does the question of what unit a shipped sink's
  `failed` counts: `HTTPSink.failed` counts abandoned requests today and the engine counts events,
  and the adopting spec states the mapping for its sinks.
- **Wrappers mapping verdicts** (`TransformSink`, `FilteringSink`, `LogstashSink`): SPEC-075.
  `MultiSink`, `KafkaSink` and `GooglePubSubSink` **never** get `deliver`; they keep `losses()` and
  `MultiSink` keeps calling each child's `emit`.
- **The closed-guard helper** and what a sink's `deliver` does after `close()`: SPEC-076.
- **The retry predicate itself**, `import_extra`, the shared validators and the chunkers: SPEC-070
  and SPEC-071. The engine defines no predicate and adds no public one: a third-party `deliver` sink
  classifies its own exceptions inside `deliver`, returning `RETRYABLE`.
- **Lifecycle, owed closes and the rest of the fork walk** (SPEC-077 to SPEC-079). The engine adds no
  global that holds a sink; FR-003 adds one named reach to the existing walk and nothing else.
- **A deadline on the inline adapter.** An engine sink's `emit` is one attempt to the worker's
  predictor, exactly as a 1.x sink's own loop inside one `emit` is (SPEC-062, Out of Scope).
- **Per-event reasons or messages in a `Verdict`**, a per-event `retry_after`, an async `deliver`,
  and any new `Outcome` member before 2.0.
- **Any change to `emit`'s contract, `SinkLosses`, `Config`, `configure()` or the worker's tunables.**
- **Deleting a test.** No mechanism is removed here, so every existing test stays.

---

## Functional Requirements

### FR-001: The `Verdict` contract, the optional `deliver` member, and its probe

#### Description:

**The types** live in `src/log_foundry/sinks/verdict.py` and are public. `Outcome` is an `Enum` of
what happened to one event:

| Member | Meaning | Counted as | Resent |
|---|---|---|---|
| `ACCEPTED` | the destination took it | nothing | never |
| `RETRYABLE` | it did not land and a resend can succeed | `failed` if rounds run out | yes |
| `REJECTED` | the destination refused it for good | `failed` | never |
| `DROPPED` | the sink refused to send it, because it could never be accepted as built | `dropped` | never |
| `UNKNOWN` | it may have landed (for example a `2xx` whose body cannot be read) | `failed` | only by an idempotent sink (FR-004) |
| `SKIPPED` | not sent, deliberately (a wrapper's filter declined it) | nothing by the engine | never |

`REJECTED` and `DROPPED` are the two forms of "rejected for good" and differ only in which
`SinkLosses` term they land in, so no count is reclassified: `SinkLosses.dropped` is "discarded
before attempting delivery" and `failed` is "attempted, not confirmed" already. `UNKNOWN` exists as
a member because a sink that read an unreadable `2xx` has adjudicated something, and the engine must
not mistake it for `RETRYABLE`.

`SKIPPED` means "not sent, and the engine counts nothing for it": it lands in none of the engine's
counters and is never resent. A sink may count its own skip through SPEC-073's run context, as
SPEC-075's wrappers do for an event their caller code failed beside delivered siblings
(`run_context().count(self, "_dropped", n, when=SETTLED)`, SPEC-058's count at SPEC-058's moment);
that count is the sink's, in its own 1.x counter, and this spec neither makes nor forbids it. It is
**not** `ACCEPTED` for the run-failed rule (FR-003), so a
filter batch of one declined event and rejected rest still fails, as invariant 7 requires; were a
declined event `ACCEPTED`, such a batch could never fail. An all-`SKIPPED` batch settles with
nothing counted by the engine. SPEC-075's wrappers use it; this spec is its one home.

**`Outcome` is closed for 1.x.** It is an input a sink writes, not a result a caller reads, so the
rule that results grow by new values (invariant 10) does not reach it, and a new member would break
a typed sink's exhaustive `match` at type-check time. A test pins its six members.

A `Verdict` addresses events by index in the list `deliver` was given: `outcomes[i]` is the fate of
`batch[i]`. A sink that re-chunks, filters or transforms maps its inner verdicts back to its own
indices before returning. `retry_after` is the server's hint in seconds, advisory only (FR-002).
`outcomes` is typed `tuple[Outcome, ...]`; at run time a `list` is also accepted and stored as a
`tuple`, for an untyped caller, and any other container type is refused.

**`deliver` is optional and probed, never declared on the `Sink` Protocol**, which stays structural
for the reason `losses()` is not declared on it. `read_deliver(sink)` decides whether a sink
**offers** `deliver`, and it reads the sink's **type**, never the instance's dynamic attributes:

- `deliver` is found with `inspect.getattr_static` on `type(sink)` and must be a plain function. A
  proxy whose `__getattr__` delegates to an inner sink therefore offers nothing, and its own `emit`
  runs, so a filtering or scrubbing proxy is never bypassed. No third-party `__getattr__` or property
  runs during the probe. A class attribute `deliver = None` is a declared opt-out: the lookup finds
  `None`, which is not a function, so a subclass of an engine sink can set it to keep delivering
  through its own `emit`.
- `deliver` is offered only when no class in `type(sink).__mro__` before the one that defines
  `deliver` defines `emit`. A subclass that overrides `emit` to scrub or stamp a batch and then calls
  `super().emit()` keeps running its override: the worker calls its `emit`, and the inherited
  adapter (FR-006) runs the engine below it.
- `log_foundry_counters` is read with `inspect.getattr_static` on the sink and must be a
  `SinkCounters` (FR-003). A sink that defines `deliver` with no counters keeps delivering through
  `emit`, so nothing is lost and the engine never counts somewhere dishonest.

`read_deliver` returns the bound `deliver` when the sink offers it and `None` otherwise. Like
`read_losses` it never raises and writes nothing. It is public for the reason `read_losses` is: a
third-party wrapper that offers `deliver` itself asks it whether its inner sink offers one.

**The public path.** `Outcome`, `Verdict`, `SinkCounters` and `read_deliver` are exported from
`log_foundry` beside `SinkLosses` and `read_losses`, and live at `log_foundry.sinks.verdict`, which
joins the documented layout the freeze covers. `__all__` goes from 30 names to 34 and no name leaves
it. `Verdict` is a frozen, keyword-only dataclass that grows by appending fields, as every public
dataclass does. `configure()`, `Config` and every existing signature are unchanged.

#### Acceptance Criteria:

- [ ] `Verdict(outcomes=(Outcome.ACCEPTED, Outcome.RETRYABLE), retry_after=2.0)` constructs, a
      `list` of outcomes is stored as a `tuple`, and `Verdict.uniform(Outcome.REJECTED, 3).outcomes`
      is three `REJECTED`. Constructing positionally raises `TypeError`, and assigning to a field
      raises.
- [ ] An `outcomes` element that is not an `Outcome`, or an `outcomes` that is a `str`, a generator
      or a `set`, raises `TypeError` naming the position or the type, never a value;
      `Verdict.uniform` with a negative count raises `ValueError`.
- [ ] `list(Outcome)` is exactly the six members above, in that order.
- [ ] `read_deliver` returns the bound method for a sink whose class defines `deliver` and whose
      `log_foundry_counters` is a `SinkCounters`, and `None` for each of: no `deliver`, a
      `deliver` that is not a function, a subclass of an engine sink that sets the class attribute
      `deliver = None` (its `emit` runs and the inherited `deliver` is never called), a `deliver`
      set only on the instance, no counters, counters
      of another type, counters behind a `property`, a proxy whose `__getattr__` delegates to an
      engine sink, and a subclass of an engine sink that overrides `emit`. It writes nothing to
      stderr and runs no `__getattr__` (a raising one is never entered).
- [ ] A sink that adds `deliver` still satisfies `isinstance(sink, Sink)`, and a class that
      inherits `Sink` and omits `emit` still cannot be instantiated. `Sink` gains no abstract member.
- [ ] `log_foundry.__all__` is the 30 names it was plus exactly `Outcome`, `Verdict`, `SinkCounters`
      and `read_deliver`; `tests/test_public_surface.py` pins them and the import path
      `from log_foundry.sinks.verdict import Outcome, Verdict, SinkCounters, read_deliver`, and every
      existing pin passes unedited. `configure()` and `Config` gain no parameter or field.
- [ ] `tests/typed_consumer/accepts.py` type-checks under `mypy --strict` with a sink whose
      `deliver` returns a `Verdict` built by keyword from a tuple, and `rejects.py` holds a
      positional `Verdict(...)` that `mypy` refuses.
- [ ] Serves invariants 7 and 10: `deliver` returning a `Verdict` is invariant 7's second form, and
      every public name added only grows the surface.

### FR-002: One engine resends only the retryable events, in bounded, idle rounds

#### Description:

The engine is `run_delivery` in `src/log_foundry/sinks/_engine.py`, private. A worker or the orphan
path calls it with the sink, the batch, the worker's retry count (or `None` on the orphan path), a
stop event and an optional decline
predicate. It calls `read_deliver` once, inside, so a swap cannot change the sink between the probe
and the run, and returns `None` when the sink does not offer `deliver` (the caller then uses `emit`).
Otherwise it runs **rounds** through the private `_run`, which the adapter shares (FR-006):

1. Round one calls `deliver` with the batch as given. Later rounds call it with a new list holding
   only the events still to send, in their original order. Indices in each verdict are mapped back
   to the original batch, and an event that has a final outcome is never offered again.
2. After a round, events marked `RETRYABLE` remain to send (and unknown ones, under FR-004's rule).
   If none remain the run ends. If the budget of item 3 is spent, the run ends with those events
   unresolved.
3. **The budget keeps the 1.x total** (Andrew's decision, 2026-11-04). In 1.x two retry layers
   multiply: a sink retries inside `emit` up to its own `max_retries`, and when `emit` raises the
   worker resends the whole batch up to its `max_retries`, so a batch survives up to
   `(sink + 1) × (worker + 1)` attempts, 16 at the defaults of 3 and 3. A spec-review probe measured
   what a single layer would lose: an `HTTPSink` answered nine `503`s then a `200` is delivered today
   on the tenth request and would be lost at the fourth, and `HTTPSink(max_retries=0)` under the
   default worker gets 4 tries today and would get 1. So, with `s` the sink's `max_retries` when that
   is an exact `int` (floored at zero) and `w` the worker's (floored at zero):
   - **worker path**, the sink declares `s`: `(s + 1) × (w + 1) − 1` further rounds, 15 at the
     defaults;
   - **worker path**, the sink declares none: `w`, as the worker's own loop gives today;
   - **orphan path**, the sink declares `s`: `s`, because 1.x has no worker resend there;
   - **orphan path**, the sink declares none: zero, one attempt, as today.

   This preserves the 1.x outage tolerance, the number of attempts a batch survives. What changes is
   what each attempt carries: **only the retryable subset is resent**, never the events already
   accepted, rejected or dropped, which is the duplicate the whole-batch resend made. The worker does
   not resend a whole batch around an engine run, since the budget already holds its share, and
   SPEC-062's prediction (item 5) still caps every round on the exit drain, so the larger budget
   never extends a `shutdown()` past its deadline.
4. **The wait keeps the 1.x schedule as well as the 1.x count** (the orchestrator's decision,
   following Andrew's). A single exponent across all fifteen worker-path rounds would reach
   `0.1 × 2**14`, about 27 minutes, before the last; in 1.x the sink's backoff restarted at 0.1 s on
   every worker attempt, and the worker applied its own, shorter backoff between whole-batch
   resends. So a run is `w + 1` **cycles** of `s + 1` attempts (one cycle of `s + 1` on the orphan
   path and in the adapter; `w + 1` cycles of one attempt for a sink that declares no `max_retries`),
   and:
   - **within a cycle**, the wait before its attempt `i + 1` is the sink's 1.x backoff,
     `BACKOFF_BASE * 2**i` with `BACKOFF_BASE = 0.1`, restarting at `i = 0` in every cycle — the
     formula the twelve modules apply today (`wait(_BACKOFF_BASE * (2**attempt), ...)`, for example
     `sinks/clickhouse.py`, and `HTTPSink._sleep_backoff`);
   - **between cycles**, the wait after cycle `c` is the worker's 1.x inter-attempt backoff,
     `min(CYCLE_BACKOFF_BASE * 2**c, CYCLE_BACKOFF_CAP)` with `CYCLE_BACKOFF_BASE = 0.01` and
     `CYCLE_BACKOFF_CAP = 0.5`, which is `Worker._emit`'s `self._stop.wait(min(0.01 * (2**attempt),
     0.5))` at `fda134a`;
   - **a server hint replaces the wait it falls on**, within a cycle as the sinks do today, and
     between cycles too, where the 1.x worker had no hint to read.

   Both schedules are module constants of `_engine.py` and the one function `cycle_backoff(c)`,
   which `Worker._emit`'s own `emit` loop calls in place of its literal, so each formula is written
   once. A hint is usable only when its type is exactly `int` or `float` (so a
   `bool` or a `str` is not); it is then bounded by `clamp_server_delay` under that function's own
   rule, with the sink's `max_retry_after` as the ceiling when the sink declares one whose type is
   exactly `int` or `float`, and the engine's `DEFAULT_MAX_RETRY_AFTER = 30.0` (the number `HTTPSink`
   carries today) only when the sink declares none or declares a non-number. So a ceiling of `inf`
   honours the hint in full, as 1.x `HTTPSink(max_retry_after=float("inf"))` does, and a ceiling
   that is zero, negative or `NaN` falls back to the backoff, as `clamp_server_delay` already
   answers it. `clamp_server_delay` also turns a negative, zero, infinite or `NaN` hint into the
   backoff. The exponent is bounded so that no `max_retries` raises `OverflowError`, and every wait
   goes through `sinks._retry.wait(delay, stop)`, which waits on the stop event when it has one,
   never `time.sleep`, and caps at `MAX_WAIT`.
5. **SPEC-062's prediction runs before every further round**, after its wait has returned: the engine
   asks the caller's decline predicate with the longest round of this run, and a `True` ends the run
   with the remaining events unresolved and `declined` set. The first round is never declined, no
   predicate declines nothing, and "longest" is per run. The worker's predicate is SPEC-062's rule,
   `now + longest + _JOIN_MARGIN > deadline` against the recorded `_exit_deadline`, moved into one
   worker method, `_retry_declined(longest)`, that the existing `emit` loop and the engine both call,
   so the comparison is written once.
6. **No lock is held across `deliver`.** The engine holds none while the sink runs, so a sink's
   transport lock is taken by the sink alone, and the counters' lock is a leaf taken only at the end
   of the run (FR-003), preserving the order the `Sink` docstring states, transport then counter.

#### Acceptance Criteria:

- [ ] A scripted sink returning `[ACCEPTED, RETRYABLE, REJECTED, RETRYABLE]` then, for the two events
      it is shown again, `[RETRYABLE, ACCEPTED]`, sees round two called with exactly the original
      events 1 and 3 (by identity, in order) and round three with exactly event 1; no event
      `ACCEPTED` or `REJECTED` is offered again, and the result resolves event 3 as accepted.
- [ ] Budget, with the worker's `max_retries=3` and an always-`RETRYABLE` sink: on the worker path a
      sink with `max_retries=0` is called exactly 4 times, `max_retries=3` exactly 16 times, and one
      with no `max_retries` 4 times; on the orphan path `max_retries=2` gives 3 calls and none gives
      1. A sink with `max_retries=3` answering `RETRYABLE` nine times and then `ACCEPTED` is delivered
      on the tenth call on the worker path, and every round after the first is handed only the
      events still `RETRYABLE`.
- [ ] Schedule at the defaults (`s = 3`, `w = 3`), worker path, always `RETRYABLE`, no hint: a
      recording stop event sees exactly the fifteen waits `0.1, 0.2, 0.4, 0.01, 0.1, 0.2, 0.4,
      0.02, 0.1, 0.2, 0.4, 0.04, 0.1, 0.2, 0.4`; none exceeds `0.4`, the largest 1.x wait at these
      defaults, and their sum is `2.87` s, equal to the 1.x worst case (four `emit`s waiting `0.7` s
      each and three worker backoffs of `0.07` s together). On the orphan path with `s = 3` the
      waits are `0.1, 0.2, 0.4`. With `w = 20` no between-cycle wait exceeds `0.5`.
- [ ] A verdict hint of `2.5` against `max_retry_after=1.0` waits `1.0`; with no
      `max_retry_after` (or `max_retry_after="30"`) a hint of `100` waits `30.0`, and with
      `max_retry_after=float("inf")` it waits `100`; `max_retry_after` of `0`, `-1` and `nan` waits
      the backoff; hints of `0`, `-1`, `inf`, `nan`, `"3"` and
      `True` wait the backoff and none raises. A hint falling between two cycles replaces that
      cycle wait. `BACKOFF_BASE`, `CYCLE_BACKOFF_BASE` and `CYCLE_BACKOFF_CAP` are each assigned
      exactly once in `_engine.py`, and `worker.py` holds no backoff literal (an AST scan finds no
      `0.01` or `0.5` in a call to `wait` there).
- [ ] A stop event set 0.1 s into a 10 s hinted wait returns the engine within 0.5 s of wall time
      and under 0.05 s of CPU, measured through `tests/test_worker_exit_budget.py`'s helper.
- [ ] `max_retries=2000` against a fast always-`RETRYABLE` sink with the stop event set returns after
      2001 calls without `OverflowError`.
- [ ] Prediction, through `deliver`: an always-`RETRYABLE` sink whose call takes 0.5 s, run under the
      worker's predicate for `shutdown(timeout=1.2)`, is called exactly 2 times and the run reports
      `declined`; `timeout=0.9` gives 1 call and `timeout=1.8` gives 3. A deadline of `None`
      declines nothing, and a batch whose first round is already past the deadline still makes it.
- [ ] Inside a `deliver` call, `counters._counter_lock.acquire(blocking=False)` succeeds and is
      released: the engine holds no lock while the sink runs.
- [ ] An AST scan of `worker.py` and `sinks/_engine.py` finds exactly one expression that adds
      `_JOIN_MARGIN` and compares the sum, and it is inside `Worker._retry_declined`.
- [ ] Serves invariants 3 and 2: every wait is bounded by `MAX_WAIT`, idle and cut by the stop event,
      a shutdown shortens a wait and never skips a round the prediction allows, and no event is
      offered after it has a final outcome.

### FR-003: Outcomes land in the sink's own counters, and the run-failed rule

#### Description:

**`SinkCounters`** (public, `sinks/verdict.py`) is the counter object a sink owns and exposes as
`log_foundry_counters`. It holds `dropped_events` and `failed_events`, both starting at 0, and a
`_counter_lock`, and `losses()` returns a `SinkLosses` read under that lock. The names are chosen to
be unique in the tree: `tests/test_deferral.py` derives "a counter some `losses()` reports as
`dropped`" by attribute name over `sinks/`, and a counter named `dropped` would meet
`NullSink.dropped`, which no `losses()` reports.

The counter lives on the sink, not the worker, so it is the same object for every worker, every
swap back to that sink and every process the sink is forked into.

**The fork walk reaches it on every sink** (invariant 12). `_fork._reinit_primitives` enters only
package-owned holders, and the sinks this spec ships the engine for are third-party ones. So at a
holder `_is_owned` refuses, the walk now reads the one attribute `log_foundry_counters` with
`inspect.getattr_static` (no third-party code runs in the at-fork handler) and, when it is a
`SinkCounters`, walks it, which replaces its lock. Nothing else of the third-party namespace is
entered, so the walk's boundary (SPEC-039 FR-005) is otherwise unchanged.

**The engine counts once, at the end of a run, through one `Deferral`** (`sinks/_deferral.py`) built on
the sink's counters, so the commit reads the lock at commit time as every deferring site does:

| Final outcome of an event | Engine's counter, settled run | Engine's counter, failed run |
|---|---|---|
| `ACCEPTED` | none | (cannot occur) |
| `REJECTED` | `failed_events` | `failed_events` |
| `RETRYABLE` unsent after the last round, or declined | `failed_events` | `failed_events` |
| in a round whose `deliver` raised, not resent (FR-004) | `failed_events` | `failed_events` |
| `UNKNOWN`, or in a round whose verdict was unreadable (FR-004) | `failed_events` | (cannot occur) |
| `DROPPED` | `dropped_events` | **nothing**: the catcher owns it |
| `SKIPPED` | nothing (a sink may count its own skip, FR-001) | nothing |

A run **fails** (`Delivery.failed`) when no event was `ACCEPTED`, at least one ended `REJECTED`,
unresolved or in a raising round, and none ended `UNKNOWN` or in an unreadable round. Otherwise it
**settles**. `SKIPPED` does not count as accepted, so `[SKIPPED, REJECTED]` fails and
`[SKIPPED] * n` settles with nothing counted by the engine. So `[REJECTED, DROPPED]` fails with `failed_events ==
1` and the `DROPPED` event in no engine term.

**This is the decision *A raising `emit` counts nothing; whoever catches the raise owns the batch's
count*, rewritten for `deliver`:** a `deliver` counts nothing itself; it reports, and the engine is
the one writer of `SinkCounters`. A failed run commits `failed_events`, the upper bound, and no
`dropped_events`, and whoever receives the failure owns the batch: the worker's abandon or declined
line and `failed_batches`, the orphan path's `orphan_lost`, or the adapter's raise and its catcher.
That is why a worker retry of an adapter's raise cannot count a drop twice. A shipped sink's
adapter may instead commit through the run context SPEC-073 adds to `_engine.py`, which keeps the
same rule for a sink whose counters are its own 1.x attributes. The engine is a fourth
catcher beside the worker, the orphan path and `MultiSink`: it owns the count of a `deliver` that
raised.

**What is exact and what is an upper bound** is invariant 2's own split, unchanged. `dropped_events`
is exact: each event in it once, and in no other term. `failed_events` is an upper bound, like every
sink's `failed`: an event of a failed run is in `failed_events` and also in its catcher's term (one
in `orphan_lost` for an orphan `REJECTED`, the batch in the worker's abandon line, and a third time
in `MultiSink`'s absorbed count when an engine child's adapter raised under it), which is invariant
2's recorded exception for `failed`. What the engine does promise is narrower: it adds each event to
`failed_events` at most once per run, never once per round.

A batch of only `DROPPED` events settles, the recorded exception to invariant 7 (nothing in it is
helped by a retry), and a partial outcome settles and leaves `flush()` true.

**The losses a sink counts but never retries are re-derived here**, one per invariant 2's list as
SPEC-058, SPEC-061 (1.1.0), SPEC-069 and SPEC-070 leave it:

| Loss invariant 2 lists | Outcome the sink returns |
|---|---|
| an unadjudicable batch response | `UNKNOWN` |
| an SQS sender fault | `REJECTED` |
| an SNS sender fault (SPEC-070) | `REJECTED` |
| an oversized event | `DROPPED` |
| an SQS id callable's per-event failure beside delivered siblings | `DROPPED` |
| a wrapper's per-event failure beside delivered siblings | `SKIPPED`, counted in the wrapper's own `_dropped` (SPEC-075) |
| an event a sink cannot encode, which SPEC-069 widens from an unrenderable HTTP event | `DROPPED` |
| a NATS core batch whose confirmation went unanswered (SPEC-061) | `UNKNOWN` |

**The engine announces** a settled run's loss in one `_diag.lost` line naming counts by term and the
number of rounds, never a value (invariant 11), unless the sink registered an announcement of its
own for the run through the run context SPEC-073 adds: such a run is announced by the sink's lines
alone, and the engine writes no settled line, so a loss is never announced twice. A failed run's
loss is announced by its catcher: the
worker's abandon or declined line, the orphan path's line, or the adapter's raise. A raising round is
also announced by `_diag.absorbed`, which names the exception's type.

The three `losses()` sinks that never get `deliver` (Kafka, Pub/Sub, `MultiSink`) keep their own
counters and `emit` path; invariant 2 holds for them exactly as it does today.

#### Acceptance Criteria:

- [ ] A settled run of `[ACCEPTED, REJECTED, DROPPED, UNKNOWN]` leaves `dropped_events == 1`,
      `failed_events == 2`, and `Delivery.failed` false. `[DROPPED] * 3` leaves `dropped_events == 3`
      and `failed_events == 0` and settles. `[REJECTED] * 2` fails with `failed_events == 2`;
      `[REJECTED, DROPPED]` fails with `failed_events == 1` and `dropped_events == 0`;
      `[REJECTED, UNKNOWN]` settles with `failed_events == 2`. `[SKIPPED, REJECTED]` fails with
      `failed_events == 1`; `[SKIPPED] * 3` settles with both `SinkCounters` fields at 0, no engine stderr line, and
      no `SKIPPED` event offered again.
- [ ] Two events `RETRYABLE` through three rounds are counted `failed_events == 2`, once each and not
      once per round; a declined run counts the same events the same way.
- [ ] A `BaseException` raised by `deliver` propagates by identity and commits nothing.
- [ ] A test sink whose `losses()` returns `log_foundry_counters.losses()` and whose `failed` is a
      property over `failed_events` reports the same numbers through `read_losses(sink)`, through
      `.failed`, and through `read_losses(wrapper)` for a third-party-style wrapper whose `losses()`
      is `read_losses(inner)`.
- [ ] Counts survive: the same sink configured, shut down and configured again keeps adding to the
      same totals; `configure(sink=b)` then `configure(sink=a)` leaves `a`'s totals as they were.
- [ ] Fork: for a structurally typed sink that does **not** subclass any package type, and again for
      one that subclasses `log_foundry.Sink`, a child forked while a parent thread holds the
      counters' lock completes a run within 5 s, its counts add to the inherited totals, and its
      `SinkCounters._counter_lock` is a different object from the parent's. A third-party sink
      whose `__getattr__` raises and whose other attributes hold an unrepaired lock is not entered.
- [ ] Kafka, Pub/Sub and `MultiSink` define neither `deliver` nor `log_foundry_counters`
      (derived by AST over `sinks/kafka.py`, `pubsub.py` and `multi.py`), and `read_deliver` returns
      `None` for each.
- [ ] `tests/test_deferral.py` derives `dropped_events` as a reported `dropped` counter and its
      `DROPPED_FLOOR` gains it; `DEFERRING_SITES` accepts a module-level function and lists
      `sinks/_engine.py`'s `_run`; the store check refuses a store to a derived counter through any
      attribute chain, not only `self.` (a fixture planting
      `sink.log_foundry_counters.dropped_events += 1` in a module function is named, and the
      `Deferral` commit is not); and no existing floor in `tests/test_deferral.py` or
      `tests/test_worker_predicate_roster.py` is lowered.
- [ ] A settled run's stderr line names counts and rounds and contains no value an event or an
      exception carried; `tests/test_diag.py`'s roster covers the new site.
- [ ] Serves invariants 2, 7 and 12: `dropped_events` is exact and never meets a catcher's term,
      `failed_events` is the documented upper bound, a failed run counts no drop, and a forked child
      never hangs on a sink's counters' lock, a third-party sink's included.

### FR-004: The two unknown outcomes, and resending only for an idempotent sink

#### Description:

The engine cannot always tell whether events landed, and it treats the two ways that happens
differently, because they differ in what is known.

**An exception escaping `deliver` fails the round's events.** `deliver` returned nothing, so no
response is in hand: this is the decision *A client exception costs its chunk, and is provable
non-delivery for it* (an exception is not a response), and it keeps invariant 7's observable, so a
sink whose `deliver` raises on every call leaves `flush()` falsy and moves `failed_batches` or
`orphan_lost`. The round's events are counted `failed_events` and count toward the run failing
(FR-003); an earlier round's `ACCEPTED` events still make the run partial, which settles. The
exception is announced by `_diag.absorbed` with `type(exc).__name__`, never its text. The engine
catches `Exception` only, so a `KeyboardInterrupt` or `CancelledError` still reaches the caller
(invariant 1). The decision's retry half — a client exception is retried — lives inside a sink's
`deliver`, which catches its client's exception and returns `RETRYABLE` (SPEC-074 does this for the
SDK sinks); an exception that escapes `deliver` is the sink saying it could not adjudicate.

**A malformed verdict, or an `UNKNOWN` outcome, settles.** `deliver` returned, so I/O may have
happened and the events may have landed. A verdict is malformed when it is not a `Verdict`, its
`outcomes` is not a tuple of exact `Outcome` members, its length is not the number of events passed,
or a field raises when read; `outcomes_of(verdict, n)` in `_engine.py`, factored out of `_run`, is
that test's one definition, and SPEC-075's wrappers share it. This is the decision *After a status line the status decides; a 2xx
whose body cannot be read is never re-sent* made general, and invariant 7's recorded exception for an
unadjudicable `2xx`: the events are counted `failed_events`, the run never fails on their account,
`flush()` stays true, and the loss is in `health().sink` and on stderr.

**Either is resent only by a sink that declares itself idempotent**, that is, a
`log_foundry_idempotent` read with `inspect.getattr_static` that `is True` (a truthy non-`bool` is
not a declaration). For an escaped exception the engine also asks SPEC-070's
`sink._retryable_error`, found statically too and then **bound**, through
`is_retryable(predicate, exc)`, never by a direct call, and resends only when it answers `True`, so a
`TypeError` is never resent even by an idempotent sink. Found statically on an instance,
`inspect.getattr_static` returns the class's unbound function, which `is_retryable` would call with
one argument short: it raises `TypeError` inside the wrapper, answers `False`, and every predicate
would be refused silently. So a predicate found on the class is bound with
`found.__get__(sink, type(sink))`, which is SPEC-070's contract (the engine receives the bound
`sink._retryable_error`) and also unwraps a `staticmethod`; one found in the instance's own
`__dict__` is used as found, as ordinary attribute access would. A sink without the predicate, every
third-party sink among them, resends no exception, and a predicate that raises answers `False`
through `is_retryable`; a sink without the predicate, every third-party sink among them, resends no
exception, and a predicate that raises answers `False` through `is_retryable`. For
an `UNKNOWN` outcome or a malformed verdict there is no exception to ask, and idempotence alone
decides. A resend is a further round: it is bounded by the retry budget and waits as FR-002 says.

#### Acceptance Criteria:

- [ ] A default sink whose `deliver` raises `ConnectionRefusedError("secret-token")` on every call
      is called once per run; its events count `failed_events == n`; the run fails; nothing is
      raised to the caller of `run_delivery`; stderr names `ConnectionRefusedError` and does not
      contain `secret-token`.
- [ ] The same sink with `log_foundry_idempotent = True` and a `_retryable_error` that accepts the
      exception is called a second time with all `n` events, and a second-round `ACCEPTED` ends with
      `failed_events == 0`; with a `_retryable_error` that refuses it, raises, or is absent, it is not
      resent.
- [ ] A shipped predicate answers through the engine: a test sink subclassing `HTTPSink`, with
      `log_foundry_idempotent = True` and a `deliver` that raises `urllib.error.URLError` once and
      then returns all `ACCEPTED`, is called twice and ends with `failed_events == 0`, because the
      inherited `HTTPSink._retryable_error` answered `True`. With the engine's binding removed (the
      mutant: the static lookup's result passed to `is_retryable` unbound), it is called once.
- [ ] `log_foundry_idempotent = 1` and `= "yes"` are not declarations: the sink is not resent.
- [ ] Each malformed verdict (a returned `None`, a `dict`, a verdict one outcome short, an outcome
      that is a bare string, a subclass whose `outcomes` raises) settles with `failed_events == n`
      for a default sink and is resent once by an idempotent one, with no `Exception` escaping the
      engine and an exception whose `__str__` raises still absorbed.
- [ ] `outcomes_of` returns the exact tuple for a well-formed verdict of length `n` and `None` for
      each malformed case above and for a well-formed verdict of length `n + 1`, raising no
      `Exception` for any; an AST check finds no second length-or-type test of a verdict in `_engine.py`.
- [ ] A verdict `UNKNOWN` is counted and not resent by a default sink, resent by an idempotent one,
      and the run settles even when nothing else was accepted.
- [ ] A raise in round two after round one accepted some events settles, leaves round one's
      `ACCEPTED` events uncounted, and counts only round two's events.
- [ ] Serves invariants 7, 2 and 1: a sink whose `deliver` raises every call is a failed batch, an
      unreadable verdict is counted and settles, an unknown event is never resent unless resending
      cannot duplicate, and no `Exception` escapes the engine.

### FR-005: The worker and the orphan path call `deliver` when offered and `emit` otherwise

#### Description:

`Worker._emit` and the orphan call in `api.py._log` each call `run_delivery` first. A `None` return
means the sink does not offer `deliver`, and the existing `emit` loop (worker) or single
`sink.emit([event])` (orphan) runs **unchanged**. Both delivery loops, `_drain` and `_final_drain`,
reach the engine through `_emit_pending` and `_emit`, so one site serves both. The probe is read per
batch, so a swap to an engine sink and back takes effect on the next batch.

**Worker.** The engine's stop event is the worker's `_stop`; the decline predicate is
`Worker._retry_declined`; `_in_flight` is set before the run and reset in the same `finally`, so the
expiry line of SPEC-062 FR-005 counts the events inside a `deliver`. A failed run increments
`failed_batches` once and writes one line, through `_diag.lost("event", len(batch), ...)` exactly as
1.x does: when the run declined, SPEC-062's `retry declined after {k} emit attempt(s); the next was
predicted to end after the shutdown() deadline` with `k` the `deliver` calls made; otherwise 1.x's
`batch abandoned after {cycles} emit attempts`, with `cycles` the cycles the run started, so a run
exhausted at the defaults reads `log-foundry: lost N event(s); batch abandoned after 4 emit
attempts`, the text `Worker._emit` writes at `fda134a` and six existing tests match. A settled run adds nothing: `flush()` stays true, as
a partial outcome does today. The worker does **not** resend a whole batch around an engine run.

**Orphan.** The call runs on the caller's thread, with the sink's `log_foundry_stop_signal` as the
stop event and no decline predicate, so retries stay on the caller's thread (invariant 4). A failed
run calls `_note_orphan_loss()` once and writes one `_diag.lost` line; the caller's exception
handling is untouched, and nothing raises into the application (invariant 1). A one-event batch
fails exactly when its event ended `REJECTED`, unresolved or in a raising round, the cases in which a
1.x `emit` raised, so `DROPPED`, `UNKNOWN` and an unreadable verdict leave `orphan_lost` at zero and
are in the sink's counters.

**The model test drives both forms.** `tests/test_invariants_model.py` gains a recorder whose
`deliver` returns mixed verdicts, run over the same seeds beside the `emit` recorder. The recorder is
its own ground truth (it knows what it returned for every event), and a spy records every
`Delivery`, so the test can hold each term to its exact share rather than to "some counted term".

#### Acceptance Criteria:

- [ ] A sink offering `deliver` whose `emit` raises `AssertionError` is delivered to through
      `deliver` by the worker and by an orphan `info()`, and `emit` is never called. A sink without
      `deliver` runs the 1.x loop: `tests/test_worker.py`, `tests/test_sink_retry.py`,
      `tests/test_worker_exit_budget.py` and `tests/test_api.py` pass unedited.
- [ ] Worker, total failure: an always-`RETRYABLE` sink with `max_retries=2` under the worker's
      `max_retries=3` is called exactly 12 times by one engine run, with no whole-batch resend around
      it; `failed_batches == 1`, the one stderr line is exactly
      `log-foundry: lost N event(s); batch abandoned after 4 emit attempts` with `N` the batch size,
      `flush()` is falsy, and `failed_events` equals the batch size. A sink whose `deliver` raises
      `ConnectionRefusedError` every call gives the same `failed_batches`, falsy `flush()` and line.
- [ ] Worker, partial: `[ACCEPTED, REJECTED]` leaves `failed_batches == 0`, `flush()` truthy,
      `failed_events == 1`, and one stderr line. A malformed verdict leaves `failed_batches == 0` and
      `flush()` truthy.
- [ ] Worker, a `deliver` sink with no `max_retries` is retried up to the worker's `max_retries`.
- [ ] Exit drain through `deliver`: SPEC-062's three D=0.5 s cases give 2, 1 and 3 calls for
      `shutdown(timeout)` of 1.2, 0.9 and 1.8, `close()` is called once, and a declined run adds one
      to `failed_batches` with SPEC-062's declined wording; a `deliver` blocked past
      `shutdown(timeout=0.2)` gives an expiry line counting its events.
- [ ] Orphan: a one-event `info()` with no span records `orphan_lost == 0` for `ACCEPTED`, `DROPPED`,
      `UNKNOWN`, `SKIPPED` and a malformed verdict, and `orphan_lost == 1` for `REJECTED` and for a raising
      `deliver`, with nothing raised to the caller; a `RETRYABLE` sink with `max_retries=2` is called
      3 times and every call's thread is the caller's, then `orphan_lost == 1`.
- [ ] Twin paths: the same eight cases (the six outcomes, a raise, a malformed verdict) sent as one
      event inside a span (worker) and outside one (orphan) leave the same `SinkLosses` after each.
- [ ] Configuring an emit-only sink after an engine sink delivers the next batch through `emit`, and
      back again through `deliver`.
- [ ] The model test runs every seed with the `deliver` recorder and holds: no event accepted twice;
      `dropped_events` equals the recorder's `DROPPED` events in settled runs exactly;
      `failed_events` equals the recorder's events that were neither accepted, `DROPPED` nor
      `SKIPPED`, across all runs
      exactly; every event of a failed run is in `failed_batches`' lost lines or `orphan_lost`; and
      every event made is accepted, `SKIPPED` or in one of those, with `tests/test_invariants_model.py`'s
      citation check still green.
- [ ] Serves invariants 2, 3, 4 and 6: the worker and orphan twins keep one ledger, the orphan path
      still carries sink I/O and the span path still does not, and `deliver` and `emit` are a twin
      pair of the same two paths.

### FR-006: An engine sink's `emit` runs the same engine inline

#### Description:

`emit_via_deliver(sink, batch, *, deliver=None)` in `sinks/_engine.py` is what a sink that defines
`deliver` makes its `emit`. Given a `deliver=` callable it uses that one, so a sink can name the bound
method explicitly (SPEC-073 does, for a subclass that opts out of offering `deliver` while its own
`emit` still runs the engine). Otherwise it finds `deliver` with the same static lookup
`read_deliver` uses, **without** the `emit`-override rule (it is the `emit` the override reaches
through `super()`). It always finds `log_foundry_counters` statically. It runs `_run` with the sink's own `max_retries`, the sink's `log_foundry_stop_signal` and no
decline predicate, and then keeps the 1.x contract: it returns `None` when the run settles and raises
`SinkDeliveryError` (a message of counts and the sink's type, no values) when it fails. `emit([])`
returns without calling `deliver`. A sink that reaches the adapter with no usable `deliver` or no
`SinkCounters`, such as one that misspelt `log_foundry_counters`, raises `TypeError` naming its type
and the missing member, from every non-empty `emit`, so the batch is a counted failure rather than
events that vanish while `flush()` reports success.

So a third-party wrapper that calls `inner.emit(batch)`, or a subclass that overrides `emit` and calls
`super().emit(batch)`, sees what 1.x showed it: a normal return on a partial outcome, an unreadable
verdict or an `UNKNOWN` event, the sink's own retries spent before it returns, `dropped` counts made
only when the call returns, and a raise when the run fails. A raise caused by an exception escaping
`deliver` invites the catcher's whole-batch retry, exactly as a raising 1.x `emit` does today; the
engine itself never resends it (FR-004), and that retry is the 1.x expected-cost trade *A client
exception costs its chunk* records. The worker never calls the adapter for a sink that offers
`deliver`, so the engine's rounds are not nested. A sink's `deliver` must not call its own `emit`.

#### Acceptance Criteria:

- [ ] An engine sink's `emit` with `max_retries=2` and an always-`RETRYABLE` verdict calls `deliver`
      3 times and raises `SinkDeliveryError`, with `failed_events == n` and `dropped_events == 0`.
- [ ] `emit` returns `None` for an all-`ACCEPTED` batch, a `[ACCEPTED, REJECTED]` batch (counts
      moved, nothing raised), an all-`DROPPED` batch (the recorded exception, counted) and an
      all-`SKIPPED` batch (nothing counted by the engine), raises for `[SKIPPED, REJECTED]`, and
      `emit([])` returns `None` without calling `deliver`.
- [ ] `emit` never raises for a run holding an `UNKNOWN` event or an unreadable verdict, even when
      nothing else was accepted, and raises `SinkDeliveryError` for a `deliver` that raised with
      nothing accepted.
- [ ] A sink whose `emit` is `emit_via_deliver` and whose counters attribute is misspelt raises
      `TypeError` naming its type from `emit([event])`, and through a real `Worker` that batch moves
      `failed_batches` and `flush()` is falsy.
- [ ] `[REJECTED, DROPPED]` raises from `emit` with `dropped_events == 0`; the same sink under a real
      `Worker` with `max_retries=1` through a third-party wrapper that delegates to `inner.emit` ends
      with `dropped_events == 0` after both worker attempts and `failed_batches == 1`.
- [ ] A flaky engine sink returning `[ACCEPTED, RETRYABLE]` then `[ACCEPTED]`, behind a third-party
      wrapper and behind a subclass whose `emit` stamps a field and calls `super().emit()`, delivers
      each event exactly once through a real `Worker`, and every delivered event carries the
      subclass's stamp (the override ran).
- [ ] A shutdown cuts an engine sink's `emit` backoff short through `log_foundry_stop_signal`.
- [ ] Serves invariants 6, 7 and 2: `emit` and `deliver` of one sink are a twin pair with one ledger,
      total failure raises and partial loss reports, and no `dropped` count is made twice across a
      catcher's retry.

---

## Data Model

```
// src/log_foundry/sinks/verdict.py  -- public; re-exported from log_foundry
class Outcome(enum.Enum):                  # closed for 1.x: exactly these six
    ACCEPTED = "accepted"
    RETRYABLE = "retryable"
    REJECTED = "rejected"
    DROPPED = "dropped"
    UNKNOWN = "unknown"
    SKIPPED = "skipped"                    # not sent; no engine counter (a sink may count its own
                                           # skip through SPEC-073's run context); never resent;
                                           # not ACCEPTED for the run-failed rule

@dataclass(frozen=True, kw_only=True)
class Verdict:
    outcomes: tuple[Outcome, ...]          # outcomes[i] is the fate of batch[i]; at run time a list
                                           # is accepted and stored as a tuple; any other container
                                           # or a non-Outcome element raises TypeError naming the
                                           # position or the type
    retry_after: float | None = None       # the server's hint in seconds; advisory, not validated
                                           # here (the engine checks its type, then clamps it)

    @classmethod
    def uniform(cls, outcome: Outcome, count: int, *, retry_after: float | None = None) -> Verdict: ...

class SinkCounters:
    dropped_events: int                    # exact; written only by the engine's Deferral commit
    failed_events: int                     # an upper bound; written only by that commit
    _counter_lock: threading.Lock          # the lock Deferral reads at commit time
    def __init__(self) -> None: ...
    def losses(self) -> SinkLosses: ...    # a snapshot under _counter_lock

def read_deliver(sink: object) -> Callable[[list[dict[str, object]]], Verdict] | None: ...
    # static lookup on type(sink); None unless the class defines deliver as a function, no class
    # earlier in the MRO defines emit, and log_foundry_counters is a SinkCounters


// src/log_foundry/sinks/_engine.py  -- private
BACKOFF_BASE: float = 0.1                  # the sinks' 1.x backoff within a cycle
DEFAULT_MAX_RETRY_AFTER: float = 30.0

@dataclass(frozen=True, kw_only=True)
class Delivery:
    attempts: int          # deliver calls made
    cycles: int            # cycles started; the abandoned line's "emit attempts"
    accepted: int
    rejected: int          # ended REJECTED
    dropped: int           # ended DROPPED
    unresolved: int        # RETRYABLE when rounds ran out or a round was declined
    raised: int            # in a round whose deliver raised, not resent
    unknown: int           # UNKNOWN, or in a round whose verdict was unreadable, not resent
    declined: bool         # a further round was refused by the decline predicate
    skipped: int           # ended SKIPPED; in no engine counter
    failed: bool           # no accepted (SKIPPED is not); some rejected, unresolved or raised;
                           # no unknown

def run_delivery(
    sink: object,
    batch: list[dict[str, object]],
    *,
    worker_retries: int | None,                       # None on the orphan path (FR-002 item 3)
    stop: threading.Event | None,
    decline: Callable[[float], bool] | None = None,   # given the longest round so far
) -> Delivery | None: ...                             # None: the sink does not offer deliver

def emit_via_deliver(
    sink: object,
    batch: list[dict[str, object]],
    *,
    deliver: Callable[[list[dict[str, object]]], Verdict] | None = None,   # None: static lookup
) -> None: ...
    # raises SinkDeliveryError on a failed run, TypeError when deliver or the counters are missing

def outcomes_of(verdict: object, n: int) -> tuple[Outcome, ...] | None: ...
    # the one definition of "malformed": the verdict's outcomes as an exact tuple of exactly n
    # Outcome members, or None when verdict is not a Verdict, its outcomes are not exact members,
    # its length is not n, or reading it raises an Exception; never raises an Exception itself;
    # shared by _run and SPEC-075's wrappers

def _run(sink, deliver, counters, batch, *, sink_retries, worker_retries, stop, decline) -> Delivery: ...
    # shared; worker_retries is 0 on the orphan path and in the adapter (one cycle)

CYCLE_BACKOFF_BASE: float = 0.01           # the worker's 1.x inter-attempt backoff
CYCLE_BACKOFF_CAP: float = 0.5
def cycle_backoff(cycle: int) -> float: ...    # min(CYCLE_BACKOFF_BASE * 2**cycle, CYCLE_BACKOFF_CAP);
                                               # Worker._emit's emit loop calls it too


// src/log_foundry/worker.py
def _retry_declined(self, longest: float) -> bool: ...   # SPEC-062's comparison, written once
```

## API / Interface Contract

The optional members a sink may add. Each is read statically; none is on the `Sink` Protocol:

```
def deliver(self, batch: list[dict[str, object]]) -> Verdict: ...   # on the class; never mutate batch
log_foundry_counters: SinkCounters        # required for deliver to be offered
log_foundry_idempotent: bool              # absent means not idempotent; only `True` counts
max_retries: int                          # existing name, read by the engine
max_retry_after: float                    # existing name, read by the engine
def _retryable_error(self, err: Exception) -> bool: ...   # SPEC-070's private predicate; shipped
                                          # sinks only; consulted for an escaped exception
```

A test sink and its use:

```python
class Scripted:
    def __init__(self, script: list[Verdict]) -> None:
        self.log_foundry_counters = SinkCounters()
        self.max_retries = 2
        self.seen: list[list[dict[str, object]]] = []
        self._script = iter(script)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        self.seen.append(list(batch))
        return next(self._script)

    def emit(self, batch: list[dict[str, object]]) -> None:   # what a shipped engine sink writes
        emit_via_deliver(self, batch)

    def losses(self) -> SinkLosses:
        return self.log_foundry_counters.losses()

    def close(self) -> None: ...

sink = Scripted([Verdict(outcomes=(Outcome.ACCEPTED, Outcome.RETRYABLE), retry_after=0.5),
                 Verdict.uniform(Outcome.ACCEPTED, 1)])
configure(sink=sink)         # the worker and the orphan path now call sink.deliver
```

## Configuration / Environment

None. No `configure()` parameter, `Config` field or environment variable is added, and the engine's
constants are module constants of `_engine.py`, unreachable from `configure()`.

## File & Folder Structure

```
src/log_foundry/
├── __init__.py                 # + Outcome, Verdict, SinkCounters, read_deliver in __all__
├── _fork.py                    # the walk reaches log_foundry_counters on a holder it does not own
├── api.py                      # orphan path: run_delivery first, emit when it returns None
├── worker.py                   # _emit: run_delivery first; _retry_declined shared with the emit loop
└── sinks/
    ├── base.py                 # Sink docstring documents the new optional members
    ├── verdict.py              # Outcome, Verdict, SinkCounters, read_deliver (public)
    └── _engine.py              # the backoffs, cycle_backoff, Delivery, run_delivery, _run, outcomes_of, emit_via_deliver

tests/
├── test_verdict.py             # FR-001
├── test_delivery_engine.py     # FR-002 to FR-004, against the scripted test sinks
├── test_delivery_paths.py      # FR-005 and FR-006: worker, orphan, adapter, wrappers, proxies
├── verdict_sinks.py            # the scripted and hostile test sinks
├── test_fork_lifecycle.py      # + the counters' lock on a third-party and a subclassed sink
├── test_invariants_model.py    # + the deliver recorder and the Delivery spy
├── test_deferral.py            # + dropped_events floor, _engine._run on the roster, chain stores
├── test_public_surface.py      # + the four names and the sinks.verdict path
└── typed_consumer/             # accepts.py, rejects.py
```

## Implementation Phases

### Phase 1: The contract

- `sinks/verdict.py`: `Outcome`, `Verdict`, `SinkCounters`, `read_deliver` with its static lookup
  and override rule; the four exports; the `Sink` docstring's new optional members.
- `tests/test_verdict.py`, the public-surface pins and the typed consumer.

### Phase 2: The engine against test sinks

- `sinks/_engine.py`: rounds, `BACKOFF_BASE`, hint and budget reading, the decline hook, the
  `Deferral` commit, the run-failed rule, the two unknown outcomes.
- `tests/verdict_sinks.py` and `tests/test_delivery_engine.py`, with `tests/test_deferral.py`'s floor,
  roster entry and chain-store check.

### Phase 3: The paths and the fork reach

- `worker.py`: `_retry_declined` and `_emit` calling `run_delivery` first; `api.py`'s orphan call;
  `emit_via_deliver`; the `_fork.py` reach.
- `tests/test_delivery_paths.py`, the fork cases, the model-test recorder, and the twin-path table.

### Phase 4: Prose and records

- `docs/invariants.md` entries 2, 3, 6, 7 and 12, `docs/architecture.md` §8, the README
  sink-authoring text, `docs/component-inventory.md`, and the decision rewrite, History lines and new
  decision named in Scope.

## Revision history

- Drafted from the arc plan's Change 2, items 1 to 9. Departures from the plan's wording, each
  made so the engine can keep "no count is reclassified": `Outcome` has five members, not three,
  because `SinkLosses` has two terms (`REJECTED` is `failed`, `DROPPED` is `dropped`) and an unreadable
  response is neither retryable nor rejected (`UNKNOWN`); and the plan's "rejections" that reach
  `orphan_lost` are `REJECTED`, unresolved and raising-round events, not `DROPPED` or `UNKNOWN`, which
  a 1.x one-event `emit` returned normally for.
- Spec review, first round. An escaped exception had settled, leaving `flush()` truthy for a sink
  that raised on every call; it now fails the run (orchestrator's decision), and only an unreadable
  verdict or `UNKNOWN` settles. The probe read `deliver` past an overriding `emit` and through a
  delegating `__getattr__`; it now reads the type statically with an override rule. The fork walk
  did not reach a third-party sink's counters; it now does, by one named attribute. "Exactly once"
  was claimed for `failed_events`, which is an upper bound by invariant 2; the split is now stated.
  The hint check is now a type check before `clamp_server_delay`, which does not filter a
  non-number. The adapter's missing-counters case, the deferral check's reach beyond `self.` stores,
  SPEC-070's predicate name, the closed `Outcome` and the surface path pin were added.
- Orchestrator addition after the first revision: `Outcome.SKIPPED`, a deliberate non-send that
  counts nothing and is not `ACCEPTED` for the run-failed rule, so SPEC-075's filtering wrapper keeps
  invariant 7. `Outcome` is now six members.
- Sibling alignment after that: the `max_retry_after` ceiling follows `clamp_server_delay` (`inf`
  honoured, the default only when the sink declares none), a class attribute `deliver = None` opts
  out, `emit_via_deliver` takes `deliver=`, and `outcomes_of` is the one definition of a malformed
  verdict.
- `SKIPPED` restated as "not sent, and the engine counts nothing for it", so SPEC-075's wrappers
  may count their own skip through SPEC-073's run context; every counting statement about it names
  the engine's counters.
- Andrew's decision, 2026-11-04: the worker-path budget is `(sink + 1) × (worker + 1) − 1` further
  rounds, keeping the 1.x total of attempts while resending only the retryable subset; the orphan path
  keeps the sink's own `max_retries`. `run_delivery`'s `default_retries` became `worker_retries`.
- The wait keeps the 1.x schedule (orchestrator, following Andrew's decision): `w + 1` cycles of
  `s + 1` attempts, the sinks' backoff restarting each cycle and the worker's `cycle_backoff` between
  cycles, and the abandoned line keeps 1.x's text with the cycle count.
- Cross-arc review: FR-003's never-retried table is re-derived from invariant 2's list as SPEC-061,
  SPEC-069 and SPEC-070 leave it (the SNS sender fault, the unencodable event and NATS's unconfirmed
  batch added, a wrapper's failure shown as SPEC-075's `SKIPPED`); FR-004 consults `_retryable_error`
  through `is_retryable`; `Worker._in_flight` is attributed to SPEC-062, not SPEC-068.
- Build-order re-read: FR-004 binds a statically found `_retryable_error` with
  `__get__(sink, type(sink))`, since the unbound function would make `is_retryable` refuse every
  predicate, and a criterion proves `HTTPSink`'s inherited predicate answers `True` through the
  engine; FR-003 states that a run whose sink registered its own announcement gets no engine
  settled line, the refinement SPEC-073 relies on.
