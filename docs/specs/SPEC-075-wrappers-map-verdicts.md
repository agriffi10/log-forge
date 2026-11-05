---
type: Spec
id: SPEC-075
title: "Wrappers Map Verdicts; MultiSink Keeps Calling Each Child's emit"
description: "The filtering, transforming and delegating wrappers offer deliver exactly when the sink they wrap does, map its verdict back to their own batch and count only their own decisions, while MultiSink offers none and keeps calling each child's emit — so a wrapped engine sink is retried and counted as it would be alone."
spec_status: Draft
depends_on: [SPEC-017, SPEC-058, SPEC-059, SPEC-066, SPEC-072, SPEC-073, SPEC-074, SPEC-080]
arc: stability-redesign
arc_order: 7
---

## Overview

SPEC-072 gives a sink an optional `deliver(batch) -> Verdict` and one engine that resends only the
events a verdict called retryable. SPEC-073 puts the HTTP family on it, and SPEC-074 puts on it the
SDK and driver sinks and the socket transport. A sink a caller wraps, `FilteringSink(HTTPSink(...))`,
`TransformSink(SQSSink(...))` or `LogstashSink(...)`, still offers only `emit`. So the worker runs
its 1.x loop around the wrapper, the wrapper calls the inner sink's `emit`, and that `emit` runs the
inner engine inline with the inner sink's own budget. On a total failure the two retry loops
multiply, and SPEC-062's shutdown prediction sees one attempt where the inner engine made several.

This spec gives each shipped wrapper a `deliver`, which it **offers exactly when the sink it wraps
offers one**. The wrapper does its own per-event work: filtering, transforming, or nothing for a
pure delegate. It calls the inner sink's `deliver` with what it forwards, and maps the inner verdict
back to its own batch's indices. **It counts only its own decisions.** The inner sink's outcomes
land in the inner sink's own counters, exactly where they land when the inner sink is configured
alone. The wrapper does that by holding the inner sink's own `SinkCounters` object, as
`SyslogSink` holds its transport's in SPEC-074. Its own drops land in its 1.x `_dropped` through
SPEC-073's run context, at the moment SPEC-058 counts them. A wrapper whose inner sink has no
`deliver` keeps its 1.x `emit` path exactly. Every wrapper's own `emit` is unchanged, so a caller
or a `MultiSink` that calls `emit` gets 1.x behaviour.

`MultiSink` gets no `deliver`: it sends one batch to several children and cannot give one honest
verdict for an event that one child took and another refused. It keeps calling each child's
`emit`. For a child on the engine, that `emit` now runs the child's engine inline. This spec states
what that does to `MultiSink`'s absorbed count and to its re-raise on total failure (SPEC-017
FR-004). The resulting extra count in `failed` is invariant 2's recorded upper bound.

It builds on SPEC-072, SPEC-073 and SPEC-074, all earlier in this arc and listed in `depends_on`.
From SPEC-072 it uses:

- the static lookups of `read_deliver` and the rule that an `emit` defined earlier in the MRO
  withholds `deliver`;
- the rule that an exception escaping `deliver` fails the run, while a malformed verdict or an
  `UNKNOWN` outcome settles;
- `outcomes_of`, the malformed-verdict check;
- `SKIPPED`.

From SPEC-073 it uses the run context and its owner-addressed `count`. From SPEC-074 it uses the
socket transport's `deliver`, the refusal marker `RunContext.refuse`, and the rosters this spec's
three classes stand outside.

It also builds on SPEC-066, which has landed, in 1.1.0, and makes `LogstashSink`'s `max_retries` an
exact, floored `int` at construction. SPEC-066 is in `depends_on`. This spec changes no constructor's signature.

Figures re-measured at `fda134a`, each with its instrument:

- Sinks that hold another sink: five (`grep -rn '_lifecycle\.release(' src/log_foundry/sinks`:
  `filtering`, `multi`, `logstash`, `transform`, `sentry`).
- Of those, four call the held sink's `emit` (`grep -rn '\.emit(' src/log_foundry`, six lines:
  the worker's `_emit`, the orphan call in `api.py`, and `filtering.py:133`, `multi.py:118`,
  `logstash.py:213`, `transform.py:213`). `SentrySink` sends envelopes through `HTTPSink._send`
  and never calls its delegate's `emit`.
- None of the four re-chunks. Each forwards one list per call: `kept`, `forwarded`, and `batch`
  twice (read at the four lines above).
- `tests/test_deferral.py` at `fda134a` holds 15 modules in `DEFERRING_SITES`, and its
  `DROPPED_FLOOR` holds five names, `_dropped` among them. SPEC-072 and SPEC-074 change both sets
  before this spec builds. This spec edits neither set, nor `IMMEDIATE`, and its own criteria
  name only `_dropped`, which the wrappers' 1.x `emit` keeps reporting.

## Scope

### In Scope

- `deliver` on `FilteringSink` and `TransformSink`, offered exactly when the inner sink offers one,
  holding the inner sink's counters and copying its retry budget at construction (FR-001).
- How those two wrappers map their own decisions and the inner verdict back to their batch. This
  covers their own count through the run context, and the twin pair it creates:
  wrapper-with-`deliver` and wrapper-with-`emit` (FR-002).
- `LogstashSink` as a pure delegate to whichever backend offers `deliver`, in HTTP and socket mode
  (FR-003).
- `MultiSink` keeping `emit` alone with engine children: its absorbed count, its retry interaction,
  and its `emit` docstring (FR-004).
- The SPEC-074 rosters the three classes stand outside, kept exact (FR-001, FR-003).
- Prose: the wrapper and `MultiSink` docstrings, `docs/invariants.md` entries 2, 6 and 7,
  `docs/architecture.md` §8's wrapper paragraph, the README's wrapper text and
  `docs/component-inventory.md`.
- At completion, three changes to the decisions in `docs/decisions/sink-delivery/`:
  - `caller-code-that-fails-for-one-event-costs-only-that-event-a-batch-of-nothing-but-failures-re-raises`
    gains this spec's own dated History line, beside the one SPEC-074 wrote for an `SQSSink` FIFO
    batch: a `FilteringSink` or `TransformSink` that offers `deliver` follows the same rule when its
    predicate or transform fails on every event — `REJECTED` with the refusal marker, one attempt, no
    longer retried by the worker, no sink counter moved, and `emit` re-raising the first exception
    by identity. Its fence and body stay.
  - `a-raising-emit-counts-nothing-whoever-catches-the-raise-owns-the-batchs-count` gains a dated
    refinement naming an engine child as a child that counted before raising.
  - One new decision records the wrapper rule: `deliver` is offered when the inner sink offers it,
    with the inner sink's counters, mapped to the wrapper's own indices, the wrapper counting only
    its own decisions, and `MultiSink` never offering it.
- Four FRs; the spec is not above the ceiling.

### Out of Scope

- **Defining or changing an `Outcome`.** `SKIPPED` and the engine's counting of it are SPEC-072's.
- **Any change to a wrapper's `emit`, its `losses()`, or to `MultiSink.emit`, `_absorbed` or
  `failed`.** A wrapper's `emit` keeps its body as it stands when this spec builds and does not
  become SPEC-072's `emit_via_deliver`, because a wrapper over an emit-only sink has no `deliver`
  for the adapter to run. On an all-failed batch it re-raises the caller's own exception by
  identity, as 1.x does.
- **`deliver` on `MultiSink`** (a fixed decision), and on `KafkaSink` and `GooglePubSubSink`.
- **`SentrySink`.** It delivers through `HTTPSink._send`, never its delegate's `emit` or
  `deliver`, so it is not a wrapper in this spec's sense; its HTTP delivery is SPEC-073's.
- **The socket transport's own `deliver`**, which is SPEC-074's. This spec only delegates to it.
- **A mapping for a re-chunking wrapper.** No shipped wrapper re-chunks (measured above).
  `HTTPSink`'s own chunk mapping is SPEC-073's.
- **Removing the extra count in `failed` that `MultiSink` makes for an engine child.** It is
  invariant 2's recorded upper bound, and recounting it would reclassify a 1.x count (FR-004).
- **Declaring a wrapper idempotent**, caching a transform's result across engine rounds, and
  per-event reasons in a `Verdict`.
- **Third-party wrappers.** They call `inner.emit` and get 1.x behaviour through SPEC-072's
  adapter. This spec changes nothing for them.
- **The closed-guard helper, lifecycle, owed closes and fork** (SPEC-076 to SPEC-079).
- **Deleting a test.** No mechanism is removed. The tests this spec edits are the rosters FR-001
  names; every other test is not edited by this spec.

---

## Functional Requirements

### FR-001: `FilteringSink` and `TransformSink` offer `deliver` exactly when their inner sink does

#### Description:

**Which class defines what.** `FilteringSink` and `TransformSink` each define `deliver` and `emit`
in their own class body, and neither inherits from a class that defines either. Nothing earlier in
the MRO than the class defining `deliver` defines `emit`, so SPEC-072's probe can offer it. The
class's own `emit` is its 1.x body, unchanged, and does not call the adapter. A caller's subclass
that overrides `emit` stands earlier in the MRO, so the probe does not offer it `deliver`, and its
`emit` runs.

**The offer is fixed at construction.** SPEC-072 reads `log_foundry_counters` statically, so it is
a plain instance attribute. When `read_deliver(inner)` is not `None`, the constructor sets it to
**the inner sink's own `SinkCounters` object**, read with `inspect.getattr_static` on the inner
sink. Otherwise it sets it to `None`. So a wrapper over any of these is never offered, and the
worker and the orphan path run its 1.x `emit`:

- an emit-only sink;
- a sink with `deliver` and no counters;
- a `MultiSink`;
- a third-party emit-only wrapper;
- a proxy whose `deliver` comes through `__getattr__`.

**The engine's totals land on the inner sink's counters.** Sharing the object is what keeps every
family's ledger single. SPEC-072's engine commits a run's event totals into the run's
`log_foundry_counters`. A SPEC-074 sink's `losses()` *is* those totals, while an `HTTPSink`'s
`losses()` reads its 1.x attributes, which its `deliver` counts into itself through the run context
(SPEC-073). With the counters shared, a wrapper run commits each outcome the inner sink returned
into exactly the object, and through exactly the run-context counts, that the bare inner sink's run
would use. A wrapper holding counters of its own would double-count an `HTTPSink` inner sink and
lose a SPEC-074 sink's.

**A call to `deliver` when nothing is offered.** A direct `deliver` on a wrapper whose
`log_foundry_counters` is `None` raises `TypeError` naming the wrapper's class. A subclass that
overrides `emit` holds the counters and is not offered `deliver`. A direct `deliver` call on it
runs normally, because the probe's override rule governs the worker and the orphan path, not
direct calls.

**The inner sink's `deliver` is looked up on every round.** Each `deliver` call takes
`read_deliver(self._inner)`. A `None` there means the inner sink stopped offering since
construction, for example because its counters were replaced. In that case the wrapper raises
`TypeError` naming the inner sink's type, which fails the run (SPEC-072). The wrapper never calls
`run_delivery` or `emit` on its inner sink from `deliver`, so the engine's rounds are not nested.

**How a type is named.** Every `TypeError` this spec adds names a type, whether the wrapper's, the
inner sink's or `LogstashSink`'s, through `_diag._STR_STR(_diag._STORED_NAME(type(x)))`, never
`type(x).__name__`. A metaclass `__name__` property cannot rename it, and SPEC-071 FR-005's guard
holds. Each names `self` or an attribute, not a parameter, so none joins that guard's pinned door
set.

**The retry budget is copied at construction.** The engine runs its rounds on the wrapper. So when
the wrapper offers `deliver`, it copies `max_retries` and `max_retry_after` from the inner sink as
plain instance attributes, with an ordinary attribute read (SPEC-074 makes some of them read-only
properties). One the inner sink lacks, or whose read raises, is not set, and the engine uses its
default. The copy is a pass-through, not a floor. The engine judges exactness, and SPEC-066's
guard, which looks for a `max(...)` floor, has nothing to see. A later assignment to the inner
sink's attribute is not seen.

**A wrapper declares no idempotence.** It sets no `log_foundry_idempotent` and defines no
`_retryable_error`, so an `UNKNOWN` or a raising round is never resent through it. A resend would
re-run the caller's code, and a non-deterministic transform would then send a different event. No
shipped sink declares idempotence anyway (SPEC-074), so a wrapped shipped sink is resent exactly as
it is alone.

**The rosters these classes stand outside.** SPEC-074's derived roster of engine sinks requires a
`def emit(self, batch)` whose body is the one call `emit_via_deliver(self, batch)`, `losses()`
returning the counters' losses, and floor and exact count thirteen. SPEC-070's loop roster, as
SPEC-074 restates it, requires `_retryable_error`, called through `is_retryable`, on each class
defining `deliver`. Each roster names its exclusions, each with its reason. This spec adds
`FilteringSink`, `TransformSink` and `LogstashSink` to both lists, as SPEC-074 says it will, and
keeps both populations at thirteen and eleven. The reason for each is that its `emit` is a 1.x body, its
`losses()` is a 1.x formula, and its `deliver` catches nothing. SPEC-074's check that no class
under `sinks/` defines `log_foundry_idempotent` stays green, because none of the three does.

#### Acceptance Criteria:

- [ ] `read_deliver` returns the bound `deliver` for each wrapper over an engine sink, and
      `wrapper.log_foundry_counters is inner.log_foundry_counters`. It returns `None` over an
      emit-only sink, a sink with `deliver` and no counters, a `MultiSink`, a third-party emit-only
      wrapper and a `__getattr__` proxy, and for a subclass of either wrapper that overrides `emit`.
- [ ] Over an emit-only inner sink the worker calls the wrapper's `emit`.
      `tests/test_wrapper_failures.py`, `tests/test_transform_coercion.py`,
      `tests/test_sinks_compose.py` and `tests/test_multi_absorbed.py` are not edited by this spec
      and pass.
- [ ] Through a real `Worker` with its default `max_retries=3`, a wrapped always-`RETRYABLE` inner
      sink with `max_retries=2` gets exactly the `deliver` calls the bare inner sink gets: 12, the
      1.x total budget `(2 + 1) × (3 + 1)`. As an orphan `info()` both get 3. An inner sink with no
      `max_retries`, or one whose read raises, gets the same count through the wrapper as bare, and
      nothing escapes.
- [ ] A hint of `2.5` against the inner sink's `max_retry_after=1.0` waits `1.0`. An inner sink
      that sets `log_foundry_idempotent = True` and answers `UNKNOWN` is not resent through the
      wrapper.
- [ ] A direct `deliver` on a non-offering wrapper raises `TypeError` naming the wrapper's class. An
      inner sink whose counters are replaced after construction makes the next round raise
      `TypeError` naming its type, and the run fails. For an inner sink class whose metaclass
      `__name__` property returns another name, the message carries the stored name.
- [ ] A forked child that finds the shared counters' lock held by a parent thread completes a run
      through the wrapper within 5 s.
- [ ] SPEC-074's engine-sink roster still finds exactly thirteen classes, and SPEC-070's restated
      loop roster eleven, each with the three classes excluded by name. Removing one exclusion
      reddens its roster naming that class. SPEC-074's `log_foundry_idempotent` check passes.
- [ ] Serves invariants 2, 6 and 12: each outcome lands on the object the bare inner sink uses, a
      wrapped engine sink retries as it does alone, and a forked child never waits on the shared
      counters' lock.

### FR-002: The two wrappers map the inner verdict to their own indices and count only their own decisions

#### Description:

Within one `deliver` call the wrapper applies the 1.x per-event step: the predicate and the level
floor, or `_apply` with its coercion (SPEC-058 FR-001). It builds the list it forwards and
remembers where each forwarded event came from. Then it gives every event of **its** batch one
outcome:

| The event, in this call | Outcome | Counted by the wrapper |
|---|---|---|
| declined: the predicate said `False`, below the level floor, or the function returned `None` | `SKIPPED` | nothing |
| the caller's code failed on it, and another event of this call was forwarded or declined | `SKIPPED` | `_dropped`, `SETTLED` |
| the caller's code failed on it, and so did every event of this call | `REJECTED`, with `run_context().refuse(first_exc)` | nothing |
| forwarded | the inner verdict's outcome at its forwarded position | nothing |
| forwarded, and `outcomes_of` judged the inner verdict malformed | `UNKNOWN` | nothing |

**Why a failed event is `SKIPPED` with the wrapper's own count.** SPEC-058 counts a per-event
failure in the wrapper's `_dropped`, only when the batch did not fail, and reports it through the
wrapper's unchanged `losses()`. If the wrapper returned `DROPPED`, the engine would commit a
`dropped_events` total into the shared counters. That total is part of a SPEC-074 sink's
`losses()`, which the wrapper's `losses()` already adds to its own `_dropped`, so the event would be
counted twice. So the verdict says `SKIPPED`, which the engine counts nowhere and never resends, and
which counts as accepted no more than `DROPPED` does. The wrapper registers
`run_context().count(self, "_dropped", n, when=SETTLED)`, which SPEC-073's run context commits into
the wrapper, under its own `_counter_lock`, only if the run settles. That is SPEC-058's count, at
SPEC-058's moment. A declined event is `SKIPPED` with no count, as the wrappers' `losses()`
docstrings say a declined event is not loss. Neither kind counts as accepted, so with a destination
that fails every call a batch holding either still fails (invariant 7).

**An all-failed batch fails at once, and no sink counts it.** This is the arc's one rule for caller
code that fails on every event of a batch, which SPEC-074 states for `SQSSink`'s FIFO id callables
and records, for SQS, in the decision *Caller code that fails for one event costs only that event*;
this spec adds the wrappers' History line to that decision (*Scope*). A batch
of nothing but failures is `REJECTED`, and the wrapper calls `run_context().refuse(first_exc)` with
the first exception its predicate or function raised (SPEC-074's refusal marker). So the run fails
and its catcher owns it (`failed_batches`, `orphan_lost`), as the 1.x re-raise made it. It is not
`RETRYABLE`: an exception from the caller's code is not one any sink declared retryable, and
SPEC-070's rule fails such an exception at once, so the batch costs one attempt and the worker no
longer re-runs a raising predicate or function on this path. Because the round is a refusal, the
engine commits nothing for it: the shared counters' `failed_events`, the inner sink's `.failed` and
`losses()`, and the wrapper's `_dropped` are all unchanged, which keeps the decision's "re-raised
unchanged and nothing is counted". The wrapper's `emit` keeps its 1.x body, which re-raises the
caller's first exception by identity. Over an emit-only inner sink, where the worker calls that
`emit`, the worker's 1.x retry of the re-raise is unchanged.

**The inner sink's own outcomes.** An `Exception` the inner sink's `deliver` raises propagates
unchanged, so the engine applies SPEC-072's rule as it would to the bare inner sink: the run fails,
and nothing is resent through a wrapper. The engine counts every event of that round in
`failed_events`, the wrapper's own `SKIPPED` events included, which is SPEC-072's raising-round
upper bound. A `BaseException` propagates too.

The wrapper reads `retry_after` only from a verdict that `outcomes_of` accepted, inside an
`except Exception` that substitutes `None`. So a raising read cannot escape `deliver`, which would
fail a run that the bare inner sink's run settles. When nothing is forwarded the inner sink is not
called. `deliver([])` returns an empty verdict.

**The engine resends the wrapper's original events.** So on a later round the predicate or function
runs again, once for each event still to send. That is what the worker's 1.x whole-batch retry did.

**`deliver` writes nothing itself.** It enters no `Deferral`, takes no lock and assigns no
attribute, so calls on the drain thread and on orphan threads share nothing (SPEC-028). Its only
count is the run-context registration above, and its only other run-context call is `refuse` on an
all-failed call. It announces a caller-code failure with one
`_diag.absorbed` line per call, written at once. The line names the wrapper class, the first
exception's type and how many events were not forwarded, and never a value. That line is true
whether or not the run then settles. It is not registered with the run context, because a sink's
registered line suppresses the engine's own settled line for the whole run (SPEC-073). That would
leave a third-party inner sink's loss unannounced.

**The twin pair.** For a wrapper that offers `deliver`, its `deliver` and its `emit` are twins: the
second runs whenever a caller or a `MultiSink` calls `emit`. On the same batch and the same inner
behaviour they forward the same events and end with the same `losses().dropped`, the same inner
attributes and the same catcher term. `failed` is excluded from the comparison: it is the upper
bound, and the two paths reach it by different counts.

#### Acceptance Criteria:

- [ ] `FilteringSink` over batch `[declined, kept, raising]`, with the inner sink answering
      `ACCEPTED`, returns `[SKIPPED, ACCEPTED, SKIPPED]`. The inner sink receives exactly
      `[kept]` by identity. The run settles with the wrapper's `_dropped == 1`, the shared counters'
      `dropped_events == 0`, and `wrapper.losses().dropped == 1`.
- [ ] Every event raising returns all `REJECTED` and calls `refuse` with the first exception; the
      inner sink is never called, and `_dropped`, the shared counters' `failed_events` and the inner
      sink's `losses()` stay unchanged. Under a `Worker(max_retries=3)` this is one `deliver` call,
      `failed_batches == 1` and `flush()` falsy. As an orphan `info()` it gives `orphan_lost == 1`
      with nothing raised. A direct `emit` of the same batch re-raises that exception by identity.
- [ ] `[declined, raising]` settles with `_dropped == 1`. An all-declined batch never calls the
      inner sink and moves nothing. `[kept, raising]` with an inner sink that answers `REJECTED`
      fails with `_dropped == 0` and `failed_batches == 1`.
- [ ] `TransformSink` over `[None-result, raising, non-dict-result, ok]` returns
      `[SKIPPED, SKIPPED, SKIPPED, inner's outcome]` and settles with `_dropped == 2`. The forwarded
      event is the coerced result `tests/test_transform_coercion.py` expects.
- [ ] Indices: over `[a declined, b, c raising, d]`, an inner verdict `[RETRYABLE, ACCEPTED]` maps
      to `[SKIPPED, RETRYABLE, SKIPPED, ACCEPTED]`. Round two gives the wrapper exactly `[b]`, and
      the transform's function runs once more, for `b` alone.
- [ ] Over a real `HTTPSink` (a stub opener), `[ok, oversized]` gives
      `wrapper.losses() == bare.losses()`, both `dropped == 1`, against a bare `HTTPSink` given the
      same batch. An always-`500` destination with `max_retries=1` under a `Worker` with
      `max_retries=1` gets 4 requests per chunk both ways, with equal `.failed` and
      `failed_batches`.
- [ ] Over a real `SQSSink` (a stubbed client), `[ok, oversized]` gives
      `wrapper.losses() == bare.losses()`, both `dropped == 1`. A batch whose every entry fails
      retryably gives the same number of client calls, equal `losses()` and equal `failed_batches`
      through the wrapper and bare.
- [ ] An inner sink raising `ConnectionRefusedError("secret")` propagates out of the wrapper's
      `deliver` by identity. Through the engine, the run fails with the same `failed_batches` as
      the bare inner sink's, and stderr does not contain `secret`.
- [ ] Each malformed inner verdict turns the forwarded events `UNKNOWN`, keeps the wrapper's own
      outcomes, and lets the run settle: a returned `None`, a `dict`, a verdict one outcome short,
      a bare-string outcome, and a `Verdict` subclass whose `outcomes` raises. A verdict whose
      `retry_after` raises when read gives `retry_after=None` with nothing escaping. A
      `KeyboardInterrupt` propagates by identity.
- [ ] A call with caller-code failures writes exactly one stderr line, which contains
      `absorbed a failure`, the wrapper's class name and the exception's type name, and does not
      contain the exception's message.
- [ ] An AST check finds no `Deferral`, no `with` on a lock and no `self.<name>` store in either
      `deliver`, and exactly one `run_context().count` call naming `"_dropped"` with
      `when=SETTLED`. A planted `self._dropped += 1` in `FilteringSink.deliver` is named by
      `tests/test_deferral.py`'s dropped-counter check, and a planted `with Deferral(self)` in
      `TransformSink.deliver` is named by its construction rule.
- [ ] Twins: for each row of the table, compare a `Worker` through the wrapper's `deliver` with a
      `Worker` through a third-party pass-through around the same wrapper (its `emit` path; worker
      and inner `max_retries=0`). Both end with the same events at the inner sink, the same
      `losses().dropped` and the same `failed_batches`.
- [ ] Inside a span, the wrapper's rounds run on the drain thread. Outside one, every round runs on
      the caller's thread.
- [ ] Serves invariants 2, 4, 6, 7, 8 and 11: each drop is in one exact term once, on every inner
      family; the two delivery paths and the wrapper's twin pair keep one ledger; total failure
      fails and partial loss reports; a transform's output is still coerced; and the new line names
      a type, never a value.

### FR-003: `LogstashSink` delegates `deliver` to whichever backend offers it

#### Description:

`LogstashSink` decides nothing per event in either mode. Its `emit` is the backend's `emit` (HTTP,
and socket mode after SPEC-074), so it is a **pure delegate**. Like the wrappers, it defines
`deliver` and `emit` in its own class body. Its backend is `self._http` in HTTP mode and
`self._socket`, SPEC-074's engine `SocketTransport`, in socket mode. When `read_deliver(backend)` is
not `None` at construction:

- its `log_foundry_counters` attribute is **the backend's own `SinkCounters` object**, read
  statically;
- its `deliver` takes `read_deliver(backend)` on each call and returns that `deliver`'s verdict, the
  same object, unmapped;
- it copies `max_retries` and `max_retry_after` from the backend, as FR-001 describes, and declares
  no idempotence.

Otherwise `log_foundry_counters` is `None`, and a direct `deliver` raises `TypeError`, naming the
class as FR-001's *How a type is named* says. Each count a
backend makes through the run context names the backend as its owner (SPEC-073), and the engine's
totals land on the backend's counters. So the unchanged `losses()` and `failed`, which already read
the backend, report a run through `LogstashSink` exactly as a run through the bare backend.

In socket mode this puts `LogstashSink` beside `SyslogSink`, which SPEC-074 makes a pure delegate to
the same transport. A socket-mode batch is now delivered through the engine on the worker's thread,
inside SPEC-062's per-round prediction, where 1.x saw one `emit`. SPEC-074 leaves what socket-mode
`LogstashSink` offers to this spec.

#### Acceptance Criteria:

- [ ] In each mode, over a backend that offers `deliver`, `read_deliver(logstash)` is the bound
      method, and `logstash.log_foundry_counters is backend.log_foundry_counters`.
      `logstash.deliver(batch)` returns the backend's verdict by identity.
- [ ] HTTP mode, with a stub opener answering `200`, then `500` always: a `Worker` through
      `LogstashSink(url=...)` and a `Worker` through a bare `HTTPSink` built with the same arguments
      make the same number of requests and end with equal `losses()`, `.failed` and
      `failed_batches`.
- [ ] Socket mode, against a peer that refuses every connect and then one that takes everything:
      `LogstashSink(host=..., port=...)` and `SyslogSink` on the same transport settings end with
      equal `losses()` and `failed_batches`, and `logstash.losses() == logstash._socket.losses()`.
- [ ] Socket mode under `shutdown(timeout=0.9)` against a refusing peer with a 0.5 s connect
      timeout makes the number of rounds SPEC-062's prediction allows for the bare transport.
- [ ] Serves invariants 2, 3 and 6: a delegated run is counted once, on the backend, a socket-mode
      batch stays inside the shutdown budget, and `LogstashSink` and its backend keep one ledger.

### FR-004: `MultiSink` offers no `deliver` and keeps calling each child's `emit`

#### Description:

`MultiSink` defines no `deliver` and no `log_foundry_counters` (SPEC-072's AST check over
`multi.py` stays). So the worker and the orphan path call `MultiSink.emit`, which calls each
child's `emit`, never a child's `deliver`, even when the child is a wrapper that offers one.
Nothing in `multi.py`'s `emit` changes. What changes is what an **engine child's** `emit` does. It
is SPEC-072's adapter, which runs the child's engine inline, with the child's budget, its stop
signal (which `MultiSink` already forwards) and no decline predicate. The adapter returns when the
run settles. When the run fails, it commits the child's counts for that run and raises
`SinkDeliveryError`. So:

- **Rounds stay inside one call.** An engine child's retries happen inside one `MultiSink.emit`. To
  the worker's SPEC-062 predictor the fan-out is still one attempt, which is the shape SPEC-062's
  Out of Scope records for any sink's own loop inside `emit`.
- **The absorbed count is unchanged, and the extra count it makes is invariant 2's recorded upper
  bound.** A raising engine child is charged `len(batch)` in `_absorbed`, only when the fan-out
  returns (SPEC-058 FR-007, SPEC-059). The child has already counted its failed events in its own
  `failed`, so each such event is in `losses().failed` twice. When the fan-out re-raises, the
  worker's or the orphan path's term counts it once more. That is invariant 2's recorded exception
  for a sink's `failed`, which SPEC-072 names for an engine child's adapter raising under a
  `MultiSink`. It is the first cost the `MultiSink.emit` docstring already accepts ("a reporting
  child that had already counted its own failures before raising"). 1.x already has the same shape
  for `HTTPSink`, whose `_abandon` counts `failed` before it raises. It is kept, because removing it
  would reclassify a 1.x count.
- **No event is counted twice in `dropped`, and none is counted once per round.** A failed child
  run commits no drop (SPEC-072, and `SETTLED` in SPEC-073's run context), so a dropped event of a
  raising child is in `_absorbed` alone. A settled child run commits its drops and returns, so
  `MultiSink` counts nothing for that child. The engine adds each event to a counter at most once
  per run, so an event is counted once per `MultiSink.emit` however many rounds the child ran.
- **A child whose run settled raises nothing.** That covers a partial, `UNKNOWN` or malformed run.
  Such a child never takes part in `MultiSink`'s total-failure re-raise, and a fan-out of such
  children never invites the worker's whole-batch resend.
- **Total failure still re-raises (SPEC-017 FR-004).** When every child raises, `MultiSink`
  re-raises the first and counts nothing. The worker retries the whole batch, and each attempt runs
  every child's engine again, so a child's `failed` grows once per worker attempt, the same recorded
  upper bound ("again each time the worker retries that batch").

The `MultiSink.emit` docstring names an engine child under the first accepted cost, and says that
a child's own rounds run inside one call.

#### Acceptance Criteria:

- [ ] `MultiSink` defines neither `deliver` nor `log_foundry_counters` (AST), and
      `read_deliver(MultiSink(engine_child))` is `None`. A wrapper child that offers `deliver` and
      whose inner `deliver` records its caller is reached only through the wrapper's `emit`.
- [ ] `MultiSink(A, B)` with `A` always-`RETRYABLE` and `max_retries=2`, `B` accepting, under a
      `Worker` with `max_retries=3`: `A.deliver` is called exactly as often as one `A.emit(batch)`
      calls it alone (3), never multiplied by the worker's budget, and `failed_batches == 0`.
      `A`'s `failed_events == n`, `_absorbed == n`, `losses().failed == 2 * n` and
      `losses().dropped == 0`.
- [ ] `A` answering `[REJECTED, DROPPED]` beside an accepting `B` leaves `A.dropped_events == 0`
      and `losses().dropped == 0`. `A` answering `[ACCEPTED, DROPPED]` leaves
      `A.dropped_events == 1`, `_absorbed == 0` and `losses().dropped == 1`.
- [ ] Two children answering only `UNKNOWN`, or returning a malformed verdict, leave
      `MultiSink.emit` returning, with `failed_batches == 0` and `_absorbed == 0`.
- [ ] Two always-`RETRYABLE` children with `max_retries=1`, under a `Worker` with `max_retries=1`:
      each child's `deliver` is called 4 times, `failed_batches == 1`, `_absorbed == 0`, and each
      child's `failed_events == 2 * n`.
- [ ] A shutdown cuts an engine child's backoff short through the forwarded stop signal, and
      `tests/test_multi_absorbed.py` is not edited by this spec and passes.
- [ ] Serves invariants 2, 3 and 7: no event is counted twice in `dropped` or once per round, the
      extra count in `failed` is invariant 2's recorded upper bound, a child's wait is cut by the
      stop signal, and only a fan-out that delivered nothing raises.

---

## Data Model

No new type. The members each of the three classes gains, all set at construction:

```
log_foundry_counters: SinkCounters | None   # the inner sink's / backend's own object, or None
max_retries: int                            # copied from the inner sink when it has one
max_retry_after: float                      # copied from the inner sink when it has one
# no log_foundry_idempotent, no _retryable_error
```

## API / Interface Contract

```
# FilteringSink, TransformSink, LogstashSink, each in its own class body:
def emit(self, batch: list[dict[str, object]]) -> None: ...         # unchanged by this spec
def deliver(self, batch: list[dict[str, object]]) -> Verdict: ...   # never mutates batch
# losses() is unchanged: FilteringSink and TransformSink keep wrapped_losses(read_losses(inner),
# self._dropped); LogstashSink keeps the backend's losses()
```

```python
inner = HTTPSink("https://collector.example/ingest")       # offers deliver (SPEC-073)
sink = FilteringSink(inner, min_level="INFO")
assert read_deliver(sink) is not None                      # the worker now calls sink.deliver
assert sink.log_foundry_counters is inner.log_foundry_counters
configure(sink=sink)

legacy = FilteringSink(MyEmitOnlySink(), min_level="INFO")
assert read_deliver(legacy) is None                        # the 1.x emit path, unchanged
```

## Configuration / Environment

None. No `configure()` parameter, `Config` field, constructor parameter or environment variable is
added.

## File & Folder Structure

```
src/log_foundry/sinks/
├── filtering.py         # + deliver and the construction-time members
├── transform.py         # + deliver and the construction-time members
├── logstash.py          # + deliver and the construction-time members, either mode
└── multi.py             # emit docstring only

tests/
├── test_wrapper_verdicts.py      # FR-001, FR-002, FR-003
├── test_multi_engine_children.py # FR-004
├── test_engine_sinks_roster.py   # the three exclusions (SPEC-074's roster and SPEC-070's restated)
└── test_deferral.py              # + the two planted-store fixtures
```

## Implementation Phases

### Phase 1: The filtering wrappers

- `deliver` and the construction-time members on `FilteringSink` and `TransformSink`; the mapping
  table and the run-context count; the `absorbed` lines; the roster exclusions;
  `tests/test_wrapper_verdicts.py`, the real-`HTTPSink` and real-`SQSSink` twins, and the deferral
  fixtures.

### Phase 2: The delegate and the fan-out

- `LogstashSink`'s `deliver` in both modes;
  `MultiSink.emit`'s docstring; `tests/test_multi_engine_children.py`.

### Phase 3: Prose and records

- The wrapper docstrings, `docs/invariants.md` entries 2, 6 and 7, `docs/architecture.md` §8, the
  README's wrapper text, `docs/component-inventory.md`, and the caller-code History line, the
  refinement and the new decision named in Scope.

## Revision history

- Drafted from the arc plan's Change 2, items 2, 5 and 8. A wrapper's `emit` is kept rather than
  made SPEC-072's adapter, because its 1.x all-failed re-raise is of the caller's own exception.
- Aligned with SPEC-072 as revised. `SKIPPED` moved to SPEC-072 and this spec's FR for it was
  removed. Properties became construction-time attributes, because SPEC-072 reads them statically.
  An inner sink's exception propagates, because SPEC-072 fails the run on it. The orchestrator
  settled that an all-failed batch is `REJECTED`, and that `MultiSink`'s extra count in `failed`
  is the recorded upper bound.
- Revised after the spec review, which found that a wrapper owning its own `SinkCounters` and adding
  them to `losses()` double-counted an `HTTPSink` inner sink, whose run-context counts already land
  on the `HTTPSink`. The fix:
  - the wrapper now holds the inner sink's counters;
  - its own failed events are `SKIPPED` with a `SETTLED` run-context count into `_dropped`, so
    `losses()` is unchanged;
  - the twin criteria now run over a real `HTTPSink` and a real `SQSSink`;
  - socket-mode `LogstashSink` delegates too (SPEC-074 left it to this spec);
  - the wrappers copy no idempotence flag;
  - the rosters they stand outside are named.

  `depends_on` gained SPEC-073 and SPEC-074, whose files are now in the directory.
- Cross-arc review: an all-failed batch now calls SPEC-074's `refuse` with the caller's first
  exception, so it is one attempt, never retried, and moves no sink counter (the `failed_events`
  commit was removed); SPEC-074 owns the caller-code decision's History line and this spec no longer
  rewrites that file. The engine-sink roster's `emit` rule is SPEC-074's one-call `def emit`, and
  both rosters' exclusions are named lists that this spec extends.
- Build-order re-read: SPEC-074 now records only the SQS half of the caller-code decision's History,
  since the wrappers' `deliver` does not exist until this spec, so this spec writes the wrappers'
  History line itself.
- 2026-11-05, the SPEC-080 follow-ups folded into the arc, at Andrew's direction: every `TypeError`
  this spec adds names its type through `_diag._STORED_NAME`, so SPEC-071 FR-005's guard holds. None
  is one of that guard's doors, since each names `self` or an attribute.
- 2026-11-05: rebased onto main after v1.1.0; the landed peer specs are now in depends_on.
