---
type: Decision
title: "A wrapper offers `deliver` exactly when what it wraps does, and counts only its own decisions"
description: "`FilteringSink`, `TransformSink` and `LogstashSink` offer `deliver` only when their inner sink or backend does, holding its own `SinkCounters`, its budget copied read-only as the engine reads it, its verdict mapped to their indices; a wrapper counts only `SKIPPED` and a `SETTLED` `_dropped`, declares no idempotence and never calls an inner `emit` from `deliver`. `MultiSink` never offers `deliver`. (SPEC-075)"
---

**A wrapper offers `deliver` exactly when what it wraps does.** `FilteringSink` and `TransformSink`
take SPEC-072's probe on the inner sink once, at construction. When the probe offers, the
wrapper's `log_foundry_counters` is **the inner sink's own `SinkCounters` object**; otherwise it is
`None`, and the worker and the orphan path call the wrapper's 1.x `emit`, which is unchanged. That
covers an emit-only sink, a sink with `deliver` and no counters, a `MultiSink`, a third-party
wrapper and a `__getattr__` proxy. `LogstashSink` is a pure delegate. In HTTP mode it offers when
the probe offers the `HTTPSink`'s `deliver`. In socket mode the `SocketTransport` defines `deliver`
and deliberately no `emit`, so the probe never offers it. There `LogstashSink` offers on the
transport's counters when they are exactly a `SinkCounters`, and calls the transport's `deliver`
directly, as `SyslogSink` does. Its verdict is the backend's own object, unmapped. Each `deliver`
takes the inner sink's `deliver` again on every round. An inner sink that stopped offering, or
whose counters were replaced, raises `TypeError` naming its type, which fails the run. A direct
`deliver` on a wrapper that offers nothing raises `TypeError` naming the wrapper. Both names are
read through `_diag._STORED_NAME`.

**Why the inner sink's counters, not the wrapper's own.** The engine commits a run's totals into
the run sink's `log_foundry_counters`. A SPEC-074 sink's `losses()` *is* those totals, and an
`HTTPSink` counts its 1.x attributes into itself through the run context. With the object shared,
a run through the wrapper commits into exactly the object, and through exactly the run-context
counts, that the bare sink's run would use. The spec review showed what the alternative costs:
counters of the wrapper's own double-counted an `HTTPSink` and lost a SPEC-074 sink's totals.

**The budget is copied, read-only, as the engine reads it.** The engine runs its rounds on the
wrapper, so at construction the wrapper copies the inner sink's `max_retries` and
`max_retry_after`. Each is read with `_static_value` and passed through the engine's own reader
for that name, `_engine.declared_count` or `_engine._seconds`, so the two cannot drift. A budget
that is absent, unreadable, behind a property, a `bool`, or anything else the engine reads as
declaring none is stored as `None`, and the wrapped sink gets the rounds it gets alone. A later
assignment to the inner sink is not seen. Both names are new public members of all three classes,
**read-only** through `_args.ReadOnlyCount` and `_args.ReadOnlySeconds`, set-only descriptors whose
refusal says the value is copied from the wrapped sink (from the backend, on `LogstashSink`). A
writable copy would let an assignment on the wrapper move the engine's budget away from the inner
sink's, and making it read-only later would break a 1.x caller. **The accepted break:** a
third-party subclass that assigns `self.max_retries` or `self.max_retry_after` now raises
`AttributeError` (Andrew's call, 2026-11-09). The stop signal is stored the same way, in the
instance's own `__dict__` through `_wrapper.ForwardedStopSignal`, and still forwarded to the inner
sink, because the engine reads a property as no signal.

**The verdict is mapped to the wrapper's own indices, and the wrapper counts only its own
decisions.** A declined event is `SKIPPED`. The wrapper never counts it, and the engine counts it
only in a round it fails as raising, where the inner `deliver` raised or its marks did not fit.
There SPEC-072's raising-round upper bound counts every event of the round, `SKIPPED` ones
included, in the shared `failed_events`. An event the caller's code failed
on, beside one forwarded or declined, is `SKIPPED` and registered as
`run_context().count(self, "_dropped", n, when=SETTLED)`. That is SPEC-058's count at SPEC-058's
moment, reported through the wrapper's unchanged `losses()`. It is not `DROPPED`, which the engine
would commit into the shared counters that the wrapper's `losses()` already adds its `_dropped` to.
A call whose every event failed is `REJECTED` with the refusal marker, under *Caller code that
fails for one event*. A forwarded event takes the inner verdict's outcome at its position, or
`UNKNOWN` when `outcomes_of` judges the verdict malformed. `RunContext.delegate` moves every mark
the inner round registered (`final_rejections`, `sink_counted`) into the wrapper's positions, and
an out-of-range mark stays invalid. For a malformed inner verdict, inside an engine run, it sets
aside every count, line and mark the inner call registered. The engine does the same for a bare
sink's unreadable round (PR 319), and the wrapper's own registrations, made before the call, stay.
A direct `deliver()` outside a run sets nothing aside, as the bare call does. A caller-code failure
writes one `_diag.absorbed` line per call, and that line is not registered with the run context.
A round that registers a line owns the announcement of every loss its verdict decided (PR 319,
per round). A registered line here would therefore take the engine's settled line away from that
round's losses, a third-party inner sink's among them, and leave them unannounced.

**Caller code runs once per event per engine run** (Andrew's calls, 2026-11-09). Both wrappers
keep what they forwarded in the run context's notes, keyed by the original event's identity with
the event kept in the entry, and the notes end with the run. **A `TransformSink` keeps each event's
forwarded result**, so a later round forwards the same object. A fresh copy per round is a stranger
to the inner sink's own identity-keyed run notes: over a FIFO `SQSSink` it landed `[b, a]` where
the bare sink lands `[a, b]`, and a standard queue's abandoned line counted one attempt where the
message had made four. **A `FilteringSink` keeps each forwarded event's decision**, so a resend is
forwarded without putting it to the predicate or the level floor again. A predicate that is not a
pure function of the event (dedupe, rate limit, sampling) would otherwise decline its own resend.
That event would become `SKIPPED`, never resent and, in a round that settles, counted nowhere, so an event the inner sink
answered `RETRYABLE` was lost silently. Behind a dedupe predicate, a real `HTTPSink` answering `503`
then `200` made one POST where the `emit` path made two. A predicate raising on the resend charged
a forwarded event to the inner `failed_events`. Only a forwarded event is kept, and only a
forwarded event can be resent, so caller code never runs in a later round of a run. The all-failed
refusal and every caller-code failure therefore arise only in the round that first sees an event.
Caller code runs outside any engine run, as on the `emit` path; a `deliver()` it calls is its own
detached run (`_engine.outside_runs`). Folded into the wrapper's round instead, a tee to an alerts
sink failed a delivered batch and rewound the alert's own loss line. The one exception to the twin
pair: the same event object twice in one batch shares one decision or result on the `deliver`
path, where `emit` asks per occurrence; a batch the library builds never repeats an identity.

**What a wrapper never does.** It declares no `log_foundry_idempotent` and defines no
`_retryable_error`, so nothing that may have landed is resent through it. It never calls
`run_delivery`, `emit` or `emit_via_deliver` on its inner sink from `deliver`, so the engine's
rounds never nest. Its `deliver` takes no lock, enters no `Deferral` and stores no attribute. Its
`emit` stays its 1.x body, which re-raises the caller's first exception by identity, because over
an emit-only inner sink there is no `deliver` for SPEC-072's adapter to run.

**`MultiSink` never offers `deliver`.** It sends one batch to several children and cannot give one
honest verdict for an event one child took and another refused. It calls each child's `emit`,
including a wrapper child that offers `deliver`. An engine child's `emit` runs its engine inline,
so the child's rounds stay inside one fan-out call. Its extra count in `failed` is invariant 2's
recorded upper bound (*A raising `emit` counts nothing*).

**Do NOT** give a wrapper counters of its own; do NOT copy the budget with an ordinary read, which
evaluates a property the bare engine reads as none; do NOT make the copied budget writable; do NOT
call an inner `emit` or `run_delivery` from a wrapper's `deliver`; do NOT declare a wrapper
idempotent; do NOT give `MultiSink` a `deliver`; do NOT copy a transform's result afresh, or ask
the predicate again about a forwarded event, on a later round of one run. (SPEC-075 FR-001 to FR-004)

## History

### 2026-11-09

- **Refined** the same day from the frame-3 review: caller code runs outside any engine run, so a `deliver()` it calls on another sink is its own detached run, never folded into the wrapper's round.
- **Refined** the same day from the frame-2 diff review (Andrew's call): a `FilteringSink` keeps each forwarded event's predicate decision for the run, beside `TransformSink`'s result cache, so a stateful predicate can no longer drop its own resend; and a malformed inner verdict's registrations are set aside only inside an engine run.
- **Settled** by SPEC-075 FR-001 to FR-004, with Andrew's calls on the read-only budget, the
  subclass-assignment break and the per-run transform cache.
