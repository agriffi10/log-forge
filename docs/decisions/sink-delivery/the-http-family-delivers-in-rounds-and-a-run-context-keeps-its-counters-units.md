---
type: Decision
title: "The HTTP family delivers in rounds on the engine, and a run context keeps its counters' units"
description: "`HTTPSink` and its eight subclasses offer `deliver`: a round sends each chunk in one request, and the engine owns every resend and wait within the 1.x total. A family-neutral run context keeps `failed` in requests and every counter in its 1.x unit. Moves: a total failure counts once per batch, a lone `413` beside a failed batch is the catcher's, a request-building fault is `dropped`, a raising `2xx` reader is `dropped_unadjudicated`. An `emit` override or a bool `_handle_response` keeps its 1.x path. (SPEC-073)"
---

**The HTTP family delivers in rounds on the engine, and a run context keeps its counters' units** —
before SPEC-073 the family answered "did it land, and may it be sent again?" in three layers.
`HTTPSink._send` retried one request with its own backoff, `HTTPSink.emit` re-chunked and searched
for a size after a `413`, and the worker resent the **whole** batch whenever `emit` raised, so a
chunk that had landed was sent again and one refused for good was re-POSTed once per worker
attempt. Now `HTTPSink.deliver(batch)` is one **round**: `_items` renders and drops what cannot be
sent alone, `_take` chunks the rest, and each chunk goes in **one** request, built as `_send` builds
one and classified by one private method, `_exchange`, which the round and `_send` share. Each
event's fate is returned by index, and the engine (*A sink may report per event*) resends only the
`RETRYABLE` ones, within the 1.x total: `(s + 1) × (w + 1)` rounds on the worker path, `s + 1` on
the orphan path and through `emit`. Nothing in `http.py` waits or loops, and one round has one
wait, so a batch whose several chunks failed retryably waits once per round where 1.x waited once
per chunk. `emit` is `emit_via_deliver(self, batch, deliver=self._round)`, so a caller of `emit`
sees the 1.x contract, and its total-failure message is the adapter's, counted in events.
`_send` keeps its 1.x signature, return, counter, line and raise for `SentrySink`'s fallback and a
third party that builds its own body; its attempts are one-request rounds through `_engine._run`,
counted into a throwaway `SinkCounters` because a request is not an event, and a `_send` called
inside another run's round is a run of its own.

**The `413` search is per run.** Its budget, smallest refused size and reduction count live in the
run's notes, so a resend in a later round starts from the budget already found, and a run searches
no more than one 1.x `emit` did. Its reduced budget is never carried into a later batch.

**A hook fault costs its chunk.** A raise from `_body`, `_prepare`, `_apply_auth` or building the
`Request` drops the chunk on `_unrenderable`, one count and one `rendering raised <Type>` line per
round, since nothing was sent and the same inputs fail the same way. A raise from a `2xx` reader, or
a result that is not one `Outcome` per event, makes the chunk `UNKNOWN` through
`HTTPSink._unadjudicated`, the one method that gives up on a response, since the request
succeeded. An escaped exception would fail the engine's run and report as undelivered a batch
whose other chunks landed. The guard covers those hooks only; a raising third-party override of
another private method escapes the round as it escaped 1.x `emit`.

**The run context keeps the units.** `failed` counts requests, the per-record counters count
events, and a per-event drop counts only when the batch did not fail, so the engine's event totals
cannot carry them without reclassifying a count in a minor release. `sinks/_engine.py` gains a
family-neutral **run context**: a `ContextVar` stack, so every drain thread, orphan caller and
nested run sees only its own, on which a sink registers `count(owner, name, n, when=...)` and
`announce(line, when=...)`. When the run ends the engine commits them through one `Deferral` per
owner, `ALWAYS`, `SETTLED` only for a settled run, `LAST` only for the run's final round, then
writes the lines outside every lock; a `BaseException` commits nothing. A refused request counts
`failed` `ALWAYS`, a still-retryable one `LAST`, a drop `SETTLED`, a per-record rejection `ALWAYS`,
and an unadjudicated chunk `ALWAYS`, or `LAST` for a sink declaring `log_foundry_idempotent`. A
settled run whose sink will write a line of its own gets no engine line, so a loss is announced
once. SPEC-074 extends the context for the SDK and driver sinks.

**What moves**, each in the 1.2.0 release notes. A total failure is counted once per batch: a
request refused for good is sent and counted once, and an outage counts `failed` once per
abandoned request of the last round, naming every attempt. A lone `413` beside a batch that
otherwise failed is not counted in `dropped_oversized`; the batch's catcher counts it. A
request-building fault moves from the catcher's term to `dropped`, and a raising `2xx` reader to
`dropped_unadjudicated`, inside `failed`. Counts land when the run ends, a `BaseException`
mid-run discards counts 1.x had already made, and a run's lines are written together.

**A 1.x override keeps its path.** `HTTPSink.__init_subclass__` withholds `deliver` (as `None`,
marked as the base's own) from a class with an `emit` override, or whose `_handle_response` comes
before its `_outcomes`, above the nearest ancestor that defines its own `deliver`. Such a class is
delivered to through `emit`, which still chunks, retries, searches and raises on total failure,
and the worker's 1.x resend is what that raise gets. A class that later adds `_outcomes` below a
withdrawn one gets `deliver` back, but never over a third party's own `deliver = None`. The
detection is by class, never by instance. The three shipped readers override `_outcomes` and keep
`_handle_response` as a 1.x shim.

**Rejected.** Keeping `_send`'s loop under the chunk path: two retry layers, the defect itself.
A per-round `413` search: a resend would rediscover a budget the run already found. Counting the
family into the engine's event totals: it reclassifies `failed`. A run context naming an HTTP type:
every shipped sink needs it, and SPEC-074 extends it. A one-attempt special case for `_send` inside
a round: nesting is reachable through any engine sink's `emit`, so each run is independent
instead. Wrapping the adapter's raise to keep 1.x's message: `emit` is the adapter. A `fates=`
keyword on `_items`: a two-parameter third-party override would raise out of the round, so items
map back to batch indices by an identity walk.

**Do NOT build** a retry loop, a backoff constant or a wait in any family module; a run-context
count of a `dropped` counter that is not `SETTLED` (`tests/test_deferral.py` refuses it); a path
from `http.py` to `_attempt` other than `_exchange` inside a round (`tests/test_classify.py`
refuses it); a `deliver` restored over a third party's `deliver = None` or below an `emit`
override; `_send`'s requests in `log_foundry_counters`; or the `413` budget carried into a later
batch. (SPEC-073 FR-001 to FR-005)

## History

### 2026-11-08

- **Settled** by SPEC-073. The engine's settled-line refinement SPEC-072 FR-003 states was added
  here, keyed on lines that will commit, because `_run` had no context to read before.
