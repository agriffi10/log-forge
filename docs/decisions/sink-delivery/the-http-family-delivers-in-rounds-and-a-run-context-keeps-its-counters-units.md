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
event's fate is returned by index: items map back to events by identity, then by unclaimed
position, so an `_items` override returning rebuilt events maps exactly, and an event no request
carries is `DROPPED` only when that many were dropped and counted, else `REJECTED`, never a
silent default, and the round announces the ones `_items` did not count itself, on a settled run,
so the engine's settled line, which leaves a round's losses to any line that round registers, is not their announcement. The engine (*A sink may report per event*) resends only the `RETRYABLE` ones, within the 1.x total: `(s + 1) × (w + 1)` rounds on the worker path, `s + 1` on
the orphan path and through `emit`. Nothing in `http.py` waits or loops, and one round has one
wait, so a batch whose several chunks failed retryably waits once per round where 1.x waited once
per chunk. `emit` is `_engine.emit_via_deliver(self, batch, deliver=self._round)`, the private
adapter behind the public `log_foundry.emit_via_deliver` (PR 312), so a caller of `emit`
sees the 1.x contract, and its total-failure message is the adapter's, counted in events.
`_send` keeps its 1.x signature, return, counter, line and raise for `SentrySink`'s fallback and a
third party that builds its own body; its attempts are one-request rounds through `_engine._run`,
counted into a throwaway `SinkCounters` because a request is not an event, and a `_send` called
inside another run's round is a run of its own.

**The `413` search is per run.** Its budget, smallest refused size and reduction count live in the
run's notes, so a resend in a later round starts from the budget already found, and a run searches
no more than one 1.x `emit` did. Its reduced budget is never carried into a later batch.

**A hook fault costs its chunk.** Every method the round reaches runs under its guard. A raise
before the chunk's request is sent — `_take`, `_known_refused`, `_body`, `_prepare`, `_apply_auth`,
building the `Request`, or the `_drop_refused` a known-refused item goes to — drops the chunk on
`_unrenderable`, one count and one `rendering raised <Type>` line per round, since nothing was
sent and the same inputs fail the same way; so does a raise from `_chunk_budget` or `_items`, for
the whole round, and a `_take` result that is empty or longer than what is left. A raise once the
request was sent — `_exchange` with its `_retryable_status` and `_give_up`, a `2xx` reader,
`_unadjudicated`, the `_drop_refused` or `_abandon_unreduced` a `413` goes to — or a reader result
that is not one `Outcome` per event, makes the chunk `UNKNOWN` through the base
`HTTPSink._unadjudicated`, since the request went out. A raise inside the attempt — `_attempt`, the
`_response_limit` it reads, the `_retryable_error` asked about its exception — never reaches the
guard: `_exchange` classifies it as the attempt's refused exception, `REJECTED` and counted in
`failed`. What the faulting hook, or a faulting reader, registered on the run's context before it
raised is discarded, so the guard's count and line are the loss's only ones.

**A loss nothing announced is announced by the round.** The engine's settled line leaves every loss
of a round that registers a line to that round's lines, and one round holds every chunk, so a loss whose hook registered none — a 1.x bool
`_handle_response` returning `False`, a third-party `_outcomes` or `_abandon_unreduced` — showed in
`log_foundry_counters` alone whenever any other line was in the round. The round now registers a line
for every chunk it settles `REJECTED`, `DROPPED` or `UNKNOWN` with no line registered while it was
handled, `SETTLED` (an idempotent sink's `UNKNOWN`, which the engine resends, `LAST`). Every shipped
hook announces its own losses, so no shipped class gains a line. The key is the chunk: a hook that
registers any line while its chunk is handled owns the announcement of every loss in it. The
`UNKNOWN` rule reads the run's idempotence (`RunContext.resends_unknown`), not the inner sink's, since
the engine decides resends on the sink it was handed, a delegate forwarding `deliver` included; so
does `_unadjudicated`. A 1.x bool shim over a shipped reader still commits its reader's line at once,
in a detached run, and records it on the calling run (`RunContext._written_elsewhere`), so neither
the round nor the engine announces that loss again. Do NOT replace this with an engine that counts
the events each sink line covers: a line carries no event identity to count by. An escaped exception would fail
the engine's run and report as undelivered a batch whose other chunks landed. The population is
derived from `_round`'s own calls (`tests/test_http_round_guards.py`), so a new hook has no
fault scenario until one is written, and the roster test fails by its name.

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
settled run's engine line names each loss decided in a round that registered no line the run will
write, or in one whose verdict the engine set aside (a raise, an unreadable verdict, marks that do
not fit), and only those, so a loss is announced at most once, and always when its round wrote no
line: the round, not the run, is the engine's key, for the reason the chunk is the HTTP round's — a
line carries no event identity, so a round that announces some of its losses owns the rest. An event
a round left resendable (`RETRYABLE`, or an idempotent sink's `UNKNOWN`) is a loss only because the
run ended on that round, so only a kept `LAST` line of that round owns it. A round the engine set
aside has what it registered discarded (`RunContext._rewind`), as the round's hook guard discards a
faulting hook's, so its counts and lines never describe outcomes the engine decided instead. SPEC-074 extends the context for the SDK and driver sinks.

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

### 2026-11-09

- **Refined** (fix/engine-announce-every-loss): the engine's settled line was written only for a run
  in which the sink registered no line at all, so a `deliver` sink outside the HTTP family — a
  subclass of an SDK or driver sink, or one written from scratch — that announced a loss in one
  round and returned another silently in a later round left the second in `log_foundry_counters`
  alone (invariant 2). The engine now keys on the round: it names each loss decided in a round that
  registered no line the run will write. A round that announces some of its losses still owns the
  rest, as an HTTP hook owns its chunk; the HTTP round's own line for an unlined chunk stays, because
  the chunk is a finer key than the round, and the engine sees that round as lined, so nothing is
  announced twice (`tests/test_engine_announces_every_loss.py`). The two diff reviews added three
  rules: a refused round's drop is named (the refusal line counts only losses); a resendable event
  is owned only by a kept `LAST` line, so the HTTP `emit` and worker twins name a reader's
  `RETRYABLE` alike; and a round whose verdict the engine set aside has its registrations
  discarded and owns nothing.

### 2026-11-08

- **Refined** after SPEC-073: the guard now covers every hook the round reaches, where it covered
  only the request builders and the `2xx` readers, and a raising `_retryable_status`, `_items`,
  `_take`, `_event_ceiling`, `_chunk_budget`, `_drop_refused` or `_abandon_unreduced` escaped the
  round and failed every event in it, landed chunks included. An event `_items` leaves out
  uncounted is now announced by the round, where beside any sink line it showed in
  `log_foundry_counters` alone (invariant 2), and so is any chunk whose hooks settled a loss with no
  line, which the frame-1 diff review found the unplaced line made reachable beside a 1.x bool
  reader's `False`. A third-party bool reader's lone `REJECTED` chunk is therefore announced by the
  round's `refused for good` line, where SPEC-073 specified the engine's settled line. These close
  what SPEC-073's delivery doc recorded as follow-ups; none changes a promise, only where the code
  fell short of one.
- **Settled** by SPEC-073. The engine's settled-line refinement SPEC-072 FR-003 states was added
  here, keyed on lines that will commit, because `_run` had no context to read before. The
  item mapping by unclaimed position, and its `REJECTED` fallback, came from the frame-1 diff
  review.
