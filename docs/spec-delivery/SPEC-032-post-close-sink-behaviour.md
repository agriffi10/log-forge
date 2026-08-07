# Completed Spec — SPEC-032: Post-Close Sink Behaviour

## What was completed?

- **Three sinks stopped losing events emitted after their own `close()`.** `KafkaSink`,
  `GooglePubSubSink` and `_RedisSink` (the base of both Redis sinks) gained the `_closed` flag
  `SQLiteSink` and `MongoDBSink` already carried, and now raise `SinkDeliveryError`. Each lost
  differently — a produce into a batch nothing would flush again, a future nothing would resolve,
  and a *successful* write over a transparently reopened connection — and all three returned
  normally, so the worker believed them.
- **The sink lint's scope gate stopped guessing.** `_sink_classes_holding_a_driver` admitted a
  module by a lazy-import heuristic or one of six hardcoded handle tokens; it is now
  `_sink_classes_with_an_emit`, covering every class in `sinks/` defining `emit`/`send_all` except
  the `base.Sink` protocol. This closes the limit SPEC-028's delivery doc recorded and had
  provisionally assigned to SPEC-031's residue.
- **Two lints now run off that gate**, so neither roster can drift: the existing concurrency-decision
  lint, and a new post-close one. Of the **34** in-scope classes, **15** have a driver double
  proving they refuse and the other **19** assert `**adds no post-close guard**` (SPEC-032 FR-003)
  in their class docstring, in three groups with three reasons (fresh connection per request, no-op
  boto3 close, delegation to a child or transport). `CLOSED_SINKS` and its after-the-fact coverage
  check are gone. Two code-derived facts outrank the docstring claim — taking a transport lock in
  `emit`, or carrying a `_closed` flag — so a state-holding sink cannot talk its way out.
- **The contract is stated where an implementer reads it** — `Sink.emit`/`Sink.close` in
  `sinks/base.py`, `architecture.md` §8, and the README's "Writing your own sink", which now shows
  the flag in its worked example.

**Deviations from the spec, both by evidence:**

- **`_RedisSink` was not in the brief.** The task named two sinks; the survey found a third, and it
  is the worst-shaped of them — unguarded it does not fail after close, it *succeeds*, leaking a
  connection nothing reaps. That is SPEC-028's `RabbitMQSink` finding repeated.
- **`SyslogSink` and `LogstashSink` joined the roster rather than taking the exemption.** Both were
  expected to be exempt; both in fact refuse, through the `SocketTransport` they hold. They now
  have doubles proving the delegation carries the refusal, rather than a docstring asserting it.
- **FR-001's Redis criterion was amended in place.** It read "fires only where the sink owns the
  client", contradicting its own next sentence. The guard fires regardless of ownership; keying it
  on ownership would leave every injected-client sink accepting after `shutdown()`, which is the
  majority configuration. Recorded in `architecture.md` §13.

## What changed from earlier specs?

- **`GooglePubSubSink.emit` no longer raises on a total failure that was not a *refusal*.** The
  check was `if batch and not published`; it is now `if refused == len(batch)`. A close landing
  mid-batch leaves events that were published but cannot be confirmed, and raising there would have
  the worker re-send them — SPEC-018's rule that only a provable non-delivery may be retried. Those
  events are counted as unconfirmed (`losses().failed`) instead. Total refusal still raises.
- **`GooglePubSubSink.close` sets the closed flag inside the same critical section as the futures
  swap** it added in SPEC-028, which is what makes `emit`'s second check exact.
- **`KafkaSink.close` and `_RedisSink.close` became idempotent.** Both previously called into the
  driver on every call.
- **SPEC-028's `CLOSED_SINKS` roster and `test_every_locked_sink_has_a_post_close_case` were
  removed**, superseded by the derived parametrization and the decision lint. The behavioural
  post-close tests they parametrized are unchanged and now cover fifteen sinks rather than ten.

## Verification

1033 tests pass; `ruff`, `mypy --strict` and `spec-lint` clean. Every new guard was mutation-checked
in place, one at a time, against the worktree's own editable install. Deleting a guard fails 2–3
tests (kafka 3, pubsub 2, redis 3 — measured, not estimated), and a guard *moved to after the
driver call* is killed by the driver-contact assertion rather than the exception one, which is the
half that matters since a `SinkDeliveryError` still comes back from that mutant. The Pub/Sub
mid-batch re-check, the two flag-ordering claims, and the ownership-independent Redis guard were
each mutated to their plausible wrong version and killed by the test naming them.

Measured end to end before and after: `log_foundry.info()` after `shutdown()` into a `KafkaSink`
went from *queued and silently lost* to refused, with the orphan path's SPEC-025 guard writing a
`_diag` line. Note that `health().failed_batches` stays at `0` for that path and correctly so — the
orphan emit never reaches the worker, so `retired` plus the stderr line are the signal, exactly as
SPEC-030 concluded. The three extras (`kafka`, `gcp-pubsub`, `redis`) are not installed in CI, so
every case runs against injected doubles; no live broker was exercised.

## Review round (pre-merge, PR #118)

Returned MERGE WITH CHANGES on green CI, with two defects the suite could not see and three
docstring claims it was not holding anyone to. All are fixed; the findings are worth keeping.

- **The new lint was weaker than the one it replaced.** The pre-SPEC-032 roster derived the
  obligation from a *code* property (`_classes_taking_a_transport_lock`); the replacement let a
  docstring settle it. Demonstrated: deleting `SQLiteSink` from the builder map and adding the
  exemption claim to its docstring turned a sink that commits and closes a connection into an
  exempt one — **suite green, 1029 → 1027, no red.** That is the silent-test-deletion shape reached
  through prose. Fixed by `_may_not_claim_it_accepts`: a transport lock in `emit` or a `_closed`
  flag now outranks the claim, restoring the old floor and extending it to the unlocked sinks the
  old roster never covered.
- **Two ordering claims were asserted in docstrings and tested by nothing.** Moving
  `GooglePubSubSink.close`'s flag assignment to *after* the swap, and `KafkaSink.close`'s to after
  the flush, both left the whole suite green while restoring the loss each placement prevents. The
  first version of the replacement test *also* passed against the Pub/Sub mutant — it parked an
  emitter and ran the close to completion before releasing it, so the flag was set by the time the
  emitter looked, whichever order the close used. The window is a few instructions wide. The test
  now observes the invariant directly (is the flag set when the closing thread leaves the critical
  section?) rather than trying to land inside it.
- **A scope hole, latent:** the gate triggered on `emit`/`send_all` only, so a subclass overriding
  just `close()` to release something — while inheriting its parent's `emit` and its parent's
  recorded decision — was judged by nobody. No shipped class does this; the trigger set now
  includes `close`, which costs zero docstring churn today.
- **`_diag.lost` was called while holding `_futures_lock`.** A blocked stderr would have held a
  lock `close()` waits on — I/O inside a transport lock, which is what SPEC-028's two-lock decision
  exists to prevent. The counter bump and the line now happen after the lock is released.
- **`KafkaSink.close` released its lock before flushing**, unlike every other guarded sink, so a
  second concurrent `close()` could return while the first was still draining. It now holds the
  lock across the flush.
- **`StdoutSink`'s new no-lock justification was reasoning this repo had already rejected** — it
  cited `TextIOWrapper.write`'s own lock, which
  `test_file_sink_keeps_each_batch_contiguous_under_concurrent_emitters` explicitly records as the
  wrong guarantee. The decision (no lock) stands and is SPEC-028's; the stated reason is now the
  honest one.
- **The exemption phrase overloaded two different claims.** `**accepts emit after close**` was true
  for `HTTPSink` and the boto3 sinks but false for the wrappers, whose inner sink may refuse. It is
  now `**adds no post-close guard**`, which is a claim about the class's own code — the same shape
  as SPEC-028's `**no** transport lock`.
- **Delivery-doc counts were wrong** and are corrected above (19 claimers, not 21; 2–3 failures per
  deleted guard, not one).

Two findings were **recorded rather than fixed**, both pre-existing: the driver-contact probe is
`lambda: 0` for the `sqlite`, `file` and `rotating` cases, so that half of their post-close
assertion is vacuous (inherited from SPEC-028); and `shutdown()` is a no-op when no worker was ever
created, so a process that only ever used the orphan path never closes its sink and still loses
post-`shutdown()` events silently. The second is recorded in `architecture.md` §13 — it is a
limit on *this* spec's headline scenario, so leaving it unstated would overstate what shipped.
