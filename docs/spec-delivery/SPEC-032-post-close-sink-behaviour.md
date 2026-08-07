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
  lint, and a new post-close one. Every sink either has a driver double proving it refuses, or
  asserts `**accepts emit after close**` (SPEC-032 FR-003) in its class docstring. `CLOSED_SINKS`
  and its coverage check are gone; the parametrization is derived. Twenty-one classes carry the
  claim, in three groups with three reasons (fresh connection per request, no-op boto3 close,
  delegation to a child or transport).
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

1029 tests pass; `ruff`, `mypy --strict` and `spec-lint` clean. Every new guard was mutation-checked
in place, one at a time, against the worktree's own editable install: deleting each guard fails
exactly one test, and a guard *moved to after the driver call* is killed by the driver-contact
assertion rather than the exception one — which is the half that matters, since a
`SinkDeliveryError` still came back from the mutant. The Pub/Sub mid-batch re-check and the
ownership-independent Redis guard were each mutated to their plausible wrong version and killed by
the test naming them.

Measured end to end before and after: `log_foundry.info()` after `shutdown()` into a `KafkaSink`
went from *queued and silently lost* to refused, with the orphan path's SPEC-025 guard writing a
`_diag` line. Note that `health().failed_batches` stays at `0` for that path and correctly so — the
orphan emit never reaches the worker, so `retired` plus the stderr line are the signal, exactly as
SPEC-030 concluded. The three extras (`kafka`, `gcp-pubsub`, `redis`) are not installed in CI, so
every case runs against injected doubles; no live broker was exercised.
