---
type: Decision
title: "The sinks keep four declared exceptions to their shared rules"
description: "four departures from a shared sink rule are deliberate, and each is a row here naming its rule, classes, reason, decision and proof: Kafka, NATS, Pub/Sub and PostgreSQL floor a timeout that works; `OTLPSink` abandons a `500`; `GooglePubSubSink` counts a close mid-batch; `OTLPSink` reads an empty `2xx` body as success. A roster derives each row from the tree, so a fifth fails until it is a row. (SPEC-070)"
---

**The sinks keep four declared exceptions to their shared rules** — the shared rules (refuse a
broken argument, retry `429` and every `5xx`, refuse after a released transport, count a body that
cannot carry the verdict) each have classes that depart from them on purpose. Before SPEC-070 the
departures were recorded one per decision, so nothing said how many there were, and a fifth could
appear in a refactor with no record at all. The table below is the one place that lists them, and
`tests/test_declared_exceptions.py` holds the same rows as data and checks each against the tree.
The rows change no behaviour.

| # | Shared rule | Departing classes | Departure | Recorded in | Proved by |
|---|---|---|---|---|---|
| 1 | A timeout that is already broken is refused, one that works is floored | `KafkaSink` (`flush_timeout`, through `kafka._usable_timeout`), `NATSSink` (`publish_timeout`, and `drain_timeout` when one is passed), `GooglePubSubSink` (`overflow_timeout`), all through `usable_timeout`; `PostgresSink` (`connect_timeout`, floored at 2 through `floor_count`) | floor, not refuse, for 1.x | [A bad argument is floored if it works and refused if it is already broken; a new one is refused](a-bad-argument-is-floored-if-it-works-and-refused-if-it-is-already-broken-a-new-one-is-refused.md) | derived: each `__init__` parameter ending in `timeout`, classed by the call that judges it |
| 2 | A status is retried if it is `429` or `5xx` | `OTLPSink` retries `429`, `502`, `503`, `504` | a `500` is abandoned on the first request | [A protocol's retry set is a private predicate hook, not a second loop](a-protocols-retry-set-is-a-private-predicate-hook-not-a-second-loop.md) | derived: the classes other than `HTTPSink` defining `_retryable_status` |
| 3 | A sink that released its transport refuses; a close mid-batch raises nothing by itself | `GooglePubSubSink` | a close that lands mid-batch counts the rest of the batch unconfirmed, not raised | [A sink that released its transport refuses; one that released nothing keeps accepting](a-sink-that-released-its-transport-refuses-one-that-released-nothing-keeps-accepting.md) | `test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise` in `tests/test_sinks_pubsub.py` |
| 4 | A body that cannot carry the verdict is counted | `OTLPSink` reads an **empty** `2xx` body as success, where `ElasticsearchSink`, `OpenSearchSink` and `HoneycombSink` count it | an OTLP success needs no `partialSuccess` | [A 2xx this sink cannot read is counted, and the `_bulk` reading is argued, never inherited](a-2xx-this-sink-cannot-read-is-counted-and-the-bulk-reading-is-bulks-alone.md) | derived: each of the four readers' `_handle_response` handed `b""`, the 1.x shim over `_outcomes` since SPEC-073 |

**Row 1.** Each of these timeouts lands on a configuration that delivers when it is floored, so the
floor is a no-op for a sane caller and a rescue for the rest, while a refusal would break a
configuration that works — `GooglePubSubSink(overflow_timeout=inf)` delivers against a healthy
client. Every other timeout is refused through `require_timeout`.
The scan finds a floor through `usable_timeout`, `max` or `floor_count`, called directly or through a
module-level function of the same module, read one level deep — which is how `KafkaSink` is found.
A floor reached through two helpers, or through a keyword, is not seen; the row's floor of four is
what makes that bound fail loudly, rather than shrink the row, when a refactor moves one of the four
out of sight.

**Row 2.** OTLP names exactly `429`, `502`, `503` and `504` retryable and forbids retrying anything
else, so `OTLPSink` overrides the family's status predicate and inherits the rest: since SPEC-073 the
one classification, `HTTPSink._exchange`, inside the delivery engine's rounds, where it inherited
`_send`'s loop.

**Row 3.** The batch's first publish was already handed to the client when the close landed and
may well arrive, so raising would have the worker re-send it (SPEC-018's rule); the rest of the
batch is counted unconfirmed instead, never left uncounted.

**Row 4.** It is the one difference in reading a body that survives SPEC-070 FR-004. An **unreadable**
`2xx` body is now counted by all four readers alike; an **empty** one is not, because the `_bulk`
and Honeycomb verdicts live only in the body while an OTLP success needs no body at all.

**The roster** (`tests/test_declared_exceptions.py`) asserts, per row, that the derived population
equals the declared classes, that the decision named exists and that this file names every declared
class, and, for row 3, that the proof test is defined where the default collection finds it. Its
fixture corpus — a sink overriding `_retryable_status`, a module flooring a timeout, a deleted proof
test, a fifth reader reading `b""` as success, a three-class row 1 — each fail naming the row they
break, and the shipped tree is clean.

**Do NOT** add a departure from a shared rule without adding its row here and to the roster's data,
and do not widen a row by hand-listing a class the derivation does not find.

## History

### 2026-11-08

- **Refined** by SPEC-073, rows unchanged: row 2's `OTLPSink` inherits the engine's rounds and the
  one classification rather than `_send`'s loop, and row 4's probe reaches each reader's
  `_outcomes` through its `_handle_response` shim. The roster's reader set includes the classes
  defining `_outcomes`.

### 2026-11-07

- **Settled** by SPEC-070 FR-005. The plan named three departures; the fourth, OTLP's empty body, is
  the one FR-004 leaves standing.
