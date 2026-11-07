---
type: Decision
title: "A sink that released its transport refuses; one that released nothing keeps accepting"
description: "both halves bind — three shipped sinks lost every post-`close()` event, while making the stateless sinks refuse would invent loss where a batch would have landed. (SPEC-032)"
---

**A sink that released its transport refuses; one that released nothing keeps accepting** — the SPEC-026 rule applied to the sink's own lifecycle, where an absorbed batch is a batch the worker believes just the same. Both halves bind: three shipped sinks lost every post-`close()` event (and Redis *succeeded*, leaking a reconnect nothing reaps), while making the stateless sinks refuse would invent loss where a batch would have delivered — so which applies is a property of the sink, recorded per class and enforced. Refusing moves no `losses()` counter: it is a failure **reported** to the worker, not one absorbed, and counting both would report one loss twice. A close landing *mid*-batch does not raise even when it catches everything — `publish()` already happened, so the total-failure test is on **refusals**, not on successes (SPEC-018's rule that only provable non-delivery may be retried). The guard is keyed on the *sink* being released, never on client ownership, or every injected-client sink would keep accepting after `shutdown()`. The lint's scope gate stopped guessing at the same time — every class in `sinks/` with an `emit`, because a roster whose completeness is the point cannot rest on a heuristic, which is exactly how two of the three sinks stayed invisible for four specs. (SPEC-032, arch §8, §13)

**`GooglePubSubSink` is the declared departure from "refuses after close"** (recorded by SPEC-070 FR-005 as row 3 of *The sinks keep four declared exceptions to their shared rules*). A `close()` that lands while `emit` is part-way through a batch does not make the call refuse or raise: every event whose `publish()` was already called, and whose future the close has orphaned, is counted on `failed` as unconfirmed — `GooglePubSubSink publish unconfirmed, the sink closed mid-batch` — because the publish may still land and a retry could duplicate it. The proof is `test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise` in `tests/test_sinks_pubsub.py`, which `tests/test_declared_exceptions.py` requires to exist.

## History

### 2026-11-07

- **Refined** by SPEC-070 FR-005: names `GooglePubSubSink`'s mid-batch close as the declared departure from this rule, and the test that proves it.

### 2026-08-07

- **Settled.**
