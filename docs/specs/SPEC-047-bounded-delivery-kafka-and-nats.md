# Spec: Bounded Delivery for `KafkaSink` and `NATSSink`

**ID:** SPEC-047
**Status:** Draft
**Last Updated:** 2026-09-01
**Depends On:** SPEC-026, SPEC-027, SPEC-032, SPEC-038, SPEC-041

## Overview

`README.md` tells a reader that every queue/stream sink publishes "within a bound". For
`KafkaSink` and `NATSSink` that sentence is half true, and the two halves fail differently. NATS
has a genuine unbounded wait on the library's single drain thread: a JetStream publish awaits an
ack per **event**, sequentially, and the batch size is not bounded by anything — measured, five
events against a stalled server cost 25.01 s, and `Worker._final_drain` hands the process's exit
backlog over as one batch. Kafka has no such wait, but every bound that governs it belongs to
`librdkafka` and none of them is reachable from the sink's constructor, so a caller working to a
deadline cannot make the sink fit inside it. This spec bounds what is unbounded, makes the
clients' own bounds reachable, and makes the README's claim true rather than narrowing it.

## Scope

### In Scope

- A bound on the **whole batch** in `NATSSink.emit`, replacing today's per-event bound.
- Reaching `nats-py`'s connect, reconnect and drain timeouts from `NATSSink`'s constructor.
- Reaching `librdkafka`'s delivery configuration from `KafkaSink`'s constructor.
- Correcting `README.md` and both class docstrings to state what is bounded, by whom, and to what
  — including three claims this spec's own measurements found wrong.

### Out of Scope

- **A log-foundry retry loop in either sink.** Decided against on measurement, not deferred; the
  reasoning is in FR-003 and this spec exists to record that decision rather than leave it to a
  documentation edit (SPEC-041 FR-004). A reviewer should read that as answered, not missing.
- **A JetStream publish retry.** It needs a `Nats-Msg-Id` dedup header first, or a lost ack becomes
  a duplicate downstream — SPEC-018's rule. That is a larger change and belongs in its own spec.
- **Lowering `message.timeout.ms`'s default.** Kept at `librdkafka`'s five minutes; FR-003 records
  why lowering it would regress the durable-buffer role arch §8 gives this sink.
- The other five queue/stream sinks, and `GooglePubSubSink`, which SPEC-038 FR-004 already bounded.
- The broader `README.md` rewrite; only the claims about these two sinks are touched.
- `NATSSink`'s `is_connected` guard. FR-004 corrects what its docstring *claims*, having measured
  the window to be far wider than recorded, but widening the guard itself is not attempted here.

---

## Functional Requirements

### FR-001: One deadline bounds a whole `NATSSink.emit`, not each event in it

#### Description:

`_publish_all` loops over the batch awaiting `target.publish(...)`, and under JetStream each await
carries the driver's own 5 s ack timeout. A bound applied per item is `n × timeout`, which is not a
bound — the lesson SPEC-038 FR-004 and FR-005 already paid for and recorded in Key Decisions. It
binds on the worker's single drain thread, and `Worker._final_drain` flattens the exit backlog into
**one** batch (SPEC-038 measured 5,980 events), so a wedged broker at exit holds `shutdown()` for
as long as the backlog is large.

Measured against a real server stalled with `docker compose pause` — which keeps the TCP connection
open, so the client stays connected and simply never acks — `emit` of 5 events took **25.01 s**,
exactly 5 × 5 s.

A new `publish_timeout` bounds the call as a whole. Each publish is given the lesser of the
driver's own timeout and the budget still remaining, and when the budget is gone the unpublished
remainder is counted into `failed` and named in the error rather than attempted.

**The deadline is not consulted through the stop signal, and must not be.** That per-event await is
the *work*, not a wait between attempts, and SPEC-038 FR-001 AC-4a's rule is that a shutdown
shortens a wait and never skips work — the rule `KafkaSink._flush_bounded` states in the imperative
after a revision cutting its flush to zero delivered 0 of 11 events. `NATSSink` has no
inter-attempt wait, so it satisfies SPEC-027's "interruptible" half vacuously and correctly, as
SPEC-041 FR-004 AC-2 already records for `KafkaSink`.

#### Acceptance Criteria:

- [ ] AC-1: `NATSSink(..., publish_timeout=T)` returns from `emit` within `T` plus one event's
      grace, for a batch whose per-event cost would otherwise exceed it. Asserted against a stalled
      server in the integration suite, and against a slow client double in the unit suite.
- [ ] AC-2: The test proving AC-1 is run against the unbounded implementation and fails there. A
      bound whose test passes before the bound exists is the vacuity this repo keeps measuring.
- [ ] AC-3: Events left unpublished when the budget expires are counted into `losses().failed`, and
      the `SinkDeliveryError` names how many were not attempted.
- [ ] AC-4: A batch in which nothing published raises `SinkDeliveryError` (SPEC-026 FR-001); one in
      which some published returns normally, exactly as today.
- [ ] AC-5: `emit` ignores `log_foundry_stop_signal` for this deadline, and a test asserts a set
      stop event does not shorten the batch — the guard against reintroducing the measured Kafka
      "0 of 11" defect.
- [ ] AC-6: A non-positive or non-finite `publish_timeout` falls back to the default, matching
      `KafkaSink._usable_timeout`'s existing rule rather than inventing a second one.

### FR-002: `NATSSink`'s connect and drain bounds are reachable from its constructor

#### Description:

`NATSSink.__init__` calls `nats.connect(servers or "nats://localhost:4222")` with no options, so
every timeout the client has is `nats-py`'s default and none can be changed without abandoning
`servers=` and injecting a whole client. Measured against a dead server, the constructor blocked
for **120.17 s** before raising `NoServersError` — 60 reconnect attempts at 2 s — on the caller's
own thread, which for `configure(sink=NATSSink(...))` is the application's startup path.

`connect_timeout`, `max_reconnect_attempts`, `reconnect_time_wait` and `drain_timeout` become
constructor arguments forwarded to `nats.connect`. Defaults are unchanged: the reconnect budget is
what buffers events across a short outage, and shortening it by default would drop events that
today survive. What changes is that a caller with an execution deadline can reach it.

#### Acceptance Criteria:

- [ ] AC-1: `NATSSink(..., connect_timeout=…, max_reconnect_attempts=…, reconnect_time_wait=…,
      drain_timeout=…)` forwards each to `nats.connect`, asserted by capturing the kwargs.
- [ ] AC-2: With `max_reconnect_attempts=1, reconnect_time_wait=0.1`, construction against a dead
      server fails in under 5 s rather than ~120 s. Run in the integration suite against a port
      nothing is listening on.
- [ ] AC-3: Omitting all four reproduces today's call exactly — no kwarg is passed that was not
      passed before, so an injected or default client sees no behaviour change.
- [ ] AC-4: Passing any of the four together with `client=` raises `ValueError`, since an injected
      client is already connected and the argument can have no effect (SPEC-043's rule that an
      argument no backend can consume is an error, not an ignore).

### FR-003: `KafkaSink` exposes `librdkafka`'s delivery bound instead of adding a second one

#### Description:

SPEC-041 FR-004 measured this sink and concluded it "adds none and needs none", and **that
conclusion stands** — this FR argues for it rather than reversing it. `produce()` is a local
hand-off, re-measured here at 0.0001 s for three messages against a dead broker, so it never holds
the drain thread; `librdkafka` retries on its own thread within `message.timeout.ms`, whose
five-minute default this spec measured rather than assumed — the delivery callback fired at
**300.18 s**.

Layering a log-foundry retry over that would be wrong in two independent ways. `produce()` having
returned means the producer has *accepted* the message and owns its delivery, so re-sending it
duplicates whatever the producer eventually lands — the rule SPEC-018 settled and this sink's own
`losses()` docstring already states. And a second bound on top of a five-minute one multiplies the
worst case rather than bounding it, which is the objection SPEC-041 raised when it declined to
close this by editing prose.

The real defect is reachability: `__init__` accepts `topic`, `producer`, `bootstrap_servers`,
`key_field` and `flush_timeout`, and nothing else reaches the producer it builds. A new
`producer_config` mapping is merged **beneath** the sink's own keys when the sink constructs the
producer, so a caller can set `message.timeout.ms`, `retries`, `retry.backoff.ms` or
`queue.buffering.max.messages` without giving up `bootstrap_servers=`.

The default stays at five minutes. Lowering it would drop messages that a process surviving a
two-minute broker outage delivers today, which is the durable-buffer role arch §8 gives this sink;
a caller whose deadline is shorter than their broker's worst outage is the one who should choose.

#### Acceptance Criteria:

- [ ] AC-1: `KafkaSink(topic, bootstrap_servers=…, producer_config={"message.timeout.ms": 1500})`
      builds a producer carrying that value, asserted by capturing the config the `Producer`
      constructor received.
- [ ] AC-2: `producer_config` cannot override `bootstrap.servers` — the sink's own key wins, and a
      test asserts the merge order rather than only that the key is present.
- [ ] AC-3: `producer_config` passed together with `producer=` raises `ValueError`; it cannot be
      applied to a producer the caller already built, and ignoring it would silently drop the
      caller's bound (SPEC-043's rule).
- [ ] AC-4: Omitting `producer_config` produces exactly today's config dict, so no existing caller's
      producer changes.
- [ ] AC-5: This spec's Revision history records the measured argument against a retry loop, so a
      later audit reads a decision rather than re-finding a gap.

### FR-004: The README and both docstrings state what is bounded, by whom, and to what

#### Description:

The claim SPEC-041 declined to narrow becomes true, and three statements this spec's measurements
found wrong are corrected in place per SPEC-021's rule — struck with the spec that closed them,
never deleted.

1. `NATSSink`'s docstring says a JetStream publish is "bounded by the driver's own timeout (5 s by
   default)". True per event and false per batch, which is the whole of FR-001.
2. The same docstring says `is_connected` "does not flip the instant the server dies", implying a
   narrow window. Measured, it was still `True` **40 s** after the server was stopped, because
   `nats-py` notices through a 120 s ping interval or a failed write. The limit is wider than
   recorded and is stated as measured.
3. `README.md` groups `KafkaSink`, `NATSSink` and `GooglePubSubSink` as sinks that "add no retry
   loop and need none — each hands off locally and returns without waiting". True of Kafka and of
   a core NATS publish; false of a JetStream one, which waits for an ack.

#### Acceptance Criteria:

- [ ] AC-1: `README.md`'s queue/stream paragraph states, for each of the two sinks, the bound that
      governs it, whose bound it is, and the parameter that reaches it.
- [ ] AC-2: Each corrected claim is struck in place with SPEC-047 named, not silently rewritten.
- [ ] AC-3: Both class docstrings state the worst-case total delay as every other retrying sink
      does, using this spec's measured figures rather than the drivers' documented ones — the 30 s
      `drain_timeout` is on the client's options and bound in neither measured case, so it is not
      claimed.
- [ ] AC-4: `architecture.md` §12 records what this spec leaves open — the `is_connected` window,
      and JetStream duplicate-safe retry — each naming what would close it, per §12's own criterion.

---

## Data Model

No new types. Four new constants and the constructor arguments that read them:

```python
# src/log_foundry/sinks/nats.py
DEFAULT_PUBLISH_TIMEOUT: float = 30.0   # bounds a whole emit(); see FR-001

class NATSSink:
    def __init__(
        self,
        subject: str,
        *,
        client: Any = None,
        jetstream: bool = False,
        servers: str | None = None,
        publish_timeout: float = DEFAULT_PUBLISH_TIMEOUT,
        connect_timeout: float | None = None,
        max_reconnect_attempts: int | None = None,
        reconnect_time_wait: float | None = None,
        drain_timeout: float | None = None,
    ) -> None: ...

# src/log_foundry/sinks/kafka.py
class KafkaSink:
    def __init__(
        self,
        topic: str,
        *,
        producer: Any = None,
        bootstrap_servers: str | None = None,
        key_field: str = "trace_id",
        flush_timeout: float = DEFAULT_FLUSH_TIMEOUT,
        producer_config: dict[str, object] | None = None,
    ) -> None: ...
```

`None` for the four NATS client timeouts means "pass nothing", which is what makes FR-002 AC-3
provable: the forwarded kwargs are absent, not defaulted to the driver's own values.

---

## API / Interface Contract

```python
# Bound a whole batch, not each event in it (FR-001).
NATSSink("logs", servers="nats://…", jetstream=True, publish_timeout=10.0)

# Fail fast at startup instead of blocking ~120 s on an unreachable server (FR-002).
NATSSink("logs", servers="nats://…", max_reconnect_attempts=3, reconnect_time_wait=0.5)

# Reach librdkafka's own delivery bound (FR-003).
KafkaSink("logs", bootstrap_servers="…", producer_config={"message.timeout.ms": 30000})
```

## Configuration / Environment

No new environment variables. No new dependencies; both sinks stay behind their existing `kafka`
and `nats` extras and keep their lazy imports.

## File & Folder Structure

```
src/log_foundry/sinks/
├── nats.py       # FR-001, FR-002, FR-004
└── kafka.py      # FR-003, FR-004
tests/
├── test_sinks_nats.py                  # FR-001, FR-002 unit coverage
├── test_sinks_kafka.py                 # FR-003 unit coverage
└── integration/
    ├── test_nats.py                    # FR-001 AC-1, FR-002 AC-2 against a real server
    └── test_kafka.py                   # FR-003 against a real broker
README.md                               # FR-004
docs/architecture.md                    # FR-004 AC-4 (§12)
```

## Implementation Phases

### Phase 1: Bound the NATS batch (FR-001)

- Add `DEFAULT_PUBLISH_TIMEOUT` and the `publish_timeout` argument, reusing `KafkaSink`'s
  `_usable_timeout` rule for a non-positive or non-finite value.
- Give `_publish_all` one deadline for the batch; pass each publish the lesser of the driver's
  timeout and the remaining budget; count and name the unattempted remainder.
- Unit tests with a slow client double, including the stop-signal test (AC-5) and the
  run-against-the-unbounded-implementation check (AC-2).
- Integration test against a paused NATS container.

### Phase 2: Reach the clients' bounds (FR-002, FR-003)

- Forward the four NATS client timeouts, omitting any left as `None`; `ValueError` alongside
  `client=`.
- Merge `producer_config` beneath the sink's own keys in `KafkaSink`; `ValueError` alongside
  `producer=`.
- Unit tests capturing the kwargs and the merged config; integration test for the fast constructor
  failure.

### Phase 3: Make the documented claims true (FR-004)

- Rewrite both class docstrings' bound statements from this spec's measurements.
- Correct the README's queue/stream paragraph, striking the superseded claims in place.
- Record the two residuals in `architecture.md` §12.

---

## Revision history

- **2026-09-01 — the "add a bounded retry" reading was rejected for `KafkaSink`, on measurement.**
  SPEC-041 FR-004 left this open as a decision rather than a documentation edit. Building the
  rejected alternative first: a `_retry`-based loop around `produce()` can only re-send messages
  `librdkafka` has already accepted and owns, duplicating downstream against SPEC-018's rule, and
  stacks its own backoff on top of a five-minute bound rather than shortening it. The gap is that
  the bound is unreachable, not that it is absent, so FR-003 exposes it. Escalated and confirmed
  before the build began, since it departs from the instruction that both sinks "get a bounded
  retry".
- **2026-09-01 — the NATS finding is a per-batch unbound, not a missing retry.** SPEC-041 measured
  a *core* publish (0.00 s, fire-and-forget) and recorded JetStream's 5 s ack timeout as the bound.
  Both are true per event; neither bounds a batch, and the exit backlog arrives as one. Measured at
  25.01 s for five events against a stalled server.
- **2026-09-01 — `NATSSink.close()`'s bound is 10 s, not the client's 30 s `drain_timeout`.**
  Asserted from the driver's options first and then measured: against a stalled server `close()`
  raised `FlushTimeoutError` at 10.00 s, and against a stopped one it returned in 0.00 s with
  `ConnectionReconnectingError`. `drain_timeout` bound in neither case, so FR-004 AC-3 forbids
  claiming it.
