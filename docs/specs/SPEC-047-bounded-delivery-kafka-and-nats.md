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
backlog over as one batch. Kafka's own delivery path is already bounded and reachable, by
`flush_timeout`; what is unreachable there is the *client's* retention and local-queue
configuration, so a caller cannot decide how long `librdkafka` should keep retrying a message
log-foundry has already handed it. This spec bounds what is unbounded, makes the clients' own
bounds reachable, and makes the README's claim true rather than narrowing it.

## Scope

### In Scope

- A bound on the **whole batch** in `NATSSink.emit`, replacing today's per-event bound.
- Reaching `nats-py`'s connect, reconnect and drain timeouts from `NATSSink`'s constructor.
- Reaching `librdkafka`'s delivery configuration from `KafkaSink`'s constructor.
- Correcting `README.md`, both class docstrings and SPEC-041's own superseded sentence to state
  what is bounded, by whom, and to what.

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
  the window to be wider than recorded, but widening the guard itself is not attempted here.
- **`NATSSink(client=X, servers="…")` silently ignoring `servers`.** Pre-existing, and the same
  shape FR-002 AC-4 makes an error for the four new arguments in the same constructor. Left alone
  deliberately: changing it would break a caller who passes both today, which is a compatibility
  decision this spec has no measurement for. Recorded in `architecture.md` §12 by FR-004 AC-4.

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

A new `publish_timeout` bounds the call as a whole. The two publish paths are bounded differently
because the driver gives them different handles, and the spec names both rather than prescribing
one uniform mechanism:

- **JetStream** — `publish()` accepts a `timeout`, so each event is given
  `min(DEFAULT_ACK_TIMEOUT, remaining budget)`. `DEFAULT_ACK_TIMEOUT` is a constant of this
  module's own, mirroring `JetStreamContext`'s private default, because the sink builds its
  context with a bare `self._client.jetstream()` and cannot read that value back.
- **Core** — `Client.publish` takes no timeout at all (verified against `nats-py` 2.15.0) and is a
  local buffer write, so the deadline can only be checked **between** events. That is enough:
  nothing on this path waits, which is why SPEC-041 measured it at 0.00 s.

`DEFAULT_PUBLISH_TIMEOUT` is **10.0 s, not 30**, and the reasoning is `KafkaSink`'s for its own
`DEFAULT_FLUSH_TIMEOUT`. `_lifecycle.DEFAULT_SHUTDOWN_TIMEOUT` is 30.0, and this budget is spent
*inside* it: `Worker.shutdown` sets the stop event, joins the drain thread against that deadline,
and `_final_drain`'s single `emit` runs on that thread. An `emit` allowed to consume the whole 30 s
expires the join, and an expired shutdown leaves the sink **open** by SPEC-027 FR-004 — so
`close()`'s drain never runs and the client's outbound buffer dies with the process. Ten leaves
room for the close that delivers it.

**The deadline is not consulted through the stop signal, and must not be.** That per-event await is
the *work*, not a wait between attempts, and SPEC-038 FR-001 AC-4a's rule is that a shutdown
shortens a wait and never skips work — the rule `KafkaSink._flush_bounded` states in the imperative
after a revision cutting its flush to zero delivered 0 of 11 events. `NATSSink` has no
inter-attempt wait, so it satisfies SPEC-027's "interruptible" half vacuously and correctly, as
SPEC-041 FR-004 AC-2 already records for `KafkaSink`. **`NATSSink` therefore keeps having no
`log_foundry_stop_signal` attribute** — `_lifecycle.offer_stop_signal` probes by `hasattr`, so that
absence is the opt-out, and adding one to make AC-5's test realistic would change what the worker
hands the sink.

**This diverges from SPEC-041 FR-004 AC-3**, which says that where a client's retry does not
satisfy SPEC-027, "bounded retry is added through `sinks/_retry`, not a second mechanism". A
deadline over work in progress is not a retry and has no inter-attempt wait for `_retry.wait` to
shorten, so that criterion does not reach this case. The nearest precedent,
`GooglePubSubSink._await_overflow`, *does* use `_retry.wait` and re-reads the stop signal — and the
two sinks are right to diverge: a Pub/Sub future has already been published and waiting on it is a
wait, while a JetStream `publish()` sends *and* awaits, so cutting it short skips work.

#### Acceptance Criteria:

- [ ] AC-1: `NATSSink(..., publish_timeout=T, jetstream=True)` returns from `emit` within
      `T + DEFAULT_ACK_TIMEOUT` for a batch whose per-event cost would otherwise exceed it.
      Asserted against a stalled server in the integration suite and a slow double in the unit
      suite.
- [ ] AC-2: The test proving AC-1 is run against the unbounded implementation and fails there.
- [ ] AC-3: Each JetStream publish receives `timeout=min(DEFAULT_ACK_TIMEOUT, remaining)`, asserted
      by capturing the timeout of the **first** and a **later** event in one batch — the first
      pins the ceiling, the later one pins that the budget is actually decreasing.
- [ ] AC-4: A batch of 200 events against a **healthy** server delivers all 200 with
      `losses().failed == 0`, so a bound that truncates a slow-but-succeeding exit backlog fails.
- [ ] AC-5: A set `log_foundry_stop_signal` does not shorten the batch, and `NATSSink` still
      declares no such attribute — asserted with `hasattr`, since that absence is what stops the
      worker offering one.
- [ ] AC-6: When the budget expires with **nothing** published, `emit` raises `SinkDeliveryError`
      naming the count not attempted, and moves **no** counter: `Worker._emit` retries the whole
      batch, so booking those events would report a loss that has not happened — the rule
      `KafkaSink.flush` already states for its own queued remainder.
- [ ] AC-7: When the budget expires with **something** published, `emit` returns normally and the
      unattempted remainder is counted into `losses().failed` — the worker will not retry, so that
      loss is real.
- [ ] AC-8: `publish_timeout` set on a `jetstream=False` sink still delivers a whole batch against
      a healthy server, and bounds one only between events.
- [ ] AC-9: A non-positive or non-finite `publish_timeout` falls back to `DEFAULT_PUBLISH_TIMEOUT`.

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

`drain_timeout` is forwarded because it does bind — `Client.drain` is
`wait_for(drain_is_done, drain_timeout)` — but it is **not** what bound this sink's `close()` in
either measured case. `drain()` then calls `self.flush()`, whose 10 s driver default is not a
`connect()` option and stays unreachable; that is the bound that actually fired, and FR-004 AC-4
records it rather than this FR claiming to have exposed it.

#### Acceptance Criteria:

- [ ] AC-1: Each of the four arguments is forwarded to `nats.connect`, asserted by capturing the
      kwargs it received.
- [ ] AC-2: With `max_reconnect_attempts=1, reconnect_time_wait=0.1`, construction against a port
      nothing is listening on fails in under 5 s rather than ~120 s.
- [ ] AC-3: Omitting all four passes **no** corresponding kwarg to `nats.connect` — asserted on the
      absence of the keys, not on their values, so today's call is reproduced exactly.
- [ ] AC-4: Passing any of the four together with `client=` raises `ValueError`; an injected client
      is already connected and the argument can have no effect (SPEC-043's rule that an argument no
      backend can consume is an error, not an ignore).

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

What is missing is narrower than "a bound": this sink's *own* delivery path is already bounded and
reachable, by `flush_timeout` since SPEC-038 FR-006. What no caller can reach is `librdkafka`'s
configuration — how long it retains a message it is retrying, how large its local queue may grow —
because `__init__` accepts only `topic`, `producer`, `bootstrap_servers`, `key_field` and
`flush_timeout`. A new `producer_config` mapping is merged **beneath** the sink's own keys when the
sink constructs the producer, so a caller can set `message.timeout.ms`, `retries`,
`retry.backoff.ms` or `queue.buffering.max.messages` without giving up `bootstrap_servers=`.

The default stays at five minutes. Lowering it would drop messages that a process surviving a
two-minute broker outage delivers today, which is the durable-buffer role arch §8 gives this sink;
a caller whose deadline is shorter than their broker's worst outage is the one who should choose.

#### Acceptance Criteria:

- [ ] AC-1: `KafkaSink(topic, bootstrap_servers=…, producer_config={"message.timeout.ms": 1500})`
      builds a producer carrying that value, asserted by capturing the config the `Producer`
      constructor received.
- [ ] AC-2: `producer_config={"bootstrap.servers": "wrong:9092"}` does **not** win — the sink's own
      key does, asserted on the resulting value rather than on the key's presence.
- [ ] AC-3: `producer_config` passed together with `producer=` raises `ValueError`.
- [ ] AC-4: Omitting `producer_config` produces exactly today's config dict, asserted by equality
      against `{"bootstrap.servers": …}`, so no existing caller's producer changes.

### FR-004: The README and both docstrings state what is bounded, by whom, and to what

#### Description:

The claim SPEC-041 declined to narrow becomes true, and the statements this spec's measurements
found wrong are corrected in place per SPEC-021's rule — struck with the spec that closed them,
never deleted. Four sites, not three:

1. `NATSSink`'s docstring says a JetStream publish is "bounded by the driver's own timeout (5 s by
   default)". True per event and false per batch, which is the whole of FR-001.
2. The same docstring says `is_connected` "does not flip the instant the server dies", implying a
   narrow window. Measured, it was still `True` **40 s after the server was stopped** — a lower
   bound on the window, not the window — because `nats-py` notices through a 120 s ping interval
   or a failed write.
3. `README.md` groups `KafkaSink`, `NATSSink` and `GooglePubSubSink` as sinks that "add no retry
   loop and need none — each hands off locally and returns without waiting". True of Kafka and of
   a core NATS publish; false of a JetStream one, which waits for an ack.
4. `SPEC-041-sink-integration-verification.md` FR-004 itself, whose text says the NATS answer is
   "bounded, because it never waits". That FR already carries an in-place superseded blockquote for
   its Pub/Sub reversal, so the precedent for striking inside it sits in the same FR.

#### Acceptance Criteria:

- [ ] AC-1: `README.md`'s queue/stream paragraph states, for each of the two sinks, the bound that
      governs it, whose bound it is, and the parameter that reaches it.
- [ ] AC-2: Each of the four corrected claims is struck in place with SPEC-047 named.
- [ ] AC-3: Both class docstrings state the worst-case total delay using this spec's measured
      figures, and do **not** claim the 30 s `drain_timeout` as `close()`'s bound, since it fired in
      neither measured case.
- [ ] AC-4: `architecture.md` §12 records the three residuals — the `is_connected` window, a
      duplicate-safe JetStream retry, and the driver's 10 s flush inside `drain()` being
      unreachable — each naming what would close it, per §12's own criterion.
- [ ] AC-5: `poetry run pytest tests/test_sink_concurrency.py` stays green, since that module's
      lints assert literal strings in `KafkaSink`'s class docstring (`**no** transport lock`,
      `SPEC-028 FR-002`) that FR-004 rewrites around.

---

## Data Model

No new types. New constants and the constructor arguments that read them:

```python
# src/log_foundry/sinks/_retry.py — extracted from kafka._usable_timeout, which hard-codes
# its own default and so cannot be reused as-is (SPEC-047 FR-001 AC-9).
def usable_timeout(value: float, default: float) -> float: ...

# src/log_foundry/sinks/nats.py
DEFAULT_PUBLISH_TIMEOUT: float = 10.0   # bounds a whole emit(); see FR-001
DEFAULT_ACK_TIMEOUT: float = 5.0        # mirrors JetStreamContext's private default

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
provable: the forwarded kwargs are absent, not defaulted to the driver's own values. Every new
argument is keyword-only, so SPEC-034's public-surface freeze is untouched.

---

## API / Interface Contract

```python
# Bound a whole batch, not each event in it (FR-001).
NATSSink("logs", servers="nats://…", jetstream=True, publish_timeout=10.0)

# Fail fast at startup instead of blocking ~120 s on an unreachable server (FR-002).
NATSSink("logs", servers="nats://…", max_reconnect_attempts=3, reconnect_time_wait=0.5)

# Reach librdkafka's own retention bound (FR-003).
KafkaSink("logs", bootstrap_servers="…", producer_config={"message.timeout.ms": 30000})
```

## Configuration / Environment

No new environment variables. No new dependencies; both sinks stay behind their existing `kafka`
and `nats` extras and keep their lazy imports.

## File & Folder Structure

```
src/log_foundry/sinks/
├── _retry.py     # usable_timeout, extracted for FR-001 AC-9
├── nats.py       # FR-001, FR-002, FR-004
└── kafka.py      # FR-003, FR-004
tests/
├── test_sinks_nats.py                  # FR-001, FR-002 unit coverage
├── test_sinks_kafka.py                 # FR-003 unit coverage
├── test_sink_retry.py                  # usable_timeout
└── integration/
    ├── test_nats.py                    # FR-001 AC-1/AC-4, FR-002 AC-2 against a real server
    └── test_kafka.py                   # FR-003 against a real broker
README.md                               # FR-004
docs/architecture.md                    # FR-004 AC-4 (§12)
docs/specs/SPEC-041-sink-integration-verification.md   # FR-004 AC-2, site 4
```

## Implementation Phases

### Phase 1: Bound the NATS batch (FR-001)

- Extract `usable_timeout(value, default)` into `sinks/_retry.py`; `kafka._usable_timeout` becomes
  a call to it, so both sinks floor a timeout by one rule rather than two.
- Add `DEFAULT_PUBLISH_TIMEOUT`, `DEFAULT_ACK_TIMEOUT` and the `publish_timeout` argument.
- Give `_publish_all` one deadline for the batch: JetStream passes
  `timeout=min(DEFAULT_ACK_TIMEOUT, remaining)`, core checks the deadline between events. Count the
  unattempted remainder only on the returning path; name it in the error on the raising path.
- Unit tests with a slow client double, covering AC-3 through AC-9, plus the
  run-against-the-unbounded-implementation check (AC-2).
- Integration tests against a paused NATS container (AC-1) and a healthy one (AC-4).

### Phase 2: Reach the clients' bounds (FR-002, FR-003)

- Forward the four NATS client timeouts, omitting any left as `None`; `ValueError` alongside
  `client=`.
- Merge `producer_config` beneath the sink's own keys in `KafkaSink`; `ValueError` alongside
  `producer=`.
- Unit tests capturing the kwargs and the merged config; integration test for the fast constructor
  failure.

### Phase 3: Make the documented claims true (FR-004)

- Rewrite both class docstrings' bound statements from this spec's measurements, leaving
  `KafkaSink`'s lint-asserted strings intact.
- Correct the README's queue/stream paragraph and strike SPEC-041 FR-004's superseded sentence.
- Record the three residuals in `architecture.md` §12.

---

## Revision history

- **2026-09-01 — the "add a bounded retry" reading was rejected for `KafkaSink`, on measurement.**
  SPEC-041 FR-004 left this open as a decision rather than a documentation edit. Building the
  rejected alternative first: a `_retry`-based loop around `produce()` can only re-send messages
  `librdkafka` has already accepted and owns, duplicating downstream against SPEC-018's rule, and
  stacks its own backoff on top of a five-minute bound rather than shortening it. The gap is that
  the *client's* configuration is unreachable, not that a bound is absent, so FR-003 exposes it.
  Escalated and confirmed before the build began, since it departs from the instruction that both
  sinks "get a bounded retry".
- **2026-09-01 — the NATS finding is a per-batch unbound, not a missing retry.** SPEC-041 measured
  a *core* publish (0.00 s, fire-and-forget) and recorded JetStream's 5 s ack timeout as the bound.
  Both are true per event; neither bounds a batch, and the exit backlog arrives as one. Measured at
  25.01 s for five events against a stalled server.
- **2026-09-01 — `NATSSink.close()`'s bound is the driver's 10 s flush, not its 30 s
  `drain_timeout`.** Read from the driver's options first and then measured: against a stalled
  server `close()` raised `FlushTimeoutError` at 10.00 s, and against a stopped one returned in
  0.00 s with `ConnectionReconnectingError`. `Client.drain` is
  `wait_for(drain_is_done, drain_timeout)` followed by an unbounded-by-that-option `self.flush()`,
  so FR-002 forwards `drain_timeout` without claiming it bounds `close()`, and FR-004 AC-4 records
  the flush as a residual.
- **2026-09-01 — `DEFAULT_PUBLISH_TIMEOUT` was 30.0 in the first draft and is 10.0.** The spec
  review found 30.0 equal to `_lifecycle.DEFAULT_SHUTDOWN_TIMEOUT`, which this budget is spent
  inside: an `emit` consuming all of it expires `Worker.shutdown`'s join, and SPEC-027 FR-004
  leaves an expired shutdown's sink **open**, so the `close()` that would drain the client's
  outbound buffer never runs. The first draft also had no criterion for a large batch against a
  *healthy* server, so a hard truncating cap satisfied every one of its criteria (now AC-4).
