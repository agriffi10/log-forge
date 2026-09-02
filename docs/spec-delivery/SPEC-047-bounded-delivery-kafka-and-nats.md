# Completed Spec — SPEC-047: Bounded Delivery for `KafkaSink` and `NATSSink`

## What was completed?

- **`NATSSink.emit` is bounded as a whole batch, not per event** (FR-001). New
  `publish_timeout` (`DEFAULT_PUBLISH_TIMEOUT = 10.0`) and `DEFAULT_ACK_TIMEOUT = 5.0`. JetStream
  gets `min(DEFAULT_ACK_TIMEOUT, remaining)`; core `publish()` takes no timeout at all and is
  bounded between events. A **raised** publish is counted as before; an event **never attempted**
  is counted only when `emit` returns, because a raise sends the batch back to `Worker._emit`'s
  retry.
- **The drivers' own bounds are reachable** (FR-002, FR-003). `NATSSink` forwards
  `connect_timeout` / `max_reconnect_attempts` / `reconnect_time_wait` / `drain_timeout` to
  `nats.connect`, omitting any left `None`; `KafkaSink` takes `producer_config`, merged **beneath**
  its own keys. Either alongside an injected `client=` / `producer=` is a `ValueError`
  (SPEC-043's rule). `publish_timeout` is deliberately outside that set — it is the sink's own
  bound over its own loop.
- **`sinks/_retry.usable_timeout(value, default)`**, extracted from `kafka._usable_timeout`, which
  hard-coded its own fallback and would have given NATS a 10 s floor drawn from Kafka's constant.
- **The docs say what is bounded, by whom, and to what** (FR-004): `README.md`'s queue/stream
  paragraph and both constructor rows, both class docstrings, and SPEC-041 FR-004's own sentence,
  each struck in place per SPEC-021's rule.

**Deliberate deviations.** No retry loop was added to either sink — the spec argues *for*
SPEC-041's measured conclusion rather than reversing it, and the Kafka half was escalated and
confirmed before the build. `message.timeout.ms`'s five-minute default is unchanged: lowering it
would drop messages a process surviving a two-minute outage delivers today (arch §8).

## What changed from earlier specs?

- **SPEC-041 FR-004's "bounded, because it never waits" is superseded** — true of a core publish,
  false of a JetStream one.
- **SPEC-041 FR-004 AC-3** ("bounded retry is added through `sinks/_retry`, not a second
  mechanism") is diverged from, argued in FR-001: a deadline over work in progress is not a retry
  and has no inter-attempt wait for `_retry.wait` to shorten. `NATSSink` still declares no
  `log_foundry_stop_signal`, so a shutdown cannot skip its work.
- `kafka._usable_timeout` keeps its name (`postgres.py`'s prose cites it) and now delegates.
- Five residuals recorded in `architecture.md` §12, one constraint in §13.

## Verification

Four gates green by exit code, plus the integration suite against real containers — six NATS tests,
including a batch bounded against a server stalled with `docker compose pause` and a 200-event
delivery that proves `timeout=` is the real driver's keyword (no unit test can, since
`_publish_all` catches every per-event exception). A new `LOG_FOUNDRY_EXTRAS=1` check pins our ack
ceiling against the driver's signature and **fails rather than skips** when the extra is absent.

Every claim of a *number* here was measured, not assumed: 25.01 s for five JetStream events
stalled, 300.18 s for librdkafka's default delivery callback, 120.17 s for the NATS constructor
against a dead server, 0.0001 s for `produce()`, 10.00 s for `close()` stalled, and 8.01 s against
an 8.00 s prediction for the exit-drain arithmetic.

Ten mutants were planted across the two phases and all ten reddened. Four review gates ran; the
diff frames found a regression this work introduced (`json.dumps` hoisted out of the `try`, costing
per-event isolation: 2 of 5 delivered with 6 duplicates) and a vacuous criterion (AC-4's double had
no time cost, so `DEFAULT_PUBLISH_TIMEOUT` could be mutated to `0.02` with the whole suite green).
