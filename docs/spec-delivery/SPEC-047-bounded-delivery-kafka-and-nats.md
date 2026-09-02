# Completed Spec — SPEC-047: Bounded Delivery for `KafkaSink` and `NATSSink`

## What was completed?

- **`NATSSink.emit` is bounded as a whole batch, not per event** (FR-001). New `publish_timeout`
  (`DEFAULT_PUBLISH_TIMEOUT = 10.0`) and `DEFAULT_ACK_TIMEOUT = 5.0`. JetStream gets
  `min(DEFAULT_ACK_TIMEOUT, remaining)`; core `publish()` takes no timeout and is bounded between
  events. A **raised** publish is counted as before; an event **never attempted** is counted only
  when `emit` returns, since a raise sends the batch back to `Worker._emit`'s retry.
- **The drivers' own bounds are reachable** (FR-002, FR-003). `NATSSink` forwards
  `connect_timeout` / `max_reconnect_attempts` / `reconnect_time_wait` / `drain_timeout` to
  `nats.connect`, omitting any left `None`; `KafkaSink` takes `producer_config`, merged **beneath**
  its own keys. Either alongside an injected `client=` / `producer=` is a `ValueError`
  (SPEC-043's rule); `publish_timeout` is deliberately outside that set, being the sink's own bound.
- **`sinks/_retry.usable_timeout(value, default)`**, extracted from `kafka._usable_timeout`, whose
  hard-coded fallback would have given NATS a 10 s floor drawn from Kafka's constant.
- **The docs say what is bounded, by whom, and to what** (FR-004): `README.md`'s queue/stream
  paragraph and both constructor rows, both class docstrings, and SPEC-041 FR-004's own sentence,
  each struck in place per SPEC-021's rule.

**Deliberate deviations.** No retry loop in either sink — the spec argues *for* SPEC-041's measured
conclusion rather than reversing it, and the Kafka half was escalated and confirmed before the
build. `message.timeout.ms`'s five-minute default stands: lowering it would drop messages a process
surviving a two-minute outage delivers today (arch §8).

## What changed from earlier specs?

- **SPEC-041 FR-004's "bounded, because it never waits" is superseded** — true of a core publish,
  false of a JetStream one.
- **SPEC-041 FR-004 AC-3** ("bounded retry through `sinks/_retry`, not a second mechanism") is
  diverged from, argued in FR-001: a deadline over work in progress is not a retry and has no
  inter-attempt wait to shorten. `NATSSink` still declares no `log_foundry_stop_signal`.
- `kafka._usable_timeout` keeps its name (`postgres.py`'s prose cites it) and now delegates.
- Five residuals in `architecture.md` §12, one constraint in §13.

## Verification

Four gates green by exit code, plus the integration suite against real containers.

**Two claims could only be settled against a real driver, and both are tested there.** The
200-event JetStream delivery proves `timeout=` is the keyword `JetStreamContext.publish` takes
(FR-001); the fast-constructor test proves the four connect kwargs *bind* rather than merely being
passed (FR-002 AC-2). The unit tests assert against fakes that accept any keyword name, and
`_publish_all` catches every per-event exception, so a wrong name would surface as a counted
failure rather than a crash — the SPEC-043 shape. `LOG_FOUNDRY_EXTRAS=1` checks pin the ack ceiling
and all four kwarg names against the drivers' signatures, and **fail rather than skip** when the
extra is absent.

Every number claimed here was measured: 25.01 s for five stalled JetStream events, 300.18 s for
librdkafka's default delivery callback, 120.17 s for the NATS constructor against a dead server,
0.0001 s for `produce()`, 10.00 s for a stalled `close()`, 8.01 s against an 8.00 s prediction for
the exit-drain arithmetic. Seventeen mutants were planted; all seventeen reddened.

**Six reviewers rather than four** — one on the spec, one on the plan, then two on *each* PR's
diff, because that gate is per branch and this shipped in two PRs. Said out loud since the grouping
was the implementer's call. The second pair earned itself: Phase 1's frames found a regression this
work introduced (`json.dumps` hoisted out of the `try`: 2 of 5 delivered, 6 duplicates) and a
criterion that could not fail (`DEFAULT_PUBLISH_TIMEOUT` mutable to `0.02`, whole suite green).

**Not done, deliberately:** no `## Specs` paragraph in `CLAUDE.md`. Prior completions added one,
but that section is the spec-ordered changelog shape `process.md` §5 names as an unpaid obligation,
and its rule is to *extend the clause for its area* instead — which the Key Decisions edit does.
