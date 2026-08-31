# Completed Spec — SPEC-041: Sink Integration Verification

## What was completed?

- **An integration job that executes the extras-backed sinks against real services**
  (`.github/workflows/integration.yml`, `tests/integration/`). Nine services in one
  `docker-compose.yml` used by both CI and a developer, every image digest-pinned. Deliberately
  **not** a required check; the FR states the revisit rule (four consecutive green weekly runs,
  then Andrew decides — a branch-protection change no agent can make).
- **The vacuity guard is a floor, not an exit code.** `pytest`'s exit 5 catches only a forgotten
  gate variable, while a fixture that skips on an unreachable service exits **0**.
  `pytest_sessionfinish` fails a run where any integration test skipped or where a module
  contributed nothing, scoped to this directory so an ordinary run is untouched.
- **AC-4's record is derived** (`tests/test_sink_integration_roster.py`): 23 modules reaching a
  real destination, 10 verified, 13 exempt with reasons. Population is the lazy-import modules
  **union** those importing *or defining* `HTTPSink`/`SocketTransport` — the obvious
  single-marker version silently omits `logstash`, AC-1's named minimum and FR-003's subject.
- **FR-002** — `PostgresSink` reconnects a broken **owned** connection at the top of each attempt,
  not in the retry branch (`max_retries=0` is reachable and would never recover). New
  `connect_timeout`, floored at libpq's minimum of 2. `close()`'s commit is now guarded.
- **FR-003** — `LogstashSink` HTTP mode defaults to `body_format="json_array"`.
- **FR-004** — Kafka and Pub/Sub verified bounded and unchanged, each docstring naming the setting
  that bounds it. `NATSSink` gained an `is_connected` guard so a sustained outage is reported.

## What changed from earlier specs?

- **`LogstashSink`'s HTTP wire form changed, and it is breaking for one configuration.** Anyone who
  set `additional_codecs => {"application/x-ndjson" => "json_lines"}` — the documented workaround
  for the old behaviour — must now pass `body_format="ndjson"`, because that setting *replaces*
  Logstash's default codec map rather than merging, making the two mutually exclusive.
  `body_format=` could not be passed at all before: SPEC-009 hardcoded it and then forwarded
  `**http_kwargs`, so it raised `TypeError: got multiple values`.
- **`NATSSink.emit` now raises when the client reports itself disconnected**, where it returned
  normally — SPEC-026 FR-001 applied to a sink that had escaped it.
- **`PostgresSink.__init__` gained `connect_timeout`**, which overrides any in the DSN. A double
  stubbing `psycopg.connect` must accept `**kwargs`.
- Five `SentrySink` tests now pin their backend instead of letting the environment pick it.

## Verification

Four gates green on both PRs (exit codes checked, not summary lines) plus the integration suite
against nine live services. Every read-only audit finding was **reproduced before being fixed** and
each fix mutation-tested by re-planting the defect: K3 lost three batches after one
`pg_terminate_backend`; K10's NDJSON arrived as one event with every field swallowed into
`message`; `psycopg.connect()` blocked **75.01 s** against a blackholed host; NATS delivered **one
of six** events with every counter at zero.

The job justified itself on its first CI run by failing three Logstash tests with
`ConnectionResetError` — the `http` input binds its port before the pipeline serves, so the
TCP-connect readiness probe went green and the first POST was reset. No local run could reproduce
it, because a laptop's container had been up for minutes.
