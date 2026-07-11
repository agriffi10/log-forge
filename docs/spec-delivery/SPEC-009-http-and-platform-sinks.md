# Completed Spec — SPEC-009: HTTP and Log-Platform Sinks

## What was completed?

A dependency-free `HTTPSink` core plus ten platform sinks that direct-ship logs to common
destinations. These are **terminal, best-effort** sinks (arch §9.1) — they couple delivery to the
destination's availability and are *not* the durable-buffer path (that is SQS/SPEC-010). All operate
on already-built event dicts (arch §8) and are `isinstance`-checkable.

- **`sinks.http`** (new) — `HTTPSink(url, *, method, headers, auth, body_format, timeout, gzip,
  max_retries)`: stdlib `urllib`, `ndjson`/`json_array` bodies, bearer/basic auth, optional gzip,
  bounded `429`/`5xx` retry honoring `Retry-After`; injectable `opener` seam for tests; exports a
  shared `merge_headers` helper (FR-001, FR-002, FR-012).
- **`sinks.elasticsearch`** (new) — `ElasticsearchSink` (`_bulk` action+source NDJSON + `items[]`
  error counting) and `OpenSearchSink` (subclass, identical protocol) (FR-003).
- **`sinks.loki`** (new) — labelled `streams` + nanosecond timestamps (FR-004).
- **`sinks.logstash`** (new) — HTTP mode (composes `HTTPSink`) or raw TCP/UDP socket mode (FR-005).
- **`sinks.syslog`** (new) — RFC 5424 over UDP / octet-counted TCP (FR-006).
- **`sinks.{datadog,splunk,newrelic,honeycomb}`** (new) — SaaS intakes, each an `HTTPSink`
  specialization with its auth header + body envelope (FR-007..FR-010).
- **`sinks.sentry`** (new) — lazy `sentry-sdk` (optional `sentry` extra) with an HTTP-envelope
  fallback and `min_level` gating (FR-011).
- **`sinks._time`** / **`sinks._socket`** (new) — ISO→epoch/nanos helper and a TCP/UDP socket
  transport (bounded reconnect retry) shared by the above.

**Deviations from the Draft:** none of substance — the `HTTPSink.opener` and `SentrySink.sdk`
arguments are private test seams the FRs implicitly require (inject-a-fake-transport). Platform
constructors accept `**http_kwargs` passthrough for gzip/timeout/retry tuning.

## What changed from earlier specs?

Purely additive to `src` (no earlier module edited). Added the optional **`sentry`** extra
(`sentry-sdk>=2.0`) to `pyproject.toml` + `poetry.lock` and CLAUDE.md's Tech Stack.

## Verification

Local gates green — `ruff` clean, `mypy --strict` clean (33 src files; lazy `sentry_sdk` import
carries a scoped `type: ignore[import-not-found]`), `pytest` **191 passed**. 55 new tests inject a
fake `urlopen` opener or fake socket (no network) covering body formats, headers/auth, gzip,
retry/abandon, `_bulk` framing + partial-failure parsing, Loki payload/ns-timestamps, Logstash HTTP
+ socket modes, RFC 5424 UDP/TCP framing, each SaaS envelope, and Sentry's SDK-present vs SDK-absent
branches.
