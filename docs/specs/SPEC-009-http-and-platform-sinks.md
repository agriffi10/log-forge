# Spec: HTTP and Log-Platform Sinks

**ID:** SPEC-009
**Status:** Completed
**Last Updated:** 2026-07-11
**Depends On:** SPEC-001, SPEC-005

## Overview

Some teams ship logs straight to a log platform or HTTP endpoint rather than through a durable queue.
This spec adds a dependency-free `HTTPSink` core (stdlib `urllib`) that POSTs a batch as NDJSON or a
JSON array, plus a family of platform sinks that specialize the endpoint, authentication, and body
shape for the common destinations: Elasticsearch/OpenSearch (`_bulk`), Grafana Loki, Logstash, RFC
5424 Syslog, Datadog, Splunk HEC, New Relic, Honeycomb, and Sentry. These are **terminal, direct-ship
sinks**: per arch §9.1 they couple application delivery to the destination's availability, so they are
offered for direct-ship use cases and are explicitly *not* the headline durable-buffer path (that is
SQS / SPEC-010). Each reuses the SPEC-005 pattern of bounded partial-failure retry, and every sink
still operates purely on already-built event dicts (arch §8).

## Scope

### In Scope

- `HTTPSink(url, *, method="POST", headers=None, auth=None, body_format="ndjson", timeout=5.0,
  gzip=False)` — dependency-free POST via `urllib.request`; the base other HTTP sinks build on.
- `ElasticsearchSink` / `OpenSearchSink` — `_bulk` NDJSON (action+source line pairs) with per-item
  error parsing.
- `LokiSink` — Loki push JSON (`streams` with labels and `[ns_ts, line]` values).
- `LogstashSink` — JSON lines over HTTP or a raw TCP/UDP socket.
- `SyslogSink` — RFC 5424 frames over UDP or TCP (stdlib `socket`).
- `DatadogSink`, `SplunkHECSink`, `NewRelicSink`, `HoneycombSink` — the SaaS logs-intake HTTP
  endpoints, each with its auth header and body envelope.
- `SentrySink` — ship error-level events to Sentry via the optional `sentry-sdk` extra (lazy import),
  falling back to an envelope HTTP POST when the SDK is absent.
- Shared per-request bounded retry with backoff on `429`/`5xx`, honoring `Retry-After` where present;
  a request failing past the bound is counted and logged, never crashing the worker.
- Optional gzip compression with the correct `Content-Encoding`/`Content-Type` headers.

### Out of Scope

- Non-logging platform features (APM, tracing, metrics ingestion) — logs only.
- A guaranteed-delivery/local-spool layer beyond the worker's retry and each sink's bounded in-request
  retry — these are best-effort direct ships; use a queue sink (SQS / SPEC-010) when durability
  matters (arch §9.1).
- Bringing in `requests`/`httpx` as a runtime dependency — the core HTTP path stays on stdlib
  `urllib`; a third-party HTTP client is not required.
- TLS client-cert / mTLS configuration beyond what `urllib`'s default SSL context offers.

---

## Functional Requirements

### FR-001: HTTPSink core

#### Description:

A dependency-free HTTP sink that POSTs a batch to a configurable endpoint — the base for every other
sink here.

#### Acceptance Criteria:

- [ ] `HTTPSink(url)` POSTs the batch to `url`; `body_format="ndjson"` (default) sends one
      `json.dumps(event)` per line, `body_format="json_array"` sends a single JSON array.
- [ ] Custom `headers` and an `auth` (bearer token / basic tuple) are applied to the request;
      `timeout` bounds the call.
- [ ] A `2xx` response is success; `429` and `5xx` trigger bounded retry with exponential backoff
      (honoring `Retry-After` when present), after which the request is counted (`failed`) and logged.
- [ ] A test can inject a fake transport (URL opener) and assert on method, URL, headers, and body
      without any network access.
- [ ] `isinstance(HTTPSink(url), Sink)` is `True`; dependency-free (`urllib` only).

### FR-002: Compression and content headers

#### Description:

Optionally gzip the body and set correct content headers.

#### Acceptance Criteria:

- [ ] `gzip=True` compresses the request body and sets `Content-Encoding: gzip`.
- [ ] `Content-Type` is set to `application/x-ndjson` for `ndjson` and `application/json` for
      `json_array` unless the caller overrides it via `headers`.

### FR-003: ElasticsearchSink / OpenSearchSink

#### Description:

Index events via the `_bulk` API.

#### Acceptance Criteria:

- [ ] Each event is serialized as a `_bulk` action line (`{"index":{"_index": target}}`) followed by
      the event source line; the payload is newline-delimited and newline-terminated.
- [ ] The sink POSTs to the configured `_bulk` endpoint with `Content-Type: application/x-ndjson` and
      any configured auth.
- [ ] The bulk response `items` array is inspected; items with an error are counted and logged, and a
      partial failure does not discard the successfully-indexed items.
- [ ] `OpenSearchSink` reuses the same bulk logic (endpoint/auth differences only).

### FR-004: LokiSink

#### Description:

Push events to Grafana Loki.

#### Acceptance Criteria:

- [ ] The sink builds a Loki push payload: one or more `streams`, each with a labels object (from a
      configurable set of event keys, e.g. `service`/`env`/`level`) and `values` of
      `[<nanosecond_timestamp_str>, <log_line>]`.
- [ ] It POSTs the payload to `/loki/api/v1/push` with `Content-Type: application/json`.
- [ ] Timestamps are rendered as nanosecond epoch strings as Loki requires, derived by parsing the
      event's ISO-8601 `timestamp` (SPEC-001), falling back to emit-time `now` if it is
      absent/unparseable.

### FR-005: LogstashSink

#### Description:

Send JSON lines to Logstash over HTTP or a raw socket.

#### Acceptance Criteria:

- [ ] `LogstashSink(url=...)` sends the batch as JSON lines over HTTP (reusing the HTTP core).
- [ ] `LogstashSink(host=..., port=..., transport="tcp"|"udp")` sends one `json.dumps(event)+"\n"` per
      event over a raw TCP or UDP socket.
- [ ] `close()` closes any open socket; a connection error is retried (bounded) then counted/logged.

### FR-006: SyslogSink

#### Description:

Emit RFC 5424 syslog frames over UDP or TCP.

#### Acceptance Criteria:

- [ ] Each event is formatted as an RFC 5424 message (`<PRI>1 TIMESTAMP HOST APP PROCID MSGID ...`),
      with `PRI` derived from a configurable facility and a severity mapped from the event `level`.
- [ ] `transport="udp"` sends a datagram per event; `transport="tcp"` uses octet-counted framing.
- [ ] The sink is dependency-free (stdlib `socket`); `close()` closes the socket.

### FR-007: DatadogSink

#### Description:

Ship to Datadog's logs intake.

#### Acceptance Criteria:

- [ ] The sink POSTs the batch (JSON array) to the Datadog logs intake URL with a `DD-API-KEY` header
      from the configured API key.
- [ ] `ddsource`, `service`, and `ddtags` are populated from configuration / event fields.
- [ ] The configurable site/region (e.g. `datadoghq.com` vs `datadoghq.eu`) determines the intake host.

### FR-008: SplunkHECSink

#### Description:

Ship to Splunk HTTP Event Collector.

#### Acceptance Criteria:

- [ ] Each event is wrapped in a HEC envelope (`{"event": <event>, "time": <epoch>, "host": ...,
      "source": ...}`) and sent to the HEC endpoint; `time` (epoch seconds) is derived by parsing the
      event's ISO-8601 `timestamp`, falling back to emit-time `now` if absent/unparseable.
- [ ] The request carries `Authorization: Splunk <token>` from the configured HEC token.
- [ ] Multiple events per request use HEC's concatenated-JSON-objects body format.

### FR-009: NewRelicSink

#### Description:

Ship to the New Relic Log API.

#### Acceptance Criteria:

- [ ] The sink POSTs the batch as a JSON array to the New Relic Log API endpoint with an `Api-Key`
      header from configuration.
- [ ] The configurable region (US/EU) selects the endpoint host.

### FR-010: HoneycombSink

#### Description:

Ship to Honeycomb's batch events API.

#### Acceptance Criteria:

- [ ] The sink POSTs the batch to `/1/batch/<dataset>` with an `X-Honeycomb-Team` header from the
      configured API key and the configured dataset in the path.
- [ ] The batch body uses Honeycomb's `[{ "data": <event> }, ...]` shape.

### FR-011: SentrySink

#### Description:

Route error-level events to Sentry, using the SDK when available.

#### Acceptance Criteria:

- [ ] When `sentry-sdk` is installed, `SentrySink` captures each qualifying event via the SDK; the
      `import sentry_sdk` happens lazily inside the sink (never at module top), so importing the module
      does not require the extra.
- [ ] When the SDK is absent, the sink falls back to POSTing a Sentry envelope over HTTP to the
      configured DSN's ingest URL.
- [ ] By default only events at/above `ERROR` are sent (configurable); non-qualifying events are
      skipped, not sent.

### FR-012: Shared failure and oversized handling

#### Description:

Every sink here handles request failures uniformly and never crashes the worker.

#### Acceptance Criteria:

- [ ] Each HTTP/socket sink retries a failed send with bounded attempts and backoff, then counts
      (`failed`) and logs the abandoned send rather than raising out of `emit` uncontrolled.
- [ ] A batch that exceeds a destination's documented body-size limit is split into smaller requests
      where the API allows batching; a single event too large to ever fit is dropped with a counted
      warning (matching the SPEC-005 `SQSSink` policy).
- [ ] `close()` releases any held resource (socket/opener) and is idempotent.

---

## Data Model

```
# src/log_forge/sinks/http.py — the shared base
HTTPSink {
  url: str
  method: str = "POST"
  headers: dict[str, str]
  auth: object | None                  # bearer str | (user, pass) tuple
  body_format: str = "ndjson"          # "ndjson" | "json_array"
  timeout: float = 5.0
  gzip: bool = False
  max_retries: int = 3
  failed: int                          # requests abandoned past the retry bound
  dropped_oversized: int
}
```

Platform sinks are thin specializations that set endpoint, auth header, and body-envelope logic on top
of the `HTTPSink` core (or, for `SyslogSink`/socket `LogstashSink`, a raw `socket`). Events are the
SPEC-001 `LogEvent` dicts.

**Timestamp note.** A `LogEvent`'s `timestamp` is an **ISO-8601 string** (`2026-07-10T12:00:00.123Z`),
not an epoch. Sinks that require epoch/nanosecond time (`LokiSink`, `SplunkHECSink`) derive it by
parsing that ISO string, falling back to emit-time `now` if it is absent/unparseable — they do not
assume an epoch field on the event.

---

## API / Interface Contract

```python
# sinks/http.py
class HTTPSink:
    def __init__(self, url, *, method="POST", headers=None, auth=None,
                 body_format="ndjson", timeout=5.0, gzip=False, max_retries=3) -> None: ...
    def emit(self, batch: list[dict]) -> None: ...
    def close(self) -> None: ...

# Representative platform constructors
class ElasticsearchSink:   # OpenSearchSink mirrors it
    def __init__(self, url, *, index, auth=None, **http_kwargs) -> None: ...
class LokiSink:
    def __init__(self, url, *, labels=("service", "env", "level"), **http_kwargs) -> None: ...
class SyslogSink:
    def __init__(self, host, port=514, *, transport="udp", facility="user", app_name="log-forge") -> None: ...
class DatadogSink:
    def __init__(self, api_key, *, site="datadoghq.com", service=None, ddtags=None) -> None: ...
class SplunkHECSink:
    def __init__(self, url, token, *, host=None, source="log-forge") -> None: ...
class SentrySink:
    def __init__(self, dsn=None, *, min_level="ERROR") -> None: ...

# Usage
import log_forge
from log_forge.sinks.datadog import DatadogSink
log_forge.configure(sink=DatadogSink(api_key="...", service="payments", site="datadoghq.eu"))
```

## Configuration / Environment

- The core `HTTPSink` and most platform sinks are dependency-free (stdlib `urllib`/`socket`).
- `SentrySink` uses the optional **`sentry`** extra (`sentry-sdk`), imported lazily; without it the
  sink uses the HTTP envelope fallback. The new extra is added to `pyproject.toml` and noted in
  CLAUDE.md's Tech Stack at implementation time.
- API keys / tokens / DSNs are passed as constructor arguments (callers may source them from their own
  env); log-forge adds no credential-resolution logic of its own.

## File & Folder Structure

```
src/log_forge/sinks/
├── http.py            # HTTPSink core (urllib, gzip, bounded retry)      (new)
├── elasticsearch.py   # ElasticsearchSink + OpenSearchSink (_bulk)       (new)
├── loki.py            # LokiSink (push API)                              (new)
├── logstash.py        # LogstashSink (HTTP or TCP/UDP socket)            (new)
├── syslog.py          # SyslogSink (RFC 5424 over UDP/TCP)               (new)
├── datadog.py         # DatadogSink                                      (new)
├── splunk.py          # SplunkHECSink                                    (new)
├── newrelic.py        # NewRelicSink                                     (new)
├── honeycomb.py       # HoneycombSink                                    (new)
└── sentry.py          # SentrySink (optional sentry-sdk, HTTP fallback)  (new)
tests/
├── test_sinks_http.py           # core: format, headers, gzip, retry (fake opener) (new)
├── test_sinks_elasticsearch.py  # _bulk framing + items[] error parsing            (new)
├── test_sinks_loki.py           # push payload + ns timestamps                     (new)
├── test_sinks_logstash.py       # HTTP + raw socket paths                          (new)
├── test_sinks_syslog.py         # RFC 5424 framing, UDP/TCP                         (new)
└── test_sinks_saas.py           # Datadog/Splunk/NewRelic/Honeycomb/Sentry envelopes (new)
```

## Implementation Phases

### Phase 1: HTTPSink core

- Implement the `urllib`-based `HTTPSink` with `ndjson`/`json_array` bodies, headers/auth, optional
  gzip, and the shared bounded-retry/oversized handling (FR-001, FR-002, FR-012).
- Test with an injected fake opener: body format, headers, gzip, `429`/`5xx` retry, and abandon-count.

### Phase 2: Self-hosted platforms — Elasticsearch/OpenSearch, Loki, Logstash, Syslog

- Implement the `_bulk` sink (with items[] error parsing), the Loki push sink, the Logstash HTTP/socket
  sink, and the RFC 5424 Syslog sink (FR-003, FR-004, FR-005, FR-006).
- Test bulk framing + partial-failure parsing, Loki payload/timestamps, Logstash transports, and
  syslog frames.

### Phase 3: SaaS intakes — Datadog, Splunk HEC, New Relic, Honeycomb, Sentry

- Implement the five SaaS sinks as `HTTPSink` specializations, plus `SentrySink`'s lazy-SDK path with
  HTTP-envelope fallback and level gating (FR-007, FR-008, FR-009, FR-010, FR-011).
- Test each envelope/auth header and Sentry's SDK-present vs SDK-absent branches (fake transport / fake
  SDK).
