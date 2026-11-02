---
type: Decision
title: "OpenTelemetry-shaped output, plural transports, send everything for now"
description: "what goes outward uses the OpenTelemetry data model and semantic-convention names, and OTLP is one more sink, never the only way out (Astra has no OTLP ingestion). **Do NOT build:** an OTel SDK/API dependency in core, in-library metrics, or a native field rename in 1.x. The mapping stays private (`_otel`) until a second consumer proves it. Sampling stays deferred and **unbuilt** — no `should_send` in code. (No spec — decided by Andrew on 2026-10-05; SPEC-056 is its first slice. arch §10, §13)"
---

> **Reversed 2026-10-05.** This entry opened "Logs-only, send everything for now — no metrics/OTel-native traces". The first half is reversed as a *direction*; the sampling half is unchanged and still true. Every other site that stated the old claim carries its own marker: `architecture.md` §1, §13 and §14, and `CLAUDE.md`'s Out of Scope line (edited in place, being byte-budgeted).

**OpenTelemetry-shaped output, plural transports, send everything for now** — Andrew decided on 2026-10-05, with no spec behind it, that the library follows industry norms and serves several outputs at once: any OTLP backend, ClickHouse, Astra (formerly KalDB) and trace views. Four parts. **(1)** What goes outward standardizes on the OpenTelemetry data model and semantic-convention names, and the transports stay plural: OTLP becomes one more sink. **(2)** OTLP is produced natively, with no OpenTelemetry SDK or API at runtime. **(3)** The "no OTel-native traces" non-goal is reversed as a direction: OTLP spans built from `span.end` are a planned item of the arc recorded in `docs/specs/ARCS.md`, and are not part of SPEC-056. In-library metrics stay out — they are derived downstream, as the OpenTelemetry Collector's `spanmetrics` connector derives them. The native JSON field names do not change in 1.x, because consumers' queries depend on them; a rename to semantic-convention names is a 2.0 question. **(4)** The mapping to the OTel shape is private (`src/log_foundry/_otel.py`), because anything public is frozen until 2.0, and it is made public only once a second consumer has proven the shape.

**The first slice's product calls** (SPEC-056, accepted by Andrew on 2026-10-05). OTLP/HTTP with **JSON encoding only** — no protobuf and no gRPC, whose generated classes would be a runtime dependency of the core. Configuration is by **explicit constructor arguments only**: no `OTEL_*` environment variable is read. `exception.type` is **module-qualified** (`module.Type`), except that a `builtins` exception uses its bare name, so an `OSError` reads `OSError`, as the semantic-convention examples do. The semantic-conventions version is **pinned at 1.44.0** (schema URL `https://opentelemetry.io/schemas/1.44.0`) and moved only deliberately, since the attribute names a consumer's queries rest on come from it.

**Why the transports stay plural.** OTLP is what Datadog, New Relic, Honeycomb, Grafana, Splunk and Elastic ingest, and ClickHouse's ClickStack and the Collector's ClickHouse exporter use a standard `otel_logs` table. Astra (`github.com/slackhq/astra`) has no OTLP ingestion at all: it takes an OpenSearch-compatible `_bulk` API (`bulkIngestApi/opensearch/BulkApiRequestParser.java`) and stores every document as a span. An OTLP-only design would therefore put Astra out of reach and force a Collector in front of every backend. Those were the reasons on the day, from sources read on 2026-10-05; they are not a standing claim about any product.

**What was rejected, and why.** *Building on the OpenTelemetry SDK:* it breaks the zero-runtime-dependency core (`docs/process/ground-rules.md`) and makes the worker and sink pipeline redundant, because the SDK brings batching and exporters of its own — a redesign, effectively 2.0. *OTLP as the only transport:* the Astra reason above. *Renaming the native fields now:* the query reason in (3). *A public mapping now:* a public name is frozen until 2.0 (*The frozen surface is the import path*), and one consumer cannot prove a shape.

**Do NOT build** an OpenTelemetry SDK or API as a dependency of the core; in-library metrics; a rename of any native field in 1.x; or a public mapping module before a second consumer exists.

**Sampling is still deferred and unbuilt** — no `should_send` exists in code, and the per-span flush makes the pipeline span-outcome-ready, *not* tail-sampling-ready. The OTLP `flags` SPEC-056 sets are the W3C trace flags with the sampled bit, because `format_traceparent` always sets it: the library records every span. OTLP traces are last in the arc because they carry the most design risk — a `span.end` dropped under queue backpressure orphans child spans in a trace view. (No spec — decided by Andrew on 2026-10-05; SPEC-056 is its first slice. arch §10, §13)

## History

### 2026-10-05

- **Reversed** by the maintainer, no spec behind it: the first title read "Logs-only, send everything for now", and the logs-only half is reversed as a direction. The sampling half stands.

### 2026-06-24

- **Settled.**
