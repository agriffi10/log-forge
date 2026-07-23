# Component Inventory

Reusable modules/services/components/hooks already built, so a new spec reuses instead of rebuilding.
**One line per item** — name, path, one-line purpose. The code is the source of truth; this is just the
index to find it. Add a row as part of the completion ritual when a spec ships something reusable.

| Name | Path | Purpose |
|------|------|---------|
| `configure` / `get_config` / `_ensure_sink` | `src/log_foundry/config.py` | Process-wide config singleton; lazy `StdoutSink` default. |
| id generators | `src/log_foundry/ids.py` | `new_trace_id` / `new_span_id` / `new_log_id` (W3C-compatible). |
| `Span` + event builders | `src/log_foundry/model.py` | `Span` dataclass; `build_event` / `start_event` / `end_event` — the arch §6 JSON schema + precedence merge. |
| context stack + baggage | `src/log_foundry/context.py` | `contextvars` current-span stack and baggage (`push/pop/current`, `get/set_baggage`). |
| `Sink` protocol | `src/log_foundry/sinks/base.py` | The `emit`/`close` output interface sinks implement. |
| `StdoutSink` | `src/log_foundry/sinks/stdout.py` | Zero-dependency JSON-lines sink (default). |
| `SQSSink` | `src/log_foundry/sinks/sqs.py` | SQS sink (optional `aws` extra): lazy boto3, count/byte chunking, Failed-list retry, oversized drop. FIFO queues supported — `MessageGroupId` defaults to the event's `trace_id`, configurable; sender faults are not retried. |
| `CallbackSink` | `src/log_foundry/sinks/callback.py` | Adapt any callable into a `Sink` (+ optional `on_close`); the escape hatch for unsupported destinations. |
| `MultiSink` | `src/log_foundry/sinks/multi.py` | Fan one batch out to several sinks; per-child failure isolation (`failed`), close-all. |
| `FilteringSink` | `src/log_foundry/sinks/filtering.py` | Forward only events passing a predicate and/or `min_level`; fail-open on unknown level, no empty-batch emit. |
| `TransformSink` | `src/log_foundry/sinks/transform.py` | Per-event reshape/redact before forwarding; `None` drops; never mutates the caller's batch. |
| `LoggingSink` | `src/log_foundry/sinks/logging_sink.py` | Bridge events into stdlib `logging` — one `LogRecord`/event through a configurable logger; level-mapped, verbatim message, reserved-attr-safe fields (flat + nested). |
| `FileSink` / `RotatingFileSink` | `src/log_foundry/sinks/file.py` | Append NDJSON to a file; rotating variant adds size/time triggers + numbered backup retention (rotate-before-exceed, no event lost). |
| `SQLiteSink` | `src/log_foundry/sinks/sqlite.py` | Batch-insert events (full JSON + projected columns) into an embedded SQLite DB; injectable connection, `create_table` opt-out, owned-vs-borrowed close. |
| `StderrSink` / `NullSink` / `MemorySink` | `src/log_foundry/sinks/util.py` | Utility sinks: NDJSON to stderr (twelve-factor); discard + `dropped` counter; in-process `.events` list with optional `maxlen` ring. |
| `HTTPSink` (+ `merge_headers`) | `src/log_foundry/sinks/http.py` | Dependency-free `urllib` POST core: ndjson/json_array, headers/auth, gzip, bounded 429/5xx retry (Retry-After); base for the platform sinks. |
| Elasticsearch / Loki / Logstash / Syslog sinks | `src/log_foundry/sinks/{elasticsearch,loki,logstash,syslog}.py` | Self-hosted platform sinks: `_bulk`, Loki push, Logstash HTTP/socket, RFC 5424 syslog. |
| SaaS sinks (Datadog/Splunk/NewRelic/Honeycomb/Sentry) | `src/log_foundry/sinks/{datadog,splunk,newrelic,honeycomb,sentry}.py` | SaaS logs intakes; `SentrySink` lazy-SDK (`sentry` extra) with HTTP-envelope fallback + level gating. |
| AWS stream sinks (Kinesis/Firehose/SNS) | `src/log_foundry/sinks/{kinesis,firehose,sns}.py` | boto3 (`aws` extra) durable-buffer sinks; count/byte chunking + partial-failure retry. |
| Queue sinks (Kafka/Redis/RabbitMQ/NATS/PubSub/EventHubs) | `src/log_foundry/sinks/{kafka,redis,rabbitmq,nats,pubsub,eventhubs}.py` | Lazy-import queue/stream sinks (one optional extra each); publish + bounded retry + close. |
| DB sinks (Mongo/Postgres/ClickHouse) | `src/log_foundry/sinks/{mongodb,postgres,clickhouse}.py` | Lazy-import DB sinks (one optional extra each); single-transaction batch insert, ownership-aware close. |
| chunk + transport helpers | `src/log_foundry/sinks/{_chunk,_time,_socket}.py` | `chunk_items`/`chunk_list`/`valid_identifier`; ISO→epoch time; TCP/UDP socket transport with reconnect retry. |
| `@trace` | `src/log_foundry/decorator.py` | Span decorator (sync + async, dispatched by `iscoroutinefunction`): lifecycle, hierarchy, non-swallowing flush. |
| level emitters + `set_baggage` | `src/log_foundry/api.py` | `debug/info/warning/error/critical` (append to span, orphan-safe) + `set_baggage` re-export. |
| `ConsoleWriter` | `src/log_foundry/console.py` | Synchronous human-readable `LEVEL   message` console echo (default `sys.stderr`). |
| `Worker` | `src/log_foundry/worker.py` | Background flush: bounded queue + daemon thread, batch by count/time, retry+backoff, drop-newest backpressure, graceful `shutdown()`. |
| `flush` | `src/log_foundry/__init__.py` (→ `Worker.flush`) | On-demand drain that does **not** retire the worker or close the sink: FIFO marker, bounded timeout, never raises, repeatable. The drain for frozen-not-exited processes (serverless). |
| `continue_trace` | `src/log_foundry/decorator.py` | Adopt an inbound trace context (W3C `traceparent` or explicit ids) + optional baggage header; re-parents an already-open **root** span and rewrites its buffered events. Total — never raises on hostile input. |
| `current_traceparent` / `current_trace_context` | `src/log_foundry/context.py` | Producer side: the current span as a `traceparent` string, or as a `(trace_id, span_id)` pair for payload fields. `None` when no span is active. |
| `current_baggage_header` (+ codec) | `src/log_foundry/context.py` | W3C `baggage` header serialize/parse, percent-encoded, 8192-byte cap; `str()` for non-string values. |
| `traceparent` codec | `src/log_foundry/ids.py` | `parse_traceparent` / `format_traceparent` + `is_valid_trace_id` / `is_valid_span_id`: strict lowercase-hex, all-zero rejection, W3C higher-version forward compatibility. |
| `FakeSink` fixture | `tests/conftest.py` | Test double recording emitted batches (+ `fake_sink`/`lf` fixtures, config reset). |
