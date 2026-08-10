# Component Inventory

Reusable modules/services/components/hooks already built, so a new spec reuses instead of rebuilding.
**One line per item** — name, path, one-line purpose. The code is the source of truth; this is just the
index to find it. Add a row as part of the completion ritual when a spec ships something reusable.

| Name | Path | Purpose |
|------|------|---------|
| `configure` / `get_config` / `_ensure_sink` | `src/log_foundry/config.py` | Process-wide config singleton; lazy `StdoutSink` default. |
| id generators | `src/log_foundry/ids.py` | `new_trace_id` / `new_span_id` / `new_log_id` (W3C-compatible). |
| `Span` + event builders | `src/log_foundry/model.py` | `Span` dataclass; `build_event` / `start_event` / `end_event` — the arch §6 JSON schema + precedence merge. |
| value sanitizer | `src/log_foundry/sanitize.py` | `coerce` / `sanitize_fields` / `truncate_str` / `truncate_tail` — makes any value JSON-safe and size-bounded, integers included (SPEC-020); total (never raises). |
| `health()` + `Health` | `src/log_foundry/__init__.py`, `worker.py` | Public snapshot of the worker's `queued` / `dropped` / `failed_batches`; never creates a worker. |
| context stack + baggage | `src/log_foundry/context.py` | `contextvars` current-span stack and baggage (`push/pop/current`, `get/set_baggage`). |
| root-span context scope | `src/log_foundry/context.py` | `push_baggage_scope` / `pop_baggage_scope` — bracket a root span so baggage is restored and the adopted context cleared on exit; total, tolerates a foreign-context token (SPEC-024). |
| `reset_context` | `src/log_foundry/context.py` | Public clear of baggage + adopted context, for callers who open no span (the orphan path) or adopt before dispatching into a child context. Never raises. |
| diagnostic channel | `src/log_foundry/_diag.py` | `absorbed` / `lost` / `rejected` + `errno_of` — **every** stderr line the library writes about itself. Exception **type** only (arch §6), detail escaped then bounded, each write total in itself. A test forbids any other module writing to stderr. Must import nothing from its own package (SPEC-025, SPEC-029). |
| `_begin` / `_end` | `src/log_foundry/decorator.py` | The `@trace` span lifecycle, guarded end to end — degraded setup rather than a failed call, one close per span, total teardown (SPEC-025). |
| `Sink` protocol | `src/log_foundry/sinks/base.py` | The `emit`/`close` output interface sinks implement (plus the loss contract below). |
| `StdoutSink` | `src/log_foundry/sinks/stdout.py` | Zero-dependency JSON-lines sink (default). |
| `SQSSink` | `src/log_foundry/sinks/sqs.py` | SQS sink (optional `aws` extra): lazy boto3, count/byte chunking, Failed-list retry, oversized drop. FIFO queues supported — `MessageGroupId` defaults to the event's `trace_id`, configurable; sender faults are not retried. |
| `CallbackSink` | `src/log_foundry/sinks/callback.py` | Adapt any callable into a `Sink` (+ optional `on_close`); the escape hatch for unsupported destinations. |
| `MultiSink` | `src/log_foundry/sinks/multi.py` | Fan one batch out to several sinks; per-child failure isolation (`failed`), close-all. |
| `FilteringSink` | `src/log_foundry/sinks/filtering.py` | Forward only events passing a predicate and/or `min_level`; fail-open on unknown level, no empty-batch emit. |
| `TransformSink` | `src/log_foundry/sinks/transform.py` | Per-event reshape/redact before forwarding; `None` drops; never mutates the caller's batch. |
| `LoggingSink` | `src/log_foundry/sinks/logging_sink.py` | Bridge events into stdlib `logging` — one `LogRecord`/event through a configurable logger; level-mapped, verbatim message, reserved-attr-safe fields (flat + nested). |
| `FileSink` / `RotatingFileSink` | `src/log_foundry/sinks/file.py` | Append NDJSON to a file; rotating variant adds size/time triggers + numbered backup retention (rotate-before-exceed, no event lost). |
| `SQLiteSink` | `src/log_foundry/sinks/sqlite.py` | Batch-insert events (full JSON + projected columns) into an embedded SQLite DB; injectable connection, `create_table` opt-out, owned-vs-borrowed close. |
| `StderrSink` | `src/log_foundry/sinks/stdout.py` | NDJSON to stderr (twelve-factor); a variant of `StdoutSink`, which is why it sits beside it. |
| `NullSink` | `src/log_foundry/sinks/null.py` | Discard everything; `.dropped` counts events. |
| `MemorySink` | `src/log_foundry/sinks/memory.py` | In-process `.events` list with optional `maxlen` ring — the sink a downstream test suite imports. |
| `HTTPSink` (+ `merge_headers`) | `src/log_foundry/sinks/http.py` | Dependency-free `urllib` POST core: ndjson/json_array, headers/auth, gzip, bounded 429/5xx retry (Retry-After); base for the platform sinks. |
| Elasticsearch / Loki / Logstash / Syslog sinks | `src/log_foundry/sinks/{elasticsearch,loki,logstash,syslog}.py` | Self-hosted platform sinks: `_bulk`, Loki push, Logstash HTTP/socket, RFC 5424 syslog. |
| SaaS sinks (Datadog/Splunk/NewRelic/Honeycomb/Sentry) | `src/log_foundry/sinks/{datadog,splunk,newrelic,honeycomb,sentry}.py` | SaaS logs intakes; `SentrySink` lazy-SDK (`sentry` extra) with HTTP-envelope fallback + level gating. |
| AWS stream sinks (Kinesis/Firehose/SNS) | `src/log_foundry/sinks/{kinesis,firehose,sns}.py` | boto3 (`aws` extra) durable-buffer sinks; count/byte chunking + partial-failure retry. |
| Queue sinks (Kafka/Redis/RabbitMQ/NATS/PubSub/EventHubs) | `src/log_foundry/sinks/{kafka,redis,rabbitmq,nats,pubsub,eventhubs}.py` | Lazy-import queue/stream sinks (one optional extra each); publish + bounded retry + close. |
| DB sinks (Mongo/Postgres/ClickHouse) | `src/log_foundry/sinks/{mongodb,postgres,clickhouse}.py` | Lazy-import DB sinks (one optional extra each); single-transaction batch insert, ownership-aware close. |
| chunk + transport helpers | `src/log_foundry/sinks/{_chunk,_time,_socket}.py` | `chunk_items`/`chunk_list`/`valid_identifier`; ISO→epoch time; TCP/UDP socket transport with reconnect retry. |
| interruptible retry wait | `src/log_foundry/sinks/_retry.py` | `wait(delay, stop)` waits on the worker's shutdown event instead of sleeping, so a backoff never holds the single drain thread past a shutdown; `clamp_server_delay(value, ceiling)` bounds and sign-checks a server-supplied delay. Both total. |
| sink loss contract | `src/log_foundry/sinks/base.py` | `SinkLosses` + `SinkDeliveryError` + `read_losses(sink)`: the raise-on-total-failure / never-raise-on-partial rules a sink must honour, plus the total probe for the optional `losses()` accessor (absent, non-callable, raising and wrong-shaped all read as "reports nothing"). |
| positional batch adjudicator | `src/log_foundry/sinks/_batch.py` | `usable_results` + `adjudicate_positional` — pair an id-less per-record response against the records it should describe, refusing to adjudicate any of them unless it describes all of them. Total (never raises). |
| `@trace` | `src/log_foundry/decorator.py` | Span decorator (sync + async, dispatched by `iscoroutinefunction`): lifecycle, hierarchy, non-swallowing flush. |
| level emitters + `set_baggage` | `src/log_foundry/api.py` | `debug/info/warning/error/critical` (append to span, orphan-safe) + `set_baggage` re-export. |
| `ConsoleWriter` | `src/log_foundry/console.py` | Synchronous human-readable `LEVEL   message` console echo (default `sys.stderr`). |
| `Worker` | `src/log_foundry/worker.py` | Background flush: bounded queue + daemon thread, batch by count/time, retry+backoff, drop-newest backpressure, graceful `shutdown()`; a terminal failure of the drain loop is recorded on `Health.stopped_reason` rather than ending delivery silently. |
| `flush` | `src/log_foundry/__init__.py` (→ `Worker.flush`) | On-demand drain that does **not** retire the worker or close the sink: FIFO marker, bounded timeout, never raises, repeatable. The drain for frozen-not-exited processes (serverless). |
| `continue_trace` | `src/log_foundry/decorator.py` | Adopt an inbound trace context (W3C `traceparent` or explicit ids) + optional baggage header; re-parents an already-open **root** span and rewrites its buffered events. Total — never raises on hostile input. |
| `current_traceparent` / `current_trace_context` | `src/log_foundry/context.py` | Producer side: the current span as a `traceparent` string, or as a `(trace_id, span_id)` pair for payload fields. `None` when no span is active. |
| `current_baggage_header` (+ codec) | `src/log_foundry/context.py` | W3C `baggage` header serialize/parse, percent-encoded, 8192-byte cap; `str()` for non-string values. |
| `traceparent` codec | `src/log_foundry/ids.py` | `parse_traceparent` / `format_traceparent` + `is_valid_trace_id` / `is_valid_span_id`: strict lowercase-hex, all-zero rejection, W3C higher-version forward compatibility. |
| `FakeSink` fixture | `tests/conftest.py` | Test double recording emitted batches (+ `fake_sink`/`lf` fixtures, config reset). |
| `run_concurrently` helper | `tests/conftest.py` | Runs N threads through one callable, started together on a `threading.Barrier` and with the switch interval tightened; returns every exception raised. The shared concurrent-emitter harness (SPEC-028). |
| SBOM builder | `scripts/make-sbom.py` | Build a CycloneDX SBOM for a **built** distribution: installs the wheel with every extra into a throwaway venv, runs the generator from a second one, writes `metadata.component` from the sdist version, then asserts (non-empty, no build-tooling leakage, extras present) and schema-validates. Run it locally with `--output`. |
| public result types | `src/log_foundry/results.py` | `FlushResult`, `ContinueResult` — a frozen dataclass with `__bool__` and `reason: str \| None`, so a one-bit return can grow a reason without changing what `if flush():` means. Imports nothing from the package (SPEC-034 FR-007). |
| sink-lifecycle helpers | `src/log_foundry/_lifecycle.py` | `close_detached` (daemon close of a swapped-out sink, returns the thread so a caller can join outside its lock), `join_closers` (capped exit grace), `closing_count` (the `Health.closing_sinks` gauge), `offer_stop_signal` (SPEC-027's `hasattr` probe). Process-global, so both delivery paths share one roster (SPEC-033). |
