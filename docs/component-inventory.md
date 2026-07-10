# Component Inventory

Reusable modules/services/components/hooks already built, so a new spec reuses instead of rebuilding.
**One line per item** — name, path, one-line purpose. The code is the source of truth; this is just the
index to find it. Add a row as part of the completion ritual when a spec ships something reusable.

| Name | Path | Purpose |
|------|------|---------|
| `configure` / `get_config` / `_ensure_sink` | `src/log_forge/config.py` | Process-wide config singleton; lazy `StdoutSink` default. |
| id generators | `src/log_forge/ids.py` | `new_trace_id` / `new_span_id` / `new_log_id` (W3C-compatible). |
| `Span` + event builders | `src/log_forge/model.py` | `Span` dataclass; `build_event` / `start_event` / `end_event` — the arch §6 JSON schema + precedence merge. |
| context stack + baggage | `src/log_forge/context.py` | `contextvars` current-span stack and baggage (`push/pop/current`, `get/set_baggage`). |
| `Sink` protocol | `src/log_forge/sinks/base.py` | The `emit`/`close` output interface sinks implement. |
| `StdoutSink` | `src/log_forge/sinks/stdout.py` | Zero-dependency JSON-lines sink (default). |
| `SQSSink` | `src/log_forge/sinks/sqs.py` | SQS sink (optional `sqs` extra): lazy boto3, count/byte chunking, Failed-list retry, oversized drop. |
| `@trace` | `src/log_forge/decorator.py` | Span decorator (sync + async, dispatched by `iscoroutinefunction`): lifecycle, hierarchy, non-swallowing flush. |
| level emitters + `set_baggage` | `src/log_forge/api.py` | `debug/info/warning/error/critical` (append to span, orphan-safe) + `set_baggage` re-export. |
| `ConsoleWriter` | `src/log_forge/console.py` | Synchronous human-readable `LEVEL   message` console echo (default `sys.stderr`). |
| `Worker` | `src/log_forge/worker.py` | Background flush: bounded queue + daemon thread, batch by count/time, retry+backoff, drop-newest backpressure, graceful `shutdown()`. |
| `FakeSink` fixture | `tests/conftest.py` | Test double recording emitted batches (+ `fake_sink`/`lf` fixtures, config reset). |
