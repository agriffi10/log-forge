# log-forge — Architecture

> A Python library for generating **consistent, structured logs** per function call,
> correlating them with shared IDs, and shipping them to a configured output sink
> (e.g. an SQS queue) for consumption by something like the ELK stack.

This document is the design reference. It records *what* we're building and *why*,
including the decisions made during initial design. It is intentionally ahead of the
code — sections describe the target design, not necessarily what's implemented yet.

---

## 1. Purpose & scope

log-forge owns the **logs** pillar of observability. It does **not** (yet) ship
metrics or traces, but it deliberately uses tracing vocabulary and ID conventions so
its output can be correlated with — or later promoted into — traces.

The unit of work is a **decorated function call**. Each call:

1. opens a **span** (start time, IDs, inherited defaults),
2. accumulates **log events** that user code emits while it runs,
3. records its own **end** on completion *or* exception,
4. hands the whole buffered queue to a background worker that flushes it to the sink.

Because the queue is buffered and flushed only at the *end* of the call, log-forge
always knows the call's outcome (success/error, duration) before it sends — see
[§10 Sampling](#10-sampling-deferred).

---

## 2. Vocabulary

We adopt standard observability terms (matches OpenTelemetry / ELK conventions):

| Term | Meaning in log-forge |
|------|----------------------|
| **trace** | One logical end-to-end flow. Shares a single `trace_id` across every nested decorated call. |
| **span** | One decorated function call. Has its own `span_id`, a start, an end, a duration, and a status. |
| **log event** | One structured record a user emits inside a span (`info`, `error`, …). Has its own `log_id`. |
| **queue** | The in-memory buffer of log events belonging to one span, flushed together at span end. |
| **sink** | The configured output destination (SQS, stdout, file, HTTP, …). |

---

## 3. ID model (two tiers + hierarchy)

Correlation is the whole point (see "Correlation ID Journey" in the design brief).
We use **four** IDs:

- **`trace_id`** — shared by *every* span and *every* log event in one logical flow.
  This is what groups related events in ELK. Generated at the **root** span; inherited
  by all descendants.
- **`span_id`** — unique per decorated call (per queue).
- **`parent_span_id`** — the `span_id` of the enclosing decorated call, or `null` for the
  root. This is what makes the call hierarchy reconstructable. *(Decision: nested calls
  inherit + build hierarchy.)*
- **`log_id`** — unique per individual log event, so a single record can be referenced.

### 3.1 ID formats — W3C Trace Context compatible

`trace_id` and `span_id` use the **W3C Trace Context** wire formats rather than arbitrary
UUIDs:

- **`trace_id`** — 16 bytes, rendered as 32 lowercase hex chars.
- **`span_id`** — 8 bytes, rendered as 16 lowercase hex chars.
- **`log_id`** — internal-only, so a UUID is fine here.

The cost today is negligible, but it means a future "adopt the inbound request's trace"
feature is just *parse the `traceparent` header* (see §12), and the emitted records stay
compatible with standard distributed-tracing tooling — not only the SQS → ELK path.

### 3.2 Span relationships (current + future)

The only relationship modeled in v1 is **parent → child** (a nested decorated call runs
*within* its parent). A second, looser relationship — **"follows-from"**, for work that
is causally linked but does *not* block the parent (fire-and-forget tasks, queue handlers
that outlive the caller) — is anticipated but deferred. The ID model already accommodates
it: such a span keeps the parent's `trace_id` but records the link as follows-from rather
than child-of.

```
trace_id = 7f3a9e...                         (one flow)
└─ span A  span_id=a1  parent=null           process_payment()
   ├─ log_id=L1  "charging card"
   ├─ log_id=L2  "gateway ok"
   └─ span B  span_id=b2  parent=a1          write_ledger()   ← nested decorated call
      └─ log_id=L3  "row inserted"
```

---

## 4. Lifecycle of a decorated call

```
@logforge.trace
def process_payment(...):       # ── on enter ──────────────────────────────
                                #  • resolve context: parent span? → inherit trace_id,
                                #    set parent_span_id;  else → new trace_id, parent=null
                                #  • mint span_id, record start_ts, capture function name,
                                #    merge config defaults
                                #  • push this span onto the context stack
    logforge.info("charging")   #  • append a log event (trace_id + new log_id stamped)
    ...                         #
                                # ── on exit (success OR exception) ─────────
                                #  • record end_ts, duration, status (ok|error)
                                #  • on error: capture exception type + traceback,
                                #    then RE-RAISE (log-forge never swallows)
                                #  • pop the context stack
                                #  • enqueue the finished span's queue to the worker
```

The decorator is **non-swallowing**: exceptions are recorded and re-raised unchanged.

---

## 5. Context propagation

*(Decision: support threads **and** asyncio.)*

- The "current span" is tracked with **`contextvars.ContextVar`**, not thread-locals.
  `contextvars` is correct under both threads and `async`/`await` (each task gets its
  own context), so users never pass the queue around manually.
- The context holds a **stack** of active spans. Entering a decorated call pushes;
  exiting pops. The top of the stack is the target for `logforge.info(...)` etc., and
  the source of `parent_span_id` for the next nested call.
- **Logging outside any span:** a top-level `logforge.info(...)` with no active span
  emits a standalone single-event queue with a fresh `trace_id` (an "orphan" log) so
  records are never silently dropped. *(Open item — see §12.)*

### 5.1 Baggage — trace-scoped dynamic context

`configure(defaults=...)` sets fields that are static for the whole process, and a
per-decorator `defaults=` sets fields for one call tree. **Baggage** fills the gap
between them: key/values set *at runtime* on the current trace that are then
auto-stamped onto **every span and log event at or below** the point they were set.

```python
@logforge.trace
def handle_request(req):
    logforge.set_baggage(tenant=req.tenant, request_id=req.id)
    process_payment(req.user_id)   # tenant + request_id appear on its logs too
```

- Baggage lives in the same `contextvars` context as the span stack, so it propagates
  across nested calls, threads, and `async` tasks automatically.
- It is merged into each event's `fields` (see §6) at emit time. Precedence, lowest to
  highest: global `defaults` → per-decorator `defaults` → baggage → explicit per-call
  fields.
- Baggage is **not** propagated across process boundaries in v1 (that travels with the
  cross-service trace-continuation work deferred in §12).

---

## 6. Log event schema

Every emitted record is JSON with named fields (structured, never free-form text —
this is what turns logs into queryable data). Base fields stamped on **every** event:

```json
{
  "timestamp":      "2024-01-15T14:23:01.842Z",
  "level":          "INFO",
  "message":        "charging card",
  "trace_id":       "7f3a9e2b1c...",
  "span_id":        "a1...",
  "parent_span_id": "null or b2...",
  "log_id":         "L1...",
  "function":       "process_payment",
  "service":        "payments",
  "version":        "2.14",
  "env":            "prod",
  "fields":         { "user_id": 4127 }
}
```

- **Auto-capture:** the decorator captures the **function name** only (`func.__qualname__`),
  used as both the span name and the `function` field. *(Decision: function name only — no
  automatic capture of arguments or return values, to avoid leaking secrets/PII.)*
- **Span boundary events:** span start and span end are themselves log events.
  The end event additionally carries `duration_ms`, `status` (`ok`/`error`), and on
  failure `error.type` + `error.stack`.
- **User fields** go in a nested `fields` object (arbitrary key/values) to keep them
  separate from reserved/base fields. High-cardinality values (`user_id`) are fine
  here — cardinality is a *metrics* concern, not a logs concern, **as long as we never
  auto-promote these into metric labels**.

### 6.1 Two ways to emit, from inside a function

While a span is active, user code has two complementary capabilities:

1. **Add a log to the span** (the default) — `logforge.info("msg", user_id=4127)`
   appends a structured event to the current span's queue. It rides the async pipeline
   to the sink (→ ELK). This is the bread-and-butter path.

2. **Echo a message to the console** — for surfacing something to an **end user at a
   terminal** or to a **Lambda's stdout** (→ CloudWatch) *immediately*, without waiting
   for the async flush. Triggered per-call via `echo=True`:

   ```python
   logforge.info("payment complete", echo=True)
   ```

Console echo characteristics:

- **Synchronous & immediate** — written at call time, independent of the background
  worker and sink. Bypasses buffering entirely.
- **Human-readable, not JSON** — emits a plain line (default `LEVEL  message`) meant for
  a person or a log scraper, distinct from the structured record sent to the sink.
- **Additive, not a redirect** — an echoed event *still* goes into the span queue and on
  to the sink. Echo just gives it a second, instant audience.
- **Configurable** — global defaults set the destination (stdout/stderr), the line
  format, and an optional `echo_level` (e.g. auto-echo everything `>= WARNING`);
  per-call `echo=` overrides. Implemented behind a tiny `ConsoleWriter`, kept separate
  from the async `Sink`.

---

## 7. Configuration

Two layers:

- **Global, once at startup** — `logforge.configure(...)`: `service`, `version`, `env`,
  the `sink`, and a `defaults` dict merged into every event.
- **Per-decorator overrides** — `@logforge.span(name=..., defaults={...})` to add or
  override fields for one call tree.

```python
import logforge

logforge.configure(
    service="payments",
    version="2.14",
    env="prod",
    sink=logforge.sinks.SQSSink(queue_url="https://sqs..."),
    defaults={"team": "checkout"},
)
```

---

## 8. Output sinks (pluggable)

A sink is a small interface so the transport is swappable:

```python
class Sink(Protocol):
    def emit(self, batch: list[dict]) -> None: ...   # ship a batch of events
    def close(self) -> None: ...                      # flush + release resources
```

Planned implementations:

- **`StdoutSink`** — JSON lines to stdout. The **default**; zero-dependency, great for
  local dev and container log scraping.
- **`SQSSink`** — batches events to an SQS queue (the headline use case → ELK).
- (later) `FileSink`, `HTTPSink`/Logstash, `KafkaSink`.

Sinks receive **already-serialized, batched** events from the worker; they should not
know about spans or context.

---

## 9. Flush pipeline (background, non-blocking)

*(Decision: background, non-blocking flush.)*

```
decorated call ends
   │  enqueue finished span's events  (fast, in-process handoff)
   ▼
[ in-memory worker queue ]  ──►  background worker thread
   │                                • drains the queue
   │                                • batches events (size/time window)
   │                                • calls sink.emit(batch)
   │                                • retries with backoff on failure
   ▼
                                  sink (SQS / stdout / …)
```

- The decorated function **returns immediately**; SQS latency and outages never block
  application code.
- **Graceful shutdown:** an `atexit` (and explicit `logforge.shutdown()`) hook drains
  the worker queue and `close()`s the sink so buffered events aren't lost on exit.
- **Backpressure policy:** if the worker queue is full (sink is down / slow), default to
  **drop-newest with a counted warning** rather than blocking the app. *(Open item — the
  drop-vs-block tradeoff should be configurable; see §12.)*

### 9.1 The sink is a durable buffer, not the final store

A core design principle: **log-forge ships to a buffer, never directly to ELK.** The
pipeline has two distinct stages of decoupling, each absorbing a different failure mode:

```
app  ──►  in-memory worker queue  ──►  durable sink (SQS)  ──►  consumer  ──►  ELK
          └ smooths short bursts ┘     └ absorbs sustained spikes ┘   └ owns indexing ┘
```

- The **in-memory worker queue** smooths *short* bursts so the app never blocks on I/O.
- The **durable sink (SQS)** absorbs *sustained* spikes and outages: if the downstream
  consumer or ELK is slow or down, events accumulate safely in the queue instead of
  being lost or back-pressuring the app. This is exactly why a durable queue is the
  default headline sink rather than a direct `HTTPSink` to Elasticsearch — a direct
  writer couples application availability to ELK availability.
- log-forge's responsibility **ends at the sink.** A separate consumer drains SQS and
  owns indexing into ELK; that component is out of scope for this library (§13).
- **Batching honors the sink's limits, but the worker stays sink-agnostic** — the worker
  flushes on fixed count/time thresholds and hands the sink whatever it accumulated; each
  sink re-chunks that batch to its own transport constraints. For SQS that means splitting
  into sends of ≤ 10 messages and ≤ 256 KB apiece. This keeps the worker dumb and lets each
  sink own the limits only it knows about.

---

## 10. Sampling (deferred)

*(Decision: send everything for now.)* Every span's queue is flushed unconditionally.

But the architecture is deliberately **tail-sampling-ready**: because we buffer the
whole queue and decide to send only at span end, we already know `status` and
`duration_ms` at decision time. We reserve a single seam for this:

```python
def should_send(span_summary) -> bool: ...   # default: always True
```

A future tail-sampling policy ("always keep errors + slow calls, sample the rest")
plugs in here with no change to the rest of the pipeline. The hook should be
**loadable from configuration**, so sampling policy can change without a code redeploy.

---

## 11. Public API sketch

```python
import logforge

logforge.configure(service="payments", version="2.14", env="prod",
                   sink=logforge.sinks.SQSSink(queue_url="..."))

@logforge.trace                      # root call: starts a new trace; captures "process_payment"
def process_payment(user_id: int):
    logforge.info("charging card", user_id=user_id)   # → span queue → sink
    write_ledger(user_id)            # nested span: same trace_id, parent=this span
    logforge.info("payment complete", echo=True)      # → queue AND printed to console now
    return "ok"

@logforge.trace
def write_ledger(user_id: int):
    logforge.debug("inserting row", user_id=user_id)

# Levels: logforge.debug / info / warning / error / critical
# Each appends to the current span's queue via contextvars; pass echo=True to also
# print the message to the console immediately.
```

> `@logforge.trace` decorates a function. The outermost decorated call starts a new
> **trace**; every decorated call (outer or nested) is a **span** within it.

---

## 12. Open items

- **Orphan logs** — confirm "emit standalone with fresh trace_id" (§5) vs warn-and-drop.
- **Backpressure** — make drop-vs-block configurable (§9); pick safe default.
- **Console echo defaults** — confirm destination (stdout vs stderr), default line format,
  and whether an `echo_level` auto-echo threshold ships in v1 (§6.1).

### Resolved
- **Decorator name** → `@logforge.trace`.
- **Auto-capture** → function name only; no args/return capture.
- **Cross-service trace continuation** (adopting an inbound `trace_id` from a
  `traceparent` request header, plus cross-process baggage — the full "Correlation ID
  Journey") → deferred to a later version. ID formats are already W3C-compatible (§3.1)
  to make this cheap.
- **"Follows-from" span relationships** for fire-and-forget / async work → deferred;
  the ID model already accommodates it (§3.2).

---

## 13. Non-goals (for now)

- Emitting metrics or OpenTelemetry-native traces (we stay logs-only, but ID-compatible).
- Querying, dashboards, or alerting — that's ELK / downstream.
- Log *routing* logic beyond a single configured sink per process.

---

## 14. Alignment with observability concepts

| Concept (design brief) | How log-forge addresses it |
|------------------------|----------------------------|
| Structured vs unstructured | JSON with named fields, always (§6). |
| Correlation ID journey | Two-tier IDs + parent/child hierarchy across nested calls (§3). |
| Head vs tail sampling | Buffer-then-flush makes us tail-sampling-ready; seam reserved (§10). |
| Cardinality explosion | High-cardinality fields allowed in logs; never auto-promoted to metric labels (§6). |
| One event, three views | Logs-only today, but trace_id/span_id make traces derivable later (§13). |
| Symptom vs cause alerting | We stamp `duration_ms` / `status` / `error.type` — the fields alerts key off (§6). |
