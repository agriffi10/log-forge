# log-foundry — Architecture

> A Python library for generating **consistent, structured logs** per function call,
> correlating them with shared IDs, and shipping them to a configured output sink
> (e.g. an SQS queue) for consumption by something like the ELK stack.

This document is the design reference. It records *what* we're building and *why*,
including the decisions made during initial design. It is intentionally ahead of the
code — sections describe the target design, not necessarily what's implemented yet.

---

## 1. Purpose & scope

log-foundry owns the **logs** pillar of observability. It does **not** (yet) ship
metrics or traces, but it deliberately uses tracing vocabulary and ID conventions so
its output can be correlated with — or later promoted into — traces.

The unit of work is a **decorated function call**. Each call:

1. opens a **span** (start time, IDs, inherited defaults),
2. accumulates **log events** that user code emits while it runs,
3. records its own **end** on completion *or* exception,
4. hands the whole buffered queue to a background worker that flushes it to the sink.

Because the queue is buffered and flushed only at the *end* of the call, log-foundry
always knows the call's outcome (success/error, duration) before it sends — see
[§10 Sampling](#10-sampling-deferred).

---

## 2. Vocabulary

We adopt standard observability terms (matches OpenTelemetry / ELK conventions):

| Term | Meaning in log-foundry |
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
@log_foundry.trace
def process_payment(...):       # ── on enter ──────────────────────────────
                                #  • resolve context: parent span? → inherit trace_id,
                                #    set parent_span_id;  else → new trace_id, parent=null
                                #  • mint span_id, record start_ts, capture function name,
                                #    merge config defaults
                                #  • push this span onto the context stack
    log_foundry.info("charging")   #  • append a log event (trace_id + new log_id stamped)
    ...                         #
                                # ── on exit (success OR exception) ─────────
                                #  • record end_ts, duration, status (ok|error)
                                #  • on error: capture exception type + traceback,
                                #    then RE-RAISE (log-foundry never swallows)
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
  exiting pops. The top of the stack is the target for `log_foundry.info(...)` etc., and
  the source of `parent_span_id` for the next nested call.
- **Logging outside any span:** a top-level `log_foundry.info(...)` with no active span
  emits a standalone single-event queue with a fresh `trace_id` (an "orphan" log) so
  records are never silently dropped. *(Open item — see §12.)*

### 5.1 Baggage — trace-scoped dynamic context

`configure(defaults=...)` sets fields that are static for the whole process, and a
per-decorator `defaults=` sets fields for one call tree. **Baggage** fills the gap
between them: key/values set *at runtime* on the current trace that are then
auto-stamped onto **every span and log event at or below** the point they were set.

```python
@log_foundry.trace
def handle_request(req):
    log_foundry.set_baggage(tenant=req.tenant, request_id=req.id)
    process_payment(req.user_id)   # tenant + request_id appear on its logs too
```

- Baggage lives in the same `contextvars` context as the span stack, so it propagates
  across nested calls, threads, and `async` tasks automatically.
- **The scope ends at the root span** (SPEC-024). "At or below the point they were set" is
  where baggage *starts*; where it stops is the close of the **root** span — the one opened
  when no other was active — at which point the baggage in effect before that span is
  restored. Nested spans deliberately do **not** reset: baggage set three calls deep must
  stay visible to its parent and to the siblings that follow it inside the same trace, which
  is the whole point of the feature. Without that boundary the scope is the whole
  `contextvars` context forever, and a long-lived process serving requests sequentially on
  one thread — the main thread, a pooled worker, a warm Lambda container — stamps one
  request's `user_id` onto the next request's events.
- **Set outside any span, baggage is a process-level default** and is restored *to* rather
  than erased, so it survives the traces beneath it. `configure(defaults=...)` is the better
  tool for that; `reset_context()` is what erases it, and is the only release available to a
  caller who uses the emitters without `@trace` (there is no root span to hang one on).
- The adopted inbound trace context (§12, SPEC-014) is released at the same point but is
  **cleared** rather than restored — it is a one-shot handoff to the trace it names.
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

1. **Add a log to the span** (the default) — `log_foundry.info("msg", user_id=4127)`
   appends a structured event to the current span's queue. It rides the async pipeline
   to the sink (→ ELK). This is the bread-and-butter path.

2. **Echo a message to the console** — for surfacing something to an **end user at a
   terminal** or to a **Lambda's stdout** (→ CloudWatch) *immediately*, without waiting
   for the async flush. Triggered per-call via `echo=True`:

   ```python
   log_foundry.info("payment complete", echo=True)
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

- **Global, once at startup** — `log_foundry.configure(...)`: `service`, `version`, `env`,
  the `sink`, and a `defaults` dict merged into every event.
- **Per-decorator overrides** — `@log_foundry.span(name=..., defaults={...})` to add or
  override fields for one call tree.

```python
import log_foundry

log_foundry.configure(
    service="payments",
    version="2.14",
    env="prod",
    sink=log_foundry.sinks.SQSSink(queue_url="https://sqs..."),
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
- **Graceful shutdown:** an `atexit` (and explicit `log_foundry.shutdown()`) hook drains
  the worker queue and `close()`s the sink so buffered events aren't lost on exit.
- **Backpressure policy:** if the worker queue is full (sink is down / slow), default to
  **drop-newest with a counted warning** rather than blocking the app. *(Open item — the
  drop-vs-block tradeoff should be configurable; see §12.)*

### 9.1 The sink is a durable buffer, not the final store

A core design principle: **log-foundry ships to a buffer, never directly to ELK.** The
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
- log-foundry's responsibility **ends at the sink.** A separate consumer drains SQS and
  owns indexing into ELK; that component is out of scope for this library (§13).
- **Batching honors the sink's limits, but the worker stays sink-agnostic** — the worker
  flushes on fixed count/time thresholds and hands the sink whatever it accumulated; each
  sink re-chunks that batch to its own transport constraints. For SQS that means splitting
  into sends of ≤ 10 messages and ≤ 256 KB apiece. This keeps the worker dumb and lets each
  sink own the limits only it knows about.

---

## 10. Sampling (deferred)

*(Decision: send everything for now.)* Every span's queue is flushed unconditionally.

Sampling is deferred **in prose only — nothing is reserved in code.** There is no
`should_send` stub, `Protocol`, or config knob today. The shape a future hook would take:

```python
def should_send(span_summary) -> bool: ...   # default: always True
```

Its natural call site is `_close_span`, immediately before `_flush`, where this span's
`status` and `duration_ms` are already known. The hook should be **loadable from
configuration**, so policy can change without a code redeploy.

**Scope limit — that is span-outcome sampling, not tail sampling.** Tail sampling decides
per *trace*, once the trace completes. This pipeline cannot do that today:

- the buffer unit is a single span (`Span.events`), not a trace;
- each span flushes at its own close, so children are already emitted before the root
  learns its outcome;
- the worker is deliberately type-erased (`Queue[object]`) and never groups by `trace_id`;
- nothing signals trace completion — `pop_span` is a bare contextvar reset.

Real tail sampling would need a trace-scoped buffer and a root-completion signal — a
redesign of §9, not a hook. A per-span policy ("keep errors and slow calls") does plug in
at the call site above, but applied naively it emits structurally broken traces (kept
parent, dropped child), since siblings decide independently.

---

## 11. Public API sketch

```python
import log_foundry

log_foundry.configure(service="payments", version="2.14", env="prod",
                   sink=log_foundry.sinks.SQSSink(queue_url="..."))

@log_foundry.trace                      # root call: starts a new trace; captures "process_payment"
def process_payment(user_id: int):
    log_foundry.info("charging card", user_id=user_id)   # → span queue → sink
    write_ledger(user_id)            # nested span: same trace_id, parent=this span
    log_foundry.info("payment complete", echo=True)      # → queue AND printed to console now
    return "ok"

@log_foundry.trace
def write_ledger(user_id: int):
    log_foundry.debug("inserting row", user_id=user_id)

# Levels: log_foundry.debug / info / warning / error / critical
# Each appends to the current span's queue via contextvars; pass echo=True to also
# print the message to the console immediately.
```

> `@log_foundry.trace` decorates a function. The outermost decorated call starts a new
> **trace**; every decorated call (outer or nested) is a **span** within it.

---

## 12. Open items

**None.** This list predates the first line of code; the three questions it carried are settled
below, and SPEC-021 reconciled it so that nothing here reads as open unless it is. An item leaves
this section by being *resolved* (with the spec that settled it) or by moving to §13 as a stated
constraint — never by being deleted quietly.

### Resolved

- **Orphan logs** (emit standalone with a fresh `trace_id` vs warn-and-drop) → **emit standalone**,
  shipped in **SPEC-002**. A level call with no active span builds a complete event with a fresh
  `trace_id` and emits it synchronously on the caller's thread. Dropping it would make the emitters
  silently conditional on decorator placement, which is the opposite of what a logging call should
  promise. That synchronous path is also why `sanitize` must be total (SPEC-017).
- **Console echo defaults** (destination, line format, an `echo_level` threshold) → **shipped in
  SPEC-002**: `console.py` echoes to **stdout**, opt-in per call, no automatic `echo_level`
  threshold. Auto-echo was declined rather than deferred — it would double every event's cost by
  default for a development convenience.
- **Backpressure** (make drop-vs-block configurable) → **not built; a constraint, not a wart.**
  Overflow is drop-newest with a counter and a throttled stderr warning (§9, SPEC-017 FR-005), and
  blocking would put sink latency back on the caller's thread — the one thing arch §9 exists to
  prevent. Making it configurable is a *feature* with its own design surface, deliberately out of
  scope in SPEC-021. Stated in §13.
- **Decorator name** → `@log_foundry.trace`.
- **Auto-capture** → function name only; no args/return capture.
- **Cross-service trace continuation** (adopting an inbound `trace_id` from a
  `traceparent` request header, plus cross-process baggage — the full "Correlation ID
  Journey") → **shipped in SPEC-014**. `continue_trace()` adopts, `current_traceparent()` /
  `current_trace_context()` / `current_baggage_header()` publish. The bet in §3.1 paid off: the
  W3C-compatible ID formats meant this really was a header parse. Propagation stays manual —
  the caller moves the header; no client patching or middleware (that would drag in the
  dependencies the core deliberately does not have).
- **"Follows-from" span relationships** for fire-and-forget / async work → deferred;
  the ID model already accommodates it (§3.2).

---

## 13. Non-goals (for now)

- Emitting metrics or OpenTelemetry-native traces (we stay logs-only, but ID-compatible).
- Querying, dashboards, or alerting — that's ELK / downstream.
- Log *routing* logic beyond a single configured sink per process.

### Known constraints

- **Trace context crosses a process boundary only when the caller carries it.** ~~A trace is
  per-process~~ — **closed by SPEC-014.** `@trace` still mints a fresh `trace_id` whenever no
  span is open, so *by default* N processes produce N unrelated traces; the difference is that
  this is now fixable rather than inherent. `continue_trace(traceparent=...)` (or explicit
  `trace_id` / `parent_span_id`) adopts an inbound context and re-parents the already-open root
  span, and `current_traceparent()` / `current_baggage_header()` publish it on the way out.

  What remains a constraint is that **propagation is manual**: the library reads and writes the
  header, the caller moves it through whatever payload already crosses the boundary. There is no
  HTTP client patching, framework middleware, or boto3 hook, and there will not be — that is
  auto-instrumentation, a different product, and it needs the dependencies the core deliberately
  does not have.

- **The payload ceilings bound each *value*, not the event as a whole.** `max_value_bytes`,
  `max_stack_bytes`, `max_keys` and `max_depth` (SPEC-017) each bound one value, so a legal event
  can still be large: 256 keys × 8192 bytes is roughly 2 MB, past SQS's 256 KB. Acceptable because
  it is *visible* — a sink with a hard limit drops the event and counts `dropped_oversized`, so
  the loss is signalled where it happens rather than passed downstream silently. A per-event byte
  ceiling was deferred by SPEC-017, again by SPEC-020, and deliberately again by SPEC-021: it is a
  feature with real design surface (what happens on breach, which fields are sacrificed first) and
  belongs in its own spec. Lower `max_value_bytes` / `max_keys` if your destination's limit is
  tight. Note `max_value_bytes` carries two units — UTF-8 bytes for a string, rendered decimal
  length for an integer (SPEC-020, SPEC-021).

- **Backpressure is drop-newest, and is not configurable.** When the worker's bounded queue is
  full a submission is discarded rather than blocking the caller (§9): `health().dropped` counts
  it and stderr warns on the first drop and every thousandth (SPEC-017 FR-005). Blocking instead
  would push sink latency back onto the decorated function, which is the failure §9 exists to
  prevent, so the default is not merely a default — it is the behaviour the design commits to.
  A configurable drop-vs-block policy remains unbuilt (see §12).

- **A `BaseException` from a sink ends the worker — recorded, not silent.** `MultiSink.emit` and
  `Worker._emit` both catch `Exception` on purpose; widening either to `BaseException` would
  swallow a `KeyboardInterrupt` raised inside a child sink and carry on to the next one, which is
  worse than the failure it would prevent. Since SPEC-019 the escape is caught by the drain
  thread's terminal guard, which records the exception *type* in `health().stopped_reason` and
  writes one stderr line stating what was held and what was still queued (SPEC-021 FR-002). The
  worker is not restarted: a thread that resurrects itself fights a process trying to exit.

- **`atexit` does not run when a serverless environment is reaped.** The graceful drain (§9) is
  registered via `atexit`, which covers a process that *exits*. A Lambda execution environment
  is frozen when the handler returns and killed later without running exit handlers, so there is
  no point at which that drain is guaranteed to run. `flush()` (SPEC-013) is the only guaranteed
  drain there, and is the first thing to check when tail events go missing in a serverless
  deployment. `shutdown()` is the wrong tool per-invocation: it is terminal, so only the first
  invocation on a warm container would log.

---

## 14. Alignment with observability concepts

| Concept (design brief) | How log-foundry addresses it |
|------------------------|----------------------------|
| Structured vs unstructured | JSON with named fields, always (§6). |
| Correlation ID journey | Two-tier IDs + parent/child hierarchy across nested calls (§3), continued across processes via W3C `traceparent` + `baggage` (SPEC-014). |
| Head vs tail sampling | Neither is built. Span-outcome sampling has a natural call site; true tail sampling needs a trace-scoped buffer we don't have (§10). |
| Cardinality explosion | High-cardinality fields allowed in logs; never auto-promoted to metric labels (§6). |
| One event, three views | Logs-only today, but trace_id/span_id make traces derivable later (§13). |
| Symptom vs cause alerting | We stamp `duration_ms` / `status` / `error.type` — the fields alerts key off (§6). |
