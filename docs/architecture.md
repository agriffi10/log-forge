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
  records are never silently dropped. It mints that `trace_id` unconditionally, so an orphan
  log never joins a context adopted via `continue_trace` — the adoption waits for the next
  root span. A caller on this path releases both values with `reset_context()` (SPEC-024).

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
  across nested calls and `async` tasks automatically. **A new thread does not inherit it** —
  a thread starts with a fresh context, and only an explicit `copy_context()` (as
  `asyncio.to_thread` does) carries anything into it. That context then persists for the life
  of the thread, which is the mechanism behind the leak the next bullet closes.
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
- Baggage crosses a process boundary **only when the caller carries it** (SPEC-014, §13):
  `current_baggage_header()` publishes it in W3C `baggage` format, `continue_trace(baggage=...)`
  adopts it. Nothing is auto-instrumented.

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
- **The rule holds for what the library says about *itself*, not just for events.** A stderr
  diagnostic is also a place caller data was not asked to reach, and an exception's message
  routinely carries the value that provoked it — so an absorbed failure is reported by
  `type(exc).__name__`, never its message, and `sanitize`'s placeholder is a type name for the
  same reason (as is SPEC-019's `Health.stopped_reason`). Every such line goes through one
  module, `src/log_foundry/_diag.py`, whose docstring states this rule and the two that travel
  with it. A test asserts no other module *calls* `stderr.write` (or `print(file=…)`, or
  `traceback.print_*`) — a lint on the idiom, not a sandbox. (SPEC-029)

### 6.1 Two ways to emit, from inside a function

While a span is active, user code has two complementary capabilities:

1. **Add a log to the span** (the default) — `log_foundry.info("msg", user_id=4127)`
   appends a structured event to the current span's queue. It rides the async pipeline
   to the sink (→ ELK). This is the bread-and-butter path.

2. **Echo a message to the console** — for surfacing something to an **end user at a
   terminal** or to a **Lambda's captured console output** (→ CloudWatch) *immediately*,
   without waiting for the async flush. It goes to **stderr**, not stdout (SPEC-031 FR-003
   corrected the earlier claim here and in §12). Triggered per-call via `echo=True`:

   ```python
   log_foundry.info("payment complete", echo=True)
   ```

Console echo characteristics:

- **Synchronous & immediate** — written at call time, independent of the background
  worker and sink. Bypasses buffering entirely.
- **Human-readable, not JSON** — emits a plain line, fixed as `f"{level:<7} {message}"`, meant
  for a person or a log scraper, distinct from the structured record sent to the sink. It is
  not a default in the sense of something overridable; there is no format setting (SPEC-031
  FR-003, as the struck-through bullet below records).
- **Additive, not a redirect** — an echoed event *still* goes into the span queue and on
  to the sink. Echo just gives it a second, instant audience.
- ~~**Configurable** — global defaults set the destination (stdout/stderr), the line
  format, and an optional `echo_level` (e.g. auto-echo everything `>= WARNING`)~~ —
  **corrected by SPEC-031 FR-003**, which found this describes a configuration surface that
  was never built and that §12 Resolved had already declined. What shipped: **per-call
  `echo=` only**, a fixed `LEVEL   message` line, and a stream chosen at `ConsoleWriter`
  construction (stderr by default) — there is no global echo destination, format or
  threshold, and no `set_stream()`. Implemented behind a tiny `ConsoleWriter`, kept separate
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

**Everything but the sink is read per event; the sink is captured.** `service`, `version`, `env`,
`defaults` and the payload ceilings are looked up through `get_config()` as each event is built, so
a later `configure()` changes them immediately. The sink is different: the background worker (§9)
takes it once, when it is lazily built on the first flush. A later `configure(sink=...)` therefore
has to retarget the worker, or the config and the behaviour disagree — which is what it did until
SPEC-030, updating `get_config().sink` while every event continued to the sink captured first.

**Sink-swap semantics** (SPEC-030 FR-003). A `sink=` passed once a worker exists:

1. drains everything submitted so far to the **previous** sink — those events were submitted for
   that destination and are not carried over;
2. reassigns the worker's sink, which keeps the queue, the drain thread, the counters and the
   `atexit` registration intact (rebuilding the worker would drop what was queued and register a
   second drain), and hands the new sink the worker's stop signal (§9, SPEC-027);
3. drains once more — a fence, not a delivery: it proves the drain thread is not still inside the
   old sink's `emit`, the one way `close()` could be called under a writer (SPEC-028);
4. closes the previous sink.

One deadline covers all four steps, so a destination that hangs in any of them cannot make
`configure()` block for a multiple of the budget. Step 4 gets what is left of it: `Sink.close()`
takes no timeout, so it runs on its own **daemon** thread and is joined for the remainder — only
the waiting is bounded. Nothing is derived from an expired join: no counter moves and no line is
written, because a slow close and a stuck one are indistinguishable at that moment and a signal
that cannot tell them apart is worse than none. What is published instead is a *live* fact,
`health().closing_sinks` — the closes running at the instant it is read, which is unambiguous in a
way an expired join is not. Both
guards are re-taken after the first drain, since it blocks: a `shutdown()` landing mid-swap must
abandon the swap, or it installs a sink nothing will ever close. A drain that cannot be confirmed
does **not** cancel the swap — the caller asked for the
new sink, and silently keeping the old one is the defect being fixed — but the previous sink is
left **open** and `health().incomplete_swaps` records it, on SPEC-027 FR-004's reasoning that a
leaked resource in a running process beats a close raced against a write. Passing the sink already
live is a no-op. After `shutdown()` the worker swaps nothing: it is retired, so the config is
updated and the retirement signals (§9) continue to apply — but the sink adopted by that call is
still delivered to and is closed by the orphan path below, since a retired worker owns nothing
further.

**The swap covers both delivery paths, but only the *close* is shared** (SPEC-033 FR-002). A
process that has only ever logged outside a span builds no worker, and steps 1–3 above have
nothing to do there: those events were emitted synchronously on the caller's thread and returned
before `configure()` was entered, so there is no queue to drain and no drain thread to fence out.
What remains is step 4, on the same terms — the same daemon closer, the same shared budget, the
same `closing_sinks` gauge, and the same refusal to derive anything from an expired join. The
consequence is that `incomplete_swaps` stays at zero on that path *by design*: it records a drain
that could not be confirmed, and there is no drain. Which sink to close comes from the identity
the orphan emitter recorded, not from the config, which by then names the new one; that record is
re-pointed at the new sink rather than cleared, so a process that swaps and then exits without
logging again still closes what it swapped to. The one writer neither path can fence out is an
orphan emitter on another application thread, which is why `sinks/base.py` requires `close()` to
tolerate a concurrent `emit` (§9, SPEC-028).

`configure()` remains a startup call and is not thread-safe. A span finishing on another thread
during a swap may land on either sink; what is guaranteed is that everything submitted *before* the
call reached the old one. The `_worker` read that selects between the two paths *is* taken under
the process lock, because it decides whether this call may close a sink at all — unlocked, a first
`@trace` mid-construction on another thread would have its sink closed underneath it.

---

## 8. Output sinks (pluggable)

A sink is a small interface so the transport is swappable:

```python
class Sink(Protocol):
    def emit(self, batch: list[dict]) -> None: ...   # ship a batch of events
    def close(self) -> None: ...                      # flush + release resources
    # optional (SPEC-026):
    # def losses(self) -> SinkLosses | None: ...      # cumulative loss this sink absorbed
```

**A sink carries two reporting obligations** (SPEC-026), because the worker's retry and
`health()` are built on them:

- **Total failure raises.** A sink that delivered *none* of a batch must let something
  propagate out of `emit`, after its own retries are spent. That is the only case where the
  worker's retry cannot create duplicates — nothing landed downstream to duplicate.
- **Partial failure does not.** A batch where some records landed must be counted, not raised:
  the worker retries whole batches, so raising would re-deliver what already arrived.

A sink that absorbs a total failure and returns normally is a sink the worker *believes* — the
retry never engages, `failed_batches` stays at zero and `flush()` returns `True` while every
event is lost. `losses()` is optional (`Sink` is structural, so a pre-SPEC-026 sink must keep
satisfying it) and is read through `sinks.base.read_losses`, which treats absent, non-callable,
raising and wrong-shaped alike as "reports nothing".

Three cases where nothing landed and the sink still must not raise, each settled by an earlier
spec: an **unadjudicable** batch response (SPEC-018 — the sink cannot prove nothing landed, so a
retry risks duplicating), an **SQS sender fault** (SPEC-016 FR-006 — provably rejected, and a
byte-identical re-send can only fail the same way), and an **oversized** event (it can never fit,
so there is nothing to retry). All three are reported through `losses()` instead.

**The same rule reaches the sink's own lifecycle** (SPEC-032): a sink whose `close()` released or
invalidated a transport must **raise** on a later non-empty `emit` rather than absorb it — a
produce into a batch nothing will flush again, or a future nothing will resolve, is total failure
by a different route. A sink holding nothing to release keeps **accepting**, because refusing a
batch that would have delivered is loss the library invented. Which applies is a property of the
sink, so each records its answer in its class docstring and a lint holds every sink to one.
`emit([])` remains a no-op either way.

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
  **drop-newest with a counted warning** rather than blocking the app. Making drop-vs-block
  configurable is **not built** and is a constraint rather than a wart — blocking would put sink
  latency back on the caller's thread, which is the one thing this section exists to prevent
  (§12, §13).
- **One drain thread, but not one caller.** The worker drains on a single thread by design —
  everything below about backoff and ordering follows from that. It is not, however, the only
  thread that reaches the sink: a level call made with no active span emits synchronously on the
  *caller's* thread (§12 Resolved), which may be any of the application's, and it does so against
  the same sink object the worker is draining into. An audit probe measured one sink entered by
  two application threads and `log-foundry-worker` at once, with overlapping calls. So `emit` and
  `close` **may be called concurrently, and a sink must tolerate it** — a requirement on
  implementations rather than something the library serializes on their behalf, since it does not
  own the orphan path's thread. `sinks/base.py` states the contract for third-party ones
  (SPEC-028).

  Which shipped sinks lock is **decided per driver, and the decision is recorded in each sink's
  docstring** — it is the vendor's contract, not something derivable from this tree. A rebindable
  stream (`FileSink`, `RotatingFileSink`), a reused socket (`SocketTransport`, `RabbitMQSink`), a
  connection with transaction scope (`SQLiteSink`, `PostgresSink`), a session-bound client
  (`ClickHouseSink`), a single-entry event loop (`NATSSink`) and a producer that is not published
  as shareable (`AzureEventHubsSink`) all take one. The sinks whose client documents its own
  thread-safety — `MongoDBSink`, the boto3 four, the Redis pair, `KafkaSink`, `SentrySink`,
  `GooglePubSubSink` — take no *transport* lock, and say why; several still guard a small piece
  of their own state (Mongo's close flag, Pub/Sub's pending-futures list). A lint (`test_every_driver_backed_sink_records_a_concurrency_decision`) fails
  any driver-backed sink that neither locks nor records a reason, because the first pass at this
  worked from a hand-written roster and missed three sinks — one of which could hang an
  application thread permanently. That is the SPEC-027 roster lesson, repeated once and now
  enforced.
- **A sink's backoff pauses the single drain thread** (SPEC-027). There is one drain thread by
  design, so a sink sleeping between attempts is not making a local decision — it is a global
  pause on log delivery, and it spans `shutdown()`, which joins that thread from `atexit`. Every
  sink therefore waits on the worker's stop event rather than `time.sleep`, so a shutdown cuts an
  in-progress backoff short; a server-supplied `Retry-After` is clamped (it is advice from a
  destination, not an instruction the application must obey); and `shutdown()` itself takes a
  timeout, because a sink blocked *in* a network call can still hold the thread. Each retrying
  sink's docstring states its worst-case total delay.

  **The signal reaches a sink with no worker too** (SPEC-033 FR-004). `Worker._offer_stop_signal`
  was the only caller, so an orphan-only process handed its sink nothing and the guarantee above
  was simply false there — a backoff on an application thread ran to completion through
  `shutdown()` and interpreter exit, and the inline close at exit could sit behind it via the
  emit lock. The orphan path now offers its own event, skipped only for a sink a **live worker
  owns**: overwriting the worker's event there would leave its drain thread serving a full
  backoff across the join, which is the pause this rule exists to remove, while skipping merely
  because a worker *exists* would strand a sink adopted after that worker retired. The event is
  replaced with a fresh one whenever it is already set — an `Event` never clears, and a sink
  still holding the shutdown's event has every subsequent backoff collapse to zero, which
  against a rate-limited destination is a tight retry loop. The contract is "cut short *by a
  shutdown*", not "never wait again".
- **"Retries with backoff on failure" means: on a failure the sink reports.** The worker can
  only retry what reaches it as an exception, so the guarantee is conditional on §8's
  raise-on-total-failure rule. A sink that swallows its own total failure gets no retry, no
  `failed_batches`, and a `flush()` that returns `True` — the library's loss reporting is the
  sink's contract as much as the worker's code (SPEC-026).
- **Loss the sink absorbs on purpose is reported, not retried** — a partially-failed batch, an
  oversized record, an unadjudicable response. `health().sink` carries the configured sink's
  `losses()` snapshot so the documented alert idiom covers it; it is nested rather than folded
  into the worker's own counters because `dropped` at the queue and `dropped` at the sink count
  different things. The sink's is what never reached the wire — usually an event that can never
  fit, but for the sinks whose client owns a local buffer (`KafkaSink`, `GooglePubSubSink`) also
  what that buffer refused, which is backpressure one layer further out.
- **A retired worker still receiving submissions is a reported state, not a prevented one**
  (SPEC-030). `shutdown()` is terminal and the worker never comes back, but `submit()` keeps
  accepting — so a process that logs again queues events nothing will drain. `health().retired`
  and `submitted_after_shutdown` name that pair, and the first such submission writes one
  throttled stderr line. It is a *pair* because `retired` alone is correct usage, and it is not
  `stopped_reason`, which stays `None` for a clean shutdown by SPEC-019's design and would
  otherwise make every well-behaved process read as failed. Neither refusing the submission nor
  restarting the thread was chosen: the second fights a process trying to exit, and the first
  would hide the mistake rather than surface it.

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


### 9.2 Four questions a guard can ask about the worker

Every guard in `decorator.py` that mentions the worker is asking exactly one of four questions,
and answering with the wrong one is this codebase's most repeated defect: three reviewers told
SPEC-033 "ownership, not liveness", each naming a different call site, each was fixed, and a
fourth shipped broken (SPEC-035 FR-001) — whose own first draft then prescribed a predicate that
would have re-broken SPEC-033 in the opposite direction.

| Question | Predicate | What it decides |
|---|---|---|
| **Existence** | `_worker is None` | is there anything to do, or a worker to build |
| **Liveness** | `_live_worker()` — not `None`, not `retired` | who **performs** an action; a retired worker performs nothing |
| **Ownership** | `_worker.sink is X` | who **owns** a close; a retired worker still owns its sink's |
| **Moment** | `Worker.draining` | whether this worker's `_stop` is *still* the sink's route to a cut-short backoff |

Liveness and ownership diverge the instant `retired` latches — which is **entry** to
`shutdown()`, not its completion. The moment is a third axis rather than a refinement of either:
`_offer_orphan_signal` needs ownership **and** the moment, because ownership alone hands a set
event to a sink still being written to (every later backoff collapses to zero) while liveness
alone strips the drain thread of the event it is about to wait on. One question is answered by a
**return value** rather than a predicate — `Worker.swap_sink` reports whether it adopted the
sink, because only the worker knows whether it got as far as reassigning.

The rule is enforced, not just written down: `tests/test_worker_predicate_roster.py` derives
every expression **in boolean position** naming the worker from `decorator.py`'s AST and fails
unless each one is declared with a category and a reason. Position rather than node shape is
load-bearing — `if _worker.retired:` asks exactly what `if not _worker.retired:` asks, and a
draft that matched on shape recognised only the second, which let a real guard through with the
whole suite green. Three limitations are measured and disclosed in the test rather than
hidden: the subject is recognised by **name**, so a guard whose local is called `owner` is
invisible until it displaces an existing row; a question hoisted through anything but a bare
boolean operator (`alive = bool(_worker)`, a tuple target, a container literal) is not followed,
though in a test position all of those are caught; and a lambda body is searched only when it is
itself boolean. `match` is likewise uncovered and unused here. The roster's failure mode is a
*missed* site rather than a wrongly classified one, which is the direction that stays findable. A new call site cannot be added without deciding which
question it asks. That is deliberately a *derived* roster and not a hand-written list, for the
reason the sink rosters are (SPEC-028, SPEC-032): the completeness is the point, and a
hand-maintained list rots.


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
  SPEC-002**: ~~`console.py` echoes to **stdout**~~ — **corrected by SPEC-031 FR-003**, which
  found the claim false against the code: `ConsoleWriter` has defaulted to **stderr** since it
  shipped, per the twelve-factor convention `StderrSink` cites (logs on stderr, the application's
  own output on stdout). Echo is opt-in per call with no automatic `echo_level`
  threshold. Auto-echo was declined rather than deferred — it would double every event's cost by
  default for a development convenience. The stream is bound at construction, so a later
  `redirect_stderr` is not honoured and an explicit `stream=` is how a test captures the output.
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

- **An orphan log can wait on the sink lock.** A sink holding mutable transport state serializes
  `emit` for the whole operation (§9, SPEC-028), so a level call made with no active span — which
  emits on the caller's own thread — can block behind an in-flight emit, including its retry
  backoff. This is a deliberate trade and the alternative is worse: unserialized, those same two
  callers corrupt the sink's state, which for `SQLiteSink` was measured crashing the interpreter
  outright rather than merely losing rows. It does not touch the traced path, where events go to
  the worker queue and the decorated function returns without waiting on anything. A caller who
  wants the orphan path off the critical path should open a span; that is what the
  buffer-then-flush pipeline is for.

- ~~**A process that only ever used the orphan path never closes its sink.**~~ — **fixed by
  SPEC-031 FR-006.** It was a defect awaiting a fix rather than a constraint this project accepts,
  and it sat here only until then. What it was: `shutdown()` was a no-op when no worker had been
  created (`_shutdown_worker` returned early), and the `atexit` registration happened *inside*
  `_get_worker`, so neither ran for a process that only made level calls with no active span —
  those emit synchronously on the caller's thread and build no worker. Measured: with a
  `KafkaSink`, `configure` → `info` → `shutdown` → `info` left `flushes=0` and the sink open, so
  every event was lost; against a *synchronous* sink the events landed and what was lost was the
  flush and the released resource. And **`health()` read all-clear** — `retired=False`,
  `submitted_after_shutdown=0`, `stopped_reason=None`, `sink=None` — because every one of those
  describes a worker, so SPEC-030's alert idiom was structurally blind to it. Found reviewing
  SPEC-032, whose post-close guard is invisible here precisely because `close()` never happened.

  What the fix is, and what it deliberately is not: the orphan branch in `api._log` records that
  an event *reached* the sink — not that a sink is configured, since `configure()` runs
  `_ensure_sink()` unconditionally and would arm a close over a `StdoutSink` nothing was ever
  written to. A single `atexit` handler covers both paths, so the close is once-only across them
  rather than the *registration* being once-only: a second handler would double-close (`atexit`
  runs LIFO) and reusing the worker's registration flag would cost a mixed process its exit drain.
  A live worker still owns the close and this defers to it. `health().retired` is synthesized from
  a module flag, with **no worker created** to answer it — the same refusal `_swap_sink` and
  `_flush_worker` already make. `submitted_after_shutdown` is deliberately *not* incremented on
  this path: SPEC-030 defines it as a submission queued where nothing will drain it, and a later
  orphan log is refused at a closed sink and announced, which is not the same claim.

  ~~**One variant is not fixed and needs its own home:**~~ — **closed by SPEC-033.** What it was:
  `configure(sink=A)` → `info()` → `configure(sink=B)` → `info()` left **A** unclosed with
  `incomplete_swaps` at zero, because `_swap_sink` returned early on a null worker. That early
  return was correct on its own terms — a process with no worker has captured no sink to *swap* —
  but it was the whole function for that path, so the handoff was unmanaged.

  What the fix is. The orphan path records the sink **object** an emit reached rather than a
  boolean saying one did: `configure()` assigns `_config.sink` before it calls the swap, so by
  the time anything could close the previous sink the config no longer names it. The record is
  **re-pointed** at the new sink rather than cleared — clearing leaves nothing armed until the
  next orphan emit, so a process that swaps and exits without logging again leaks the *new* sink,
  which is a case the boolean got right; re-pointing is also what the worker path does, since
  `Worker.shutdown` closes `self.sink` whether or not anything reached it since the swap. There
  is no drain and no fence: orphan emits are synchronous and have returned, and the one writer a
  fence could not exclude is the same one `Worker._close_swapped_out` documents itself as not
  covering, which is why `sinks/base.py` requires `close()` to tolerate a concurrent `emit`.

  Two things came with it that the same boolean had been hiding, both found by review of the
  spec rather than by the audit. `_orphan_sink_closed` made the close once per **process**, so a
  sink configured after `shutdown()` was closed by nothing at all — measured losing a
  locally-buffering sink's whole batch while `health()` read `retired=True`,
  `submitted_after_shutdown=0`, `failed_batches=0`, since SPEC-030's pair needs a worker to count
  a submission. The latch is now the closed sink's identity, so the rule is once per *sink*. And
  an orphan-only process never received a SPEC-027 stop signal — `Worker._offer_stop_signal` was
  the only caller — so "a shutdown cuts a backoff short" was false on this path.

  Two guards are keyed on **ownership** rather than on a worker existing, and the distinction is
  load-bearing: `Worker.swap_sink` returns early once `_shutdown_done`, so a retired worker keeps
  its old sink forever while every orphan event goes to a newly configured one. `_worker.sink is
  owed` still declines on an *expired* shutdown, which is what the original guard existed for.
  `incomplete_swaps` is untouched throughout — it records a drain that could not be confirmed,
  and there is no drain here.

- **A sink handed back after being swapped out is closed twice, on both paths.** The orphan
  path's closed-sink latch (SPEC-033) is a **single slot**, so it is the latch *moving* to a
  second sink that re-admits the first: `configure(A)` → `info()` → `configure(B)` →
  `configure(A)` closes A again. The **worker path behaves identically** — measured
  `A.closes=2, B.closes=1` — and `configure()`'s own docstring already says the previous sink
  "must not be handed back to a later call", so this is a documented user error rather than a
  divergence. `sinks/base.py` requires `close()` to be idempotent, which is what makes it
  tolerable. Tracking every sink ever closed would pin them all against garbage collection to fix
  what the sibling path does not fix either.

  Two further shapes need a concurrent emit during `configure()`, which that call's documented
  "not thread-safe" already covers, and are recorded because the single slot is what permits
  them. An emit that resolved sink A, was preempted, and resumes after **two** swaps finds the
  latch on B, re-arms A, and orphans the live sink. And an emit that resolved the old sink and
  arrives after the *worker* branch cleared the record re-arms it, so `Worker.swap_sink` closes
  that sink and the ownership guard — finding `_worker.sink` is now the new one — closes it
  again at exit. That one is **measured** (`A.closed == 2`, with a preemption point injected at
  `_ensure_sink`) and is **pre-existing**: it reproduces identically before SPEC-033, because the
  worker branch has never recorded a closed sink. The orphan branch's equivalent *is* closed, by
  `_orphan_closed_sink`, and a test pins it.

- **`Worker._release_waiters` reads `queue.Queue`'s internals, and there is no public
  alternative.** It takes `self._queue.mutex` and iterates `self._queue.queue` — both private —
  to find the `flush()` markers still queued when nothing will ever read them again, so no
  caller sits out its full timeout, and no caller who passed `timeout=None` waits forever. The
  audit that produced SPEC-024..031 flagged it; **SPEC-031 FR-005 records it rather than
  changing it**, per SPEC-021's rule that an open item is closed by being fixed, settled, or
  recorded. It is called on the terminal-failure path and, since the shutdown-sentinel fix, on
  the clean shutdown path too — the same enqueue-after-the-drain race reaches both.

  It stays a **read**. A write was built and reverted: the first fix for a stranded `_SHUTDOWN`
  sentinel rebuilt the deque without it, and rested on the claim that "nothing in this module
  ever blocks on `put`" — which is false, `flush()` uses a blocking `put` by design, so freeing
  capacity without notifying `not_full` would leave that caller parked with space available.
  Fixing the sentinel by **ordering** instead (queue it before setting `_stop`, and break the
  drain loop on it) makes stranding impossible while the drain loop is running, rather than
  repairable, and needs no write at all. The put is skipped for a thread that is already gone,
  since a terminally dead drain will never read a wake-up and one queued for it would strand
  permanently — the one path the ordering cannot reach, and not a silent one, because
  `stopped_reason` is non-`None` there. The gate is `_drain_finished` rather than `is_alive()`,
  because the thread stays alive throughout `_terminal_failure`, which writes to stderr and can
  block on a slow reader; the flag is set before that call.

  The residual is that a marker stranded by the sibling race is answered but left queued,
  so `health().queued` counts it; removing it would mean deleting a *specific* item, which
  `Queue` cannot do, and draining to reach it is not available either — post-shutdown
  submissions must stay queued, since that is exactly what `submitted_after_shutdown` reports
  (SPEC-030).

  Why no alternative: the markers must be *read* and not *consumed*, and **every public method
  `Queue` has either removes an item or reports only a count** — none of them inspects without
  removing. That property is stated rather than enumerated on purpose: a list here rots. It was
  written once as `get`/`put`/`qsize`, corrected in review to add `empty`/`full`/`task_done`/
  `join`, and was *still* incomplete, because Python 3.13 added `Queue.shutdown()` — which CI
  gates on. The property has held across every version; the roster has not. The draining
  alternative
  (get everything, answer the markers, put the rest back) would destroy the queued event-lists
  that `health().queued` and SPEC-019's terminal-failure line report as the evidence of what was
  lost. Snapshotting under the queue's own mutex is also what makes it impossible to miss a
  marker mid-iteration.

  What would break: a future CPython that renames or removes `Queue.mutex` or `Queue.queue`, or
  changes `queue` to a non-iterable container. The failure would be an `AttributeError` or
  `TypeError` inside `_release_waiters`, which swallows it — so the visible symptom would be
  flush waiters silently timing out after a terminal worker failure, not a crash. A test
  exercises the method against a queue holding a mix of markers and event-lists, so the change
  surfaces as a test failure on a new interpreter rather than as that silence.

- **A borrowed client outlives the sink that used it, and the sink refuses regardless.** Closing a
  sink built on an injected client (`SQSSink(client=…)`, `RedisListSink(client=…)`,
  `MongoDBSink(client=…)`) does **not** close that client: it is the caller's to release, and
  reaping a connection pool an application still uses elsewhere would be the library reaching
  outside its own lifetime. The consequence is that a closed sink's client will happily take a
  write, which is why the post-close refusal is keyed on the *sink* being released rather than on
  ownership (SPEC-032 FR-001). A guard keyed on ownership would leave every injected-client sink
  accepting after `shutdown()` — the majority configuration in tests and in any application that
  manages its own pool.

- **`shutdown()`'s timeout bounds the drain, not the sink's `close()`.** This narrows SPEC-027
  FR-004, and the narrowing is SPEC-028's doing: `close()` now takes the sink's emit lock, so an
  application thread parked on the orphan path inside a driver call with no timeout of its own
  delays the close with no ceiling. `shutdown(timeout=...)` bounds `thread.join()` and, since
  SPEC-030, the grace it grants a swapped-out sink's close — but not this close, the live sink's,
  which stays inline and unbounded. Running *this* close on a joinable daemon thread was built and
  **reverted**: at
  interpreter exit that daemon is killed wherever it has reached — and for `SQLiteSink` that can
  be *inside* `commit()`, which is the partial write FR-004 exists to avoid rather than the
  leaked handle it knowingly accepts — and it could not tell a slow-but-successful close from a
  stuck one, so it reported `ShutdownTimeout` and "left open" for closes that had completed.
  A wrong signal is worse than a slow one. Bounding this properly needs the sink's `close()` to
  be interruptible, which is a change to the sink contract rather than to the worker.

  ~~**The same gap reaches `configure(sink=...)`**~~ — **closed by SPEC-030's follow-up.** It did:
  a late sink swap closed the previous sink inline on the caller's thread, so a `KafkaSink` whose
  broker was unreachable blocked `configure()` inside `producer.flush()` (measured at 8.0 s against
  a 5 s budget), and any SPEC-028 locking sink blocked behind an orphan-path writer holding the
  emit lock. The swap's deadline now covers that close too (§7).

  **What made the fix available here** is that the wrong-signal objection is dissolved rather than
  argued with: **nothing is derived from an expired join.** No counter moves and no line is
  written, so a slow close can never latch a loss on a healthy swap — `incomplete_swaps` keeps its
  narrower meaning, a *drain* that could not be confirmed. The operator is not left blind, though:
  `health().closing_sinks` reports the closes running at the instant it is read, a live fact rather
  than an inference from a timeout.

  **The closer is a daemon, and neither thread flag is sufficient on its own** — both were built
  and measured. Non-daemon: CPython joins non-daemon threads *before* running `atexit`, so a single
  hung close stopped the exit drain from ever running — the **live** sink never drained or closed,
  its buffered events lost, the application's own exit handlers never run, and the process hung
  until killed. Daemon alone loses the opposite case: a close that is slow but *succeeding* is
  killed at exit, and for a sink whose `close()` **is** its delivery (`KafkaSink.close()` flushes
  the producer) that is its whole buffer — measured, the same swap kept those events under a
  non-daemon thread and lost them under a daemon.

  So the flag is not the mechanism; **the capped grace is.** `shutdown()` drains and closes the
  live sink, then joins any outstanding closer for `DEFAULT_CLOSER_GRACE`, carved from its own
  budget. A slow close finishes and a hung one costs only the grace. The cap is what does the
  work: without it a stuck close would hold a process at exit for the whole 30 s shutdown budget,
  and a close still running here already had the swap's entire budget, so it is far more likely
  stuck than slow. Running *after* the live sink's close is defence in depth rather than the
  guarantee — measured, both orders deliver the live sink identically, because the cap returns
  control long before anything is at risk. It is still the right order, and pinned by a test: it
  is what holds if an external deadline kills the process during the grace.

  Two residual costs, recorded rather than hidden. First, a `close()` still running after the grace
  is abandoned, losing that sink's own tail, with `closing_sinks` as the only warning. Second — and
  this is `_close_if_owed`'s objection genuinely reaching a swap, where it did not while the close
  ran inline — an abandoned close is killed *wherever it has reached*, which for `SQLiteSink` can
  be inside `commit()`. That is the partial write SPEC-027 FR-004 ranks worse than a leaked handle,
  now reachable here, and the grace is what makes it unlikely rather than impossible: the sink must
  still be stuck after the swap's budget *and* the grace, which for a swapped-out SQLite connection
  means an orphan-path writer has held its emit lock across both. Neither weakens `shutdown()`,
  whose close stays inline. Configure the sink before the first log where you can — that path has
  no worker and nothing to close.

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

- **A context released at a root span is released in *that span's* context.** Baggage and the
  adopted trace context are taken back out when the root span closes (SPEC-024), but the write
  lands in whichever `contextvars` context the span's `finally` runs in. Adopt *outside* a span
  and then dispatch it into a child context — any `asyncio.Task`, including the one
  `asyncio.run` creates — and the clear lands in the copy while the parent keeps the adoption.
  `contextvars` offers no way to write to a parent context, so this cannot be fixed in the
  library; it is closed by documentation and a test that pins it. The documented placement —
  `continue_trace()` on the entry point's first line, inside the span — is unaffected, and
  `reset_context()` is the remedy for a caller who adopts before dispatching.

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
  invocation on a warm container would log. That mistake is no longer silent — `health().retired`
  with a non-zero `submitted_after_shutdown` is its signature (§9, SPEC-030).

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
