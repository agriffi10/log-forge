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
- **A task that outlives its span** takes that same orphan path (SPEC-036 FR-004). `contextvars`
  copies the *same* `Span` object into every task created inside a span, so a fire-and-forget
  `create_task` can log after its parent has returned and its buffer has been handed to the
  worker. The span carries a `closed` flag set at close, and `api._log` reads it **at append
  time** — the only place that can, since nothing in the library looks at a span again once
  `_close_span` returns. The event is therefore delivered as a standalone one-event span rather
  than appended to a buffer nothing will emit, and it is counted in `orphan_lost` if the emit
  fails. What it gives up is its correlation: a fresh `trace_id`, so it leaves the trace and not
  just the span, and no ordering guarantee relative to the `span.end` that preceded it. Await the
  task inside the span if its logs must stay in the trace.

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
- **A value that cannot be *rendered* is replaced, never passed through or clipped.** Two cases,
  and they share one rule: an integer past `sys.get_int_max_str_digits()` becomes
  `<int: ~N digits>` (SPEC-020), and a non-finite float becomes `<float: nan>` / `<float: inf>` /
  `<float: -inf>` (SPEC-037 FR-003). `json.dumps` writes `NaN` and `Infinity` happily and RFC
  8259 defines neither, so passing them through hands a strict consumer a record it rejects
  whole — the same shape as the integer, which raises instead. Both keep *which* value it was
  rather than coercing to `None` or `0.0`: a wrong number is worse than a visibly elided one.
  Both set `truncated: true`, because a substitution nobody can see is a silent change to the
  caller's data. Both live in `sanitize`, not at the sinks — one pass at assembly is correct by
  consequence for all 40-odd `json.dumps` call sites and reaches the non-JSON sinks too.
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
`defaults` and the payload ceilings are looked up through `config._live_config()` as each event
  is built — the internal read, which hands back the live object rather than the copy
  `get_config()` owes a caller (SPEC-034 FR-003), so
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
    # optional, each probed by name — never required, so no sink stops satisfying this:
    # def losses(self) -> SinkLosses | None: ...      # cumulative loss absorbed (SPEC-026)
    # log_foundry_stop_signal: threading.Event | None # interruptible backoff (SPEC-027)
    # def reacquire_after_fork(self) -> None:  # strand a forked child's inherited
    #                                                 # buffer (SPEC-039)
```

`sinks/base.py` is authoritative for all three — what each promises, and for the last one which
sinks are actually asked, which is narrower than "whichever define it".

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

**An explicit `flush()` drains three places, not one** (SPEC-036): the worker's queue, the events
still buffered on spans open in the **calling context** (FR-001), and whatever the sink is holding
in its own **client** (FR-002 — `KafkaSink` hands to librdkafka, `GooglePubSubSink` to an
unresolved future, `SentrySink` to the SDK's background transport, `LoggingSink` to a handler
chain). The third runs **after** the queue drain, because the queue's events have to reach the
client buffer before it is emptied, and it is an optional `flush()` probed by `sinks.base.flush_sink`
— which **propagates** a failure where `read_losses` swallows one, since a swallowed flush failure
is a sink the worker believes. A wrapper sink forwards it, or a buffering child is unreachable
behind it (the SPEC-027 lesson about the stop signal, repeated). Without the second, a
`flush()` made inside a `@trace`d function had by construction nothing to drain — an in-span
event lives on `span.events` until the span closes — so the serverless recipe the README
published delivered nothing while every counter read clean.

The sweep leaves each span open, hands its buffer to the worker by **swap** (clearing would empty
the same list object the worker was just handed), and completes the boundary events' baggage
*before* they leave, because SPEC-015 does that at close by iterating `span.events`. A swept
`span.start` therefore carries the baggage as of the flush rather than as of the close, which is a
real semantic change and the alternative is mutating an event the worker already owns (§9.2,
SPEC-028). It builds the worker when it has something to submit, which narrows SPEC-013's
"a process that never logged has nothing to drain" rather than contradicting it.

Its bound is the calling context, and that is honest rather than incidental: `contextvars` offers
no way to enumerate another thread's or task's context. A span swept this way is marked, and a
later `continue_trace()` on it refuses the trace context rather than re-parenting a buffer whose
events have already left — otherwise one span carries two trace ids, which is the SPEC-024
category of wrong data rather than lost data.

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
- **A forked child repairs itself, in a fixed order, and only the child** (SPEC-039). `fork`
  copies the memory and leaves the threads behind, so a child inherits a worker whose drain
  thread does not exist and locks held by threads that do not exist — measured, the child's
  events were never delivered while `health()` read clean on every term, and 19 of 60 children
  hung permanently in `info()` on the application's own thread. `_fork.py` registers **one**
  `os.register_at_fork(after_in_child=…)` handler, whose order of work is the contract:

  1. **Re-initialise every lock and event the library owns**, found by walking this package's
     modules and descending only into objects it defines and plain containers. A lock that was
     not held is replaced too — asking whether one is held has no answer that is not itself a
     race — and an identity memo keeps two holders of one primitive sharing it, which is what
     stops a sink's `log_foundry_stop_signal` drifting from the worker's `_stop`. An AST lint
     forbids building a primitive anywhere the walk cannot write it back, so a lock added by a
     later spec is picked up with no edit.
  2. **Re-acquire what can be re-acquired.** A fork landing inside `emit`, after the write loop
     and before the flush, leaves both processes holding the same pending bytes; the child
     strands its copy (`dup2` to `/dev/null`, then reopen in **append** mode) through the
     optional `reacquire_after_fork()` hook `sinks/base.py` documents (§8). The name says the
     larger half (SPEC-042 FR-005): a sink that returns from it has claimed the transport as
     **this process's own**, which is what makes releasing it safe later. Which hooks returned
     is published, not acted on — ownership is `_lifecycle`'s.
  3. **Run the registered handlers**, `_lifecycle`'s inherited-sink marking first and
     `decorator`'s worker rebuild after it. The worker is rebuilt **in place** with a fresh
     queue and zeroed counters, so ownership guards keyed on `_worker.sink is X` survive; a
     retired parent forks a retired child, since a fork does not undo a `shutdown()`.

  The order is the whole of it: a lock re-initialised *after* a handler that takes it is a
  handler that hangs, on the child's only thread with nothing left to interrupt it. Nothing is
  registered for the parent side — `before` does not run for a C-level fork at all (uWSGI calls
  `PyOS_AfterFork_Child` only), so the child handler has to be sufficient regardless, and a
  parent-side handler would buy a partial fix for a measured 1.20 s hold on the forking thread.

  **The child's contract is three verbs: repair, deliver, and release only what it acquired
  here** (SPEC-042). The first two are the steps above. The third is the one a fork makes
  non-obvious: the child inherits the parent's sink *object* — one socket, one SQLite handle,
  one file, two processes — and beyond the re-acquisition the library neither clones nor
  re-opens it, so closing it is the parent's transport going away. It used to do exactly that,
  twice over (§13, both struck). Now the library records which process was **handed** each sink,
  at `configure()` and at the lazy default, and every close it performs consults that record —
  the three lifecycle sites and the five shipped wrapper sinks alike. A child refuses the object
  it inherited and closes the one it built itself.

  **Unrecorded has to be unclaimable, not merely unreleasable**, which is why the child marks
  what it inherited *before* any handler runs. Write-once alone defends only a record that
  already exists, so where the parent's walk recorded nothing a child could `configure()` its
  way into genuine ownership and destroy the transport legitimately — measured, through a
  third-party wrapper the stamp walk may not descend into.

  The deployment advice survives as the **recommendation** rather than as an "or else": build a
  connection-holding sink in the worker process. Call `configure()` from gunicorn's `post_fork`
  rather than at preload, or give the master a sink whose `close()` costs nothing to share.
  `README.md` says the same where a user deploying prefork will find it.

  Reconfiguring in the child is no longer harmful for a sink the library was handed here **and
  whose ownership the record therefore decides correctly** — the child either refuses it, or has
  re-acquired a copy of its own through the hook and is closing that. Two exceptions, and both
  are §13 residuals rather than wrinkles in the wording. A master that *builds* a connection sink
  and never configures it leaves the child as the first process to hand it over, so the child
  acquires it legitimately and closes it (#1). And a sink that **returns from the hook without
  having re-acquired everything it holds** — the reachable case being a subclass of a shipped sink
  that adds a transport — has told the record it owns the whole object, so the child closes the
  part the parent class never knew about (#2). Note the mechanism differs between the safe cases:
  refusal covers the sinks with no hook, and re-acquisition covers the ones with it. Naming only
  refusal would be wrong for every `FileSink`, which reads *releasable* in the child by design. Third-party state is out of scope by construction — a driver's locks, threads
  and descriptors are not the library's to swap, and reaching into them would be a fork fix
  that breaks a driver (§13).

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

Every guard that mentions the worker is classified as exactly one of four categories, and
answering with the wrong one is this codebase's most repeated defect: three reviewers told
SPEC-033 "ownership, not liveness", each naming a different call site, each was fixed, and a
fourth shipped broken (SPEC-035 FR-001) — whose own first draft then prescribed a predicate that
would have re-broken SPEC-033 in the opposite direction.

Since SPEC-040 each question is **one method on `_lifecycle._state`**, and a call site *selects*
one rather than composing a predicate. The state and the guards live in `_lifecycle.py`;
`decorator.py` keeps the decorator and the span machinery and reaches the worker only through
`_lifecycle._get_worker()`.

| Category | Method | What it decides |
|---|---|---|
| **Existence** | `_state.worker_exists()` | is there anything to do, or a worker to build |
| **Liveness** | `_state.live_worker()` — not `None`, not `retired` | who **performs** an action; a retired worker performs nothing. Reading `retired` to *report* it is the same question, not a fifth |
| **Ownership** | `_state.worker_owns(sink)` | who **owns** a close; a retired worker still owns its sink's |
| **Ownership ∧ moment** | `_state.worker_owns_now(sink)` | whose stop event the sink should be holding **now** |

**None of the four takes the lifecycle lock.** Four guards ask a question with it already held
(`_get_worker`'s inner check, `_close_orphan_sink`, `_swap_sink`, and `_offer_orphan_signal`
through all three of its callers), so a non-reentrant acquire inside a question would deadlock
them; and `_get_worker`'s outer check is deliberately unlocked on the `@trace` hot path. Each
read is a single atomic reference load, and a caller needing consistency across two reads takes
the lock itself. The added call costs ~6.6 ns, measured, against an ~18.6 µs span.

The fourth is a **conjunction**, not a new question, which is why it is named for both terms
rather than for `Worker.draining` alone. A category named for the moment by itself would have no
site — the moment never decides anything on its own here — and a contributor reaching for one
would be reaching for something the roster does not accept.

Liveness and ownership diverge the instant `retired` latches — which is **entry** to
`shutdown()`, not its completion. The moment is independent of both rather than a refinement of
either:
`_offer_orphan_signal` needs ownership **and** the moment, because ownership alone hands a set
event to a sink still being written to (every later backoff collapses to zero) while liveness
alone strips the drain thread of the event it is about to wait on. One ownership question is
answered by a **return value** rather than a predicate — `Worker.swap_sink` reports whether it
adopted the sink, because the decline is taken between its two lock acquisitions and nothing
outside observes it.

The rule is enforced, not just written down: `tests/test_worker_predicate_roster.py` derives
every expression **in boolean position** naming the worker from the ASTs of `_lifecycle.py`
**and** `decorator.py` and fails
unless each one is declared with a category and a reason. Position rather than node shape is
load-bearing — `if _worker.retired:` asks exactly what `if not _worker.retired:` asks, and a
draft that matched on shape recognised only the second, which let a real guard through with the
whole suite green. Three limitations are measured and disclosed in the test rather than
hidden: the subject is recognised by **name**, matched as a substring, so
`if owner is None:` is invisible while `if owner.retired:` is caught and `if networker:` is
over-matched; a question hoisted through anything but a bare boolean operator
(`alive = bool(_worker)`, a tuple target, a container literal) is not followed, though in a test
position all of those are caught; and a lambda body is searched only when it is itself boolean.
`match` is likewise uncovered and unused here. Each is a scope decision: following every value an
arbitrary expression could carry is a walker nobody can reason about. A new call site cannot be added without deciding which
question it asks. That is deliberately a *derived* roster and not a hand-written list, for the
reason the sink rosters are (SPEC-028, SPEC-032): the completeness is the point, and a
hand-maintained list rots.

What it is complete about is guards that **name the worker**, which is narrower than the
ownership question itself. The orphan path decides who owns a close with no worker in the
expression — `_note_orphan_emit`'s `sink is _state._orphan_sink`, `_close_orphan_sink`'s
`owed is None`,
`_adopt_declined_swap`'s re-arm guard, `_swap_sink`'s `old is None or old is new_sink` — and none
is filed. SPEC-035 FR-003's own defect lived in that family, as an assignment rather than a
predicate, so a green roster is not evidence about it. Widening the sentinels to reach it would
match every sink comparison in the module, so it is recorded here instead of quietly extended.


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

This list predates the first line of code; the three questions it carried are settled under
*Resolved*, and SPEC-021 reconciled it so that nothing here reads as open unless it is. An item
leaves this section by being *resolved* (with the spec that settled it) or by moving to §13 as a
stated constraint — never by being deleted quietly.

**What belongs here rather than in §13.** §13 states what the design *will not* do — limits that
are correct and permanent. This section holds what is genuinely unfinished: a defect nobody has
scheduled, or a question deliberately left for later. The two had drifted together, and until
SPEC-045's completion this heading read "**None.**" while §13 carried four such items — a register
saying it was empty when it was not. Each entry below names what would close it.

### Open

- **A sink can be released while it is still live inside the graph that replaced it.**
  `configure(A)` then `configure(MultiSink(A, B))`, or the reverse: the swap closes A as the
  outgoing sink while A is a child of the new one. Pre-dates SPEC-045, which pinned only that it
  must not become a *loss* — A keeps taking events through the wrapper, is owed a further close,
  and delivers everything. **Closed by** not releasing a sink reachable from the graph just handed
  over, which is decidable only against a live config read.
- **The exit close runs the owed sinks inline and in sequence**, so one slow close delays every
  other owed close and one that never returns takes the rest with it — measured, a 5 s close made
  `shutdown(timeout=1.0)` return after 5.01 s. Introduced by SPEC-045, which made the owed-close
  record a set without changing how the set is drained. **Closed by** bounding or parallelising
  that drain; it is not the same limit as the unbounded `Sink.close` in §13, which is a protocol
  constraint rather than a scheduling one.
- **Whether the predicate roster still earns its weight** (SPEC-040 FR-005 AC-1).
  `tests/test_worker_predicate_roster.py` is ~1,500 lines policing what is now four methods, and
  roughly half of it is the seam lint guarding the prose in its own data table rather than the
  rule. The case for retiring it got *weaker* during SPEC-040: widening its scope immediately
  filed two sites that had gone unfiled for two specs. **Closed by** evidence a year of
  maintenance provides — whether any post-SPEC-040 defect was caught by it, or by nothing.
- **`worker.py` (~1,370 lines) and `_lifecycle.py` (~1,610) are unsplit and unscheduled**
  (SPEC-040 FR-005 AC-2). `Worker` owns the drain thread, the queue, the retry, the counters, the
  swap and the shutdown; the questions *inside* it are one object's own state, which is why
  SPEC-035 FR-002 drew the roster's module boundary where it did. Neither is a defect; both are
  recorded so the next reader knows the split was considered. **Closed by** a spec that does it,
  or by a decision that the shape is right.

### Resolved

- **Orphan logs** (emit standalone with a fresh `trace_id` vs warn-and-drop) → **emit standalone**,
  shipped in **SPEC-002**. A level call with no active span builds a complete event with a fresh
  `trace_id` and emits it synchronously on the caller's thread. Dropping it would make the emitters
  silently conditional on decorator placement, which is the opposite of what a logging call should
  promise. That synchronous path is also why `sanitize` must be total (SPEC-017), and why it needs
  a loss counter of its own: `Health` describes a worker and this path has none, so until
  **SPEC-036 FR-003** a process logging only this way reported all zeros over total, permanent
  loss. `orphan_lost` covers everything inside that guard — a sink that fails to construct as well
  as one that raises — and `in_span_lost` is its counterpart for an event that could not be *built*
  inside a span. Two fields, because one can mean the destination or the data and the other can
  only mean the data.
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

**What this section is.** A limit the design accepts and will keep accepting — usually because the
alternative was built and measured worse, or because fixing it would change a published contract.
It is **not** a backlog: something genuinely unfinished belongs in §12, which names what would
close it. A closed item is struck in place with the spec that closed it (SPEC-021's rule) and its
reasoning is *not* repeated here — that lives in CLAUDE.md's Key Decisions and the delivery doc,
and a third copy is a fork with no merge.

- **An event logged from a task that outlives its span leaves its trace.** `contextvars` copies the
  same `Span` object into every task created inside a span, so a fire-and-forget `create_task` can
  log after its parent returned. Since **SPEC-036 FR-004** the span's buffer is detached at submit
  and the span carries a `closed` flag, so that append is routed to the orphan path at append time
  rather than landing in a buffer nothing will emit — it is delivered, and counted in `orphan_lost`
  if it is not. **One window is narrower but not closed:** `api._log` reads the flag, then builds
  the event, then appends, and that sequence is not atomic against the close. A thread sharing the
  span (via `copy_context().run`, since a bare thread starts with a fresh context) can be inside
  `build_event` when the span closes, and its append then lands in the post-swap list — lost, with
  neither counter moving. Two asyncio *tasks* cannot reach it, because they cannot interleave
  inside a synchronous `_log`. It is recorded rather than closed because the fix is a per-span lock
  on the hottest path in the library, and the exposure is a shared-span thread logging at the exact
  instant of the detach.

  **SPEC-036 FR-001 widens this from the close to any flush.** The sweep performs the same detach
  on an **open** span, so `span.closed` is `False` and the orphan route the paragraph above relies
  on never engages — a racing append lands in the post-swap buffer and is delivered only if that
  span is swept or closed again. The character of the exposure changes with it: a close happens
  once per span, while FR-001 exists precisely so a long-lived span can be flushed in a loop. The
  window is still load-then-store on one attribute.

  **Two windows are involved and they take different fixes, which an earlier note conflated.** The
  *detach-vs-detach* race — a sweep and a close both rebinding `span.events` — is closed:
  `_sweep_lock` is taken by both `_sweep_open_spans` and `_flush`. One statement made that gap
  narrow rather than closed (no `CALL` between the load and the store, so today's GIL cannot switch
  inside it: 0 of 500 unforced trials, 10 of 10 with an opcode-level preemption), and
  `requires-python` has no upper bound, so a free-threaded build removes the accident. The lock
  costs +0.4% single-threaded and +0.9% across eight threads on the traced path, which is noise.
  The *append* race above — `api._log` reading the flag, building, then appending — is the one that
  stays open: closing it needs a **per-span** lock on the hottest path in the library, which is a
  different instrument and a real cost, and it is declined here rather than in passing.

  `Span.swept` is not a synchronization primitive either, and the guarantee it carries is
  single-threaded. `continue_trace` reads it and then re-parents across an ordinary function call,
  so a sweep landing in that gap still splits one span across two trace ids — reproduced 10 of 10
  when forced, 0 of 400 unforced, and reachable only with a shared span plus a concurrent
  `continue_trace` and `flush()`. The flag is now set *before* the detach, which narrows the
  reader's window and errs toward refusing, but only a lock held across the guard and the re-parent
  would close it. What it cannot keep is its correlation: it becomes a standalone one-event span with
  a **fresh `trace_id`**, so it leaves the trace it was logically part of, not merely the span.
  `contextvars` offers no way to recover the parent once the span is gone, and the choice is
  against losing the event outright. Ordering is not promised either. A task whose logs must stay
  in the trace should be awaited inside the span.
- **An orphan log can wait on the sink lock.** A sink holding mutable transport state serializes
  `emit` for the whole operation (§9, SPEC-028), so a level call made with no active span — which
  emits on the caller's own thread — can block behind an in-flight emit, including its retry
  backoff. This is a deliberate trade and the alternative is worse: unserialized, those same two
  callers corrupt the sink's state, which for `SQLiteSink` was measured crashing the interpreter
  outright rather than merely losing rows. It does not touch the traced path, where events go to
  the worker queue and the decorated function returns without waiting on anything. A caller who
  wants the orphan path off the critical path should open a span; that is what the
  buffer-then-flush pipeline is for.

- ~~**A process that only ever used the orphan path never closes its sink**, and a sink
  adopted after `shutdown()` is closed by nothing.~~ — **fixed by SPEC-031 FR-006 and
  SPEC-033.** The reasoning is in CLAUDE.md's Key Decisions ("The close is once-only across
  both delivery paths" and "A sink handoff is owned by whoever is delivering") and in
  [SPEC-031](spec-delivery/SPEC-031-audit-small-corrections.md) and
  [SPEC-033](spec-delivery/SPEC-033-orphan-path-sink-handoff.md); it is not repeated here.

- ~~**A sink handed back after being swapped out is closed twice, on both paths**, and two
  concurrent `configure(sink=…)` calls double-close a sink.~~ — **the entry was wrong in both
  halves, and SPEC-045 corrects it rather than deleting it, because it was believed and acted
  on.** A sink that takes an event *after* its close has something new to flush, so a second
  close is owed rather than spurious: refusing it was built and measured stranding 2 of 3
  events on a wrapper graph and losing on 31 of 80 lifecycle-fuzz seeds against 0 before.

  The real defect was the opposite one — **the live sink closed by nobody**. The record of
  which sinks the orphan path owed a close for was a single slot, so arming a second discarded
  the first: measured with every `configure()` call sequential on one thread and only an
  ordinary `info()` racing, `C.closes == 0` on the sink every event was going to. No lock
  around `configure()` reaches that, which is why its documented "not thread-safe" was not the
  explanation. `_orphan_owed` now holds every owed sink and needs no record of *closed* ones,
  so the pinning objection this entry used to carry never applied.
  Full reasoning in [SPEC-045](spec-delivery/SPEC-045-every-owed-close-is-performed.md).

  **The limit that remains:** `health().inherited_sink` reads the record's **last** entry,
  which arming order does not make the installed sink — `_swap_sink` seeds the record with the
  new sink and a preempted emit then appends the superseded one, so the order can be
  `[live, superseded]`. The answer is unchanged from before SPEC-045; the config is the
  authority for "installed", and correcting the field is its own change.

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
  which stays inline and unbounded. **It reaches the orphan path as well as the worker's**
  (SPEC-044 FR-006): a process that only ever logged outside a span closes through
  `_close_orphan_sink`, which calls `release(owed)` inline before `_shutdown_worker` consults its
  deadline at all. Measured 6.01 s against `shutdown(timeout=2.0)` on both paths, with a
  6-second `close()` — the 30.01 s figure recorded below came from a 30-second one, and the
  elapsed time tracks the close, never the timeout. A test now pins both paths, so a later change
  that bounds this close fails until the documentation moves with it. Running *this* close on a
  joinable daemon thread was built and
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

- **A diagnostic can be written while the lifecycle lock is held (SPEC-035 FR-006).** The
  process-wide lock in `_lifecycle.py` is released before anything that blocks on a destination —
  `_lifecycle.release(owed)` runs outside it, and a detached release only starts a thread — with one exception:
  three sites can reach `_diag` while still holding it, so a wedged stderr stalls every orphan
  emit and every first `@trace` in the process behind it.

  - `_note_orphan_emit` → `_offer_orphan_signal` → `_lifecycle.offer_stop_signal` → `_diag.absorbed`
  - `_adopt_declined_swap` → the same path (this site arrived with SPEC-035 FR-003)
  - `_swap_sink` → that path, **and** `_lifecycle.release(..., detached=True)`, which writes on a thread-start
    failure, also under the lock

  `_close_orphan_sink` is deliberately not one — its `_diag` write sits outside the `with`.
  It is an **error path only**: `offer_stop_signal` writes just when a sink's `log_foundry_stop_signal`
  setter objects, and a detached release just when the interpreter refuses a thread. The fix —
  returning a flag and writing after the release — spreads one diagnostic decision across two
  functions at all three sites to save a write that happens only then, so it is **recorded rather
  than taken**. `Worker.submit` is the counter-example and is deliberately inconsistent with
  these: it writes its queue-full line *outside* its own lock for exactly this reason, which is
  the shape to copy if a fourth site ever sits on a hot path. Found as C5 by the 2026-08-07
  audit; the AC that recorded it named two sites, and re-auditing the rule rather than the line
  (`docs/process.md`) found the third.

- **A fork's repair stops at the library's own objects, and five things sit outside it**
  (SPEC-039 FR-005). The child's walk descends into what this package defines and the plain
  containers those objects hold (§9); everything below is reachable only by reaching into
  somebody else's state, which would be a fork fix that breaks a driver.

  - **A third-party client that buffers across `emit`.** `KafkaSink`'s producer holds a local
    batch and `GooglePubSubSink` holds unresolved futures, so a fork mid-batch there can still
    duplicate what the parent had queued or strand it in a child that never resolves it. This is
    a *different* hazard from the shared handle below — that one is the caller's object, this is
    a buffer nobody in this process can address — and the `reacquire_after_fork()` hook
    cannot help, because there is no descriptor to redirect.
  - **`StdoutSink` carries the file sinks' duplication hazard and is deliberately not fixed.**
    It flushes once per batch exactly as they do, so the window is identical — but `sys.stdout`
    is a **process**-owned buffer, and discarding the application's pending output to protect
    the library's own is not a trade a logging library may make. This occupant was not expected
    when the hook was designed and is the reason it is per-sink rather than global.
  - **A third-party sink's own locks are not repaired, and its hook is never called.** A sink
    satisfying `Sink` *structurally* — which is how every shipped sink satisfies it — is outside
    the ownership test, so a lock it holds stays held by a thread that does not exist and a
    buffer it owns stays inherited, even when a wrapper this library owns is holding it.
    Inheriting from `Sink` or from a shipped sink is what brings it inside. The one consequence
    that would otherwise break an earlier guarantee is closed separately: the worker rebuild
    **re-offers** its fresh stop signal, so SPEC-027's interruptible backoff still holds for a
    sink the walk cannot enter.
  - **The converse costs a mixed-base class its foreign attributes.** Ownership is keyed on the
    whole MRO, because subclassing a shipped sink is a documented extension point and a
    defining-module test walked straight past a `_lock` that `FileSink.__init__` built — measured,
    the child hung. So `class MySink(FileSink, ThirdPartyBase)` has its *foreign* primitives
    replaced too, measured. A separately **held** client is still untouched; the two cannot be
    told apart from the instance, and refusing the mixed case would mean refusing the sink's own
    lock with it.
  - **A sink the application holds but never gave the library is not reached at all** — the walk
    starts from this package's modules. Same accepted boundary the locks already carry.

- **A forked child releases only a transport it acquired in *this* process, and what that
  cannot decide is listed here** (SPEC-042). The record is stamped when the library is handed a
  sink — `configure(sink=…)` and `_ensure_sink()`'s lazy default, over the whole reachable sink
  graph — and every close the library performs consults it. A child marks everything it
  inherited before any fork handler runs, so "no record" is unclaimable rather than merely
  unreleasable. Eight things it does **not** settle:

  1. **A sink the parent held only in application state.** Build a connection sink at import in
     a gunicorn master, never hand it to the library, and let the child's `post_fork` call
     `configure(sink=that_object)`: the child is the *first* process to hand it over, so it
     acquires it legitimately and closes the parent's transport at exit. Measured. Undecidable
     inside the rule — FR-001 releases what it was handed, and FR-001 AC-3 requires a child's
     configured sink to be releasable — and nothing distinguishes the two without marking the
     whole heap. **This is the only route where the library was never told** — a 17-shape
     claiming matrix refuses everywhere else. It is not the only remaining destructive close:
     #2 below is the other, and there the library was told something *untrue*. Both end with the
     record answering `True` through its recorded branch; what differs is the input, and that is
     what picks the remedy — deployment discipline here, a subclass honouring its contract there.
     §9's advice covers both.
  2. **A sink that returns from `reacquire_after_fork()` without having re-acquired everything
     it holds.** Returning normally *is* the claim and the library cannot check it. The reachable
     case is inheritance: `class MySink(FileSink)` that also holds a socket inherits a hook which
     re-opens only the file, returns, and thereby claims the whole object — measured, the child
     then closes the parent's connection. `sinks/base.py` states the obligation; a subclass that
     cannot honour it should define the member to raise, which is refused and therefore safe.
     It must be the sink the library holds **directly**: inside a `MultiSink` the wrapper is
     refused first, so the over-claiming child is never reached and the transport survives
     (measured). Listed here rather than with the leaks below because it is the opposite
     polarity — an over-claim, not a refusal.
  3. **A sink the library was never handed at all** — a wrapper mutated after `configure()`
     walked it. Refused through that wrapper, so it leaks a handle rather than being closed on a
     guess. No shipped sink mutates itself after `__init__` (AST-scanned).
  4. **A re-acquired child under a refused wrapper.** Only the children implement the hook, so a
     child inheriting `MultiSink(FileSink, FileSink)` re-stamps the two files while the wrapper
     keeps the parent's mark and stays refused — leaving them reachable only through something
     nothing will release. A leak; nothing is lost, since `emit` flushes per batch.
  5. **A third-party wrapper's own `close()`.** A user's wrapper closes its children directly and
     the library never sees the call, so the refusal cannot reach it. Same boundary SPEC-039
     FR-005 draws; the remedy is the one §9 gives.
  6. **The stamp walk's container bound.** It scans a container one level and does not recurse,
     which is what keeps `configure()` at ~2 ms against a measured 1,109 ms unbounded on a
     `MemorySink` holding 100k events. A sink two container hops below an owned holder is
     therefore unrecorded — refused, so leaked rather than closed. Its sibling trade: the walk
     tests sink-shaped before container-shaped, so an owned *non-sink* container subclass, and a
     container-subclass sink's own members, lose reach there. Neither is a destructive close, and
     the child's marking walk compensates.
  7. **`_marking_failed` catches an escaping fault and the visit ceiling, not a partial walk.**
     Every read inside the walk is absorbed a level down in `_fork`, so a walk that quietly
     reached less than everything raises nothing and leaves the flag clear; the sinks it missed
     are unrecorded rather than marked.
  8. **The record never shrinks.** One entry per sink ever handed to the library, each pinned by
     a strong reference — required, because an `id` is reusable once its object dies and a
     collected sink closes itself. Startup-scale in an application, since `configure()` is a
     startup call.

  What a refused close costs, measured from the shipped `close()` bodies: `KafkaSink` and
  `GooglePubSubSink` do not deliver their buffer, `NATSSink` does not drain its loop, and
  `SQLiteSink` and `PostgresSink` do not **commit** — for those two the child's inserts are left
  uncommitted on a connection the parent also holds, which is nonetheless the safer outcome,
  since committing there writes into a transaction the parent may be mid-way through. A
  flush-without-release hook **shipped in SPEC-036 FR-002**, which was handed this roster of five
  rather than the two its own FR named. Of the five, three implement it — `KafkaSink`,
  `GooglePubSubSink` and `NATSSink` — while `SQLiteSink` and `PostgresSink` do not, because both
  commit inside `emit`, so nothing is outstanding once one returns and the commit in their `close`
  is belt and braces rather than a buffer. That qualifies what this roster measured from the close
  bodies alone. Its own population sweep then found three more: `SentrySink`, `LoggingSink`, and
  the three wrapper sinks, which forwarded no flush at all.

- **`_fork._reinit_primitives` can exhaust memory, not merely hang** (SPEC-039, measured under
  SPEC-042). Its container read is `list(container)`, so a `list` subclass with a
  non-terminating `__iter__` reachable from any sink never returns: measured **5.7 GB RSS in
  nine minutes**, and the parent could not kill the child because the parent was being starved.
  SPEC-042 bounded its own two walks — the stamp walk and the child's marking walk both read
  through a capped helper, and tripping the cap sets `_marking_failed` and refuses everything
  unrecorded, a leak rather than a destructive close. The repair walk is unbounded still, and it
  is SPEC-039's to change: recorded as *exhausts memory* rather than *hangs*, because the two
  call for different operator responses.

- **A sink shared across a fork is shared, and both processes act on it** (SPEC-039 FR-005
  AC-1). Beyond the buffer discard the library neither clones nor re-opens the inherited sink,
  so one socket, one SQLite handle or one file is now written by two processes — see §9 for the
  caller's remedy. Two consequences are worth naming because they are not obvious from that
  sentence. `RotatingFileSink` lets **both** processes rotate: a child can rename the file the
  parent is writing to, and each keeps its own `_size`, which is why the discard hook leaves
  that counter alone rather than claiming a precision a shared file cannot support. And both
  processes hold an orphan-path close record for the same sink object, so ~~**each closes its own
  copy at exit** — deliberately left rather than fixed, since neither side can tell whether the
  other still needs it~~ — **fixed by SPEC-042**: the child refuses, because the record now says
  which process acquired the sink, and neither side has to guess.

  ~~**And a `configure(sink=…)` in the child closes the inherited sink immediately.** Measured
  with a socket sink whose `close()` writes a goodbye the server acts on: the child's
  `configure()` sent it and the parent's next write failed with `ECONNRESET`.~~ — **fixed by
  SPEC-042 FR-001/FR-002.** The record is stamped when the library is *handed* a sink and
  consulted at the one place it closes one, so a child refuses the object it inherited. Both
  swap paths, `shutdown()`, the exit close and the five wrapper sinks all go through it; a
  re-run of that same socket probe across eight route combinations produced no goodbye at all.

  Both halves of the mechanism are worth stating exactly, because this paragraph is what a later
  spec will be scoped from. **The trigger is not "the parent logged"** — it is that an emit has
  reached the inherited sink object in *this process's* record before the `configure()` runs,
  which the parent arms by logging before the fork (usual under preload) and **the child arms
  for itself** by logging before it reconfigures. Measured in all four combinations. **And both
  swap paths close it**, not just the worker's: `Worker.swap_sink` drains, installs, *fences with
  a second drain* and then closes — the fence being what makes the close safe within one process
  and, provably, does nothing across two — while `_lifecycle._swap_sink`'s no-worker branch hands
  the old sink to `_lifecycle.release(..., detached=True)` with neither drain. A process that only ever logged
  outside a span takes the second, so "there is no worker here" is not an escape.

  §9's remedy stayed "build a connection-holding sink in the worker process" rather than "rebuild
  it in the child" *because* the obvious phrasing of the advice performed the damage sooner and
  more completely than the hazard it avoided. **That is no longer true of a sink the library was
  handed here** — SPEC-042 has the child either refuse it or close a copy it re-acquired for
  itself — so the advice survives as the better
  deployment rather than as an "or else". It is still an "or else" for the two cases above: a
  sink the parent built and never configured (#1), and a subclass that inherits the
  re-acquisition hook while holding a transport the hook does not re-acquire (#2).
  Whether the library should **disown** an inherited sink in the child was **SPEC-042**, which
  settled it on the
  distinction this record could not draw: a child may release only a transport it acquired **in
  this process** — by being handed the sink here, or by re-acquiring it through FR-004's hook,
  which after the reopen is exactly what the file sinks did and what a connection sink did not
  (measured: the child's `FileSink` holds a different descriptor from the parent's, while a
  socket-holding sink holds the same one).
  It is not SPEC-040's: that one is a pure refactor whose Out of Scope forbids behaviour change
  and directs this kind of finding elsewhere, so it records the defect as evidence for its own
  motivation and nothing more. What stays true either way is the residual SPEC-042 accepts — a
  shared sink whose `close()` performs delivery loses whatever the child had buffered in it. Those
  sinks now have a flush that delivers without releasing (SPEC-036 FR-002), so a child can push its
  own buffer before it exits; what the residual costs is unchanged for a sink that has none.

- **`Worker._reinit_after_fork` installs `self._thread` only after `start()` succeeds**
  (SPEC-039 FR-002), so for an instant a live drain thread coexists with the inherited dead one
  in that attribute. Safe only because the drain thread never reads it — `_run`, `_drain`,
  `_terminal_failure` and `_release_waiters` do not, and every reader is on a caller thread —
  which nothing enforces. The alternative was measured and is worse: assigning first leaves a
  worker whose `start()` raised reading `draining` forever and handing the next `shutdown()` a
  `RuntimeError` from `join`, out of a public call documented to raise nothing.

- **`atexit` does not run when a serverless environment is reaped.** The graceful drain (§9) is
  registered via `atexit`, which covers a process that *exits*. A Lambda execution environment
  is frozen when the handler returns and killed later without running exit handlers, so there is
  no point at which that drain is guaranteed to run. `flush()` (SPEC-013) is the only guaranteed
  drain there, and is the first thing to check when tail events go missing in a serverless
  deployment. `shutdown()` is the wrong tool per-invocation: it is terminal, so only the first
  invocation on a warm container would log. That mistake is no longer silent — `health().retired`
  with a non-zero `submitted_after_shutdown` is its signature (§9, SPEC-030).

- **A forked child closing a transport it never opened is fixed by ownership records, not by
  this refactor** (SPEC-040 FR-005 AC-3). It was handed here by SPEC-039, is this spec's shape
  exactly — ownership asked ad hoc, with no path able to ask "is this even mine?" — and is a
  **behaviour** change, which SPEC-040 forbids itself. **SPEC-042 shipped the fix**, and its
  release helper is now one of the functions that moved onto the owner's module. The two stayed
  independent, as intended: 042 did not wait on this refactor, and this refactor absorbed none
  of it.

- ~~**Six lifecycle races found by SPEC-040's execution frame over its own diff.**~~ —
  **five closed by SPEC-044**, each now pinned by a committed reproduction in
  `tests/test_lifecycle_races.py` rather than by the scratch harness that found it. Every one
  reproduced byte-identically on the pre-SPEC-040 tree, so the refactor caused none of them.
  The sixth is the `shutdown(timeout=…)` limit stated above and is not repeated here. What each
  race was, and the depth counter, close registry and latch that closed them, is in
  [SPEC-044](spec-delivery/SPEC-044-lifecycle-races.md) and CLAUDE.md's Key Decisions.

## 14. Alignment with observability concepts

| Concept (design brief) | How log-foundry addresses it |
|------------------------|----------------------------|
| Structured vs unstructured | JSON with named fields, always (§6). |
| Correlation ID journey | Two-tier IDs + parent/child hierarchy across nested calls (§3), continued across processes via W3C `traceparent` + `baggage` (SPEC-014). |
| Head vs tail sampling | Neither is built. Span-outcome sampling has a natural call site; true tail sampling needs a trace-scoped buffer we don't have (§10). |
| Cardinality explosion | High-cardinality fields allowed in logs; never auto-promoted to metric labels (§6). |
| One event, three views | Logs-only today, but trace_id/span_id make traces derivable later (§13). |
| Symptom vs cause alerting | We stamp `duration_ms` / `status` / `error.type` — the fields alerts key off (§6). |
