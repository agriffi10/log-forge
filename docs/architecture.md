# log-foundry — Architecture

> A Python library for generating **consistent, structured logs** per function call,
> correlating them with shared IDs, and shipping them to a configured output sink
> (e.g. an SQS queue) for consumption by something like the ELK stack.

This document is the design reference. It records *what* we're building and *why*,
including the decisions made during initial design. It is intentionally ahead of the
code — sections describe the target design, not necessarily what's implemented yet.

---

## 1. Purpose & scope

log-foundry owns the **logs** pillar of observability. It does **not** ship metrics, and
OTLP traces are a planned arc item, not a built one. It deliberately uses tracing
vocabulary and ID conventions so its output correlates with — or is promoted into — traces.
The direction for what goes outward is the OpenTelemetry data model over plural transports:
OTLP is one sink among several, never the only way out.

> **Superseded in part, 2026-10-05.** This section said the library "does **not** (yet) ship
> metrics or traces" and stays logs-only. The direction is reversed for traces and for the
> data model; in-library metrics stay out. Full entry: `docs/decisions/public-api/`; first
> slice: SPEC-056.

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
  adopts it. Nothing is auto-instrumented. The header is rendered from the same coercion an
  event's fields pass through and is capped at the 8192 bytes the receiving parser accepts, by
  whole members (SPEC-064).

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

- **Auto-capture:** the decorator captures the **function name** only (`func.__qualname__`
  where the callable has one, its type's name otherwise — SPEC-055), resolved once at
  decoration and used as both the span name and the `function` field. *(Decision: function name only — no
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
  caller's data. Both live in `sanitize`, not at the sinks — one pass at assembly is ~~correct by
  consequence for all 40-odd `json.dumps` call sites~~ — superseded in part by SPEC-058 — correct
  for every event assembly built, and reaches the non-JSON sinks too. It is not correct by
  consequence for an event a `TransformSink` returns, because the function runs after assembly and
  can put any value back in. So the transform checks each result with invariant 8's observable and
  coerces one that fails, for type only — the same `sanitize` rules, no configured ceiling, one
  fixed depth bound — and an HTTP sink drops an event whose render still raises, alone, rather
  than losing the batch.

  > **Superseded in part by SPEC-069.** Not only an HTTP sink: every record-building sink now
  > encodes the batch through one guarded step before its first bulk call, so an event any of
  > them cannot encode is dropped alone, counted `dropped`, and a batch of nothing encodable
  > settles. And "at the sinks" is no longer empty: `PostgresSink` and `MongoDBSink` repair a
  > value their destination refuses, after assembly, marked `truncated`. The decisions live in
  > `docs/decisions/sink-delivery/` (*An event a sink cannot encode costs only itself*) and
  > `docs/decisions/event-assembly/` (*A destination's refusal is repaired at the sink*).
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
   second drain), and hands the new sink the worker's stop signal (§9, SPEC-027) — or, while a
   `shutdown()` is closing that sink, once that close is over (`docs/decisions/sink-lifecycle/`);
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
left **open for now** and `health().incomplete_swaps` records it, on SPEC-027 FR-004's reasoning
that a leaked resource in a running process beats a close raced against a write.

> **Superseded in part, SPEC-050 FR-004.** "Open" used to mean *forever*. The objection is about
> that instant — the drain thread may still be inside the old sink's `emit` — and it expires when
> that thread does, so the sink is recorded and closed by the first `shutdown()` that finds
> `is_alive()` false, detached and bounded by the same closer grace a swapped-out close already
> gets. An expired `shutdown()` still leaves it open, because the thread is still live, and the
> next call performs the close instead. What changed the decision was measuring the cost: for a
> sink whose `close()` *is* the delivery, "left open" is nine events lost, not a leaked handle.
> Full entry in `docs/decisions/sink-lifecycle/`.

Passing the sink already live is a no-op. After `shutdown()` the worker swaps nothing: it is retired, so the config is
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

`configure()` remains a startup call. Two calls passing a sink at once are serialized by
`config._sink_lock`, held from the config write to the end of the swap, so whichever runs second
wins both the config and delivery and every sink is closed once; the wait comes out of the second
call's swap budget, and a wait that outlasts it proceeds unserialized with a `pending` line. Before
the lock, the two swaps' post-retarget decisions interleaved: measured at `641dfc86`, 12 of 100
runs closed a sink twice and 15 of 100 left the config naming one sink while delivery went to the
other. A span finishing on another thread during a swap may land on either sink; what is
guaranteed is that everything submitted *before* the call reached the old one. The `_worker` read that selects between the two paths *is* taken under
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
    # def deliver(self, batch) -> Verdict: ...        # per-event outcomes (SPEC-072), with
    # log_foundry_counters: SinkCounters              # required for deliver to be offered
    # log_foundry_idempotent: bool                    # only `True` counts
```

`sinks/base.py` is authoritative for all of these — what each promises, and for
`reacquire_after_fork` which sinks are actually asked, which is narrower than "whichever define
it"; `sinks/verdict.py` and `sinks/_engine.py` are authoritative for `deliver`.

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

~~Three~~ cases where nothing landed and the sink still must not raise, each settled by an earlier
spec: an **unadjudicable** batch response (SPEC-018 — the sink cannot prove nothing landed, so a
retry risks duplicating), an **SQS sender fault** (SPEC-016 FR-006 — provably rejected, and a
byte-identical re-send can only fail the same way; an **SNS sender fault** too since SPEC-070,
below), and an **oversized** event (it can never fit,
so there is nothing to retry). All three are reported through `losses()` instead. — Superseded
in part by SPEC-058, which settles three more cases the same way, counted rather than raised: an
**unrenderable** event in the HTTP family, alone or beside oversized ones (the sink's own render
of identical input fails identically), a wrapper batch of **one failure and every other event
deliberately dropped**, and an SQS batch of **an id-callable failure and an oversized event**.
Beneath those three sits SPEC-058's principle: a raising `emit` counts nothing, and whoever
catches the raise — the worker, the orphan path, or a `MultiSink` that swallowed it — owns the
batch's count. It holds at every shipped site that decides a per-event drop: SPEC-058 applied it
to the drops `TransformSink`, `FilteringSink`, `SQSSink._chunks` and `HTTPSink._items` decide and
to `MultiSink`'s absorbed count, and SPEC-059 to the rest — the oversize drops of the
record-building sinks and the socket transport, the hand-offs `KafkaSink` and `GooglePubSubSink`
see refused, and `HTTPSink._drop_refused`'s `413`. So a `dropped` count is made only by an `emit`
that returned, and a check in `tests/test_deferral.py` holds every counter some `losses()`
reports as `dropped` to being written by that commit alone. What can still land before a raise
and again on the worker's retry is a sink's own `failed` count, an upper bound, the SQS and SNS
sender faults included — `SQSSink._send` and, since SPEC-070, `SNSSink._send` count one before
`emit` raises for a recoverable loss beside it.

> **Superseded for the HTTP family by SPEC-073:** its drops (`HTTPSink._items`, `_drop_refused`'s
> `413`, and a chunk whose request could not be built) register `SETTLED` on the delivery engine's
> run context and are committed by `_engine._commit` only when the run settles, and its `failed`
> commits once when the run ends rather than before a raise and again on the worker's retry, which
> no longer resends a shipped family sink's batch. Decision: *The HTTP family delivers in rounds on
> the engine, and a run context keeps its counters' units* (`docs/decisions/sink-delivery/`).

> **Superseded for the SDK, driver and socket sinks by SPEC-074:** `SQSSink`, `SNSSink`,
> `KinesisSink`, `FirehoseSink`, `ClickHouseSink`, `MongoDBSink`, `PostgresSink`, both Redis sinks,
> `RabbitMQSink`, `AzureEventHubsSink`, `NATSSink` and `SocketTransport` (which `SyslogSink` forwards
> to and socket-mode `LogstashSink`'s `emit` runs the adapter over) define `deliver` — one attempt
> per chunk, message or frame per round — and an `emit` that is the inline adapter. Their retry
> loops, backoff constants and hand-built `SinkLosses` are gone: `losses()` returns the engine's
> `SinkCounters`, `failed` is a property over `failed_events` (assignable), and a drop registers
> `SETTLED` on the run context, committed with the totals under one acquisition. SQS and SNS read
> `Failed` and MongoDB `writeErrors` through `_batch.adjudicate_keyed`; a sender fault is a final
> rejection, so a batch lost only to them still returns; a closed sink's round, a NATS latch and an
> SQS batch whose every id callable raised are refusals counted by their catcher alone; an SQS FIFO
> group keeps its order for `max_retries + 1` sends and holds nothing in a run's last round. Decisions: *A sink round resends only what
> failed, and an SQS FIFO group keeps its order* and *No shipped sink declares itself idempotent*.

SPEC-070 widens the first two of the original three cases. An **SNS sender fault** joins the SQS
one — the same required `SenderFault` field, read through the same `sinks/_classify.sender_fault`,
counted and never retried, a batch lost only to sender faults returning — and a Kinesis or Firehose
reply whose failed count its coded records contradict is an unadjudicable response (*A sink that
delivered nothing raises; one that delivered something reports*).

**The same rule reaches the sink's own lifecycle** (SPEC-032): a sink whose `close()` released or
invalidated a transport must **raise** on a later non-empty `emit` rather than absorb it — a
produce into a batch nothing will flush again, or a future nothing will resolve, is total failure
by a different route. A sink holding nothing to release keeps **accepting**, because refusing a
batch that would have delivered is loss the library invented. Which applies is a property of the
sink, so each records its answer in its class docstring and a lint holds every sink to one.
`emit([])` remains a no-op either way.

**A sink may report per event instead of raising for the batch** (SPEC-072). The optional
`deliver(batch) -> Verdict` returns one `Outcome` per event, by index — `ACCEPTED`, `RETRYABLE`,
`REJECTED`, `DROPPED`, `UNKNOWN` or `SKIPPED` — and an optional `retry_after` hint. `Outcome`,
`Verdict`, `SinkCounters`, the probe `read_deliver` and the adapter `emit_via_deliver` are public
(`sinks/verdict.py`, re-exported from `log_foundry`); `deliver` is never declared on the `Sink`
Protocol. The probe reads the sink's **type** and never runs a `__getattr__`, a property or a metaclass hook: `deliver` must be a plain
function in a class namespace, the `emit` ordinary lookup resolves must be a plain function on
or below that class, defined by some class and not one set on the instance, and no class may
define `__getattribute__` as a plain function (so an `emit` override that stamps or scrubs keeps
running, in a subclass, on the instance or through `__getattribute__`), and `log_foundry_counters` must be exactly a
`SinkCounters`; a class attribute `deliver = None` opts out.

One private engine, `sinks/_engine.py`, runs the rounds. The worker and the orphan path call
`run_delivery` first and fall back to `emit` unchanged when the sink does not offer `deliver`; an
engine sink's own `emit` is `emit_via_deliver`, which runs the same engine inline and keeps the 1.x
`emit` contract for a wrapper calling `inner.emit(batch)`. Only the events a verdict called
`RETRYABLE` are offered again, within the 1.x total of attempts — `(s + 1) × (w + 1)` on the worker
path, `s + 1` on the orphan path and in the adapter, `s` and `w` the sink's and the worker's
`max_retries` read as exact `int`s — and the 1.x wait schedule: the sinks' 0.1 s backoff restarting
each cycle, the worker's `cycle_backoff` between cycles, a usable hint replacing the wait it falls
on under the sink's `max_retry_after` (30 s when it declares none), every wait cut short by the
stop event when there is one, and SPEC-062's prediction asked before every further round. The worker does not resend a whole
batch around an engine run. The engine holds no lock while `deliver` runs.

The engine is the one writer of the sink's `SinkCounters`, at the end of the run through one
`Deferral`. A run **fails** when nothing was accepted and some event ended `REJECTED`, unresolved
or in a raising round, with none `UNKNOWN` or unreadable; a failed run commits only
`failed_events` and its catcher owns the batch — the worker's `failed_batches` and abandon or
declined line, `orphan_lost`, or the adapter's `SinkDeliveryError`. A settled run commits
`dropped_events` too and is announced in one `_diag.lost` line. **This extends SPEC-058's
principle to `deliver`:** a `deliver` counts nothing itself, and the engine is a fourth catcher, for
a `deliver` that raised. An exception escaping `deliver` fails its round's events (it is not a
response); an `UNKNOWN` outcome or an unreadable verdict settles, counted, because the events may
have landed; either is resent only by a sink declaring `log_foundry_idempotent = True`, and the
exception only when the sink's `_retryable_error` accepts it. The fork repair reads
`log_foundry_counters` on a sink it does not enter and replaces that one lock (invariant 12). No
shipped sink defines `deliver` at SPEC-072. The decisions are *A raising `emit` counts nothing,
nor does a `deliver`* and *A sink may report per event* in `docs/decisions/sink-delivery/`.

**A wrapper stands on the engine when what it wraps does** (SPEC-075). `FilteringSink`,
`TransformSink` and `LogstashSink` offer `deliver` exactly when their inner sink or backend offers
it: socket-mode `LogstashSink` offers on the `SocketTransport`'s counters, as `SyslogSink` does,
because the transport defines no `emit` for the probe to judge. An offering wrapper holds the
inner sink's own `SinkCounters`, so the engine's totals for a run through it land where the bare
sink's would. It copies the inner sink's `max_retries` and `max_retry_after` at construction, as
the engine reads them, into new public read-only names, and stores its stop signal where the
engine's static reader finds it while still forwarding it. So the worker runs one engine loop
with the wrapped sink's own budget, where it used to run its whole-batch loop around the inner
sink's inline engine. `FilteringSink` and `TransformSink` map the inner verdict back to their own
indices and count only their own decisions: a declined event is `SKIPPED`, a caller-code failure is
`SKIPPED` and counted on the wrapper's `_dropped` `SETTLED`, and a call whose every event failed is
`REJECTED` with the refusal marker. `RunContext.delegate` moves the inner round's marks into the
wrapper's positions, and for a malformed inner verdict inside a run sets aside what the inner
call registered, as the engine does for a bare sink. Caller code runs once per event per run:
`TransformSink` keeps each event's result and `FilteringSink` each forwarded event's decision, so a
later round forwards the same object without asking the function or the predicate again. `LogstashSink` returns its backend's verdict unmapped. No wrapper
declares idempotence or calls its inner sink's `emit` from `deliver`, and each wrapper's own `emit`
keeps its 1.x body. `MultiSink` offers no `deliver`: it cannot give one verdict for an event one
child took and another refused, so it keeps calling each child's `emit`, and an engine child's
rounds run inside that one call. A failed engine child's events land in its own `failed` and again
in `MultiSink`'s absorbed count, invariant 2's recorded upper bound. The decision is *A wrapper
offers `deliver` exactly when what it wraps does, and counts only its own decisions*.

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
  sink's contract as much as the worker's code (SPEC-026). A sink that offers `deliver` is the
  other form (SPEC-072, §8): the worker runs the delivery engine instead of its `emit` loop, the
  engine's budget already holds the worker's share of retries, and a failed run moves
  `failed_batches` once with the 1.x abandon or declined line.
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

  1. **Pin the inherited record.** Every record entry whose sink is alive is held strongly before
     anything else runs (SPEC-081). The record holds a sink the library no longer holds only
     weakly, so without the pin a sink the child stops referencing could be collected there and
     its finalizer could close the parent's transport. The pin takes no lock and its fault is
     absorbed; it runs before the roster, and before the rebind of the published re-acquisition
     list, which drops a list that may hold one.
  2. **Re-initialise every lock and event the library owns**, in two steps sharing one identity memo
     (SPEC-079). First the package's **global** primitives, listed in one roster,
     `_fork._GLOBAL_PRIMITIVES` — the runtime's state, stop, record and close-registry locks, the
     config and sink-change locks, the decorator's two and the console's — which
     `tests/test_fork_roster.py` holds complete from a live sweep of every module global and an AST
     scan of every class that assigns a primitive to `self`. Then **one walk from the runtime's
     roots**, the configured sink, the worker and the late worker: its repair frontier enters this
     package's objects and sinks, reads a container one level deep and replaces every lock and event
     on each package holder; its marking frontier follows what the repair frontier forwarded, a
     third-party wrapper's namespace and the containers, first in first out under a visit ceiling,
     then the record's live entries other than a closed package sink; a trip marks the inherited
     sinks on the heap instead (SPEC-081). A closed package sink here means an instance of a shipped
     class exactly (`_fork._defined_here(type(sink))`), never a subclass of one or of `Sink`, whose
     `close()` is the caller's code. A lock that was not held is replaced too — asking whether one
     is held has no answer that is not itself a race — and the memo keeps two holders of one
     primitive sharing it, which is what stops a sink's `log_foundry_stop_signal` drifting from the
     worker's `_stop` and from the roster's `_state._stop`. Every read the walk makes is static, so
     no attribute hook of a sink runs. A registry of live `SinkCounters` repairs every engine sink's
     counters whatever holds them. An AST lint forbids building a primitive anywhere the repair
     cannot write it back.
  3. **Re-acquire what can be re-acquired.** A fork landing inside `emit`, after the write loop
     and before the flush, leaves both processes holding the same pending bytes; the child
     strands its copy (`dup2` to `/dev/null`, then reopen in **append** mode) through the
     optional `reacquire_after_fork()` hook `sinks/base.py` documents (§8). The name says the
     larger half (SPEC-042 FR-005): a sink that returns from it has claimed the transport as
     **this process's own**, which is what makes releasing it safe later. Which hooks returned
     is published, not acted on — ownership is `_lifecycle`'s.
  4. **Run the registered handlers**, in order: the reclaim of every sink whose hook returned,
     the clearing of what no thread will finish (in-flight closes, the token sets, the
     no-worker shutdown count and the late worker, then a re-settle of the stored state), the
     adoption of a `sink_factory` (FR-006 of SPEC-079), and the worker rebuild. The worker is rebuilt **in place** with a fresh
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
  what it inherited and the parent never recorded, *before* any handler runs. Write-once alone
  defends only a record that already exists, so where the parent's walk recorded nothing a child
  could `configure()` its way into genuine ownership and destroy the transport legitimately —
  measured, through a third-party wrapper the stamp walk may not descend into.

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


### 9.2 Three questions a guard can ask about the worker

> **Superseded by SPEC-054 FR-004.** This section read *four questions* until the owed-close
> record was merged. With one record, "who owns this sink's close" has one answer and is no
> longer a question a call site can get wrong, so **ownership** and **ownership ∧ moment** are
> gone and what remains of the conjunction is the moment on its own — carrying **two**
> predicates, because an abandoned drain answers them oppositely. The full entry is
> `docs/decisions/pipeline/` → *A worker guard asks one of three questions*.

Every guard that mentions the worker is classified as exactly one of three categories, and
answering with the wrong one is this codebase's most repeated defect: three reviewers told
SPEC-033 "ownership, not liveness", each naming a different call site, each was fixed, and a
fourth shipped broken (SPEC-035 FR-001) — whose own first draft then prescribed a predicate that
would have re-broken SPEC-033 in the opposite direction.

Since SPEC-040 each question is **one method on `_lifecycle._state`**, and a call site *selects*
one rather than composing a predicate. `_state` is the one instance of `Runtime`, the class that
holds all of the lifecycle bookkeeping. SPEC-077 renamed it from `_Lifecycle` and moved onto it the
closer roster, the in-flight-close registry, the ownership record and their locks, which had been
module globals. The state and the guards live in `_lifecycle.py`. `decorator.py` keeps the
decorator and the span machinery and reaches the worker only through `_lifecycle._get_worker()`.

| Category | Method | What it decides |
|---|---|---|
| **Existence** | `_state.worker_exists()` | is there anything to do, or a worker to build |
| **Liveness** | `_state.live_worker()` — the worker while its `_epoch` is the current retirement count | who **performs** an action; a retired worker performs nothing. Reading the count to *report* it is the same question, not a fourth |
| **Moment** | `_state.in_flight(sink)` for the signal refresh, `_state.held(sink)` for the close; `worker.claim_stop(deadline)` for the stop | is anything using this sink **now**, or is a stop already under way. Two predicates in one category, because an abandoned drain answers them oppositely; `claim_stop` is the same question asked as a transition |

**None of the three takes the lifecycle lock.** Several guards ask a question with it already
held (`_get_worker`'s inner check, `_close_owed`, `_swap_sink`, and `_will_hand` through all
of its callers), so a non-reentrant acquire inside a question would deadlock them; and `_get_worker`'s outer check is deliberately unlocked on the `@trace` hot path. Each
read is a single atomic reference load, and a caller needing consistency across two reads takes
the lock itself. The added call costs ~6.6 ns, measured, against an ~18.6 µs span. The two
moment predicates do take a leaf lock — the close registry's, `_state._closes._lock`, since
SPEC-078 merged `_closing_now_lock` and `_closers_lock` into it — which is the order the shipped
code already established: the signal refresh (`_offer_orphan_signal` then, `_will_hand` now)
reached that lock under the lifecycle lock before SPEC-054 folded the read into the question,
and nothing acquires the lifecycle lock beneath it.

**No sink's stop-signal setter runs under a library lock a call from inside it would wait on**
(fix after SPEC-079; `docs/decisions/sink-lifecycle/`). The setter is the sink's own code, and
one calling back into the library on the same thread hung the process wherever the hand ran under
the lifecycle lock, the sweep lock or the factory wrapper's build lock. Each path now decides under
the lock and hands after releasing it, with a hand token covering what the deciding section
published, so no orphan log and no `flush()` reaches a sink that section armed before the sink
holds its signal. The drain thread is the exception: it can emit a batch to a just-built worker's
sink before the build's hand lands, which the decision records under *What is left*. The factory
call itself runs under no lock either, and a factory inside `flush()`, `shutdown()` or a
sink-changing `configure()` makes the emits waiting on its build refuse rather than wait, because
that call can be waiting on a drain thread that is one of them. One section publishes with no
token: during a stop, a worker build whose sink has a close in flight marks that close instead of
handing, and the close's discharge pays the hand with the current signal, so the close keeps the
signal the shutdown set (`docs/decisions/sink-lifecycle/`).

**The close registry holds every owed close and every close in flight** (SPEC-078 FR-001).
`Runtime._closes`, a `_CloseRegistry`, replaced `_owed`, `_closing_now`, `_closers` and the
closer's local thread list. `take` moves a sink from owed to in flight in one call under the
lifecycle lock, and each in-flight entry records its rule — `INLINE` on the calling thread,
`JOINED` (a shutdown's fan-out, joined in full, SPEC-046) or `GRACED` (detached, given the
shutdown's one grace) — and its thread, and holds no sink. Both moment predicates end in
`_CloseRegistry.in_flight`; `health().closing_sinks` counts the `GRACED` entries whose thread is
alive. **A shutdown spends the closer grace once**: every graced wait of one call ends at that
call's `grace_end`, fixed at its first graced wait, less whatever the call already spent waiting
for a swap in flight to take its decision (`_Operation.grace_spent`). That wait comes first: a
`shutdown()` landing between a swap's retarget and its decision drained into a sink the swap had
installed but not yet armed and returned with it open, so it waits on each such swap's
`_Operation.decided`, within the grace, before it drains. A swap releases the sink it displaced
only while the owed record still owes it (`_CloseRegistry.owes`), since a sink the record no
longer owes was taken, and closed, by somebody else. `docs/decisions/pipeline/` → *A shutdown
waits for a swap in flight to decide* has the measurements.

**`State` is stored and decides** (SPEC-078 FR-003, FR-004). `Runtime._current` holds the
settled state — `RUNNING`, `CLOSED` or `UNCONFIGURED`, from the worker, the retirement count and the
owed record, never the config — written only by `Runtime._move` at the end of the four critical
sections that can change it. `Runtime._column()` reads the token sets and then `_current`, under
the lock the caller holds, and is the column of the transition table, which is total over six
calls by five states. Only `_swap_sink` consults it, because its `DRAINING` cell is the one whose
answer differs: with no live worker a swap during a drain declines, while that drain still has a
take of the owed record ahead of it (its `_Operation.drained` is unset; `Runtime._take_ahead()`). `Runtime.state` stays the
lock-free reporting view and still reports a resolved-but-unwritten config as `RUNNING`; no guard
selects it. `CLOSED` keeps its arrows (a span after a worker-less `shutdown()` builds a live
worker). `Runtime` holds three locks, `_state._lock`, the registry's leaf and `_owned_lock`, and
`docs/decisions/pipeline/` → *Lifecycle state has one holder, Runtime* says why they are not
one.

**The moment is one category with two predicates, and a transition**, and that is stated here rather than left to
be rediscovered at a site. The transition is `worker.claim_stop(deadline)` (SPEC-062): it asks the moment as an act, setting `_stopped`, recording the deadline and taking `Worker._lock`, so it is not a third predicate. `in_flight(sink)` asks whether the worker is *draining* into it, so
an abandoned drain counts as **over** and the sink gets a fresh stop event rather than SPEC-033
FR-004's tight retry loop. `held(sink)` asks whether the worker's thread is *alive* and may be
inside it, so the same state counts as **still inside** and the sink is left open (SPEC-027
FR-004). Both were measured on the expired-shutdown state — SPEC-033 FR-002 closing under a live
writer, SPEC-035 FR-001 the fresh event never arriving — and a single predicate is wrong for one
of the two sites whichever way it is written.

Liveness and the moment diverge the instant the retirement count moves — which is **entry** to
`shutdown()`, not its completion. One question is answered by a **return value** rather than a
predicate: `Worker.retarget` reports what it did, because the decline is taken between its two
lock acquisitions and nothing outside observes it.

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
SPEC-045's completion this heading read "**None.**" while §13 carried six such items among its
constraints — a register saying it was empty when it was not. Each entry below names what would
close it.

### Open

- **One value `ClickHouseSink`'s typed columns refuse costs its whole insert chunk** (invariant 8;
  no spec — deferred past 1.2.0 by Andrew, 2026-11-13). A non-`str`, non-`None` value or a lone
  surrogate in a `Nullable(String)` column, or a `str`, `list` or `dict` in `duration_ms`'s
  `Nullable(Float64)`, fails the chunk's one `insert`; every event in it is counted in `failed`
  and announced. Only a direct `emit`, or a `TransformSink` rewriting a projected key, brings one.
  The measurement and the kinds: the *Out* sentence of
  `docs/decisions/event-assembly/a-destinations-refusal-is-repaired-at-the-sink-and-marked-as-assembly-marks.md`.
  **Closed by** a `column_value`-style rule in `ClickHouseSink._row` that binds or drops the one
  value as the SQLite and PostgreSQL sinks do, plus a hostile-value corpus that runs one bad event
  through every column-projecting sink against the real services, since the same defect has now
  been found in three sinks.

- **A sink can be released while it is still live inside the graph that replaced it.**
  `configure(A)` then `configure(MultiSink(A, B))`, or the reverse: the swap closes A as the
  outgoing sink while A is a child of the new one. Pre-dates SPEC-045, which pinned only that it
  must not become a *loss* — A keeps taking events through the wrapper, is owed a further close,
  and delivers everything. **Closed by** not releasing a sink reachable from the graph just handed
  over, which is decidable only against a live config read.
- **Whether the predicate roster still earns its weight** (SPEC-040 FR-005 AC-1).
  `tests/test_worker_predicate_roster.py` polices what is now **three** methods, and roughly half
  of it is the seam lint guarding the prose in its own data table rather than the rule. The case
  for retiring it got *weaker* during SPEC-040: widening its scope immediately filed two sites
  that had gone unfiled for two specs. It got weaker again in SPEC-054, which is what narrows this
  item rather than closing it: the merge would have dropped `_worker_health`'s only retirement
  guard out of the roster silently — `"retired"` is not a substring of `"retirements"`, and a
  keyword argument is not a position the walker files — leaving the re-derived floor to bless the
  shrink. So it caught, in one refactor, exactly the class of defect it exists for. What is still
  open is the *cost*, not the value. **Closed by** evidence a year of maintenance provides —
  whether any further defect is caught by it, or by nothing.
- **`worker.py` and `_lifecycle.py` are unsplit and unscheduled** (SPEC-040 FR-005 AC-2).
  They are the two largest modules in `src/log_foundry/`, and both still grow with the specs
  that settle lifecycle questions. This entry deliberately carries no line counts:
  `wc -l src/log_foundry/worker.py src/log_foundry/_lifecycle.py` is the answer. The pair this
  entry used to carry were written by `dcb07c3` and were already one line stale in the commit
  that carried them; `a58dfff`, the very next commit, moved one of the two outright, and by
  `3c973b9` both were wrong. `Worker` owns the drain thread, the queue, the retry, the counters, the
  swap and the shutdown; the questions *inside* it are one object's own state, which is why
  SPEC-035 FR-002 drew the roster's module boundary where it did. Neither is a defect; both are
  recorded so the next reader knows the split was considered. **Closed by** a spec that does it,
  or by a decision that the shape is right.
- **A forked child's `NATSSink` refuses rather than reconnects** (SPEC-061 FR-001; invariant 12).
  The sink runs its loop on a thread the child does not inherit, and it acquired no connection of
  its own, so in a child `emit` and `flush` raise `SinkDeliveryError` and `close()` returns without
  touching the inherited client, loop or thread. A `reacquire_after_fork` hook would connect during
  the fork handler, where a connect blocks for up to the driver's whole reconnect budget on the
  child's first instruction. **Closed by** a hook that connects lazily on the child's first call,
  under a bound of its own, and a decision on what the child does while that connect is pending.
- **A forked child's `SQLiteSink` refuses rather than reopens** (invariant 12; no spec — a defect
  fix). A SQLite connection cannot cross a fork, so the child refuses every `emit` with
  `SinkDeliveryError` before taking any lock, `close()` returns having touched nothing, and the
  inherited connection is stranded at the fork, never freed (§13). Unlike `NATSSink`'s `REJECTED`
  verdict, the refusal reaches the worker as a raised `SinkDeliveryError` from an emit-only sink,
  which it retries like any other failure before abandoning the batch, so a busy child's `flush()`
  is slow — the same retry a closed emit-only sink's refusal gets. Reopening the database in the
  child is what SQLite asks for and what Linux allows, and it was built and measured rather than
  assumed: on macOS a fresh connection in the child faulted in `os_log` too, once the parent had
  used SQLite, in anything from none to every child of a run depending on timing, because the
  hazard is Apple's `libsqlite3` logging after a fork rather than the connection. Refusing is the
  one answer that holds on both platforms. A child that wants a database of its own builds a sink
  of its own with `configure(sink_factory=...)` — on Linux; on macOS that sink's fresh connection
  carries the same `os_log` hazard, which no sink can remove. **Closed by** a way to tell, before the first
  statement, that the `libsqlite3` in use does not log through `os_log`, and a decision that a
  child on such a build reopens.
- **A driver reader that dies while the status stays CONNECTED is invisible to the probe**
  (SPEC-061 FR-002, the residual; invariant 7). The driver still cancels PONG futures of its own
  (`drain()`, `Subscription._drain`, and after a reconnect), and a reader task that ends while
  `is_connected` stays true makes `flush()` return without a round trip. The sink's answer rule
  cannot see that from the public API, and nothing it may call can. A `client.close()` cancelled
  at the close deadline (FR-004 step 2) leaves the same shape from the other side: the driver's
  transport may still be open, so the socket can outlive `close()` until the process exits.
  **Closed by** the driver exposing whether its reader is alive, or a last-heard-from timestamp,
  as public API.
- **A `NATSSink` whose driver has spent its reconnect budget is closed for good** (SPEC-061, Out of
  Scope; invariant 7). Once `max_reconnect_attempts` is exhausted the driver closes the client and
  the sink refuses every call, reported rather than silent, until the process restarts. **Closed
  by** a sink that rebuilds its client after the budget is spent, under a bound, which is its own
  spec because a rebuild during an outage is a reconnect loop of the sink's own.
- **A timed-out JetStream publish is not retried, and cannot safely be** (SPEC-047, Out of Scope).
  A lost ack is indistinguishable from a lost publish, so re-sending duplicates whatever landed
  (SPEC-018's rule). **Closed by** setting a `Nats-Msg-Id` header per event so the server
  deduplicates, which makes a retry safe and is its own spec. Since SPEC-061 the same "no ack"
  is treated two ways depending on how the wait ended: an ack wait that expires (every one the
  budget caps does, inside the sink's own wait) raises and its batch can be retried by the
  worker when nothing else in it landed, while a publish the driver never returns from is
  abandoned and counted, never retried (`docs/invariants.md`, invariant 2's recorded
  exception). The same header closes both.
- **`GooglePubSubSink.close()`'s bound does not cover an unboundable future** (SPEC-048 FR-004).
  A future whose `result()` takes no `timeout` is resolved unbounded, because that is the only wait
  it accepts and SPEC-036 measured that counting it instead invents loss on publishes that were
  going to succeed. A client handing out futures that are both unboundable *and* slow therefore
  holds `close()` for its own deadline, once per future: measured 27.0 s for nine against a 3 s
  stand-in, where the bounded path took 2.0 s for sixty. Strictly better than before SPEC-048,
  where **every** future took that path, and unreachable through `google-cloud-pubsub`'s own
  future, which accepts a timeout — but `client=` is a frozen public parameter. **Closed by**
  resolving them on a joinable thread, which bounds the close at the cost of a thread that may
  outlive it; waiting on a future is a read and causes no delivery, so SPEC-036's rule forbids
  *counting* them, not abandoning the wait.
- **A `RotatingFileSink` whose rotation keeps failing retries it once per event** (SPEC-048
  FR-006). The absorbed failure re-seeds `_size` from a file already over `max_bytes`, so the size
  trigger stays true and every later event attempts a close, a rename, a reopen and a `getsize` on
  the drain thread — measured 598 attempts over 600 events. Nothing is lost or duplicated, and the
  *diagnostic* is damped to one line per outage on `PostgresSink._reconnect_if_broken`'s rule, so
  what remains is I/O rather than noise. The time trigger is already damped by its re-arm.
  **Closed by** deferring the next attempt until the file has grown by another `max_bytes`, which
  trades a rotation the caller asked for against the retry cost.
- **`MongoDBSink` bounds `pymongo`'s socket wait at 30 s; the server-selection wait stays the
  driver's** (SPEC-049 FR-005). `pymongo`'s `socketTimeoutMS` default is `None` — a read that
  never returns holds the drain thread for good; SPEC-049's authoring measured `emit` at 60.66 s
  for two attempts against a peer that accepts TCP and never replies, 3.12 s with a bound. The
  library forwards `socketTimeoutMS=30000` (`DEFAULT_SOCKET_TIMEOUT`) only when neither
  `socket_timeout=` nor the URI's query names one — the default never overrides a value the caller
  wrote, an explicit keyword does (pika's precedence, not psycopg's, because the URI lets the two be
  told apart). `serverSelectionTimeoutMS` (driver default 30 s, finite) is exposed as
  `server_selection_timeout` and not overridden. **Closed by** a deployment measurement showing 30 s
  is the wrong side of the line, which supersedes the number in place.
- **`RabbitMQSink` bounds `pika`'s blocked-connection wait at 30 s; `socket_timeout` and
  `stack_timeout` stay the driver's** (SPEC-049 FR-005). `pika`'s `blocked_connection_timeout`
  default is `None`: a broker under a memory or disk alarm sends `Connection.Blocked` and every
  publish then waits indefinitely on the drain thread. `DEFAULT_BLOCKED_CONNECTION_TIMEOUT` is set
  on the parameters `_connect` builds — the URL and the no-URL constructor alike, and on every
  reconnect — only when the URL's query named no `blocked_connection_timeout`; an explicit keyword
  overrides the URL. **Recorded from the driver's documented default and not executed by
  SPEC-049:** neither the blocking behaviour nor pika's parsing of `?blocked_connection_timeout=`
  from the URL query into the attribute was measured against a broker here — both were re-read from
  the installed package (pika 1.4.4), and the unit tests encode that premise through a stand-in.
  `socket_timeout` (10 s) and `stack_timeout` (15 s) are finite and exposed on the same terms.
  **Closed by** an integration run against a broker under a memory alarm, which supersedes this
  entry in place rather than leaving it read as settled.
- **`ClickHouseSink` exposes `clickhouse-connect`'s 300 s `send_receive_timeout` rather than
  overriding it** (SPEC-049 FR-005). The driver's default is finite, so FR-001's rule leaves it with
  the driver — SPEC-049's authoring measured one call at 300.01 s against a non-replying peer versus
  3.00 s with an explicit bound, and `_insert` makes `max_retries + 1` such attempts per chunk on the
  drain thread. `send_receive_timeout=` is forwarded to `get_client` only when given. **Closed by** a
  library default, if a deployment shows 300 s per attempt is the wrong side of the line — the
  decision `psycopg`'s `connect_timeout` took in SPEC-041.
- **`ElasticsearchSink` reads a `2xx` object without a truthy `errors` as success, unverified.** It
  does not check that the object is `_bulk`-shaped, so a server answering every POST with
  `{"error": {"type": "auth"}, "status": 500}`, or with
  `{"items": [{"index": {"error": "x"}}, {"index": {"error": "x"}}]}`, leaves `flush()` truthy,
  `failed_batches` at zero, `health().sink` at `failed=0` and stderr empty (reproduced at
  `6399298`). Undecodable and non-object bodies are counted; this residue is not, because routing
  an absent `errors` to `dropped_unadjudicated` would charge every success from an endpoint that
  omits the key. **Closed by** deciding it with the Astra `_bulk` profile spec, which is where an
  endpoint's response shape gets settled.

- **`NATSSink`'s float timeouts are forwarded to the driver unchecked** (SPEC-066, found while
  closing the int doors; invariant 13). `connect_timeout`, `reconnect_time_wait` and
  `drain_timeout` go into `nats.connect(**supplied)` exactly as given whenever they are not
  `None` — no type check and no value check — so a `nan`, `inf` or a negative reaches `nats-py`
  from the constructor. SPEC-061 also reads `drain_timeout` for the sink's own close deadline,
  through `usable_timeout`, so a non-number there now raises that comparison's unnamed `TypeError`
  at construction, while the driver still receives it unjudged. They are float doors, not the int
  doors SPEC-066 closed, and `nats.py` was being rewritten by SPEC-061 while SPEC-066 was built,
  so SPEC-066 changed only `max_reconnect_attempts` there. **Closed by** judging each by the
  sink-delivery rule — refused if no value of that kind has a working configuration to protect,
  floored through `usable_timeout` if one does.
- **A `float` timeout too large for a socket constructs and fails every `emit`** (SPEC-066
  second diff review, found while closing the int doors; invariant 13). `_retry.require_timeout`
  accepts any positive finite number, and `socket.settimeout` raises `OverflowError` above about
  `9.2e9` seconds (the platform's `PyTime_t`), so `HTTPSink(timeout=1e10)` constructs and no
  `emit` can deliver. ~~Its `emit` raises that `OverflowError` uncounted — not an `OSError`, so the
  retry guard never sees it.~~ Since SPEC-070 FR-001 the HTTP family catches it — since SPEC-073
  in `HTTPSink._exchange`, the one classification the round and `_send` share — counts the
  request in `failed` at its one attempt with a `connection error, OverflowError` line and raises
  `SinkDeliveryError`, every `emit` (*A sink declares what it retries, and anything else fails at
  once*); the value still never delivers, so the item stays open. It is a `float` door of SPEC-049's, not an `int` door, so SPEC-066 did not change it.
  **Closed by** an upper bound in `require_timeout` at what the socket accepts, refused rather
  than floored if no value above it delivers — once the bound and its message are settled.
- **The after-shutdown submission line announces a loss the final drain can still deliver**
  (SPEC-068; invariant 2). `Worker._count_undeliverable` writes `lost N submission(s)` ("nothing
  will drain these") for a submission made after the retirement count moved, but one made before
  `_final_drain`'s last read is still taken by that drain and delivered, or announced again by
  `_emit` if it fails. `Worker.submit`'s docstring already calls the line over-strong.
  **Closed by** writing that `lost` line only once the final drain's last read of the queue is
  done or the drain thread is dead.
- **`flush()` and `configure(sink=…)` called on the drain thread wait on themselves** (SPEC-068
  spec review; invariant 3). A sink's `emit` that calls either waits on the thread it runs on:
  measured, a 1.009 s `timed-out` flush and a 5.007 s stranded swap. SPEC-068 FR-006 closed this
  shape for `shutdown()` only. **Closed by** a decided answer for a flush made on the drain thread,
  which needs a new `FlushResult.reason` on a frozen public surface (SPEC-034), and a swap that does
  not wait on the thread it runs on.
- **A shutdown sentinel can still strand behind the final drain on the ordinary path** (SPEC-068
  diff review; invariant 2). The first caller queues `_SHUTDOWN` and then sets the stop event, but
  a drain thread that wakes on the stop event first — at the end of an emit, or from a backoff —
  can run `_final_drain` and make its last read before the put lands, so the sentinel is never
  consumed and `health().queued` reads 1 for good with `submitted_after_shutdown` at 0. Measured
  by SPEC-068's second diff review in roughly 1 to 8 of 300 races against the worker as it stood
  before SPEC-068; it predates that spec, which closed the same shape
  for the caller that takes over a drain-thread join (that one no longer queues a second sentinel).
  **Closed by** putting the sentinel before the stop event can wake the drain, or by
  `_final_drain` re-reading the queue after the stop until it has taken a sentinel.
- **A `shutdown()` called inside an orphan-path sink's `emit` closes that sink from inside its own
  emit** (SPEC-068 diff review; invariant 5). With no span active, `log_foundry.info()` emits on the
  caller's own thread (`api.py`), and `Runtime.held` asks only about the worker's thread, so the
  closer takes and closes the sink under the emit that called it. A sink whose `emit` and `close`
  share a non-reentrant lock never returns from `info()`. The same before SPEC-068. **Closed by** a
  per-thread inside-emit marker set around the orphan path's `sink.emit`, which `held()` consults.

### Resolved

- ~~**`configure(sink=object())` is accepted, and every batch is then lost on the drain
  thread**~~ (SPEC-080 frame-2 review; invariant 13) → **closed by SPEC-079 for what `configure()`
  can decide.** Reproduced at `4f0c5741`: a traced call's three events ended in `lost 3 event(s);
  batch abandoned after 4 emit attempts`. `configure(sink=…)`, and the inner sinks of
  `FilteringSink`, `TransformSink` and `MultiSink`, now refuse a value that can never be a sink —
  exactly one of a closed list of builtin types, `_sink_arg._UNPATCHABLE` — with a `TypeError`
  naming the argument and the type. A missing `close` is never refused, and a value that could
  still gain an `emit` is accepted (Andrew's decisions of 2026-11-05); the user-class case that
  never gains one is §13's constraint.

- ~~**`_fork._reinit_primitives` can exhaust memory, not merely hang.**~~ → **closed by SPEC-079.**
  Its container read was `list(container)`, so a `list` subclass with a non-terminating
  `__iter__` reachable from any sink never returned — measured 5.7 GB RSS in nine minutes, with
  the parent unable to kill the child because it was being starved. The module-wide walk is
  deleted; the one walk from the runtime's roots reads every container through its builtin
  base's own iteration, so a subclass's `__iter__` never runs, and bounds every read by
  `_MARKING_CEILING`. A graph genuinely larger than the ceiling trips the marking frontier, which
  degrades to a leaked handle; the repair frontier reads one level and has no visit cap.
  `tests/test_sink_ownership.py::test_a_runaway_container_trips_the_ceiling_and_refuses_everything`
  runs it in a real child.

  > **Superseded in part by SPEC-081.** A trip no longer degrades to a leaked handle by refusing
  > every unrecorded sink: the child marks the sink-shaped objects on its inherited heap instead
  > and carries on as a normal child, and only a heap scan that raises or meets a frozen heap, or a
  > walk that raises, leaves it refusing every unrecorded sink and every `sink=` the library did not
  > build. The same test now reads the flag clear and the unrecorded sink still refused. Decision:
  > `docs/decisions/sink-lifecycle/` (*A process releases only a transport it acquired here*).

- ~~**An empty `auth` token or pair is accepted, and sends an empty credential**~~ (HTTP auth fix's
  frame-2 review; invariant 13) → **closed by the 1.2.0 follow-up fix (no spec)**, by refusal, at
  Andrew's direction. `HTTPSink(url, auth="")` sent `Authorization: Bearer ` and
  `auth=("", "")` sent `Basic Og==` (reproduced at `a8d87119`), so against an endpoint requiring
  credentials every request was refused for the life of the process. Both are now a `ValueError`
  at construction from `HTTPSink` and every sink forwarding `auth`, naming `auth` and never a
  value, judged on the exact-`str` copy after the `Authorization` conflict, so an empty `auth`
  beside that header gets the conflict's message and `SplunkHECSink` keeps its own refusal of any
  `auth`. A pair with one empty half, `("key", "")` or `("", "token")`, still constructs: RFC 7617
  allows it and real services take a key as the user with no password, so it was not already
  broken. `tests/test_sinks_http_auth.py` guards both across every forwarder. The reasoning:
  *`auth` beside an `Authorization` header is refused, never resolved by precedence*, in
  `docs/decisions/sink-delivery/`.
- ~~**A caller's `headers=` silently replaces a platform sink's own credential header**~~ (HTTP auth
  fix's build; invariant 13) → **closed by the HTTP auth fix (no spec)**, by refusal. A caller
  name folding to Splunk's `Authorization`, Datadog's `DD-API-KEY`, New Relic's `Api-Key` or
  Honeycomb's `X-Honeycomb-Team`, in any case, used to be sent instead of the required `token` or
  `api_key`; `merge_headers`, called by each with `argument=`, now refuses it at construction,
  naming the argument it would replace and no value, the set being every key of the base each sink
  hands it. Without `argument=` it lets the caller win as in 1.x, so a third-party sink's override
  of its own default still sends what it sent. So `SplunkHECSink`'s `auth=` refusal, which says the
  sink sends its token as `Authorization`, is true in every case. Its twin, two caller names
  folding to one wire name (`{"x-a": "1", "X-A": "2"}`), used to send only the last and is refused
  by `_valid_headers`. Guards: `tests/test_sinks_http_header_roster.py`, a run-time roster that
  replays as a caller header each header one default first-attempt emit per HTTP sink sends, and
  requires a refusal; and, for what that one request never exercises (a header written only under
  `gzip=True`, on a retry or 413 split, or for a non-default option), the AST route scan in
  `tests/test_sinks_http_header_collisions.py`, which names a subscript write or an `.update` or
  `.setdefault` call on any `headers` attribute, in any method, and a write of a `"headers"` key.
  The reasoning: *A caller header that would silently replace another is refused*, in
  `docs/decisions/sink-delivery/`.
- ~~**An `auth` that unpacks only once is accepted at construction, and every `emit` then
  fails**~~ (SPEC-071 frame-2 review; invariant 13) → **closed by the HTTP auth fix (no spec)**.
  Credentials are copied once, at construction, as exact `str` (`_diag._STR_STR`; a pair unpacked
  once by `http._copied_pair`), and the `Authorization` value is built and checked from the copy
  then, once; every request writes that stored value, so no caller code runs per request and
  `HTTPSink(url, auth=iter(("u", "p")))` sends Basic auth on every one. A pair outside
  `tuple[str, str]` used to construct and send whatever its unpack gave (a set's hash order, a
  mapping's keys, a `bytes` half's `repr`). A `set`, `frozenset` or mapping is now refused by real
  type before it is unpacked, and a half that is not a `str` after, each a `ValueError` naming only
  the real types; every other value the unpack refused keeps its message.
- ~~**`auth=` beside a caller's `Authorization` header is silently ignored**~~ (SPEC-071 frame-2
  review; invariant 13) → **closed by the HTTP auth fix (no spec)**, by refusal. Only the
  exact-case spelling won; under any other case `auth` won and the header was dropped, since
  `urllib` folds the names. `auth` beside an `Authorization` header in any case, matched by the
  name `urllib` sends, is now a `ValueError` at construction from `HTTPSink` and every sink
  forwarding `auth`, and `SplunkHECSink`, which sends its token as that header, refuses any `auth`.
  `tests/test_sinks_http_auth.py` guards both entries across every shipped subclass. The
  reasoning: *`auth` beside an `Authorization` header is refused, never resolved by precedence*,
  in `docs/decisions/sink-delivery/`.
- ~~**`configure()`'s stamps, its numeric ceilings, the `_retry` doors and `OTLPSink`'s endpoint
  test their arguments with `isinstance` and name them with `type(value).__name__`**~~ (SPEC-080
  frame-2 review; invariant 13, the "argument named" half) → **closed by SPEC-071 FR-005**. The
  seven doors (`config._require_text` and `_require_positive`, `_retry.exact_seconds`,
  `require_positive`, `exact_int` and `wait_budget`, and `OTLPSink`'s endpoint) now test
  `issubclass(type(v), T)` and name the type through `_diag._STR_STR(_diag._STORED_NAME(type(v)))`,
  so a value whose `__class__` property claims or raises, or whose metaclass `__name__` property
  renames its type, gets the door's own `TypeError` naming the argument and the real class, and
  none of its getters runs. An honest wrong type's message is byte-identical. `_retry._exact_number`
  routes the same way, so a real `float` subclass whose `__class__` claims `int` is read as the
  float it holds where it used to raise `int.__index__`'s descriptor error. The spec also closed
  sites the entry did not list: the HTTP sinks' `auth`, which a value claiming `str` sent down the
  token route into its own `__contains__`. It is now judged by its real type, so such a value takes
  the `(user, password)` route without its `__class__` or `__contains__` running, and gets the
  existing `ValueError` naming `auth` when it cannot be unpacked as a pair; one that unpacks into
  two items is accepted and sent as Basic auth, its `__iter__` run. And seven callable constructor
  arguments that accepted a non-callable and then failed in `emit` or `close()`, now refused at
  construction through `_args.require_callable`.
  > **Superseded in part by the HTTP auth fix (no spec).** A value that unpacks into two items is
  > accepted only when both are real `str` and it is not a `set`, `frozenset` or mapping; anything
  > else is refused naming its real types. Its `__iter__` runs once, at construction, and every
  > request sends the value built then. The decision: *`auth` beside an `Authorization` header is
  > refused, never resolved by precedence*, in `docs/decisions/sink-delivery/`.
  `tests/test_type_door_roster.py` holds the ten doors by name and reports any `isinstance` on a
  door's parameter outside `decorator._refuse_unusable`'s kept proxy test. The reasoning: *An
  argument door asks the type, never the value*, in `docs/decisions/sink-delivery/`.
- ~~**Queued items are announced twice on an expired exit drain**~~ (SPEC-062 frame-2 review;
  invariant 2) → **closed by SPEC-068**. The `ShutdownTimeout` line called the queue lost, and the
  decline or abandon line announced the same items again once `_final_drain` took them; its count
  was `qsize()`, so it also called the sentinel `stop()` had just queued a lost item. The line is
  now written by `_diag.pending`, which claims no loss, and its queued clause counts submissions
  only (`Worker._queued_submissions`), so each event queued or in flight at the expiry is announced
  lost at most once. The unconfirmed-swap line, its twin, was rewritten the same way and now fits
  `_diag`'s detail bound instead of shipping cut off mid-word.
- ~~**A second `shutdown()` after an expired one returns at once, whatever its budget, with the
  sink still open**~~ (SPEC-062 frame-2 review; invariants 3 and 5) → **decided final by
  SPEC-068**. An expired `shutdown()` is final: a later caller waits only for a stop still in
  progress, and the sink an expired call left open is closed by the first call that finds the
  drain thread ended. **The cost:** in an `atexit` exit after an expired `shutdown()`, a buffering
  sink whose drain thread is still inside an emit is neither flushed nor closed. **The reason:** an
  exit that has already spent the caller's budget does not spend a second one — a waiting `atexit`
  would add `DEFAULT_SHUTDOWN_TIMEOUT` (30 s) to every exit over a wedged sink. Reasoning in
  `docs/decisions/pipeline/`.
- ~~**`_reinit_after_fork(resume=True)` does not reset `_stopped`**~~ (SPEC-062 frame-2 review;
  invariant 12) → **closed by SPEC-068**. Resetting `_stopped` alone was measured and is not
  enough: the child also inherits the stop event **set**, and its resumed drain loop exited at
  once. The resume branch now resets `_stopped` and the owed join and takes its event from
  `refresh_stop_signal()`, as a new worker does; a resumed child delivers and declines retries
  against its own deadline. The route is internal: only a direct `Worker.stop()` leaves a live
  worker stopped.

- ~~**A mapping key built from a shared sub-structure is rendered whole before any budget applies**~~
  (SPEC-057, Accepted Limits; invariants 1 and 8) → **closed by SPEC-063 FR-001**. A key whose real
  type subclasses `tuple`, `list`, `dict`, `set` or `frozenset` (and not `Enum`) is no longer
  rendered by `str(key)`. It is coerced by the pass as a value at the entry's value depth, so it
  spends a node for itself and one per member per path, its members are charged to the byte budget,
  and `max_depth`, `max_keys` and the cycle check apply; its text is the JSON of the coerced form,
  clipped to `max_value_bytes`. Measured at `71e42dd`, `f = frozenset({f, (f, i)})` applied 14, 16,
  18 and 20 times cost 0.008, 0.034, 0.142 and 0.572 s of CPU as one key, doubling per level, and a
  tuple key of 64 references to one 1 MiB string rendered 64 MiB before it became `<size limit>`;
  forty levels now return in milliseconds with `truncated` set. A container key is walked by
  `members()` or `mapping()` and never through `_dispatch`, whose `isinstance(value, UUID)` branch
  would hand a real tuple that claims `UUID` through `__class__` to the builtin `repr`. A `bytes` or
  `bytearray` key is priced at `4 * len + 3` before it is rendered (a 64 MiB `b"\x00"` key peaked
  at 256 MiB). The remaining cost is stated, not open: an object that is itself the key keeps
  `str()`, and `_coerce_unbounded` extends its accepted DAG cost to a DAG key. An `Enum` member
  whose real type is a builtin container or bytes is written as `Type.NAME`, computed, never
  `str()`, so a `(tuple, ReprEnum)` member holding the DAG, or a real tuple an `mro()` forges into
  `Enum`, no longer hangs or writes a member's `repr`. Decision:
  `docs/decisions/event-assembly/a-route-that-trusts-the-callers-object-admits-it-by-exact-type-or-an-unbound-accessor.md`.

- ~~**An object that only claims a scalar type is routed as that type and reaches the event as
  itself**~~ (SPEC-057 sweep; invariant 8) → **closed by SPEC-063 FR-002**. Every route that returns
  the caller's object, or lets its method decide a measurement, admits it by exact type or after an
  unbound `int.__int__` or `float.__float__`, which return an exact number for a genuine subclass and
  raise `TypeError` for an impostor, now a `<unserializable: T>` placeholder naming its real type.
  `kind in _PLAIN_SCALARS` is `kind is bool` and the `isinstance(value, bool)` branch is gone; the
  Enum member routes and the float key branch follow the same rules; a real `str`, `bytes`,
  `dict`, `tuple`, `list`, `set` or `frozenset` subclass is routed by `issubclass(type(value), ...)`
  ahead of the render-through `isinstance` branches, and the `Enum` branch, whose fallback is
  `str()`, is entered only by a real `Enum` or by a non-builtin such as a proxy (a real `tuple`
  claiming `Enum` wrote `(SECRET,)` before that test); a container key is tested ahead of `_key`'s
  claim-based `str`, `int` and `float` tests; and `_key` admits an `int` key by
  `type(key) is not bool`.
  Measured at `71e42dd`, the three routes the entry named each put the object itself in the event, as
  did an `Enum` member whose `.value` was a `float` impostor, a route it had not named. `isinstance`
  stays only on the render-through routes, so a transparent proxy of a `dict` or `datetime` still
  renders. The same edit made `_type_name` read the stored name through `str.__str__`, and read it
  in `model` and `_diag` too, where a metaclass `__name__` property had cost a span its events and
  silenced the announcement of a counted loss.
- ~~**`_diag.rejected` echoes an inbound value through a `repr()` of the whole value**~~ (SPEC-064
  system-frame review; invariants 1 and 11) → **closed by SPEC-067**. `rejected` called
  `repr(value)` and only then cut the result to `_MAX_REJECTED_ECHO`, so a shared-sublist DAG passed
  to `continue_trace` never returned, and a value whose `__repr__` raised gave
  `reason="rejected"` with no line written, because the guard swallowed the whole write
  (measured at `7501e5e`). The echo now runs no code of the value: a `str`, exact or a subclass, is
  its first 64 characters read through `str`'s own slots, quoted, escaped and cut; anything else is
  `<TypeName>`, read through `type`'s own descriptor; and `<?>` stands in when even that fails, so
  the line is always written. The spec also closed the parser half, which the entry did not list:
  `continue_trace` raised into the caller on a `str` subclass with an overridden method or on a
  value whose `__class__` is a property, because the parsers called `isinstance` and then the
  value's own methods; and a valid `str` subclass was adopted as itself, so a later
  `current_traceparent()` raised that subclass's own exception. The parsers now accept only an
  exact `str`, and `continue_trace` turns a subclass into the exact `str` it holds once, on entry.
  The reasoning and its rejected alternatives: *An inbound value runs no code of its own anywhere
  on the inbound path*.
- ~~**`@trace(name=...)` tests its argument with `isinstance(name, str)`**~~ (SPEC-067 frame-2
  review; invariant 13) → **closed by SPEC-080**. `isinstance` reads the value's own `__class__`,
  so a `name=` whose `__class__` property returned `str` decorated cleanly and every call then
  wrote "absorbed a failure while opening a span (TypeError); this call runs untraced" and ran
  untraced, and a raising one raised its own exception at decoration (reproduced at `97436c7`).
  `name=` is now routed on its real type, `issubclass(type(name), str)`, in `trace`'s own body: a
  `str` subclass is kept as the exact `str` it holds (`str.__str__`), so its `__bool__` no longer
  runs at decoration, and anything else not `None` raises the `TypeError` at the
  `trace(name=...)` call itself, before any decorator is applied, naming its type's stored name.
  The spec also closed the decorator's sibling sites, which the entry did not list. `_span_name`'s
  fallback read `type(fn).__name__`, so a metaclass `__name__` property returning `1` decorated and
  then absorbed every call, and one that raised raised at decoration; it now keeps a `__qualname__`
  only by real type, as an exact `str`, and otherwise reads the type's stored name through
  `type`'s own descriptor. The `str` slip was tested with `isinstance` behind the descriptor test
  and echoed with an unbounded `repr` (a 10,000-character `str` gave a 10,069-character message,
  measured at `253432f`); it is now tested first, by real type, and echoed through `_diag._echo`,
  cut at 64 characters, and a callable that merely claims `str` is traced. The misordered-descriptor
  message read `fn.__func__` unguarded and named the type through `type(fn).__name__`, and the
  non-callable message read the type name the same way; both now read only what they have checked,
  through `_misordered` and `_stored_name`. The descriptor test for a proxy keeps `isinstance`, last
  and reached only by a callable, on purpose. The reasoning, the check order and its trade-off:
  *`@trace` routes what it is handed on its real type, and only a callable runs its own code at
  decoration*.
- ~~**A float passes the `max(x, 0)` floor on `max_retries` and `backup_count`, and breaks every
  `emit`**~~ (SPEC-065 system-frame review; invariant 13) → **closed by SPEC-066**. Every `int`
  floor in a sink constructor and the worker — `max_retries` on twelve sink constructors and
  `Worker.__init__`, `RotatingFileSink`'s `max_bytes` and `backup_count`, `HTTPSink`'s
  `max_batch_count`, `PostgresSink`'s `connect_timeout` and `GooglePubSubSink`'s `max_pending` —
  goes through `_retry.floor_count`, which refuses a non-`int` with a `TypeError` naming the class,
  the argument and the type, and floors the exact `int` the value holds (`int.__index__`), so a
  negative still floors and a subclass whose comparison lies can no longer keep one. Each judgement
  precedes any client, connection or file the constructor builds. `GooglePubSubSink(max_pending=inf)`,
  which meant no bound, is refused with the rest. `tests/test_numeric_door_roster.py` derives the
  sites from the AST and fails naming any new `int` floor or range check left unjudged.
- ~~**`flush(timeout=)` and `shutdown(timeout=)` take `inf` and `nan` unchecked**~~ (SPEC-065
  system-frame review; invariants 1 and 13) → **closed by SPEC-066**. Both pass `timeout` through
  `_retry.wait_budget` at entry, before anything is claimed or counted: `inf`, or any value above
  `threading.TIMEOUT_MAX`, is the unbounded wait `None` is; `nan` is refused with `ValueError` and a
  value that is not an `int`, a `float` or `None` with `TypeError`, both naming `timeout`; a value
  below `-threading.TIMEOUT_MAX` becomes `0.0`, so no negative overflows the deadline sum. Zero and
  negatives keep their meaning, and a refused `shutdown()` leaves the process unretired, so a retry
  with a good value drains and closes.
- ~~**`SocketTransport(port=2.5)` is accepted and raises out of every `emit`**~~ (SPEC-065
  system-frame review; invariant 13) → **closed by SPEC-066**. The port goes through
  `_retry.exact_int`, rebound to the parameter, and the `0`–`65535` range check runs on the exact
  `int` it returns, so a float, `nan`, `inf` or `str` is refused with a `TypeError` naming `port`,
  and an `int` subclass is judged by the number it holds. `SyslogSink` and `LogstashSink` refuse it
  through the transport they build. `NATSSink.max_reconnect_attempts` was closed the same way.

- ~~**`configure()` accepts a ceiling that is not an `int`**~~ (SPEC-057 system-frame review;
  invariant 13) → **closed by SPEC-065**. `config._require_positive` tested only `value < 1`, so a
  `float`, `nan` or `inf` was stored as given and `sanitize` then did integer arithmetic on it on
  every event. It now refuses a value that is neither `None` nor an `int` instance with a
  `TypeError` naming the ceiling and the type, never flooring it, and stores an accepted `bool` or
  `int` subclass as the exact `int` it holds (`int.__index__`), so a subclass whose comparison lied
  about a zero can no longer reach `sanitize`. The sink-side twins, `_retry.require_positive`,
  `require_timeout` and `usable_timeout`, were closed with it: they test and return the exact
  builtin, and the two callers that discarded the return now use it.
- ~~**`format_baggage_header` renders every baggage value with `str()`, so it is neither bounded
  nor total**~~ (SPEC-057 system-frame review; invariants 1 and 6) → **closed by SPEC-064**.
  `current_baggage_header()` now renders through `sanitize_fields`, one pass over the whole baggage
  under assembly's ceilings and budgets, so a shared-sublist DAG returns and a value whose
  `__str__` raises becomes `<unserializable: T>`; the guard wraps the whole per-member render and
  names the type through `sanitize._type_name`. Two further failures the entry did not list are
  closed with it: a lone surrogate, reachable from `set_baggage` and from an inbound header through
  `continue_trace`, made `quote` raise `UnicodeEncodeError`, and an integer past CPython's
  4,300-digit limit made `str` raise `ValueError`. The header is also capped at
  `BAGGAGE_MAX_BYTES` by whole members, as a prefix, because a value at the default ceiling alone
  exceeded the size the next process's `parse_baggage_header` accepts and had it refuse the whole
  header; a member with an empty key is skipped for the same reason, and a drop is announced on
  stderr as a count. The spoofed-scalar route this entry left open was closed by SPEC-063: no
  object survives coercion as itself any more, so the guard is a backstop for any coercer. The reasoning and its rejected alternatives: *The outbound baggage
  header renders through assembly's coercion and is capped by whole members*.
- ~~**`HoneycombSink` never reads its per-event verdict**~~ (found by the system-frame review of
  #283; invariant 2) → **closed by the fix that overrides `HoneycombSink._handle_response`**.
  `/1/batch/<dataset>` answers `200` with one `{"status": ...}` per event, and the sink inherited
  the default hook, which ignores the body, so an event rejected inside the `200` was counted
  nowhere. The array is now read positionally and all of the chunk or none of it: a status outside
  `2xx` is counted on `rejected_events`, and a body that is empty, unreadable (`b""`), not an array
  of objects, the wrong length or holding a non-integer status is counted whole on
  `dropped_unadjudicated` — both feed `losses().failed`, and neither is re-sent. A chunk whose every
  event was rejected is undelivered, so it raises (invariant 7). The reading and its rejected
  alternatives: *A Honeycomb batch response is a per-event verdict, read all of a chunk or none of
  it*.
- ~~**`NATSSink`'s exit drain can still exceed `shutdown()`'s join** (SPEC-047 FR-001).
  `publish_timeout` bounds one `emit`, but `Worker._emit` retries a failing batch
  `max_retries + 1` times — four at the default — and its inter-attempt wait returns immediately
  once the stop event is set, which it is for the whole of `_final_drain`. So the exit drain's
  worst case is `(max_retries + 1) x publish_timeout`: 40 s at the defaults against a 30 s join,
  measured at **8.01 s against an 8.00 s prediction** with `publish_timeout=2.0`. An expired join
  leaves the sink open (SPEC-027 FR-004). It is recorded rather than fixed because closing it
  means bounding the worker's retry of an *already-bounded* batch, which is every sink's question
  and not this sink's. **Closed by** a spec that gives `Worker._emit` a total budget, or by the
  caller lowering `publish_timeout` to 7.0 or below.~~ → **closed by SPEC-062 FR-001**. It was
  every sink's question, and it is answered at the worker: `Worker._emit` times each attempt and
  declines a retry predicted to end after the `shutdown()` deadline — the batch's longest attempt
  so far plus a 0.1 s join margin past it. The first attempt is always made and no knob is added,
  the budget being the caller's `timeout`. At NATS's defaults the third attempt is declined at
  20 s and the close runs; the exit drain's worst case is the join plus up to two emits — the
  one in flight when the budget runs out, and the first attempt of a tail batch, which is never
  declined (SPEC-062 FR-004's residual). Measured at `71e42dd` with a 0.5 s failing
  sink under `shutdown(timeout=1.2)`: 3 attempts, the join expired and `close()` ran 0 times;
  after, 2 attempts and one close, pinned by `tests/test_worker_exit_budget.py`.

- ~~**`health().inherited_sink` reads the owed record's last entry**~~ (SPEC-045; invariant 12) →
  **closed by SPEC-054 FR-005**. Arming order does not make a sink the installed one: `_swap_sink`
  seeds the record with the new sink and a preempted emit then appends the superseded one, so the
  order could be `[live, superseded]` and the field could name a sink this process had stopped
  delivering to. It answers from the **config** now, which is what this section already named as
  the authority for "installed", and `_worker_health`'s `sink` field takes the same authority in
  the same edit — answering the two from different places is what let them disagree. Pinned on
  both delivery paths and in a forked child, by
  `tests/test_sink_ownership.py::test_health_reports_an_inherited_sink_on_the_worker_path_too` and
  its orphan-path sibling.

- ~~**`NATSSink(max_reconnect_attempts=0)` makes the connect loop unbounded**~~ (SPEC-047 FR-002;
  invariant 13) → **closed by SPEC-049 FR-004**. `nats-py` retires a server from its pool only
  under `if max_reconnect_attempts > 0`, so a non-positive value never retires one and the
  constructor never returned — measured by SPEC-047, both `0` and `-1` still blocking at 30 s, one
  probe past 400 s, where `1` raises in 2.02 s. SPEC-047 documented rather than corrected it,
  because overriding a driver's documented behaviour surprises a caller who knows it, and named a
  `ValueError` as the closure. 1.0 is the major version that could take the small breaking change:
  a non-positive value is now refused at construction, and refusing is not the overriding that
  sentence rejected — the caller is told rather than quietly given different behaviour.
- ~~**`NATSSink(client=X, servers=…)` ignores `servers` silently**~~ (SPEC-047, Out of Scope;
  invariant 13) → **closed by SPEC-049 FR-004**. The same shape SPEC-047 FR-002 made a
  `ValueError` for the four connect timeouts, in the same constructor, left alone because
  changing it would break a caller passing both. `servers` now joins that conflict message,
  checked from a structure separate from the forwarded options so `nats.connect(servers or …,
  **supplied)` never receives it twice.
- ~~**A lone surrogate leaves `build_event` intact and costs the whole batch in every sink that
  encodes a string strictly**~~ (2026-09-04 audit, N1; invariant 8) → **fixed in SPEC-055
  FR-001**. `sanitize._measured` encoded with `errors="replace"` only to *measure*, so
  `os.fsdecode(b"file-\xff.txt")` left assembly intact; the JSON sinks escaped it and every sink
  binding `function` as a raw column raised on it, measured on `SQLiteSink` at `98c7e78` as
  `lost 3 event(s); batch abandoned after 4 emit attempts` with `failed_batches=1`. The clip
  helpers now return the replaced string — one U+FFFD per surrogate, `truncated` set — as an
  exact `str` measured through `str.__str__`, which reaches `function`, `message`, `fields`
  (keys and values), baggage, `defaults` and the `error` sub-document on both paths, and so
  every sink the entry listed. The four prose sites that asserted the pass-through were
  reworded with the fix. Full reasoning: `docs/decisions/event-assembly/` ("An event is safe by construction").
- ~~**`service`, `version` and `env` reach the event without passing through `sanitize`**~~
  (invariant 8) → **half fixed in SPEC-055 FR-001, half recorded**. `configure()` now refuses a
  stamp that is not a `str` (`TypeError`) or does not encode as UTF-8 (`ValueError`), naming the
  argument, before the sink is stamped into the ownership record, and stores `str.__str__` of
  it (invariant 13's shape); the surrogate route to the column sinks is closed. What stays is
  the ceiling: an over-long stamp is copied unbounded, recorded on `invariants.md` §8 as an
  exception rather than clipped, because it is a value the caller wrote once and can see.
- ~~**A mapping key whose `__str__` raises replaces the whole sibling mapping, unmarked**~~
  (2026-09-04 audit, N4; invariant 8) → **fixed in SPEC-055 FR-004**. `key()` is total: a key
  whose rendering raises becomes `<unserializable key: T>` and sets `truncated`, so the siblings
  keep their values, nested and at the top level of `fields`. The collision the entry asked the
  spec to settle is accepted rather than numbered — two hostile keys of one type collapse onto
  one placeholder, the later wins, and the collision is marked because each replaced key sets
  `truncated` — since a numbered placeholder is the first step toward one that carries the key's
  identity. Both placeholders now go through `text()`, so a type named longer than
  `max_value_bytes` honours the ceiling too.
- ~~**`NATSSink.close()`'s real bound is the driver's, and it is not reachable**~~ (SPEC-047
  FR-002; invariant 3) → **closed by SPEC-061 FR-004**. The sink no longer calls the driver's
  `drain()`, which swallows a cancellation and runs its `finally` flush to the driver's own 10 s —
  measured at `71e42dd` as 12.00 s under a 2 s `wait_for` once a JetStream client holds its
  response-inbox subscription. `close()` drives its own sequence under one deadline, `drain_timeout`
  or 1.0 s by default: a server round trip, then `client.close()` as a task cancelled on expiry,
  then the loop stopped and its thread joined; the stated worst case is the deadline plus 2 s.
- ~~**`NATSSink`'s `is_connected` guard has a wide, unmeasured window**~~ (SPEC-047 FR-004;
  invariant 7) → **closed by SPEC-061 FR-002**. Measured at `71e42dd`, the window was 75 of 75
  emits accepted over 148 s under `docker pause`, because the driver's ping task only ran while a
  call was in progress. The loop now runs on a sink-owned thread for the sink's life, and for a
  client the sink connected itself `emit` publishes only after the server answers a PING inside
  `publish_timeout`; the flag stays in front of the round trip as the cheap refusal. A core batch
  is confirmed by a second round trip or counted (FR-003).
- **The exit close ran the owed sinks in sequence** — introduced by SPEC-045, which made the
  owed-close record a set without changing how the set is drained → **fixed in SPEC-046**: the
  closes run concurrently and every one is joined, so the cost is the slowest rather than their
  sum, measured 8.02 s → under 4 s for four 2-second closes. Reusing SPEC-030's detached closer
  was tried and reverted — its grace caps at `DEFAULT_CLOSER_GRACE`, which abandoned 3 of 4. What
  remains is not this item but §13's: a *single* `Sink.close` is still unbounded, so one stuck
  sink still holds the exit.

- **Orphan logs** (emit standalone with a fresh `trace_id` vs warn-and-drop) → **emit standalone**,
  shipped in **SPEC-002**. A level call with no active span builds a complete event with a fresh
  `trace_id` and emits it synchronously on the caller's thread. Dropping it would make the emitters
  silently conditional on decorator placement, which is the opposite of what a logging call should
  promise. That synchronous path is also why `sanitize` must be total (SPEC-017), and why it needs
  a loss counter of its own: `Health` describes a worker and this path has none, so until
  **SPEC-036 FR-003** a process logging only this way reported all zeros over total, permanent
  loss. `orphan_lost` covers everything inside that guard — a sink that fails to construct as well
  as one that raises — and `in_span_lost` is its counterpart for an event that could not be *built*
  inside a span. Two fields, because one can mean the destination or the data and the other
  never the destination — the data, or no drain thread at all (SPEC-050 FR-003).
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

- ~~Emitting metrics or OpenTelemetry-native traces (we stay logs-only, but ID-compatible).~~ —
  **superseded in part, 2026-10-05.** In-library metrics stay a non-goal: they are derived
  downstream. OpenTelemetry-native traces are no longer one — OTLP spans built from `span.end`
  are a planned arc item, not yet built. Full entry: `docs/decisions/public-api/`.
- Querying, dashboards, or alerting — that's ELK / downstream.
- Log *routing* logic beyond a single configured sink per process.

### Known constraints

**What this section is.** A limit the design accepts and will keep accepting — usually because the
alternative was built and measured worse, or because fixing it would change a published contract.
It is **not** a backlog: something genuinely unfinished belongs in §12, which names what would
close it. A closed item is struck in place with the spec that closed it (SPEC-021's rule) and its
reasoning is *not* repeated here — that lives in `docs/decisions/` (the generated index lists its fence, and CLAUDE.md's Key
Decisions table names the area) and the delivery doc,
and a third copy is a fork with no merge.

- **`flush()` reports success over events `NATSSink`'s publish deadline absorbed.** Measured with
  a slow-but-healthy JetStream and `publish_timeout=0.2`: `flush()` returned `True` with 4 of 42
  events delivered and `health().sink` reading `failed=38`. This is SPEC-026's routing working as
  designed — absorbed partial loss is reported through `Health.sink`, and `flush()`'s verdict is
  built on `failed_batches`, which a returning `emit` does not move (SPEC-021). What SPEC-047 FR-001
  adds is that `NATSSink` can now absorb loss for a **timing** reason against a destination that is
  not failing, which it could not before. The alternative — raising when the deadline drops
  anything — would send a partially delivered batch back to `Worker._emit`'s retry and duplicate
  whatever landed, which is SPEC-018's rule and the reason partial failure must not raise.

- **`NATSSink` cannot drive a client it did not connect, so an injected client's liveness is the
  caller's.** A `nats-py` client is bound to the loop it connected on, and a flush driven from a
  second loop returned only at its own timeout — 3.001 s in three of three runs at `71e42dd`
  (SPEC-061, M10). So the server round trip before a publish, the confirmation after a core batch
  and the round trip at close apply only to a client the sink built from `servers=`; an injected
  `client=` keeps the driver's `is_connected` flag as its only guard, is never probed, and is
  closed rather than drained. A caller who injects a client owns its liveness.

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

  **Since SPEC-074 and SPEC-076 the wait is one round, never a backoff, and the lock is the
  closed guard's.** An engine sink holds its transport lock for one round inside `deliver` and
  waits between rounds with no lock held, so an orphan caller waits for at most the round the
  drain thread is inside. A `SERIAL` guard (the five engine sinks that cannot share their
  transport, and `FileSink`, `RotatingFileSink` and `SQLiteSink`) holds its lock across each
  section; a `CLOSE_ONLY` guard (`MongoDBSink`, the Redis sinks, `KafkaSink`) takes none, so on
  those an orphan call does not wait at all.

- ~~**A process that only ever used the orphan path never closes its sink**, and a sink
  adopted after `shutdown()` is closed by nothing.~~ — **fixed by SPEC-031 FR-006 and
  SPEC-033.** The reasoning is in `docs/decisions/pipeline/` ("The close is once-only across
  both delivery paths"), `docs/decisions/sink-lifecycle/` ("A sink handoff is owned by whoever is delivering") and in
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
  **The concurrent half was real after all** (2026-11-11): SPEC-078's review measured two
  concurrent `configure(sink=…)` calls closing one sink twice with nothing written in between,
  which `config._sink_lock` now prevents (§7).

- **`Worker._release_waiters` reads `queue.Queue`'s internals, and there is no public
  alternative.** It takes `self._queue.mutex` and iterates `self._queue.queue` — both private —
  to find the `flush()` markers still queued when nothing will ever read them again, so no
  caller sits out its full timeout. **It is one of three things that carry that guarantee, not
  the whole of it** (SPEC-050 FR-001): this sweep answers what is queued *at the moment it runs*,
  `flush()`'s own re-check answers a marker that arrived after it, and `flush()`'s sliced put
  answers a caller that could not enqueue one at all. "No caller who passed `timeout=None` waits
  forever" is the three together; each alone leaves a hole the others cover. The
  audit that produced SPEC-024..031 flagged it; **SPEC-031 FR-005 records it rather than
  changing it**, per SPEC-021's rule that an open item is closed by being fixed, settled, or
  recorded. It is called on the terminal-failure path, on the clean shutdown path since the
  shutdown-sentinel fix, and on the expiry path since SPEC-050 FR-001 — the same
  enqueue-after-the-drain race reaches all three.

  **The private read now covers only half of what the method answers** (SPEC-050 FR-001). A
  marker the drain thread has already dequeued is held in that thread's local, where no queue
  read can reach it, so those are tracked separately and answered **before** this read and
  outside its `try`. The blast radius below is therefore unchanged rather than widened: a CPython
  change still costs waiters whose markers were queued, and never one whose marker was in flight.

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

  **`Worker._queued_submissions` is the second reader** (SPEC-068 FR-001). It takes the same
  `Queue.mutex` and iterates the same `Queue.queue` to count the submissions still queued —
  every entry that is neither the `_SHUTDOWN` sentinel nor a flush marker, `_final_drain`'s own
  classification — for the expiry line and the unconfirmed-swap line, which `qsize()` cannot
  serve because it counts the sentinel and the markers too. Its blast radius is smaller than
  `_release_waiters`': the same CPython change would make it return 0, so both lines would lose
  their queued clause and say nothing false. A test replaces the mutex with one that cannot be
  entered and asserts exactly that.

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
  SPEC-030, the grace it grants a swapped-out sink's close — **one grace per call** since
  SPEC-078, whose close registry ends every graced wait of a `shutdown()` at one instant, where two
  waits each computing their own had measured 4.00 s against a 2.0 s grace — but not this close,
  the live sink's, which stays inline and unbounded, nor a shutdown's own fan-out closes, which are
  joined in full (SPEC-046). **The cost does not multiply by the number of sinks owed a
  close** (SPEC-046): they run concurrently and are all joined, so this limit is one close, not
  their sum. **It reaches the orphan path as well as the worker's**
  (SPEC-044 FR-006): a process that only ever logged outside a span closes through
  `_close_orphan_sink`, which calls `release(owed)` inline before `_shutdown_worker` consults its
  deadline at all. Measured 6.01 s against `shutdown(timeout=2.0)` on both paths, with a
  6-second `close()`, and 30.01 s against the same timeout with a 30-second one — the
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

  **SPEC-076 narrows what the close waits for, not the release.** A close on a sink's closed
  guard refuses every later caller from the moment it begins, so it waits behind at most one
  section: for a `SERIAL` guard the one holding the lock when the close began — one round of an
  engine sink, bounded by the driver call's own timeout where it has one, or one batch's write —
  rather than any number of callers queued on a lock that is not FIFO; for a `CLOSE_ONLY` guard at
  most `CLOSE_OVERLAP_WAIT` (0.5 s), after which it releases and a call still inside is counted or
  meets the closed client. The release itself — `client.close()`, `producer.flush()`, a file's
  `close()` — stays as unbounded as above, and what bounds the exit is still
  `DEFAULT_CLOSER_GRACE` for a detached close.

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

- **Nested-span memory is O(depth²).** The span stack is a copied tuple and each
  `contextvars.Token` pins its predecessor, so a call nested N deep holds N tuples of up to N
  entries until it unwinds. Irrelevant at real depths — 100 deep is about 50 KB and the stack is
  clean afterwards — and measured so the shape is on record (2026-09-04 audit, N10, at `456e9b7`):
  `tracemalloc` peak 5.3 MB at depth 1,000, 69.2 MB at 4,000, 266.5 MB at 8,000. A depth that makes
  this matter has recursed past anything a trace should describe.
- **The payload ceilings bound each *value*, and the pass budgets of 131,072 values and 16 MiB
  of text bound the work, not the event's size at a destination.** `max_value_bytes`,
  `max_stack_bytes`, `max_keys` and `max_depth` (SPEC-017) each bound one value, so a legal event
  can still be large: 256 keys × 8192 bytes is roughly 2 MB, past SQS's 256 KB — measured at
  `456e9b7` as a 2,060,506-byte event, serializable and marked `truncated` (2026-09-04 audit,
  N11). Acceptable because
  it is *visible* — a sink with a hard limit drops the event and counts `dropped_oversized`, so
  the loss is signalled where it happens rather than passed downstream silently. SPEC-057 bounds
  each coercion pass as a whole: at most 131,072 values visited and 16 MiB of text charged, and
  the value that crosses either budget becomes a marked `<node limit>` or `<size limit>` after
  which nothing is visited. A transform's types-only coercion runs outside both. That is a *work* budget on the caller's thread, not a limit tuned to
  any destination: `json.dumps` escapes a control character as six bytes, so the serialised worst
  case is about six times the byte budget per pass (96 MiB), and a boundary event takes two
  passes, `build_event`'s and `backfill_baggage`'s, so about twice that. ~~A per-event byte
  ceiling was deferred by SPEC-017, again by SPEC-020, and deliberately again by SPEC-021: it is a
  feature with real design surface (what happens on breach, which fields are sacrificed first) and
  belongs in its own spec.~~ Reversed by SPEC-057: a breach is a placeholder and a stop, and walk
  order decides what is sacrificed. Lower `max_value_bytes` / `max_keys` if your destination's
  limit is tight; that, not the budgets, is what meets it. Note `max_value_bytes` carries two
  units — UTF-8 bytes for a string, rendered decimal length for an integer (SPEC-020, SPEC-021).

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
  (`docs/process/reviewer-contract.md`) found the third.

  **Narrowed when the stop-signal hand left the lock** (fix after SPEC-079): `offer_stop_signal`
  now runs with no lifecycle lock held on every path, so its `_diag.absorbed` no longer writes
  under it. Of the paths above, only `_swap_sink`'s detached release, which writes on a
  thread-start failure, still writes while holding it.

- **A fork's repair stops at the library's own objects, and the things below sit outside it**
  (SPEC-039 FR-005, SPEC-079). The child's one walk starts from the runtime's roots — the
  configured sink, the worker and the late worker — and replaces locks only on what this package
  defines; it reaches a package object behind a third-party holder, or two containers down, from
  what its repair frontier forwards to its marking frontier, which repairs and re-acquires it
  **within that frontier's ceiling** (§9). What follows is reachable only by reaching into
  somebody else's state, which would be a fork fix that breaks a driver, or lies beyond that
  reach.

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
    sink the walk cannot enter. The one exception is a sink the forking thread was being offered
    (SPEC-083). The child runs nested inside that sink's setter call, so the rebuild skips it, and
    the child's `shutdown()` sets the event that sink holds instead. §13 has the entry *A child
    forked inside a stop-signal setter keeps the event it was being offered*. A **shipped** sink
    held inside a third-party wrapper is repaired and re-acquired since SPEC-079, from the
    forwarded seeds, within the marking ceiling; past a tripped ceiling it is neither. Its
    `SinkCounters`, and every engine sink's, are repaired
    whatever holds them, from a registry of the live ones.
  - **A package object reachable only through a callable is neither repaired nor marked**
    (SPEC-079). A third-party wrapper that keeps `self._emit = file_sink.emit`, a closure cell or
    a `functools.partial` holds the sink where no reading walk follows: descending into callables
    routinely escapes into application state through a callback's closure or a bound method's
    `self`, which would trip the ceiling in every child and make each one pay for a scan of its
    whole heap, and refuse every inherited sink it finds there (the bounded record's residual 6,
    below). Hand the library the sink itself, or build it in the worker process with
    `sink_factory=`.
  - **The converse costs a mixed-base class its foreign attributes.** Ownership is keyed on the
    whole MRO, because subclassing a shipped sink is a documented extension point and a
    defining-module test walked straight past a `_lock` that `FileSink.__init__` built — measured,
    the child hung. So `class MySink(FileSink, ThirdPartyBase)` has its *foreign* primitives
    replaced too, measured. A separately **held** client is still untouched; the two cannot be
    told apart from the instance, and refusing the mixed case would mean refusing the sink's own
    lock with it.
  - **A sink the application holds but never gave the library is not reached at all** — the walk
    starts from the runtime's roots, and such a sink is reachable from none of them. Same accepted
    boundary the locks already carry; `sink_factory=` is the library's way to build the sink in
    the worker process instead.

- **A sink `configure()` cannot prove unpatchable is accepted, and if it never gains an `emit`
  its batches are lost counted and announced** (SPEC-079 FR-005). Of the values themselves, only
  exactly one of a closed list of builtin types is refused at `configure(sink=…)` and in the
  wrappers; a graph that contains itself is refused as well, under the next item. A user-defined class
  with no `emit` is a heap type, which can always gain one, so it is accepted: on the worker path
  its batches are counted in `failed_batches`, on the orphan path in `orphan_lost`, and both are
  announced. Another unpatchable C type — `memoryview`, an extension type — is accepted the same
  way. The rejected predicate, "not a heap type and no instance `__dict__`", refused
  `weakref.proxy(sink)` and `super(Sub, inst)`, which deliver.

- **A sink loop is refused only where the library can see it** (the sink-loop fix, 2026-11-11).
  `_sink_arg.loop_in` follows the package wrappers' forwarding positions and `super` objects,
  statically, at a wrapper's constructor, at `configure(sink=…)` and at a factory build; a `super`
  is followed only when its `emit` resolves to a package wrapper's or a subclass's, and a `weakref.proxy` is caught
  only when it points back at a node already on the walk's path. Four loops are not found and still
  recurse until `RecursionError`, duplicating an `emit`'s batch on the way: one through a
  third-party sink's own code, since a sink that merely keeps a reference to its parent must not be
  refused; one that closes only beneath a `weakref.proxy` to a node off the path, whose referent no
  static read reaches; one formed after the last check ran — a slot assigned later, or two factory
  wrappers whose factories each wrap the other, building at the same moment on two threads; and one
  deeper than `_LOOP_CEILING` visits. The rejected runtime re-entry guard would catch them at a cost on every
  wrapper call (`docs/decisions/sink-lifecycle/`).

- ~~**After a tripped marking ceiling, a child that re-configures an unrecorded inherited sink
  owns it and closes it** (SPEC-042, SPEC-079). A marking that trips `_MARKING_CEILING` sets
  `_marking_failed`, which refuses the close of every sink that stays unrecorded. But a child
  that hands such a sink to `configure()` again — a bare re-hand, with the parent having held it
  only inside a third-party wrapper the tripped marking never reached — has `stamp` record it as
  the child's own, and the child's `shutdown()` then closes the parent's transport. Measured
  identically at `9b238733` and at SPEC-079's head; the fix is work of its own.~~ → **closed by
  SPEC-081.** A tripped child gives every sink-shaped object on its inherited heap that has no
  live entry a `_FOREIGN` one, and refuses the parent's sink re-handed bare, in a `MultiSink` or in
  a `FilteringSink`. Where that scan raises or meets a frozen heap, or the walk itself raises,
  `stamp` records as `_FOREIGN` every sink that it is handed with no live entry, unless the
  library built it (the bounded record's residuals 3 and 4, below).

- **A sink behind a `weakref.proxy` configured as the sink is not repaired in a forked child**
  (SPEC-079). FR-005 accepts a `weakref.proxy` as `sink=`, and the child records it, so it is
  never claimed; but its referent is reachable only by calling into the proxy, which the repair
  never does, so a lock the referent holds stays held by a thread that does not exist, and with
  a parent thread inside it at the fork the child's first emit blocks. A `super(Sub, inst)` is
  repaired: its `__self__` is read in C and the walk passes through it. Configure the sink
  itself, or build it in the worker process with `sink_factory=`.

- **A caller-wrapped, unbuilt factory sink builds in a child a sink nothing closes** (SPEC-079).
  If a caller wraps `get_config().sink` — the private `_FactorySink` — in a wrapper of its own
  and configures that, the config carries no factory, so a forked child inherits the parent's
  wrapper rather than adopting its own. Its first emit builds and stamps a sink of the child's,
  which the inherited, parent-owned wrapper chain never releases: a leaked handle per child.
  Configure `sink_factory=` itself rather than wrapping what it installs.

- ~~**The ownership record grows for the life of the process, and a forked child walks all of it**
  (SPEC-079). `_state._owned` holds a strong reference to every sink the process ever stamped
  (SPEC-042 FR-001), and a child's marking frontier seeds from every one of those references,
  bounded by `_MARKING_CEILING`. A long-lived process that reconfigures with buffering sinks and
  then forks therefore grows the walk until every child trips the ceiling, announces it, and
  refuses to close an inherited sink it has no record of — a leaked handle — with one exception
  that is a destructive close: a sink the child hands to `configure()` again itself, which the
  next item states. Measured in the suite at `44db79a6`: one xdist worker's record had grown to 92,940
  marking visits by the end of a run, which is how
  `test_a_child_of_a_process_that_built_no_worker_is_silent` failed; the suite's own reset now
  clears the record between tests. The remedy — the record seeds not descending into a
  superseded sink's caller data, or released sinks pruned from the record — changes SPEC-042's
  settled strong-reference record and is work of its own.~~ → **closed by SPEC-081.** An entry
  holds its sink strongly only while the library holds it, and weakly once it is closed or
  superseded, so a superseded sink is collected when the application drops it and its dead entry
  is pruned. A child's record seeds leave out a closed sink of a shipped class exactly, which is
  what the closed, superseded `MemorySink`s were. The suite's reset stays, as test isolation.

- **A forked child releases only a transport it acquired in *this* process, and what that
  cannot decide is listed here** (SPEC-042). The record is stamped when the library is handed a
  sink — `configure(sink=…)` and `_ensure_sink()`'s lazy default, over the whole reachable sink
  graph — and every close the library performs consults it. A child marks what its walk reaches
  that the parent never recorded, before any *other* fork handler runs, so "no record" is
  unclaimable for everything that walk reached. Eight things it does **not** settle:

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
  7. **`_state._marking_failed` catches an escaping fault and a failed heap scan, not a partial
     walk.** It is set when the walk itself raises, or when the heap scan that a tripped visit
     ceiling falls back to raises or meets a frozen heap (SPEC-081); a trip whose scan succeeds
     leaves it clear. Every read inside the walk is absorbed a level down in `_fork`, so a walk
     that quietly reached less than everything raises nothing and leaves the flag clear, and runs
     no heap scan; the sinks it missed are unrecorded rather than marked.
  8. **The record is bounded by what is alive, not by what was handed over.** An entry pins its
     sink only while the library holds it (SPEC-081), and a dead entry answers for nothing and is
     pruned, so the record follows the sinks still alive. What that leaves — a sink that cannot be
     weakly referenced, a sink the capture did not reach once its holder is gone, and the rest —
     is the bounded record's nine residuals, the next item.

  What a refused close costs, measured from the shipped `close()` bodies: `KafkaSink` and
  `GooglePubSubSink` do not deliver their buffer, `NATSSink` does not drain its loop, and
  `SQLiteSink` and `PostgresSink` do not **commit** — for those two the child's inserts are left
  uncommitted on a connection the parent also holds, which is nonetheless the safer outcome,
  since committing there writes into a transaction the parent may be mid-way through. A child's
  `SQLiteSink` no longer inserts anything to leave uncommitted: it refuses every `emit` (§12). A
  flush-without-release hook **shipped in SPEC-036 FR-002**, which was handed this roster of five
  rather than the two its own FR named. Of the five, three implement it — `KafkaSink`,
  `GooglePubSubSink` and `NATSSink` — while `SQLiteSink` and `PostgresSink` do not, because both
  commit inside `emit`, so nothing is outstanding once one returns and the commit in their `close`
  is belt and braces rather than a buffer. That qualifies what this roster measured from the close
  bodies alone. Its own population sweep then found three more: `SentrySink`, `LoggingSink`, and
  the three wrapper sinks, which forwarded no flush at all.

- **The bounded ownership record leaves nine residuals** (SPEC-081). The record pins a sink
  only while the library holds it, captures the sinks beneath one before it goes weak, reads a
  dead entry as absent, is pinned first in a child, and falls back to the heap when a child's
  marking trips. What that does not settle:

  1. **A sink that cannot be weakly referenced keeps a strong entry.** The record grows by one
     entry per such sink handed over or captured, as every entry grew before SPEC-081. A
     `weakref.proxy` is one, whether configured or captured, but it pins only the proxy. No
     shipped sink is one.
  2. **What the capture does not reach stays unrecorded once its holder is gone.** A holder the
     application keeps is a seed, so this applies only after the holder is collected: the rest of
     a capture that tripped `_MARKING_CEILING` or raised, which its line names; a sink more than
     one container level below a third-party holder; a sink reachable only through a bound
     method, a closure cell or a `functools.partial`; a sink placed beneath a superseded sink
     after that sink was demoted; and, beneath a holder whose class subclasses `Sink` or a shipped
     sink, anything past the package reach, since `_fork._is_owned` accepts such a holder and the
     capture enters it as it enters a package holder, never with the third-party reach. While the
     application keeps such a holder it is still a seed, since only an exact shipped class is left
     out of the seeds. If the application keeps such a sink and drops the holder, a child that
     hands the sink over again claims it, as it would a sink never handed over at all (item 1 of
     the list above).
  3. **The last resort leaves library-built graphs half-owned and fresh `sink=` sinks unclosed.**
     In a child whose heap scan failed or whose walk raised, a fresh `sink=` is never closed, with
     the costs the refused-close paragraph above lists. A factory that returns a wrapper has only
     the root recorded as the child's, so the wrapper's members are refused their close. Such a
     child also keeps every `sink=` that it is handed strongly, as a `_FOREIGN` entry, so its record
     grows by one entry per such sink for its life; that is the safe direction, since a strong
     entry is never answered for another object.
  4. **A frozen heap sends a tripped child to the last resort.** A gunicorn-style prefork server
     that calls `gc.freeze()` and whose children trip the marking gets the last resort in every
     such child, so a fresh `sink=` there is not closed. Use `sink_factory=`, which the last
     resort still closes. On CPython 3.12 "frozen" is read against a floor taken when the package
     is imported, because 3.12 keeps a few hundred immortal tuples in the permanent generation with
     no `gc.freeze()` (measured 375); a `gc.freeze()` made **before** the package is imported is
     then absorbed by the floor, so a tripped child of such a process scans a heap that hides the
     frozen objects, and a frozen sink it hands over again is unrecorded and claimable. On 3.13
     and later the floor is zero and every freeze is seen.
  5. **A C-extension sink type without GC support** is not in `gc.get_objects()`. It stays
     unrecorded in a scanning child, as in a normal child whose walk did not reach it.
  6. **A scanning child pays in refusals and in memory.** An inherited sink the parent never
     handed over is refused rather than closed. Garbage sinks not yet collected at the fork are
     pinned. Every page holding an inherited GC-tracked object is copied, because taking a
     reference writes its count — reasoned from how CPython stores reference counts, not
     measured.
  7. **A finalizer that runs under another library lock can still hang.** A collection that fires
     while the thread holds `_state._lock`, `_config_lock` or the close registry's lock can run a
     finalizer that forwards into a library wrapper's `close()`, and that can block on the lock
     the thread holds. This was already possible for a sink never handed over; SPEC-081 widens it
     to superseded ones. It is narrow: no shipped sink defines `__del__`, and a plain third-party
     sink's `close()` takes no library lock. The library's own reference drops inside those
     sections are not audited for the same effect. A finalizer under `_owned_lock` itself can
     still deadlock two threads: one that forwards a close whose entry carries a stop-signal hand
     a worker build declined reaches `_hand_after_close`, which takes `_state._lock` with
     `_owned_lock` held, the reverse of the `_state._lock` then `_owned_lock` that `_swap_sink`'s
     first section takes through `releasable`. It needs that declined-hand mark on the very sink
     being finalized.
  8. **Another library's at-fork handlers run before the pin.** An
     `os.register_at_fork(after_in_child=…)` handler registered before log-foundry's runs first.
     If it drops the child's last reference to an inherited sink whose entry was weak, that sink
     is collected before the pin runs. A collection the child's own first allocations trigger can
     do the same to an inherited sink that was already unreachable cyclic garbage in the parent,
     before the pin reaches it. The repair is never wrapped in `gc.disable()` (the order-of-work
     decision), so this is stated, not closed.
  9. **A re-pin costs a stamp walk of the live graph, and a wrapper the caller closes directly
     re-pins once per child.** Only the outermost performed release on a thread re-pins, so a
     wrapper the library closes re-pins once. Measured with a configured `MemorySink` holding
     100k events: one re-pin about 5 ms, a 20-member `MultiSink` shutdown 5.5 ms (0.05 ms before
     SPEC-081), and the same wrapper closed by the caller about 99 ms.

- **A sink shared across a fork is shared, and both processes act on it** (SPEC-039 FR-005
  AC-1). Beyond the buffer discard the library neither clones nor re-opens the inherited sink,
  so one socket or one file is now written by two processes — see §9 for the caller's remedy.
  A SQLite handle is the exception: a child's `SQLiteSink` refuses every `emit` rather than
  write through it (§12). Two consequences are worth naming because they are not obvious from that
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
  in that attribute. ~~Safe only because the drain thread never reads it~~ — struck (SPEC-068
  FR-006): `Worker.claim_stop` and `Worker.stop` ask whether the caller is the drain thread by
  comparing against `self._thread`, and they run on it when a sink's `emit` calls `shutdown()`.
  Inside the window that comparison would read false, the call would join the dead thread and the
  closer could free the sink under an emit in flight. It is safe because the window is
  unreachable: the new thread reaches `sink.emit` only through the queue, which the repair has
  just replaced empty, and nothing can fill it before the assignment, since the fork handler runs
  on the child's only thread. Nothing enforces that. The alternative was measured and is worse:
  assigning first leaves a worker whose `start()` raised reading `draining` forever and handing the
  next `shutdown()` a `RuntimeError` from `join`, out of a public call documented to raise nothing.

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
  [SPEC-044](spec-delivery/SPEC-044-lifecycle-races.md) and `docs/decisions/pipeline/` →
  "A worker guard asks one of three questions". It is not in CLAUDE.md: the fence for that
  entry names none of the three.

- **On macOS a forked child dies of `SIGSEGV` if it finalizes an inherited, unclosed
  `sqlite3.Connection`** — and the library is neither the cause nor able to fix it. The chain is
  entirely below Python: `connection_close` reaches `sqlite3_log`, which reaches Apple's `os_log`,
  which is not fork-safe, and the child faults inside `_os_log_preferences_refresh`. macOS records
  that native stack for every occurrence in `~/Library/Logs/DiagnosticReports/python3.*.ips` (read
  the `EXC_BAD_ACCESS` ones — an `EXC_CRASH` entry is the abort *after* faulthandler re-raised),
  and a script that imports nothing from this package reproduces it on both 3.12 and 3.13: build a
  `sqlite3.Connection` into a reference cycle, `fork`, and `gc.collect()` in the child.
  **It is macOS-only in practice** — CI is `ubuntu-latest`, so this never gated a build; it cost
  three local sessions instead.

  **It needs a connection that was already garbage before the fork**, which is narrower than it
  first looks. A connection reachable only from a *non-surviving thread's* frame is safe: CPython
  does not clear those frames in the child, so the reference leaks rather than becoming
  collectable (measured 0/20, against 13/20 for the same connection made garbage on the main
  thread). A sink the library was handed is safe for a second, independent reason — a forked
  child pins every entry of `_state._owned` whose sink was alive at the fork before anything else
  runs (SPEC-081; until then the record held every sink strongly for the process's life) — so a
  live `SQLiteSink` is never finalized in a child while it *runs*. One that was already cyclic
  garbage in the parent can be collected before the pin reaches it (the bounded record's residual
  8, above). ~~That held at
  the child's exit too~~ — it did not: interpreter finalization clears module globals and runs the
  cyclic collector, which finalized the inherited connection there, and a child of a process that
  had configured a `SQLiteSink` and logged died of `SIGSEGV` on a plain `sys.exit`, with no call
  into the sink at all. **Closed** by a fork handler in `sinks/sqlite.py` that leaks one reference
  to every live sink's inherited connection the moment the child exists, so it is never
  unreachable and never finalized; a `__del__` cannot do it, since the collector runs every
  finalizer in an unreachable set before it looks for resurrection. The same fix makes the child
  refuse every `emit`, because the connection's *use* faulted in `os_log` as well (§12). A
  connection the application itself leaked before the fork is still the caller's, as above.

  What the library **does** supply is the moment. `_reinit_after_fork` is the first *substantial*
  allocating work in the child — `threading._after_fork` is registered earlier and allocates a
  little — so it is usually the pass that trips the collector, and the crash therefore presents
  inside this package under a rotating set of frames. **A crash whose frame rotates is not located
  at any of those frames**, which is the trap that cost the three sessions.

  `gc.disable()` around the repair was built and measured, and is **rejected**: it greened the
  reproducing suite command outright while a child that *keeps running* still died, because the
  harness's children `os._exit` before collecting anything — and it silently re-enabled the
  collector in a child whose parent had disabled it. Consumers that fork should close their
  connections first. The suite does two things rather than one: `tests/test_sinks_sqlite.py` no
  longer leaks, and **every fork site in the suite collects in the parent before forking**, which
  is the part that generalises — it leaves the child nothing to finalize whatever leaks later, and
  is neither sqlite-specific nor version-specific. That rule is derived rather than listed, by a
  lint over the test modules, because a hand-written roster of fork sites is one that goes stale
  silently. Detecting the leak instead was built and
  rejected: the `unclosed database` `ResourceWarning` postdates 3.12, so it covered one leg of a
  two-leg CI matrix, and under `-n 12` an unraisable raised in a worker's teardown never reaches
  the session exit code. The rates behind all of this are in the commit that closed it, measured
  against its parent, rather than restated here where nobody can re-run them.
  (No spec — a defect fix.)

- **A transform output nested deeper than the C stack allows can crash the process.** Measured
  by the SPEC-058 implementation planner on macOS at `7042810`: `json.dumps` on a 20,000-deep
  nest killed the interpreter with `SIGBUS` on a thread with a 1 MiB stack under CPython 3.13.1,
  raised `RecursionError` at 2 MiB and 4 MiB, and raised `RecursionError` even at 256 KiB under
  3.12.12; Linux frame sizes are unmeasured. SPEC-058 FR-001's check adds that `json.dumps` in
  front of inner sinks that never serialized (`MemorySink`, `CallbackSink`, `LoggingSink`), but
  the same output already crashes every JSON sink's own `json.dumps`, and assembly bounds
  library-built events at `max_depth`, so only a transform can produce one. It is a limit of
  invariant 1's reach: a process-killing signal is not an exception any guard catches. (SPEC-058)

  > **Superseded in part by SPEC-069.** Every record-building sink's `json.dumps` now runs under
  > the encode guard (`sinks/_encode.py`), so where such a nest raises `RecursionError` the sink
  > drops that one event, counted, instead of losing the batch. The crash itself is unchanged: a
  > `SIGBUS` on a small thread stack is still beyond any guard. The decision lives in
  > `docs/decisions/sink-delivery/` (*An event a sink cannot encode costs only itself*).

- **A child forked inside a stop-signal setter keeps the event it was being offered.** A sink's
  `log_foundry_stop_signal` setter can start a process, for example a fork-context
  `multiprocessing.Process`, whose child leaves through `os._exit`. That child stays inside the
  setter call for its whole life. It is never handed its own signal for that sink, because handing
  it runs the setter inside itself: measured at `53b68203`, a setter holding its own lock hung such
  a child. The library records every event it is offering on the offering thread. A child shares
  the forking thread's record with all of its own threads, so none of them hands that sink again.
  That holds within the child only: a grandchild forked by another thread of the child does not
  carry the view (the third limit below). Leaving an inherited offer re-owes the child's signal
  debt, so another thread paying that debt early does not leave the sink on the parent's event. The child keeps the forking thread's events, re-initialised, and its `shutdown()` sets
  them, so a backoff on the event the sink holds is still cut short (SPEC-083). Three limits remain:
  - **A setter that forks before it stores the event** leaves the sink holding what it held before
    the call. The child's `shutdown()` does not set that event unless the fork repair replaced it,
    which it does for a sink that subclasses `Sink`. Closing this would need a per-sink record of
    earlier hands, which means a reference to every sink ever handed a signal, and that is what
    SPEC-081 bounded the ownership record to avoid.
  - **After the child's `shutdown()`, the sink keeps the event that shutdown set** for the rest of
    the child's life. Its later backoffs on that event do not wait, which is the tight-retry shape
    SPEC-033 FR-004 removed elsewhere, bounded by the sink's own retry count. A sink that subclasses
    `Sink` already behaved this way in such a child at `53b68203`. Handing it a fresh signal means
    running its setter inside the call that forked.
  - **A grandchild forked by another thread of a child that is still inside a lock-holding setter
    hangs on its first log.** `_clear_after_fork` replaces the shared record with the new forking
    thread's, so the child's view does not carry into that grandchild. It is not closed, because
    the control hangs too: with no fork inside a hand at all, one thread inside a locked setter and
    another thread forking also hangs the child. That is the generic hazard of forking while
    another thread holds a lock the caller's code owns, which no record can close.

  The decision lives in `docs/decisions/sink-lifecycle/` (*A child forked inside a stop-signal
  setter keeps its mark and the event it was offered, and its shutdown sets that event*).
  (SPEC-083)

---

## 14. Alignment with observability concepts

| Concept (design brief) | How log-foundry addresses it |
|------------------------|----------------------------|
| Structured vs unstructured | JSON with named fields, always (§6). |
| Correlation ID journey | Two-tier IDs + parent/child hierarchy across nested calls (§3), continued across processes via W3C `traceparent` + `baggage` (SPEC-014). |
| Head vs tail sampling | Neither is built. Span-outcome sampling has a natural call site; true tail sampling needs a trace-scoped buffer we don't have (§10). |
| Cardinality explosion | High-cardinality fields allowed in logs; never auto-promoted to metric labels (§6). |
| One event, three views | Logs first; `trace_id`/`span_id` make traces derivable, and OTLP traces from `span.end` are a planned arc item, not built (§13; superseded 2026-10-05 — `docs/decisions/public-api/`). |
| Symptom vs cause alerting | We stamp `duration_ms` / `status` / `error.type` — the fields alerts key off (§6). |
