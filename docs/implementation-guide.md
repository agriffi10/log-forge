# log-forge — Implementation Guide

A step-by-step path from the empty package to a working library, built to match
[`architecture.md`](architecture.md). Section references like *(arch §5)* point back to it.

**How to use this guide:** build in the phase order below. Each phase is a *vertical
slice* — it ends with something you can run and verify before moving on. The code blocks
are **skeletons and the tricky parts in full**; the straightforward method bodies are
left for you. Each phase says *what* it accomplishes and *why* it exists.

> **Naming note:** the installable package is `log_forge` (underscore). The architecture
> doc writes `logforge` as friendly shorthand — same thing. This guide uses the real
> `log_forge` import. If you'd rather type `logforge`, rename the distribution later;
> don't let it block you now.

---

## Module map

Build the package toward this layout. Each module owns one architecture concept, and the
arrows show who imports whom (dependencies point *downward* — no cycles):

```
src/log_forge/
├── __init__.py        Public façade: configure, trace, debug/info/…, set_baggage, shutdown
├── config.py          Global config (service/version/env/sink/defaults)        (arch §7)
├── ids.py             W3C-compatible id generation                             (arch §3.1)
├── model.py           Span + log-event construction & JSON serialization       (arch §6)
├── context.py         contextvars: span stack + baggage                        (arch §5)
├── console.py         ConsoleWriter — synchronous echo                         (arch §6.1)
├── api.py             debug/info/warning/error/critical + set_baggage          (arch §6.1)
├── decorator.py       @trace (sync + async)                                    (arch §4)
├── worker.py          Background flush worker + graceful shutdown              (arch §9)
└── sinks/
    ├── base.py        Sink protocol                                            (arch §8)
    ├── stdout.py      StdoutSink (dev default)                                 (arch §8)
    └── sqs.py         SQSSink (headline path to ELK)                           (arch §8, §9.1)

Dependency direction:  ids, config  ◄─ model ◄─ context ◄─ api/decorator ◄─ __init__
                                         sinks ◄─ worker ◄─ decorator
```

Your current `core.py` (`LogForge`) and `modules/v1/log.py` (`Log`) are scaffolding from
the setup phase. `LogForge` can become the internal singleton that the façade functions
delegate to (or you can drop it for a pure module-level API — your call). `Log` is
superseded by `model.py`.

---

## Phase 0 — Decide the public shape first

**Goal:** write the usage you *want*, before implementing anything. This is your contract;
every phase below makes a piece of it real.

```python
import log_forge
from log_forge.sinks.stdout import StdoutSink

log_forge.configure(service="payments", version="2.14", env="prod", sink=StdoutSink())

@log_forge.trace
def process_payment(user_id: int) -> str:
    log_forge.set_baggage(request_id="req-123")     # rides every log below (arch §5.1)
    log_forge.info("charging card", user_id=user_id)
    write_ledger(user_id)
    log_forge.info("payment complete", echo=True)   # also printed to console now
    return "ok"

@log_forge.trace
def write_ledger(user_id: int) -> None:
    log_forge.debug("inserting row", user_id=user_id)

process_payment(4127)
log_forge.shutdown()   # flush before exit
```

**Why first:** locking the call-site shape keeps every internal decision honest. If a
later phase makes this example awkward to write, the design drifted — stop and fix it.

---

## Phase 1 — Config (arch §7)

**Goal:** a single place to hold process-wide settings, set once at startup.

**Why:** every log event needs `service`/`version`/`env` stamped on it, and the decorator
and worker both need to find the configured sink. A global config is the simplest thing
that lets the rest of the code stay decoupled from *how* it was configured.

```python
# config.py
from dataclasses import dataclass, field

@dataclass
class Config:
    service: str = "unknown"
    version: str = "0.0.0"
    env: str = "dev"
    sink: "Sink | None" = None              # set in configure(); default applied lazily
    defaults: dict = field(default_factory=dict)

_config = Config()                          # module-level singleton

def configure(**kwargs) -> None:
    """Replace/patch global config. Call once at startup."""
    global _config
    # TODO: update _config fields from kwargs; default the sink to StdoutSink() if None
    ...

def get_config() -> Config:
    return _config
```

**Watch out:** don't import `sinks` at the top of `config.py` (that would create a cycle —
see the dependency arrows). Default the sink *inside* `configure()` with a local import.

---

## Phase 2 — IDs (arch §3.1)

**Goal:** generate W3C Trace Context-compatible identifiers.

**Why:** using the standard wire formats now (instead of UUIDs) is nearly free and makes
the deferred cross-service work a simple header parse later (arch §12).

```python
# ids.py
import os
import uuid

def new_trace_id() -> str:
    return os.urandom(16).hex()   # 32 lowercase hex chars (128-bit)

def new_span_id() -> str:
    return os.urandom(8).hex()    # 16 lowercase hex chars (64-bit)

def new_log_id() -> str:
    return uuid.uuid4().hex       # internal-only, format is our choice
```

**Watch out:** the W3C spec says the all-zero id is invalid. With `os.urandom` the odds are
astronomically low, so don't over-engineer it — just don't substitute a non-random source.

---

## Phase 3 — Data model (arch §6)

**Goal:** represent a span and turn its log events into the exact JSON schema from arch §6.

**Why:** this is the heart of "structured, never free-form." Centralizing serialization
here means every event has identical shape — the property that makes logs queryable in ELK.

```python
# model.py
import time
from dataclasses import dataclass, field

@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str                              # the function name (arch §6 auto-capture)
    start_ts: float
    defaults: dict = field(default_factory=dict)   # per-decorator overrides
    events: list[dict] = field(default_factory=list)

def build_event(span: Span, level: str, message: str, *,
                fields: dict, baggage: dict) -> dict:
    """Assemble one log record in the arch §6 schema.

    Precedence (low → high): config defaults → span.defaults → baggage → fields  (arch §5.1)
    """
    from .config import get_config
    from .ids import new_log_id
    cfg = get_config()
    merged = {**cfg.defaults, **span.defaults, **baggage, **fields}
    return {
        "timestamp": _iso_now(),
        "level": level,
        "message": message,
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "log_id": new_log_id(),
        "function": span.name,
        "service": cfg.service,
        "version": cfg.version,
        "env": cfg.env,
        "fields": merged,
    }

def _iso_now() -> str:
    # TODO: UTC ISO-8601 with milliseconds + 'Z'  (e.g. 2024-01-15T14:23:01.842Z)
    ...
```

Add two helpers you'll call from the decorator:

- `start_event(span)` → a `build_event(..., level="INFO", message="span.start", ...)`.
- `end_event(span, status, exc=None)` → builds the end record and adds `duration_ms`,
  `status`, and on error `error.type` + `error.stack` (arch §6). Use
  `time.monotonic()` for duration, not wall-clock, so clock changes can't produce
  negative durations.

**Watch out:** keep `model.py` free of context/decorator imports. It only *builds* records;
it doesn't know where the "current" span lives. That's the next phase.

---

## Phase 4 — Context: span stack + baggage (arch §5)

**Goal:** track the active span and baggage per execution flow, with zero manual passing,
working under both threads and `asyncio`.

**Why:** `log_forge.info(...)` has to find "the span I'm inside" on its own. `contextvars`
is the one mechanism that does this correctly across threads *and* async tasks (each task
inherits a copy). This is the linchpin of the whole ergonomic API.

```python
# context.py
import contextvars
from .model import Span

_span_stack: contextvars.ContextVar[tuple[Span, ...]] = \
    contextvars.ContextVar("log_forge_span_stack", default=())
_baggage: contextvars.ContextVar[dict] = \
    contextvars.ContextVar("log_forge_baggage", default={})

def current_span() -> Span | None:
    stack = _span_stack.get()
    return stack[-1] if stack else None

def push_span(span: Span):
    """Push and return a token; pass it back to pop_span in a finally block."""
    return _span_stack.set(_span_stack.get() + (span,))

def pop_span(token) -> None:
    _span_stack.reset(token)               # token-based reset is async/thread-safe

def get_baggage() -> dict:
    return _baggage.get()

def set_baggage(**kv) -> None:
    _baggage.set({**_baggage.get(), **kv})  # new dict — never mutate in place
```

**Watch out — two real footguns:**

1. **Never mutate the default mutable value of a `ContextVar`.** The `default=()` /
   `default={}` object is shared across all contexts. Always `.set(new_value)` with a
   *new* tuple/dict (as above). Mutating in place corrupts other flows.
2. **Use the token/`reset` pattern, not a manual pop.** `reset(token)` restores the exact
   prior state even when tasks branch — a hand-rolled `pop()` will desync under async.

---

## Phase 5 — Sink protocol + StdoutSink (arch §8)

**Goal:** define the output interface and one implementation you can see immediately.

**Why:** coding against a `Sink` protocol (not against SQS) is what lets you build and test
the whole pipeline locally with `StdoutSink`, then swap in SQS with zero changes elsewhere.

```python
# sinks/base.py
from typing import Protocol

class Sink(Protocol):
    def emit(self, batch: list[dict]) -> None: ...   # ship a batch of event dicts
    def close(self) -> None: ...                      # flush + release resources
```

```python
# sinks/stdout.py
import json
import sys

class StdoutSink:
    def __init__(self, stream=sys.stdout):
        self._stream = stream

    def emit(self, batch: list[dict]) -> None:
        for event in batch:
            self._stream.write(json.dumps(event) + "\n")
        self._stream.flush()

    def close(self) -> None:
        self._stream.flush()
```

**Watch out:** the sink receives *already-built* event dicts and knows nothing about spans
or context (arch §8). Keep it that dumb — it's what makes sinks trivially swappable.

---

## Phase 6 — The `@trace` decorator, synchronous first (arch §4)

**Goal:** open a span on enter, close it on exit (success *or* exception), and — for now —
flush it straight to the sink. This is your **first end-to-end runnable slice.**

**Why synchronous first:** get the span lifecycle, nesting, and exception handling correct
against a simple direct flush. The background worker (Phase 9) then only changes *where the
finished span goes*, not the lifecycle logic.

```python
# decorator.py
import functools
import time
from . import context
from .config import get_config
from .ids import new_trace_id, new_span_id
from .model import Span, start_event, end_event

def _open_span(name: str, defaults: dict | None) -> Span:
    parent = context.current_span()
    trace_id = parent.trace_id if parent else new_trace_id()      # inherit or start (arch §3)
    span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        parent_span_id=parent.span_id if parent else None,        # hierarchy (arch §3)
        name=name,
        start_ts=time.monotonic(),
        defaults=defaults or {},
    )
    span.events.append(start_event(span))
    return span

def _close_span(span: Span, status: str, exc: BaseException | None) -> None:
    span.events.append(end_event(span, status=status, exc=exc))
    _flush(span)

def _flush(span: Span) -> None:
    get_config().sink.emit(span.events)        # Phase 9 replaces this with worker.submit()

def trace(func=None, *, name=None, defaults=None):
    """Usable as @trace or @trace(name=..., defaults=...)."""
    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            span = _open_span(name or fn.__qualname__, defaults)
            token = context.push_span(span)
            try:
                result = fn(*args, **kwargs)
                _close_span(span, "ok", None)
                return result
            except BaseException as exc:
                _close_span(span, "error", exc)
                raise                          # never swallow (arch §4)
            finally:
                context.pop_span(token)
        return wrapper
    return decorate(func) if func else decorate
```

**Checkpoint:** wire up `__init__.py` minimally (`configure`, `trace`, and a temporary
`info`) and run the Phase 0 example. You should see JSON span-start/end lines on stdout,
with a shared `trace_id` and `write_ledger`'s `parent_span_id` pointing at
`process_payment`'s `span_id`. **That's the architecture working end to end.**

**Watch out:** catch `BaseException`, not `Exception`, so `KeyboardInterrupt`/timeouts are
still recorded — but always re-`raise`. And record the end event *before* `_flush`, so the
flushed queue is complete.

---

## Phase 7 — Logging API + console echo (arch §6.1)

**Goal:** the `debug/info/warning/error/critical` methods, plus `echo=` for immediate
console output.

**Why:** this is the "add a log to the span" capability (path 1) and the "show it to a
human/Lambda now" capability (path 2) from arch §6.1 — the two ways user code emits.

```python
# console.py
import sys

class ConsoleWriter:
    def __init__(self, stream=sys.stderr):
        self._stream = stream

    def write(self, event: dict) -> None:
        # human-readable, NOT json (arch §6.1)
        self._stream.write(f'{event["level"]:<7} {event["message"]}\n')
        self._stream.flush()
```

```python
# api.py
from . import context
from .model import build_event
from .console import ConsoleWriter

_console = ConsoleWriter()

def _log(level: str, message: str, *, echo: bool = False, **fields) -> None:
    span = context.current_span()
    if span is None:
        # orphan log (arch §5 / §12 open item): emit standalone so nothing is lost
        # TODO: build a one-off span+event with a fresh trace_id and flush it
        ...
        return
    event = build_event(span, level, message,
                        fields=fields, baggage=context.get_baggage())
    span.events.append(event)              # path 1: rides the async pipeline → sink
    if echo:
        _console.write(event)              # path 2: synchronous, immediate

def info(message, *, echo=False, **fields):    _log("INFO", message, echo=echo, **fields)
def debug(message, *, echo=False, **fields):   _log("DEBUG", message, echo=echo, **fields)
def warning(message, *, echo=False, **fields): _log("WARNING", message, echo=echo, **fields)
def error(message, *, echo=False, **fields):   _log("ERROR", message, echo=echo, **fields)
def critical(message, *, echo=False, **fields):_log("CRITICAL", message, echo=echo, **fields)

# set_baggage just re-exports context.set_baggage
```

**Watch out:** echo is **additive** (arch §6.1) — the event still goes into the span
queue *and* gets printed. Don't make `echo=True` a redirect.

---

## Phase 8 — Async support in the decorator (arch §5)

**Goal:** make `@trace` work on `async def` functions too.

**Why:** the concurrency decision was threads *and* asyncio. `contextvars` already handles
context correctly for tasks; you just need an async-aware wrapper so the span closes when
the coroutine actually finishes (not when it's created).

```python
# decorator.py  (add to the existing trace())
import asyncio

def decorate(fn):
    if asyncio.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def awrapper(*args, **kwargs):
            span = _open_span(name or fn.__qualname__, defaults)
            token = context.push_span(span)
            try:
                result = await fn(*args, **kwargs)
                _close_span(span, "ok", None)
                return result
            except BaseException as exc:
                _close_span(span, "error", exc)
                raise
            finally:
                context.pop_span(token)
        return awrapper
    # ... else the sync wrapper from Phase 6
```

**Watch out:** the *only* difference is `async`/`await`. Resist refactoring the two wrappers
into one clever helper — the duplication is clearer than the abstraction here, and the
sync/async split is a hard boundary in Python.

---

## Phase 9 — Background worker: make flush non-blocking (arch §9)

**Goal:** stop flushing inline. Hand finished spans to a background thread that batches and
emits, so decorated functions return immediately.

**Why:** this is the arch §9 decision — application code must never block on sink I/O, and
the sink is a *buffer* (arch §9.1). This phase changes only `_flush()`; the lifecycle from
Phase 6 is untouched, which is exactly why we did it synchronously first.

```python
# worker.py
import atexit
import queue
import threading

class Worker:
    def __init__(self, sink, *, batch_size=10, flush_interval=1.0, max_queue=10_000):
        self._sink = sink
        self._q: queue.Queue = queue.Queue(maxsize=max_queue)
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._dropped = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        atexit.register(self.shutdown)

    def submit(self, events: list[dict]) -> None:
        try:
            self._q.put_nowait(events)
        except queue.Full:
            self._dropped += 1            # drop-newest + count (arch §9 backpressure)

    def _run(self) -> None:
        # TODO: loop until stopped:
        #   - drain up to batch_size event-lists (or wait flush_interval)
        #   - flatten into one batch, respecting the sink's size limits (arch §9.1)
        #   - call self._sink.emit(batch); catch + retry/backoff on failure
        ...

    def shutdown(self) -> None:
        # TODO: signal stop, drain remaining queue, emit final batch(es),
        #       then self._sink.close()   (arch §9 graceful shutdown)
        ...
```

Now rewire the decorator's flush:

```python
# decorator.py
def _flush(span):
    _get_worker().submit(span.events)   # was: get_config().sink.emit(...)
```

Create the worker lazily from config (one per process) and expose `log_forge.shutdown()`
that calls `worker.shutdown()`.

**Watch out:** the worker thread is a `daemon` so it can't hang interpreter exit, but daemon
threads are killed *without* draining — that's why the explicit `atexit`/`shutdown()` drain
matters. Test that a program which exits right after a log still flushes.

---

## Phase 10 — SQSSink (arch §8, §9.1)

**Goal:** the real headline sink — batches events to an SQS queue.

**Why:** SQS is the durable buffer that decouples your app from ELK availability (arch
§9.1). The worker already batches; the sink just has to respect SQS's limits.

```python
# sinks/sqs.py
import json

class SQSSink:
    MAX_BATCH = 10            # SQS SendMessageBatch hard limit
    MAX_BYTES = 256 * 1024    # 256 KB per batch (arch §9.1)

    def __init__(self, queue_url: str, client=None):
        import boto3                       # local import: keep boto3 optional
        self._client = client or boto3.client("sqs")
        self._queue_url = queue_url

    def emit(self, batch: list[dict]) -> None:
        # TODO: chunk `batch` into ≤10 messages AND ≤256 KB, json.dumps each event,
        #       call send_message_batch; inspect the response's Failed list and
        #       retry/log those entries.
        ...

    def close(self) -> None:
        pass                               # nothing buffered inside the sink itself
```

Add `boto3` as an **optional** dependency so stdout-only users stay dependency-free. In
`pyproject.toml`:

```toml
[project.optional-dependencies]
sqs = ["boto3>=1.34"]
```

…installed with `pip install log-forge[sqs]` / `poetry install --extras sqs`.

**Watch out:** the worker batches by *count*, but SQS also caps by *bytes*. The sink must
re-chunk on size, not assume the worker's batch already fits. One oversized event should be
logged and dropped, not crash the whole batch.

---

## Phase 11 — Seams already in place (arch §10, §3.2)

You don't build these now, but the code above is shaped so they drop in cleanly:

- **Sampling (arch §10):** add `should_send(span_summary) -> bool` and call it in
  `_close_span` before `_flush`. Default `True` = "send everything" (the current decision).
  Because the span is complete at that point, a future tail-sampling policy ("keep errors +
  slow calls") has `status` and `duration_ms` available with no pipeline change.
- **Follows-from relationships (arch §3.2):** `_open_span` could accept a `link=` for
  causal-but-non-blocking work; it would keep the parent's `trace_id` but record the link
  differently. The ID model already supports it.

---

## Suggested commit sequence

Each phase is a clean commit and a natural test boundary:

1. config + ids + model (pure functions — unit-test in isolation)
2. context (test push/pop nesting and baggage merge)
3. sink protocol + StdoutSink
4. `@trace` sync + minimal façade → **first runnable demo**
5. logging API + echo
6. async `@trace`
7. background worker + shutdown
8. SQSSink + optional extra

---

## Testing tip

Inject a fake sink and assert on the dicts — no SQS, no stdout parsing:

```python
class FakeSink:
    def __init__(self): self.batches = []
    def emit(self, batch): self.batches.append(batch)
    def close(self): pass

def test_nested_spans_share_trace_and_link_parent():
    sink = FakeSink()
    log_forge.configure(service="t", sink=sink)
    # For deterministic tests, flush synchronously (skip the worker) or call
    # log_forge.shutdown() to drain before asserting.
    ...
    events = [e for batch in sink.batches for e in batch]
    trace_ids = {e["trace_id"] for e in events}
    assert len(trace_ids) == 1                      # one trace across nested calls
    # assert the child's parent_span_id == the parent's span_id
```

**Why a fake sink:** it tests the part you wrote (span lifecycle, IDs, schema, context)
without the part you didn't (the network). For worker tests, prefer draining via
`shutdown()` over sleeping, so tests stay fast and deterministic.
