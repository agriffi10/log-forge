# Spec: Background Flush Worker and Graceful Shutdown

**ID:** SPEC-004
**Status:** Completed
**Last Updated:** 2026-07-09
**Depends On:** SPEC-001

## Overview

Until now a finished span flushes inline — the decorated function blocks on `sink.emit`. This
spec makes flushing non-blocking (architecture §9): finished spans are handed to a background
worker thread that batches events and emits them, so decorated functions return immediately and
application code never blocks on sink I/O. The worker batches by size and time, retries with
backoff on emit failure, and applies a backpressure policy (drop-newest with a counted warning)
when its queue is full so a slow or down sink can never back-pressure the app. A graceful
`atexit` hook and an explicit `log_foundry.shutdown()` drain the queue and `close()` the sink so
buffered events survive process exit. This phase changes only *where a finished span goes* —
the span lifecycle from SPEC-001/003 is untouched, which is exactly why it was built
synchronously first.

## Scope

### In Scope

- A `Worker` owning a bounded in-memory queue and a daemon thread that drains it.
- `submit(events)` — a fast, non-blocking, in-process handoff from the decorator.
- Batching finished-span event lists by count (`batch_size`) and time (`flush_interval`) into a
  single `sink.emit(batch)` call.
- Retry with backoff on `emit` failure.
- Backpressure: when the queue is full, drop-newest and increment a counter (the app never
  blocks).
- Graceful shutdown: drain remaining queue, emit final batches, then `sink.close()`, wired to
  `atexit` and exposed as `log_foundry.shutdown()`.
- Rewiring the decorator's `_flush(span)` to `worker.submit(span.events)`; the worker is
  created lazily (one per process) from the configured sink.

### Out of Scope

- `SQSSink` and byte-size-aware re-chunking of a batch — SPEC-005 (the worker batches by count
  and time here; sink-specific byte limits are the sink's concern).
- Making the drop-vs-block backpressure policy configurable — the v1 default is drop-newest;
  exposing a block/backpressure mode is deferred.
- Sampling / `should_send` — deferred (architecture §10); the seam is called out but not built.
- Multiple concurrent workers or per-sink worker pools — one worker per process.

---

## Functional Requirements

### FR-001: Non-blocking submit

#### Description:

The decorator hands a finished span's events to the worker without blocking on sink I/O.

#### Acceptance Criteria:

- [ ] `Worker.submit(events)` enqueues the event list via `queue.Queue.put_nowait` and returns
      immediately without calling the sink.
- [ ] A decorated function whose sink `emit` blocks/sleeps still returns promptly (the emit runs
      on the worker thread, not the caller's).
- [ ] The decorator's `_flush(span)` calls `worker.submit(span.events)` instead of
      `sink.emit(...)` directly.

### FR-002: Batching

#### Description:

The worker thread drains its queue and coalesces events into batches by count and time window.

#### Acceptance Criteria:

- [ ] The worker emits a batch when it has accumulated `batch_size` event-lists **or**
      `flush_interval` seconds have elapsed since the last emit, whichever comes first.
- [ ] Each `sink.emit` call receives a flat `list[dict]` (the queued per-span event lists
      flattened into one batch).
- [ ] With a fake sink, N submitted spans result in exactly the emitted events, in order, with
      no loss and no duplication under normal operation.

### FR-003: Retry with backoff

#### Description:

A failing `sink.emit` is retried rather than crashing the worker thread.

#### Acceptance Criteria:

- [ ] When `sink.emit(batch)` raises, the worker retries the batch with backoff (bounded number
      of attempts) rather than dropping it on the first error.
- [ ] The worker thread survives an `emit` exception and continues processing subsequent
      batches.
- [ ] Repeated failure past the retry bound does not deadlock the worker or the app (the batch
      is abandoned with a counted warning, and draining continues).

### FR-004: Backpressure (drop-newest + count)

#### Description:

When the queue is full (sink down/slow), the app must not block; excess is dropped and counted.

#### Acceptance Criteria:

- [ ] The worker queue is bounded (`max_queue`).
- [ ] When `submit` finds the queue full, it drops the newest submission and increments a
      `dropped` counter instead of blocking.
- [ ] The `dropped` count is observable (attribute/metric) so drops are not silent.

### FR-005: Graceful shutdown

#### Description:

On explicit shutdown or interpreter exit, buffered events are flushed and the sink is closed.

#### Acceptance Criteria:

- [ ] `Worker.shutdown()` signals the thread to stop, drains all remaining queued event-lists,
      emits them, then calls `sink.close()`.
- [ ] `shutdown()` is registered via `atexit` and exposed as `log_foundry.shutdown()`.
- [ ] A program that logs and then exits immediately still flushes those events (the explicit
      drain runs even though the worker thread is a daemon).
- [ ] `shutdown()` is idempotent — a second call after shutdown does not raise or double-close.

### FR-006: Lazy per-process worker

#### Description:

Exactly one worker is created per process, from the configured sink, on first use.

#### Acceptance Criteria:

- [ ] The worker is created lazily on the first `_flush` (or first `submit`) using
      `get_config().sink`.
- [ ] Subsequent flushes reuse the same worker instance (one worker per process).

---

## Data Model

```
# src/log_foundry/worker.py
Worker {
  sink: Sink
  queue: queue.Queue          # bounded by max_queue; holds per-span list[dict]
  batch_size: int = 10
  flush_interval: float = 1.0
  max_queue: int = 10_000
  dropped: int = 0
  thread: threading.Thread    # daemon
}
```

---

## API / Interface Contract

```python
# worker.py
class Worker:
    def __init__(self, sink: Sink, *, batch_size=10, flush_interval=1.0, max_queue=10_000): ...
    def submit(self, events: list[dict]) -> None    # non-blocking; drop-newest when full
    def shutdown(self) -> None                       # drain + sink.close(); idempotent

# decorator.py
def _flush(span: Span) -> None:
    _get_worker().submit(span.events)                # was: get_config().sink.emit(span.events)

# __init__.py
def shutdown() -> None                               # calls the process worker's shutdown()

# Example
process_payment(4127)
log_foundry.shutdown()     # flush buffered events before exit
```

## Configuration / Environment

No new environment variables or dependencies. Worker tunables (`batch_size`, `flush_interval`,
`max_queue`) are constructor defaults for this spec; surfacing them through
`log_foundry.configure(...)` is not required here.

## File & Folder Structure

```
src/log_foundry/
├── worker.py          # Worker: queue, daemon thread, batching, retry, shutdown   (new)
├── decorator.py       # _flush rewired to worker.submit; lazy _get_worker()
└── __init__.py        # + shutdown
tests/
└── test_worker.py     # submit non-blocking, batching, retry, backpressure, drain (new)
```

## Implementation Phases

### Phase 1: Worker core (submit + batching + drain loop)

- Implement `Worker` with the bounded queue, daemon thread, and the `_run` drain/batch loop
  (FR-001, FR-002).
- Implement `shutdown()` drain + `sink.close()`, idempotent, registered via `atexit`
  (FR-005).
- Test batching, non-blocking submit, and drain-before-exit with a fake (optionally blocking)
  sink; prefer draining via `shutdown()` over sleeping.

### Phase 2: Resilience + rewire

- Add retry-with-backoff around `sink.emit` and the drop-newest+count backpressure path
  (FR-003, FR-004).
- Add lazy `_get_worker()` and rewire the decorator's `_flush` to `worker.submit`; expose
  `log_foundry.shutdown()` (FR-006).
- Test retry survival, `dropped` counting under a full queue, and end-to-end decorate→submit→
  drain.
