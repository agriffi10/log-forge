---
type: Decision
title: "`flush()` reports delivery, and answers from the drain that carried the events"
description: "a marker that finds nothing pending inherits the outcome of the emit ahead of it in the FIFO — otherwise a second concurrent flush reports success for events the first one just abandoned. **Narrowed: a `shutdown()` that expires answers the outstanding markers pessimistically, so `abandoned` no longer implies the drain adjudicated — `ok=True` is untouched.** (SPEC-021, SPEC-036, SPEC-050)"
---

**`flush()` reports delivery, and answers from the drain that carried the events** — the marker brings the emit's *outcome* back, so `True` means the sink took them, not merely that a drain ran. A marker that finds nothing pending inherits the outcome of the emit that carried what was ahead of it in the FIFO — otherwise a second concurrent flush reports success for events the first one just abandoned. It is not a verdict on every batch ever sent; `health().failed_batches` is the cumulative record. (SPEC-021) **What it reports *over* is three places, not one** (SPEC-036): the queue, the buffers on spans still **open** in the calling context, and the sink's own **client** buffer. Each was a place `flush()` returned success while events sat undelivered — the in-span case zero of two events on the README's own recipe. A span is swept and left open, its boundary events backfilled first (so they carry the baggage as of the flush, not the close) and its buffer detached by swap, never cleared; a swept span then makes `continue_trace()` refuse, or one span carries two trace ids. The bound is the calling context, because `contextvars` cannot enumerate another thread's. A sink that buffers in a driver implements the optional `flush()`, probed by `flush_sink` — which **propagates** where `read_losses` swallows, since a swallowed flush failure is a sink the worker believes. **Narrowed 2026-09-02 (SPEC-050 FR-001):** a `shutdown()` that *expires* answers the outstanding markers pessimistically rather than leaving them to a drain it has just given up on — including one the drain thread is holding, one taken after that sweep, which answers itself, one whose `flush()` enqueued it after the sweep had run, and one that never reached a full queue at all — the last two caught by `flush` itself, before and after its put, consulting the same flag. **Four** producers of `abandoned`, each found by running a probe rather than reading the fix, and three of them by someone re-running the previous fix's own probe. So a `False`/`abandoned` no longer implies the drain adjudicated it. The converse is untouched and is what the entry is for: `delivered` starts `False` and is written only by the owning drain, so no added path can produce `ok=True` over lost events. The alternative was a `flush(timeout=None)` — documented as supported — waiting forever, measured still alive after `shutdown()` returned.

## History

### 2026-09-02

- **Narrowed** by SPEC-050 FR-001: a `shutdown()` that expires answers the outstanding markers pessimistically.

### 2026-07-30

- **Settled.**
