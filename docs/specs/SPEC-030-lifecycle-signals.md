# Spec: Lifecycle Signals — Post-Shutdown Logging and Late Reconfiguration

**ID:** SPEC-030  
**Status:** Draft  
**Last Updated:** 2026-08-05  
**Depends On:** SPEC-013, SPEC-019

## Overview

Two ways of using this library produce total, permanent, silent log loss. Both are user error. Both
are documented as user error. Neither produces any signal at all.

**Logging after `shutdown()`.** `shutdown()` is terminal by design (SPEC-013), and `__init__.py`
warns that calling it per-invocation in a serverless handler means "the first invocation on a warm
container would log and every later one would silently log nothing". But `decorator._worker` is
never reset, so later calls keep submitting to the retired worker: events land in a queue nothing
drains. Measured after `shutdown()`, three traced calls delivered nothing and `health()` returned
`queued=3, dropped=0, failed_batches=0, stopped_reason=None`. The documented alert idiom —
`if h.dropped or h.failed_batches or h.stopped_reason` — cannot fire, because `stopped_reason` is
`None` for a *clean* shutdown by deliberate design (SPEC-019 chose that so a never-created worker
would not read as failed).

**Reconfiguring the sink after the first log.** `_get_worker()` captures the sink once
(`decorator.py:209`). A later `configure(sink=...)` updates `_config.sink` — so `get_config().sink`
reports the new sink — while every event continues to the old one. Measured: sink A received 4
events, sink B received 0, and the config claimed B. Nothing is documented, and `configure`'s own
docstring invites the mistake by promising that "repeated calls compose rather than reset".

The library already treats invisible loss as a defect in its own right; that is the whole SPEC-017
arc, and SPEC-019's reasoning applies verbatim here — the reading eventually changes, but into the
wrong signal or none at all. This spec makes both states detectable.

## Scope

### In Scope

- A distinguishable `health()` reading for a retired worker still receiving submissions.
- A one-time stderr warning when logging continues after `shutdown()`.
- Making a late `configure(sink=...)` either take effect or say why it cannot.
- Documenting both lifecycles where the mistake is made.

### Out of Scope

- **Making `shutdown()` non-terminal, or auto-restarting the worker.** SPEC-013 settled the two
  drains as deliberately distinct, and SPEC-019 settled that a dead worker is reported rather than
  resurrected — "a thread that resurrects itself fights a process trying to exit". `flush()` remains
  the answer for a process that will log again.
- **A liveness check in `submit()`.** SPEC-019 excluded this explicitly: a per-submission check is a
  hot-path change on the caller's thread, and SPEC-017 already shipped one regression of that exact
  shape. Detection stays in `health()` and in a throttled warning, not in the fast path.
- **Making `configure()` thread-safe or callable at arbitrary times.** It remains a startup call.
  This spec addresses the silence, not the concurrency (which SPEC-028 covers for sinks).
- **Reconfiguring anything other than the sink after first log.** `service`, `version`, `env`,
  `defaults` and the ceilings are read per event through `get_config()` and already take effect
  immediately; only `sink` is captured. Stated so an implementer does not rebuild the worker for a
  `defaults` change.

---

## Functional Requirements

### FR-001: A retired worker still receiving submissions is visible in `health()`

#### Description:

`health()` must distinguish "cleanly shut down and idle" from "cleanly shut down and still being
handed events that will never be delivered". The first is correct usage; the second is total silent
loss.

`stopped_reason` keeps its SPEC-019 meaning — `None` after a clean shutdown — and the new state is
reported by a separate field, because conflating them would make a correct shutdown read as a
failure in every process that shuts down properly.

#### Acceptance Criteria:

- [ ] After `shutdown()` with no further logging, `health()` reports the retired state and a
      submitted-after-shutdown count of zero.
- [ ] After `shutdown()` followed by N decorated calls, the count reflects the submissions that were
      accepted and cannot be delivered.
- [ ] `stopped_reason` remains `None` after a clean shutdown (SPEC-019 FR-003 unchanged).
- [ ] A worker that was never shut down reports the count as zero and the retired state as false.
- [ ] A process that never logged still gets the zeroed snapshot with no worker created.
- [ ] The existing fields keep their positions and meanings; the new ones are appended.
- [ ] The README's alert idiom covers the new state, with one line on the remedy (use `flush()`, not
      `shutdown()`, in a process that will log again).

### FR-002: The first post-shutdown submission warns

#### Description:

One stderr line, on the first submission after `shutdown()`, so the mistake is visible to an
operator who is reading logs rather than polling `health()`. Throttled thereafter on the same
principle as the queue-overflow warning — a handler making this mistake makes it on every
invocation, and a line per event would be its own outage.

The check must not cost the hot path anything in the normal case: it is a single already-loaded
boolean read, on a flag that is only ever set once.

#### Acceptance Criteria:

- [ ] The first submission after `shutdown()` writes exactly one line, naming what happened and what
      to use instead.
- [ ] Subsequent submissions are throttled, following `_DROP_WARN_EVERY`'s existing convention.
- [ ] A process that shuts down and logs nothing further writes no line.
- [ ] The write is guarded and never reaches the caller (SPEC-029 FR-003).
- [ ] Normal submission is not measurably slowed.

### FR-003: A late `configure(sink=...)` takes effect or reports that it cannot

#### Description:

The current behaviour — silently continuing to the old sink while the config reports the new one —
is not defensible in either direction. Two options were considered and one chosen.

**Chosen:** `configure(sink=...)` after a worker exists **rebuilds the worker's delivery target**,
draining the old sink and closing it first, so the call means what it says. Swapping a sink is a
legitimate operation (a test replacing `StdoutSink` with a `MemorySink`, an application that
resolves its destination after reading remote config), and the failure today is that it *appears*
to work.

**Rejected:** raising on a late `sink=`. It would break the many callers who call `configure()` once
at startup but after an import-time log line, turning a silent wrong-sink into a hard crash for a
case that is usually benign.

The drain is bounded and its outcome reported, so a hung old sink cannot make `configure()` hang.

#### Acceptance Criteria:

- [ ] `configure(sink=B)` after events have gone to sink A causes subsequent events to reach B.
- [ ] Events submitted before the call are drained to A before the swap, not lost and not sent to B.
- [ ] Sink A is closed exactly once; sink B is not closed.
- [ ] The swap is bounded in time; if the drain does not complete, `configure()` still returns and
      the failure is recorded in `health()` and on stderr.
- [ ] `configure(sink=A)` where A is already the active sink is a no-op — no drain, no close.
- [ ] `configure()` with no `sink=` argument never rebuilds anything, whatever else it changes.
- [ ] Calling `configure(sink=...)` before any logging behaves exactly as today (no worker exists;
      nothing to drain).
- [ ] `configure(sink=...)` after `shutdown()` does not resurrect the worker; it updates the config
      and the FR-001/FR-002 signals continue to apply.

### FR-004: Both lifecycles are documented where the mistake is made

#### Description:

The docstrings that invite each mistake are corrected at the point of invitation, not only in a
README section a reader arrives at afterwards.

#### Acceptance Criteria:

- [ ] `configure()`'s docstring states what happens to a late `sink=` — that it swaps the live
      target, drains and closes the previous sink, and is bounded — qualifying "repeated calls
      compose rather than reset".
- [ ] `shutdown()`'s docstring states that later logging is accepted, undeliverable, and reported
      through the FR-001 field.
- [ ] `health()`'s docstring documents the new fields.
- [ ] The README's serverless guidance names the `health()` reading that catches the
      `shutdown()`-per-invocation mistake.
- [ ] `architecture.md` §7 (configuration) records the sink-swap semantics.

---

## Data Model

```python
# src/log_foundry/worker.py

class Health(NamedTuple):
    queued: int
    dropped: int
    failed_batches: int
    stopped_reason: str | None = None
    sink: SinkLosses | None = None          # SPEC-026
    retired: bool = False                   # new — shutdown() has completed
    submitted_after_shutdown: int = 0       # new — accepted, undeliverable


# on Worker, guarded by the existing _lock:
self.submitted_after_shutdown = 0
```

`retired` is a boolean where `stopped_reason` is a string, and deliberately so: SPEC-019 rejected an
`alive` flag because it would read `False` for a process that never logged. That objection does not
apply here — `health()` returns a zeroed snapshot with `retired=False` for a never-created worker,
which is true. The distinction is that `retired` describes an action the caller took, not a failure
the library detected.

If SPEC-026 has not landed, `retired` and `submitted_after_shutdown` are appended after
`stopped_reason` and SPEC-026's `sink` field appends after them. Order of arrival decides positions;
neither spec may renumber the other's.

---

## API / Interface Contract

```python
# The swap, in outline (config.py / decorator.py):

def _swap_sink(new_sink: Sink, *, timeout: float = 5.0) -> None:
    """Drain the current worker to its old sink, close it, and retarget. Never raises."""
    worker = decorator._worker
    if worker is None or worker.sink is new_sink:
        return
    worker.flush(timeout)      # everything submitted so far goes to the old sink
    old, worker.sink = worker.sink, new_sink
    old.close()                # guarded, per SPEC-025 FR-004


# Caller side — the reading that catches the serverless mistake:
h = log_foundry.health()
if h.retired and h.submitted_after_shutdown:
    ...  # shutdown() was called per-invocation; use flush()
```

Reassigning `worker.sink` rather than rebuilding the `Worker` keeps the queue, the thread, the
counters and the `atexit` registration intact — rebuilding would drop whatever was queued and
re-register the drain.

## Configuration / Environment

None new. `configure(sink=...)` gains defined semantics it did not have.

## File & Folder Structure

```
src/log_foundry/
├── worker.py       # modified — retired/submitted_after_shutdown, the throttled warning
├── config.py       # modified — late-sink swap, docstring
├── decorator.py    # modified — the swap helper's access to the worker
└── __init__.py     # modified — health()/shutdown() docstrings

tests/
├── test_worker.py  # modified — post-shutdown submission counting and warning
└── test_config.py  # modified — late sink swap, drain ordering, no-op, post-shutdown

docs/architecture.md  # modified — §7 sink-swap semantics
README.md             # modified — alert idiom, serverless guidance
```

## Implementation Phases

### Phase 1: Post-shutdown visibility

- `retired` + `submitted_after_shutdown` on `Health` and `Worker`; the throttled first-submission
  warning.
- Tests: clean shutdown reads retired with a zero count; N later calls count N; `stopped_reason`
  stays `None`; a never-shut-down worker reads false/zero; the warning fires once and is guarded.

### Phase 2: The sink swap

- `_swap_sink` and its wiring into `configure()`; bounded drain; guarded close.
- Tests: events before and after land in the right sinks; old sink closed once; same-sink no-op; no
  rebuild without `sink=`; pre-first-log behaviour unchanged; post-shutdown does not resurrect.

### Phase 3: Documentation

- `configure()`, `shutdown()`, `health()` docstrings; README alert idiom and serverless guidance;
  `architecture.md` §7.
