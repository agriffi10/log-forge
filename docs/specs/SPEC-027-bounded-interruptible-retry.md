# Spec: Bounded, Interruptible Retry

**ID:** SPEC-027  
**Status:** Draft  
**Last Updated:** 2026-08-05  
**Depends On:** SPEC-004, SPEC-009, SPEC-013

## Overview

Every sink that retries does so by sleeping the one thread that delivers anything. The worker owns a
single drain thread by design (arch §9), so a sink's backoff is not a local decision — it is a global
pause on log delivery, and it is held across `shutdown()`, which joins that thread with no timeout
(`worker.py:257`) and is registered via `atexit` (`decorator.py:207`).

Two of those sleeps are unbounded in the wrong direction. `HTTPSink._sleep_backoff`
(`http.py:195-198`) passes a server-supplied `Retry-After` straight to `time.sleep` with no ceiling
and no sign check: a destination returning `Retry-After: 86400` stalls all logging for a day, and a
measured `Retry-After: 8` with the default `max_retries=3` blocked `log_foundry.shutdown()` for
**22.01 seconds** — a process that hangs at exit because a log endpoint asked it to. A negative value
makes `time.sleep` raise `ValueError`, which inside a span is absorbed by `_emit` and on the orphan
path reaches the caller.

The third problem is the mirror image: `SQSSink._send` (`sqs.py:214-241`) retries in a tight loop
with no sleep at all, alone among the sinks — while its own docstring names throttling as the
retryable case, which is precisely the failure an immediate retry makes worse.

And every sink's sleep is a bare `time.sleep`, which cannot be interrupted. The worker's own backoff
already knows better: it uses `self._stop.wait(...)` (`worker.py:469`) so a shutdown cuts it short.
The sinks have no access to that signal.

This spec puts a ceiling on every wait, makes every wait interruptible by shutdown, and gives
`SQSSink` the backoff its peers already have.

## Scope

### In Scope

- A ceiling and a sign guard on `Retry-After`.
- A shared, interruptible sleep the sinks use instead of `time.sleep`.
- A way for the worker to pass its stop signal to a sink, without making sinks depend on the worker.
- Backoff between `SQSSink`'s retry attempts.
- A bound on how long `shutdown()` will wait for the drain thread.
- Documenting the total worst-case delay a sink can impose.

### Out of Scope

- **Changing retry *counts*.** `max_retries=3` everywhere stays; this spec governs how long each
  wait may be and whether it can be interrupted, not how many there are.
- **Whether a sink raises after exhausting its retries.** That is SPEC-026 FR-001. The two specs
  compose: SPEC-026 decides what is reported, this one decides how long getting there may take.
- **Making sink I/O asynchronous or concurrent.** One drain thread is an architectural decision
  (arch §9) and is not reopened. A thread pool per sink is a much larger design.
- **Honouring `Retry-After` in HTTP-date form.** `_parse_retry_after` already falls back to
  exponential backoff for it, which is correct and stays.
- **Jitter.** Worth having in a multi-process deployment, but it changes no failure mode described
  here and would make the delay bounds this spec asserts harder to test. Deliberately deferred.

---

## Functional Requirements

### FR-001: `Retry-After` is bounded and sign-checked

#### Description:

A server-supplied delay is advice from a destination, not an instruction the application must obey.
It is clamped to a ceiling and rejected when not a sane positive number, falling back to the sink's
own exponential backoff.

The ceiling is a constructor argument with a default, not a hard constant: a caller shipping to a
platform that legitimately asks for a two-minute pause should be able to allow it, and a caller with
a tight execution deadline should be able to lower it. The default is chosen so the *total* worst
case across a full retry budget stays well inside a typical serverless timeout.

#### Acceptance Criteria:

- [ ] `Retry-After: 86400` results in a wait of at most `max_retry_after` seconds.
- [ ] A negative `Retry-After` never reaches `time.sleep`; the sink falls back to exponential
      backoff and no `ValueError` is raised on any path, in-span or orphan.
- [ ] `Retry-After: 0`, `NaN` and `inf` are likewise rejected in favour of exponential backoff.
- [ ] A `Retry-After` at or below the ceiling is honoured exactly as today.
- [ ] The HTTP-date form still falls back to exponential backoff (unchanged).
- [ ] `max_retry_after` is a keyword argument on `HTTPSink` with a documented default, inherited by
      every platform subclass.

### FR-002: A sink's wait is interruptible by shutdown

#### Description:

Sinks stop calling `time.sleep` and call a shared helper that waits on an optional stop signal. When
the worker is shutting down, an in-progress backoff returns immediately and the sink proceeds to its
next attempt or gives up, rather than holding the drain thread for the full delay.

The signal reaches the sink without inverting the dependency: `sinks/` must not import `worker`.
The worker sets an attribute on the sink if the sink advertises one, using the same optional-protocol
probe SPEC-026 uses for `losses()`. A sink never constructed by a worker simply never has one, and
its waits are uninterruptible exactly as today.

#### Acceptance Criteria:

- [ ] A shared helper in `sinks/` waits for a given delay, returning early if a supplied
      `threading.Event` is set.
- [ ] `HTTPSink`, `SocketTransport`, `PostgresSink`, `ClickHouseSink` and every other sink that
      sleeps between attempts uses it.
- [ ] With a sink mid-backoff on a long delay, `log_foundry.shutdown()` completes promptly rather
      than waiting out the delay.
- [ ] A sink used standalone, with no worker and no signal, backs off exactly as it does today.
- [ ] Setting the signal does not abort an in-flight network call — only the wait between attempts.
      (Stated so an implementer does not attempt socket-level cancellation.)
- [ ] Nothing in `sinks/` imports `worker`.

### FR-003: `SQSSink` backs off between attempts

#### Description:

`SQSSink._send` gains the exponential backoff every other retrying sink has, using the FR-002
helper so it is interruptible too. Its retry loop otherwise keeps its current behaviour: only
`Failed` entries are re-sent, sender faults are abandoned immediately (SPEC-016 FR-006), and
successful entries are never re-sent.

#### Acceptance Criteria:

- [ ] Consecutive `send_message_batch` attempts for the same chunk are separated by a growing delay.
- [ ] The delay is interruptible per FR-002.
- [ ] A first attempt that fully succeeds sleeps not at all.
- [ ] Sender-fault abandonment still happens on the attempt that observed it, with no backoff
      before it.
- [ ] The existing SPEC-016 FIFO tests pass unchanged.

### FR-004: `shutdown()` cannot block forever

#### Description:

`Worker.shutdown()` joins the drain thread with no timeout. FR-002 removes the common cause of a
long join, but a sink blocked in a network call with a generous socket timeout can still hold it,
and `shutdown()` runs from `atexit` where an unbounded wait is a hung process.

`shutdown()` takes a timeout. On expiry it returns, having stopped what it can, and records that the
drain did not complete — it does not kill the thread, which is not possible for a Python thread and
would leave a sink mid-write if it were.

#### Acceptance Criteria:

- [ ] `Worker.shutdown(timeout=...)` returns within approximately that bound even when the drain
      thread is blocked.
- [ ] `log_foundry.shutdown()` exposes the timeout with a documented default, and `None` means wait
      indefinitely (the current behaviour, available on request).
- [ ] An expired shutdown is recorded so `health()` distinguishes it from a clean one, and writes
      one stderr line.
- [ ] The sink is not closed when the drain did not complete, since the thread may still be using
      it; this is stated in the docstring.
- [ ] A normal shutdown is unaffected: it still drains fully, closes the sink, and stays idempotent.
- [ ] The `atexit`-registered shutdown uses the bounded form.

### FR-005: The worst-case delay is documented

#### Description:

A caller with an execution deadline — the serverless case SPEC-013 exists for — needs to know the
maximum time a sink can hold the drain thread, as a function of the settings they control.

#### Acceptance Criteria:

- [ ] Each retrying sink's class docstring states its worst-case total delay in terms of
      `max_retries`, its backoff base, and (for HTTP) `max_retry_after`.
- [ ] The README states the same for the default configuration, in one line, beside the `flush()`
      guidance.
- [ ] `architecture.md` §9 notes that sink backoff pauses the single drain thread.

---

## Data Model

```python
# src/log_foundry/sinks/_retry.py  (new)

def wait(delay: float, stop: threading.Event | None = None) -> None:
    """Sleep `delay` seconds, returning early if `stop` is set. Never raises; ignores a non-positive delay."""


def clamp_server_delay(value: float | None, ceiling: float) -> float | None:
    """Return a sane, bounded server-supplied delay, or None to fall back to exponential backoff."""


# On every retrying sink — set by the worker when present, never required:
self.stop_signal: threading.Event | None = None
```

```python
# src/log_foundry/worker.py

def shutdown(self, timeout: float | None = 30.0) -> None: ...

class Health(NamedTuple):
    ...
    stopped_reason: str | None = None   # also set to "ShutdownTimeout" on an expired drain
```

Reusing `stopped_reason` rather than adding a field: an expired shutdown and a dead thread mean the
same thing to a reader — the drain did not finish and events were lost — and SPEC-019 chose a reason
string precisely so the vocabulary could extend without changing the alert's shape.

---

## API / Interface Contract

```python
# HTTPSink, in outline:

def _sleep_backoff(self, attempt: int, retry_after: float | None) -> None:
    server = clamp_server_delay(retry_after, self.max_retry_after)
    delay = server if server is not None else _BACKOFF_BASE * (2**attempt)
    wait(delay, self.stop_signal)


# Worker, wiring the signal without importing anything new:
if hasattr(self.sink, "stop_signal"):
    self.sink.stop_signal = self._stop
```

Defaults: `max_retry_after=30.0`, `Worker.shutdown(timeout=30.0)`. With `max_retries=3` the
worst-case HTTP delay is 3 × 30 s = 90 s of backoff, which is why FR-004's bound matters even after
FR-001 — the ceiling bounds each wait, the shutdown timeout bounds the total.

## Configuration / Environment

- `HTTPSink(max_retry_after: float = 30.0)` — new keyword argument, inherited by platform subclasses.
- `log_foundry.shutdown(timeout: float | None = 30.0)` — new argument on an existing function.

No environment variables.

## File & Folder Structure

```
src/log_foundry/
├── worker.py            # modified — bounded shutdown, stop-signal wiring, timeout reason
├── __init__.py          # modified — shutdown(timeout=...)
└── sinks/
    ├── _retry.py        # new — wait(), clamp_server_delay()
    ├── http.py          # modified — clamped Retry-After, interruptible wait, max_retry_after
    ├── _socket.py       # modified — interruptible wait
    ├── sqs.py           # modified — backoff between attempts
    └── (each retrying sink)  # modified — interruptible wait

tests/
├── test_worker.py       # modified — bounded shutdown, signal wiring
├── test_sinks_http.py   # modified — Retry-After clamp, sign guard, interruption
├── test_sinks_sqs.py    # modified — backoff between attempts
└── test_sinks_syslog.py # modified — interruptible socket backoff

docs/architecture.md     # modified — §9 note
README.md                # modified — worst-case delay, shutdown timeout
```

## Implementation Phases

### Phase 1: The shared wait

- `sinks/_retry.py` with `wait` and `clamp_server_delay`.
- Wire `stop_signal` from the worker; convert `HTTPSink` and `SocketTransport`.
- Tests: clamping, sign/NaN/inf rejection, early return on the signal, standalone behaviour.

### Phase 2: The remaining sinks

- Convert every other retrying sink to `wait`; add `SQSSink`'s backoff.
- Tests per sink, including that SPEC-016's FIFO suite is unaffected.

### Phase 3: Bounded shutdown

- `Worker.shutdown(timeout=...)`, the expired-drain record, and the `atexit` path.
- Tests: a blocked drain returns within the bound, records the reason, leaves the sink open, and a
  clean shutdown is unchanged.

### Phase 4: Documentation

- Per-sink worst-case docstrings; README line; `architecture.md` §9.
