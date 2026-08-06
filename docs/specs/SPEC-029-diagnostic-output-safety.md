# Spec: Diagnostic Output Safety

**ID:** SPEC-029  
**Status:** In Progress  
**Last Updated:** 2026-08-06  
**Depends On:** SPEC-017, SPEC-019

## Overview

The library writes to stderr in about twenty places, every one of them announcing loss. Two rules
govern those lines, both already settled and both already violated.

The first is arch §6: caller data does not go where it was not asked for. `Worker._terminal_failure`
states the reasoning precisely — *"a sink's exception text can carry event data"* — and reports the
exception's type name only. `sanitize._placeholder` refuses `repr(value)` for the same reason,
because a `repr` routinely prints attribute values and a credential held on a client object would
land in the log. Yet twelve sink sites interpolate `{err!r}`: `postgres.py:76`, `clickhouse.py:98`,
`mongodb.py`, `multi.py:63`, `http.py:131`, `_socket.py:86`, `nats.py`, `redis.py`, `rabbitmq.py`,
`pubsub.py`, `eventhubs.py`, `kafka.py`. `PostgresSink` is the sharpest case: `_row` binds the whole
`json.dumps(event)` as a statement parameter, and a psycopg error repr routinely carries the failing
statement and its parameters — so the diagnostic for a failed insert can reprint the very event,
PII included, into a stream the operator was not asked to secure.

The second is that a diagnostic must never cost more than the thing it describes. `Worker.submit`
and `Worker._terminal_failure` both wrap their stderr writes and say why. `Worker._emit`
(`worker.py:462`) does not — so on a closed or wedged stderr, abandoning one batch raises out of
`_emit`, through `_drain`, into `_run`'s `BaseException` handler, and the drain thread dies for
good. Verified: `stopped_reason='ValueError'`, thread not alive. `_socket.py:84` has the same
unguarded shape on the same thread.

This spec routes every diagnostic through one helper that applies both rules, once.

## Scope

### In Scope

- A single internal module owning every stderr line the library writes.
- Type-name-only exception reporting, with a bounded, escaped detail channel for the cases where a
  bare type name is not diagnosable.
- Guarding every write, so no diagnostic can kill a thread or reach a caller.
- Converting all existing call sites.
- A test asserting no new `sys.stderr.write` appears outside the module.

### Out of Scope

- **Routing diagnostics through the `logging` module or a user-supplied handler.** A tempting
  generalization, but the library's own failures must not depend on the machinery that may be
  failing, and `LoggingSink` already exists for the opposite direction. stderr stays the channel.
- **Changing *when* a line is written.** Each existing site keeps its trigger and its throttle —
  `_DROP_WARN_EVERY` included. Only the formatting and the guard change.
- **Removing lines in favour of `health()`.** SPEC-026 makes the counters readable; the lines stay,
  because an operator reading logs and an operator polling `health()` are different people.
- **The `_warn_rejected` echo in `decorator.py`.** It already bounds and `repr`-escapes an
  attacker-controllable value deliberately (SPEC-014), and that reasoning is sound. It moves to the
  new module unchanged in behaviour.
- **Structured (JSON) diagnostics.** These lines are for humans; the structured stream is the sink.

---

## Functional Requirements

### FR-001: One module owns every diagnostic line

#### Description:

A new internal module provides the writers. Every `sys.stderr.write` in `src/log_foundry/` is
replaced by a call into it, so the two rules are applied in one place rather than remembered at
twenty call sites — which is how twelve of them came to disagree with the other eight.

#### Acceptance Criteria:

- [ ] The module exposes a small set of writers covering the existing shapes: an absorbed exception,
      a counted loss, and a rejected inbound value.
- [ ] Every line still begins `log-foundry: ` and remains a single line.
- [ ] No `sys.stderr.write` call remains in `src/log_foundry/` outside the module.
- [ ] A test asserts that, so a future call site cannot quietly reintroduce one.
- [ ] The existing messages' information content is preserved — counts, sink names, attempt numbers
      and the throttle behaviour are unchanged.

### FR-002: An exception is reported by type, not by `repr`

#### Description:

The rule `Worker._terminal_failure` follows becomes the rule everywhere: an exception is named by
`type(exc).__name__`, and its message, `args` and `repr` are not written.

Where a bare type name is genuinely not diagnosable — `OSError` alone does not tell an operator
whether the socket was refused or the host unknown — the writer accepts an explicit, caller-chosen
detail string built from values the library controls (an `errno`, an HTTP status, a count). The
detail is never derived from the exception's text, and is bounded and escaped as `_warn_rejected`
already bounds and escapes an inbound value.

#### Acceptance Criteria:

- [ ] No diagnostic line contains `repr(exc)`, `str(exc)`, or `exc.args`.
- [ ] A `PostgresSink` insert failing against a driver whose exception repr contains the event's
      JSON produces a line containing the exception type and not the event data — the test states
      the leak it prevents.
- [ ] `OSError`-driven lines (`SocketTransport`, `HTTPSink` connection errors) still identify the
      failure well enough to act on, via `errno` or an equivalent library-controlled value.
- [ ] Any detail string is truncated to a documented bound and has newlines and control characters
      escaped, so a value that reached it could not forge a second line.
- [ ] `_warn_rejected`'s existing bounded-`repr` behaviour for inbound trace context is preserved
      exactly (its input is an inbound *header*, not an exception, and it is the one place a bounded
      `repr` is correct).

### FR-003: A diagnostic can never be the failure

#### Description:

Every write is guarded. A closed, wedged or broken stderr causes a line to be skipped, never an
exception — on the worker thread, where it currently kills the drain, and on a caller's thread,
where it would reach the application.

The guard is inside the module, so the property holds for every call site by construction rather
than by each remembering to wrap.

#### Acceptance Criteria:

- [ ] With a `sys.stderr` whose `write` raises, abandoning a batch leaves the worker thread alive
      and draining, and `stopped_reason` stays `None` — the case that fails today.
- [ ] The same holds for `SocketTransport`'s abandonment line and every other sink's.
- [ ] The same holds on the caller's thread: a broken stderr plus a failing orphan emit still
      returns normally.
- [ ] The associated counter is incremented **before** the write is attempted, at every site, so the
      record survives a failed announcement (the ordering `_terminal_failure` already documents).
- [ ] A `BaseException` from the write — a `KeyboardInterrupt` landing mid-write — still propagates.

### FR-004: The rules are documented where the next writer will look

#### Description:

The module's docstring states both rules and why, so the next diagnostic added does not have to
rediscover them from `_terminal_failure`'s comment.

#### Acceptance Criteria:

- [ ] The module docstring states the type-name rule with its arch §6 justification and the
      `PostgresSink` example.
- [ ] It states the guard rule and the record-before-announce ordering.
- [ ] It states that stderr is the channel and why the `logging` module is not.
- [ ] `architecture.md` §6 gains a line pointing at it, so the rule is discoverable from the
      principle rather than only from the code.

---

## Data Model

```python
# src/log_foundry/_diag.py

_MAX_DETAIL = 200   # bound on any caller-supplied detail string

def absorbed(where: str, exc: BaseException, detail: str = "") -> None:
    """A failure the library caught and did not propagate. Type name only. Never raises."""

def lost(what: str, count: int, detail: str = "") -> None:
    """A counted loss: N events/messages/batches dropped or abandoned. Never raises."""

def rejected(reason: str, value: object) -> None:
    """An inbound value the library refused — bounded, repr-escaped (SPEC-014). Never raises."""
```

`_diag.py` imports nothing from the package, so it can be used from any module — `worker`,
`decorator`, `api`, and every sink — without an import cycle. It is the same discipline
`sanitize.py` follows for the same reason.

SPEC-025 introduces `absorbed` for its own three call sites. Whichever spec builds first ships the
module in this shape; the other adopts it. They must not each invent one.

---

## API / Interface Contract

```python
# Before — worker.py:462, unguarded, and the shape repeated across sinks:
sys.stderr.write(
    f"log-foundry: abandoned a batch of {len(batch)} event(s) after "
    f"{retries + 1} failed emit attempts\n"
)

# After:
with self._lock:
    self.failed_batches += 1          # record first — the announcement is best-effort
_diag.lost("batch", len(batch), f"after {retries + 1} failed emit attempts")


# Before — postgres.py:76, leaking the driver's repr:
sys.stderr.write(f"log-foundry: PostgresSink abandoned {len(batch)} event(s) ... ({err!r})\n")

# After:
_diag.lost("event", len(batch), f"PostgresSink, {self.max_retries + 1} attempts, {type(err).__name__}")
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── _diag.py           # new — the writers, the rules, the guard
├── worker.py          # modified — three sites, including the unguarded one
├── decorator.py       # modified — _warn_rejected moves here
├── api.py             # modified — SPEC-025's absorbed-failure lines
└── sinks/             # modified — every stderr site (~15 files)

tests/
├── test_diag.py       # new — type-name rule, bounding, escaping, guard
└── test_worker.py     # modified — broken stderr no longer kills the drain
```

## Implementation Phases

### Phase 1: The module

- `_diag.py` with the three writers, the bound, the escaping and the guard.
- `test_diag.py`: no `repr`/`str` of an exception; detail bounded and escaped; a raising stderr is
  absorbed; a `BaseException` still propagates.

### Phase 2: Core call sites

- Convert `worker.py` (including the unguarded `_emit` line), `decorator.py`'s `_warn_rejected`, and
  `api.py`; enforce record-before-announce at each.
- Tests: broken stderr leaves the drain thread alive and `stopped_reason` `None`.

### Phase 3: Sinks

- Convert every sink stderr site; remove all `{err!r}` interpolation.
- Tests: the `PostgresSink` leak case; `SocketTransport`'s guard; each converted line still carries
  its counts.

### Phase 4: Enforcement and docs

- The test asserting no `sys.stderr.write` outside `_diag.py`.
- `architecture.md` §6 pointer.
