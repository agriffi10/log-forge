# Spec: Worker Liveness and Terminal-Failure Reporting

**ID:** SPEC-019  
**Status:** Draft  
**Last Updated:** 2026-07-29  
**Depends On:** SPEC-004, SPEC-017

## Overview

Every event the library delivers passes through one background drain thread. That thread has no
terminal-failure path: if anything escapes its loop, it stops, and nothing records that it stopped.
The application keeps logging normally, submissions keep landing in a queue no one is draining, and
`health()` — the accessor SPEC-017 shipped precisely so an operator can notice absorbed loss —
keeps returning a snapshot that looks fine.

The reading eventually changes, but into the *wrong* signal. Once the bounded queue fills, `dropped`
starts climbing, which already means something else: backpressure, a destination that cannot keep
up. That has a different remedy (tune the batch, fix or scale the sink) from a dead drain thread
(restart the process, nothing will ever drain again), and today the two are indistinguishable from
`health()` — delayed by however long the queue takes to fill.

This spec gives the drain thread a terminal-failure path: an escape is recorded, announced once, and
reported through `health()` at the moment it happens. It does not try to keep the thread alive. As
in SPEC-017 and SPEC-018, the goal is that a loss the library cannot prevent is never mistaken for
success.

## Scope

### In Scope

- A guard around the drain loop so no exit from it goes unrecorded.
- A new `Health` field reporting that terminal failure, and the `Worker` attribute behind it.
- One stderr line, matching the convention the other loss paths already use.
- Unit tests for the terminal-failure path, which today has no coverage.
- README + docstring guidance so the new field is discoverable beside the existing counters.

### Out of Scope

- **Restarting or resurrecting the worker.** A thread that rebuilds itself after a `SystemExit`
  fights a process that is trying to exit. The library reports the failure; the operator restarts
  the process. This is a deliberate non-goal, not an omission.
- **Detecting the dead worker from `submit()`.** A liveness check per submission is a hot-path
  change on the caller's thread, and SPEC-017 already shipped one regression of exactly that shape
  (a diagnostic added to a hot path became a hot-path failure). Detection belongs in `health()`,
  which the caller polls on its own schedule.
- **`flush()`.** It already returns `False` immediately when the thread is not alive
  (`worker.py:161`), so a dead worker does not cost a caller its timeout. No change needed.
- **What `_emit` catches.** Its `except Exception` + bounded retry + `failed_batches` accounting is
  the *non*-terminal path and stays exactly as it is. This spec covers only what escapes it.
- **Bounding non-`str` scalars in `sanitize.py`.** A real remaining hole in SPEC-017's "no value is
  unbounded" intent, but a different module and a different concern (payload safety, not worker
  liveness). It belongs to a payload-safety follow-up, not here.
- Any new config key, environment variable, or constructor argument.

---

## Functional Requirements

### FR-001: The drain loop has a terminal-failure path

#### Description:

`Worker._run` must not be able to terminate without recording why. Its body is guarded such that
any exception escaping the loop — `Exception` or `BaseException` — is caught, recorded on the
worker, and announced, after which the thread exits.

The guard catches `BaseException`, not `Exception`. `SystemExit` is the motivating case: CPython's
thread bootstrap discards it silently, so today it produces no traceback, no counter, and no trace
of any kind. It is also the class most likely to arrive from a third-party client that calls
`sys.exit()` on a configuration fault. Broader than `BaseException` is not possible; narrower
leaves the silent case silent.

The guard does **not** swallow-and-continue. Looping onward past a `KeyboardInterrupt` or
`SystemExit` would be a worse failure than the one being fixed. The thread records and exits.

#### Acceptance Criteria:

- [ ] A sink whose `emit` raises `SystemExit` causes the worker thread to stop with the failure
      recorded, rather than stopping with nothing recorded.
- [ ] The same holds for `KeyboardInterrupt` and for a bare `BaseException` subclass.
- [ ] The recorded value is the exception's **type name only** (e.g. `"SystemExit"`), never its
      message, args, or `repr` — consistent with `sanitize.py`'s type-name placeholder and the
      arch §6 rule against putting caller data where it was not asked for.
- [ ] The thread is no longer alive once the failure is recorded; it does not resume draining.
- [ ] An ordinary `Exception` raised by a sink is unaffected: it is still caught inside `_emit`,
      still retried to `max_retries`, still counted on `failed_batches`, and the thread keeps
      running. It does not set the terminal-failure field.
- [ ] Nothing escapes into any caller thread — `submit`, `flush`, `shutdown`, and a decorated
      function all behave as they do today when the worker has died (architecture §4).

### FR-002: The terminal failure is announced once, on stderr

#### Description:

The thread writes one line before exiting, matching the format the other loss paths already use:
the `log-foundry:` prefix, what happened, and how much was in hand when it happened.

Recording precedes announcing. A closed or broken stderr must not be able to cost the record —
`submit()` already wraps its own warning for this reason, and the same applies here with more
force, since this line is written exactly once and cannot be re-emitted later.

#### Acceptance Criteria:

- [ ] A terminal failure writes exactly one line to stderr, prefixed `log-foundry:`, naming the
      exception type and the number of undrained event-lists the thread was holding.
- [ ] The exception's message is not written — type name only, as in FR-001.
- [ ] If the stderr write itself raises, the failure is still recorded and readable through
      `health()`; the write is best-effort.
- [ ] A worker that runs and shuts down cleanly writes no such line.

### FR-003: `health()` reports the terminal failure

#### Description:

`Health` gains one field, `stopped_reason`, so an operator polling `health()` can tell a dead drain
thread from backpressure at the moment it happens rather than inferring it from `dropped` once the
queue fills.

The field is `str | None` rather than a liveness boolean on purpose. `health()` returns a **zeroed**
snapshot for a process that has never logged, and a boolean `alive` would read `False` there — a
false alarm for every process that has not logged yet. A reason string has no such trap: `None`
means "no terminal failure", which is the truth for a live worker, a never-created worker, and a
cleanly shut-down one alike. It also keeps the field parallel to `dropped` and `failed_batches`,
where a falsy value means nothing went wrong, so the README's existing alert idiom extends by one
term instead of changing shape.

#### Acceptance Criteria:

- [ ] `Health` carries `stopped_reason: str | None`, appended after `failed_batches`.
- [ ] It is `None` for a live worker, for a process that has never logged (which creates no
      worker), and after a clean `shutdown()`.
- [ ] It is the exception type name after a terminal failure, and stays readable afterwards —
      including after a subsequent `shutdown()`, which must not clear it.
- [ ] It is read under the same lock as the other counters, so a concurrent `health()` never sees a
      half-updated snapshot.
- [ ] The existing fields keep their positions and meanings; `h.queued`, `h.dropped`,
      `h.failed_batches`, and index access to them are unchanged.

### FR-004: The new field is documented where the counters are

#### Description:

`stopped_reason` is discoverable in the same places `dropped` and `failed_batches` are, including
what distinguishes it from a climbing `dropped`.

#### Acceptance Criteria:

- [ ] The README's `health()` example includes `stopped_reason` in its alert condition.
- [ ] The README states what a non-`None` value means operationally — the drain thread is gone,
      nothing further will be delivered, and the process needs restarting — as against a climbing
      `dropped`, which means the destination is not keeping up.
- [ ] The `Health` docstring documents the field in its existing `Attributes:` section, and
      `log_foundry.health()`'s docstring mentions it alongside the other two.
- [ ] The tuple-unpacking break in the Data Model note below is stated in the README's release
      guidance or the delivery doc, so it is not discovered by a user.

---

## Data Model

```python
# src/log_foundry/worker.py

class Health(NamedTuple):
    queued: int
    dropped: int
    failed_batches: int
    stopped_reason: str | None   # new — exception type name, or None if the drain never died


# on Worker, beside `dropped` / `failed_batches`, guarded by the same `_lock`:
self.stopped_reason: str | None = None
```

Appending the field keeps attribute access (`h.dropped`) and index access (`h[1]`) working, which
is the only usage the README has ever advertised. It **does** break full tuple-unpacking —
`queued, dropped, failed = health()` now raises `ValueError`. That is an accepted, stated break for
a 0.x library, not an oversight; it is the cost of `Health` having been a `NamedTuple`.

---

## API / Interface Contract

```python
# Worker._run, in outline — the guard wraps the whole body, so the post-loop
# final drain is inside it too:

def _run(self) -> None:
    try:
        ...  # existing drain loop, then self._final_drain(pending)
    except BaseException as exc:
        with self._lock:
            self.stopped_reason = type(exc).__name__
        try:
            sys.stderr.write(
                f"log-foundry: worker thread stopped on {type(exc).__name__}; "
                f"{len(pending)} undrained event-list(s), nothing further will be delivered\n"
            )
        except Exception:
            pass  # the record above is what matters; the line is best-effort
        return


# Caller side — the README alert idiom, extended by one term:
h = log_foundry.health()
if h.dropped or h.failed_batches or h.stopped_reason:
    ...  # logs were lost
```

## Configuration / Environment

None. No new config keys, environment variables, or constructor arguments.

## File & Folder Structure

```
src/log_foundry/
├── worker.py          # modified — Health field, Worker attribute, the _run guard
└── __init__.py        # modified — health() docstring only

tests/
└── test_worker.py     # modified — terminal-failure paths

README.md              # modified — health() guidance
```

## Implementation Phases

### Phase 1: The terminal-failure path

- Add `stopped_reason` to `Health` and to `Worker.__init__`, under the existing `_lock` discipline.
- Guard `_run`'s body: record, announce best-effort, exit.
- Extend `test_worker.py`: a `SystemExit`-raising sink records the type name and stops the thread; a
  `KeyboardInterrupt` likewise; an ordinary `Exception` still retries and counts `failed_batches`
  without setting the field; `health()` stays readable through a later `shutdown()`; a clean run
  leaves the field `None` and stderr empty.

### Phase 2: Documentation

- README: `stopped_reason` in the `health()` example and the alert condition, plus one line on how
  it differs operationally from a climbing `dropped`, and a note on the tuple-unpacking break.
- Docstrings: the `Health` `Attributes:` section and `log_foundry.health()`.
