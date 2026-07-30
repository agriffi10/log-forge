# Spec: Open-Item Cleanup

**ID:** SPEC-021  
**Status:** Draft  
**Last Updated:** 2026-07-30  
**Depends On:** SPEC-013, SPEC-017, SPEC-019, SPEC-020

## Overview

Four robustness specs in a row (SPEC-017 through SPEC-020) each left a "Notes for the next spec"
list, and `architecture.md` §12 has carried an "Open items" list since before the first line of code
was written. Together that is seventeen delivery-doc notes and three architecture items, and a
reader today cannot tell which are live defects, which were fixed by a later spec, and which are
settled decisions that merely read like unfinished business.

Most are the third kind. Two are the second — SPEC-017 warns that non-`str` scalars are unbounded
and that a dead worker moves no counter, both of which SPEC-020 and SPEC-019 have since fixed, so
the notes are now actively misleading. A small number are genuine warts, and one of those is a false
success signal of the exact kind these four specs were written to eliminate: `flush()` returns
`True` when the drain it forced was abandoned.

This spec fixes what is broken, and reconciles the rest so that nothing in the repository reads as
open unless it is. An item is closed by being fixed, by being recorded as a settled decision, or by
being written down as a known constraint — never by being deleted quietly.

## Scope

### In Scope

- `flush()` reporting whether the drain it forced actually reached the sink.
- The terminal-failure stderr line accounting for everything undelivered, not only what the drain
  thread happened to be holding.
- Making the integer ceiling exact for negative values.
- Documenting the two units `max_value_bytes` now carries.
- Reconciling every delivery-doc note and every `architecture.md` §12 item: fixed, settled, or
  recorded as a constraint.

### Out of Scope

- **A per-event byte ceiling.** Deferred by SPEC-017 and again by SPEC-020, and deferred again here
  deliberately: it is a feature with real design surface — what happens on breach, which fields are
  sacrificed — and it would dominate a cleanup spec. It is recorded as a Known Constraint in
  `architecture.md` instead, since it already carries a visible signal (a sink drops the event and
  counts `dropped_oversized`), making it a documented limitation rather than a silent loss.
- **Renaming `max_value_bytes` or adding a second ceiling key.** SPEC-020 ruled out a new config
  key; re-opening that for a cosmetic gain would be a breaking config change.
- **Making `MultiSink` isolate a `BaseException` from a child.** It catches `Exception` on purpose.
  Widening it would swallow a `KeyboardInterrupt` raised inside a child sink and continue to the
  next one, which SPEC-019 already ruled is worse than the failure it would prevent. Since SPEC-019
  the escape is caught and named by the worker's terminal guard, so it is recorded, not silent.
- **Narrowing the `bit_length()` trust boundary** (SPEC-020) or **unifying the two batch-response
  correlation styles** (SPEC-018). Both are settled decisions; this spec records them as such.
- **Making backpressure drop-vs-block configurable** (`architecture.md` §12). A feature, not a
  wart — recorded as a constraint with the current behaviour stated, not built here.

---

## Functional Requirements

### FR-001: `flush()` reports whether the drain it forced was delivered

#### Description:

`flush()` returns `True` when the worker has emitted everything submitted before the call. It
currently returns `True` whenever the drain *ran*, including when the emit it forced failed every
retry and the batch was abandoned — verified against `v0.7.1`: a sink that always raises yields
`flush() is True` with `failed_batches == 1`.

That is a false success signal, and it is worst exactly where `flush()` matters most. SPEC-013 built
it for the serverless path, where a handler flushes before the environment freezes and `atexit`
never runs; a `True` there is the caller's only evidence the tail of the queue survived.

The marker already travels the queue and is already answered by the drain thread. It carries the
outcome back rather than merely signalling that it was reached. The waiter must still be released on
every path — a flush that fails must return `False` promptly, never strand the caller until timeout.

#### Acceptance Criteria:

- [ ] `flush()` returns `True` when the events it forced were emitted successfully.
- [ ] `flush()` returns `False` when the batch it forced was abandoned after exhausting retries.
- [ ] `flush()` returns `True` when there was nothing pending to drain — an empty drain is a
      successful one, not a failure.
- [ ] A failing flush still releases its waiter promptly; the caller does not wait out `timeout`.
- [ ] A `flush()` that succeeds after the sink recovers mid-retry returns `True`, since the events
      did reach the sink.
- [ ] The existing returns are unchanged: `False` after `shutdown()`, `False` on a dead worker,
      `False` on timeout, `False` when the queue is too full to accept the marker.
- [ ] `shutdown()` is unaffected — it does not return a value and does not gain one.
- [ ] A `flush()` racing `shutdown()` still answers its marker from the final drain, and answers it
      with the same outcome rule.

### FR-002: The terminal-failure line accounts for everything undelivered

#### Description:

SPEC-019's stderr line reports how many event-lists the drain thread was holding when it died. Events
still sitting in the bounded queue are equally undelivered and are not in that number, so a reader
of "1 undrained event-list(s)" can conclude far less was lost than actually was.

The line reports both: what was in hand and what was still queued behind it.

#### Acceptance Criteria:

- [ ] The terminal-failure line states the number of event-lists held **and** the number still
      queued at the moment the thread died.
- [ ] It remains exactly one line, prefixed `log-foundry:`, still naming the exception type only
      and never its message.
- [ ] Reading the queue size cannot itself raise or block; if it is unavailable the line is still
      written, and the record in `stopped_reason` is still set first.

### FR-003: The integer ceiling is exact for negative values

#### Description:

SPEC-020 bounds an integer by the decimal length of its magnitude, so the minus sign is not counted.
`Config(max_value_bytes=10)` therefore admits `-10**9`, which renders as eleven bytes. Only reachable
at very small configured ceilings, but the ceiling is documented as a ceiling.

#### Acceptance Criteria:

- [ ] A negative integer whose rendered length including the sign exceeds the ceiling is replaced.
- [ ] The positive value of the same magnitude, if it fits, is still admitted — the two agree on
      *rendered* length, which is what the ceiling measures.
- [ ] The common path is unchanged: ordinary negative integers stay `int`, at full precision.
- [ ] The FR-002 guarantee of SPEC-020 still holds — no integer is admitted that `str()` refuses.

### FR-004: Every open item is reconciled

#### Description:

No note in a delivery doc, and no entry in `architecture.md` §12, may read as open unless it is.
Each is resolved one of three ways, and each keeps its reasoning:

- **Fixed** — struck through or rewritten to say which spec closed it.
- **Settled** — moved to `CLAUDE.md` Key Decisions or the relevant docstring, as a decision with its
  rationale rather than an outstanding question.
- **Constraint** — written into `architecture.md` §13 Known Constraints, stating the current
  behaviour and why it is acceptable.

The two stale SPEC-017 notes are the priority: "non-`str` scalars are still unbounded" and "a
`BaseException` from a child sink … kills the worker thread with no counter moved" are both false as
of SPEC-020 and SPEC-019, and a reader trusting them would be misled about the library's guarantees.

#### Acceptance Criteria:

- [ ] SPEC-017's non-`str`-scalar note records that SPEC-020 closed it; SPEC-019's restatement of
      the same item does too.
- [ ] SPEC-017's `BaseException`/no-liveness-signal note records what SPEC-019 closed and what
      remains true (a child's `BaseException` still ends the worker — now named, not silent).
- [ ] `architecture.md` §12's three items are each moved to Resolved with the spec that settled
      them, or restated as a constraint: orphan-log behaviour and console-echo defaults shipped in
      SPEC-002; configurable backpressure did not and is a constraint.
- [ ] `architecture.md` §13 gains the per-event-size constraint, stating the per-value ceilings, the
      resulting worst case, and the `dropped_oversized` signal.
- [ ] The remaining delivery-doc notes each end in one of the three states, with none left as a bare
      observation.
- [ ] No delivery doc is edited to remove a note without replacing it with its resolution — the
      history stays legible.

---

## Data Model

```python
# src/log_foundry/worker.py — _FlushMarker gains an outcome

class _FlushMarker:
    __slots__ = ("delivered", "event")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.delivered = True   # an empty drain is a successful one
```

`delivered` is written by the drain thread before `event.set()` and read by the waiter after
`event.wait()` returns, so the `Event` provides the ordering and no additional lock is needed.

---

## API / Interface Contract

```python
def flush(self, timeout: float | None = 5.0) -> bool:
    """Drain everything submitted before this call through the sink, without stopping.

    True once the worker has *delivered* them; False on timeout, on a worker already shut down
    or dead, or when the drain ran and the batch was abandoned.
    """
```

No signature change. The only difference is that `True` now means what it always claimed.

## Configuration / Environment

None. No new config keys, environment variables, or constructor arguments.

## File & Folder Structure

```
src/log_foundry/
├── worker.py          # modified — marker outcome (FR-001), stderr line (FR-002)
├── sanitize.py        # modified — the sign (FR-003)
└── config.py          # modified — max_value_bytes docstring (FR-004)

tests/
├── test_worker.py     # modified — flush outcomes, the stderr line
└── test_sanitize.py   # modified — negative bound

docs/
├── architecture.md            # modified — §12 reconciled, §13 gains two constraints
├── spec-delivery/SPEC-017..020-*.md   # modified — notes resolved in place
└── ../CLAUDE.md               # modified — Key Decisions for the settled items

README.md              # modified — flush() semantics, max_value_bytes units
```

## Implementation Phases

### Phase 1: `flush()` truthfulness

- `_FlushMarker.delivered`; `_emit` reports success; both the marker branch and the final drain set
  the outcome before releasing the waiter.
- Tests: delivered, abandoned, nothing-pending, recovered-mid-retry, the four existing `False`
  paths unchanged, and a `flush()` racing `shutdown()`.

### Phase 2: The smaller fixes

- FR-002's stderr line; FR-003's sign; the `max_value_bytes` docstring and README note.
- Tests for each.

### Phase 3: Reconciliation

- Work FR-004 through: the two stale SPEC-017 notes, SPEC-019's restatement, `architecture.md` §12
  and §13, then the remaining notes.
- Add the Key Decisions lines for the settled items.
