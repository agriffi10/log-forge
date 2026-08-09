# Spec: Caller Safety and Serialization

**ID:** SPEC-037  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-017, SPEC-020, SPEC-025

## Overview

Two promises this library makes in its first paragraph, each broken on paths their own spec did
not check.

**SPEC-025 guaranteed the library never fails the caller** and verified it on the orphan path.
Inside a span it does not hold: `api._log` guards only the orphan branch, on the recorded grounds
that the in-span branch "only appends to a list" — but it calls `build_event`, which calls
`truncate_str`, which calls `value.encode`.

```
lf.info(some_exception)   orphan path → returned normally
                          in-span     → CALLER GOT AttributeError: 'ValueError' object has no attribute 'encode'
```

Identical call, opposite outcomes. And the decorator's own handler then records the span with
`status=error` and an `error.type` of `AttributeError` that the caller's code never raised — so
it is wrong data *and* a broken caller.

**SPEC-017 guaranteed an event is safe for any sink to serialize** and built `sanitize` to make
it true. `NaN` and `Infinity` pass straight through: `_dispatch` returns `float` unchanged, and
every sink then does a bare `json.dumps`. The output is not valid JSON (RFC 8259 has no such
tokens), carries no `truncated` marker to hint at it, and a strict consumer — Fluent Bit, a
Logstash `json` codec, Jackson behind Elasticsearch — drops the record with nothing on the
library side to see.

Both were found by the 2026-08-07 audit (A2, S1) and both are now `xfail` cells in
`tests/test_promises.py`, which is the harness that would have caught them: each is a promise
verified on one path, or for one type, out of several.

## Scope

### In Scope

- Guarding the in-span emit so no `api._log` call can fail the caller.
- Not fabricating an `error.type` on a span whose function did not raise.
- `NaN` and `Infinity` in `sanitize`.

### Out of Scope

- **Widening the guard to `BaseException`.** SPEC-025 FR-004 settled it: a `KeyboardInterrupt` or
  `SystemExit` is the operator's or the runtime's intent and must reach the caller.
- **Validating `message` or field *types* at the call site.** Raising a `TypeError` on
  `info(exc)` would be a different broken promise. The library coerces, as SPEC-017 established.
- **A per-event byte ceiling.** Deferred by SPEC-017, again by SPEC-020, again by SPEC-021, and
  again here; it is a feature with real design surface.
- **`allow_nan=False` on every sink's `json.dumps`.** That moves the failure into the sinks'
  counted paths — better than today, but it *loses the event* where FR-003 keeps it, and it
  would have to be repeated in 40-odd call sites, which is exactly what SPEC-017 built `sanitize`
  to avoid.

---

## Functional Requirements

### FR-001: No `info()` call can fail the caller, in a span or out of one

#### Description:

The in-span branch of `api._log` is unguarded. It is not "only a list append": `build_event`
runs the message and every field through `sanitize`, and any value whose type surprises it can
raise. `info(exc)` and `info(some_object)` are ordinary slips that `mypy` catches only where the
call site is typed.

The guard is the one the orphan branch already has — catch `Exception`, never `BaseException`,
announce through `_diag.absorbed`, lose the event rather than the call.

**The reason the branch was left unguarded is recorded in the docstring and is wrong**, so the
fix must correct the reasoning as well as the code, or the next person removes the guard for the
same stated reason.

#### Acceptance Criteria:

- [ ] AC-1: `info(ValueError("x"))` returns normally inside a span, inside an async span, and
      outside one. `tests/test_promises.py`'s two `xfail` cells for this lose their markers,
      which `strict=True` forces.
- [ ] AC-2: The absorbed failure is announced exactly once through `_diag`, naming the exception
      **type** only (arch §6).
- [ ] AC-3: `KeyboardInterrupt` and `SystemExit` still propagate from inside a span.
- [ ] AC-4: The decorated function still returns its value normally, and the span still closes
      with `status=ok`.
- [ ] AC-5: The event that could not be built is lost and counted, not silently dropped — it
      goes to the same counter SPEC-036 FR-003 adds for the orphan path if the call was an orphan,
      and to the span's own path otherwise. The two must not double-count; a test asserts the
      total.
- [ ] AC-6: The stale reasoning in `api._log`'s docstring is replaced with what is actually true:
      the branch calls `build_event`, and `build_event` can raise.
- [ ] AC-7: Mutation-tested — removing the guard fails AC-1 on all three paths.

### FR-002: A span does not report an error its function did not raise

#### Description:

With FR-001 in place the caller no longer sees the `AttributeError`, but the second half of A2
stands on its own: when a library-internal failure escapes into the decorator's handler, the span
is recorded `status=error` with an `error.type` that names a library exception rather than
anything the user's code did.

That is the `error` field lying about the traced function. An operator reading
`error.type=AttributeError` on `charge()` will look in `charge()`.

#### Acceptance Criteria:

- [ ] AC-1: A span whose function returned normally is recorded `status=ok`, even when a
      library-internal failure occurred during it.
- [ ] AC-2: A span whose function genuinely raised is unchanged — `status=error` and the caller's
      own exception type, which is SPEC-001's contract and must not regress.
- [ ] AC-3: The library-internal failure is still visible, through `_diag` and the FR-001
      counter — this FR moves it out of the `error` field, it does not hide it.
- [ ] AC-4: A test asserts the two cases side by side, since the risk is a fix that makes every
      span read `ok`.

### FR-003: `NaN` and `Infinity` are replaced, not passed through

#### Description:

`sanitize._dispatch` returns `float` unchanged. Every sink then calls `json.dumps`, which happily
writes `NaN`, `Infinity` and `-Infinity` — tokens RFC 8259 does not define. A strict consumer
rejects the whole record.

The precedent is exact and already in the codebase: SPEC-020 faced a value that could not be
*rendered* — an integer past `sys.get_int_max_str_digits()` — and replaced it with
`<int: ~N digits>` rather than clipping it, on the grounds that a wrong number is worse than a
visibly elided one. A non-finite float is the same problem with the same answer: replace with a
marker that says what it was, set the event's `truncated` flag so the substitution is
discoverable, and keep the event.

Applying it in `sanitize` rather than at the sinks is SPEC-017's own decision: one pass at
assembly, correct by consequence for all 40-odd `json.dumps` call sites, and it reaches the
non-JSON sinks too.

#### Acceptance Criteria:

- [ ] AC-1: `info("m", ratio=float("nan"))` produces an event that a strict JSON parser accepts
      — `json.loads(..., parse_constant=<raise>)` does not raise. All four
      `tests/test_promises.py` serialization cells lose their `xfail` markers.
- [ ] AC-2: `nan`, `inf` and `-inf` each become a distinguishable marker, so a reader can tell
      which one was there. Following SPEC-020's shape, e.g. `<float: nan>`.
- [ ] AC-3: The event's `truncated` marker is set, as it is for every other substitution
      `sanitize` makes — otherwise the replacement is itself a silent change to the data.
- [ ] AC-4: Ordinary finite floats are untouched, including `0.0`, negative zero, and values at
      the edge of float precision. A test covers them so the fix cannot over-reach.
- [ ] AC-5: Non-finite floats nested in a mapping, in a sequence, and as a **mapping key** are all
      covered — SPEC-020 had to handle keys separately and this will too.
- [ ] AC-6: `architecture.md` §6 and the README's "field values are coerced and bounded" section
      state the substitution, alongside the integer one it mirrors.
- [ ] AC-7: The hot path cost is not raised measurably for ordinary floats — the check is a
      `math.isfinite` on a value already being dispatched, and a benchmark in the test proves it
      rather than asserting it.

---

## Data Model

No new state and no `Health` field. FR-001's counter is SPEC-036 FR-003's `orphan_lost` where the
call was an orphan; the in-span case needs its own decision recorded in that FR's AC-5.

## API / Interface Contract

No public signature changes. Two behaviour changes a caller can observe:

```python
lf.info(ValueError("x"))              # in a span: was AttributeError, now absorbed
lf.info("m", ratio=float("nan"))      # fields: {"ratio": "<float: nan>"}, truncated: True
```

Both are corrections of documented promises rather than new behaviour, so neither is breaking in
the semver sense — but the second changes what lands in the event stream, and belongs in the
release notes.

## Implementation Phases

### Phase 1: FR-003 — `sanitize`

Self-contained, four `xfail` cells cleared, and no interaction with the others.

### Phase 2: FR-001 — the in-span guard

### Phase 3: FR-002 — the fabricated `error.type`

After FR-001, because FR-001 removes the common way of reaching it and FR-002 must be tested
against what remains.
