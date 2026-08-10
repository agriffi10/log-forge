# Spec: Caller Safety and Serialization

**ID:** SPEC-037  
**Status:** Draft  
**Last Updated:** 2026-08-09  
**Depends On:** SPEC-017, SPEC-020, SPEC-025, SPEC-034 (FR-008 only)

~~**Depends On:** … SPEC-036 — FR-001 AC-5 routes an absorbed **orphan** failure into the
`orphan_lost` counter SPEC-036 FR-003 adds, so it cannot be built before it~~ — struck
(SPEC-021). The coupling was real and it was **load-bearing on the wrong thing**: FR-001's guard
and FR-003's `NaN` replacement are each correct with no counter at all, and neither needs a field
on `Health` to be true. What needed 036 was the *accounting*, which is one AC out of twenty-one.

So this spec builds **before** 036 and its counters follow: FR-001 AC-5 is split, and its second
half (AC-5c) is deferred to 036, which is where `orphan_lost` is invented and where the pair can
be designed together — the thing AC-5 argues for at length and could not have while it ran first.
The dependency on SPEC-034 is FR-008 only, and is the reversed arc order: with `Health` a frozen
dataclass, appending `in_span_lost` costs no index proof.

This is the smallest spec in the arc, both of its fixes are local, and it clears six `xfail`
cells — so it should be **built first of the behaviour work**, which is what decoupling it makes
possible.

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
- [ ] AC-5: The event that could not be built is lost and **announced** — `_diag.absorbed`, per
      AC-2 — not silently dropped. Counting it is AC-5c, and is deferred to SPEC-036 with the
      counters: the guard is what makes the promise true, and it is true with or without a field
      on `Health`. Splitting the two is what let this spec come off 036's critical path.
- [ ] AC-5c: **Deferred to SPEC-036.** When the counters land, an orphan call goes to that spec's
      `orphan_lost` and an in-span call to a **second, appended** field, `in_span_lost`. The alternative is widening `orphan_lost` to mean *events
      lost before reaching the worker* on both paths, and it is rejected on SPEC-026's own test —
      **whether one number would hide which fix applies.** It would: the two counters aggregate
      different failure populations. `orphan_lost` covers everything inside the orphan guard,
      construction and build failures **and a failing `sink.emit`** (036 FR-003 AC-6), because
      that path emits synchronously. The in-span path cannot lose an event at `emit` — that is
      `failed_batches` — so this counter can only ever be an assembly failure. `orphan_lost`
      climbing means *the destination or the data*; `in_span_lost` climbing means *the data*,
      always. Different remediation, so two fields, which is also why SPEC-019, SPEC-026 and
      SPEC-030 each appended rather than overloading a neighbour. **Neither an ordering argument
      nor "a field already published" is offered here**: both drafts are unbuilt, and a name is
      cheap to change until it ships — a justification resting on that would be circular with
      036 FR-003 AC-3, which cites this AC in turn.
- [ ] AC-5a: **Moved to SPEC-036 with AC-5c.** ~~The new field carries the same obligations 036
      FR-003 discharged for `orphan_lost`: appended, indices 0..9 unchanged,
      `test_existing_health_fields_keep_their_positions` gaining `h[10]`, its `len(h)` line having
      moved under 036 FR-003 AC-10…~~ — struck with the ordering (SPEC-021). Under the reversed
      arc `Health` is already a frozen dataclass when either field lands, so there is no index to
      prove, no `len(h)` line to relocate and no eleventh *position* to pin — only a name and a
      docstring. What survives the move and must not be lost with it: `Health`'s `Attributes:`
      block documents the field, `tests/conftest.py`'s reset fixture clears it, and if the counter
      takes its own lock, SPEC-039's derived fork roster picks it up — derived precisely so a lock
      added later needs no edit there.
- [ ] AC-5b: The two are asserted **separately, and neither absorbs the other** — the phrasing
      036 FR-003 AC-4 already uses for `orphan_lost` against `failed_batches`. Deliberately *not*
      a criterion on their sum: with different failure populations the total is a number nobody
      can act on, and pinning it would teach a later reader they are two halves of one counter,
      which is the reading AC-5 just rejected.
- [ ] AC-6: The stale reasoning in `api._log`'s docstring is replaced with what is actually true:
      the branch calls `build_event`, and `build_event` can raise.
- [ ] AC-7: Mutation-tested — removing the guard fails AC-1 on all three paths.

### FR-002: A span does not report an error its function did not raise

#### Description:

With FR-001 in place the caller no longer sees the `AttributeError` — and that closes the *only
known* route to this, which is why this FR is scoped down to an assertion rather than a mechanism.

The decorator's wrapper catches `BaseException` from `fn(...)` and **cannot distinguish** a
library-internal exception from a user one; it has no marker to key on. Inventing one — tagging
library exceptions so the wrapper can re-classify them — would be a real mechanism with real cost,
and it would guard against a case that, after FR-001, has no reachable path.

So this FR asserts the property and does not build for it. If a later audit finds a second route
into the wrapper from library code, this FR is where the mechanism gets specified, and the ACs
below are what will already be in place to catch it.

#### Acceptance Criteria:

- [ ] AC-1: A span whose function returned normally is recorded `status=ok` even though a
      library-internal failure occurred during it — asserted through FR-001's own case
      (`info(ValueError(...))` in a span), which after FR-001 is absorbed. This is the assertion
      that FR-001 fixed both halves of A2, not a second mechanism.
- [ ] AC-2: A span whose function genuinely raised is unchanged — `status=error` and the caller's
      own exception type, which is SPEC-001's contract and must not regress.
- [ ] AC-3: The library-internal failure is still visible, through `_diag` and the FR-001
      counter — this FR moves it out of the `error` field, it does not hide it.
- [ ] AC-4: A test asserts the two cases side by side, since the risk is a fix that makes every
      span read `ok`.
- [ ] AC-5: The FR records that no other route from library code into the wrapper is known, and
      that finding one reopens this FR with a mechanism rather than an assertion.

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

**No `Health` field is added here.** The counters are AC-5c and land with SPEC-036, against a
`Health` that SPEC-034 FR-008 has already converted to a frozen dataclass:

```python
# src/log_foundry/worker.py — in SPEC-036, not here
@dataclass(frozen=True)
class Health:
    ...
    orphan_lost: int = 0      # SPEC-036 FR-003
    in_span_lost: int = 0     # SPEC-037 AC-5c, added in the same spec so the pair is designed once
```

## API / Interface Contract

No public signature changes. Three behaviour changes a caller can observe:

```python
lf.info(ValueError("x"))              # in a span: was AttributeError, now absorbed
lf.info("m", ratio=float("nan"))      # fields: {"ratio": "<float: nan>"}, truncated: True
```

~~`len(lf.health())` 10 -> 11~~ — struck: no field is added here, and by the time one is,
`Health` is a dataclass with no `len()` at all. Both remaining changes are corrections of
documented promises rather than new behaviour, so neither is breaking in the semver sense — but
the second changes what lands in the event stream, and belongs in the release notes.

## Implementation Phases

### Phase 1: FR-003 — `sanitize`

Self-contained, four `xfail` cells cleared, and no interaction with the others.

### Phase 2: FR-001 — the in-span guard

### Phase 3: FR-002 — the fabricated `error.type`

After FR-001, because FR-001 removes the common way of reaching it and FR-002 must be tested
against what remains.
