# Spec: Integer Value Bounds

**ID:** SPEC-020  
**Status:** Completed  
**Last Updated:** 2026-07-29  
**Depends On:** SPEC-017

## Overview

SPEC-017 made an event safe by construction: every value is coerced and size-bounded once at
assembly, so no sink can be handed a payload JSON refuses. One type slipped through — Python's
arbitrary-precision `int`, which `sanitize` returns untouched.

That is not merely untidy. CPython 3.11+ refuses to convert an integer of more than
`sys.get_int_max_str_digits()` decimal digits — **4300 by default** — and raises `ValueError`
instead. `json.dumps` inherits the limit. So an event carrying such an integer is exactly the
JSON-hostile payload SPEC-017 exists to prevent, and it fails in the two ways SPEC-017 FR-001 was
written to remove:

- **On the orphan path** — `info(...)` with no active span emits synchronously on the caller's
  thread — the `ValueError` is raised **into the calling application**. Verified against the current
  build: `log_foundry.info("m", n=10**5000)` raises.
- **Inside a span**, the same value reaches the sink on the worker thread, where `json.dumps` raises
  and the retry loop abandons the whole flattened batch — taking co-batched events from unrelated
  spans with it.

The threshold is far lower than the size of the numbers usually imagined here. 4300 digits is about
1.8 KB; `int.from_bytes(blob, "big")` over a small binary payload clears it, as does an RSA modulus
past ~14000 bits. This spec bounds `int` the way every other value is already bounded, closing the
last hole in the guarantee.

## Scope

### In Scope

- Bounding `int` in `sanitize.py`, wherever an integer can reach an event — including nested in
  mappings and sequences, and as an `Enum` member's `.value`.
- A detection path that cannot itself raise on the values it is meant to catch.
- The replacement representation for an over-long integer, and the `truncated` marker that flags it.
- Unit tests, including a regression test that the orphan path no longer raises.

### Out of Scope

- **`float`, `Decimal`, `bool`, and every other scalar.** `float` is capped at ~24 characters by
  IEEE-754, `Decimal` already routes through `text()` and is bounded, `bool` is trivially small.
  `int` is the only numeric type with no ceiling, and the only one `json.dumps` can refuse.
- **Raising or lowering `sys.set_int_max_str_digits`.** It is process-global interpreter state that
  a *library* must not mutate on its applications' behalf; the ceiling here is the library's own
  config, applied to the library's own payloads.
- **A new config key.** `max_value_bytes` already means "how large may one value be", and an
  integer's decimal length is directly comparable to it. Adding `max_int_digits` would be a second
  knob for the same question.
- **Byte-based bounds on the whole event.** Still out of scope, as SPEC-017 left it — the ceilings
  bound per value, not per event.
- **Changing what `json.dumps` does anywhere in `sinks/`.** All 40+ call sites stay bare, correct by
  consequence of assembly-time sanitization. Fixing this in the sinks would be the per-destination
  duplication SPEC-017 deliberately avoided.

---

## Functional Requirements

### FR-001: An integer too long to render is replaced, not passed through

#### Description:

`sanitize` must not emit an integer whose decimal representation exceeds the value ceiling. Such an
integer is replaced by a placeholder that names the type and the magnitude, in the same shape as the
existing `<unserializable: …>` fallback, and the event's `truncated` marker is set.

The replacement is a string where a number stood. That is a deliberate, and precedented, trade: the
alternative is dropping digits, which silently changes the value — a wrong number is worse than a
visibly elided one — and SPEC-017 already established that a value which cannot be represented
becomes a type-naming placeholder. Consumers with a strict numeric mapping on that field may reject
the document; they reject *one* document, where today the whole batch is lost or the caller's thread
raises.

The effective ceiling is the smaller of `max_value_bytes` and the interpreter's own conversion
limit. A `max_value_bytes` above `sys.get_int_max_str_digits()` cannot be honoured — rendering such
an integer is exactly what raises — so the interpreter's limit wins whenever it is lower.

#### Acceptance Criteria:

- [ ] An `int` whose decimal length is within the effective ceiling is returned **unchanged and
      still an `int`** — the overwhelmingly common case must not become a string or lose precision.
- [ ] An `int` whose decimal length exceeds the effective ceiling is replaced by a placeholder
      string naming the type and the approximate digit count.
- [ ] Negative integers are bounded identically; the sign does not consume the budget in a way that
      makes the two directions disagree.
- [ ] `bool` is unaffected — it is an `int` subclass, and `True` must stay `True`, not become `1` or
      a placeholder.
- [ ] The replacement sets the same `truncated` flag that a clipped string sets, so an event carrying
      one is marked exactly as any other ceiling-hit event is.
- [ ] `json.dumps` succeeds on any event containing an integer field, at any magnitude, with the
      default configuration.

### FR-002: The check cannot raise on the values it exists to catch

#### Description:

The size test must not be `len(str(value))`. `str()` on an over-long integer raises the very
`ValueError` this spec removes, so a naive check would move the crash rather than fix it.

Detection uses `int.bit_length()`, which is O(1) and total, converting to a decimal-digit bound
arithmetically. The digit count reported in the placeholder is derived the same way and is therefore
approximate — stated as such rather than presented as exact.

#### Acceptance Criteria:

- [ ] `coerce` and `sanitize_fields` are total for every `int`, including one far beyond the
      interpreter's conversion limit — no `ValueError` escapes, at any magnitude.
- [ ] `str()` is never called on an integer before its length has been shown to be within the
      effective ceiling.
- [ ] The bound derived from `bit_length()` never admits an integer that `str()` would then refuse:
      where the arithmetic is inexact it errs toward replacing, never toward passing through.

### FR-003: The hole is closed everywhere an integer can reach an event

#### Description:

An integer reaches an event as a field value, nested inside a mapping or sequence at any depth, or
as an `Enum` member's `.value` — which `sanitize` currently returns raw for plain scalars. All of
these route through the same bounding rule.

#### Acceptance Criteria:

- [ ] A bare field value is bounded.
- [ ] An integer nested inside a mapping, a list, a tuple, and a set is bounded.
- [ ] An `Enum` whose `.value` is an over-long integer is bounded rather than returned raw.
- [ ] Exactly one implementation of the rule exists in `sanitize.py`; no call site repeats it.

### FR-004: The orphan path no longer raises into the caller

#### Description:

The application-facing symptom is the reason this is a defect rather than a rough edge, and it gets
a test of its own — the same shape as the SPEC-017 FR-001 regression tests, which cover unserializable
values but not this.

#### Acceptance Criteria:

- [ ] `log_foundry.info("m", n=10**5000)` with no active span returns normally and emits an event.
- [ ] The same call inside a span emits a batch the sink accepts, and does not abandon the batch.
- [ ] A co-batched event from an unrelated span is delivered intact alongside it.

---

## Data Model

No new types. The coercion table gains one rule:

```python
# src/log_foundry/sanitize.py — conceptually, inside _Coercer.value

# int (and int subclasses that are not bool):
#   decimal length within the effective ceiling -> the int, unchanged
#   otherwise                                   -> "<int: ~N digits>", truncated = True
#
# effective ceiling = min(cfg.max_value_bytes, sys.get_int_max_str_digits())
# detection         = value.bit_length(), never len(str(value))
```

The placeholder mirrors the existing `<unserializable: TypeName>` shape so an operator meets one
elision convention, not two.

---

## API / Interface Contract

No public signature changes. `coerce`, `sanitize_fields`, `configure`, and the event schema are all
unchanged; the only visible difference is which values survive the pass.

```python
import log_foundry as lf

lf.info("payment", amount=4200)            # unchanged: still the integer 4200
lf.info("blob", n=int.from_bytes(b, "big"))  # "<int: ~4820 digits>", event.truncated = True
```

## Configuration / Environment

None. `max_value_bytes` (default 8192) is reused as the ceiling; no new key, environment variable,
or constructor argument. Note that with the default configuration the interpreter's 4300-digit limit
is the binding constraint, not `max_value_bytes`.

## File & Folder Structure

```
src/log_foundry/
└── sanitize.py         # modified — the int rule + the bit_length-based test

tests/
├── test_sanitize.py    # modified — the rule, directly
└── test_api.py         # modified — the orphan-path regression (FR-004)

README.md               # modified — the coercion table's int row
```

## Implementation Phases

### Phase 1: The bounding rule

- Add the integer rule to `_Coercer.value`, ahead of the existing plain-scalar and `int`/`float`
  branches, and to the `Enum` member path.
- Derive the effective ceiling and the `bit_length()` bound; make sure the inexact direction errs
  toward replacing.
- Extend `tests/test_sanitize.py`: in-range ints unchanged and still `int`, over-long ints replaced,
  negatives, `bool` untouched, nesting in mapping/list/tuple/set, an int-valued `Enum`, the
  `truncated` flag, and totality at absurd magnitudes.

### Phase 2: The application-facing regression and docs

- Add the FR-004 tests: the orphan path returns normally, the in-span path delivers, and a
  co-batched event from another span survives.
- README: the coercion table's `int` row, noting the interpreter limit is the binding constraint by
  default.
