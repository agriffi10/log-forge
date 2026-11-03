---
type: Spec
id: SPEC-065
title: "configure() Refuses a Ceiling That Is Not an int"
description: "Makes configure() refuse a non-int payload ceiling at the call and store an accepted one as an exact int, so no float, bool or spoofing subclass reaches sanitize."
spec_status: Draft
depends_on: [SPEC-017, SPEC-034, SPEC-049, SPEC-055, SPEC-057]
---

## Overview

`configure()` takes four payload ceilings — `max_value_bytes`, `max_stack_bytes`, `max_keys` and
`max_depth` — typed `int | None`, and checks them with `config._require_positive`, which tests only
`value is not None and value < 1`. Anything that is not less than 1 passes and is stored as given,
and `sanitize` then does integer arithmetic on it on every event. `docs/architecture.md` §12 records
the defect (*`configure()` accepts a ceiling that is not an `int`*) and defers its closing to this
spec; invariant 13 is the promise it breaks, and invariant 8's *every configured ceiling holds
exactly* is the one it breaks downstream.

Every figure below is anchored to `71e42dd`, the tree this spec was drafted on, and was read by
running `configure()` and then `info("x", s="a" * 200, b=b"ab", l=[1, 2, 3, 4],
d={"a": {"b": {"c": 1}}})` into a `MemorySink`, for each of the four ceilings unless a ceiling is
named.

- **A float is accepted and stored as a float.** `2.5`, `nan` and `inf` pass for all four. With
  `max_value_bytes=2.5` the event's `fields` came out `{}`; with `nan` the event was lost, stderr
  reading "absorbed a failure while emitting an orphan log (TypeError)"; with `inf` the 200-byte
  string was written whole, unbounded, and `b"ab"` became `<unserializable: bytes>`. `max_keys=2.5`
  kept three of the four list entries, and `max_keys` or `max_depth` of `inf` or `nan` are
  effectively unbounded.
- **A `str` is refused only by accident.** `"5"` raises the comparison's own
  `TypeError: '<' not supported between instances of 'str' and 'int'`, which names neither the
  ceiling nor the type that was wrong.
- **A `bool` and a plain `int` subclass are stored as themselves and work as the int they hold.**
  `max_keys=True` and `max_depth=True` behave exactly as `1`, `max_value_bytes=True` as `1`, and
  `Sub(3)` for `class Sub(int)` as `3`; `get_config().max_keys` reports `True`.
- **A subclass that overrides the comparison reaches the zero ceiling SPEC-034 closed.**
  `class Spoof(int)` with `__lt__` returning `False` and an underlying value of 0 is accepted and
  stored, so a ceiling of 0 empties every event it touches — the defect `Config`'s docstring says
  SPEC-034 closed. A `Spoof(3)` that also overrides `__add__` and `__sub__` made `fields` come out
  `{}` for `max_value_bytes`.
- **The unbound accessor reads the int's own storage.** `int.__index__(x)` returns an exact `int`
  for any `int` instance and ignores a subclass's `__index__` and `__int__`: `int.__index__(Spoof(7))`
  is `7` of type `int`, and `int.__index__(True)` is `1` of type `int`.

The rule applied is the settled one in
`docs/decisions/sink-delivery/a-bad-argument-is-floored-if-it-works-and-refused-if-it-is-already-broken-a-new-one-is-refused.md`,
which this spec refines on one point rather than reverses. That decision floors a value of the
declared type that still works and refuses one that is already broken; its precedents refuse a
value *outside* the declared type — SPEC-049's `require_timeout` refused `None`, "a runtime-working,
mypy-invalid 0.x spelling" — and SPEC-055's `_require_text` refuses a non-`str` stamp and stores
`str.__str__(value)`. So the test "does it work" is asked of values of the declared type. A value
outside `int | None` — any `float`, a `str`, anything that is not an `int` instance — is refused at
the door even where it happened to work, and is **not** floored (no `math.ceil`), because that would
hide a caller's mistake behind a ceiling they never chose. A `bool` or other `int` subclass *is* of
the declared type and works like the int it holds, so it is accepted and stored as that exact
`int`, mirroring `_require_text`.

## Scope

### In Scope

- `configure()` refuses a non-`int` ceiling with a `TypeError` naming the ceiling and the type
  (FR-001).
- An accepted `int` instance — `bool` or subclass — is stored as an exact `int`, and the `< 1`
  test runs on that value (FR-002).
- Every new refusal precedes every assignment and the ownership stamp (FR-003).

### Out of Scope

- **Any file but `src/log_foundry/config.py` and `tests/test_config.py`, besides docs.**
  `sanitize.py`, `model.py` and `api.py` belong to SPEC-063, building in a parallel session; the
  fix needs none of them, because a ceiling that is an exact `int` on arrival is what they already
  assume.
- **The sink-side siblings, swept and unchanged.** `_retry.require_positive` already refuses `2.5`,
  `nan` and `inf` with a named `TypeError` and accepts `True` and `Sub(3)`, which work as 1 and 3;
  `_retry.require_timeout` accepts `2.5` (a valid duration) and refuses `nan` and `inf` with
  `ValueError`; `usable_timeout` floors `nan` and `inf` to the default. Those are the decision's
  settled behaviour on the floor side, and the pins in `tests/test_sink_retry.py` exist to fail a
  change to them.
- **`flush(timeout=)` and `shutdown(timeout=)`.** They are not constructors and validate nothing;
  nothing here adds a check to them.
- **Validating `Config` on direct construction.** `Config` is a frozen dataclass with no
  `__post_init__`, and it is in `log_foundry.__all__`, so `Config(max_keys=2.5)` constructs. Known
  residue, not widened: nothing in the library accepts a `Config` as input — `_rebind` copies the
  module global with `replace()` and `configure()` is the only route to a change, as `Config`'s
  docstring says — so a hand-built one reaches no event. Adding `__post_init__` would also tighten
  the frozen public surface (invariant 10) for no path that matters.
- **Flooring a float**, by `math.ceil`, `int()` or otherwise. See the Overview: it hides the
  mistake.
- **A maximum on a ceiling.** `max_value_bytes=10**30` is an `int` and stays accepted; invariant 8's
  pass budgets bound the work, not this check.

### Twin paths (invariant 6)

- **The four ceilings.** All four go through one helper and are swept in every criterion, so a
  fifth ceiling cannot be left on the old check.
- **`_require_text` and the ceiling helper.** Both refuse a wrong type with the same message shape
  and store the exact builtin; a stamp and a ceiling are the same promise at the same door.
- **Worker and orphan paths.** Both read the stored config through `_live_config`, so a ceiling
  that is an exact `int` on arrival serves both with no change to either.

---

## Functional Requirements

### FR-001: A ceiling that is not an int is refused, naming it

#### Description:

`configure()` raises `TypeError` for any of the four ceilings whose value is neither `None` nor an
`int` instance. The message follows `_require_text`: `f"{name} must be an int, got
{type(value).__name__}"`. `None` still means *not supplied*. The ceiling is not floored,
converted or coerced: the stored config is untouched.

#### Acceptance Criteria:

- [ ] For each of the four ceilings and each of `2.5`, `100.0`, `float("nan")`, `float("inf")`,
      `"5"` and `object()`, `configure(<ceiling>=value)` raises `TypeError`, and the message
      contains the ceiling's name and the value's type name (`float`, `str`, `object`) — asserted
      on the message, since the comparison's own `TypeError` for `"5"` names neither.
- [ ] `100.0` is refused, not floored to `100`: it is of integral value and outside the declared
      type, and `get_config()` afterwards still reports the previous ceiling.
- [ ] `configure(max_keys=None)` and a call naming no ceiling leave every ceiling as it was, and
      `1` and `sys.maxsize` are accepted for all four and stored equal to what was passed.
- [ ] `0` and `-1` still raise `ValueError` naming the ceiling, as the existing pin
      `test_a_non_positive_ceiling_is_rejected` asserts.
- [ ] Serves invariant 13 (a bad argument is refused where it is written, never later on the drain
      thread), and invariant 8 (every configured ceiling holds exactly), which a float ceiling
      breaks at assembly.

### FR-002: An accepted int is stored as an exact int, and the floor test runs on it

#### Description:

A ceiling that is an `int` instance — `bool` or any subclass — is of the declared type and works
like the int it holds, so it is accepted. It is normalised with `int.__index__(value)`, which
reads the integer from the int's own storage and ignores a subclass's `__index__`, `__int__`,
`__lt__`, `__add__` and `__sub__`. The `< 1` test, the message of the existing `ValueError` and
the stored value all use that exact `int`. So `get_config().max_keys` reports `1` for `True`, and a
subclass that overrides arithmetic or comparison never reaches `sanitize`.

#### Acceptance Criteria:

- [ ] For each of the four ceilings, `configure(<ceiling>=True)` stores `1`, and
      `type(getattr(get_config(), <ceiling>)) is int` holds, so `True` is not stored as a `bool`.
- [ ] For each ceiling, a `class Sub(int)` instance with value 3 is stored as an exact `int` equal
      to 3, and an `IntEnum` member is stored as the exact `int` it holds.
- [ ] A `Spoof(3)` whose `__lt__`, `__add__` and `__sub__` are overridden to misbehave, passed as
      `max_value_bytes`, produces the same `MemorySink` event for the probe in the Overview as
      `max_value_bytes=3` does — where at `71e42dd` its `fields` came out `{}`.
- [ ] `False`, `Sub(0)` and a `Spoof(0)` whose `__lt__` returns `False` each raise `ValueError`
      naming the ceiling and reporting `0`, and the ceiling is unchanged. The message is built from
      the exact `int`, so a subclass whose `__repr__` raises still yields the `ValueError` and no
      other exception.
- [ ] `True` as `max_keys` produces the same event as `max_keys=1` for the probe in the Overview —
      the floor side is protected, not turned into a refusal.
- [ ] Serves invariant 13 and invariant 8, whose *every configured ceiling holds exactly* is
      unreachable while a subclass can redefine what the ceiling means.

### FR-003: A refused call changes nothing

#### Description:

`configure()` already promises that every ceiling and every stamp is validated before anything is
assigned and before the sink is stamped into the ownership record, so a rejected call leaves both
exactly as it found them. The new refusals and the normalisation take part in that promise: all
four ceilings are validated, and their exact-`int` values held in locals, ahead of the first
assignment, with the order of the existing checks otherwise unchanged.

#### Acceptance Criteria:

- [ ] After `configure(service="before", max_keys=7)`, the call
      `configure(service="new", max_keys=2.5)` raises `TypeError`, and `get_config()` equals the
      earlier snapshot field for field — `service` is still `"before"`, `max_keys` still `7`.
- [ ] A sink passed with a refused ceiling, `configure(sink=s, max_depth=float("nan"))`, raises
      `TypeError` and `id(s)` is not in `_lifecycle._owned`, as `test_a_rejected_stamp_leaves_the_config_and_the_sink_unstamped`
      asserts for a stamp.
- [ ] When two ceilings are bad in one call, one of them raising `TypeError` and the other a
      non-positive `ValueError`, one exception is raised and `get_config()` is unchanged.
- [ ] With a worker already running on sink `a`, `configure(sink=b, max_keys=2.5)` raises
      `TypeError`, and afterwards `get_config().sink is a`, `a` has not been closed and `b` has
      received no event.
- [ ] Serves invariant 13 and invariant 5, whose *ownership is recorded when the library is handed
      a sink* must not record a sink the call then refuses.

---

## Data Model

```
// src/log_foundry/config.py
def _require_positive(name: str, value: object) -> int | None
    // None            -> None                       (not supplied)
    // not an int      -> raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    // int instance    -> exact = int.__index__(value)
    //                    exact < 1 -> raise ValueError(f"{name} must be >= 1, got {exact}")
    //                    else      -> exact          (type(exact) is int)
```

`Config`'s fields keep their annotations, `int`. `configure()`'s signature is unchanged
(`max_*: int | None = None`), so no public name moves (invariant 10).

## File & Folder Structure

```
src/log_foundry/config.py     # _require_positive returns the exact int; configure() uses it
tests/test_config.py          # the matrix and pins for FR-001 to FR-003
```

## Implementation Phases

### Phase 1: The check and its tests

- Change `_require_positive` as in the Data Model: `isinstance` test first, then
  `int.__index__`, then the `< 1` test on the result; return the exact int or `None`.
- In `configure()`, assign the four results to locals ahead of `_require_text`, and use the
  locals in `changed`.
- Add to `tests/test_config.py` the four-ceiling by six-value refusal matrix, the `True`/`Sub`/
  `IntEnum` storage pins, the `Spoof` probe (run against `71e42dd`'s `_require_positive` first, to
  watch it redden), and the atomicity pins beside `test_a_rejected_call_leaves_the_config_untouched`.

### Phase 2: Documentation

- `configure()`'s `Raises:` names the new `TypeError` for a non-`int` ceiling, and its `Args:` say
  an `int` subclass is stored as the exact `int`.
- Move the §12 item *`configure()` accepts a ceiling that is not an `int`* to `### Resolved` as
  closed by SPEC-065.
- Refine the decision file named in the Overview with a dated History entry (2026-11-03): the
  floor-or-refuse test is asked of values of the declared type, a value outside it is refused at
  the door and never floored, and a `bool` or other `int` subclass is floor-side and stored as an
  exact `int`.
- Add a section to `docs/release-notes/v1.1.0.md`: a non-`int` ceiling is now a `TypeError` at
  `configure()` — a visible behaviour change for a caller who passed `2.5`, `100.0` or `inf`, which
  previously ran with the damage measured above.
