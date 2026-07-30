# Completed Spec — SPEC-020: Integer Value Bounds

## What was completed?

SPEC-017 promised an event is safe by construction — coerced and bounded once at assembly, so no
sink is handed a payload JSON refuses. `int` was the one type passed through untouched, and CPython
3.11+ refuses to render an integer past `sys.get_int_max_str_digits()` decimal digits (**4300** by
default), raising `ValueError` that `json.dumps` inherits. Both failures SPEC-017 FR-001 removed
were therefore still reachable — through a number instead of an object.

- **`_Coercer.integer`** replaces an over-long integer with `<int: ~N digits>` and sets `truncated`.
  In-range integers are returned unchanged and still `int`: an ID or an amount keeps its type and
  full precision (FR-001).
- **Replaced, not clipped.** Dropping digits silently changes the value, and a wrong number is worse
  than a visibly elided one, so this reuses SPEC-017's type-naming placeholder shape rather than
  inventing a second elision style.
- **The size test is `bit_length()`, never `len(str(v))`** (FR-002) — the obvious check raises the
  very error being fixed. The bit_length→digits ratio is rounded *up*, so the estimate errs toward
  replacing and can never admit an integer `str()` would then refuse.
- **The ceiling is `min(max_value_bytes, sys.get_int_max_str_digits())`**; a configured ceiling above
  the interpreter's cannot be honoured. A limit of `0` (interpreter limit disabled) is handled.
- **Applied at every entry point** (FR-003): the exact-type branch, `int` subclasses, an `Enum`
  member's `.value`, mapping **keys**, and everything nested by recursion.
- **The application-facing failures are regression tests** (FR-004) in `test_api.py`, beside
  SPEC-017's own orphan-path tests.

**Deliberate deviation:** Phase 2 called for "the coercion table's `int` row", but the README had no
coercion table — nor any mention of the ceilings, nor of SPEC-017's `truncated` field, which was
never documented. The minimal section that row needs was added under *Event schema*, along with the
missing `truncated` row. Slightly wider than the spec's letter.

## What changed from earlier specs?

- **SPEC-017's coercion table gains a rule.** An integer field may now arrive as a string. Only ever
  for a value that could not otherwise be rendered at all — but a consumer with a strict numeric
  mapping on that field will reject the document. That costs *one* document, where before it cost
  the caller's thread or a whole batch.
- **SPEC-017's `key()` is fixed, not merely extended.** A non-`str` mapping key was rendered with a
  bare `str()`, which raised on an over-long integer; the failure was caught upstream in `value()`
  and replaced the **whole mapping** with `<unserializable: dict>`, leaving `truncated` at `False`.
  One hostile key destroyed every sibling key, unmarked — the silent collateral loss SPEC-017 and
  SPEC-018 exist to remove. Found by the fresh-context review, not by the original build.

## Notes for the next spec

*Reconciled by SPEC-021 — one of these was a defect it fixed; the rest are settled or constraints.*

- **The bound trusts `bit_length()`.** An `int` subclass overriding it to understate its magnitude
  defeats the check, and nothing raises, so the totality guard never engages. Documented in
  `integer()` as the trust boundary it is; narrowing it would cost the common path a `type()` check
  to catch only a value engineered to lie about itself.
  → **Settled** (SPEC-021, named in its Out of Scope). Worth noting that the *sign* test added by
  SPEC-021 is deliberately **not** inside that boundary: it uses an unbound `int.__lt__`, because a
  `<` on a subclass dispatches to user code that can raise, and a raise there would replace the
  whole enclosing mapping. `bit_length()` remains the one trusted call.
- ~~**The sign is not counted against the byte budget.** `Config(max_value_bytes=10)` admits
  `-10**9`, which renders as 11 bytes. Only reachable at absurdly small configured ceilings.~~
  → **Fixed by SPEC-021 (FR-003).** The ceiling measures rendered length, sign included. It also
  closed a case this note did not notice: at a small ceiling an over-long negative *key* was
  admitted by `integer()` and then clipped by `text()`, so the key rendered as a truncated number
  rather than being named as elided.
- **Over-replacement is bounded and tiny** — the largest admitted value is `2**14284 - 1`, itself a
  full 4300-digit number, so the rule only clips the top ~18% of the final digit band.
  → **Settled** (SPEC-021). Quantifies the `log10(2)` over-estimate as acceptable; not an action.
  SPEC-021 adds one value to it: a negative integer sitting exactly on the interpreter's own limit,
  since that limit counts digits and this ceiling counts rendered length.
- **`max_value_bytes` now means two things** — UTF-8 bytes for a string, decimal digits for an
  integer. They coincide for ASCII digits, but a future ceiling change should keep that in mind.
  → **Documented by SPEC-021 (FR-004)**, in `Config` and the README, rather than renamed or split
  — a new or renamed config key would be a breaking change for a cosmetic gain.
- **The ceilings still bound per *value*, not per event.** Unchanged from SPEC-017 and still out of
  scope.
  → **Constraint** (SPEC-021), now stated in `architecture.md` §13. See SPEC-017's note.

## Verification

Local: 547 tests pass, `ruff check` clean, `mypy --strict` clean, `spec-lint` clean. CI green on
3.12 and 3.13 (PR [#66](https://github.com/agriffi10/log-forge/pull/66)). A fresh-context review
verified the arithmetic independently — checking `2**(b-1)`, `2**b - 1` and a midpoint against
`len(str(...))` for every bit-length to 40000, with zero under-estimates — and found the `key()`
path above, which was fixed before merge. It also showed the FR-004 co-batch test could not fail
(the fixture flushes per span, so the two spans were never co-batched); it now drives a real
`Worker` and was confirmed to fail against the old behaviour, where the batch is abandoned and the
unrelated span's events die with it.
