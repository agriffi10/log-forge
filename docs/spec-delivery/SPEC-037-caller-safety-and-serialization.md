# Completed Spec — SPEC-037: Caller Safety and Serialization

## What was completed?

- **FR-003** — `sanitize._Coercer.real()` replaces a non-finite float with `<float: nan>` /
  `<float: inf>` / `<float: -inf>` and sets `truncated`, mirroring SPEC-020's integer rule.
  Covers the value, nested mappings, sequences, **mapping keys**, `float` subclasses and
  float-valued `Enum` members.
- **FR-001** — `api._log`'s in-span branch is guarded. `info(exc)` returned normally on the
  orphan path and killed the decorated function inside a span; both paths now absorb and
  announce by type.
- **FR-002** — asserted, not built, as the FR specifies: with FR-001 in place the only known
  route to a fabricated `error.type` is closed, and the two cases are tested side by side.
- Six `xfail` cells cleared in `tests/test_promises.py` (four serialization, two caller-safety).
  Suite 1272 → 1301.

**Deviation:** none in substance. Three criteria are marked `[→]` rather than `[x]` —
`in_span_lost` and `orphan_lost` are deferred to SPEC-036, where the pair is designed together;
that split was made when this spec was decoupled from 036, not during the build.

## What changed that a later spec should know?

- **`float` is no longer in `sanitize._PLAIN_SCALARS`.** That set is read by the exact-type path
  *and* the `Enum` branch, so anything adding a scalar rule must check both. Removing float
  without giving `Enum` its own rule sends a float-valued member to `str()`.
- **A non-finite mapping key renders as `<float: nan>`, not `"nan"`.** It went through `str()`
  before — valid JSON, so no sink complained, but it lost the type and set no marker.
- **`api._log`'s in-span branch can no longer raise**, which means SPEC-036 FR-004's
  append-at-a-closed-span routing lands inside a guard that already absorbs.
- **SPEC-036 owes the counters.** `orphan_lost` and `in_span_lost` are both its FR-003 now.

## Anything deliberately left open?

The counters (above). No `Health` field was added here.

## Evidence

Thirteen mutants run, eleven killed. The two survivors are recorded rather than hidden: removing
the exact-type `if kind is float` fast path is behaviourally identical once `float` has left
`_PLAIN_SCALARS` (the `isinstance` arm catches it), and the benchmark's calibrated sensitivity is
an order of magnitude — it survived 5 extra `str()` calls per value and failed at 10, which its
docstring now states rather than implying a tighter bound.

Removing the in-span guard fails **the in-span and async paths**, not all three: the orphan path
has its own guard and cannot fail. AC-1 holds; an earlier version of this sentence overstated the
evidence for it.

AC-7's benchmark is measured **against a neighbour rather than the clock**: an absolute
wall-clock budget is a race with the machine, so the comparison is against `integer()`, which
does `bit_length()`, two multiplications, a division and an unbound `int.__lt__` where this does
one `math.isfinite`.

Audit A2 reproduced before the fix and after: `info(ValueError(...))` returned normally on the
orphan path and raised `AttributeError: 'ValueError' object has no attribute 'encode'` into the
caller inside a span — the same call, opposite outcomes. Afterwards the function returns its
value and the span records `status=ok` with no `error` field.
