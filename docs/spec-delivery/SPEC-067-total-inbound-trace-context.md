# Completed Spec — SPEC-067: Total Inbound Trace Context — a Bounded Echo and Parsers That Run No Code of the Value

## What was completed?

Five FRs, built as `887b263..556606f`, in `_diag.py`, `ids.py`, `context.py` and `decorator.py`.
The principle: **no code of an inbound value runs inside the library, and a refusal is always
announced, in bounded form.** It closes the `architecture.md` §12 item on `_diag.rejected`'s echo,
and the parser half the item did not list.

- **The echo runs no code of the value** (FR-001): `rejected` computes its echo through the new
  private `_diag._echo`. A `str`, exact or a subclass, is its first 64 characters through `str`'s
  own slots, quoted, escaped and cut; anything else is `<TypeName>` through `type`'s own
  `__name__` descriptor, sliced at 65 so a long name is cut with a `…`; an `Exception` from
  computing it writes the line with `<?>`. `bytes` is now echoed `<bytes>`. Guarded by
  `tests/test_diag.py` (the metaclass, class-property, hostile-name, 64/65 boundary, non-`str`,
  hostile-subclass, short-literal, long-`str` and `<?>` tests, and the two inverted SPEC-029
  tests).
- **Its cost does not follow the value** (FR-002): `tests/test_inbound_totality.py` compares
  `rejected`'s CPU against `repr` of the same 10^8-character `str` and `bytes`, bounds peak
  `tracemalloc` at 64 KiB for an exact `str`, a plain subclass and `bytes`, and returns from the
  shared-sublist DAG inside a 5 s deadline.
- **The parsers accept only an exact `str`** (FR-003): `parse_traceparent`, `is_valid_trace_id`,
  `is_valid_span_id` and `parse_baggage_header` test `type(x) is str` instead of `isinstance`.
  Guarded by the parametrised table over all four in `tests/test_ids.py`.
- **`continue_trace` normalises once and is total** (FR-004): the new private `decorator._exact`
  turns a `str` subclass into its exact `str` through `str.__str__` on entry, for all four
  arguments. Guarded by `tests/test_inbound_totality.py`: the probe-by-argument rejection table,
  one adoption test per argument, the Probe G `current_traceparent()` test, the swept-span refusal
  and the stream-fault verdict tests. Three guards keep the entry point from regrowing a path
  around that normalisation:
  - The table's single-argument columns are held equal to `continue_trace`'s signature.
  - An AST check, `_normalisation_problems`, requires the body to open with one
    `p = cast(…, _exact(p))` per parameter. A fixture corpus asserts the reason it gives for each
    failure shape: a missing parameter, a late reassignment, the wrong name and a duplicate.
  - Two columns pair a valid `traceparent` with a hostile `trace_id` or `parent_span_id`. Each
    asserts the header is adopted, one "traceparent wins" line is written and no hook runs.

  Each guard was shown to redden under a mutant that left the rest of the suite green.
- **The prose** (FR-005): docstrings, the rewritten sentence in the failure-paths decision on
  `_diag`, superseded markers at SPEC-029's three spec sites and its delivery doc, and a
  `docs/release-notes/v1.1.0.md` entry. The probes the three test files share (`str_probe`,
  `object_probe`, `armed`, `fan_out`) live in `tests/conftest.py`.

Decision: `docs/decisions/failure-paths/an-inbound-value-runs-no-code-of-its-own-anywhere-on-the-inbound-path.md`.

## Measured base figures

From the spec, anchored as it anchors them. At `2e790e9`, through `continue_trace`: a
shared-sublist DAG (`[y] * 16` seven times deep) passed as `traceparent` or `baggage` did not
return in 3 s (2.97 s of CPU, no line written); a value whose `__repr__` raised returned
`reason="rejected"` with no line; a 50,000,000-character `str` cost 0.10 to 0.11 s of CPU inside
`rejected`. At `7dc881f`: a valid `str` subclass was adopted as itself and `current_traceparent()`
then raised its `RuntimeError`; on a 10^8-character `str` subclass the slot slice peaks at 105 bytes
of traced memory and a full copy at 100,000,041 (`str.__str__` of it alone).

## What changed from earlier specs?

- **SPEC-014 / SPEC-029**: `rejected`'s echo is no longer a `repr` of the value; an exact `str` of at
  most 64 characters is echoed byte-identically, a longer one may quote differently, and a non-`str`
  is named by type. SPEC-029's spec and delivery doc carry superseded markers.
- **SPEC-014**: the four parsers refuse a `str` subclass called directly, and `continue_trace` stores
  an adopted subclass as its exact `str`.
- **SPEC-064**: its release-notes sentence on `continue_trace` and `parse_baggage_header` is
  qualified to an exact `str` header.

## What changed for a caller

In `docs/release-notes/v1.1.0.md`, *Fixed*: the type-name echo, the long-`str` quoting, the
parsers' exact-`str` rule and the exact-`str` adoption. No public name or signature changed.

## Follow-ups

- **Owed until the PR's CI is green:** the `str.__str__`, `str.__getitem__` and
  `type.__dict__["__name__"]` semantics the change relies on, and the `isinstance` fallback to
  `__class__` the tests probe, are CPython behaviour; this worktree's interpreter is 3.13, so CI's
  3.12 leg is what settles them on 3.12.
- Left open, outside this spec: the spoofed-scalar routes on the assembly path (`architecture.md`
  §12, *An object that only claims a scalar type*), the same `isinstance`-reads-`__class__` class
  of defect; and the parsers' linear cost on a very long exact `str`, which the spec bounds only
  for the echo.

## Verification

The tests named above; the gate results are recorded in the PR.
