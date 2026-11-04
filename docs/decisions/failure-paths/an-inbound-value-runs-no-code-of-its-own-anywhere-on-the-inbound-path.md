---
type: Decision
title: "An inbound value runs no code of its own anywhere on the inbound path"
description: "the parsers accept only `type(x) is str`, never `isinstance`, which reads `__class__`; `continue_trace` turns a `str` subclass into its exact `str` once, on entry, through `str.__str__`; `rejected` echoes a `str` by its first 64 characters through `str`'s own slots, anything else as `<TypeName>` through `type`'s descriptor, `<?>` if that fails. (SPEC-067)"
---

**An inbound value runs no code of its own anywhere on the inbound path** — `continue_trace`, the
four parsers it calls (`ids.parse_traceparent`, `ids.is_valid_trace_id`, `ids.is_valid_span_id`,
`context.parse_baggage_header`) and `_diag.rejected`, which announces a refusal. The values come off
the wire and are `str` only by annotation, which a caller not running mypy is free to ignore, so a
method, property or metaclass hook of the value is the caller's code running inside the library's
promise never to fail the caller (invariant 1), and inside a diagnostic whose words must be true
(invariant 11).

- **The parsers accept only an exact `str`**, tested as `type(x) is str`, and return `None` or
  `False` for anything else without touching it. `isinstance(x, str)` is not used on an inbound
  value anywhere on this path, because CPython falls back to reading the value's `__class__` when
  the real type check fails and propagates anything but `AttributeError` from it: a raising
  `__class__` property raised into the caller, and one answering `str` passed the check and then
  had `.strip()` or `len()` called on a non-`str`. A subclass that passes the check runs its own
  `strip`, `split`, `encode`, `__len__`, `__iter__` and `__ne__` in the parse.
- **`continue_trace` normalises each of its four arguments once, on entry** (`decorator._exact`): a
  value whose real type is a `str` subclass, tested as `issubclass(type(x), str)` (an MRO check that
  consults `str`'s metaclass, never the value's), becomes the exact `str` it holds through
  `str.__str__`, a C-level copy that calls no method of the subclass; anything else is passed on
  untouched, to be refused and announced. So a valid header in a `str` subclass is still adopted,
  and what is stored, put on events and later formatted by `current_traceparent()` is the exact
  `str`. Adopting the subclass as itself is what let `current_traceparent()` raise its
  `__format__` into the caller later, from a validated id, because a valid subclass is valid.
  The parameters are reassigned through `cast`, not rebound to new names, so no later line can
  read an argument the normalisation skipped.
- **`rejected` decides the route from the real type and reads only through `str`'s and `type`'s
  own slots** (`_diag._echo`). A `str`, exact or a subclass, is its first 64 characters taken with
  `str.__getitem__`, quoted by `str.__repr__`, escaped and cut. Anything else is `<TypeName>`,
  the name read with `type.__dict__["__name__"].__get__(type(x))`, which a metaclass `__name__`
  property or `__getattribute__` cannot divert; the name is sliced at 65 so that a printable name
  longer than 64 is cut with a `…` rather than silently, and escaped, because a class's
  `__name__` may be a `str` subclass, any length, and hold a newline. If computing the echo raises
  an `Exception`, the line is written with `<?>`: `"rejected"` from `continue_trace` means the
  refusal was announced, so the line may not depend on the value. A `BaseException` still
  propagates (SPEC-029). The diagnostic channel's own rules are the sibling decision *One module
  writes every diagnostic, so the rules are applied once rather than remembered twenty-eight
  times*; this entry is the inbound path's reasoning, and that one does not repeat it.

The work is therefore independent of the value's size and shape. Measured through
`continue_trace` at `2e790e9`: a shared-sublist list (`[y] * 16` seven times deep) did not return
in 3 s, and a 50,000,000-character `str` cost 0.10 to 0.11 s of CPU inside `rejected`; on a
10^8-character `str` subclass at `7dc881f` the slot slice peaks at 105 bytes of traced memory
where a full copy peaks at 100,000,085.

**Rejected.**

- *Keeping the `repr` and cutting it shorter* (`reprlib`, or `repr` with a length cap). A `repr`
  is the value's own code: it can raise, which the old guard swallowed together with the write so
  `"rejected"` was reported with no line, and it can return a raw newline. And the cut comes
  after the work: `repr` walked the whole value first, which is why the shared-sublist list never
  returned and the long `str` cost in proportion to its length.
- *Refusing a `str` subclass outright.* It would stop adopting a valid header that a framework
  hands over as a `str` subclass, a behaviour change for a valid input, when the characters a
  subclass holds can be read without running any of its code.
- *Normalising only in `continue_trace` and leaving the parsers on `isinstance`.* Each parser should
  be total on its own: they are twin paths (invariant 6), a parser called by anything other than
  `continue_trace` would reopen the hole, and a refactor that skipped the normalisation for one
  argument would leave that argument merely refused, which a rejection table cannot tell from
  correct. That is why the adoption tests state a `str` subclass for each argument separately.

**Do NOT** call `isinstance`, or anything that dispatches through the value's own type
(`str()`, `repr()`, `len()`, a method such as `.strip()`), on an inbound value before it is known
to be an exact `str`; route on `type(x) is str` or `issubclass(type(x), str)` and read through the
base type's own slots. Do not bound
`reason`: it is a literal the library supplies, escaped but not cut. (SPEC-067, invariants 1
and 11)

## History

### 2026-11-04

- **Settled.**
