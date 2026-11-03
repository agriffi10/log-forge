# Completed Spec — SPEC-064: Bounded, Total Baggage Header

## What was completed?

Three FRs, built as `3d68a18..8bd4504`, closing the `architecture.md` §12 item on
`format_baggage_header`. `current_baggage_header()` no longer renders with a bare `str()`:

- **Bounded render** (FR-001): `format_baggage_header` makes one `sanitize.sanitize_fields` pass over
  the whole baggage, under assembly's ceilings and SPEC-057's budgets, read from the live config; a
  DAG returns in assembly's time. Each coerced value is rendered as text and percent-encoded. A
  header value is now the coerced value the event carries — a `datetime` in ISO form, an `Enum` as its
  value, a tuple as a list — which is the caller-visible change.
- **Total render** (FR-002): `_render_member` guards the whole per-member render, `str()` and both
  `quote` calls, and names a failing value's type through `sanitize._type_name`. A lone surrogate
  from `set_baggage` or an inbound `continue_trace` header, a raising `__str__` and an integer past
  4,300 digits all return. The guard binds at render, not intake; `set_baggage` and
  `parse_baggage_header` are unchanged.
- **The cap and the diagnostic** (FR-003): the header is at most `BAGGAGE_MAX_BYTES`, by whole members
  as a prefix; an empty or unrenderable key is skipped without ending it; each drop is counted from a
  module-level `itertools.count` (`_dropped_members`) and announced through `_diag.lost`, throttled on
  `_diag.WARN_EVERY`, carrying a count and never a key or a value.

`sanitize.py` is not edited. The README row for `current_baggage_header()` and both functions'
docstrings state the new behaviour. Decision:
`docs/decisions/trace-model/the-outbound-baggage-header-renders-through-assemblys-coercion-and-is-capped-by-whole-members.md`.

Deviations, one line each: the spec's `test_diag.py` roster entry did not exist (its walkers cover
`context.py` unchanged); the build added `_render_member` and `_announce_dropped` as private helpers.

## What changed from earlier specs?

- **SPEC-014**: its baggage codec serialised with `str()` for non-string values; that is now the
  coerced value's `str()`, and the 8192-byte cap, which was only parse-side, now binds the formatter
  too. SPEC-014's delivery doc is a frozen record and keeps its original wording.
- **SPEC-057**: the budgets now also bound the outbound baggage header, a second reader of
  `sanitize_fields` beside assembly and `backfill_baggage`.

## What changed for a caller

In `docs/release-notes/v1.1.0.md`, *Fixed*: the header's value text, the 8192-byte cap by whole
members, the skipped empty key and the stderr line `log-foundry: lost N baggage member(s)`. No public
name or signature changed.

## Verification

Six gates green at `8bd4504` (3,667 passed). Against the unfixed `context.py`, 55 of the 66 new tests
in `tests/test_baggage_header.py` fail, each for its stated reason: the DAG is killed by the alarm,
the raising `__str__` raises `RuntimeError`, the surrogate raises `UnicodeEncodeError` on both intake
routes, 4,300 digits raise `ValueError`, and an over-cap header is refused by the parser. Four mutants
were planted and each reddened a test: the guard removed, a guard around `str()` alone, `except
BaseException`, and the key written into the diagnostic detail.
