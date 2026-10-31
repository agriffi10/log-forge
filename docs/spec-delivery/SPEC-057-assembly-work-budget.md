# Completed Spec — SPEC-057: Assembly Work Budget — Node and Byte Budgets per Pass, and Reads Bounded by the Ceiling

## What was completed?

Every coercion pass over an event's fields is now bounded in work, closing a shared-sublist hang
on the caller's thread. All of it is in `src/log_foundry/sanitize.py`; no config key and no public
name was added, and `sanitize.__all__` and `log_foundry.__all__` are unchanged.

- **A node budget** (FR-001): `_MAX_NODES` = 131,072 values per pass, counting every value entered,
  scalar or container, `<depth limit>` and `<circular>` included. The value that would be one past it
  becomes `<node limit>`, the event is marked `truncated`, nothing after it is visited, and every
  open container ends after the member it was writing. The order of checks is node, then depth, then
  the type's rule.
- **A byte budget** (FR-002): `_MAX_RENDERED_BYTES` = 16 MiB of text charged per pass, in three
  classes (text from the caller, after clipping; text the pass produces, at the larger of before and
  after clipping; an integer, at its rendered-length estimate). The value that would cross it becomes
  `<size limit>`, whole, and the walk stops on the same rule. A mapping entry's **key is rendered and
  charged before its value is visited**, and an integer key is priced at a lower bound before it is
  rendered. Both budgets are private module constants read when a pass starts; the counters live on
  `_Coercer`.
- **Reads bounded by the ceiling** (FR-003): `truncate_str`, `truncate_tail` and the bytes-like branch
  of `_dispatch` read a prefix of `max_bytes + 1`, through unbound builtin accessors. The integer
  measure is the **unbound `int.bit_length`**, because a subclass returning `nan` switched the budget
  off. A `memoryview` is read only after an **exact-type guard**, and an object that merely claims
  to be one is a type-name placeholder. The output is byte-identical to the unbounded computation,
  flag included, save the one recorded exception in the spec.
- **The promise restated** (FR-004): `docs/invariants.md` §8, `architecture.md` §13, the `Config`
  docstring and the README. The register entry and its fence are "Each coercion pass is bounded by a
  node and a byte budget" in `docs/decisions/event-assembly.md`.

A transform's types-only coercion (SPEC-058) runs outside both budgets, through
`_Coercer(budgeted=False)`, so a transform's output is still never clipped. That was decided when
this spec rebased onto SPEC-058: the two passed their tests together while the budgets cut it.

The spec's *Accepted Limits* stand as written there and are not restated here.

## What changed from earlier specs?

- SPEC-017's "ceilings bound per value, not per event" is narrowed, not withdrawn: the four ceilings
  still bound a value, and the pass is now bounded as a whole. The per-event byte ceiling that
  SPEC-017, SPEC-020 and SPEC-021 deferred is answered as a work budget, not a size limit.
- SPEC-055's clippers return the same `(text, flag)`; only the work to produce it changed.
- SPEC-020's integer ceiling now measures through the unbound `int.bit_length`, not the value's own.

**What a caller can now see.** A release note is owed at the next tag (no `docs/release-notes/` file
exists yet for it, `v1.0.1` being tagged):

- An event can be cut with `<node limit>` or `<size limit>`, and the siblings after the cut are dropped.
- A `Decimal` or other produced text over 16 MiB becomes `<size limit>` where it was clipped.
- A non-contiguous `memoryview` and the other bytes-likes render identically, in bounded work.
- A `bytes` or `bytearray` subclass overriding `__bytes__` or `__buffer__` is now rendered from the
  bytes it stores.
- An object that only claims to be a `memoryview` or an `int` becomes a type-name placeholder.

**Hot-path cost.** Same interpreter (CPython 3.13.1), `timeit` min-of-30, each side run twice
interleaved, `main` at `324936e` against the branch at `6916951` (not re-measured after the frame-2
fixes, which changed the integer path's `bit_length` accessor and added two guards off the common
path):
`build_event` on a five-field event 6,061–6,346 ns → 6,731–7,162 ns (+670 ns, +11%); `sanitize_fields`
on the same event +494 ns (+25%); 200 integers +9,777 ns (+33%, about 49 ns each); 200 short strings
+13,689 ns (+35%, about 68 ns each). Accepted as the cost of the per-value checks the spec requires,
after an inlining pass recovered the avoidable part.

## Verification

The tests are `tests/test_assembly_budget.py` and new rows in `tests/test_sanitize_corpus.py`. The
run-once-against-`main` criteria were settled with `main`'s `sanitize.py` at `324936e` under the test
file of `6916951`: the shared-sublist DAG, the shared long-string DAG and the produced-key nest
failed by the 5-second alarm on both routes; the shared 10-million-digit `Decimal` × 256 failed by
assertion (`main` finished it in about 1.9 s); and all 12 allocation cases exceeded the bound, at
33–67 MB peak. Mutation: 67 of 67 mutants killed at `b282c1d` and 16 of 16 at `8b2a543`. Four
follow-ups were found and not built, each an `architecture.md` §12 entry: a mapping key rendered
whole before any budget applies, the type-impostor routes, `format_baggage_header` neither bounded
nor total, and non-`int` ceilings accepted at `configure()`.
