# Completed Spec — SPEC-018: Batch Response Adjudication

## What was completed?

`KinesisSink` and `FirehoseSink` learn which records failed **positionally** — the response is a
parallel array with no ids, so entry *i* describes record *i*. Nothing checked the arrays were the
same length. A short or absent one truncated the retry list, usually to empty, and an empty retry
list read as "everything landed": records the destination never confirmed, reported as delivered,
with no counter moved and nothing on stderr. That is the silent-loss shape SPEC-017 removed
elsewhere, surviving in the two sinks SPEC-017 did not touch.

- **`sinks/_batch.py`** (new, internal) — `adjudicate_positional` returns an `Adjudication(retry,
  unadjudicated)`. Lengths equal → today's selection, unchanged. Lengths differ → **nothing** is
  adjudicated, not even the overlapping prefix: a mismatch is evidence the arrays are not aligned,
  so entry *i* is not known to describe record *i*. `error_key` is a parameter so a third
  positional response naming its error differently needs no fork (FR-001, FR-003).
- **`usable_results`** normalizes the response field before the length check. Not in the spec —
  see the deviation note below.
- **An unadjudicated chunk is abandoned, not retried** (FR-002). The API accepted the call and
  reported a failure count, so some of the chunk almost certainly landed and re-sending would
  duplicate it — SPEC-017's ruling on partial `MultiSink` failure, applied here. The loss is made
  visible the way these sinks already make loss visible: a new **`dropped_unadjudicated`** counter
  per sink and one stderr line naming the class, the records abandoned, and both lengths.
- **`failed` keeps its meaning** — "the destination told us these failed" — and is untouched on
  this path. `emit` does not raise (arch §4).
- **21 new tests**, 8 exercising the helper independently of either sink.

**Deliberate deviation:** the spec typed the response array `list[dict[str, Any]]` and stopped
there. A client returning `{"Records": None}` — key present, value not a list — made `len(results)`
raise, *and* the stderr line reporting the loss called `len(results)` too, so the report died on
the way out. `usable_results` closes it: a list of mappings passes through, anything else describes
nothing and routes to the same counted, audible abandonment. Adding it to `_batch.py` rather than
to each sink keeps FR-003's single implementation and leaves `adjudicate_positional`'s signature
exactly as the spec's API contract states.

## What changed from earlier specs?

- **SPEC-010's two `_send` methods.** Both `zip(..., strict=False)` calls are gone, along with the
  comments that stood in for this spec. Behaviour on a well-formed response is byte-identical —
  the pre-existing SPEC-010 retry tests are unmodified and still pass.
- **Both sinks gain a third public counter.** An operator reading `.failed` and
  `.dropped_oversized` now has `.dropped_unadjudicated` beside them, documented in the README and
  in both class docstrings. Nothing was added to `health()`, which reports worker counters only.
- **No change** to `SQSSink`/`SNSSink` (they select by explicit `Id` and cannot mis-pair),
  `max_retries`, chunk sizing, or the oversize drop path.

## Notes for the next spec

*Reconciled by SPEC-021 — each note is marked settled or recorded as a constraint. None was a
defect; this spec's notes were the cleanest of the four.*

- **The two correlation styles are still not unified**, deliberately. The AWS APIs differ and the
  id-keyed one is already safe; a third positional sink should reuse `_batch`, and a third
  id-keyed one should not.
  → **Settled** (SPEC-021). Already recorded in `CLAUDE.md` Key Decisions, and named in
  SPEC-021's Out of Scope so it is not reopened by a later reader mistaking it for a to-do.
- **An absent failure count is still read as zero failures.** Out of scope by ruling, not
  omission: `put_records`/`put_record_batch` always return the count, and treating its absence as
  a failure would make every reasonable fake client in a consumer's test suite start warning.
  → **Settled** (SPEC-021). A ruling, restated here as one: the AWS APIs always return the count,
  so its absence describes a fake client rather than a delivery failure.
- **`dropped_unadjudicated` is a client-shape signal as much as a loss signal.** A non-zero value
  against real AWS should not happen; it usually means the client is a fake, a proxy, or a
  compatibility layer that isn't AWS-shaped.
  → **Settled** (SPEC-021). Operator guidance, not an open item; it lives in the counter's own
  documentation in the README and both class docstrings.

## Verification

Local: 518 tests pass, `ruff check` clean, `mypy --strict` clean over 48 source files, `spec-lint`
clean. CI green on 3.12 and 3.13 (PR [#60](https://github.com/agriffi10/log-forge/pull/60)); the
new PEP 695 generic `NamedTuple` was additionally executed under 3.12.12 directly, since 3.12 is
the CI floor and that syntax is newer than the rest of the codebase uses. A fresh-context review
checked the diff against all 20 acceptance criteria and returned no functional defects; its two
substantive findings — an untested FR-001 criterion and the `None`-shaped response above — were
both fixed before merge. Nothing was deferred to deploy: every path is covered by a fake client.
