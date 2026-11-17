# Completed Spec — SPEC-096: SQSSink and SNSSink Take Their Message Limit as an Argument, and Never Send Past the Service's

## What was completed?

- **The argument** (FR-001). `SQSSink` and `SNSSink` each take `max_message_bytes: int | None =
  None`, keyword-only and appended last, stored as the public attribute `max_message_bytes`. Each
  `__init__` judges it after every refusal it made before and before any client is built: read
  through `_retry.exact_int`, then refused with `ValueError` outside 1,024 to 1,048,576
  (`_LIMIT_FLOOR` and `_SERVICE_MAX_BYTES`, private `Final` constants in `sinks/sqs.py` and
  `sinks/sns.py`), the value rendered by `sanitize._int_text`.
- **Applying it** (FR-002, FR-003). Each sink gains `_limit()`: `max_message_bytes`, else
  `MAX_BYTES` read through `self` (an `is None` test), capped at 1,048,576. Each run of `emit`
  reads it once, in its first round, and keeps it in the run's notes (`run_context().notes(self)`
  under `"limit"`), so every later round of the run uses the same value on both sinks.
  `SQSSink._chunks` carries it on `_Plan.limit`; `_prepare` takes it by keyword and
  `_announce_oversized` takes it as an argument, so the line names the run's limit rather than
  `MAX_BYTES` at settle time. `SNSSink._round` passes it to `_bodies` and to `chunk_items`, whose
  `size_of` is now the body's UTF-8 length rather than its characters.
- **`MAX_BYTES`** (FR-004) keeps 262,144 on both classes and its 1.x reach as the default limit.
- **The record** (FR-005): the docstrings, `sanitize._MAX_RENDERED_BYTES`'s "256 KiB by default",
  the README's AWS section, `docs/release-notes/v1.2.1.md` (an *Added* and a *Changed* section, a
  tenth pin exception), `architecture.md` §9.1, §13 and §12 (the `SQSSink.MAX_BYTES` item moved
  whole to *Resolved*; the `KinesisSink` item stays open), the coercion-budget decision's fact
  with a dated History line, and a new decision in `docs/decisions/sink-delivery/`.
  `tests/conformance_classes.py` does not exist, so SPEC-092's build reads the two names from
  the tree.
- **Tests:** `tests/test_sinks_message_limit.py` (FR-001 to FR-004 on both sinks, including the
  twin-path table at four limits), `tests/message_limit_cases.py` (the recorder) with its fixture
  `tests/data/spec096_requests_10ba21ee.json`, the refusal and `_HUGE_DOORS` rows in
  `tests/test_numeric_doors.py`, `LIMIT_GAINED` in `tests/test_sinks_encode.py`, and both classes
  in `tests/typed_consumer/accepts.py`.

## What changed from earlier specs?

- SPEC-005, SPEC-010 and SPEC-016: a `MAX_BYTES` raised above 1,048,576 is capped there, so a
  message over 1 MiB is dropped and counted in `dropped_oversized` rather than sent and refused,
  and requests split under 1 MiB.
- SPEC-010: an `SNSSink` request is costed in UTF-8 bytes, not characters. No default request
  changes, since `json.dumps` writes ASCII; a subclass whose `_encode` writes UTF-8 gets smaller
  requests.
- The SPEC-092 prototype, `docs/audits/2026-11-14-probes/sinks/spec092_corpus.py`: its
  `Worker._emit` wrapper now forwards keyword arguments. It had taken only `event_lists` since
  `14711e7e` gave `_emit` the keyword-only `exit_batch` (SPEC-090), so every row raised in the
  worker and read `harness-error:batch-count`.

## Verification

- **The 1.x requests.** The six default-limit cases (A, B, C, E, F1, F2) were recorded entry by
  entry against a `git archive` of `10ba21ee`, and re-recorded against `61b942c0`, the branch's
  base on `main`, byte-identical; the branch's requests equal the recording apart from each `Id`.
- **SPEC-092's corpus** (`spec096_corpus_rows.py`), on the repaired prototype: 256 rows, 86 red;
  0 rows moved at 1,048,576 and at 1,024, and 7 moved at the 16-byte control, all SQS or SNS
  rows, which proves the patch reached them. Before the repair every row read
  `harness-error:batch-count` and the control moved none.
- **Mutation sweep.** Eight planned mutants, all killed. `_limit` testing truthiness rather than
  `is None` is not equivalent: 0 is refused at construction, but `max_message_bytes` is a public
  attribute a caller may reassign afterwards, and a 0 assigned then is the limit, so a 10-byte
  event is dropped with a line naming 0 where the truthiness mutant falls back to 262,144 and
  sends it; it is killed on each sink by the test that reassigns 0. Four further mutants were run and killed: `_prepare` and `_bodies` judging by
  `self.MAX_BYTES`, `_limit` ignoring the argument (each on both sinks), the floor off by one
  on `SQSSink`, and the cap off by one on `SNSSink`. A fifth, the oversize line formatting
  `self._limit()` when the run settles rather than the limit the round read, is killed on each
  sink by a request that reassigns `MAX_BYTES` before the line is written. A sixth, re-reading
  the limit every round (the per-round read each sink had before the system-frame review), is
  killed on each sink by a request that lowers `max_message_bytes` from 1 MiB to 2,048 and times
  out: at the run's 1 MiB both sinks resend the 10-byte and the 300 KiB event together and drop
  nothing, where the per-round read made SNS drop the large event and SQS send it alone.
- The spec's "§8" was §9.1; the spec is corrected in its revision history.
