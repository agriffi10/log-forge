# Completed Spec — SPEC-096: SQSSink and SNSSink Take Their Message Limit as an Argument, and Never Send Past the Service's

## What was completed?

- **The argument** (FR-001). `SQSSink` and `SNSSink` each take `max_message_bytes: int | None =
  None`, keyword-only and appended last, stored as the public attribute `max_message_bytes`. Each
  `__init__` judges it after every refusal it made before and before any client is built: read
  through `_retry.exact_int`, then refused with `ValueError` outside 1,024 to 1,048,576
  (`_LIMIT_FLOOR` and `_SERVICE_MAX_BYTES`, private `Final` constants in `sinks/sqs.py` and
  `sinks/sns.py`), the value rendered by `sanitize._int_text`.
- **Applying it** (FR-002, FR-003). Each sink gains `_limit()`: `max_message_bytes`, else
  `MAX_BYTES` read through `self` (an `is None` test), capped at 1,048,576. `SQSSink._chunks`
  reads it once per round and carries it on `_Plan.limit`; `_prepare` takes it by keyword and
  `_announce_oversized` takes it as an argument, so the line names the round's limit rather than
  `MAX_BYTES` at settle time. `SNSSink._round` reads it once and passes it to `_bodies` and to
  `chunk_items`, whose `size_of` is now the body's UTF-8 length rather than its characters.
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
  entry against a `git archive` of `10ba21ee`, and re-recorded against `3949936f` byte-identical;
  the branch's requests equal the recording apart from each `Id`.
- **SPEC-092's corpus** (`spec096_corpus_rows.py`), on the repaired prototype: 256 rows, 86 red;
  0 rows moved at 1,048,576 and at 1,024, and 7 moved at the 16-byte control, all SQS or SNS
  rows, which proves the patch reached them. Before the repair every row read
  `harness-error:batch-count` and the control moved none.
- **Mutation sweep.** Eight planned mutants, seven killed. The survivor, `_limit` testing
  truthiness rather than `is None`, is equivalent: the only falsy `int`, 0, is refused at
  construction. Four further mutants were run and killed.
- The spec's "§8" was §9.1; the spec is corrected in its revision history.
