# Completed Spec — SPEC-016: FIFO Queue Support for `SQSSink`

## What was completed?

`SQSSink` can now ship to a FIFO queue. It previously built every entry as
`{"Id", "MessageBody"}`, which a FIFO queue rejects for want of a `MessageGroupId` — and the
rejection is a *per-entry sender fault*, so nothing raised: the entries landed in the `Failed`
list, the retry loop re-sent the same invalid entries four times, and the batch ended as one line
on stderr with every event lost.

- **FIFO detection** — a `.fifo` URL suffix selects FIFO behaviour (AWS mandates the suffix, so it
  is a contract, not a heuristic); `fifo=True/False` overrides. Decided once in `__init__`
  (FR-001).
- **`MessageGroupId`** — defaults to the event's own `trace_id`. SQS orders *within* a group, and a
  trace is the unit whose events should stay ordered; per-trace groups also keep traces
  independent, so the queue delivers them in parallel rather than serializing the process behind
  one group. Overridable with a constant (strict global ordering) or a callable (FR-002).
- **`MessageDeduplicationId`** — defaults to the event's `log_id`, already a per-event UUID, so
  SQS's five-minute window never collapses two distinct records (FR-003).
- **`_chunks` returns `_Prepared(body, group_id, dedup_id)`**, resolving the ids once. Deriving
  them again at send time would be a latent bug, not just waste: the dedup fallback mints a fresh
  UUID, so the byte budget would be billed for one value while another went on the wire.
- **Byte accounting** — the ids are costed against the 256 KB request limit; the oversized-event
  drop still judges the body alone, so droppability does not depend on queue type (FR-005).
- **Sender faults are not retried** (FR-006) — a retry re-sends an entry byte-identical, so a fault
  in the request itself can only fail the same way. They are counted immediately and the SQS
  `Code` is named in the warning; throttles and internal errors (`SenderFault: false`) still retry
  under the bound. A missing flag degrades to retrying, so an unfamiliar response shape never
  silently drops.

## What changed from earlier specs?

- **SPEC-005 standard-queue behaviour is untouched** — entries stay exactly `{"Id",
  "MessageBody"}`, and all 11 existing tests in `test_sinks_sqs.py` pass unmodified (FR-004).
- **One visible behaviour change for existing users:** a `SenderFault: true` entry is now abandoned
  after one attempt rather than four, so `failed` increments sooner and the stderr text differs.
  No existing test emits that flag — SPEC-005's fake client already marked every simulated failure
  `SenderFault: false` — so the distinction was one the suite anticipated but the sink never acted
  on.
- **Baggage-driven grouping works only because of SPEC-015.** The documented per-span recipe
  (`message_group_id=lambda e: e["fields"].get("group") or e["trace_id"]`) reads baggage out of
  `fields`. Before SPEC-015 the boundary events carried `baggage={}`, so a span's start and end
  would have fallen through to `trace_id` while its mid-span events grouped by baggage — splitting
  one span across two FIFO groups.
- Two spec corrections were made during the build: the `_chunks` return shape above, and two
  FR-002 criteria for cases left underspecified (a callable returning blank falls back; a blank
  *constant* raises `ValueError` at construction).
- No public API change beyond three new keyword arguments on `SQSSink.__init__`, all optional.

## Verification

Full gate green locally: `pytest` **387 passed** (from 351), `ruff` clean, `mypy --strict` clean
over 46 files, `spec-lint` 0 failures.

The 36 new tests were **mutation-tested rather than trusted green**. Phase 1: dropping the FIFO
byte accounting fails 2, deriving ids for standard queues fails 3, cross-wiring a group id to the
wrong event fails 3. Phase 2: retrying everything fails 4, never retrying fails 5, dropping the
SQS `Code` from the warning fails exactly 1.

Known limitation, documented in the module docstring: ordering is best-effort across a retry
boundary — a retried entry lands after a same-group entry that already succeeded. Holding a group
back on one failure would trade log delivery for ordering the consumer can rebuild from
`timestamp`.
