# Spec: Batch Response Adjudication

**ID:** SPEC-018  
**Status:** Draft  
**Last Updated:** 2026-07-28  
**Depends On:** SPEC-010, SPEC-017

## Overview

`KinesisSink` and `FirehoseSink` send a batch and then read the response to learn which records
failed, so only those are retried. They learn it **positionally** — the response carries a parallel
array with no identifiers, so entry *i* is taken to describe record *i*. That correlation is only
meaningful while the two arrays are the same length, and nothing checks that they are. When they
disagree the pairing silently truncates: the retry list comes back short or empty, the sink reads an
empty retry list as "everything landed", and records the destination never confirmed are reported as
delivered. No counter moves and nothing is written to stderr.

That is the same silent-loss shape SPEC-017 was written to remove — logging that loses data without
saying so — surviving in the two sinks SPEC-017 did not touch. This spec makes the positional
correlation check its own precondition, and makes a batch the sink cannot adjudicate an audible,
counted loss instead of a silent success.

## Scope

### In Scope

- A shared helper that adjudicates a positional batch response and reports how many records the
  response failed to describe.
- `KinesisSink` and `FirehoseSink` adopting it, including a new per-sink counter and a stderr line.
- Unit tests for the mismatch paths, which today have no coverage in either sink.

### Out of Scope

- **`SQSSink` and `SNSSink`.** They correlate by explicit `Id` — `Failed` entries carry the id of
  the entry they describe, and the sinks select by id, not position. They cannot truncate, so they
  need no change. This spec deliberately does not unify the two correlation styles; the AWS APIs
  differ and the id-keyed one is already safe.
- **Treating an absent failure count as a failure.** `put_records` / `put_record_batch` always
  return `FailedRecordCount` / `FailedPutCount`, and the sinks' existing `if not
  response.get(...): return` stays as-is. A response with no count field is read as zero failures.
  Changing that would make every reasonable fake client in a consumer's test suite start warning,
  for a case AWS does not produce.
- **Retrying an unadjudicated chunk.** See FR-002 — abandoning is the deliberate choice, not an
  omission.
- Any change to `max_retries`, chunk sizing, the oversize drop path, or the `health()` surface.
- The other queue/stream sinks (Kafka, Redis, RabbitMQ, NATS, Pub/Sub, Event Hubs). None of them
  adjudicate a per-record response array.

---

## Functional Requirements

### FR-001: Positional adjudication requires the arrays to agree in length

#### Description:

Before using a positional batch response to decide which records to retry, the sink must confirm
the response describes exactly as many records as were sent. If the counts differ, the response
cannot be used to adjudicate **any** record in the chunk — not even the overlapping prefix, because
a length disagreement is evidence the arrays are not aligned, so entry *i* is not known to describe
record *i*.

#### Acceptance Criteria:

- [ ] When the results array length equals the records length, the records selected for retry are
      exactly those whose paired result carries a truthy `ErrorCode` — identical to today's
      behaviour.
- [ ] When the results array is shorter than the records array, no record is selected for retry and
      the chunk is reported as unadjudicated with a count equal to the number of records sent.
- [ ] When the results array is longer than the records array, the chunk is likewise reported as
      unadjudicated with a count equal to the number of records sent.
- [ ] When the failure count is non-zero and the results array is absent entirely, the chunk is
      reported as unadjudicated with a count equal to the number of records sent.
- [ ] When the failure count is zero or absent, the sink returns success without consulting the
      results array, and nothing is counted as unadjudicated.

### FR-002: An unadjudicated chunk is abandoned, counted, and reported — never a silent success

#### Description:

A chunk the sink cannot adjudicate is abandoned rather than retried, and the loss is made visible
through the same two mechanisms the sinks already use for the losses they absorb: a counter on the
sink instance and one line on stderr.

Abandoning rather than retrying follows SPEC-017's ruling on partial `MultiSink` failure: the API
accepted the call and reported a failure count, so some records in the chunk almost certainly
landed, and re-sending the whole chunk would duplicate them. A duplicate is a cost paid downstream
forever; an abandoned record is a loss counted here and now. The reason to fix this at all is that
today it is *neither* — it is a loss reported as a success.

#### Acceptance Criteria:

- [ ] Each sink exposes `dropped_unadjudicated`, initialised to `0`, alongside the existing
      `failed` and `dropped_oversized` counters.
- [ ] An unadjudicated chunk increments `dropped_unadjudicated` by the number of records in that
      chunk.
- [ ] An unadjudicated chunk writes exactly one line to stderr, prefixed `log-foundry:`, naming the
      sink class, the number of records abandoned, and the two array lengths that disagreed.
- [ ] An unadjudicated chunk causes no further `put_records` / `put_record_batch` call for that
      chunk — the retry loop stops.
- [ ] An unadjudicated chunk does **not** increment `failed`, which continues to mean "the
      destination told us these failed".
- [ ] `emit` does not raise on an unadjudicated chunk; the caller's thread is unaffected
      (architecture §4).
- [ ] A batch whose chunks are all adjudicated normally leaves `dropped_unadjudicated` at `0` and
      writes nothing to stderr.

### FR-003: One shared implementation, so the two sinks cannot drift

#### Description:

The adjudication rule lives in one internal helper under `log_foundry.sinks`, used by both sinks,
rather than being written twice. The two `_send` methods are already near-identical; duplicating the
new check would leave two places for a future edit to diverge, and a third positional sink would
have no obvious thing to reuse.

#### Acceptance Criteria:

- [ ] Exactly one implementation of the length check and the retry-selection exists in `src/`.
- [ ] Both `KinesisSink._send` and `FirehoseSink._send` obtain their retry list from it.
- [ ] The helper is not exported from `log_foundry.sinks.__init__` or the package root — it is
      internal, like `_chunk` and `_socket`.
- [ ] The helper is covered by unit tests that exercise it directly, independent of either sink.
- [ ] The helper takes the error key as a parameter rather than hard-coding `ErrorCode`, so a future
      positional response using a different key needs no fork.

### FR-004: The new counter is documented where the others are

#### Description:

`dropped_unadjudicated` is discoverable in the same places `failed` and `dropped_oversized` are, so
an operator wiring up an alert finds all three together.

#### Acceptance Criteria:

- [ ] The README's sink-counter guidance lists `dropped_unadjudicated` alongside `.failed` and
      `.dropped_oversized`.
- [ ] Both sinks' class docstrings state that the counter exists and what a non-zero value means.
- [ ] No entry is added to `log_foundry.health()`, which reports worker counters only.

---

## Data Model

```python
# src/log_foundry/sinks/_batch.py

class Adjudication(NamedTuple):
    retry: list[dict[str, Any]]   # records the response explicitly flagged as failed
    unadjudicated: int            # records whose outcome the response did not describe (0 normally)
```

`retry` is non-empty only when `unadjudicated == 0`: either the response describes the chunk and the
sink acts on it, or it does not and the chunk is abandoned. The two are never both non-zero.

---

## API / Interface Contract

```python
# src/log_foundry/sinks/_batch.py

def adjudicate_positional(
    records: list[_T],
    results: list[dict[str, Any]],
    *,
    error_key: str = "ErrorCode",
) -> Adjudication:
    """Pair a positional batch response against the records it should describe."""


# KinesisSink._send / FirehoseSink._send, per attempt:
verdict = adjudicate_positional(records, response.get("Records", []))
if verdict.unadjudicated:
    self.dropped_unadjudicated += verdict.unadjudicated
    sys.stderr.write(...)
    return
records = verdict.retry
if not records:
    return
```

## Configuration / Environment

None. No new config keys, environment variables, or constructor arguments.

## File & Folder Structure

```
src/log_foundry/sinks/
├── _batch.py          # new — Adjudication + adjudicate_positional
├── kinesis.py         # modified — uses the helper, adds dropped_unadjudicated
└── firehose.py        # modified — uses the helper, adds dropped_unadjudicated

tests/
├── test_sinks_batch.py     # new — the helper, directly
├── test_sinks_kinesis.py   # modified — mismatch paths
└── test_sinks_firehose.py  # modified — mismatch paths

README.md              # modified — counter guidance
```

## Implementation Phases

### Phase 1: The helper

- Add `src/log_foundry/sinks/_batch.py` with `Adjudication` and `adjudicate_positional`.
- Add `tests/test_sinks_batch.py` covering equal lengths, short results, long results, empty
  results, empty records, and a non-default `error_key`.

### Phase 2: Adopt it in both sinks

- Rewrite `KinesisSink._send` and `FirehoseSink._send` to adjudicate through the helper.
- Add `dropped_unadjudicated` to both constructors and the stderr line to both unadjudicated paths.
- Remove the two `strict=False` `zip` calls and the comments that stood in for this spec.
- Extend `test_sinks_kinesis.py` and `test_sinks_firehose.py`: a short response abandons and counts,
  a well-formed response is unchanged, and no extra send happens after an unadjudicated chunk.

### Phase 3: Documentation

- README: add `dropped_unadjudicated` to the counter guidance.
- Both sink class docstrings: state the counter and its meaning.
