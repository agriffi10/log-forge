# Completed Spec — SPEC-059: Retry-Safe Drop Counts in Every Sink

## What was completed?

Six FRs, built as `5ec40bf..fb97ae1`. The principle is SPEC-058's, now holding at every shipped
site: **a raising `emit` counts nothing, and a per-event drop is counted and announced only when the
`emit` that decided it returns** — through the one `Deferral` (`sinks/_deferral.py`), with the
worker, the orphan path and `MultiSink` owning whatever a raise leaves.

- **Oversize drops** (FR-001): `KinesisSink`, `FirehoseSink`, `SNSSink`, `MongoDBSink` and
  `AzureEventHubsSink` take the calling `emit`'s `Deferral`; Event Hubs enters it outside `_lock`.
- **The socket transport** (FR-002): `SyslogSink.emit` and `LogstashSink.emit`'s socket branch enter
  the ledger, owned by the transport, and pass it to `SocketTransport.send_all` and `_sendable` as a
  required parameter.
- **Refused hand-offs** (FR-003): `KafkaSink` and `GooglePubSubSink` count `rejected` through a
  `Deferral` enclosing the produce/publish loop, `_reap()` and the total-failure raise; each line
  names its own exception type, bound when the refusal was decided.
- **`HTTPSink._drop_refused`** (FR-004) counts through `emit`'s ledger on both routes into it.
- **The gate** (FR-005, `tests/test_deferral.py`): ten more roster sites, `IMMEDIATE` re-derived,
  and a check that derives every counter a `losses()` reports as `dropped` and refuses any store to
  it outside `__init__`, with a corpus asserting failure text and silence and a floor of five names.
- **Prose** (FR-006): the claims the above made false, restated in the docstrings, README,
  invariants §2 and architecture §8.

Deviations, one line each: the build ran in eight commits, two added for a mutation-found gap in the
derived check's corpus and for narrowing four restated sentences that said more than the code does;
prose was restated beyond FR-006's list, in `HTTPSink`'s `dropped_oversized` attribute entry and the
Kafka and Pub/Sub `rejected` entries; the derived check binds locals through plain names and
unpacking only; an all-`413` batch makes one request, not three, because equal-size events after the
first `413` are never offered.

## What changed for a caller

The list whoever prepares the next tag draws on (no `docs/release-notes/` file exists past v1.0.1):

- A drop at every shipped sink is counted **once** on a batch the worker retries, and **zero times
  at the sink** on one it abandons, where the abandon line is the only count.
- Kafka and Pub/Sub `rejected` (`sink.dropped`) no longer counts a batch refused whole: that sink
  raises, and the count is `failed_batches` and the abandon line, or `orphan_lost` outside a span. A
  batch refused on one attempt and accepted on the next counts nothing. An orphan refusal is in
  `orphan_lost` only; it used to be in `dropped` as well.
- Oversize, refusal and `413` lines are written after the requests, when `emit` returns, not as each
  drop is decided; a Pub/Sub refusal line can trail by up to one reap wait. The Event Hubs
  and socket oversize lines are no longer written under the sink's transport lock.
- Under `MultiSink`, the oversized or refused event of a child that raises moves from `dropped` to
  the fan-out's absorbed `failed`.
- README's `sink.dropped` row and the paragraph below it are restated to match.
- No public name or signature changed. `SocketTransport.send_all` and `_sendable` now require a
  ledger; both are private, and `SocketTransport` is not exported.

## What changed from earlier specs?

- SPEC-058's follow-up is closed: the nine sinks and `HTTPSink._drop_refused` now defer. Marked done
  in its delivery doc and in the `sink-delivery` register entry.
- `_socket.py`'s "nothing here can fix it" and "`dropped` is an exact count" claims are restated, as
  is `MultiSink.emit`'s "not yet deferring" cost, now a cost only for a third-party sink.
- SPEC-038 FR-007's datagram drop counts on return, not before the send.
- Invariants §2 and architecture §8 are restated; §2's `Guarded:` line gains `tests/test_deferral.py`.

## Follow-ups and open limits

- **"Partial delivery, then a raise"** is the sibling defect, unwritten as a spec: `LoggingSink`'s
  per-event handler calls, and `HTTPSink`'s uncaught `http.client.HTTPException`. This spec defers
  the drop on that path and does not change what raises. Measured in the spec's *Out of Scope*.
- **The derived check reads syntax.** Its docstring lists what it does not see: a store through an
  alias of `self`, a mutating method call, and a counter reported through a call or a property.

## Verification

At `fb97ae1`: `pytest` 3004 passed, 10 skipped (exit 0); `ruff check`, `mypy`, `docstring-lint.py`,
`spec-lint.sh` and `docs-lint.sh` exit 0. The extras leg passed 3010 with 4 skipped; Python 3.12.12
passed 3003 with 11 skipped, the extra skip being `test_version`, which has no distribution metadata
in that venv. 45 mutants were all killed: 30 on the derived check, 10 guards, 4 deferral-matters and
1 planted store. At the base, 52 of the new behavioural tests failed and 34 passed, every pass an
unchanged criterion or a named guard. `pytest --collect-only` shows no name collected at `ae105bb`
removed or renamed. Not run: `tests/integration` (no Docker daemon here); owed on CI.
