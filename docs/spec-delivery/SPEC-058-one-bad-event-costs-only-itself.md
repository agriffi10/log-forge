# Completed Spec — SPEC-058: One Bad Event Costs Only Itself

## What was completed?

Seven FRs, built as `26f0e55..11e4e56` in the order the plan fixed: the helper, `MultiSink`, the
transform, the wrappers, SQS, HTTP, the prose. The principle they share: **a raising `emit` counts
nothing; whoever catches the raise — the worker, the orphan path or `MultiSink` — owns the batch's
count.**

- **One deferral helper** (`sinks/_deferral.py`): `Deferral` holds one call's pending drops and
  commits them on a clean exit only, under the owner's `_counter_lock`; `wrapped_losses` is the
  wrappers' `losses()` rule. An AST check with a fixture corpus (`tests/test_deferral.py`) refuses a
  second accumulate-then-commit in the five modules that use it, following `self.<method>()` calls
  within each class; a class whose `emit` enters a `Deferral` unlisted is itself a finding, and
  the stores kept immediate on purpose are named in its `IMMEDIATE` allowlist.
- **`MultiSink` counts every raise it swallows** (FR-007), a reporting child's included, and only
  when it returns; all children raising re-raises the first and counts nothing.
- **A transform's output is checked, then coerced for type only** (FR-001): invariant 8's
  observable passes the same object through; a failure becomes `sanitize._coerce_unbounded`'s
  output — assembly's per-type rules, no configured ceiling, one fixed depth bound of 100.
- **One failing event costs one event** (FR-002, FR-003): `TransformSink`'s function and
  `FilteringSink`'s predicate; each wrapper reports its own count through `losses()`.
- **`SQSSink`'s FIFO id callables** cost only their event, and the oversize drop beside them counts
  once (FR-004).
- **`HTTPSink._items` renders per event** (FR-005): an unrenderable event is dropped alone, and both
  drops `_items` decides count only when `emit` returns; `ElasticsearchSink.losses()` (and so
  `OpenSearchSink`) folds the new count too.
- **Every claim made false is restated** (FR-006): the nine `which sanitize prevents` lines, the
  wrapper, SQS and HTTP docstrings, invariants §2/§7/§8, architecture §6/§8, a new §13 Known
  Constraint; the reversed SPEC-036 clause is struck in place at each site that stated it.

Deviations, one line each: the wrappers' lock is `_counter_lock`, not `_lock`, which the
transport-lock lint reads as a transport lock; `sanitize.py` is touched append-only and the coercion
runs `_Coercer` under a stand-in config (a `Config` validates its ceilings), pinned by a test that
derives the attributes the coercer reads; the small-stack criterion runs in its own interpreter so a
crash is a named failure; the plan review added a third accepted cost to FR-007 (a raising wrapper
child is charged `len(batch)`, declined events included); two `MultiSink` tests were renamed because
their names stated the reversed claim.

## What changed for a caller

The list whoever prepares the next tag draws on (no `docs/release-notes/` file exists past v1.0.1):

- `MultiSink.losses().failed` now includes `len(batch)` for **every** child raise it swallows, a
  reporting child's too; it was silent children only. Three accepted over-counts: a reporting child's
  own `failed` also counts; ~~a not-yet-deferred sink's counted drop is in `dropped` and in
  `failed`~~ — done by SPEC-059 (no shipped sink counts before raising now; it remains for a
  third-party sink); a raising wrapper child is charged declined events as well. When every child
  raises nothing is counted by the fan-out (a silent child used to be counted per attempt).
- New stderr lines, one per batch, naming the class and the exception **type**: `lost N event(s)`
  from `TransformSink`, `FilteringSink`, `SQSSink` (id callables) and the HTTP sinks (unrenderable).
- A transform's non-JSON output (`bytes`, `datetime`, NaN, a lone surrogate) is coerced, not a failed
  batch; a result nested past 100 levels is cut at `<depth limit>` and marked `truncated`.
- A transform's non-`dict`, non-`None` result is now a per-event failure (it was forwarded).
- A count decided at the five deferring sites — the drops of `TransformSink`, `FilteringSink`,
  `SQSSink._chunks` and `HTTPSink._items`, and `MultiSink`'s absorbed count — lands when `emit`
  returns: a retried batch counts a drop once, an abandoned one zero times at the sink.
  ~~`HTTPSink._drop_refused`'s `413` drop~~, a sink's own `failed` (the SQS sender fault included) and
  ~~the nine sinks under *Follow-ups*~~ still count as they happen — the two struck are done by
  SPEC-059; a sink's own `failed` alone still does. The oversize lines of `HTTPSink`
  and `SQSSink` print after the requests, not before.
- `TransformSink.losses()` and `FilteringSink.losses()` are non-`None` once the wrapper has counted a
  drop, over an inner sink that reports nothing. `HTTPSink`'s all-unrenderable batch settles without
  raising, so a single unrenderable event moves from `orphan_lost` to `health().sink.dropped`.

## What changed from earlier specs?

- SPEC-036 FR-005's "a reporting child is left to report itself" is reversed in part. Struck and
  marked at `multi.py`, the SPEC-036 spec and delivery doc, and the sink-delivery register entry.
- SPEC-026's wrapper `losses()` pass-through is now conditional on the wrapper's own count.
- The SQS and HTTP oversize drops (SPEC-016, SPEC-017) count on a normal return, not before the send.
- Invariants §2 and §7 gain Recorded exceptions; §8's premise now holds at the transform too.

## Follow-ups and open limits

- ~~**The drop recount in the other sinks** is a separate spec: `SyslogSink`/`LogstashSink`
  (`_socket.py`), `MongoDBSink`, `KinesisSink`, `FirehoseSink`, `SNSSink`, `AzureEventHubsSink`,
  `KafkaSink`, `GooglePubSubSink`, and `HTTPSink._drop_refused`'s conditional path. It applies
  `Deferral` to each and revisits `_socket.py`'s "nothing here can fix it" and the "`dropped` is an
  exact count" claims, both left standing.~~ — done by SPEC-059: each applies `Deferral`, and both
  claims are restated.
- **`LoggingSink`'s per-event caller code** is a sibling the spec does not name, pre-existing and
  not a regression: `emit` calls `logger.handle` per event, so a `logging.Filter` raising for one
  event leaves after the events before it were handled and the retry re-delivers them. Measured
  at `ed21cc6`: one such event of 7 gave 16 handled records for 4 distinct events, then "lost 7;
  batch abandoned". Recorded in the spec's *Out of Scope*; a follow-up spec's.
- **Unmeasured:** the 4 MiB small-stack criterion assumes the check raises `RecursionError` rather
  than killing the process; measured on macOS only (3.13.1 dies at 1 MiB). Linux frame sizes on CI
  are unmeasured. The criterion runs in a child interpreter, so a crash there fails that one test
  with the child's return code (a negative signal number) rather than taking an xdist worker down.
- SPEC-057 also edits the coercer and §8; whichever lands second rebases (`INDEX.md` Arcs).

## Verification

At `11e4e56`: `pytest` 2817 passed, 10 skipped; `ruff check`, `mypy`, `docstring-lint.py`,
`spec-lint.sh` and `docs-lint.sh` green. Not run here: the Linux CI matrix and the real-service
integration tests.
