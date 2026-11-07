# Completed Spec — SPEC-071: Shared Kit, Part One: import_extra, One Injected-Client Refusal and the Chunker Rule

## What was completed?

Five FRs, built from `2368ade3` in six commits ending at `607f1f46`. The sinks' repeated lazy
imports, injected-client refusals and SQS's chunk boundary each moved into one shared helper, so a
new site cannot be written locally, and the argument doors stopped asking a value what it is.

- **Pins first** (FR-001): the integration roster's lazy-import population is held by name, and
  `_SITE_FLOOR`'s `_lifecycle` entry rose from 43 to the derived 44, both before any `src/` change.
  `tests/test_extras_sites.py` and `tests/test_sinks_sqs_chunks.py` were recorded against the
  unmodified code and not edited after.
- **`import_extra`** (FR-002, `sinks/_extras.py`): seventeen sites in fourteen modules import their
  extra through it, and a missing one raises the private `MissingExtraError`, a
  `ModuleNotFoundError` naming the extra and its `pip install`. `AzureEventHubsSink` resolves
  `EventData` at construction, so an injected producer without the extra is refused there. The
  `type: ignore[import-not-found]` comments and the `google.cloud.*` mypy override are gone; `mypy
  --strict` is clean with every extra installed. Guard: `tests/test_extras_roster.py`.
- **One injected-client refusal** (FR-003, `sinks/_args.refuse_with_injected`): every option a
  sink builds its client from is refused beside an injected one, nine of them newly, with one
  message; `PostgresSink.connect_timeout` defaults to `None`, meaning 5. `SQLiteSink.database` is
  the recorded exception. Guard: `tests/test_injected_client_roster.py`, which derives the options
  from the build path and fails closed.
- **`SQSSink._chunks` on `chunk_items`** (FR-004): the costed size is carried on `_Prepared.size`;
  `HTTPSink._take` and Azure's batching keep their own. Guard: `tests/test_chunk_helper_roster.py`.
- **The type doors on the real type** (FR-005): the seven doors and `_retry._exact_number` test
  `issubclass(type(v), T)` and name the stored type name; the HTTP sinks' `auth` routes the same
  way; `sinks/_args.require_callable` refuses a non-callable at the seven callable arguments. Guard:
  `tests/test_type_door_roster.py` (ten doors, exact), table: `tests/test_type_doors.py`.

Records: decisions *An argument door asks the type, never the value* and *A sink imports its extra
through `import_extra`, and a missing one is refused naming it*, both new in
`docs/decisions/sink-delivery/`; the bad-argument decision there refined (build options beside an
injected client, and a type refusal's reach to stored callables); invariant 13's first *Recorded
exceptions* line; `architecture.md` §12's `isinstance`-doors entry moved to *Resolved*.

**Deviation:** `tests/test_numeric_doors.py`'s four setup sites were edited (spec amended at
`4684e985`); no assertion or message in it changed.

## What changed from earlier specs?

- The five existing injected-client refusals (SPEC-043, SPEC-049) keep their prefix; only the tail
  changed, now shared.
- In `ClickHouseSink` and `RabbitMQSink` the refusal still precedes `max_retries`'s type judgement
  (SPEC-066), so a newly refused option beside a float `max_retries` is now a `ValueError`.
- SPEC-016's `SQSSink` refuses a non-callable id source at construction, on a standard queue too.
- SPEC-056's `test_the_mypy_accommodation_is_in_place` was deleted with the mechanism it pinned.

## Verification

The six gates locally, and `mypy --strict` with every extra installed on Python 3.12, measured at
`aeb0f0ab` and again at `607f1f46`. Owed on the PR: the Integration workflow's service run,
which the `src/log_foundry/sinks/**` edits trigger and which cannot settle before the push.
