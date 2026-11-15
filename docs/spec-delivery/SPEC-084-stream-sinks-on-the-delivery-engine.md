# Completed Spec — SPEC-084: Stream Sinks on the Delivery Engine: Never Resend What the Stream Took

## What was completed?

- **Both stream sinks on `deliver`** (FR-001), in `sinks/stdout.py`. `StdoutSink` holds
  `log_foundry_counters` and `log_foundry_stop_signal`, `emit` is the adapter, and `deliver` is
  one round through `_engine.one_round`. `StderrSink` keeps only its `__init__`, which still binds
  `sys.stderr` at construction. `losses()` is `None` until something is counted. Neither sink has
  a `max_retries`, so the inline `emit` and the orphan path run one round. `_unencodable`,
  `_counter_lock` and the `Deferral` that used them are gone.
- **The round** (FR-002). It runs a pre-flush, then one `write` per encodable line, then one final
  `flush()`, and it judges each event by which call raised (`_lose`). A handed line is `UNKNOWN`
  and is never resent. A line that was not handed is `RETRYABLE` or `REJECTED`. A raise that is
  not an `OSError` took nothing. On the exact unbuffered shape (`_unbuffered`), a `write` that
  returned is `ACCEPTED`. The fault latch, `_faulted`, makes a handed line `REJECTED` until a
  round's final flush returns.
- **The predicate and the lines** (FR-003). `_retryable_error` is the `errno` set
  `_RETRYABLE_ERRNOS` (`ENOSPC`, `EDQUOT`, `EFBIG`, `EIO`, `EAGAIN`). Every encodable event is
  marked sent at the round's start. A losing round registers up to two `Lost` groups, the handed
  lines `ALWAYS` and the rest `LAST` or `ALWAYS`, each written with `raised_line`.
- **The raise** (FR-004). A losing round registers `delivery_error(err)` through SPEC-082's
  `RunContext.raise_on_failure`, so a failed inline run raises the stream's own class and
  `SinkDeliveryError`. The engine gained nothing. The sinks have no closed guard and no fork hook,
  and `close()` only flushes.
- **Tests**: `tests/test_stream_sink_deliver.py` (FR-001 to FR-004, with a call-recording stream
  double) and `tests/test_stream_sink_partial_write.py`. The second file is POSIX-only. It holds
  the `RLIMIT_FSIZE` proof at six caps in three modes, the dead pipe at `buffer_size=4096`, the
  unbuffered shape (matched by exact type, so a subclass gets the general rule), and its
  sustained fault.
- **The records**: invariants 2 and 7. In `docs/decisions/sink-delivery/`, three decisions are
  rewritten and one is new, *A stream sink reports what the stream took and never resends it*.
  Also the inventory rows, `docs/release-notes/v1.2.1.md`, and `architecture.md` §12's
  staleness note.

**Deviations the plan review settled (2026-11-15), each recorded in the spec's revision history.**

- **The floor test's name.** The draft named `test_a_tree_of_twelve_fails_the_floor`. The test as
  it stood after SPEC-082 was `test_a_tree_of_fourteen_fails_the_floor`, and it is now
  `test_a_tree_of_fifteen_fails_the_floor`.
- **`StdoutSink` joins `tests/test_classify.py`'s `_SWEEP_EXCLUSIONS`, not `ENGINE_EXCLUSIONS`**,
  and `_LOOP_FLOOR` moves from 13 to 14. `ENGINE_EXCLUSIONS` would have taken the sink out of the
  engine-round roster altogether, and then nothing would check that its round asks
  `is_retryable(self._retryable_error, err)`. `_SWEEP_EXCLUSIONS` skips only the runtime sweep,
  which assumes a sink `max_retries` and drives `ConnectionError`. This matches how SPEC-082 took
  the file sinks.
- **`StderrSink`'s docstring** repeated the sentence saying nothing is left pending between calls,
  beside its own client-buffer claim. It changes the way `StdoutSink`'s does, and FR-004 now names
  it.

**Two calls made in the build that the spec leaves open.**

- **The latched group's line** reads `<Class>, N attempt(s), <Type>; after an earlier fault; they
  may have landed`. The spec fixes only the ending. `; after an earlier fault` was chosen to
  distinguish this line from the `UNKNOWN` group's `; outcome unknown`. No test asserts the
  latched line's text: a grep for `earlier fault` over `tests/` finds nothing.
- **On macOS, a line-buffered dead pipe raises at the pre-flush** of every batch after the first,
  so its batches fail without the latch. Only the block-buffered dead-pipe case reaches the latch
  through a real stream. This was re-run for this record on Python 3.13.1: block-buffered, every
  batch raised at a `write`; line-buffered, the first raised at a `write` and the next two at the
  pre-flush. The latch's own criterion is held by the double in
  `test_the_latch_rejects_handed_lines_until_a_clean_round`.

## What changed from earlier specs?

- **SPEC-069 FR-001 and FR-004.** The stream sinks' unencodable events are no longer counted in a
  private `_unencodable` under a `Deferral`. They land in the engine's `dropped_events` when a run
  settles, and in `failed_events` with the rest when it fails. The stream sinks now count `failed`
  for the first time, once per run.
- **SPEC-070.** The predicate reaches the stream sinks. `EPIPE`, `EBADF`, an `errno` of `None`, a
  closed stream's `ValueError` and any exception that is not an `OSError` cost one attempt, where
  1.x's worker resent the batch on every raise.
- **SPEC-036 FR-002.** Both docstrings keep the no-client-buffer claim and drop "nothing … is left
  pending between calls".
- **SPEC-075.** A `TransformSink` or `FilteringSink` around a stream sink now offers `deliver`, so
  a caller-code failure writes the engine round's `absorbed a failure while …` line in place of
  the 1.x `lost` line. `test_model_b_a_wrapper_turning_reporting_loses_nothing` reads
  `failed=15`, where it read 9.
- **Rosters.** Engine sinks go from 15 to 16 and engine rounds from 13 to 14. `sinks/stdout.py`
  leaves `DEFERRING_SITES`, `StdoutSink` leaves `_PRIMITIVE_HOMES`, and both sinks join
  `ENGINE_GAINED`. The `stdout` rig and the fork-lifecycle cases read the counters' lock and
  `dropped_events`.

## Residues

These are the seven residues in the spec's *Overview*, each with its measurement there and in
invariant 7:

- a torn prefix that the next line joins;
- a short write on the unbuffered shape;
- the straddling batch, which settles;
- the latch over-counting a landed line;
- the latch handing a line to a catcher that resends whole;
- another writer's fault making a dropped line read `ACCEPTED`;
- a fork's shared stream buffer.

## Timing (FR-002)

A 1,000-event batch went through `StdoutSink.emit` into a real file opened with `open(path, "w")`,
which was truncated after each run, outside the timer. The events were assembled-shape events of
410.89 bytes a line on average. Each figure is the median of 100 runs, timed with
`time.perf_counter`, after one warm-up `emit`. The nine pairs alternate which side runs first.

The two sides:

- **Before:** 1.x's `sinks/stdout.py` at `9b54b370`, loaded as a module of its own.
- **After:** the built sink at `c96c1068`. That commit is from before the rebase, and its `src/`
  is identical to `68035e3a`'s (`git diff c96c1068 68035e3a -- src/` is empty).

The run used Python 3.13.1 on macOS 27.2.1, arm64, with 10 cores and no containers running. The
one-minute load was still falling from a test run, and each row gives the reading taken after its
pair.

| pair | first | before (ms) | after (ms) | ratio | one-minute load |
|---|---|---|---|---|---|
| 1 | before | 2.224 | 3.351 | 1.51 | 6.3 |
| 2 | after | 2.289 | 3.346 | 1.46 | 6.3 |
| 3 | before | 2.256 | 3.285 | 1.46 | 6.3 |
| 4 | after | 2.181 | 3.232 | 1.48 | 6.1 |
| 5 | before | 2.227 | 3.214 | 1.44 | 6.1 |
| 6 | after | 2.158 | 3.191 | 1.48 | 6.1 |
| 7 | before | 2.188 | 3.204 | 1.46 | 6.1 |
| 8 | after | 2.189 | 3.213 | 1.47 | 6.1 |
| 9 | before | 2.191 | 3.242 | 1.48 | 6.1 |

The median of the nine ratios is 1.47, against a bound of 2.0.

## Release notes

The caller-visible changes are in `docs/release-notes/v1.2.1.md`, under *Changed*.

## Verification

The six local gates and the two diff reviews run on this branch before the push, and the PR
records them. Every workflow runs on `ubuntu-latest`, so CI runs the POSIX-only file on Linux,
and the local runs are on macOS. Windows, which no workflow
covers, skips that file.
