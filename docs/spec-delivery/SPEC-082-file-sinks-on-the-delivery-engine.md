# Completed Spec — SPEC-082: File Sinks on the Delivery Engine: Resend Only What Did Not Land

## What was completed?

- **The engine addition** (Phase 1), in `sinks/_engine.py`. `RunContext.raise_on_failure(error)`
  registers the error the inline adapter raises, by identity, when the run fails. It is not
  cleared between rounds, so the last call wins; `_mark` and `_rewind` carry it as a sixth member;
  `_close` drops it; `Delivery.raise_on_failure` carries it out of a failed run, and a last-round
  refusal takes precedence. SPEC-084 reuses it.
- **Both file sinks on `deliver`** (FR-001, FR-003, FR-004), in `sinks/file.py`. `FileSink` and
  `RotatingFileSink` hold `log_foundry_counters` and `log_foundry_stop_signal`, `emit` is the
  adapter, and `deliver` is one round through `open(path, "ab", buffering=0)`. A round joins its
  lines, writes them, and judges each event by the counts the write calls returned. The retry
  predicate is an `errno` set. `losses()` is `None` until something is counted.
- **The tail** (FR-002). `_Tail` is the unwritten rest of a line a write cut partway. It is
  written first in every round and in the release a `close()` runs, and before any rotation. The
  run keeps `(event, tail)` in its notes and finds its torn occurrence as the first offered event
  that is that object. A torn event the run will not be offered again is `UNKNOWN`.
- **The stored count** (FR-002). `_take` stores each write call's return with
  `counts.extend(map(stream.write, (chunk,)))`. One idempotent step (`_shorten` for a tail,
  `_Writes._judge` for lines) brings the tail, `_size` and the owed byte order mark to the stored
  counts, and runs again when anything passes through.
- **`RotatingFileSink`** (FR-003) stages lines and writes them before a rotation is tried.
  `_size` is set from the bytes taken. A reopen that fails after a failed rotation is reported
  per event, which closes the residue `_rotate_or_continue` recorded.
- **The fork hook** (FR-005) drops the inherited tail, reopens in append mode and closes the
  inherited handle. `_reopen_discarding` and its `dup2` step are gone.
- **The records**: invariants 2, 7 and 12; six decisions in `docs/decisions/sink-delivery/`
  rewritten or given a History entry, the pipeline decision on what a child inherits, and one new
  decision, *A file write reports each event by the bytes taken, and a torn line is finished
  once*; `architecture.md` (five sites); `README.md` (two sentences); the inventory rows.

**Deviations from the spec, each a reversible technical call.**

- The write sequence raises what stopped it and each sink's own `_round` holds the
  `except Exception` that asks its predicate. `tests/test_classify.py`'s engine-round roster
  requires that shape. `_take` therefore catches nothing.
- The torn occurrence is identified by object identity and order, not by a run position, which a
  sink handed only a round's batch cannot know. Every position before a tear is final, so the
  first offered event that is the torn object is the torn occurrence.
- Lines end in `os.linesep`, which is what 1.x's text stream wrote on every platform.
- A write call that returns no count (`None`, zero, out of range) ends the sequence as `EAGAIN`,
  which is retried, so the loop cannot spin on a handle that takes nothing. FR-002 has said so
  since the diff review.
- A round of nothing encodable writes nothing, a pending tail included.
- The FIFO and real-signal criteria are in their own file, `tests/test_file_sink_interrupt.py`,
  each case in a child process, because a handler that raises in the test runner's main thread
  would interrupt the runner.
- Phases 1 and 2 landed as one commit after the engine addition's. The 1.x `RotatingFileSink`
  could not share the module's fork hook, test rigs and roster counts with the new `FileSink`.
- **One divergence from 1.x the spec's rule causes.** The spec owes a byte order mark only on a
  seekable handle at offset 0. On a regular file that is what 1.x wrote for the three codecs
  that have a mark, and on a pipe it is what 1.x wrote for `utf-16` and `utf-32`. For
  `utf-8-sig` on a pipe it is not: 1.x wrote the mark on every open, and the built sink writes
  none. The rule stays as written for this reason: measured at `a0f44b5f` on Python 3.13.1,
  macOS, a second `FileSink` opened on the same FIFO wrote the mark again, so a reader that
  stayed across the two found a mark in the middle of its stream.
- **One other difference in bytes, found by the sweep that checked the mark.** For every codec
  name in `encodings.aliases` on Python 3.13.1, into an empty and a non-empty regular file, the
  built sink writes what a text stream opened in append mode writes, as 1.x's sink did, except
  for the `iso2022_*` codecs into a non-empty file. There the text stream wrote the three bytes
  `ESC ( B` ahead of its first line and the sink writes none. The spec's revision history
  records both.

## What changed from earlier specs?

- **SPEC-069 FR-004.** The file sinks no longer count `len(batch)` into a private `_failed` per
  attempt. `failed` counts each event that did not land once per run. Their line gains
  `N attempt(s),`. The dual-typed raise stays on an inline `emit` whose run landed nothing, built
  now for any `Exception` the handle raises.
- **SPEC-048 FR-006.** Flush-before-rotate became write-before-rotate. Its three tests still
  pass, but on an unbuffered handle they do not guard the ordering;
  `tests/test_rotating_file_sink_deliver.py`'s first two tests do.
- **SPEC-039 FR-004.** The file sinks' child strands no buffer. The tests that forked "between a
  write and its flush" are retired and restated on the tail in `tests/test_file_sink_fork.py`.
- **SPEC-070.** The file sinks' predicate is the first that does not accept builtin
  `ConnectionError` and `TimeoutError`. They are excluded by name from `tests/test_classify.py`'s
  retry sweep, whose four properties `tests/test_file_sink_deliver.py` holds for them.
- **SPEC-074 and SPEC-076 rosters.** Engine sinks 13 to 15, engine rounds 11 to 13, `deliver`
  lockers six to eight, `emit` lockers three to one; the file sinks left the deferral roster and
  the emit-only closed population.
- **The worker's 1.x loop** has no file-sink guard any more. `tests/test_final_refusal.py`'s
  `subclass` shape and its refusal-after-a-transient-failure test run on `SQLiteSink`.

## Residues

- A run the worker declines, or a sink closed during a backoff, after a round that reported the
  torn event `RETRYABLE` can count that event in `failed_batches` as well as `failed`. Nothing is
  written twice: `close()` finishes the line.
- An exception a signal handler raises after the write section has finished and before `emit`
  returns reaches a retrying caller with the batch on disk, as in 1.x.
- A second exception landing while the idempotent step runs for the first is not covered.
- Under `MultiSink`, a file child's run that landed nothing raises on each of the worker's
  attempts and its `failed` moves once per `emit` call, as every engine child's does.

## Timing (FR-001)

A 1,000-event batch through `FileSink.emit`, 1.x's module at `a0f44b5f` against the built one,
the spec's events (315.78 bytes a line on average, `utf-8`), Python 3.13.1 on macOS, 10 cores.
Each figure is the wall-clock median of 100 runs; pairs alternate which side runs first.

| pair | before (ms) | after (ms) | ratio | one-minute load |
|---|---|---|---|---|
| 1 | 1.76 | 2.58 | 1.46 | 3.5 |
| 2 | 1.72 | 2.56 | 1.49 | 3.5 |
| 3 | 1.72 | 2.56 | 1.49 | 3.4 |
| 4 | 1.73 | 2.57 | 1.49 | 3.4 |
| 5 | 1.72 | 2.58 | 1.50 | 3.4 |
| 6 | 1.73 | 2.57 | 1.49 | 3.4 |
| 7 | 1.72 | 2.59 | 1.50 | 3.4 |
| 8 | 1.73 | 2.59 | 1.49 | 3.4 |
| 9 | 1.73 | 2.59 | 1.50 | 3.4 |

The median of the nine ratios is 1.49, against a bound of 2.0.

## Release notes owed

No pending file existed in `docs/release-notes/` on `main` when this spec completed, so the
caller-visible changes are recorded here for the release after 1.2.0:

- A `FileSink` or `RotatingFileSink` write that fails partway returns where it raised. What did
  not land is counted in `losses().failed`; through the worker it is resent, and only it.
- On a run of one round, an inline `emit` or a `MultiSink` child, a partial write loses the rest
  of that batch, counted. 1.x could deliver it, with the head of the batch on disk twice.
- `failed` counts each lost event once per run, not once per worker attempt.
- An error no resend cures (`EACCES`, `EPERM`, `EROFS`, an `OSError` with no `errno`, any
  exception that is not an `OSError`) costs one attempt. 1.x's worker resent on every raise.
- A closed file-sink subclass that inherits `emit` is not resent, even if it would re-open
  itself. A subclass that wants the resend overrides `emit`.
- The loss line reads `<Class>, N attempt(s), <Type>` where 1.x wrote `<Class>, <Type>`, each
  followed by `errno=N` only when the exception carries one.
- A codec that is not a text encoding is refused at construction, now before any file is
  created.
- Both sinks gain `deliver`, `log_foundry_counters` and `log_foundry_stop_signal`.
- `FileSink(fifo, encoding="utf-8-sig")` no longer writes a byte order mark into a pipe. 1.x
  wrote one on every open, a second open of the same FIFO included. `utf-16` and `utf-32` wrote
  none into a pipe and write none now.
- A third-party wrapper whose `emit` calls the file sink's `emit` is a one-round caller too,
  with the inline `emit` and a `MultiSink` child. When a write lands part of a batch, the rest
  is lost and counted, and `emit` returns. The count shows in the file sink's own `losses()`
  and in its one `lost N event(s); FileSink, 1 attempt(s), OSError errno=N` line, and nowhere
  the worker reads: `flush()` returns true, `failed_batches` stays 0, and `health().sink` stays
  `None` for a wrapper that offers no `losses()`. In 1.x the raise reached the worker through
  the wrapper, and its resend put the head of the batch on disk twice.
- An orphan log, one written outside any span, that a file sink lands nothing of is announced by
  the engine's line. The sink writes `lost 1 event(s); FileSink, 1 attempt(s), OSError errno=N`
  and the orphan path then writes
  `lost 1 event(s); orphan log not delivered after 1 delivery round(s)`. In 1.x, for the same
  `ENOSPC`, the sink's line had no `1 attempt(s),` and the second line was
  `absorbed a failure while emitting an orphan log (OSError); the event was lost`. `orphan_lost`
  and `losses()` count the same as before.

## Verification

The six local gates were run on the branch. Beyond them, what CI runs on it: the whole suite
under Python 3.12 as well as 3.13 on macOS, and under both on Linux in `python:*-slim`
containers; the integration suite against the nine real services, and the unit suite in the
all-extras environment. Forty-six mutants of the engine addition, `sinks/file.py` and the
worker's final-refusal guard were each killed by a whole test file for the stated reason; the
mutant that stores a write call's count with a Python-level assignment failed all four FIFO
tests on both Python versions, on macOS and on Linux. The fork, real-signal and subprocess tests
were run 25 times in a row under `pytest -n 12` at a one-minute load of 12 or more with no
failure. Not run: the GitHub runner itself, and Windows, which no workflow covers.

The tests added at the first diff review were each watched to fail under a planted mutant: the
fork hook's re-test of the byte order mark, a round of nothing encodable under a pending tail
for each sink, and the real-worker twin of FR-004's second criterion.
