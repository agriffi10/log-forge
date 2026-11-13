---
type: Spec
id: SPEC-084
title: "Stream Sinks on the Delivery Engine: Never Resend What the Stream Took"
description: "Moves StdoutSink and StderrSink onto deliver, so a stream that raises partway reports each event by what the stream was handed, counts what did not land once per run, and is never handed a line again that it already took, which closes the duplicate lines the worker's whole-batch retry writes today."
spec_status: Draft
depends_on: [SPEC-028, SPEC-031, SPEC-032, SPEC-069, SPEC-070, SPEC-072, SPEC-074, SPEC-076, SPEC-082]
---

## Overview

`StdoutSink` and `StderrSink` write each line of a batch to their stream and then flush once. When
the stream raises partway (a full disk under a redirected stdout, a closed pipe, a file-size
limit), the exception escapes uncounted: the two sinks count no `failed` at all, so `losses()`
reports only `dropped`, and only the worker's abandon line says anything. The worker then resends
the whole batch, and every line the stream had already taken is written again. Measured at
`864d71e5` with a file-size cap partway through a 200-event batch and lifted before the next
round, 43 to 200 of the 200 events landed twice, depending on the cap and the stream's buffering
(the table is under *Revision history*). 1.2.0 ships this as an accepted edge, recorded as an
exception to invariant 7.

SPEC-082 closes the same edge for the file sinks by writing through a descriptor of their own and
counting the bytes each write call took. The stream sinks cannot do that. They write to a `TextIO`
the caller or the process owns, with no descriptor of their own, so they cannot see what reached
the operating system. A `write` that returned means only that the stream took the line, and after
a raise the stream does not say whether it kept or discarded what it had not yet written. This spec
therefore puts each stream sink on `deliver(batch) -> Verdict` (SPEC-072) with a verdict rule built
on the one thing the sink can see: **which lines it handed to the stream**. A line it handed is
never handed again. A line it did not hand is resent when a resend can cure the error. One stream
shape gets an exact rule instead: the unbuffered stream `python -u` gives `sys.stdout`, where a
`write` that raised is known to have taken nothing (FR-002). A run that fails raises, as 1.x did,
an error every 1.x `except` clause still catches. That raise reuses SPEC-082's private engine
addition, `RunContext.raise_on_failure` and `Delivery.raise_on_failure`, so this spec depends on
SPEC-082 and builds after it. It targets the first release after 1.2.0.

**What remains, stated plainly.** Duplicates are closed in this sense: no line the stream took is
written again *by the sink*. Six things remain, because the sink cannot see inside a stream it does
not own:

1. **A torn prefix.** A stream that discards the unwritten tail of a line it cut leaves that line's
   prefix in its output. The next line anyone writes to the stream joins the prefix, and the joined
   line does not parse. Measured at the block-buffered 10,500 and 12,000-byte caps, where that next
   line belonged to an event the run called `ACCEPTED`. It can recur on every fault, not once per
   cap: on a non-blocking stream, `BlockingIOError` left 3 torn lines in one run.
2. **A short write on the unbuffered shape.** `FileIO.write` can return a short count, which
   `TextIOWrapper` ignores. That line is torn and called `ACCEPTED`, and the next line joins it.
   Measured with the review's prototype: at every cap, one unparseable line holding two `ACCEPTED`
   events. 1.x has this too.
3. **The straddle.** On a buffered stream, the batch during which a fault begins hands at least one
   line before the raise (it did in all ten sustained-fault configurations probed). That run
   settles, with its handed lines `UNKNOWN`, counted and announced, and the next batch fails at the
   pre-flush and raises. This is the analogue of invariant 7's recorded NATS case: only the batch
   that straddles the fault's onset is counted rather than raised.
4. **A stream that discards its buffer on every fault** passes the next round's pre-flush. A
   sustained fault there settles each batch, with its handed lines `UNKNOWN`, rather than failing
   it. Measured at the block-buffered 10,500-byte cap, where rounds after the first raised at a
   `write`, not at the pre-flush. In the other 9 of the 10 sustained faults probed, every round
   after the first raised at the pre-flush.
5. **Another writer's fault.** Another writer on the same stream, such as the application's own
   `print()` to a redirected stdout, can hit a fault that makes the text layer drop this section's
   pending lines. This section's final `flush()` then returns, and those lines are called
   `ACCEPTED` though none landed: 67 lines in the review's probe. 1.x has this too.
6. **A fork.** A forked child's flush writes the parent's kept bytes too, so a line can still appear
   twice through the stream the two processes share, as `architecture.md`'s fork-hazard paragraph
   already says.

Phase 3 rewrites invariant 7's partial-write exception to drop the stream sinks' duplicates and
record these six residues in their place.

## Scope

### In Scope

- `StdoutSink` and `StderrSink` (`src/log_foundry/sinks/stdout.py`) offer `deliver`, hold
  `log_foundry_counters` and `log_foundry_stop_signal`, and make `emit` the engine adapter, as
  SPEC-074's engine sinks do. `StderrSink` inherits all of it.
- The round: a pre-flush, one `write` per line, one final `flush()`, and each event's outcome
  decided by which call raised, with the exact rule for the unbuffered shape (FR-002).
- A retry predicate for the stream sinks, counting once per run, and each lost round's line (FR-003).
- The inline `emit` raising the dual-typed error through SPEC-082's `RunContext.raise_on_failure`.
  This spec adds nothing to the engine itself.
- Both sinks keep accepting after `close()`, and keep no fork hook.
- The records listed under Phase 3.

### Out of Scope

- The file sinks (SPEC-082) and `SQLiteSink`.
- **Flushing per line.** It is rejected for cost. Measured at 2.725 ms against 1.020 ms per
  1,000-event batch into a file, and 1.999 ms against 0.961 ms into `/dev/null` (encode included,
  on one machine in one run). It would cut a fault's `UNKNOWN` events to one line, but the default
  sink would run two to three times slower on every batch to buy precision on a fault path. With one
  flush per round, a fault on a buffered stream can cost up to the stream's buffer's worth of
  handed lines as `UNKNOWN`: counted and announced, never duplicated.
- **Writing a guard newline after a raise** to terminate a possibly torn prefix. It is rejected
  because the sink cannot tell a kept tail from a discarded one. In the common case, every
  line-buffered cap measured, it would write a blank line into a stream it does not own, and the
  torn prefix still would not parse.
- **Reaching inside the stream for what it holds.** The sink reads only documented attributes of
  exact CPython types (`write_through` and `buffer`), and only to recognise the one shape where a
  raise is known to have taken nothing. It never reads byte counts or a buffer's contents. Any other
  `TextIO` gets the general rule, because it is a stream the caller or the process owns, and even
  CPython's buffered layers do not say whether a raise kept or dropped bytes.
- A transport lock (SPEC-028 FR-002 stands), a closed guard (SPEC-032: they release nothing), a fork
  hook (they own no buffer), a `max_retries` attribute (the inline `emit` stays one round, as 1.x's
  `emit` was one call), durability (no `fsync`; `ACCEPTED` means what 1.x's returned flush meant),
  and a `log_foundry_idempotent` declaration (*No shipped sink declares itself idempotent*).
- `architecture.md`'s fork-hazard paragraph on `StdoutSink`. It stands as SPEC-082 rewords it,
  since the stream's buffer is still the process's.

---

## Functional Requirements

### FR-001: Both stream sinks offer `deliver` and join the engine

#### Description:

`StdoutSink` defines `deliver(batch) -> Verdict` as one round per call, through
`_engine.one_round`. It holds `log_foundry_counters: SinkCounters`, which the engine alone writes
(SPEC-072), and `log_foundry_stop_signal: threading.Event | None = None`. Its `emit` is exactly
`emit_via_deliver(self, batch)`, the body `tests/test_engine_sinks_roster.py` requires.
`StderrSink` defines none of these and inherits all of them. It keeps only its own `__init__`,
whose default of `sys.stderr` is resolved at construction, so SPEC-031 FR-003's binding is
unchanged for both.

`_unencodable` and `_counter_lock` retire with the `Deferral` that used them. `losses()` returns
`None` until something is counted, SPEC-069's rule, so a process that only logs clean events still
reads `health().sink is None`; otherwise it returns the counters' `losses()`.

Neither sink has a `max_retries`. The inline `emit` and the orphan path therefore run one round,
as 1.x's `emit` was one call, and the worker spends its own `max_retries` across rounds.

Both sinks stay out of the roster test's runtime list (`_RIGGED`). Its three runtime tests assume
that `losses()` equals `log_foundry_counters.losses()`, that it is never `None`, and that the sink
has a `failed` attribute. The first two contradict the `None`-until-counted rule above, and the
third contradicts FR-001's three public names. The runtime coverage of the stream sinks is
`tests/test_delivery_paths.py`'s pattern and `tests/test_stream_sink_deliver.py`.

#### Acceptance Criteria:

- [ ] `tests/test_engine_sinks_roster.py`'s AST scan finds `StdoutSink`, the class that defines
      `deliver`, with its floor raised from 15 (where SPEC-082 leaves it) to 16. It holds
      `StderrSink` by its check that no class below the one defining `deliver` defines `emit`.
- [ ] The worker, the orphan path and the inline adapter all reach `deliver` for both sinks, held by
      `tests/test_delivery_paths.py`'s pattern.
- [ ] A batch that fails nowhere writes into a `StringIO` exactly the bytes 1.x's `emit` wrote for
      the same batch, for both sinks.
- [ ] `losses()` is `None` after a clean batch, and neither sink has a `max_retries`, `_unencodable`
      or `_counter_lock` attribute.
- [ ] `StderrSink()._stream is sys.stderr`, and a stream swapped in by `contextlib.redirect_stderr`
      after construction receives nothing.
- [ ] Each sink's public names grow by exactly `deliver`, `log_foundry_counters` and
      `log_foundry_stop_signal`, held by `tests/test_sinks_encode.py`'s `ENGINE_GAINED`.
- [ ] These keep invariants 6, 10 and 2: the three paths run one engine, the surface only grows,
      and the counters have one writer.

### FR-002: A round reports each event by what the stream took

#### Description:

A round encodes its events once per run (`_encode.encode_round`). If nothing is encodable, it
touches the stream not at all, and every event is `DROPPED` (SPEC-069 FR-002). Otherwise it calls
`self._stream.flush()` first (the **pre-flush**), then one `write` per encodable line in batch
order, then one `flush()`. Each event's outcome follows from which call raised. On any stream:

| What happened | Encodable events |
|---|---|
| the pre-flush raised | none was handed to the stream: every one `RETRYABLE` or `REJECTED`, as the predicate (FR-003) answers, and no `write` is called |
| the pre-flush returned and the `write` of line `i` raised | every line up to and including `i` `UNKNOWN`; every later line `RETRYABLE` or `REJECTED` by the predicate |
| every `write` returned and the final `flush()` raised | every one `UNKNOWN` |
| every call returned | every one `ACCEPTED` |

**The unbuffered shape.** When a call raises, the round checks whether the stream is exactly the
shape `python -u` and `PYTHONUNBUFFERED=1` give `sys.stdout`: `type(stream) is io.TextIOWrapper`,
`stream.write_through` is true, and `type(stream.buffer) is io.FileIO`. The types are compared
exactly, so no caller code runs, and the check is read only after a raise, so a clean round pays
nothing. On that shape each `write` is one `write(2)`. A `write` that returned put its line in the
kernel, and one that raised took nothing, so the rows after the pre-flush change:

| What happened, on the unbuffered shape | Encodable events |
|---|---|
| the `write` of line `i` raised | every line before `i` `ACCEPTED`; line `i` and every later line `RETRYABLE` or `REJECTED` by the predicate |
| every `write` returned and the final `flush()` raised | every one `ACCEPTED` |

Without this rule, nothing is ever kept on that shape, so the pre-flush always passes, and every
run of a sustained fault settled with its first line `UNKNOWN` and never raised.

The outcome never reads `RunContext.last`. A line the stream was not handed has the same outcome
on every round, and a line it was handed is `UNKNOWN` on every round (on any stream but the
unbuffered shape).

**Why `UNKNOWN`, not `RETRYABLE`, for a handed line.** A `write` that returned means only that the
stream took the line, and a `write` that raised may have taken part or all of its own line.
Measured: a line-buffered stream at a 10,500-byte cap kept the 60-byte tail of a cut line and
finished it on the next flush. After a raise, CPython's text stream sometimes keeps the unwritten
bytes and writes them at its next flush, and sometimes discards them (its text layer also drops its
pending chunk). It never says which, so resending a handed line can duplicate it. The engine never
resends `UNKNOWN` for a sink that does not declare `log_foundry_idempotent` (SPEC-072), and neither
sink declares it. `ACCEPTED` means what 1.x's returned flush meant. No durability is added.

**Why the pre-flush.** It keeps invariant 7's "a sink that delivered nothing raises" on a buffered
stream. A stream that kept bytes from an earlier fault raises at the pre-flush of every later round,
before anything of that round is handed. So a sustained fault fails those runs, and they raise,
rather than settling with every line `UNKNOWN`. The batch during which the fault began is the
straddle (residue 3). Its measured cost is nil: 1.043 ms against 1.020 ms per 1,000-event batch
into a file.

**Concurrency.** The sinks still take no transport lock (SPEC-028 FR-002), so another section's
lines can interleave with this one's at line granularity, as today. When a raise comes from bytes
another section handed, this section's split still holds: the lines it handed are `UNKNOWN`, and
the lines it did not hand are `RETRYABLE` or `REJECTED`. What does not hold is `ACCEPTED` after
another writer's fault: the text layer can drop this section's pending lines, and when this
section's final flush then returns, they are called `ACCEPTED` though none landed (residue 5).

#### Acceptance Criteria:

- [ ] With a stream double recording call order, a pre-flush raising `OSError(ENOSPC)` on a round
      that is not the run's last returns every event `RETRYABLE`, and no `write` call is recorded.
- [ ] With the third line's `write` raising `ENOSPC` on a buffered stream, events 1 to 3 are
      `UNKNOWN` and the rest `RETRYABLE`.
- [ ] With every `write` returning and the final `flush()` raising `ENOSPC` on a buffered stream,
      every event is `UNKNOWN`.
- [ ] A clean round records exactly one `flush` before the first `write` and one after the last, and
      no other `flush`.
- [ ] An all-unencodable batch records no `write` and no `flush` call.
- [ ] POSIX only: an unbuffered-shape stream (`io.TextIOWrapper(io.FileIO(path, "w"),
      write_through=True)`) over a file capped at exactly two lines' bytes reports events 1 and 2
      `ACCEPTED` and the rest `RETRYABLE`. The same stream built from a `TextIOWrapper` subclass
      reports events 1 to 3 `UNKNOWN`, because the shape is matched by exact type.
- [ ] POSIX only: an unbuffered-shape stream over a file at its size cap fails three batches in a
      row through a real `Worker` (`failed_batches == 3`), and the inline `emit` raises for each of
      three batches.
- [ ] POSIX only: over a real pipe whose reader has closed (`SIGPIPE` ignored), block-buffered at
      `buffer_size=4096`, a first 200-event `emit` returns with `losses().failed == 200`, and a
      second raises an instance of `BrokenPipeError` and `SinkDeliveryError`.
- [ ] A double whose pre-flush raises `BrokenPipeError` makes the inline `emit` raise, and through a
      real `Worker` leaves `flush()` falsy and `failed_batches == 1`.
- [ ] POSIX only, in a subprocess: one 200-event batch of about 120-byte lines is sent through a
      real `Worker(batch_size=200)` at its default `max_retries`, to a `StdoutSink` over a real file
      under `RLIMIT_FSIZE` (`SIGXFSZ` ignored). This runs at each cap of 5,000, 8,192, 10,500,
      12,000, 16,000 and 20,000 bytes, in three modes:
      - block-buffered and line-buffered: `io.TextIOWrapper` over `io.BufferedWriter(raw,
        buffer_size=4096)`, `line_buffering` False and True, whose raw handle lifts the limit after
        its first `EFBIG`;
      - the unbuffered shape, the limit lifted when the first `deliver` returns, by the recording
        subclass that also reads the verdicts (a `FileIO` subclass would not be the shape).

      In every case:
      - no event appears in the file twice;
      - at most one line fails to parse, and the block-buffered 10,500 case has exactly one;
      - every event the run called `ACCEPTED` is in the file, except the events whose lines make up
        that one unparseable line;
      - `losses().failed` equals the run's `UNKNOWN` count, read from the verdicts `deliver`
        returned;
      - `failed_batches == 0`.
- [ ] The delivery doc records the median time of a 1,000-event batch through `emit`, before and
      after this spec, on one machine in one run. The after time is no more than 2.0 times the
      before. The review's prototype measured 1.52 to 1.69 times, mostly `encode_round`'s per-run
      cache, and the bound leaves headroom over that without admitting a regression past it.
- [ ] These keep invariants 7 and 2: no line the stream took is handed to it again, a sink that
      delivered nothing raises, and no event the stream was not handed is called `ACCEPTED`.

### FR-003: The stream sinks retry what a resend can cure, and count once per run

#### Description:

`_retryable_error(err)` answers `True` only for an `OSError` whose `errno` is one of `ENOSPC`,
`EDQUOT`, `EFBIG`, `EIO` or `EAGAIN` (which covers `BlockingIOError` from a non-blocking stream),
each resolved with `getattr(errno, name, None)` so a platform lacking one skips it. Everything else
is `REJECTED` at its first attempt: `EPIPE` (the reader is gone), `EBADF`, an `errno` of `None`, a
`ValueError` from a closed stream, a `UnicodeEncodeError`, and any exception that is not an
`OSError`. `EMFILE` and `ENFILE`, which SPEC-082's set holds, are left out because a stream sink
opens nothing. `EINTR` never reaches the sink, because CPython retries it internally (PEP 475).
This is SPEC-070's rule (*A sink declares what it retries, and anything else fails at once*)
reaching the stream sinks as they join the engine. 1.x's worker resent them on every raise.

**Counting.** An event that did not land is in `failed_events` once per run, which is new: 1.x's
stream sinks counted no `failed` at all. An `UNKNOWN` event is in `failed_events` as the recorded
upper bound, since it may land later. An unencodable event is `DROPPED`, counted in
`dropped_events` when the run settles. When the run fails, it is counted in `failed_events` with
the rest (`unencodable_in_run`, `When.FAILED`). That is the rule `_RedisSink`, `PostgresSink` and
SPEC-082's file sinks follow, so one rule holds for every encoding engine sink.

**The line.** At the start of each round, the sink marks every encodable event of the round as sent
(`_rounds.mark_sent`), as `_RedisSink` marks what it carried, so a line's attempt count includes a
round whose pre-flush raised. A round that lost events registers its lines through `_rounds.Lost`
with `raised_line`, by `Lost`'s rule: `UNKNOWN` and `REJECTED` events `When.ALWAYS`, `RETRYABLE`
events `When.LAST`. Each line names the sink's class, the round's exception type and, for an
`OSError`, `errno=N` (`_diag.errno_of`), never event data. The `UNKNOWN` group's line ends with
`; they may have landed`, the clause the HTTP family's `UNKNOWN` line already writes
(`"<Class>, outcome unknown; they may have landed"` in `sinks/http.py`), so it does not read as a
loss. The engine and the worker's abandon line add nothing to these lines.

#### Acceptance Criteria:

- [ ] Through the worker at `max_retries=3`, with a pre-flush raising `ENOSPC` on every round, one
      batch of `n` leaves `losses().failed == n` (not `4n`), `failed_batches == 1`, and `deliver`
      called 4 times.
- [ ] That same run writes exactly one line, naming the sink's class, `4 attempt(s)`, `OSError` and
      `errno=28`, and no event data.
- [ ] Through the inline `emit` (one round), a round whose third line's `write` raises `ENOSPC` on a
      buffered stream writes two lines: one for events 1 to 3 ending `; they may have landed`, and
      one for the rest.
- [ ] A round's line for its `UNKNOWN` events is written even when a later round of the run accepts
      the rest.
- [ ] Through the worker, a pre-flush raising `BrokenPipeError` (`EPIPE`) gets exactly one
      `deliver` call, and the engine's wait is never entered.
- [ ] A closed stream's `ValueError` is `REJECTED` at the first attempt.
- [ ] `losses()` is `None` after a clean batch. After a settled run with `d` unencodable events and
      `f` lost, it is `SinkLosses(dropped=d, failed=f)`. After a failed run of `n` events, of which
      `u > 0` were unencodable, through the worker at `max_retries=3`, it is
      `SinkLosses(dropped=0, failed=n)`.
- [ ] These keep invariants 2, 3, 7 and 11: each loss is counted once per run and announced, a
      non-retryable error waits nothing, a run that delivered nothing fails, and each line names a
      type, never a value, and says what happened.

### FR-004: The inline `emit` keeps 1.x's raise, and the sinks keep accepting after `close()`

#### Description:

Each round whose stream raised calls `RunContext.raise_on_failure` (SPEC-082) with the error that
`_driver_error.delivery_error` builds from the stream's exception: an instance of the stream's own
class and of `SinkDeliveryError`, with the original as `__cause__`. When the dual type cannot be
built, it passes the original. So when a run fails, the inline `emit` raises something every 1.x
`except` clause still catches; 1.x raised the stream's exception raw. A run that settles, because
something was `ACCEPTED` or `UNKNOWN`, returns.

There is no closed guard (SPEC-032: they release nothing). `deliver` after `close()` writes as
before and never refuses through `final_refusal`. `close()` still only flushes, raises whatever the
flush raises, and counts nothing.

There is no fork hook. The sinks own no buffer; the stream's buffer is the process's. Once
`_counter_lock` retires, `StdoutSink` constructs no primitive of its own (its counters' lock belongs
to `SinkCounters`), so its row leaves `tests/test_fork_roster.py`'s `_PRIMITIVE_HOMES`.

`StdoutSink`'s docstring keeps the claim that it keeps **no** client buffer (SPEC-036 FR-002), but
its sentence "nothing of this sink's is left pending between calls" becomes: the sink keeps
nothing, but the stream may still hold lines a run reported `UNKNOWN`.

#### Acceptance Criteria:

- [ ] An inline `emit` whose run fails on `BrokenPipeError` raises an instance of `BrokenPipeError`,
      `OSError` and `SinkDeliveryError`, with the original as `__cause__`.
- [ ] An inline `emit` whose run fails on a closed stream's `ValueError` raises an instance of
      `ValueError` and `SinkDeliveryError`.
- [ ] A run with an `UNKNOWN` event returns normally, with `losses().failed` equal to the `UNKNOWN`
      count.
- [ ] After `close()`, a batch into a `StringIO` lands and moves no counter, and
      `tests/test_sink_post_close.py`'s rows for both sinks pass unchanged.
- [ ] A `close()` whose flush raises raises that error and moves no counter.
- [ ] Two threads emitting 50 batches each into one sink over a `StringIO` leave every line parsing
      and each event in it once.
- [ ] Neither sink defines `reacquire_after_fork`. `StdoutSink`'s row leaves
      `tests/test_fork_roster.py`'s `_PRIMITIVE_HOMES`, and
      `test_every_class_holding_a_primitive_has_a_home` passes.
- [ ] A forked child completes an `emit` while a parent thread holds the sink's
      `log_foundry_counters._counter_lock`.
- [ ] `StdoutSink`'s docstring keeps the three claims `tests/test_sink_concurrency.py` reads:
      `**no** transport lock` with SPEC-028 FR-002, `**no** client buffer` with SPEC-036 FR-002,
      and `**adds no post-close guard**`. It no longer says nothing is left pending between calls.
- [ ] These keep invariants 1, 5, 7, 10 and 12: the dual type falls back to the original rather
      than failing, a close releases nothing twice, a failed run raises, every 1.x `except` still
      matches, and a child repairs the counters' lock.

---

## Data Model

```python
# src/log_foundry/sinks/stdout.py
class StdoutSink:  # StderrSink inherits every member; it overrides only __init__'s default stream
    _stream: TextIO                                         # unchanged: the caller's or the process's
    log_foundry_counters: SinkCounters                      # the engine's one writer (SPEC-072)
    log_foundry_stop_signal: threading.Event | None = None  # set by the library, as on every engine sink
    # retired: _unencodable, _counter_lock

_RETRYABLE_ERRNOS: frozenset[int]  # ENOSPC, EDQUOT, EFBIG, EIO, EAGAIN, each via getattr(errno, name, None)
```

## API / Interface Contract

```python
# Both stream sinks: the SPEC-074 shape
def deliver(self, batch: list[dict[str, object]]) -> Verdict  # one round, via _engine.one_round
def emit(self, batch: list[dict[str, object]]) -> None         # exactly emit_via_deliver(self, batch)
def _retryable_error(self, err: Exception) -> bool             # FR-003's errno set
def losses(self) -> SinkLosses | None                          # None until something is counted
def close(self) -> None                                        # flushes; raises what the flush raises
def _encode(self, event: dict[str, object]) -> str             # unchanged: json.dumps(event) + "\n"

# Module-level, read only after a call raised (the name may change in the plan)
def _unbuffered(stream: object) -> bool:
    return (
        type(stream) is io.TextIOWrapper
        and bool(stream.write_through)
        and type(stream.buffer) is io.FileIO
    )

# Used, not added (SPEC-082): RunContext.raise_on_failure(error) and Delivery.raise_on_failure
```

## File & Folder Structure

```
src/log_foundry/sinks/
└── stdout.py                         # both sinks on deliver; Deferral, _unencodable, _counter_lock retired
tests/
├── test_stream_sink_deliver.py       # FR-001 to FR-004 (new)
├── test_stream_sink_partial_write.py # the RLIMIT_FSIZE subprocess proof, three modes (new, POSIX only)
├── test_engine_sinks_roster.py       # StdoutSink in ENGINE_SINKS, floor 15 -> 16; neither sink in _RIGGED
├── test_fork_roster.py               # StdoutSink's row leaves _PRIMITIVE_HOMES
├── test_deferral.py                  # the sinks/stdout.py row leaves DEFERRING_SITES
├── test_sinks_encode.py              # ENGINE_GAINED gains both sinks
├── sink_rigs.py                      # the stream rigs read dropped_events on log_foundry_counters
├── test_fork_lifecycle.py            # the two stream-sink counter-lock cases read the counters' lock
├── test_public_surface.py            # the _config_lock test's docstring describes the new __init__
├── test_sinks_stdout.py              # kept; its flush counts are lower bounds
├── test_sinks_util.py                # kept
└── test_sink_post_close.py           # unchanged
```

What each listed test change is for:

- `tests/test_engine_sinks_roster.py`: `StdoutSink` joins `ENGINE_SINKS` and `FLOOR` becomes 16.
  `test_a_tree_of_twelve_fails_the_floor` hard-codes the floor in its expected message, so the
  message moves with it. Neither sink joins `_RIGGED`, for FR-001's reason.
- `tests/test_fork_roster.py`: once `_counter_lock` retires, `StdoutSink` assigns no primitive to
  `self`, so its `_PRIMITIVE_HOMES` row is stale and `test_every_class_holding_a_primitive_has_a_home`
  reports it as `STALE_HOME` until the row leaves.
- `tests/test_deferral.py`: `DEFERRING_SITES` lists the sites whose `emit` enters a `Deferral`, and
  its check reports a listed `emit` that enters none ("enters no Deferral"). The stream sinks' `emit`
  becomes the adapter and enters none, so the `sinks/stdout.py` row leaves, as SPEC-074 took its
  sinks off and SPEC-073 took `sinks/http.py`, and the table's docstring names SPEC-084 beside them.
- `tests/sink_rigs.py`: the `stdout` rig's counter becomes `dropped_events` on
  `sink.log_foundry_counters`, as the SPEC-074 rigs read it. Its `calls()` now also sees the
  pre-flush's `flush` entry.
- `tests/test_fork_lifecycle.py`: the two SPEC-069 cases that hold `sink._counter_lock`, and the
  positive control that puts it back, hold `sink.log_foundry_counters._counter_lock` instead.
- `tests/test_public_surface.py`: the assertion is unchanged, since `config.py` still calls only
  `StdoutSink()` under `_config_lock`. Its docstring's account of what `StdoutSink.__init__` does
  (binds the stream, creates the unencodable counter and its lock) is rewritten: it binds the
  stream and builds a `SinkCounters`, which creates a lock and adds itself to a `WeakSet`, and
  neither blocks.

## Implementation Phases

### Phase 1: The two sinks on `deliver`

- `StdoutSink`: `log_foundry_counters`, `log_foundry_stop_signal`, the round with the unbuffered
  shape (FR-002), the predicate, `mark_sent` and the lines (FR-003), the raise through
  `raise_on_failure`, `losses()`, `close()` and the docstring (FR-004). `StderrSink` keeps only its
  `__init__`.
- The test changes listed above, and `tests/test_stream_sink_deliver.py`.

### Phase 2: The end-to-end proofs and the timing

- `tests/test_stream_sink_partial_write.py`: the `RLIMIT_FSIZE` subprocess proof at every cap in
  all three modes, the unbuffered shape's sustained fault, and the closed-pipe straddle.
- The median 1,000-event `emit` time before and after, for the delivery doc.

### Phase 3: Records (the completion ritual)

- Invariant 7: rewrite the second recorded exception. The stream sinks' duplicates are gone.
  Record the six residues of *Overview*, each with its measurement: the torn prefix, the short
  write on the unbuffered shape, the straddle (beside the NATS case it mirrors), the stream that
  discards its buffer on every fault, another writer's fault, and the fork.
- Invariant 2: the stream sinks join the engine sinks whose `failed` lands once per run, and a
  handed line's `UNKNOWN` joins the engine's upper bound on `failed`.
- Rewrite these decisions in place (`docs/decisions/sink-delivery/`), each with a dated History
  entry:
  - *A sink's own driver error is counted, announced and raised as both types*: the stream sinks'
    half of the partial-write paragraph is resolved, with the residues named;
  - *An event a sink cannot encode costs only itself, and a batch of nothing encodable settles,
    counted*: the stream sinks' drops no longer land in `_unencodable` but in the engine's
    `dropped_events`;
  - *A raising `emit` counts nothing, nor does a `deliver`*: `stdout` leaves the roster of
    deferring sites.
- A new decision, `docs/decisions/sink-delivery/a-stream-sink-reports-what-the-stream-took-and-never-resends-it.md`:
  a stream sink reports a handed line `UNKNOWN` and never resends it, pre-flushes each round, reads
  the unbuffered shape exactly, and rejects per-line flushing, a guard newline and reading what the
  stream holds, with the measurements.
- `StdoutSink`'s docstring loses its accepted-edge sentence (Phase 1). The inventory rows for both
  sinks say they are engine sinks.
- The pending release notes of the release after 1.2.0 (`docs/release-notes/`). The caller-visible
  changes are:
  - the stream sinks now count `failed`;
  - a failed run raises an instance of both the stream's exception class and `SinkDeliveryError`;
  - a non-retryable error costs one attempt;
  - a buffered fault's handed lines are counted `failed` rather than written again;
  - on a buffered stream, the batch during which a fault begins returns where 1.x's `emit` raised,
    and later batches raise.

---

## Revision history

- **2026-11-13, at authoring.** Measured at `864d71e5` on Python 3.13.1, macOS: 200 events of 120
  bytes, the file size capped and lifted before the next round, up to four rounds. *Rule* is
  FR-002's round with the engine resending only `RETRYABLE`; *1.x* is the whole-batch resend.

  | buffering | cap | 1.x duplicated | 1.x torn | rule duplicated | rule torn | rule `UNKNOWN` | of which landed | `ACCEPTED` not parseable |
  |---|---|---|---|---|---|---|---|---|
  | block | 5,000 | 70 | 0 | 0 | 0 | 140 | 70 | 0 |
  | block | 8,192 | 70 | 0 | 0 | 0 | 140 | 70 | 0 |
  | block | 10,500 | 87 | 1 | 0 | 1 | 140 | 88 | 1 |
  | block | 12,000 | 100 | 1 | 0 | 1 | 140 | 101 | 1 |
  | block | 16,000 | 140 | 0 | 0 | 0 | 200 | 140 | 0 |
  | block | 20,000 | 200 | 0 | 0 | 0 | 200 | 200 | 0 |
  | line | 5,000 | 43 | 0 | 0 | 0 | 43 | 43 | 0 |
  | line | 8,192 | 70 | 0 | 0 | 0 | 70 | 70 | 0 |
  | line | 10,500 | 89 | 0 | 0 | 0 | 89 | 89 | 0 |
  | line | 12,000 | 102 | 0 | 0 | 0 | 102 | 102 | 0 |
  | line | 16,000 | 136 | 0 | 0 | 0 | 136 | 136 | 0 |
  | line | 20,000 | 170 | 0 | 0 | 0 | 170 | 170 | 0 |

  The rule duplicates nothing anywhere. With line buffering every `UNKNOWN` event landed, so
  nothing was lost. With block buffering the text layer discards its pending chunk on a raise, so
  some `UNKNOWN` events never landed; they are counted in `failed` and announced, and not resent,
  where 1.x resent them and duplicated others. A sustained-fault probe, the fault never
  lifted and four rounds each, over ten configurations: a pipe whose reader had closed, at 3 and 200
  lines, and a capped file at 3 lines under a 100-byte cap and 200 lines under 5,000 and 10,500,
  each block- and line-buffered. In 9 of the 10, every round after the first raised at the
  pre-flush; the block-buffered 10,500 cap, where later rounds raised at a `write`, is residue 4's
  case. Cost, per 1,000-event batch with encode
  included, on one machine in one run: the pre-flush round 1.043 ms against 1.020 ms into a file;
  a per-line flush 2.725 ms against 1.020 ms into a file and 1.999 ms against 0.961 ms into
  `/dev/null`.
- **2026-11-13, at authoring.** The brief this spec was written from said the roster would find
  both sinks and move its floor from 15 to 17. `tests/test_engine_sinks_roster.py` finds a class
  only when it defines `deliver` in its own body, so it finds `StdoutSink` alone, and holds the
  inheriting `StderrSink` by its check on subclasses; the floor goes to 16. SPEC-070 joins
  `depends_on`, since FR-003 is its rule.
- **2026-11-13, spec review.** The review found that the stream `python -u` gives `sys.stdout`
  never keeps bytes, so under the draft's rule a sustained fault there settled every batch and never
  raised. FR-002 now reads that one shape exactly, and a prototype with the shape rule failed all
  three batches of a sustained fault both on a file at its cap and on a broken pipe. The review also
  completed the residues (the short write, the straddle, another writer's fault, the fork), marked
  each round's events as sent at its start so a pre-flush line counts its attempts, kept the stream
  sinks out of the roster's runtime list and moved `StdoutSink` out of the fork roster's homes. It
  raised the timing bound to 2.0 times, because a prototype written to the spec measured 1.52 times
  the 1.x `emit`, and 1.69 times with `mark_sent` on every round, mostly `encode_round`'s per-run
  cache, which 1.25 times could not pass while the spec adds nothing to the engine.
