---
type: Spec
id: SPEC-082
title: "File Sinks on the Delivery Engine: Resend Only What Did Not Land"
description: "Moves FileSink and RotatingFileSink onto deliver, so a write that fails partway reports each event by the bytes the operating system took and the engine resends only the events that did not land, which closes the duplicate lines a partial write and the worker's whole-batch retry produce today."
spec_status: Draft
depends_on: [SPEC-048, SPEC-069, SPEC-070, SPEC-072, SPEC-074, SPEC-075, SPEC-076]
---

## Overview

When a `FileSink` or `RotatingFileSink` write fails partway through a batch (a full disk, a quota,
a file-size limit), the lines written before the failure stay on disk, the sink raises, and the
worker resends the whole batch. If the fault clears before the resend, every line that had already
landed lands again. Measured at `c21755f1` with `RLIMIT_FSIZE` capping the file partway through a
200-event batch and lifted before the second attempt, 69 to 200 of the 200 events landed twice,
depending on the cap. At one cap a line was also torn, because the buffered text stream sometimes
keeps the unwritten bytes and sometimes throws them away (the table is under *Revision history*).
1.2.0 ships this as an accepted edge, recorded as an exception to invariant 7 by the SPEC-069
follow-up fix of 2026-11-13.

This spec closes it for the two file sinks. Each one offers `deliver(batch) -> Verdict`
(SPEC-072), writes through a handle whose every write call says how many bytes the operating system
took, and reports an event `ACCEPTED` only when its whole line was taken. The delivery engine then
resends only the events that did not land. A line cut in half is finished, once, before anything
else reaches its file, and a run that cut a line is never handed back for a whole-batch resend. It
targets the first release after 1.2.0.

## Scope

### In Scope

- `FileSink` and `RotatingFileSink` (`src/log_foundry/sinks/file.py`) offer `deliver`, hold
  `log_foundry_counters` and `log_foundry_stop_signal`, and make `emit` the engine adapter, as
  SPEC-074's thirteen engine sinks do.
- Writing through an unbuffered binary handle, and deciding each event's outcome from the byte
  count the write calls returned.
- A torn line: the unwritten remainder of a line a write cut partway, kept by the sink and written
  before anything else reaches that file.
- `RotatingFileSink`'s rotation on the new write path, under SPEC-048 FR-006's flush-before-rotate
  rule.
- A retry predicate for the file sinks, and the inline `emit` keeping SPEC-069's raise as both
  `SinkDeliveryError` and the driver's own `OSError` class when nothing landed. That needs one
  private addition to the engine's run context and its `Delivery` record (*API / Interface
  Contract*).
- A forked child never finishing its parent's torn line.
- The records listed under Phase 4.

### Out of Scope

- **`StdoutSink` and `StderrSink` belong in a sibling spec, not this one.** They write to a stream
  the caller or the process owns, through the `TextIO` protocol, with no descriptor of their own, so
  the sink cannot see what reached the operating system. A write that returned means only that the
  stream took the line. Lines a failed `flush()` leaves in the stream's buffer may still be written
  by the stream later, so the honest outcome for them is `UNKNOWN`, not `RETRYABLE`. That is a
  different verdict rule from this spec's. They also hold no closed guard (they release nothing,
  SPEC-032), have no fork hook, and count no `failed` at all today. The sibling, SPEC-084, depends
  on this spec's `RunContext.raise_on_failure` and builds after it; this spec does not depend on
  the sibling. Until the sibling lands, invariant 7's recorded exception stays in
  place for the stream sinks.
- `SQLiteSink`. It keeps SPEC-069's driver-error path and stays off the engine.
- **Truncating the file back to where the batch began.** It is rejected because a forked child
  appending to the same path would lose the lines it wrote in between (invariant 12).
- **Flushing per line and reporting instead of raising.** It is rejected because the unwritten rest
  of the batch is lost rather than resent. Its cost is a second reason. The 1.2.0 ruling measured a
  per-line flush at about 11 times a single flush on a write-only basis (1.89 ms against 0.17 ms per
  1,000-event batch). This spec's review measured a per-line-flush `emit` at about 2.3 times the
  whole 1.x `emit`, encode included (2.02 ms against 1.48 ms written separately, on one machine in
  one run).
- Cross-process coordination. If another process appends to the same path between a partial write
  and the completion of its torn line, that process's first line joins the torn prefix and does not
  parse. A child forked while a tail is pending is that case too (FR-005). No single process can
  prevent this, and `FileSink`'s docstring already puts two processes appending to one path out of
  scope.
- Durability. `ACCEPTED` means the write call returned the bytes as taken, exactly what 1.x's
  `flush()` returning meant. No `fsync` is added.
- A `max_retries` on either file sink. The inline `emit` stays one round, as 1.x's `emit` was one
  call, and the worker's `max_retries` budgets the worker path, as it does today.
- `architecture.md` §12's item on a rotation that keeps failing being retried once per event.

---

## Functional Requirements

### FR-001: `FileSink` reports each event by the bytes the operating system took

#### Description:

`FileSink` defines `deliver` as one round per call (`_engine.one_round`). A round encodes the
round's events once per run (`_encode.encode_round`). It turns the encodable lines into bytes in
batch order, writes them through an unbuffered binary handle (`open(path, "ab", buffering=0)`,
still held as `_stream`) in a loop, and adds up what each write call returned. When a write raises
`OSError` after `k` bytes of the round's own lines have been taken:

- an event whose line's last byte lies below byte `k` is `ACCEPTED`;
- the event whose line has some bytes below `k` and some at or after it is the torn event (FR-002);
- every later event is `RETRYABLE` or `REJECTED`, as the retry predicate (FR-004) answers for the
  error;
- the torn event is `RETRYABLE` when the predicate retries the error and the round is not the run's
  last (`RunContext.last`), and `UNKNOWN` otherwise (FR-002).

A round in which every write returned is `ACCEPTED` throughout.

**Encoding.** The construction refuses a codec that is not a text encoding with `LookupError`,
as 1.x's `open()` did, so a codec such as `"base64"`, which the incremental encoder would accept,
is never reached at write time. A byte order mark is written once, as the first bytes of the first
line to reach an empty file. It belongs to that line's byte count and is still owed until a write
call has taken it, so a round that fails before it lands writes it in a later round.

The buffered text stream is retired because its state after a failure is not a statement of what
landed. Measured at `c21755f1`, CPython's `BufferedWriter` sometimes kept the unwritten bytes and
wrote them at its next flush. At other caps it discarded them, and the next write was appended to
a line cut in half. Either way it never says how many bytes reached the file. The unbuffered write
of the joined bytes measured no slower than today's `emit` (0.84 ms against 0.94 ms per
1,000-event batch, encode included, on one machine in one run). That is the write alone: the
engine's per-run bookkeeping around it, mostly `encode_round`'s per-run cache, makes the whole
`emit` slower than 1.x's, which is what the timing criterion below bounds. A write that the kernel accepts
partially returns the exact count before the next call raises: under a 10,500-byte cap, one call
returned `10500` and the next raised `EFBIG`.

#### Acceptance Criteria:

- [ ] With a handle double that takes `k` bytes of a round that is not the run's last and then
      raises `OSError(ENOSPC)`, the verdict's outcome tuple equals the expected one at each of these
      `k`:
      - `k = 0`: every event `RETRYABLE`;
      - a byte inside the third line: the third event torn and `RETRYABLE`;
      - the offset of the third line's newline: the third event torn with a one-byte tail;
      - the offset just past that newline: the third event `ACCEPTED`;
      - one byte short of the whole round.
- [ ] A subprocess sets `RLIMIT_FSIZE` and `FileSink`'s handle lifts the limit after its first
      `EFBIG`. A 200-event batch then delivered through the worker at its default `max_retries`
      leaves the file holding each event exactly once, with every line parsing as JSON. `flush()` is
      truthy, `failed_batches` is 0 and `health().sink is None`. This holds at each cap of 5,000,
      8,192, 10,500, 12,000, 16,000 and 20,000 bytes, and the 10,500 case is the one that tore a
      line before this spec.
- [ ] For a batch that fails nowhere, the file's bytes equal what 1.x's text stream wrote for the
      same batch. This is checked for `utf-8`, and for `utf-16` both into an empty file (one byte
      order mark, at the start) and into a non-empty one (none).
- [ ] With `utf-16` into an empty file and a round whose write takes 0 bytes and raises, the
      resent round writes the byte order mark first and the file holds exactly one.
- [ ] `FileSink(path, encoding="no-such-codec")` and `FileSink(path, encoding="base64")` both
      raise `LookupError` at construction.
- [ ] The delivery doc records at least five interleaved before-and-after pairs of the median time
      of a 1,000-event batch through `FileSink.emit`, on one machine, each a median of its own runs.
      The median of the pairs' ratios is no more than 2.0. A prototype measured 1.50 to 1.56 times,
      so 2.0 leaves the build about a third in headroom (*Revision history*).
- [ ] The rule above keeps invariants 7 and 2: no event a round called `ACCEPTED` is written again,
      and no event that did not land is called `ACCEPTED`.

### FR-002: A torn line is finished before anything else reaches its file, once, and never resent whole

#### Description:

When a write cuts a line partway, the line's unwritten remainder is the *tail*. **The tail belongs
to the sink; the torn occurrence belongs to the run.** The sink keeps the tail, so any section can
finish it. The run that cut it records, in its own `RunContext.notes(self)`, which occurrence was
torn and which tail record was cut from it. Nothing about the run is kept on the sink after the run
ends.

**Every write section, and the release a `close()` runs, writes a pending tail before any other
byte reaches that file.** The tail cannot be dropped, because its prefix is already on disk and
anything appended after the prefix would join it into one line that does not parse, which is the
10,500-byte case. A tail that fails partway shrinks by what was taken. While a tail is still
pending after a section's attempt, that section writes nothing else, and its round's events are
`RETRYABLE` or `REJECTED` as the predicate answers.

When the engine offers the torn occurrence again in a later round of its run, the round does one of
two things:

- if the run's tail record is still pending, it writes the tail rather than the whole line, and
  reports the event `ACCEPTED` once the tail is taken;
- if another section (the orphan path, an inline `emit`, a concurrent worker run) has already
  finished that tail, it reports the event `ACCEPTED` without writing anything for it.

**A torn event the run will not offer again is `UNKNOWN`, never `REJECTED`, and never
`RETRYABLE` on the run's last round.** That covers a tear on the last round and a tear under an
error the predicate does not retry. Its prefix is on disk and its tail will be written by the next section on this sink, so it may land. If it
were `RETRYABLE` or `REJECTED`, a run in which nothing else landed would fail, and every catcher that resends
whole would write the line again after the tail: `MultiSink`, which offers no `deliver` and is
resent by the worker's 1.x loop, a third-party wrapper calling `inner.emit`, or a caller's own
`except OSError` retry. The spec review reproduced that with a prototype, which put event 0 on
disk twice. As `UNKNOWN`, the run settles (SPEC-072's rule), so `emit` returns and no catcher resends.
The event is counted once in `failed_events`. Because it may land later, that is the recorded upper
bound on `failed`, and it is never also in a catcher's term (`failed_batches`, `orphan_lost`),
since a settled run has no catcher.

#### Acceptance Criteria:

- [ ] A round that cuts event 3's line partway, followed by a round offered events 3 to `n`, leaves
      the file with event 3's remaining bytes immediately after its prefix, then events 4 to `n`. Every
      line parses, each event appears once, and event 3 is `ACCEPTED` in the second round.
- [ ] A second round whose tail write takes `j` bytes and raises leaves the tail `j` bytes shorter.
      It writes no other byte, and its verdict holds no `ACCEPTED`.
- [ ] Run A tears event 1, and a second section on the same sink runs in another thread (driven by
      `run_concurrently`) before A's next round. That section writes A's tail and then its own line.
      A's next round writes nothing for event 1 and reports it `ACCEPTED`. The file holds each event
      once and every line parses.
- [ ] A `MultiSink` wrapping a `FileSink`, driven by a real `Worker`, with a tear inside the batch's
      first line and the fault cleared before the worker's next attempt, leaves each event on disk
      exactly once and every line parsing. The same holds for an inline `FileSink.emit` followed by a
      caller resending the same batch. In both, the first `emit` returned rather than raised.
- [ ] A tear inside the first line under `EACCES` on the first of four rounds reports the torn
      event `UNKNOWN` and the rest `REJECTED`, and the run settles, so `emit` returns.
- [ ] A run that ends with a tail pending reports the torn event `UNKNOWN` and settles. That event
      is in `failed_events` once, and in neither `failed_batches` nor `orphan_lost`. The next batch's
      first round writes the tail before its own lines, and every line in the file parses.
- [ ] A batch holding one event object twice, with the first occurrence torn, writes that object's
      whole line exactly once on the resend (for the second occurrence) and the first occurrence's
      tail exactly once.
- [ ] `close()` with a tail pending tries the tail once and closes the descriptor whatever that try
      does. It raises the `OSError` when the try failed and returns when it succeeded. A second
      `close()` is a no-op.
- [ ] These keep invariants 7, 2 and 5: no byte of a tail is written twice, the torn event is
      counted at most once and in one term, and a close releases the descriptor once even when the
      tail fails.

### FR-003: `RotatingFileSink` keeps both rules across a rotation

#### Description:

`RotatingFileSink` takes FR-001's round and FR-002's tail. Its rotation decision stays per event,
before that event's line, on the same triggers. SPEC-048 FR-006's constraint is that every event of
the batch written before a rotation is on disk before the rotation can fail. On this path that
becomes: the bytes of the lines before the rotation point are written, and every write call for
them has returned, before `_rotate` runs. A round that fails before the rotation point attempts no
rotation. **A rotation never starts while a tail is pending.** The tail is finished into the file
that holds its prefix first, and if it cannot be finished, nothing else is written and no rotation
is tried in that round.

The reopen after a failed rotation can itself raise, which `_rotate_or_continue` records today as a
residue that duplicates the prefix. It now reports the lines written before the rotation as
`ACCEPTED` and the rest `RETRYABLE` or `REJECTED`, so the residue closes. `_size` grows by the
bytes the write calls returned, not by the bytes the sink meant to write. An absorbed rotation
failure behaves as SPEC-048 FR-006 left it: one line per outage, and the batch continues on the
un-rotated file.

#### Acceptance Criteria:

- [ ] A batch whose fourth event triggers a size rotation, with a handle double that raises
      `ENOSPC` on the write after the rotation, leaves events 1 to 3 in the backup and reported
      `ACCEPTED`. The rest are `RETRYABLE`, and resending them writes them to the fresh file only,
      each once.
- [ ] A write that raises before the rotation point leaves `_rotate` uncalled in that round,
      asserted on a call count.
- [ ] With a tail pending and a rotation due, the tail lands in the file holding its prefix before
      any rename. When the tail write raises, no rename or removal is attempted, asserted on the
      backup files' contents.
- [ ] A rotation whose reopen raises `EMFILE`, after three lines of the batch were written, reports
      those three `ACCEPTED`. Resending the rest through the worker leaves each of the batch's events
      on disk exactly once.
- [ ] After a round whose write took `k` bytes and then raised, `_size` has grown by exactly `k`.
- [ ] An absorbed rotation failure still writes exactly one line per outage and continues on the
      un-rotated file. SPEC-048 FR-006's tests in `tests/test_sinks_file.py` pass, with only their
      stream doubles adapted to the binary handle (`_stream` keeps its name).
- [ ] These keep invariants 7 and 2 across a rotation.

### FR-004: The file sinks count once per run, retry what a resend can cure, and keep 1.x's `emit`

#### Description:

Both sinks hold a `SinkCounters` as `log_foundry_counters`, which the engine alone writes
(SPEC-072). An event that did not land is counted in `failed_events` once per run, not `len(batch)`
per worker attempt as SPEC-069 FR-004 counts today. An unencodable event is `DROPPED`, counted in
`dropped_events` when the run settles. When the run fails, it is counted in `failed_events` with the
rest (`unencodable_in_run`, `When.FAILED`), which keeps 1.x's whole-batch count exactly as
`_RedisSink` and `PostgresSink` kept it in SPEC-074. `losses()` keeps returning `None` until
something is counted (SPEC-069), so a process that only logs clean events still reads
`health().sink is None`.

`_retryable_error` answers `True` only for an `OSError` whose `errno` a resend can cure, because
space can be freed, a quota or limit raised, or a descriptor released: `ENOSPC`, `EDQUOT`, `EFBIG`,
`EIO`, `EAGAIN`, `EMFILE` and `ENFILE`, each resolved with `getattr(errno, name, None)` so a
platform lacking one skips it. Anything else, `EACCES`, `EPERM`, `EROFS` and an `errno` of `None`
among them, is `REJECTED` at its first attempt. This is SPEC-070's rule (*A sink declares what it
retries, and anything else fails at once*) reaching the file sinks as they join the engine. 1.x's
worker resent them on every raise.

A round that lost events registers its line as SPEC-074's sinks do, through `_rounds.Lost` with
`raised_line`: `When.LAST` for a retryable error and `When.ALWAYS` otherwise, naming the sink's
class, the round's exception type and, as SPEC-069's line did, `errno=N`. The engine and the
worker's abandon line add nothing to it.

The inline `emit` keeps SPEC-069's promise that no `except` clause changes. When a run fails, it
raises the dual-typed error that `_driver_error.delivery_error` builds from the `OSError` of the
round that ended the run (`OSError`, its own subclass such as `PermissionError`, and
`SinkDeliveryError`, with the original as `__cause__`), or the original when the dual type cannot be
built. A run that landed anything, or tore a line, settles instead: `emit` returns and the unlanded
events are counted, which is invariant 7's rule for a sink that delivered something. A closed or
closing sink refuses through `final_refusal` and `RunContext.refuse` and counts nothing, as every
engine sink does since SPEC-076.

#### Acceptance Criteria:

- [ ] Through the worker with `max_retries=3` and a handle that raises `ENOSPC` on every write,
      one batch of `n` leaves `failed == n` (not `4n`) and `failed_batches == 1`, for both sinks.
- [ ] Through the worker, a handle that raises `EACCES` gets exactly one `deliver` call and no
      backoff wait, asserted on the call count and on the engine's wait never being entered. One that
      raises `ENOSPC` gets `max_retries + 1` calls.
- [ ] An inline `emit` whose run lands nothing on `PermissionError` raises an exception that is an
      instance of `PermissionError`, `OSError` and `SinkDeliveryError`, with the original as
      `__cause__`. One whose run landed some events returns normally, and `losses().failed` equals the
      number that did not land.
- [ ] `losses()` is `None` after a batch that lands whole. After a settled run with `d` unencodable
      events and `f` events lost, it is `SinkLosses(dropped=d, failed=f)`. After a failed run of `n`
      events of which `u` were unencodable, it is `SinkLosses(dropped=0, failed=n)`, so
      `tests/test_driver_error.py`'s `SinkLosses(0, 2)` case holds unchanged.
- [ ] A closed sink's `emit` raises the `final_refusal` error by identity and moves no counter.
      A `close()` from another thread during a worker backoff returns before the next round would
      start, and that round writes nothing and refuses through `final_refusal` with no counter moved.
- [ ] A round lost to `ENOSPC` registers exactly one line through `Lost`, naming the sink's class,
      `OSError` and `errno=28` and no event data. It is written once per run, on the last round.
- [ ] The worker, the orphan path and the inline adapter all reach `deliver`, held by
      `tests/test_delivery_paths.py`'s pattern. `tests/test_engine_sinks_roster.py` finds both sinks,
      with its floor raised from 13 to 15.
- [ ] These keep invariants 2, 3, 6, 7, 10 and 11.

### FR-005: A forked child never finishes its parent's torn line

#### Description:

A tail is the parent's: its prefix came from the parent's write, and the parent finishes it. A
child that inherited a pending tail and wrote it too would put those bytes on disk twice.
`reacquire_after_fork` therefore drops the inherited tail without writing it and opens a descriptor
of the child's own on the path. That keeps the 1.x reopen, which also picks up the active file after
a parent's rotation. The unbuffered handle leaves no inherited buffer to strand, so the `os.dup2`
stranding step goes with the text stream. A closed sink's hook still returns without reopening
(SPEC-042).

The child's first line still lands after the parent's torn prefix if the child writes before the
parent finishes its tail. That line then does not parse. This is the cross-process case under *Out
of Scope*, which no single process can prevent.

#### Acceptance Criteria:

- [ ] Under a real `os.fork`, with a tail pending in the parent, the child's batch writes none of
      the tail's bytes. After the parent's next round the tail's bytes appear in the file exactly
      once, asserted on a marker string unique to the tail.
- [ ] The child's `close()` leaves the parent's descriptor open, and the parent's next write
      succeeds.
- [ ] A closed sink's `reacquire_after_fork` returns without opening anything, as before.
- [ ] These keep invariant 12.

---

## Data Model

```python
# src/log_foundry/sinks/file.py: the state each file sink adds (names may change in the plan)
class FileSink:  # RotatingFileSink adds the same
    log_foundry_counters: SinkCounters               # the engine's one writer (SPEC-072)
    log_foundry_stop_signal: threading.Event | None   # set by the library, as on every engine sink
    _stream: io.FileIO                                 # open(path, "ab", buffering=0); 1.x's name kept
    _tail: _Tail | None                                # the pending tail, None when no line is torn

@dataclass
class _Tail:
    remaining: bytes   # the unwritten bytes; shrinks by what each write call takes

# In the run's notes (RunContext.notes(sink)), never on the sink:
#   the torn occurrence's position in the run, and the _Tail object cut from it, compared by
#   identity to sink._tail to learn whether it is still pending.
```

## API / Interface Contract

```python
# Both file sinks: the SPEC-074 shape
def deliver(self, batch: list[dict[str, object]]) -> Verdict  # one round, via _engine.one_round
def emit(self, batch: list[dict[str, object]]) -> None         # exactly emit_via_deliver(self, batch)
def _retryable_error(self, err: Exception) -> bool             # FR-004's errno set

# src/log_foundry/sinks/_engine.py: one private addition, in three places
class RunContext:
    def raise_on_failure(self, error: Exception) -> None:
        """Records the error the inline adapter raises, by identity, if this run fails.

        Unlike refuse(), it marks nothing final and moves no count to the catcher: the run's
        events are counted into failed_events as for any failed run. The last call in the run
        wins; _rewind restores it with the marks, and _close drops it. A refusal recorded in the
        run's last round takes precedence over it. Outside any run it does nothing.
        """

@dataclass(frozen=True, kw_only=True)
class Delivery:
    raise_on_failure: Exception | None = None   # read by the adapter after the context closes
```

`test_engine_sinks_roster.py` requires an engine sink's `emit` body to be exactly
`emit_via_deliver(self, batch)`, which is why the dual-typed raise goes through the run context
rather than through a wrapper in `emit`.

## File & Folder Structure

```
src/log_foundry/sinks/
├── file.py        # both sinks on deliver; the text stream and _reopen_discarding retired
└── _engine.py     # RunContext.raise_on_failure, Delivery.raise_on_failure, the adapter's raise
tests/
├── test_file_sink_deliver.py          # FR-001, FR-002, FR-004 (new)
├── test_rotating_file_sink_deliver.py # FR-003 (new)
├── test_file_sink_partial_write.py    # the RLIMIT_FSIZE subprocess proof (new, POSIX only)
├── test_file_sink_fork.py             # FR-005 under a real os.fork (new)
├── test_engine_sinks_roster.py        # floor 13 -> 15
├── test_run_context.py                # raise_on_failure, its precedence and its rewind
├── test_sinks_file.py                 # stream doubles adapted to the binary handle
├── test_driver_error.py               # its text-stream double replaced; its counts unchanged
├── test_deferral.py                   # the file sinks leave the immediate-store allowlist
├── test_fork_lifecycle.py             # the file sinks' fork cases on the new hook
└── test_sink_ownership.py             # the file sink case on the new handle
```

## Implementation Phases

### Phase 1: The engine addition and `FileSink` on `deliver`

- `RunContext.raise_on_failure`, `Delivery.raise_on_failure` and the adapter raising it, with
  `test_run_context.py` cases for precedence, rewind and close.
- `FileSink`: the unbuffered handle, the encoding check and byte order mark, the round, the tail and
  the run's torn occurrence, the predicate, the counters, `losses()` and `close()` (FR-001, FR-002,
  FR-004).

### Phase 2: `RotatingFileSink` on `deliver`

- The same round with per-event rotation, the tail-before-rotation rule, `_size` from the bytes the
  write calls returned, and the reopen residue closed (FR-003, FR-004).

### Phase 3: Fork, roster and the end-to-end proofs

- `reacquire_after_fork` for both sinks (FR-005), the roster floor, the `RLIMIT_FSIZE` subprocess
  test, the `MultiSink` case, and the timing measurement for the delivery doc.

### Phase 4: Records (the completion ritual)

- Invariant 7: remove the file sinks from the partial-write exception the SPEC-069 follow-up fix
  recorded, and keep the stream sinks in it. Invariant 2: drop the file sinks from the sentence on a
  sink's own `failed` landing before a raise, and add the torn event's `UNKNOWN` to the engine's
  upper bound on `failed`.
- Rewrite these decisions in place (`docs/decisions/sink-delivery/`), each with a dated History
  entry:
  - *A sink's own driver error is counted, announced and raised as both types*: the file sinks'
    accepted edge is resolved, and the dual type is kept on the inline `emit` of a run that landed
    nothing;
  - *A sink that delivered nothing raises; one that delivered something reports*: its SPEC-069
    sentence on the file sinks;
  - *A raising `emit` counts nothing*: the file sinks' immediate `_failed` leaves the allowlist it
    names.
- `architecture.md`: the fork-hazard paragraph that says `StdoutSink` "carries the file sinks'
  duplication hazard". The file sinks no longer have a buffer to carry, so the paragraph is reworded
  for `StdoutSink` alone.
- `_rotate_or_continue`'s residue paragraph, the inventory row, and the release notes of the release
  after 1.2.0. The caller-visible changes for those notes are:
  - a partial write returns rather than raising;
  - `failed` counts once per run;
  - a non-retryable `errno` costs one attempt;
  - a non-text codec is refused at construction.

---

## Revision history

- **2026-11-13, at authoring.** The brief this spec was written from said no line was torn,
  because `BufferedWriter` keeps the unwritten tail. Re-running the brief's probe at `c21755f1`
  (200 events of 120 bytes, Python 3.13.1, macOS, the cap lifted before the second `emit`) showed
  that holds only at some caps:

  | cap (bytes) | events landed twice | torn lines | what the buffer did |
  |---|---|---|---|
  | 5,000 | 69 | 0 | kept 3,280 unwritten bytes and wrote them on the retry |
  | 8,192 | 69 | 0 | |
  | 10,500 | 86 | 1 | discarded the 60 unwritten bytes of event 87 |
  | 12,000 | 100 | 0 | |
  | 16,000 | 138 | 0 | |
  | 20,000 | 200 | 0 | |

  At 10,500, event 87's 60-byte prefix stayed on disk and the retry's first line was appended to it,
  giving one 179-byte line that does not parse. That case is why FR-002 forbids dropping a tail and
  why FR-001's subprocess criterion runs it.
- **2026-11-13, spec review.** The review moved the torn occurrence from a sink slot into the run's
  notes, after reproducing a duplicate when another section finished the tail between rounds. It
  made a tear the run will not offer again `UNKNOWN`, after reproducing `MultiSink` resending a failed run whole. It
  also restated the close-during-backoff criterion in SPEC-074's form, kept 1.x's failed-run count
  for unencodable events, added the codec check, and pointed Phase 4 at the records that exist.
- **2026-11-13, SPEC-084 authored.** *Out of Scope*'s first bullet said the two specs share no
  code path; SPEC-084 reuses this spec's `raise_on_failure`, so it now says the sibling depends on
  this one and builds after it.
- **2026-11-13, timing bound measured.** FR-001's timing criterion asked for the after time to be
  no more than 1.25 times the before, from one measurement. SPEC-084's review had measured its
  stream-sink prototype at 1.52 to 1.69 times, so this one was measured before the build. The
  prototype subclassed `FileSink` at `717d0c68` with this spec's round: the unbuffered binary
  handle, each event encoded once per run through `encode_round`, the round's events passed to
  `mark_sent`, the joined bytes written in as many write calls as the kernel needed, each event's
  outcome taken from the bytes those calls returned, and the run-notes and tail checks on every
  round. It ran through `emit_via_deliver` against 1.x's `FileSink.emit`, with 1,000 events of
  313 to 316 bytes each, on Python 3.13.1, macOS, load average about 4. Each figure is the median
  of its own runs, and pairs alternate which side runs first:

  | pairs | runs per side | before (ms) | after (ms) | median ratio | range |
  |---|---|---|---|---|---|
  | 9 | 100 | 1.73 to 1.84 | 2.65 to 2.77 | 1.50 | 1.47 to 1.57 |
  | 7 | 200 | 1.81 to 1.89 | 2.83 to 2.87 | 1.56 | 1.51 to 1.57 |

  Without `mark_sent` the nine pairs gave 1.38 (1.34 to 1.43). A profile put about a third of the
  after time in `encode_round`'s own bookkeeping and under half in the `json.dumps` it wraps, with
  `mark_sent` near 7% and the write itself small, so the cost is the engine's per-run cache, as
  SPEC-084 found, and 1.25 times cannot be met while this spec adds nothing to `encode_round`. The
  bound is now 2.0 times, the same as SPEC-084's, judged on the median ratio of at least five
  interleaved pairs, because SPEC-084's single runs reached 2.49 and 3.52 times under load. The
  gap from 1.56 to 2.0 is headroom for whatever a full build's round carries on the success path
  that the prototype did not, such as the byte order mark's bookkeeping. The 0.84 against 0.94 ms
  figure under FR-001 measured the write alone, not the engine around it.
