---
type: Spec
id: SPEC-082
title: "File Sinks on the Delivery Engine: Resend Only What Did Not Land"
description: "Moves FileSink and RotatingFileSink onto deliver, so a write that fails partway reports each event by the bytes the operating system took and the engine resends only the events that did not land (on a one-round run they are counted, not resent), which closes the duplicate lines a partial write and the worker's whole-batch retry produce today."
spec_status: Completed
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
resends only the events that did not land, where the run has a later round: on a one-round run (an
inline `emit`, a `MultiSink` child) the events after a failure are counted, not resent. A line cut
in half is finished, once, before anything else reaches its file, and a run that cut a line is
never handed back for a whole-batch resend. It targets the first release after 1.2.0.

## Scope

### In Scope

- `FileSink` and `RotatingFileSink` (`src/log_foundry/sinks/file.py`) offer `deliver`, hold
  `log_foundry_counters` and `log_foundry_stop_signal`, and make `emit` the engine adapter, as
  SPEC-074's thirteen engine sinks do.
- Writing through an unbuffered binary handle, and deciding each event's outcome from the byte
  count the write calls returned.
- A torn line: the unwritten remainder of a line a write cut partway, kept by the sink and written
  before anything else reaches that file.
- A write call that raises something other than an `OSError`, and a `BaseException` passing
  through a write section: in both, the sink's state follows the counts its write calls returned
  (FR-001, FR-002).
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
  of the batch is lost rather than resent.
- Cross-process coordination. If another process appends to the same path, or truncates it,
  between a partial write and the completion of its torn line, a line does not parse: an appended
  line joins the torn prefix, and after a truncation the tail lands alone at the head of the file.
  A child forked while a tail is pending is the first case too (FR-005). No single process can
  prevent this, and `FileSink`'s docstring already puts two processes appending to one path out of
  scope.
- **A `deliver` wrapper that copies the events between rounds.** A file sink finds the event whose
  line a write cut partway by object identity, so a wrapper that hands it copies in a later round
  makes the torn event look new: its line is written whole after its tail, and that copy is
  counted nowhere. The requirement is stated on the `deliver` contract in `sinks/base.py` and in
  `architecture.md`: a wrapper forwards the same event objects in every round of a run, as every
  shipped wrapper does.
- Durability. `ACCEPTED` means the write call returned the bytes as taken, exactly what 1.x's
  `flush()` returning meant. No `fsync` is added.
- A `max_retries` on either file sink. The inline `emit` stays one round, as 1.x's `emit` was one
  call, and the worker's `max_retries` budgets the worker path, as it does today.
- **Resending only the unlanded events of a `MultiSink` child.** A `MultiSink` child gets one
  round inside the fan-out's `emit`, so the events after a tear are counted and not written
  (FR-002). Resending only those needs `MultiSink` itself on `deliver`, which is not this spec.
- `architecture.md` §12's item on a rotation that keeps failing being retried once per event.

---

## Functional Requirements

### FR-001: `FileSink` reports each event by the bytes the operating system took

#### Description:

`FileSink` defines `deliver` as one round per call (`_engine.one_round`). A round encodes the
round's events once per run (`_encode.encode_round`). It turns the encodable lines into bytes in
batch order, writes them through an unbuffered binary handle (`open(path, "ab", buffering=0)`,
still held as `_stream`) in a loop, and adds up what each write call returned. When an `Exception`
is raised inside that loop after `k` bytes of the round's own lines have been taken:

- an event whose line's last byte lies below byte `k` is `ACCEPTED`;
- the event whose line has some bytes below `k` and some at or after it is the torn event (FR-002);
- every later event is `RETRYABLE` or `REJECTED`, as the retry predicate (FR-004) answers for the
  error;
- the torn event is `RETRYABLE` only when the predicate retries the error and the round is inside
  an engine run (`RunContext._in_run()`) and is not that run's last (`RunContext.last`). It is
  `UNKNOWN` otherwise, a direct
  `deliver` outside any run included (FR-002). ~~`RETRYABLE` whenever the round is not the run's
  last~~: superseded 2026-11-14 (*Revision history*).

A round in which every write returned is `ACCEPTED` throughout.

**A direct `deliver` outside an engine run never reports a torn event `RETRYABLE`.** There
`RunContext.last` answers false, and the notes that say which occurrence was torn end with the
call. A caller that resent a `RETRYABLE` torn event would see its tail written by the next call's
section and then its whole line written again, which is the duplicate this spec closes. As
`UNKNOWN` it is not resent, and the next section on the sink finishes its line.

**The round handles any `Exception` raised inside its write loop, not only an `OSError` from a
write call.** The bytes taken before it decide the outcomes and the tail exactly as for an
`OSError`. That is true of an exception a signal handler raises just after a write call returned
as well (the `signal.alarm` idiom raising `TimeoutError`), because FR-002 stores each call's count
where no handler can run first. The predicate answers false for it (FR-004), so the later events
are `REJECTED` and a torn event is `UNKNOWN`. With `OSError` alone handled, a handle that took 9
bytes and then raised `ValueError` (or a `MemoryError` from slicing the remainder) under
`MultiSink` left a cut line with no tail, and the worker's whole-batch resend glued the next line
to it, which is the defect this spec closes. A `BaseException` that is not an `Exception` is
FR-002's.

**One residue stays, as it was in 1.x.** An exception a signal handler raises after the write
section has finished and before `emit` returns reaches the caller with the batch already on disk,
so a caller that retries on it writes the batch again. No design measured at the review of this
spec's revision closed it (*Revision history*).

**Encoding.** The construction refuses a codec that is not a text encoding with `LookupError`,
as 1.x's `open()` did, so a codec such as `"base64"`, which the incremental encoder would accept,
is never reached at write time. A byte order mark is owed only when the handle is seekable and at
offset 0 when it is opened, and the same test is made when the handle is reopened after a fork
(FR-005). ~~which is what 1.x's text stream wrote~~: superseded 2026-11-15, since 1.x wrote a
`utf-8-sig` mark into a pipe on every open (*Revision history*). An owed mark is written once, as
the first bytes of the first line to reach the file. It belongs to that line's byte count and is
still owed until a write call has taken it, so a round that fails before it lands writes it in a
later round. A path that cannot seek is owed none: `FileSink(fifo, encoding="utf-16")` constructs
and writes no mark, as 1.x did.

The buffered text stream is retired because its state after a failure is not a statement of what
landed. Measured at `c21755f1`, CPython's `BufferedWriter` sometimes kept the unwritten bytes and
wrote them at its next flush. At other caps it discarded them, and the next write was appended to
a line cut in half. Either way it never says how many bytes reached the file. The unbuffered write
of the joined bytes measured no slower than today's `emit` (0.84 ms against 0.94 ms per
1,000-event batch, encode included, on one machine in one run). That is the encode and the write
without the engine: the engine's per-run bookkeeping around them, mostly `encode_round`'s per-run cache, makes the whole
`emit` slower than 1.x's, which is what the timing criterion below bounds. A write that the kernel accepts
partially returns the exact count before the next call raises: under a 10,500-byte cap, one call
returned `10500` and the next raised `EFBIG`.

#### Acceptance Criteria:

- [ ] Inside an engine run of more than one round, with a handle double that takes `k` bytes of a
      round that is not the run's last and then raises `OSError(ENOSPC)`, the verdict's outcome
      tuple equals the expected one at each of these `k`:
      - `k = 0`: every event `RETRYABLE`;
      - a byte inside the third line: the third event torn and `RETRYABLE`;
      - the offset of the third line's newline: the same outcome tuple as the case above, and the
        pending tail is exactly one byte long;
      - the offset just past that newline: the third event `ACCEPTED`;
      - one byte short of the whole round.
- [ ] A direct `deliver` outside any run, with a handle double that tears the first of three lines
      under `ENOSPC`, reports `UNKNOWN, RETRYABLE, RETRYABLE`. A caller that then calls `deliver`
      with the two `RETRYABLE` events leaves each of the three events on disk exactly once, with
      every line parsing.
- [ ] With a handle double that takes 9 bytes and then raises `ValueError`, a four-event batch
      through a `MultiSink` and a real `Worker` leaves, once the next section on the sink has run,
      every line parsing and no event on disk twice. The same holds with `MemoryError` in place of
      `ValueError`.
- [ ] A subprocess sets `RLIMIT_FSIZE` and `FileSink`'s handle lifts the limit after its first
      `EFBIG`. A 200-event batch then delivered through the worker at its default `max_retries`
      leaves the file holding each event exactly once, with every line parsing as JSON. `flush()` is
      truthy, `failed_batches` is 0 and `health().sink is None`. This holds at each cap of 5,000,
      8,192, 10,500, 12,000, 16,000 and 20,000 bytes, and the 10,500 case is the one that tore a
      line before this spec.
- [ ] For a batch that fails nowhere, the file's bytes equal what 1.x's text stream wrote for the
      same batch. This is checked for `utf-8`, and for `utf-16` both into an empty file (one byte
      order mark, at the start) and into a non-empty one (none). On POSIX only, it is also checked
      for `utf-16` into a FIFO: the sink constructs, and the reader receives the bytes 1.x's text
      stream sends, which hold no mark.
- [ ] With `utf-16` into an empty file and a round whose write takes 0 bytes and raises, the
      resent round writes the byte order mark first and the file holds exactly one.
- [ ] `FileSink(path, encoding="no-such-codec")` and `FileSink(path, encoding="base64")` both
      raise `LookupError` at construction.
- [ ] The delivery doc records at least five interleaved before-and-after pairs of the median
      wall-clock time of a 1,000-event batch through `FileSink.emit`, on one machine, each a median
      of its own runs, with the events and encoding of the prototype in *Revision history* (lines
      of 312 to 316 bytes, `utf-8`). The pairs are taken with the one-minute load average below the
      machine's core count, and the load average is recorded beside them. The median of the pairs'
      ratios is no more than 2.0. The prototype measured 1.50 to 1.56 times, so 2.0 leaves the
      build about a third in headroom at that event size.
- [ ] The rule above keeps invariants 7 and 2: no event a round called `ACCEPTED` is written again,
      and no event that did not land is called `ACCEPTED`.

### FR-002: A torn line is finished before anything else reaches its file, once, and the library never resends it whole

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
`RETRYABLE` on the run's last round.** That covers a tear on the last round, a tear under an
error the predicate does not retry, and a tear in a direct `deliver` outside any run (FR-001). Its prefix is on disk and its tail will be written by the next section on this sink, so it may land. If it
were `RETRYABLE` or `REJECTED`, a run in which nothing else landed would fail, and every catcher that resends
whole would write the line again after the tail: `MultiSink`, which offers no `deliver` and is
resent by the worker's 1.x loop, a third-party wrapper calling `inner.emit`, or a caller's own
`except OSError` retry. The spec review reproduced that with a prototype, which put event 0 on
disk twice. As `UNKNOWN`, the run settles (SPEC-072's rule), so `emit` returns and no catcher resends.
The event is counted once in `failed_events`. Because it may land later, that is the recorded upper
bound on `failed`. When the run ends on the tearing round, the event is never also in a catcher's
term (`failed_batches`, `orphan_lost`), since a settled run has no catcher. ~~It is never also in a
catcher's term~~: superseded 2026-11-14 by the residue below.

**The residue is a torn event left `RETRYABLE` by a run that then stops.** A torn event reported
`RETRYABLE` on a round that was not the run's last stays unresolved when the worker then declines
the run (the exit drain), or when the sink is closed during the backoff. If nothing else landed,
that run fails, and the torn event can be counted in `failed_batches` as well as `failed_events`. That
is the upper bound invariant 2 already records: an event of a failed run is also in its catcher's
term. Nothing is written twice, because the worker never resends an engine run whole and `close()`
finishes the tail.

**On a one-round run, the events after a tear are counted as lost and are not written.** An inline
`emit`, a `MultiSink` child, a third-party wrapper calling `emit` and the orphan path each give the
sink one round, so a round that tears is the run's last. The torn event is `UNKNOWN`, the run
settles and `emit` returns. The events after the torn one did not land and are not offered again:
they are counted in `failed_events` and are absent from the file. 1.x raised there, and a catcher
that resent the batch whole wrote those events and put the head of the batch on disk twice. For
`MultiSink` that resend happened only when every child raised, because it re-raises to the worker
only then. A transient partial write under one of these callers therefore loses the rest of that
batch, counted, where 1.x could deliver it with duplicates. A file sink the worker drives directly
has later rounds, and there the engine resends what did not land.

**The count a write call returns is stored where no signal handler can run first, and the sink's
state follows the stored counts when an exception passes through.** Two rules give that, for a
tail's write calls and a round's lines alike.

- The count a write call returns is stored with no bytecode boundary between the call returning
  and the store. An exception a signal handler raises (a `KeyboardInterrupt`, or the `signal.alarm`
  idiom's `TimeoutError`) is delivered at a bytecode boundary. With a Python-level assignment it
  lands after the call has returned and before its count is stored, and the count is lost. The
  reference form is under *Data Model*.
- The sink's state (the tail shortened or recorded, `RotatingFileSink`'s `_size`, the owed byte
  order mark) is brought to the stored counts by one idempotent step. That step also runs when a
  `BaseException` or an `Exception` passes through anywhere from the first write call to the end
  of the step, so an exception that lands after the last count is stored and before the state is
  updated is covered too.

A `BaseException` then propagates unchanged, with no counter moved and no verdict returned. The
step reads no offset and no file size, so a handle that cannot seek is covered like any other. It
covers the exception that passes through; a second one that lands while the step is running for
the first is not covered.

A write call that returns no usable count (anything that is not an integer, `None` included,
zero, or a number outside the bytes still unwritten) has not said what it took. The sequence
ends there as an `OSError` carrying `EAGAIN`, which the predicate retries (FR-004), and the state
follows the counts stored before it. Added 2026-11-15 (*Revision history*).

**This hardens a pipe beyond 1.x and makes a regular file's bookkeeping exact. It repairs no
regression.** A blocking write to a pipe is cut short by a signal and returns a partial count, so
on a FIFO an interrupt really cuts a line, and 1.x left that cut line too. On a regular file a
write is taken whole and an interrupt never cuts a line, in 1.x or here. What a lost count damages
there is bookkeeping: a second byte order mark, or a stale `_size`. ~~1.x's `BufferedWriter` did
not have this exposure~~: superseded 2026-11-14, since that is true only on a regular file, where
no line is cut either way (*Revision history*).

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
- [ ] ~~A `MultiSink` child's torn batch leaves each event on disk exactly once~~: superseded
      2026-11-14, because it cannot hold for a batch of more than one event (*Revision history*).
      A `MultiSink` wrapping a `FileSink`, driven by a real `Worker`, is handed a batch of `n > 1`
      events. The handle tears the first line, and the fault is lifted after that first failure.
      The file sink's `emit` is called exactly once and returns. Once the next section on the sink
      has run (the next batch or `close()`), every line parses, no event is on disk twice, the torn
      event is on disk once, events 2 to `n` are absent, and the child's `losses().failed == n`.
- [ ] The inline twin: the same tear through an inline `FileSink.emit`, in a caller that resends
      the batch on `except OSError`, never resends, because `emit` returned. Once the next section
      has run, the file is the one the criterion above describes.
- [ ] A tear inside the first line under `EACCES` on the first of four rounds reports the torn
      event `UNKNOWN` and the rest `REJECTED`, and the run settles, so `emit` returns.
- [ ] A run that ends on the round that tore a line reports the torn event `UNKNOWN` and settles.
      That event is in `failed_events` once, and in neither `failed_batches` nor `orphan_lost`. The
      next batch's first round writes the tail before its own lines, and every line in the file
      parses.
- [ ] A worker run whose first round tears a line under `ENOSPC`, reporting the torn event
      `RETRYABLE`, and whose next round the worker declines, writes nothing more in that run. After
      `close()` the torn event is on disk once and every line parses.
- [ ] A batch holding one event object twice, with the first occurrence torn, writes that object's
      whole line exactly once on the resend (for the second occurrence) and the first occurrence's
      tail exactly once.
- [ ] `close()` with a tail pending tries the tail once and closes the descriptor whatever that try
      does. It raises the `OSError` when the try failed and returns when it succeeded. A second
      `close()` is a no-op.
- [ ] POSIX only, for `FileSink` and `RotatingFileSink`: the sink writes a batch larger than the
      pipe's capacity to a FIFO nobody drains, and a timer signal whose handler raises
      `KeyboardInterrupt` fires while the write is blocked. The exception propagates by identity
      and no counter moves. Once a reader drains the pipe and the sink's next section has run,
      every line the reader received parses and a marker unique to the cut line appears once.
- [ ] POSIX only: the same, with the interrupt landing inside a pending tail's write. Every line
      the reader received parses, and a marker unique to the tail appears once.
- [ ] On a regular file, a handle double really writes and returns a count that raises
      `KeyboardInterrupt` at its first use by the sink. With `utf-16` into an empty file, the file
      holds exactly one byte order mark after the next section. On `RotatingFileSink`, `_size` has
      grown by exactly the bytes taken.
- [ ] With `utf-16` into an empty file, a `KeyboardInterrupt` raised before any byte is taken
      records no tail and leaves the byte order mark owed: the next section writes exactly one.
- [ ] With the count stored by a Python-level assignment in place of the reference form, the
      first FIFO criterion above fails.
- [ ] These keep invariants 7, 2 and 5: no byte of a tail is written twice, a line a write call
      left cut gets its tail whether an `OSError`, another `Exception` or a `BaseException`
      followed the call, the torn event of a run that ends on the tearing round is counted once
      and in one term, a `BaseException` moves no counter, and a close releases the descriptor
      once even when the tail fails.

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

SPEC-048 FR-006's tests no longer guard the write-before-rotate ordering. On an unbuffered handle
there is no buffered line for `_rotate`'s close to lose, so
`test_a_failed_flush_still_leaves_every_event_on_disk` cannot fail for its stated reason, and at
the plan review a mutant that rotated before writing the staged lines passed all three FR-006
tests. The ordering is guarded by the first and second criteria below.

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
      un-rotated file, and SPEC-048 FR-006's tests in `tests/test_sinks_file.py` pass (`_stream`
      keeps its name). The write-before-rotate ordering is guarded by this FR's first and second
      criteria: a build that rotates before writing the staged lines fails one of them.
      `test_a_failed_flush_still_leaves_every_event_on_disk` stays, with its double adapted to the
      binary handle and its docstring reworded for what it still proves, and is not named as the
      ordering's guard.
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
platform lacking one skips it. Anything else, `EACCES`, `EPERM`, `EROFS`, an `errno` of `None` and
an exception that is not an `OSError` at all (FR-001) among them, is `REJECTED` at its first
attempt. This is SPEC-070's rule (*A sink declares what it
retries, and anything else fails at once*) reaching the file sinks as they join the engine. 1.x's
worker resent them on every raise.

A round that lost events registers its line as SPEC-074's sinks do, through `_rounds.Lost` with
`raised_line`: `When.LAST` for a retryable error and `When.ALWAYS` otherwise, naming the sink's
class, the round's exception type and, as SPEC-069's line did, `errno=N` when the exception
carries one. The engine and the worker's abandon line add nothing to it. **The line's text
changes.** Through `raised_line` its detail reads `<Class>, N attempt(s), <Type> errno=N`, where
1.x wrote `<Class>, <Type> errno=N`. The `errno=N` appears only when the exception carries an
integer `errno`: the detail is `FileSink, 1 attempt(s), TypeError` for a `TypeError`, and
`FileSink, 1 attempt(s), OSError` for an `OSError` with none. **A torn event left `UNKNOWN` has a
line of its own**, `When.ALWAYS`, through the same `raised_line` with the suffix
`; outcome unknown; they may have landed`, the words the HTTP sinks write for an event they
cannot adjudicate. The next section finishes its line, so a line calling it lost would say what
did not happen (invariant 11). It is still counted in `failed` with the rest, the upper bound
invariant 2 records. The events after it keep the plain line. A torn event a round left
`RETRYABLE` is in no line of that round, and when a later round finishes it, in none at all.
Added 2026-11-15 (*Revision history*).

The inline `emit` keeps SPEC-069's promise that no `except` clause changes. When a run fails, it
raises the dual-typed error that `_driver_error.delivery_error` builds from the exception of the
round that ended the run, recorded through `raise_on_failure(delivery_error(err) or err)`. For an
`OSError` that is `OSError`, its own subclass such as `PermissionError`, and `SinkDeliveryError`,
with the original as `__cause__`, or the original when the dual type cannot be built. An exception
that is not an `OSError` takes the same route, so a run that landed nothing on a `ValueError`
raises an instance of `ValueError` and `SinkDeliveryError`, and a caller's `except ValueError`
still catches it. A run that landed anything, or tore a line, settles instead: `emit` returns and
the unlanded events are counted, which is invariant 7's rule for a sink that delivered something.
A closed or closing sink refuses through `final_refusal` and `RunContext.refuse` and counts
nothing, as every engine sink does since SPEC-076.

**A closed file sink is not resent, which supersedes 1.x for a caller's subclass.** In 1.x the
worker resent a caller's subclass of a file sink that inherits `emit` and re-opens itself after a
close (`tests/test_final_refusal.py`, the `subclass` shape of
`test_a_callers_sink_that_routes_emit_without_a_frame_keeps_the_resend`). On the engine the worker
calls that subclass's `deliver`, and a closed sink's round is a final refusal, so the subclass is
no longer resent. A subclass that wants the resend overrides `emit`. The worker's 1.x loop keeps
both of its guards on a specimen still off the engine: that test's `subclass` shape and
`test_a_refusal_after_a_transient_failure_stops_the_resend_where_it_lands` move to `SQLiteSink`.
At the plan review the second was the only test that failed when the worker abandoned a final
refusal on its first attempt only.

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
- [ ] An inline `emit` whose run lands nothing on a `ValueError` from the handle raises an
      exception that is an instance of both `ValueError` and `SinkDeliveryError`.
- [ ] Through the worker with `max_retries=3`, a handle that raises `TypeError` on every write gets
      exactly one `deliver` call and no backoff wait. `losses()` and `failed_batches` equal what a
      run exhausted on `ENOSPC` leaves for the same batch, and the sink's one line ends
      `FileSink, 1 attempt(s), TypeError`.
- [ ] `losses()` is `None` after a batch that lands whole. After a settled run with `d` unencodable
      events and `f` events lost, it is `SinkLosses(dropped=d, failed=f)`. After a failed run of `n`
      events of which `u` were unencodable, it is `SinkLosses(dropped=0, failed=n)`, so
      `tests/test_driver_error.py`'s `SinkLosses(0, 2)` case holds unchanged.
- [ ] A closed sink's `emit` raises the `final_refusal` error by identity and moves no counter.
      A `close()` from another thread during a worker backoff returns before the next round would
      start, and that round writes nothing and refuses through `final_refusal` with no counter moved.
- [ ] Through the worker, a closed `FileSink` subclass that inherits `emit` and would re-open
      itself in the first backoff gets exactly one `deliver` call, and no backoff is entered.
- [ ] In `tests/test_final_refusal.py`, the `subclass` shape of
      `test_a_callers_sink_that_routes_emit_without_a_frame_keeps_the_resend` and
      `test_a_refusal_after_a_transient_failure_stops_the_resend_where_it_lands` run on `SQLiteSink`
      and pass. With the worker changed to abandon a final refusal on its first attempt only, the
      second fails.
- [ ] A round lost to `ENOSPC` that cut no line registers exactly one line through `Lost`,
      naming the sink's class, `OSError` and `errno=28` and no event data. It is written once per
      run, on the last round. For an inline `FileSink.emit` of two events it reads
      `log-foundry: lost 2 event(s); FileSink, 1 attempt(s), OSError errno=28`, and for an
      `OSError` with no `errno` it ends `FileSink, 1 attempt(s), OSError`.
- [ ] An inline `FileSink.emit` of three events whose first write takes five bytes and then
      raises `ENOSPC` writes two lines:
      `log-foundry: lost 1 event(s); FileSink, 1 attempt(s), OSError errno=28; outcome unknown; they may have landed`
      and `log-foundry: lost 2 event(s); FileSink, 1 attempt(s), OSError errno=28`, and
      `losses()` is `SinkLosses(dropped=0, failed=3)`. A run whose last round tears a line writes
      the same two groups, and a run whose torn line a later round finishes writes no line.
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
#   the torn event object and the _Tail object cut from it, the tail compared by identity to
#   sink._tail to learn whether it is still pending. The first offered event that is the torn
#   object is the torn occurrence.
#   Superseded 2026-11-15 (Revision history): this read "the torn occurrence's position in the
#   run", which a sink handed only a round's batch cannot know.

# The reference form for storing a write call's count (FR-002). `counts` is a list that exists
# before the call, and the append happens inside C, so no bytecode boundary falls between the
# write returning and its count being stored:
counts.extend(map(self._stream.write, (chunk,)))
# The form FR-002 rules out, where a signal handler can raise before the store:
#   wrote = self._stream.write(chunk)
```

That no bytecode boundary falls inside the reference form is a property of CPython, not of the
language, so FR-002's FIFO criterion and its mutation pin it on every Python version CI runs.

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
├── file_handles.py                    # the binary handle doubles the new tests share (new helper)
├── test_file_sink_deliver.py          # FR-001, FR-002, FR-004, and the retry sweep's four
│                                      #   properties for the file sinks (new)
├── test_rotating_file_sink_deliver.py # FR-003 (new)
├── test_file_sink_partial_write.py    # the RLIMIT_FSIZE subprocess proof (new, POSIX only)
├── test_file_sink_fork.py             # FR-005 under a real os.fork (new)
├── sink_rigs.py                       # the file rigs on a binary handle double and the engine's counters
├── test_engine_sinks_roster.py        # floor 13 -> 15
├── test_classify.py                   # engine-round floor 11 -> 13; the file sinks excluded by name
│                                      #   from the retry sweep (no max_retries, an errno predicate)
├── test_run_context.py                # raise_on_failure, its precedence and its rewind
├── test_final_refusal.py              # the file sinks join the engine cases; two 1.x-loop guards
│                                      #   move to SQLiteSink (FR-004)
├── test_sinks_file.py                 # the FR-006 flush test's double adapted and its docstring
│                                      #   reworded (FR-003); nothing else changes
├── test_sinks_encode.py               # through the rig; the frozen surface gains the engine names
├── test_sink_concurrency.py           # the deliver and emit locker sets
├── test_sink_post_close.py            # the file sinks move to the engine's closed population
├── test_driver_error.py               # its text-stream double replaced and the line's new text;
│                                      #   its counts unchanged
├── test_deferral.py                   # the file sinks leave the immediate-store allowlist
├── test_fork_lifecycle.py             # the file sinks' fork cases on the new hook
├── test_fork_one_walk.py              # the inherited-bytes window restated on the tail
├── test_fork_roster.py                # the file sinks assign no primitive to self any more
└── test_sink_ownership.py             # no stream-double change; prose only
```

The retry sweep in `tests/test_classify.py` sets a sink's own `max_retries` and expects
`ConnectionError` to be retried, and the file sinks have neither a `max_retries` nor a predicate
that retries by exception class. They are excluded from the sweep by name, with that reason, and
its four properties are tested for them in `tests/test_file_sink_deliver.py`: an exception no
resend cures costs one attempt and is counted as exhaustion is, a retried error gets
`max_retries + 1` attempts, a `KeyboardInterrupt` propagates and moves no counter, and a predicate
that raises fails at once naming the original exception.

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
  sink's own `failed` landing before a raise, and add to the engine's upper bound on `failed` the
  torn event's `UNKNOWN` and FR-002's residue, the torn event of a run that is declined or closed
  after the tearing round. Invariant 12: its sentence "a value the child inherits is stranded, not
  detached" gains the file sinks' case, a dropped tail and no buffer to strand.
- Rewrite these decisions in place (`docs/decisions/sink-delivery/`), each with a dated History
  entry:
  - *A sink's own driver error is counted, announced and raised as both types*: the file sinks'
    accepted edge is resolved, and the dual type is kept on the inline `emit` of a run that landed
    nothing, where it is now built for any `Exception` the handle raises, not only an `OSError`;
  - *A sink that delivered nothing raises; one that delivered something reports*: its SPEC-069
    sentence on the file sinks;
  - *A raising `emit` counts nothing*: the file sinks' immediate `_failed` leaves the allowlist it
    names;
  - *An event a sink cannot encode costs only itself…*: the closing sentence of its *Do NOT*
    paragraph, on a stream or file that fails partway through a batch, is reworded, since the file
    sinks no longer leave that duplicate and the stream sinks still do;
  - *A sink declares what it retries, and anything else fails at once*: its fence and its body
    both say every predicate also accepts builtin `ConnectionError` and `TimeoutError`. The file
    sinks' `errno` predicate accepts neither, so both sentences are rewritten for that;
  - *A refusal no retry can cure…*: a History entry only, for FR-004's closed file-sink subclass
    and the two guards moved to `SQLiteSink`.
- The pipeline decision *A value the child inherits is stranded, never merely detached*
  (`docs/decisions/pipeline/`), rewritten in place with a dated History entry: the `dup2` step is
  retired for the file sinks.
- One new decision file in `docs/decisions/sink-delivery/`, fence and entry together, carrying this
  spec's fences: the tail belongs to the sink and the torn occurrence to the run; a tear the run
  will not offer again is `UNKNOWN`; do NOT truncate, do NOT flush per line, do NOT keep run state
  on the sink, do NOT add an inline retry.
- `architecture.md`:
  - the fork-hazard paragraph that says `StdoutSink` "carries the file sinks' duplication hazard".
    The file sinks no longer have a buffer to carry, so the paragraph is reworded for `StdoutSink`
    alone;
  - the fork repair's description of the child stranding its copy with `dup2` to `/dev/null`;
  - the `SERIAL` guard sentence counting "the five engine sinks" beside `FileSink`,
    `RotatingFileSink` and `SQLiteSink`;
  - "the discard hook" in the paragraph on both processes rotating;
  - "nothing is lost, since `emit` flushes per batch".
- `README.md`: the sentence that a forked child "re-opens any buffered stream it inherited", and
  the sentence listing which sinks offer `deliver`.
- `_rotate_or_continue`'s residue paragraph and the inventory row.
- The release notes. They go into the pending `docs/release-notes/` file if one exists on `main`
  when this spec completes, and otherwise into the delivery doc under a "Release notes owed"
  heading. The caller-visible changes for those notes are:
  - a partial write returns rather than raising;
  - a one-round run's partial write (an inline `emit`, a `MultiSink` child) loses the rest of that
    batch, counted (FR-002);
  - `failed` counts once per run;
  - a non-retryable `errno` costs one attempt;
  - a closed file-sink subclass that inherits `emit` is not resent (FR-004);
  - the loss line reads `<Class>, N attempt(s), <Type>` where 1.x wrote `<Class>, <Type>`, each
    followed by `errno=N` only when the exception carries one, and a torn event left `UNKNOWN`
    has a line of its own ending `; outcome unknown; they may have landed` (FR-004);
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
  312 to 316 bytes per line, newline included, on Python 3.13.1, macOS, load average about 4. Each figure is the median
  of its own runs, and pairs alternate which side runs first. Event `i` was
  `{"timestamp": "2026-11-13T12:00:00.000000Z", "level": "info", "event": f"handled.request.{i}",
  "trace_id": "a" * 32, "span_id": "b" * 16, "parent_span_id": "c" * 16, "status": "ok",
  "duration_ms": 12.5, "service": "orders", "fields": {"k": i, "route": "/orders/{id}"}}`, written
  in `utf-8`, with both files emptied before each pair:

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
  that the prototype did not, such as the byte order mark's bookkeeping; the spec review measured
  an incremental encoder per line at 1.59. The 0.84 against 0.94 ms figure under FR-001 measured
  the encode and the write without the engine.
- **2026-11-13, timing bound's spec review.** The review reproduced the ratios (1.56, 1.51, and 1.42
  without `mark_sent`) and confirmed the attribution: with `encode_round` replaced by a plain list
  the ratio was 1.20. It found that the engine's cost is per event, so the ratio falls as events
  grow: 2.11 to 2.13 at 33 to 35-byte lines, 1.85 at 140-byte lines and 1.12 at 2.3 KB lines. The
  criterion therefore names the prototype's events. The review also found that *Out of Scope* gave a
  per-line-flush `emit` as about 2.3 times 1.x's from figures (2.02 ms against 1.48 ms) that give
  1.36. Under a 2.0 bound that cost is no reason to reject per-line flushing, so the bullet now
  rests on its first reason alone, the unwritten rest of the batch being lost.
- **2026-11-14, plan review.** Revised mid-build to record what the review of the implementation
  plan settled. The five FRs keep their numbers; what changed, and why:
  - *FR-002's `MultiSink` criterion.* It asked for each event on disk exactly once, which cannot
    hold for a batch of more than one event: the inline `emit` is one round, so the tearing round
    is the run's last, the torn event is `UNKNOWN`, the run settles and `emit` returns. The plan's
    prototype, with five events through `MultiSink` and a real `Worker`, measured one `emit` call,
    `flush()` truthy, `failed_batches == 0` and the child's `losses().failed == 5`, and once the
    next section had finished the tail the file held the first event only. The criterion now says
    what holds, its inline twin is a criterion of its own, FR-002's description states the
    consequence for every one-round run, and resending only a `MultiSink` child's unlanded events
    is under *Out of Scope*.
  - *The torn event outside an engine run.* FR-001's rule read `RunContext.last`, which answers
    false for a direct `deliver`, where the run's notes also end with the call. The torn event is
    now `RETRYABLE` only inside an engine run whose round is not its last. FR-001's first
    criterion says it is driven inside such a run, its one-byte-tail case asserts the tail's
    length (its outcome tuple equals the mid-line case's), and a direct-`deliver` criterion is
    added.
  - *A closed sink's subclass.* FR-004 now states that a caller's file-sink subclass inheriting
    `emit` is no longer resent after a close, which 1.x did, and moves the two tests that guard the
    worker's 1.x loop to `SQLiteSink`.
  - *The catcher's term.* FR-002 said a torn event is never also in a catcher's term. That holds
    for a run that ends on the tearing round. The residue, a run declined or closed after a round
    that reported the torn event `RETRYABLE`, is recorded with a criterion for the decline.
  - *An exception that is not an `OSError`.* The plan let one escape the round, as it escaped 1.x's
    `emit`. The plan reviewer showed that a handle taking 9 bytes and then raising `ValueError` or
    `MemoryError` under `MultiSink` left a cut line with no tail, which the worker's whole-batch
    resend glued the next line to. The round now handles any `Exception` raised inside its write
    loop, and FR-004 keeps the caller's `except` clause for it. *In Scope* gained a line for this
    and for the next item.
  - *A `BaseException` through a write section.* A new requirement under FR-002, which the plan had
    recorded as a residue with no code. It widens the spec on the project owner's standing rule
    that a failure mode judged unreachable in practice is guarded anyway. The plan reviewer's probe
    raised `KeyboardInterrupt` from a timer signal around a write loop on a real unbuffered
    append-mode file. In every interrupt that landed (36 of 36; 30 of 30 when the probe was re-run
    for this revision) a write call had returned and its count was lost, and the descriptor's
    offset equalled the bytes the file had gained. The guard "record a tail from the count known
    so far" was rejected, because that count is short by exactly the call that matters.
  - *The byte order mark on a path that cannot seek.* The prototype asked the handle for its
    offset, so `FileSink(fifo, encoding="utf-16")` raised `ESPIPE` at construction and leaked the
    descriptor. 1.x constructs and writes no mark there (re-run at `20281c89` on Python 3.13.1,
    macOS), so the mark is owed only on a seekable handle at offset 0, and the byte-parity
    criterion gained the FIFO case.
  - *The loss line.* FR-004 routes it through `raised_line`, which adds `N attempt(s),` to 1.x's
    text. FR-004 and Phase 4's caller-visible changes now say so.
  - *FR-003's sixth criterion.* It listed SPEC-048 FR-006's tests as passing with adapted doubles.
    On an unbuffered handle `test_a_failed_flush_still_leaves_every_event_on_disk` cannot fail for
    its stated reason, and the plan reviewer's mutant that rotated before writing the staged lines
    passed all three FR-006 tests, so the ordering's guard is now named as this FR's first and
    second criteria.
  - *The timing criterion.* The bound is unchanged. The plan measured 2.62 by wall clock at a load
    average of 28, and the plan reviewer 1.45 at 5.7 on the same prototype, with the wall-clock and
    thread-CPU medians agreeing only in the second. The criterion now names the clock and asks for
    a one-minute load below the machine's core count, recorded beside the pairs.
  - *File & Folder Structure and Phase 4.* The affected-tests list was short: the suite run against
    the prototype failed tests in files the list did not name, and two files it named needed no
    stream-double change. Phase 4 gained the records that still state a claim this spec changes,
    one new decision file for this spec's fences, three caller-visible changes, and where the
    release notes go while no pending file exists.
- **2026-11-14, review of the revision.** The spec review of the entry above replaced one
  mechanism and corrected what was written around it:
  - *FR-002's `BaseException` requirement: the offset guard is replaced.* Three measurements
    retired it. It was skipped on a handle that cannot seek, and a FIFO is the one handle on which
    an interrupt really cuts a line: a blocking write to a pipe is cut short by a signal and returns
    a partial count. With the guard skipped, 101 of 109 and 118 of 123 real interrupts left a line
    that does not parse (the reviewer's figures, as are the next two; the prototype was not re-run
    for this entry). "The descriptor's end-of-file offset" also named two quantities, and the
    descriptor's own offset is wrong whenever another descriptor appended between two of the sink's
    sections, which is FR-005's supported case: a tail of 1 byte was recorded where 52 were owed.
    And the difference had no lower bound. The requirement is now that a write call's count is
    stored with no bytecode boundary before the store, and that one idempotent step brings the
    sink's state to the stored counts, running on the exception path too. With the reference form
    in *Data Model* the stored count was exact in 40 of 40 interrupts on Python 3.12.12 and on
    3.13.1, where a Python-level store was short in 40 of 40 (re-run for this entry on 3.13.1 only,
    with the same result). The cannot-seek exemption and the cross-process corner went with the
    guard. One limit is stated in their place: a second exception landing while the step runs for
    the first is not covered.
  - *The sentence about 1.x.* "1.x's `BufferedWriter` did not have this exposure" is true only on
    a regular file, where a write is taken whole and no line is cut either way. On a FIFO 1.x left
    the same cut line: 97 of 103 and 100 of 106 interrupts at the review, 99 of 116 and 110 of 120
    when re-run for this entry. So the requirement hardens a pipe beyond 1.x and makes a regular
    file's bookkeeping exact (one byte order mark, a true `_size`). It repairs no regression.
  - *The `BaseException` criteria.* A Python-level double that writes and then raises in place of
    returning cannot exercise a store made in C, and a real `io.FileIO.write` never behaves that
    way, so that shape is dropped. The criteria now use a real FIFO and a real signal, a double
    whose returned count raises at its first use, an interrupt before any byte is taken, and the
    mutation that proves the FIFO criterion is live.
  - *FR-001's `Exception` rule.* An exception a signal handler raises (the `signal.alarm` idiom's
    `TimeoutError`) lands in the same window as a `KeyboardInterrupt`, so FR-001 now says the rule
    rests on FR-002's stored count. FR-001 also records the residue no design closed: a handler's
    exception raised after the write section has finished and before `emit` returns reaches a
    retrying caller with the batch on disk. It duplicated 7 to 13 batches in 400 rounds under
    every variant measured and on 1.x (12 and 13 on 1.x when re-run for this entry).
  - *The loss line.* `errno=N` appears only when the exception carries one, which FR-004, its
    criteria and Phase 4's caller-visible list now say.
  - *Phase 4.* *A sink declares what it retries, and anything else fails at once* is rewritten in
    place, not given a History entry only, because its fence and body say every predicate accepts
    builtin `ConnectionError` and `TimeoutError` and the file sinks' accepts neither. *A sink's own
    driver error…* also records that the dual type is built for any `Exception` the handle raises.
  - *Smaller shapes.* "Inside an engine run" names `RunContext._in_run()`. FR-003's sixth criterion
    lost its either/or. The *In Scope* bullet and FR-002's title say no more than the mechanism
    gives: the state follows the counts the write calls returned, and it is the library that never
    resends a torn line whole. The Overview and the frontmatter `description` say that a one-round
    run counts the events after a failure and does not resend them.
- **2026-11-15, at completion.** Recorded by the build, with no requirement changed:
  - *The byte order mark on a path that cannot seek.* FR-001 says a mark owed only on a seekable
    handle at offset 0 "is what 1.x's text stream wrote". Measured at completion on Python 3.13.1,
    macOS, that is true of `utf-16` and `utf-32` and false of `utf-8-sig`: into a FIFO 1.x's text
    stream wrote the `utf-8-sig` mark. The build follows the rule as written, so
    `FileSink(fifo, encoding="utf-8-sig")` writes no mark where 1.x wrote one. The criterion
    checks `utf-16`. ~~Into a regular file the two agree for every codec probed. Whether to
    match 1.x for `utf-8-sig` on a pipe is left open for the project owner~~: superseded
    2026-11-15 by the diff-review entry below, which names what was swept and closes the question.
  - *Where the criteria are tested.* FR-002's FIFO and real-signal criteria are in
    `tests/test_file_sink_interrupt.py`, a file *File & Folder Structure* does not list, each
    case in a child process: a handler that raises in the test runner's main thread would
    interrupt the runner. The fork tests retired from `tests/test_fork_lifecycle.py` (the three
    that parked a parent between a write and its flush, and the one that flushed an inherited
    buffer to the null device) are restated on the tail in `tests/test_file_sink_fork.py`.
  - *The data model.* The run's notes hold the torn event object and its tail record, not a
    position in the run, which a sink handed only a round's batch cannot know: every position
    before a tear is final, so the first offered event that is that object is the torn
    occurrence. The write sequence raises what stopped it, and each sink's own round handler
    asks its predicate, which is the shape `tests/test_classify.py`'s engine-round roster reads.
- **2026-11-15, diff review.** Corrections from the first review of the diff. No requirement
  changed, and the byte order mark's rule is the one FR-001 already states:
  - *What the rule matches in 1.x.* FR-001 no longer says the rule "is what 1.x's text stream
    wrote". It is what 1.x wrote on a regular file for the three codecs that have a mark
    (`utf-8-sig`, `utf-16`, `utf-32`), and on a pipe for `utf-16` and `utf-32`. For `utf-8-sig`
    on a pipe 1.x wrote the mark on every open. The review re-measured at `a0f44b5f` that a
    second `FileSink` opened on the same FIFO wrote the mark again, so a reader that stayed
    across the two found a mark in the middle of its stream. That is the reason the rule stays
    as written and this sink writes none there, which closes what the completion entry left open.
  - *What "every codec probed" was.* The completion entry's sentence named no population. The
    sweep is every codec the `encodings.aliases` table names on Python 3.13.1, macOS, plus
    `utf-8-sig`, `locale`, `UTF8` and `Utf-16`, each into an empty and a non-empty regular file
    with two emits, comparing the sink's bytes with a text stream's opened in append mode.
    1.x's sink at `a0f44b5f` matches the text stream in every case. The built sink matches it in
    every case but one family: for the `iso2022_*` codecs into a non-empty file the text stream
    wrote the three bytes `ESC ( B` ahead of its first line, and the sink writes none.
  - *The data model.* The comment under *Data Model* said the notes hold a position in the run.
    It is marked in place: they hold the torn event object and its tail record.
  - *A write call that returns no count.* FR-002 now says that a return that is not an
    integer, zero or a number outside the bytes still unwritten ends the sequence as `EAGAIN`.
    The build already did this, and `tests/test_file_sink_deliver.py` holds it for `None`, `0`,
    `-1`, a number past the end and a string.
  - *FR-004's second criterion.* It says "through the worker", and its test drove
    `_engine.run_delivery`. A twin on a real `Worker` now sits beside it, with the engine's wait
    recorded, so the criterion keeps its wording.
- **2026-11-15, second diff review.** The system frame found one line that said what did not
  happen, one wrapper requirement stated nowhere, and one gap in *Out of Scope*:
  - *The torn event's line.* The round's one line counted a torn `UNKNOWN` event among the lost,
    though the next section finishes its line: an inline `emit` whose first write took five bytes
    wrote `lost 3 event(s)` and left event 1 whole on disk after the next `emit` (invariant 11).
    FR-004 now gives that event a line of its own ending `; outcome unknown; they may have
    landed`, the HTTP sinks' words for an event they cannot adjudicate, and gains a criterion for
    the two lines. The first ENOSPC criterion now says it covers a round that cut no line. The
    counters are unchanged: the torn event stays in `failed`, the upper bound invariant 2 records.
  - *A wrapper that copies the events.* A third-party `deliver` wrapper that hands the file sink
    copies between rounds had the torn line written whole after its tail, counted nowhere. *Out
    of Scope* now says so, and the `deliver` contract in `sinks/base.py` and `architecture.md`
    states the requirement the shipped wrappers already meet.
  - *A truncation.* *Out of Scope*'s cross-process bullet now covers another process truncating
    the file as well as appending to it: after a truncation the tail lands alone at the head of
    the file and does not parse.
