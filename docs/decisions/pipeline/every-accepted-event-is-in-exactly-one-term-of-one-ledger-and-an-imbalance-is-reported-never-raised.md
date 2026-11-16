---
type: Decision
title: "Every accepted event is in exactly one term of one ledger, and an imbalance is reported, never raised"
description: "`health().events` counts in events, from the call that asks for one (`accepted`) to exactly one term: a gauge, an exit or a loss. It is kept as two balances that meet at `Worker.submit`, and `unaccounted` (what the terms cannot place) is checked at every `shutdown()` and after every test. Never raise on an imbalance. Never add a lock to the accept path: one thread writes a counter, or a lock its site already holds guards it. Never read a snapshot in an order other than `_read_ledger`'s. Never change an existing `Health` field's unit. (SPEC-088)"
---

**One unit, credited when asked for.** An event is credited to `accepted` at the call that asks
for it, before the library tries to build it. That covers `span.start` and `span.end` in
`decorator._begin` and `_end`, and the in-span log and the orphan log in `api._log`. After that
the event is in exactly one term:
- the gauges `open_spans`, `queued` and `in_flight`;
- the exits `delivered` and `skipped`;
- the losses `in_span_lost`, `orphan_lost`, `overflow_lost`, `batch_lost`, `sink_lost`,
  `drain_lost` and `stranded`.

A credit that waited for the build would hide exactly the events that the 2026-11-14 audit's
finding 17 lost. `dropped` stays in submissions and `failed_batches` stays in batches, because no
existing `Health` field changes its unit. The ledger is a new field.

**Two balances that meet at `Worker.submit`.** The caller's side balances what it credited
against what it lost, what the orphan path did and what it handed over. The worker's side
balances what its queue took in, plus what overflowed, against the queue, the drain and its
exits. A hand-over is credited after `submit` returns. So the meeting residual (handed over, less
taken in, less overflowed) is never positive for a correct submission, and a positive one is a
submission `submit` lost without a count. Kept apart, each side can be asserted alone.

**Measured where a remainder would hide a missed debit.** `open_spans` and `queued` are
remainders. The orphan path's `in_flight` is a gauge that its own cell raises and lowers, and so
is the drain's, which only the drain thread writes. As remainders, a missing debit would have
read as an event forever in flight.

**Reported, never raised.** Invariant 1 forbids an imbalance from reaching the application. So
`_check_ledger` ends every `shutdown()` and writes one `_diag` line when `unaccounted` is above
zero and differs from the last count it announced. A fault inside the check is absorbed and
announced by type. The check counts only positive residuals, because a call in progress reads
negative. It does not assert `open_spans == 0`, because a `shutdown()` from inside a traced call
legitimately leaves events buffered. The suite's autouse fixture reads at rest, so it checks the
orphan residual with both signs and checks `open_spans` against the spans its own context holds.
The `ledger_exempt` marker is strict.

**Exact counters with no new lock on the accept path.** A counter has one writer thread, or a lock
its site already holds guards it:
- each caller thread writes its own cell;
- the drain thread writes its terms;
- the entered count is written in `_CountingQueue._put`, under the queue's mutex;
- `overflow_lost` is written under `Worker._lock`;
- the two older loss counters are written under `decorator._loss_lock`.

The only new lock is the registration lock. A thread takes it once, on its first credit, and a
reader takes it once per pass. It is an `RLock` over an immutable pair that is replaced by
compare-and-set, so a handler or `__del__` that logs inside it cannot wait on its own thread.
`tests/test_ledger_write_sites.py` derives every write site from `src/` and refuses any shape
other than these.

**The read order.** `_read_ledger` reads in this order:
1. the process worker;
2. its drain's liveness, taking count, held events and exits;
3. `in_span_lost`;
4. every cell's hand-over, in one pass;
5. the queue's entered count and `overflow_lost`;
6. every cell's span credits, in a second pass, from a fresh snapshot of the cells;
7. the orphan cells, cell by cell (credit, gauge, exits);
8. `orphan_lost`.

The rule behind the order: read a count before any count that was written earlier in the life of
the same events. Read this way, `accepted` is never below the worker path's exits and losses,
`open_spans` and `queued` never read below zero, and the three residuals never read positive
while calls run.

## Rejected alternatives

- **A bare shared `counter += 1`.** On 3.12, 3.13 and 3.14 it lost no update, but only by the
  interpreter's accident. `docs/best-practices/python/python.md` §13 forbids relying on that.
- **A lock around each credit.** It cost 78 to 95 ns on the spec's prototype at `ae0b7674`,
  against 30 to 32 ns for a cell credit there.
- **Reading the ledger cell by cell.** `asyncio.to_thread` hands over in a cell other than the
  owner's. With the hand-overs read after the credits, the shared-span test reads
  `unaccounted=1`.
- **Reading the worker's exits after the credits.** The eight-thread reader then reads `accepted`
  below the exits and losses.
- **`_drain_finished` as "the drain has ended".** `_run` sets it before the terminal debit, so a
  held snapshot read `in_flight=2` as `unaccounted=2`.
- **Retiring a cell when `Thread.is_alive()` is false.** A `_thread` thread is a `_DummyThread`
  that reports itself alive forever.
- **Calling the ledger the "in-library metrics" that `CLAUDE.md` rules out.** A count of events
  that never reached the sink cannot be derived downstream of the sink, and nothing ships it.

## Do NOT build

- An exception raised for an imbalance.
- A restarted drain thread.
- A ledger counter written outside the shapes above.
- A credit moved after the build.
- A change to the unit of `dropped`, `failed_batches` or `queued`.
- A `flush()` that reads `_ledger_snapshot()` in this spec. That is SPEC-089's.

## History

### 2026-11-15

- **Settled** by SPEC-088. The read order was revised during the build from cell by cell to two
  passes with the worker's terms first, and the spec records why in place.
