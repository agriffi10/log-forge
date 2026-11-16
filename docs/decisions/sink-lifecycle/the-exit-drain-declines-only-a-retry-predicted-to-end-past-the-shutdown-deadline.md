---
type: Decision
title: "The exit drain declines a retry, or a later batch, predicted to end past the shutdown deadline"
description: "the first attempt of the exit drain's first batch is always made; a retry is declined when the batch's longest attempt plus a 0.1 s join margin is predicted to end past the `shutdown()` deadline, and a later exit batch is declined unattempted once an exit batch has declined a retry or the longest attempt any exit batch has made predicts it would overrun; each declined batch is counted, and the budget is the caller's `timeout`, so there is no knob. (SPEC-062, SPEC-090)"
---

**The exit drain declines a retry, or a later batch, predicted to end past the shutdown deadline** — `Worker._emit` retries a failing batch `max_retries + 1` times, and its wait between attempts is `self._stop.wait(...)`, which returns at once because the stop event is set for the whole of `_final_drain`. The exit drain therefore cost up to `max_retries + 1` times one `emit`, and when that passed the join the join expired and the sink was left open (SPEC-027 FR-004). Measured at `71e42dd` with a sink whose `emit` waits 0.5 s and raises, under `shutdown(timeout=1.2)`: 1.21 s, 3 attempts, `close()` called 0 times and `lost 0 item(s)`, though three events were inside the emit; after, 1.01 s, 2 attempts and one close.

The rule: the **first attempt of the exit drain's first batch is always made**, and a deadline of `None` declines nothing. Before each later retry, `now + the batch's longest attempt + _JOIN_MARGIN > deadline` declines it (`Worker._retry_declined`). Since SPEC-090 the exit drain sends its backlog in several batches (*A backlog drains in larger batches, and capacity is not a setting*, `docs/decisions/pipeline/`), so the same question is asked **before a later exit batch's first attempt**, with the longest attempt any exit batch has made, and that batch is declined unattempted when the prediction says it would overrun or when an exit batch has already declined a retry, through the 1.x loop or an engine run that reports `declined`. Once one batch is declined every later one is, while a deadline stands: a deadline of `None` declines nothing, latch or not, and a caller that takes over an owed join can record one after a batch was declined. An engine run is timed whole and counts as one attempt, which overstates it and so declines no later than the true attempts would. An exit batch is one whose first attempt begins after `claim_stop` recorded the deadline. The deadline is the caller's `shutdown()` deadline, recorded by `Worker.claim_stop` in the same decision, under `Worker._lock`, that wins the once-only stop gate, and recorded before any stop event the worker holds is set, so a backoff the stop cuts short wakes to find it. The one exception is a stop claimed on the drain thread itself, which cannot join: the next caller off that thread takes the gate over and records its own deadline in its place, so the deadline is still that of the caller whose join follows (SPEC-068; *An expired shutdown is final, and a join the drain thread owes is handed over once*, `docs/decisions/pipeline/`). A declined retry counts once in `failed_batches` and writes its own `lost` line. A declined batch counts once in `failed_batches`, its events go to `batch_lost`, and the declined batches are announced together in one line, `lost <n> event(s); <k> exit batch(es) declined unattempted; each was predicted to end after the shutdown() deadline`; the exit drain's abandoned batches after its first are tallied the same way into one `<k> more exit batch(es) abandoned` line, written by the exit drain when it ends or by `stop()` when its join expires, before its expiry line, whichever takes the tally first. The expiry line adds the events the drain thread holds, `health().events.in_flight`. No public signature changes and no knob is added: the budget is the caller's `timeout`, and a knob would let a caller recreate the overrun.

**Measured with three exit batches** (SPEC-090, `docs/audits/2026-11-14-probes/core-pipeline/spec090_exit_budget.py`, the sink above behind a gated first batch with 30 submissions queued, `shutdown(timeout=1.2)`). On the build at `be686953`, on 3.12.12 and 3.13.1 alike: 1.07 s, the join completed, 2 attempts, `close()` once, `failed_batches=3`, SPEC-062's retry line for 20 events and one line for the other 40, `2 exit batch(es) declined unattempted`. The same exit drain making every exit batch's first attempt (the probe's `batched` mode, at `75e7bf1e`): 1.20 s, the join expired, 3 attempts, `close()` never run. At interpreter exit with a healthy but slow sink (`p04_exit.py 0.02 10000`) the process now exits inside its budget with `close()` run, and its one declined line counts the events the sink was not handed (8,918 on 3.13.1 and 9,260 on 3.12.12, each adding to 20,000 with what the sink received), where before the join expired with 19,980 events in one call.

**Why this refines "a shutdown shortens a wait; it must never skip work"** rather than breaking it: no attempt in flight is cut short, no sink reads anything new, and a retry or a batch predicted to end after the deadline is not work the process gets, since the join expires, the sink is left open and the process exits or is frozen. The trade is that a retry or a batch whose real duration would have been shorter than the longest attempt seen can be declined though it would have fitted, taken because the alternative is a sink open for the life of the process. A second trade, since SPEC-090: in a process that goes on running after an expired `shutdown()`, the batches the exit drain declined are counted lost, where the drain thread used to deliver them late.

**Rejected:**

- **Making every exit batch's first attempt** (SPEC-090's first draft). With one exit batch the first-attempt rule cost at most one extra attempt; with many it costs one per batch, and against the slow failing sink above it turned a completed join into an expired one with the sink left open (measured above, the `batched` mode).
- **A closer-grace reserve subtracted from the deadline.** The first design reserved the closer grace so `close()` would fit. It declined retries that would have delivered, and its premise was false: the worker's own sink is closed inline on the calling thread after the join, so the grace bounds only a bystander's wait and a detached close. Replaced by `_JOIN_MARGIN`, which covers only the moments the drain thread needs after its last attempt to be seen to end. The budget reserves nothing for `close()`, whose duration stays invariant 3's recorded exception.
- **Deciding the gate in one critical section and recording the deadline in another.** Measured at one run in 60 under two concurrent `shutdown()` callers, 0.9 s and 1.8 s: the drain declined against 1.8 while the 0.9 s caller did the join, that join expired, the other returned as second caller, and the sink was never closed. Gate and record are one decision, so the deadline the drain declines against is always that of the caller whose join follows.
- **Recording after the lifecycle's `_stop.set()`.** `_state._stop` is the live worker's own event, so setting it wakes a backoff before the deadline is written. The claim moved inside the critical section, before the set.
- **Joining against another caller's deadline.** A second caller waits against its own timeout; making it share the first caller's would let one `shutdown()` return later than it was asked to, breaking invariant 3.
- **A public knob** (a retry budget on `configure()` or `shutdown()`). It would let a caller recreate the overrun the rule removes, and the caller's `timeout` already says how long they will wait.

What stays out: a sink's own retry loop inside one `emit` (the budget measures a whole `emit` as one attempt), `flush()` and the sink swap (a late answer there is still delivery), the orphan path (one attempt by construction), and `Sink.close()` being unbounded. (SPEC-062; arch §12)

## History

### 2026-11-16

- **Refined** by SPEC-090 FR-003: "the first attempt of every batch is always made" became "the
  first attempt of the exit drain's first batch is always made", because the exit drain now sends
  its backlog in several batches and making each one's first attempt expired the join. A later
  exit batch is declined unattempted once one is predicted to overrun, counted and announced in
  one line, and the exit drain's abandon lines are aggregated likewise. Title, fence and entry
  rewritten in place; the measured outcome restated with three exit batches. The file keeps its
  name, which names the rule as SPEC-062 settled it. The diff review found the latch declining under a
  deadline of `None` recorded by a takeover, with a line naming a deadline there was not; the
  deadline is now read first, so `None` declines nothing, as the rule always said.

### 2026-11-04

- **Refined** by SPEC-068: the deadline is written in the decision that wins the gate, or once more
  by the caller that takes over a join the drain thread owed. Before, the entry read as though the
  winning decision were the only writer, which a `shutdown()` made inside a sink's `emit` broke —
  that caller won the gate and then raised out of `Thread.join`.

### 2026-11-03

- **Settled** by SPEC-062. Refines *A shutdown shortens a *wait*; it must never skip *work**.
