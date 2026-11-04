---
type: Decision
title: "The exit drain declines only a retry predicted to end past the shutdown deadline"
description: "the exit drain makes every batch's first attempt and declines a retry only when the batch's longest attempt plus a 0.1 s join margin is predicted to end past the `shutdown()` deadline; the budget is the caller's `timeout`, so there is no knob. (SPEC-062)"
---

**The exit drain declines only a retry predicted to end past the shutdown deadline** — `Worker._emit` retries a failing batch `max_retries + 1` times, and its wait between attempts is `self._stop.wait(...)`, which returns at once because the stop event is set for the whole of `_final_drain`. The exit drain therefore cost up to `max_retries + 1` times one `emit`, and when that passed the join the join expired and the sink was left open (SPEC-027 FR-004). Measured at `71e42dd` with a sink whose `emit` waits 0.5 s and raises, under `shutdown(timeout=1.2)`: 1.21 s, 3 attempts, `close()` called 0 times and `lost 0 item(s)`, though three events were inside the emit; after, 1.01 s, 2 attempts and one close.

The rule: the **first attempt of every batch is always made**, a deadline of `None` declines nothing, and before each later retry `now + the batch's longest attempt + _JOIN_MARGIN > deadline` declines it. The deadline is the caller's `shutdown()` deadline, recorded by `Worker.claim_stop` in the same decision, under `Worker._lock`, that wins the once-only stop gate, and recorded before any stop event the worker holds is set, so a backoff the stop cuts short wakes to find it. The one exception is a stop claimed on the drain thread itself, which cannot join: the next caller off that thread takes the gate over and records its own deadline in its place, so the deadline is still that of the caller whose join follows (SPEC-068; *An expired shutdown is final, and a join the drain thread owes is handed over once*, `docs/decisions/pipeline/`). A declined retry counts once in `failed_batches` and writes its own `lost` line, and the expiry line adds the events in flight, counted when the emit returns. No public signature changes and no knob is added: the budget is the caller's `timeout`, and a knob would let a caller recreate the overrun.

**Why this refines "a shutdown shortens a wait; it must never skip work"** rather than breaking it: no attempt in flight is cut short, no sink reads anything new, and a retry predicted to end after the deadline is not work the process gets, since the join expires, the sink is left open and the process exits or is frozen. The trade is that a retry whose real duration would have been shorter than the batch's longest attempt can be declined though it would have fitted, taken because the alternative is a sink open for the life of the process.

**Rejected:**

- **A closer-grace reserve subtracted from the deadline.** The first design reserved the closer grace so `close()` would fit. It declined retries that would have delivered, and its premise was false: the worker's own sink is closed inline on the calling thread after the join, so the grace bounds only a bystander's wait and a detached close. Replaced by `_JOIN_MARGIN`, which covers only the moments the drain thread needs after its last attempt to be seen to end. The budget reserves nothing for `close()`, whose duration stays invariant 3's recorded exception.
- **Deciding the gate in one critical section and recording the deadline in another.** Measured at one run in 60 under two concurrent `shutdown()` callers, 0.9 s and 1.8 s: the drain declined against 1.8 while the 0.9 s caller did the join, that join expired, the other returned as second caller, and the sink was never closed. Gate and record are one decision, so the deadline the drain declines against is always that of the caller whose join follows.
- **Recording after the lifecycle's `_stop.set()`.** `_state._stop` is the live worker's own event, so setting it wakes a backoff before the deadline is written. The claim moved inside the critical section, before the set.
- **Joining against another caller's deadline.** A second caller waits against its own timeout; making it share the first caller's would let one `shutdown()` return later than it was asked to, breaking invariant 3.
- **A public knob** (a retry budget on `configure()` or `shutdown()`). It would let a caller recreate the overrun the rule removes, and the caller's `timeout` already says how long they will wait.

What stays out: a sink's own retry loop inside one `emit` (the budget measures a whole `emit` as one attempt), `flush()` and the sink swap (a late answer there is still delivery), the orphan path (one attempt by construction), and `Sink.close()` being unbounded. (SPEC-062; arch §12)

## History

### 2026-11-04

- **Refined** by SPEC-068: the deadline is written in the decision that wins the gate, or once more
  by the caller that takes over a join the drain thread owed. Before, the entry read as though the
  winning decision were the only writer, which a `shutdown()` made inside a sink's `emit` broke —
  that caller won the gate and then raised out of `Thread.join`.

### 2026-11-03

- **Settled** by SPEC-062. Refines *A shutdown shortens a *wait*; it must never skip *work**.
