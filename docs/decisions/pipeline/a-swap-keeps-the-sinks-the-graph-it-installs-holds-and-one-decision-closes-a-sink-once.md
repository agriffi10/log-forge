---
type: Decision
title: "A swap keeps the sinks the graph it installs holds, and one decision closes a sink once"
description: "a `configure()` reads the graph it installs from its argument, and an owed sink in it is ceded, not released: it joins that graph and its holder's forward closes it; every release one decision starts carries a `_Keep`, so a forward skips a sink kept, beneath one still owed, or closed by that decision; a written member beneath a caller's wrapper is closed after it, never beside it. Never leave a kept sink a swap would release owed as a root; never take to close a sink beneath one in use; never re-read the configuration at a release; never keep a per-sink count or defer the displaced graph's release; never order a member behind a holder whose close is the package's; never skip the after-holder close on the strength of the member's type or a wrapper's say-so. (SPEC-087, SPEC-101)"
---

> **Refined by SPEC-098 (2026-11-20).** Of the five readers below that decide by the identity of
> the worker's top-level sink, `Runtime.in_flight` is deleted with the signal refresh, its one
> reader; four remain, and the reasoning about a kept sink left owed as a root is unchanged.

**The defect.** `configure(A)` then `configure(MultiSink(A, B))`, or the reverse, closed A at the
swap while A was a member of the graph just installed, and a sink that has released its transport
refuses every later event: a shipped `FileSink` kept 1 of 4 orphan events and 2 of 8 span events,
at every tag since `v1.0.0` (2026-11-14 audit, finding 4; `docs/architecture.md` §12, *Resolved*).
The two directions fail differently. Forward, the swap released A itself as a stale or displaced
sink. Reverse, it released the outgoing wrapper, which the new graph does not reach, and the
wrapper's own forwarded release (`release(member, owner=wrapper)`) closed A.

**Part 1: the swap reads the graph it installs, and cedes an owed sink in it.** `_swap_sink`
reads the installed graph once, before its first critical section and with no lock held, with
`_installed_graph`: the sink it is handed and every sink `_walk_beneath` finds beneath it, the walk
`_capture` already made (SPEC-081 FR-002), factored out so there is one walk beneath a sink rather
than two. Membership is by identity. It reads its argument and nothing else. A sink in that graph
is **kept**. Where the swap would have released a kept sink — a stale owed sink, or the displaced
sink once its fence is confirmed — it calls `_CloseRegistry.cede`, which takes it out of the owed
record and closes nothing, in the critical section that arms the holder: the only section with no
worker, and the section after the retarget with one, where a kept stale sink the first section
passed over joins too. A kept sink the swap would have left owed stays owed as before: one
displaced without a confirmed fence, one a retired worker keeps, one that is held. A ceded sink is
then a member of the installed graph and nothing else, which is the state `configure()` of that
graph alone produces, and it is closed when its holder is released.

**Why out of the record, not left owed.** Five readers decide by the identity of the worker's
top-level sink: `Runtime.held` (through `Worker.may_be_inside`), `Runtime.in_flight`,
`Worker.holds_unfenced`, `_inline_close_choice` and the stale loop's `stale is old`. To each, a
kept sink left in the record as a root is one nothing is using, while the drain thread is inside
it through the wrapper. Measured on SPEC-087's first draft, which left it owed: an expired
`shutdown(timeout=0.3)` closed the kept sink under the live drain thread and took 6.03 s when the
sink's `close` shared its `emit` lock, and a later swap's stale loop closed it before its drain,
so 4 of 10 queued events arrived. Out of the record: 0.31 s, not closed under the thread, and 10
of 10.

**Part 2: one decision closes a sink once.** A decision is one `_swap_sink` call or one take in
`_close_owed`. Every release a decision starts carries a `_Keep`, held for the length of
`sink.close()` on the performing thread, and a shipped wrapper's forwarded release made inside it,
at any depth of wrapper, asks `_Keep.spares` after the `releasable` check and skips a sink that is:

- **kept** — in the installed graph. Without it the reverse direction closes the live sink.
- **remaining** — in the graph of a sink the record still holds once the section's takes are
  made. Without it, with a retired drain thread inside `MultiSink(A, B)` and `MultiSink(A, C)`
  armed beside it, a `shutdown()` that took the second closed A under that thread.
- **closed** — already claimed by this decision. A take records its sink with a token no forward
  holds, before any of the decision's closes start; a forward claims with one `dict.setdefault`
  and its own token, so of two that race, one closes. Without it, two owed wrappers sharing a
  member and taken in one pass closed it twice, overlapping.

A skipped forward performs no close, registers no close in flight and returns as a refused release
does; the wrapper's own `close()` still runs and its other members are still closed. Outside a
release the library started there is no keep, so a caller closing a wrapper themselves closes
every member, as before (invariant 10).

**Part 3: no take closes a sink lying beneath one still in use.** A log with no span reads the
configured sink and arms it a moment later (`api.py`, `_ensure_sink()` then `_note_orphan_emit`).
If a swap that keeps that sink runs between the two, the arm lands after the cede and the kept sink
is a root in the record again. Forced on the real path, the unguarded mechanism then closed it
under the drain thread at an expired `shutdown(timeout=0.3)`, which took 10.01 s, and a later swap
closed it before its drain, so 5 of 9 events arrived; guarded, 0.31 s and 9 of 9. So
`_close_owed`, after its take, reads the graphs of what the take left in the record and hands no
closer a taken sink lying in one: it stays among the sinks the call did not hand off, and the
put-back the function already has returns it to the record. And `_swap_sink`, when the record holds
more than one sink, reads each owed sink's graph before its first section and passes over, in the
stale loop, a sink lying beneath the worker's current sink or one `Runtime.held` (`_lies_beneath`);
a sink in use armed after that read makes every stale sink pass over. Kept is tested first, so a
sink both kept and beneath one in use is ceded. The guard adds no lock, no lock site and no second
put-back.

**A read that trips its bound or raises** keeps what it had found, and the caller goes on. It
writes one `_diag` line each time, naming no sink change because three callers make it, saying the
read stopped short and that a shared sink may be closed while still in use. Nothing is stored to
make it once-only; `_announce_capture` and its flag stay the demotion's.

**Part 4: a written sink beneath a caller's wrapper is closed after it, never beside it**
(SPEC-101). A **caller's wrapper** is a holder whose `close` is not defined in the package, read
statically (`_ships_close`, through `_fork._member_defined_here`, so no object code runs): a
subclass of a shipped wrapper that overrides `close` is one, and one that inherits it is not. Under
Parts 1 to 3 a kept sink's close behind such a holder was the wrapper's own `close()`, which no rule
in `release` reaches. A wrapper that leaves its member alone (`Leaves`) left a sink the library
wrote to closed by nobody, and one that closes its member (`Mine`) closed it beside the record's own
close where the record also owed it, two closes overlapping. The library cannot tell the two apart,
so it closes the member after the wrapper in both cases:

- **Attached for as long as the wrapper lives.** `_CloseRegistry` records each sink it arms
  (`_written`) and, for each caller's wrapper, the members attached to it (`_after`), both under its
  `_lock` and dropped as their sinks are collected. At every `configure(sink=…)`, before its first
  critical section, each written sink in the installed graph is attached to the closest caller's
  wrapper reaching it, the one with the smallest graph (`_attach_written`). A forwarded release the
  keep spares attaches its member the same way, among the remaining sinks, or moves it into the
  installed graph when that graph keeps it (`_attach_skipped`).
- **Closed after each library close of the wrapper.** `release()` runs `_release_after` on the
  thread that closed the holder, once its `close()` has returned or raised and before its entry is
  discharged, with the same decision's keep. It skips a member the record owes, one with a close in
  flight and one the keep spares. The attachment outlives the close, so a wrapper written to and
  closed again has its member closed again, as a shipped `MultiSink(A)` does through its forward
  (SPEC-045).
- **One decision closes a member it takes after a caller's wrapper it takes.** After Part 3's
  pass-over, `_close_owed`'s take and the swap's stale take order each taken sink lying in a taken
  caller's wrapper's graph behind the closest such wrapper (`_order_after`). The member keeps its
  taken entry, in flight until it is closed once after the wrapper and dropped, so a bystander
  `shutdown()` waits for it. An ordered attachment sits beside a persistent one of the same sink and
  never replaces it.
- **A shipped holder is never ordered.** Its forward already skips a member the decision took, and
  ordering behind it ties the member to the holder's slowest close: `spec101_slow_holder.py`'s
  `multi` row read 3.0 s with shipped holders ordered and 0.0 s without.
- **Every twin path carries it**, because it lives in `release()`: the swap's closer, `_close_owed`'s
  inline close and fan-out (`shutdown()`, the exit drain, a sink the orphan path armed) and a
  shipped forward over a nested caller's wrapper. An interrupt puts a holder back with its
  persistent members, and its ordered members transitively, each entry discharged once
  (`_put_back_ordered`). A take's release that performs no close releases the members ordered
  behind it on their own, in the same mode (`_release_ordered_alone`). A forked child's
  `releasable` refuses an inherited holder first, and `_clear_after_fork` clears both records
  under the lock it already takes, so the fork roster gains no lock.

What it costs, recorded as exceptions to invariant 5: `Mine(A)` closes A itself and the library
closes it again after, in sequence, which the protocol's idempotent close absorbs; a member ordered
behind a caller's wrapper is closed only when that wrapper's `close()` returns, so a slow close
delays it and one that never returns leaves it unclosed; and a sink the library first writes
through a caller's wrapper is that wrapper's to close, because the library never delivered to it
directly. Andrew chose this rule, (a), on 2026-11-17 over three measured alternatives (SPEC-101,
*Evidence*):

- **(b) Skipping the second close of a member whose shipped closed guard already reads closed.** It
  helps only the sinks holding a `ClosedGuard` at `_guard`, on which a second close already returns
  at once, and it keys the library's close on the member's type.
- **(c) A public declaration by which a wrapper says it closes its members.** A declared `Mine`
  gets one close, at the cost of new public surface, and an undeclared one reads as (a).
- **(d) No fix, with the caller's duty documented.** `Leaves` stays at no close, against
  invariant 5's first promise.

**What this does not reach**, recorded in `docs/architecture.md` §13 with measured counts: a
`sink_factory` that has not built, a sink past the read's bound, a displaced caller's wrapper that
closes its member while that member is the live sink, and a sink the library first writes through
a caller's wrapper. The wrapper that leaves its member alone, which §12 held open, is closed by
Part 4. `README.md`, *Writing your own sink*, gives a caller who writes a wrapper the two routes
that lose nothing.

**Rejected, each built and measured for SPEC-087** (its *Evidence*, on a 32-case matrix whose
shared sink refuses after close):

- **Deferring the outgoing graph's release until the shared sink's own.** 26 of 32, but 200
  reconfigurations left 201 sinks owed, closed none of 200 unshared members before shutdown, and
  closed the shared sink 201 times. With one close per decision added, still 201 owed. The owed
  record growing with every reconfiguration is the defect SPEC-081 removed. Deferring only a
  displaced caller's wrapper: 30 of 32, and 199 of 201 closes of A overlapping at shutdown.
- **A stored per-sink reference count in the registry.** 22 of 32 and 43 failed tests: the count
  is stale wherever the graph changes after the arm, so a built `sink_factory` lost 2 of 3, and
  keeping it fresh means walking the graph at each arm, which
  `test_an_orphan_log_performs_no_stamp_walk` forbids.
- **Reading the live configuration at each forwarded release.** It delivers the same events as
  this rule, and costs two things: `_swap_sink` answers from module state rather than its argument,
  which five tests that call it directly fail on; and a later sink change answers for an earlier
  one, so a first swap's closer still inside a slow close closed the shared sink a second time
  after a second swap had displaced and closed it, 2 closes against 1.
- **Leaving a kept sink owed, as a root** — Part 1's measurement above.
- **The guard as a put-back section of its own before the closes.** It closed the sequence and
  added a lock site and a second put-back, failing `tests/test_runtime_holder.py`'s lock sites and
  `tests/test_lock_held_discharges.py`.
- **Keeping a member owed when its forward is skipped for a remaining holder.** It closes A once
  behind a wrapper that leaves its member alone, and twice, overlapping, behind one that closes
  it, which the library cannot tell apart; closing a member after its holder is what closes it,
  and that is Part 4 (SPEC-101).

**Do NOT** leave a kept sink the swap would have released in the record as a root: the five
readers above decide by the identity of the worker's top-level sink and do not see the drain
thread inside it. A kept sink displaced without a confirmed fence, or still held, stays owed as
before, where the worker's own record still names it, and is closed by its own take. **Do NOT** take, to
close, a sink that lies beneath one still in use; pass it over and leave it owed. **Do NOT** re-read
the configuration at a release: the swap answers from the sink it is handed, and a later change
must not answer for an earlier one. **Do NOT** keep a stored per-sink count of holders. **Do NOT**
defer the displaced graph's release to spare a shared sink. **Do NOT** let a forwarded release
outside a release the library started consult a keep: a caller's own `close()` of a wrapper closes
every member. A new route out of the owed record is a `_CloseRegistry` method, as `cede` is, so
every move keeps one home (SPEC-078 FR-001). **Do NOT** order a member behind a holder whose
`close` is the package's: its forward already skips what the decision took, and the order only
delays the member to the holder's slowest close. **Do NOT** skip the after-holder close on the
strength of the member's type or a wrapper's say-so: the library cannot see whether a wrapper
closed its member, and a second close in sequence is what the sink protocol already absorbs.

## History

### 2026-11-24

- **Refined** by SPEC-101, closing `architecture.md` §12's *A sink the library wrote to and never
  closes, behind a wrapper that leaves its member alone*: Part 4 added, the fence extended with
  the after-holder close and its two "never" items, and *What this does not reach* no longer
  lists the wrapper that leaves its member alone. A wrapper that closes its member now gives it
  two closes in sequence where the record also owes it, rather than two overlapping.

### 2026-11-20

- **Refined** by SPEC-098: `Runtime.in_flight`, one of the five readers named, is deleted.

### 2026-11-15

- **Settled** by SPEC-087, closing audit finding 4. It refines two neighbours in place the same
  day: *A shutdown waits for a swap in flight to decide, and a swap releases only what the record
  owes*, whose fenced branch now cedes a kept sink, and *Lifecycle state has one holder, Runtime,
  and the stored State decides the transitions*, whose registry gains `cede`.
