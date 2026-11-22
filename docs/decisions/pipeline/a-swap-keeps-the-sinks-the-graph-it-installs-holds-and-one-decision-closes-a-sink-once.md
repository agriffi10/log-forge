---
type: Decision
title: "A swap keeps the sinks the graph it installs holds, and one decision closes a sink once"
description: "a `configure()` reads the graph it installs once, from its argument, and an owed sink in it is ceded, not released: it joins that graph and its holder's forward closes it; every release one decision starts carries a `_Keep`, so a forward skips a sink kept, beneath one still owed, or already closed by that decision. Never leave a kept sink the swap would have released owed as a root (an unfenced or held one stays owed, as before, closed by its own take); never take to close a sink beneath one in use; never re-read the configuration at a release; never keep a stored per-sink count; never defer the displaced graph's release. (SPEC-087)"
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

**What this does not reach**, recorded in `docs/architecture.md` §13 with measured counts: a
`sink_factory` that has not built, a sink past the read's bound, a caller's wrapper that closes its
member itself, and one that leaves its member alone, which §12 also holds open with a **Closed by**
in the lifecycle command-path spec. `README.md`, *Writing your own sink*, gives a caller who writes
a wrapper the two routes that lose nothing.

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
  it, which the library cannot tell apart; the closer ordering a member after its holder is what
  would close it, and that is the lifecycle command-path spec's.

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
every move keeps one home (SPEC-078 FR-001).

## History

### 2026-11-20

- **Refined** by SPEC-098: `Runtime.in_flight`, one of the five readers named, is deleted.

### 2026-11-15

- **Settled** by SPEC-087, closing audit finding 4. It refines two neighbours in place the same
  day: *A shutdown waits for a swap in flight to decide, and a swap releases only what the record
  owes*, whose fenced branch now cedes a kept sink, and *Lifecycle state has one holder, Runtime,
  and the stored State decides the transitions*, whose registry gains `cede`.
