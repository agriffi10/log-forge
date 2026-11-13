---
type: Decision
title: "A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable"
description: "the record is stamped when the library is *handed* a sink, over the whole reachable graph, and every close consults it. Write-once alone defends only a record that already exists. (SPEC-042)"
---

**A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable** — the record is stamped when the library is *handed* a sink (`configure()`, the lazy default) over the whole reachable graph, and every close consults it, the five shipped wrappers included. Write-once alone was not enough: it defends a record that already exists, so where the parent's bounded walk saw nothing a child could `configure()` its way into genuine ownership and destroy the parent's transport legitimately — measured on a socket. A fork handler marks everything inherited that the parent never recorded `_FOREIGN` — it writes only where no live entry stands, so a sink the parent stamped keeps that pid and is refused by it — **before any other handler runs**, which is what gives "no record" a terminal state for everything that walk reached; what it missed stays unrecorded and is the §13 residual. The one default that is *not* refusal is a sink no wrapper the library holds is releasing: that is the caller's own object, and refusing there turned `FilteringSink(inner).close()` into a silent no-op. Descent is bounded on a measurement (1,109 ms → 2 ms on a 100k-event `MemorySink`) and every gap it leaves fails toward a leak. `reacquire_after_fork()` is renamed from the discard: returning from it *is* a claim of ownership, which is why a subclass adding a transport must override it. The residual — a sink the parent held only in application state, first handed over by the child — is undecidable inside the rule and recorded in §13, not asserted away. (SPEC-042, arch §9, §13)

**The record pins a sink only while the library still holds it** (SPEC-081). Each entry is an
`_Entry` holding the acquiring pid, a weak reference made once with no callback, and a strong
reference, the only field that toggles. It is strong while the sink is reachable by the stamp walk
from a live delivery root (the configured sink, the live worker's and the late worker's), owed a
close, or being closed. `_repin`, run on every exit of `_swap_sink` and after every close `release`
performs, demotes every other entry of this process to weak, and a performed close also sets
`closed`. An epoch (`Runtime._owned_clock`, bumped by `stamp` and `reclaim`) keeps a re-pin from
demoting a sink handed over while it walked, and keeps a close from marking `closed` a sink handed
over again while that close ran. The last strong reference to a demoted sink is dropped from a
local list after every lock section, so a finalizer it runs meets no library lock. A sink that
cannot be weakly referenced (no `__weakref__` slot, a `weakref.proxy` among them) keeps a strong
entry. The strong-for-life record had two reasons, and each is now kept another way:

- **`id` reuse.** An entry whose sink has died, or whose sink is not the object asked about, is
  absent to every reader and overwritten by every writer (`_entry_for`), and `_repin` prunes it
  under the record lock. A new object of the same type lands on a just-dead sink's `id` almost every
  time (99.93% over 10,000 trials, SPEC-081 FR-003), so a dead entry read as a record would refuse a
  fresh sink or lend it a dead sink's verdict.
- **A finalizer closing a collected sink in a child.** A forked child pins every inherited entry
  whose sink is alive, **first**, before the roster replaces any lock (`_pin_inherited`, through
  `_fork.register_pin`), taking no lock. Entries naming another process, the parent's stamps and the
  walk's `_FOREIGN` marks, are never demoted.

**Before an entry goes weak, the sinks beneath it are captured** (`_capture`), each with an entry
of its own, weak where it can be and marked `captured`. A strong entry was what let a child's marking reach a sink
held only inside a superseded third-party wrapper; once the wrapper can be collected, that sink
must answer for itself. The capture reads a package holder as the stamp walk does and a
third-party holder as the marking does, one container level, statically, bounded by
`_MARKING_CEILING` objects entered, and a trip or raise is announced once. The child's record seeds
are the owed sinks and every live entry **except a closed sink of a shipped class exactly**
(`_fork._defined_here(type(sink))`): a shipped sink released its transport through its closed
guard, while a closed `Sink` subclass or third-party sink ran the caller's `close()` and stays a
seed. That exclusion is what took the closed, superseded `MemorySink`s out of the marking.

**The record's lock is re-entrant.** A weakly held sink can be collected while a thread is inside a
record section, and a finalizer forwarding a close into a library wrapper then reaches
`releasable` and `release` on that thread, which hung on a plain `Lock`. Every section iterates a
snapshot. The one remaining inversion, a finalizer whose close pays a declined stop-signal hand and
so takes `_state._lock` under `_owned_lock`, is a residual in arch §13.

**Past a tripped marking, unrecorded stays unclaimable.** A child whose marking frontier trips
`_MARKING_CEILING`, or which inherited a failed marking, scans its inherited heap (`_mark_heap`
over `gc.get_objects()`, static predicates only, inside the at-fork walk, so whatever it finds
was inherited) and gives every sink-shaped object or `weakref` proxy that it finds there with no live entry a
strong `_FOREIGN` one. It then clears `_marking_failed` and carries on as a normal child. This is Andrew's
decision of 2026-11-12, taken with the question tool. **A frozen heap is a failed scan**, since
`gc.get_objects()` cannot see the permanent generation. **The last resort**, when the scan raises,
meets a frozen heap, or the walk itself raises: `_marking_failed` stays set, `releasable` refuses
every sink that has no record, and `stamp` records as `_FOREIGN` every sink that it is handed with
no live entry, except a root the library built itself (`stamp(sink, built=True)`, passed only by the lazy
default, `configure()`'s `sink_factory=` wrapper, `_FactorySink._checked` and the child's factory
adoption). Its first refused `sink=` is announced once, pointing at `sink_factory=`.

Rejected: a weak-reference callback to prune dead entries, because it runs inside the collector on
whatever thread allocates; and unfreezing a frozen heap to scan it, then refreezing, because that
changes the application's collector state behind its back, freezing for the child's life whatever
was in generations 0 to 2 at the fork. Do NOT build repair into the heap scan: it marks, and
repairs no lock and collects no hook. (SPEC-081, arch §9, §13)

## History

### 2026-11-13

- **Revised by SPEC-081.** The fence is unchanged, and this revision is what makes it true again:
  **Defect B was a hole in it.** A child whose marking tripped set `_marking_failed`, which refused
  the close of a sink that stayed unrecorded, but a child that handed such a sink to `configure()`
  again had `stamp` record it as the child's own and closed the parent's transport at its
  `shutdown()`, bare, in a `MultiSink` or in a `FilteringSink`. Unrecorded had become claimable.
  The heap fallback and the last resort close it. The body also gains the bounded record, for
  Defect A: every sink ever handed over was pinned for the process's life, and at `9f4d4dfd` a
  child forked after 500 `configure()` calls tripped the marking. It now carries the weak entry,
  the capture, the dead-entry rule, the re-entrant lock and the inherited pin.

### 2026-11-10

- **Refined by SPEC-079**: the marking now runs inside the one walk a forked child runs, as its
  marking frontier (`_lifecycle._walk_after_fork`), and its reach is unchanged: forwarded seeds from
  the live roots, then the owed and ownership records.

### 2026-09-02

- **Settled.**
