---
type: Decision
title: "A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable"
description: "the record is stamped when the library is *handed* a sink, over the whole reachable graph, and every close consults it. Write-once alone defends only a record that already exists. (SPEC-042)"
---

**A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable** — the record is stamped when the library is *handed* a sink (`configure()`, the lazy default) over the whole reachable graph, and every close consults it, the five shipped wrappers included. Write-once alone was not enough: it defends a record that already exists, so where the parent's bounded walk saw nothing a child could `configure()` its way into genuine ownership and destroy the parent's transport legitimately — measured on a socket. A fork handler marks everything inherited that the parent never recorded `_FOREIGN` — it `setdefault`s, so a sink the parent stamped keeps that pid and is refused by it — **before any other handler runs**, which is what gives "no record" a terminal state for everything that walk reached; what it missed stays unrecorded and is the §13 residual. The one default that is *not* refusal is a sink no wrapper the library holds is releasing: that is the caller's own object, and refusing there turned `FilteringSink(inner).close()` into a silent no-op. Descent is bounded on a measurement (1,109 ms → 2 ms on a 100k-event `MemorySink`) and every gap it leaves fails toward a leak. `reacquire_after_fork()` is renamed from the discard: returning from it *is* a claim of ownership, which is why a subclass adding a transport must override it. The residual — a sink the parent held only in application state, first handed over by the child — is undecidable inside the rule and recorded in §13, not asserted away. (SPEC-042, arch §9, §13)

## History

### 2026-09-02

- **Settled.**
