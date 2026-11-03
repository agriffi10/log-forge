---
type: Decision
title: "The outbound baggage header renders through assembly's coercion and is capped by whole members"
description: "one `sanitize_fields` pass over the baggage, guarded at render not intake; at most `BAGGAGE_MAX_BYTES` as a prefix, never a packing; an empty key skipped; a drop announced as a count, never a key. (SPEC-064)"
---

**The outbound baggage header renders through assembly's coercion and is capped by whole members.** `format_baggage_header` makes one `sanitize.sanitize_fields` pass over the whole baggage, under the node and byte budgets of SPEC-057 and the configured ceilings, then renders each coerced value as text (an exact `str` as it is, anything else with `str()`) and percent-encodes it. The guard binds at **render**, not at intake: `set_baggage` still stores arbitrary objects, because the event coerces them at assembly (invariant 8: once, at assembly), and the inbound route (`continue_trace`, then `parse_baggage_header`) already yields only capped `str` values. Before this, the header rendered with a bare `str()`: a shared-sublist DAG never returned, a value whose `__str__` raises raised into the caller, a lone surrogate (reachable from both `set_baggage` and an inbound header) raised `UnicodeEncodeError` from `quote`, and an integer past CPython's 4,300-digit limit raised `ValueError` — against `Raises: None.` (invariant 1), and in disagreement with event assembly over the same baggage (invariant 6).

**The guard wraps the whole per-member render**, `str()` and both `quote` calls, because a `__str__` returning a `str` subclass with a raising `encode` gets past `str()` and fails in `quote`. A failing value becomes `<unserializable: T>`, named by `sanitize._type_name` (`"?"` when the name itself raises), never by `repr`. It catches `Exception`, never `BaseException`.

**The header is at most `BAGGAGE_MAX_BYTES` (8192), by whole members, as a prefix.** Members are kept in the coerced mapping's order while the header, commas included, fits; the first one that does not fit ends it, and it and every member after it are dropped even where a later one is small. A partial member is never emitted, and one member that alone exceeds the cap yields `""`. The cap is the size the next log-foundry process's `parse_baggage_header` accepts, so the formatter never emits what its twin refuses whole. For the same reason a member whose encoded key is empty (`=1`, which the parser refuses) is skipped without ending the header.

**A drop is announced** as `log-foundry: lost N baggage member(s)` through `_diag.lost`, throttled on `_diag.WARN_EVERY`, carrying a running total and never a key or a value (invariant 11). The total is a module-level `itertools.count(1)`, one `next()` per dropped member; a call writes if any value it drew is 1 or a multiple of `WARN_EVERY`, so each crossing is announced by exactly one call under concurrent callers. A member dropped by `max_keys` and a value clipped by a ceiling are not announced: the event carries `truncated` for them and the header has no slot for one.

**Rejected.**

- *Coercing at intake* (`set_baggage`): it would change what events carry, coerce twice, and leave the inbound route and the render unguarded for the same reason.
- *One pass per member*: the budgets would bound each member and not the header, and a baggage of many members would spend them many times over.
- *Rendering a value as JSON*, as the event carries it: it would turn the dict a caller already sends as its repr into JSON, a change nobody asked for; the header keeps `str()` of the coerced value.
- *Packing* (skip a misfit, keep a later small member): the header's content would then depend on a later member's size and not only on the order the caller set them in. Prefix semantics are predictable and match what a truncation means everywhere else.
- *A `health()` counter*: `health()` is the ledger of events (invariant 2), a baggage member is not one, and a counter widens the frozen public surface (invariant 10) for a loss the stderr line already announces.
- *A bare `+=`, or a lock, around the total*: `+=` can lose an increment so a line's count would not reproduce (invariant 11), and a new lock in `context.py` is a fork hazard (invariant 12); `next()` on a count is atomic under the GIL and needs neither.

**Do NOT** edit `sanitize.py` for this (a spoofed-scalar object that survives coercion can still reach `str()` here, with the same exposure assembly has, until its sibling spec closes the routes), and do not add a W3C 64-member cap: `max_keys` bounds the count at 256 and the byte cap is what the parser enforces. (SPEC-064)

## History

### 2026-11-03

- **Settled.**
