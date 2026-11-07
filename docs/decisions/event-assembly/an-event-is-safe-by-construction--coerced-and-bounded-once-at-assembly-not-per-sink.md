---
type: Decision
title: "An event is safe by construction — coerced and bounded once at assembly, not per sink"
description: "`build_event` runs every value through `sanitize.py`, so a sink's `json.dumps` of what assembly built is correct by consequence; since SPEC-069 each runs under a guard, and two destinations repair what they refuse. The unserializable fallback is a type name, never `repr()`. Ceilings bound each *value*; each pass is bounded too. A surrogate becomes U+FFFD, marked; a hostile key costs only itself. A transform's output is the exception: it is coerced at the transform instead. (SPEC-017, SPEC-055, SPEC-057, SPEC-058, SPEC-069)"
---

> **Narrowed by SPEC-069 (2026-11-06).** The `json.dumps` calls in `sinks/` are no longer bare: every record-building sink encodes each event under a guard before its first bulk call (`sinks/_encode.py`), so an event it still cannot encode — reachable only through a third-party wrapper or a direct `emit` — costs only itself. And "not per sink" stops at the destination: `PostgresSink` and `MongoDBSink` repair a value assembly allows and their destination refuses, after assembly, marked as assembly marks. Full entries: [An event a sink cannot encode costs only itself](../sink-delivery/an-event-a-sink-cannot-encode-costs-only-itself-and-a-batch-of-nothing-encodable-settles-counted.md) and [A destination's refusal is repaired at the sink and marked as assembly marks](a-destinations-refusal-is-repaired-at-the-sink-and-marked-as-assembly-marks.md).

> **Extended by SPEC-058.** "Once, at assembly" holds for every event `build_event` builds, not for what a `TransformSink` returns: its function runs after assembly, so its result is checked and coerced for type only at the transform. Full entry: [A transform's output is checked against invariant 8's observable and coerced for type only, never clipped](a-transforms-output-is-checked-against-invariant-8s-observable-and-coerced-for-type-only-never-clipped.md).

> **Narrowed by SPEC-057 (2026-10-19).** "Ceilings bound per *value*, not per event" is still true of
> the four ceilings, but it stopped being the whole of the promise: each assembly pass is now also
> bounded as a whole, at 131,072 values visited and 16 MiB of text charged. Those budgets bound the
> work on the caller's thread and not the event's size at any destination. The full entry is "Each
> coercion pass is bounded by a node and a byte budget", in this area.

**An event is safe by construction — coerced and bounded once at assembly, not per sink** — `build_event` runs every value through `sanitize.py`, so ~~all 40+ bare `json.dumps` calls in `sinks/` are correct by consequence~~ — superseded by SPEC-069: every sink's `json.dumps` of an assembled event is correct by consequence, and each runs under the encode guard as the backstop for what assembly did not build — it costs one pass per event rather than one per destination (`MultiSink`), and the guarantee reaches the non-JSON sinks too. The unserializable fallback is a type-name placeholder, never `repr()`, so the fix cannot widen the PII exposure arch §6 prevents. The four ceilings bound each *value*, not the event's size at a destination; SPEC-057 bounds the pass itself. (SPEC-017, SPEC-057) **No string leaves assembly that cannot encode, and a stamp that cannot is refused at `configure()`** (SPEC-055 FR-001, FR-004): the clippers return an exact `str` — measured through `str.__str__`, so a subclass's `encode` or `__str__` cannot divert it — with every lone surrogate replaced by U+FFFD and `truncated` set, on the rule `real()` applies to a non-finite float: a substitution nobody can see is a silent change to the data. One U+FFFD per surrogate, because `errors="replace"` on encode writes `?` and a `surrogatepass` round trip writes three. Undecodable `bytes` are marked the same way. The three config stamps bypass assembly on purpose (the hottest path), so `configure()` refuses a non-`str` or non-UTF-8 stamp before the ownership stamp and stores `str.__str__` of it; an over-long stamp is recorded on `invariants.md` §8 rather than clipped. A mapping key whose rendering raises is confined to its own marked `<unserializable key: T>` placeholder — the guard is in `key()`, not in `mapping()`'s loop, whose other two failures already have owners — and two hostile keys of one type colliding on one placeholder is accepted over a numbered placeholder, the first step toward one carrying the key's identity. Both placeholders go through `text()`, since `type.__name__` is writable.

## History

### 2026-11-06

- **Narrowed** by SPEC-069: sinks encode under a guard, and `PostgresSink` and `MongoDBSink` repair what their destination refuses, after assembly.

### 2026-11-02

- **Narrowed** by SPEC-057 (its marker above dates the spec 2026-10-19; it landed after SPEC-058): ceilings bound per value, and each coercion pass also has a node and a byte budget.

### 2026-10-22

- **Extended** by SPEC-058: "once, at assembly" holds for what `build_event` builds, not for what a `TransformSink` returns.

### 2026-07-27

- **Settled.**
