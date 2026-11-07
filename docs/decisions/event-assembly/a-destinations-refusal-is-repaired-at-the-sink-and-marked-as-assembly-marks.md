---
type: Decision
title: "A destination's refusal is repaired at the sink and marked as assembly marks"
description: "Assembly cannot know the destination, so a value it allows and a destination refuses is repaired by that sink, on a copy, and delivered: `PostgresSink` turns every U+0000 into U+FFFD, `MongoDBSink` an integer outside int64 into `<int: ~N digits>` and a NUL in a key into U+FFFD, each marked `truncated`, never counted. Only a refusal the pipeline can produce, measured against the real service, decidable from one event. (SPEC-069)"
---

**Repair what the destination refuses, and deliver the event.** Invariant 8 makes every assembled event valid JSON, but two destinations refuse values valid JSON allows, and those values are reachable from `@trace`, `info()` and `configure()`: PostgreSQL's JSONB and its text columns refuse U+0000 (JSONB cannot hold `\u0000` because `text` cannot), and MongoDB's BSON refuses an integer outside `[-2**63, 2**63-1]` and a NUL in a key name. Each refusal cost the whole batch, retried to no effect — Postgres commits every chunk in one transaction, so one bad row rolled back all of them — and `configure(service="s\x00x")` lost every event to Postgres, since the stamps bypass assembly. Andrew chose on 2026-11-04 that the sink repairs only what its destination refuses and delivers the event, carrying SPEC-058's principle (a bad event costs only itself) to the point where nothing is lost. Only the sink can take this step, because only the sink knows its destination; assembly's promise is unchanged.

**The admission rule.** A refusal is repaired when the pipeline, `configure()` or a post-assembly path can produce it, a real driver or service was measured refusing the whole bulk call for it, and the sink can decide it from that one event alone. PostgreSQL's U+0000 and MongoDB's two refusals meet it. A refusal nobody has measured is not repaired; an event that still fails after the repair is unencodable and is dropped alone, counted and announced (*An event a sink cannot encode costs only itself*).

**How each is detected and repaired.** Both run inside the sink's encode function (`sinks/_encode.py`), on a **copy** rebuilt container by container, so the batch is never mutated: a `MultiSink` sibling and the worker's retry are handed the original objects.

- `PostgresSink` matches the JSON text it already builds against `NUL_ESCAPE`, `(?<!\\)(?:\\\\)*\\u0000` — a `u0000` whose preceding backslash run has odd length — so a clean event costs one search and the six characters `\u0000` written as text, which JSONB accepts, cost no walk. The search is a filter, not the decision: `repair_nul` walks for a real U+0000 and returns `None` when it finds none, so it is the walk that leaves the lookalike alone. On a match `repair_nul` replaces every U+0000 in every string value and key, at every depth, with U+FFFD before encoding again; the projected text columns are top-level values of the same event, the `service`, `version` and `env` stamps included, so one repair covers the JSON and the columns with one representation.
- `MongoDBSink` asks the driver's own `bson.encode`, imported lazily on the first `emit` that reaches the check and read by name, never at construction: a `bson` that is missing or has no `encode` disables the check rather than raising, so an injected client with no driver installed is never repaired. When it refuses, `repair_bson` replaces an out-of-range integer with assembly's too-large-number placeholder through `sanitize.int_placeholder` — factored out of `sanitize._int_text`, with a test holding `_Coercer`'s two inline copies and `_otel._int_digits` equal to it — and a NUL in a key with U+FFFD; a NUL in a *value* is left, because BSON accepts it, and a `bool` is never read as an integer. The existing `json.dumps` size measure still decides the 16 MB drop.

**Marked as assembly marks, and not a loss.** A repaired copy carries `truncated: True`, the mark assembly sets for a lone surrogate replaced by U+FFFD and for its too-large-number placeholder; an event the repair did not change gets no `truncated` key and is byte-identical to before. Two keys that repair to one text keep the later value, marked, as assembly resolves two hostile keys on one placeholder. Nothing is counted and nothing is announced, as with assembly's coercion (invariant 2).

**Do NOT** repair in assembly what only one destination refuses; do NOT mutate the batch; do NOT repair a refusal no real service was measured making; do NOT repair the JSON text by substring; do NOT import `bson` at construction or let a missing one raise; do NOT count or announce a repair. Out: a lone surrogate reaching `PostgresSink` through a direct `emit` (assembly and `TransformSink` replace them, so it is outside the pipeline and unmeasured), and SQLite column values that cannot be bound. (SPEC-069 FR-003)

## History

### 2026-11-06

- **Settled** by SPEC-069; Andrew chose on 2026-11-04 to repair and deliver rather than drop.
- **Corrected** the same day by the system-frame diff review: the entry said a substring test would
  repair the six-character lookalike. The walk decides that, and returns `None` for it; the
  pattern is a filter that keeps the walk off clean text.
