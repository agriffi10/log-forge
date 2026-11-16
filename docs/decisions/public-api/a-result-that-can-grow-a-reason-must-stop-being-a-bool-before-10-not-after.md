---
type: Decision
title: "A result that can grow a reason must stop being a `bool` before 1.0, not after"
description: "a `NamedTuple` cannot be retrofitted — a non-empty tuple is always truthy, so every `if flush():` would silently keep passing. `FlushResult`/`ContinueResult` grow by new reason values and by appended keyword fields, never by a changed `__bool__`. (SPEC-034, SPEC-089)"
---

**A result that can grow a reason must stop being a `bool` before 1.0, not after** — `flush()` answered five outcomes with one bit and `continue_trace()` two. A `NamedTuple` cannot be retrofitted (a non-empty tuple is always truthy, so every `if flush():` would silently keep passing), so `FlushResult`/`ContinueResult` carry `__bool__` plus a `reason`, and grow by new reason values and by appended keyword fields, never by a changed `__bool__` — the rule this decision exists for. SPEC-089 appended `FlushResult`'s five loss counts as keyword-only fields defaulting to zero, with their total as a read-only `lost` property and `bool()` still `ok` alone. `Worker.flush` carries the type too: the five outcomes are distinguishable only there. For the same reason `Health` and `SinkLosses` became frozen dataclasses — six specs had each argued their appended field left the indices undisturbed, and with a dataclass there are no indices. (SPEC-034 FR-007, FR-008)

## History

### 2026-11-16

- **Refined** by SPEC-089: a result also grows by appended keyword fields. `FlushResult` gained `overflow_lost`, `batch_lost`, `sink_lost`, `drain_lost`, `stranded` and the read-only `lost`; `__bool__` is unchanged, which is the rule this decision exists for, and `ContinueResult` gained nothing.

### 2026-08-10

- **Settled.**
