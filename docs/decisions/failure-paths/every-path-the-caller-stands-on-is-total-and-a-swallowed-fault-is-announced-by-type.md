---
type: Decision
title: "Every path the caller stands on is total, and a swallowed fault is announced by *type*"
description: "never `BaseException` — a `KeyboardInterrupt` or `SystemExit` is the operator's or the runtime's intent and must reach the caller. (SPEC-025)"
---

**Every path the caller stands on is total, and a swallowed fault is announced by *type*** — the decorator (setup, body, close, teardown), the orphan emitter and its echo, and `shutdown()` with its `atexit` drain all absorb an `Exception` and report one `_diag.absorbed` line rather than raising. Never `BaseException`: a `KeyboardInterrupt` or `SystemExit` is the operator's or the runtime's intent and must reach the caller — the same line SPEC-019 drew in the opposite direction for the worker thread, where the *absence* of a handler was the defect. A pre-body fault degrades to an **untraced call**, never a failed one; a failed close is announced, not retried (the once-only flag stays ahead of it, because a second `close()` on a partially released sink is worse than an unclosed one). Only the type is written, never the message (arch §6). `_diag` must import nothing from its own package. (SPEC-025)

## History

### 2026-08-05

- **Settled.**
