---
type: Decision
title: "Buffer-then-flush, background, non-blocking"
description: "the app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)"
---

**Buffer-then-flush, background, non-blocking** — span queue flushed at span end by a worker thread; app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)

## History

### 2026-06-24

- **Settled.**
