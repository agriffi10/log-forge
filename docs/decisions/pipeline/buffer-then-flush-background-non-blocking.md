---
type: Decision
title: "Buffer-then-flush, background, non-blocking"
description: "the app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)"
---

**Buffer-then-flush, background, non-blocking** — a span's events buffer on the span and are handed to a worker thread at span end, and, since SPEC-091, the worker also takes what every open span has buffered past its `span.start` about once a flush interval (*An open span's events ship on the worker's interval*, this area); app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)

## History

### 2026-11-17

- **Refined** by SPEC-091: "span queue flushed at span end" gains the interval sweep. An open
  span's buffered events, past its held `span.start`, ship about once a flush interval rather
  than only at its close. The non-blocking rule stands: the open and a level call take no lock.

### 2026-06-24

- **Settled.**
