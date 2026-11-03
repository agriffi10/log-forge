---
type: Decision
title: "Two drains, deliberately distinct"
description: "`shutdown()` is terminal; `flush()` drains on demand and leaves everything running. A frozen-not-exited process needs the second, and `atexit` never runs there. (SPEC-013)"
---

**Two drains, deliberately distinct** — `shutdown()` is terminal (stops the worker, closes the sink); `flush()` drains on demand and leaves everything running. A frozen-not-exited process (serverless) needs the second, and `atexit` never runs there. (SPEC-013)

## History

### 2026-07-22

- **Settled.**
