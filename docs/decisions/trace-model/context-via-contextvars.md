---
type: Decision
title: "Context via `contextvars`"
description: "not thread-locals — correct under threads and asyncio; holds a span stack plus baggage. (arch §5)"
---

**Context via `contextvars`**, not thread-locals — correct under threads and asyncio; holds a span stack + baggage. (arch §5)

## History

### 2026-06-24

- **Settled.**
