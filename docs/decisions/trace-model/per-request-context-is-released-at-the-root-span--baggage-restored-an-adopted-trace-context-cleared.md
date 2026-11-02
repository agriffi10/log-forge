---
type: Decision
title: "Per-request context is released at the root span — baggage restored, an adopted trace context cleared"
description: "the asymmetry is deliberate: restoring an inbound context puts back an adoption made *before* the span, leaving a warm container joining the first caller's trace forever. (SPEC-024)"
---

**Per-request context is released at the root span — baggage restored, an adopted trace context cleared** — the asymmetry is deliberate. Baggage set before any span is a process-level default, so it is restored *to*; an inbound context is a one-shot handoff to the trace it names, and restoring it would put back an adoption made *before* the span, leaving a warm container joining the first caller's trace forever. Consequences: one `continue_trace()` serves one root span (a batch needs one per record, or one `@trace` entry point), and the release lands in the context the span's `finally` runs in — so adopting outside a span and dispatching into an `asyncio.Task` needs `reset_context()`, recorded as a constraint in arch §13. Nested spans never reset: "at or below" is where baggage starts, the root span's close is where it stops. (SPEC-024, arch §5.1)

## History

### 2026-09-02

- **Settled.**
