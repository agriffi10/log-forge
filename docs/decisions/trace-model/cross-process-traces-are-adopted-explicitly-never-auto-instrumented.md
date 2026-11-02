---
type: Decision
title: "Cross-process traces are adopted explicitly, never auto-instrumented"
description: "no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014)"
---

**Cross-process traces are adopted explicitly, never auto-instrumented** — `continue_trace()` takes a W3C `traceparent`/baggage the *caller* moved; no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014, arch §12)

## History

### 2026-07-22

- **Settled.**
