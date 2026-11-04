---
type: Decision
title: "Cross-process traces are adopted explicitly, never auto-instrumented"
description: "no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014)"
---

**Cross-process traces are adopted explicitly, never auto-instrumented** — `continue_trace()` takes a W3C `traceparent`/baggage the *caller* moved; no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014, arch §12) Untrusted includes
its type: no code of an inbound value runs while it is parsed, adopted or echoed, which
`failure-paths/an-inbound-value-runs-no-code-of-its-own-anywhere-on-the-inbound-path.md` settles
(SPEC-067).

## History

### 2026-11-04

- **Refined (SPEC-067).** A pointer to the failure-paths decision that holds
  `continue_trace` and its parsers to running no code of an inbound value.

### 2026-07-22

- **Settled.**
