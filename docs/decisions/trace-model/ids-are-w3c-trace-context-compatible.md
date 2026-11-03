---
type: Decision
title: "IDs are W3C Trace Context compatible"
description: "`trace_id` 16B/32hex, `span_id` 8B/16hex, `log_id` UUID, so adopting tracing later stays cheap. (arch §3.1)"
---

**IDs are W3C Trace Context compatible** — `trace_id` 16B/32hex, `span_id` 8B/16hex, `log_id` UUID; makes future trace adoption cheap. (arch §3.1)

## History

### 2026-06-24

- **Settled.**
