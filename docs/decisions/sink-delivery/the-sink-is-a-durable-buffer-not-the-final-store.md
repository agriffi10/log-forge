---
type: Decision
title: "The sink is a durable buffer, not the final store"
description: "ship to SQS, which absorbs spikes and outages, and let a separate consumer index into ELK. `StdoutSink` is the zero-dep default. (arch §8, §9.1)"
---

**The sink is a durable buffer, not the final store** — ship to SQS (absorbs spikes/outages), a separate consumer indexes into ELK. `StdoutSink` is the zero-dep default. (arch §8, §9.1)

## History

### 2026-06-24

- **Settled.**
