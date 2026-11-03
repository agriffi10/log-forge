---
type: Decision Area
title: "Sink contract: waiting, concurrency and shutdown"
governs: [src/log_foundry/sinks/**]
---

The settled decisions about how a sink waits, who may call it at once, and what a shutdown may
shorten. Read the fences; pull an entry only when you need the reasoning.
