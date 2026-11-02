---
type: Decision Area
title: "Pipeline: buffer, worker, drain"
governs: [src/log_foundry/worker.py, src/log_foundry/_lifecycle.py, src/log_foundry/_fork.py]
---

The settled decisions for the buffer, the flush worker and the two drains, including what a forked
child repairs. Read the fences; pull an entry only when you need the reasoning.
