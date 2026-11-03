---
type: Decision
title: "A protocol that is exported is a protocol that will be inherited"
description: "`Sink`'s members are `@abstractmethod`: empty bodies let a subclass with one typo instantiate happily and return `None` from `emit`, losing events with every counter at zero. (SPEC-034)"
---

**A protocol that is exported is a protocol that will be inherited** — `Sink`'s members were empty-bodied and not `@abstractmethod`, so a subclass with one typo instantiated happily and its inherited `emit` returned `None`: three events gone, `flush()` truthy, every counter zero. `mypy` refused it and only the runtime did not. Structural satisfaction is untouched, which matters because no shipped sink inherits it. (SPEC-034 FR-005)

## History

### 2026-08-10

- **Settled.**
