---
type: Decision
title: "A dead worker is reported, not restarted — and as a *reason*, not a liveness flag"
description: "`Health.stopped_reason` is `None` for a live worker, a never-created one, **and** a cleanly shut-down one, so it extends the alert idiom by a term. No auto-restart: a thread that resurrects itself fights a process trying to exit. (SPEC-019)"
---

**A dead worker is reported, not restarted — and as a *reason*, not a liveness flag** — the drain loop is guarded end to end and records the exception type that ended it (`Health.stopped_reason`), because `dropped` climbing already means backpressure and must not double as "the thread is gone". A reason string is `None` for a live worker, a never-created one, **and** a cleanly shut-down one, so it extends the alert idiom by a term; an `alive` flag would read `False` on every process that has not logged yet. No auto-restart: a thread that resurrects itself fights a process trying to exit. Type name only, never the exception message (arch §6). (SPEC-019)

## History

### 2026-07-29

- **Settled.**
