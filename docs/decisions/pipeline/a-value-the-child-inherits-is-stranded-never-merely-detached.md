---
type: Decision
title: "A value the child inherits is stranded, never merely detached"
description: "`dup2` to `/dev/null` *and* reopen in **append** mode: rebinding alone leaves the old object flushing to the real file, and `\"w\"` truncates a log shared with the parent. The hook is **per-sink** on purpose: `StdoutSink` has the same window and must *not* be fixed. (SPEC-039)"
---

**A value the child inherits is stranded, never merely detached** — `dup2` to `/dev/null` *and* reopen in **append** mode. Rebinding the stream alone leaves the old object flushing to the real file when the GC reaches it; reopening in `"w"` truncates a log shared with the parent, which passed 1626 tests because every test forked with the file empty on disk, making truncate and append the same program. The hook is per-sink precisely because `StdoutSink` has the same window and must **not** be fixed. (SPEC-039 FR-004)

## History

### 2026-11-10

- **Refined by SPEC-079**: the hooks are collected from the repair frontier and from the marking
  frontier's forwarded seeds, so a live sink behind a third-party holder, or two containers below a
  package holder, is now stranded too; a sink reached only from the records is never hooked.

### 2026-08-10

- **Settled.**
