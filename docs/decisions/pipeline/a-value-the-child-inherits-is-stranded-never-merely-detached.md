---
type: Decision
title: "A value the child inherits is stranded, never merely detached"
description: "A forked child never writes what its parent had pending: a file sink drops the parent's pending tail and reopens in **append** mode (`\"w\"` truncates a log shared with the parent), and a sink holding a buffer strands it, `dup2` to `/dev/null`, because rebinding alone leaves the old object flushing to the real file. The hook is **per-sink** on purpose: `StdoutSink` has the same window and must *not* be fixed. (SPEC-039, SPEC-082)"
---

**A value the child inherits is stranded, never merely detached** — for a sink that holds a buffer, `dup2` to `/dev/null` *and* reopen in **append** mode. Rebinding the stream alone leaves the old object flushing to the real file when the GC reaches it; reopening in `"w"` truncates a log shared with the parent, which passed 1626 tests because every test forked with the file empty on disk, making truncate and append the same program. The hook is per-sink precisely because `StdoutSink` has the same window and must **not** be fixed. (SPEC-039 FR-004)

**The file sinks have no buffer to strand since SPEC-082, so their hook drops a tail instead.** They write through an unbuffered handle, and every write call hands its bytes to the operating system, so a child inherits nothing a later flush could write and the `dup2` step is retired for them. What a child can inherit is a pending tail, the unwritten rest of a line the parent's write cut partway. It is the parent's to finish, so `reacquire_after_fork` drops it without writing it, before anything in the hook can raise, then reopens the path in append mode and closes the inherited object, which closes only the child's copy of the descriptor. The rule in the title holds in its new form: a child that merely kept the tail would write the parent's bytes a second time, measured as the tail's marker on disk twice with the hook taken away (`tests/test_file_sink_fork.py`). The append-not-truncate half is unchanged. A child that writes before the parent finishes its tail still lands its first line after the parent's cut prefix; no single process can prevent that, and two processes appending to one path are out of scope. (SPEC-082 FR-005)

**Do NOT** write an inherited tail in the child; do NOT reopen in `"w"` mode; do NOT bring back a buffer on the file sinks to save a write call.

## History

### 2026-11-15

- **Refined by SPEC-082**: the file sinks write through an unbuffered handle, so the `dup2`
  step is retired for them and their hook drops the parent's pending tail instead. The
  append-mode reopen and the per-sink reason stay.

### 2026-11-10

- **Refined by SPEC-079**: the hooks are collected from the repair frontier and from the marking
  frontier's forwarded seeds, so a live sink behind a third-party holder, or two containers below a
  package holder, is now stranded too; a sink reached only from the records is never hooked.

### 2026-08-10

- **Settled.**
