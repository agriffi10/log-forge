---
type: Decision
title: "A value the child inherits is stranded, never merely detached"
description: "A forked child never writes what its parent had pending: a file sink drops the parent's pending tail and reopens in **append** mode (`\"w\"` truncates a log shared with the parent), and a sink holding a buffer strands it, `dup2` to `/dev/null`, because rebinding alone leaves the old object flushing to the real file. The hook is **per-sink** on purpose: `StdoutSink` has the same window and must *not* be fixed. The child also forgets the parent's open spans before its drain resumes, so its sweep ships none of them. (SPEC-039, SPEC-082, SPEC-091)"
---

**A value the child inherits is stranded, never merely detached** — for a sink that holds a buffer, `dup2` to `/dev/null` *and* reopen in **append** mode. Rebinding the stream alone leaves the old object flushing to the real file when the GC reaches it; reopening in `"w"` truncates a log shared with the parent, which passed 1626 tests because every test forked with the file empty on disk, making truncate and append the same program. The hook is per-sink precisely because `StdoutSink` has the same window and must **not** be fixed. (SPEC-039 FR-004)

**The file sinks have no buffer to strand since SPEC-082, so their hook drops a tail instead.** They write through an unbuffered handle, and every write call hands its bytes to the operating system, so a child inherits nothing a later flush could write and the `dup2` step is retired for them. What a child can inherit is a pending tail, the unwritten rest of a line the parent's write cut partway. It is the parent's to finish, so `reacquire_after_fork` drops it without writing it, before anything in the hook can raise, then reopens the path in append mode and closes the inherited object, which closes only the child's copy of the descriptor. The rule in the title holds in its new form: a child that merely kept the tail would write the parent's bytes a second time, measured as the tail's marker on disk twice with the hook taken away (`tests/test_file_sink_fork.py`). The append-not-truncate half is unchanged. A child that writes before the parent finishes its tail still lands its first line after the parent's cut prefix; no single process can prevent that, and two processes appending to one path are out of scope. (SPEC-082 FR-005)

**The child forgets the parent's open spans before its drain resumes** (SPEC-091 FR-003). The worker sweeps every registered open span, so a child that kept the parent's registry would ship events the parent had pending: without the step, `spec091_fork.py`'s child wrote all 8 of the parent's pre-fork events again; with it, none. `_lifecycle._forget_open_spans_after_fork` clears the registry and the deferred closes, registered with `_fork.register_child_handler` after `_restart_ledger_after_fork` and immediately before `_rebuild_worker_after_fork`, never from `decorator` (which would run after the rebuild) and never with `os.register_at_fork`. The forking thread's own copied spans stay on their contexts and still ship at their close in the child, as before.

**Do NOT** write an inherited tail in the child; do NOT let a child's sweep reach a span the parent registered; do NOT reopen in `"w"` mode; do NOT bring back a buffer on the file sinks to save a write call.

## History

### 2026-11-17

- **Refined by SPEC-091**: the child forgets the open-span registry before its drain thread
  resumes, so the worker's interval sweep never ships what the parent had pending on an open
  span. The per-sink hooks are unchanged.

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
