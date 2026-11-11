---
type: Decision
title: "A sink factory builds each process's own sink lazily, and sink= refuses only what can never work"
description: "`configure(sink_factory=f)` never calls `f`: the first emit in each process does, through a private wrapper, and a forked child is handed a new wrapper and builds its own — never in the at-fork handler. A failed build is a counted delivery failure the next emit retries. `sink=` and the wrappers' inner sinks are refused only when exactly one of a closed list of builtin types, or when the graph loops back through a package wrapper; a missing `close` is never refused. (SPEC-079, sink-loop fix; arch §13; invariants §13)"
---

**`sink_factory=` is where a process's sink comes from when each process must build its own**
(SPEC-079 FR-005, FR-006). `sink` and `sink_factory` are one setting: both in one call is a
`ValueError`, a later `sink=` clears the factory and a later factory replaces the sink. The
factory is installed behind a private wrapper, `sinks._factory._FactorySink`, which is the
configured sink and calls the factory on its **first emit in each process**, on whichever thread
emits, under a build lock, then stamps, hands the stop signal to and stores what it built. A
forked child is given a new wrapper by `_lifecycle._adopt_factory_after_fork`, which calls no
factory, does no I/O and waits on nothing, so the child's first emit builds the child's own sink,
which the child owns and closes. The parent's wrapper stays inherited, owed and refused its
close. What it built is inherited too and is refused its close, unless it re-acquires its
transport in the child — a built `FileSink` does, so the child's record names the child and the
sink reads releasable there — yet the child never closes even that one, because only the
inherited wrapper reaches it and the child does not release that; its descriptor stays open for
the child's life. A caller that wraps the installed wrapper itself in a wrapper of its own leaves
the config without a factory, so a child inherits rather than adopts, builds its own sink on its
first emit, and nothing closes it (`architecture.md` §13). A same-thread emit from inside the factory is refused rather than re-entering
or deadlocking.

**Lazy in every process is Andrew's decision of 2026-11-04**, taken with two costs stated. A
factory that raises, or returns no sink, is not refused at `configure()`: it is announced by the
wrapper (throttled, naming the type, never a value), then raised as `SinkDeliveryError` for the path
that made the call to count — the worker retries under `max_retries`, the orphan path counts
`orphan_lost` — and the next emit calls the factory again. And `get_config().sink` is the private
wrapper rather than a sink the caller built. The rejected alternative, calling the factory inside
`configure()`, would have refused a raising factory where it is written, but built a sink in a
prefork master that never logs. `configure()` does refuse what it can check: a factory that is not
callable, or whose signature cannot bind zero arguments (`config._require_factory`). The factory
must build a new sink on each call; one returning an object that existed before the fork is not
detected.

**`sink=` is refused only when it can never be a sink** (Andrew's decisions of 2026-11-05).
`_sink_arg.require_sink` refuses a value whose type is exactly one of `object`, `str`, `bytes`,
`bytearray`, `int`, `float`, `complex`, `bool`, `list`, `tuple`, `dict`, `set`, `frozenset` and
`range`, at `configure(sink=…)` and in `FilteringSink`, `TransformSink` and `MultiSink`, naming the
argument and the type. A missing `close` is never refused: an emit-only sink delivers. A value that
could still gain an `emit` — a class with `emit = None` whose `start()` binds it, a
`SimpleNamespace`, a module, a function — is accepted.

- **Rejected: the heap-type flag** ("not a heap type, and no instance `__dict__`"). Probed at
  `4fbfd60`, it refuses `weakref.proxy(sink)` and `super(Sub, inst)`, both of which deliver, because
  a C type's own attribute lookup can forward and Python cannot tell. So the refused set is a
  closed list, and another unpatchable C type is accepted and loses its batches counted.
- **Rejected: refusing a sink with no static `emit`.** It refused sinks that deliver, one whose
  `emit` is bound after `configure()`.
- **Accepted limit:** a user class with no `emit` is a heap type, which can always gain one, so it
  is accepted; if it never does, its batches are lost counted and announced (arch §13).

**A sink graph that contains itself is refused too** (Andrew's decision of 2026-11-11, the
sink-loop fix, no spec). `require_sink` runs `_sink_arg.loop_in` after the type test: a static,
bounded depth-first walk over the forwarding positions `_fork._CHILD_SINK_SLOTS` names, through a
`super` object whose `emit` resolves to a package wrapper's (a cooperative mixin's `super` resolving
to the caller's own `emit` delivers, and is not followed), and back from a `weakref.proxy` to a node
already on the path, found among that node's `weakref.getweakrefs`. A wrapper's constructor passes itself as `owner=`, so a
child leading back to it is a `ValueError` before anything is stored — the re-run `__init__` and the
subclass passing `self` to its base; `configure(sink=…)` refuses a graph holding any wrapper that
leads back to itself; and `_FactorySink`'s build treats a built graph that leads back to it, or
loops on its own, as a failed build, announced and raised as `SinkDeliveryError`, the result dropped
unclosed. No such graph worked: measured at `641dfc86`, one `emit` on a `MultiSink` holding itself
and a `MemorySink` wrote the batch 998 times, and every `flush()` raised `RecursionError`; a
`FilteringSink` or `TransformSink` holding itself raised it from `emit`, `flush` and `close`.

- **Rejected: following every attribute**, as the fork walk's marking frontier does. A sink of the
  caller's own that keeps a reference to its parent and never calls it would be refused while it
  works. Only package positions are known to forward every call.
- **Rejected: a runtime re-entry guard** on every wrapper call. It would catch the loops below, at a
  thread-local read per wrapper call across some two dozen methods.
- **Accepted limits** (arch §13): a loop through a third-party sink's own code, one closing only
  beneath a `weakref.proxy` to a node off the path, one formed after the last check (a slot assigned
  later, or two factory wrappers building around each other at once), and one deeper than
  `_LOOP_CEILING` (10,000 visits) are not found, and recurse as before.

**Do NOT** call the factory in `configure()` or in any fork handler. **Do NOT** refuse a sink for a
missing `close`, or by a rule rather than the closed list. **Do NOT** test the sink argument with
`isinstance` or read any of its attributes at `configure()` other than statically, through the
package forwarding positions. **Do NOT** follow a third-party sink's attributes when looking for a
loop. (SPEC-079 FR-005, FR-006; the sink-loop fix)

## History

### 2026-11-11

- **Widened** by the sink-loop fix (no spec), on Andrew's decision of 2026-11-11: a sink graph that
  loops back through a package wrapper is refused at a wrapper's constructor and at
  `configure(sink=…)`, and is a failed build in `_FactorySink`. The title, the fence and the last
  **Do NOT** say so; following third-party attributes and a runtime guard were rejected.

### 2026-11-10

- **Settled** by SPEC-079, on Andrew's decision of 2026-11-04 (lazy in every process) and his
  decisions of 2026-11-05 (no `close` requirement; only an unpatchable sink is refused).
