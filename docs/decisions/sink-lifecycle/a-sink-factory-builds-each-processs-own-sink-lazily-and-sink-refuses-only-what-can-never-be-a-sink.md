---
type: Decision
title: "A sink factory builds each process's own sink lazily, and sink= refuses only what can never be a sink"
description: "`configure(sink_factory=f)` never calls `f`: the first emit in each process does, through a private wrapper, and a forked child is handed a new wrapper and builds its own — never in the at-fork handler. A failed build is a counted delivery failure the next emit retries. `sink=` and the wrappers' inner sinks are refused only when exactly one of a closed list of builtin types; a missing `close` is never refused. (SPEC-079; arch §13; invariants §13)"
---

**`sink_factory=` is where a process's sink comes from when each process must build its own**
(SPEC-079 FR-005, FR-006). `sink` and `sink_factory` are one setting: both in one call is a
`ValueError`, a later `sink=` clears the factory and a later factory replaces the sink. The
factory is installed behind a private wrapper, `sinks._factory._FactorySink`, which is the
configured sink and calls the factory on its **first emit in each process**, on whichever thread
emits, under a build lock, then stamps, hands the stop signal to and stores what it built. A
forked child is given a new wrapper by `_lifecycle._adopt_factory_after_fork`, which calls no
factory, does no I/O and waits on nothing, so the child's first emit builds the child's own sink,
which the child owns and closes. The parent's wrapper and what it built stay inherited, owed and
refused their close. A same-thread emit from inside the factory is refused rather than re-entering
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

**Do NOT** call the factory in `configure()` or in any fork handler. **Do NOT** refuse a sink for a
missing `close`, or by a rule rather than the closed list. **Do NOT** test the sink argument with
`isinstance` or read any of its attributes at `configure()`. (SPEC-079 FR-005, FR-006)

## History

### 2026-11-10

- **Settled** by SPEC-079, on Andrew's decision of 2026-11-04 (lazy in every process) and his
  decisions of 2026-11-05 (no `close` requirement; only an unpatchable sink is refused).
