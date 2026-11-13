---
type: Decision
title: "A refusal no retry can cure is marked, and the worker abandons it at once"
description: "a shipped sink that is closed or closing, or a forked child's `SQLiteSink` or `NATSSink`, raises the `SinkDeliveryError` `sinks/_refusal.final_refusal` builds, exact in type and text and carrying a private instance mark; the worker abandons such a batch at the attempt that raised it, one `failed_batches` and the abandoned line with the attempts made, and `MultiSink` re-raises one only when every child's raise was one. It is final only while every frame below its catcher and every sink in the called graph is the package's own; any other raise, a third-party sink's or a shipped refusal that reached the worker through a sink of the caller's own, keeps the 1.x resend. (maintainer's choice, 2026-11-12)"
---

**A refusal no retry can cure is marked, and the worker abandons it at once** — the worker re-sent
any batch an emit-only sink raised on, up to its `max_retries`, with `cycle_backoff` between
attempts, so a refusal that can never succeed cost every batch `max_retries + 1` calls and three
backoff waits before it was counted. Measured on the forked-child `SQLiteSink` fix at `2ff9de9e`
(#342): a child running 2000 `@trace` calls took 17.3 s to `flush()`, about 85 ms a batch, 4
attempts each. The same
resend already met every closed emit-only sink (`SQLiteSink`, `FileSink`, `RotatingFileSink`,
`KafkaSink`, `GooglePubSubSink`), and a closed engine sink reached through its `emit`, which is what
`MultiSink` and a third-party wrapper call. An engine sink called by the worker was already spared it,
since its `REJECTED` verdict is final (SPEC-072).

**The mechanism.** `sinks/_refusal.py` defines `final_refusal(message)`, which builds an exact
`SinkDeliveryError` with the message given and sets a private attribute on the instance, and
`is_final_refusal(err, sink)`, true only for an exact `SinkDeliveryError` carrying it, whose
traceback below the catcher's own frame passes through no module outside `log_foundry`, **and**
whose catcher called a sink graph that is the package's own (`_sink_arg.only_package_sinks`). Every
closed-guard section's refusal (twelve sections at the time of writing), `GooglePubSubSink`'s and
`NATSSink`'s own closed and closing refusals, `NATSSink`'s wedged-loop refusal on `emit` and on
`flush`, which closes the sink, and the two forked-child refusals build their error through it.
The `flush()` refusals among them (`KafkaSink`, `GooglePubSubSink`, `NATSSink`) carry the mark for
one rule's sake and nothing reads it there, since no caller retries a `flush`. `Worker._emit`'s 1.x loop treats a final refusal
as it treats the last attempt: `failed_batches` moves once and the abandoned line names the attempts
actually made, `batch abandoned after 1 emit attempts` when the first raised it, the wording an engine
sink's `REJECTED` run already wrote. The orphan path makes one attempt anyway and is unchanged.

**Provenance is the traceback, because the mark travels with the instance.** A sink of the
caller's own that catches a shipped sink's refusal and re-raises it by identity would otherwise
pass the mark on, and the worker would abandon a batch that sink may have arranged to succeed —
measured by the first diff review: a wrapper that swaps a fresh `FileSink` in for a closed one and
re-raises cost 1 call and lost the event, where 1.x cost 2 and delivered it. Its frame is in the
traceback, so the refusal is not final. The catcher's own frame, the first entry, is skipped, since
it is the one asking. On the shipped paths the chain holds the catcher's frame and the package's
only — no `contextlib` frame for the guard's `with` — for a direct closed sink and one behind
`MultiSink` or `TransformSink`. A subclass that overrides `emit` and calls `super().emit` is the
caller's code too, and keeps the resend.

**The traceback cannot see a caller's sink that only forwards, so the receiver is checked too.**
The second diff review found four shapes that choose where `emit` goes and return before the call,
leaving no frame: a `__getattr__` proxy, `self.emit = inner.emit` rebound on a swap, a property
returning the inner sink's bound method, and a subclass of a shipped sink that re-initialises
itself and inherits `emit`. On each, 1.x's resend reached the fresh sink the caller had swapped in
and the traceback test alone lost the batch. So `only_package_sinks(sink)` walks the graph the
catcher called, statically through the package wrappers' forwarding positions as `loop_in` does,
and requires every node's exact type to be defined in the package and none to hold an `emit` in
its own instance `__dict__`. The two halves cover different carriers: the traceback, a caller's
code that runs during the call (a re-raise, a `CallbackSink` callback, a `TransformSink`
function); the receiver, a caller's object that only routed it. Every doubt — a `super`, a
`weakref.proxy`, a graph past the walk's ceiling — answers "not final", which costs only the 1.x
resend. **Rejected: the type of the configured sink alone** (`type(worker.sink)`): a shipped wrapper
over a caller's proxy over a shipped sink passes it.

**Nothing a caller sees changes but the count.** The type is exactly `SinkDeliveryError` and the
message is today's, so `type(err) is SinkDeliveryError`, `str(err)` and every diagnostic naming the
type read as before. The mark is an instance attribute, never a class or a public name, so the
public surface does not grow (invariant 10). A subclass of `SinkDeliveryError` is never final, which
keeps SPEC-069's dual-typed driver errors, a transient `database is locked` among them, on the
resend.

**`MultiSink` re-raises a final refusal only when every child's raise was one.** It re-raised the
first child's error when every child failed; now it re-raises the first error that is not a final
refusal, or the first child's when all are. Otherwise one closed child would stop the resend a
sibling's transient failure is owed. Its counts are unchanged: it still counts nothing when it
re-raises.

**Why private, and why not the verdict.** Two recorded decisions pulled two ways, and the maintainer
chose this on 2026-11-12. The verdict is the recorded home for a final refusal (*A sink may report
per event*), but SPEC-074 fixed `KafkaSink` and `GooglePubSubSink` off the engine and kept the local
sinks off it so SPEC-069's dual-typed driver error reaches the caller as raised; putting five sinks
on the engine to spare a refused batch its retries is a spec, not a fix. And *A protocol's retry set
is a private predicate hook, not a second loop* rejected a per-sink retry set on the worker as a
change to every sink's `emit` contract. This mark changes no third-party sink's contract: it is
private, only a shipped sink raises it, it is honoured only when nothing but the package carried
it, and a sink of the caller's own keeps the 1.x resend whatever it raises.
**Rejected: a public `SinkRefusedError`** any sink could raise. It is the contract change that
decision rejected, grows the public surface, and would need its own release story; nothing outside
the package needs it yet.

**Do NOT build** a closed-guard section whose refusal builds `SinkDeliveryError` directly, a final
refusal for a failure a later attempt could cure (a sink factory's yield refusal is one: the build
may have landed by the next attempt), or a mark carried on a class. `tests/test_final_refusal.py`
holds every `with <...>.enter() as <name>:` section under `sinks/` to refusing through
`final_refusal`, derived from the tree with a floor of twelve and a fixture corpus asserting each
failure reason and a silent case; pins one `emit` call for a refused batch on every emit-only sink
and on the forked `SQLiteSink`, and `max_retries + 1` for an unmarked, a dual-typed, a transient
and a marked raise from a caller's frame; pins the replacing wrapper's delivered resend and the
four frameless forwarding shapes' (and a `TransformSink` over one); holds `only_package_sinks` to a
corpus of package and foreign graphs; pins the
NATS closing, forked and wedged refusals and the two buffering sinks' flush refusals as final; and
pins `MultiSink`'s choice both ways. It counts `emit` calls through `sys.monitoring`, since a
counting wrapper would itself be a foreign frame. `tests/test_sinks_sqlite_fork.py` pins the measured
case under a real `os.fork`.

## History

### 2026-11-12

- **Settled.** The maintainer chose the private mark over putting the refusing sinks on the engine,
  a public refusal type, or leaving the resend. Two provenance tests were added in the same
  change: the traceback, after the first diff review showed the mark alone carried through a
  caller's re-raise, and the receiver graph, after the second showed the traceback alone missed a
  caller's sink that forwards `emit` by attribute. The same class of defect twice is why the
  second answer is a structural check over the graph rather than another frame test.
