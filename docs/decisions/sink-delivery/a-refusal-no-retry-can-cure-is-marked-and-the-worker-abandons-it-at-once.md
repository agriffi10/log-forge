---
type: Decision
title: "A refusal no retry can cure is marked, and the worker abandons it at once"
description: "a shipped sink that is closed or closing, or a forked child's `SQLiteSink` or `NATSSink`, raises the `SinkDeliveryError` `sinks/_refusal.final_refusal` builds, exact in type and text and carrying a private instance mark; the worker abandons such a batch at the attempt that raised it, one `failed_batches` and the abandoned line with the attempts made, and `MultiSink` re-raises one only when every child's raise was one. It is final only when the sink that refused is one the package's own graph forwarded the batch to and no caller frame carried it; any other raise, a third-party sink's or a shipped refusal that reached the worker through a sink of the caller's own, keeps the 1.x resend. (maintainer's choice, 2026-11-12)"
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

**The mechanism.** `sinks/_refusal.py` defines `final_refusal(refuser, message)`, which builds an
exact `SinkDeliveryError` with the message given and records on the instance a private mark and
the `id` of the sink that refused (an `int`, so the error still pickles and keeps no sink alive),
and `is_final_refusal(err, sink)`, which the two catchers ask with the sink they called. Every
closed-guard section's refusal (twelve sections at the time of writing), `GooglePubSubSink`'s and
`NATSSink`'s own closed and closing refusals, `NATSSink`'s wedged-loop refusal on `emit` and on
`flush`, which closes the sink, and the two forked-child refusals build their error through it,
passing `self`. The `flush()` refusals among them (`KafkaSink`, `GooglePubSubSink`, `NATSSink`)
carry the mark for one rule's sake and nothing reads it there, since no caller retries a `flush`.
`Worker._emit`'s 1.x loop treats a final refusal as it treats the last attempt: `failed_batches`
moves once and the abandoned line names the attempts actually made, `batch abandoned after 1 emit
attempts` when the first raised it, the wording an engine sink's `REJECTED` run already wrote. The
orphan path makes one attempt anyway and is unchanged.

**Provenance: final only when the library itself forwarded the batch to the sink that refused.**
The mark travels with the instance, so on its own it would let a sink of the caller's own pass a
shipped refusal on and lose a retry that sink had arranged — a wrapper that swaps a fresh `FileSink`
in for a closed one, measured at 1 call and the event lost where 1.x took 2 and delivered it. Three
reviews each found a new route of that one class: a re-raise by identity; then routes that choose
where `emit` goes and return before the call, leaving no frame (a `__getattr__` proxy, a rebound
`self.emit`, a property, a re-initialising subclass that inherits `emit`); then a `CallbackSink`
whose callable dispatches through a property, a class declaring `__module__ = "log_foundry..."`,
`FileSink.emit` replaced on the class, and an instance `emit` planted under a `str`-subclass key.
The answer that closes the class rather than the instance is positive: `is_final_refusal` requires,
beyond the exact type and the mark,

- that every traceback frame below the catcher's own, the first entry, is in a `log_foundry`
  module, which catches a caller's code that runs during the call — a re-raise, a callback, a
  `TransformSink` function, or a plain-function `emit` written outside the package; and
- that the refuser is in `_sink_arg.package_graph(sink)`: the ids of every node reached from the
  called sink through the package wrappers' forwarding positions, walked statically as `loop_in`
  is, plus the values each node holds in its instance `__dict__` (so `SyslogSink`'s and
  socket-mode `LogstashSink`'s `SocketTransport`, which refuses for them, is in). The walk answers
  `None` — not final — unless every node's exact type is defined in the package, its `emit` along
  its MRO is exactly a plain function (no property or other descriptor that routes and returns),
  and its instance `__dict__` has only exact `str` keys and none named `emit`. A sink a caller's
  object or callable routed to is never in the graph, since only a forwarding position forwards.

Every doubt — a `super`, a `weakref.proxy`, an unreadable node, a graph past the walk's ceiling —
answers "not final", which costs only the 1.x resend. On the shipped paths the traceback holds the
catcher's frame and the package's only, with no `contextlib` frame for the guard's `with`.
**Rejected: the configured sink's type alone**, which a shipped wrapper over a caller's proxy over
a shipped sink passes; **a filename test on each node's `emit`**, measured redundant — a plain
function written outside the package runs in a frame the traceback half rejects, and the mutant
removing it survived every case.

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
nine frameless routes' above, each of which delivered on 1.x; holds `package_graph` to a corpus of
package and foreign graphs, a held transport and a callback's target among them; pins the NATS
closing, forked and wedged refusals and the two buffering sinks' flush refusals as final; and
pins `MultiSink`'s choice both ways. It counts `emit` calls through `sys.monitoring`, since a
counting wrapper would itself be a foreign frame. `tests/test_sinks_sqlite_fork.py` pins the measured
case under a real `os.fork`.

## History

### 2026-11-12

- **Settled.** The maintainer chose the private mark over putting the refusing sinks on the engine,
  a public refusal type, or leaving the resend. Provenance was added in the same change, after
  three reviews each found a caller's route the previous answer missed; the third answer — the
  refuser must be in the graph the library forwarded to — is positive, where the first two
  listed what a caller's route looks like.
