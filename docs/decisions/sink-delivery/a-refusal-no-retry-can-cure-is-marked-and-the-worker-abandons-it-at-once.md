---
type: Decision
title: "A refusal no retry can cure is marked, and the worker abandons it at once"
description: "a shipped sink that is closed or closing, or a forked child's `SQLiteSink` or `NATSSink`, raises the `SinkDeliveryError` `sinks/_refusal.final_refusal` builds, exact in type and text and carrying a private instance mark; the worker abandons such a batch at the attempt that raised it, one `failed_batches` and the abandoned line with the attempts made, and `MultiSink` re-raises one only when every child's raise was one. Any other raise, a third-party sink's included, keeps the 1.x resend. (maintainer's choice, 2026-11-12)"
---

**A refusal no retry can cure is marked, and the worker abandons it at once** — the worker re-sent
any batch an emit-only sink raised on, up to its `max_retries`, with `cycle_backoff` between
attempts, so a refusal that can never succeed cost every batch `max_retries + 1` calls and three
backoff waits before it was counted. Measured on the forked-child `SQLiteSink` fix (#342): a child
running 2000 `@trace` calls took 17.3 s to `flush()`, about 85 ms a batch, 4 attempts each. The same
resend already met every closed emit-only sink (`SQLiteSink`, `FileSink`, `RotatingFileSink`,
`KafkaSink`, `GooglePubSubSink`), and a closed engine sink reached through its `emit`, which is what
`MultiSink` and a third-party wrapper call. An engine sink called by the worker was already spared it,
since its `REJECTED` verdict is final (SPEC-072).

**The mechanism.** `sinks/_refusal.py` defines `final_refusal(message)`, which builds an exact
`SinkDeliveryError` with the message given and sets a private attribute on the instance, and
`is_final_refusal(err)`, true only for an exact `SinkDeliveryError` carrying it. Every closed-guard
section's refusal (twelve sections at the time of writing), `GooglePubSubSink`'s and `NATSSink`'s own
closed and closing refusals, `NATSSink`'s wedged-loop refusal, which closes the sink, and the two
forked-child refusals build their error through it. `Worker._emit`'s 1.x loop treats a final refusal
as it treats the last attempt: `failed_batches` moves once and the abandoned line names the attempts
actually made, `batch abandoned after 1 emit attempts` when the first raised it, the wording an engine
sink's `REJECTED` run already wrote. The orphan path makes one attempt anyway and is unchanged.

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
private, only a shipped sink raises it, and a sink of the caller's own keeps the 1.x resend.
**Rejected: a public `SinkRefusedError`** any sink could raise. It is the contract change that
decision rejected, grows the public surface, and would need its own release story; nothing outside
the package needs it yet.

**Do NOT build** a closed-guard section whose refusal builds `SinkDeliveryError` directly, a final
refusal for a failure a later attempt could cure (a sink factory's yield refusal is one: the build
may have landed by the next attempt), or a mark carried on a class. `tests/test_final_refusal.py`
holds every `with <...>.enter() as <name>:` section under `sinks/` to refusing through
`final_refusal`, derived from the tree with a floor of twelve and a fixture corpus asserting each
failure reason and a silent case; pins one `emit` call for a refused batch on every emit-only sink
and on the forked `SQLiteSink`, and `max_retries + 1` for an unmarked, a dual-typed and a transient
raise; and pins `MultiSink`'s choice both ways. `tests/test_sinks_sqlite_fork.py` pins the measured
case under a real `os.fork`.

## History

### 2026-11-12

- **Settled.** The maintainer chose the private mark over putting the refusing sinks on the engine,
  a public refusal type, or leaving the resend.
