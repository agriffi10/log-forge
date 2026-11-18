---
type: Decision
title: "An event a NATS round never handed to the driver is resendable"
description: "in a `NATSSink` round where something was published, an event `_publish_all` never handed to the driver is `RETRYABLE`, not sink-counted, and its `publish_timeout` line is `LAST`: it is a definitive non-delivery, so a resend cannot duplicate it. An abandoned publish is never resent. A raised publish stays `REJECTED` there, while a round that publishes nothing, a resend round included, is answered whole as before, so its raised publish is resent and counted once per call it raised in. (SPEC-102)"
---

**An event a NATS round never handed to the driver is resendable.** `NATSSink` bounds a whole round
by one `publish_timeout`, and when the budget runs out partway through a batch, the events it never
reached are left unattempted. Until SPEC-102 a round where something else was published answered
them `REJECTED` and announced them `ALWAYS`, so the worker never offered them again. Since SPEC-090
a batch under a backlog grows to what the sink delivers in half a flush interval, so a JetStream
`NATSSink` whose budget was shorter than that lost the tail of a backlog batch: 193 of 400 events
acked at `be686953` with a 2 ms ack, 200 spans and a 0.2 s budget, where 1.x acked all 400.

**The rule.** `_verdict_of` answers an `unattempted` event `RETRYABLE` in a round where at least one
event is `ok` or `abandoned`, and leaves it out of the sink-counted set; `_register` registers the
round's `lost <n> event(s); NATSSink publish_timeout <t>s` line `When.LAST`, so it is written only
when the run ends on that round with those events unresolved, and the engine then counts them once,
through `unresolved`. When the run ends on a later round, that round names what it left; if the
abandoned-publish latch refused it, the engine's `refused by NATSSink after others landed` line names
the refused events instead. The resends are the engine's existing rounds, inside `max_retries + 1`
and its backoff, so no wait is added. `emit` runs one round and records what 1.x recorded; a direct
`deliver` outside any run commits its `LAST` line at once.

**Why resending is safe.** An unattempted event is a *definitive non-delivery*: `_publish_all` never
handed it to the driver, so no attempt wrote it and a resend cannot duplicate it, which is what *No
shipped sink declares itself idempotent* asks of any resend. That is the footing of *A client
exception costs its chunk, and is provable non-delivery for it*. SPEC-094, a Draft when this was
settled, answers its definitive reports `REJECTED` because each is a refusal every attempt would
meet again; this one is a spent
budget, and the next round brings a fresh one. It covers the events after a publish abandoned at the
deadline too: ~~the latch refuses every later round `RETRYABLE` before it publishes while that
publish is pending~~ no later round publishes while that publish is pending, so a resend never
overtakes it, and the tail is delivered only if the publish finishes before the rounds run out.
~~On the exit drain the waits between rounds are zero, which leaves it little room to, so such a
tail is lost there unless it does, counted once and named.~~

> **Refined by SPEC-104.** A round made within one `publish_timeout` of the abandonment waits for
> that publish on the loop until the window ends, and is refused `RETRYABLE` only if it is still
> pending then, or once the window has aged; so on the exit drain too, where the engine's waits
> between rounds are zero, a tail behind a publish that finishes inside its window is delivered,
> and one behind a longer stall is still counted once and named. The latch's rule lives in *A
> driver's liveness flag is not evidence* (`docs/decisions/sink-lifecycle/`).

**The bound.** A backlog batch is delivered when its delivery fits in `max_retries + 1` rounds, four
at the default, of one `publish_timeout` each. At a 0.2 s budget that is 0.8 s against SPEC-090's
0.5 s target. One probe run in twenty on 3.13.1 outlasted it, losing 39 events
counted and named, and so did one of the diff review's sixty, under CPU starvation. That residual is `architecture.md` §12's open item *A JetStream `NATSSink`
backlog batch whose delivery outlasts the worker's rounds still loses its tail*.

**What stays as it was, and the asymmetry it widens.** A publish that raised stays `REJECTED` in a
round where something was published. A publish abandoned at the deadline stays `UNKNOWN`, or
`ACCEPTED` where a core confirmation judges it, and is never resent. A round that published nothing
is answered whole, every event `RETRYABLE` and sink-counted with its `SETTLED_LAST` line (SPEC-088),
and that holds for a round resending a tail: when such a round's only publish raises, the next round
resends it and it is counted in `failed` once per round it raised in, in either mode, which before
SPEC-102 could arise only in a batch's first round. For a deterministic raise, such as nats-py's
`MaxPayloadError` or `OutboundBufferLimitError`, that is an over-count of `failed` only, which the
event ledger counts once: invariant 2's NATS exception, 1.x's own per-call count. For a JetStream
ack wait that expired it is also the asymmetry invariant 2 records, since the server may have
persisted the event, which closes only with the `Nats-Msg-Id` header, §12's own item.

**Rejected.** A run-context signal telling a resend round from a first round, so that a resend
round's raised publish could stay `REJECTED`: declined by the orchestrating session at the spec
review, because the nothing-published rule is 1.x's ledger and the `Nats-Msg-Id` item is what makes
any such resend safe. A larger default budget, a per-event budget, or a cap on the batch `NATSSink`
is handed: the batch rule is SPEC-090's, and `publish_timeout` keeps its meaning, one bound for one
round.

**Do NOT build** a resend of an abandoned publish, or of a raised one in a round where something was
published, until the `Nats-Msg-Id` item lands; and do not count an unattempted event a later round
delivered.

## History

### 2026-11-18

- **Marked** where it said the latch refuses every later round while the abandoned publish is
  pending: since SPEC-104 a round in the publish's first window waits for it, so a tail behind a
  transient stall is delivered on the exit drain too. The rule here, and its order, are unchanged.

### 2026-11-17

- **Settled** by SPEC-102 FR-001 to FR-003, measured with `spec090_nats_ack_backlog.py 0.002 200 0.2`
  at `b35ad63a`, `00ec9029` and `06b7ff41`: 19 of 20 runs on 3.13.1 and 3 of 3 on 3.12.12 acked 400 of 400.
