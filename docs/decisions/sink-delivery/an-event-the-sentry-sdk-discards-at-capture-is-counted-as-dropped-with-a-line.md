---
type: Decision
title: "An event the Sentry SDK discards at capture is counted as dropped, with a line"
description: "`SentrySink`'s SDK backend reads `capture_event`'s return: `None` is an event the SDK discarded, counted in `discarded` (reported as `losses().dropped`) when `emit` returns, with one line per `emit`; sampling and `before_send` drops count too, because the library cannot tell them from an SDK that cannot deliver. A batch with any discard returns rather than raising, since a resend would roll the sampler again. (SPEC-093)"
---

**What the SDK reports, and what the sink now reads.** `SentrySink._capture` used to drop what
`capture_event` returned, so every event the call did not raise on went into `sent`, and an SDK that
kept 1 event in 5 read `sent=25` with nothing lost (2026-11-14 audit, finding 11). sentry-sdk 2.74.0,
the version `poetry.lock` pins, documents `Client.capture_event` as returning an event id that "May be
`None` if there is no DSN set or of if the SDK decided to discard the event for other reasons", and
returns `None` for an ignored error, a recursion guard, a `before_send` or event-processor drop, the
error sample rate, and no transport; `Scope.capture_event` passes that through. On the SDK branch the
sink now reads `None` as a third answer, **discarded**. The event moves no other counter; a new
attribute, `discarded`, counts it, and `losses()` reports it as `dropped`. The count is made through
the `emit`'s own `Deferral`, so it lands only when `emit` returns, and each `emit` that discarded
anything writes one line, `lost N event(s); SentrySink, discarded by the Sentry SDK at capture`. Any
value other than `None`, including `""`, `0` or an object, reads as captured, as in 1.x. The HTTP
fallback is unchanged. What the SDK loses after it returned an id, a full transport queue or a rate
limit on its worker thread, it never reports, and that stays SPEC-092's `sdk-transport-drop`
departure.

**A batch with a discard returns.** `emit` raises only when no qualifying event was sent and none was
discarded, which is 1.x's total failure. A batch of discards and failures with nothing sent returns,
its failures counted at capture in `transport_errors` (or the HTTP fallback's `failed`) as in 1.x and
its discards in `discarded`, because a resend would roll the sampler again and hand `before_send` an
event it had already dropped. A batch of nothing but failures raises as before. That is a recorded
exception to invariant 7.

**Andrew's choice, 2026-11-15.** Asked how a discarded event should be counted, he answered
**"Dropped, with a line (Recommended)"**. He was shown the precedent the other way:
`SentrySink.losses()` says that reporting a configured filter as loss would make the alert idiom fire
on every INFO log, and `FilteringSink` counts the events it declines as `SKIPPED`, counted nowhere. He
was told that deliberate sampling and `before_send` drops would show up as counted loss with a line,
and chose it, since the library cannot tell sampling from an SDK that cannot deliver: both return
`None`. A stand-in `client=` whose `capture_event` returns nothing therefore reads as discarding every
event.

**Do NOT** count a discarded event in `sent` or in `failed`; do NOT raise for a batch that discarded
anything; do NOT write the `discarded` count outside the `emit`'s `Deferral`
(`tests/test_deferral.py` lists `SentrySink.emit` among its deferring sites).

## History

### 2026-11-20

- **Settled** by SPEC-093 FR-002, with Andrew's answer of 2026-11-15. `tests/test_sentry_discard.py`
  holds it.
