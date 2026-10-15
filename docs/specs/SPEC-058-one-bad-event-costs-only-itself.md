# Spec: One Bad Event Costs Only Itself — Transform Output, Per-Event Callables and Retry-Safe Drop Counts

**ID:** SPEC-058
**Status:** Draft
**Last Updated:** 2026-10-15
**Depends On:** SPEC-006, SPEC-016, SPEC-017, SPEC-026, SPEC-036

## Overview

A batch is the worker's unit of retry, so anything that raises inside one `emit` costs every event
in the batch, not the one that provoked it. Assembly closed that door for the events themselves
(invariant 8: every value is coerced once, at `build_event`, so a sink may `json.dumps` bare), but
four places still let one event take its batch down. Every measurement below was taken at
`324936e`, the tree this spec is written on.

- **A transform can break invariant 8's premise.** `TransformSink` runs its function *after*
  assembly, so the function can put `bytes`, a `datetime` or a NaN into an event, and 25 files under
  `sinks/` call `json.dumps` bare (`grep -l 'json.dumps(' src/log_foundry/sinks/*.py`).
  *Probe A:* `TransformSink(LokiSink(..., opener=fake), fn)`, where `fn` puts `bytes` into the one
  event whose `fields.i == 5`, over 31 `@trace` spans with one `info` each: one 30-event batch is
  abandoned after 4 emit attempts, `failed_batches == 1`, `flush()` reports `'abandoned'`, and 63 of
  93 events arrive — `LokiSink._render` raised `TypeError` out of `HTTPSink._items`. *Probe B:* the
  same `fn` in front of `StdoutSink(stream=StringIO())`, 10 spans: **64 lines are written for 16
  distinct events**, each four times, while the worker's abandon line says "lost 30 event(s)".
  `StdoutSink` wrote the events ahead of the bad one and then raised, and the worker retried the
  whole batch. That breaks invariant 2 ("nothing counted twice") and invariant 7 (a sink that
  delivered something reports) at once.
- **Caller code that raises for one event costs the whole batch**, at three sites, each of which
  documents it under `Raises:` today: `TransformSink`'s function, `FilteringSink`'s predicate and
  `SQSSink`'s FIFO `message_group_id` / `message_deduplication_id` callables (resolved in
  `_chunks`). The siblings are never forwarded and the worker's abandon line reports them lost.
- **The HTTP family renders the whole batch before any request.** `HTTPSink._items` calls
  `_render` on every event first, so one event whose `_render` raises abandons the batch. The same
  shape is in `_render` of `HTTPSink`, `LokiSink`, `DatadogSink`, `ElasticsearchSink`,
  `HoneycombSink` and `SplunkHECSink` (`NewRelicSink` inherits the base one), whose six docstrings
  say "TypeError: … which ``sanitize`` prevents" — false once a transform runs.
- **A per-event drop counted before a point where the same `emit` can still raise is counted again
  on every worker retry.** *Probe C:* `HTTPSink(max_batch_bytes=2000, max_retries=0)` with an
  opener that always answers 500, one span carrying one `info` with a 5,000-byte field:
  `dropped_oversized == 4` for **one** event, and the worker's abandon line counted the same event
  again ("lost 3 event(s)"). *Probe D:* `SQSSink` with an always-failing client, `max_retries=0`
  and `max_value_bytes=500_000`, one 300 KB field: `dropped == 4` for one event. A sweep of the
  files in `sinks/` for the shape — a count reported through `losses().dropped`, followed by a
  raise in the same `emit` — found it in 18 sink classes and measured the inflation in SNS (4
  counts for 1 event), Pub/Sub and Kafka (12 for 3), HTTP `_items` (4 for 1) and Syslog UDP (4 for
  1). `sinks/_socket.py`'s `_sendable` docstring already records the inflation for the socket sinks
  and claims "Nothing here can fix it … the fix belongs one level up". That claim is wrong for the
  deferral below, which is sink-local.

**The deferral rule**, stated once because three FRs apply it: *a per-event drop decided during an
`emit` is counted and announced only when that `emit` returns normally; when it raises, nothing it
decided is counted, because the worker owns the batch.* The worker retries the whole batch, so the
same drops are decided again on the next attempt, and on abandonment its own line counts the batch
once. Counting inside a raising `emit` is therefore counting once per attempt. The rule is
implemented once, as one private helper that FR-002, FR-004 and FR-005 share, so that applying it
to the other sinks (Out of Scope) is mechanical.

Four calls were settled with the maintainer before this draft and are recorded here as made: the
repair for a transform's output happens **at the transform** (FR-001); a caller callable that fails
for one event costs **only that event** (FR-002); the SQS id callables are **folded in** beside
the wrappers (FR-004); and `HTTPSink._items` renders **per event** as a backstop (FR-005).

## Scope

### In Scope

- A transform's output reaching the inner sink JSON-safe, by a check and a types-only coercion that
  clips nothing (FR-001).
- A wrapper's caller callable failing for one event costing only that event, under the ledger rule
  and the deferral rule (FR-002).
- Each wrapper reporting its own drops through `losses()` (FR-003).
- `SQSSink`'s id callables costing only their event, and its oversize drop counted once (FR-004).
- `HTTPSink._items` rendering per event, and its drops counted only when `emit` returns (FR-005).
- Restating every claim the above made false or incomplete, in source and in the two docs that
  carry them (FR-006).

### Out of Scope

- **`sinks/_time.py`'s exception set.** One unverified report says CPython on Windows raises
  `OSError` from a naive pre-1970 `datetime.timestamp()`, which `epoch_seconds` and `epoch_nanos`
  (used by Loki's and Splunk's `_render`) do not catch. Nobody here could run Windows, so the
  premise stays unverified and `_time.py` is not changed; FR-005's per-event guard is the backstop
  and bounds the cost, if the report is real, to the one event. Only a transform can produce a
  naive timestamp — assembly always stamps UTC with a `Z` (`model._iso_now`).
- **Re-applying the size ceilings to a transform's output.** FR-001 explains why a full
  `sanitize_fields` pass is the wrong tool. What a transform adds is the caller's code choosing the
  size, and FR-006 records that as an exception to invariant 8 rather than clipping it.
- **The drop-class inflation in the sinks this spec does not otherwise edit:** `SyslogSink` and
  `LogstashSink` (through `sinks/_socket.py`), `MongoDBSink`, `KinesisSink`, `FirehoseSink`,
  `SNSSink`, `AzureEventHubsSink`, `KafkaSink` and `GooglePubSubSink`; and `HTTPSink._drop_refused`'s
  conditional path (a `413`-dropped item, then a later chunk whose response read raises an
  exception `_send` does not catch). Each needs its own reading of *when its `emit` raises*, which is
  what the deferral rule keys on, so applying FR-002's helper to each is a separate spec's work. That
  spec also revisits `_socket.py`'s recorded exception and the "`dropped` is an exact count" claims
  (`sinks/_socket.py` `_sendable`, `sinks/http.py` `_drop_refused`), which this spec leaves standing.
  Recounting `failed` is out of scope here and there: `SinkLosses` documents `failed` as an upper
  bound.
- **`CallbackSink`.** Its function runs once per batch and is the terminal destination, not code
  that runs once per event in front of one.
- **Changing what a *total* failure raises.** When every event of a batch fails, the first
  exception is re-raised unchanged and nothing is counted — today's behaviour, byte for byte,
  because the worker's retry and its abandon line already own that case and a wrapper count beside
  them would count the batch twice.
- **Any new public name or `configure()` key.** Every public name is frozen for 1.x (invariant 10);
  the counts, the helper and the coercion entry point are private.

### Twin paths (invariant 6)

- **Worker path and orphan path.** Both call the configured sink's `emit`; the worker with the
  drained batch, the orphan path (`api.py`) with a **one-event** batch, absorbing a raise into
  `orphan_lost`. Every FR's behaviour holds on both, and the one-event batch is the reason the
  ledger rule exists: an event whose callable fails *is* the whole batch there, so the exception is
  re-raised and `orphan_lost` counts it once, where a wrapper count as well would count it twice.
- **A sink's `emit` and its `flush`.** The wrappers' `flush` only forwards and carries no events,
  so nothing here changes it; no FR adds a drop that `flush` could also decide.
- **`MultiSink` with one of these as a child.** The child's own `losses()` is summed with its
  siblings', so a wrapper's drop count appears once in the total and nothing is counted twice
  (FR-003).

---

## Functional Requirements

### FR-001: A transform's output reaches the inner sink JSON-safe — coerced at the transform, never clipped

#### Description:

Every `dict` the function returns is checked once with one expression, which is invariant 8's
observable: `json.dumps(result, allow_nan=False, ensure_ascii=False).encode("utf-8")`. It is the
stricter of the two readings on purpose — a NaN and a lone surrogate both pass a bare
`json.dumps`, and the second is what the SQL sinks' drivers reject once it reaches a column
(SPEC-055). A result that passes is
forwarded as **the same object**, untouched. A result that fails — with any `Exception`, a
`RecursionError` included — is replaced by a **types-only coercion**: the conversions assembly
already applies (`sanitize`'s per-type rules — `bytes` decoded, `datetime` to ISO text,
`Decimal` and `UUID` to text, a non-finite float to its marker, a lone surrogate to U+FFFD, a
cycle to its marker, anything else to the type-name placeholder) with **no size ceiling**: no
`max_value_bytes`, `max_keys` or `max_depth` clipping. `truncated: True` is set on the event when
the coercion made a substitution assembly marks (a non-finite float, a lone surrogate, undecodable
bytes); a type-name placeholder does not set it, which is invariant 8's existing rule. The
coercion builds a new mapping and never mutates the one the function returned. A coercion is not
loss: nothing is counted and nothing is announced, because assembly does neither. A result the
coercion still cannot make pass the check, or whose coercion raised, is the same failure as the
function raising (FR-002).

*Why types-only.* Running `sanitize_fields` over a whole event pushes `fields` one level deeper, so
`max_depth` cuts a level early, and clips a long `error.stack`-style string to `max_value_bytes`
rather than `max_stack_bytes`. Measured at `324936e`, a types-only pass left all 60 events of a
hostile-input corpus (8 KB+ strings, NaN, lone surrogates, 40 KB stacks, 300-key maps, 8-deep
nesting) unclipped, at about 6 µs per event. *Why check first.* A valid result is untouched by
construction, and only a result that already failed ever meets the coercer. A peer spec in flight
(SPEC-057) adds per-pass node and byte budgets to `sanitize.py`; the coercion here runs under
whatever pass-level budget `sanitize.py` carries when it runs, and this spec adds none and removes
none.

The coercion entry point is a new private module function in `src/log_foundry/sanitize.py`, taking
the event mapping and returning the coerced mapping and the `truncated` flag; its name is an
implementation detail, `__all__` does not change, and no public name is added (invariant 10). The
size ceilings do not bound what a transform adds — FR-006 records that on invariant 8.

#### Acceptance Criteria:

- [ ] A result that passes the check is forwarded as the same object (`is`) with no `truncated` key
      added; a result that fails it is forwarded as a different, new mapping and the function's
      returned dict is unchanged afterwards (deep-equal to a copy taken before `emit`).
- [ ] Probe A, re-run: all 93 events reach the Loki opener's bodies, `failed_batches == 0`,
      `flush()` is ok, and the value that was `bytes` arrives as its decoded text.
- [ ] Probe B, re-run: exactly 30 lines are written for 30 distinct `log_id`s, none twice, and
      `failed_batches == 0`.
- [ ] Per type, literal expectations: valid-UTF-8 `bytes` becomes its text; a `datetime` its ISO
      string; a `Decimal` and a `UUID` their text; NaN and `inf` their markers with `truncated: True`;
      a lone surrogate U+FFFD with `truncated: True`; undecodable `bytes` a replaced string with
      `truncated: True`; an instance of an unknown class its type-name placeholder with `truncated`
      absent.
- [ ] A NaN, and separately a lone surrogate, as the only fault in a result are coerced — both pass
      a bare `json.dumps`, so a check weaker than the one above would forward them.
- [ ] No ceiling: a result that fails the check for one reason and also carries a string of three
      times `max_value_bytes`, a 300-key mapping, an 8-deep nest and a 40 KB `error.stack` reaches the
      sink with all four equal to the input, and `truncated` absent.
- [ ] A result nested 20,000 deep, whose check raises `RecursionError`, reaches the inner sink as an
      event that passes the check, and no `RecursionError` escapes `emit`.
- [ ] A coercion writes nothing to stderr and leaves `losses()` and `health()` as they were
      (invariant 2: a coercion is not loss).
- [ ] The same transform on the orphan path (`info()` outside a span) delivers the coerced event,
      with `orphan_lost == 0` (invariant 6).
- [ ] `log_foundry.sanitize.__all__` and the frozen public surface are unchanged, and
      `tests/test_public_surface.py` passes (invariant 10).
- [ ] Removing the coercion reddens the Probe A and Probe B criteria; coercing unconditionally,
      without the check, reddens the identity criterion.
- [ ] Serves invariant 8 (a transform's output is held to its observable), invariant 6 on both
      delivery paths, and invariant 10.

### FR-002: Caller code that fails for one event costs only that event

#### Description:

The sites are `TransformSink.emit` (the function) and `FilteringSink.emit` / `_passes` (the
predicate; its evaluation order is unchanged — predicate first, then the level floor). For
`TransformSink`, a result that is neither `None` nor a `dict` is the same failure as a raise: the
wrapper builds a `TypeError` naming its own class and the type returned, never the value.

*The ledger rule.*

1. **If every event in the batch failed this way** — at least one event, nothing forwarded and
   nothing deliberately dropped by a `None` return, a `False` predicate or the level floor — the
   first failure's exception is re-raised unchanged and **nothing is counted or announced** by the
   wrapper. That is today's behaviour on that path: the worker's retry and its abandon line own the
   batch, and on the orphan path it is absorbed into `orphan_lost` exactly as today.
2. **Otherwise** the failed events are set aside and the rest, if any, is forwarded. If the inner
   `emit` raises, it propagates and **nothing is counted** — the worker retries the whole batch, the
   callable runs again, and counting now would count the same event once per attempt. Only once the
   inner `emit` has returned normally, or was not called because everything else was deliberately
   dropped, are the failures counted, in the wrapper's own drop count (FR-003), and announced
   **once per batch** through `_diag.lost("event", n, detail)`, where `detail` names the wrapper
   class and the first failure's exception **type**, never its value or message (invariant 11).

*Accepted cost.* An event whose callable failed transiently is lost without a retry when its
siblings succeeded, because retrying the batch would re-deliver the siblings, and duplicates break
invariant 2 worse than one counted loss. The guard catches `Exception`, never `BaseException`
(invariant 1's convention), so a `KeyboardInterrupt` from the callable propagates untouched.

The pending drops of one `emit` are local to that call, so the worker's `emit` and orphan callers
on other threads never share them (SPEC-028); the deferral rule is one private helper that
FR-002, FR-004 and FR-005 all use, defined once. Its name, module and shape are implementation
detail the criteria do not bind.

#### Acceptance Criteria:

- [ ] `TransformSink(StdoutSink(stream=StringIO()), fn)` where `fn` raises `ValueError` for the one
      event with `fields.i == 5`, over 10 spans: 29 lines are written, each distinct event once;
      `failed_batches == 0`; `losses().dropped == 1`; and stderr carries exactly one
      `lost 1 event(s)` line, naming `TransformSink` and `ValueError`.
- [ ] A `FilteringSink` predicate raising for one event of a batch behaves identically: the others
      are forwarded in order, one is counted, one line is written naming `FilteringSink` and the
      exception type. The predicate runs before the level floor, so a below-floor event whose
      predicate raises is a failure, not a quiet drop.
- [ ] A callable that raises for **every** event: the first exception is re-raised and is the same
      object (`is`) the callable raised, the inner `emit` is not called, `losses()` is unchanged,
      the wrapper writes no line, and the worker's abandon line is the only line written with
      `failed_batches == 1`.
- [ ] A batch with one failure and every other event dropped by `None` (or `False`, or the level
      floor): the inner `emit` is not called, the failure is counted once and announced once, and
      `emit` returns normally.
- [ ] Retry probe, forwarding a failure: with an inner sink whose first `emit` raises and second
      succeeds, after the first attempt the wrapper has counted nothing and written no line; after
      the second it has counted the failed event exactly once and written exactly one line.
- [ ] Retry probe, abandonment: with an inner sink that raises on every attempt, the wrapper's count
      stays 0 and it writes no line, and the worker's abandon line is the only count of the batch.
- [ ] A `TransformSink` function returning a `str` for one event of several drops that event as a
      failure, and a sentinel value inside the returned object appears nowhere on stderr
      (invariant 11); returning a `str` for **every** event re-raises a `TypeError` naming the class
      and the type `str`, counts nothing, and does not call the inner `emit`.
- [ ] A callable raising an exception whose message contains a sentinel, with the event carrying a
      sentinel field: neither sentinel appears on stderr; only the exception type name does
      (invariant 11).
- [ ] An empty batch calls nothing, counts nothing and returns normally; a `KeyboardInterrupt`
      raised by the callable propagates out of `emit` uncounted.
- [ ] Two concurrent `emit` calls on one wrapper keep separate pending drops: while call A's inner
      `emit` is parked, call B (one failure, inner succeeds) commits and announces its own one drop,
      and A then raising leaves the count at B's one.
- [ ] Orphan path: `info()` outside a span through a wrapper whose callable raises for that event
      is absorbed into `orphan_lost == 1` exactly as before, with the wrapper's count 0 and no line
      from it (invariant 6: the one-event batch is the all-failed case).
- [ ] `transform.py`, `filtering.py`, `sqs.py` and `http.py` take their deferral from the one private
      helper; none defines its own accumulate-then-commit state.
- [ ] Serves invariants 2, 7 and 11, and invariant 6 on both delivery paths.

### FR-003: Each wrapper reports its own drops through `losses()`

#### Description:

`TransformSink.losses()` and `FilteringSink.losses()` are both `return read_losses(self._inner)`
today, so a failure counted by FR-002 would have nowhere to go. Each now returns `None` **only**
when the inner sink reports `None` *and* the wrapper's own count is zero; otherwise
`SinkLosses(dropped=inner.dropped + own, failed=inner.failed)`, reading an inner `None` as zeros —
the rule `MultiSink.losses()` applies to a child that reports nothing (SPEC-036 FR-005). The count
is a **private** attribute guarded by a lock, because `emit` runs on the drain thread and, on the
orphan path, on caller threads; it is reported only through `losses()`, so no public name appears
(invariant 10). The existing statement that events a wrapper *declines* to forward are not loss — a
`None` return, a `False` predicate, the level floor — stays true: only a callable's failure is
counted.

#### Acceptance Criteria:

- [ ] Inner reports `None` and the wrapper has counted nothing: `losses() is None`, for both
      wrappers.
- [ ] Inner reports `None` and the wrapper has counted `n`: `losses() == SinkLosses(dropped=n,
      failed=0)`.
- [ ] Inner reports `SinkLosses(dropped=a, failed=b)` and the wrapper has counted `n`:
      `losses() == SinkLosses(dropped=a + n, failed=b)`.
- [ ] Inner reports `SinkLosses(dropped=0, failed=0)` and the wrapper has counted nothing: the
      result is that non-`None` zero, not `None`.
- [ ] A `None` return, a `False` predicate and a below-floor level never move the count, however
      many events they drop.
- [ ] `health().sink` through the worker equals the sum, and a `MultiSink` holding the wrapper plus
      a sibling reports `dropped` equal to the wrapper's `losses().dropped` plus the sibling's, with
      no count appearing twice (invariant 6).
- [ ] `TransformSink(FilteringSink(...))` counts each wrapper's own failures once, in its own
      `losses()`, and the outer reports the sum.
- [ ] Sixteen threads each calling `emit` on a two-event batch whose second event fails lose no
      increment: the final count is exactly 16.
- [ ] After `os.fork()` repair, a child that inherited the wrapper while a parent thread held its
      count lock can call `emit` and `losses()` without blocking (invariant 12).
- [ ] Neither wrapper's public attribute names change (the sets of names not starting with `_`,
      before and after, are equal) and `tests/test_public_surface.py` passes (invariant 10).
- [ ] Serves invariant 2 (a drop counted in exactly one term) and invariant 7.

### FR-004: `SQSSink`'s FIFO id callables cost only their event, and its oversize drop is counted once

#### Description:

`_chunks` resolves `message_group_id` and `message_deduplication_id` per event on a FIFO queue. A
raise from either for one event drops that event, under FR-002's ledger rule: if every event of the
batch failed (no event reached a chunk and none was dropped as oversized), the first exception is
re-raised and nothing is counted; otherwise the failed events are counted, and announced once per
batch, only once `emit` is known not to raise. The count is private and folded into
`losses().dropped`; the public `dropped_oversized` keeps meaning oversize only.

In the **same function**, the existing oversize drop gets the same deferral. `_chunks` counts and
announces it before any `_send`, and `emit` can then raise `SinkDeliveryError` and be retried
(Probe D: `dropped == 4` for one event). It is a twin of the new drop in the function this FR
already restructures, and its announcement keeps today's shape — one line per oversized event —
but is written only when `emit` returns normally. `emit` raises `SinkDeliveryError` when every chunk
failed and something recoverable was lost, and re-raises the first id-callable exception when every
event failed; a raise from either leaves nothing counted by `_chunks`. On a standard queue the id
callables are never consulted, and only the oversize deferral applies.

#### Acceptance Criteria:

- [ ] `message_group_id` raising for one event of a FIFO batch of N: the fake client receives N-1
      entries, not the failed one; `losses().dropped == 1`; one `lost 1 event(s)` line names
      `SQSSink` and the exception type.
- [ ] The same for `message_deduplication_id` raising for one event.
- [ ] Every event's id callable raises: the first exception is re-raised and is the same object, the
      client is never called, `losses()` is unchanged and no line is written.
- [ ] One event whose id callable raises plus one that is oversized, nothing else: `emit` returns
      normally, the client is never called, and `losses().dropped == 2`.
- [ ] Retry probe, id failure: a client that fails the whole first `emit` (so it raises
      `SinkDeliveryError`) and serves the second — after the first, `losses().dropped == 0` and no
      line; after the second, `losses().dropped == 1` and exactly one line.
- [ ] Retry probe, oversize (Probe D, re-run): after the worker abandons the batch,
      `dropped_oversized == 0`, `losses().dropped == 0`, no oversize line is written, and the abandon
      line is the only count of the event; with a client that fails once then succeeds, the oversized
      event is counted exactly once and its line written exactly once.
- [ ] `dropped_oversized` is not moved by an id-callable failure.
- [ ] On a standard queue (not `.fifo`), neither callable is called, and an oversized event is still
      counted only when `emit` returns normally.
- [ ] A callable's exception message and the event's field values appear nowhere on stderr
      (invariant 11).
- [ ] Orphan path: `info()` outside a span with a raising id callable is absorbed into
      `orphan_lost == 1`, `losses().dropped == 0` (invariant 6).
- [ ] The deferral is the helper FR-002 defines, not a second copy.
- [ ] Serves invariants 2, 7 and 11, and invariant 6 on both delivery paths.

### FR-005: `HTTPSink` renders per event, and counts what it dropped only when `emit` returns

#### Description:

`HTTPSink._items` renders and sizes every event before any request, so one event whose `_render` or
`_item_size` raises abandons the batch. `_render` and `_item_size` for one event now run under a
guard that catches `Exception`, never `BaseException`, and a raise drops that event only. All drops
decided in `_items` — oversize **and** unrenderable — are counted and announced only when `emit`
does not raise; when it raises `SinkDeliveryError`, nothing from `_items` is counted and nothing is
announced, because the worker owns the batch. That closes Probe C for the whole HTTP family, since
`NewRelicSink`, `OpenSearchSink` and the rest reach `_items` through the one `emit`. The unrenderable
count is private and folded into `losses().dropped`; `dropped_oversized` keeps meaning oversize
only. That fold-in must reach every subclass's `losses()`: `ElasticsearchSink` (and so
`OpenSearchSink`) overrides it and reads `dropped_oversized` directly.

A batch in which every event is unrenderable or oversized settles **without raising**, counted and
announced — the same rule as an all-oversized batch today, because re-rendering identical input in
the sink's own code fails identically and a retry cannot help. That is deliberately not FR-002's
all-failed rule, which re-raises: a caller's callable can fail differently on the next attempt,
and the sink's own render of identical input cannot. The announce line for unrenderable
events names the class and the first exception **type** (invariant 11); the oversize line keeps
its existing shape. This is also the backstop for the unverified Windows report in *Out of Scope*:
if a naive pre-1970 `datetime.timestamp()` does raise `OSError` there, the cost is bounded to the
one event.

#### Acceptance Criteria:

- [ ] A batch of N where `_render` raises for one event: one request carries the other N-1, the
      failed one is counted once in `losses().dropped`, one line names the class and the exception
      type, and `failed_batches == 0`.
- [ ] The same where `_item_size` raises for one event (`LokiSink` overrides it).
- [ ] Every event unrenderable: `emit` returns normally, makes no request, counts N, writes one line,
      and `failed_batches == 0`. Every event oversized behaves as it does today.
- [ ] A batch mixing oversize, unrenderable and sendable events: `dropped_oversized` counts only
      the oversize ones, `losses().dropped` counts oversize plus unrenderable, and the sendable ones
      are delivered.
- [ ] Over every `HTTPSink` subclass, derived from `__subclasses__()` transitively rather than a
      hand list, one unrenderable event gives `losses().dropped == 1` — `ElasticsearchSink` and
      `OpenSearchSink` included.
- [ ] Retry probe (Probe C, re-run): after the worker abandons the batch, `dropped_oversized == 0`,
      `losses().dropped == 0`, no per-event-ceiling line is written, and the abandon line is the only
      count of the event; with an opener that answers 500 once then 200, the oversized event is
      counted exactly once and its line written exactly once.
- [ ] Retry probe, unrenderable: the same two runs with an unrenderable event count it zero times
      and exactly once respectively.
- [ ] A `KeyboardInterrupt` raised by `_render` propagates out of `emit`, with nothing counted.
- [ ] A sentinel in the exception message or in the event's fields appears nowhere on stderr
      (invariant 11).
- [ ] Orphan path: a single unrenderable event through `info()` outside a span settles inside the
      sink — `losses().dropped == 1`, `orphan_lost == 0` — the way an all-oversized one does today,
      and the event is in exactly one term (invariant 6).
- [ ] The deferral is the helper FR-002 defines, not a second copy.
- [ ] Serves invariants 2, 7, 8 and 11, and invariant 6 on both delivery paths.

### FR-006: Every claim the above made false or incomplete is restated

#### Description:

This FR cites invariant 8 rather than opting out: its centre is the page's own text — §8's premise
and a new Recorded exception — and the docstrings beside it restate the same claim at the places it
was repeated. The sites, by path:

- **Nine `Raises:` lines** saying "TypeError: If the event is not JSON-serializable, which
  ``sanitize`` prevents": `_render` in `sinks/http.py`, `sinks/loki.py`, `sinks/datadog.py`,
  `sinks/elasticsearch.py`, `sinks/honeycomb.py` and `sinks/splunk.py`; `_row` in `sinks/clickhouse.py`
  and `sinks/postgres.py`; `_frame` in `sinks/syslog.py`. Each is restated to name both sources —
  assembly, or a `TransformSink`'s coercion — and the six `_render` lines add that `HTTPSink._items`
  drops an event whose render raises anyway.
- **`sinks/transform.py`:** the class docstring, `emit` and `losses` — coercion, the per-event
  failure, what still raises (only the all-failed re-raise and the inner sink's own exception), and
  what is counted.
- **`sinks/filtering.py`:** `emit`, `_passes` and `losses` — "Whatever the predicate raises" no
  longer describes a batch that has any other event.
- **`sinks/sqs.py`:** `_group_id`, `_dedup_id`, `_chunks`, `emit` and `losses` — "Whatever a
  caller-supplied callable raises" and the oversize wording.
- **`sinks/http.py` and `sinks/elasticsearch.py`:** the `HTTPSink` class `Attributes:`, `_items`,
  `emit` and both `losses` docstrings, for the per-event guard and the unrenderable fold-in.
- **`docs/invariants.md` §8:** the premise reads as holding at assembly *and* at a transform's
  coercion, and a Recorded exception says the ceilings bound the values of an assembled event and do
  not bound what a transform adds. **§2's Recorded exceptions:** its count of losses counted but
  never retried (three) is restated to include a wrapper's or an SQS id callable's per-event failure
  when its siblings were delivered, and an unrenderable HTTP event.
- **`docs/architecture.md`:** §6's statement that one pass at assembly is correct by consequence for
  every `json.dumps` call site, and §8's "three cases where nothing landed and the sink still must
  not raise", which gains the unrenderable event beside the oversized one.

The decisions-register entry is not part of this FR; the completion ritual writes it.

#### Acceptance Criteria:

- [ ] `grep -rn 'sanitize.* prevents' src/` finds nothing, and each of the nine sites names both
      sources of the guarantee; the six `_render` sites also name the `_items` guard.
- [ ] The `TransformSink` class docstring and `emit` / `losses` docstrings state the coercion, the
      per-event failure with its `Raises:` limited to the all-failed re-raise and the inner sink's
      own exception, and the count; `FilteringSink`, `SQSSink` and `HTTPSink` docstrings do the same
      for their sites, and none of the three greps
      `grep -rn 'Whatever the mapping' src/log_foundry/sinks/`,
      `grep -rn 'Whatever the predicate' src/log_foundry/sinks/` and
      `grep -rn 'Whatever a caller' src/log_foundry/sinks/` finds anything.
- [ ] `docs/invariants.md` §8 and §2 carry the restatements above, and §8's `Guarded:` line names
      the test that asserts FR-001.
- [ ] `docs/architecture.md` §6 and §8 are restated as above.
- [ ] `sh scripts/docs-lint.sh`, `poetry run python scripts/docstring-lint.py` and
      `sh scripts/spec-lint.sh` pass, and every test that asserts a docstring (`grep -rn '__doc__'
      tests/`) still passes.
- [ ] `sinks/_socket.py` and `HTTPSink._drop_refused` are not edited (Out of Scope).
- [ ] Serves invariant 8, which the invariants page's text and the docstrings must state alike.

---

## API / Interface Contract

Nothing public changes. Constructors, `emit`, `flush` and `close` keep their signatures; the only
observable additions are non-zero values in `losses().dropped` and the lines FR-002, FR-004 and
FR-005 write.

```
# unchanged public signatures
TransformSink(inner: Sink, fn: Callable[[dict[str, object]], dict[str, object] | None])
FilteringSink(inner: Sink, *, predicate=None, min_level=None)

# changed behaviour of an existing method
TransformSink.losses() -> SinkLosses | None
FilteringSink.losses() -> SinkLosses | None
    # None only if inner reports None AND the wrapper's own count is 0; else
    # SinkLosses(dropped=inner.dropped + own, failed=inner.failed), an inner None read as zeros

# private, src/log_foundry/sanitize.py — the name is the implementation's; nothing is exported
_coerce_unbounded(event: dict[str, object]) -> tuple[dict[str, object], bool]
    # types-only coercion, no size ceiling; the flag is `truncated`

# private, one definition shared by transform.py, filtering.py, sqs.py and http.py
# the deferral rule: per-emit local pending drops, committed to the owner's own count and
# announced once per batch, only after the emit is known not to raise
```

## File & Folder Structure

```
src/log_foundry/
├── sanitize.py                 # the private types-only coercion
└── sinks/
    ├── transform.py            # FR-001, FR-002, FR-003, FR-006
    ├── filtering.py            # FR-002, FR-003, FR-006
    ├── sqs.py                  # FR-004, FR-006
    ├── http.py                 # FR-005, FR-006
    ├── elasticsearch.py        # FR-005 (losses), FR-006
    ├── loki.py  datadog.py  honeycomb.py  splunk.py          # FR-006 (_render docstring)
    ├── clickhouse.py  postgres.py  syslog.py                 # FR-006 (_row / _frame docstring)
    └── <the deferral helper's home>                          # defined once; FR-002
docs/
├── invariants.md               # FR-006: §8 and §2's Recorded exceptions
└── architecture.md             # FR-006: §6 and §8
tests/                          # one group per FR, beside the existing per-sink test files
```

## Implementation Phases

### Phase 1: Wrappers

- The private types-only coercion in `sanitize.py` and the check-then-coerce step in
  `TransformSink` (FR-001).
- The deferral helper, `TransformSink` and `FilteringSink` per-event failure under the ledger rule
  (FR-002), and their own drop counts through `losses()` (FR-003).

### Phase 2: Terminal sinks

- `SQSSink`: the id callables and the oversize deferral in `_chunks` and `emit` (FR-004).
- `HTTPSink._items` and `emit`, the unrenderable fold-in in `HTTPSink.losses()` and
  `ElasticsearchSink.losses()` (FR-005).

### Phase 3: Prose

- The docstrings, `docs/invariants.md` and `docs/architecture.md` sites FR-006 lists.

---

## Revision history

- **2026-10-15 — first draft.** Written at `324936e`. Probes A, B and C were re-run against this
  tree while drafting and reproduced the counts stated in the Overview; Probe D was first run
  without `max_value_bytes=500_000` and counted nothing, because assembly clips a 300 KB string at
  its 8 KB ceiling — the event is only oversized for SQS once the ceiling is raised, which is why
  the probe states the setting. The 18-sink sweep and the 60-event corpus timing are carried from
  the evidence behind the decisions rather than re-run here.
- **Considered and not taken:** running `sanitize_fields` over the whole event as the coercion.
  It is rejected on the reasoning in FR-001's description — it pushes `fields` one level deeper and
  clips a long stack to the value ceiling rather than the stack ceiling. No earlier draft or build
  did this; it is recorded so the alternative is not proposed again.
