# Spec: Retry-Safe Drop Counts in Every Sink

**ID:** SPEC-059
**Status:** Completed
**Last Updated:** 2026-10-23
**Depends On:** SPEC-026, SPEC-036, SPEC-038, SPEC-058

## Overview

A batch is the worker's unit of retry, so an `emit` that decides a per-event drop and then raises
is retried whole and decides the same drop again. SPEC-058 settled the rule for that — *a raising
`emit` counts nothing; whoever catches the raise owns the batch's count* — shipped one helper,
`Deferral`, and applied it at five sites. It left every other sink where it was (its Out of Scope;
its delivery doc's *Follow-ups*): nine sinks, and one path in `HTTPSink`, still count a drop the
instant they decide it. So a retried batch counts the same event once per attempt, an event that
was delivered on the retry stays counted as lost, and on the orphan path one event sits in two
loss terms (invariant 2). This spec applies SPEC-058's rule to all of them with the same helper,
and adds a gate check so a new sink cannot bring the defect back. It introduces no new principle:
SPEC-058's Overview carries the argument — the three catchers are the worker's abandon line, the
orphan path's `orphan_lost` and `MultiSink`'s absorbed count — and its deferral rule is quoted
here, its argument not repeated: *a per-event drop decided during an `emit` is counted and
announced only when that `emit` returns normally.*

Every figure below is anchored to `33a0cd3`, the tree this spec was drafted on. The orchestrating
session ran each probe; the drafting session re-ran the Kafka and Syslog UDP probes and the gate
prototype and read the same figures. Nothing is carried from SPEC-058's measurements at `324936e`.
Each probe runs through the real `Worker`, `Worker(sink, batch_size=len(batch), max_retries=3)`, so
a raising `emit` is attempted four times. The batch is handed to the worker directly, so
assembly's size ceilings do not clip it; the sink module's backoff `wait` is replaced by a no-op;
there is one batch per run. *Down* means the destination fails every attempt, *once* that it fails
the first attempt and serves the second.

- **Probe A — an oversize drop followed by a raise is counted once per attempt.** A batch of
  two, one oversized event and one ordinary event, the client failing every send, each sink's own
  `max_retries=0`. `SyslogSink(transport="udp", max_datagram_bytes=600)` and
  `LogstashSink(host=…, transport="udp", max_datagram_bytes=600)`, both through
  `SocketTransport._sendable`, with a 2,000-byte event: down reads `dropped_oversized == 4` for
  one event, four datagram-limit lines, and the abandon line "lost 2 event(s)"; once reads 2 for
  one event. `KinesisSink` (1.1 MB event), `FirehoseSink` (1.1 MB), `SNSSink` (300 KB),
  `MongoDBSink` (17 MB, `insert_many` raising `ConnectionError`) and `AzureEventHubsSink` (a fake
  producer whose batch holds 600 bytes, `send_batch` raising) give identical figures: 4 for one
  event when down, 2 when the destination fails once.
- **Probe B — a refused hand-off is counted once per attempt, and counted though it was
  delivered.** `KafkaSink` whose `produce` raises `BufferError`, and `GooglePubSubSink` whose
  `publish` raises, three events. Down reads `rejected == 12` for three events, twelve refusal lines, and
  the abandon line "lost 3 event(s)". Once — every hand-off refused on attempt 1, all accepted on
  attempt 2 — reads `rejected == 3` although **all three events were delivered**: a counted loss
  that never happened.
- **Probe C — the orphan path puts one event in two terms.** One refused event through
  `log_foundry.info()` outside a span, Kafka and Pub/Sub alike, reads `losses() == SinkLosses(dropped=1, failed=0)`
  **and** `orphan_lost == 1`.
- **Probe D — `HTTPSink._drop_refused`'s conditional path.**
  `HTTPSink(max_batch_count=1, max_retries=0)` with an opener that answers `413` to the 2,000-byte event's lone request (sent first), and for
  the ordinary event's request answers `200` with a `read()` that raises
  `http.client.IncompleteRead`. That is an `http.client.HTTPException`, which is not an `OSError`,
  so `_send`'s `except (URLError, OSError)` does not catch it, `_deliver` does not catch it, and
  `emit` raises. Down reads `dropped_oversized == 4` for one event, four "refused at the smallest
  request" lines, and the abandon line "lost 2 event(s)"; once reads 2.
- **The population is complete.** A sweep at `33a0cd3` of every expression a `losses()` passes as
  `dropped` — `grep -rn 'SinkLosses(dropped=' src/`, the multi-line `SinkLosses(` calls, and
  `wrapped_losses` — finds exactly these counters: `dropped_oversized` (`SocketTransport`,
  `HTTPSink` and its subclasses, Kinesis, Firehose, SNS, MongoDB, Event Hubs, SQS),
  `_unrenderable` (HTTP), `_id_failed` (SQS), `rejected` (Kafka, Pub/Sub) and `_dropped`
  (`TransformSink`, `FilteringSink`). Every other `losses()` reports `dropped=0` or, for
  `MultiSink`, its children's sum. Every write to one of those attributes outside `__init__` at
  that commit is one this spec converts — the nine sinks' and `_drop_refused`'s; the others
  already go through `Deferral`.
- **The gate, prototyped.** Run over the unconverted tree with the ten new sites in
  `DEFERRING_SITES` and `IMMEDIATE` emptied, `deferral_findings` reports, besides the
  `dropped_oversized` and `rejected` stores this spec moves into the `Deferral`, **fifteen stores
  in thirteen methods** that have to be allowed on purpose: `failed` in
  `SocketTransport._send_one`, `AzureEventHubsSink._send`, `FirehoseSink._send`,
  `KinesisSink._send`, `MongoDBSink.emit`, `GooglePubSubSink.emit`, `GooglePubSubSink._resolve`,
  `GooglePubSubSink._drain_pending` and `SNSSink._send`; `dropped_unadjudicated` in
  `FirehoseSink._send` and `KinesisSink._send`, which feeds `failed` and not `dropped`; and state
  that is not a count — `_sock` in `SocketTransport._socket` and `_reset`, `_futures` in
  `GooglePubSubSink._reap` and `_await_overflow`.

## Scope

### In Scope

- An oversize drop counted only when `emit` returns, in the five record-building sinks (FR-001).
- The same for the datagram limit in the socket transport, which `SyslogSink` and `LogstashSink`
  share (FR-002).
- A refused hand-off counted only when `emit` returns, in `KafkaSink` and `GooglePubSubSink`
  (FR-003).
- The same for `HTTPSink._drop_refused` (FR-004).
- The gate: ten more sites, an allowlist re-derived, and a derived check that a `dropped` counter
  is written only by a `Deferral` commit (FR-005).
- Restating every claim the above makes false (FR-006).
- No `emit` raises in a case where it did not raise before, or stops raising in one where it did:
  the deferral moves counts and lines only. Each FR's down probe also asserts the worker's
  `failed_batches == 1`, which pins that.

### Out of Scope

- **"Partial delivery, then a raise" — a sibling defect, not this one.** This spec fixes counts
  that inflate across retries. A raise after something was *delivered* duplicates the delivery,
  which is a different defect (invariant 7: a sink that delivered something reports, it does not
  raise), and mending it turns on a product call this spec does not take. Two instances at
  `33a0cd3`. *(a)* `LoggingSink` with a logger-level `logging.Filter` raising for the fourth of
  seven events: 12 records handled for 3 distinct events, then "lost 7 event(s); batch
  abandoned" (SPEC-058's second diff review measured 16 for 4 with a different filter placement,
  at `ed21cc6`); and with two handlers, the second raising for every record, the first handler
  received the same event four times while the abandon line said "lost 3". SPEC-058's ledger rule
  re-raises when every event failed, which is that second shape, so applying it would not remove
  the duplication; what removes it depends on which handler outcome counts as delivery, and no
  spec has made that call. *(b)* `HTTPSink` with `max_batch_count=1`, the first chunk delivered
  and the second chunk's response `read()` raising `IncompleteRead`: `_send` catches only
  `URLError`/`OSError`, so `emit` raised, and the first event was sent four times while the
  abandon line said "lost 2". FR-004 defers the drop on that path but does not change what
  raises.
- **Recounting `failed`**, including `dropped_unadjudicated` and Elasticsearch's `item_errors`,
  which feed `failed`: `SinkLosses` documents `failed` as an upper bound. The lines written beside
  those counts stay where they are, `AzureEventHubsSink._send`'s under its `_lock` included.
- **A third-party sink that counts before raising.** The principle binds the shipped sinks;
  `MultiSink`'s accepted cost (FR-006 restates it) remains for such a sink.
- **`NullSink.dropped` and `SentrySink.skipped`.** Neither is reported as `losses().dropped` and
  neither `emit` raises after counting.
- **Any public name or `configure()` key** (invariant 10). `send_all`, `_sendable`,
  `_drop_refused`, `_records`, `_bodies`, `_documents` and `_send_batch` are private, and
  `SocketTransport` is not exported.

### Twin paths (invariant 6)

- **Worker path and orphan path.** The orphan path hands `emit` a one-event batch: an oversized
  event alone never raises and is counted once (unchanged), and a refused event alone is the
  all-refused case, so only `orphan_lost` counts it.
- **A sink's `emit` and its `flush`.** Kafka's and Pub/Sub's `flush` count `failed` only, which
  does not change, and no FR adds a drop `flush` could also decide.
- **`MultiSink` with one of these as a child.** SPEC-058 FR-007 now owns the batch a child raised
  on, so the oversized or refused event leaves `dropped` and sits once in the absorbed `failed`.
  Each of FR-001 to FR-004 carries one `MultiSink` criterion using its own child.

---

## Functional Requirements

### FR-001: An oversize drop in the record-building sinks counts only when `emit` returns

#### Description:

`KinesisSink._records`, `FirehoseSink._records`, `SNSSink._bodies`, `MongoDBSink._documents` and
`AzureEventHubsSink._send_batch` decide an oversize drop before any request and count it at once,
so a raise later in the same `emit` leaves it counted once per worker attempt. Each now takes the
calling `emit`'s `Deferral` and counts `dropped_oversized` and announces through it, the shape
SPEC-058 FR-005 gave `HTTPSink._items`. The `Deferral` is the outermost block of the work in
`emit`, so a raise from anywhere beneath it discards the ledger. The announcement keeps today's
shape — one line per oversized event, same text — written only when `emit` returns. A line whose
text carries the event's size (Kinesis's and Firehose's) binds that size when the drop is
decided, not when the line is written, so two oversized events of different sizes in one batch each
name their own.

When each `emit` raises, which is what discards the ledger:

- `KinesisSink` and `FirehoseSink`: `SinkDeliveryError` when at least one chunk was sent, none
  delivered and none unadjudicable. A raise from building a record (`json.dumps`) or a
  `BaseException` from the client also leaves `emit`.
- `SNSSink`: `SinkDeliveryError` when at least one chunk was sent and none delivered; also a
  raise from building a body (`json.dumps`), a malformed `Failed` entry (`entry["Id"]` in
  `_send`), or a `BaseException` from the client.
- `MongoDBSink`: closed on entry, before `_documents`, so nothing has been decided; a connection
  error past the retry bound; a bulk write whose every document was rejected. A bulk write that
  stored some returns normally and so commits.
- `AzureEventHubsSink`: closed on entry, before packing; every attempted send failed; and the
  driver's `create_batch` raising.

`AzureEventHubsSink.emit` holds `_lock` across `_send_batch` and today writes the oversize line
under it, which SPEC-028's two-lock decision exists to avoid. The `Deferral` is therefore entered
outside `_lock` (the ledger first, the lock inside it), so the commit and the line run after the
lock is released. The failed-send count and line `_send` writes stay as they are.

#### Acceptance Criteria:

Each criterion below holds for all five sinks unless it names one, and is one parametrised test.

- [ ] Down (Probe A, through the worker): `dropped_oversized == 0` and
      `losses().dropped == 0`, no oversize line is written, `failed_batches == 1`, and the
      abandon line is the only count of the oversized event.
- [ ] Once: `dropped_oversized == 1`, `losses().dropped == 1`, exactly one oversize line, and
      `failed_batches == 0`.
- [ ] A batch of nothing but oversized events makes no request, returns normally, and counts all
      N (unchanged).
- [ ] A batch of one oversized and one ordinary event, the client raising `KeyboardInterrupt` on
      the send: `emit` raises it with nothing counted and no oversize line written.
- [ ] MongoDB: a bulk write that stored some documents, beside an oversized one, returns, counts
      the oversized drop and counts the rejects.
- [ ] MongoDB and Event Hubs, closed: `emit` raises `SinkDeliveryError` and counts nothing
      (unchanged).
- [ ] Event Hubs: `sink._lock.locked()` is `False` at the moment the oversize line is written.
- [ ] Kinesis and Firehose: two oversized events of different sizes in one batch write two lines,
      each naming its own size.
- [ ] Orphan path: one oversized event through `info()` outside a span, with
      `configure(max_value_bytes=…)` raised above the sink's own limit so assembly does not clip
      it first, is counted once with `orphan_lost == 0` (unchanged).
- [ ] `MultiSink(<the sink with a failing client>, MemorySink())` given one oversized and one
      ordinary event reads `dropped == 0` and `failed == <the child's own failed> + 2`; today the
      oversized event is in `dropped` and in `failed`.
- [ ] Serves invariants 2, 6, 7 and 11.

### FR-002: The socket transport's oversize drop counts only when the calling sink's `emit` returns

#### Description:

`SocketTransport._sendable` discards a UDP datagram over `max_datagram_bytes` and counts it at
once, under the send lock `send_all` holds. When the sendable remainder then fails totally,
`send_all` raises, the worker retries the batch and the same frame is dropped again. Its
docstring records the inflation as an exception and says the fix "belongs one level up". It does,
and it is the sink's `emit`: the boundary the worker retries.

*The decision.* The `Deferral` is entered in `SyslogSink.emit` and in `LogstashSink.emit`'s socket
branch, with the transport as its owner (`Deferral(self._socket)`; the transport already has the
`_counter_lock` the commit takes), and `send_all(messages, pending)` and
`_sendable(messages, pending)` take that ledger as a **required** parameter. Three reasons. The
commit then sits at the boundary the worker actually retries, rather than relying on `send_all`
being the last statement of every caller. The gate's construction rule — a `Deferral` only as a
`with` item in an `emit` — needs no change. And a required parameter means mypy refuses a caller
that forgets the ledger. The cost: the tests that call `send_all` directly
(`grep -rn 'send_all(' tests/`) pass a ledger, edited by hand, and a `pytest --collect-only -q`
diff shows no test removed or renamed. `LogstashSink`'s HTTP branch delegates to `HTTPSink.emit`,
which already defers, so it is untouched.

When `send_all` raises: a closed transport raises before anything is decided; otherwise it raises
`SinkDeliveryError` when the sendable remainder is non-empty and none of it reached the socket.
An all-oversized UDP batch sends nothing and returns. TCP has no datagram limit, so `_sendable`
drops nothing there. The datagram-limit line keeps its shape — one line per call that dropped
anything, carrying the count — and is written outside the transport's `_lock`, because the commit
runs when the sink's `emit` returns, after `send_all` has released it.

#### Acceptance Criteria:

Each criterion holds for `SyslogSink` and for the socket branch of `LogstashSink`, over UDP with
`max_datagram_bytes=600` and a 2,000-byte event, unless it says otherwise.

- [ ] Down (Probe A, through the worker): the transport's `dropped_oversized == 0` and
      `losses().dropped == 0`, no datagram-limit line is written, `failed_batches == 1`, and the
      abandon line is the only count of the oversized event.
- [ ] Once: the transport's `dropped_oversized == 1`, one datagram-limit line, and
      `failed_batches == 0`.
- [ ] An all-oversized UDP batch sends nothing, returns normally and counts all N (unchanged).
- [ ] TCP drops nothing and writes no datagram-limit line, however large the frame (unchanged).
- [ ] A closed transport raises `SinkDeliveryError` with nothing counted (unchanged).
- [ ] `transport._lock.locked()` is `False` at the moment the datagram-limit line is written.
- [ ] `send_all(messages)` and `_sendable(messages)` without a ledger are refused with a
      `TypeError`, and `poetry run mypy` is clean over the callers.
- [ ] Every test that called `send_all` directly passes a ledger, and every name
      `pytest --collect-only -q` lists at `33a0cd3` is still listed, under the same name, after
      the build (it adds names — FR-005 adds some to a file that existed at base — but removes
      or renames none).
- [ ] Orphan path: one oversized frame through `info()` outside a span, with
      `configure(max_value_bytes=…)` raised above `max_datagram_bytes` so assembly does not clip
      it first, is counted once with `orphan_lost == 0` (unchanged).
- [ ] `MultiSink(SyslogSink(<udp, failing>), MemorySink())` given one oversized and one ordinary
      event reads `dropped == 0` and `failed == <the transport's own failed> + 2`.
- [ ] Serves invariants 2, 6, 7 and 11.

### FR-003: A refused hand-off counts only when `emit` returns

#### Description:

`KafkaSink.emit` and `GooglePubSubSink.emit` count `rejected` and announce each refusal the
moment `produce` or `publish` raises. When every event is refused the sink raises, the worker
retries, and each attempt counts the same refusals again — and when attempt 2 accepts them, the
count records a loss that never happened. This is SPEC-058 FR-002's ledger rule arriving at a
sink's own hand-off: when every event is refused the sink raises and counts nothing, and the
catcher owns the batch. Both `emit`s count `rejected` and announce through a `Deferral` whose
`with` encloses the produce/publish loop, `_reap()` (Pub/Sub) and the total-failure raise. One
line per refused event, same text, written only when `emit` returns. Each line names its own
event's exception type (`type(err).__name__`), and that type is bound when the refusal is
decided, not when the line is written, so two refusals of different types in one batch each name
their own.

When each `emit` raises: `KafkaSink` when closed on entry, when every message was refused, and
when anything raises mid-loop (`json.dumps`, `_key`, `poll`). `GooglePubSubSink` when closed on
entry, when every event was refused, and when `_reap`, which may wait for overflow, raises. A
partial refusal returns and commits.

What stays immediate, because it is `failed` and an upper bound: a Kafka delivery callback run by
`poll(0)` counts `failed` as it happens; Pub/Sub's `failed` for a future orphaned by a mid-batch
close, and `_reap`'s `failed`, likewise. Accepted cost: the refusal lines are written after the
loop, and after `_reap()` returns for Pub/Sub, so a line can trail its refusal by up to one
reap wait, which `overflow_timeout` bounds; writing it earlier is counting before the `emit` is
known to return.

#### Acceptance Criteria:

Each criterion holds for both sinks.

- [ ] Down (Probe B, three events, every hand-off refused on every attempt):
      `rejected == 0` and `losses().dropped == 0`, no refusal line is written,
      `failed_batches == 1`, and the abandon line "lost 3 event(s)" is the only count.
- [ ] Once (every hand-off refused on attempt 1, all accepted on attempt 2): `rejected == 0`, all
      three events reach the producer or client on the second attempt, and `failed_batches == 0`.
- [ ] One refusal of three, the others accepted: `emit` returns, `rejected == 1`,
      `losses().dropped == 1`, and one refusal line is written.
- [ ] Two refusals of different exception types beside one accepted event write two lines, each
      naming its own type.
- [ ] A refusal followed by a raise elsewhere in the same `emit` (Kafka's `poll`, Pub/Sub's
      `_reap`) leaves `rejected` unmoved and writes no line.
- [ ] Closed on entry: `emit` raises `SinkDeliveryError` with nothing counted (unchanged).
- [ ] A Kafka delivery callback's `failed`, and Pub/Sub's orphaned-future and `_reap` `failed`,
      are each observed to have moved before `emit` returns.
- [ ] Orphan path (Probe C): one refused event through `info()` outside a span reads
      `orphan_lost == 1` and `losses().dropped == 0`; today both are 1.
- [ ] `MultiSink(<the sink refusing every hand-off>, MemorySink())` given three events reads
      `dropped == 0` and `failed == 3`.
- [ ] Serves invariants 2, 6, 7 and 11.

### FR-004: `HTTPSink._drop_refused` counts through the calling `emit`'s `Deferral`

#### Description:

A lone item the destination answers `413`, and a lone item `_known_refused` discards without
offering it, are dropped by `_drop_refused`, which counts `dropped_oversized` at once. `emit`
already holds a `Deferral` (SPEC-058 FR-005), so `_drop_refused(chunk, pending)` counts and
announces through it, and both routes into it defer. The line keeps its shape: one per call, same
text.

Once a drop has settled a chunk, `chunks and not settled` cannot be true, so `emit` raises after
a `_drop_refused` only through a later chunk's exception that `_deliver` does not catch — an
`http.client.HTTPException` such as the `IncompleteRead` of Probe D — or through a
`BaseException`. That is the path this FR closes; it does not change what raises (Out of Scope).
`dropped` is an exact count because of this deferral and not because the drop is permanent, and
`failed` stays an upper bound.

#### Acceptance Criteria:

- [ ] Probe D, down: `dropped_oversized == 0` and `losses().dropped == 0`, no "refused at
      the smallest request" line, `failed_batches == 1`, and the abandon line is the only count.
- [ ] Probe D, once: the event is counted once, one line is written, and
      `failed_batches == 0`.
- [ ] The never-offered route defers the same way: a batch of two equal-size oversized events —
      the first answered `413`, the second discarded by `_known_refused` — followed by an ordinary
      event whose response read raises counts 0 when down, and counts each of the two events
      once, with two lines, when once.
- [ ] A batch whose every event is answered `413` settles, returns normally and counts all N
      (unchanged).
- [ ] Over every `HTTPSink` subclass, derived transitively from `__subclasses__()`, the down
      probe counts 0 in `losses().dropped`; `ElasticsearchSink` and `OpenSearchSink`, which fold
      `dropped_oversized` into their own `losses()`, are among them.
- [ ] Orphan path: a single event the opener answers `413`, through `info()` outside a span,
      settles inside the sink, is counted once, and reads `orphan_lost == 0` (unchanged). The
      opener answers by request and not by size, so `max_value_bytes` need not be raised.
- [ ] `MultiSink(HTTPSink(<Probe D>), MemorySink())` reads `dropped == 0` and
      `failed == <the HTTPSink's own failed> + 2`.
- [ ] Serves invariants 2, 6 and 7.

### FR-005: The gate covers every site, and a `dropped` counter is written only by a `Deferral` commit

#### Description:

`tests/test_deferral.py` holds the SPEC-058 sites to the one helper. This FR widens it in three
parts.

*1. The roster.* `DEFERRING_SITES` gains `sinks/_socket.py` → `SocketTransport: (send_all,
_sendable)`; `sinks/syslog.py` → `SyslogSink: (emit,)`; `sinks/logstash.py` → `LogstashSink:
(emit,)`; `sinks/kinesis.py` → `KinesisSink: (emit, _records)`; `sinks/firehose.py` →
`FirehoseSink: (emit, _records)`; `sinks/sns.py` → `SNSSink: (emit, _bodies)`;
`sinks/mongodb.py` → `MongoDBSink: (emit, _documents)`; `sinks/eventhubs.py` →
`AzureEventHubsSink: (emit, _send_batch)`; `sinks/kafka.py` → `KafkaSink: (emit,)`;
`sinks/pubsub.py` → `GooglePubSubSink: (emit,)`. The check already requires a named module to
import `Deferral` from the helper's module, so `_socket.py` imports it for the `pending`
annotation (an import under `TYPE_CHECKING` satisfies it). `SocketTransport` has no `emit`, so
its entry is the one the derived roster rule cannot require.

*2. The allowlist.* `IMMEDIATE` loses `("HTTPSink", "_drop_refused")` and gains the thirteen
methods the Overview lists — fifteen stores — each with a one-line reason in the docstring:
`failed` counts that are an upper bound by design (Out of Scope), `dropped_unadjudicated`, which
feeds `failed`, and state that is not a count. The list is what the gate found over the
unconverted tree; the build re-derives it by running the gate over the converted tree with the
allowlist emptied, not by copying it, and `stale_immediate` holds it exact. No entry may name an
attribute some `losses()` reports as `dropped`; the tree test asserts that against the derived set
below. The corpus runner changes too: it read the shipped `IMMEDIATE` for two cases
(`MultiSink.emit`, and `HTTPSink._drop_refused` in "an allowed store is silent in the method the
allowlist names"), so removing the second entry would redden a case that tests the allowlist
mechanism and not the entry. It now passes the corpus its own fixed allowlist holding those two
entries; every case's source and expected findings are unchanged.

*3. A derived check.* Parts 1 and 2 hold a *site* to the helper, so a counter written in a method
no roster entry reaches, or in a sink nobody listed, passes both. The new check holds the
*counter*: after `__init__`, an attribute that some `losses()` reports as `dropped` is written
only by a `Deferral` commit.

- *Derivation, from the tree and never by hand.* Every `self.<attr>` read in the `dropped`
  argument (keyword, or first positional) of every `SinkLosses(...)` call — the callee spelt bare
  or as an attribute — and in the `own` argument (keyword, or second positional) of every
  `wrapped_losses(...)` call, in any function under `src/`. The calls are not required to sit in a
  method named `losses`: a sink that builds its report in a helper would otherwise contribute
  nothing, and a wider derivation fails closed — a false positive is a finding someone reads, a
  false negative is a counter nobody checks. The derivation includes a local name, in the same
  function, whose assigned value reads `self.<attr>` — every `self.<attr>` read anywhere in that
  value counts — which is how `TransformSink` and `FilteringSink` read `self._dropped`, and how
  `dropped = self.dropped_oversized + self._unrenderable` followed by a `SinkLosses` call passing
  `dropped=dropped` contributes both names. The step is **one hop**: a local's own assigned value,
  never the locals that value reads in turn. `MultiSink.losses` passes
  `sum(child.dropped for child in reported)`, and `reported` is built from `self._sinks`; a
  transitive step would put `_sinks` in the derived set, and every attribute a comprehension over
  children touches with it, which widens the set without guarding a count.
- *The finding.* Every store to one of those attributes — the forms `_self_stores` already
  recognises: assignment, augmented assignment, annotated assignment, `for`, `with` and
  comprehension targets, unpacking, `setattr(self, …)` — anywhere in a class's code except
  `__init__`'s own statements (a closure or lambda defined in `__init__` runs after construction
  and is scanned, as is a method under a class-body `if` or `try`), of any class under `src/`, is
  a finding naming the module, line, class, method and attribute. It
  matches by attribute name across the tree rather than within the reporting class, so it needs no
  inheritance resolution (`ElasticsearchSink.losses` reads attributes `HTTPSink` writes); the
  cost is that an unrelated class storing a same-named attribute outside `__init__` is a finding
  too, and renames it.
- *Dotted paths, and a computed name.* On the reading side a chain `self.a.b` derives its **last**
  name, `b` — the attribute that holds the count; the objects on the way to it are not counts, and
  deriving them would make a legitimate rebinding of one (a lazily opened client, say) a finding.
  On the storing side a derived name matches at **any** position in the stored path, so
  `self._socket.dropped_oversized += 1` is a finding even though its first name is `_socket`. A
  `setattr(self, <name>, …)` whose name is not a string literal is a finding too, since nothing
  shows it is not a counter. Both rules fail closed; neither fires on the tree at `33a0cd3`.
- *No allowlist.* A `dropped` count is exact by promise (invariant 2), so a store that has to be
  immediate is not excused here; breaking that promise should mean editing the gate, deliberately.
- *The floor.* The tree test asserts the derived set contains `dropped_oversized`,
  `_unrenderable`, `_id_failed`, `rejected` and `_dropped`. A floor and not an equality, so a new
  counter needs no gate edit, and a refactor that blinds the derivation reddens instead of
  shrinking the guard silently.
- *What it does not see*, stated in the function's docstring as `deferral_findings` states its
  own: it reads syntax, so a store through an alias of `self`, a mutating method call, and a
  counter reported through a call or a property rather than a plain `self.<attr>` read are not
  findings. `Deferral.__exit__` writes through `owner`, not `self`, which is why the one permitted
  writer is invisible to a check that refuses every `self` store.

#### Acceptance Criteria:

- [ ] `test_every_deferring_site_takes_the_one_helper` is green over the converted tree with the
      ten new roster entries, and `stale_immediate` reports nothing.
- [ ] The tree test asserts no `IMMEDIATE` entry names an attribute in the derived set, and that
      the derived set contains the five floor names; making the derivation skip `wrapped_losses`
      reddens it naming `_dropped`.
- [ ] The new check is green over the converted tree: no store to a derived counter outside
      `__init__` in any class under `src/`.
- [ ] A store to `self.rejected` added to a `KafkaSink` method the roster does not reach reddens
      the tree test with a finding naming `kafka.py`, its line, `KafkaSink`, that method and
      `rejected`.
- [ ] The corpus asserts the finding text for each derivation form: `dropped=` keyword; first
      positional; callee spelt as an attribute; a sum of two attributes; a local name assigned
      from `self.<attr>`; a local assigned from a sum of two attributes; `wrapped_losses` with
      `own=` and with the second positional; and a counter reported by one class and stored by
      another.
- [ ] The corpus asserts the finding text for each store form `_self_stores` recognises, for a
      derived name stored at a non-first position of a dotted path, and for a `setattr(self, …)`
      whose name is computed; and asserts that a chained read `self.a.b` derives `b` and not `a`.
- [ ] The corpus asserts silence for: a store in `__init__`; a store to `failed`, which no
      `losses()` reports as `dropped`; a count through `pending.count("rejected")`; a class whose
      `losses()` reports `dropped=0`; and a `dropped` argument that reads no `self` attribute
      (a sum over children).
- [ ] Each check of the new function, deleted or inverted in turn, reddens its matching corpus
      case — the derivation forms, the callee spellings, the local-name step, the store scan, the
      `__init__` exemption and the cross-class match — and the PR records which case reddened
      for which check.
- [ ] The existing corpus cases keep their sources and expected findings, are green, and a
      `pytest --collect-only -q` diff shows no test removed or renamed.
- [ ] Serves invariant 2.

### FR-006: Every claim this spec makes false is restated

#### Description:

The sites, by path, each found by a sweep at `33a0cd3`:

- **`src/log_foundry/sinks/_socket.py`:** the class `Attributes:` entry for `dropped_oversized`;
  `send_all` (the new parameter, and what a raise leaves counted); and `_sendable` — its paragraph
  recording the inflation, "Nothing here can fix it … the fix belongs one level up if it is ever
  taken", and "Everywhere else ``dropped`` is an exact count, so the exception matters".
- **`src/log_foundry/sinks/http.py`:** `emit`'s "The ``413`` and abandonment counters below keep
  counting as they happen, which is outside SPEC-058's scope", and `_drop_refused`'s "which is why
  ``dropped`` can stay an exact count while ``failed`` is only an upper bound" — now true because
  of the deferral, and says so.
- **Each converted sink's docstrings that say when the drop is counted:** `kinesis.py` and
  `firehose.py` `_records`; `sns.py` `_bodies`; `mongodb.py` `_documents` and `emit`;
  `eventhubs.py` `_send_batch` and `emit`; `kafka.py` `emit` ("A partial refusal is counted and
  left alone") and `losses`; `pubsub.py` `emit`; `syslog.py` and `logstash.py` `emit`. Each
  carries the new `pending` argument where it takes one.
- **`src/log_foundry/sinks/multi.py`, `emit`:** "a sink not yet deferring is counted once in its
  own ``dropped`` and once here". No shipped sink is any longer; it is restated as a cost that
  remains only for a third-party sink that counts a drop before raising.
- **`src/log_foundry/sinks/_deferral.py`:** `_Counted`'s "a deferred ``dropped_oversized``
  increment and an immediate one from ``_drop_refused``" — no immediate `dropped` increment
  remains — and `count`'s "one helper serves five owner types", restated without a count.
- **`tests/test_deferral.py`:** the `DEFERRING_SITES` and `IMMEDIATE` docstrings, and the tree
  test's "five sites".
- **`docs/invariants.md` §2:** the paragraph beginning "At the five sites SPEC-058 defers" is
  restated so that a `dropped` count is made only by an `emit` that returned, at every shipped
  site, guarded by FR-005's check; the exception for a sink's own `failed` stays. §2's `Guarded:`
  line gains `tests/test_deferral.py`.
- **`docs/architecture.md`:** the passage in §8 ("SPEC-058 applies it at five sites … and the nine
  sinks a follow-up spec owns"), restated the same way.
- **`README.md`, which is also the PyPI page:** the `sink.dropped` row of the health-fields table
  ("an oversized record, or one the client refused outright") and the paragraph below it saying
  `KafkaSink` and `GooglePubSubSink` "also count what their client refused outright". After FR-003
  a refusal is counted in `sink.dropped` only when the rest of its batch was handed off; a batch
  refused whole raises, so the worker's retry engages and `failed_batches` records it
  (`orphan_lost` outside a span), or, under a `MultiSink` whose sibling delivered, the fan-out
  absorbs the raise and counts the batch in its own `sink.failed`. Both are restated, and the
  paragraph says once that a drop is counted only when the sink's `emit` returns, so a retried
  batch counts it once.

Listed so each claim has an owner, and restated by the completion ritual rather than by this FR:
the fence and the entry "A raising `emit` counts nothing…" in `docs/decisions/sink-delivery.md`
(its "which nine sinks and `HTTPSink._drop_refused` still do", its *One helper, written once*
paragraph naming five modules and `_drop_refused` in the allowlist, the "not-yet-deferred sink"
cost in the `MultiSink` entry beside it, and its Do NOT paragraph's last sentence); the
`docs/component-inventory.md` deferral-helper row ("Used by `transform`, `filtering`, `multi`,
`sqs` and `http`"); and, in `docs/spec-delivery/SPEC-058-one-bad-event-costs-only-itself.md`, the
*Follow-ups* bullet and the two *What changed for a caller* bullets that say the nine sinks still
count as they happen — each gets a done-by marker naming SPEC-059, the old text left readable.

#### Acceptance Criteria:

- [ ] `grep -rn 'Nothing here can fix' src/` and `grep -rn 'one level up'
      src/log_foundry/sinks/` find nothing.
- [ ] `grep -n 'refused outright' README.md` finds nothing, and the README's `sink.dropped` row
      says a batch refused whole is counted by the worker, not in `sink.dropped`.
- [ ] `grep -n 'nine sinks' docs/invariants.md docs/architecture.md` finds nothing, and
      `grep -rn 'not yet deferring' src/` finds nothing.
- [ ] Every docstring the Description lists says the drop is counted and announced only when
      `emit` returns, and none says it is counted as it happens.
- [ ] `docs/invariants.md` §2 states that a `dropped` count is made only by a returned `emit` at
      every shipped site, keeps the `failed` exception, and names `tests/test_deferral.py` in its
      `Guarded:` line.
- [ ] `sh scripts/docs-lint.sh`, `poetry run python scripts/docstring-lint.py` and
      `sh scripts/spec-lint.sh` pass, and every test that asserts a docstring
      (`grep -rn '__doc__' tests/`) still passes.
- [ ] Serves invariant 2, which the invariants page's text and the docstrings must state alike.

---

## API / Interface Contract

Nothing public changes. `losses()` keeps its shape everywhere. The private signatures that change:

```
# private, src/log_foundry/sinks/_socket.py — the ledger is required, never defaulted
SocketTransport.send_all(messages: list[bytes], pending: Deferral) -> None
SocketTransport._sendable(messages: list[bytes], pending: Deferral) -> list[bytes]

# private, one ledger parameter each, always the calling emit's
HTTPSink._drop_refused(chunk: list[_Item], pending: Deferral) -> None
KinesisSink._records(batch: list[dict[str, object]], pending: Deferral) -> list[dict[str, Any]]
FirehoseSink._records(batch: list[dict[str, object]], pending: Deferral) -> list[dict[str, Any]]
SNSSink._bodies(batch: list[dict[str, object]], pending: Deferral) -> list[str]
MongoDBSink._documents(batch: list[dict[str, object]], pending: Deferral)
    -> list[dict[str, object]]
AzureEventHubsSink._send_batch(batch: list[dict[str, object]], pending: Deferral) -> None

# where each emit enters the ledger
SyslogSink.emit / LogstashSink.emit (socket branch):
    with Deferral(self._socket) as pending:
        self._socket.send_all(frames, pending)
AzureEventHubsSink.emit:
    with Deferral(self) as pending, self._lock:      # the ledger outside the lock
        ...
KafkaSink.emit / GooglePubSubSink.emit:
    with Deferral(self) as pending:                  # loop, _reap() and the total-failure raise
        ...
```

In `tests/test_deferral.py` the new check, its floor and its corpus sit beside
`deferral_findings`; their names are the implementation's and the criteria do not bind them.

## File & Folder Structure

```
src/log_foundry/sinks/
├── _socket.py          # FR-002 (send_all, _sendable take the ledger), FR-006
├── syslog.py           # FR-002, FR-006
├── logstash.py         # FR-002, FR-006
├── kinesis.py  firehose.py  sns.py  mongodb.py  eventhubs.py    # FR-001, FR-006
├── kafka.py  pubsub.py                                           # FR-003, FR-006
├── http.py             # FR-004, FR-006
├── multi.py            # FR-006 (emit docstring)
└── _deferral.py        # FR-006 (docstrings)
tests/
├── test_deferral.py    # FR-005: roster, IMMEDIATE, the derived check and its corpus; FR-006
└── <one group per FR beside each sink's existing test file; direct send_all callers (FR-002)>
docs/
├── invariants.md       # FR-006: §2
└── architecture.md     # FR-006: §8
```

## Implementation Phases

### Phase 1: Oversize drops

- The five record-building sinks take the `Deferral` (FR-001).
- The socket transport, `SyslogSink` and `LogstashSink` take the ledger as a required parameter,
  and the direct `send_all` tests are edited (FR-002).
- Each site lands with its `DEFERRING_SITES` entry and the `IMMEDIATE` entries for the stores the
  gate then finds in it (FR-005 parts 1 and 2), because converting a sink without its roster entry
  already reddens `test_every_deferring_site_takes_the_one_helper` ("enters a Deferral but is not
  listed in DEFERRING_SITES").

### Phase 2: Refusals and `413`

- `KafkaSink.emit` and `GooglePubSubSink.emit` (FR-003).
- `HTTPSink._drop_refused` (FR-004).
- Each site again lands with its roster and allowlist entries, including the removal of
  `("HTTPSink", "_drop_refused")` from `IMMEDIATE`, which converting `_drop_refused` makes stale,
  and the corpus runner's own fixed allowlist, which has to land with that removal (FR-005
  part 2).

### Phase 3: The derived check

- The derived check with its floor, its corpus and the tree-test assertions (FR-005 part 3). The
  existing gate stays green after every phase because each site lands with its entries; the derived
  check comes last because it is only green once every site is converted.

### Phase 4: Prose

- The docstrings, `docs/invariants.md` §2 and `docs/architecture.md` §8 that FR-006 lists.

One PR.

---

## Revision history

- **2026-10-23 — first draft.** Written at `33a0cd3`. The orchestrating session ran every probe in
  the Overview and the population sweep at that commit; nothing is carried from SPEC-058's
  `324936e` measurements. The drafting session re-ran the Kafka (down, once) and Syslog UDP (down,
  once) probes, the gate prototype that produced the fifteen stores in thirteen methods, and the
  check that `IncompleteRead` is an `HTTPException` and not an `OSError`, and read the same
  figures; the remaining probes were not re-run by it.
- **2026-10-23 — spec review at `23788c2`: one major, five minors, all fixed.** The reviewer
  reproduced probes A–D and the population claim independently at `23788c2` (whose `src/` equals
  `33a0cd3`), with its own AST sweep.
  - The refusal line's exception type (FR-003): the major. The type is bound when the line is
    registered, not when it is written; the reviewer reproduced the defect with a loop-local
    closure that wrote both lines naming the last type.
  - The `KeyboardInterrupt` criterion (FR-001) required no oversized event, so it passed against
    today's code; it now sends one oversized and one ordinary event.
  - The derivation (FR-005) is widened: no `losses`-only restriction, and the local-name step reads
    the whole assigned value, one hop deep (a transitive step would derive `MultiSink._sinks`).
  - The phase order: each site lands with its `DEFERRING_SITES` and `IMMEDIATE` entries, since the
    shipped gate reddens otherwise.
  - SNS's raise reading (FR-001) is as complete as Kinesis's.
  - The orphan-path criteria (FR-001, FR-002) run with `max_value_bytes` raised above the sink's
    own limit; FR-004's does not need it.
- **2026-10-23 — three amendments found by the implementation plan at `ae105bb`.** The plan
  prototyped every FR and ran the whole suite. (1) "Bound when the line is registered" is
  restated as "bound when the drop is decided": the idiom the old wording suggests, a
  default-argument lambda, fails `mypy --strict` ("Cannot infer type of lambda") at all four sites,
  and `SQSSink`'s collect-then-announce shape satisfies the criteria. (2) FR-005 states how a
  dotted path is read and stored — the last name of a read chain is derived, a derived name matches
  at any position of a stored path, and a computed `setattr` name is a finding — where the draft
  was silent and first-name matching would miss `self._socket.dropped_oversized += 1`. (3) FR-006
  gains `README.md`'s `sink.dropped` row and the Kafka/Pub/Sub paragraph beneath it, which FR-003
  makes false for a batch refused whole and which the PyPI page carries.
- **2026-10-23 — plan review at `8b6a147`: one major, five minors, answered in the plan.** One
  spec change: FR-002's collect-only criterion became "every name listed at `33a0cd3` is still
  listed, under the same name", since the build adds names — some in files that existed at base
  (FR-005's own tests in `test_deferral.py`) — and "equal" could not hold over any set of files.
- **2026-10-23 — diff review frame 1 at `7b0a042`: three minors, all fixed** — the derived check
  now scans closures in `__init__` and methods under class-body blocks; a stale ordinal in the
  gate's docstring; the README, `KafkaSink.losses` and FR-006 name `MultiSink` as the third
  catcher of a batch refused whole.
- **Considered and not taken:** putting the `Deferral` inside `SocketTransport.send_all`. It needs
  the gate's construction rule widened to a second entry name, and it makes the commit depend on
  `send_all` being the last statement of its callers. And an allowlist for the derived check: a
  `dropped` count is exact by promise, so a store that has to be immediate belongs to a counter
  that is not reported as `dropped`.
