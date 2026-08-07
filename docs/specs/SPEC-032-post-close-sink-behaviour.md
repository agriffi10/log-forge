# Spec: Post-Close Sink Behaviour

**ID:** SPEC-032  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-026, SPEC-028, SPEC-030

## Overview

A sink that has been closed still accepts work. `KafkaSink` takes a produce into a producer nothing
will flush again, `GooglePubSubSink` appends a future nothing will ever resolve, and the Redis sinks
reconnect a client they had just disconnected. All three return normally, so the worker believes
them: the retry never engages, `health().failed_batches` stays at zero, `health().sink` reads
`(0, 0)`, and `flush()` reports `True` while the events are gone.

This is the failure shape SPEC-026 was written to end, reached one call later — not at a destination
that is down, but at a transport the library itself has already released. Two specs have now declined
it as out of their scope and handed it on. SPEC-028 found it while locking the sinks that hold
transport state and could not fix it there, because neither sink takes a lock and its enforced roster
is derived from the ones that do. SPEC-030 established that its own signals cannot see it: `retired`
and `submitted_after_shutdown` describe the *worker*, and a level call made with no active span emits
straight into the sink on the caller's thread, never passing through `submit` at all. Measured on
`main`, one `log_foundry.info()` after `shutdown()` into a `KafkaSink` loses the event with
`submitted_after_shutdown` reading `0`.

The rule this spec adds is small and already shipped twice: a closed sink refuses a batch in the
library's own vocabulary instead of reaching a driver it has no right to touch. What has been missing
is not the rule but a way to tell which sinks owe it, which is why the fix arrives with the roster
that finds them.

## Scope

### In Scope

- The post-close guard in `KafkaSink`, `GooglePubSubSink` and `_RedisSink`.
- Stating the post-close obligation in `sinks/base.py`, where the `Sink` protocol already states the
  concurrency one.
- Widening the shared lint scope gate in `tests/test_sink_concurrency.py` from "classes a heuristic
  believes hold a driver" to "every sink class that defines `emit`", closing the limit SPEC-028
  recorded, and deriving both the concurrency roster and the post-close roster from it.
- Recording, per class, the sinks whose `close()` releases nothing and which therefore keep
  accepting — a decision made visible rather than assumed.

### Out of Scope

- **Changing what the library *signals* about use after `shutdown()`.** `Health.retired` and
  `submitted_after_shutdown` are SPEC-030's and are not extended here. This spec makes the sink
  refuse and count; how the process-level mistake is reported is settled.
- **A new `Health` field or a new counter type.** A refused batch raises, so it already reaches
  `failed_batches`, the worker's retry and `flush()`'s verdict through the paths SPEC-026 built.
- **Reopening a closed sink**, or any "restart" affordance. A closed sink stays closed, on SPEC-019's
  reasoning that a component which resurrects itself fights a process trying to exit.
- **Making stateless sinks refuse.** `HTTPSink` and its platform subclasses open a fresh connection
  per request and the boto3 sinks hold no closable state, so a post-close emit there still delivers.
  Refusing it would turn a working call into a lost event, which is the defect inverted.
- **The `SPEC-031` residue.** SPEC-028's delivery doc provisionally assigned the lint-scope widening
  there; it moves here because the post-close roster is derived from that gate and would inherit the
  gap. Nothing else in SPEC-031 changes.
- **Process-level concurrency**, unchanged from SPEC-028: two processes against one file or one
  SQLite database remain out of scope.

---

## Functional Requirements

### FR-001: A closed sink refuses a batch instead of reaching its driver

#### Description:

`KafkaSink`, `GooglePubSubSink` and `_RedisSink` (the base of `RedisStreamsSink` and
`RedisListSink`) gain the `_closed` flag `SQLiteSink` and `MongoDBSink` already carry, checked in
`emit` and set in `close`.

Each loses differently, and the difference is worth stating because it is what makes a single rule
the right answer:

- **`KafkaSink`** — `close()` calls `producer.flush()`, which is the only thing that drains the
  client's local batch and services its delivery callbacks. A later `produce()` is accepted into that
  batch and nothing flushes it again, so the message dies with the process: `emit` returns normally,
  `failed` never moves and no callback ever fires. Measured with a `confluent-kafka`-shaped double:
  one event delivered, one queued and lost, `losses()` `(0, 0)`.
- **`GooglePubSubSink`** — `close()` swaps the pending-futures list out and resolves what it took. A
  later `publish()` appends to the fresh list, and nothing will ever call `result()` on it. That is
  the same unresolved-future loss SPEC-028 fixed *inside* `close`, reachable again from outside it.
  Measured: two futures created, one resolved, one pending, `losses()` `(0, 0)`.
- **`_RedisSink`** — `close()` disconnects a client the sink owns, but `redis-py`'s pool reconnects
  transparently on the next command, so a post-close emit **succeeds** by opening a connection
  nothing will ever reap. Not loss but a leak, and the same one SPEC-028's review found in
  `RabbitMQSink`, where `_active_channel` reopened whatever `close()` had released. Measured: one
  reconnect after close.

The guard is placed so an empty batch is still a no-op and the driver is never touched, matching the
two sinks that already do this. Where the sink has a transport lock the check goes inside it, as
`SQLiteSink`'s does; where it does not, it is a plain read of the flag, as `MongoDBSink`'s is.

#### Acceptance Criteria:

- [ ] `KafkaSink.emit`, `GooglePubSubSink.emit` and `_RedisSink.emit` raise `SinkDeliveryError` on a
      non-empty batch after `close()`, with a message naming the sink and the count in the form the
      shipped sinks already use.
- [ ] None of the three touches its driver after `close()` — no `produce`, no `publish`, no
      `pipeline`. The assertion is on driver contact, not only on the exception, because a double
      that breaks after close would let a deleted guard still pass.
- [ ] `emit([])` after `close()` returns without raising, for all three.
- [ ] `close()` is idempotent on all three, and a second call reaches the driver no further than the
      first.
- [ ] The Redis guard fires only where the sink owns the client. A borrowed client the sink never
      closed is still refused after `close()` — the sink is closed either way, and a caller's client
      surviving is not permission to keep writing through a released sink.
- [ ] `losses()` on a refused batch is unchanged: refusing is not a loss the sink absorbed, it is a
      failure it reported, so the batch reaches `failed_batches` through the worker rather than
      `losses()`. Stated in each `emit` docstring.
- [ ] Each of the three appears in the parametrized post-close test in
      `tests/test_sink_concurrency.py`, with a driver double that keeps *succeeding* after close, so
      an emit slipping past the guard lands rather than failing for an unrelated reason.
- [ ] Each test kills its mutant: with the guard deleted, the driver-contact assertion fails.

### FR-002: The `Sink` protocol states what `emit` after `close` must do

#### Description:

`sinks/base.py` documents `close()` as callable during an in-flight `emit` (SPEC-028) and `emit` as
obliged to raise on total failure (SPEC-026), but says nothing about the ordering the other way
round. A third-party sink author reading the contract today has no way to know the rule exists, and
the three shipped violations are evidence the rule is not obvious from the shape of the code.

The obligation is conditional, and stating the condition is the whole point: a sink whose `close()`
released or invalidated something must refuse a later `emit`; a sink whose `close()` released nothing
must keep accepting. Both halves are the same principle — never silently accept work you cannot
deliver — and the second half is why "always refuse after close" would be wrong.

#### Acceptance Criteria:

- [ ] `Sink.emit`'s docstring states that a sink whose `close()` released or invalidated transport
      state must raise on a non-empty batch afterwards rather than absorbing it, and why: an absorbed
      batch is one the worker believes, so retry, `failed_batches` and `flush()`'s verdict are all
      inert (the SPEC-026 FR-001 reasoning, applied to the sink's own lifecycle rather than the
      destination's).
- [ ] It states the converse explicitly: a sink holding nothing to release keeps accepting, because
      refusing a deliverable batch is loss the library invented.
- [ ] `Sink.close`'s docstring notes that the flag a `close()` sets is what a later `emit` reads, so
      the two are one decision rather than two.
- [ ] `emit([])` remains a no-op after `close()` in the contract as well as the code — an empty batch
      has not failed to deliver.
- [ ] `docs/architecture.md` §8 records the rule in one sentence alongside the existing sink
      obligations.

### FR-003: Both sink rosters are derived from every sink class, not from a heuristic

#### Description:

`_sink_classes_holding_a_driver` in `tests/test_sink_concurrency.py` admits a module by one of two
guesses — that it imports something inside a function, or that its source contains one of six
hardcoded handle tokens. SPEC-028 recorded the consequence: a lock added to `HTTPSink.emit` would be
invisible to the lint, and so would any post-close roster derived from it. That is not hypothetical
for this spec — `KafkaSink` and `GooglePubSubSink` are both in scope by the lazy-import guess today,
but the guess is what decides it, and the roster whose completeness is being relied on must not rest
on a guess.

The gate widens to every class in `sinks/` that defines `emit` or `send_all`, which is the honest
fix SPEC-028 named. The `Sink` protocol in `base.py` is excluded by name: it is the contract, not an
implementation.

Widening the gate brings 16 further classes into the *concurrency* lint, all of them wrappers,
`urllib` subclasses or in-memory sinks that genuinely hold no transport lock. Each records that in
its class docstring using the exemption phrase the lint already recognises. This is deliberate
churn: SPEC-028's lint exists to make the decision mandatory and visible, and a class silently
outside its scope has made no decision at all.

The post-close roster gets the same treatment and its own exemption phrase, so a sink opts out by
asserting its `close()` releases nothing rather than by being unreachable by a heuristic.

#### Acceptance Criteria:

- [ ] The scope gate returns every class in `sinks/*.py` defining `emit` or `send_all`, excluding
      `base.Sink`, and no longer inspects imports or matches source tokens.
- [ ] `test_every_driver_backed_sink_records_a_concurrency_decision` passes over the widened set,
      with each newly in-scope class either locking in `emit` or carrying the existing
      `**no** transport lock` / `SPEC-028 FR-002` claim.
- [ ] A new lint asserts every in-scope class either refuses a post-close emit or carries an
      explicit claim that its `close()` releases nothing. The claim is a fixed phrase checked as a
      substring, as SPEC-028's is, so a docstring merely *mentioning* close cannot satisfy it.
- [ ] "Refuses a post-close emit" is established behaviourally, by the parametrized test, not by
      reading the source for a `_closed` token — a flag checked in the wrong place would otherwise
      satisfy the lint.
- [ ] `CLOSED_SINKS` and `_BUILDER_CLASSES` stop being a hand-written roster whose *coverage* is
      checked after the fact: the parametrization is derived from the same scan, and a sink with no
      double fails the run rather than being skipped.
- [ ] Adding a new sink class with a releasing `close()` and no guard fails the suite. Demonstrated
      by a temporary class in the test run, not asserted in prose.
- [ ] The two limits SPEC-028 recorded are re-stated as they now stand: the lock check still proves
      only that a lock is entered on the path, and the scope gate no longer guesses.

### FR-004: The sinks that keep accepting are recorded, not assumed

#### Description:

Per SPEC-021's rule, a decision not to change something is closed by being recorded. Twenty-odd
sinks keep accepting after `close()` and that is correct in every case, but "correct" is currently
indistinguishable from "never considered" — which is exactly how the three defects survived four
specs that touched every sink in the package.

Three groups, three different reasons, and each says its own:

- `HTTPSink` and its platform subclasses (Elasticsearch, Loki, Datadog, Splunk, New Relic,
  Honeycomb, Sentry) — `urllib` opens a fresh connection per request, so `close()` has nothing to
  release and a later `emit` delivers normally.
- The boto3 sinks (`SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`) — the client is the caller's
  or the SDK's, and `close()` is a documented no-op.
- The wrappers (`MultiSink`, `FilteringSink`, `TransformSink`, `CallbackSink`) and the local sinks
  (`StdoutSink`, `LoggingSink`, `NullSink`, `MemorySink`) — a wrapper forwards `close()` and its
  child enforces the rule; `SyslogSink` and `LogstashSink` forward to `SocketTransport` and
  `HTTPSink`, which already do. A wrapper adding its own guard would refuse batches its child would
  have accepted.

#### Acceptance Criteria:

- [ ] Each in-scope class that keeps accepting carries the FR-003 exemption claim in its class
      docstring, naming which of the three reasons applies to it.
- [ ] The wrapper sinks' docstrings state that the rule is the child's to enforce, so a reader does
      not add a guard that would break delegation.
- [ ] `SyslogSink` and `LogstashSink` state that their post-close refusal comes from the transport
      they hold, and a test shows a closed `SyslogSink` refusing through `SocketTransport` rather
      than through a guard of its own.
- [ ] `docs/architecture.md` §13 Known Constraints records that a borrowed client outlives the sink
      that used it: closing a sink built on an injected client does not close the client, and the
      sink refuses regardless.
- [ ] No behaviour changes under this FR.

---

## Data Model

No new types, no signature changes, no new `Health` field.

```python
# kafka.py, pubsub.py, redis.py — the flag SQLiteSink and MongoDBSink already carry:
class KafkaSink:
    def __init__(self, ...) -> None:
        ...
        self._closed = False
        self._close_lock = threading.Lock()   # separate from _counter_lock; see SPEC-028
```

## API / Interface Contract

```python
# The rule, identical in all three, in the vocabulary the shipped sinks use:
def emit(self, batch: list[dict[str, object]]) -> None:
    if not batch:
        return
    if self._closed:
        raise SinkDeliveryError(
            f"KafkaSink produced none of {len(batch)} message(s): the sink is closed"
        )
    ...

# tests/test_sink_concurrency.py — the gate stops guessing:
def _sink_classes_with_an_emit() -> list[tuple[str, ast.ClassDef]]:
    """Every class in sinks/ defining emit or send_all, except the base.Sink protocol."""
```

## Configuration / Environment

None.

## File & Folder Structure

```
src/log_foundry/
├── sinks/
│   ├── base.py          # modified — the post-close obligation on the protocol (FR-002)
│   ├── kafka.py         # modified — the guard (FR-001)
│   ├── pubsub.py        # modified — the guard (FR-001)
│   ├── redis.py         # modified — the guard on _RedisSink (FR-001)
│   ├── http.py          # modified — exemption claim (FR-003, FR-004)
│   ├── datadog.py       # modified — exemption claim
│   ├── elasticsearch.py # modified — exemption claim
│   ├── honeycomb.py     # modified — exemption claim
│   ├── loki.py          # modified — exemption claim
│   ├── splunk.py        # modified — exemption claim
│   ├── sentry.py        # modified — exemption claim
│   ├── sqs.py           # modified — exemption claim
│   ├── sns.py           # modified — exemption claim
│   ├── kinesis.py       # modified — exemption claim
│   ├── firehose.py      # modified — exemption claim
│   ├── multi.py         # modified — exemption claim (delegation)
│   ├── filtering.py     # modified — exemption claim (delegation)
│   ├── transform.py     # modified — exemption claim (delegation)
│   ├── callback.py      # modified — exemption claim (delegation)
│   ├── syslog.py        # modified — exemption claim (transport enforces)
│   ├── logstash.py      # modified — exemption claim (transport enforces)
│   ├── logging_sink.py  # modified — exemption claim
│   ├── stdout.py        # modified — exemption claim
│   └── util.py          # modified — exemption claims (NullSink, MemorySink)

tests/
└── test_sink_concurrency.py   # modified — widened gate, derived rosters, the three new cases

docs/architecture.md           # modified — §8 the rule, §13 the borrowed-client constraint
```

## Implementation Phases

### Phase 1: The scope gate and the rosters

- Widen `_sink_classes_holding_a_driver` into a gate over every sink class defining `emit`, excluding
  `base.Sink`; rename it for what it now does.
- Derive the post-close parametrization from it instead of `CLOSED_SINKS`, and add the second lint.
- Add the exemption claims to the 16 newly in-scope classes so the concurrency lint stays green.
- At the end of this phase the suite fails on exactly the three sinks FR-001 fixes, which is the
  evidence the roster works.

### Phase 2: The three guards

- `_closed` in `KafkaSink`, `GooglePubSubSink` and `_RedisSink`, with driver doubles that keep
  succeeding after close.
- Mutation-check each new assertion against the guard it claims to enforce, one at a time.

### Phase 3: The contract and the record

- The post-close obligation in `Sink.emit` / `Sink.close`, and the one-sentence rule in
  architecture.md §8.
- The FR-004 exemption reasons, the wrapper-delegation note, and the §13 borrowed-client constraint.
