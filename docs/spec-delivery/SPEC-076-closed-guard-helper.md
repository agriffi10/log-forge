# Completed Spec — SPEC-076: The Closed-Guard Helper

## What was completed?

- `sinks/_guard.py`, a private leaf: `ClosedGuard`, `GuardMode`, `Section` and
  `CLOSE_OVERLAP_WAIT` (0.5 s). A guard is `OPEN`, then `CLOSING` from the moment a `close()`
  begins, then `CLOSED` from the moment its one release begins. Every `enter()` outside `OPEN` is
  refused holding no lock. A second close waits for the first one's release and never returns
  through it.
- Eleven classes adopt it at `self._guard`, each opening every guarded member with
  `if not section:` and its answer:
  - `SERIAL`: `ClickHouseSink`, `PostgresSink`, `AzureEventHubsSink`, `RabbitMQSink`,
    `SocketTransport`, `FileSink`, `RotatingFileSink` and `SQLiteSink`. One lock spans each section
    and the close. A close refuses every later caller at once, including one already queued on the
    lock, so it waits for the one section inside and never releases under it.
  - `CLOSE_ONLY`: `MongoDBSink`, `_RedisSink` (both Redis sinks) and `KafkaSink`, whose `emit` and
    `flush` are both sections. Sections run concurrently. A close waits at most
    `CLOSE_OVERLAP_WAIT` for them, then releases, and a section the release overtook reads
    `released_during`.
- Two stay local, each with its reason in its class docstring. `NATSSink` keeps SPEC-061's closed
  and closing state: its pid check runs before its lock, its wedged-loop backstop marks it closed
  from a waiting caller, and its `close()` raises `TimeoutError`. `GooglePubSubSink` is not an
  engine sink, and its rule for an overlapping close is the orphaned-future count it already kept.
- The Kafka window is closed. An `emit` overtaken by the release counts every message it handed to
  the producer in `failed`, an upper bound, with one line:
  `lost N message(s); KafkaSink, produced while the sink was closing`.
- A closed or closing engine sink's `deliver` (the five `SERIAL` engine adopters, `MongoDBSink`,
  `_RedisSink`, and `NATSSink` from its own state) answers through `RunContext.refuse` with its
  1.x closed error and `REJECTED`, making no transport call.
- The 1.x private names stay readable on all eleven as read-only properties: `_closed` returns the
  guard's `closed`, and `_lock` (`SERIAL`) or `_close_lock` (`CLOSE_ONLY`) returns the guard's lock.

## What changed from earlier specs?

- A closing adopter refuses from the moment `close()` begins rather than when its release does.
  `SocketTransport.close` is now idempotent. A `CLOSE_ONLY` close now waits up to 0.5 s for a call
  still inside, where it waited for none. The details a caller sees are in
  `docs/release-notes/v1.2.0.md`.
- A subclass that assigns `_closed`, `_lock` or `_close_lock` on an adopter now gets
  `AttributeError`, and `_closed` reads true while a close is still waiting.
- `tests/test_sink_concurrency.py`'s two lints were re-derived to see the guard, with new fixtures,
  and an AST check holds the refused-branch opening. `("KafkaSink", "emit")` joined
  `tests/test_deferral.py`'s `IMMEDIATE`. `tests/test_fork_lifecycle.py`'s one assignment of
  `sink._lock` now targets `sink._guard._lock`. No test was deleted and no floor lowered.
- Decisions: one new one, *A closing sink refuses at once; a SERIAL close waits for the one call
  inside, a CLOSE_ONLY close waits a bounded time*. *A sink tolerates concurrent callers* gained a
  dated 2026-11-09 entry. *A sink that released its transport refuses* is unchanged, as the spec
  says. `docs/invariants.md` 3, 5, 7 and 12, `architecture.md` §13 and the inventory were updated.

## Verification

- The claims above are pinned by `tests/test_closed_guard.py` (FR-001 to FR-003 and the pid
  adoption, both modes), `tests/test_sink_post_close.py` (FR-004 and the aliases, worker and
  orphan), `tests/test_sinks_kafka.py` (the window), `tests/test_sink_concurrency.py` (FR-002 and
  the lints) and `tests/test_fork_lifecycle.py` (FR-006). The PR's CI run is the record of the full
  suite.
- **Owed to CI:** this branch touches `src/log_foundry/sinks/`, so the integration job "sinks
  against real services" runs on the PR. The doubles stand in for every driver locally; the real
  `pymongo`, `redis-py` and `confluent-kafka` closes are only exercised there.

## Open and accepted edges

- Accepted: the release itself (`client.close()`, `producer.flush()`, a file's `close()`) stays as
  unbounded as `architecture.md` §13 records. Only the wait for a call already inside is bounded.
- Accepted: a Redis call that outlives the bound can reopen a connection nothing reaps, the leak
  the SPEC-032 decision records. The alternative was an unbounded wait on a call bounded only by the client's own socket timeout.
- Accepted: Kafka's overtaken count includes messages the release's flush may have delivered. It
  is invariant 2's upper bound for `failed`, never an undercount.
- Accepted: an interrupt landing inside a `CLOSE_ONLY` `enter()` between registering the section
  and entering its `try` leaks one count, so a later close waits the whole bound. Still bounded.
- Unchanged: `GooglePubSubSink.close` stays unguarded. A second call drains an empty list.
