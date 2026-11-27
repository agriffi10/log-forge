---
type: Spec
id: SPEC-107
title: "RabbitMQSink Leaves an Injected Connection Open at Close"
description: "Makes RabbitMQSink.close() close only the channel the sink opened and leave open a pika connection the caller passed in, as the README and the method's own docstring already say, so an application that shares its connection with this sink keeps it after shutdown()."
spec_status: Draft
depends_on: [SPEC-010, SPEC-032, SPEC-076]
---

## Overview

An application can hand `RabbitMQSink` a `pika` connection it already holds, with `connection=`,
and share it between its own consumers and the sink. The library's documentation says such a
connection stays the application's: the README's *Ownership* rule reads "A resource the sink opens
itself is closed on `shutdown()`; an injected one is left open for you to manage", and
`RabbitMQSink.close`'s docstring says it closes "the channel and, when owned, the connection". The
code does otherwise. `RabbitMQSink._release` closes the connection with no ownership test, so the
application loses its connection at `shutdown()`, and its consumers with it. The class disagrees
with itself, since `RabbitMQSink._reset` does test ownership before it closes the connection, and
with its siblings: the Redis pair, `ClickHouseSink`, `MongoDBSink`, `PostgresSink` and `SQLiteSink`
each leave an injected client or connection open at close.

Andrew was shown the contradiction on 2026-11-27 and chose "Fix the behaviour": the documentation
is right and the code is the defect. This spec makes `close()` leave a borrowed connection open,
on every path that closes the sink, while still closing the channel, which is the sink's own.

The original criterion, SPEC-010 FR-006, reads "`close()` closes the channel and connection", with
no ownership clause, where its neighbour FR-005 says of the Redis sinks that an injected client is
not closed. SPEC-107 supersedes that clause of SPEC-010 FR-006.

## Scope

### In Scope

- `RabbitMQSink.close()` closing the channel the sink opened and, only when the sink opened it,
  the connection (FR-001).
- One release rule for the sink's two release sites, `_release` and `_reset`, held by a guard
  derived from the module rather than from a list (FR-001).
- What a closed sink holds and does: a borrowed connection's reference is kept, a later `emit` is
  refused as it is for an owned connection, and a borrowed sink never opens a connection of its
  own (FR-002).
- The same outcome on every path by which the library closes the sink, and against a real broker
  (FR-003).
- The record: the docstrings, the superseded clause of SPEC-010, the decision, the invariants
  page, the README and the pending release notes (FR-004).

**The twin paths** (invariant 6). Inside the sink, `_release` (at close) and `_reset` (after a
failed publish) are two releases of one transport, and a borrowed and an owned connection are the
two modes of each. Outside it, every close the library makes of a sink it holds is one call,
`sink.close()` in `_lifecycle.release`, reached from `shutdown()`, the exit drain, a swap, an owed
close, the orphan path and a shipped wrapper's forward; a caller's own `sink.close()` is the
remaining route. All of them run `RabbitMQSink.close()`, the closed guard and `_release`, once.

### Out of Scope

- **`src/log_foundry/sinks/_rounds.py` and the settlement of a raised call inside
  `RabbitMQSink._round`.** A later spec moves that; this one edits neither.
- **Any other sink's ownership behaviour.** This spec reads the siblings as precedent and changes
  none of them, and it writes no rule that speaks for a sink other than `RabbitMQSink`.
- **A roster over every sink that takes an injected client.** None exists today
  (`tests/test_injected_client_roster.py` holds construction-time refusals, and
  `tests/test_sink_release_roster.py` the library's own closers of a sink), and building one would
  rule on sinks this spec does not change. The guard this spec leaves is scoped to
  `sinks/rabbitmq.py` (FR-001).
- **A connection a forked child inherits.** `RabbitMQSink` defines no `reacquire_after_fork` and
  compares no pid, and this spec adds neither. What stays as it is: the library never closes an
  inherited sink in a child, because `_lifecycle.releasable` refuses it, and a `close()` the
  caller makes in a child runs the same release as in the parent, so after this spec it leaves a
  borrowed connection open there too and still closes the inherited channel and an inherited owned
  connection.
- **An argument that restores the old close.** A caller who wants the connection closed closes it.
- **Publisher confirms, and the rule that a borrowed connection is not probed** (SPEC-093). The
  release does not read `confirm`.
- **The channel.** It is still closed at close in both modes, on the thread that closes the sink,
  as today.
- **Any change to the closed guard, the lifecycle or the fork repair.** No file under
  `src/log_foundry/` changes but `sinks/rabbitmq.py`.

---

## Functional Requirements

### FR-001: `close()` closes the channel the sink opened, and the connection only when the sink opened it

#### Description:

The channel is the sink's own in both modes. `_active_channel` opens it on whichever connection
the sink holds, and the sink never borrows a channel
(`docs/decisions/sink-delivery/rabbitmqsink-asks-the-broker-to-confirm-only-when-told-to-and-a-lost-confirm-is-unknown.md`),
so the release closes it and drops its reference for a borrowed connection as for an owned one.
The connection is closed, and its reference dropped, only when `_owns_connection` is true.

`_reset` already states exactly that rule. `_release` therefore calls `_reset` rather than
restating it, so the ownership test is written once and the two sites cannot drift apart again.
The guard still hands `_release` to `ClosedGuard.close`, which runs it once with the lock held
and the state already `CLOSED`; neither `close()` nor the guard changes.

No test asserts today that an owned connection is closed at `close()`: the only close test builds
the sink over an injected connection. This FR adds that assertion, since without it a release that
closed no connection at all would pass.

#### Acceptance Criteria:

- [ ] For a sink built with `connection=conn`, after one `emit` and `close()`, `conn.closed` is
      false and the channel the sink opened on it is closed, with `confirm=False` and with
      `confirm=True`. At `35b2f55d` this fails on `conn.closed`.
- [ ] After a second `close()` on that sink, the channel's `close` has been called once in all and
      `conn.close` not at all.
- [ ] `close()` on a borrowed sink that never emitted calls neither `conn.channel()` nor
      `conn.close()`.
- [ ] When the channel's `close()` raises, the sink's `close()` returns normally, `conn.close` is
      not called and `sink._channel` is `None`.
- [ ] For a sink built with `url=` over the stub `pika` of `tests/test_sinks_rabbitmq.py`, after
      one `emit` and `close()`, the connection the stub built is closed, its channel is closed and
      `sink._connection` is `None`. A release that closes no connection fails this.
- [ ] A test reads `src/log_foundry/sinks/rabbitmq.py` as an AST and collects every statement that
      passes `self._connection` to `_safe_close`, calls `self._connection.close()`, or assigns
      `None` to `self._connection`. It fails, naming the enclosing method, for any such statement
      that is not inside an `if` whose test reads `self._owns_connection`, and it fails when it
      collects none. Run over the module as it stands at `35b2f55d`, it fails naming `_release`.
- [ ] `tests/test_sinks_rabbitmq.py::test_close_closes_channel_and_connection`, which asserts
      `conn.closed is True` for an injected connection, is replaced by the tests of the first and
      fifth criteria, and no test asserts that an injected connection is closed.
- [ ] Serves invariants 5 and 6.

### FR-002: A closed sink keeps a borrowed connection's reference, refuses as an owned one does, and never opens a connection of its own

#### Description:

After `close()`, `self._connection` still holds the caller's object when the connection was
borrowed, and is `None` when the sink opened it, as today. The siblings are the precedent: none of
them drops its reference in its release, borrowed or owned.

Keeping the reference is what rules out a leak. `_active_channel` connects whenever it finds
`self._connection` is `None`, and for a borrowed sink `_connect()` would build a connection from
no URL, since `url=` is refused beside `connection=`: `pika`'s default parameters, a broker on
localhost. Nothing would ever close it, because `_owns_connection` is false and the guard's
release has already run. `self._connection` has four writers: the constructor, `_release`,
`_reset` and that reconnect. With FR-001, neither release stores `None` for a borrowed sink, so
the reconnect is unreachable for one.

The refusal after close does not change and does not depend on ownership. The closed guard
refuses a round from the moment a close begins, keyed on the sink being released and never on who
owns the client
(`docs/decisions/sink-delivery/a-sink-that-released-its-transport-refuses-one-that-released-nothing-keeps-accepting.md`),
so a closed borrowed sink refuses with the same error as a closed owned one and reaches no driver.

**One trade is accepted.** With the reference kept, a call that goes around the guard, such as a
subclass calling `_active_channel()` on a closed sink, opens a new channel on the caller's
connection where it used to open a connection of the sink's own. The connection is the caller's
and is still open, and a channel on it ends when the caller closes it; the alternative was a
connection nothing closes.

#### Acceptance Criteria:

- [ ] After `close()` on a sink built with `connection=conn`, `sink._connection is conn` and
      `sink._channel` is `None`. At `35b2f55d` this fails: the reference is `None`.
- [ ] On a sink built with `connection=conn` whose `_connect` is replaced by a recorder, the
      recorder is never called across a publish that raises, the `close()` and a direct
      `_active_channel()` call made after the close. At `35b2f55d` the last step calls it.
- [ ] After `close()`, for a borrowed and for an owned sink alike, `emit([event])` raises
      `SinkDeliveryError` with the message `RabbitMQSink published none of 1 message(s): the sink
      is closed`, `deliver([event])` returns every outcome `REJECTED`, `losses()` is unchanged, no
      channel is opened and `_connect` is not called. This passes at `35b2f55d` too; it is here to
      hold the refusal while the release changes under it.
- [ ] With the closed refusal removed from `_round`,
      `tests/test_sink_concurrency.py::test_a_closed_sink_refuses_a_batch_instead_of_reaching_its_driver`
      fails for its `rabbitmq` case because the post-close `emit` returned instead of raising; this
      guard is mutation-tested, because with the reference kept a missing refusal would publish on
      the caller's connection after close.
- [ ] `tests/test_sink_post_close.py`, `tests/test_closed_guard.py`, `tests/test_rabbitmq_confirm.py`
      and `tests/test_sinks_broker_engine.py` pass with no edit.
- [ ] Serves invariants 6 and 7.

### FR-003: Every path by which the library closes the sink leaves a borrowed connection open

#### Description:

The library closes a sink it holds at one call, `sink.close()` in `_lifecycle.release`, made only
after `_lifecycle.releasable` allows it. `shutdown()`, the exit drain, a swap's close of a
displaced sink, an owed close, the orphan path's close and a shipped wrapper's forwarded release
all arrive there, so FR-001's change in `_release` covers each of them and no lifecycle code
changes. Each path, and whether it changes:

| Path | Changes | Why |
|---|---|---|
| `RabbitMQSink.close`, then `ClosedGuard.close`, then `_release` | yes | FR-001 |
| `RabbitMQSink._reset`, after a failed publish or a failed probe | no | it already tests `_owns_connection` |
| `RabbitMQSink._active_channel`'s reconnect | no | unreachable for a borrowed sink once no release drops its reference (FR-002) |
| `RabbitMQSink._probe_owned` | no | it reads only an owned connection |
| `_lifecycle.release` and everything that calls it | no | it reaches the connection only through `close()` |
| `ClosedGuard` | no | it runs the release it is handed, once, and in a forked child adopts state and closes nothing |
| The fork repair in `_fork` | no | it replaces the guard's locks and event; `RabbitMQSink` has no `reacquire_after_fork`, so nothing of the sink's runs in it |

The exit drain takes the owed record as `shutdown()` does and closes through the same call, so it
is covered by the same criterion and not driven separately.

#### Acceptance Criteria:

- [ ] With a `RabbitMQSink` built with `connection=conn` and configured as the sink, in each of
      these cases `conn.closed` is false, the channel the sink opened is closed and that channel's
      `close` was called once: (a) a traced call, then `shutdown()`; (b) a level call outside any
      span, then `shutdown()`; (c) a level call, then a `configure(sink=other)` that displaces the
      sink, then `shutdown()`; (d) the sink held as a member of a `MultiSink`, a traced call, then
      `shutdown()`. At `35b2f55d` each fails on `conn.closed`.
- [ ] In `tests/integration/test_rabbitmq.py`, against the compose broker, a sink built over a
      real `pika.BlockingConnection` passed as `connection=` publishes a batch and is closed;
      afterwards the connection's `is_open` is true, the channel the sink used reads `is_closed`,
      and a channel the test then opens on the same connection reads every message of the batch
      from the queue. It passes in a local run of the integration suite before the push.
- [ ] The diff changes no file under `src/log_foundry/` other than `sinks/rabbitmq.py`.
- [ ] `inspect.signature(RabbitMQSink)` is unchanged, and `tests/test_public_surface.py` and
      `tests/typed_consumer/` pass with no edit.
- [ ] `tests/test_fork_lifecycle.py`, `tests/test_sink_ownership.py`,
      `tests/test_sink_release_roster.py`, `tests/test_injected_client_roster.py`,
      `tests/test_conformance.py` and `tests/test_conformance_gate.py` pass with no edit to them or
      to `tests/conformance_pins.py`, `tests/conformance_classes.py` and
      `tests/conformance_doubles.py`, so the rows of `RabbitMQSink[owned]`,
      `RabbitMQSink[borrowed]`, `RabbitMQSink[confirmed]` and `RabbitMQSink[confirmed-borrowed]`
      keep their verdicts, pins and departures.
- [ ] Serves invariants 5, 6, 10 and 12.

### FR-004: The record states what `RabbitMQSink` closes

#### Description:

The docstrings in `sinks/rabbitmq.py` are the sink's prose, and two of them state the old
behaviour or cite the superseded clause. The completion ritual
(`docs/process/completion-ritual.md`) records the rest. A caller can see this change, so step 6
owes the pending release notes an entry; *Compatibility*, below, says what it tells a caller.

`README.md`'s *Ownership* sentence is already right and is not edited.

#### Acceptance Criteria:

- [ ] `RabbitMQSink._release`'s docstring no longer says it closes the connection and drops each
      reference without condition. It says the release closes and drops the channel, closes and
      drops the connection only when the sink opened it, and leaves a borrowed connection open
      with its reference kept, and why the reference is kept.
- [ ] `RabbitMQSink.close`'s docstring cites SPEC-107 for the ownership rule in place of
      SPEC-010's "FR-006", and says a borrowed connection is left open.
- [ ] The class docstring and the `connection` entry of `__init__`'s `Args:` each say that a
      connection passed in is never closed by the sink, and that the channel the sink opens on it
      is.
- [ ] `emit`'s `Raises:` clause and `_active_channel`'s docstring say that the reconnect is
      reached only by a sink that opened its own connection.
- [ ] The two phrases `tests/conformance_pins.py` quotes from `sinks/rabbitmq.py`, "With
      `confirm=False`, the default, a publish that returned is all this sink can know" and "A
      borrowed connection is not probed before a round, so a publish on one that died while idle
      reads as a lost confirm", are still in the file word for word.
- [ ] The comment in `tests/test_sink_concurrency.py`'s `rabbitmq` builder that begins "close()
      nulls the connection" is corrected: the builder's connection is borrowed, so a missing
      refusal would open a channel on it, which the builder still counts.
- [ ] In `docs/specs/SPEC-010-queue-and-stream-sinks.md`, the clause "`close()` closes the channel
      and connection" of FR-006 is struck through in place and followed by a marker reading
      "superseded by SPEC-107 FR-001", saying an injected connection is left open.
- [ ] A new decision,
      `docs/decisions/sink-lifecycle/rabbitmqsink-closes-the-channel-it-opened-and-never-a-connection-it-was-handed.md`,
      carries the fence and its entry: the channel is closed in both modes, the connection only
      when owned, a borrowed reference is kept so the sink never connects on its own, the
      ownership test is written once, and it speaks for `RabbitMQSink` alone. Its History opens on
      the completion date, and `sh scripts/gen-index.sh` is run.
- [ ] In `docs/invariants.md`, the fifth entry gains one sentence saying that `RabbitMQSink`'s
      release closes the channel it opened and leaves open a connection the caller handed it,
      citing SPEC-107, and its *Guarded* list names `tests/test_sinks_rabbitmq.py` for it. The
      sentence names no other sink.
- [ ] `README.md`'s *Queue & stream* section says that a `RabbitMQSink` given a connection opens
      its own channel on it and closes only that channel at `shutdown()`. The *Ownership* sentence
      is unchanged.
- [ ] `docs/release-notes/v1.2.1.md` gains, under *Fixed*, an entry headed for SPEC-107 saying
      what *Compatibility* says; its list of exceptions gains one bullet for code that relies on
      the sink closing a connection it was handed; the count in the sentence that introduces that
      list equals the number of bullets, and the sentence after the list places the new bullet
      under *Fixed*; and the list of specs the release carries names SPEC-107.
- [ ] `sh scripts/spec-lint.sh`, `sh scripts/docs-lint.sh` and
      `poetry run python scripts/docstring-lint.py` exit 0.
- [ ] This FR serves no invariant: it is the record of a change FR-001 to FR-003 make, judged by
      the completion ritual's own rules.

---

## Compatibility

Version 1.2.0 is released and closes an injected connection, so this is a change a caller can
see. It is a fix that brings the code to the contract the README and the method's docstring
publish, and Andrew chose it on 2026-11-27.

- **What a caller is told.** Under *Fixed* in the pending release notes: up to and including
  1.2.0, `RabbitMQSink` closed a `pika` connection passed as `connection=` when the sink was
  closed, by `shutdown()`, at exit or by a `configure()` that released it, contrary to the
  README's *Ownership* rule. It now closes only the channel it opened on that connection. Code
  that relied on the sink closing the connection must close it itself after `shutdown()`. A
  connection the sink opened from `url=` is closed as before.
- **What does not change.** The constructor's signature, every public name and the typed
  consumer (invariant 10): no call that 1.x accepted is refused, so the invariants page's tenth
  entry gains no approved exception. The refusal after close and its error text are unchanged.
- **What the register says a release may change.** `docs/decisions/public-api/` freezes names,
  import paths and signatures, and `docs/decisions/release-supply-chain/` settles how a version
  is cut; neither holds a rule on what a patch release may change in behaviour. The pending notes
  file, `docs/release-notes/v1.2.1.md`, already carries close-behaviour fixes with a compatibility
  bullet each (SPEC-087, SPEC-101), and says its own number is settled when it is tagged. Nothing
  in this spec depends on that number.

## Data Model

No type changes. The two attributes this spec governs, and what each holds:

```python
# src/log_foundry/sinks/rabbitmq.py — RabbitMQSink
self._owns_connection: bool   # connection is None at construction; never reassigned
self._connection: Any
#   borrowed: the caller's object from construction onwards, through every reset and after close
#   owned:    the connection the sink opened; None after a reset or a close, until it reconnects
self._channel: Any            # the sink's own in both modes; None after a reset or a close
```

## API / Interface Contract

No public surface changes: `RabbitMQSink`'s signature, attributes and error text are as they are.
The private shape the build changes:

```python
def _release(self) -> None: ...
#   what ClosedGuard.close runs once: calls self._reset(), so the ownership test has one home

def _reset(self) -> None: ...
#   unchanged: closes and drops the channel; closes and drops the connection only when
#   self._owns_connection
```

## File & Folder Structure

```
src/log_foundry/sinks/rabbitmq.py          # FR-001, FR-002; docstrings for FR-004
tests/test_sinks_rabbitmq.py               # FR-001, FR-002, FR-003 (close, reference, library paths, AST guard)
tests/integration/test_rabbitmq.py         # FR-003 (a real pika connection, borrowed)
tests/test_sink_concurrency.py             # FR-004 (one comment)
docs/specs/SPEC-010-queue-and-stream-sinks.md                                              # FR-004
docs/decisions/sink-lifecycle/rabbitmqsink-closes-the-channel-it-opened-and-never-a-connection-it-was-handed.md   # FR-004
docs/invariants.md, README.md, docs/release-notes/v1.2.1.md                                # FR-004
```

## Implementation Phases

### Phase 1: The release and its tests (FR-001, FR-002, FR-003)

- Write the borrowed and owned close tests, the reference and reconnect tests, the library-path
  cases and the AST guard, and watch each one that names `35b2f55d` fail there.
- Make `_release` call `_reset`.
- Mutation-test the closed refusal against the `rabbitmq` post-close case.
- Add the borrowed-connection test to the integration module and run the integration suite
  locally.

### Phase 2: The record (FR-004)

- Rewrite the docstrings and the test comment.
- Mark the superseded clause of SPEC-010, write the decision, add the invariants sentence, the
  README sentence and the release-notes entry, and regenerate the indexes.

## Evidence

Every line below was read at `35b2f55d`.

- **The defect.** `src/log_foundry/sinks/rabbitmq.py:173` sets `_owns_connection`; `close` (`:431`)
  hands `_release` to the guard (`:447`); `_release` (`:449`) closes the channel and then the
  connection with no ownership test (`:464` to `:469`); `_reset` (`:671`) tests `_owns_connection`
  before closing the connection (`:686`). `README.md:645` states the *Ownership* rule.
- **Every reader and writer of the connection**, from a grep of `sinks/rabbitmq.py` for the two
  names. `_connection` is written at `:174` (constructor), `:469` (`_release`), `:657`
  (`_active_channel`'s reconnect) and `:688` (`_reset`, owned only), and read at `:467`, `:468`,
  `:629`, `:631`, `:656`, `:659`, `:686` and `:687`. `_owns_connection` is written at `:173` and
  read at `:629` and `:686`. `_reset` is called at `:506`, `:525` and `:637` of the same file, and
  `_release` is called only by the guard it is handed to, at
  `src/log_foundry/sinks/_guard.py:303`.
- **The library's one close of a sink.** `sink.close()` at `src/log_foundry/_lifecycle.py:3086`,
  inside `release` (`:2952`), after `releasable` (`:2851`). `_lifecycle.py`, `_fork.py` and
  `worker.py` name neither `_connection` nor `_channel`, and only `sinks/file.py` defines
  `reacquire_after_fork` (`:991`, `:1462`).
- **The siblings.** Each guards its release on ownership and keeps its reference:
  `sinks/redis.py:400`, `sinks/clickhouse.py:423`, `sinks/mongodb.py:649`, `sinks/postgres.py:784`
  and `sinks/sqlite.py:456`, with `tests/test_sinks_redis.py::test_injected_client_is_not_closed`,
  `tests/test_sinks_clickhouse.py::test_injected_client_not_closed`,
  `tests/test_sinks_mongodb.py::test_injected_client_not_closed`,
  `tests/test_sinks_postgres.py::test_close_commits_injected_but_does_not_close` and
  `tests/test_sinks_sqlite.py::test_close_commits_injected_connection_but_does_not_close_it`.
- **What pins today's behaviour.** `tests/test_sinks_rabbitmq.py:90` to `:98`, whose line 97
  asserts `conn.closed is True` over an injected `FakeConnection`, and SPEC-010 FR-006
  (`docs/specs/SPEC-010-queue-and-stream-sinks.md:142` to `:143`). `tests/integration/test_rabbitmq.py`
  builds every sink from `url=`, so no integration test borrows a connection.
- **Measured, not only read.** In a scratch export of `35b2f55d` with `_release` changed to call
  `_reset` and nothing else touched, the whole suite was run once on Python 3.13. The one failure
  the change caused was `test_close_closes_channel_and_connection`, at its line 97. Three soak
  tests failed there because the export is not a git checkout (`git ls-files` exits 128), and one
  timing test in `tests/test_sanitize.py` failed under the parallel run and passed alone. So no
  other test in the suite pins the old close, and the conformance rows, the post-close tests, the
  rosters and the fork tests are green with the release changed. The instrument's limit: a test
  that starts a child interpreter may import the installed tree and not the export. With the
  closed refusal also removed from `_round` in that export,
  `test_a_closed_sink_refuses_a_batch_instead_of_reaching_its_driver[rabbitmq]` failed with
  `DID NOT RAISE SinkDeliveryError`.
