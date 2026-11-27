# Completed Spec — SPEC-107: RabbitMQSink Leaves an Injected Connection Open at Close

## What was completed?

- **The release** (FR-001), in `sinks/rabbitmq.py`: `RabbitMQSink._release` now calls
  `_reset()`, where it closed the channel and then the connection with no ownership test. The
  channel is closed and dropped in both modes; the connection is closed and dropped only when the
  sink opened it. A `pika` connection passed as `connection=` is left open at `close()`, on every
  path that closes the sink. That one statement is the whole behaviour change; no other file
  under `src/log_foundry/` changed.
- **The kept reference** (FR-002): a closed borrowed sink still holds the caller's connection, so
  `_active_channel`'s reconnect is reached only by a sink that opened its own. The refusal after
  close is untouched and reads the same for a borrowed and an owned sink.
- **The guard** (FR-001): `connection_release_sites(source)` in `tests/test_sinks_rabbitmq.py`, an
  AST function over a module's source text that returns the statements releasing
  `self._connection` and, of those, the ones not under the ownership test. Six standing cases in
  four tests: no test, a negated test, an `else` branch, an `or`, the bare test (asserting
  silence) and the live module (asserting it collected `_reset`'s site). It is scoped to this
  module and does not follow an alias.
- **The tests**: in `tests/test_sinks_rabbitmq.py`, the borrowed and owned closes, the second
  close, the close with no channel, a channel close that raises, the kept reference, the recorder
  on `_connect`, the refusal for both modes, and four library paths (`shutdown()` after a traced
  call, after a level call, after a displacing `configure()`, and as a `MultiSink` member); the
  `confirm=True` close in `tests/test_rabbitmq_confirm.py`; and in
  `tests/integration/test_rabbitmq.py` a real borrowed `pika.BlockingConnection` that outlives the
  sink and reads the batch back, for both values of `confirm`.
- **The record** (FR-004): the docstrings of the class, `__init__`'s `connection`, `emit`,
  `close`, `_release`, `_active_channel` and `_reset`; the new sink-lifecycle decision
  *`RabbitMQSink` closes the channel it opened, and never a connection it was handed*; the README's
  *Queue & stream* paragraph; and `docs/release-notes/v1.2.1.md` (a *Fixed* entry and a
  thirty-ninth compatibility bullet).
- **No inventory row.** The guard hard-codes this sink's attribute names and is not reusable, and
  a row would offer it as the cross-sink roster the spec ruled out.

## What changed from earlier specs?

- **SPEC-010 FR-006's "`close()` closes the channel and connection" is superseded** and marked in
  place: an injected connection is left open.
- **`test_close_closes_channel_and_connection` is removed.** It asserted the old close over an
  injected connection, and the borrowed and owned close tests replace it.
- **SPEC-032's `rabbitmq` post-close builder** in `tests/test_sink_concurrency.py` loses its
  `sink._connect` stand-in and its comment is corrected: the builder's connection is borrowed, so
  nothing reaches `_connect`, and a missing refusal now opens a channel on that connection, which
  the builder still counts.
- **Five findings outside the spec's criteria are open in `architecture.md` §12**, recorded and
  not fixed: `AzureEventHubsSink` and `NATSSink` close an injected client against the README's
  *Ownership* sentence; `RabbitMQSink.close()` compares no pid in a forked child; the integration
  harness probes RabbitMQ with a bare connect; `MODULE_FLOORS`' docstring; and two `NATSSink`
  tests that each failed once under load.

## Verification

- **Test ids, from the runner** (`pytest --collect-only -q`) over `tests/test_sinks_rabbitmq.py`,
  `tests/test_rabbitmq_confirm.py` and `tests/test_sink_concurrency.py`: 137 at `71fb3b9c` and 156
  at `a00162ea`, 20 added and the one above removed. The integration module gains one test with
  two ids, which the runner here could not collect without `pika`.
- **The closed refusal, re-run at `a00162ea`** against a scratch copy of `src/` with the refusal
  removed from `_round`:
  `test_a_closed_sink_refuses_a_batch_instead_of_reaching_its_driver[rabbitmq]` fails with
  `DID NOT RAISE SinkDeliveryError`, and the test's other fourteen ids pass.
- **The implementer's measurements at `41360123`**, the head before the rebase onto `71fb3b9c`,
  not re-run here: the same mutant failed that case before and after the builder edit; a release
  that closes only the channel failed only
  `test_close_closes_an_owned_connection_and_drops_its_reference`; a one-time sweep of nine clause
  mutants of the guard, each failing a named case, left nothing in the repo; with
  `sinks/rabbitmq.py` planted whole from `35b2f55d`, the integration test's two ids failed on
  `borrowed.is_open`; and the integration test passed against the compose broker.
- **On the completed tree**, Python 3.13 on macOS: `poetry run pytest`, `spec-lint.sh`,
  `docs-lint.sh` and `docstring-lint.py` each exited 0. The completion changed no code, so it ran
  neither `ruff` nor `mypy`; those two gates and the integration suite are the branch's to run
  before its push.
