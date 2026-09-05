# Completed Spec — SPEC-049: Sink Construction Validation and Driver Bounds

## What was completed?

A bad sink argument now fails where it is written, under one rule stated once in FR-001: the library
floors a value that works today, refuses one that is already broken, and refuses a new argument.

- **The HTTP family refuses at construction** (FR-001): an unusable `timeout`, CR/LF in a header name,
  value or bearer token, and a URL whose scheme is not `http`/`https` — inherited by every subclass,
  named in a roster test. New helpers `_retry.require_timeout` / `require_positive`, raising siblings of
  `usable_timeout`; the two shipped floor-side callers are pinned.
- **Degenerate bounds refused** (FR-002): `SocketTransport(timeout=)`, `ClickHouseSink`/`PostgresSink`
  `chunk_size`, `chunk_list`'s own documented `ValueError`; `ClickHouseSink.emit` raises when a non-empty
  batch produced no chunk. `GooglePubSubSink(overflow_timeout=)` is **floored**, the third `usable_timeout`
  caller — a deviation from the first draft, because `inf` delivers against a healthy client.
- **`RotatingFileSink`** (FR-003): a non-positive `interval` is refused, `when=None` included;
  `max_bytes<0` and `backup_count<0` are floored to `0`, which they already behaved as.
- **An argument no backend can use is an error** (FR-004): `LogstashSink` refuses socket-only arguments
  beside `url=` and HTTP-only keywords beside `host=`/`port=` (`transport`/`max_datagram_bytes` now
  default to `None`); `SentrySink` refuses a DSN missing its scheme, host, key or project where the
  fallback is built; `SyslogSink(app_name=)` and `ElasticsearchSink(index=)` refuse whitespace or empty;
  `NATSSink` refuses a non-positive `max_reconnect_attempts` and `servers` beside `client=`.
- **Driver bounds** (FR-005): `MongoDBSink(socket_timeout=, server_selection_timeout=)` with
  `socketTimeoutMS=30000` applied only when the URI names none; `RabbitMQSink(blocked_connection_timeout=,
  socket_timeout=, stack_timeout=)` applied inside `_connect` so a reconnect keeps it, URL value
  preserved; `ClickHouseSink(send_receive_timeout=)` forwarded only when given. Each refused beside an
  injected client. Three `architecture.md` §12 entries; the pika one says it was not executed.
- **`SocketTransport`'s abandonment line** reports the attempts actually made (FR-006).
- **Prose** (FR-007): `_rollover_seconds` is module-level; `_reconnect_if_broken`'s dedent fixed;
  `LoggingSink.emit`'s `Raises:` says what it can observe; `tests/test_prose_layout.py` closes both classes.

## What changed from earlier specs?

- SPEC-047's `test_a_falsy_connect_bound_is_still_forwarded` and `NATSSink`'s "passed through rather than
  corrected" docstring are superseded in place; its two §12 open items are Resolved.
- SPEC-043's `test_http_mode_ignores_the_datagram_limit` became `..._refuses_...`.
- SPEC-011's `MongoDBSink`, `RabbitMQSink` and `ClickHouseSink` gained keyword-only arguments (additive).

## Verification

Six local gates green on every commit; 40 mutants planted and restored from a scratchpad copy — 39
reddened on the message assertion, the one expected-equivalent retry case stayed green; `--collect-only`
diffed against `212fd16` (one superseded test renamed, all else additions). Premises re-probed at
`212fd16` before the build; one spec review, one plan review, two diff frames before the push. Not
executed: pika's blocked-connection behaviour and the three drivers' wall-clock waits (recorded in §12).
