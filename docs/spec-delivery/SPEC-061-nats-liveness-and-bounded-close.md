# Completed Spec — SPEC-061: `NATSSink` Liveness and a Bounded Close

## What was completed?

Five FRs, all in `src/log_foundry/sinks/nats.py`, built as `0c04c90..cc54227`. No public name or
signature changed and no constructor argument was added.

- **The loop thread** (FR-001): one daemon thread, `log-foundry-nats-loop`, runs the sink's event
  loop from construction to `close()`, so the driver's reader, ping and reconnect tasks run between
  calls. `emit`, `flush` and `close` submit one coroutine each under `self._lock` and wait under a
  bound plus `CALL_SLACK`; on expiry the loop is treated as wedged, the sink closes and the thread
  is joined under `LOOP_JOIN_TIMEOUT`. A forked child refuses before taking the lock (pid check).
- **The liveness probe and its two latches** (FR-002, FR-001): for a client the sink connected,
  `emit` publishes only after a PING through the driver's public `flush(timeout=PROBE_FLUSH_TIMEOUT)`
  is answered inside `publish_timeout`. The probe is never cancelled, at most one is outstanding, a
  pending one is reused, and it answers only if it completed, the client still reports connected
  and the reconnect counter has not moved. The probe latch refuses every later caller at once while
  that PING has gone unanswered for `publish_timeout`; the abandoned-publish latch (`_stuck`)
  refuses every later `emit` and `flush`, on owned and injected clients, while a publish the
  deadline abandoned is still pending. The probe latch is read before the lock; the
  abandoned-publish latch before it and again after it.
- **The confirmed-or-counted core batch** (FR-003): after the driver's `pending_data_size` reads
  zero, a fresh PING confirms the batch; unanswered or undrained, the published events are counted
  in `failed`, announced once, and `emit` returns rather than raises.
- **The bounded close and flush** (FR-004): `close()` no longer calls `drain()`; it runs a round
  trip, `client.close()` as a task cancelled on expiry, then the loop stop and join, under one
  deadline (`drain_timeout`, else `DEFAULT_CLOSE_TIMEOUT` = 1.0 s). `flush()` goes through the probe.
- **The single driver-timeout rule**: every timeout the sink hands the driver for a wait the driver
  reports by raising is `_driver_timeout(...)`, ending `DRIVER_DEADLINE_MARGIN` (0.05 s) inside the
  sink's own wait, or `PROBE_FLUSH_TIMEOUT`, which deliberately outlives it; every latch ages from
  the start of the call whose budget the stall spent. A test holds the first half on the source from a pinned roster.
- **Tests**: `tests/test_sinks_nats_liveness.py` (fakes for FR-001 to FR-004 and the fork child,
  79 tests collected at `cc54227`) and `tests/test_sinks_nats_matrix.py`, the wait-and-latch matrix
  gated by mutation (96 tests collected at `cc54227`); `tests/integration/test_nats.py` gains the
  stall, restart, late-PONG, close-bound (core and JetStream) and landing tests.
- Deviations the diff reviews forced, one line each: the abandoned-publish latch and counting an
  injected client's abandoned publish (frame 2, finding 1); a probe left behind by an undrained
  confirmation, aged from the start of the call whose budget it spent (frames 2 and 3); `close()` skipping its
  round trip while a publish is abandoned (frame 3, finding 3); an injected `flush()` run as an
  uncancelled task under `publish_timeout`; the timing rule and the matrix (frame 3, AC-9).

## What was measured

From the spec, at `71e42dd` against `nats-py` 2.16.0 under Docker: under `docker pause` 75 of 75
emits were accepted over 148 s with nothing counted (M1); after `stop`/`start` the old sink still
refused 40 s later (M2); a cancelled PONG wait killed the driver's reader on a late PONG (M3). The
prototype (M4) put a one-event emit at 0.47 ms against 0.04 ms and 500 events at 1.26 ms against
0.90 ms, resumed 0.50 s after a restart, refused each paused emit in 1.00 s, and closed against a
paused server in 2.0 s where the old sink took 10.0 s. Five of five unconfirmed publishes landed
after `unpause` (M6); `wait_for(drain(), 2.0)` took 12.00 s in JetStream mode (M7); `close()` with
20 MB pending was still running at 8 s (M8); a flush while reconnecting returned in 0.000 s with no
PONG (M9); a flush from a second loop returned only at its 3.001 s timeout in three of three (M10).
During the build, against a real server at `publish_timeout=1.0`: without the timing rule 0 of 4
ack-starved emits raised, where all did at `fda134a`, before this spec; a latch dated at the
deadline made the next caller wait 1.002 s. The integration tests assert a stall refused within `1.0 + CALL_SLACK + 1.0` s and a
close within `DEFAULT_CLOSE_TIMEOUT + LOOP_JOIN_TIMEOUT + 1.0` s.

## What changed from earlier specs?

- SPEC-047: `close()` no longer calls `drain()`; `drain_timeout` is still forwarded to
  `nats.connect` but now sets the sink's own close deadline. Its two `architecture.md` §12 items
  (the `is_connected` window, the unreachable `close()` bound) are Resolved.
- SPEC-028's serialization is kept, now across a cross-thread submit;
  `tests/test_sink_concurrency.py`'s NATS re-entry test is rewritten for it.
- SPEC-036 `flush()` is bounded by `publish_timeout`; the driver's `FlushTimeoutError` no longer escapes.
- The sink-delivery fence "a sink that delivered nothing raises" is refined for the unconfirmed
  core batch; a new sink-lifecycle decision records the probe and the timing rule. Invariants 2, 4,
  7 and 12 carry recorded exceptions.

## Residuals

Recorded in `architecture.md` §12 as open items: a forked child's `NATSSink` refuses rather than
reconnects; a driver reader that dies while the status stays CONNECTED is invisible to the probe;
a sink whose driver has spent its reconnect budget is closed for good. The `Nats-Msg-Id` item now
also names the JetStream no-ack asymmetry. §13 gains the constraint that an injected client's
liveness is the caller's (M10).

## Verification

`spec-lint.sh`, `docs-lint.sh` and `docstring-lint.py` exit 0 at the completion commit. The four
NATS unit-test files (`tests/test_sinks_nats*.py`, `tests/test_sink_concurrency.py`) passed 276
with 4 skipped in three of four parallel runs at `cc54227`; the fourth failed
`test_an_undrained_buffer_leaves_a_probe_for_the_latch_to_age` on its message assertion (the next
call reused the probe instead of meeting the latch), and it passed five of five serially. Not run
here: `tests/integration` and the full suite.
