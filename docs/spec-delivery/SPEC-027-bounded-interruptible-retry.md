# Completed Spec — SPEC-027: Bounded, Interruptible Retry

## What was completed?

Every sink that retries did so by sleeping the one thread that delivers anything. The worker owns a
single drain thread by design (arch §9), so a sink's backoff was never a local decision — it was a
global pause on log delivery, and it was held across `shutdown()`, which joins that thread and runs
from `atexit`.

- **`Retry-After` is bounded and sign-checked** (FR-001). `HTTPSink` passed a server-supplied delay
  straight to `time.sleep` with no ceiling: a measured `Retry-After: 8` with the default
  `max_retries=3` blocked `shutdown()` for 22.01 seconds, `86400` would have stalled logging for a
  day, and a negative value made `time.sleep` raise — absorbed inside a span, but reaching the
  caller on the orphan path. `clamp_server_delay` now bounds it to `max_retry_after` (default 30 s,
  a constructor argument) and rejects anything non-positive or non-finite in favour of the sink's
  own backoff.
- **Every wait is interruptible** (FR-002). `sinks/_retry.py`'s `wait(delay, stop)` waits on the
  worker's shutdown `Event` rather than sleeping. The worker pushes that event onto the sink,
  probed with `hasattr` — the optional-protocol shape SPEC-026 used for `losses()` — so `sinks`
  still never imports `worker`, and a sink used standalone backs off exactly as before.
- **`SQSSink` backs off** (FR-003). It was alone among the retrying sinks in re-sending
  immediately, while its own docstring named throttling as the retryable case — precisely the
  failure an instant retry makes worse.
- **`shutdown()` cannot block forever** (FR-004). It takes a `timeout` (default 30 s; `None` still
  waits indefinitely). On expiry it returns having stopped what it could and records
  `stopped_reason="ShutdownTimeout"`.
- **The worst case is written down** (FR-005): each retrying sink's class docstring, one README
  paragraph beside the `flush()` guidance, and a note in `architecture.md` §9.

**The wrapper forward, which the plan did not anticipate.** The worker sets `stop_signal` on the
*configured* sink — and `SyslogSink`, `LogstashSink`, `SentrySink`, `MultiSink`, `FilteringSink`
and `TransformSink` are not where the waiting happens. Set on a wrapper the signal reached nothing
and the backoff one level down stayed uninterruptible: the defect moved rather than fixed. Each now
forwards through a property, and a child that refuses is absorbed so its siblings still get theirs.
A test constructing one of every retrying sink is what found it.

**Deliberate deviations.** (1) `Retry-After: 0` is rejected rather than honoured as "retry now" — a
rate-limiting destination saying "wait zero seconds" is far more likely truncated than meant.
(2) An expired shutdown does **not** close the sink, which the spec asked for and is worth
restating: the drain thread may still be inside `emit`, so closing the transport under it turns a
slow shutdown into a corrupt one. (3) `stopped_reason` is not overwritten if one is already set — a
thread that died on `SystemExit` is worse news than the timeout that followed it.

## What changed from earlier specs?

- **`log_foundry.shutdown()` gained a `timeout` argument**, defaulting to 30 s where it previously
  waited indefinitely. A process that relied on the unbounded wait passes `timeout=None`.
- **`Health.stopped_reason` has a new value**, `"ShutdownTimeout"`. It extends SPEC-019's
  vocabulary rather than adding a field, which is what a reason string was chosen for: an expired
  shutdown and a dead thread mean the same thing to a reader.
- **`HTTPSink` gained `max_retry_after`**, inherited by every platform subclass.
- **Sink test fixtures patch `log_foundry.sinks._retry.time.sleep`**, not each sink's own `time`.
  Nine test modules moved.
- **`sinks/_retry.py` imports nothing from its own package**, joining `_diag` and `sanitize` as a
  leaf helper.

## Verification

Local: 925 tests pass (48 new), `ruff` and `mypy --strict` clean over 50 source files, `spec-lint`
clean. CI green on 3.12 and 3.13.

Every change was mutation-tested individually: removing the clamp, the sign/NaN check, the
interruptible branch, the unusable-delay guard, the worker's wiring, each wrapper's forward,
SQS's backoff, the bounded join, the do-not-overwrite guard, and the leave-the-sink-open branch
each fail at least one test. Two survivors were fixed rather than accepted — a `stop_signal` check
that scanned module source (and so passed against a sink whose `__init__` no longer set it) became
a check on constructed instances, and an `atexit` test asserting only a signature default became
one that asserts the value is actually forwarded.

One testing decision is worth recording. The interruptibility tests join a worker thread with a
timeout rather than asserting on elapsed time: against an *uninterruptible* wait the latter blocks
for the full delay, which reads in CI as a hung run rather than a regression. The mutation run
proved the point — the first attempt at a `time.sleep` mutant hung the harness for two minutes
before the tests were rewritten this way.
