# Completed Spec — SPEC-017: Payload and Failure Safety

## What was completed?

The library promised that logging never breaks the calling application and that a broken
destination degrades logging and nothing more. It broke that promise in three reachable ways.
An event is now **safe by construction**: coerced and size-bounded once at assembly, so every
sink sees a payload that JSON accepts and that has a ceiling.

- **`sanitize.py`** (new) — coercion + the four ceilings, total by contract. Doing this once at
  assembly rather than per sink means all 40-plus bare `json.dumps` call sites in `sinks/` became
  correct *by consequence*; none of them changed. It is also one pass per event rather than one
  per destination (which matters under `MultiSink`), and it extends the guarantee to the sinks
  that never call `json.dumps` at all — `postgres`, `mongo`, `sqlite` (FR-001, FR-002).
- **The orphan-path crash is gone.** `api._log` emits synchronously on the caller's own thread
  when no span is active, so `info("m", when=datetime.now())` raised `TypeError` into user code.
  Inside a span the same field destroyed the whole flattened batch, taking co-batched events from
  unrelated spans with it. Both fixed, both tested (FR-001).
- **`error.message` and `error.module`** — the message previously existed only inside the
  free-text `stack` blob, so filtering on it meant substring-matching a traceback: the exact
  anti-pattern the structured schema exists to avoid. `type` and `stack` are unchanged
  byte-for-byte for an untruncated exception, guarded by a test (FR-003).
- **`MultiSink` re-raises when *every* child fails** so the worker's retry engages. Partial
  success stays isolated — retrying there would duplicate into the healthy children. Total
  failure delivered nothing, so it has no duplicates to create (FR-004).
- **`log_foundry.health()`** — the worker's `dropped`/`failed_batches` lived on a module-private
  global with no accessor, while the README told users to read `worker.dropped`. Queue overflow
  was also the only failure path writing nothing to stderr; it now warns on the first drop and
  every thousandth (FR-005).
- **Four configurable ceilings** with validation that runs before any assignment, so a rejected
  call cannot half-apply (FR-006).

## What changed from earlier specs?

- **The event schema gains two optional keys.** `truncated: true` appears only when a ceiling
  fired (absent, never `false`); `error` gains `message` and `module`. `error.type` was
  deliberately *not* qualified in place — that would break every existing dashboard.
- **`message` and `function` are now bounded.** They are base fields, but both are
  caller-supplied — `function` via `@trace(name=...)` and, on the orphan path, from the message
  itself. Leaving either out kept `info(huge_string)` unbounded.
- **A behaviour change for `MultiSink` users:** an all-children-down fan-out now raises out of
  `emit`, which the worker catches and retries. `failed` therefore counts *calls*, not batches —
  *n* children × `max_retries + 1` attempts. That is the visible cost of the loss no longer being
  silent. One existing test (`test_multi_logs_child_failure_to_stderr`) used a single failing
  child and became an all-failed case; it asserts the same stderr line inside `pytest.raises`.
- **A silent stored-type change for the non-JSON sinks.** A `datetime` field that `MongoDBSink`
  previously stored as a BSON date is now stored as an ISO-8601 string; the same applies to
  `postgres`, `sqlite` and `clickhouse`. This follows from coercing at assembly and is the price
  of the guarantee holding for those sinks at all.
- **SPEC-015's backfill is exempt from `max_keys`.** `backfill_baggage` merges into an
  already-capped mapping; re-capping there would drop the correlation keys SPEC-015 shipped to
  add.

## Notes for the next spec

*Reconciled by SPEC-021 — each note below is now marked fixed, settled, or recorded as a
constraint. The notes are kept as written so the history stays legible.*

- **The ceilings bound per *value*, not per *event*.** A flat 256-key × 8 KB mapping is a legal
  ~2 MB event, still past SQS's 256 KB. Byte-based bounds were explicitly out of scope; if event
  size ever needs a hard cap, that is a new spec, not a tweak.
  → **Constraint** (SPEC-021). Still true, and now stated in `architecture.md` §13 with the
  reason it is acceptable: a sink with a hard limit drops the event and counts
  `dropped_oversized`, so the loss is visible rather than silent. A per-event ceiling was
  deferred again, deliberately — it is a feature, not a cleanup.
- ~~**Non-`str` scalars are still unbounded** — `info("m", n=10**100000)` emits a ~100 KB number.
  Spec-consistent by the letter of the coercion table, not by the intent of "no value is
  unbounded".~~
  → **Fixed by SPEC-020.** An integer too long to render is replaced by `<int: ~N digits>`;
  SPEC-021 then made the ceiling count the minus sign. `float` and `bool` are bounded by their
  own representations. This note was *false* from SPEC-020 onward and is struck rather than
  deleted, because it was the note that motivated that spec.
- **`TransformSink` runs *after* sanitization**, so a transform returning a `datetime` re-poisons
  the event. It is the designated redaction seam (SPEC-006), so this is a documented sharp edge
  rather than a defect.
  → **Settled** (SPEC-021). Working as designed: a redaction seam that ran *before* sanitization
  could not see the values it is there to redact. The sharp edge is documented on
  `TransformSink` itself — a transform owns the JSON-safety of what it returns.
- ~~**A `BaseException` from a child sink** still escapes `MultiSink.emit` and `Worker._emit`
  (both catch `Exception`), killing the worker thread with no counter moved. Pre-existing, but
  `health()` is now the advertised loss detector and has no liveness signal — the loss surfaces
  only once the queue fills and `dropped` climbs.~~
  → **Half fixed by SPEC-019, half settled.** The missing liveness signal is gone:
  `health().stopped_reason` names the exception type that ended the drain thread, and one stderr
  line reports it, so the loss no longer waits for `dropped` to climb. What remains true — and is
  now a stated constraint, not an open item — is that a child's `BaseException` still *ends* the
  worker. Both catches stay at `Exception` on purpose; widening them would swallow a
  `KeyboardInterrupt` raised inside a child sink and continue to the next one. See
  `architecture.md` §13.
- **The fresh-context review caught a regression this spec introduced**: the FR-005 overflow
  warning was an unguarded `sys.stderr.write` on the caller's thread, which reintroduced exactly
  the raise-into-the-caller failure FR-001 removes. Worth remembering that a diagnostic added on
  a hot path is itself a hot-path change.
  → **Settled** (SPEC-021): a retrospective lesson, never an open item. It has since repeated —
  SPEC-020's review found the same shape in `key()`, and SPEC-021's found it again in the sign
  test's `<` on an `int` subclass.
