# Spec: Pre-1.0 Corrections

**ID:** SPEC-034  
**Status:** Draft  
**Last Updated:** 2026-08-07  
**Depends On:** SPEC-026, SPEC-030, SPEC-031, SPEC-033

## Overview

`1.0.0` puts the public API under semantic versioning, which makes two classes of problem
urgent that were not urgent before. First, an inconsistency in a shipped signature stops being
a wart and becomes a promise: after the freeze it costs a **major version** to fix. Second, a
claim the README is about to make on the library's behalf has to be true, and a review of that
README found one that is not.

Both were found by independent review while preparing the release, not by the audit arc, and
neither is large. They are grouped for the reason SPEC-031 grouped its residue: five one-FR
specs would be worse paperwork than one spec with five FRs.

**FR-005 is the substantial one and should be read first.** The rest are mechanical signature
changes. FR-005 is the last instance of the SPEC-026 shape — a path that loses events while
`flush()` returns `True` and `health()` reads all zeros — surviving on the one path with no
worker to report through. **Measured on `734a9b2`**, five `info()` calls with no active span
against a sink whose `emit` raises:

```
flush() after 5 orphan-path losses -> True
health() -> queued=0 dropped=0 failed_batches=0 stopped_reason=None sink=None
            retired=False submitted_after_shutdown=0 incomplete_swaps=0 closing_sinks=0
```

Every event lost, both signals clean, and only stderr says anything. The README the release
ships with says "silence is not success anywhere" and tells a serverless reader to check
exactly the fields that read zero here.

## Scope

### In Scope

- Making `SQSSink`'s injected client keyword-only, as all nine siblings already are.
- Renaming `SentrySink(sdk=)` to `client=`.
- Aligning `KinesisSink(partition_key_field=)` with `KafkaSink(key_field=)`.
- Making `RotatingFileSink(backup_count=0)` safe, or loudly documented.
- **FR-005:** reporting loss on the synchronous (no-worker) path through `flush()` and
  `health()`.

### Out of Scope

- **Renaming `producer=` or `connection=` to `client=`.** A survey of every sink shows the
  injection kwarg follows the **service's own vocabulary**, consistently: `client` (10 sinks),
  `producer` (Kafka, Azure Event Hubs — both genuinely producers), `connection` (Postgres,
  RabbitMQ, SQLite — all connections), `logger` (`LoggingSink`). That is a convention, not
  drift. `sdk` is the only name with no family, which is why FR-002 moves it and nothing else.
- **Any new `Health` field.** SPEC-030 settled that vocabulary, SPEC-031 and SPEC-033 both
  declined to extend it, and the README now promises `1.x` only ever *appends* to `Health`.
  FR-005 must fit the existing fields or it is out of scope for this release.
- **Making the orphan path non-blocking.** It emits on the caller's thread by design (arch §9),
  and `architecture.md` §13 already records the cost. FR-005 makes its loss *visible*; it does
  not move the work off that thread.
- **A `CHANGELOG` file.** Release notes live in `docs/release-notes/` and on the GitHub Release.
- **The remaining README corrections** (a stale `error` schema row, an incomplete `HTTPSink`
  signature, an over-broad claim about queue-sink retries, `batch_size`/`flush_interval` being
  named as tunables when no public API exposes them). Those are documentation-only and ship in
  the README PR, not here.

---

## Functional Requirements

### FR-001: `SQSSink`'s injected client is keyword-only

#### Description:

`SQSSink(queue_url, client=None, *, ...)` accepts a positional client. Every other injectable
sink — `SNSSink`, `KinesisSink`, `FirehoseSink`, `MongoDBSink`, `RedisStreamsSink`,
`RedisListSink`, `ClickHouseSink`, `NATSSink`, `GooglePubSubSink` — is keyword-only. `SQSSink`
is the headline sink of the documented SQS → ELK path, so freezing the positional form makes
the one exception the most visible one.

This is a **breaking change**, taken now precisely because `1.0.0` has not shipped. Nothing in
the repository or the README uses the positional form.

#### Acceptance Criteria:

- [ ] AC-1: `SQSSink(url, client=fake)` works; `SQSSink(url, fake)` raises `TypeError`.
- [ ] AC-2: A test asserts every injectable sink takes its client keyword-only, derived from the
      sink roster rather than a hand-written list — the roster lesson SPEC-027, SPEC-028 and
      SPEC-032 each recorded.
- [ ] AC-3: No call site in `src/`, `tests/` or `README.md` passes it positionally.

### FR-002: `SentrySink` injects through `client=`

#### Description:

`SentrySink(dsn=None, *, sdk=None, ...)` is the only sink whose injection kwarg has no family
(see Out of Scope for the survey). `sdk` also describes the *module* rather than the thing being
injected, which is what the other names get right.

#### Acceptance Criteria:

- [ ] AC-1: `SentrySink(client=fake_sdk)` works and `sdk=` raises `TypeError` — no alias. An
      alias kept for compatibility would have to live for the whole of `1.x`, which is the cost
      this spec exists to avoid paying.
- [ ] AC-2: The parameter is covered by FR-001 AC-2's roster test.
- [ ] AC-3: `README.md`'s injected-client list names it.

### FR-003: `KinesisSink` and `KafkaSink` name the key field alike

#### Description:

`KinesisSink(partition_key_field="trace_id")` and `KafkaSink(key_field="trace_id")` are the same
concept — which event field supplies the ordering/partitioning key — under two names.

**This FR is the weakest of the five and its reasoning is recorded so a reviewer can overturn
it.** The evidence cuts both ways: each name is its own service's vocabulary (AWS calls it
`PartitionKey`, Kafka calls it `key`), which is exactly the convention FR-002's Out of Scope
bullet *defends* for `producer`/`connection`. Against that: those two describe an injected
object the caller already holds and names that way, whereas this is a log-foundry concept —
which of *our* event's fields to read — and SQS's equivalent is a third name again
(`message_group_id`). The decision is to align on `key_field`, the shorter and less
service-specific of the two, and to accept that a Kinesis reader must map it to `PartitionKey`
once.

#### Acceptance Criteria:

- [ ] AC-1: `KinesisSink(stream, key_field="tenant")` works; `partition_key_field=` raises
      `TypeError`.
- [ ] AC-2: The attribute is renamed with the parameter, so `sink.key_field` reads on both.
- [ ] AC-3: `README.md`'s Kinesis row and the sink's docstring use the new name, and the
      docstring says which service field it becomes.

### FR-004: `RotatingFileSink` never silently discards on rotation

#### Description:

With `backup_count=0` — the **default** — `_rotate` calls `os.remove(self._path)`
(`sinks/file.py:331`), so every event written since the last rotation is destroyed. It is
uncounted, absent from `losses()`, and invisible to `health()`. Configure
`RotatingFileSink("app.log", max_bytes=10_000_000)` and the sink silently throws away 10 MB of
logs at each rollover.

This mirrors the stdlib `RotatingFileHandler`, which is the argument for keeping it — and it
contradicts everything SPEC-026 established about a sink that loses data reporting it, which is
the argument that wins. The default changes to `backup_count=1`, so the out-of-the-box
configuration keeps one generation. `backup_count=0` stays available and is what a caller who
genuinely wants truncation asks for — but it counts what it destroys.

#### Acceptance Criteria:

- [ ] AC-1: `RotatingFileSink(path, max_bytes=N)` with no `backup_count` retains one previous
      generation; a rotation leaves `path` and `path.1`.
- [ ] AC-2: `backup_count=0` still truncates, and each rotation adds the discarded events to
      `losses().dropped`, so `health().sink` reports them.
- [ ] AC-3: The count is of **events**, not bytes, since `losses()` counts events everywhere
      else; the sink tracks writes since the last rotation.
- [ ] AC-4: The docstring and the README row state that `backup_count=0` discards, and what it
      costs.
- [ ] AC-5: A test writes past `max_bytes` with `backup_count=0` and asserts the loss is
      counted, not merely that the file is smaller.

### FR-005: The synchronous path reports its loss

#### Description:

A level call with no active span emits on the caller's thread, with no worker between them
(arch §9, SPEC-002). SPEC-025 FR-003 then wraps that emit in a guard so a broken destination
cannot fail the caller — correctly — but nothing records that the event was lost. `flush()`
and `health()` both describe the worker, and on this path there is none, so both report
success over total loss.

This is the SPEC-026 shape exactly: *a path that absorbs a total failure is a path the caller
believes.* SPEC-026 fixed it inside the sinks; SPEC-033 fixed the lifecycle around them; this
is the last place it survives, and it is the one the README's serverless recipe points at.

**It must fit the existing `Health` fields** (see Out of Scope). Two already mean the right
thing and the choice between them is the design decision here:

- `failed_batches` — "batches abandoned after the retry budget was spent". An orphan emit *is*
  a one-event batch that was abandoned; there is no retry, but there is no retry precisely
  because there is no worker to run one.
- `sink` (`SinkLosses`) — the sink's own absorbed loss. Wrong for this: the sink did not absorb
  anything, it **raised**, which is what SPEC-026 requires of it. The loss was absorbed by
  `api._log`.

`failed_batches` is the correct home. The counter must live outside the worker, since there may
be no worker, and be surfaced by `decorator._worker_health()` on both branches — added to the
worker's own count where one exists, so a mixed process reports one total rather than two
partial ones.

`flush()` must also stop lying. It returns `True` when there is no worker (`_flush_worker`), on
the reasoning that a process that never logged has nothing to drain — true when written,
false once a synchronous path can lose events. It should report `False` when anything was lost
on this path since the process started, matching SPEC-021's rule that `flush()` reports
delivery rather than merely that a drain ran.

#### Acceptance Criteria:

- [ ] AC-1: Five `info()` calls with no span against a sink whose `emit` raises leave
      `health().failed_batches == 5`.
- [ ] AC-2: `flush()` returns `False` in that process, and `True` in one where the same calls
      succeeded.
- [ ] AC-3: A mixed process — orphan losses **and** worker losses — reports the sum in one
      `failed_batches`, not two separate counts, and a test asserts the sum rather than either
      part.
- [ ] AC-4: The stderr line SPEC-025 already writes is unchanged; this adds a counter, not a
      second announcement.
- [ ] AC-5: `health()` still creates no worker (SPEC-031 FR-006, SPEC-033), and the counter is
      readable with `_worker` unset.
- [ ] AC-6: No new `Health` field, and `Health._fields` is unchanged.
- [ ] AC-7: A successful orphan emit moves nothing.
- [ ] AC-8: A sink that raises on a *console echo* rather than the emit does not count — the
      event reached the sink, and SPEC-025 treats the echo as separate.
- [ ] AC-9: Each assertion is mutation-tested.

---

## Data Model

```python
# src/log_foundry/decorator.py — module state
_orphan_failed = 0          # NEW: events lost on the synchronous path (FR-005).
                            #   Lives outside Worker because there may be no worker; added to
                            #   the worker's own `failed_batches` when one exists, so a mixed
                            #   process reports one total.
```

No change to `Health`, `SinkLosses`, or any public signature except the four in FR-001..FR-004.

## API / Interface Contract

```python
SQSSink(queue_url: str, *, client: Any = None, ...)              # was: client positional
SentrySink(dsn: str | None = None, *, client: Any = None, ...)   # was: sdk=
KinesisSink(stream_name: str, *, key_field: str = "trace_id", ...)   # was: partition_key_field=
RotatingFileSink(path: str, *, backup_count: int = 1, ...)       # was: 0

# unchanged signatures, new behaviour
flush(timeout: float | None = 5.0) -> bool    # False when the synchronous path lost events
health() -> Health                            # failed_batches includes synchronous-path loss
```

## Implementation Phases

### Phase 1: The signature freeze (FR-001..FR-003)

- Make `SQSSink`'s client keyword-only; rename `SentrySink.sdk` → `client`; rename
  `KinesisSink.partition_key_field` → `key_field` (parameter and attribute).
- Add the roster test that derives the injectable-sink list rather than hand-writing it.
- Update every call site, docstring and README row.

### Phase 2: `RotatingFileSink` (FR-004)

- Default `backup_count=1`; count discarded events into `losses().dropped` when it is 0.
- Docstring and README row.

### Phase 3: Synchronous-path loss (FR-005)

- `_orphan_failed` in `decorator`, incremented from `api._log`'s guard; summed in
  `_worker_health()` on both branches; `_flush_worker` reports it.
- Tests for FR-005, mutation-tested, with the mixed-process case first since it is the one that
  can double-count or under-count.
