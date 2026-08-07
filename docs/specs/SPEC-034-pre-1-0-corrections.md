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
neither is large. They are grouped for the reason SPEC-031 grouped its residue: four one-FR
specs would be worse paperwork than one spec with four FRs.

**FR-004 is the substantial one and should be read first.** The rest are mechanical signature
and default changes. FR-004 is the last instance of the SPEC-026 shape — a path that loses events while
`flush()` returns `True` and `health()` reads all zeros — surviving on the one path with no
worker to report through. **Measured on `734a9b2`**, five `info()` calls with no active span
against a sink whose `emit` raises:

```
health() -> queued=0 dropped=0 failed_batches=0 stopped_reason=None sink=None
            retired=False submitted_after_shutdown=0 incomplete_swaps=0 closing_sinks=0
```

Every event lost, every field clean, and only stderr says anything. (`flush()` also returns
`True` here, and FR-004 explains why that is correct and must stay.) The README the release
ships with says "silence is not success anywhere" and tells a serverless reader to check
exactly the fields that read zero here.

## Scope

### In Scope

- Making `SQSSink`'s injected client keyword-only, which is the rule every other sink follows.
- Renaming `SentrySink(sdk=)` to `client=`.
- Changing `RotatingFileSink`'s default to keep one generation, and documenting what it costs.
- **FR-004:** reporting loss on the synchronous (no-worker) path, through `health()`.

### Out of Scope

- **Renaming `producer=` or `connection=` to `client=`.** A survey of every sink shows the
  injection kwarg follows the **service's own vocabulary**, consistently: `client` (10 sinks),
  `producer` (Kafka, Azure Event Hubs — both genuinely producers), `connection` (Postgres,
  RabbitMQ, SQLite — all connections), `logger` (`LoggingSink`). That is a convention, not
  drift. `sdk` is the only name with no family, which is why FR-002 moves it and nothing else.
- **Aligning `KinesisSink(partition_key_field=)` with `KafkaSink(key_field=)`.** Cut from this
  spec after review. It is a breaking change that does not buy the consistency it exists for:
  SQS's equivalent is a third name again (`message_group_id`) and this would not touch it, so
  two names for "which event field orders this record" survive either way. The service-vocabulary
  convention above defends both names as they stand, and a genuine alignment across all three
  sinks is a `1.x` discussion with a real argument behind it, not a pre-freeze correction.
- **Making `flush()` report prior synchronous loss.** Cut after review — see FR-004, which
  records why it is the wrong signal and which shipped documents forbid it.
- **Counting events discarded by `RotatingFileSink`'s rotation.** Cut after review — see FR-003.
- **Making the orphan path non-blocking.** It emits on the caller's thread by design (arch §9),
  and `architecture.md` §13 already records the cost. FR-004 makes its loss *visible*; it does
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

`SQSSink(queue_url, client=None, *, ...)` accepts a positional client.

**The first draft of this FR claimed "every other injectable sink is keyword-only", which is
false** — `LoggingSink(logger)`, `StdoutSink(stream)` and `StderrSink(stream)` all take theirs
positionally, and the spec's own Out-of-Scope survey names `logger` as an injection kwarg. The
rule those three follow, and `SQSSink` breaks, is narrower and is what this FR actually enforces:

> **A sink's positional parameters identify its destination. An injected client or transport
> object is keyword-only.**

Measured across every sink class, five take more than one positional parameter —
`HoneycombSink(api_key, dataset)`, `SplunkHECSink(url, token)`, `SyslogSink(host, port)`,
`TransformSink(inner, fn)` and `SQSSink(queue_url, client)`. In the first four, both parameters
*are* the destination (or, for the wrapper, its two subjects). `SQSSink` is the only one whose
second positional is an injected transport sitting beside an identifier that already names the
destination. `StdoutSink(stream)` and `LoggingSink(logger)` are not violations under this rule:
there the stream or logger **is** the destination identity — there is no other identifier — which
is exactly why they are excluded rather than swept in.

`SQSSink` is also the headline sink of the documented SQS → ELK path, so freezing its positional
form makes the sole exception the most visible one.

This is a **breaking change**, taken now precisely because `1.0.0` has not shipped. Nothing in
the repository or the README uses the positional form.

#### Acceptance Criteria:

- [ ] AC-1: `SQSSink(url, client=fake)` works; `SQSSink(url, fake)` raises `TypeError`.
- [ ] AC-2: A test asserts that **no sink class anywhere in `sinks/` takes a parameter named
      `client`, `sdk`, `producer` or `connection` positionally**. The rule is derived from
      parameter *names* across the whole package, not from a hand-written sink list, so a sink
      added later is covered without anyone remembering to add it — the roster lesson SPEC-027,
      SPEC-028 and SPEC-032 each recorded. Those four names are the injected-transport family;
      `stream` and `logger` are deliberately not in it, per the rule above, and the test's
      docstring says why.
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

### FR-003: `RotatingFileSink`'s default keeps a generation, and says what it costs

#### Description:

With `backup_count=0` — the **default** — `_rotate` calls `os.remove(self._path)`
(`sinks/file.py:331`), so every event written since the last rotation is destroyed. Configure
`RotatingFileSink("app.log", max_bytes=10_000_000)` and the sink silently throws away 10 MB of
logs at each rollover. The default becomes `backup_count=1`.

**The first draft also required counting the discarded events into `losses().dropped`, and that
is dropped from this FR — it was wrong twice over.** It would not have delivered what the FR
claimed: with `backup_count=1` the *second* and every later rotation still `os.remove`s a full
generation (`file.py:326-328`), so the new default would have gone on discarding uncounted while
the FR's title said otherwise. And `dropped` is defined as "an event the sink discarded **before
attempting delivery**" (`sinks/base.py:31-35`), which a rotated-out event is not — it was
written and flushed to disk. Neither `dropped` nor `failed` fits "delivered, then aged out".

The resolution is to name what pruning actually is. `RotatingFileSink` is a **bounded ring
buffer**: retention is the configuration, and discarding the oldest generation is that
configuration working, not loss the sink absorbed. That is why no count is added — for *any*
`backup_count`, including 0. What was genuinely wrong is that the default made the ring one
generation deep, so the "buffer" held nothing across a rollover, and that the docs never said
so.

#### Acceptance Criteria:

- [ ] AC-1: `RotatingFileSink(path, max_bytes=N)` with no `backup_count` retains one previous
      generation; a rotation leaves `path` and `path.1`.
- [ ] AC-2: `backup_count=0` still truncates on rotation, unchanged.
- [ ] AC-3: No counter is added and `losses()` is not implemented on this sink — the docstring
      states that rotation is retention rather than loss, so a later reader does not read its
      absence as an oversight (the reason SPEC-028 requires every sink to record its decision).
- [ ] AC-4: The class docstring and the README row state that `backup_count=0` discards
      everything since the last rotation, **and** that the new default costs up to
      2 × `max_bytes` on disk — a behaviour change for anyone who chose `max_bytes` to bound
      disk usage.
- [ ] AC-5: A test writes past `max_bytes` twice with the default and asserts both generations
      exist, so the second-rotation case the first draft missed is pinned.

### FR-004: The synchronous path reports its loss

#### Description:

A level call with no active span emits on the caller's thread, with no worker between them
(arch §9, SPEC-002). SPEC-025 FR-003 then wraps that emit in a guard so a broken destination
cannot fail the caller — correctly — but nothing records that the event was lost. `health()`
describes the worker, and on this path there is none, so it reports success over total loss.

This is the SPEC-026 shape exactly: *a path that absorbs a total failure is a path the caller
believes.* SPEC-026 fixed it inside the sinks; SPEC-033 fixed the lifecycle around them; this is
the last place it survives, and it is the one the README's serverless recipe points at.

**Two things the first draft got wrong, both corrected here.**

*It folded the count into `failed_batches`.* That field is defined in four places as batches
abandoned **after the retry budget was spent** (`worker.py:78`, `README.md:796`, `:844`,
`architecture.md:462`), and delivery continues afterwards. An orphan loss has no retry budget,
no batch — it is one event — and no worker that continues. The sum would also mix units, per
*batch* against per *event*. SPEC-026 faced this exact choice and nested rather than flattened,
"because one number would make the remedies indistinguishable". So this appends a **new**
`Health` field. The first draft forbade that on the grounds that `1.x` only appends to `Health`
— which has it backwards: appending is precisely what is *permitted*, and is what SPEC-026 did.

*It made `flush()` return `False` for any prior synchronous loss.* SPEC-021 rejected that in
those words: "counting it here would make every later empty flush report a failure it did not
incur" (`worker.py:414-421`), with `_nothing_lost_since` "deliberately not a running 'has
anything ever failed' flag" (`:1043-1051`) and the public docstring stating "a batch lost before
the call belongs to `health`" (`__init__.py:44`). In the very recipe this FR cites, one
transient failure would make `flush()` report undelivered logs for the life of a warm container.
A window-based alternative does not exist either: an orphan emit is synchronous and complete
before `flush()` is entered, so any honest window is empty. **`flush()` is left alone.**
`health()` is the documented channel for cumulative loss, and once the new field exists the
recipe reads it there.

#### Acceptance Criteria:

- [ ] AC-1: Five `info()` calls with no span against a sink whose `emit` raises leave
      `health().unbuffered_failed == 5`.
- [ ] AC-2: `flush()` is unchanged — it still returns `True` in that process, and a test pins
      that, with a comment naming SPEC-021 so a later reader does not "fix" it.
- [ ] AC-3: The new field is **appended** to `Health`, so every existing field keeps its index;
      `Health._fields[:9]` is unchanged.
- [ ] AC-4: A mixed process reports orphan losses in `unbuffered_failed` and worker losses in
      `failed_batches`, separately — a test asserts both, and that neither absorbs the other.
- [ ] AC-5: The counter is incremented under its **own** lock, not `_worker_lock`: the orphan
      path runs on arbitrary application threads, and `_worker_lock` is held across sink handoffs
      by `_swap_sink`/`_close_orphan_sink` (SPEC-028's dedicated-counter-lock rule).
- [ ] AC-6: A loss anywhere inside the orphan guard counts — a sink that fails to *construct* or
      an event that fails to build, not only a failing `emit` — since `api._log` wraps all three
      and the event is lost either way. A test covers the construction failure specifically,
      because an increment placed after `sink.emit` would pass AC-1 and fail this.
- [ ] AC-7: `health()` still creates no worker, and the field reads correctly with `_worker`
      unset **and** with a worker present (SPEC-031 FR-006, SPEC-033).
- [ ] AC-8: A successful orphan emit moves nothing.
- [ ] AC-9: The stderr line SPEC-025 already writes is unchanged; this adds a counter, not a
      second announcement.
- [ ] AC-10: `README.md`'s alert idiom and the `Health` table gain the new field as their own
      term, and `__init__.py`'s `health()` docstring names it.
- [ ] AC-11: Each assertion is mutation-tested.

---

## Data Model

```python
# src/log_foundry/worker.py
class Health(NamedTuple):
    ...                          # the nine existing fields, positions unchanged
    unbuffered_failed: int = 0   # NEW, appended (FR-004): events lost on the synchronous
                                 #   no-span path, where there is no worker and no retry.
                                 #   Separate from `failed_batches`, which counts *batches* a
                                 #   worker abandoned after spending its retry budget.

# src/log_foundry/decorator.py — module state
_unbuffered_failed = 0           # the counter behind it; lives outside Worker because there may
_unbuffered_lock = threading.Lock()   # be no worker. Its own lock, never `_worker_lock`
                                 #   (SPEC-028), which is held across sink handoffs.
```

No change to `SinkLosses`, to `flush()`, or to any public signature except the two in
FR-001..FR-002 and `RotatingFileSink`'s default.

## API / Interface Contract

```python
SQSSink(queue_url: str, *, client: Any = None, ...)              # was: client positional
SentrySink(dsn: str | None = None, *, client: Any = None, ...)   # was: sdk=
RotatingFileSink(path: str, *, backup_count: int = 1, ...)       # was: 0

# unchanged signature, one appended field
health() -> Health                            # + unbuffered_failed
flush(timeout: float | None = 5.0) -> bool    # unchanged, deliberately (FR-004)
```

## Implementation Phases

### Phase 1: The signature freeze (FR-001, FR-002)

- Make `SQSSink`'s client keyword-only; rename `SentrySink`'s `sdk` → `client`, attribute
  included.
- Add the name-derived roster test (no sink takes `client`/`sdk`/`producer`/`connection`
  positionally).
- Update every call site, docstring and README row.

### Phase 2: `RotatingFileSink` (FR-003)

- Default `backup_count=1`. No counter.
- Class docstring and README row: what `backup_count=0` discards, and the 2 × `max_bytes`
  footprint the new default implies.

### Phase 3: Synchronous-path loss (FR-004)

- Append `unbuffered_failed` to `Health`; `_unbuffered_failed` + its own lock in `decorator`,
  incremented from `api._log`'s guard; surfaced by `_worker_health()` on both branches.
- README alert idiom, `Health` table, and `health()`'s docstring.
- Tests for FR-004, mutation-tested, with the mixed-process and sink-construction-failure cases
  first — those are the two an increment in the wrong place still passes.
