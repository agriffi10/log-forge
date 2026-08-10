# log-foundry — Project Memory

Loaded every session — keep it lean. Deep docs live in `docs/` and are pulled **on demand**:
- `@docs/process.md` — how we work: spec lifecycle, session rhythm, completion ritual (read once)
- `@docs/architecture.md` — system design + Known Constraints / Non-goals (read the section you need)
- `@docs/implementation-guide.md` — phase-by-phase build guide that mirrors architecture.md (reference)
- `@docs/specs/INDEX.md` — the spec index + status (one row per spec)
- `@docs/specs/SPEC-XXX-*.md` — the spec you're implementing
- `@docs/component-inventory.md` — reusable modules already built
- `@docs/spec-delivery/SPEC-XXX-*.md` — what a past spec delivered (pull only when a dependency points to one)
- `@docs/best-practices/INDEX.md` — domain coding rulebooks (Python); route here, then load only the section(s) you need

---

## Project Overview

A Python library that generates **consistent, structured (JSON) logs per decorated function call**,
correlates them with shared trace/span IDs, buffers them, and ships them to a configured sink (stdout
by default; SQS as the headline path → ELK). It owns the **logs** pillar of observability and uses
tracing vocabulary/ID conventions so output can later correlate with traces. Status: early
implementation against the design in `architecture.md`.

## Layout

- `src/log_foundry/` — the library (src layout; distribution `log-foundry` on PyPI, import
  `log_foundry` — install and import names match. The project was originally named `log-forge`,
  which PyPI rejects as too similar to the unrelated pre-existing `logforge`).
  Target module map (see implementation-guide.md): `config`, `ids`, `model`, `context`, `console`,
  `api`, `decorator`, `worker`, `sinks/{base,stdout,sqs}`. SPEC-001 shipped `config`, `ids`, `model`,
  `context`, `decorator`, `sinks/{base,stdout}` + the `configure`/`trace` façade; SPEC-002 added
  `api` (emitters + `set_baggage`) and `console` (echo); SPEC-003 made `@trace` async-aware;
  SPEC-004 added `worker` (background flush) + `shutdown`; SPEC-005 added `sinks/sqs` (`SQSSink`,
  optional `aws` extra — renamed from `sqs` in SPEC-010). Plus three leaf helpers no module map
  anticipated, none of which imports anything from the package: `sanitize` (SPEC-017),
  `_diag` (SPEC-025, owned by SPEC-029) and `results` (SPEC-034 FR-007 — `FlushResult` /
  `ContinueResult`). The full module map is now built; the setup-phase
  `core.py` + `modules/v1/` have been removed.
- `tests/` — pytest suite (`conftest.py`, `test_*.py`).
- `docs/` — architecture, implementation guide, specs, spec-delivery, templates.

## Tech Stack

| Layer | Tech |
|---|---|
| Language | Python **>= 3.12**, fully typed (PEP 561 `py.typed`) — CI gates on 3.12 **and** 3.13 |
| Packaging | Poetry + `poetry-dynamic-versioning` backend, src layout; version derived from Git tags, **never hand-edited** |
| Publishing | PyPI as **`log-foundry`** via `release.yml` — Trusted Publishing (OIDC), no stored token |
| Runtime deps | **none**; optional extras — `aws` (boto3), `sentry` (sentry-sdk), queue/stream: `kafka`, `redis`, `amqp`, `nats`, `gcp-pubsub`, `azure-eventhubs`, and database: `clickhouse`, `mongo`, `postgres` |
| Concurrency | `contextvars` (threads + asyncio); background flush worker thread |
| Test | `pytest` (`asyncio_mode=auto`, `--strict-markers`), `pytest-asyncio`, `pytest-cov` |
| Supply chain | optional `security` group: `cyclonedx-bom` (SBOM), `pip-audit` (advisories) — SPEC-023 |
| Lint / types | `ruff` (line-length 100), `mypy --strict` over `src` |

**Don't add dependencies without noting them here first.** Keeping the core dependency-free is a
deliberate constraint — new runtime deps belong behind an optional extra (as `aws`/`boto3` is).

## Code Conventions

- **mypy strict, fully typed** — no untyped defs; ship the `py.typed` marker.
- **The decorator never swallows exceptions** — record (status=error, type, stack) and **re-raise
  unchanged** (architecture §4).
- **No auto-capture of args/return values** — function name only, to avoid leaking secrets/PII (arch §6).
- Structured JSON only — named fields, never free-form text; user fields go in nested `fields` (arch §6).
- `ruff` line-length 100; keep modules single-concept per the module map (no import cycles).
- **Docstrings are the only prose in `src/` — no inline comments.** Every function, method and class
  carries a Google-style docstring: a description of **≤3 sentences**, then `Args:` / `Returns:` /
  `Raises:`, each filled with `None.` where it doesn't apply. Reasoning that would have been an
  inline comment belongs *in* the docstring. Module docstrings are one line. `# noqa` / `# type:
  ignore` are directives, not comments, and stay. Three docstrings are asserted by tests — `_diag`'s
  module docstring, `sinks/sqs`'s, and `Sink.emit`'s — so check before trimming those.
- When writing/refactoring Python, consult `@docs/best-practices/INDEX.md` → `python/python.md` first and load only the relevant section(s); the repo's `ruff`/`mypy` config wins over PEP 8 defaults.

## Common Commands

```bash
poetry self add "poetry-dynamic-versioning[plugin]"   # one-time: resolve the tag-derived version locally
poetry install --with dev          # set up
poetry run pytest                  # test
poetry run ruff check .            # lint
poetry run mypy                    # typecheck (src)
sh scripts/spec-lint.sh            # lint specs (structure + banned headers)

# Supply-chain tooling (SPEC-023). The `security` group is optional — `--with dev` never installs it.
poetry install --with security --all-extras    # audit tooling + every optional extra
poetry run pip-audit                           # advisories in the resolved environment
poetry run cyclonedx-py environment "$(poetry env info --path)" -o sbom.cdx.json
```

**After installing extras locally, recreate the venv before trusting `mypy`.** With the optional
deps present the `type: ignore[import-not-found]` comments in `sinks/` become "unused" and
`mypy --strict` fails — CI never installs extras, so its no-extras environment is the contract.
Uninstalling is not enough: `azure/` and `google/` leave namespace directories behind that mypy
still reads as installed. `poetry env remove --all && poetry install --with dev` is the fix.

**Releasing:** `git tag -a vX.Y.Z && git push origin vX.Y.Z` → `release.yml` publishes to PyPI.
Merges to `main` publish a `X.Y.Z.devN` pre-release. Never add a `version` key to `pyproject.toml`.
`poetry-dynamic-versioning` rewrites `pyproject.toml` in place whenever it resolves a version —
`python -m build` **and** `poetry install` with the plugin active — and the round-trip reorders keys
(it moves `[tool.poetry] version` out from under its comment). Harmless but noisy: `git checkout --
pyproject.toml` after. Always `git diff pyproject.toml` before committing.

## Specs

Index + status: `@docs/specs/INDEX.md`. Each spec file's header carries its own `Status`.
**Current work:** the SPEC-001..005 core arc is **fully Completed** (Core Span Pipeline →
Logging API + console echo → async `@trace` → background flush worker + shutdown → SQSSink +
`aws` extra). The **sink-expansion** arc (SPEC-006..011) is **fully Completed**: composition/adapter sinks →
stdlib logging bridge → local file + embedded → HTTP/platform (Elasticsearch, Loki, Logstash,
Syslog, Datadog, Splunk, New Relic, Honeycomb, Sentry) → queue/stream (Kafka, Redis, RabbitMQ,
NATS, Pub/Sub, Event Hubs, Kinesis, Firehose, SNS) → database (Mongo, Postgres, ClickHouse). Each
third-party transport sits behind its own optional extra (lazy-imported). **SPEC-012 (release and
distribution) is Completed** — the package ships to PyPI as `log-foundry`. A follow-up (no spec
— a mechanical rename) renamed the import package `log_forge` → `log_foundry`. **SPEC-013
(AWS Lambda compatibility) is Completed** — floor lowered to Python 3.12 (CI matrix 3.12 + 3.13)
and a repeatable `flush()` added. **SPEC-014 (cross-process trace continuation) is Completed** —
`continue_trace()` adopts a W3C `traceparent` + `baggage`; `current_traceparent()` and friends
publish it. **SPEC-015 (baggage on boundary events) is Completed** — `span.start`/`span.end` were
built with `baggage={}` hardcoded, so the events carrying `duration_ms`/`status` were invisible to
a baggage filter; one backfill at span close now completes both.
**SPEC-016 (FIFO queue support for `SQSSink`) is Completed** — a `.fifo` URL now selects FIFO
behaviour and every entry carries a `MessageGroupId` (the event's `trace_id` by default,
configurable) plus a `log_id` dedup id; sender faults are no longer retried.
**SPEC-017 (payload and failure safety) is Completed** — an unserializable field used to raise into
the caller on the orphan path and destroy the whole batch inside a span; nothing bounded any value;
and an all-children-down `MultiSink` reported success so the worker's retry never ran. Events are
now coerced and size-bounded at assembly, `error` carries `message`/`module`, and `health()` exposes
the worker's loss counters.
**Latest release: `v0.10.1`** (SPEC-023 — and the first release carrying an SBOM; `v0.10.0` shipped
without one and its GitHub Release is unrepairable, see the delivery doc. `v0.9.0` was
SPEC-022 + the extras-floor raise; `v0.8.0` was SPEC-021, `v0.7.1`
SPEC-020, `v0.7.0` SPEC-018 + SPEC-019, `v0.6.0` SPEC-017, `v0.5.0` SPEC-016, `v0.4.0` SPEC-015,
`v0.3.0` SPEC-013 + SPEC-014, `v0.2.0` the rename, `v0.1.0` the first stable).
**SPEC-018 (batch response adjudication) is Completed** — `KinesisSink`/`FirehoseSink` adjudicated a
batch response positionally without checking the arrays line up, so a short response truncated the
retry list and the chunk reported success: the same silent-loss shape as SPEC-017, in the two sinks
it did not reach. The length check now lives in `sinks/_batch.py`, used by both, and an
unadjudicable chunk is abandoned against a new `dropped_unadjudicated` counter.
**SPEC-019 (worker liveness and terminal-failure reporting) is Completed** — the drain thread had no
terminal-failure path, so anything escaping its loop (`SystemExit` above all, which CPython's thread
bootstrap discards silently) stopped delivery with nothing recorded, while `health()` kept reporting
a healthy snapshot until the queue filled and `dropped` climbed — the wrong signal, since `dropped`
already means backpressure. `_run` is now guarded and `Health` carries `stopped_reason`.
**SPEC-018 + SPEC-019 ship together in `v0.7.0`** (as SPEC-013 + SPEC-014 did in `v0.3.0`).
**SPEC-020 (integer value bounds) is Completed** — `int` was the one type `sanitize` returned
unbounded, and CPython 3.11+ refuses to render one past 4300 digits (`sys.get_int_max_str_digits`),
so `json.dumps` raised: into the caller on the orphan path, and into a whole abandoned batch inside
a span. An over-long integer is now replaced by `<int: ~N digits>`, mapping keys included. It closed
the last hole in SPEC-017's own guarantee. Shipped in **`v0.7.1`**.
**SPEC-021 (open-item cleanup) is Completed** — the 017..020 arc left 18 delivery-doc notes and
`architecture.md` §12 had carried 3 open items since before the first line of code, two of them
false by then. Every one is now fixed, settled, or recorded as a constraint, and §12 carries no
open items. The real wart is gone: `flush()` returned `True` when the drain it forced was
abandoned — a false success in the serverless path it was built for. It now reports whether
anything was lost while the call was outstanding. Also: the terminal-failure line counts the queue,
and the integer ceiling counts the minus sign.

**SPEC-022 (security scanning in CI) is Completed** — the first spec that touches no `src/` file.
Nothing in CI looked for a vulnerability beyond `ruff`'s single-file `S` rules, while `release.yml`
held `id-token: write` against PyPI and called every action by a mutable tag. Now: CodeQL
(`python` + `actions`, `extended`, weekly, **default setup** — a repo setting, so do **not** add a
`codeql.yml`, which default setup would disable and silently stop uploading), `dependency-review`
on PRs, `dependabot.yml` version updates with cooldowns, `zizmor` over the workflows, `SECURITY.md`
with private reporting, and every action SHA-pinned. Free throughout because the repo is public —
though validity checks and generic secret patterns are **not** free: they need an org-owned repo
with GitHub Secret Protection, and `PATCH /repos` returns 200 while ignoring them.

**SPEC-023 (supply-chain transparency and dependency auditing) is Completed** — SPEC-022's scanners
all look *inward*, so nothing described what had shipped and nothing re-examined the eleven extras
after the merge that pinned them. Adds a CycloneDX SBOM per release (`scripts/make-sbom.py`,
published as a GitHub Release asset — the repo had nine tags and zero Releases), a weekly gating
`pip-audit` across all extras, OpenSSF Scorecard, and an optional `security` Poetry group. Touches
no `src/` file. Three of its own acceptance criteria were amended by evidence: FR-001's generator
could not read a PEP 621 project, FR-003's idempotency was impossible under immutable releases, and
FR-006 overstated what a missing PAT costs.

**SPEC-026 (sink loss visibility) is Completed** — four specs built a loss-reporting apparatus
(`failed_batches`, `dropped_unadjudicated`, `stopped_reason`, `flush()`'s verdict) that fired against
no shipped remote transport: each sink counted its own failures on a private attribute with no
accessor and returned normally, so with a dead syslog socket `flush()` read `True` and `health()`
read all zeros while every message was lost. Total failure now raises across the whole sink family,
absorbed loss is readable through `Health.sink`, and `sinks/base.py` states the contract a
third-party sink must satisfy.

**SPEC-027 (bounded, interruptible retry) is Completed** — every retrying sink slept the one
thread that delivers anything, so a sink's backoff was a global pause on log delivery held across
`shutdown()`, which joins that thread from `atexit`. `HTTPSink` passed a server-supplied
`Retry-After` straight to `time.sleep`: a measured `Retry-After: 8` blocked `shutdown()` for 22 s,
`86400` would have stalled logging for a day, and a negative value made `time.sleep` raise. Every
wait now goes through `sinks/_retry.py` and is cut short by a shutdown, `SQSSink` gained the backoff
it alone lacked, and `shutdown()` takes a timeout.

**SPEC-028 (the sink concurrency contract) is Completed** — sinks were built for one caller and
`file.py`/`sqlite.py` said so, but the orphan path has emitted on the *caller's* thread since
SPEC-002, against the sink the worker is draining into, and `base.py` stated no contract at all.
`emit`/`close` now document that concurrency, the sinks holding transport state take a lock, and
every loss counter takes a second one. Unlocked `SQLiteSink` turned out to kill the interpreter
(bus error), not lose rows.

**SPEC-030 (lifecycle signals) is Completed** — two documented user errors produced total,
permanent, *silent* loss. Logging after `shutdown()` kept submitting to the retired worker, and
`health()` read `queued=3, dropped=0, failed_batches=0, stopped_reason=None` — the alert idiom
could not fire, because `stopped_reason` is `None` for a clean shutdown by SPEC-019's design. And
`configure(sink=...)` after the first log updated what `get_config()` reports while every event
continued to the sink the worker captured (measured: A got 4, B got 0, the config claimed B).
`Health` gained `retired`/`submitted_after_shutdown`/`incomplete_swaps`, and a late `sink=` now
swaps the live target.

**SPEC-032 (post-close sink behaviour) is Completed** — the item SPEC-028 found, could not fix
inside its own roster, and handed to SPEC-030, which established it was sink-level loss rather than
lifecycle signalling and handed it on again. A closed sink still accepted work: `KafkaSink` produced
into a batch nothing would flush again, `GooglePubSubSink` appended a future nothing would resolve,
and the Redis sinks *succeeded* by reconnecting a client they had just disconnected — measured, one
`info()` after `shutdown()` lost the event with every counter reading zero. All three now refuse.
It also took SPEC-028's recorded lint-scope gap off SPEC-031, because the post-close roster derives
from that gate: scope is now every sink class with an `emit`, and each records its post-close
decision or has a double proving it refuses.

**SPEC-031 (audit small corrections) is Completed** — the last of the 2026-08-05 audit arc, and
the arc is now fully shipped. Five FRs were residue: `RotatingFileSink` measured its rotation
interval on the wall clock (a backward NTP step deferred rotation indefinitely), `_make_udp`
hardcoded `AF_INET` so UDP syslog to an IPv6 host silently could not work, four documentation
claims contradicted the code, `sanitize` read the interpreter's integer limit per value on a hot
path, and `Worker._release_waiters`'s use of `queue.Queue` internals is now a recorded §13
constraint rather than a flagged wart. **FR-006 was the exception** and shipped in its own PR: a
process that only ever logs outside a span creates no worker, so `atexit` was never registered
(it is registered *inside* `_get_worker`) and `shutdown()` returned early — the sink was never
closed, and `health()` read all-clear because every field describes a worker that does not exist.
One item is deliberately **still open** and needs its own spec: the orphan-path sink swap
(`configure(sink=A)` → `info()` → `configure(sink=B)` leaves A unclosed, `incomplete_swaps` at
zero), recorded in `architecture.md` §13.

**SPEC-029 (diagnostic output safety) is Completed** — twelve of the twenty-eight stderr sites
printed `repr(exception)` against the arch §6 rule `Worker._terminal_failure` cites for not doing
it (a psycopg repr reprints the statement *and* the bound event), and two were unguarded on the
worker thread, where announcing one lost batch ended delivery for good. Every line now goes through
`_diag`, and a test forbids any other module writing to stderr.

**SPEC-025 (the library must not fail the caller) is Completed** — three surviving instances of the
SPEC-017 shape, where the exception the caller received was one the library invented: an unguarded
`_close_span` failed a function that had already returned *and* emitted a contradictory second
`span.end`; a bare `info()` outside a span propagated a sink's failure; and `shutdown()` raised out
of `atexit` while the once-only flag left the sink unclosed. Every guard now catches `Exception`,
never `BaseException`. Shipped `_diag.py` (SPEC-029 owns it) and, on instruction, widened scope to
the pre-body setup so a fault there degrades to an untraced call instead of failing one.

**SPEC-024 (context lifetime) is Completed** — the arc's first, and the only finding that put
*wrong* data in the log stream rather than losing it. Baggage and the adopted trace context were
written into `contextvars` and never taken back out, so on a thread serving requests sequentially
one request's `user_id` reached the next request's events and a handler kept joining a trace whose
process had exited. `@trace` now releases both at the **root** span, `reset_context()` covers the
caller who opens no span, and `architecture.md` §5.1 states where the scope ends rather than only
where it starts.

**SPEC-035 (shutdown lifecycle) is Completed** — the two regressions SPEC-033 put on `main`, plus
the older one under them. An orphan log concurrent with `shutdown()` replaced the sink's
`stop_signal` with a fresh unset event — the one the drain thread was about to wait on — because
the skip was keyed on liveness, which goes false at shutdown *entry*; a swap racing `shutdown()`
left its new sink in the config, installed nowhere and recorded nowhere; and `shutdown()`'s
idempotent path returned in under a millisecond without waiting for the drain it found running
(measured: nothing delivered, sink never closed, process gone in 0.39 s). It also shipped the
enumeration that stops the first of those recurring: `tests/test_worker_predicate_roster.py`
walks `decorator.py`'s AST and fails unless every worker question declares one of four categories
and a reason. **Its fork FR became SPEC-039**, moved out once everything else had shipped.

**SPEC-034 (the public API freeze) is Completed** — the arc's first, taken first rather than last
so that `Health` is a frozen dataclass before two more specs append to it. Four phases: the
signature fixes that freeze at 1.0 (`SQSSink(*, client)`, `SentrySink(client=)`, `Sink`/`Config`/
`read_losses`/`get_baggage` exported, `stop_signal` → `log_foundry_stop_signal`); `get_config()`
no longer handing out the live mutable singleton; `echo`/`message`/`fields` as documented reserved
words with `fields=` as the escape hatch; and `FlushResult`/`ContinueResult` plus the
`NamedTuple` → frozen-dataclass conversion. **Three of its four blocking findings were regressions
it introduced and none was visible in its own diff** — see `docs/audits/HANDOFF-2026-08-10.md`.

**SPEC-038 (sink correctness) is Completed** — ten defects where a sink did not match what its
destination requires, in three PRs. The worst was not sink-specific in origin: `_final_drain`
hands the exit backlog over as one batch (5,980 events measured) and the HTTP family never
re-chunked, so `HTTPSink.emit` is now a template method owning the chunk loop with
`_render`/`_body`/`_handle_response` as the hooks. Also: Postgres's unguarded rollback, Pub/Sub's
append-only futures, Firehose's missing NDJSON delimiter and its `1024*1024`-vs-1,024,000
ceiling, Kinesis's uncharged `PartitionKey`, Kafka's unbounded close flush, Syslog retrying
`EMSGSIZE`, unbounded Redis destinations, `RotatingFileSink`'s generation-destroying default, and
the `sinks.util` move. **Twelve review rounds found seven blocking defects, five of them
introduced by a previous round's fix** — four being one mistake repeated, recorded in Key
Decisions as "a shutdown shortens a wait, never work".

**SPEC-037 (caller safety and serialization) is Completed** — two promises the library makes in
its first paragraph, each broken on a path its own spec did not check. `NaN`/`Infinity` passed
through `sanitize` into `json.dumps`, which writes tokens RFC 8259 does not define, so a strict
consumer rejected the whole record; each is now replaced by a `<float: …>` marker with
`truncated` set, on SPEC-020's reasoning. And `api._log`'s in-span branch was unguarded on the
recorded grounds that it "only appends to a list" — it calls `build_event`, so `info(exc)`
returned normally outside a span and killed the decorated function inside one, *and* the
decorator recorded an `error.type` the caller never raised. Six `xfail` cells cleared.

**The 2026-08-07 audit arc's build order was reversed** to `034 → 037 → 038 → 036 → 039 → 041 →
040`, and most of it no longer blocks `v1.0.0`. Reasoning in `docs/specs/INDEX.md`; the short form
is that scheduling `Health`'s NamedTuple→dataclass conversion *last* was what forced two later
specs to append fields as tuple members and prove indices, and then forced the freeze spec to undo
both — and that converting first makes the arc's remaining counters, hooks and reasons additive,
so they are free in `1.x`.

`docs/implementation-guide.md` remains the phase-level build reference behind the specs.

---

## Key Decisions (settled — don't re-litigate; detail in architecture.md)

- **Unit of work = a decorated call** (`@log_foundry.trace`); outermost call starts a trace, every call is
  a span within it. (arch §4)
- **IDs are W3C Trace Context compatible** — `trace_id` 16B/32hex, `span_id` 8B/16hex, `log_id` UUID;
  makes future trace adoption cheap. (arch §3.1)
- **Context via `contextvars`**, not thread-locals — correct under threads and asyncio; holds a span
  stack + baggage. (arch §5)
- **Buffer-then-flush, background, non-blocking** — span queue flushed at span end by a worker thread;
  app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)
- **The sink is a durable buffer, not the final store** — ship to SQS (absorbs spikes/outages), a
  separate consumer indexes into ELK. `StdoutSink` is the zero-dep default. (arch §8, §9.1)
- **Logs-only, send everything for now** — no metrics/OTel-native traces; sampling is deferred and
  **unbuilt** — no `should_send` exists in code, and the per-span flush makes the pipeline
  span-outcome-ready, *not* tail-sampling-ready. (arch §10, §13)
- **Version comes from Git tags, published to PyPI as `log-foundry`** — tags cut releases,
  merges to `main` publish `.devN` pre-releases. (SPEC-012)
- **Two drains, deliberately distinct** — `shutdown()` is terminal (stops the worker, closes the
  sink); `flush()` drains on demand and leaves everything running. A frozen-not-exited process
  (serverless) needs the second, and `atexit` never runs there. (SPEC-013)
- **Cross-process traces are adopted explicitly, never auto-instrumented** — `continue_trace()`
  takes a W3C `traceparent`/baggage the *caller* moved; no client patching or middleware, which
  would need the deps the core refuses. Inbound context is untrusted and confers no authority.
  (SPEC-014, arch §12)
- **Boundary events take the span's *final* baggage; mid-span events keep the moment's** — one
  backfill at close completes `span.start`/`span.end` (which describe the whole span and carry the
  outcome), while an `info` is left exactly as it was emitted. Backfilling everything would also
  invert `build_event`'s precedence by letting baggage beat a per-call field. (SPEC-015)
- **A FIFO message group is a trace, not the process** — `MessageGroupId` defaults to the event's
  `trace_id`: SQS orders *within* a group, and a trace is the unit whose events must stay ordered,
  while per-trace groups keep traces parallel instead of serializing everything behind one group.
  Overridable with a constant or a callable. Ordering is best-effort across a retry boundary, and
  sender faults are abandoned rather than re-sent byte-identical. (SPEC-016)
- **An event is safe by construction — coerced and bounded once at assembly, not per sink** —
  `build_event` runs every value through `sanitize.py`, so all 40+ bare `json.dumps` calls in
  `sinks/` are correct by consequence, it costs one pass per event rather than one per destination
  (`MultiSink`), and the guarantee reaches the non-JSON sinks too. The unserializable fallback is a
  type-name placeholder, never `repr()`, so the fix cannot widen the PII exposure arch §6 prevents.
  Ceilings bound per *value*, not per event. (SPEC-017)
- **A value too large to *render* is replaced, never clipped** — `int` is the one type with no
  natural ceiling, and CPython refuses to render one past `sys.get_int_max_str_digits()`, so an
  over-long integer becomes `<int: ~N digits>`. Truncating digits would silently change the number,
  and a wrong number is worse than a visibly elided one. Detection is `bit_length()`, never
  `len(str(v))` — the obvious check raises the very error being prevented — with the ratio rounded
  so it errs toward replacing. (SPEC-020)
- **A positional response adjudicates all of a chunk or none of it** — an id-less per-record array
  must prove it describes the records sent (same length, right shape) before entry *i* may be read
  as record *i*; a mismatch is evidence of misalignment, so even the overlapping prefix is refused.
  What it cannot adjudicate is **abandoned and counted** (`dropped_unadjudicated`), never retried —
  the API reported a failure count, so some of the chunk landed and re-sending would duplicate
  downstream forever, while an abandoned record is a loss counted here and now. Id-keyed responses
  (`SQSSink`, `SNSSink`) select by `Id`, cannot mis-pair, and are deliberately not unified with
  this. (SPEC-018)
- **A dead worker is reported, not restarted — and as a *reason*, not a liveness flag** — the drain
  loop is guarded end to end and records the exception type that ended it (`Health.stopped_reason`),
  because `dropped` climbing already means backpressure and must not double as "the thread is gone".
  A reason string is `None` for a live worker, a never-created one, **and** a cleanly shut-down one,
  so it extends the alert idiom by a term; an `alive` flag would read `False` on every process that
  has not logged yet. No auto-restart: a thread that resurrects itself fights a process trying to
  exit. Type name only, never the exception message (arch §6). (SPEC-019)
- **`flush()` reports delivery, and answers from the drain that carried the events** — the marker
  brings the emit's *outcome* back, so `True` means the sink took them, not merely that a drain
  ran. A marker that finds nothing pending inherits the outcome of the emit that carried what was
  ahead of it in the FIFO — otherwise a second concurrent flush reports success for events the
  first one just abandoned. It is not a verdict on every batch ever sent; `health().failed_batches`
  is the cumulative record. (SPEC-021)
- **An open item is closed by being fixed, settled, or recorded as a constraint — never deleted** —
  a note that is merely removed takes its reasoning with it, and a reader cannot tell a live defect
  from a decision that reads like one. Superseded notes are struck through in place and marked with
  the spec that closed them; `architecture.md` §12 carries no open items and §13 states the
  constraints. (SPEC-021)
- **Every action is pinned to a commit SHA, and the pins are maintained, not frozen** — a mutable
  tag on a workflow holding `id-token: write` against PyPI is a silent path from a third-party
  repository to every consumer's `pip install`, so `pypa/gh-action-pypi-publish` is pinned away
  from the `release/v1` branch PyPA itself recommends: a lagging pin fails loudly at release time,
  a compromised action fails forever. Dependabot's `github-actions` ecosystem moves the pins, which
  is what makes pinning affordable — the version comment must stay exactly `# vX.Y.Z` or it silently
  stops rewriting it. Pin to the tip of the major in use; a major bump is its own reviewable PR.
  (SPEC-022)
- **A scanner that exits zero has not said "clean"** — zizmor in SARIF mode and CodeQL both report
  to code scanning and pass the job regardless of findings, deliberately: Advanced Security owns
  triage, and blocking belongs in a ruleset. Only `dependency-review` fails a build. So the alert
  count is the verdict, never the check mark — and a green audit is not evidence a *setting* is
  present (zizmor's `dependabot-cooldown` stops at the first passing entry). State the setting.
  (SPEC-022)
- **An extra's floor is a published contract — moved deliberately, never by a bot** — Dependabot's
  first `pip` PR raised `boto3`/`sentry-sdk`/`pika` past floors that already admitted the new
  release. Those raises were **kept** (staying near-current on boto3 is worth the narrowing) but
  `versioning-strategy: increase-if-necessary` stays, so the floors now move only when a human
  decides they should. A floor raise is a contract change: it cuts a release **minor**, not patch.
  (`v0.9.0`)
- **An SBOM describes the published artifact, and is generated from it** — `make-sbom.py` installs
  the built wheel with every extra into a throwaway venv and describes *that*, because runtime
  dependencies are empty by design and the extras are the whole dependency surface. The generator
  runs from a *second* venv or it lists itself and its ~30 dependencies as the library's (measured:
  98 components vs 43). `cyclonedx-py`'s `poetry` mode cannot read this project at all — it wants
  `[tool.poetry].name`, and PEP 621 puts the name in `[project]`, the same misreading
  `dependabot.yml` documents. An empty SBOM, one versioned `0.0.0`, or one carrying build tooling
  fails the job: an inaccurate SBOM is worse than none, because it looks authoritative. (SPEC-023)
- **Release assets are attached to a draft, never to a published release** — this repository has
  immutable releases enabled, so assets freeze at publish: create-as-draft → upload → publish. And
  deleting an immutable release does **not** free its tag name, so a botched release is repaired
  only by a new version tag, never by recreating the old one. Both were learned by shipping
  `v0.10.0` without its SBOM and then making it unrepairable. The job is deliberately *not*
  idempotent — a re-run that claimed to replace an asset it cannot touch would be lying. (SPEC-023)
- **`pip-audit` gates, and audits the extras or it audits nothing** — `dependency-review` only sees
  a PR's dependency *diff*, so an advisory against an already-pinned dependency is invisible to it;
  the weekly re-examination is the point. `--no-root` is load-bearing (Poetry installs the project
  editable, and `--strict` refuses an editable distribution), and `--strict` is on because a
  silently skipped package is an unaudited one. Suppressions are per-advisory with written reasons
  in `.github/pip-audit-ignores.txt`, never by severity or package. (SPEC-023)
- **Per-request context is released at the root span — baggage restored, an adopted trace context
  cleared** — the asymmetry is deliberate. Baggage set before any span is a process-level default,
  so it is restored *to*; an inbound context is a one-shot handoff to the trace it names, and
  restoring it would put back an adoption made *before* the span, leaving a warm container joining
  the first caller's trace forever. Consequences: one `continue_trace()` serves one root span (a
  batch needs one per record, or one `@trace` entry point), and the release lands in the context
  the span's `finally` runs in — so adopting outside a span and dispatching into an `asyncio.Task`
  needs `reset_context()`, recorded as a constraint in arch §13. Nested spans never reset: "at or
  below" is where baggage starts, the root span's close is where it stops. (SPEC-024, arch §5.1)
- **Every path the caller stands on is total, and a swallowed fault is announced by *type*** — the
  decorator (setup, body, close, teardown), the orphan emitter and its echo, and `shutdown()` with
  its `atexit` drain all absorb an `Exception` and report one `_diag.absorbed` line rather than
  raising. Never `BaseException`: a `KeyboardInterrupt` or `SystemExit` is the operator's or the
  runtime's intent and must reach the caller — the same line SPEC-019 drew in the opposite
  direction for the worker thread, where the *absence* of a handler was the defect. A pre-body
  fault degrades to an **untraced call**, never a failed one; a failed close is announced, not
  retried (the once-only flag stays ahead of it, because a second `close()` on a partially
  released sink is worse than an unclosed one). Only the type is written, never the message
  (arch §6). `_diag` must import nothing from its own package. (SPEC-025)
- **One module writes every diagnostic, so the rules are applied once rather than remembered
  twenty-eight times** — which is exactly how twelve sites came to print `repr(exception)` while
  the other eight printed a type name, and how two came to be unguarded. `_diag` owns
  `absorbed`/`lost`/`rejected`: an exception is named by `type(exc).__name__`, and where that is
  not diagnosable (an `OSError` is not "refused" vs "host unknown") the caller passes a detail
  built from values the *library* controls — an `errno`, an HTTP status, an attempt count — never
  from the exception's text. Any detail is escaped **then** bounded, so the bound governs what is
  written, and `isprintable()` is the escape test rather than a C0 table: `splitlines()` breaks on
  three separators such a table misses, so a newline count would call a forged line safe. The one
  bounded `repr` is `rejected`, whose input is an inbound *header* rather than an exception — and
  it is escaped afterwards anyway, because `repr` escaping newlines is a property of the built-ins,
  not of `repr`. A test forbids any other module writing to stderr; it is a lint on the idiom
  (`stderr.write`, `print(file=…)`, `traceback.print_*`), not a sandbox. (SPEC-029, arch §6)
- **A sink that delivered nothing raises; one that delivered something reports** — the worker's
  retry, `failed_batches` and `flush()`'s verdict all run on an exception, so a sink that absorbs a
  total failure is a sink the worker *believes*: retry never engages, counters stay at zero, and
  `flush()` returns `True` while everything is lost. Raising is safe exactly when nothing landed,
  because there is nothing downstream to duplicate — which is also why partial failure must **not**
  raise (the worker retries whole batches). Absorbed loss goes to an optional `losses()`, aggregated
  into `Health.sink` and kept *nested*: `dropped` at the queue is backpressure, `dropped` at the sink
  is an event that never reached the wire, and one number would hide which fix applies.
  `losses()` is probed by name rather than declared on the Protocol, so a pre-SPEC-026 sink still
  satisfies `Sink`. Three cases stay silent by prior decision — an unadjudicable response (SPEC-018:
  cannot prove nothing landed, so a retry may duplicate), an SQS sender fault (SPEC-016: provably
  rejected, a byte-identical re-send can only fail again), and an oversized event (nothing to retry).
  The first is suppressed batch-wide and the second only when nothing *recoverable* was also lost:
  "unknown" and "rejected" are not the same claim. `losses().failed` is an upper bound on loss, not
  a count of it. (SPEC-026, arch §8, §9)
- **A sink's wait is bounded, interruptible, and never taken on a destination's word** — one
  drain thread means a sink's backoff pauses *all* delivery, and it spans `shutdown()`, so
  `time.sleep` is the wrong primitive: every sink waits on the worker's stop event, pushed onto it
  by the worker (`hasattr` probe, as with `losses()`) so `sinks` still never imports `worker`. A
  wrapper sink forwards it to whatever actually holds the retry loop — set on a wrapper the
  signal reaches nothing, which moves the defect rather than fixing it. `Retry-After` is advice,
  not an instruction: clamped to `max_retry_after`, and rejected outright when non-positive or
  non-finite (the test is `not (value > 0)`, because `value <= 0` reads `False` for `NaN`). Zero is
  rejected too — a rate-limiting destination saying "wait zero seconds" is far more likely
  truncated than meant. `shutdown()` is bounded because a sink blocked *in* a call still holds the
  thread, and an expired one leaves the sink **open**: the drain thread may still be inside
  `emit`, and a leaked resource in an exiting process beats a corrupt write. It reports through
  `stopped_reason` (`"ShutdownTimeout"`) rather than a new field, extending SPEC-019's vocabulary
  as that spec intended. (SPEC-027, arch §9)
- **A sink tolerates concurrent callers; the library cannot serialize them for it** — the worker
  drains on one thread, but a level call with no active span emits on the *caller's* thread, so
  `emit`/`close` are called concurrently against one sink object and `sinks/base.py` states that as
  a requirement on implementations. It cannot be a promise: the library does not own that thread.
  The lock is held for the **whole** operation assuming exclusivity, `threading.Lock` not `RLock`
  (a sink re-entering its own `emit` is a bug an `RLock` would hide), and `close()` takes it too, so
  it waits rather than releasing under a writer. Per driver, not per family: Postgres locks (one
  connection, one transaction), ClickHouse locks (per-session state, not published as shareable),
  **Mongo does not** (`pymongo` is thread-safe with its own pool, and the goal is correctness under
  concurrency, not the removal of parallelism) — each says which requirement it satisfies, Mongo
  included, or its bare emit reads as an oversight. Counters take a **second, dedicated** lock,
  ordered transport → counter: sharing one would make `health()` block behind an in-flight insert
  and its backoff, contradicting SPEC-026's "safe to call during an emit". The accepted cost is that
  an orphan log can now wait on the lock (arch §13) — bounded by SPEC-027, and the alternative is
  measured: unlocked `SQLiteSink` does not lose rows, it kills the interpreter with a bus error.
  The counter race is **not reproducible without injecting a preemption point** — a bare `+=` lost
  zero across 1.6M concurrent increments, though a property on the counter's storage does reproduce
  real loss — so the tests assert the increment happens *inside* the lock, the property that
  survives free-threading. **Which sinks lock is decided per driver and recorded in each sink's
  docstring, enforced by a lint**, because the first pass worked from the spec's hand-written file
  list and missed three sinks — `NATSSink` re-entering its own event loop could hang an application
  thread permanently. That is SPEC-027's roster lesson repeated; a roster in prose is not a roster
  the tests check. (SPEC-028, arch §9, §13)
- **A terminal `shutdown()` and a captured sink are both reported, not prevented** — the two
  lifecycle mistakes the library documented and then stayed silent about. Logging after
  `shutdown()` is still *accepted*: refusing it would hide the mistake and restarting the worker
  would fight a process trying to exit, so `Health` reports the **pair** `retired` +
  `submitted_after_shutdown` — a pair because `retired` alone is correct usage, and a new pair
  because `stopped_reason` is `None` after a clean shutdown and must stay that way (SPEC-019). The
  check in `submit` is one unlocked read of a write-once flag, which is not the *liveness* check
  SPEC-019 excluded from the hot path. A late `configure(sink=...)` swaps the live target — drain
  to the old sink, reassign (never rebuild: the queue, thread, counters and `atexit` registration
  survive), **fence with a second drain**, then close — because the first drain only proves the
  *pre-swap* events landed, and closing while the drain thread is inside `emit` is what SPEC-028
  exists to prevent. A drain that cannot be confirmed does not cancel the swap (the caller asked
  for that sink) but leaves the old one **open** and counts `incomplete_swaps`, on SPEC-027
  FR-004's reasoning that a leaked resource beats a close raced against a write. **One deadline
  covers all four steps**, the close included: `Sink.close` takes no timeout, so it runs on a
  **daemon** thread joined for the remainder. The wrong-signal objection SPEC-028 reverted for is
  **dissolved by deriving no signal from an expired join** — no counter, no line, so a slow close
  can never latch a loss on a healthy swap — and the live fact is published instead, as
  `Health.closing_sinks`, a gauge that falls as well as rises and is deliberately *not* a term in
  the alert idiom. **Neither thread flag is sufficient alone and both were built:** non-daemon
  stopped `atexit` from ever running (CPython joins non-daemon threads first), losing the *live*
  sink; daemon alone kills a slow-but-succeeding close, losing the buffer of a sink whose
  `close()` *is* its delivery. So the flag is not the mechanism — **the capped grace is**:
  `shutdown()` closes the live sink, then joins any outstanding closer for
  `DEFAULT_CLOSER_GRACE`, carved from its own budget so it neither extends shutdown nor lets a
  stuck close hold the exit for the full 30 s, and granted on the idempotent path too (an expired
  first call returns before reaching it). Running *after* the live sink's close is defence in
  depth, not the guarantee — both orders measure identically, since the cap returns control first
  — but it is the right order and is pinned by a test. What SPEC-028 refused to abandon was the sink still being delivered to; this one is
  fenced out by two confirmed drains — but its interpreter-exit objection *does* reach here once
  the close outlives `configure()`, so §13 records that an abandoned close can land inside a
  `commit()`. `shutdown()`'s own close stays inline. (SPEC-030, arch §7, §9, §13)
- **A sink that released its transport refuses; one that released nothing keeps accepting** — the
  SPEC-026 rule applied to the sink's own lifecycle, where an absorbed batch is a batch the worker
  believes just the same. Both halves bind: three shipped sinks lost every post-`close()` event
  (and Redis *succeeded*, leaking a reconnect nothing reaps), while making the stateless sinks
  refuse would invent loss where a batch would have delivered — so which applies is a property of
  the sink, recorded per class and enforced. Refusing moves no `losses()` counter: it is a failure
  **reported** to the worker, not one absorbed, and counting both would report one loss twice. A
  close landing *mid*-batch does not raise even when it catches everything — `publish()` already
  happened, so the total-failure test is on **refusals**, not on successes (SPEC-018's rule that
  only provable non-delivery may be retried). The guard is keyed on the *sink* being released, never
  on client ownership, or every injected-client sink would keep accepting after `shutdown()`. The
  lint's scope gate stopped guessing at the same time — every class in `sinks/` with an `emit`,
  because a roster whose completeness is the point cannot rest on a heuristic, which is exactly how
  two of the three sinks stayed invisible for four specs. (SPEC-032, arch §8, §13)
- **The close is once-only across both delivery paths; the `atexit` *registration* is not the
  thing being guarded** — a process that only ever logs outside a span builds no worker, so
  nothing owned its sink's close and nothing performed it. The arming lives in `api._log`'s
  orphan branch and fires **after** the emit returns, because what has to be recorded is that an
  event *reached* the sink: keying on a configured sink is the obvious phrasing and is wrong,
  since `configure()` runs `_ensure_sink()` unconditionally and would close a `StdoutSink`
  nothing was ever written to. One `atexit` handler covers both paths — `_shutdown_worker` drains
  a worker if there is one and closes the orphan sink otherwise — which is what makes a single
  registration under the existing flag correct. Two handlers double-close (`atexit` runs LIFO)
  and reusing the flag for a worker-only handler costs a mixed process its exit drain
  (SPEC-004 FR-005); **both traps are green against every in-process test**, and trap A is caught
  by exactly one test in the suite — the orphan→span subprocess case. A live worker still owns
  the close, which is what makes a mixed process one `close()` in either order, and it inherits
  the worker's reasons for *not* closing (an expired shutdown leaves the sink open). No worker
  is created to answer any of this: `health().retired` is synthesized from a module flag, the
  same refusal `_swap_sink` and `_flush_worker` already make. `submitted_after_shutdown` is
  deliberately **not** incremented here — SPEC-030 defines it as a submission queued where
  nothing will drain it, and a later orphan log is refused at a closed sink and announced, which
  is not the same claim; `retired` alone is what stops being vacuous. (SPEC-031 FR-006, arch §13)
- **A value on the wire is measured on the clock that cannot move** — `RotatingFileSink`'s
  rotation deadline is `time.monotonic()`, as `Span.start_ts` already was, because a wall-clock
  deadline is defeated by any step larger than the interval. The *label* stays wall-clock
  wherever one exists; here none does, since backups are numbered rather than timestamped.
  (SPEC-031 FR-001)
- **A sink handoff is owned by whoever is delivering, and "a worker exists" is not "a worker owns
  this sink"** — `_swap_sink` returned early on a null worker, so a late `configure(sink=...)` in a
  process that only logs outside a span left the previous sink open with `incomplete_swaps` at
  zero. The orphan path now records the sink **object** an emit reached, because `configure()`
  assigns `_config.sink` *before* the swap runs and a boolean cannot say which sink is owed; the
  record is **re-pointed** at the new sink rather than cleared, since clearing leaks the new one in
  a process that swaps and exits without logging again — a case the boolean got right. No drain and
  no fence: orphan emits are synchronous and have returned, and the one writer a fence could not
  exclude is the one `Worker._close_swapped_out` already documents itself as not covering.
  `incomplete_swaps` stays **worker-only** — it means an unconfirmed *drain*, and widening it would
  stop telling an operator whether events were misrouted or a close was merely slow. Two guards key
  on **ownership** (`_worker.sink is X`), because `Worker.swap_sink` returns early once
  `_shutdown_done`, so a retired worker keeps its old sink forever while events go to a newly
  configured one — the identity form still declines on an *expired* shutdown, which is what the
  original guard existed for. Review of the spec found two more instances of the same boolean:
  the once-only close was per **process**, so a sink configured after `shutdown()` was closed by
  nothing (measured losing a buffering sink's whole batch while `health()` read `retired=True`,
  `submitted_after_shutdown=0` — SPEC-030's pair needs a worker to count a submission), and
  `Worker._offer_stop_signal` was SPEC-027's only caller, so this path's backoffs were never
  interruptible. The stop event is **replaced, never cleared**: an `Event` cannot be un-set and
  `_retry.wait` returns instantly on a set one, so a live sink holding the shutdown's event backs
  off not at all. The closer machinery moved to `_lifecycle.py` because the state must be
  process-global — a closer started before any worker existed must still be counted by
  `closing_sinks` and still granted the exit grace. (SPEC-033, arch §7, §9, §13)
- **A worker guard asks one of four questions, and the set is enforced rather than remembered** —
  existence, liveness, ownership, and ownership ∧ moment (`architecture.md` §9.2). The fourth is
  new and is the one site where the arc's own "ownership, not liveness" slogan is wrong: bare
  ownership skips the stop-signal offer for a worker whose shutdown has *finished*, leaving a live
  sink holding a set event that can never clear, and liveness alone un-skips for the whole drain
  and hands the drain thread an event nobody will set. Both measured. Three reviewers had each
  named a different site, each was fixed, and a fourth shipped — so the fix is not a fifth
  correction but `tests/test_worker_predicate_roster.py`, which derives every site from
  `decorator.py`'s AST and fails on one that declares no category. Its subject vocabulary is
  derived too, to a fixpoint: a function whose return value names the worker is itself a worker
  name, or `worker = _snapshot()` rebinds a guard's category behind a neutral name with everything
  green. A roster that hand-lists anything — sites or tokens — rots. (SPEC-035, arch §9.2)
- **A public accessor hands out a copy; the library reads the live object** — `get_config()` and
  `get_baggage()` copy, because a public getter documented "do not mutate" is a promise the
  caller's slip breaks silently, while `config._live_config()` and `context._live_baggage()` are
  the per-event reads, since `build_event` runs one to three config reads and one baggage read
  **per event** and a copy there allocates per event. Both copies are **one level**: deep-copying
  arbitrary caller objects inside an accessor that must never raise trades a narrow sharing bound
  for a wide new failure, so the bound is stated and pinned rather than closed. Freezing `Config`
  also turned every write into a read-modify-write, and one writer (`_ensure_sink`) runs on the
  orphan logging path — measured, one concurrent `info()` permanently reverted `configure()` in
  268 of 2000 trials — so `_config_lock` serializes the writers while reads stay lock-free.
  (SPEC-034 FR-003, FR-005)
- **A result that can grow a reason must stop being a `bool` before 1.0, not after** — `flush()`
  answered five outcomes with one bit and `continue_trace()` two. A `NamedTuple` cannot be
  retrofitted (a non-empty tuple is always truthy, so every `if flush():` would silently keep
  passing), so `FlushResult`/`ContinueResult` carry `__bool__` plus a `reason`, and grow by new
  reason values only. `Worker.flush` carries the type too: the five outcomes are distinguishable
  only there. For the same reason `Health` and `SinkLosses` became frozen dataclasses — six specs
  had each argued their appended field left the indices undisturbed, and with a dataclass there
  are no indices. (SPEC-034 FR-007, FR-008)
- **A protocol that is exported is a protocol that will be inherited** — `Sink`'s members were
  empty-bodied and not `@abstractmethod`, so a subclass with one typo instantiated happily and
  its inherited `emit` returned `None`: three events gone, `flush()` truthy, every counter zero.
  `mypy` refused it and only the runtime did not. Structural satisfaction is untouched, which
  matters because none of the 34 shipped sinks inherits it. (SPEC-034 FR-005)
- **A reserved word needs exactly one route through, including its own name** — `echo` and
  `message` were parameters stealing ordinary words from the field namespace, and `fields=` is
  the escape hatch, so `fields` becomes the third reserved word and `fields={"fields": …}` must
  work. The keyword form wins a collision (`{**base, **overrides}`), and the merge **absorbs** a
  non-mapping rather than raising: it runs in the emitter, outside `api._log`'s orphan guard, so
  an unguarded merge broke SPEC-025's promise on all four paths. (SPEC-034 FR-004)
- **A shutdown shortens a *wait*; it must never skip *work*** — `Worker.shutdown` sets the stop
  event **before** joining the drain thread, so it is set for the whole of `_final_drain`. Any
  sink consulting it to do *less* is therefore degrading itself on the exit drain, which is the
  one path a serverless process has. Measured four times in one spec: `HTTPSink` ending its 413
  search delivered **nothing** of a 2,000-event backlog that had been going out in 30 requests;
  `KafkaSink` cutting its flush to zero delivered **0 of 11**, since `produce()` is a local
  hand-off and `flush()` is the only thing that drains the producer. `_retry.wait` is the one
  place the signal belongs — it shortens a backoff and cancels no attempt. (SPEC-038 FR-001
  AC-4a, FR-006 AC-3)
- **A destination's limit is found by halving the *budget*, not the chunk** — recursive
  chunk-halving is `2N-1` requests (11,954 measured for one exit backlog), because each accepted
  size is rediscovered in every branch; halving the budget re-chunks the remainder once per
  reduction and converges in `log2(ratio)` (8 for a 5 MB default against a 20 kB endpoint).
  Capping the recursion *depth* instead is the trap: a 250× ratio needs ~8 halvings, so a cap of
  4 delivered 2 events of 2,000. Each reduction halves **what was refused**, not the nominal
  budget. A `413` is never retried — it is a verdict on the bytes — and a size already refused is
  not asked about again, **except under `gzip`**, where the refusal is uncompressed and the
  destination judges the wire. (SPEC-038 FR-001)
- **A subclass that inherits a method is still in the roster** — scope keyed on *defining* one
  makes membership a function of where code happens to sit, and moving five `emit`s into a base
  dropped those classes out of two lints in a single commit, 34 → 29, with the suite green. Only
  the sibling roster noticed, and only because it had a floor guard. Both rosters now scope on
  defines-or-inherits, both carry floors, a class overriding neither `emit` nor `close` may answer
  from the ancestor whose code it runs, and **a test asserts the two cover the same classes** —
  they had already drifted twice on trigger and on base spelling. (SPEC-038 FR-001 AC-1a/AC-1b)
- **A bound is only a bound if it is measured where it binds** — the recurring shape behind five
  of this spec's seven blocking defects. A wall-clock assertion cannot see a busy-spin (a slice
  loop bounded at 1.00 s wall burned 3.5 M calls and a pegged core, so the test asserts **CPU**
  time); a timeout applied per *item* is `n × timeout`, not a bound; and a chunk size is a floor
  division, so a byte-charging test only bites at sizes where the division tips — the same test
  was vacuous at 1,012 bytes (the count limit binds first) and again at 9,000, and now asserts
  its own sensitivity as a precondition. (SPEC-038 FR-004, FR-005)
- **One name everywhere: `log-foundry` / `log_foundry`** — the import package was renamed from
  `log_forge` in `v0.2.0` so it matches the distribution name. Breaking for `0.1.x` users; no
  compatibility shim was shipped. Historical `log-forge` mentions survive only where they name
  the PyPI-rejected original.

## Out of Scope (don't build)

Metrics or OTel-native traces · querying / dashboards / alerting (that's ELK/downstream) · log routing
beyond one configured sink per process · **auto**-instrumented propagation — no HTTP-client patching,
middleware or boto3 hooks; the caller moves the header (cross-process continuation itself shipped in
SPEC-014) · `tracestate` · sampling · "follows-from" span relationships (deferred).

---

## Session Workflow

**Start:** (1) this file; (2) the spec you're implementing (`@docs/specs/SPEC-XXX`); (3) skim `@docs/component-inventory.md` for reuse and pull only the architecture.md section / implementation-guide phase you need — don't read architecture.md whole. (4) Confirm CI is green on `main`; investigate failures before building. (5) Branch from fresh `main`. (6) Generate an implementation plan from the spec's phases, validate it against the spec (FRs + acceptance criteria covered, reuse used, nothing out of scope), and confirm it before writing code.

**During:** every file-changing task goes on its own branch and opens a PR — never commit to `main` directly. After a phase, stop and summarize what was built and how it maps to the plan. Specs carry no Open Questions — triage emergent issues by kind: **reversible/technical** ones you decide in-session (update the spec if scope changes); **product-changing or ambiguous** ones you stop and escalate to the human with options + a recommendation, never silently decide.

**Review:** code review and verification run in a **fresh context** (new session or subagent), never the session that wrote the code — check the diff against the spec's acceptance criteria, not just "looks fine."

**PRs & main:** before opening a PR, get the formatter, linter, and unit tests green locally. Watch every PR to completion — never open-and-abandon. **Green CI is not a review, and does not authorize a merge.** Every PR gets an independent fresh-context review (subagent or new session) *before* it merges: push → CI green → review → address findings → merge. CI cannot see a test that passes against the bug it claims to catch, a lock taken in the wrong order, or an acceptance criterion ticked with no evidence — SPEC-028 merged green and a review then found a sink that could hang an application thread forever. `main` is always watched: after any merge confirm it went green, and if `main` fails, diagnose immediately and fix it with a new PR before anything else.

**On spec completion — keep the always-loaded files lean:**
1. Set the spec file's `Status: Completed`.
2. Update the one-line row in `@docs/specs/INDEX.md` (status only — don't add prose).
3. Write a short delivery doc at `docs/spec-delivery/SPEC-XXX-<name>.md` from the template.
4. If it added reusable modules, add a one-line row to `@docs/component-inventory.md`.
5. A *new architectural decision* gets one line in Key Decisions above (+ a pointer) — never a paragraph.
