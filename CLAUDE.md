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
  optional `aws` extra — renamed from `sqs` in SPEC-010). The full module map is now built; the setup-phase `core.py` +
  `modules/v1/` have been removed.
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
- When writing/refactoring Python, consult `@docs/best-practices/INDEX.md` → `python/python.md` first and load only the relevant section(s); the repo's `ruff`/`mypy` config wins over PEP 8 defaults.

## Common Commands

```bash
poetry self add "poetry-dynamic-versioning[plugin]"   # one-time: resolve the tag-derived version locally
poetry install --with dev          # set up
poetry run pytest                  # test
poetry run ruff check .            # lint
poetry run mypy                    # typecheck (src)
sh scripts/spec-lint.sh            # lint specs (structure + banned headers)
```

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
and a repeatable `flush()` added. **Latest release: `v0.2.0`** (the rename; `v0.1.0` was the
first stable); SPEC-013 is unreleased and wants a **minor** bump (`v0.3.0` — additive `flush`,
widened floor). **In flight: SPEC-014** (cross-process trace continuation).
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
- **Logs-only, send everything for now** — no metrics/OTel-native traces; sampling deferred but a
  `should_send` seam is reserved (tail-sampling-ready). (arch §10, §13)
- **Version comes from Git tags, published to PyPI as `log-foundry`** — tags cut releases,
  merges to `main` publish `.devN` pre-releases. (SPEC-012)
- **Two drains, deliberately distinct** — `shutdown()` is terminal (stops the worker, closes the
  sink); `flush()` drains on demand and leaves everything running. A frozen-not-exited process
  (serverless) needs the second, and `atexit` never runs there. (SPEC-013)
- **One name everywhere: `log-foundry` / `log_foundry`** — the import package was renamed from
  `log_forge` in `v0.2.0` so it matches the distribution name. Breaking for `0.1.x` users; no
  compatibility shim was shipped. Historical `log-forge` mentions survive only where they name
  the PyPI-rejected original.

## Out of Scope (don't build)

Metrics or OTel-native traces · querying / dashboards / alerting (that's ELK/downstream) · log routing
beyond one configured sink per process · cross-service trace continuation & cross-process baggage
(deferred; IDs already W3C-compatible) · "follows-from" span relationships (deferred).

---

## Session Workflow

**Start:** (1) this file; (2) the spec you're implementing (`@docs/specs/SPEC-XXX`); (3) skim `@docs/component-inventory.md` for reuse and pull only the architecture.md section / implementation-guide phase you need — don't read architecture.md whole. (4) Confirm CI is green on `main`; investigate failures before building. (5) Branch from fresh `main`. (6) Generate an implementation plan from the spec's phases, validate it against the spec (FRs + acceptance criteria covered, reuse used, nothing out of scope), and confirm it before writing code.

**During:** every file-changing task goes on its own branch and opens a PR — never commit to `main` directly. After a phase, stop and summarize what was built and how it maps to the plan. Specs carry no Open Questions — triage emergent issues by kind: **reversible/technical** ones you decide in-session (update the spec if scope changes); **product-changing or ambiguous** ones you stop and escalate to the human with options + a recommendation, never silently decide.

**Review:** code review and verification run in a **fresh context** (new session or subagent), never the session that wrote the code — check the diff against the spec's acceptance criteria, not just "looks fine."

**PRs & main:** before opening a PR, get the formatter, linter, and unit tests green locally. Watch every PR to completion and merge it as soon as CI is green — never open-and-abandon. `main` is always watched: after any merge confirm it went green, and if `main` fails, diagnose immediately and fix it with a new PR before anything else.

**On spec completion — keep the always-loaded files lean:**
1. Set the spec file's `Status: Completed`.
2. Update the one-line row in `@docs/specs/INDEX.md` (status only — don't add prose).
3. Write a short delivery doc at `docs/spec-delivery/SPEC-XXX-<name>.md` from the template.
4. If it added reusable modules, add a one-line row to `@docs/component-inventory.md`.
5. A *new architectural decision* gets one line in Key Decisions above (+ a pointer) — never a paragraph.
