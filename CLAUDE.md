# log-foundry — Project Memory

Loaded every session — keep it lean. The method lives in `docs/process/`, one file per part. Its
router and the session rhythm load **with** this file (the two imports below); everything else is
pulled **on demand**, when the router's table says:

@docs/process/INDEX.md
@docs/process/session-rhythm.md

- `@docs/process/reviewer-contract.md` — the review gate in full (pull before briefing any reviewer)
- `@docs/process/model-routing.md` — which model does which job (pull before delegating anything)
- `@docs/process/completion-ritual.md` — the five steps at spec completion + the doc-hygiene rules
- `@docs/decisions/INDEX.md` — the Key Decisions register: one file per area, fences first (the table below names the areas)
- `@docs/invariants.md` — the numbered promises as observables; a spec's criteria and the system-frame diff review cite them by number (read before writing or reviewing either)
- `@docs/architecture.md` — system design + Known Constraints / Non-goals (read the section you need)
- `@docs/implementation-guide.md` — phase-by-phase build guide that mirrors architecture.md (reference)
- `@docs/specs/INDEX.md` — the spec index + status (one row per spec)
- `@docs/specs/SPEC-XXX-*.md` — the spec you're implementing
- `@docs/component-inventory.md` — reusable modules already built
- `@docs/spec-delivery/SPEC-XXX-*.md` — what a past spec delivered (pull only when a dependency points to one)
- `@docs/best-practices/INDEX.md` — domain coding rulebooks (Python); route here, then load only the section(s) you need
- `.claude/rules/` — one path-scoped pointer per governed tree; fires when a matching file is opened with Read (a backstop, not the mechanism)
- `.claude/agents/` — the subagent roles the routing table names, each carrying its default model

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
  `api`, `decorator`, `worker`, `sinks/{base,stdout,sqs}` — shipped across SPEC-001–005 (`docs/spec-delivery/`
  says which; `sinks/sqs` is the optional `aws` extra). Plus three leaf helpers the map did not
  anticipate, importing nothing from the package at runtime — `sanitize`, `_diag` and `results` —
  and two non-leaves that do: `_lifecycle` and `_fork` (which spec: `docs/component-inventory.md`).
- `tests/` — pytest suite (`conftest.py`, `test_*.py`).
- `docs/` — decisions register, architecture, implementation guide, specs, spec-delivery, templates.

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
  carries a Google-style docstring opening on a **one-sentence summary line** (ends in `.`, ≤100
  chars, blank line after it); below it, reasoning that would have been a comment belongs *in* the
  docstring, **unbounded**. A function or method then carries `Args:` / `Returns:` / `Raises:`, each
  filled `None.` where it doesn't apply; a **class** takes `Attributes:` for its public attributes
  instead, if it has any. Module docstrings are one line. `# noqa` / `# type:` / `# pragma:` are
  directives, not comments, and stay. Some docstrings are asserted by tests (`grep -rn '__doc__'
  tests/`) — check before trimming. `docstring-lint.py` gates the summary line, the comment ban, the
  trio and the module docstring; the `None.` filling and `Attributes:` are on you.
- When writing/refactoring Python, consult `@docs/best-practices/INDEX.md` → `python/python.md` first and load only the relevant section(s); the repo's `ruff`/`mypy` config wins over PEP 8 defaults.

## Common Commands

```bash
poetry self add "poetry-dynamic-versioning[plugin]"   # one-time: resolve the tag-derived version locally
poetry install --with dev          # set up
poetry run pytest                  # test (parallel by default: -n 12 — serial is several times slower)
poetry run pytest -n 0             # ...serially — REQUIRED for --pdb and -s (xdist discards -s output silently)
poetry run ruff check .            # lint
poetry run mypy                    # typecheck (src)
sh scripts/spec-lint.sh            # lint specs (structure, banned headers, invariant citations)
sh scripts/spec-lint-test.sh       # prove its checks still fire (if you changed it)
sh scripts/docs-lint.sh            # the always-loaded tier: shape + budgets (run before every push)
sh scripts/docs-lint-test.sh       # prove docs-lint's own checks still fire (if you changed it)
poetry run python scripts/docstring-lint.py   # the docstring rule over src/ (before every push)
sh scripts/docstring-lint-test.sh             # prove its checks still fire (if you changed it)

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
Merges to `main` publish no `.devN` for now (`publish-dev` disabled in `release.yml`, which says
why). Never add a `version` key to `pyproject.toml`.
`poetry-dynamic-versioning` rewrites `pyproject.toml` in place whenever it resolves a version —
`python -m build` **and** `poetry install` with the plugin active — and the round-trip reorders keys
(it moves `[tool.poetry] version` out from under its comment). Harmless but noisy: `git checkout --
pyproject.toml` after. Always `git diff pyproject.toml` before committing.

## Specs

Index + status: `@docs/specs/INDEX.md`; what each spec shipped: `@docs/spec-delivery/`; which release
carried it: `@docs/spec-delivery/RELEASES.md`. Phase-level build reference: `@docs/implementation-guide.md`.
**This section carries only what is live and recorded nowhere else** — when a spec closes, prune here
rather than appending. A completed spec's narrative belongs in its delivery doc, not in the file that
loads every session — `561a9f6` cut the Specs section this file had accumulated.

**Current work:** none in flight.

## Key Decisions (settled — don't re-litigate)

Settled decisions live in `docs/decisions/`, **one file per area**. Each area file opens with its
**Fences** — one line per decision, the claim and its constraint — and carries the full entries
behind them. **Read an area's fences before working in it.** A new decision is a fence and an entry
in its area file, never a line here; a new *area* is a new row here and in `docs/decisions/INDEX.md`,
in the same order. This table is the authority for the set of areas; `scripts/docs-lint.sh` holds
the register and the rules to it, and refuses anything in this section but this prose and the table.

| Area | Fences |
|---|---|
| Trace model and context | `docs/decisions/trace-model.md#fences` |
| Pipeline: buffer, worker, drain | `docs/decisions/pipeline.md#fences` |
| Event assembly: safety and bounds | `docs/decisions/event-assembly.md#fences` |
| Sink contract: delivery and its verdict | `docs/decisions/sink-delivery.md#fences` |
| Sink contract: waiting, concurrency and shutdown | `docs/decisions/sink-lifecycle.md#fences` |
| Failure paths and diagnostics | `docs/decisions/failure-paths.md#fences` |
| Public API surface | `docs/decisions/public-api.md#fences` |
| Release, supply chain and naming | `docs/decisions/release-supply-chain.md#fences` |
| Working rules: findings, rosters and testing bounds | `docs/decisions/working-rules.md#fences` |

## Out of Scope (don't build)

Metrics or OTel-native traces · querying / dashboards / alerting (that's ELK/downstream) · log routing
beyond one configured sink per process · **auto**-instrumented propagation — no HTTP-client patching,
middleware or boto3 hooks; the caller moves the header (cross-process continuation itself shipped in
SPEC-014) · `tracestate` · sampling · "follows-from" span relationships (deferred).

---

## Session Workflow

**Start:** the session rhythm (`@docs/process/session-rhythm.md`) loads with this file — follow it literally: read the spec you're building in full, skim `@docs/component-inventory.md` and pull only the `architecture.md` section you need, confirm CI is green on `main`, branch from fresh `main` (with other sessions running, in your **own worktree** off `origin/main`, never the shared checkout), set the spec `Status: In Progress` and its INDEX row in that first commit, plan, and put the plan through the reviewer gate before the first line of code. Decide the **PR grouping** yourself and say it in a sentence — it draws no reviewer. If this file and the rhythm disagree, this file wins — fix the drift there in the same session.

**Delegate, and pick the model by the job:** the main session orchestrates. Each artifact — spec, plan, code, every review, every sweep — goes to a subagent from `.claude/agents/` on the model `@docs/process/model-routing.md` names for that job (Sonnet writes specs and docs; Opus reviews, and Fable builds-and-reviews what Fable wrote; Opus implements, Fable when the complexity rule triggers — concurrency, lifecycle, fork, the frozen public surface; Haiku enumerates). When unsure, one tier up, never down.

**Review — three kinds of artifact, four reviews, one blocking gate:** a **spec**, an implementation **plan** and a **diff** each go to a reviewer in a **fresh context**, never the context that produced them. **Counts: one on the spec, one on the plan, two on every diff — and no fifth artifact draws a reviewer.** The two on a diff are two *frames*, not two rounds, both **before the push**: one reads the change against the spec's criteria and `best-practices/python`, the other starts from the **system** — for each numbered invariant in `@docs/invariants.md` the change touches, does it still hold on every *twin path* — and on code one of them **builds** the thing rather than reading it. A spec is not Draft-ready, a plan does not start code, and **a branch does not reach the remote**, until every finding is **fixed or flagged** — a rejection costs a sentence out loud. **The diff review gates the PUSH, not the merge, because green CI is not a review.** Cap same-frame rounds at two, then rotate the frame; exit on the *class* of finding shrinking, never on a round count, and never below the counts above. **A REPLACED artifact restarts its gate; a revised one does not.** **A gate is not tested by running it on what it guards** — it owes a fixture corpus asserting failure text, silence cases included. **Never carry an evidence sentence between repos** — re-measure it here, anchored to a commit. Every reviewer runs this repo's gates against the branch. Full contract, with the measured evidence behind every clause: `@docs/process/reviewer-contract.md`.

**Spec size — one slice, not a whole feature:** aim for **3–6 FRs**; past **8**, write a second spec beside the first and record the pair's build order as an arc in `@docs/specs/INDEX.md` rather than growing one. The second spec restarts at FR-001 (IDs are spec-local). Every FR names the invariant(s) it serves from `@docs/invariants.md` by number, or says `serves no invariant` in those words. `spec-lint.sh` **warns** above 8 rather than failing, because a genuinely indivisible spec may sit above the line — say so in one line under *Scope → In Scope* and let the reviewer accept or reject it. Full rule: `@docs/process/authoring-a-spec.md`.

**PRs & main:** before pushing, get the diff through the review gate above, and get this repo's six gates green locally — `poetry run ruff check .`, `poetry run mypy`, `poetry run pytest`, `sh scripts/spec-lint.sh`, and — **before every PR, since nothing in CI runs either** — **`sh scripts/docs-lint.sh`** and **`poetry run python scripts/docstring-lint.py`** (plus each one's `-test.sh` corpus whenever you touch that linter). **`ruff format` is NOT among them**: it is not a CI gate and this repo is not clean under it, so format only the files you edited (`@docs/process/operational-traps.md`). **When several agent sessions share this repo** — they routinely do — the PR queue serialises the remote: one PR open at a time, in the order asked, `main` green before the next, and you join the line only once your gates and reviews are green, because the queue is not a review. Install and read `scripts/pr-queue/` first; uninstalled it enforces nothing, silently. Protocol: `scripts/pr-queue/PROTOCOL.md`; brief each session from `@docs/templates/multi-agent-briefing.md`; the rest: `@docs/process/several-agents.md`. Watch every PR to completion and merge it as soon as CI is green — never open-and-abandon. **Key the watch on the current head sha** — a bare `gh pr checks --watch` can exit clean against the *previous* commit's checks. `main` is always watched: after any merge confirm it went green, and if `main` fails, diagnose immediately and fix it with a new PR before anything else.

**On spec completion:** run the five steps in `@docs/process/completion-ritual.md` — status, INDEX row, delivery doc, inventory row, and the register entry with its Contents row **before** its fence in the area file. A reversal updates the old entry in place and leaves a superseded marker at every other site still stating the old claim.

**Doc-size guardrail:** this file and its two imports are every session's fixed cost. `scripts/docs-lint.sh` holds the set **the router's table names** to a byte budget, holds Key Decisions to an intro and one area table, holds every area file in `@docs/decisions/INDEX.md` to fences-first with an entry behind every fence, holds `docs/process/`, `.claude/rules/` and `.claude/agents/` to their shapes, and refuses one shape of unanchored evidence — a dated measurement with no commit to re-measure from — in the docs and source files the script names. **It is a local pre-push gate, deliberately not a CI job** — run it before every push. This tier is where regrowth actually happened: appending took this file from 7,350 bytes at `ad898fc8` to 89,340 at `e60b60d` with the rule stated in it throughout, which is the argument for a script over a paragraph. When a budget fires, move detail down a tier behind a pointer and re-ratchet; after a structural cut leave headroom and say why beside the number, because a budget pinned at the measurement makes the next spec to settle a decision pay for it by pruning another area's fences. Full rule set: `@docs/process/completion-ritual.md` → *Anti-regrowth & doc hygiene*.
