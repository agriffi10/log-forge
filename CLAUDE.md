# log-foundry — Project Memory

Loaded every session — keep it lean. Deep docs live in `docs/` and are pulled **on demand**, except
`process.md`, which is the contract this file summarises:
- `@docs/process.md` — how we work: spec lifecycle, session rhythm, completion ritual (**read every session**)
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
poetry run pytest                  # test (parallel by default: -n 12, ~35 s not ~133 s)
poetry run pytest -n 0             # ...serially — REQUIRED for --pdb and -s (xdist discards -s output silently)
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

Index + status: `@docs/specs/INDEX.md`; what each spec shipped: `@docs/spec-delivery/`; which release
carried it: `@docs/spec-delivery/RELEASES.md`. Phase-level build reference: `@docs/implementation-guide.md`.
**This section carries only what is live and recorded nowhere else** — when a spec closes, prune here
rather than appending. A completed spec's narrative belongs in its delivery doc, not in the file that
loads every session; 308 lines of it lived here until 2026-09-02.

**Current work:** none in flight — every spec in the index is Completed.

## Key Decisions (settled — don't re-litigate)

**Grouped by AREA, not by spec — a register ordered by spec number is a changelog.** Each line is the
claim and the fence; the reasoning, the rejected alternatives and the full "do NOT build" wording live
in `@docs/decisions.md`, where every line below has an entry under the same bold label. Read the entry
for an area before working in it. A line here is **never the only home of a fact**, and a completion
**replaces or extends the clause for its area** rather than appending a new one.


### The trace model and its context


- **Unit of work = a decorated call** — `@log_foundry.trace`; the outermost call starts a trace, every call is a span within it. (arch §4)

- **IDs are W3C Trace Context compatible** — `trace_id` 16B/32hex, `span_id` 8B/16hex, `log_id` UUID, so adopting tracing later stays cheap. (arch §3.1)

- **Context via `contextvars`** — not thread-locals — correct under threads and asyncio; holds a span stack plus baggage. (arch §5)

- **Cross-process traces are adopted explicitly, never auto-instrumented** — no client patching or middleware, which would need the deps the core refuses. Inbound context is untrusted and confers no authority. (SPEC-014)

- **Boundary events take the span's *final* baggage; mid-span events keep the moment's** — backfill only at close. Backfilling everything inverts `build_event`'s precedence and lets baggage beat a per-call field. (SPEC-015)

- **Per-request context is released at the root span — baggage restored, an adopted trace context cleared** — the asymmetry is deliberate: restoring an inbound context puts back an adoption made *before* the span, leaving a warm container joining the first caller's trace forever. (SPEC-024)

- **A reserved word needs exactly one route through, including its own name** — `fields` is the third reserved word and `fields={"fields": …}` must work. The keyword form wins a collision, and the merge **absorbs** a non-mapping rather than raising. (SPEC-025, SPEC-034)


### The pipeline: buffer, worker, drain


- **Buffer-then-flush, background, non-blocking** — the app never blocks on sink I/O; graceful drain on `atexit`/`shutdown()`. (arch §9)

- **Two drains, deliberately distinct** — `shutdown()` is terminal; `flush()` drains on demand and leaves everything running. A frozen-not-exited process needs the second, and `atexit` never runs there. (SPEC-013)

- **`flush()` reports delivery, and answers from the drain that carried the events** — a marker that finds nothing pending inherits the outcome of the emit ahead of it in the FIFO — otherwise a second concurrent flush reports success for events the first one just abandoned. (SPEC-021, SPEC-036)

- **The close is once-only across both delivery paths; the `atexit` *registration* is not the thing being guarded** — the arming fires **after** the emit returns, because what must be recorded is that an event *reached* the sink; keying on a configured sink is the phrasing that misses it. (SPEC-004, SPEC-030, SPEC-031)

- **A worker guard asks one of four questions, and the set is enforced rather than remembered** — existence, liveness, ownership, and ownership ∧ moment. Bare ownership skips the stop-signal offer for a worker whose shutdown has *finished*, leaving a live sink on a set event that never clears. (arch §9.2)

- **Only the forked *child* is repaired, and its order of work is the contract** — locks and events, then inherited buffers, then the registered handlers — a lock re-initialised after a handler that takes it is a handler that hangs. `before` does not run for a C-level fork at all. (SPEC-039)

- **A value the child inherits is stranded, never merely detached** — `dup2` to `/dev/null` *and* reopen in **append** mode: rebinding alone leaves the old object flushing to the real file, and `"w"` truncates a log shared with the parent. (SPEC-039)


### Event assembly: safety and bounds


- **An event is safe by construction — coerced and bounded once at assembly, not per sink** — `build_event` runs every value through `sanitize.py`, so the bare `json.dumps` calls in `sinks/` are correct by consequence. The unserializable fallback is a type name, never `repr()`. Ceilings bound per *value*. (SPEC-017)

- **A value too large to *render* is replaced, never clipped** — an over-long int becomes `<int: ~N digits>`; a truncated number is silently wrong. Detection is `bit_length()`, never `len(str(v))` — the obvious check raises the very error it prevents. (SPEC-020)

- **A value on the wire is measured on the clock that cannot move** — rotation deadlines are `time.monotonic()`, since a wall-clock deadline is defeated by any step larger than the interval. The *label* stays wall-clock wherever one exists. (SPEC-031)


### The sink contract: delivery and its verdict


- **The sink is a durable buffer, not the final store** — ship to SQS, which absorbs spikes and outages, and let a separate consumer index into ELK. `StdoutSink` is the zero-dep default. (arch §8, §9.1)

- **A FIFO message group is a trace, not the process** — `MessageGroupId` defaults to the event's `trace_id`, which keeps traces parallel instead of serialising everything behind one group. Ordering is best-effort across a retry boundary, and sender faults are abandoned rather than re-sent. (SPEC-016)

- **A positional response adjudicates all of a chunk or none of it** — a mismatch is evidence of misalignment, so even the overlapping prefix is refused. What cannot be adjudicated is abandoned and counted (`dropped_unadjudicated`), never retried. (SPEC-018)

- **A sink that delivered nothing raises; one that delivered something reports** — a sink that absorbs a total failure is a sink the worker *believes*: retry never engages, counters stay at zero, and `flush()` returns `True` while everything is lost. (SPEC-026, SPEC-043)

- **A sink that released its transport refuses; one that released nothing keeps accepting** — both halves bind — three shipped sinks lost every post-`close()` event, while making the stateless sinks refuse would invent loss where a batch would have landed. (SPEC-032)


### The sink contract: waiting, concurrency and shutdown


- **A sink's wait is bounded, interruptible, and never taken on a destination's word** — one drain thread means a sink's backoff pauses *all* delivery and spans `shutdown()`, so every sink waits on the worker's stop event and `time.sleep` is the wrong primitive. (SPEC-027, SPEC-038, SPEC-047)

- **A sink tolerates concurrent callers; the library cannot serialize them for it** — a level call with no active span emits on the *caller's* thread, so `emit`/`close` are called concurrently against one sink object. It is a requirement on implementations, not a promise the library can keep. (SPEC-028)

- **A terminal `shutdown()` and a captured sink are both reported, not prevented** — logging after `shutdown()` is still *accepted* — refusing would hide the mistake and restarting the worker would fight a process trying to exit. `Health` reports the **pair** `retired` + `submitted_after_shutdown`. (SPEC-030)

- **A sink handoff is owned by whoever is delivering, and "a worker exists" is not "a worker owns this sink"** — the orphan path records the sink **object** an emit reached, because `configure()` assigns `_config.sink` *before* the swap runs and a boolean cannot tell the two apart. (SPEC-033)

- **A shutdown shortens a *wait*; it must never skip *work*** — the stop event is set for the whole of `_final_drain`, so any sink consulting it to do *less* degrades itself on the exit drain — the one path a serverless process has. (SPEC-038)

- **A destination's limit is found by halving the *budget*, not the chunk** — recursive chunk-halving is `2N-1` requests because each accepted size is rediscovered in every branch; capping the recursion *depth* instead hides the loss. (SPEC-038)

- **A subclass that inherits a method is still in the roster** — scope a roster on defines-or-inherits: keying on *defining* a method made membership a function of where code sits, and dropped five classes out of two lints in one commit with the suite green. (SPEC-038)

- **A bound is only a bound if it is measured where it binds** — assert **CPU** time, not wall clock, or a busy-spin passes; a timeout applied per *item* is `n × timeout`, not a bound. (SPEC-038)

- **A process releases only a transport it acquired *here*, and unrecorded must be unclaimable rather than merely unreleasable** — the record is stamped when the library is *handed* a sink, over the whole reachable graph, and every close consults it. Write-once alone defends only a record that already exists. (SPEC-042)


### Failure paths and diagnostics


- **A dead worker is reported, not restarted — and as a *reason*, not a liveness flag** — `Health.stopped_reason` is `None` for a live worker, a never-created one, **and** a cleanly shut-down one, so it extends the alert idiom by a term. No auto-restart: a thread that resurrects itself fights a process trying to exit. (SPEC-019)

- **Every path the caller stands on is total, and a swallowed fault is announced by *type*** — never `BaseException` — a `KeyboardInterrupt` or `SystemExit` is the operator's or the runtime's intent and must reach the caller. (SPEC-025)

- **One module writes every diagnostic, so the rules are applied once rather than remembered twenty-eight times** — `_diag` owns `absorbed`/`lost`/`rejected`, and an exception is named by `type(exc).__name__`, never `repr()`. Twelve sites printed the repr and two were unguarded before the rules had one home. (SPEC-029)


### The public API surface


- **Logs-only, send everything for now** — no metrics or OTel-native traces. Sampling is deferred and **unbuilt** — no `should_send` exists in code — and the per-span flush makes the pipeline span-outcome-ready, *not* tail-sampling-ready. (arch §10, §13)

- **An extra's floor is a published contract — moved deliberately, never by a bot** — `versioning-strategy: increase-if-necessary` stays, so floors move only when a human decides they should. A floor raise is a contract change. (SPEC-022)

- **A public accessor hands out a copy; the library reads the live object** — a public getter documented "do not mutate" is a promise the caller's slip breaks silently; `_live_config()`/`_live_baggage()` are the per-event reads. (SPEC-034)

- **A result that can grow a reason must stop being a `bool` before 1.0, not after** — a `NamedTuple` cannot be retrofitted — a non-empty tuple is always truthy, so every `if flush():` would silently keep passing. `FlushResult`/`ContinueResult` grow by new reason values only. (SPEC-034)

- **A protocol that is exported is a protocol that will be inherited** — `Sink`'s members are `@abstractmethod`: empty bodies let a subclass with one typo instantiate happily and return `None` from `emit`, losing events with every counter at zero. (SPEC-034)


### Release, supply chain and naming


- **Version comes from Git tags, published to PyPI as `log-foundry`** — tags cut releases; merges to `main` publish `.devN` pre-releases. (SPEC-012)

- **Every action is pinned to a commit SHA, and the pins are maintained, not frozen** — a mutable tag on a workflow holding `id-token: write` against PyPI is a silent path from a third-party repository into every consumer's install. A lagging pin fails loudly; a compromised action fails forever. (SPEC-022)

- **A scanner that exits zero has not said "clean"** — the alert count is the verdict, never the check mark — zizmor and CodeQL pass the job regardless of findings by design, and only `dependency-review` fails a build. (SPEC-022)

- **An SBOM describes the published artifact, and is generated from it** — `make-sbom.py` describes the built wheel installed with every extra, and runs from a *second* venv or it lists its own ~30 dependencies as the library's. (SPEC-023)

- **Release assets are attached to a draft, never to a published release** — immutable releases freeze assets at publish, so it is create-as-draft → upload → publish. Deleting an immutable release does **not** free its tag name; a botched release is repaired only by a new version tag. (SPEC-023)

- **`pip-audit` gates, and audits the extras or it audits nothing** — `dependency-review` only sees a PR's dependency *diff*, so the weekly re-examination is the point. `--no-root` is load-bearing and `--strict` is on, because a silently skipped package is a silently unaudited one. (SPEC-023)

- **One name everywhere: `log-foundry` / `log_foundry`** — the import package was renamed from `log_forge` in `v0.2.0` to match the distribution name — breaking for `0.1.x`, no shim. Historical `log-forge` mentions survive only where they name the PyPI-rejected original.


### Working rules for findings and open items


- **An open item is closed by being fixed, settled, or recorded as a constraint — never deleted** — a note merely removed takes its reasoning with it, and a reader cannot tell a live defect from a decision that reads like one. Supersede in place, struck through and marked with the spec that closed it. (SPEC-021)

- **A read-only finding is not closed until it has been run, and the job that runs it needs a floor rather than an exit code** — fourteen sink modules reach a third party through eleven optional extras and none was ever executed, so the sinks most likely to be in production were the least verified. (SPEC-041)

## Out of Scope (don't build)

Metrics or OTel-native traces · querying / dashboards / alerting (that's ELK/downstream) · log routing
beyond one configured sink per process · **auto**-instrumented propagation — no HTTP-client patching,
middleware or boto3 hooks; the caller moves the header (cross-process continuation itself shipped in
SPEC-014) · `tracestate` · sampling · "follows-from" span relationships (deferred).

---

## Session Workflow

**Start:** (1) this file, then **`@docs/process.md` — read it every session, not once**: it is the contract this file only summarises; (2) the spec you're implementing (`@docs/specs/SPEC-XXX`); (3) skim `@docs/component-inventory.md` for reuse and pull only the architecture.md section / implementation-guide phase you need — don't read architecture.md whole. (4) Confirm CI is green on `main`; investigate failures before building. (5) Branch from fresh `main`, and set the spec's `Status: In Progress` + its INDEX row in that first commit. (6) Generate an implementation plan from the spec's phases, validate it against the spec (FRs + acceptance criteria covered, reuse used, nothing out of scope), **send it to one fresh-context reviewer**, then build. (7) Decide the **PR grouping** yourself and say it in a sentence — as few PRs as the dependencies allow. It draws no reviewer.

**During:** those two reviews are the only gates on *starting*, so **build straight through to completion**, summarizing a phase in passing but never ending the turn on it (a summary that ends the turn *is* a request for approval). Every file-changing task goes on its own branch and opens a PR — never commit to `main` directly. Specs carry no Open Questions — triage emergent issues by kind: **reversible/technical** ones you decide in-session (update the spec if scope changes); **product-changing or ambiguous** ones you stop and escalate to the human with options + a recommendation, never silently decide.

**Review — three artifacts, one blocking gate:** a **spec**, an implementation **plan** and a **diff** each go to a reviewer in a **fresh context** (new session or subagent), never the context that produced them. **Counts: one on the spec, one on the plan, two on the diff — and no fifth artifact draws a reviewer** (the PR grouping is the implementer's call, made in a sentence) — two *frames*, not two rounds, both before the push; the diff gets two because it is the widest artifact and the last one before the branch is public, one frame reading it against the spec's criteria and the domain rules, the other starting from the **system** rather than the diff. Both counts are **floors**, and they work differently: on a **diff**, a clean first review does *not* close the gate; on a **spec or plan**, one clean review *does*, and what makes one a floor is that a revised artifact re-enters the gate as a new one. **A REPLACED artifact restarts its gate; a revised one does not** — fixing findings in place answers the gate, but throwing the design away and writing a new one produces something nobody has reviewed. Where it happens decides the cost: replaced at the spec or plan gate, the later gates still see it fresh and nothing extra is owed; replaced *after* the diff reviews, those reviews examined something that no longer exists, so it owes a fifth — in the frame that catches the replacement's class of defect, usually execution — said out loud rather than spent quietly. Measured: SPEC-045 was replaced late and the fifth reviewer found both remaining blocking gaps, each a mutant that passed the whole suite; SPEC-046 was replaced at its spec gate and the ordinary four then found defects at every remaining gate, two of them correctness bugs before the first line of code. A spec is not Draft-ready, a plan does not start code, and **a branch does not reach the remote**, until every finding is either **fixed** or **flagged** — a rejection costs a sentence out loud, and only goes in writing when it carries a lesson worth keeping. **The diff review gates the PUSH, not the merge**, because **green CI is not a review**: CI cannot see a test that passes against the bug it claims to catch, a lock taken in the wrong order, or an acceptance criterion ticked with no evidence — SPEC-028 merged green and a review then found a sink that could hang an application thread forever. Cap same-frame rounds at two, then rotate the frame; exit on the *class* of finding shrinking, never on a round count — and never below the counts above. Brief the reviewer that "this is sound" is a valid verdict, make it cite where it looked, and tell round N+1 what round N fixed. On a code diff, one of the two must **build** the thing, not read it — and every reviewer runs the repo's gates against the branch. When the risk is what a change *removed*, enumerate the population with a sweep instead of reviewing a sample. Full contract: `@docs/process.md` §3 → *The reviewer contract*.

**Spec size — one slice, not a whole feature:** aim for **3–6 FRs**; past **8**, write a second spec beside the first and record the pair's build order as an arc in `@docs/specs/INDEX.md` rather than growing one. The second spec restarts at FR-001 (IDs are spec-local). `spec-lint.sh` **warns** above 8 rather than failing, because a genuinely indivisible spec may sit above the line — say so in one line under *Scope → In Scope* and let the reviewer accept or reject it. Cut on a seam the system already has, never at the FR where the count ran out. Full rule: `@docs/process.md` §4.

**PRs & main:** before pushing, get the diff through the review gate above, and get this repo's five gates green locally — `poetry run ruff check .`, `poetry run mypy`, `poetry run pytest`, `sh scripts/spec-lint.sh`, `sh scripts/docs-lint.sh`. **`ruff format` is NOT among them**: it is not a CI gate and this repo is not clean under it, so format only the files you edited (process.md §6). Watch every PR to completion and merge it as soon as CI is green — never open-and-abandon. **Key the watch on the current head sha** — a bare `gh pr checks --watch` can exit clean against the *previous* commit's checks. `main` is always watched: after any merge confirm it went green, and if `main` fails, diagnose immediately and fix it with a new PR before anything else.

**On spec completion — keep the always-loaded files lean:**
1. Set the spec file's `Status: Completed`.
2. Update the one-line row in `@docs/specs/INDEX.md` (status only — don't add prose).
3. Write a short delivery doc at `docs/spec-delivery/SPEC-XXX-<name>.md` from the template.
4. If it added reusable modules, add a one-line row to `@docs/component-inventory.md`.
5. A *new architectural decision* gets its **full entry in `@docs/decisions.md` first**, then one line in Key Decisions above — never a paragraph there, and never the only home of the fact. Add it to the AREA it belongs to, replacing or extending that area's clause rather than appending a new one. If it **supersedes** an earlier decision, add an in-place superseded marker (short blockquote) at every doc site still stating the old claim. Reasoning belongs in the spec/delivery doc.

**Doc-size guardrail:** this is the always-loaded file — if an edit pushes a section past a few
lines, the detail belongs in a `docs/` file behind a pointer. Same for `INDEX.md` (status rows only)
and the component inventory. **Key Decisions is grouped by AREA and carries fences, not history** —
it is not a per-spec changelog, and `## Specs` is not one either. Both were exactly that until
2026-09-02, when this file was cut from 89,340 bytes to a digest over `@docs/decisions.md`; the rule
had been stated here and in `@docs/process.md` §5 the whole time and lost anyway, roughly forty
times running. **`scripts/docs-lint.sh` now enforces it on every PR** — the byte budget, the digest
line cap, and an entry in the register behind every digest line. The budget is a **ratchet**: when it
fires, cut and re-ratchet at the new measurement, never raise it to fit the edit in hand. Full rule
set: `@docs/process.md` §5 → *Anti-regrowth & doc hygiene*.