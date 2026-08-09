# The Spec-Driven Development Process

How work gets done in this repo. `CLAUDE.md` carries the terse, always-loaded rules; this doc is the
fuller **method** they point to — read it once to understand the rhythm, then let CLAUDE.md's Session
Workflow be your in-session checklist. It is read on demand, not every session.

Goal: each feature is **specified before it's built**, built in **reviewable phases** off a
**validated plan**, landed as a **single PR on green CI**, and recorded in **lean, layered docs** so
the next session starts cheap.

---

## 1. Where truth lives (and why it's layered)

The docs are tiered by how often they're loaded. Keep each tier in its lane — the always-loaded tier
is deliberately small and **must not regrow**.

| Tier | File(s) | Loaded | Authoritative for |
|---|---|---|---|
| Always | `CLAUDE.md` | every session | conventions, key decisions, session workflow |
| Status | `docs/specs/INDEX.md` + each spec header | on demand | spec **status** (one row per spec) |
| The work | `docs/specs/SPEC-XXX-*.md` | the one you're building | requirements + phases |
| Why | `docs/architecture.md` | the *section* you need | design rationale + Known Constraints |
| Reuse | `docs/component-inventory.md` | skim for reuse | modules/services/components already built |
| Rulebooks | `docs/best-practices/INDEX.md` → domain doc | the section(s) you need | domain coding rules (Python) |
| History | `docs/spec-delivery/SPEC-XXX-*.md` | when a dependency points to one | what a past spec shipped |
| Method | `docs/process.md` (this file) | once | how we work |

**Context discipline (session-start token cost matters):**
- Read **only the current spec** in full.
- **Never** read `architecture.md` or delivery docs whole — pull the one section you need.
- Delegate *dependency* delivery-doc reading to a subagent brief rather than loading it into the main
  loop.

---

## 2. The spec lifecycle

Specs move **Draft → In Progress → Completed** (the status in each spec's header is authoritative; the
`INDEX.md` row mirrors it).

- **Draft** — written and refined, but **do not build until told.** Specs are often authored well
  ahead of implementation. A Draft spec sitting in the repo is not a signal to start it. A spec is not
  Draft-ready while it still has unresolved questions (see §4).
- **In Progress** — exactly one spec at a time is in flight. Set when you branch to build it.
- **Completed** — merged on green CI, delivery doc written (see §5).

**Arcs.** Related specs can be grouped into *arcs* with an explicit **build order** documented in
`INDEX.md`. Build in that order; arcs can have non-obvious dependencies.

---

## 3. The session rhythm

This is the operating loop, start to finish. CLAUDE.md's *Session Workflow* is the condensed version.

**Start of session**
1. Read CLAUDE.md, then the **current** spec in full. Don't infer scope from a prior conversation —
   read the spec file as it exists now.
2. Skim `component-inventory.md` for reuse; pull only the `architecture.md` section / dependency
   delivery-doc you actually need.
3. Confirm CI is green on `main`. Investigate failures before building.
4. **Branch from fresh `main`.**
5. **Generate and validate a plan before writing code.** Turn the spec's Implementation Phases into a
   concrete implementation plan, then validate it against the spec — every FR + acceptance criterion is
   covered, reuse from `component-inventory.md` is used, and nothing out of scope crept in. Confirm the
   plan before building. The validated plan — not per-phase checkpoints baked into the spec — is what
   gates the work.

**During the build — one spec, in phases**
- Every file-changing task is done on its **own branch** and opened as a **PR** — automatically, without
  waiting to be asked. Never commit to `main` directly.
- Before opening a PR, run the project's **formatter, linter, and unit tests** locally and get them
  green. These quality gates are a pre-PR step — don't push red and leave CI to discover it.
- Work the validated plan's phases in order. After each phase, **stop and summarize** what was built and
  how it maps back to the plan before continuing.
- Before writing Python, route through `docs/best-practices/INDEX.md` → `python/python.md` and load only
  the relevant section(s). Apply the rules as you write; flag (don't silently break) any that conflict
  with existing code or the repo's `ruff`/`mypy` config.
- Specs ship with **no Open Questions** — they're resolved during authoring (§4). An issue that emerges
  mid-build is triaged by *kind*, not parked:
  - **Reversible / technical** (naming, file layout, which helper to reuse, an obvious bug fix): just
    decide in-session and keep moving. If it changes scope or contradicts the spec, **update the spec**
    rather than leaving the divergence implicit.
  - **Product-changing / ambiguous** (anything that alters behavior the user would notice, or a call
    with no clearly-right answer): **stop and escalate to the human.** Don't silently pick — surface the
    options with a recommendation. Auto-deciding these is how an autonomous run drifts away from what
    was actually wanted.

**Reviewing the work — in a fresh context**
- Code review and verification run in a **fresh context** (a new session or a subagent), **never the
  session that wrote the code.** A self-reviewing agent assumes its own output was intended and rubber-
  stamps it; a clean reviewer catches what the author can't see. The reviewer checks the diff against
  the spec's acceptance criteria **and the relevant `best-practices/` rules** (route via its INDEX),
  not just "does it look fine."

**Rotating the frame — why round count is the wrong exit criterion**

Review rounds converge on the frame they are given, not on correctness. Measured on SPEC-033
(2026-08-07): **eight** rounds of independent review — two on the spec, three on its revisions,
three on the code — after which the merged result still carried two defects, one of which was a
regression of a guarantee an earlier spec had shipped. Rounds 1–3 found design defects; rounds
4–8 increasingly found wording. The *first* differently-framed pass afterwards found ~25 defects,
several of them years old.

So the loop rotates the frame instead of adding rounds:

- **Cap same-frame review at two rounds.** After the second, switch frame rather than iterate:
  adversarial *execution*, whole-module fresh eyes (not the diff), or concurrency.
- **Exit on a new frame finding nothing**, not on the current frame converging. "The reviewer
  found nothing" means "nothing within the frame I gave it."
- **At least one pass must start from the library, not the diff.** Every diff-scoped review is
  structurally blind to a defect that predates the diff — which is how `flush()` came to be blind
  to an open span, breaking the documented serverless recipe, through ten specs of review.
- **Reasoning finds wording; execution finds defects.** Every finding that mattered in the
  2026-08-07 audit was *run*: 5,980 events in one emit, 19 of 60 forked children hung, 0 of 9
  events delivered. Require a reproduction, not an argument.
- **A spec touching lifecycle or concurrency gets an execution harness, not a review.**
  `tests/conftest.py::run_concurrently` and injected preemption points exist for this; a race is
  not findable by reading.

**When a review changes a rule, re-audit the rule — not the line**

A review finding is usually reported as an instance ("this call site uses the wrong predicate").
Fixing the cited line and moving on is how the same defect survives repeated review: three
separate reviewers told SPEC-033 "ownership, not liveness", each naming a different call site,
each was fixed, and a fourth site shipped broken.

- When a finding is an instance of a **rule**, the fix is a test that enumerates **every** site of
  that rule and asserts each one's category — the same discipline the sink rosters already use,
  and for the same reason: a hand-maintained list rots and a derived one does not.
- **Scope added mid-review restarts the clock.** Widening a spec in response to a finding is often
  right, but the widening arrives late, gets the least scrutiny, and inherits confidence it has
  not earned. Two of SPEC-033's regressions were in scope added during its own review.

**Landing the spec — watch PRs and watch `main`**
- **Every PR is watched to completion and merged as soon as CI is green** — never open a PR and walk
  away. A spec's PR merges only on green.
- **`main` is always watched.** After any merge, confirm `main`'s build went green. If `main` fails,
  **diagnose immediately and fix it with a new PR** — a red `main` is the top priority and blocks
  starting the next spec.
- **Re-verify `main` is green** before starting the next spec (land before starting the next).
- Then run the completion ritual (§5).

---

## 4. Authoring a spec

Specs are written from `docs/templates/spec-template.md`. What makes a spec *buildable*:

- **Overview** — user/business intent, no implementation detail. Understandable cold.
- **Scope: In / Out** — explicitly list what's *excluded*, especially anything a reader would
  reasonably assume is included.
- **Functional Requirements** — one FR per discrete, testable behavior, with binary pass/fail
  **Acceptance Criteria** covering happy path, error path, and edges. Sequential IDs so a prompt can
  say "implement FR-001 through FR-003 only."
- **Data Model / Interface Contract** — language-native types, not prose. Explicit shapes produce
  better-typed output. Note the target path.
- **Implementation Phases** — each phase is one session's worth of work and maps to a discrete,
  reviewable unit. Phases are the input to the implementation plan generated at build time (§3); don't
  bake per-phase checkpoints into the spec.
- **No Open Questions.** Resolve every decision while authoring — a spec doesn't reach Draft-ready with
  unanswered questions. Issues that only surface during the build are handled in-session (§3), not
  parked in the spec.

`scripts/spec-lint.sh` enforces the structural side of this in CI: it **fails** a spec that is missing
a required section or that contains an "Open Questions" / "Checkpoint" heading, and **warns** on
unfilled placeholders or FRs without acceptance criteria.

---

## 5. Completion ritual (keep the always-loaded tier lean)

When a spec is done, in the same pass:
1. Set the spec file header `Status: Completed`.
2. Update its one-line row in `docs/specs/INDEX.md` (**status only** — no prose).
3. Write a **short** delivery doc at `docs/spec-delivery/SPEC-XXX-<name>.md` from
   `docs/templates/spec-completion-template.md` — *what shipped + what changed*, under ~40 lines, **no
   code/config pasted** (the code + component-inventory are the source of truth for reuse).
4. If reusable modules/services/components were added, add a **one-line** row to
   `docs/component-inventory.md`.
5. A *new architectural decision* gets **one line** in CLAUDE.md's Key Decisions (+ a pointer) — never a
   paragraph. Reasoning lives in the spec/delivery doc.

**Anti-regrowth.** If a doc disagrees with the code, fix or delete it — don't let stale state
accumulate. Don't add prose to the always-loaded tier.

---

## 6. Operational traps that only bite in CI / on deploy

These pass locally and fail later — check them as part of the work, not as an afterthought. Keep this
list project-specific; seed it the first time a trap bites and never again.

- _(example)_ A test runner that needs a specific working directory or config to pick up its
  environment — running it from the repo root vs. the package dir changes behavior.
- _(example)_ Config/env values the app reads at runtime must also be wired into the deploy/build
  environment, or production ships them undefined.
- **`OSError`'s concrete type is per-platform.** CPython maps an `errno` to an `OSError` *subclass*
  at construction, from a table that differs by OS — `OSError(111, …)` is a `ConnectionRefusedError`
  on CI's Linux and a plain `OSError` on macOS (ECONNREFUSED is 61 there). Never assert a hardcoded
  type name for a constructed `OSError`; derive it (`type(exc).__name__`). Bit in SPEC-029 Phase 3.
- **`ruff format` is not a CI gate, and this repo is not clean under it.** Running it over a
  directory rewrites files your change never touched (and it reformats code blocks inside `.md`).
  Format only the files you edited, and check `git status` before committing.
- _(add your own as they bite — one line each, with the fix.)_

---

## 7. Project ground rules that shape the process

- **Don't add dependencies** without first noting them in CLAUDE.md's Tech Stack.
- _(add the load-bearing constraints that shape how work is done here — e.g. "no backend," "library
  must stay dependency-free," "single supported runtime version.")_
