# The session rhythm

This is the operating loop, start to finish. It loads with CLAUDE.md at launch, so it is read every
session by mechanism rather than by instruction. It is the **one** copy of this procedure:
CLAUDE.md's *Session Workflow* points here rather than restating it, and keeps only what is not a
step: delegation, the review gate, spec size, the multi-session rules, completion and the doc-size
guardrail. **If this file and CLAUDE.md ever
disagree, CLAUDE.md wins** — fix the drift here in the same session you notice it.

**Start of session**
1. Read CLAUDE.md, then the **current** spec in full. Don't infer scope from a prior conversation —
   read the spec file as it exists now.
2. Skim `component-inventory.md` for reuse; pull only the `architecture.md` section / dependency
   delivery-doc you actually need.
3. Confirm CI is green on `main`. Investigate failures before building.
4. **Branch from fresh `main`** — with other sessions running, in your **own worktree** off
   `origin/main`, never the shared checkout — and set the spec's frontmatter `spec_status: In Progress` and
   regenerate the index in the same commit — that transition is what makes "exactly one spec in flight" (`spec-lifecycle.md`) legible
   to the next session, and nothing gates it, so it is missed by being skipped rather than by failing.
   A delegated implementer builds from that commit's SHA on a branch of its own (`agent-roles.md`).
5. **Generate and validate a plan before writing code.** Turn the spec's Implementation Phases into a
   concrete implementation plan, then validate it against the spec — every FR + acceptance criterion is
   covered, reuse from `component-inventory.md` is used, and nothing out of scope crept in. The plan —
   not per-phase checkpoints baked into the spec — is what gates the work.
6. **Put the plan through the reviewer gate before the first line of code** (`reviewer-contract.md`).
7. **Decide the PR grouping yourself, and say it in a sentence.** It is the implementer's call, not
   a reviewer gate. Group consecutive phases into as few PRs as the dependencies allow — a phase is
   a unit of work, not a unit of PR, and the useful boundaries are inert-vs-live, either side of a
   switchover, a deletion following its last caller, and a release step that must land before what
   depends on it.

**The plan review is the only one that gates the START of the build**, and once it has been
answered, the build runs to completion without checking in: the user confirmed the work when they
set the spec going, and a per-phase check-in re-asks that question once per phase. This does **not**
retire the diff reviews, which gate the push at the other end of the build and are blocking too.
The counts — spec, plan, diff, diff, and no fifth unless an artifact is **replaced** after its gate
has run — are CLAUDE.md's *Review* paragraph; `reviewer-contract.md` carries the rest.

**During the build — one spec, in phases**
- Every file-changing task is done on its **own branch** and opened as a **PR** — automatically, without
  waiting to be asked. Never commit to `main` directly.
- **The two diff reviews (CLAUDE.md → *Review*) run BEFORE the push, not before the merge.** Commit
  locally, run the gates, send the diff to the two fresh-context frames, fix or explicitly reject
  every finding, *then* push and open the PR; rotating the frame happens in the same window. A push
  is the point of no return for the review, as the merge is for CI. **Green CI is not a review** —
  why pushing first inverts the gate, and the green merge that proved it: `reviewer-contract.md` →
  *The review gates the push, not the merge*.
- **Once the phases are built, run the completion ritual (`completion-ritual.md`) on this branch**,
  before the gates and the two diff reviews, so the reviewers see its edits and they land in the
  spec's own squash merge.
- Before pushing, run **this repo's six gates** locally and get them green: `poetry run ruff check .`,
  `poetry run mypy`, `poetry run pytest`, `sh scripts/spec-lint.sh`, `sh scripts/docs-lint.sh`,
  `poetry run python scripts/docstring-lint.py`, and each linter's `-test.sh` corpus if you touched that
  linter, the shared frontmatter reader or the index generator — plus `sh scripts/pr-queue-test.sh` if you touched the queue or its hook, and
  `sh scripts/pr-queue/install-test.sh` if you touched its installer.
  **`docs-lint.sh` and `docstring-lint.py` are in that set and nothing in CI runs either.** They are a pre-push step — don't
  push red and leave CI to discover it. **`ruff format` is deliberately NOT a gate here** — the repo
  is not clean under it and running it over a directory rewrites files your change never touched
  (`operational-traps.md`). Format only the files you edited.
- **The gates a document names are a floor, not the set — before every push, also run locally
  whatever CI will run on the branch.** A gate no list names is the one that goes unrun, and green
  from the named gates says nothing about it. Read `.github/workflows/` for the jobs that will
  actually run, not just this list — including after a rebase, when a job can newly go red on work
  that was green before `main` moved.
- Work the **reviewed** plan's phases in order, **straight through to completion**. Summarize a phase
  in passing where it is worth saying, but do not end the turn on it — a summary that ends the turn
  *is* a request for approval, whatever its wording says, and so is a list of options written out in
  prose — when a decision genuinely needs the human, ask it (the triage below says how). Re-review the plan only if the phase
  changed it — a phase that revises the plan has produced a new artifact, and it goes through the
  gate as one. A spec revised mid-build does this from the other side: if a phase now delivers a
  requirement the spec no longer carries, that plan is a new artifact too.
- **An acceptance criterion that cannot settle before the push does not pass the pre-push review —
  it is recorded as owed.** A criterion closing "against a green CI run" is undecidable while the
  branch is still local, and the failure mode is a reviewer ticking it vacuously, which is the exact
  defect the spec review exists to catch. Name it in the PR body as owed, settle it on the green run,
  and do not merge until it is settled. If a spec has several of these, that is the dependency the PR
  grouping is for: the job lands in the PR before the one whose criteria depend on it.
- **Stop only for a question that genuinely needs an answer:** a product-changing or ambiguous call,
  a finding that changes scope, a phase discovering the plan was wrong. Reporting is not the same act
  as asking, and doing the first while intending the second is how the build stalls.
- Specs ship with **no Open Questions** — they're resolved during authoring (`authoring-a-spec.md`). An issue that emerges
  mid-build is triaged by *kind*, not parked — and the test for the kind is **what the wrong choice
  would cost**: a revert or a follow-up commit, decide; a product outcome the user would have to live
  with, ask:
  - **Reversible / technical** (naming, file layout, which helper to reuse, an obvious bug fix): just
    decide in-session and keep moving. If it changes scope or contradicts the spec, **update the spec**
    rather than leaving the divergence implicit.
  - **Product-changing / ambiguous** (anything that alters behavior the user would notice, or a call
    with no clearly-right answer): **stop and escalate to the human.** Don't silently pick — ask with
    the **question tool** (`AskUserQuestion`), the options as selections and the recommended one
    first, and go on with the answer; options written out in prose that end the turn are the same
    stop by another name.
    Auto-deciding these is how an autonomous run drifts away from what was actually wanted.
  - **The spec contradicting itself** (two FRs that cannot both hold): a spec defect, not a preference
    call, and the tie-break is evidence — implement **each** reading, run the suite, and record the
    verdict in the spec. One reading run to red settles nothing. `reviewer-contract.md` (*Adjudicating two FRs that cannot both hold*) carries the
    method, and the three outcomes that escalate instead of settling.

**Landing the spec — watch PRs and watch `main`**
- **The instruction to build the spec is the authorization for everything in this section.** Pushing
  the reviewed branch, opening the PR, watching it and merging it on green are the last steps of the
  build, not new decisions: do not stop to ask for them, and do not stop after announcing them — a
  session whose turn ends at "I'll open the PR" has not built the spec. What still stops the session
  is a product-changing call or a permission the harness refuses; say which, and ask with the
  question tool.
- **A branch reaches the remote already reviewed.** The gate above is the precondition for the push,
  so a PR opens carrying work whose findings are already fixed or answered. If a review round happens
  after a push anyway — a late finding, a rotated frame, a reviewer that ran long — its fixes are
  committed and reviewed locally before the next push, rather than each one going up as it lands.
- **Every PR is watched to completion and merged as soon as CI is green** — never open a PR and walk
  away. A spec's PR merges only on green.
- **The merge takes the owner bypass, and that is the normal path here, not an escape hatch:**
  `gh pr merge <n> --squash --admin --delete-branch`. A plain `--squash` refuses with "the base
  branch policy prohibits the merge", because the `main` ruleset asks for an approval that the
  account which pushed cannot give (the ruleset and the evidence: `operational-traps.md`). **The
  bypass replaces the branch protection, never the review gate** — the two fresh-context diff
  reviews before the push are what make the merge safe. If `--delete-branch` reports it cannot
  remove the local branch, a worktree still holds it; remove the worktree, then the branch.
- **Key the watch on the current head sha, never a bare `gh pr checks --watch`** — it can exit clean
  against the **previous** commit's checks, and a hand-written shell condition can invert and print
  "settled" while a job is still running. Both report a green that is not there.
  `gh pr view <n> --json headRefOid,statusCheckRollup` returns the sha and the checks against it in
  one call, and the shipped `.claude/settings.json` covers it where `gh api` is not.
- **`main` is always watched.** After any merge, confirm `main`'s build went green. If `main` fails,
  **diagnose immediately and fix it with a new PR**, through the queue's red-trunk path
  (`scripts/pr-queue/PROTOCOL.md`) — a red `main` is the top priority and blocks starting the next
  spec.
- **Re-verify `main` is green** before starting the next spec (land before starting the next).

