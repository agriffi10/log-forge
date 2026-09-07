# The session rhythm

This is the operating loop, start to finish. It loads with CLAUDE.md at launch, so it is read every
session by mechanism rather than by instruction; CLAUDE.md's *Session Workflow* is the condensed
version. **If this file and CLAUDE.md ever disagree, CLAUDE.md wins** — fix the drift here in the
same session you notice it (two hand-synced copies of one procedure is how numbering bugs happen).

**Start of session**
1. Read CLAUDE.md, then the **current** spec in full. Don't infer scope from a prior conversation —
   read the spec file as it exists now.
2. Skim `component-inventory.md` for reuse; pull only the `architecture.md` section / dependency
   delivery-doc you actually need.
3. Confirm CI is green on `main`. Investigate failures before building.
4. **Branch from fresh `main`**, and set the spec header's `Status: In Progress` plus its `INDEX.md`
   row in the same commit — that transition is what makes "exactly one spec in flight" (`spec-lifecycle.md`) legible
   to the next session, and nothing gates it, so it is missed by being skipped rather than by failing.
5. **Generate and validate a plan before writing code.** Turn the spec's Implementation Phases into a
   concrete implementation plan, then validate it against the spec — every FR + acceptance criterion is
   covered, reuse from `component-inventory.md` is used, and nothing out of scope crept in. The plan —
   not per-phase checkpoints baked into the spec — is what gates the work.
6. **Put the plan through the reviewer gate before the first line of code** (`reviewer-contract.md`). A plan is reviewed for the same reason a PR is: the author cannot see what they assumed.
7. **Decide the PR grouping yourself, and say it in a sentence.** It is the implementer's call, not
   a reviewer gate. Group consecutive phases into as few PRs as the dependencies allow — a phase is
   a unit of work, not a unit of PR, and the useful boundaries are inert-vs-live, either side of a
   switchover, a deletion following its last caller, and a release step that must land before what
   depends on it.

**The plan review is the only one that gates the START of the build**, and once it has been
answered, the build runs to completion without checking in. The user
confirmed the work when they set the spec going; a per-phase check-in re-asks a question already
answered, and on a twelve-phase spec it asks it twelve times. This does **not** retire the diff
review — that one gates the push, at the other end of the build, and it is blocking too
(`reviewer-contract.md`). Three reviews stand between a spec going In Progress and its PR
merging: one on the plan before the first line of code, and two on the diff — all three before the
first push. With the spec's own review that is **four artifacts that draw a reviewer** — spec,
plan, diff, diff — and no fifth, unless an artifact is **replaced** rather than revised after its
gate has run (*A replaced artifact restarts its gate*, in `reviewer-contract.md`).

**During the build — one spec, in phases**
- Every file-changing task is done on its **own branch** and opened as a **PR** — automatically, without
  waiting to be asked. Never commit to `main` directly.
- **The diff reviews run BEFORE the push, not before the merge — and there are two of them.** Commit
  locally, run the gates, send the diff to **two** fresh-context reviewers in **different frames** —
  one reading the change against the spec's criteria and the Python rulebook, the other starting from
  the system and, for each numbered promise in `docs/invariants.md` the change touches, asking
  whether it still holds on every twin path — fix or explicitly reject every finding, *then* push and
  open the PR. Pushing first and reviewing
  after inverts the gate: the branch is already public, the fixes
  arrive as follow-up commits, and the review reads as commentary on something that has already
  happened rather than as the thing that decides whether it should. Rotating the frame (`reviewer-contract.md`) happens
  in the same window. A push is the point of no return for the review, the same way the merge is the
  point of no return for CI. **Green CI is not a review and never was** — CI cannot see a test that
  passes against the bug it claims to catch, a lock taken in the wrong order, or an acceptance
  criterion ticked with no evidence. SPEC-028 merged green and a review then found a sink that could
  hang an application thread forever; that review is the one that now happens before the push.
- Before pushing, run **this repo's six gates** locally and get them green: `poetry run ruff check .`,
  `poetry run mypy`, `poetry run pytest`, `sh scripts/spec-lint.sh`, `sh scripts/docs-lint.sh`,
  `poetry run python scripts/docstring-lint.py`, and each linter's `-test.sh` corpus if you touched that linter.
  **The last two are in that set and nothing in CI runs either.** They are a pre-push step — don't
  push red and leave CI to discover it. **`ruff format` is deliberately NOT a gate here** — the repo
  is not clean under it and running it over a directory rewrites files your change never touched
  (`operational-traps.md`). Format only the files you edited.
- Work the **reviewed** plan's phases in order, **straight through to completion**. Summarize a phase
  in passing where it is worth saying, but do not end the turn on it — a summary that ends the turn
  *is* a request for approval, whatever its wording says. Re-review the plan only if the phase
  changed it — a phase that revises the plan has produced a new artifact, and it goes through the
  gate as one.
- **An acceptance criterion that cannot settle before the push does not pass the pre-push review —
  it is recorded as owed.** A criterion closing "against a green CI run" is undecidable while the
  branch is still local, and the failure mode is a reviewer ticking it vacuously, which is the exact
  defect the spec review exists to catch. Name it in the PR body as owed, settle it on the green run,
  and do not merge until it is settled. If a spec has several of these, that is the dependency the PR
  grouping is for: the job lands in the PR before the one whose criteria depend on it.
- **Stop only for a question that genuinely needs an answer:** a product-changing or ambiguous call,
  a finding that changes scope, a phase discovering the plan was wrong. Reporting is not the same act
  as asking, and doing the first while intending the second is how the build stalls.
- Before writing Python, route through `docs/best-practices/INDEX.md` → `python/python.md` and load only
  the relevant section(s). Apply the rules as you write; flag (don't silently break) any that conflict
  with existing code or the repo's `ruff`/`mypy` config.
- Specs ship with **no Open Questions** — they're resolved during authoring (`authoring-a-spec.md`). An issue that emerges
  mid-build is triaged by *kind*, not parked:
  - **Reversible / technical** (naming, file layout, which helper to reuse, an obvious bug fix): just
    decide in-session and keep moving. If it changes scope or contradicts the spec, **update the spec**
    rather than leaving the divergence implicit.
  - **Product-changing / ambiguous** (anything that alters behavior the user would notice, or a call
    with no clearly-right answer): **stop and escalate to the human.** Don't silently pick — surface the
    options with a recommendation. Auto-deciding these is how an autonomous run drifts away from what
    was actually wanted.

**Landing the spec — watch PRs and watch `main`**
- **A branch reaches the remote already reviewed.** The gate above is the precondition for the push,
  so a PR opens carrying work whose findings are already fixed or answered. If a review round happens
  after a push anyway — a late finding, a rotated frame, a reviewer that ran long — its fixes are
  committed and reviewed locally before the next push, rather than each one going up as it lands.
- **Every PR is watched to completion and merged as soon as CI is green** — never open a PR and walk
  away. A spec's PR merges only on green.
- **The merge takes the owner bypass, and that is the normal path here, not an escape hatch.** The
  `main` ruleset squash-merges only and sets `require_last_push_approval`, which asks for an approval
  from someone who did not push — unobtainable for a PR authored and pushed by the only account on
  this repo, whatever `required_approving_review_count` says. `gh pr merge <n> --squash`
  refuses with "the base branch policy prohibits the merge"; the merge is
  `gh pr merge <n> --squash --admin --delete-branch` — #230, merged as `3a4d337`, is the one this
  paragraph was written from, and no PR merged between #121 and #231 carries a GitHub review at all.
  That count is not evidence the bypass was forced: reviews here happen as fresh-context diff reviews
  before the push, so none was ever requested on the platform. The refusal names the base branch
  policy rather than a clause, and the same rule also sets `required_review_thread_resolution` and
  `require_extra_approval_for_unattributed_changes`, so treat the approval requirement as the reason
  rather than any one parameter. **The bypass replaces the branch protection, never the review gate** — the two
  fresh-context diff reviews happened before the push and are what makes the merge safe. If
  `--delete-branch` reports it cannot remove the local branch, a worktree still holds it; remove the
  worktree, then the branch.
- **Key the watch on the current head sha, never a bare `gh pr checks --watch`** — it can exit clean
  against the **previous** commit's checks, and a hand-written shell condition can invert and print
  "settled" while a job is still running. Both report a green that is not there.
- **`main` is always watched.** After any merge, confirm `main`'s build went green. If `main` fails,
  **diagnose immediately and fix it with a new PR** — a red `main` is the top priority and blocks
  starting the next spec.
- **Re-verify `main` is green** before starting the next spec (land before starting the next).
- Then run the completion ritual (`completion-ritual.md`).

