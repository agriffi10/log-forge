# Operational traps that only bite in CI / on deploy

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
- **`poetry run <tool>` silently falls back to a global tool when the worktree's `.venv` is
  unpopulated.** It reports failures that belong to a *different* interpreter's site-packages —
  in a fresh worktree it produced this project's most-warned-about symptom, eight
  `Unused "type: ignore"` mypy errors, while `poetry run python -c "import boto3"` in the same
  venv raised `ModuleNotFoundError`. It also writes `.mypy_cache`, so the correct mypy then reads
  a cache built by another version and keeps failing. Run `poetry install --with dev` in every new
  worktree first, confirm with `poetry run which mypy`, and `rm -rf .mypy_cache` after any
  suspected fallback. A `pytest` fallback is worse than noisy: it means a "no-extras gate" run was
  never no-extras. Bit in SPEC-041.
- **A service that has bound its port has not necessarily begun serving.** Logstash's `http` input
  accepts TCP connections before its pipeline runs, so a connect-based readiness probe goes green
  and the first real request is met with `ConnectionResetError`. It passed on a laptop for weeks
  of container uptime and failed on the integration job's first CI run. A readiness probe asks the
  question a client asks — a real request, or Kafka's metadata call, never a bare connect. Bit in
  SPEC-041.
- **A branch cut before the frontmatter layout conflicts somewhere that does not say so.** Git reads
  the old `docs/specs/INDEX.md` as renamed to `docs/specs/ARCS.md`, so an edit to the old index conflicts
  in ARCS.md and re-inserts the status table; every gate stays green if both sides are kept. Take
  HEAD's ARCS.md, and move the edit: a status to the spec's `spec_status`, an order to `arc`/`arc_order`,
  narrative to the arc's heading. A spec with the old header gets frontmatter in place of it
  (`knowledge-layout.md` → *A spec's keys*). A modify/delete on `docs/decisions/<area>.md` is **not**
  resolved by `git rm` alone, which drops the new entry with the gates green: write the entry as its own
  file in `docs/decisions/<area>/`, or rewrite the decision it changes and date it in its History. Then
  `sh scripts/gen-index.sh`. Neither gate reads for conflict markers, so grep for them. Frozen records —
  Completed specs, delivery docs, audits, release notes — keep the paths they were written with:
  `docs/decisions/<area>.md` now reads `docs/decisions/<area>/`, and `docs/specs/INDEX.md` reads
  `docs/specs/index.yaml` or `docs/specs/ARCS.md`.
- **A CodeQL default-setup run on `main` can go red on its upload alone, and only a new commit
  clears the red.** Run `42062242018` on `7dbe5254` failed "Analyze (actions)" at "Perform CodeQL
  Analysis": the SARIF exported, then "Uploading results" stalled and the step failed with no error
  line, so no analysis for that commit appears under `code-scanning/analyses`. `gh run rerun`
  refused that run ("cannot be retried"; its event is `dynamic`), and the weekly scheduled re-scan
  does not help, because `queue.sh` reads a sha as red while any run on it failed. The job log is
  what decides: one ending at "Uploading results" is an upload stall that any new commit clears,
  while an extraction or query failure is an ordinary red needing a real fix. A missing
  `/language:<lang>` row for the failed job's language only confirms nothing was uploaded, which
  is true of both. Either way it is an `IS RED`, handled as `scripts/pr-queue/PROTOCOL.md` says.
- **`gh pr merge <n> --squash` refuses on this repo; `--admin` is the normal path, not a trap
  door.** The `main` ruleset squash-merges only and sets `require_last_push_approval`, which asks for
  an approval from someone who did not push — unobtainable for a PR authored and pushed by the only
  account on this repo, whatever `required_approving_review_count` says. The plain merge refuses with
  "the base branch policy prohibits the merge"; `gh pr merge <n> --squash --admin --delete-branch`
  merges. #230, merged as `3a4d337`, is the one this was written from, and no PR merged between #121
  and #231 carries a GitHub review at all. That count is not evidence the bypass was forced: reviews
  here happen as fresh-context diff reviews before the push, so none was ever requested on the
  platform. The refusal names the base branch policy rather than a clause, and the same rule also
  sets `required_review_thread_resolution` and `require_extra_approval_for_unattributed_changes`, so
  treat the approval requirement as the reason rather than any one parameter. The bypass replaces
  the branch protection, never the review gate (`session-rhythm.md` → *Landing the spec*).
- _(add your own as they bite — one line each, with the fix.)_

