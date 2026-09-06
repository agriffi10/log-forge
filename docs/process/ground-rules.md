# Project ground rules that shape the process

The constraints that decide how work is done here, rather than how the code is written. The code
rules themselves are owned by `CLAUDE.md` (Tech Stack, Code Conventions) and the Python rulebook —
this file states only what a session has to plan around, and points at the owner rather than
restating it.

- **Don't add dependencies** without first noting them in CLAUDE.md's Tech Stack. **The core stays
  dependency-free**: a new runtime dependency goes behind an optional extra, as `aws`/`boto3` does.
  A spec that needs one has a scope question to settle before it has a plan.
- **The supported Python set has one authority — the CI matrix.** Every other statement of it is a
  restatement bound to that matrix, not to its neighbours (`docs/decisions/working-rules.md`). A spec
  that moves the floor moves the matrix first.
- **Two of this repo's gates run nowhere but locally** — `scripts/docs-lint.sh` and
  `scripts/docstring-lint.py`, plus each one's `-test.sh` corpus. Nothing in CI runs either, so a
  session that skips them pushes a branch that is red on a gate no job will ever report
  (`session-rhythm.md` holds the full pre-push list).
- **The public surface is frozen ahead of 1.0** — a change to it is a decision, not an
  implementation detail, and belongs in `docs/decisions/public-api.md` before it belongs in code.
