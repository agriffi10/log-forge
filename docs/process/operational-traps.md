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
- _(add your own as they bite — one line each, with the fix.)_

