# Completed Spec — SPEC-012: PyPI Publishing and Dynamic Versioning

## What was completed?

`log-forge` is published on PyPI as **[`log-foundry`](https://pypi.org/project/log-foundry/)**;
first stable release **`v0.1.0`**. The version is no longer hand-edited — it is derived from Git
tags at build time, so it cannot drift from what Git says.

- **`pyproject.toml`** — `[project]` declares `dynamic = ["version"]`, `[tool.poetry]` keeps a
  `0.0.0` placeholder, build backend is `poetry_dynamic_versioning.backend`. `metadata = false`
  drops the `+<hash>` local segment, which PyPI refuses outright (FR-001).
- **`log_forge.__version__`** — read from `importlib.metadata.version("log-foundry")`, falling
  back to `"0.0.0"` in an uninstalled checkout; exported in `__all__` (FR-002).
- **`.github/workflows/release.yml`** (new) — `test` (reuses `ci.yml`) → `build` →
  `publish-dev` / `publish-release` (FR-003..FR-007).
- **`ci.yml`** — gained a `workflow_call` trigger; PR/push behavior unchanged (FR-007).

| Trigger | Version | Published as | `skip-existing` |
|---|---|---|---|
| merge to `main` | `X.Y.Z.devN` | dev pre-release | on |
| push tag `v*` | `X.Y.Z` | stable release | **off** |

**Deviations from the Draft, all deliberate:**

- **Distribution renamed `log-forge` → `log-foundry`.** PyPI's similarity check collapses
  separators, so `log-forge` collides with the unrelated, pre-existing `logforge`. Only the
  distribution name moved — the import name is still `log_forge`, and no module or API was
  renamed. Anything doing `importlib.metadata` lookups must use `log-foundry`, or `__version__`
  silently degrades to `"0.0.0"`.
- **TestPyPI dropped entirely.** It is a separate instance needing its own account and 2FA. Dev
  pre-releases on production PyPI preserve the property that mattered — a tagged release is
  never the first time the upload path runs. Cost: every merge permanently consumes a
  `X.Y.Z.devN` version number. `0.0.2.dev1` and `0.0.2.dev2` exist on PyPI from bring-up; they
  sort below `0.1.0` and are harmless.
- **Baseline tag is `v0.0.1`, not `v0.1.0`.** The baseline must be pushed *before* `release.yml`
  exists (a `v*` push is what triggers a release), which consumes it — so the debut release
  became `v0.1.0`.
- **Two publish jobs, not one.** A stable release deliberately omits `skip-existing` so
  re-pushing a published tag fails loudly instead of reporting a green no-op.
- **Added beyond the spec:** `twine check`, a no-local-segment guard, an exactly-one-sdist-and-wheel
  assertion, and a tag-vs-built-version check that blocks a release publishing under the wrong
  number. Artifact actions run on Node 24 (`upload-artifact@v7` / `download-artifact@v8`).

## What changed from earlier specs?

Nothing in the runtime, public API, or sinks. Packaging only: the static `version` key is gone
(do not add it back), and `README.md` / `CLAUDE.md` record the install-vs-import split.

**Gotcha:** `poetry-dynamic-versioning` rewrites `pyproject.toml` in place during a local
`python -m build` and the round-trip can reorder keys. Check `git diff` after building locally;
CI is unaffected (fresh checkout).

## Verification

Local gates green — `ruff` clean, `mypy --strict` clean (46 src files), `pytest` **275 passed**
(5 new version tests, including the not-installed fallback). Version derivation verified against
real tags from a clean clone: at a tag → exact `X.Y.Z`; N commits past → `X.Y.Z+1.devN` with no
local segment. The tag-match guard was tested against pass/mismatch/dev-version cases.

Verified in production, not just locally: `publish-dev` shipped `0.0.2.dev1`/`0.0.2.dev2` on
merges, `publish-release` shipped `0.1.0` on the tag, each skipping correctly on the other
trigger. `pip install log-foundry` into a clean venv installs **`0.1.0`** (not a `.devN`),
imports as `log_forge`, reports `__version__ == "0.1.0"`, and traces a call end to end.
Trusted Publishing (OIDC) authenticated on the first attempt with no stored credential.
