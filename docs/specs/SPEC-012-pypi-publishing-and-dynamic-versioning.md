# Spec: PyPI Publishing and Dynamic Versioning

**ID:** SPEC-012
**Status:** Completed
**Last Updated:** 2026-07-22
**Depends On:** None

## Overview

Today `log-foundry` is built locally with Poetry and its version is a hand-edited
`version = "0.1.0"` line in `pyproject.toml`; nothing is published anywhere and there is no
release automation. This spec makes the package installable with `pip install log-foundry` by
adding an automated release pipeline, and it removes the hand-edited version entirely so the
version number is derived from Git tags instead. Every merge to `main` builds the package and
publishes a **development pre-release** (`0.1.1.devN`) to production PyPI, which continuously
exercises the exact publish path; `pip install log-foundry` still resolves to the latest stable
release, because pip ignores pre-releases unless `--pre` is passed. A real release happens only
when a maintainer pushes an annotated version tag such as `v0.2.0`. All uploads authenticate
with PyPI's Trusted Publishing (OpenID Connect), so there is no long-lived API token stored in
the repository. The result is that cutting a release is a single `git tag` away, the version can
never drift from what Git says, and the publish machinery is proven on every merge before it is
ever asked to ship a tagged release.

**A single index.** An earlier draft of this spec rehearsed publishing against the TestPyPI
sandbox. TestPyPI is a wholly separate instance requiring its own account and 2FA enrolment;
that overhead was judged not worth it. Publishing dev pre-releases to production PyPI preserves
the property that mattered — the upload path is never first exercised by a tagged release —
at the cost of permanently consuming a version number per merge.

## Scope

### In Scope

- Publishing under the distribution name **`log-foundry`**. PyPI rejects `log-forge` as too
  similar to the unrelated, pre-existing `logforge` project — its similarity check collapses
  separators, so `log-forge` and `logforge` are treated as the same name. The **import name
  stays `log_forge`**: `pip install log-foundry`, then `import log_forge`. Only the
  distribution name changes; no module, package directory, or public API is renamed.

  > **Superseded:** a later change (post-SPEC-012, shipped in `0.2.0`) renamed the import
  > package `log_forge` → `log_foundry` so the install and import names finally match. The
  > paragraph above records the state SPEC-012 delivered.
- Deriving the package version from Git tags with the `poetry-dynamic-versioning` build
  backend, replacing the static `version` field in `pyproject.toml`.
- Exposing a runtime `log_foundry.__version__` attribute sourced from the installed package
  metadata, so the library reports the same version it was built and published under.
- A new `.github/workflows/release.yml` workflow that builds a source distribution (sdist) and
  a wheel, then publishes them.
- Continuous publishing of a development pre-release (`X.Y.Z.devN`) to **production PyPI** on
  every push to `main`, with `skip-existing: true` so re-runs are tolerated.
- Tagged release publishing to **PyPI** on every pushed annotated tag matching `v*`, with
  `skip-existing` **off** so re-tagging an already-published version fails loudly.
- Authenticating both uploads with **Trusted Publishing (OIDC)** through
  `pypa/gh-action-pypi-publish`, using a single GitHub Environment named `pypi`.
- Gating every publish behind the existing lint, type-check, and test suite by reusing the
  current `ci.yml` as a called workflow, so a red build can never publish.
- Establishing an initial baseline tag (`v0.0.1`) so that development versions read sensibly
  before the first real release. The baseline is pushed *before* `release.yml` exists, so it
  never triggers a publish — which also means it is consumed by setup and cannot itself be the
  debut release. The first production PyPI release is therefore `v0.1.0`.

### Out of Scope

- Fully automated version bumping from commit messages (for example `python-semantic-release`
  with Conventional Commits). This spec keeps the version choice in the maintainer's hands via
  the tag; commit-driven bumping can be layered on later without changing the publish jobs.
- Publishing a **stable** release on ordinary merges to `main`. Routine merges publish only
  `X.Y.Z.devN` pre-releases, which pip will not install by default; a stable release is
  deliberate and tag-gated. This is a considered reinterpretation of "publish on merges to
  main" and is called out here so it is not assumed to include stable releases.
- Using the **TestPyPI** sandbox at all. It is a separate instance needing its own account and
  2FA enrolment; dev pre-releases on production PyPI serve the same rehearsal purpose. No
  `testpypi` GitHub Environment and no `test.pypi.org` trusted publisher are required.
- True PEP 440 **release candidates** (`0.1.0rc1`). Merges to `main` produce `.devN` builds,
  which is the idiomatic every-commit form; an `rcN` would require its own deliberate tag.
- Signing artifacts (for example Sigstore attestations), building compiled/binary wheels, or a
  multi-platform build matrix. The package is pure Python, so one universal wheel plus one
  sdist is sufficient.
- Changelog generation, GitHub Release note automation, and documentation-site publishing.
- Any change to the library's runtime behavior, public API, or sinks.
- Renaming the import package, the GitHub repository, or the `log-foundry` brand used in prose,
  runtime log prefixes (`log-foundry: …`), and sink defaults (`app_name`, `_DDSOURCE`). Only the
  PyPI distribution name moves to `log-foundry`.

---

## Functional Requirements

### FR-001: Version is derived from Git tags, not hand-edited

#### Description:

The package version is computed at build time from the repository's Git tags by
`poetry-dynamic-versioning`, and the static `version` field is removed from the `[project]`
table so no one edits a version by hand again.

#### Acceptance Criteria:

- [ ] `[project]` declares `dynamic = ["version"]` and no longer contains a literal `version`
      key.
- [ ] `[build-system]` uses `build-backend = "poetry_dynamic_versioning.backend"` and lists
      `poetry-dynamic-versioning` in `requires`.
- [ ] Building at a commit tagged `vX.Y.Z` produces distributions whose version metadata is
      exactly `X.Y.Z`.
- [ ] Building at an untagged commit that is N commits ahead of the latest tag produces a valid
      PEP 440 public development version (for example `0.1.1.devN`) that carries **no** local
      version segment (no `+<hash>` suffix), because PyPI rejects uploads whose version
      contains a local segment.
- [ ] The built sdist and wheel filenames and internal metadata all report the derived version
      consistently.

### FR-002: Runtime `__version__` attribute

#### Description:

`log_foundry` exposes its installed version at runtime so callers and bug reports can read it,
sourced from the distribution metadata rather than a second hand-maintained constant.

#### Acceptance Criteria:

- [ ] `import log_foundry; log_foundry.__version__` returns the installed distribution version as a
      string.
- [ ] The value is read via `importlib.metadata.version("log-foundry")`, so it always matches the
      version the wheel was published under.
- [ ] When the package is not installed (running directly from a source checkout), accessing
      `__version__` does not raise; it falls back to `"0.0.0"`.
- [ ] `"__version__"` is included in the module's `__all__`.

### FR-003: Release workflow builds sdist and wheel

#### Description:

A GitHub Actions workflow builds both distribution formats from a full-history checkout so the
dynamic version can be resolved, and hands the artifacts to the publish jobs.

#### Acceptance Criteria:

- [ ] `.github/workflows/release.yml` exists and triggers on pushes to `main` and on pushes of
      tags matching `v*`.
- [ ] The build job checks out with `fetch-depth: 0` so the full tag history is available to the
      version backend.
- [ ] The build job runs `python -m build`, producing exactly one sdist and one wheel in
      `dist/`, and uploads them as a workflow artifact for the publish jobs to consume.
- [ ] The build job runs only after the test gate (FR-007) succeeds.

### FR-004: Continuous development pre-release publish on merge to main

#### Description:

Every push to `main` publishes the built development pre-release (`X.Y.Z.devN`) to production
PyPI, so the publish pipeline is exercised end to end on each merge. Because pip does not
install pre-releases unless `--pre` is passed, `pip install log-foundry` continues to resolve to
the latest stable release.

#### Acceptance Criteria:

- [ ] On a push to `main` (and not on a tag), the workflow publishes the built artifacts to
      production PyPI.
- [ ] The published version is a `.devN` pre-release, never a stable `X.Y.Z`.
- [ ] The dev publish step sets `skip-existing: true`, so a re-run that produces an
      already-uploaded development version is tolerated rather than failing the workflow.
- [ ] The dev publish job runs in the GitHub Environment named `pypi` and requests
      `id-token: write`.
- [ ] The tagged-release publish job does **not** run on a plain push to `main`.
- [ ] `pip install log-foundry` in a clean environment installs the latest **stable** release,
      not the newest `.devN`.

### FR-005: Tagged release publish to PyPI on a version tag

#### Description:

Pushing an annotated version tag builds and publishes a stable release to PyPI.

#### Acceptance Criteria:

- [ ] On a push of a tag matching `v*`, the workflow publishes the built artifacts to PyPI.
- [ ] The version published equals the tag with the leading `v` removed (tag `v0.2.0` publishes
      version `0.2.0`).
- [ ] The release publish job runs in the GitHub Environment named `pypi` and requests
      `id-token: write`.
- [ ] The release publish step does **not** set `skip-existing`, so re-pushing a tag whose
      version is already on PyPI fails the workflow instead of silently succeeding.
- [ ] The dev pre-release publish job does **not** run on a tag push.

### FR-006: Trusted Publishing authentication with no stored secrets

#### Description:

Both uploads authenticate to PyPI using Trusted Publishing (OIDC); the repository stores no
PyPI password or API token.

#### Acceptance Criteria:

- [ ] Publishing uses `pypa/gh-action-pypi-publish@release/v1` with no `password`/`api-token`
      input.
- [ ] Each publish job grants `id-token: write` (and nothing broader than `contents: read`
      besides it).
- [ ] No PyPI credential is present in repository or environment secrets.
- [ ] A trusted publisher is registered on `pypi.org` for this repository, the `release.yml`
      workflow file, and the environment name `pypi` (see Configuration / Environment).
- [ ] Both publish jobs use the same `pypi` environment, so one publisher registration covers
      the dev pre-release and the tagged release.

### FR-007: Publish is gated on a green build

#### Description:

No artifact is published unless the existing lint, type-check, and test suite passes for the
same commit.

#### Acceptance Criteria:

- [ ] `ci.yml` is made callable by other workflows (it gains a `workflow_call` trigger) without
      changing its existing pull-request and push behavior.
- [ ] `release.yml` invokes `ci.yml` as a job and both the build and publish jobs depend on it.
- [ ] If lint, type checking, or tests fail, neither the build job nor either publish job runs.

---

## Data Model

There is no runtime data model. The one model that matters is the mapping from Git state to the
produced version, which the build backend computes:

```
# Git state at build time            ->  produced PEP 440 version
HEAD is exactly on tag v0.1.0         ->  0.1.0
HEAD is 3 commits after tag v0.1.0    ->  0.1.1.dev3          (bumped patch, dev counter = distance)
No tags exist yet                     ->  0.0.0.dev<distance> (why FR reserves an initial v0.1.0 baseline tag)

# Rules enforced by config (see pyproject changes):
#   - style   = pep440   -> versions are valid PEP 440
#   - bump    = true     -> development versions sort AFTER the last release
#   - metadata = false   -> no "+<commit-hash>" local segment, so uploads are accepted
```

---

## API / Interface Contract

### `pyproject.toml` changes

```toml
[project]
name = "log-foundry"
# The version is derived from Git tags at build time (poetry-dynamic-versioning).
# Do not add a literal `version` key back here.
dynamic = ["version"]
# ... description, authors, license, readme, requires-python, dependencies unchanged ...

[tool.poetry]
# Poetry still requires this field to exist; it is only a placeholder.
# The real value is injected from Git tags during the build.
version = "0.0.0"
packages = [
    { include = "log_foundry", from = "src" },
]
# include / exclude unchanged

[tool.poetry-dynamic-versioning]
enable = true
vcs = "git"
style = "pep440"
bump = true       # development builds sort after the last release (e.g. 0.1.1.devN)
metadata = false  # drop the +<hash> local segment so PyPI accepts the upload

[build-system]
requires = ["poetry-core>=2.0.0,<3.0.0", "poetry-dynamic-versioning>=1.4.0,<2.0.0"]
build-backend = "poetry_dynamic_versioning.backend"
```

### `src/log_foundry/__init__.py` change

```python
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

try:
    __version__ = _dist_version("log-foundry")
except PackageNotFoundError:  # running from a source tree that isn't installed
    __version__ = "0.0.0"

# ... existing imports and shutdown() ...

__all__ = [
    "configure",
    "get_config",
    "trace",
    "debug",
    "info",
    "warning",
    "error",
    "critical",
    "set_baggage",
    "shutdown",
    "__version__",
]
```

### `.github/workflows/ci.yml` change (make it reusable)

Only the trigger block changes; the existing steps are unchanged.

```yaml
on:
  pull_request:
  push:
    branches: [main]
  workflow_call:   # allow release.yml to run this same gate
```

### `.github/workflows/release.yml` (new)

```yaml
name: Release

on:
  push:
    branches: [main]
    tags: ["v*"]

permissions:
  contents: read

jobs:
  # Gate: reuse the existing CI so a publish can never run on a red build.
  test:
    uses: ./.github/workflows/ci.yml

  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
        with:
          fetch-depth: 0        # full history + tags so the version can be derived

      - name: Set up Python
        uses: actions/setup-python@v6
        with:
          python-version: "3.13"

      - name: Build sdist and wheel
        run: |
          python -m pip install --upgrade build
          python -m build

      - name: Upload built distributions
        uses: actions/upload-artifact@v4
        with:
          name: dist
          path: dist/

  publish-dev:
    # Every merge to main ships a 0.1.1.devN pre-release to PyPI. pip ignores pre-releases
    # unless --pre is passed, so `pip install log-foundry` still gets the stable release.
    if: github.ref == 'refs/heads/main'
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: pypi
      url: https://pypi.org/p/log-foundry
    permissions:
      id-token: write          # required for Trusted Publishing (OIDC)
    steps:
      - name: Download built distributions
        uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/

      - name: Publish dev pre-release to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          skip-existing: true   # tolerate a re-run that produces a duplicate dev version

  publish-release:
    # A pushed version tag (vX.Y.Z) ships a stable release to PyPI.
    # Deliberately no skip-existing: re-tagging a published version must fail loudly.
    if: startsWith(github.ref, 'refs/tags/v')
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: pypi
      url: https://pypi.org/p/log-foundry
    permissions:
      id-token: write
    steps:
      - name: Download built distributions
        uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
```

### Cutting a release (maintainer usage)

```bash
# main is green and carries the changes you want to ship.
git tag -a v0.2.0 -m "log-foundry 0.2.0"
git push origin v0.2.0
# The tag push triggers release.yml -> build -> publish-release (version 0.2.0).
```

## Configuration / Environment

**Trusted publisher registration (one-time).** Because `log-foundry` does not yet exist on
PyPI, use the "pending publisher" flow on `pypi.org` (Your account → Publishing). Register a
GitHub Actions publisher with: PyPI project name `log-foundry`; owner and repository matching
this GitHub repository; workflow filename `release.yml`; and environment name `pypi`. The
environment field must be exactly `pypi` or left blank — blank is permissive and still matches,
but any other value fails the OIDC exchange at upload time, which is the most common
trusted-publishing failure. A pending publisher does not reserve the project name until the
first successful publish, so let the first `main` merge run soon after registering to claim the
name.

**GitHub Environments.** Create one environment in the repository settings, `pypi`, matching
the name in `release.yml`. It carries no protection rules: releases are fully automatic on a
tag push, on the reasoning that pushing the tag is itself the deliberate act. Adding a
required-reviewer rule later would insert a human approval before each upload.

**Local contributor setup.** Because the version now comes from Git, contributors installing
with Poetry need the plugin registered in their Poetry so local `poetry build` and
`poetry install` resolve the version: `poetry self add "poetry-dynamic-versioning[plugin]"`.
The CI build uses `python -m build`, which reads the backend directly and needs no Poetry
plugin. Add this line, and a note that the version is tag-derived, to the "Common Commands"
section of `CLAUDE.md`.

**No secrets.** Do not add any `PYPI_*` secret; Trusted Publishing replaces them.

## File & Folder Structure

```
.github/workflows/
├── ci.yml            # CHANGED: add `workflow_call:` trigger (steps unchanged)
├── release.yml       # NEW: test gate -> build -> publish (dev on main, release on v* tags)
└── spec-lint.yml     # unchanged
pyproject.toml        # CHANGED: dynamic version, poetry-dynamic-versioning config + backend
src/log_foundry/
└── __init__.py       # CHANGED: expose __version__ from importlib.metadata
CLAUDE.md             # CHANGED: note tag-derived version + plugin install in Common Commands
```

## Implementation Phases

### Phase 1: Dynamic versioning

- Edit `pyproject.toml`: replace the `[project]` `version` key with `dynamic = ["version"]`,
  add the `[tool.poetry]` `version = "0.0.0"` placeholder, add the
  `[tool.poetry-dynamic-versioning]` block, and switch `[build-system]` to the
  `poetry_dynamic_versioning.backend` build backend (FR-001).
- Add the `__version__` attribute to `src/log_foundry/__init__.py` and list it in `__all__`
  (FR-002).
- Create the initial baseline tag `v0.1.0` on `main` so development versions read as
  `0.1.x.devN` rather than `0.0.0.devN`.
- Verify locally: `python -m build`, then confirm the built wheel's version is `0.1.0` on the
  tag and a `0.1.1.devN` form (no `+hash`) one commit later. Confirm `pip install dist/*.whl`
  then `python -c "import log_foundry; print(log_foundry.__version__)"` prints `0.1.0`.

### Phase 2: Reusable CI and dev pre-release publishing on main

- Add the `workflow_call` trigger to `ci.yml` (FR-007).
- Add `.github/workflows/release.yml` with the `test` (reused CI), `build`, and `publish-dev`
  jobs (FR-003, FR-004, FR-007).
- Register the pending trusted publisher on `pypi.org` and create the `pypi` environment
  (FR-006).
- Merge to `main` and confirm a `X.Y.Z.devN` pre-release appears on PyPI. This first publish is
  also what converts the pending publisher into a real one and claims the project name.

### Phase 3: Tagged release publishing

- Add the `publish-release` job to `release.yml`, without `skip-existing` (FR-005, FR-006).
- Push `v0.1.0` and confirm the stable release lands on PyPI, that `pip install log-foundry` in
  a clean environment installs `0.1.0` rather than a newer `.devN`, and that
  `python -c "import log_foundry; print(log_foundry.__version__)"` reports `0.1.0`.

### Phase 4: Documentation and completion

- Update `CLAUDE.md` Common Commands and Tech Stack to note the tag-derived version and the
  Poetry plugin install, and add a "Publishing" note pointing at `release.yml`.
- Add a short "Installation" and release-process note to `README.md`.
- Run the standard spec-completion ritual from `docs/process/completion-ritual.md`: set this spec's status to
  Completed, update the row in `docs/specs/INDEX.md`, and write the delivery doc under
  `docs/spec-delivery/`.
