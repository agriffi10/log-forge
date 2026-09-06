# Release, supply chain and naming — decisions

The settled decisions about how a release is cut, what the supply-chain gates prove, and the one
name. Read the fences; pull an entry only when you need the reasoning.

## Contents

- [Fences](#fences)
- [Version comes from Git tags, published to PyPI as `log-foundry`](#version-comes-from-git-tags-published-to-pypi-as-log-foundry)
- [Every action is pinned to a commit SHA, and the pins are maintained, not frozen](#every-action-is-pinned-to-a-commit-sha-and-the-pins-are-maintained-not-frozen)
- [A scanner that exits zero has not said "clean"](#a-scanner-that-exits-zero-has-not-said-clean)
- [An SBOM describes the published artifact, and is generated from it](#an-sbom-describes-the-published-artifact-and-is-generated-from-it)
- [Release assets are attached to a draft, never to a published release](#release-assets-are-attached-to-a-draft-never-to-a-published-release)
- [`pip-audit` gates, and audits the extras or it audits nothing](#pip-audit-gates-and-audits-the-extras-or-it-audits-nothing)
- [One name everywhere: `log-foundry` / `log_foundry`](#one-name-everywhere-log-foundry--log_foundry)

## Fences

- **Version comes from Git tags, published to PyPI as `log-foundry`** — tags cut releases; merges to `main` publish **no** `.devN` for now (`publish-dev` off). (SPEC-012)
- **Every action is pinned to a commit SHA, and the pins are maintained, not frozen** — a mutable tag on a workflow holding `id-token: write` against PyPI is a silent path from a third-party repository into every consumer's install. A lagging pin fails loudly; a compromised action fails forever. The version comment must read exactly `# vX.Y.Z` or Dependabot silently stops rewriting the pin — that comment is what "maintained" rests on. (SPEC-022)
- **A scanner that exits zero has not said "clean"** — the alert count is the verdict, never the check mark — zizmor and CodeQL pass the job regardless of findings by design, and only `dependency-review` fails a build. (SPEC-022)
- **An SBOM describes the published artifact, and is generated from it** — `make-sbom.py` describes the built wheel installed with every extra, and runs from a *second* venv or it lists its own ~30 dependencies as the library's. (SPEC-023)
- **Release assets are attached to a draft, never to a published release** — immutable releases freeze assets at publish, so it is create-as-draft → upload → publish. Deleting an immutable release does **not** free its tag name; a botched release is repaired only by a new version tag. (SPEC-023)
- **`pip-audit` gates, and audits the extras or it audits nothing** — `dependency-review` only sees a PR's dependency *diff*, so the weekly re-examination is the point. `--no-root` is load-bearing and `--strict` is on, because a silently skipped package is a silently unaudited one. (SPEC-023)
- **One name everywhere: `log-foundry` / `log_foundry`** — the import package was renamed from `log_forge` in `v0.2.0` to match the distribution name — breaking for `0.1.x`, no shim. Historical `log-forge` mentions survive only where they name the PyPI-rejected original.

---

### Version comes from Git tags, published to PyPI as `log-foundry`

**Version comes from Git tags, published to PyPI as `log-foundry`** — tags cut releases, merges to `main` publish `.devN` pre-releases. (SPEC-012)

> **Suspended, not reversed.** The `pypi` environment gained a deployment-branch policy admitting only `v*` tags, so a deployment from `refs/heads/main` is refused before `publish-dev`'s first step and every merge to `main` went red. The job is left in place with `if: false` and a restoration note; the tag path is untouched. While it is off, the second half of this decision is not true of the repository, and the property it bought — that a tagged release is never the first attempt at the upload path — is not held.


### Every action is pinned to a commit SHA, and the pins are maintained, not frozen

**Every action is pinned to a commit SHA, and the pins are maintained, not frozen** — a mutable tag on a workflow holding `id-token: write` against PyPI is a silent path from a third-party repository to every consumer's `pip install`, so `pypa/gh-action-pypi-publish` is pinned away from the `release/v1` branch PyPA itself recommends: a lagging pin fails loudly at release time, a compromised action fails forever. Dependabot's `github-actions` ecosystem moves the pins, which is what makes pinning affordable — the version comment must stay exactly `# vX.Y.Z` or it silently stops rewriting it. Pin to the tip of the major in use; a major bump is its own reviewable PR. (SPEC-022)


### A scanner that exits zero has not said "clean"

**A scanner that exits zero has not said "clean"** — zizmor in SARIF mode and CodeQL both report to code scanning and pass the job regardless of findings, deliberately: Advanced Security owns triage, and blocking belongs in a ruleset. Only `dependency-review` fails a build. So the alert count is the verdict, never the check mark — and a green audit is not evidence a *setting* is present (zizmor's `dependabot-cooldown` stops at the first passing entry). State the setting. (SPEC-022)


### An SBOM describes the published artifact, and is generated from it

**An SBOM describes the published artifact, and is generated from it** — `make-sbom.py` installs the built wheel with every extra into a throwaway venv and describes *that*, because runtime dependencies are empty by design and the extras are the whole dependency surface. The generator runs from a *second* venv or it lists itself and its ~30 dependencies as the library's (measured: 98 components vs 43). `cyclonedx-py`'s `poetry` mode cannot read this project at all — it wants `[tool.poetry].name`, and PEP 621 puts the name in `[project]`, the same misreading `dependabot.yml` documents. An empty SBOM, one versioned `0.0.0`, or one carrying build tooling fails the job: an inaccurate SBOM is worse than none, because it looks authoritative. (SPEC-023)


### Release assets are attached to a draft, never to a published release

**Release assets are attached to a draft, never to a published release** — this repository has immutable releases enabled, so assets freeze at publish: create-as-draft → upload → publish. And deleting an immutable release does **not** free its tag name, so a botched release is repaired only by a new version tag, never by recreating the old one. Both were learned by shipping `v0.10.0` without its SBOM and then making it unrepairable. The job is deliberately *not* idempotent — a re-run that claimed to replace an asset it cannot touch would be lying. (SPEC-023)


### `pip-audit` gates, and audits the extras or it audits nothing

**`pip-audit` gates, and audits the extras or it audits nothing** — `dependency-review` only sees a PR's dependency *diff*, so an advisory against an already-pinned dependency is invisible to it; the weekly re-examination is the point. `--no-root` is load-bearing (Poetry installs the project editable, and `--strict` refuses an editable distribution), and `--strict` is on because a silently skipped package is an unaudited one. Suppressions are per-advisory with written reasons in `.github/pip-audit-ignores.txt`, never by severity or package. (SPEC-023)


### One name everywhere: `log-foundry` / `log_foundry`

**One name everywhere: `log-foundry` / `log_foundry`** — the import package was renamed from `log_forge` in `v0.2.0` so it matches the distribution name. Breaking for `0.1.x` users; no compatibility shim was shipped. Historical `log-forge` mentions survive only where they name the PyPI-rejected original.


