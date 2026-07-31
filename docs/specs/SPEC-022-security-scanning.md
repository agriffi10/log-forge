# Spec: Security Scanning in CI

**ID:** SPEC-022
**Status:** Completed
**Last Updated:** 2026-07-31
**Depends On:** SPEC-012

## Overview

`log-foundry` publishes to PyPI from a workflow holding `id-token: write`, and it is installed by
other people's applications. Today nothing in CI looks for a vulnerability. What exists is real but
partial: GitHub secret scanning with push protection is on, Dependabot **security** alerts are on,
and `ruff` selects the `S` (bandit) rules so single-file security patterns already fail the build.
What is missing is everything that needs to reason across files or across time — interprocedural
taint analysis, a check on dependencies a pull request *introduces*, an audit of the workflows
themselves, and any mechanism that keeps versions current rather than waiting for an advisory to be
published.

This spec closes those four gaps using tooling that is free on this repository, because it is
public: CodeQL code scanning, `actions/dependency-review-action`, `zizmor` for GitHub Actions
static analysis, and a `dependabot.yml` that adds scheduled **version** updates on top of the
security updates already running. It also SHA-pins every action the repository uses, which zizmor's
default policy requires and which the new Dependabot `github-actions` ecosystem then maintains
automatically.

**The threat this is actually about.** The library has **zero runtime dependencies** by deliberate
constraint, so "dependency scanning" here covers the dev toolchain, the twelve optional extras, and
— the part that matters most — the GitHub Actions supply chain. `release.yml` exchanges an OIDC
token for the right to publish `log-foundry` to PyPI. An action compromised at a mutable tag is a
direct, silent path from a third-party repository to every consumer's `pip install`. That is the
highest-value thing on this list, and it is the reason the action-pinning work is in scope rather
than deferred.

## Scope

### In Scope

- CodeQL code scanning over both languages GitHub detects for this repository, `python` and
  `actions`, via **default setup** — a repository setting, not a workflow file.

  > **Amended 2026-07-31.** This spec was authored calling for *advanced* setup
  > (`.github/workflows/codeql.yml`). Default setup was enabled by hand before the build began,
  > and the two are mutually exclusive in one direction: default setup "overrides existing CodeQL
  > setups by disabling any existing CodeQL workflows, and blocking any CodeQL analysis API
  > uploads." A `codeql.yml` added now would run and silently fail to upload its results. Default
  > setup already delivers what FR-001 and FR-002 asked for — both languages, weekly — so the
  > workflow file is dropped rather than the coverage. What is given up is configuration in
  > version control, an explicit matrix, and a controllable cron minute; what is gained is one
  > less file to maintain and no `uses:` pins to keep current.
- A new `.github/dependabot.yml` enabling scheduled **version** updates for two ecosystems: `pip`
  (Poetry) and `github-actions`.
- A new `.github/workflows/dependency-review.yml` running `actions/dependency-review-action` on
  pull requests, failing the job when a PR introduces a dependency with a known vulnerability.
- A new `.github/workflows/zizmor.yml` running `zizmorcore/zizmor-action`, uploading SARIF to
  GitHub code scanning.
- **Pinning every `uses:` reference in the repository to a full commit SHA**, with a trailing
  `# vX.Y.Z` comment, across `ci.yml`, `release.yml`, `spec-lint.yml`, and the two new workflows.
  This is not optional decoration: since zizmor v1.20.0 the default `unpinned-uses` policy is
  blanket hash-pinning for *all* actions, so a tag-pinned `actions/checkout@v5` is a High-severity
  finding. The alternative — a `zizmor.yml` restoring the pre-1.20 `"actions/*": ref-pin` behavior —
  was considered and rejected, because the Dependabot `github-actions` ecosystem added by this same
  spec removes the maintenance cost that made SHA pins unattractive in the first place.
- An `ignore` rule in `dependabot.yml` holding `ruff` to patch updates within `0.16.x`, preserving
  the deliberate `>=0.16,<0.17` bound documented in `pyproject.toml`.
- A `SECURITY.md` disclosure policy, since `isSecurityPolicyEnabled` is currently false and the
  repository is a published package with no stated way to report a vulnerability privately.

### Out of Scope

- **License checking in dependency review.** `deny-licenses` looks attractive for an MIT project,
  but the `postgres` extra depends on `psycopg`, which is **LGPL-3.0**; any GPL-family denial list
  would fail on an optional extra the project deliberately ships. Choosing a license policy that
  accounts for that is its own decision, not a rider on this one.
- **`pip-audit`** (`pypa/gh-action-pip-audit`). It overlaps Dependabot's advisory coverage almost
  entirely, emits no SARIF, and its distinguishing value — auditing a *resolved* environment —
  would only apply if this spec also installed all twelve extras, which it does not.
- **Standalone `bandit` or `gitleaks` jobs.** Both duplicate something already running: `ruff`
  selects `S`, and GitHub secret scanning with push protection is enabled at the repository level.
  Adding them would produce a second stream of the same findings.
- **Making any new scan a required status check.** That is a branch-protection ruleset change, not
  a file in this repository, and the `main` ruleset already requires a human review. CodeQL and
  zizmor findings surface as code-scanning alerts by design; only dependency review fails a job.
- **Auto-remediation.** No job may push a fix, and zizmor's auto-fix mode is not enabled. A finding
  is reported to a human.
- **Artifact signing, SLSA provenance, or Sigstore attestations** on the published wheel. Related
  supply-chain hardening, deliberately a separate piece of work.
- **Scanning the optional extras' resolved dependency trees.** `poetry install --with dev` does not
  install extras, and installing twelve of them across two Python versions to scan them is a cost
  this spec does not take on.
- Any change to the library's runtime behavior, public API, sinks, or `src/` at all. This spec
  touches only `.github/`, `SECURITY.md`, and the documentation files named by the completion
  ritual.

---

## Functional Requirements

### FR-001: CodeQL scans the Python source

#### Description:

CodeQL analyzes `src/` for the vulnerability classes `ruff`'s single-file `S` rules cannot see —
anything requiring dataflow across function or module boundaries.

#### Acceptance Criteria:

- [ ] `GET /repos/{owner}/{repo}/code-scanning/default-setup` reports `state: "configured"`.
- [ ] Its `languages` array includes `python`.
- [ ] Its `schedule` is `weekly`. The scheduled run is the point: CodeQL's query packs improve
      over time, so unchanged code can acquire a finding without any commit.
- [ ] A completed analysis for `/language:python` is visible at
      `GET /repos/{owner}/{repo}/code-scanning/analyses`.
- [ ] No `.github/workflows/codeql.yml` exists. Advanced setup would be disabled by default setup
      and would upload nothing; an inert workflow that appears to be scanning is worse than none.

### FR-002: CodeQL also scans the workflows themselves

#### Description:

GitHub detects `actions` as a scannable language for this repository. CodeQL's Actions queries
catch workflow-level issues its Python queries do not, and this repository's workflows hold
publish credentials.

#### Acceptance Criteria:

- [ ] The default-setup `languages` array includes `actions` alongside `python`.
- [ ] A completed analysis for `/language:actions` is visible at the analyses endpoint.
- [ ] `query_suite` is `extended`, not `default`. Extended raises recall at some cost in
      precision, which a codebase this size absorbs easily. (Default setup exposes `default` and
      `extended`; the `security-and-quality` suite is not offered here, which is no loss — its
      maintainability queries duplicate `ruff`, which already gates CI.)
- [ ] Changing the query suite re-runs the analysis; the change is confirmed by re-reading the
      endpoint after that run completes, not from the `PATCH` response, which returns a `run_id`
      while the old value is still in place.

### FR-003: Dependabot keeps the Python toolchain current

#### Description:

Dependabot security updates already react to published advisories. This adds scheduled version
updates so the dev toolchain and the optional extras do not sit on stale releases between
advisories.

#### Acceptance Criteria:

- [ ] `.github/dependabot.yml` exists and declares a `pip` ecosystem entry for directory `/`.
      (`pip` is the correct ecosystem value for Poetry projects; there is no `poetry` value.)
- [ ] The schedule is `weekly`, and `open-pull-requests-limit` is set to a value that bounds PR
      noise rather than leaving the default.
- [ ] Dependabot updates `poetry.lock` in the same pull request as any `pyproject.toml` change.
      A constraint change landing without a regenerated lockfile hard-fails CI's `poetry install`,
      so a PR that changes one without the other is a bug in the configuration.
- [ ] An `ignore` entry holds `ruff` to patch updates only, excluding
      `version-update:semver-minor` and `version-update:semver-major`.
- [ ] That `ignore` entry carries a comment pointing at the rationale already written in
      `pyproject.toml`: ruff's default rule set is not stable across minors, so a minor bump is a
      deliberate decision, not an automatic one.
- [ ] Dev-group dependencies (`[tool.poetry.group.dev.dependencies]`) and the extras under
      `[project.optional-dependencies]` both produce update pull requests. If they do not, an
      `allow: - dependency-type: all` entry is added and the reason noted.

### FR-004: Dependabot maintains the GitHub Actions supply chain

#### Description:

With every action SHA-pinned per FR-007, something must move those pins forward. Dependabot's
`github-actions` ecosystem does this and keeps the trailing version comment in sync.

#### Acceptance Criteria:

- [ ] `.github/dependabot.yml` declares a `github-actions` ecosystem entry for directory `/`,
      scheduled `weekly`.
- [ ] A Dependabot pull request against a SHA-pinned `uses:` updates both the SHA and the trailing
      `# vX.Y.Z` comment.
- [ ] Every version comment is written as **exactly** `# vX.Y.Z` with the version last in the
      comment and no trailing prose. Dependabot skips the comment rewrite when the comment carries
      additional text, which would silently leave a stale version annotation next to a fresh SHA.
- [ ] The entry covers all workflow files in `.github/workflows/`, including the two added by
      this spec. CodeQL needs no entry — default setup runs no workflow of ours and so has no
      `uses:` pin to maintain, which is one of the reasons it was kept (see Scope).

### FR-005: A pull request cannot introduce a vulnerable dependency

#### Description:

Dependency review diffs the dependency graph between the base and head of a pull request and fails
when the PR adds a dependency with a known vulnerability — catching it before merge rather than as
an alert afterwards.

#### Acceptance Criteria:

- [ ] `.github/workflows/dependency-review.yml` exists and triggers on `pull_request` only.
- [ ] The job fails when a pull request introduces a dependency with a vulnerability at or above
      the configured severity threshold.
- [ ] `fail-on-severity` is set explicitly rather than left at the default.
- [ ] `deny-licenses` is **not** configured (see Out of Scope — `psycopg` is LGPL-3.0).
- [ ] The job requests `contents: read`, plus `pull-requests: write` only if the PR-comment summary
      is enabled.
- [ ] The workflow notes that it requires the dependency graph, which is confirmed enabled for this
      repository (the SBOM endpoint currently returns 64 packages).

### FR-006: The workflows are themselves audited

#### Description:

`zizmor` performs static analysis on GitHub Actions workflows — template injection, excessive
permissions, unpinned actions, artifact poisoning. This repository's `release.yml` holds
`id-token: write` against PyPI Trusted Publishing, which is exactly the blast radius zizmor exists
to protect.

#### Acceptance Criteria:

- [ ] `.github/workflows/zizmor.yml` exists and triggers on `pull_request` and on `push` to `main`.
- [ ] It runs `zizmorcore/zizmor-action` pinned to a full commit SHA. The action publishes no
      moving major tag, so a `@v0` style reference is not available and must not be invented.
- [ ] The job declares `security-events: write` and `contents: read`, and findings are uploaded to
      GitHub code scanning as SARIF.
- [ ] The job does **not** fail on findings. In SARIF mode the action deliberately exits zero so
      that Advanced Security handles triage; this matches how CodeQL reports in FR-001 and is
      recorded as a comment rather than left as a surprise.
- [ ] A zizmor run against the repository after FR-007 lands reports **no** `unpinned-uses`
      findings.

### FR-007: Every action is pinned to a commit SHA

#### Description:

All `uses:` references across every workflow move from mutable tags and branches to immutable
commit SHAs. This is what makes FR-006 pass, and independently it is the control that matters most
for a repository that can publish to PyPI.

#### Acceptance Criteria:

- [ ] No `uses:` line in `.github/workflows/` references a tag (`@v5`), a branch (`@release/v1`),
      or any other mutable ref. Every one is a 40-character commit SHA.
- [ ] Each pinned line carries a trailing `# vX.Y.Z` comment naming the version that SHA
      corresponds to, in the exact form FR-004 requires.
- [ ] `pypa/gh-action-pypi-publish@release/v1` is pinned to the SHA of a tagged release. PyPA
      recommends the rolling `release/v1` branch; this spec deliberately departs from that
      recommendation and records why — a mutable ref on the one job holding a PyPI publishing
      token is the single highest-consequence unpinned reference in the repository, and a lagging
      pin fails loudly at release time whereas a compromised action fails silently forever.
- [ ] Each SHA is resolved from the upstream repository at implementation time and verified to
      correspond to the tag named in its comment. No SHA is copied from this spec, which does not
      contain any.
- [ ] `ci.yml`, `release.yml`, and `spec-lint.yml` all still pass after the change, and a release
      dry-run path is reasoned about explicitly before `release.yml` is merged.
- [ ] The internal `uses: ./.github/workflows/ci.yml` reusable-workflow reference is left as a
      local path and is not SHA-pinned; it refers to this repository.

### FR-008: A private disclosure path exists

#### Description:

The repository is a published package with `isSecurityPolicyEnabled: false`, so a researcher who
finds a vulnerability has no documented private channel and the plausible next step is a public
issue.

#### Acceptance Criteria:

- [ ] `SECURITY.md` exists at the repository root or under `.github/`, and
      `isSecurityPolicyEnabled` reports true afterwards.
- [ ] It states which versions are supported, how to report privately, and an expected response
      window.
- [ ] It directs reporters to GitHub private vulnerability reporting, and that feature is enabled
      on the repository.
- [ ] It does not promise a bounty or a fixed remediation deadline.

### FR-009: No new job inflates its own permissions

#### Description:

Two new workflows are added to a repository whose release path depends on tightly scoped tokens.
Each must request the minimum it needs, so that adding security scanning does not itself widen the
attack surface it exists to reduce.

#### Acceptance Criteria:

- [ ] Every new workflow declares a top-level `permissions:` block; none relies on the repository
      default.
- [ ] No new workflow requests `id-token: write`, `contents: write`, or `packages: write`.
- [ ] No new workflow triggers on `pull_request_target`, which would run a fork's changes against
      the base repository's secrets.
- [ ] `release.yml`'s existing permissions are unchanged by this spec.
- [ ] A zizmor run reports no `excessive-permissions` findings against the new workflows.

---

## Data Model

There is no runtime data model. The model that matters is which control covers which surface, and
where each finding is reported:

```
surface                          control                       reports to        gates a PR?
-------------------------------  ----------------------------  ----------------  -----------
src/ single-file patterns        ruff `S` (already in ci.yml)   CI job failure    yes  (existing)
src/ interprocedural dataflow    CodeQL `python`                code scanning     no   (FR-001)
workflow definitions             CodeQL `actions` + zizmor      code scanning     no   (FR-002, FR-006)
dependency added by a PR         dependency-review-action       job failure       yes  (FR-005)
dependency with a new advisory   Dependabot security updates    Dependabot PR     no   (already on)
dependency merely out of date    Dependabot version updates     Dependabot PR     no   (FR-003)
action supply chain              SHA pins + Dependabot          Dependabot PR     no   (FR-004, FR-007)
committed credentials            secret scanning + push prot.   push rejection    yes  (already on)
```

Only two things fail a build: `ruff`, which already does, and dependency review. Everything else
raises an alert for a human, which is the deliberate posture — see Out of Scope on required checks.

---

## API / Interface Contract

Every `uses:` below is shown **unpinned for readability**. FR-007 requires that each one be a full
commit SHA with a `# vX.Y.Z` comment when written to disk; the SHAs are resolved at implementation
time, deliberately not recorded here, and verified against the tag they claim.

### CodeQL default setup (a repository setting, not a file)

There is no workflow to write. The configuration lives behind
`/repos/{owner}/{repo}/code-scanning/default-setup`, and this is the target state:

```jsonc
{
  "state": "configured",
  "languages": ["actions", "python"],  // both languages GitHub detects here
  "query_suite": "extended",           // FR-002; `default` is the out-of-box value
  "schedule": "weekly",                // newly published queries reach unchanged code
  "threat_model": "remote",
  "runner_type": "standard"
}
```

```bash
# Raise the query suite. Returns a run_id and re-runs the analysis; the endpoint keeps
# reporting the OLD suite until that run finishes, so verify by re-reading, not from the response.
gh api -X PATCH repos/agriffi10/log-forge/code-scanning/default-setup -f query_suite=extended
gh api repos/agriffi10/log-forge/code-scanning/default-setup --jq '.query_suite'
```

### `.github/dependabot.yml` (new)

```yaml
version: 2
updates:
  # Poetry is served by the `pip` ecosystem; there is no `poetry` value.
  - package-ecosystem: "pip"
    directory: "/"
    schedule:
      interval: "weekly"
    open-pull-requests-limit: 5
    groups:
      dev-dependencies:
        patterns: ["*"]
        update-types: ["minor", "patch"]
    ignore:
      # The `>=0.16,<0.17` bound in pyproject.toml is a decision, not an oversight: ruff's
      # DEFAULT rule set is not stable across minors (0.16.0 widened it from 59 rules to 413).
      # Left alone, Dependabot rewrites a two-sided range rather than respecting it, because
      # this project resolves to the `increase` strategy — `[tool.poetry]` carries no `name`
      # key, so Dependabot classifies it as an application rather than a library. Patch
      # updates inside 0.16.x are welcome; crossing the bound stays a human decision.
      - dependency-name: "ruff"
        update-types:
          - "version-update:semver-minor"
          - "version-update:semver-major"

  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
    open-pull-requests-limit: 5
```

### `.github/workflows/dependency-review.yml` (new)

```yaml
name: Dependency Review

on: pull_request

permissions:
  contents: read

jobs:
  review:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pull-requests: write     # only for comment-summary-in-pr
    steps:
      - uses: actions/checkout@<sha>   # vX.Y.Z
      - name: Review dependencies introduced by this PR
        uses: actions/dependency-review-action@<sha>   # vX.Y.Z
        with:
          fail-on-severity: moderate
          comment-summary-in-pr: on-failure
          # Deliberately no deny-licenses: the `postgres` extra pulls psycopg (LGPL-3.0),
          # so any GPL-family denial would fail on a dependency this project ships on purpose.
```

### `.github/workflows/zizmor.yml` (new)

```yaml
name: Zizmor

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  zizmor:
    runs-on: ubuntu-latest
    permissions:
      security-events: write
      contents: read
    steps:
      - uses: actions/checkout@<sha>   # vX.Y.Z
      - name: Audit the workflows
        uses: zizmorcore/zizmor-action@<sha>   # vX.Y.Z
        # advanced-security defaults to true: findings become SARIF in code scanning and the
        # job exits zero rather than failing. That is intentional and matches CodeQL — Advanced
        # Security owns triage. To make a finding blocking, use a ruleset, not a job failure.
```

### Resolving a SHA (implementation method, FR-007)

```bash
# The tag a pin claims must be the tag the SHA actually is. Resolve, then verify.
gh api repos/actions/checkout/git/ref/tags/v5 --jq '.object.sha'
```

## Configuration / Environment

**Already enabled; this spec depends on but does not change them.** Dependency graph (confirmed:
the SBOM endpoint returns 64 packages — dependency review fails without it), Dependabot security
updates, secret scanning, and secret scanning push protection.

**Repository settings this spec does change.** Private vulnerability reporting must be enabled for
FR-008. No new secret, environment, or variable is introduced — every tool here is free on a public
repository and authenticates with the automatic `GITHUB_TOKEN`.

**Unavailable, not merely unconfigured.** Two secret-scanning sub-settings — validity checks and
non-provider patterns (renamed "generic patterns" in the UI) — read `disabled` and cannot be
enabled here. They require an **organization-owned** repository with **GitHub Secret Protection**,
a Team/Enterprise product; a personal account cannot enable or purchase them, public repo or not.
`PATCH /repos/{owner}/{repo}` returns 200 and silently changes nothing because
`secret_scanning_validity_checks` is not in that endpoint's request schema at all — it exists only
on the org and enterprise code-security-configuration endpoints — and GitHub ignores unrecognized
body properties. The docs page saying otherwise is stale. Recorded here as a constraint so it is
not rediscovered as a bug. What *is* free on a public repository, and already on: secret scanning
alerts and push protection.

**CodeQL default setup stays on, and advanced setup must not be added.** The two are mutually
exclusive in one direction — default setup disables any CodeQL workflow and blocks CodeQL analysis
API uploads, while adding a workflow does *not* turn default setup off. Reverting to advanced setup
would mean explicitly disabling default setup first, then adding `codeql.yml`. See the amendment
note in Scope for why this spec no longer does that.

## File & Folder Structure

```
.github/
├── dependabot.yml                    # NEW: pip + github-actions version updates
└── workflows/
    ├── ci.yml                        # CHANGED: SHA-pin uses: (steps otherwise unchanged)
    ├── release.yml                   # CHANGED: SHA-pin uses:, incl. gh-action-pypi-publish
    ├── spec-lint.yml                 # CHANGED: SHA-pin uses:
    ├── dependency-review.yml         # NEW: PR-time vulnerable-dependency gate
    └── zizmor.yml                    # NEW: workflow static analysis -> SARIF
                                      # (no codeql.yml — default setup is a repo setting, FR-001)
SECURITY.md                           # NEW: private disclosure policy
CLAUDE.md                             # CHANGED: one Key Decisions line + pointer
docs/specs/INDEX.md                   # CHANGED: one row
docs/spec-delivery/
└── SPEC-022-security-scanning.md     # NEW: delivery doc
```

## Implementation Phases

### Phase 1: Pin the existing actions

- Resolve the current SHA for every `uses:` in `ci.yml`, `release.yml`, and `spec-lint.yml`, and
  verify each against the tag it will be commented with (FR-007).
- Rewrite each reference as a 40-character SHA with a trailing `# vX.Y.Z` comment in the exact
  form FR-004 requires. Leave `uses: ./.github/workflows/ci.yml` as a local path.
- Treat `pypa/gh-action-pypi-publish` as the careful one: it is pinned away from the rolling
  `release/v1` branch PyPA recommends, and it is the job that publishes to PyPI. Reason through
  the release path before merging.
- Confirm CI and spec-lint pass on the PR. This phase changes no behavior, so a green run is the
  whole verification.

### Phase 2: CodeQL

- **Mostly already done, and it changes no files.** Default setup was enabled by hand on
  2026-07-31 and both analyses have completed; the query suite was raised to `extended` the same
  day. This phase is verification plus triage, not construction.
- Confirm the endpoint reports `state: configured`, `languages: ["actions", "python"]`,
  `query_suite: extended`, `schedule: weekly`, and that a completed analysis exists for each
  language (FR-001, FR-002).
- Confirm no `codeql.yml` was added, which would upload nothing.
- Triage whatever the first `extended` run produces. A finding is either a real defect worth its
  own spec, or a false positive to be dismissed with a written reason — not left sitting unread.
  Record the disposition in the delivery doc.

### Phase 3: Dependency review and Dependabot

- Add `.github/workflows/dependency-review.yml` (FR-005, FR-009).
- Add `.github/dependabot.yml` with both ecosystems, the grouping, and the `ruff` ignore
  (FR-003, FR-004).
- After merge, confirm Dependabot runs and that the first `pip` PR carries a regenerated
  `poetry.lock` alongside any `pyproject.toml` change. Confirm the first `github-actions` PR
  updates both a SHA and its version comment.
- If dev-group or extras dependencies produce no PRs, add `allow: - dependency-type: all` and
  note why.

### Phase 4: zizmor, security policy, and completion

- Add `.github/workflows/zizmor.yml` pinned by SHA (FR-006, FR-009).
- Confirm the run reports no `unpinned-uses` and no `excessive-permissions` findings — Phase 1 is
  what makes this pass, so a finding here means Phase 1 missed a reference.
- Add `SECURITY.md` and enable private vulnerability reporting (FR-008).
- Run the completion ritual from `docs/process.md`: set this spec to Completed, update the row in
  `docs/specs/INDEX.md`, write `docs/spec-delivery/SPEC-022-security-scanning.md`, and add one
  Key Decisions line to `CLAUDE.md`.
