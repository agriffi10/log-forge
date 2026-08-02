# Spec: Supply-Chain Transparency and Dependency Auditing

**ID:** SPEC-023
**Status:** Draft
**Last Updated:** 2026-08-02
**Depends On:** SPEC-012, SPEC-022

## Overview

SPEC-022 gave this repository scanners that look *inward* — CodeQL over its own source, zizmor over
its own workflows, `dependency-review` over what a pull request *introduces*. Nothing looks outward
at what has already shipped. A consumer who installs `log-foundry[aws,kafka,postgres]` has no
machine-readable statement of what that pulled in, and nothing in CI ever re-examines the eleven
optional extras for an advisory published after the code that depends on them was merged.
`dependency-review` only sees a diff; a CVE announced against an already-pinned `boto3` produces no
pull request for it to fail. Dependabot raises an alert in that case, but an alert is a notification
on a dashboard, not a red build.

This spec closes both gaps: publish a CycloneDX SBOM with every release, so consumers can audit what
they installed without reconstructing the dependency graph from `pyproject.toml`; and add a
scheduled `pip-audit` over the full extras surface, so a newly published advisory turns something
red on its own. It also adds OpenSSF Scorecard as a standing measurement of the supply-chain posture
SPEC-022 built, so drift in it is visible rather than assumed. Like SPEC-022, this spec touches no
file under `src/`.

## Scope

### In Scope

- A CycloneDX SBOM generated from `poetry.lock`, covering all optional extras, built on every
  release-workflow run and attached to a GitHub Release on a version tag.
- Creating a GitHub Release on a version tag — the repository publishes to PyPI today and has
  **no** GitHub Releases at all, so there is currently nothing for a release asset to attach to.
- A scheduled `pip-audit` over the resolved environment including every extra, gating (non-zero
  exit on a known advisory) with a documented, reasoned suppression mechanism.
- An OpenSSF Scorecard workflow reporting to code scanning, with results published to the OpenSSF
  public API.
- A `security` Poetry dependency group holding the two audit tools, so their versions are locked
  and Dependabot maintains them.
- Every new action SHA-pinned in the `# vX.Y.Z` comment form Dependabot rewrites, with `permissions`
  stated per workflow and per job (SPEC-022 FR-007, FR-009 extended to the new files).
- Documentation of where the SBOM lives and how to reproduce both scans locally.

### Out of Scope

- **PEP 770 in-wheel SBOM** (`.dist-info/sboms/`). The standard is accepted and Hatchling implements
  it, but this project builds through `poetry-core` via `poetry-dynamic-versioning.backend`, which
  has no `sbom-files` support — there is no way to emit one without changing build backends. It is
  also near-worthless here specifically: PEP 770 exists to describe *bundled* non-Python
  dependencies, and this wheel is pure Python with zero runtime dependencies, so the in-wheel
  document would be empty of exactly the thing it was designed to record. Revisit if `poetry-core`
  ships support.
- **SPDX output.** One format, not two. CycloneDX is the OWASP-published format and the one
  `cyclonedx-py` generates natively from `poetry.lock`; a second format doubles the surface that can
  go stale without doubling what anyone learns.
- **Build provenance attestation / artifact signing** (`actions/attest-build-provenance`, Sigstore).
  It is the obvious next step and is what Scorecard's `Signed-Releases` check wants, but it changes
  the publish path SPEC-012 built and verified, and folding it into a spec that only *adds* files
  would destroy that verification story. Its own spec.
- **A Dependency-Track (or equivalent) SBOM server.** Consuming SBOMs continuously needs a hosted
  service; this spec produces the artifact, it does not operate a platform.
- **Semgrep, OWASP ZAP, OWASP Dependency-Check.** Assessed and rejected: Semgrep's OWASP-mapped
  rulesets are substantially subsumed by CodeQL's `extended` suite, which already performs
  interprocedural dataflow on Python; ZAP is a DAST tool needing a listening endpoint, and this
  library's HTTP sinks are outbound clients with nothing to scan; Dependency-Check's Python analyzer
  is weak next to OSV/GHSA and would duplicate Dependabot with more NVD noise.
- **Opening an issue automatically when a scheduled scan fails.** A failed scheduled run already
  notifies; an `issues: write` permission to restate it is not worth the grant.
- **The two `sinks/` findings that surfaced while assessing this** — `SQLiteSink` interpolates an
  unvalidated table name, and `S608` is suppressed repository-wide rather than per-file. Both touch
  `src/` and belong in their own spec; this one keeps SPEC-022's property of changing no library
  code.
- **`pip-audit` on every pull request.** `dependency-review` already fails a PR that introduces a
  vulnerable dependency, reading the same lockfile diff. The unique value here is the *scheduled*
  re-examination of dependencies nobody is currently touching.

---

## Functional Requirements

### FR-001: An SBOM is generated from the locked dependency graph

#### Description:

Generate a CycloneDX SBOM from `pyproject.toml` + `poetry.lock` during the release workflow's
`build` job, covering all optional extras. Runtime dependencies are empty by design, so an SBOM
generated without `--all-extras` describes nothing — the extras *are* the dependency surface.

#### Acceptance Criteria:

- [ ] The `build` job produces a CycloneDX JSON document at a deterministic path.
- [ ] The document's root component is the distribution `log-foundry`, typed as a library.
- [ ] The document contains a component for every package reachable from the eleven entries in
      `[project.optional-dependencies]`, transitively — verified by asserting that at least
      `boto3`, `confluent-kafka`, `psycopg`, `pymongo` and `sentry-sdk` are present by name.
- [ ] The job fails if the document contains zero components, rather than uploading an empty SBOM.
- [ ] The document validates against the CycloneDX schema for the spec version it declares.
- [ ] Development dependencies (`pytest`, `ruff`, `mypy`, and the new `security` group) are **not**
      components of the SBOM: it describes what a consumer installs, not what built it.

### FR-002: The SBOM states the version that was actually published

#### Description:

The version in `pyproject.toml` is the literal placeholder `0.0.0`; the real value is injected from
Git tags at build time by `poetry-dynamic-versioning`. An SBOM generated against the un-rewritten
file would name a version that was never published, which is worse than no SBOM — it is a
confidently wrong one. `cyclonedx-py` has no flag to override the root component's version, so the
version must be resolved into `pyproject.toml` before the SBOM is generated (the plugin's own
`poetry dynamic-versioning` command does this; the implementation plan confirms the mechanism).

#### Acceptance Criteria:

- [ ] The SBOM's root component version equals the version of the built sdist/wheel in `dist/`.
- [ ] On a `vX.Y.Z` tag, that version equals `X.Y.Z` — asserted in the job, alongside the existing
      tag/build agreement check, and the job fails on disagreement.
- [ ] On a merge to `main`, the version is the `X.Y.Z.devN` pre-release actually uploaded.
- [ ] The SBOM's root component version is never `0.0.0`.
- [ ] `pyproject.toml` is not left rewritten in a way that changes what is built or published —
      the existing `build` and `publish` steps produce byte-identical distributions to those they
      produce today.

### FR-003: The SBOM is published where a consumer can find it

#### Description:

Attach the SBOM to a GitHub Release created on a version tag. No GitHub Release exists today — the
release workflow publishes to PyPI and stops — so the release itself must be created here. On a
merge to `main` the SBOM is uploaded as a workflow artifact instead, which keeps the generation path
exercised on every merge rather than first attempted at release time (the same reasoning SPEC-012
applied to `publish-dev`).

#### Acceptance Criteria:

- [ ] A push of a `vX.Y.Z` tag creates a GitHub Release named for that tag with the SBOM attached as
      a release asset.
- [ ] The release is created with `GITHUB_TOKEN` via the `gh` CLI — no third-party action, and so no
      new pin to maintain.
- [ ] The job creating the release requests `contents: write` and does **not** request
      `id-token: write`; the PyPI-publishing jobs keep `id-token: write` and do **not** gain
      `contents: write`.
- [ ] Release creation runs only after the PyPI publish succeeds — a GitHub Release must not exist
      for a version that failed to ship.
- [ ] Re-running a release for an existing tag does not fail the workflow on "release already
      exists"; the asset is replaced.
- [ ] Every workflow run, tagged or not, uploads the SBOM as a workflow artifact.

### FR-004: A scheduled audit re-examines the full dependency surface

#### Description:

Run `pip-audit` weekly against an environment resolved with every extra installed, so an advisory
published after a dependency was pinned turns a build red without waiting for someone to open a
pull request that touches it.

#### Acceptance Criteria:

- [ ] The audit runs on a weekly schedule, on push to `main`, and on `workflow_dispatch`.
- [ ] The audited environment includes all optional extras — a vulnerable transitive dependency of
      any one of the eleven extras is detected.
- [ ] The job exits non-zero when `pip-audit` reports at least one known advisory.
- [ ] The job succeeds and reports "no known vulnerabilities" when there are none.
- [ ] A run on a Python version where an extra is unresolvable (e.g. the `clickhouse` marker's
      `python_version < '3.15'` bound) does not fail the job for that reason alone.

### FR-005: An unfixable advisory is suppressed with a written reason, never silently

#### Description:

An advisory with no fixed release, or one that does not apply to how this library uses the package,
must be suppressible — otherwise the first such case turns the scan into noise everyone learns to
ignore. It must be suppressed the way this repository already suppresses a lint rule: named
individually, in a tracked file, with the reason beside it.

#### Acceptance Criteria:

- [ ] Suppressions are per-advisory (a specific `GHSA-`/`PYSEC-` id), never a blanket severity
      threshold or a wholesale package exclusion.
- [ ] Each suppression carries a written reason in the same file, in the style of the `ignore` block
      in `pyproject.toml`.
- [ ] The suppression list is empty on merge if nothing currently needs suppressing — the mechanism
      exists, unused, rather than being invented under time pressure during an incident.
- [ ] A suppressed advisory does not suppress any other advisory for the same package.

### FR-006: OpenSSF Scorecard measures the posture SPEC-022 built

#### Description:

Add the Scorecard workflow, reporting SARIF to code scanning and publishing results to the OpenSSF
public API. SPEC-022's controls are currently asserted by the specs that added them; Scorecard makes
them continuously measured, so a regression (an unpinned action, a widened permission, a lapsed
branch rule) shows up without anyone re-reading the workflows.

#### Acceptance Criteria:

- [ ] Scorecard runs weekly and on push to the default branch, with the `branch_protection_rule`
      trigger its documentation requires.
- [ ] Results upload to code scanning as SARIF and appear as alerts, without disturbing the existing
      CodeQL default-setup upload or zizmor's SARIF category.
- [ ] The job requests `security-events: write`, `id-token: write` and `contents: read` — and the
      spec records in a comment that this `id-token` is audienced to the OpenSSF API and is not the
      PyPI publishing token, since a reader of `release.yml` will otherwise read the two as the same
      grant.
- [ ] `publish_results` is enabled.
- [ ] The job does not fail the build on a low score — consistent with the settled decision that a
      scanner's exit code is not its verdict; only `dependency-review` and `pip-audit` gate.
- [ ] Checks that Scorecard cannot evaluate without a stored personal access token (notably
      `Branch-Protection`) are documented as deliberately inconclusive. No PAT is created: a stored
      long-lived credential is the exact class of thing SPEC-022's OIDC-and-SHA-pins design avoids,
      and one score line is not worth reintroducing it.

### FR-007: The audit tooling is locked and maintained, not fetched ad hoc

#### Description:

`cyclonedx-bom` and `pip-audit` go in an optional Poetry group so their versions live in
`poetry.lock` and Dependabot's existing `pip` ecosystem maintains them — but are not installed by
the ordinary `poetry install --with dev` that every CI matrix leg runs.

#### Acceptance Criteria:

- [ ] Both tools are declared in a `[tool.poetry.group.security.dependencies]` block marked
      `optional = true`.
- [ ] `poetry install --with dev` does **not** install them; the two new workflows install them
      explicitly.
- [ ] `poetry.lock` is regenerated and committed in the same pull request as the `pyproject.toml`
      change — CI's `poetry install` hard-fails on the split.
- [ ] Both appear in Dependabot's existing `pip` grouping and inherit its cooldown.
- [ ] `pyproject.toml` carries no `version` key and no reordered `[tool.poetry]` keys after the
      change (the `poetry-dynamic-versioning` round-trip wart).

### FR-008: New actions are pinned and permissions are stated

#### Description:

Extend SPEC-022 FR-007 and FR-009 to every workflow and step this spec adds.

#### Acceptance Criteria:

- [ ] Every `uses:` added by this spec references a full commit SHA with a trailing `# vX.Y.Z`
      comment in exactly the form Dependabot rewrites (version last, no trailing prose).
- [ ] Every new workflow states top-level `permissions`, and every job states its own.
- [ ] Every new `actions/checkout` sets `persist-credentials: false` unless the job demonstrably
      needs the credential, in which case the need is stated in a comment.
- [ ] `zizmor` reports no new findings against the added workflows.

### FR-009: The result is documented where a consumer will look

#### Description:

An SBOM nobody can locate is not transparency. Record where it is published and how to reproduce
both scans locally.

#### Acceptance Criteria:

- [ ] `SECURITY.md` states that an SBOM is attached to each GitHub Release and names its format.
- [ ] `README.md` links to the releases page from its existing security section.
- [ ] `CLAUDE.md`'s Common Commands gains the local invocations for the SBOM build and the audit.
- [ ] Both documented local commands run clean on a fresh checkout.

---

## Data Model

```
# The SBOM artifact
sbom.cdx.json          # CycloneDX JSON, spec version pinned in the generating command
  metadata.component:
    type:     "library"
    name:     "log-foundry"
    version:  "<resolved release version — never 0.0.0>"    # FR-002
  components: [...]    # every package reachable from all extras, transitively   # FR-001

# Release asset name — tag-qualified so a downloaded file is self-identifying
log-foundry-<X.Y.Z>.cdx.json
```

## API / Interface Contract

No library API changes. This spec adds no public symbol, and `src/` is untouched.

## Configuration / Environment

**New files**

| Path | Purpose |
|---|---|
| `.github/workflows/pip-audit.yml` | FR-004, FR-005 — scheduled dependency audit |
| `.github/workflows/scorecard.yml` | FR-006 — OpenSSF Scorecard |

**Modified**

| Path | Change |
|---|---|
| `.github/workflows/release.yml` | SBOM generation in `build`; a new release-creation job (FR-001..003) |
| `pyproject.toml` + `poetry.lock` | the optional `security` group (FR-007) |
| `SECURITY.md`, `README.md`, `CLAUDE.md` | FR-009 |

**Tooling versions at authoring time** — the implementation pins the SHA for the tag current when
it runs, which may be later than these:

- `cyclonedx-bom` 7.3.1 (provides the `cyclonedx-py` CLI; `poetry` subcommand, `--all-extras`,
  `--mc-type library`)
- `pip-audit` — CLI run inside the Poetry environment, not via `pypa/gh-action-pip-audit`, matching
  how `release.yml` already runs `build` and `twine` directly. One fewer pin to maintain.
- `ossf/scorecard-action` v2.4.4
- `github/codeql-action/upload-sarif` — already pinned in `zizmor.yml`; reuse that exact pin.

**No new repository settings.** Everything here is a file in the repository, unlike SPEC-022's
CodeQL default setup. Nothing in this spec can be silently disabled by a UI toggle.

## File & Folder Structure

```
.github/
└── workflows/
    ├── ci.yml                  (unchanged)
    ├── dependency-review.yml   (unchanged)
    ├── release.yml             (modified — FR-001, FR-002, FR-003)
    ├── spec-lint.yml           (unchanged)
    ├── zizmor.yml              (unchanged)
    ├── pip-audit.yml           (new — FR-004, FR-005)
    └── scorecard.yml           (new — FR-006)
pyproject.toml                  (modified — FR-007)
poetry.lock                     (regenerated — FR-007)
SECURITY.md                     (modified — FR-009)
README.md                       (modified — FR-009)
CLAUDE.md                       (modified — FR-009)
```

## Implementation Phases

### Phase 1: Audit tooling, locked

- Add the optional `security` Poetry group with `cyclonedx-bom` and `pip-audit`.
- Regenerate and commit `poetry.lock`; confirm `poetry install --with dev` is unchanged and that
  `pyproject.toml` carries no reintroduced `version` key.
- Establish both local commands and add them to `CLAUDE.md`.

### Phase 2: SBOM generation and publication

- Generate the SBOM in `release.yml`'s `build` job with the version resolved (FR-002), assert the
  component floor and the version, and upload it as a workflow artifact.
- Add the release-creation job: runs after `publish-release`, `contents: write` only, `gh release
  create` with the SBOM attached, idempotent on re-run.
- Verify on the real path — a merge to `main` must produce the artifact, and the dev publish must
  still succeed.

### Phase 3: Scheduled audit

- Add `pip-audit.yml`: all extras installed, gating, with the reasoned per-advisory suppression
  mechanism in place and empty.
- Confirm both outcomes are reachable — a clean run passes, and a deliberately introduced vulnerable
  pin fails the job (reverted before merge).

### Phase 4: Scorecard, and the documentation

- Add `scorecard.yml` with its documented triggers and permissions, `publish_results` on, SARIF to
  code scanning.
- Confirm its SARIF lands as alerts without disturbing CodeQL default setup or zizmor's category,
  and record which checks are inconclusive without a PAT.
- Update `SECURITY.md` and `README.md`; run `zizmor` over all seven workflows.
