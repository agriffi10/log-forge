# Completed Spec — SPEC-022: Security Scanning in CI

## What was completed?

Nothing in CI looked for a vulnerability beyond `ruff`'s single-file `S` rules. Four controls now
do, all free because the repository is public, and every action is pinned to a commit SHA. No file
under `src/` changed.

- **CodeQL** over `python` and `actions`, `query_suite: extended`, weekly — a repository setting,
  not a workflow (see the deviation below).
- **`dependency-review.yml`** (FR-005) — fails a PR that *introduces* a vulnerable dependency, at
  `fail-on-severity: moderate`. The only scan added here that blocks a merge; the rest report to
  code scanning.
- **`dependabot.yml`** (FR-003, FR-004) — scheduled version updates for `pip` (Poetry) and
  `github-actions`, on top of the security updates already enabled. Both carry a `cooldown`.
- **`zizmor.yml`** (FR-006) — workflow static analysis, SARIF to code scanning, pinned by SHA
  because the action publishes no moving major tag.
- **Every action SHA-pinned** (FR-007) across all five workflows, each with a `# vX.Y.Z` comment in
  the exact form Dependabot rewrites. Includes `pypa/gh-action-pypi-publish`, deliberately moved
  off the rolling `release/v1` branch PyPA recommends — it is the job holding `id-token: write`
  against PyPI.
- **`SECURITY.md`** + private vulnerability reporting enabled (FR-008), and every workflow now
  states its `permissions` rather than inheriting the default (FR-009).

**Deliberate deviations.** (1) **CodeQL ships as default setup, not the advanced setup the spec was
authored with.** Default setup was enabled by hand mid-spec, and it is not additive — it disables
any CodeQL workflow and blocks CodeQL API uploads, so a `codeql.yml` would have run on every PR and
uploaded nothing. The spec was amended in [#73](https://github.com/agriffi10/log-forge/pull/73)
rather than the setting reverted; `query_suite` was raised to `extended` to recover the recall
FR-002 asked for. (2) **The two secret-scanning sub-settings named in Configuration are unavailable,
not merely unconfigured** — validity checks and generic patterns require an org-owned repository
with GitHub Secret Protection, which a personal account cannot purchase. `PATCH /repos` returns 200
and silently ignores them because the field is absent from that endpoint's request schema. Recorded
as a constraint. (3) **Actions were pinned to the tip of the major already in use**, not the newest
major — `checkout` was on v5 (v7 current) and `setup-python` on v6 (v7 current), drift the tag pins
had hidden. Folding a behavioral upgrade into a pinning commit would have destroyed its
verification story; both majors were left for Dependabot, which opened them within minutes as
[#76](https://github.com/agriffi10/log-forge/pull/76) and
[#77](https://github.com/agriffi10/log-forge/pull/77).

**Findings triaged** (FR-001, FR-006). zizmor's first run reported 7 warnings — no `unpinned-uses`
and no `excessive-permissions`, so FR-006 and FR-009 held on the first try. Both audits that did
fire were real and were fixed in the same PR: `artipacked` ×5 (`actions/checkout` persists the
token on disk; none of these jobs runs git, and the publish path uses OIDC, so
`persist-credentials: false` everywhere) and `dependabot-cooldown` ×2 (nothing made Dependabot wait
before adopting a fresh release — the exact shape this spec targets). Zero open zizmor alerts
afterwards. CodeQL's first `extended` run over `src/` reported nothing.

## What changed from earlier specs?

- **SPEC-012's `release.yml` no longer tracks `pypa/gh-action-pypi-publish@release/v1`.** It is
  SHA-pinned, so a fix shipped to that branch no longer arrives automatically; Dependabot moves the
  pin instead. Verified on the real publish path — the `publish-dev` job uploaded successfully
  after the change, not just CI.
- **`ci.yml` and `spec-lint.yml` gained `permissions: contents: read`**, and every checkout gained
  `persist-credentials: false`. Any future job in these workflows that needs to push must now ask
  for those explicitly.

## Verification

Local per PR: `ruff` clean, `mypy --strict` clean, 568 tests pass, `spec-lint` clean. CI green on
3.12 and 3.13 across [#74](https://github.com/agriffi10/log-forge/pull/74),
[#75](https://github.com/agriffi10/log-forge/pull/75) and
[#78](https://github.com/agriffi10/log-forge/pull/78) — ten checks on the last of these, up from
three before this spec. `main` green and the Release job's PyPI upload confirmed after the pinning
change.

FR-003 and FR-004 were verified against Dependabot's real output rather than assumed. It rewrote
both the SHA and its `# vX.Y.Z` comment across all four `checkout` occurrences
([#76](https://github.com/agriffi10/log-forge/pull/76),
[#77](https://github.com/agriffi10/log-forge/pull/77)); the first `pip` PRs changed `poetry.lock`
and `pyproject.toml` **in the same PR** — the split that hard-fails CI's `poetry install`;
grouping held, with three minor/patch updates bundled into
[#79](https://github.com/agriffi10/log-forge/pull/79) and mypy's major arriving alone in
[#80](https://github.com/agriffi10/log-forge/pull/80); optional-extra dependencies (`boto3`,
`botocore`) produced updates without the `allow: dependency-type: all` fallback FR-003 held in
reserve, so it was not needed; and `ruff` was left untouched, confirming the `ignore` protects the
`>=0.16,<0.17` bound.
