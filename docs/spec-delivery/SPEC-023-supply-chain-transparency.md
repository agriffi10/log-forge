# Completed Spec — SPEC-023: Supply-Chain Transparency and Dependency Auditing

## What was completed?

SPEC-022's scanners all look *inward* — CodeQL at this source, zizmor at these workflows,
`dependency-review` at what a PR *introduces*. So nothing described what had already shipped, and
nothing re-examined the eleven optional extras after the merge that pinned them: an advisory
published against an already-pinned dependency produces no diff for `dependency-review` to fail.
Both gaps are closed. Like SPEC-022, no library behaviour changed.

- **CycloneDX SBOM per release** (FR-001..003) — `scripts/make-sbom.py`, wired into `release.yml`.
  Published as a GitHub Release asset; the repository had nine tags and **zero** Releases before
  this, so the release itself had to be created too.
- **`pip-audit.yml`** (FR-004, FR-005) — weekly, plus push to `main` and `workflow_dispatch`, over
  every extra, gating. Per-advisory suppression lives in `.github/pip-audit-ignores.txt` with the
  reason beside each id; it ships empty.
- **`scorecard.yml`** (FR-006) — OpenSSF Scorecard to code scanning, non-gating, `publish_results`
  on.
- **Optional `security` Poetry group** (FR-007) holding `cyclonedx-bom` and `pip-audit`, so both are
  locked and Dependabot-maintained but absent from `poetry install --with dev`.
- **`SECURITY.md` + `README.md`** (FR-009) state where the SBOM lives, what it covers, and — plainly
  — that it is **not signed**.

**Three amendments, each forced by evidence rather than preference.**

1. **FR-001's mechanism was wrong.** The spec was authored around `cyclonedx-py poetry`, which
   cannot read this project: it takes the root component from `[tool.poetry].name`, absent here
   because the name lives in `[project]` under PEP 621, and dies with `KeyError: 'name'`. Supplying
   the key yields **zero components** — the parser then reads `[tool.poetry.dependencies]` while all
   extras live in `[project.optional-dependencies]`. Same root cause `dependabot.yml` already
   documents. Replaced by `cyclonedx-py environment`, which reads installed distributions.
2. **FR-003's idempotency criterion was unachievable.** Struck through in place: immutable releases
   make "a re-run replaces the asset" impossible, so the job now fails loudly instead.
3. **FR-006's PAT assumption was wrong, in the flattering direction.** The criterion expected
   `Branch-Protection` to be inconclusive without a stored token. The job token read the ruleset
   and scored it 6/10. The decision (no PAT) stands; only its stated cost was overstated.

## What changed from earlier specs?

- **SPEC-012's `release.yml` gained two steps and a job.** The `build` job generates and
  version-checks the SBOM on *every* run, not only on tags — same reasoning SPEC-012 applied to
  `publish-dev`. The new `github-release` job is the only one holding `contents: write` and
  deliberately holds no `id-token`; the publish jobs keep the inverse.
- **`S608` narrowing and the `SQLiteSink` identifier dedupe** shipped alongside as unspecced
  hygiene (#85), after an initial report of a `SQLiteSink` injection hole proved wrong —
  `sqlite.py:52` already validated the table name.

## Verification

Local per PR: `ruff`, `mypy --strict` (48 files), 568 tests, `spec-lint`, and `zizmor` over all
seven workflows — all clean. CI green on 3.12 and 3.13 across #86, #87, #88, #89, #90, #91.

Verified on the real path, not merely locally:

- SBOM on `main`: `log-foundry 0.9.1.dev4, 43 components`, `built=… sbom=…` agreeing.
- `pip-audit` green on `main`; the gate proven both ways — `jinja2==3.1.2` (5 advisories) exits 1,
  all five suppressed exits 0, and **one** of five suppressed still exits 1.
- Scorecard SARIF landed in three categories alongside CodeQL default setup.
- **FR-003 end to end on `v0.10.1`**: the asset is publicly downloadable, 43 components, all extras
  present, no build tooling, release immutable.

**The cost of that last line.** `v0.10.0` published to PyPI correctly and shipped **without** its
SBOM — this repository has immutable releases enabled, so assets freeze at publish, and the job
created the release before uploading. Deleting the release to recreate it made things worse:
GitHub permanently reserves the tag name of any release that was ever immutable, so `v0.10.0` can
now carry no release at all. `v0.10.1` was cut to obtain a correct one. The fix is
create-as-draft → upload → publish. FR-003 was flagged as unverifiable-without-a-tag throughout the
build; flagging it did not prevent it, which is the lesson worth keeping.

## Notes for the next spec

- **Build provenance attestation** (`actions/attest-build-provenance`) is the natural successor and
  what Scorecard's `Signed-Releases` wants. Deferred deliberately: it changes the publish path
  SPEC-012 verified. The GitHub Release this spec added is its prerequisite.
- **The `main` ruleset has no `required_status_checks` rule** (Scorecard: `no status checks found
  to merge onto branch 'main'`). A PR can merge with red CI; only the review gate blocks it. A
  repository setting, not a file — it needs a decision.
- **`postgres.py:49` has a real `arg-type` error** (`str | None` where `str` is expected) visible
  only when `psycopg` is installed. CI never installs extras, so `mypy --strict` cannot see it.
