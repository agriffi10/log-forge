# Releases — which specs shipped in which version

Moved here on 2026-09-02 from `CLAUDE.md`'s `## Specs` section, which was pruned to its live
contents. This pairing was recorded **nowhere else**: git tags carry the versions, the delivery docs
carry what each spec shipped, and nothing joined the two. It is history, so it belongs in the
delivery tier rather than in the file that loads every session.

Tags are the source of truth for what exists; this table is the source of truth for what each tag
carried. Add a row when a release is cut — **with one deliberate exception, immediately below**,
where the top row is written in the commit its tag will be cut from and therefore names a tag that
does not exist yet.

**`v1.0.0`'s row is written in the commit its tag is cut from, and that is deliberate.**
Everywhere else the rule is *add a row when a release is cut*, because the table's one job is
saying what each tag actually shipped. A row added *after* the tag, though, leaves the table a
release behind for as long as nobody notices, and this is the release where that matters most:
the notes file links this table at `blob/v1.0.0/…`, an absolute tagged URL, so the table a reader
arrives at through the release is the one in the tagged commit. (`release.yml` does check out the
tag, but its `sparse-checkout` is `docs/release-notes` and it never materialises this file — the
link is what pins it, so keep that link tag-absolute rather than relative.) The row below
therefore describes a tag that does not exist yet, and stops being a claim about the future the
moment `v1.0.0` is pushed.

**Which makes the row a hostage to the tag actually being cut, so re-read it before you cut.**
Three ways it goes wrong, in the order they are likely. **A spec completes first:** the row's
`SPEC-024 – SPEC-055` closing number is then stale, and a slipping tag is exactly when that
happens — `docs/specs/INDEX.md` is where the range actually ends, so re-derive it rather than
trusting the number here. **The tag is cut under a different number:** the row is wrong, and so is
the notes file's name — correct both, and see the paragraph below for why the name matters.
**The tag is never cut:** the row describes nothing, and should be reverted to a "not yet
released" note rather than left standing.

**The notes file is coupled to the tag by NAME.** `release.yml` looks for
`docs/release-notes/<tag>.md`, so cutting anything other than `v1.0.0` misses it silently, falls
back to `--generate-notes`, and puts a hundred spec-level commit subjects into a release body that
cannot afterwards be amended. If the next tag is not `v1.0.0`, rename the file to match it and
re-read its opening paragraph, which is written for a `1.0.0` specifically.

| Version | Carried |
|---|---|
| `v1.0.0` | SPEC-024 – SPEC-055 — the three pre-1.0 audit arcs, the API freeze, and the first release under semantic versioning. Notes: `docs/release-notes/v1.0.0.md` |
| `v0.10.1` | SPEC-023 — the first release carrying an SBOM |
| `v0.10.0` | SPEC-023, shipped **without** its SBOM; the GitHub Release is unrepairable (see the SPEC-023 delivery doc) |
| `v0.9.0` | SPEC-022, plus the extras-floor raise |
| `v0.8.0` | SPEC-021 |
| `v0.7.1` | SPEC-020 |
| `v0.7.0` | SPEC-018 + SPEC-019 |
| `v0.6.0` | SPEC-017 |
| `v0.5.0` | SPEC-016 |
| `v0.4.0` | SPEC-015 |
| `v0.3.0` | SPEC-013 + SPEC-014 |
| `v0.2.0` | the `log_forge` → `log_foundry` rename (no spec — a mechanical change) |
| `v0.1.0` | the first stable release |
| `v0.0.1` | the first tag cut, before the package was published under this name |
