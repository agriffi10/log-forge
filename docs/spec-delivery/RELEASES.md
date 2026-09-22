# Releases — which specs shipped in which version

Moved here on 2026-09-02 from `CLAUDE.md`'s `## Specs` section, which was pruned to its live
contents. This pairing was recorded **nowhere else**: git tags carry the versions, the delivery docs
carry what each spec shipped, and nothing joined the two. It is history, so it belongs in the
delivery tier rather than in the file that loads every session.

Tags are the source of truth for what exists; this table is the source of truth for what each tag
carried. **`CHANGELOG.md` at the repository root is the user-facing half of the same act**: this
table maps a version to its SPECs, that file tells a caller what changed. Cutting a release owes
both, and nothing gates either. Add a row when a release is cut — **except when a row is
pre-written in the commit its tag will be cut from**, which is how a top row can arrive naming a
tag that does not exist yet. Two rows here were written that way, for reasons of different
strength: `v1.0.0`'s because something in the tagged tree links this table and would otherwise
reach a copy missing its own row, `v1.0.1`'s only because its release preparation was a single
commit with no later one to add the row in.

**`v1.0.0`'s row was written in the commit its tag was cut from, and that was deliberate.**
Everywhere else the rule is *add a row when a release is cut*, because the table's one job is
saying what each tag actually shipped. A row added *after* the tag, though, leaves the table a
release behind for as long as nobody notices, and that release is where it mattered most: its
notes file links this table at `blob/v1.0.0/…`, an absolute tagged URL, so the table a reader
arrives at through the release is the one in the tagged commit. (`release.yml` does check out the
tag, but its `sparse-checkout` is `docs/release-notes` and it never materialises this file — the
link is what pins it, so keep that link tag-absolute rather than relative.) A pre-written row
therefore describes a tag that does not exist yet, and stops being a claim about the future the
moment its tag is pushed.

**`v1.0.1`'s row is pre-written for a weaker reason**, and it is worth saying so rather than
letting it borrow the argument above. Nothing in that tag's tree links this table, so nothing
forced the row into the tag commit; the release was prepared in one commit and that commit is the
one the tag is cut from, so there was no later commit to add it in. Convenience, not mechanism.

**Which makes any pre-written row a hostage to the tag actually being cut, so re-read it before
you cut.** Three ways it goes wrong. **A spec completes first:** on `v1.0.0`'s row the exposure
was the `SPEC-024 – SPEC-055` closing number going stale, and a slipping tag is exactly when that
happens; a row claiming **no** SPECs, as `v1.0.1`'s does, is falsified the same way and less
visibly, since nothing in the row itself looks like a number to re-check. `docs/specs/INDEX.md` is
where the range actually ends, so re-derive from it either way. **The tag is cut under a different
number:** the row is wrong, and so is the notes file's name — correct both, and see the paragraph
below for why the name matters. **The tag is never cut:** the row describes nothing, and should be
reverted to a "not yet released" note rather than left standing.

**The notes file is coupled to the tag by NAME.** `release.yml` looks for
`docs/release-notes/<tag>.md`, so cutting a tag with no matching file misses it silently, falls
back to `--generate-notes`, and puts a hundred spec-level commit subjects into a release body that
cannot afterwards be amended. If the tag actually cut differs from the one a pre-written notes file
names, rename the file to match and re-read its opening paragraph — each notes file is written for
the specific release it names, not as a template for the next one.

| Version | Carried |
|---|---|
| `v1.0.1` | No SPECs — no library code changed. It publishes the README work that landed after the `v1.0.0` tag: the page rewritten to read as a released `1.0.0` (#240, which carries the widened freeze wording), its links made absolute so they resolve on PyPI (#242), and a Changelog project URL. Notes: `docs/release-notes/v1.0.1.md` |
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
