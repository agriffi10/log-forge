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
tag that does not exist yet. Four rows here were written that way, for reasons of different
strength: `v1.0.0`'s because something in the tagged tree links this table and would otherwise
reach a copy missing its own row, `v1.0.1`'s, `v1.0.2`'s and `v1.1.0`'s only because each release
preparation was a single commit with no later one to add the row in.

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
letting it borrow the argument above. Nothing in that tag's tree links this table **tag-absolutely**
— `README.md` links it at `blob/main`, which follows the branch rather than reaching the tagged
copy — so nothing forced the row into the tag commit; the release was prepared in one commit and that commit is the
one the tag is cut from, so there was no later commit to add it in. Convenience, not mechanism.
**`v1.0.2`'s row is pre-written for the same weaker reason**: its notes link `CHANGELOG.md` and the
three delivery docs tag-absolutely, but not this table. **So is `v1.1.0`'s**: its notes link
nothing outside themselves, since the lead's links are anchors within the file, and nothing else
in its tree links this table at `blob/v1.1.0`.

**Which makes any pre-written row a hostage to the tag actually being cut, so re-read it before
you cut.** Three ways it goes wrong. **A spec completes first:** on `v1.0.0`'s row the exposure
was the `SPEC-024 – SPEC-055` closing number going stale, and a slipping tag is exactly when that
happens; a row claiming **no** SPECs, as `v1.0.1`'s does, is falsified the same way and less
visibly, since nothing in the row itself looks like a number to re-check. `v1.0.2`'s row names
`SPEC-057 – SPEC-059` and says `SPEC-056` ships nothing because it is a Draft, so that spec
completing before the tag falsifies it. `v1.1.0`'s names SPEC-056, SPEC-061 – SPEC-068 and
SPEC-080 and five fixes by PR number, so a spec or a library fix landing before the tag falsifies
it the same way. That is not hypothetical: `v1.0.2`'s row was written before #283 and #284 landed,
was tagged naming two no-spec fixes where the tag carried four, and was corrected after the tag. `docs/specs/index.yaml` (generated from each spec's
frontmatter; the old `INDEX.md`'s status table moved there, its arcs to `ARCS.md`) is where
the range actually ends, so re-derive from it either way. **The tag is cut under a different
number:** the row is wrong, and so is the notes file's name — correct both, and see the paragraph
below for why the name matters. **The tag is never cut:** the row describes nothing, and should be
reverted to a "not yet released" note rather than left standing. In every case re-check the
`CHANGELOG.md` heading too: its date is pre-written with the row, and a tag that slips makes it
wrong.

**The notes file is coupled to the tag by NAME.** `release.yml` looks for
`docs/release-notes/<tag>.md`, so cutting a tag with no matching file misses it silently, falls
back to `--generate-notes`, and puts a hundred spec-level commit subjects into a release body that
cannot afterwards be amended. If the tag actually cut differs from the one a pre-written notes file
names, rename the file to match and re-read its opening paragraph — each notes file is written for
the specific release it names, not as a template for the next one.

| Version | Carried |
|---|---|
| `v1.1.0` | SPEC-056, SPEC-061 – SPEC-068 and SPEC-080 — the OTLP sink, `NATSSink` liveness and a bounded close, the exit drain fitted to `shutdown()`'s budget, assembly keys and scalars, a bounded and total baggage header, `int`-only `configure()` ceilings and sink arguments with `flush()`/`shutdown()` timeouts judged at entry, a total `continue_trace`, shutdown-expiry bookkeeping, and `@trace` judging its arguments by their real type; plus five library fixes with no spec, #286 and #287 (`HoneycombSink` counts the events Honeycomb rejects, and refuses a dataset that is not one path segment), #291 (HTTP base URLs, `DatadogSink`'s `site` and `SentrySink`'s DSN path), #295 (an HTTP sink URL carrying a user or password) and #300 (an `@` outside an HTTP sink URL's host, and a `SentrySink` DSN on port `0`). One public import path added, `log_foundry.sinks.otlp.OTLPSink`; no existing public name or signature changed. Notes: `docs/release-notes/v1.1.0.md` |
| `v1.0.2` | SPEC-057 – SPEC-059 — assembly bounded in work, one bad event costing only itself, and retry-safe drop counts in every sink; plus four library fixes with no spec, #255 (`LokiSink` epoch nanoseconds exact), #276 (`ElasticsearchSink` and `OpenSearchSink` count a `2xx` `_bulk` body with no readable verdict instead of ignoring it or re-sending), #283 (an HTTP sink decides by the status once a status line arrived, and never re-sends a `2xx` whose body it could not read) and #284 (the HTTP family refuses at construction a URL, method, header or credential `http.client` cannot send). SPEC-056 (OTLP) is a Draft and shipped nothing. No public name or signature changed. Notes: `docs/release-notes/v1.0.2.md` |
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
