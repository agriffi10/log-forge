# Releases — which specs shipped in which version

Moved here on 2026-09-02 from `CLAUDE.md`'s `## Specs` section, which was pruned to its live
contents. This pairing was recorded **nowhere else**: git tags carry the versions, the delivery docs
carry what each spec shipped, and nothing joined the two. It is history, so it belongs in the
delivery tier rather than in the file that loads every session.

Tags are the source of truth for what exists; this table is the source of truth for what each tag
carried. Add a row when a release is cut.

| Version | Carried |
|---|---|
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
