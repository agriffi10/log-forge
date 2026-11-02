---
type: Decision
title: "Release assets are attached to a draft, never to a published release"
description: "immutable releases freeze assets at publish, so it is create-as-draft → upload → publish. Deleting an immutable release does **not** free its tag name; a botched release is repaired only by a new version tag. (SPEC-023)"
---

**Release assets are attached to a draft, never to a published release** — this repository has immutable releases enabled, so assets freeze at publish: create-as-draft → upload → publish. And deleting an immutable release does **not** free its tag name, so a botched release is repaired only by a new version tag, never by recreating the old one. Both were learned by shipping `v0.10.0` without its SBOM and then making it unrepairable. The job is deliberately *not* idempotent — a re-run that claimed to replace an asset it cannot touch would be lying. (SPEC-023)

## History

### 2026-08-02

- **Settled.**
