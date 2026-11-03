---
type: Decision
title: "Version comes from Git tags, published to PyPI as `log-foundry`"
description: "tags cut releases; merges to `main` publish **no** `.devN` for now (`publish-dev` off). (SPEC-012)"
---

**Version comes from Git tags, published to PyPI as `log-foundry`** — tags cut releases, merges to `main` publish `.devN` pre-releases. (SPEC-012)

> **Suspended, not reversed.** The `pypi` environment gained a deployment-branch policy admitting only `v*` tags, so a deployment from `refs/heads/main` is refused before `publish-dev`'s first step and every merge to `main` went red. The job is left in place with `if: false` and a restoration note; the tag path is untouched. While it is off, the second half of this decision is not true of the repository, and the property it bought — that a tagged release is never the first attempt at the upload path — is not held.

## History

### 2026-09-06

- **Suspended, not reversed:** the `pypi` environment refuses `publish-dev` from `main`, so merges publish no `.devN`.

### 2026-07-22

- **Settled** (SPEC-012).
