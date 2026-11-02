---
type: Decision
title: "Every action is pinned to a commit SHA, and the pins are maintained, not frozen"
description: "a mutable tag on a workflow holding `id-token: write` against PyPI is a silent path from a third-party repository into every consumer's install. A lagging pin fails loudly; a compromised action fails forever. The version comment must read exactly `# vX.Y.Z` or Dependabot silently stops rewriting the pin — that comment is what \"maintained\" rests on. (SPEC-022)"
---

**Every action is pinned to a commit SHA, and the pins are maintained, not frozen** — a mutable tag on a workflow holding `id-token: write` against PyPI is a silent path from a third-party repository to every consumer's `pip install`, so `pypa/gh-action-pypi-publish` is pinned away from the `release/v1` branch PyPA itself recommends: a lagging pin fails loudly at release time, a compromised action fails forever. Dependabot's `github-actions` ecosystem moves the pins, which is what makes pinning affordable — the version comment must stay exactly `# vX.Y.Z` or it silently stops rewriting it. Pin to the tip of the major in use; a major bump is its own reviewable PR. (SPEC-022)

## History

### 2026-07-31

- **Settled.**
