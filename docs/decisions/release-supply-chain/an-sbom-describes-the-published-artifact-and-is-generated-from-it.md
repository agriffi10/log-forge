---
type: Decision
title: "An SBOM describes the published artifact, and is generated from it"
description: "`make-sbom.py` describes the built wheel installed with every extra, and runs from a *second* venv or it lists its own ~30 dependencies as the library's. (SPEC-023)"
---

**An SBOM describes the published artifact, and is generated from it** — `make-sbom.py` installs the built wheel with every extra into a throwaway venv and describes *that*, because runtime dependencies are empty by design and the extras are the whole dependency surface. The generator runs from a *second* venv or it lists itself and its ~30 dependencies as the library's (measured: 98 components vs 43). `cyclonedx-py`'s `poetry` mode cannot read this project at all — it wants `[tool.poetry].name`, and PEP 621 puts the name in `[project]`, the same misreading `dependabot.yml` documents. An empty SBOM, one versioned `0.0.0`, or one carrying build tooling fails the job: an inaccurate SBOM is worse than none, because it looks authoritative. (SPEC-023)

## History

### 2026-08-02

- **Settled.**
