---
type: Decision
title: "`pip-audit` gates, and audits the extras or it audits nothing"
description: "`dependency-review` only sees a PR's dependency *diff*, so the weekly re-examination is the point. `--no-root` is load-bearing and `--strict` is on, because a silently skipped package is a silently unaudited one. (SPEC-023)"
---

**`pip-audit` gates, and audits the extras or it audits nothing** — `dependency-review` only sees a PR's dependency *diff*, so an advisory against an already-pinned dependency is invisible to it; the weekly re-examination is the point. `--no-root` is load-bearing (Poetry installs the project editable, and `--strict` refuses an editable distribution), and `--strict` is on because a silently skipped package is an unaudited one. Suppressions are per-advisory with written reasons in `.github/pip-audit-ignores.txt`, never by severity or package. (SPEC-023)

## History

### 2026-08-02

- **Settled.**
