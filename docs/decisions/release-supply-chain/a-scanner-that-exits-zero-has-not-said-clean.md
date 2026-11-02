---
type: Decision
title: "A scanner that exits zero has not said \"clean\""
description: "the alert count is the verdict, never the check mark — zizmor and CodeQL pass the job regardless of findings by design, and only `dependency-review` fails a build. (SPEC-022)"
---

**A scanner that exits zero has not said "clean"** — zizmor in SARIF mode and CodeQL both report to code scanning and pass the job regardless of findings, deliberately: Advanced Security owns triage, and blocking belongs in a ruleset. Only `dependency-review` fails a build. So the alert count is the verdict, never the check mark — and a green audit is not evidence a *setting* is present (zizmor's `dependabot-cooldown` stops at the first passing entry). State the setting. (SPEC-022)

## History

### 2026-07-31

- **Settled.**
