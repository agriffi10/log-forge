---
type: Decision
title: "CI runs on Linux only: a macOS leg was offered and declined"
description: "the behavioural suite runs on `ubuntu-latest` and gains no macOS leg, by Andrew's choice of 2026-11-14, so a macOS-only defect is found on a developer's machine or not at all. Do not add one to `.github/workflows/ci.yml` as the fix for a platform defect; reopening this is his call. (2026-11-14 audit)"
---

The test job in `.github/workflows/ci.yml` runs on `ubuntu-latest`, on each supported Python, and
that is the whole platform matrix. The 2026-11-14 audit recommended adding a macOS leg, and Andrew
declined it on 2026-11-14 in the sentence that accepted the audit's other recommendations: "I'll
take your recommendations other than the mac os specific one". He gave no reason, and none is
recorded here on his behalf.

**What prompted the recommendation.** Two defects in the record showed on macOS and not on the
Linux runner, both a crash inside Apple's `os_log`, which `libsqlite3` reaches and which is not
safe after a fork. `01fd73cd` is the first: a forked child died of `SIGSEGV` while finalising an
inherited SQLite connection, and it was fixed in the tests. `2ff9de9e` is the same mechanism in the
product ten weeks later: a forked child of a process that had configured `SQLiteSink` and logged
died of `SIGSEGV`, and the sink now refuses in a child. The audit's verification frame calls both
macOS-only, and records that the developer's machine is macOS, so the pre-push gates, which run
there, are where such a defect shows.

**What the decision accepts.** A defect that needs macOS to appear is found by the local gates or
by a user, never by CI. The open item that carried this recommendation is finding 23 of
`docs/audits/2026-11-14-post-1.2.0-audit.md`, filed in `docs/architecture.md` §12 as *CI cannot
see a hang, a platform difference or an untested line*; the rest of that item stays open, and this
part of it does not.

**Do NOT build:** a macOS leg in the test job, which is what was offered and declined. Reopening it
is Andrew's call.

## History

### 2026-11-14

- **Settled** by Andrew, in chat, after the audit's findings were reported.
