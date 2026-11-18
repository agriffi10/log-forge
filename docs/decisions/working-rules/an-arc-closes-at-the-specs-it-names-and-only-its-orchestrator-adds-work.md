---
type: Decision
title: "An arc closes at the specs it names, and only its orchestrator adds work"
description: "a build or fix session starts no other session, writes or revises no spec but its own and opens no PR but its own; a finding outside its criteria goes into a §12 entry or its report and stops there, and only the orchestrator, with Andrew's yes, turns one into a spec or a session. Specs are drafted one at a time, only once their dependencies have landed. Do not let a session's finding become new scope on its own. (Andrew, 2026-11-18)"
---

The 2026-11-14 audit arc was planned as SPEC-085 to SPEC-091 and grew to SPEC-085 to SPEC-105 in
four days. Over the same period, the open items in `docs/architecture.md` §12 rose from 47 to 55.
Both counts were read with `git show <ref>:docs/architecture.md`, at the audit's merge `5cb0f449`
and at `9ee07a88`. Most of the growth came from sessions that started other sessions:

- SPEC-090's build session started the sessions that wrote SPEC-102 and SPEC-103, and a test-fix
  session. Those sessions in turn started the one that wrote SPEC-104 and SPEC-105, and a second
  test fix.
- The spec-writer session started trace and fix sessions.

Andrew approved none of it as scope, and the orchestrator learned of each one after the fact.
Several of those specs were drafted in parallel, against code that then moved:

- SPEC-105 was written on a design of SPEC-103 that SPEC-103's rewrite dropped, and Andrew held it.
- Most of the Draft specs carry `file:line` premises or prototypes pinned to commits that
  later merges left behind. SPEC-097's prototype applies to `ead40798`, and SPEC-100's to
  `03dbf502`.

Asked on 2026-11-18 whether to adopt a scope freeze with the remaining Draft specs built in lanes,
Andrew answered "Adopt it".

**The audit arc's scope is now fixed** at the eleven specs that were Draft on that day:

- **The lifecycle lane**, in order: SPEC-097, SPEC-098, SPEC-101 and SPEC-099. SPEC-105 stays held
  until SPEC-097 lands.
- **The sinks lane**, in order: SPEC-092, SPEC-093, then SPEC-094 and SPEC-095, then SPEC-100.
- **Standing alone:** SPEC-104.

Every other §12 item waits for work Andrew chooses later.

**What the decision accepts.** A known defect found mid-build waits in §12 instead of being fixed
in the session that found it, even when that session has the context. The cost is re-reading that
context later. The return is a scope a person has agreed to, and specs written against the `main`
they will build on.

**How it is held.** By instruction only: `docs/process/several-agents.md` states the rule, and every
build brief repeats it. No gate refuses a session that starts another.

**Do NOT build:** a build or fix session that starts another session, drafts a spec other than its
own, or opens a second PR. Reopening this is Andrew's call.

## History

### 2026-11-18

- **Settled** by Andrew, in chat, after the arc's growth was measured and put to him.
