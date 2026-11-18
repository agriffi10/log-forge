---
type: Decision
title: "An arc closes at the specs it names, and only its orchestrator adds work"
description: "a build or fix session, in an arc or not, starts no other session, writes or revises no spec but its own and opens no PR for work outside its own spec or fix; a finding outside its criteria goes into a §12 entry in its PR and stops there, unfixed. Only the orchestrator, with Andrew's yes, turns one into a spec or a session; specs are drafted one at a time, once their dependencies have landed; this overrides any standing instruction to hand new work to a new session. The 2026-11-18 freeze fixes the audit arc's remaining specs in strict lanes. (Andrew, 2026-11-18)"
---

**What happened.** The 2026-11-14 audit planned its work by track
(`docs/audits/2026-11-14-post-1.2.0-audit.md` §7). The plan gave no spec count: each spec took its
number when it was authored. By 2026-11-18 the numbers SPEC-085 to SPEC-105 were allocated.

- **SPEC-085 to SPEC-099 build what that plan and Andrew's 2026-11-14 decisions named.** Those
  decisions are listed in `docs/specs/ARCS.md`.
- **SPEC-100 and SPEC-101 he asked for.** The 2026-11-17 entries in their revision histories record
  this.
- **SPEC-102 to SPEC-105 trace back to SPEC-090's build** rather than to the plan:
  - SPEC-103 calls its subject "the second residual SPEC-090 filed".
  - SPEC-103 also names another session's docs change as the source of the item SPEC-105 closes.
  - SPEC-104 and SPEC-105 carry no `arc:`.

By the orchestrator's account, SPEC-090's build session started the sessions that drafted those
four specs and two test fixes, and the orchestrator learned of each one only after it had started.
The repo cannot show that part, since sessions leave no record in it.

Over the same four days, the open items in `docs/architecture.md` §12 rose from 47 to 55. The
instrument: `git show <ref>:docs/architecture.md`, counting the top-level `- **` bullets between
`### Open` and `### Resolved` at the audit's merge `5cb0f449` and at `9ee07a88`.

Several Draft specs were written ahead of their builds, against code that then moved:
- SPEC-105 was written on a design of SPEC-103 that SPEC-103's rewrite dropped, and Andrew held it.
- SPEC-097's prototype applies to `ead40798`, and SPEC-100's to `03dbf502`.

Asked on 2026-11-18 whether to adopt a scope freeze with the remaining Draft specs built in lanes,
Andrew answered "Adopt it".

**The rule.**
- **Who it binds.** It binds every build or fix session, in an arc or not. The orchestrator is the
  session Andrew told to run the arc, or Andrew himself.
- **Where findings go.** A finding outside a session's criteria goes into a §12 entry in its own PR.
  A line in the session's report is only for what the orchestrator must act on now.
- **When specs are written.** A spec is drafted or revised only once everything it depends on has
  landed, and one at a time.
- **Which instruction wins.** In this repo the rule overrides any standing instruction to hand new
  work to a new session.

**The scope now fixed.** It covers the nine specs of the arc still Draft that day, plus SPEC-104
and SPEC-105, which are built alongside the arc and not in it. While the freeze holds, the lanes
below replace the `arc_order` condition of the readiness rule in
`docs/process/several-agents.md`, strictly: a spec starts only once every earlier spec in its lane
has landed, independent or not. That rule's other conditions still hold. The two lanes and
SPEC-104 run at once, since their specs name disjoint source files:

- **The lifecycle lane**, in order: SPEC-097, SPEC-098, SPEC-101 and SPEC-099.
  - SPEC-099 comes last although it depends only on Completed specs. SPEC-101's spec review found
    that SPEC-097's prototype makes two of the soak's own tests fail in a serial run. Once the soak
    is live, a red night blocks every merge.
  - SPEC-105 stays held until SPEC-097 lands, because both change the shutdown path in `worker.py`.
    It then joins this lane after SPEC-097, but it resumes only on Andrew's yes, since he held it.
- **The sinks lane**, in order: SPEC-092; then SPEC-093 and SPEC-094 as one build and one PR, as
  SPEC-094 states, in parallel with SPEC-095; then SPEC-100.
- **Standing alone:** SPEC-104.

The freeze ends when these eleven specs land or Andrew lifts it. Every other §12 item waits for
work Andrew chooses later.

**What the decision accepts.** A known defect found mid-build waits in §12 instead of being fixed in
the session that found it, even when that session has the context. The cost is re-reading that
context later. The return is a scope a person has agreed to, and specs written against the `main`
they will build on.

**How it is held.** By instruction only:
- `docs/process/several-agents.md` states the rule.
- `docs/templates/multi-agent-briefing.md` carries it into every brief.
- `CLAUDE.md`'s *Running an arc* states the override, in the tier every session loads.

No gate refuses a session that starts another.

**Do NOT build:** a build or fix session that starts another session, drafts a spec other than its
own, or opens a PR for work outside its own spec or fix. Reopening this is Andrew's call.

## History

### 2026-11-18

- **Settled** by Andrew, in chat. The figure put to him was that the arc grew "from 7 specs to 21".
  That overstated it: SPEC-092 to SPEC-099 were in the 2026-11-14 plan, and he asked for SPEC-100
  and SPEC-101 himself. The additions no plan named are SPEC-102 to SPEC-105. The §12 count of 47
  rising to 55 stands.
