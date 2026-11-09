# Several agents on one repo

The default is one spec in flight (`spec-lifecycle.md`). Sessions here routinely run several at once
against this repo, each in its own worktree, so their *trees* never collide but the **remote still
does**: two PRs open at once means each is tested against a `main` the other is about to move, and a
merge race turns a green PR red for a reason neither agent caused. Serialising the remote while
parallelising the work is the whole idea, and the invariant is **one PR open at a time, taken in the
order agents asked, with `main` settled and green before the next**.

- **The queue lives outside every worktree and outside the repo.** A lock inside a worktree is
  invisible to peers; a lock inside the repo is a file that itself conflicts. `scripts/pr-queue/`
  holds the implementation; `install.sh` copies it to a shared directory under `$HOME` and points a
  `pre-push` wrapper at it. **Do not hand-roll the shell** — these checks are easy to get subtly
  wrong in a way that fails *open*, which is the one failure mode that matters. Until `install.sh`
  has been run the queue enforces nothing, silently.
- **Four commands, and the polling one is `turn`:** `ticket` gets in line, `turn` exits 0 when it is
  your turn *and* the remote is clear, `acquire` takes the lock, `release` drops it — on **every**
  exit path, including failure. Poll `turn`, never `acquire`; each `turn` call is also the heartbeat
  that keeps your place. A holder that stops is this design's one real failure.
- **The lock covers the whole PR lifecycle** — fetch, rebase, re-run your gates, push, open, watch
  to green, merge, confirm `main` — not just the push. The fetch is **inside** the lock because the
  wait is exactly when peers merge: rebase before you get in line and you push a base the remote has
  already moved past. One ticket per PR, released between them, so a multi-PR spec does not
  hold the line for its whole duration.
- **The hook checks your base, not just your lock.** `pre-push` carries a second refusal beside the
  lock's: a push whose commits do not contain the remote's current `main`. Holding the lock does not
  exempt you from it, which is what makes the fetch above part of the lifecycle rather than advice.
- **Release only when you are stopping, and know it is one-way.** `turn` and `acquire` refuse while
  a non-draft PR is open and do not exempt your own, and `release` drops your ticket with the lock,
  so a session that releases after opening its PR does not get the lock back on demand: it re-enters
  the queue behind anyone who arrived while it held it. Releasing when you stop is still right — an
  abandoned lock outlives your session and clears only on the stale-lock timer — but it is not a
  pause button.
- **Merging needs no lock; pushing does.** A session whose CI is green can watch, merge and drop the
  ticket without the lock. Any push of an enforced branch is refused while you do not hold it, which
  is the case a red CI puts you in, and `PR_QUEUE_BYPASS=1` is the documented way through. Which
  branches are enforced is a per-install setting: **read `enforce-branches` in the queue directory**
  rather than assuming, and rather than re-running `install.sh` to find out, which rewrites this
  checkout's hook. A fresh install writes `.` — every branch — but an
  existing setting is KEPT — so a queue installed before that default changed still enforces
  whatever it was given, and only an explicit `install.sh '<ere>'` re-points it.
- **A DRAFT PR breaks that reasoning, in both directions.** Drafts are invisible to the open-PR check
  `turn` and `acquire` use, so releasing with one open lets a peer take the lock and open a second PR;
  un-drafting and merging then moves `main` under their green branch — the race the queue exists to
  prevent. They are *not* invisible to the stale-lock reaper, which counts them on purpose as evidence
  the holder is still alive, so a lock abandoned behind a draft is never reaped and a peer waiting on
  the timer waits forever — escalate instead of polling. Keep the lock, or close the draft; and if you
  have already released with only a draft open, re-ticket and acquire immediately, because until a
  peer beats you to it the queue will hand the lock straight back.
- **The behaviour above is defined by `scripts/pr-queue/queue.sh` and `pre-push`, not by this page.**
  The exact refusals, exit codes and which check answers first are theirs, and a copy here is a copy
  that goes stale; `scripts/pr-queue/PROTOCOL.md` carries the rest.
- **The queue is not a review.** It is the last thing between an already-reviewed branch and the
  remote. This repo's gates and both diff reviews still come first, in that order.
- **Every remote check fails closed; enforcement fails open on CONSENT and closed on EVIDENCE.** The lock only orders the agents that
  take it, so `turn` asks the remote directly too — and a `gh` that *errors* returns empty output,
  which reads as "no PRs open" unless you check the exit status. Enforcement is the opposite case:
  linked worktrees share `.git/hooks` through the common git dir, so the hook fires for sessions that
  never agreed to the queue, and blocking those would be worse than the problem it solves. That is
  the consent half. On evidence the hook is the opposite: an unreadable remote, a missing trunk or an
  `enforce-branches` pattern it cannot compile each **refuse**, because a check that cannot run must
  never read as a check that found nothing.
- **Never move the shared checkout out from under a peer.** Build in your own worktree off fresh
  `origin/main` — not the shared tree, not a peer's branch — and leave the tree on the branch you
  found it on. Uncommitted changes in a shared tree are not yours to commit, stash or revert.
- **Brief each session explicitly** from `docs/templates/multi-agent-briefing.md`: its own worktree
  off fresh `origin/main`, who else is running and on what, the files two agents will both edit, and
  that a rebase conflict is never resolved by discarding a peer's work. An unbriefed session does the
  reasonable thing for a session working alone, which is exactly what breaks a parallel run.

**A supervisor session** watches the queue, main and where sessions' work meets, relays what git
will not catch, and archives finished sessions — `/supervise` (`.claude/skills/supervise/SKILL.md`),
driven by `scripts/supervisor/`. It never pushes, merges or takes a lock for another session; when
one is running, it is the escalation point a briefing names, and it puts to Andrew what only he can
decide.

Full protocol, configuration and the stale-entry rules: `scripts/pr-queue/PROTOCOL.md`.

**Running an arc through — one build session per spec**

A session told to build an arc **orchestrates** it and builds none of the specs itself: it starts a
new build session per spec, and the instruction to build the arc is the instruction to build each of
its specs (`spec-lifecycle.md`), so it does not ask again between them. An arc run is a multi-agent
run from its first session. Confirm the queue is installed (the `Q=` line of `.git/hooks/pre-push`
in the main checkout) and run `install.sh` only if it is not, never just to find out (above); then
brief every session from `docs/templates/multi-agent-briefing.md`, queue section included.

- **Start every ready spec, without asking — at the outset and again each time a spec lands.** A spec
  is ready when it is `Draft` on `origin/main` (the status a spec takes once reviewed, `spec-lifecycle.md`),
  `spec-lint` reports no failure for it there, and every earlier spec in `arc_order` that it is not
  independent of has landed. With no independent specs this is one session at a time, in
  `arc_order`. Start every ready spec before stopping to ask about one that is not.
- **Two specs are independent** when neither needs the other's result, whether through `depends_on`
  or through something the earlier one builds, and they touch different source files. The files every
  completion ritual edits (the generated indexes, `CLAUDE.md`'s `## Specs`, the component inventory)
  do not count; name them in each briefing's *Files you and a peer will both touch*. Arcs can carry
  dependencies the frontmatter does not name, so when independence is not clear, chain them.
- **Landed is checked, never taken from a report.** After `git fetch origin`: the spec reads
  `Completed` on `origin/main` (`git show origin/main:<spec path>`), every PR its session named is
  merged, `main` is green at a head that contains them, and the spec holds neither the queue lock nor
  a ticket. A spec already `Completed` when the orchestrator starts counts as landed. A turn-end note
  that reports progress (a plan drafted, a review running) is not a finish, so nothing starts on one.
- **Check for a landing whenever a session it started ends a turn.** Where the harness does not
  report that, poll `origin/main` for the spec's status rather than end the orchestrator's own turn:
  an orchestrator that goes quiet with sessions running is the stall this rule exists to prevent.
- **Archive each finished session in the step that verifies its landing,** wherever the harness can
  archive sessions. That covers every session the orchestrator started, a fix or a review session as
  much as a build. This is the orchestrator's own authority, from Andrew asking for the arc, not the
  supervisor's; it borrows only the supervise skill's *Archiving* checklist to tell finished
  (`.claude/skills/supervise/SKILL.md`): `done_check.py` prints `ARCHIVABLE`, or for a session with
  no PR its tail shows its conclusion delivered and its chips resolved; the session is not running;
  and its transcript tail is neither a question waiting on Andrew nor a background wait. A session the
  checklist says to keep after its spec has landed (a pending chip, a dirty worktree) is left
  unarchived and named in the arc report; it does not hold up the next spec.
- **Stop the chain and ask Andrew** when:
  - a session ends blocked, or ends with its spec not landed by the check above;
  - a session is waiting on Andrew's answer: relay its question verbatim;
  - `main` is red: start nothing until it is green again, and ask before the chain resumes. The
    session whose merge turned it red fixes it (`CLAUDE.md`); start a fix session only if that one
    has ended;
  - the next spec is `In Progress` from a session this orchestrator did not start (name its branch,
    its PR and any queue lock it holds), is neither `Draft` nor `Completed`, fails `spec-lint`, or
    depends on a spec outside the arc that is not `Completed`.
- **When the arc's last spec lands,** archive its session and report the arc done to Andrew,
  with the lessons its sessions reported.
