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
- **The lock covers the whole PR lifecycle** — rebase, push, open, watch to green, merge, confirm
  `main` — not just the push. One ticket per PR, released between them, so a multi-PR spec does not
  hold the line for its whole duration.
- **Releasing it early costs more than the lock.** `turn` and `acquire` refuse while any
  **non-draft** PR is open on the remote and do not exempt your own, so a session that releases
  after opening its PR cannot take the lock back until that PR lands. `release` drops your ticket
  too, so `turn` answers `NO_TICKET` until you `ticket` again — re-ticket first, and then it names
  your own PR back to you. Measured with PR #230 open, before it merged as `3a4d337`.
- **What you can still finish without the lock, and what you cannot.** Merging needs no lock, so a
  session whose CI is green can watch, merge and drop the ticket. **A push does need it**: the
  pre-push hook compares the pushed branch against the lock holder's, and with the lock free every
  `spec/` or `docs/` branch is refused — which is exactly the case a red CI puts you in. That push
  takes `PR_QUEUE_BYPASS=1`, and `scripts/pr-queue/PROTOCOL.md` asks you to say you used it. The
  safe order is therefore to release only when you are stopping, and to expect the bypass if you
  release and then have to fix something.
- **The recovery above does not hold for a DRAFT PR.** Drafts are invisible to `turn`, so releasing
  with a draft open lets a peer take the lock and open a second PR; when you then un-draft and
  merge, you move `main` under their green branch — the race the queue exists to prevent. With a
  draft open, keep the lock or close the draft.
- **The queue is not a review.** It is the last thing between an already-reviewed branch and the
  remote. This repo's gates and both diff reviews still come first, in that order.
- **Every remote check fails closed; enforcement fails open.** The lock only orders the agents that
  take it, so `turn` asks the remote directly too — and a `gh` that *errors* returns empty output,
  which reads as "no PRs open" unless you check the exit status. Enforcement is the opposite case:
  linked worktrees share `.git/hooks` through the common git dir, so the hook fires for sessions that
  never agreed to the queue, and blocking those would be worse than the problem it solves.
- **Never move the shared checkout out from under a peer.** Build in your own worktree off fresh
  `origin/main` — not the shared tree, not a peer's branch — and leave the tree on the branch you
  found it on. Uncommitted changes in a shared tree are not yours to commit, stash or revert.
- **Brief each session explicitly** from `docs/templates/multi-agent-briefing.md`: its own worktree
  off fresh `origin/main`, who else is running and on what, the files two agents will both edit, and
  that a rebase conflict is never resolved by discarding a peer's work. An unbriefed session does the
  reasonable thing for a session working alone, which is exactly what breaks a parallel run.

Full protocol, configuration and the stale-entry rules: `scripts/pr-queue/PROTOCOL.md`.
