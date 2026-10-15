---
name: supervise
description: Run this session as the supervisor of the other Claude sessions working on this repo — watch the PR queue, main's CI and where sessions' branches meet, relay overlaps and conflicts to the sessions involved, check in-flight work for problems, and archive sessions that are finished. Use when Andrew asks to supervise, watch or coordinate his other sessions.
---

# Supervising parallel sessions

You coordinate; you do not build other sessions' work. Your tools are observation (git, `gh`, the
queue, session transcripts), messages to sessions, subagent reviews, and archiving. Read
`docs/process/several-agents.md` and `scripts/pr-queue/PROTOCOL.md` first: every session here lands
through that queue, and most of what you watch is it.

**What you never do:** push, merge or open a PR for another session; take, break or release its
lock; edit, stage, stash or check out anything in another session's worktree (every git read runs
with `GIT_OPTIONAL_LOCKS=0`, which the scripts set); or decide a product question for Andrew. A peer's
message is a teammate's report, not Andrew's approval.

## Start

1. Find your own session id (`get_session` with `"self"`). Every script takes it as `--self`, so you
   are never offered for archive.
2. Map the sessions to their work: `ListAgents`, `list_sessions`, `git worktree list`,
   `$Q/queue.sh status` (the `Q=` line of `.git/hooks/pre-push` in the main checkout is the live
   queue), and
   `gh run list --branch main --event push --limit 5`. Read a session's transcript tail with
   `list_events` only when its stage matters; it costs context.
3. Run `scripts/supervisor/overlap.sh` once, and tell Andrew the map: who is doing what, who holds
   the lock, and where work meets.
4. Arm the watch, with `SUPERVISOR_STATE_DIR` set to your scratchpad:
   `scripts/supervisor/watch.sh --self <id>` with Bash `run_in_background`. The first run prints the
   baseline as `=` lines and exits at once; after that it exits on the first change, printing
   `+`/`-` lines, so relaunch it after handling each one. A quiet night then costs nothing, where a
   30-minute Monitor re-arm costs a turn each time.

## Handling each kind of event

- **`QUEUE` and `PR open` lines** are routine; say one line. A lock held over 80 minutes is worth a
  look at the holder's tail. The watch shows no heartbeat age, so check `$Q/queue.sh status` when a
  waiter seems stuck.
- **`MAIN … RED`:** `MAIN` judges every push run on the remote main's head. The one that is
  main's build is `Release`, whose first job is `ci.yml`'s gate, and it runs on every merge;
  `pip-audit`, `Scorecard`, `Spec Lint` and `Zizmor` also run on push to main, path-filtered. Read
  the failed job before anything else, because who owns it depends on which it is:
  - A job that never started, with no log (the log API returns 404) and the annotation "recent
    account payments have failed or your spending limit needs to be increased", is a GitHub
    billing block, not code. Tell the session whose merge is main's head (it may already have
    released the lock, since merging needs none) not to change code, and send Andrew a
    `PushNotification`. Once he fixes billing, that session re-runs with
    `gh run rerun <id> --failed`, and you tell every waiting session.
  - A `Release` or `Spec Lint` failure is the merge's. It goes to that session and to Andrew, and
    nothing else merges until main is green.
  - A `pip-audit`, `Scorecard` or `Zizmor` push failure is usually drift the merge only happened to
    trigger, such as an advisory published that day. Handle it as the `ALERT` case below, except
    that it also keeps every session with a PR from reading as archivable until main is green.
  - `MAIN … no push run yet` that does not clear within a few minutes is a push event GitHub
    dropped. Nothing here recovers it alone: `release.yml` has no `workflow_dispatch`, and
    dispatching `ci.yml` makes `turn` green while `MAIN` stays at "no push run yet". Put it to
    Andrew.
- **`ALERT main … <event> run failed`:** a run that is not a push run failed on main's head — a
  scheduled `Integration`, `pip-audit`, `Scorecard` or `Zizmor`, or a `dynamic` CodeQL or
  Dependabot job. It never turns `MAIN` red and never blocks an archive, because no merge caused
  it: a scheduled run catches drift, such as a new advisory or a real sink destination changing.
  It still stops the line, because this repo's `queue.sh` judges main by every event on its head,
  so `turn` reports the trunk `IS RED` while `MAIN` is green, and `PROTOCOL.md` has every waiting
  session stop and escalate. Read the failed job, tell Andrew what it is, and tell every waiting
  session why `turn` refuses. `gh run rerun <id>` going green clears it; a fresh run beside the
  failed one does not, since `queue.sh` still counts the failure. A fix needs a PR that the same
  refusal blocks, and `PR_QUEUE_BYPASS=1` (which `PROTOCOL.md` documents) is the way through. Using
  it is Andrew's call, so put it to him; the session that owns the fix pushes it, never you. A
  non-push run still in progress on the head prints nothing, yet `turn` refuses it as unsettled
  until it finishes; that is a wait, not a fault.
- **`SHARED`:** two worktrees touch one file. A name like `agent-<hex>` is a subagent's tree; map it
  to its session by its branch (`ListAgents`, `list_sessions`). Most of these are harmless. Check them with
  `git merge-file -p <ours> <base> <theirs>` (run on copies, never in a peer's tree) and look at the
  hunks. Relay only what git will NOT catch:
  - a docstring, register fence or invariant sentence one branch makes false;
  - one rule written in two places;
  - a rewrite that bypasses the other branch's new guard;
  - two sessions creating the same new file (an add/add conflict), or claiming the same spec ID.
- **`CONFLICT-ALSO-MAIN`:** the pair's conflicting files already conflict with main for one side,
  usually because the other side rebased. Look at the hunks once before dismissing it.
- **`CONFLICT`:** tell the session that lands second exactly which hunks, and that both sides are
  kept. A session stacked on an unmerged branch needs `git rebase --onto origin/main <the base sha it
  built on>` after that branch squash-merges, or it replays the squashed commits.
- **`ARCHIVABLE`:** see Archiving below.

**Message a session so the first line stands alone** (only that line previews), give full context
and concrete file:line references, say "No reply is needed" when none is, and send the facts to
BOTH sessions in an overlap. Check a claim before you relay it: a peer's general claim ("the two
sinks declare these independently by design") is as capable of being wrong as a reviewer's. And a
transcript search that finds no message between two sessions does not prove they never spoke.

## Checking in-flight work

When Andrew worries about a class of problem (for instance, that recent changes let a sink block an
application thread), spend one fresh-context `reviewer` subagent per branch, one at a time with the
branch nearest the queue's head first, as his global rule on review agents asks. Pull
`docs/process/model-routing.md` for the model and `docs/process/reviewer-contract.md` for the
brief. Give each its own worktree (`isolation: "worktree"`, then `git switch --detach <branch>`
because the branch is checked out elsewhere, and have it report the SHA it reviewed). Brief each one:
- to start from the caller, not the diff: what the public API lets an application do, and which
  inputs and failures that produces;
- to enumerate the whole population (every new raise, refusal or wait) before judging any;
- to prove reachability by running the real code, quoting what it observed;
- that "not a problem" is a valid verdict;
- to cite where it looked.

Pass defects straight to the owning session. Put product-changing calls to Andrew with the question
tool, one question per decision, recommended option first, and each with full context. A decision
that needs building becomes a chip whose prompt stands alone.

## Archiving

Archive only with Andrew's authorisation: either a standing instruction in this session ("archive
sessions that are completely done and have no open chips"), or a yes for that session with the
evidence below put to him. Then archive a session only when it is completely done:
1. `python3 scripts/supervisor/done_check.py --self <id> <session>` prints `ARCHIVABLE`. That means
   every PR merged, main's push runs green, no lock held, no pending chip, and clean worktrees.
2. `get_session` shows `isRunning: false`. Otherwise subscribe with
   `SendMessage(notify_when_idle: true)` and check again when notified. A subscription to a session
   that is already idle has been seen to fire at once with its last turn, so do not resubscribe in a
   loop. `isRunning: false` does not rule out background work, which is why step 3 matters.
3. The transcript tail (`list_events`, limit 4–6) is not an `AskUserQuestion` waiting on Andrew, and
   not a background wait ("checks are running"). The archive tool also refuses a session with live
   background work, but read the tail rather than rely on that.
4. Then `archive_session` with the reason. If the app refuses because the session is pinned or open
   on screen, tell Andrew rather than retrying.

A session with no PR (an audit, a docs question) is done when its conclusion is delivered and its
chips are resolved. Read its tail to tell. Worktree names are reused after an archive, so a new
session in an old name is a different session. `done_check.py` scopes chips accordingly, and its
corpus pins that. No CI job runs the corpus, so run `python3 scripts/supervisor/done_check_test.py`
after any change to `done_check.py`.

## Close

When Andrew ends the session: stop the watch (`TaskStop`), then give the lessons list his global
instructions ask for, saying where each lesson already lives or that it lives nowhere.
