# Agent roles — which agent does which job

The main session is an **orchestrator**. When it is given a task it delegates each artifact — the
spec, the plan, the code, every review, every sweep — to a subagent in the role the **job** calls
for, and keeps for itself only what the process assigns to the orchestrator: triage of emergent
issues, escalation to the human, the push, the watch and the merge.

**Every subagent runs on the model that started the task.** The roles live in `.claude/agents/` and
each declares `model: inherit`, which selects the main conversation's model. A role differs from
another by its brief, its tools and its isolation, never by its model.

## The table

| Job | Agent |
|---|---|
| Write or revise a spec | `spec-author` |
| Review a spec (its one gate) | `reviewer` |
| Write the implementation plan | `implementer` |
| Review the plan | `reviewer` |
| Implement, or rewrite under review pressure | `implementer` |
| Diff review, frame 1 — the change against the spec's criteria and `best-practices/python` | `reviewer` |
| Diff review, frame 2 — start from the system, cite `docs/invariants.md` by number, and on code build it and run the suites | `reviewer` |
| Completion ritual, delivery doc, any docs-only change | `spec-author` |
| Enumerate a population, measure, sweep, find every site of a rule | `scout` |

**Precedence.** Rows are read top-down against the artifact. A change whose diff contains code is
*implementation*, whatever documents it also touches; a change with no code in its diff is
*docs-only*. A review is always a `reviewer` row, whatever produced the artifact.

## The implementer's branch

The `implementer` and `reviewer` roles run in a worktree of their own, and that worktree is cut from
the shared checkout's HEAD (observed: `main`), not from the caller's branch. Git also refuses to
check out a branch another worktree holds. So the delegated implementer owns the branch the spec
is pushed from:

- The orchestrator commits the `In Progress` transition (`session-rhythm.md`, step 4) in its own
  worktree, then briefs the implementer with that commit's exact SHA and a branch name that is the
  implementer's own, never the orchestrator's session branch, which git would refuse. Committing
  before delegating is enough to make the SHA reachable, because every worktree shares the repo's
  object store.
- The implementer creates that branch from that SHA in its own worktree (`git switch -c <name>
  <sha>`), confirms it with `git log --oneline -1`, builds there, and names in its report the SHA
  it started from.
- The completion ritual (`completion-ritual.md`) is committed on the implementer's branch, in the
  implementer's worktree, before the diff reviews, because that is the branch that is pushed; the
  `spec-author` has no worktree of its own and would otherwise commit on the orchestrator's unpushed
  session branch. The orchestrator briefs the `spec-author` with that worktree's absolute path and
  the branch name, and the reviewers then review the head that includes the ritual.
- The orchestrator runs both diff reviews against that branch's head SHA. Each reviewer is briefed
  with the exact SHA, checks it out detached in its own worktree and confirms its tree with
  `git log --oneline -1` before reviewing.
- The orchestrator runs `acquire`, the push and the PR from the implementer's worktree path, because
  the queue lock records the branch it was acquired on (`scripts/pr-queue/PROTOCOL.md`).

## Fences

- **Write `model: inherit`, never omit it.** An omitted `model` falls back to
  `CLAUDE_CODE_SUBAGENT_MODEL` when that is set; `inherit` selects the main conversation's model
  ahead of it. That order holds from Claude Code v2.1.251; before it the variable came first and
  overrode `inherit` too.
- **Pass no `model` on an Agent tool call.** A per-invocation `model` overrides the role's
  `inherit`.
- **Never set `CLAUDE_CODE_SUBAGENT_MODEL`**, with or without `CLAUDE_CODE_SUBAGENT_MODEL_FORCE`,
  in settings or the environment. Forced, it makes every agent ignore its `model`, `inherit`
  included, and run on the model the variable names.
- **Delegate only to the roles above, never to a built-in agent.** The built-ins do not all run
  on the main model: Explore runs on Opus under a Fable session on some accounts, general-purpose
  follows `CLAUDE_CODE_SUBAGENT_MODEL`, and claude-code-guide runs on Haiku. Sweep with `scout`.
- **A reviewer started as a new session, not a subagent, is started on the model of the session that
  asked for it.** `inherit` reaches only subagents.
- `scripts/docs-lint.sh` holds the agent files to their shape: frontmatter at byte 0, `name` equal
  to the file stem, `model: inherit` and nothing else, and every agent this table names exists as a
  file and every file is named here. It also holds every row of this table to two cells under a
  `| Job | Agent |` header, fails a leftover `model-routing.md`, and fails a `.claude/settings.json`
  that sets `CLAUDE_CODE_SUBAGENT_MODEL` or its `_FORCE` twin as a key.
