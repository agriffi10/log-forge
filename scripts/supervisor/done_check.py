"""Which of this checkout's desktop sessions look finished enough to archive, and why the rest are not.

A session is ARCHIVABLE only when every one of these holds:
- every PR it opened has merged, and main's push runs on the newest head are green;
- it does not hold the PR-queue lock;
- no task chip it offered is still pending;
- each of its worktrees is clean, with no commits beyond a merged PR's head.

Whether the session is idle, or waiting on a question to the user, is not visible from here. The
supervisor checks ``isRunning`` and the transcript tail before it archives anything.

**This reads the Claude desktop app's own session store, which is not a public interface.** The
record's ``resolvedBackgroundTaskSuggestions`` says which chips were started or dismissed, and the
transcripts under ``~/.claude/projects`` say which chips were offered. If the app changes either
shape, this stops finding chips and would call a session with a pending chip archivable — so the
chip scoping is the part ``done_check_test.py`` pins.

**A session's transcripts are chosen by when they began, not by folder alone.** Worktree names are
reused once a session is archived, so a worktree's project folder holds earlier sessions' transcripts
and their chips, and the main checkout's folder holds every session ever run there. Counting those
reported chips the current session never offered. A worktree session takes the transcripts that
began after it was created; a main-checkout session takes only its own ``cliSessionId`` file.

Usage::

    python3 scripts/supervisor/done_check.py --self <this session's id> [session id ...]

Prints one tab-separated line per session: id, worktree name, title, and ``ARCHIVABLE`` or
``keep: <reasons>``.

Attributes:
  STORE: The desktop app's session store.
  PROJECTS: Where Claude Code writes session transcripts, one folder per working directory.
  CREATED_SLACK_MS: How far before a session's ``createdAt`` its first transcript may begin; the
    transcript's first line is written a moment after the record.
  CHIP_ID: The chip id in the sentence a ``spawn_task`` result opens with. Text that quotes such a
    result (a grep of a transcript, say) still matches, which only over-counts: a session kept
    open by a chip it never offered is the safe mistake.
  TIMESTAMP: A transcript line's timestamp, with or without a space after the colon.
  UNKNOWN_LOCK: What ``lock_branch`` returns when the queue itself cannot be read.
  GIT: The resolved ``git`` executable.
  GH: The resolved ``gh`` executable.
  _ENV: The environment every subprocess gets, with ``GIT_OPTIONAL_LOCKS=0``.
"""

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

STORE = os.path.expanduser("~/Library/Application Support/Claude/claude-code-sessions")
PROJECTS = os.path.expanduser("~/.claude/projects")
CREATED_SLACK_MS = 60_000
CHIP_ID = re.compile(r"Noted \(position \d+, task_id: (task_[0-9a-f]{8})\)")
TIMESTAMP = re.compile(r'"timestamp":\s*"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)')
UNKNOWN_LOCK = "<queue unreadable>"
GIT = shutil.which("git") or "git"
GH = shutil.which("gh") or "gh"
_ENV = dict(os.environ, GIT_OPTIONAL_LOCKS="0")


def _run(argv: list[str], cwd: str | None = None) -> subprocess.CompletedProcess[str]:
    """Runs a fixed tool invocation read-only, without taking optional git locks in a peer's tree.

    Args:
      argv: The executable and its arguments; every executable here is ``GIT`` or ``GH``.
      cwd: The directory to run in.

    Returns:
      The completed process, never raising on a non-zero exit.

    Raises:
      None.
    """
    return subprocess.run(argv, cwd=cwd, capture_output=True, text=True, check=False, env=_ENV)  # noqa: S603


def belongs(cwd: str, repo: str) -> bool:
    """Whether a session working in ``cwd`` belongs to the checkout at ``repo``.

    It does when ``cwd`` is the checkout or a path under it. A bare string prefix would also claim
    a sibling folder whose name merely starts with this one's.

    Args:
      cwd: The session's working directory.
      repo: The main checkout.

    Returns:
      True for the checkout itself or any path inside it.

    Raises:
      None.
    """
    return cwd == repo or cwd.startswith(repo + "/")


def repo_root() -> str:
    """Returns the main checkout this script belongs to, whichever worktree it runs from.

    Args:
      None.

    Returns:
      The absolute path of the main checkout.

    Raises:
      RuntimeError: If the script is not inside a git checkout.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    r = _run([GIT, "-C", here, "rev-parse", "--path-format=absolute", "--git-common-dir"])
    if r.returncode != 0:
        raise RuntimeError(f"not inside a git checkout: {here}")
    return os.path.dirname(r.stdout.strip())


def git(repo: str, *args: str, cwd: str | None = None) -> tuple[int, str]:
    """Runs a read-only git command without taking optional locks in a peer's tree.

    Args:
      repo: The main checkout, used when ``cwd`` is not given.
      *args: The git arguments.
      cwd: The worktree to run in.

    Returns:
      The exit status and stripped stdout.

    Raises:
      None.
    """
    r = _run([GIT, *args], cwd=cwd or repo)
    return r.returncode, r.stdout.strip()


def slug(path: str) -> str:
    """Returns the project-folder name Claude Code derives from a working directory.

    Args:
      path: An absolute working directory.

    Returns:
      The path with every non-alphanumeric character replaced by a hyphen.

    Raises:
      None.
    """
    return re.sub(r"[^A-Za-z0-9]", "-", path)


def first_timestamp_ms(path: str) -> float | None:
    """Returns when a transcript began, from the first timestamp among its opening lines.

    Args:
      path: A transcript ``.jsonl`` file.

    Returns:
      Epoch milliseconds, or None when no timestamp is found.

    Raises:
      OSError: If the file cannot be read.
    """
    with open(path, errors="replace") as fh:
        for _ in range(50):
            line = fh.readline()
            if not line:
                break
            m = TIMESTAMP.search(line)
            if m:
                return datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC).timestamp() * 1000
    return None


def transcripts(session: Mapping[str, Any], repo: str, projects: str = PROJECTS) -> list[str]:
    """Returns the transcripts that belong to this session and to no earlier one.

    A worktree transcript that cannot be dated is kept rather than dropped: dropping it would hide
    any chip it offered, and a hidden chip reads as nothing pending.

    Args:
      session: The session record from the desktop store.
      repo: The main checkout.
      projects: The transcripts root.

    Returns:
      Paths of the session's own transcripts.

    Raises:
      None.
    """
    wt = session.get("worktreePath") or session.get("cwd")
    if not wt or wt == repo:
        own = os.path.join(projects, slug(repo), f"{session.get('cliSessionId')}.jsonl")
        return [own] if os.path.exists(own) else []
    created = float(session.get("createdAt") or 0) - CREATED_SLACK_MS
    kept = []
    for path in glob.glob(os.path.join(projects, slug(wt), "*.jsonl")):
        began = first_timestamp_ms(path)
        if began is None or began >= created:
            kept.append(path)
    return kept


def pending_chips(session: Mapping[str, Any], repo: str, projects: str = PROJECTS) -> list[str]:
    """Returns the chips this session offered that the user has neither started nor dismissed.

    Args:
      session: The session record from the desktop store.
      repo: The main checkout.
      projects: The transcripts root.

    Returns:
      Sorted chip ids still pending.

    Raises:
      None.
    """
    offered: set[str] = set()
    for path in transcripts(session, repo, projects):
        with open(path, errors="replace") as fh:
            for line in fh:
                if "task_id: task_" in line:
                    offered.update(CHIP_ID.findall(line))
    resolved = set((session.get("resolvedBackgroundTaskSuggestions") or {}).keys())
    return sorted(offered - resolved)


def pr_state(repo: str, number: int) -> tuple[str, str | None]:
    """Returns a PR's state and head sha, as GitHub has them now.

    The session record's own PR state goes stale; GitHub is asked instead.

    Args:
      repo: The main checkout.
      number: The PR number.

    Returns:
      The state (``UNKNOWN`` when the read failed) and the head sha.

    Raises:
      None.
    """
    r = _run([GH, "pr", "view", str(number), "--json", "state,headRefOid"], cwd=repo)
    if r.returncode != 0:
        return "UNKNOWN", None
    d = json.loads(r.stdout)
    return d["state"], d["headRefOid"]


def merged_branch_head(repo: str, branch: str, head: str) -> bool:
    """Whether a merged PR's head is exactly this commit, so a squash merge already carries it.

    Args:
      repo: The main checkout.
      branch: The worktree's branch.
      head: The worktree's HEAD sha.

    Returns:
      True if a merged PR from ``branch`` had ``head`` as its head.

    Raises:
      None.
    """
    r = _run([GH, "pr", "list", "--state", "merged", "--head", branch, "--json", "headRefOid",
              "--jq", ".[].headRefOid"], cwd=repo)
    return r.returncode == 0 and head in r.stdout.split()


def main_is_green(repo: str) -> bool:
    """Whether every push run on the remote main's head concluded cleanly.

    The head is read from the remote rather than taken from the newest run: a push event GitHub
    dropped leaves the newest run on the previous commit, green, while the merge that is main's
    head was never deployed. A head with no push run, or any failed read, is not green.

    Args:
      repo: The main checkout.

    Returns:
      True only for a settled, clean head.

    Raises:
      None.
    """
    head = _run([GIT, "ls-remote", "origin", "refs/heads/main"], cwd=repo)
    sha = head.stdout.split()[0] if head.returncode == 0 and head.stdout.strip() else None
    r = _run([GH, "run", "list", "--branch", "main", "--event", "push", "--limit", "20",
              "--json", "headSha,status,conclusion"], cwd=repo)
    if sha is None or r.returncode != 0:
        return False
    mine = [x for x in json.loads(r.stdout) if x["headSha"] == sha]
    return bool(mine) and all(
        x["status"] == "completed" and x["conclusion"] in ("success", "skipped", "neutral") for x in mine
    )


def lock_branch(repo: str) -> str | None:
    """Returns the branch holding the PR-queue lock, reading the queue path from the pre-push hook.

    A queue that cannot be found is not a free lock: a lost ``Q=`` line would otherwise wave through
    a session that still holds it, so that case is ``UNKNOWN_LOCK`` and keeps every session with a PR.

    Args:
      repo: The main checkout.

    Returns:
      The branch name; None when the lock is free; ``UNKNOWN_LOCK`` when the queue cannot be read.

    Raises:
      None.
    """
    try:
        with open(os.path.join(repo, ".git", "hooks", "pre-push")) as fh:
            m = re.search(r"^Q=['\"]?([^'\"\n]+)['\"]?$", fh.read(), re.MULTILINE)
    except OSError:
        return UNKNOWN_LOCK
    if not m or not os.path.isdir(m.group(1)):
        return UNKNOWN_LOCK
    try:
        with open(os.path.join(m.group(1), "lock", "holder")) as fh:
            for line in fh:
                if line.startswith("branch "):
                    return line.split(" ", 1)[1].strip()
    except FileNotFoundError:
        return None
    except OSError:
        return UNKNOWN_LOCK
    return UNKNOWN_LOCK


def scratch_worktrees(repo: str, name: str, cli_session_id: str | None) -> list[str]:
    """Returns the scratchpad worktrees a session created.

    A worktree session's scratchpad sits under a folder named for its worktree; a main-checkout
    session's sits under the main checkout's folder, told apart only by the session's own id. Both
    are matched, and a match on a reused worktree name only keeps a session open.

    Args:
      repo: The main checkout.
      name: The session's worktree name, or the checkout's name for a main-checkout session.
      cli_session_id: The session's transcript id.

    Returns:
      Absolute paths of those worktrees.

    Raises:
      None.
    """
    _, out = git(repo, "worktree", "list", "--porcelain")
    paths = [line[9:] for line in out.splitlines() if line.startswith("worktree ")]
    marks = [f"claude-worktrees-{name}/"] + ([f"/{cli_session_id}/scratchpad/"] if cli_session_id else [])
    return [p for p in paths if "/scratchpad/" in p and any(m in p for m in marks)]


def problems_for(session: Mapping[str, Any], repo: str, green: bool, held: str | None) -> list[str]:
    """Returns every reason this session is not yet archivable.

    Args:
      session: The session record from the desktop store.
      repo: The main checkout.
      green: Whether main is green.
      held: The branch holding the queue lock, if any.

    Returns:
      Reasons, empty when the session is archivable.

    Raises:
      None.
    """
    problems: list[str] = []
    prs = [p["prNumber"] for p in (session.get("prs") or [])]
    heads = {n: pr_state(repo, n) for n in prs}
    if not prs:
        problems.append("no PR")
    problems.extend(f"PR #{n} {state}" for n, (state, _) in heads.items() if state != "MERGED")
    pending = pending_chips(session, repo)
    if pending:
        problems.append("pending chips " + ",".join(pending))
    if prs and not green:
        problems.append("main not green")
    branches = {p.get("branch") for p in (session.get("prs") or [])} | set(session.get("writtenBranches") or [])
    if prs and held == UNKNOWN_LOCK:
        problems.append("queue unreadable, so the lock holder is unknown")
    elif held and held in branches:
        problems.append(f"holds queue lock on {held}")
    merged_heads = {h for state, h in heads.values() if state == "MERGED" and h}
    wt = session.get("worktreePath") or session.get("cwd") or repo
    for path in [wt, *scratch_worktrees(repo, os.path.basename(wt), session.get("cliSessionId"))]:
        if not os.path.isdir(path) or path == repo:
            continue
        readable, head = git(repo, "rev-parse", "HEAD", cwd=path)
        if readable != 0:
            problems.append(f"unreadable worktree {os.path.basename(path)}")
            continue
        _, dirty = git(repo, "status", "--porcelain", cwd=path)
        _, ahead = git(repo, "rev-list", "--count", "origin/main..HEAD", cwd=path)
        _, branch = git(repo, "branch", "--show-current", cwd=path)
        if dirty:
            problems.append(f"dirty {os.path.basename(path)}")
        if ahead != "0" and head not in merged_heads and not (branch and merged_branch_head(repo, branch, head)):
            problems.append(f"unmerged commits in {os.path.basename(path)}")
    return problems


def main() -> int:
    """Prints each non-archived session of this checkout with its verdict.

    Args:
      None.

    Returns:
      The process exit status.

    Raises:
      None.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--self", dest="self_id", required=True, help="this session's id, never offered")
    parser.add_argument("--no-fetch", action="store_true", help="skip git fetch; watch.sh has fetched")
    parser.add_argument("only", nargs="*", help="limit to these session ids")
    args = parser.parse_args()
    repo = repo_root()
    if not args.no_fetch:
        git(repo, "fetch", "-q", "origin")
    green, held = main_is_green(repo), lock_branch(repo)
    for record in sorted(glob.glob(os.path.join(STORE, "*", "*", "local_*.json"))):
        with open(record) as fh:
            session = json.load(fh)
        sid = session["sessionId"]
        if session.get("isArchived") or sid == args.self_id or not belongs(str(session.get("cwd", "")), repo):
            continue
        if args.only and sid not in args.only:
            continue
        problems = problems_for(session, repo, green, held)
        name = os.path.basename(session.get("worktreePath") or session.get("cwd") or "")
        verdict = "ARCHIVABLE" if not problems else "keep: " + "; ".join(problems)
        print(f"{sid}\t{name}\t{session.get('title', '')[:50]}\t{verdict}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
