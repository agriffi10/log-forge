"""The fixture corpus for ``done_check.py``'s chip scoping, the guard whose failure is silent.

If a session's pending chip goes uncounted, the supervisor archives a session the user still has a
decision waiting in, and nothing says so. The two ways that happens are both about WHICH transcripts
are read: a worktree folder holding an earlier, archived session's transcripts, and the main
checkout's folder holding every session run there. Each case below builds a fake transcripts root
and a session record and asserts the exact pending set, including a case that must be empty.

Usage: ``python3 scripts/supervisor/done_check_test.py`` — no arguments, standard library only,
non-zero exit if any case does not behave as declared. It needs no desktop app and no network.

Attributes:
  REPO: A fake main checkout path; nothing is read from it.
  CASES: Every case, run in order, each in its own fake transcripts root.
"""

import json
import os
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import done_check

REPO = "/fake/checkout"


def _transcript(root: str, cwd: str, name: str, stamp: str, chips: list[str]) -> None:
    """Writes a transcript that began at ``stamp`` and offered ``chips``.

    Args:
      root: The fake transcripts root.
      cwd: The working directory the transcript belongs to.
      name: The file's base name, without ``.jsonl``.
      stamp: The first line's timestamp, ``YYYY-MM-DDTHH:MM:SS``.
      chips: Chip ids the transcript's spawn_task results name.

    Returns:
      None.

    Raises:
      OSError: If the file cannot be written.
    """
    folder = os.path.join(root, done_check.slug(cwd))
    os.makedirs(folder, exist_ok=True)
    lines = [json.dumps({"type": "user", "timestamp": f"{stamp}.000Z"})]
    lines += [json.dumps({"type": "user", "content": f"Noted (position 1, task_id: {c})."}) for c in chips]
    with open(os.path.join(folder, f"{name}.jsonl"), "w") as fh:
        fh.write("\n".join(lines) + "\n")


def _ms(stamp: str) -> float:
    """Returns epoch milliseconds for a ``YYYY-MM-DDTHH:MM:SS`` UTC stamp.

    Args:
      stamp: The timestamp.

    Returns:
      Epoch milliseconds.

    Raises:
      ValueError: If the stamp is malformed.
    """
    return datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC).timestamp() * 1000


def case_worktree_ignores_an_earlier_sessions_chips(root: str) -> list[str]:
    """A reused worktree name: the earlier session's pending chip must not count, the current one's must.

    Args:
      root: A fresh fake transcripts root.

    Returns:
      Failure messages, empty when the case holds.

    Raises:
      None.
    """
    wt = f"{REPO}/.claude/worktrees/brave-franklin-12d67f"
    _transcript(root, wt, "earlier", "2026-09-15T13:00:00", ["task_aaaaaaaa"])
    _transcript(root, wt, "current", "2026-09-16T12:00:05", ["task_bbbbbbbb", "task_cccccccc"])
    session = {"worktreePath": wt, "createdAt": _ms("2026-09-16T12:00:00"),
               "resolvedBackgroundTaskSuggestions": {"task_bbbbbbbb": "started_notified"}}
    got = done_check.pending_chips(session, REPO, root)
    return [] if got == ["task_cccccccc"] else [f"reused worktree: expected ['task_cccccccc'], got {got}"]


def case_main_checkout_reads_only_its_own_transcript(root: str) -> list[str]:
    """A main-checkout session: another session's transcript in the same folder must not count.

    Args:
      root: A fresh fake transcripts root.

    Returns:
      Failure messages, empty when the case holds.

    Raises:
      None.
    """
    _transcript(root, REPO, "someone-else", "2026-09-16T14:00:00", ["task_dddddddd"])
    _transcript(root, REPO, "mine", "2026-09-16T12:00:05", ["task_eeeeeeee"])
    session = {"worktreePath": None, "cliSessionId": "mine", "createdAt": _ms("2026-09-16T12:00:00"),
               "resolvedBackgroundTaskSuggestions": None}
    got = done_check.pending_chips(session, REPO, root)
    return [] if got == ["task_eeeeeeee"] else [f"main checkout: expected ['task_eeeeeeee'], got {got}"]


def case_every_chip_resolved_is_silent(root: str) -> list[str]:
    """All offered chips started or dismissed: nothing is pending.

    Args:
      root: A fresh fake transcripts root.

    Returns:
      Failure messages, empty when the case holds.

    Raises:
      None.
    """
    wt = f"{REPO}/.claude/worktrees/nice-brattain-d5d424"
    _transcript(root, wt, "current", "2026-09-16T12:00:05", ["task_ffffffff", "task_00000000"])
    session = {"worktreePath": wt, "createdAt": _ms("2026-09-16T12:00:00"),
               "resolvedBackgroundTaskSuggestions": {"task_ffffffff": "dismissed", "task_00000000": "started_notified"}}
    got = done_check.pending_chips(session, REPO, root)
    return [] if got == [] else [f"all resolved: expected [], got {got}"]


def case_undatable_transcript_still_counts(root: str) -> list[str]:
    """A transcript with no readable timestamp is kept, so its chip is not hidden.

    Args:
      root: A fresh fake transcripts root.

    Returns:
      Failure messages, empty when the case holds.

    Raises:
      None.
    """
    wt = f"{REPO}/.claude/worktrees/zen-germain-ae21d7"
    folder = os.path.join(root, done_check.slug(wt))
    os.makedirs(folder)
    with open(os.path.join(folder, "undated.jsonl"), "w") as fh:
        fh.write(json.dumps({"type": "user", "content": "Noted (position 1, task_id: task_12345678)."}) + "\n")
    session = {"worktreePath": wt, "createdAt": _ms("2026-09-16T12:00:00"), "resolvedBackgroundTaskSuggestions": {}}
    got = done_check.pending_chips(session, REPO, root)
    return [] if got == ["task_12345678"] else [f"undatable transcript: expected ['task_12345678'], got {got}"]


def case_record_with_cwd_only_is_scoped_to_its_worktree(root: str) -> list[str]:
    """A record that names its worktree only in ``cwd`` still reads that worktree's transcripts.

    Args:
      root: A fresh fake transcripts root.

    Returns:
      Failure messages, empty when the case holds.

    Raises:
      None.
    """
    wt = f"{REPO}/.claude/worktrees/vivid-lamport-0a1b2c"
    _transcript(root, wt, "current", "2026-09-16T12:00:05", ["task_abcdef12"])
    session = {"cwd": wt, "createdAt": _ms("2026-09-16T12:00:00"), "resolvedBackgroundTaskSuggestions": {}}
    got = done_check.pending_chips(session, REPO, root)
    return [] if got == ["task_abcdef12"] else [f"cwd-only record: expected ['task_abcdef12'], got {got}"]


CASES: list[Callable[[str], list[str]]] = [
    case_worktree_ignores_an_earlier_sessions_chips,
    case_main_checkout_reads_only_its_own_transcript,
    case_every_chip_resolved_is_silent,
    case_undatable_transcript_still_counts,
    case_record_with_cwd_only_is_scoped_to_its_worktree,
]


def main() -> int:
    """Runs every case in its own fake transcripts root and reports each failure.

    Args:
      None.

    Returns:
      0 when every case holds, 1 otherwise.

    Raises:
      None.
    """
    failed = 0
    for case in CASES:
        with tempfile.TemporaryDirectory() as root:
            failures = case(root)
        failed += bool(failures)
        for failure in failures:
            print(f"FAIL {case.__name__}: {failure}")
    print(f"{len(CASES) - failed}/{len(CASES)} cases passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
