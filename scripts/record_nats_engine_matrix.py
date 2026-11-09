"""Records SPEC-074's NATS differential matrix against 1.x at ``93113e1b`` (FR-004).

Usage::

    poetry run python scripts/record_nats_engine_matrix.py --worktree <scratch>/wt-93113e1b

It creates the worktree when it is missing (``git worktree add --detach <path> 93113e1b``) and
never runs inside the repository's own checkout. The harness, ``tests/nats_engine_matrix_harness.py``,
runs in a child process with ``<worktree>/src`` first on ``PYTHONPATH``, ahead of the editable
install that would otherwise resolve ``log_foundry`` back to this tree; the child checks it is
running 1.x before writing ``tests/data/nats_engine_matrix_93113e1b.json``.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
import sys

COMMIT = "93113e1b"
ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "tests" / "data" / f"nats_engine_matrix_{COMMIT}.json"
COMMAND = "poetry run python scripts/record_nats_engine_matrix.py --worktree <scratch>/wt-93113e1b"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--worktree", required=True, type=pathlib.Path)
    args = parser.parse_args()
    tree = args.worktree.resolve()
    if tree == ROOT or ROOT in tree.parents:
        print("refusing: the 1.x worktree must lie outside this checkout", file=sys.stderr)
        return 2
    if not tree.exists():
        subprocess.run(  # noqa: S603
            ["git", "-C", str(ROOT), "worktree", "add", "--detach", str(tree), COMMIT],  # noqa: S607
            check=True,
        )
    head = subprocess.run(  # noqa: S603
        ["git", "-C", str(tree), "rev-parse", "HEAD"],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not head.startswith(COMMIT):
        print(f"refusing: {tree} is at {head}, not {COMMIT}", file=sys.stderr)
        return 2
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(tree / "src"), str(ROOT / "tests")])}
    check = (
        "import log_foundry, pathlib, sys; "
        f"sys.exit(0 if pathlib.Path(log_foundry.__file__).is_relative_to({str(tree)!r}) else 3)"
    )
    if subprocess.run([sys.executable, "-c", check], env=env, check=False).returncode:  # noqa: S603
        print("refusing: log_foundry did not resolve to the 1.x worktree", file=sys.stderr)
        return 3
    harness = ROOT / "tests" / "nats_engine_matrix_harness.py"
    subprocess.run(  # noqa: S603
        [sys.executable, str(harness), str(OUT), head, COMMAND], env=env, check=True
    )
    print(f"wrote {OUT.relative_to(ROOT)} from {head}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
