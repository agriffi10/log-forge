"""SPEC-095 probe: runs SPEC-092's prototype corpus against two trees and prints the rows that move.

Written on 2026-11-16 while authoring SPEC-095, against 97a03f45; not one of the audit's probes.
Each tree is a directory holding `src/`, `tests/` and `docs/audits/2026-11-14-probes/sinks/`, as
`git archive <ref> src tests docs/audits/2026-11-14-probes` writes it. The corpus is run in a
fresh interpreter per tree with that tree's own `src`, `tests` and corpus files first on the
path, so a tree carrying SPEC-095's corpus changes is judged by them. Run from anywhere:

    python -B spec095_compare.py <before tree> <after tree> [<corpus tree>]

With a third argument, both trees are judged by that tree's corpus files instead of their own,
which is how the spec shows that the corpus change alone moves no row. It prints each tree's
totals line, then every row whose verdict differs, then the moves counted by kind.
"""

from __future__ import annotations

import collections
import os
import subprocess
import sys


def verdicts(tree: str, corpus: str) -> tuple[dict[str, str], str]:
    """One row name to its verdict, and the totals line, from one run of `spec092_run.py`."""
    probes = os.path.join(corpus, "docs", "audits", "2026-11-14-probes", "sinks")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [os.path.join(tree, "src"), os.path.join(tree, "tests"), probes]
    )
    done = subprocess.run(
        [sys.executable, "-B", os.path.join(probes, "spec092_run.py")],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    found: dict[str, str] = {}
    totals = ""
    for line in done.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[2] == "green":
            found[parts[0]] = "green"
        elif len(parts) >= 4 and parts[2] == "red":
            found[parts[0]] = parts[3]
        elif " rows, " in line:
            totals = line.strip()
    return found, totals


def main() -> int:
    before_tree, after_tree = sys.argv[1], sys.argv[2]
    corpus = sys.argv[3] if len(sys.argv) > 3 else None
    before, before_totals = verdicts(before_tree, corpus or before_tree)
    after, after_totals = verdicts(after_tree, corpus or after_tree)
    print(f"before: {before_totals}")
    print(f"after:  {after_totals}")
    moved = [(name, before[name], after.get(name, "absent")) for name in before]
    moved = [one for one in moved if one[1] != one[2]]
    moved += [(name, "absent", after[name]) for name in after if name not in before]
    for name, old, new in sorted(moved):
        print(f"{name:58} {old} -> {new}")
    kinds = collections.Counter((old, new) for _, old, new in moved)
    print(f"\n{len(moved)} row(s) move")
    for (old, new), count in sorted(kinds.items()):
        print(f"{count:4}  {old} -> {new}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
