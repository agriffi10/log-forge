"""SPEC-095 probe: which corpus rows each mutant of the resend count moves, on a prototype tree.

Written on 2026-11-16 while authoring SPEC-095, against 97a03f45; not one of the audit's probes.
It needs a tree carrying `spec095_prototype.patch` (or `spec095_prototype_after_094.patch`), whose
corpus files read the resend count. Run from that tree's root, with `-P` so that this script's
own directory, which holds the unpatched corpus, is not put first on the path:

    PYTHONPATH=src:tests:docs/audits/2026-11-14-probes/sinks python -B -P \\
        <path to>/spec095_mutants.py

Each mutant is applied in this process, every row is run, the mutant is undone, and the rows
whose verdict differs from the unmutated run are printed, counted by the verdict they moved to.
"""

from __future__ import annotations

import collections
import dataclasses
from typing import Any

import spec092_corpus as corpus
import spec092_table as table

from log_foundry import _diag
from log_foundry.sinks import _classify, _engine


def verdicts() -> dict[str, tuple[str, ...]]:
    """Every row's verdict under the current state of the modules."""
    return {row.name: corpus.run(row)[0] for row in table.rows()}


def measure(label: str, owner: Any, name: str, replacement: Any, base: dict[str, Any]) -> None:
    """Runs every row with `owner.name` replaced, and prints the rows that move."""
    original = getattr(owner, name)
    setattr(owner, name, replacement)
    try:
        moved = verdicts()
    finally:
        setattr(owner, name, original)
    changed = {row: (base[row], now) for row, now in moved.items() if now != base[row]}
    print(f"== {label}: {len(changed)} row(s) move")
    for (before, after), count in sorted(collections.Counter(changed.values()).items()):
        print(f"  {count:3}  {'+'.join(before) or 'green'} -> {'+'.join(after) or 'green'}")


def main() -> int:
    base = verdicts()
    measure("every raise may have landed", _classify, "may_have_landed", lambda err: True, base)
    measure(
        "the run context drops every mark",
        _engine.RunContext,
        "may_have_landed",
        lambda self, indices: None,
        base,
    )
    measure("the resend line writes nothing", _diag, "resent", lambda *a, **k: None, base)
    replace = dataclasses.replace

    def without_resent(obj: Any, **changes: Any) -> Any:
        changes.pop("resent", None)
        return replace(obj, **changes)

    measure("Delivery.resent never set", _engine.dataclasses, "replace", without_resent, base)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
