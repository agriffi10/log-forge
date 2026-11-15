"""SPEC-092 prototype: each check of the assertion against a mutant planted where it must fire.

Written on 2026-11-15 while authoring SPEC-092, against 6bd0b66e; not one of the audit's probes.
Run from the repository root, with the shared interpreter:

    PYTHONPATH=docs/audits/2026-11-14-probes/sinks python -B \\
        docs/audits/2026-11-14-probes/sinks/spec092_mutants.py

For each mutant it exports `src/` and `tests/` with `git archive HEAD src tests` into a temporary
directory, applies one exact text replacement to one file there (refusing if the text is not found
exactly once), runs every row in a child interpreter against that export, and prints the rows
whose verdict moved, with old and new verdicts. The two harness mutants replace one function of
the corpus module in the child instead. Nothing in the working tree is written.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent

SOURCE_MUTANTS = {
    "unannounced": (
        "src/log_foundry/_diag.py",
        'def lost(what: str, count: int, detail: str = "") -> None:\n',
        'def lost(what: str, count: int, detail: str = "") -> None:\n    return\n',
    ),
    "ambiguous-when-knowable": (
        "src/log_foundry/sinks/redis.py",
        "outcome = Outcome.RETRYABLE if retryable else Outcome.REJECTED",
        "outcome = Outcome.UNKNOWN",
    ),
}
"""A sink-side mutant per check no unmutated row needs alone, each the defect the check exists for.

`unannounced`: the one writer of a loss line writes nothing. `ambiguous-when-knowable`: a
Redis pipeline that raised before anything was taken is called `UNKNOWN`, the cop-out a fix that
calls every raise ambiguous would make."""

HARNESS_MUTANTS = {
    "partition (differential oracle)": (
        "fate_of",
        'Outcome.REJECTED: "lost"',
        'Outcome.REJECTED: "landed"',
    ),
    "duplicate (over-firing)": ("judge", "held[t] > 1)", "held[t] >= 1)"),
}
"""Two harness mutants: the reconstruction misreading one outcome, which only the comparison with
the engine's own `Delivery` catches, and one check's boundary moved, which every green row must
catch by turning red (the silence half of the gate)."""

CHILD = r"""
import inspect, json, sys
import spec092_corpus as corpus
from spec092_table import rows
mutant = json.loads(sys.argv[1])
if mutant:
    name, old, new = mutant
    fn = getattr(corpus, name)
    text = inspect.getsource(fn)
    assert text.count(old) == 1, (name, old)
    namespace = vars(corpus)
    exec(compile(text.replace(old, new), corpus.__file__, "exec"), namespace)
out = {}
for row in rows():
    try:
        out[row.name] = list(corpus.run(row)[0])
    except Exception as err:
        out[row.name] = ["harness-error:" + type(err).__name__]
print(json.dumps(out))
"""


def verdicts(src: Path, harness: tuple[str, str, str] | None) -> dict[str, list[str]]:
    env = dict(
        os.environ, PYTHONPATH=os.pathsep.join([str(src), str(src.parent / "tests"), str(HERE)])
    )
    out = subprocess.run(  # noqa: S603
        [sys.executable, "-B", "-c", CHILD, json.dumps(harness)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def export(root: Path, into: Path) -> Path:
    archive = subprocess.run(  # noqa: S603
        ["git", "-C", str(root), "archive", "HEAD", "src", "tests"],  # noqa: S607
        capture_output=True,
        check=True,
    )
    subprocess.run(  # noqa: S603
        ["tar", "-x", "-C", str(into)],  # noqa: S607
        input=archive.stdout,
        check=True,
    )
    return into / "src"


def main() -> int:
    root = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="spec092-base-") as base:
        baseline = verdicts(export(root, Path(base)), None)
    for check, (path, old, new) in SOURCE_MUTANTS.items():
        with tempfile.TemporaryDirectory(prefix="spec092-mut-") as tmp:
            src = export(root, Path(tmp))
            target = Path(tmp) / path
            text = target.read_text()
            if text.count(old) != 1:
                print(f"{check}: the mutant's text is not found exactly once in {path}")
                return 1
            target.write_text(text.replace(old, new))
            moved = {
                k: (baseline[k], v) for k, v in verdicts(src, None).items() if v != baseline[k]
            }
        print(f"\n{check}: {len(moved)} row(s) moved")
        print(f"  ({sum(1 for was, _ in moved.values() if not was)} of them were green)")
        for name, (was, now) in moved.items():
            print(f"  {name}: {'+'.join(was) or 'green'} -> {'+'.join(now) or 'green'}")
    for label, mutant in HARNESS_MUTANTS.items():
        with tempfile.TemporaryDirectory(prefix="spec092-har-") as tmp:
            moved = {
                k: (baseline[k], v)
                for k, v in verdicts(export(root, Path(tmp)), mutant).items()
                if v != baseline[k]
            }
        print(f"\n{label}: {len(moved)} row(s) moved")
        was_green = sum(1 for was, _ in moved.values() if not was)
        print(f"  ({was_green} of them were green)")
        for name, (was, now) in moved.items():
            print(f"  {name}: {'+'.join(was) or 'green'} -> {'+'.join(now) or 'green'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
