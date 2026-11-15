"""SPEC-092 prototype: runs every row and prints its verdict; `--mutants` measures the checks.

Written on 2026-11-15 while authoring SPEC-092, against 6bd0b66e; not one of the audit's probes.
Run from the repository root:

    PYTHONPATH=src:tests:docs/audits/2026-11-14-probes/sinks python -B \\
        docs/audits/2026-11-14-probes/sinks/spec092_run.py [--mutants] [--detail]

It prints one line per row, `green` or `red <checks>`, then the totals and the wall time. To run
it against an older tree, put `git archive <ref> src tests | tar -x -C <dir>` first on the path:
`PYTHONPATH=<dir>/src:<dir>/tests:docs/audits/2026-11-14-probes/sinks`. With
`--mutants` it reruns every row with each check removed and prints, per check, the rows whose
verdict that removal changes.
"""

from __future__ import annotations

import sys
import time

from spec092_corpus import CHECKS, run
from spec092_table import rows


def main() -> int:
    detail = "--detail" in sys.argv
    started = time.perf_counter()
    table = rows()
    verdicts = {}
    for row in table:
        try:
            verdict, info = run(row)
        except Exception as err:
            verdict, info = (f"harness-error:{type(err).__name__}:{err}",), {}
        verdicts[row.name] = verdict
        mark = "green" if not verdict else "red " + "+".join(verdict)
        line = f"{row.name:58} {row.klass:17} {mark}"
        if detail and verdict:
            line += f"   {info}"
        print(line)
    elapsed = time.perf_counter() - started
    red = [name for name, v in verdicts.items() if v]
    print(f"\n{len(table)} rows, {len(table) - len(red)} green, {len(red)} red, {elapsed:.2f} s")
    if "--mutants" in sys.argv:
        for check in CHECKS:
            changed = []
            for row in table:
                try:
                    verdict, _ = run(row, frozenset({check}))
                except Exception as err:
                    verdict = (f"harness-error:{type(err).__name__}",)
                if verdict != verdicts[row.name]:
                    changed.append(row.name)
            print(f"without {check}: {len(changed)} row(s) change: {', '.join(changed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
