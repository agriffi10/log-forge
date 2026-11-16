"""SPEC-096 probe: does any SPEC-092 conformance row depend on SQSSink's or SNSSink's byte limit?

Written on 2026-11-16 while authoring SPEC-096, against 10ba21ee; not one of the audit's probes.
It runs every row of SPEC-092's prototype with `SQSSink.MAX_BYTES` and `SNSSink.MAX_BYTES` at
today's 262,144, then again at 1,048,576 (the alternative default) and at 1,024 (the floor SPEC-096
sets), and prints every row whose verdict differs from the first run. A last run at 16 bytes is the
control: there every event is oversized, so some SQS or SNS row must move, or the patch never
reached the rows and the runs before it prove nothing. It then
prints the two classes' `__init__` parameters as SPEC-092's `init_parameters` reads them, and the
total over the rostered classes, which SPEC-092 FR-001 records as `PARAMETERS`. Run from the
repository root:

    PYTHONPATH=src:tests:docs/audits/2026-11-14-probes/sinks python -B \\
        docs/audits/2026-11-14-probes/sinks/spec096_corpus_rows.py
"""

from __future__ import annotations

from spec092_corpus import run
from spec092_table import init_parameters, rows

from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqs import SQSSink


def verdicts() -> dict[str, tuple[str, ...]]:
    out = {}
    for row in rows():
        try:
            verdict, _ = run(row)
        except Exception as err:
            verdict = (f"harness-error:{type(err).__name__}",)
        out[row.name] = tuple(verdict)
    return out


def main() -> None:
    baseline = verdicts()
    red = sum(1 for v in baseline.values() if v)
    print(f"baseline at MAX_BYTES={SQSSink.MAX_BYTES}: {len(baseline)} rows, {red} red")
    for limit in (1024 * 1024, 1024, 16):
        SQSSink.MAX_BYTES = limit
        SNSSink.MAX_BYTES = limit
        try:
            moved = {k: v for k, v in verdicts().items() if v != baseline[k]}
        finally:
            SQSSink.MAX_BYTES = 256 * 1024
            SNSSink.MAX_BYTES = 256 * 1024
        print(f"MAX_BYTES={limit}: {len(moved)} row(s) moved")
        for name, verdict in sorted(moved.items()):
            print(f"   {name}: {baseline[name]} -> {verdict}")
    params = init_parameters()
    print(f"SQSSink parameters: {params['SQSSink']}")
    print(f"SNSSink parameters: {params['SNSSink']}")
    print(
        f"total parameter names over {len(params)} classes: {sum(len(v) for v in params.values())}"
    )


if __name__ == "__main__":
    main()
