"""SPEC-092 build: the corpus's mutant runs, its three older trees, its runtime and its scope check.

Written on 2026-11-18 by SPEC-092's build; not one of the audit's probes. Where `spec092_mutants.py`
ran the prototype, this runs the build's corpus: `tests/conformance_{classes,backend,doubles,
pins}.py`, copied out of an export of `HEAD` into a directory of their own that is put FIRST on
the path, in front of the export's `src` and `tests`, so a child judges with the branch's corpus
whatever tree it imports `log_foundry` and `conftest` from. Each child prints both modules'
`__file__`. Run from the repository root, with the project's interpreter, once the corpus is
committed:

    poetry run python -B docs/audits/2026-11-14-probes/sinks/spec092_build_mutants.py

It prints every row each run moves, with old and new verdicts, and exits 1 when a criterion of
SPEC-092 it measures does not hold:

- FR-003: with `_diag.lost` returning at once, `SQSSink[standard]-refused-always` includes
  `unannounced`; with `_RedisSink._round` answering `UNKNOWN`, both Redis sinks' `refused-once`
  read `ambiguous-when-knowable`; with the `sink_lost` increment of a settled run in
  `Worker._emit` removed, `SQSSink[standard]-partial` reads `ledger`. Each in an export of `HEAD`.
- FR-003, in-process on `HEAD`: each check removed from `judge` (the four named rows), `fate_of`
  reading `REJECTED` as landed, and `duplicate` firing at one copy, with every row each moves.
- FR-002: with `_classify`'s `sys.modules` empty, so `driver_classes` answers `()`, the named
  rows move; every row that moved is listed.
- FR-005: against `a2cd4b36^`, `9b54b370^` and `a0f44b5f`, with the ledger off, the rows that
  move from an export of `6bd0b66e`, also with the ledger off, are exactly the ones FR-005 names.
- The runtime of `tests/test_conformance.py` under `pytest -n 0`, three times, with the
  one-minute load; and `git diff --exit-code origin/main -- src`'s exit code.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path.cwd()
HELPERS = (
    "conformance_classes.py",
    "conformance_backend.py",
    "conformance_doubles.py",
    "conformance_pins.py",
)

SOURCE_MUTANTS = {
    "diag-lost-silent": (
        "src/log_foundry/_diag.py",
        "    try:\n        table = _FOLD.table\n        if table is not None and not running_total",
        "    return\n    try:\n        table = _FOLD.table\n"
        "        if table is not None and not running_total",
    ),
    "redis-round-unknown": (
        "src/log_foundry/sinks/redis.py",
        "outcome = Outcome.RETRYABLE if retryable else Outcome.REJECTED",
        "outcome = Outcome.UNKNOWN",
    ),
    "worker-sink-lost-dropped": (
        "src/log_foundry/worker.py",
        "sink_lost=size - delivery.accepted - delivery.skipped,",
        "sink_lost=0,",
    ),
}
"""One exact replacement each, refused unless its text is found exactly once."""

HARNESS_MUTANTS = (
    "without:partition",
    "without:silent-loss",
    "without:duplicate",
    "without:lost-but-held",
    "without:unannounced",
    "without:ambiguous-when-knowable",
    "without:ledger",
    "fate_of:rejected-landed",
    "duplicate:at-least-once",
    "driver_classes:empty",
)

OLDER = {
    "a2cd4b36^": {
        "StdoutSink-taken-then-raised": ["duplicate"],
        "StderrSink-taken-then-raised": ["duplicate"],
    },
    "9b54b370^": {
        "StdoutSink-taken-then-raised": ["duplicate"],
        "StderrSink-taken-then-raised": ["duplicate"],
        "FileSink-refused-once": ["lost-but-held"],
        "RotatingFileSink-refused-once": ["lost-but-held"],
    },
    "a0f44b5f": {
        "StdoutSink-taken-then-raised": ["duplicate"],
        "StderrSink-taken-then-raised": ["duplicate"],
        "FileSink-refused-once": ["lost-but-held"],
        "RotatingFileSink-refused-once": ["lost-but-held"],
    },
}
"""FR-005: each older tree's rows that read otherwise than at `6bd0b66e`, and their verdicts."""

CHILD = r"""
import json, sys, types
import log_foundry
import conformance_backend as cb
import conformance_classes as cc

mode = json.loads(sys.argv[1])
harness = mode["harness"]
if harness and harness.startswith("without:"):
    gone = harness.split(":", 1)[1]
    assert gone in [n for n, _ in cb.CHECKS], gone
    cb.CHECKS = tuple(c for c in cb.CHECKS if c[0] != gone)
elif harness == "fate_of:rejected-landed":
    from log_foundry.sinks.verdict import Outcome
    cb.FATES[Outcome.REJECTED] = "landed"
elif harness == "duplicate:at-least-once":
    def at_least_once(events, backend, report):
        return any(backend.held[t] >= 1 for t in events)
    cb.CHECKS = tuple((n, at_least_once if n == "duplicate" else p) for n, p in cb.CHECKS)
elif harness == "driver_classes:empty":
    from log_foundry.sinks import _classify
    _classify.sys = types.SimpleNamespace(modules={})
elif harness:
    raise SystemExit(f"unknown harness mutant {harness}")
out = {"__file__": log_foundry.__file__, "__corpus__": cb.__file__}
for row in cc.rows():
    try:
        out[row.id] = list(cb.run_row(row, ledger=mode["ledger"]).verdict)
    except Exception as err:
        out[row.id] = ["harness-error:" + type(err).__name__ + ":" + str(err)[:100]]
print(json.dumps(out))
"""


def export(ref: str, into: pathlib.Path) -> None:
    """`git archive <ref> src tests`, extracted into `into`."""
    archive = subprocess.run(
        ["git", "-C", str(ROOT), "archive", ref, "src", "tests"], capture_output=True, check=True
    )
    subprocess.run(["tar", "-x", "-C", str(into)], input=archive.stdout, check=True)


def verdicts(tree: pathlib.Path, helpers: pathlib.Path, mode: dict[str, object]) -> dict[str, list[str]]:
    """Every row's verdict in a child judging with `helpers` against `tree`."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(helpers), str(tree / "src"), str(tree / "tests")])
    done = subprocess.run(
        [sys.executable, "-B", "-c", CHILD, json.dumps(mode)],
        env=env,
        cwd=str(tree),
        capture_output=True,
        text=True,
        check=False,
    )
    if done.returncode:
        raise SystemExit(f"child failed:\n{done.stderr[-3000:]}")
    data: dict[str, list[str]] = json.loads(done.stdout.strip().splitlines()[-1])
    print(f"  log_foundry.__file__         = {data['__file__']}")
    print(f"  conformance_backend.__file__ = {data['__corpus__']}")
    errors = [k for k, v in data.items() if v and isinstance(v, list) and v[0].startswith("harness")]
    if errors:
        print(f"  HARNESS ERRORS: {errors}")
    return data


def moved(base: dict[str, list[str]], now: dict[str, list[str]]) -> dict[str, tuple[list[str], list[str]]]:
    """Rows whose verdict differs, with old and new."""
    return {
        k: (base[k], now[k])
        for k in base
        if not k.startswith("__") and base[k] != now.get(k)
    }


def show(label: str, changes: dict[str, tuple[list[str], list[str]]]) -> None:
    """Prints a run's moved rows."""
    green = sum(1 for was, _ in changes.values() if not was)
    print(f"{label}: {len(changes)} row(s) moved, {green} of them green before")
    for name, (was, now) in sorted(changes.items()):
        print(f"    {name}: {'+'.join(was) or 'green'} -> {'+'.join(now) or 'green'}")


def main() -> int:
    """Runs everything, prints it, and returns 1 if any criterion failed."""
    failures: list[str] = []
    head = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--short=8", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    print(f"SPEC-092 build results at HEAD {head}, Python {sys.version.split()[0]}\n")
    with tempfile.TemporaryDirectory(prefix="spec092-build-") as scratch:
        work = pathlib.Path(scratch)
        helpers = work / "helpers"
        helpers.mkdir()
        head_tree = work / "head"
        head_tree.mkdir()
        export("HEAD", head_tree)
        for name in HELPERS:
            shutil.copy(head_tree / "tests" / name, helpers / name)

        print("== HEAD, ledger on")
        base = verdicts(head_tree, helpers, {"ledger": True, "harness": None})
        red = {k: v for k, v in base.items() if not k.startswith("__") and v}
        print(f"  {len(base) - 2} rows, {len(red)} red")

        for label, (path, old, new) in SOURCE_MUTANTS.items():
            tree = work / label
            tree.mkdir()
            export("HEAD", tree)
            target = tree / path
            text = target.read_text(encoding="utf-8")
            if text.count(old) != 1:
                failures.append(f"{label}: its text is not found exactly once in {path}")
                continue
            target.write_text(text.replace(old, new), encoding="utf-8")
            print(f"\n== source mutant {label}: {path}")
            changes = moved(base, verdicts(tree, helpers, {"ledger": True, "harness": None}))
            show(label, changes)
            now = {k: v[1] for k, v in changes.items()}
            if label == "diag-lost-silent":
                got = now.get("SQSSink[standard]-refused-always", base["SQSSink[standard]-refused-always"])
                if "unannounced" not in got:
                    failures.append(f"{label}: SQSSink[standard]-refused-always reads {got}")
            elif label == "redis-round-unknown":
                for row in ("RedisStreamsSink-refused-once", "RedisListSink-refused-once"):
                    if now.get(row) != ["ambiguous-when-knowable"]:
                        failures.append(f"{label}: {row} reads {now.get(row)}")
            elif label == "worker-sink-lost-dropped":
                if now.get("SQSSink[standard]-partial") != ["ledger"]:
                    failures.append(f"{label}: SQSSink[standard]-partial reads {now.get('SQSSink[standard]-partial')}")

        for harness in HARNESS_MUTANTS:
            print(f"\n== in-process mutant {harness}")
            changes = moved(base, verdicts(head_tree, helpers, {"ledger": True, "harness": harness}))
            show(harness, changes)
            now = {k: v[1] for k, v in changes.items()}
            expect = {
                "without:partition": ("KafkaSink-never-acknowledged", ["silent-loss"]),
                "without:silent-loss": ("SyslogSink[tcp]-peer-closed", []),
                "without:duplicate": ("HTTPSink-reply-lost", []),
                "without:lost-but-held": ("MongoDBSink-reply-lost", []),
            }
            if harness in expect:
                row, want = expect[harness]
                if now.get(row, base[row]) != want:
                    failures.append(f"{harness}: {row} reads {now.get(row, base[row])}")
            elif harness == "fate_of:rejected-landed":
                if "partition" not in now.get("SQSSink[standard]-partial", []):
                    failures.append(f"{harness}: SQSSink[standard]-partial lacks partition")
            elif harness == "duplicate:at-least-once":
                for row, verdict in base.items():
                    if row.endswith("-clean") and not verdict and row != "NullSink-clean":
                        if "duplicate" not in now.get(row, []):
                            failures.append(f"{harness}: {row} does not read duplicate")
            elif harness == "driver_classes:empty":
                for sink in ("SQSSink[standard]", "ClickHouseSink", "RedisListSink"):
                    for fault in ("taken-then-raised", "reply-lost"):
                        if f"{sink}-{fault}" not in now:
                            failures.append(f"{harness}: {sink}-{fault} did not move")

        print("\n== FR-005: 6bd0b66e, ledger off (the baseline)")
        old_base_tree = work / "t6bd0b66e"
        old_base_tree.mkdir()
        export("6bd0b66e", old_base_tree)
        baseline = verdicts(old_base_tree, helpers, {"ledger": False, "harness": None})
        print(f"  {sum(1 for k, v in baseline.items() if not k.startswith('__') and v)} red")
        print("\n== HEAD, ledger off, against 6bd0b66e (rows a dependency moved)")
        show("HEAD", moved(baseline, verdicts(head_tree, helpers, {"ledger": False, "harness": None})))
        for ref, want in OLDER.items():
            tree = work / re.sub(r"\W", "_", ref)
            tree.mkdir()
            export(ref, tree)
            print(f"\n== FR-005: {ref}, ledger off")
            changes = moved(baseline, verdicts(tree, helpers, {"ledger": False, "harness": None}))
            show(ref, changes)
            got = {k: v[1] for k, v in changes.items()}
            if got != want:
                failures.append(f"{ref}: moved {got}, FR-005 names {want}")

    print("\n== runtime: pytest tests/test_conformance.py -n 0, three runs")
    for _ in range(3):
        started = time.perf_counter()
        done = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/test_conformance.py", "-n", "0", "-q",
             "-p", "no:cacheprovider"],
            capture_output=True,
            text=True,
            check=False,
        )
        wall = time.perf_counter() - started
        summary = done.stdout.strip().splitlines()[-1]
        print(f"  exit {done.returncode}, {wall:.2f} s wall, one-minute load {os.getloadavg()[0]:.2f}: {summary}")
        if done.returncode or wall >= 10:
            failures.append(f"runtime: exit {done.returncode}, {wall:.2f} s")

    scope = subprocess.run(
        ["git", "-C", str(ROOT), "diff", "--exit-code", "--quiet", "origin/main", "--", "src"],
        check=False,
    )
    print(f"\n== git diff --exit-code origin/main -- src: exit {scope.returncode}")
    if scope.returncode:
        failures.append("src/ differs from origin/main")

    print("\n== criteria")
    for failure in failures:
        print(f"  FAILED {failure}")
    print("  all held" if not failures else f"  {len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
