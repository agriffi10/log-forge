"""SPEC-095 build: the row moves, the corpus-only check and the mutants, each in an export.

Written on 2026-11-20 by SPEC-095's build; not one of the audit's probes. It runs the build's
corpus — `tests/conformance_{classes,backend,doubles,pins}.py` — the way `spec092_build_mutants.py`
does: each tree is a `git archive <ref> src tests pyproject.toml` export under one temporary root,
and the corpus files of the judged ref are copied into a directory of their own put FIRST on the
path, in front of the export's `src` and `tests`. Every child asserts that `log_foundry` and
`conformance_backend` were imported from the export and the helpers directory it was handed, and
stops otherwise, so a mutant can never be judged against the editable install. Run from the
repository root with any interpreter that has the dev dependencies (the children need only
`pytest` and its plugins; `log_foundry` comes from each export):

    poetry run python -B docs/audits/2026-11-14-probes/sinks/spec095_build_mutants.py

It prints every row each run moves, with old and new verdicts, and exits 1 when a criterion it
measures does not hold:

- FR-004 AC2: against the base (the merge-base with `origin/main`, the corpus before this spec),
  exactly the 50 rows of the base's `reply-resent` pin move from `duplicate` to green,
  `MongoDBSink`'s `taken-then-raised` and `reply-lost` from `lost-but-held` to green, and the 19
  rows of the five departures SPEC-095 adds from green to `resent-when-knowable`; no other row.
- FR-004 AC3: the head's corpus against the base's `src` moves no row.
- FR-002 AC7: `_classify.may_have_landed` answering True moves exactly the 34 `refused-once` and
  `refused-always` rows of the HTTP family (its four wrapper variants and `SentrySink[http]`
  included) and the two TCP socket variants, to `resent-when-knowable`.
- FR-002 AC8: `RunContext.may_have_landed` doing nothing makes the base's 50 `reply-resent` rows
  read `duplicate` again.
- FR-004 AC5: `_diag.resent` writing nothing gives `unannounced` to exactly the rows that count
  a resend.
- FR-004 AC6: `Delivery.resent` never set gives `partition` to every per-event row that counts a
  resend and moves no other row; and with `_run` never committing a `resent_into` count — the
  route `HTTPSink._send` takes since the build moved that commit into the engine —
  `SentrySink[http]`'s two such rows, judged per batch, read `duplicate` and nothing else moves.
- FR-004 AC7: `resent-when-knowable` removed from `judge` frees exactly the 19 departure rows.
- FR-003 AC6: with condition 3 removed, `test_a_caller_set_id_is_never_read_as_landed` fails;
  with condition 4 removed, `test_a_first_send_refused_on_id_is_rejected` fails; both pass in the
  unmutated export. Run with `pytest -n 0` inside the export, its `pyproject.toml` read as the ini.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path.cwd()
HELPERS = (
    "conformance_classes.py",
    "conformance_backend.py",
    "conformance_doubles.py",
    "conformance_pins.py",
)
HTTP_FAMILY = (
    "HTTPSink",
    "DatadogSink",
    "SplunkHECSink",
    "NewRelicSink",
    "LokiSink",
    "OTLPSink",
    "LogstashSink[http]",
    "SentrySink[http]",
    "ElasticsearchSink",
    "OpenSearchSink",
    "HoneycombSink",
    "FilteringSink[over-http]",
    "TransformSink[over-http]",
    "MultiSink[over-http]",
    "_FactorySink[over-http]",
    "SyslogSink[tcp]",
    "LogstashSink[tcp]",
)
"""FR-002 AC7's 17 variants: the HTTP family, its wrappers and `SentrySink[http]`, and TCP."""

SPEC_095_DEPARTURES = (
    "aws-client-retries",
    "firehose-coded-may-land",
    "eventhubs-client-retries",
    "clickhouse-error-unclassed",
    "redis-client-retries",
)

SOURCE_MUTANTS = {
    "classify-may-have-landed-true": (
        "src/log_foundry/sinks/_classify.py",
        "    kind = type(err)\n    if issubclass(kind, urllib.error.HTTPError):",
        "    return True\n    kind = type(err)\n    if issubclass(kind, urllib.error.HTTPError):",
    ),
    "context-drops-marks": (
        "src/log_foundry/sinks/_engine.py",
        "            self._reached.extend(indices)",
        "            pass",
    ),
    "diag-resent-silent": (
        "src/log_foundry/_diag.py",
        '    try:\n        table = _FOLD.table\n        if table is not None and type(count) is int:\n'
        '            key = ("resent", _suffix(detail))',
        '    return\n    try:\n        table = _FOLD.table\n        if table is not None and type(count) is int:\n'
        '            key = ("resent", _suffix(detail))',
    ),
    "delivery-resent-unset": (
        "src/log_foundry/sinks/_engine.py",
        "            delivery = replace(delivery, resent=resent)",
        "            pass",
    ),
    "resent-into-dropped": (
        "src/log_foundry/sinks/_engine.py",
        "        if resent_into is not None and resent and resend_line is not None:",
        "        if False:",
    ),
}
"""One exact replacement each, refused unless its text is found exactly once; the `_diag`
mutant's `return` lands after the docstring, as the first statement of `resent`'s body."""

MONGO_MUTANTS = {
    "mongo-without-condition-3": (
        '                and "_id" not in carried[at]\n',
        "tests/test_sinks_mongodb_resend.py::test_a_caller_set_id_is_never_read_as_landed",
    ),
    "mongo-without-condition-4": (
        "                and sends_of(self, carried[at]) > 1\n",
        "tests/test_sinks_mongodb_resend.py::test_a_first_send_refused_on_id_is_rejected",
    ),
}
"""FR-003 AC6: the clause removed from `_settle_bulk`, and the test that must then fail."""

MONGO_NODES = (
    "tests/test_sinks_mongodb_resend.py::test_a_caller_set_id_is_never_read_as_landed",
    "tests/test_sinks_mongodb_resend.py::test_a_first_send_refused_on_id_is_rejected",
)

CHILD = r"""
import json, pathlib, sys
mode = json.loads(sys.argv[1])
import log_foundry
import conformance_backend as cb
import conformance_classes as cc
import conformance_pins as cp
for module, root in ((log_foundry, mode["tree"]), (cb, mode["helpers"])):
    where = pathlib.Path(module.__file__).resolve()
    if pathlib.Path(root).resolve() not in where.parents:
        raise SystemExit(f"{module.__name__} came from {where}, not from {root}")
harness = mode["harness"]
if harness and harness.startswith("without:"):
    gone = harness.split(":", 1)[1]
    assert gone in [n for n, _ in cb.CHECKS], gone
    cb.CHECKS = tuple(c for c in cb.CHECKS if c[0] != gone)
elif harness:
    raise SystemExit(f"unknown harness mutant {harness}")
out = {
    "__file__": log_foundry.__file__,
    "__corpus__": cb.__file__,
    "__pins__": {p.name: list(p.rows) for p in cp.PINS},
    "__departures__": {d.name: list(d.rows) for d in cp.DEPARTURES},
    "rows": {},
}
for row in cc.rows():
    try:
        result = cb.run_row(row)
        report = result.report
        resent = sum(getattr(report, "resent", {}).values()) + getattr(report, "resent_counted", 0)
        out["rows"][row.id] = {
            "verdict": list(result.verdict),
            "resent": resent,
            "per_event": report.fates is not None,
        }
    except Exception as err:
        out["rows"][row.id] = {
            "verdict": ["harness-error:" + type(err).__name__ + ":" + str(err)[:100]],
            "resent": 0,
            "per_event": False,
        }
print(json.dumps(out))
"""


def export(ref: str, into: pathlib.Path) -> None:
    """`git archive <ref> src tests pyproject.toml`, extracted into `into`."""
    archive = subprocess.run(
        ["git", "-C", str(ROOT), "archive", ref, "src", "tests", "pyproject.toml"],
        capture_output=True,
        check=True,
    )
    subprocess.run(["tar", "-x", "-C", str(into)], input=archive.stdout, check=True)


def _env(tree: pathlib.Path, first: pathlib.Path | None = None) -> dict[str, str]:
    env = dict(os.environ)
    paths = [str(first)] if first is not None else []
    env["PYTHONPATH"] = os.pathsep.join([*paths, str(tree / "src"), str(tree / "tests")])
    return env


def verdicts(tree: pathlib.Path, helpers: pathlib.Path, harness: str | None = None) -> dict:
    """Every row's verdict, resend count and reading in a child judging with `helpers`."""
    mode = {"tree": str(tree), "helpers": str(helpers), "harness": harness}
    done = subprocess.run(
        [sys.executable, "-B", "-c", CHILD, json.dumps(mode)],
        env=_env(tree, helpers),
        cwd=str(tree),
        capture_output=True,
        text=True,
        check=False,
    )
    if done.returncode:
        raise SystemExit(f"child failed:\n{done.stdout[-2000:]}\n{done.stderr[-3000:]}")
    data = json.loads(done.stdout.strip().splitlines()[-1])
    print(f"  log_foundry.__file__         = {data['__file__']}")
    print(f"  conformance_backend.__file__ = {data['__corpus__']}")
    errors = [k for k, v in data["rows"].items() if v["verdict"][:1] and "harness" in v["verdict"][0]]
    if errors:
        print(f"  HARNESS ERRORS: {errors}")
    return data


def moved(base: dict, now: dict) -> dict[str, tuple[list[str], list[str]]]:
    """Rows whose verdict differs, with old and new."""
    return {
        k: (v["verdict"], now["rows"][k]["verdict"])
        for k, v in base["rows"].items()
        if v["verdict"] != now["rows"].get(k, {}).get("verdict")
    }


def show(label: str, changes: dict[str, tuple[list[str], list[str]]]) -> None:
    """Prints a run's moved rows."""
    print(f"{label}: {len(changes)} row(s) moved")
    for name, (was, now) in sorted(changes.items()):
        print(f"    {name}: {'+'.join(was) or 'green'} -> {'+'.join(now) or 'green'}")


def red_count(data: dict) -> tuple[int, int]:
    """`(rows, red)`."""
    rows = data["rows"]
    return len(rows), sum(1 for v in rows.values() if v["verdict"])


def pytest_in(tree: pathlib.Path, nodes: tuple[str, ...]) -> tuple[int, str]:
    """Runs `nodes` with `pytest -n 0` inside `tree`, after checking it imports the export."""
    probe = subprocess.run(
        [sys.executable, "-B", "-c", "import log_foundry; print(log_foundry.__file__)"],
        env=_env(tree),
        cwd=str(tree),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if tree.resolve() not in pathlib.Path(probe).resolve().parents:
        raise SystemExit(f"pytest in {tree} would import log_foundry from {probe}")
    print(f"  log_foundry.__file__ = {probe}")
    done = subprocess.run(
        [sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "-n", "0", "-q", *nodes],
        env=_env(tree),
        cwd=str(tree),
        capture_output=True,
        text=True,
        check=False,
    )
    return done.returncode, done.stdout


def main() -> int:
    """Runs everything, prints it, and returns 1 if any criterion failed."""
    failures: list[str] = []
    head = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--short=8", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    base_ref = subprocess.run(
        ["git", "-C", str(ROOT), "merge-base", "HEAD", "origin/main"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()[:8]
    print(f"SPEC-095 build results at HEAD {head} against base {base_ref}, ", end="")
    print(f"Python {sys.version.split()[0]}\n")
    with tempfile.TemporaryDirectory(prefix="spec095-build-") as scratch:
        work = pathlib.Path(scratch)
        trees: dict[str, pathlib.Path] = {}
        helpers: dict[str, pathlib.Path] = {}
        for label, ref in (("base", base_ref), ("head", "HEAD")):
            trees[label] = work / label
            trees[label].mkdir()
            export(ref, trees[label])
            helpers[label] = work / f"helpers-{label}"
            helpers[label].mkdir()
            for name in HELPERS:
                shutil.copy(trees[label] / "tests" / name, helpers[label] / name)

        print("== base tree, base corpus")
        base = verdicts(trees["base"], helpers["base"])
        print("  %d rows, %d red" % red_count(base))
        print("== head tree, head corpus")
        now = verdicts(trees["head"], helpers["head"])
        print("  %d rows, %d red" % red_count(now))
        changes = moved(base, now)
        show("\n== FR-004 AC2: base -> head", changes)
        reply_resent = set(base["__pins__"]["reply-resent"])
        mongo = {"MongoDBSink-taken-then-raised", "MongoDBSink-reply-lost"}
        departing = {r for d in SPEC_095_DEPARTURES for r in now["__departures__"][d]}
        want = (
            {r: (["duplicate"], []) for r in reply_resent}
            | {r: (["lost-but-held"], []) for r in mongo}
            | {r: ([], ["resent-when-knowable"]) for r in departing}
        )
        print(f"  expected: {len(reply_resent)} + {len(mongo)} + {len(departing)} = {len(want)}")
        if changes != want:
            extra = sorted(set(changes) ^ set(want))
            differ = sorted(k for k in set(changes) & set(want) if changes[k] != want[k])
            failures.append(f"AC2: moved set differs; symmetric difference {extra}; {differ}")
        if len(want) != 71:
            failures.append(f"AC2: expected 71 rows, derived {len(want)}")

        print("\n== FR-004 AC3: base tree, head corpus")
        alone = verdicts(trees["base"], helpers["head"])
        alone_moves = moved(base, alone)
        show("corpus change alone", alone_moves)
        if alone_moves:
            failures.append(f"AC3: the corpus alone moved {len(alone_moves)} row(s)")

        counting = {k for k, v in now["rows"].items() if v["resent"]}
        print(f"\n  rows that count a resend at head: {len(counting)}")

        for label, (path, old, new) in SOURCE_MUTANTS.items():
            tree = work / label
            tree.mkdir()
            export("HEAD", tree)
            target = tree / path
            text = target.read_text(encoding="utf-8")
            if text.count(old) != 1:
                failures.append(f"{label}: its text is found {text.count(old)} times in {path}")
                continue
            target.write_text(text.replace(old, new), encoding="utf-8")
            print(f"\n== source mutant {label}: {path}")
            mutated = verdicts(tree, helpers["head"])
            changes = moved(now, mutated)
            show(label, changes)
            after = {k: v[1] for k, v in changes.items()}
            if label == "classify-may-have-landed-true":
                rows = {f"{s}-{f}" for s in HTTP_FAMILY for f in ("refused-once", "refused-always")}
                expect = {r: ["resent-when-knowable"] for r in rows}
                if after != expect or len(expect) != 34:
                    failures.append(f"FR-002 AC7: {label} moved {sorted(after)}")
            elif label == "context-drops-marks":
                wrong = {r: after.get(r) for r in reply_resent if after.get(r) != ["duplicate"]}
                if wrong:
                    failures.append(f"FR-002 AC8: {label}: {wrong}")
            elif label == "diag-resent-silent":
                gained = {k for k, (was, is_) in changes.items() if "unannounced" in is_}
                if gained != counting or any(
                    set(is_) - set(was) != {"unannounced"} for was, is_ in changes.values()
                ):
                    failures.append(
                        f"FR-004 AC5: gained {len(gained)}, counting {len(counting)}, "
                        f"difference {sorted(gained ^ counting)}"
                    )
            elif label == "delivery-resent-unset":
                per_event = {k for k in counting if now["rows"][k]["per_event"]}
                gained = {k for k, (was, is_) in changes.items() if "partition" in is_}
                if gained != per_event or set(changes) != per_event:
                    failures.append(
                        f"FR-004 AC6: partition on {sorted(gained ^ per_event)}, "
                        f"moved {sorted(set(changes) ^ per_event)}"
                    )
            elif label == "resent-into-dropped":
                sentry = {"SentrySink[http]-taken-then-raised", "SentrySink[http]-reply-lost"}
                if after != {row: ["duplicate"] for row in sentry}:
                    failures.append(f"FR-004 AC6: {label} moved {after}")

        print("\n== in-process mutant without:resent-when-knowable")
        freed = moved(now, verdicts(trees["head"], helpers["head"], "without:resent-when-knowable"))
        show("without:resent-when-knowable", freed)
        if freed != {r: (["resent-when-knowable"], []) for r in departing}:
            failures.append(f"FR-004 AC7: freed {sorted(freed)}")

        print("\n== FR-003 AC6: the MongoDB conditions, by pytest in an export")
        code, out = pytest_in(trees["head"], MONGO_NODES)
        print(f"  unmutated: exit {code}; {out.strip().splitlines()[-1]}")
        if code != 0:
            failures.append(f"FR-003 AC6: the unmutated export fails:\n{out[-1500:]}")
        for label, (clause, node) in MONGO_MUTANTS.items():
            tree = work / label
            tree.mkdir()
            export("HEAD", tree)
            target = tree / "src/log_foundry/sinks/mongodb.py"
            text = target.read_text(encoding="utf-8")
            if text.count(clause) != 1:
                failures.append(f"{label}: its clause is found {text.count(clause)} times")
                continue
            target.write_text(text.replace(clause, ""), encoding="utf-8")
            code, out = pytest_in(tree, MONGO_NODES)
            failed = [line for line in out.splitlines() if line.startswith("FAILED")]
            print(f"  {label}: exit {code}; {failed}")
            if code != 1 or [line.split(" ")[1] for line in failed] != [node]:
                failures.append(f"FR-003 AC6: {label} failed {failed}, not {node}")

    print("\n" + ("ALL CRITERIA HOLD" if not failures else "FAILURES:"))
    for line in failures:
        print(f"  - {line}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
