"""SPEC-093 and SPEC-094 build: the re-plants, the verdict comparisons and the probes, in exports.

Written on 2026-11-20 by the SPEC-093/094 build; not one of the audit's probes. Every tree here is
a `git archive` export under one temporary root, and every child runs with that tree's `src`
first on `PYTHONPATH` and prints `log_foundry.__file__`, which the runner refuses unless it lies
inside the export: the editable install otherwise resolves back to the worktree and every mutant
"passes". A re-plant is one edit, asserted to match exactly once, in a fresh export of `HEAD`;
nothing is restored in place. Run from the repository root, with the project's interpreter:

    poetry run python -B docs/audits/2026-11-14-probes/sinks/spec093_094_build_mutants.py \\
        [--docker] [--only NAME ...]

`--docker` also runs the TCP re-plants' tests in `python:3.12-slim` (Linux loopback), installing
pytest in the throwaway container. It prints every result; it does not judge them, since several
criteria name a platform and the build records what each platform read.

- SPEC-093 FR-001: the four close-rule re-plants, against `tests/test_socket_close_corpus.py` and
  `tests/test_socket_ack_check.py`, on macOS and (with `--docker`) Linux.
- SPEC-093 FR-005: rule 4 removed, the liveness probe removed, `confirm_delivery` never called,
  each through `tests/test_conformance.py`; and the verdict comparison of criterion 2, the
  settlement commit's corpus and SPEC-092's delivered corpus each against the build's first base's
  `src` and the settlement commit's `src`. **That comparison needs the settlement commit
  `a17a28a4`, a local commit of the build on base `c67931cd`, before SPEC-095 landed; it was
  squashed away before the merge and is not on the remote, so in a clone without it the
  comparison is skipped with a message.**
- SPEC-094 FR-001 and FR-002: the per-round wait as one blocking flush, the first slice ending at
  the wait's start, the pause removed and `pause_on` answering the set signal, each read through
  `spec094_stuck_partition.py` and `spec094_first_slice_and_pause.py`, which print wall time, CPU
  time and outcomes.
- SPEC-094 FR-003: the Kafka wait removed and the Pub/Sub double's local `PermissionDenied`, through
  `tests/test_conformance.py`.
- The rebase onto SPEC-095 and SPEC-098: `origin/main`'s corpus judged against its own `src`, at
  the commit the build was rebased onto, beside `HEAD`'s corpus against `HEAD`'s `src`, printing
  every shared row whose verdict moved and every row only one side has. That is SPEC-093 FR-005's
  and SPEC-094 FR-003's second criteria read on the merged tree.
- The probes the criteria name, against `HEAD`: `probe_socket.py` (and in Docker), `probe_misc.py`,
  `probe_sqs.py`, `probe_aws_more.py`, `spec094_stuck_partition.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[4]
PROBES = "docs/audits/2026-11-14-probes/sinks"
SETTLEMENT = "a17a28a4"
"""SPEC-093's settlement commit, local to the build: its corpus read 268, 62, 15 and 191."""
MAIN = "c67931cd"
"""`origin/main` when the build began, before SPEC-095 and SPEC-098 landed."""
REBASED_ONTO = "295ba803"
"""`origin/main` when the build was rebased, with SPEC-095 and SPEC-098 landed."""

SOCKET_TESTS = ["tests/test_socket_close_corpus.py", "tests/test_socket_ack_check.py"]
CORPUS = ["tests/test_conformance.py"]

REPLANTS: dict[str, dict[str, object]] = {
    "093-end-check-counts-fin": {
        "file": "src/log_foundry/sinks/_socket.py",
        "old": 'and _end_state(held, self._ack_wait) == "reset":',
        "new": 'and _end_state(held, self._ack_wait) in ("reset", "fin"):',
        "tests": SOCKET_TESTS,
        "docker": True,
    },
    "093-unacked-answers-0": {
        "file": "src/log_foundry/sinks/_socket.py",
        "old": "        queued = _unacked(sock)\n",
        "new": "        queued = 0\n",
        "tests": SOCKET_TESTS,
        "docker": True,
    },
    "093-peer-state-reads-no-so-error": {
        "file": "src/log_foundry/sinks/_socket.py",
        "old": (
            "        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR) in _RESET_ERRNOS:\n"
            '            return "reset"\n'
        ),
        "new": "",
        "tests": SOCKET_TESTS,
        "docker": True,
    },
    "093-reset-errnos-emptied": {
        "file": "src/log_foundry/sinks/_socket.py",
        "old": (
            "_RESET_ERRNOS = frozenset({errno.ECONNRESET, errno.EPIPE, errno.ECONNABORTED, "
            "errno.ETIMEDOUT})"
        ),
        "new": "_RESET_ERRNOS: frozenset[int] = frozenset()",
        "tests": SOCKET_TESTS,
        "docker": True,
    },
    "093-rule-4-removed": {
        "file": "src/log_foundry/sinks/_socket.py",
        "old": "                self._unconfirm(unconfirmed, fates)\n                unconfirmed = []\n",
        "new": "                unconfirmed = []\n",
        "tests": CORPUS,
    },
    "093-liveness-probe-removed": {
        "file": "src/log_foundry/sinks/rabbitmq.py",
        "old": "            self._probe_owned()\n",
        "new": "",
        "tests": CORPUS,
    },
    "093-confirm-delivery-never-called": {
        "file": "src/log_foundry/sinks/rabbitmq.py",
        "old": 'confirm = getattr(channel, "confirm_delivery", None) if self._confirm else None',
        "new": "confirm = None",
        "tests": CORPUS,
    },
    "094-kafka-one-blocking-flush": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": "            slice_ = max(0.0, min(deadline - now, _REPORT_SLICE))\n",
        "new": "            slice_ = max(0.0, deadline - now)\n",
        "probe": ["spec094_stuck_partition.py"],
    },
    "094-kafka-first-slice-at-start": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": "        first_slice_end = began + _REPORT_SLICE\n",
        "new": "        first_slice_end = began\n",
        "probe": ["spec094_first_slice_and_pause.py"],
        "tests": ["tests/test_kafka_report_wait.py"],
    },
    "094-kafka-no-pause": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": "                wait(slice_ - (now - slice_start), pause_on(stop))\n",
        "new": "                pass\n",
        "probe": ["spec094_first_slice_and_pause.py"],
    },
    "094-kafka-pause-on-returns-the-set-stop": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": "                wait(slice_ - (now - slice_start), pause_on(stop))\n",
        "new": "                wait(slice_ - (now - slice_start), stop)\n",
        "probe": ["spec094_first_slice_and_pause.py"],
    },
    "094-pubsub-first-slice-at-start": {
        "file": "src/log_foundry/sinks/pubsub.py",
        "old": "        first_slice_end = began + _POLL_INTERVAL\n",
        "new": "        first_slice_end = began\n",
        "probe": ["spec094_first_slice_and_pause.py"],
        "tests": ["tests/test_pubsub_round_wait.py"],
    },
    "094-pubsub-no-pause": {
        "file": "src/log_foundry/sinks/pubsub.py",
        "old": "                    wait(slice_ - (time.monotonic() - now), pause_on(stop))\n",
        "new": "                    pass\n",
        "probe": ["spec094_first_slice_and_pause.py"],
    },
    "094-kafka-wait-removed": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": "            drained = self._await_reports(reports, produced)\n",
        "new": "            drained = False\n",
        "tests": CORPUS,
    },
    "094-pubsub-double-raises-local-permission-denied": {
        "file": "tests/conformance_doubles.py",
        "old": (
            "            raise driver_error(\n"
            '                "google.api_core.exceptions", "InvalidArgument", '
            '"the topic refused the publish"\n'
            "            )\n"
        ),
        "new": '            raise PermissionDenied("the topic refused the publish")\n',
        "tests": CORPUS,
    },
    "review-kafka-empty-reading-type-guard-removed": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": (
            "            if _diag.is_a(remaining, int) and not _diag.is_a(remaining, bool) "
            "and remaining == 0:\n"
        ),
        "new": "            if remaining == 0:\n",
        "tests": ["tests/test_kafka_report_wait.py::test_only_the_int_zero_is_an_empty_reading"],
    },
    "review-kafka-empty-reading-bool-exclusion-removed": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": (
            "            if _diag.is_a(remaining, int) and not _diag.is_a(remaining, bool) "
            "and remaining == 0:\n"
        ),
        "new": "            if _diag.is_a(remaining, int) and remaining == 0:\n",
        "tests": ["tests/test_kafka_report_wait.py::test_only_the_int_zero_is_an_empty_reading"],
    },
    "review-kafka-one-blocking-flush-against-the-test": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": "            slice_ = max(0.0, min(deadline - now, _REPORT_SLICE))\n",
        "new": "            slice_ = max(0.0, deadline - now)\n",
        "tests": [
            "tests/test_kafka_report_wait.py::"
            "test_one_stuck_message_costs_later_rounds_one_slice_not_the_timeout"
        ],
    },
    "review-kafka-table-captures-the-counters-lock": {
        "file": "src/log_foundry/sinks/kafka.py",
        "old": "        self._counters = counters\n",
        "new": (
            "        self._counters = type("
            "'Captured', (), {'_counter_lock': counters._counter_lock})()\n"
        ),
        "tests": [
            "tests/test_kafka_report_wait.py::"
            "test_a_late_report_served_in_a_forked_child_never_takes_the_parents_lock"
        ],
    },
    "wait-at-close-pubsub-close-flag-only": {
        "file": "src/log_foundry/sinks/pubsub.py",
        "old": (
            "        for future in kept:\n"
            "            if not _waitable(future):\n"
            "                continue\n"
        ),
        "new": (
            "        for future in kept[:0]:\n"
            "            if not _waitable(future):\n"
            "                continue\n"
        ),
        "tests": [
            "tests/test_pubsub_round_wait.py::"
            "test_a_normal_exit_lands_every_publish_the_exit_drain_left_unconfirmed",
            "tests/test_pubsub_round_wait.py::"
            "test_a_close_over_futures_that_never_resolve_is_bounded_and_counts_nothing",
        ],
    },
    "execution-review-pubsub-pid-check-removed": {
        "file": "src/log_foundry/sinks/pubsub.py",
        "old": "        if self._kept_by != pid:\n",
        "new": "        if False:\n",
        "tests": [
            "tests/test_pubsub_round_wait.py::"
            "test_a_forked_childs_own_close_waits_on_nothing_its_parent_kept",
        ],
    },
    "execution-review-pubsub-keep-ignores-the-close": {
        "file": "src/log_foundry/sinks/pubsub.py",
        "old": "            if not self._closed:\n                self._unconfirmed = [",
        "new": "            if True:\n                self._unconfirmed = [",
        "tests": [
            "tests/test_pubsub_round_wait.py::test_a_round_settling_after_the_close_keeps_nothing",
        ],
    },
    "execution-review-pubsub-keep-never-prunes": {
        "file": "src/log_foundry/sinks/pubsub.py",
        "old": (
            "                self._unconfirmed = [f for f in self._kept_here() if _waitable(f)] + futures\n"
        ),
        "new": "                self._unconfirmed = self._kept_here() + futures\n",
        "tests": [
            "tests/test_pubsub_round_wait.py::"
            "test_each_keep_drops_the_futures_already_kept_that_have_resolved",
        ],
    },
}
"""Each re-plant: the file, the one edit, and what reads it."""

CHILD = r"""
import json, sys
import log_foundry
import conformance_backend as cb
import conformance_classes as cc
out = {"__file__": log_foundry.__file__, "__corpus__": cb.__file__}
for row in cc.rows():
    try:
        out[row.id] = list(cb.run_row(row).verdict)
    except Exception as err:
        out[row.id] = ["error:" + type(err).__name__]
print(json.dumps(out))
"""


def export(ref: str, into: pathlib.Path) -> None:
    """`git archive <ref>`, extracted into `into`."""
    into.mkdir(parents=True)
    archive = subprocess.run(
        ["git", "-C", str(ROOT), "archive", ref], capture_output=True, check=True
    )
    subprocess.run(["tar", "-x", "-C", str(into)], input=archive.stdout, check=True)


def env_for(tree: pathlib.Path, *first: pathlib.Path) -> dict[str, str]:
    """The child environment: `first`, then the tree's `src` and `tests`, on the path."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [*(str(p) for p in first), str(tree / "src"), str(tree / "tests")]
    )
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def imports_from(tree: pathlib.Path) -> str:
    """`log_foundry.__file__` in the tree's environment, refused unless inside the tree."""
    where = subprocess.run(
        [sys.executable, "-B", "-c", "import log_foundry; print(log_foundry.__file__)"],
        env=env_for(tree),
        cwd=str(tree),
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if not where.startswith(str(tree)):
        raise SystemExit(f"{tree} imports log_foundry from {where}")
    return where


def pytest_run(tree: pathlib.Path, tests: list[str]) -> str:
    """The tests' failures and summary, run serially in the tree."""
    done = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-n",
            "0",
            "-q",
            "-rf",
            *tests,
        ],
        env=env_for(tree),
        cwd=str(tree),
        capture_output=True,
    )
    text = done.stdout.decode("utf-8", "replace")
    keep = [
        ln
        for ln in text.splitlines()
        if ln.startswith(("FAILED", "ERROR")) or " passed" in ln or " failed" in ln
    ]
    return "\n".join(keep) + f"\n  exit {done.returncode}"


def docker_run(tree: pathlib.Path, tests: list[str]) -> str:
    """The tests run in `python:3.12-slim`, on Linux loopback."""
    script = (
        "pip install -q --root-user-action=ignore pytest >/dev/null 2>&1; "
        'PYTHONPATH=/w/src python -c "import log_foundry;print(log_foundry.__file__)"; '
        f"PYTHONPATH=/w/src python -m pytest -p no:cacheprovider -o addopts= -q -rf {' '.join(tests)} "
        "| grep -E '^(FAILED|ERROR)| passed| failed'"
    )
    done = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{tree}:/w",
            "-w",
            "/w",
            "-e",
            "PYTHONDONTWRITEBYTECODE=1",
            "python:3.12-slim",
            "sh",
            "-c",
            script,
        ],
        capture_output=True,
        text=True,
    )
    return done.stdout.strip() + (
        f"\n  stderr: {done.stderr.strip()[-400:]}" if done.returncode else ""
    )


def probe(tree: pathlib.Path, name: str) -> str:
    """One probe, run from its directory in the tree."""
    done = subprocess.run(
        [sys.executable, "-B", name],
        env=env_for(tree),
        cwd=str(tree / PROBES),
        capture_output=True,
        text=True,
        timeout=600,
    )
    return (done.stdout + done.stderr).strip()


def verdicts(tree: pathlib.Path, corpus: pathlib.Path) -> dict[str, list[str]]:
    """Every row's verdict, judged with `corpus`'s files against `tree`'s `src`."""
    done = subprocess.run(
        [sys.executable, "-B", "-c", CHILD],
        env=env_for(tree, corpus / "tests"),
        cwd=str(tree),
        capture_output=True,
        text=True,
    )
    if done.returncode:
        raise SystemExit(f"child failed:\n{done.stderr[-3000:]}")
    data: dict[str, list[str]] = json.loads(done.stdout.strip().splitlines()[-1])
    print(f"    log_foundry from {data.pop('__file__')}; corpus from {data.pop('__corpus__')}")
    return data


def compare(label: str, before: dict[str, list[str]], after: dict[str, list[str]]) -> None:
    """Prints the shared rows whose verdict differs, then the rows only one side has."""
    shared = sorted(set(before) & set(after))
    moved = [r for r in shared if before[r] != after[r]]
    print(f"  {label}: {len(shared)} shared rows, {len(moved)} moved")
    for row in moved:
        print(
            f"      {row}: {'+'.join(before[row]) or 'green'} -> {'+'.join(after[row]) or 'green'}"
        )
    for side, only in (("before", set(before) - set(after)), ("after", set(after) - set(before))):
        if only:
            print(f"    {len(only)} row(s) only {side}:")
        for row in sorted(only):
            verdict = (before if side == "before" else after)[row]
            print(f"      {row}: {'+'.join(verdict) or 'green'}")


def have(ref: str) -> bool:
    """Whether this clone holds the commit `ref`."""
    return (
        subprocess.run(
            ["git", "-C", str(ROOT), "cat-file", "-e", f"{ref}^{{commit}}"],
            capture_output=True,
        ).returncode
        == 0
    )


def main() -> int:
    """Runs everything asked for and prints it."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--docker", action="store_true")
    parser.add_argument("--only", nargs="*")
    args = parser.parse_args()
    head = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--short=8", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    print(
        f"SPEC-093/094 build results at HEAD {head}, Python {sys.version.split()[0]}, {sys.platform}"
    )
    with tempfile.TemporaryDirectory(prefix="spec093-094-build-") as scratch:
        work = pathlib.Path(scratch)
        base = work / "head"
        export("HEAD", base)
        wanted = args.only
        for name, plant in REPLANTS.items():
            if wanted and name not in wanted:
                continue
            tree = work / name
            shutil.copytree(base, tree)
            path = tree / str(plant["file"])
            text = path.read_text(encoding="utf-8")
            old = str(plant["old"])
            if text.count(old) != 1:
                raise SystemExit(
                    f"{name}: the edit matches {text.count(old)} times in {plant['file']}"
                )
            path.write_text(text.replace(old, str(plant["new"])), encoding="utf-8")
            print(f"\n== re-plant {name} ({plant['file']}); imports {imports_from(tree)}")
            if "tests" in plant:
                print(
                    "  macOS pytest:\n    "
                    + pytest_run(tree, list(plant["tests"])).replace("\n", "\n    ")
                )  # type: ignore[arg-type]
            if plant.get("docker") and args.docker:
                print(
                    "  Linux pytest:\n    "
                    + docker_run(tree, list(plant["tests"])).replace("\n", "\n    ")
                )  # type: ignore[arg-type]
            for script in plant.get("probe", []):  # type: ignore[attr-defined]
                print(f"  probe {script}:\n    " + probe(tree, script).replace("\n", "\n    "))
        if not wanted or "comparisons" in wanted:
            print("\n== verdict comparisons")
            if have(SETTLEMENT):
                settled, main_tree = work / "settlement", work / "main"
                export(SETTLEMENT, settled)
                export(MAIN, main_tree)
                print(f"  SPEC-093 FR-005 criterion 2, the settlement corpus, on base {MAIN}:")
                compare(
                    "main src -> settlement src",
                    verdicts(main_tree, settled),
                    verdicts(settled, settled),
                )
                print("  SPEC-093 FR-005 criterion 2, SPEC-092's delivered corpus:")
                compare(
                    "main src -> settlement src",
                    verdicts(main_tree, main_tree),
                    verdicts(settled, main_tree),
                )
            else:
                print(
                    f"  skipped: the settlement commit {SETTLEMENT} is not in this clone; it was "
                    f"a local commit of the SPEC-093/094 build on base {MAIN}, squashed away "
                    "before the merge, and its comparison is recorded in "
                    "spec093_094_build_results.txt"
                )
            onto = work / "onto"
            export(REBASED_ONTO, onto)
            print(f"  The rebase: {REBASED_ONTO}'s corpus and src -> HEAD's corpus and src:")
            compare(f"{REBASED_ONTO} -> HEAD", verdicts(onto, onto), verdicts(base, base))
        if not wanted or "probes" in wanted:
            print(f"\n== probes against HEAD; imports {imports_from(base)}")
            for script in (
                "probe_socket.py",
                "probe_misc.py",
                "probe_sqs.py",
                "probe_aws_more.py",
                "spec094_stuck_partition.py",
                "spec094_first_slice_and_pause.py",
            ):
                print(f"  {script}:\n    " + probe(base, script).replace("\n", "\n    "))
            if args.docker:
                done = subprocess.run(
                    [
                        "docker",
                        "run",
                        "--rm",
                        "-v",
                        f"{base}:/w",
                        "-w",
                        f"/w/{PROBES}",
                        "-e",
                        "PYTHONDONTWRITEBYTECODE=1",
                        "python:3.12-slim",
                        "sh",
                        "-c",
                        'PYTHONPATH=/w/src python -c "import log_foundry;print(log_foundry.__file__)"; '
                        "PYTHONPATH=/w/src python -B probe_socket.py",
                    ],
                    capture_output=True,
                    text=True,
                )
                print(
                    "  probe_socket.py in python:3.12-slim:\n    "
                    + done.stdout.strip().replace("\n", "\n    ")
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
