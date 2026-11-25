"""SPEC-099: the edits the soak's own process will make cannot break SPEC-099's tests.

Two edits are certain to come. A **clearing pull request** files a failed night: it may change only
``tests/soak_known.py`` and ``docs/architecture.md``, so if any SPEC-099 test judged the machinery
against the live table, the filing would fail CI and the PR queue, which waits for that very
filing, would never clear. And the spec that **closes the orphan late-write item** strikes it,
moves it under ``### Resolved`` and removes its row and ceiling, which should cost only the tests
whose job is that row. Each edit is applied to a copy of the tracked tree, and SPEC-099's test
modules are run against the copy in a subprocess.

Cost: one copy of the tracked tree and two nested sessions of SPEC-099's modules at ``-n 4``,
about half a minute in all on a 10-core machine; the copy is made once per session.
"""

from __future__ import annotations

import os
import pathlib
import re
import shutil
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

SPEC_099_TESTS = (
    "tests/test_soak_known.py",
    "tests/test_model_soak_workflow.py",
    "tests/test_model_oracles.py::test_the_unowed_repeat_rule",
    "tests/test_invariants_model.py::test_the_soak_is_wired",
    "tests/test_invariants_model.py::test_the_soak_records_the_worker_path",
    "tests/test_invariants_model.py::test_a_gating_failure_in_a_soak_item_is_never_excused",
    "tests/test_invariants_model.py::test_a_known_soak_item_is_an_expected_failure_and_anything_else_fails",
)
"""SPEC-099's test modules and items, this one excluded: it would run itself."""

LIVE_CONTENT_TESTS = frozenset(
    {
        "tests/test_soak_known.py::test_the_late_write_predicate[orphan-late-write]",
        "tests/test_soak_known.py::test_the_late_write_predicate[on-the-worker-thread]",
        "tests/test_soak_known.py::test_the_late_write_predicate[note-after-the-last-take]",
        "tests/test_soak_known.py::test_the_late_write_predicate[arm-after-it]",
        "tests/test_soak_known.py::test_the_late_write_predicate[two-late-writes-the-earlier-on-the-worker-thread]",
    }
)
"""The tests whose job is the orphan row's own predicate: closing its item removes them with it."""

NEW_LEAD = "A late write on the drain thread after its sink's last close, filed by a future night"
"""The open item a clearing pull request files in the future-edit check."""


def _copy_tree(dest: pathlib.Path) -> pathlib.Path:
    """Copies the tracked tree to ``dest`` and makes it a repository whose index lists it.

    Args:
      dest: An empty directory.

    Returns:
      The copy's root.

    Raises:
      subprocess.CalledProcessError: If git cannot list or index the tree.
    """
    listed = subprocess.run(
        ["git", "ls-files", "-z"],  # noqa: S607
        cwd=ROOT,
        capture_output=True,
        check=True,
        timeout=60,
    ).stdout.decode("utf-8")
    for path in filter(None, listed.split("\0")):
        source = ROOT / path
        if source.is_file():
            target = dest / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
    for args in (["git", "init", "-q"], ["git", "add", "-A"]):
        subprocess.run(args, cwd=dest, env=env, check=True, capture_output=True, timeout=120)  # noqa: S603
    return dest


def _edit(path: pathlib.Path, old: str, new: str) -> None:
    """Replaces one exact occurrence in a file, failing when it is not there exactly once.

    Args:
      path: The file.
      old: The text to replace.
      new: Its replacement.

    Returns:
      None.

    Raises:
      None.
    """
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, (path, old[:80])
    path.write_text(text.replace(old, new), encoding="utf-8")


def clearing_edit(root: pathlib.Path) -> None:
    """Applies what a clearing pull request may: one filing, one new open item, one new row.

    Args:
      root: A copy of the tree.

    Returns:
      None.

    Raises:
      None.
    """
    known = root / "tests" / "soak_known.py"
    _edit(
        known,
        "FILED_NIGHTS: dict[str, str] = {}",
        f'FILED_NIGHTS: dict[str, str] = {{\n    "45999999999-1": "{NEW_LEAD}",\n}}',
    )
    _edit(
        known,
        "KNOWN: tuple[Known, ...] = (ORPHAN_LATE_WRITE,)",
        "def worker_late_write(events, closes):\n"
        "    return bool(closes.at_return) and any(\n"
        '        isinstance(e, Write) and e.thread == "log-foundry-worker" for e in events\n'
        "    )\n\n\n"
        f'WORKER_LATE_WRITE = Known("worker-write-after-close", "{NEW_LEAD}", "late-write", '
        "worker_late_write)\n\n"
        "KNOWN: tuple[Known, ...] = (ORPHAN_LATE_WRITE, WORKER_LATE_WRITE)",
    )
    _edit(
        root / "docs" / "architecture.md",
        "\n### Open\n",
        f"\n### Open\n\n- **{NEW_LEAD}** (filed by the future-edit check).\n",
    )


def closing_edit(root: pathlib.Path) -> None:
    """Applies the orphan item's closure: struck and moved under Resolved, its row and ceiling gone.

    Args:
      root: A copy of the tree.

    Returns:
      None.

    Raises:
      None.
    """
    architecture = root / "docs" / "architecture.md"
    text = architecture.read_text(encoding="utf-8")
    start = text.index("- **An orphan log that passed its owed check before a `shutdown()`")
    end = text.index("\n- ", start + 1)
    bullet = "- ~~**" + text[start + len("- **") : end].replace("**", "**~~", 1)
    text = (text[:start] + text[end + 1 :]).replace(
        "\n### Resolved\n", "\n### Resolved\n\n" + bullet + "\n", 1
    )
    architecture.write_text(text, encoding="utf-8")
    known = root / "tests" / "soak_known.py"
    source = known.read_text(encoding="utf-8")
    source = re.sub(
        r"\ndef orphan_late_write\(.*?\n\n\nORPHAN_LATE_WRITE = Known\(.*?\n\)\n",
        "\n",
        source,
        flags=re.DOTALL,
    )
    source = source.replace(
        "KNOWN: tuple[Known, ...] = (ORPHAN_LATE_WRITE,)", "KNOWN: tuple[Known, ...] = ()"
    )
    assert "orphan_late_write" not in source and "ORPHAN_LATE_WRITE" not in source, source[:400]
    known.write_text(source, encoding="utf-8")
    ceilings = root / "tests" / "soak_ceilings.py"
    lines = ceilings.read_text(encoding="utf-8").splitlines(keepends=True)
    kept = [line for line in lines if not line.lstrip().startswith('"orphan-write-after-close":')]
    assert len(kept) == len(lines) - 1
    ceilings.write_text("".join(kept), encoding="utf-8")


def _run(root: pathlib.Path) -> tuple[int, set[str], str]:
    """Runs SPEC-099's tests in a copy of the tree.

    Args:
      root: The copy.

    Returns:
      pytest's exit status, the ids that failed or errored, and the output's tail.

    Raises:
      None.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("LOG_FOUNDRY_MODEL_")}
    env["GIT_CONFIG_GLOBAL"] = "/dev/null"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    done = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-n",
            "4",
            "-p",
            "no:cacheprovider",
            "-rfE",
            *SPEC_099_TESTS,
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    failed = set(re.findall(r"^(?:FAILED|ERROR) (\S+)", done.stdout, re.MULTILINE))
    return done.returncode, failed, done.stdout[-3000:] + done.stderr[-2000:]


@pytest.fixture(scope="module")
def tree_copy(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """One copy of the tracked tree for this module, which each test copies again to edit.

    Args:
      tmp_path_factory: pytest's factory.

    Returns:
      The copy's root.

    Raises:
      None.
    """
    return _copy_tree(tmp_path_factory.mktemp("tree"))


def test_a_clearing_pull_request_passes_spec_099s_tests(
    tree_copy: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """FR-006: one filing, one new open item and one new worker-thread row break no SPEC-099 test."""
    root = tmp_path / "clearing"
    shutil.copytree(tree_copy, root, symlinks=True)
    clearing_edit(root)
    status, failed, tail = _run(root)
    assert (status, failed) == (0, set()), tail


def test_closing_the_orphan_item_costs_only_its_own_rows_tests(
    tree_copy: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """FR-002: closing the orphan item fails exactly the tests whose job is its row, by name."""
    root = tmp_path / "closing"
    shutil.copytree(tree_copy, root, symlinks=True)
    closing_edit(root)
    status, failed, tail = _run(root)
    assert status == 1, tail
    assert failed == LIVE_CONTENT_TESTS, (sorted(failed ^ LIVE_CONTENT_TESTS), tail)
