"""SPEC-099: the edits the soak's own process will make cannot break SPEC-099's tests.

Two edits are certain to come. A **clearing pull request** files a failed night: it may change only
``tests/soak_known.py`` and ``docs/architecture.md``, so if any SPEC-099 test judged the machinery
against the live table, or matched the clearing file's literal text, the filing would fail CI and
the PR queue, which waits for that very filing, would never clear. And the spec that **closes the
orphan late-write item** strikes it, moves it under ``### Resolved`` and removes its row and
ceiling, which should cost only the tests whose job is that row. Each edit is applied to a copy of
the tracked tree, and SPEC-099's test modules are run against the copy in a subprocess.

Every edit here locates its target structurally, the dict and the tuple through the AST, so it
applies to a tree that already carries filings and rows as well as to today's, and every edit
asserts it changed something. The clearing scenario files two nights, one after the other, and
the nested run includes this module, behind :data:`NESTED`, so a copy that already carries a
filing runs these tests too; under :data:`NESTED` a test applies its edits but starts no further
session, which is what ends the recursion.

Cost: one copy of the tracked tree and two nested sessions of SPEC-099's modules at ``-n 4``, each
of which copies the tree again for this module's own nested tests: about half a minute in all on a
10-core machine.
"""

from __future__ import annotations

import ast
import os
import pathlib
import re
import shutil
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

NESTED = "LOG_FOUNDRY_SOAK_FUTURE_EDITS_NESTED"
"""Set in the nested session: this module's tests there edit a copy and start no session."""

SPEC_099_TESTS = (
    "tests/test_soak_known.py",
    "tests/test_model_soak_workflow.py",
    "tests/test_soak_future_edits.py",
    "tests/test_model_oracles.py::test_the_unowed_repeat_rule",
    "tests/test_invariants_model.py::test_the_soak_is_wired",
    "tests/test_invariants_model.py::test_the_soak_records_the_worker_path",
    "tests/test_invariants_model.py::test_a_gating_failure_in_a_soak_item_is_never_excused",
    "tests/test_invariants_model.py::test_a_known_soak_item_is_an_expected_failure_and_anything_else_fails",
)
"""SPEC-099's test modules and items, this one included behind :data:`NESTED`."""

ORPHAN_LEAD = "- **An orphan log that passed its owed check before a `shutdown()` took its sink"
"""How the orphan late-write item's bullet starts while it is open."""

LIVE_CONTENT_TESTS = frozenset(
    {
        "tests/test_soak_known.py::test_the_late_write_predicate[orphan-late-write]",
        "tests/test_soak_known.py::test_the_late_write_predicate[on-the-worker-thread]",
        "tests/test_soak_known.py::test_the_late_write_predicate[note-after-the-last-take]",
        "tests/test_soak_known.py::test_the_late_write_predicate[arm-after-it]",
        "tests/test_soak_known.py::test_the_late_write_predicate[two-late-writes-the-earlier-on-the-worker-thread]",
        "tests/test_soak_future_edits.py::test_closing_the_orphan_item_costs_only_its_own_rows_tests",
    }
)
"""The tests whose job is the open orphan item and its row: closing it fails exactly these.

The five are the row's own predicate corpus. The sixth is this module's closing test, whose job is
the live orphan item, and which in a tree where that item is already closed reports so rather than
closing it twice.
"""

FILINGS = (
    (
        "45999999991-1",
        "A late write on the drain thread after its sink's last close, filed night one",
    ),
    ("45999999992-2", "A second shape the soak met, filed night two"),
)
"""Two nights the clearing scenario files, one after the other, with distinct ids and leads."""


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


def _statement(source: str, name: str) -> ast.stmt:
    """Returns the one top-level statement that assigns or defines ``name``.

    Args:
      source: A module's text.
      name: The name.

    Returns:
      The statement.

    Raises:
      None.
    """
    found = []
    for node in ast.parse(source).body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == name for t in targets):
                found.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == name:
            found.append(node)
    assert len(found) == 1, (name, len(found))
    return found[0]


def _replace_statement(source: str, node: ast.stmt, new: str) -> str:
    """Returns ``source`` with one top-level statement's lines replaced, failing if nothing changed.

    Args:
      source: A module's text.
      node: The statement, from :func:`_statement`.
      new: Its replacement, whole lines without the final newline.

    Returns:
      The new text.

    Raises:
      None.
    """
    lines = source.splitlines(keepends=True)
    assert node.end_lineno is not None
    replaced = "".join(lines[: node.lineno - 1]) + new + "\n" + "".join(lines[node.end_lineno :])
    assert replaced != source, "the edit changed nothing"
    return replaced


def _known_elements(source: str) -> list[str]:
    """Returns the source of each element of the ``KNOWN`` tuple, however many there are.

    Args:
      source: ``tests/soak_known.py``'s text.

    Returns:
      Each element's source.

    Raises:
      None.
    """
    node = _statement(source, "KNOWN")
    value = node.value
    assert isinstance(value, ast.Tuple), "KNOWN is not a tuple literal"
    elements = [ast.get_source_segment(source, elt) for elt in value.elts]
    assert all(elements), elements
    return [str(element) for element in elements]


def _write_known(source: str, elements: list[str]) -> str:
    """Returns ``source`` with ``KNOWN`` rewritten to hold exactly ``elements``.

    Args:
      source: ``tests/soak_known.py``'s text.
      elements: The rows' source, in order.

    Returns:
      The new text.

    Raises:
      None.
    """
    inside = ", ".join(elements) + ("," if len(elements) == 1 else "")
    return _replace_statement(
        source, _statement(source, "KNOWN"), f"KNOWN: tuple[Known, ...] = ({inside})"
    )


def clearing_edit(root: pathlib.Path, night: str, lead: str) -> None:
    """Applies what a clearing pull request may: one filing, one new open item, one new row.

    Each target is found structurally: the ``FILED_NIGHTS`` dict whether empty or populated, and
    the ``KNOWN`` tuple whatever rows it already holds. The row's predicate reads a late write on
    the drain thread, the shape a worker-path filing would add.

    Args:
      root: A copy of the tree.
      night: The night's ``<run id>-<attempt>``.
      lead: The new open item's lead.

    Returns:
      None.

    Raises:
      None.
    """
    known = root / "tests" / "soak_known.py"
    source = known.read_text(encoding="utf-8")
    filed = _statement(source, "FILED_NIGHTS")
    assert isinstance(filed, ast.AnnAssign) and isinstance(filed.value, ast.Dict)
    entries = [
        f"    {ast.get_source_segment(source, k)}: {ast.get_source_segment(source, v)},"
        for k, v in zip(filed.value.keys, filed.value.values, strict=True)
    ]
    entries.append(f'    "{night}": "{lead}",')
    body = "\n".join(entries)
    source = _replace_statement(source, filed, f"FILED_NIGHTS: dict[str, str] = {{\n{body}\n}}")
    suffix = night.replace("-", "_")
    rows = _known_elements(source)
    source = _replace_statement(
        source,
        _statement(source, "KNOWN"),
        f"def worker_late_write_{suffix}(events, closes):\n"
        "    return bool(closes.at_return) and any(\n"
        '        isinstance(e, Write) and e.thread == "log-foundry-worker" for e in events\n'
        "    )\n\n\n"
        f'ROW_{suffix} = Known("worker-write-{suffix}", "{lead}", "late-write", '
        f"worker_late_write_{suffix})\n\n"
        f"KNOWN: tuple[Known, ...] = ({', '.join([*rows, f'ROW_{suffix}'])},)",
    )
    known.write_text(source, encoding="utf-8")
    architecture = root / "docs" / "architecture.md"
    text = architecture.read_text(encoding="utf-8")
    assert text.count("\n### Open\n") == 1
    architecture.write_text(
        text.replace("\n### Open\n", f"\n### Open\n\n- **{lead}** (filed night {night}).\n"),
        encoding="utf-8",
    )


def closing_edit(root: pathlib.Path) -> None:
    """Applies the orphan item's closure: struck and moved under Resolved, its row and ceiling gone.

    The row, its predicate and its ceiling are found structurally, so the closure applies whatever
    other rows and filings the tree has gathered; each removal asserts it removed something.

    Args:
      root: A copy of the tree.

    Returns:
      None.

    Raises:
      None.
    """
    architecture = root / "docs" / "architecture.md"
    text = architecture.read_text(encoding="utf-8")
    assert text.count(ORPHAN_LEAD) == 1, "the orphan item is not open here"
    start = text.index(ORPHAN_LEAD)
    end = text.index("\n- ", start + 1)
    bullet = "- ~~**" + text[start + len("- **") : end].replace("**", "**~~", 1)
    moved = (text[:start] + text[end + 1 :]).replace(
        "\n### Resolved\n", "\n### Resolved\n\n" + bullet + "\n", 1
    )
    assert moved != text and moved.count(ORPHAN_LEAD) == 0
    architecture.write_text(moved, encoding="utf-8")
    known = root / "tests" / "soak_known.py"
    source = known.read_text(encoding="utf-8")
    rows = _known_elements(source)
    assert "ORPHAN_LATE_WRITE" in rows, rows
    source = _write_known(source, [row for row in rows if row != "ORPHAN_LATE_WRITE"])
    for name in ("ORPHAN_LATE_WRITE", "orphan_late_write"):
        source = _replace_statement(source, _statement(source, name), "")
    assert "orphan_late_write" not in source and "ORPHAN_LATE_WRITE" not in source
    known.write_text(source, encoding="utf-8")
    ceilings = root / "tests" / "soak_ceilings.py"
    lines = ceilings.read_text(encoding="utf-8").splitlines(keepends=True)
    kept = [line for line in lines if not re.match(r'\s*"orphan-write-after-close":', line)]
    assert len(kept) == len(lines) - 1, "no orphan ceiling to remove"
    ceilings.write_text("".join(kept), encoding="utf-8")


def _run(root: pathlib.Path) -> tuple[int, set[str], str]:
    """Runs SPEC-099's tests in a copy of the tree, this module among them behind :data:`NESTED`.

    Args:
      root: The copy.

    Returns:
      pytest's exit status, the ids that failed or errored, and the output's tail.

    Raises:
      None.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("LOG_FOUNDRY_MODEL_")}
    env.update({NESTED: "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"})
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
    """FR-006: two filings in turn, each with its open item and a worker-thread row, break no test.

    No test, that is, beyond those already failing for being about a live item that has closed:
    none while the orphan item is open, :data:`LIVE_CONTENT_TESTS` once it is closed.

    Two, so the second lands on a tree the first already filed; in the nested session this test
    runs on that tree too, and files two more under the next free run ids.
    """
    root = tmp_path / "clearing"
    shutil.copytree(tree_copy, root, symlinks=True)
    known = root / "tests" / "soak_known.py"
    filings = []
    for night, lead in FILINGS:
        run_id, attempt = night.split("-")
        while f'"{run_id}-{attempt}"' in known.read_text(encoding="utf-8"):
            run_id = str(int(run_id) + 10)
        filings.append((f"{run_id}-{attempt}", f"{lead}, as {run_id}"))
        clearing_edit(root, *filings[-1])
    filed = known.read_text(encoding="utf-8")
    assert all(f'    "{night}": "{lead}",' in filed for night, lead in filings), filed
    if os.environ.get(NESTED):
        return
    status, failed, tail = _run(root)
    expected = set() if _orphan_open(tree_copy) else set(LIVE_CONTENT_TESTS)
    assert (status, failed) == (1 if expected else 0, expected), (sorted(failed ^ expected), tail)


def _orphan_open(root: pathlib.Path) -> bool:
    """Returns whether the orphan late-write item is still open in a tree.

    Once it is closed, its row's corpus and the closing test fail in that tree until the closing
    spec removes them, whatever else changes; so the filing scenario expects exactly those then.

    Args:
      root: A tree.

    Returns:
      Whether its bullet is still under ``### Open``.

    Raises:
      None.
    """
    return ORPHAN_LEAD in (root / "docs" / "architecture.md").read_text(encoding="utf-8")


def test_closing_the_orphan_item_costs_only_its_own_rows_tests(
    tree_copy: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """FR-002: closing the orphan item fails exactly :data:`LIVE_CONTENT_TESTS`, by name.

    Its job is the live orphan item: in a tree where that item is already closed there is nothing
    to close, and the test fails saying so, which is one of the names the closure expects.
    """
    if not _orphan_open(tree_copy):
        pytest.fail("the orphan item is closed in this tree: remove this test with it")
    root = tmp_path / "closing"
    shutil.copytree(tree_copy, root, symlinks=True)
    closing_edit(root)
    if os.environ.get(NESTED):
        return
    status, failed, tail = _run(root)
    assert status == 1, tail
    assert failed == LIVE_CONTENT_TESTS, (sorted(failed ^ LIVE_CONTENT_TESTS), tail)
