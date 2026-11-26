"""SPEC-099 FR-002, FR-003, FR-004: the nightly soak's workflow, seeds and report cannot rot unseen.

``.github/workflows/model-soak.yml`` is read as text, as ``tests/test_python_version_roster.py``
reads the workflows, because no YAML parser is a dependency. Every check is a function from the
workflow's text to findings, so each has a planted row here asserting it fires, and the live file
asserting it is silent: this is a gate, and a gate's check that goes quiet is invisible. The seeds
rule and the report are held the same way, by rows asserting the exit status and the reason.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import pathlib
import re
import subprocess
import sys
import tomllib
import tracemalloc
from typing import TYPE_CHECKING

import pytest

import soak_ceilings
import soak_known
from model_oracles import SEEDS_VARIABLE, SOAK_VARIABLE
from soak_known import Known
from test_model_oracles import open_item_leads

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = pathlib.Path(__file__).resolve().parent.parent
TESTS = ROOT / "tests"
WORKFLOW = ROOT / ".github" / "workflows" / "model-soak.yml"
TEXT = WORKFLOW.read_text(encoding="utf-8")

_SPEC = importlib.util.spec_from_file_location(
    "model_soak_report", ROOT / "scripts" / "model-soak-report.py"
)
assert _SPEC is not None and _SPEC.loader is not None
report_mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(report_mod)

ROOTS = ("test_invariants_model", "conftest")
"""The two modules whose imports under ``tests/`` the pull-request path filter must cover."""

FIXED_PATHS = (
    ".github/workflows/model-soak.yml",
    ".github/actions/install-poetry/**",
    "scripts/model-soak-report.py",
    "tests/soak_known.py",
    "tests/soak_verdict.py",
    "tests/soak_ceilings.py",
    "pyproject.toml",
    "poetry.lock",
)
"""The paths beside the derived modules that define the soak or install what it runs."""


def derived_modules(tests: pathlib.Path, roots: tuple[str, ...] = ROOTS) -> set[str]:
    """Returns every module directly under ``tests/`` that the roots import, directly or not.

    Read from the AST: every ``import`` and ``from ... import`` with no leading dot, anywhere in a
    module, whose top-level name is a module directly under ``tests/``, followed through the
    modules it reaches. The roots are included.

    Args:
      tests: The ``tests/`` directory.
      roots: The modules to start from.

    Returns:
      The module names.

    Raises:
      SyntaxError: If a module does not parse.
    """
    local = {path.stem for path in tests.glob("*.py")}
    seen: set[str] = set()
    todo = list(roots)
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        tree = ast.parse((tests / f"{name}.py").read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                tops = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                tops = [node.module.split(".")[0]]
            else:
                continue
            todo.extend(top for top in tops if top in local and top not in seen)
    return seen


def _lines(text: str, pattern: str) -> list[str]:
    """Returns the lines of ``text`` that ``pattern`` matches whole.

    Args:
      text: The workflow's text.
      pattern: A regular expression for one whole line.

    Returns:
      The matching lines.

    Raises:
      None.
    """
    return [line for line in text.splitlines() if re.fullmatch(pattern, line)]


def check_cron(text: str) -> list[str]:
    """(1) The nightly schedule, off the hour."""
    found = re.search(r'^  schedule:\n    - cron: "37 9 \* \* \*"$', text, re.MULTILINE)
    return [] if found else ['no `schedule: - cron: "37 9 * * *"`']


def check_dispatch(text: str) -> list[str]:
    """(2) A dispatch with its one optional ``seeds`` input."""
    found = re.search(r"^  workflow_dispatch:\n    inputs:\n      seeds:$", text, re.MULTILINE)
    return [] if found else ["no `workflow_dispatch` with a `seeds` input"]


def check_switch(text: str) -> list[str]:
    """(3) The soak switch, exactly ``1``."""
    return (
        []
        if _lines(text, r'          LOG_FOUNDRY_MODEL_SOAK: "1"')
        else ['no `LOG_FOUNDRY_MODEL_SOAK: "1"`']
    )


def check_run_number(text: str) -> list[str]:
    """(4) The run number, which a re-run keeps, chooses the window."""
    wanted = r"          RUN_NUMBER: \$\{\{ github\.run_number \}\}"
    return [] if _lines(text, wanted) else ["no `RUN_NUMBER: ${{ github.run_number }}`"]


def check_expressions(text: str) -> list[str]:
    """(5) Every ``${{ }}`` is the concurrency group or a whole ``env`` value, never in ``run:``.

    Read over every line of the file, not the ``run:`` line alone: an expression on a later line
    of a ``run: |`` block is inside ``run:`` too.
    """
    group = r"  group: model-soak-\$\{\{ [^}]+ \}\}"
    env_value = r"          [A-Z][A-Z0-9_]*: \$\{\{ [^}]+ \}\}"
    return [
        f"an expression outside the group and env values: {line.strip()!r}"
        for line in text.splitlines()
        if "${{" in line and not (re.fullmatch(group, line) or re.fullmatch(env_value, line))
    ]


def _steps(text: str) -> list[str]:
    """Returns each step of the job, as its text.

    Args:
      text: The workflow's text.

    Returns:
      The steps, in order.

    Raises:
      None.
    """
    return text.split("\n    steps:\n", 1)[-1].split("\n      - ")


def check_report_step(text: str) -> list[str]:
    """(6) The report runs whatever happened, over ``soak.xml``, with the seeds, to the summary."""
    steps = [step for step in _steps(text) if "model-soak-report.py report" in step]
    if len(steps) != 1:
        return [f"{len(steps)} report steps, wanted 1"]
    step = steps[0]
    wanted = (
        "if: always()",
        'report soak.xml "$SEEDS" --summary "$GITHUB_STEP_SUMMARY"',
        "SEEDS: ${{ steps.seeds.outputs.seeds }}",
    )
    findings = [f"the report step lacks {part!r}" for part in wanted if part not in step]
    if "--junitxml=soak.xml" not in text:
        findings.append("the soak step writes no `--junitxml=soak.xml`")
    return findings


def check_group_prefix(text: str) -> list[str]:
    """(7) The concurrency group is prefixed, since groups are repository-wide."""
    return (
        [] if _lines(text, r"  group: model-soak-\$\{\{.*") else ["no `model-soak-` group prefix"]
    )


def check_permissions(text: str) -> list[str]:
    """(8) Read-only: ``contents: read`` at the top, and nothing that writes anywhere."""
    findings = (
        []
        if re.search(r"^permissions:\n  contents: read\n", text, re.MULTILINE)
        else ["no top-level `permissions: contents: read`"]
    )
    findings += [
        f"a scope that writes: {line.strip()!r}"
        for line in _lines(text, r".*:\s*write(-all)?\s*(#.*)?")
    ]
    return findings


def check_credentials(text: str) -> list[str]:
    """(9) Every checkout leaves no token on disk."""
    checkouts = text.count("uses: actions/checkout@")
    kept = len(_lines(text, r"\s+persist-credentials: false(\s+#.*)?"))
    return (
        []
        if checkouts and kept == checkouts
        else [f"{checkouts} checkouts, {kept} without credentials"]
    )


def check_pins(text: str) -> list[str]:
    """(10) Every ``uses:`` is local or pinned to a 40-character SHA with its ``# vX.Y.Z``."""
    findings = []
    for line in _lines(text, r"\s+(- )?uses: .*"):
        ref = line.split("uses: ", 1)[1]
        if ref.startswith("./"):
            continue
        if not re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40} # v\d+\.\d+\.\d+", ref):
            findings.append(f"not pinned with its version: {ref!r}")
    return findings


def check_python(text: str) -> list[str]:
    """(11) The floor, ``"3.12"``, and nothing else."""
    pins = _lines(text, r"\s+python-version:.*")
    if not pins or any(line.strip() != 'python-version: "3.12"' for line in pins):
        return [f'not exactly `python-version: "3.12"`: {pins}']
    return []


def filter_paths(text: str) -> list[str]:
    """Returns the ``pull_request`` path filter's entries, unquoted.

    Args:
      text: The workflow's text.

    Returns:
      The paths.

    Raises:
      None.
    """
    block = text.split("\n  pull_request:\n    paths:\n", 1)[-1]
    paths = []
    for line in block.splitlines():
        entry = re.fullmatch(r'      - "([^"]+)"', line)
        if entry is None:
            break
        paths.append(entry[1])
    return paths


def check_path_filter(text: str) -> list[str]:
    """(12) The filter names every module the soak imports from ``tests/``, and the fixed paths."""
    named = set(filter_paths(text))
    wanted = [*FIXED_PATHS, *(f"tests/{name}.py" for name in sorted(derived_modules(TESTS)))]
    return [f"the pull_request filter does not name {path}" for path in wanted if path not in named]


def selection(text: str) -> list[str]:
    """Returns the ids the workflow's own pytest arguments collect, at seeds 0 to 1.

    Args:
      text: The workflow's text.

    Returns:
      The collected ids, after the file name.

    Raises:
      None.
    """
    command = re.search(r'run: poetry run pytest (tests/\S+) -k "([^"]+)"', text)
    if command is None:
        return []
    env = {k: v for k, v in os.environ.items() if not k.startswith("LOG_FOUNDRY_MODEL_")}
    env.update({SOAK_VARIABLE: "1", SEEDS_VARIABLE: "0-1"})
    out = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            command[1],
            "-k",
            command[2],
            "--collect-only",
            "-q",
            "-n",
            "0",
            "-p",
            "no:cacheprovider",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    ).stdout
    return [line.split("::", 1)[1] for line in out.splitlines() if "::" in line]


def check_selection(text: str) -> list[str]:
    """(13) The selection is exactly 6 soak items and the eight of the wiring set at seeds 0-1."""
    ids = selection(text)
    soaked = [item for item in ids if item.startswith(report_mod.SOAK)]
    rest = sorted(set(ids) - set(soaked))
    if len(soaked) != 6 or rest != sorted(report_mod.WIRING):
        return [f"the selection collects {len(soaked)} soak items and {rest}"]
    return []


def check_timeout(text: str) -> list[str]:
    """(15) The job's own timeout, which ``MAX_SEEDS`` is derived against."""
    return [] if _lines(text, r"    timeout-minutes: 30") else ["no `timeout-minutes: 30`"]


QUEUE = (ROOT / "scripts" / "pr-queue" / "queue.sh").read_text(encoding="utf-8")


def check_queue_names(text: str, queue: str = QUEUE, file_name: str = WORKFLOW.name) -> list[str]:
    """(16) The workflow's ``name:`` and file name are the ones the PR queue reads it by.

    ``scripts/pr-queue/queue.sh`` leaves runs named ``SOAK_WORKFLOW`` out of the head's reading
    and lists the nights by ``SOAK_FILE``. Renaming ``name: Model soak`` alone would put the
    soak's runs back into the head's reading, so a red dispatch would read ``IS RED`` with
    nothing to say why; renaming the file makes the queue's list fail, which waits, loudly.

    Args:
      text: The workflow's text.
      queue: ``queue.sh``'s text.
      file_name: The workflow's file name.

    Returns:
      One finding per name that disagrees.

    Raises:
      None.
    """
    named = re.search(r"^name: (.+)$", text, re.MULTILINE)
    soak_workflow = re.search(r'^SOAK_WORKFLOW="([^"]+)"$', queue, re.MULTILINE)
    soak_file = re.search(r'^SOAK_FILE="([^"]+)"$', queue, re.MULTILINE)
    findings = []
    if named is None or soak_workflow is None or named[1] != soak_workflow[1]:
        findings.append(f"the workflow's name {named and named[1]!r} is not SOAK_WORKFLOW")
    if soak_file is None or soak_file[1] != file_name:
        findings.append(f"the workflow's file {file_name!r} is not SOAK_FILE")
    return findings


CHECKS: dict[str, Callable[[str], list[str]]] = {
    "cron": check_cron,
    "dispatch": check_dispatch,
    "switch": check_switch,
    "run-number": check_run_number,
    "expressions": check_expressions,
    "report-step": check_report_step,
    "group-prefix": check_group_prefix,
    "permissions": check_permissions,
    "credentials": check_credentials,
    "pins": check_pins,
    "python": check_python,
    "path-filter": check_path_filter,
    "selection": check_selection,
    "timeout": check_timeout,
    "queue-names": check_queue_names,
}
"""The workflow checks over its text; check 14, the seeds rule, is held by its own tests."""


@pytest.mark.parametrize("name", sorted(CHECKS))
def test_the_workflow_holds(name: str) -> None:
    """FR-003, FR-004: every check is silent over the live workflow."""
    assert CHECKS[name](TEXT) == []


_KEY = "test_soak_the_close_count_under_concurrency or test_the_soak_is_wired or test_the_soak_records_the_worker_path or test_a_name_the_soak_cannot_wrap_fails_it"
"""The workflow's ``-k`` as written."""


@pytest.mark.parametrize(
    ("name", "old", "new", "says"),
    [
        ("cron", '"37 9 * * *"', '"0 9 * * *"', "cron"),
        ("dispatch", "  workflow_dispatch:\n", "  workflow_run:\n", "workflow_dispatch"),
        (
            "switch",
            'LOG_FOUNDRY_MODEL_SOAK: "1"',
            'LOG_FOUNDRY_MODEL_SOAK: "0"',
            "LOG_FOUNDRY_MODEL_SOAK",
        ),
        (
            "run-number",
            "RUN_NUMBER: ${{ github.run_number }}",
            "RUN_NUMBER: ${{ github.run_id }}",
            "run_number",
        ),
        ("expressions", '--asked="$ASKED"', '--asked="${{ inputs.seeds }}"', "outside the group"),
        ("report-step", "        if: always()\n", "", "if: always()"),
        ("group-prefix", "group: model-soak-${{", "group: ci-${{", "prefix"),
        ("permissions", "  contents: read\n", "  contents: write\n", "write"),
        (
            "credentials",
            "persist-credentials: false",
            "persist-credentials: true",
            "without credentials",
        ),
        ("pins", "@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1", "@v7", "not pinned"),
        ("python", 'python-version: "3.12"', 'python-version: "3.13"', "3.12"),
        ("path-filter", '      - "tests/model_sinks.py"\n', "", "tests/model_sinks.py"),
        ("path-filter", '      - "tests/sink_rigs.py"\n', "", "tests/sink_rigs.py"),
        ("path-filter", '      - "tests/soak_verdict.py"\n', "", "tests/soak_verdict.py"),
        (
            "selection",
            f'-k "{_KEY}"',
            '-k "test_soak_the_close_count_under_concurrency"',
            "6 soak items and []",
        ),
        ("timeout", "timeout-minutes: 30", "timeout-minutes: 60", "timeout-minutes"),
        ("queue-names", "name: Model soak\n", "name: Model Soak\n", "is not SOAK_WORKFLOW"),
    ],
    ids=[
        "cron",
        "dispatch",
        "switch",
        "run-number",
        "expressions",
        "report-step",
        "group-prefix",
        "permissions",
        "credentials",
        "pins",
        "python",
        "filter-drops-a-direct-import",
        "filter-drops-a-module-reached-only-indirectly",
        "filter-drops-soak-verdict",
        "selection-narrowed",
        "timeout",
        "workflow-renamed",
    ],
)
def test_each_workflow_check_can_actually_fail(name: str, old: str, new: str, says: str) -> None:
    """Each check fires, naming what it found, when one planted edit breaks what it holds.

    The edit must land exactly once, so a row cannot plant nothing and pass. The two path-filter
    rows drop ``tests/model_sinks.py``, which the model file imports directly, and
    ``tests/sink_rigs.py``, which neither root imports directly.
    """
    assert TEXT.count(old) == 1, old
    findings = CHECKS[name](TEXT.replace(old, new))
    assert findings, f"{name} is silent over the planted edit"
    assert any(says in finding for finding in findings), findings


def test_the_queue_names_check_reads_the_queues_side_too() -> None:
    """(16) A ``SOAK_FILE`` or ``SOAK_WORKFLOW`` changed in ``queue.sh`` alone is named too."""
    assert QUEUE.count('SOAK_FILE="model-soak.yml"') == 1
    renamed = QUEUE.replace('SOAK_FILE="model-soak.yml"', 'SOAK_FILE="soak.yml"')
    assert check_queue_names(TEXT, renamed) == [
        "the workflow's file 'model-soak.yml' is not SOAK_FILE"
    ]
    assert QUEUE.count('SOAK_WORKFLOW="Model soak"') == 1
    renamed = QUEUE.replace('SOAK_WORKFLOW="Model soak"', 'SOAK_WORKFLOW="Soak"')
    assert check_queue_names(TEXT, renamed) == [
        "the workflow's name 'Model soak' is not SOAK_WORKFLOW"
    ]


def test_the_derived_set_follows_imports_through_the_modules_they_reach() -> None:
    """FR-004: the set reaches ``sink_rigs``, which neither root imports directly."""
    found = derived_modules(TESTS)
    assert {
        "test_invariants_model",
        "conftest",
        "soak_verdict",
        "soak_known",
        "sink_rigs",
    } <= found, found


@pytest.mark.parametrize(
    ("run_number", "window"),
    [
        (1, "5000-9999"),
        (2, "10000-14999"),
        (29, "145000-149999"),
        (30, "0-4999"),
        (31, "5000-9999"),
    ],
)
def test_the_seeds_are_the_run_numbers_window(run_number: int, window: str) -> None:
    """(14) FR-003: the window is the run number's, nothing else, wrapping after 30 runs.

    Several run numbers, because a rule reading anything else could match one of them by chance.
    """
    assert report_mod.seeds("schedule", "", run_number) == window
    assert report_mod.seeds("workflow_dispatch", "", run_number) == window


def test_a_night_soaks_no_more_than_a_dispatch_may_ask_for() -> None:
    """FR-003: ``WINDOW`` is held to the time bound ``MAX_SEEDS`` is derived against.

    Both rest on one derivation: the slowest cost per seed measured on ``ubuntu-latest``, and a
    third again, inside the 1,020 s the job leaves the soak. A window raised past the bound would
    let a slow night with a hung item be killed by the job's timeout with no report, and a killed
    night is a failed night, which stops the PR queue until it is filed.
    """
    assert report_mod.WINDOW <= report_mod.MAX_SEEDS, (report_mod.WINDOW, report_mod.MAX_SEEDS)


def test_the_hang_guard_leaves_the_soak_the_budget_its_bound_rests_on() -> None:
    """FR-003: the job's 1,800 s less the install and ``faulthandler_timeout`` leaves 1,020 s.

    ``MAX_SEEDS`` is derived against 1,020 s: 1,800 s of ``timeout-minutes: 30``, less 180 s for the
    install and the hung-item guard, which is ``faulthandler_timeout`` in ``pyproject.toml``. A
    guard raised for a slow test elsewhere would shrink that budget with the suite still green, so
    this reads the real value rather than the 600 s the derivation's prose names.
    """
    config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    guard = int(config["tool"]["pytest"]["ini_options"]["faulthandler_timeout"])
    assert 30 * 60 - 180 - guard >= 1020, guard


def test_the_dispatch_input_names_the_bound_the_script_enforces() -> None:
    """FR-003: the ``seeds`` input's description states ``MAX_SEEDS``, the bound ``seeds`` refuses past.

    The description is a second copy of the number, written by hand; a re-derivation that moved only
    the constant would leave the form a dispatcher reads promising seeds the script then refuses.
    """
    stated = f"at most {report_mod.MAX_SEEDS:,} seeds;"
    assert stated in WORKFLOW.read_text(encoding="utf-8"), stated


@pytest.mark.parametrize(
    ("event", "asked", "chosen"),
    [
        ("pull_request", "", "0-99"),
        ("workflow_dispatch", "1,\n2, 4", "1-2,4"),
        ("workflow_dispatch", " 5 ", "5"),
        ("workflow_dispatch", "4,4,3-4", "3-4"),
        ("workflow_dispatch", "0-4999", "0-4999"),
    ],
    ids=["pull-request", "a-newline-normalised", "spaces", "overlap", "exactly-the-bound"],
)
def test_the_seeds_rule(event: str, asked: str, chosen: str) -> None:
    """(14) FR-003: a pull request soaks 0-99, and a dispatch's seeds come back normalised."""
    assert report_mod.seeds(event, asked, 7) == chosen


def _cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Runs the report script's command line.

    Args:
      *args: Its arguments.

    Returns:
      The finished process.

    Raises:
      None.
    """
    return subprocess.run(  # noqa: S603
        [sys.executable, str(ROOT / "scripts" / "model-soak-report.py"), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.parametrize(
    ("asked", "status", "stdout", "stderr"),
    [
        ("0-5000", 1, "", "at most 5000"),
        ("0-3\nfoo=bar", 1, "", "is refused"),
        ("1;id", 1, "", "is refused"),
        ("$(id)", 1, "", "is refused"),
        ("-1", 1, "", "is refused"),
        ("0-4999", 0, "0-4999\n", ""),
    ],
    ids=[
        "one-over-the-bound",
        "a-second-line",
        "a-semicolon",
        "a-substitution",
        "a-leading-hyphen",
        "the-bound",
    ],
)
def test_the_seeds_command_refuses_with_exit_1_and_no_output(
    asked: str, status: int, stdout: str, stderr: str
) -> None:
    """FR-003: a refused dispatch exits 1 and writes nothing to stdout, so nothing to the output file."""
    done = _cli("seeds", "--event=workflow_dispatch", f"--asked={asked}", "--run-number=3")
    assert (done.returncode, done.stdout) == (status, stdout), done
    assert stderr in done.stderr, done.stderr


def test_a_dispatch_is_bounded_before_its_seeds_are_built() -> None:
    """FR-003: a range far past the bound is refused without building it, under 1 MB traced."""
    tracemalloc.start()
    try:
        with pytest.raises(ValueError, match="at most"):
            report_mod.seeds("workflow_dispatch", "0-300000", 2)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 1_000_000, peak


_SHAPE = "fixture-late-write"
_CEILING = 4
_FIXTURE_KNOWN = (Known(_SHAPE, "A fixture lead", "late-write", lambda _events, _closes: True),)
"""A fixed known table and ceiling for the report's corpus, which is about the report, not the
live rows: a clearing pull request's new row, or a spec closing an item, changes none of it."""


def _fixture_ceiling(name: str) -> int:
    """The fixture rows' ceilings: 4 for the fixture row, the unmeasured 3 for anything else.

    Args:
      name: A row's name.

    Returns:
      Its ceiling.

    Raises:
      None.
    """
    return _CEILING if name == _SHAPE else soak_ceilings.UNMEASURED_CEILING


def _known(shapes: str = _SHAPE) -> str:
    """A known soak item's body: the property and the expected failure with the known text."""
    return (
        f'<properties><property name="soak_known" value="{shapes}"/></properties>'
        f'<skipped type="pytest.xfail" message="known soak finding: {_SHAPE} (§12: x)&#10;sink 3">r</skipped>'
    )


def _xml(bodies: list[str], wiring: dict[str, str] | None = None) -> str:
    """Returns a JUnit document: one soak item per body, three forms a seed, and the wiring set.

    Args:
      bodies: Each soak item's inner XML, empty for a pass.
      wiring: The wiring items to write and their bodies; all eight passing when ``None``.

    Returns:
      The document.

    Raises:
      None.
    """
    forms = report_mod.FORMS_SOAKED
    cases = [
        f'<testcase classname="t" name="{report_mod.SOAK}{forms[i % 3]}-seed{i // 3}]">{body}</testcase>'
        for i, body in enumerate(bodies)
    ]
    cases.extend(
        f'<testcase classname="t" name="{name}">{(wiring or {}).get(name, "")}</testcase>'
        for name in report_mod.WIRING
        if wiring is None or name in wiring
    )
    return f'<?xml version="1.0"?><testsuites><testsuite name="pytest">{"".join(cases)}</testsuite></testsuites>'


_SEEDS_FOR_CEILING = f"0-{(_CEILING + 1) // 3 + 1}"
_ITEMS_FOR_CEILING = 3 * ((_CEILING + 1) // 3 + 2)


@pytest.mark.parametrize(
    ("bodies", "seeds", "wiring", "status", "says"),
    [
        ([""] * 6, "0-1", None, 0, "clean: 6; known: 0; new: 0"),
        ([_known()] + [""] * 5, "0-1", None, 0, f"1 x {_SHAPE}, ceiling {_CEILING}"),
        (
            ['<failure message="CloseBroken:late-write@1">r</failure>'] + [""] * 5,
            "0-1",
            None,
            0,
            "LOG_FOUNDRY_MODEL_SOAK=1 LOG_FOUNDRY_MODEL_SEEDS=0 poetry run pytest -n 0",
        ),
        ([""] * 5, "0-1", None, 1, "5 soak items ran where 6 were asked for"),
        (
            ['<skipped type="pytest.skip" message="skip">r</skipped>'] * 6,
            "0-1",
            None,
            1,
            "6 soak items were skipped",
        ),
        (
            ['<skipped type="pytest.xfail" message="flaky under load">r</skipped>'] + [""] * 5,
            "0-1",
            None,
            1,
            "1 soak items were skipped",
        ),
        (
            ['<skipped type="pytest.xfail" message="known soak finding: x">r</skipped>'] + [""] * 5,
            "0-1",
            None,
            1,
            "1 soak items were skipped",
        ),
        (
            [
                (
                    '<properties><property name="soak_known" value="x"/></properties>'
                    '<skipped type="pytest.xfail" message="flaky">r</skipped>'
                )
            ]
            + [""] * 5,
            "0-1",
            None,
            1,
            "1 soak items were skipped",
        ),
        (
            [""] * 6,
            "0-1",
            {report_mod.WIRING[0]: ""},
            1,
            "did not run: ['test_the_soak_is_wired[deliver]'",
        ),
        (
            [""] * 6,
            "0-1",
            dict.fromkeys(report_mod.WIRING, '<failure message="x">r</failure>'),
            1,
            "did not pass: ['test_the_soak_is_wired[emit]'",
        ),
        (
            [_known()] * (_CEILING + 1) + [""] * (_ITEMS_FOR_CEILING - _CEILING - 1),
            _SEEDS_FOR_CEILING,
            None,
            1,
            f"Ceiling broken:** {_SHAPE} held {_CEILING + 1} items",
        ),
        (
            [_known()] * _CEILING + [""] * (_ITEMS_FOR_CEILING - _CEILING),
            _SEEDS_FOR_CEILING,
            None,
            0,
            f"{_CEILING} x {_SHAPE}, ceiling {_CEILING}",
        ),
        ([_known(f"{_SHAPE},{_SHAPE}")] + [""] * 5, "0-1", None, 0, f"1 x {_SHAPE}, ceiling"),
        ([_known("no-such-shape")] + [""] * 5, "0-1", None, 1, "no-such-shape names no row"),
    ],
    ids=[
        "clean",
        "known",
        "new-with-its-rerun",
        "one-short",
        "all-skipped",
        "xfail-not-known",
        "xfail-known-text-without-the-property",
        "xfail-property-without-the-known-text",
        "wiring-absent",
        "wiring-failed",
        "over-the-ceiling",
        "at-the-ceiling",
        "one-item-one-shape-on-two-sinks",
        "a-known-item-naming-no-row",
    ],
)
def test_the_report(
    tmp_path: pathlib.Path,
    bodies: list[str],
    seeds: str,
    wiring: dict[str, str] | None,
    status: int,
    says: str,
) -> None:
    """FR-004: the floor, the wiring and the ceiling each set the exit status, and say why.

    A new finding is listed with the command that re-runs it and leaves the report's status at 0:
    pytest's own exit is what turns that run red, and the report's floor is for what pytest's exit
    cannot see.
    """
    xml = tmp_path / "soak.xml"
    xml.write_text(_xml(bodies, wiring), encoding="utf-8")
    text, got = report_mod.report(xml, seeds, _FIXTURE_KNOWN, _fixture_ceiling)
    assert got == status, text
    assert says in text, text


def test_the_report_writes_its_summary_and_exits_with_its_status(tmp_path: pathlib.Path) -> None:
    """FR-004: the command writes the summary file itself and exits 1 on a broken floor."""
    xml = tmp_path / "soak.xml"
    xml.write_text(_xml([""] * 5), encoding="utf-8")
    summary = tmp_path / "summary.md"
    done = _cli("report", str(xml), "0-1", "--summary", str(summary))
    assert done.returncode == 1, done
    assert "5 soak items ran where 6 were asked for" in summary.read_text(encoding="utf-8")
    missing = _cli("report", str(tmp_path / "absent.xml"), "0-1")
    assert missing.returncode == 1 and "no readable JUnit XML" in missing.stdout, missing


def unknown_rows(text: str | None = None, known: tuple[Known, ...] | None = None) -> list[str]:
    """Returns every ``KNOWN`` row whose item is not an open lead of §12 in ``text``.

    Args:
      text: ``docs/architecture.md``'s text, or ``None`` for the file.

    Returns:
      The rows' names.

    Raises:
      None.
    """
    leads = open_item_leads(text)
    rows = soak_known.KNOWN if known is None else known
    return [row.name for row in rows if " ".join(row.item.split()) not in leads]


def test_every_known_shape_names_an_open_item() -> None:
    """FR-002: a row whose item has left ``### Open`` fails, so the spec closing it removes it."""
    assert unknown_rows() == []


def _lead_bullet(text: str) -> tuple[int, int]:
    """Returns where the late-write row's bullet starts and ends in ``text``.

    Args:
      text: ``docs/architecture.md``'s text.

    Returns:
      The bullet's start and end offsets.

    Raises:
      None.
    """
    start = text.index("- **A fixture lead**")
    end = text.index("\n- ", start + 1)
    return start, end


_FIXTURE_ARCHITECTURE = (
    "## 12. Open items\n\n### Open\n\n- **A fixture lead** (the fixture row's item).\n"
    "- **Another open item** (x).\n\n### Resolved\n\n- ~~**An old item**~~ closed.\n"
)
"""A fixed §12 for the open-item check's own corpus, so closing a live item changes none of it."""


def test_the_open_item_check_can_actually_fail() -> None:
    """FR-002: the row is reported when its item is struck through, or moved under Resolved."""
    text = _FIXTURE_ARCHITECTURE
    assert unknown_rows(text, _FIXTURE_KNOWN) == []
    start, end = _lead_bullet(text)
    struck = text[:start] + "- ~~**" + text[start + len("- **") :]
    assert unknown_rows(struck, _FIXTURE_KNOWN) == [_SHAPE]
    bullet = text[start:end]
    moved = (text[:start] + text[end:]).replace(
        "\n### Resolved\n", "\n### Resolved\n\n" + bullet + "\n", 1
    )
    assert unknown_rows(moved, _FIXTURE_KNOWN) == [_SHAPE]


PHRASES = (
    "no workflow sets",
    "set by no workflow",
    "run only on request",
    "soak run on request",
    "run on request",
    "gating nothing",
    "gates nothing",
)
"""What SPEC-086's record said of the soak before SPEC-099 reversed it (FR-005)."""

GENERIC = frozenset({"run on request", "gating nothing", "gates nothing"})
"""The phrases ordinary prose uses of other things, read only in a paragraph about the soak."""

_PHRASE_ROOTS = ("docs/invariants.md", "docs/architecture.md", "docs/decisions", "tests")
_BINARY = (".pyc", ".png", ".jpg", ".gif", ".ico", ".gz", ".zip")


def phrase_findings(name: str, text: str) -> list[str]:
    """Returns each place a reversed phrase still stands in one file's text.

    The text is read a paragraph at a time, a paragraph being a run of non-blank lines, joined
    across its line breaks with its whitespace collapsed, so a phrase split by a wrap is still
    found. A phrase passes only inside a ``~~...~~`` span of a paragraph that also names
    ``SPEC-099``: struck, and marked with the spec that struck it. The generic phrases in
    :data:`GENERIC` are read only in a paragraph that mentions the soak, so a later, unrelated use
    of "gates nothing" is not refused.

    Args:
      name: The file's path, for the finding.
      text: The file's text.

    Returns:
      One finding per phrase that stands, naming the file.

    Raises:
      None.
    """
    findings = []
    for paragraph in re.split(r"\n[ \t]*\n", text):
        joined = " ".join(paragraph.split())
        struck = [m.span() for m in re.finditer(r"~~.+?~~", joined)]
        marked = "SPEC-099" in joined
        about_the_soak = "soak" in joined.lower()
        for phrase in PHRASES:
            if phrase in GENERIC and not about_the_soak:
                continue
            for found in re.finditer(re.escape(phrase), joined):
                inside = any(low <= found.start() and found.end() <= high for low, high in struck)
                if not (inside and marked):
                    findings.append(f"{name}: {phrase!r} stands in {joined[:120]!r}")
    return findings


def _tracked_text_files() -> dict[str, str]:
    """Returns the tracked text files FR-005 reads, by path, without the file holding the list.

    ``git ls-files`` names only what is tracked, so nothing untracked, ``__pycache__`` included, is
    read.

    Returns:
      Each file's path and text.

    Raises:
      None.
    """
    listed = subprocess.run(  # noqa: S603
        ["git", "ls-files", "-z", "--", *_PHRASE_ROOTS],  # noqa: S607
        cwd=ROOT,
        capture_output=True,
        check=True,
        timeout=60,
    ).stdout.decode("utf-8")
    own = pathlib.Path(__file__).resolve().relative_to(ROOT).as_posix()
    texts = {}
    for path in filter(None, listed.split("\0")):
        if path == own or path.endswith(_BINARY):
            continue
        try:
            texts[path] = (ROOT / path).read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue
    return texts


def test_the_record_no_longer_says_the_soak_runs_only_on_request() -> None:
    """FR-005: no reversed phrase stands in the record, except struck and marked with SPEC-099.

    The files read are the tracked text files under ``docs/invariants.md``,
    ``docs/architecture.md``, ``docs/decisions/`` and ``tests/``, except this one, which holds
    the list.
    """
    texts = _tracked_text_files()
    assert "docs/invariants.md" in texts and "tests/test_invariants_model.py" in texts, sorted(
        texts
    )
    findings = [f for path, text in sorted(texts.items()) for f in phrase_findings(path, text)]
    assert findings == [], findings


@pytest.mark.parametrize(
    ("text", "stands"),
    [
        ("The soak gates\nnothing at all.\n", True),
        ("The soak ~~gates nothing~~, reversed by SPEC-099.\n", False),
        ("The soak ~~gates nothing~~, reversed.\n\nSPEC-099 says so in another paragraph.\n", True),
        ("SPEC-099 runs the soak nightly; before, it gates nothing.\n", True),
        ("The exporter gates nothing until it is configured.\n", False),
    ],
    ids=[
        "bare-across-a-wrap",
        "struck-and-marked",
        "struck-marked-only-in-another-paragraph",
        "bare-in-a-paragraph-naming-spec-099",
        "a-generic-phrase-in-a-paragraph-not-about-the-soak",
    ],
)
def test_the_phrase_check_can_actually_fail(text: str, stands: bool) -> None:
    """FR-005: only a phrase struck in a paragraph that names SPEC-099 passes.

    A bare phrase fails, across a wrap too, and a struck one whose own paragraph does not name
    SPEC-099 fails though another paragraph of the same text does: the mark is read per paragraph.
    """
    assert bool(phrase_findings("x.md", text)) is stands
