"""The supported-Python versions are stated in nine places; this makes them agree.

`.github/workflows/ci.yml`'s `python-version` matrix is the **authority**, because it is the only
one of the nine that is evidence rather than a claim: it is the set of runtimes a gate actually
ran on. Every other site restates it, and until this file nothing checked that they agreed. That
was measured, not assumed — the matrix was mutated to `["3.12", "3.13", "3.14"]` and all five repo
gates (ruff, mypy, pytest, spec-lint, docs-lint) exited 0. Nothing in `tests/` or `scripts/` read
`ci.yml`, and `scripts/make-sbom.py` parses `pyproject.toml` for `[project.optional-dependencies]`
alone.

Both directions of drift are silent, and they fail differently:

- **Adding** a version to the matrix without adding its classifier *understates* what the package
  supports — a resolver and a PyPI reader are told less than CI proves.
- **Removing** one, or raising `requires-python`, leaves a classifier asserting a runtime nothing
  tests. That is the exact defect the hand-written classifier list exists to fix: left absent,
  poetry-core derives classifiers from the `requires-python` *range*, so `>=3.12` puts
  `Programming Language :: Python :: 3.14` in the built wheel's METADATA — verified by building
  one — for a runtime no CI job has ever run.

The sites are **derived where they can be** rather than hand-kept, for the reason
`docs/decisions.md` gives for the SPEC-035 and SPEC-040 rosters: "a roster that hand-lists
anything — sites or tokens — rots". The workflow floor pins are found by globbing
`.github/workflows/`, so a workflow added tomorrow is covered the day it lands rather than the day
someone remembers this file. The three prose sites cannot be derived — prose has no schema — so
each is anchored on the most *structural* thing on its line (a table-cell label, a link target, a
list-item marker) rather than on wording, and a missing anchor is a failure, never a skip.

**The claims are of two kinds, and the difference is load-bearing.** A *set* claim ("CI gates 3.12
and 3.13") must equal the matrix; a *floor* claim (`requires-python`, "Python >= 3.12", a
workflow's single pinned interpreter) must equal the matrix's lowest version. A site carrying both
— the README bullet and CLAUDE.md's Tech Stack row each do — is checked both ways, because set
equality alone passes a bullet reading "Python >= 3.13 ... gates on 3.12 and 3.13": every number
present is right and the floor is wrong.

**Not** checked: that the versions are real CPython releases, that the interpreter running this
test is one of them, or anything about the prose around the numbers. This binds nine statements of
one fact to each other; it does not adjudicate the fact.

The README's required-check names (`test / test (py3.12)`) are branch-protection check NAMES, so a
matrix change also moves a repository setting that no file here can see. They are bound anyway,
because a stale README is how that setting gets missed — and `check_name_template` covers the seam
that makes the binding meaningful: the matrix job's `name:`. Hardcode that and the check names stop
being derived, leaving the README agreeing with a matrix it no longer describes.

Parsed with `re`, not a YAML or TOML reader. PyYAML is not in the dev group and the core is
deliberately dependency-free (CLAUDE.md: "Don't add dependencies without noting them here first");
`tomllib` is in the stdlib but reads only one of the nine sites, leaving the rest on regexes
anyway. The cost of a regex is that a reformatted source can stop matching, so **every extractor
refuses an empty result** rather than reporting an agreement it never checked.

**The checks are functions over text, and `_MUTANTS` below is their fixture corpus.** Running a
gate against the artifact it guards proves the artifact passes, not that the gate works; the
corpus asserts the failure REASON for every check, and carries silence cases so a false positive
is caught too. `test_every_check_has_a_red_and_a_green_mutant` closes it: a check added here
without corpus coverage fails rather than shipping unproven.
"""

import dataclasses
import pathlib
import re
from collections.abc import Callable

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_WORKFLOWS = _ROOT / ".github" / "workflows"

# A `MAJOR.MINOR` token that is not part of a longer dotted version. The lookarounds are the whole
# point: a bare `\b3\.\d+\b` matches the `3.12` inside `1.3.12` and inside `3.12.4`, either of
# which would let a prose site agree with the matrix for the wrong reason. The trailing one admits
# a following `.` that is a full stop -- a first draft used `(?![\w.])` and went silent on a
# sentence ENDING in a version, which is how prose is ordinarily written.
_VERSION = re.compile(r"(?<![\w.])(\d+\.\d+)(?!\w)(?!\.\d)")

# The matrix, as a YAML flow sequence. `python-version: ${{ matrix.python-version }}` on the
# `setup-python` step does not match -- it has no `[...]` -- and neither does a block sequence,
# which the corpus proves is reported rather than silently read as empty.
_MATRIX = re.compile(
    r"^[ \t]*python-version:[ \t]*\[(?P<entries>[^\]\n]*)\][ \t]*(?:#.*)?$", re.MULTILINE
)

# A single pinned interpreter, as `release.yml`, `integration.yml` and `pip-audit.yml` write it.
_SCALAR_PIN = re.compile(
    r"^[ \t]*python-version:[ \t]*[\"'](?P<v>[^\"'\n]*)[\"'][ \t]*(?:#.*)?$", re.MULTILINE
)

_QUOTED_VERSION = re.compile(r"^[\"'](\d+\.\d+)[\"']$")

# Only the LOWER bound is bound to the matrix. `requires-python` carries no upper bound on purpose
# (CLAUDE.md), and adding one stays a decision this file does not pre-empt.
_REQUIRES_PYTHON = re.compile(r"^requires-python\s*=\s*[\"'](?P<spec>[^\"'\n]*)[\"']", re.MULTILINE)
_LOWER_BOUND = re.compile(r">=\s*(\d+\.\d+)")

_CLASSIFIER = re.compile(
    r"^\s*[\"']Programming Language :: Python :: (?P<rest>[^\"'\n]*)[\"']", re.MULTILINE
)

# The job-name template that turns a matrix entry into a branch-protection check name.
_JOB_NAME_TEMPLATE = re.compile(
    r"^[ \t]*name:[ \t]*test \(py\$\{\{[ \t]*matrix\.python-version[ \t]*\}\}\)[ \t]*$",
    re.MULTILINE,
)

_CHECK_NAME = re.compile(r"test / test \(py(?P<v>\d+\.\d+)\)")


@dataclasses.dataclass(frozen=True)
class Sources:
    """The text of every file that states a supported Python version.

    Text, not paths: the checks below are functions over strings so the corpus can drive them with
    mutated copies. Reading the repository happens in exactly one place, `_repo_sources`.
    """

    ci: str
    pyproject: str
    readme: str
    claude: str
    workflows: dict[str, str]


def _key(version: str) -> tuple[int, ...]:
    """Sort `MAJOR.MINOR` numerically, so 3.9 orders below 3.12 rather than above it.

    Args:
      version: A `MAJOR.MINOR` string.

    Returns:
      The parts as integers.

    Raises:
      None.
    """
    return tuple(int(part) for part in version.split("."))


def _show(versions: object) -> str:
    """Render a version collection for a failure message, naming the empty case explicitly.

    Args:
      versions: Any iterable of version strings.

    Returns:
      A sorted, readable rendering, or `(none)`.

    Raises:
      None.
    """
    items = sorted(versions, key=_key)  # type: ignore[call-overload]
    return str(items) if items else "(none)"


def _matrix(sources: Sources) -> list[str]:
    """The CI matrix versions, lowest first — the authority every other site is checked against.

    Returns an empty list rather than raising when `ci.yml` cannot be read exactly. `check_matrix`
    reports why, and every other check then compares its site against an empty set and complains
    too, which is the loud failure a silently-empty authority would not produce.

    Args:
      sources: The repository text.

    Returns:
      The `MAJOR.MINOR` strings from the matrix, or `[]` if it did not parse exactly.

    Raises:
      None.
    """
    found = _MATRIX.findall(sources.ci)
    if len(found) != 1:
        return []
    entries = [entry.strip() for entry in found[0].split(",") if entry.strip()]
    matches = [_QUOTED_VERSION.match(entry) for entry in entries]
    if not matches or not all(matches):
        return []
    return sorted((m.group(1) for m in matches if m), key=_key)


def _floor(matrix: list[str]) -> str | None:
    """The lowest gated version, or `None` when the matrix did not parse.

    Args:
      matrix: The parsed matrix.

    Returns:
      The minimum version, or `None`.

    Raises:
      None.
    """
    return matrix[0] if matrix else None


def _one_line(text: str, anchor: re.Pattern[str], where: str) -> tuple[str | None, list[str]]:
    """The single line matching `anchor`, or a complaint about none or several.

    Args:
      text: The file's contents.
      anchor: A `re.MULTILINE` pattern matching a whole line.
      where: How to name the site in a complaint.

    Returns:
      The matched line (or `None`) and any complaints.

    Raises:
      None.
    """
    found = anchor.findall(text)
    if len(found) != 1:
        return None, [
            (
                f"{where}: expected exactly one line matching {anchor.pattern!r}, found "
                f"{len(found)}. The statement was reworded, duplicated or moved; re-anchor it here "
                "rather than deleting the check, or the site silently stops being bound to the "
                "CI matrix."
            )
        ]
    return found[0], []


def _python_versions_in(line: str, matrix: list[str]) -> set[str]:
    """The `MAJOR.MINOR` tokens on a prose line that can be Python version claims.

    Prose has no schema, so the scan has to decide what a bare `0.16` on the same line is. It keeps
    only tokens whose MAJOR is at or above the lowest one CI gates, on the ground that Python's
    major version never decreases: a smaller major cannot be a claim about this package's supported
    runtimes, while `4.0` on a line about a 3.x package certainly is. That admits a neighbouring
    tool version — CLAUDE.md's Tech Stack row names `ruff` and `mypy` — without blinding the check
    to the drift it exists for. A tool at 3.x or above on one of these three lines would be a false
    positive; the failure prints the line, and the answer is to re-scope that site rather than to
    widen this filter.

    Args:
      line: The anchored line.
      matrix: The parsed matrix, whose lowest major sets the cutoff.

    Returns:
      The version tokens on the line that are Python claims.

    Raises:
      None.
    """
    floor = _floor(matrix)
    cutoff = _key(floor)[0] if floor else 0
    return {v for v in _VERSION.findall(line) if _key(v)[0] >= cutoff}


# The prose sites: (label, attribute of `Sources`, whole-line anchor). Anchored on structure — a
# table-cell label, a link target, a list-item marker — rather than on wording, so ordinary editing
# of the sentence around the numbers does not redden this.
_PROSE_SITES = (
    (
        "README.md, the Requirements bullet",
        "readme",
        re.compile(r"^- \*\*Python .*$", re.MULTILINE),
    ),
    (
        "README.md, the `ci.yml` row of the Continuous integration table",
        "readme",
        re.compile(r"^\|\s*\[`ci\.yml`\]\([^)]*\)\s*\|.*$", re.MULTILINE),
    ),
    (
        "CLAUDE.md, the Language row of the Tech Stack table",
        "claude",
        re.compile(r"^\|\s*Language\s*\|.*$", re.MULTILINE),
    ),
)

# The subset whose sentence also states a FLOOR, with the pattern that extracts it.
_FLOOR_SITES = (
    (
        "README.md, the Requirements bullet",
        "readme",
        re.compile(r"^- \*\*Python [≥>]=? ?(?P<v>\d+\.\d+)\*\*", re.MULTILINE),
    ),
    (
        "CLAUDE.md, the Language row of the Tech Stack table",
        "claude",
        re.compile(r"^\|\s*Language\s*\|\s*Python \*\*[≥>]=? ?(?P<v>\d+\.\d+)\*\*", re.MULTILINE),
    ),
)


def check_matrix(sources: Sources, matrix: list[str]) -> list[str]:
    """`ci.yml`'s matrix parses to a duplicate-free list of quoted `MAJOR.MINOR` versions."""
    if not matrix:
        return [
            (
                "ci.yml's `python-version` matrix did not parse. A block sequence "
                '(`python-version:` then `- "3.12"`) reads as zero matches here, and an unquoted '
                "entry is a YAML float — 3.10 becomes 3.1. Rewrite `_MATRIX` for the new shape; do "
                "not leave this roster comparing every site in the repo against nothing."
            )
        ]
    if len(set(matrix)) != len(matrix):
        return [f"ci.yml's `python-version` matrix has a duplicate: {matrix}"]
    return []


def check_classifier_versions(sources: Sources, matrix: list[str]) -> list[str]:
    """`pyproject.toml`'s `Programming Language :: Python :: X.Y` set equals the CI matrix."""
    declared = [m.group("rest").strip() for m in _CLASSIFIER.finditer(sources.pyproject)]
    if not declared:
        return [
            (
                "pyproject.toml declares no `Programming Language :: Python` classifiers. Left "
                "absent, poetry-core derives them from the `requires-python` RANGE — `>=3.12` puts a "
                "`:: 3.14` classifier in the built wheel, asserting support for a runtime no CI job "
                "runs. The explicit list is what stops that; it is not optional."
            )
        ]
    versions = {entry for entry in declared if _VERSION.fullmatch(entry)}
    if versions != set(matrix):
        return [
            (
                f"pyproject.toml classifiers claim Python {_show(versions)}; ci.yml gates "
                f"{_show(matrix)}. A classifier is a claim about what is TESTED — unlike "
                "`requires-python`, which is the install-time contract and correctly stays open."
            )
        ]
    return []


def check_classifier_majors(sources: Sources, matrix: list[str]) -> list[str]:
    """The bare-major classifiers (`:: 3`, `:: 3 :: Only`) track the matrix's majors too."""
    declared = [m.group("rest").strip() for m in _CLASSIFIER.finditer(sources.pyproject)]
    majors = {entry.split(" ")[0] for entry in declared if entry.split(" ")[0].isdigit()}
    expected = {version.split(".")[0] for version in matrix}
    if majors != expected:
        return [
            (
                f"pyproject.toml declares major-version classifiers for Python {sorted(majors)}, but "
                f"ci.yml gates majors {sorted(expected)}. `:: 3` and `:: 3 :: Only` are claims of the "
                "same kind as the MAJOR.MINOR entries and go stale the same way."
            )
        ]
    return []


def check_requires_python_floor(sources: Sources, matrix: list[str]) -> list[str]:
    """`requires-python`'s lower bound is the matrix minimum, so the floor is a tested runtime."""
    match = _REQUIRES_PYTHON.search(sources.pyproject)
    if match is None:
        return ["pyproject.toml declares no `requires-python`."]
    lower = _LOWER_BOUND.search(match.group("spec"))
    if lower is None:
        return [
            (
                f"`requires-python = {match.group('spec')!r}` has no `>=MAJOR.MINOR` lower bound to "
                "compare against ci.yml's matrix. Re-anchor this check rather than removing it."
            )
        ]
    floor = _floor(matrix)
    if lower.group(1) != floor:
        return [
            (
                f"`requires-python` admits Python {lower.group(1)}, but the lowest version ci.yml "
                f"gates is {floor}. The declaration is only worth what the test run behind it proves."
            )
        ]
    return []


def check_workflow_pins(sources: Sources, matrix: list[str]) -> list[str]:
    """Every workflow that pins one interpreter pins the lowest gated version.

    Swept rather than listed: `release.yml` builds the published wheel, `integration.yml` runs the
    live-service suite and `pip-audit.yml` audits the resolved environment, and each pins the floor
    deliberately. A workflow added tomorrow is covered without an edit here.
    """
    floor = _floor(matrix)
    pins = {
        name: found
        for name, text in sorted(sources.workflows.items())
        if (found := [m.group("v") for m in _SCALAR_PIN.finditer(text)])
    }
    if not pins:
        return [
            (
                'No workflow pins a single `python-version: "X.Y"`. Three did when this was written; '
                "if the shape changed, re-anchor `_SCALAR_PIN` rather than letting this pass on an "
                "empty sweep."
            )
        ]
    wrong = {name: found for name, found in pins.items() if set(found) != {floor}}
    if wrong:
        return [
            (
                f"These workflows pin a Python that is not the lowest gated version ({floor}): "
                f"{wrong}. A single pin is a floor claim — building or auditing on a runtime the "
                "matrix does not gate is the drift this roster exists to catch. If one of these "
                "deliberately wants the NEWEST gated version instead, that is a decision: record it "
                "and split this check."
            )
        ]
    return []


def check_prose_versions(sources: Sources, matrix: list[str]) -> list[str]:
    """Each prose statement of the supported versions names the matrix's set, and only it."""
    complaints = []
    for label, attribute, anchor in _PROSE_SITES:
        line, trouble = _one_line(getattr(sources, attribute), anchor, label)
        complaints.extend(trouble)
        if line is None:
            continue
        stated = _python_versions_in(line, matrix)
        if stated != set(matrix):
            complaints.append(
                f"{label} names Python {_show(stated)}; ci.yml gates {_show(matrix)}.\n"
                f"  {line.strip()}"
            )
    return complaints


def check_prose_floor(sources: Sources, matrix: list[str]) -> list[str]:
    """Each prose `>= X.Y` names the matrix minimum, which set equality alone does not catch."""
    floor = _floor(matrix)
    complaints = []
    for label, attribute, anchor in _FLOOR_SITES:
        match = anchor.search(getattr(sources, attribute))
        if match is None:
            complaints.append(
                f"{label}: no `>= MAJOR.MINOR` floor found by {anchor.pattern!r}. Re-anchor it "
                "rather than dropping the check."
            )
        elif match.group("v") != floor:
            complaints.append(
                f"{label} claims a floor of Python {match.group('v')}; the lowest version ci.yml "
                f"gates is {floor}."
            )
    return complaints


def check_readme_check_names(sources: Sources, matrix: list[str]) -> list[str]:
    """The `test / test (pyX.Y)` names the README calls required are the matrix's.

    These are branch-protection check NAMES. No file here can read that setting, so a matrix change
    moves a repository setting silently; keeping the README honest is the only in-repo trace of it.
    """
    named = {m.group("v") for m in _CHECK_NAME.finditer(sources.readme)}
    if not named:
        return [
            (
                "README.md names no `test / test (pyX.Y)` check. It documented the branch-protection "
                "required checks when this was written; re-anchor rather than drop."
            )
        ]
    if named != set(matrix):
        return [
            (
                f"README.md calls the required checks Python {_show(named)}; ci.yml gates "
                f"{_show(matrix)}. Changing the matrix renames these check runs, which also means "
                "editing the `main` ruleset — a repository setting, not a file."
            )
        ]
    return []


def check_name_template(sources: Sources, matrix: list[str]) -> list[str]:
    """`ci.yml`'s matrix job names itself from the matrix, which is what makes the names derived."""
    if not _JOB_NAME_TEMPLATE.search(sources.ci):
        return [
            (
                "ci.yml's test job no longer names itself `test (py${{ matrix.python-version }})`. "
                "The branch-protection check names are derived from the matrix through that template; "
                "a literal name breaks the derivation `check_readme_check_names` relies on."
            )
        ]
    return []


_CHECKS: dict[str, Callable[[Sources, list[str]], list[str]]] = {
    "matrix": check_matrix,
    "classifier_versions": check_classifier_versions,
    "classifier_majors": check_classifier_majors,
    "requires_python_floor": check_requires_python_floor,
    "workflow_pins": check_workflow_pins,
    "prose_versions": check_prose_versions,
    "prose_floor": check_prose_floor,
    "readme_check_names": check_readme_check_names,
    "name_template": check_name_template,
}


def _read(path: pathlib.Path) -> str:
    """Read a repo file, failing the roster rather than erroring if it moved.

    Args:
      path: The file to read.

    Returns:
      Its text.

    Raises:
      None.
    """
    if not path.is_file():
        pytest.fail(
            f"{path.relative_to(_ROOT)} is missing. This roster binds the supported-Python "
            "versions across the repo; a site that moved must be re-anchored here, not dropped."
        )
    return path.read_text(encoding="utf-8")


def _repo_sources() -> Sources:
    """The live repository's text — the only place any of these files is read.

    Args:
      None.

    Returns:
      The repository's `Sources`.

    Raises:
      None.
    """
    workflows = sorted(_WORKFLOWS.glob("*.yml"))
    if not workflows:
        pytest.fail(f"No workflows found under {_WORKFLOWS.relative_to(_ROOT)}.")
    return Sources(
        ci=_read(_WORKFLOWS / "ci.yml"),
        pyproject=_read(_ROOT / "pyproject.toml"),
        readme=_read(_ROOT / "README.md"),
        claude=_read(_ROOT / "CLAUDE.md"),
        workflows={path.name: _read(path) for path in workflows},
    )


@pytest.mark.parametrize("name", sorted(_CHECKS), ids=sorted(_CHECKS))
def test_the_repo_agrees_with_the_ci_matrix(name: str) -> None:
    """Every site in this repository states the versions `ci.yml` actually gates."""
    sources = _repo_sources()
    complaints = _CHECKS[name](sources, _matrix(sources))
    assert not complaints, "\n".join(complaints)


# --------------------------------------------------------------------------------------------
# The fixture corpus.
#
# Synthetic on purpose. Deriving the corpus from the live files would make the expectation come
# from the thing under test, and would also tie proving the gate works to the repository being in
# a state the gate accepts. These fixtures are minimal, green by construction, and mutated below.
# --------------------------------------------------------------------------------------------

_FIXTURE = Sources(
    ci=(
        "jobs:\n"
        "  test:\n"
        "    strategy:\n"
        "      matrix:\n"
        '        python-version: ["3.12", "3.13"]\n'
        "    name: test (py${{ matrix.python-version }})\n"
        "    steps:\n"
        "      - uses: actions/setup-python@abc\n"
        "        with:\n"
        "          python-version: ${{ matrix.python-version }}\n"
    ),
    pyproject=(
        'requires-python = ">=3.12"\n'
        "classifiers = [\n"
        '    "Development Status :: 5 - Production/Stable",\n'
        '    "Programming Language :: Python :: 3",\n'
        '    "Programming Language :: Python :: 3.12",\n'
        '    "Programming Language :: Python :: 3.13",\n'
        '    "Programming Language :: Python :: 3 :: Only",\n'
        "]\n"
    ),
    readme=(
        "## Requirements\n"
        "\n"
        "- **Python ≥ 3.12** — the full gate (ruff, mypy, pytest) runs on 3.12 and "
        "3.13 in CI.\n"
        "\n"
        "| Check | Does | When | Fails the build |\n"
        "|---|---|---|---|\n"
        "| [`ci.yml`](.github/workflows/ci.yml) | ruff → mypy → pytest, on 3.12 "
        "**and** 3.13 | every PR | yes |\n"
        "\n"
        "The required checks are named `test / test (py3.12)` and `test / test (py3.13)`.\n"
    ),
    claude=(
        "| Layer | Tech |\n"
        "|---|---|\n"
        "| Language | Python **>= 3.12**, fully typed (PEP 561 `py.typed`) — CI gates on "
        "3.12 **and** 3.13 |\n"
        "| Packaging | Poetry |\n"
    ),
    workflows={
        "ci.yml": '        python-version: ["3.12", "3.13"]\n',
        "release.yml": '          python-version: "3.12"\n',
        "pip-audit.yml": '          python-version: "3.12"\n',
    },
)


def _mutate(**edits: tuple[str, str]) -> Sources:
    """A copy of `_FIXTURE` with one substitution applied per named field.

    Args:
      **edits: `field=(old, new)`. `old` must occur in that field, so a corpus case cannot go
        quiet by silently editing nothing.

    Returns:
      The mutated `Sources`.

    Raises:
      None.
    """
    changed: dict[str, object] = {}
    for field, (old, new) in edits.items():
        if field == "workflows":
            texts = {k: v.replace(old, new) for k, v in _FIXTURE.workflows.items()}
            assert texts != _FIXTURE.workflows, f"corpus mutation matched nothing: {old!r}"
            changed[field] = texts
            continue
        text: str = getattr(_FIXTURE, field)
        assert old in text, f"corpus mutation matched nothing in {field}: {old!r}"
        changed[field] = text.replace(old, new, 1)
    return dataclasses.replace(_FIXTURE, **changed)  # type: ignore[arg-type]


# (label, the check that must complain or None for a silence case, the mutated sources, a
# substring the complaint must contain). The substring is the point: a check that reddens for the
# wrong reason is not the check the corpus claims to prove.
_MUTANTS: tuple[tuple[str, str | None, Sources, str], ...] = (
    ("unchanged fixture", None, _FIXTURE, ""),
    (
        "matrix gains 3.14",
        "classifier_versions",
        _mutate(ci=('["3.12", "3.13"]', '["3.12", "3.13", "3.14"]')),
        "ci.yml gates ['3.12', '3.13', '3.14']",
    ),
    (
        "matrix gains 3.14, seen from the prose",
        "prose_versions",
        _mutate(ci=('["3.12", "3.13"]', '["3.12", "3.13", "3.14"]')),
        "Requirements bullet names Python ['3.12', '3.13']",
    ),
    (
        "matrix gains 3.14, seen from the required-check names",
        "readme_check_names",
        _mutate(ci=('["3.12", "3.13"]', '["3.12", "3.13", "3.14"]')),
        "calls the required checks Python ['3.12', '3.13']",
    ),
    (
        "matrix drops 3.12, raising the floor",
        "requires_python_floor",
        _mutate(ci=('["3.12", "3.13"]', '["3.13"]')),
        "`requires-python` admits Python 3.12",
    ),
    (
        "matrix drops 3.12, seen from the prose floor",
        "prose_floor",
        _mutate(ci=('["3.12", "3.13"]', '["3.13"]')),
        "claims a floor of Python 3.12",
    ),
    (
        "matrix drops 3.12, seen from the workflow pins",
        "workflow_pins",
        _mutate(ci=('["3.12", "3.13"]', '["3.13"]')),
        "pin a Python that is not the lowest gated version (3.13)",
    ),
    (
        "matrix rewritten as a YAML block sequence",
        "matrix",
        _mutate(ci=('python-version: ["3.12", "3.13"]', 'python-version:\n          - "3.12"')),
        "did not parse",
    ),
    (
        "matrix entry unquoted, which YAML reads as a float",
        "matrix",
        _mutate(ci=('["3.12", "3.13"]', "[3.12, 3.13]")),
        "did not parse",
    ),
    (
        "matrix lists a version twice",
        "matrix",
        _mutate(ci=('["3.12", "3.13"]', '["3.12", "3.12"]')),
        "has a duplicate",
    ),
    (
        "job name hardcoded, so the check names stop being derived",
        "name_template",
        _mutate(ci=("name: test (py${{ matrix.python-version }})", "name: test (py3.12)")),
        "no longer names itself",
    ),
    (
        "classifiers absent, leaving poetry-core to derive them",
        "classifier_versions",
        _mutate(
            pyproject=(
                (
                    "classifiers = [\n"
                    '    "Development Status :: 5 - Production/Stable",\n'
                    '    "Programming Language :: Python :: 3",\n'
                    '    "Programming Language :: Python :: 3.12",\n'
                    '    "Programming Language :: Python :: 3.13",\n'
                    '    "Programming Language :: Python :: 3 :: Only",\n'
                    "]\n"
                ),
                "",
            )
        ),
        "declares no `Programming Language :: Python` classifiers",
    ),
    (
        "classifier for 3.13 deleted",
        "classifier_versions",
        _mutate(pyproject=('    "Programming Language :: Python :: 3.13",\n', "")),
        "classifiers claim Python ['3.12']",
    ),
    (
        "classifier for 3.14 added",
        "classifier_versions",
        _mutate(
            pyproject=(
                '    "Programming Language :: Python :: 3 :: Only",',
                '    "Programming Language :: Python :: 3.14",',
            )
        ),
        "classifiers claim Python ['3.12', '3.13', '3.14']",
    ),
    (
        "major classifier bumped past the gated major",
        "classifier_majors",
        _mutate(
            pyproject=(
                '"Programming Language :: Python :: 3",',
                '"Programming Language :: Python :: 4",',
            )
        ),
        "major-version classifiers for Python ['3', '4']",
    ),
    (
        "requires-python floor raised above the matrix",
        "requires_python_floor",
        _mutate(pyproject=('requires-python = ">=3.12"', 'requires-python = ">=3.13"')),
        "admits Python 3.13, but the lowest version ci.yml gates is 3.12",
    ),
    (
        "requires-python deleted",
        "requires_python_floor",
        _mutate(pyproject=('requires-python = ">=3.12"\n', "")),
        "declares no `requires-python`",
    ),
    (
        "requires-python loses its lower bound",
        "requires_python_floor",
        _mutate(pyproject=('requires-python = ">=3.12"', 'requires-python = "<4.0"')),
        "has no `>=MAJOR.MINOR` lower bound",
    ),
    (
        "a workflow pin moves off the floor",
        "workflow_pins",
        _mutate(workflows=('python-version: "3.12"', 'python-version: "3.13"')),
        "not the lowest gated version (3.12)",
    ),
    (
        "every workflow pin removed, so the sweep finds nothing",
        "workflow_pins",
        _mutate(workflows=('python-version: "3.12"', "python-version: FIXME")),
        "empty sweep",
    ),
    (
        "README Requirements bullet drops a version",
        "prose_versions",
        _mutate(readme=("runs on 3.12 and 3.13 in CI", "runs on 3.12 in CI")),
        "Requirements bullet names Python ['3.12']",
    ),
    (
        "README Requirements floor raised while its set stays right",
        "prose_floor",
        _mutate(readme=("- **Python ≥ 3.12**", "- **Python ≥ 3.13**")),
        "claims a floor of Python 3.13",
    ),
    (
        "README ci.yml table row drops a version",
        "prose_versions",
        _mutate(readme=("on 3.12 **and** 3.13 | every PR", "on 3.12 | every PR")),
        "`ci.yml` row of the Continuous integration table names Python ['3.12']",
    ),
    (
        "README ci.yml table row de-anchored",
        "prose_versions",
        _mutate(readme=("| [`ci.yml`](.github/workflows/ci.yml) |", "| ci.yml |")),
        "expected exactly one line matching",
    ),
    (
        "README ci.yml table row duplicated, so its anchor is no longer unique",
        "prose_versions",
        _mutate(
            readme=(
                "| [`ci.yml`](.github/workflows/ci.yml) | ruff",
                (
                    "| [`ci.yml`](.github/workflows/ci.yml) | ruff, on 3.12 **and** 3.13 | x | y |\n"
                    "| [`ci.yml`](.github/workflows/ci.yml) | ruff"
                ),
            )
        ),
        "found 2",
    ),
    (
        "README required-check names lose one leg",
        "readme_check_names",
        _mutate(
            readme=("`test / test (py3.12)` and `test / test (py3.13)`", "`test / test (py3.12)`")
        ),
        "calls the required checks Python ['3.12']",
    ),
    (
        "README required-check names respelled past their anchor",
        "readme_check_names",
        _mutate(
            readme=(
                "`test / test (py3.12)` and `test / test (py3.13)`",
                "`test (3.12)` and `test (3.13)`",
            )
        ),
        "names no `test / test (pyX.Y)` check",
    ),
    (
        "CLAUDE.md Language row drops a version",
        "prose_versions",
        _mutate(claude=("CI gates on 3.12 **and** 3.13", "CI gates on 3.12")),
        "Language row of the Tech Stack table names Python ['3.12']",
    ),
    (
        "CLAUDE.md Language row floor raised while its set stays right",
        "prose_floor",
        _mutate(claude=("Python **>= 3.12**", "Python **>= 3.13**")),
        "claims a floor of Python 3.13",
    ),
    (
        "CLAUDE.md Language row de-anchored",
        "prose_versions",
        _mutate(claude=("| Language | Python", "| Runtime | Python")),
        "expected exactly one line matching",
    ),
    (
        "CLAUDE.md Language row claims a Python above the gated major",
        "prose_versions",
        _mutate(claude=("CI gates on 3.12 **and** 3.13", "CI gates on 3.12, 3.13 **and** 4.0")),
        "names Python ['3.12', '3.13', '4.0']",
    ),
    # Silence cases. Half a gate's regressions are false positives, so the corpus has to prove
    # what it does NOT complain about.
    (
        "SILENCE: prose reworded around the same numbers",
        None,
        _mutate(
            readme=(
                "the full gate (ruff, mypy, pytest) runs on 3.12 and 3.13 in CI.",
                "every gate we run is exercised on 3.12 and 3.13.",
            )
        ),
        "",
    ),
    (
        "SILENCE: a sentence ENDS on a version, with no trailing word",
        None,
        _mutate(readme=("runs on 3.12 and 3.13 in CI.", "is exercised on 3.12 and 3.13.")),
        "",
    ),
    (
        "SILENCE: an unrelated dotted version appears elsewhere in the file",
        None,
        _mutate(readme=("## Requirements", "Tested against boto3 1.3.12.\n\n## Requirements")),
        "",
    ),
    (
        "SILENCE: a tool version below the gated major shares the CLAUDE.md row",
        None,
        _mutate(claude=("(PEP 561 `py.typed`)", "(PEP 561 `py.typed`, ruff 0.16, mypy 2.3)")),
        "",
    ),
    (
        "SILENCE: a classifier that is not a version claim is added",
        None,
        _mutate(pyproject=('    "Programming Language :: Python :: 3",', '    "Typing :: Typed",')),
        "",
    ),
)


@pytest.mark.parametrize(
    ("expected", "sources", "reason"),
    [(m[1], m[2], m[3]) for m in _MUTANTS],
    ids=[m[0] for m in _MUTANTS],
)
def test_each_mutant_is_caught_by_the_right_check_for_the_right_reason(
    expected: str | None, sources: Sources, reason: str
) -> None:
    """Every corpus mutant reddens exactly the check that owns it, and says why."""
    matrix = _matrix(sources)
    complaints = {name: check(sources, matrix) for name, check in _CHECKS.items()}
    reddened = {name for name, found in complaints.items() if found}

    if expected is None:
        assert not reddened, (
            f"A change that alters no version claim reddened {sorted(reddened)}: {complaints}"
        )
        return

    assert expected in reddened, (
        f"{expected} stayed green on a mutant it owns. Checks that did complain: {sorted(reddened)}"
    )
    assert any(reason in complaint for complaint in complaints[expected]), (
        f"{expected} complained for a different reason than the corpus claims. Wanted "
        f"{reason!r}, got: {complaints[expected]}"
    )


def test_every_check_has_a_red_and_a_green_mutant() -> None:
    """No check ships without corpus coverage, and the corpus has silence cases.

    The roster rule applied to this file itself: a check added above without a mutant below would
    otherwise be a guard nobody has ever seen fail.
    """
    covered = {expected for _, expected, _, _ in _MUTANTS if expected is not None}
    assert covered == set(_CHECKS), (
        f"Checks with no corpus mutant: {sorted(set(_CHECKS) - covered)}. Add one that reddens "
        "the check and names the reason, or the check is unproven."
    )
    silences = [label for label, expected, _, _ in _MUTANTS if expected is None]
    assert len(silences) >= 2, (
        "The corpus needs cases that assert SILENCE — roughly half a gate's regressions are "
        f"false positives. Found: {silences}"
    )
