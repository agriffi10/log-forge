"""One fact, restated across eight files; this makes the restatements agree.

The supported-Python versions are stated in `ci.yml`, `release.yml`, `integration.yml`,
`pip-audit.yml`, `pyproject.toml`, `README.md`, `CLAUDE.md`, and both best-practices docs. No count
is given here on purpose — `_CHECKS`, `_PROSE_SITES` and `_FLOOR_SITES` below are the enumeration,
and a number in this sentence would be a claim nothing re-measures.

`.github/workflows/ci.yml`'s `python-version` matrix is the **authority**, because it is the only
one that is evidence rather than a claim: it is the set of runtimes a gate actually ran on. The
sites below restate it, and until this file nothing checked that they agreed. That
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

**The matrix is an authority only under conditions this file cannot fully verify**, and saying so
is the honest form of the claim. `check_setup_python_consumes_matrix` covers one of them — pin the
`setup-python` step to a literal and both legs install the same interpreter while every claim in
the repo stays true on paper — and `check_name_suffix` covers a second. The rest are named and not
checked: the workflow has to be **triggered** (delete `on: pull_request` and this roster is green
over a gate nothing runs), the job must not be skipped by an `if:` or a path filter, and no
`exclude:` may thin the matrix. Each is a fresh instance of one class — the authority can be
neutralised upstream — and CLAUDE.md's rule is that a recurring class wants a structural answer
rather than another check. The structural answer is this paragraph: the roster establishes that
every *restatement* matches the matrix, not that the matrix ran.

The sites are **derived where they can be** rather than hand-kept, for the reason
`docs/decisions.md` gives for the SPEC-035 and SPEC-040 rosters: "a roster that hand-lists
anything — sites or tokens — rots". The workflow pins are found by globbing
`.github/workflows/`, so a workflow added tomorrow is covered the day it lands rather than the day
someone remembers this file. The three prose sites cannot be derived — prose has no schema — so
each is anchored on the most *structural* thing on its line (a table-cell label, a link target, a
list-item marker) rather than on wording, and a missing anchor is a failure, never a skip.

**The claims are of two kinds, and the difference is load-bearing.** A *set* claim ("CI gates 3.12
and 3.13") must equal the matrix; a *floor* claim (`requires-python`, mypy's `python_version`, a
workflow's single pinned interpreter, "Python >= 3.12") must equal the matrix's lowest version. A
site carrying both — the README bullet and CLAUDE.md's Tech Stack row each do — is checked both
ways, because set equality alone passes a bullet reading "Python >= 3.13 ... gates on 3.12 and
3.13": every number present is right and the floor is wrong.

**Not** checked, and the first item is the one to read. **A restatement on a line no anchor reaches
is unbound** — the boundary is the line, not the file, which is the shape of every gap found here so
far. In files outside `Sources` that is all of them: `src/log_foundry/decorator.py`'s flush-lock
docstring says "the floor is ``>=3.12``", `docs/architecture.md` says 3.13 "added
`Queue.shutdown()` — which CI gates on", and the *comments* in `pyproject.toml` and the workflows
explain the floor in prose this reads straight past. Raising the floor means editing those by hand
with nothing to remind you; reaching into `src/` for a sentence is a wider change than this file has
made, and a half-bound `src` would read as covered. Inside the four prose files the gap is closed
from the other side by `check_version_lines_are_classified`, so a *new* unanchored line naming a
gated version cannot arrive unnoticed.

Distinguish all of that from a third kind that is deliberately **not** a restatement and must not be
"fixed" to agree: a claim about when a *language feature* appeared. `sanitize.py` says CPython 3.11+
raises past `sys.get_int_max_str_digits()`; the rulebook says `X | None` is 3.10+ syntax. Both are
history, true regardless of what this package supports, and aligning them with the matrix would make
them wrong. **Nothing filters them out by shape** — an earlier draft of this paragraph credited
`_python_versions_in`'s gated-major cutoff, which is false: it keeps 3.10 and 3.11 happily, and its
own docstring says what it is really for. What keeps them out is **anchoring** (`sanitize.py` is not
in `Sources`; the `X | None` bullet is on no anchored line) and, inside the swept files, the
explicit `_NOT_A_SUPPORT_CLAIM` classification.

Also not checked: that the versions are real CPython releases, that the interpreter running this
test is one of them, or anything about the prose around the numbers. This binds the restatements it
names to each other; it does not adjudicate the fact.

**Two of the sites are documentation the repo routes agents through**, and they are here because
they are the two that have already drifted. `ae7447d` — this branch's own parent — exists to
correct a `>= 3.13` floor stated twice in `docs/best-practices/python/python.md` and once in the
`INDEX.md` that routes every Python task to it. A stale floor there does not merely read wrong: it
instructs the next session to write a call the floor cannot run. The roster bound CLAUDE.md, the
file that stayed correct, before it bound the two that did not.

The README's `test / test (pyX.Y)` strings are branch-protection check-run NAMES, and both halves
are bound: the prefix is the id of the `release.yml` job that `uses:` this workflow, the suffix is
the matrix job's own `name:`. Bound, but stated carefully — `main`'s ruleset requires **no** status
checks today, which `ci.yml` records having checked rather than assumed. So a matrix change breaks
nothing on the remote the moment it lands; what it changes is the string anyone would have to
require *if* a check is ever made required, and no file in this repository can see that setting to
correct it later.

Parsed with `re`, not a YAML or TOML reader. PyYAML is not in the dev group and the core is
deliberately dependency-free (CLAUDE.md: "Don't add dependencies without noting them here first");
`tomllib` is in the stdlib but reads only three of the twelve sites, leaving the rest on regexes
anyway. The cost of a regex is that a reformatted source can stop matching, so **every extractor
refuses a result it cannot read** rather than reporting an agreement it never checked — including
the *partly* unreadable case, which is the one a plain "refuse an empty sweep" misses: two quoted
pins keep a sweep non-empty while a third, unquoted, goes uninspected.

**The checks are functions over text, and `_MUTANTS` below is their fixture corpus.** Running a
gate against the artifact it guards proves the artifact passes, not that the gate works; the corpus
asserts the failure REASON for every check, and carries silence cases so a false positive is caught
too. Several mutants are shaped to discriminate a guard from its absence rather than merely to be
wrong — a *partly* unquoted matrix, a duplicated row whose copies disagree, a dotted number on the
anchored line rather than beside it — because a mutant that both the guarded and the unguarded code
handle identically proves nothing about the guard.
`test_every_check_has_a_red_and_a_green_mutant` closes it: a check added here without corpus
coverage fails rather than shipping unproven.
"""

import dataclasses
import pathlib
import re
from collections.abc import Callable, Iterable

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_WORKFLOWS = _ROOT / ".github" / "workflows"

# A `MAJOR.MINOR` token that is not part of a longer dotted version. The lookarounds are the whole
# point: a bare `\b3\.\d+\b` matches the `3.12` inside `1.3.12` and inside `3.12.4`, either of
# which would let a prose site agree with the matrix for the wrong reason. The trailing one admits
# a following `.` that is a full stop -- a first draft used `(?![\w.])` and went silent on a
# sentence ENDING in a version, which is how prose is ordinarily written. Each clause has its own
# corpus mutant, on the anchored line, naming a version the matrix does NOT gate; anywhere else, or
# with a version it does, guarded and unguarded behave identically and prove nothing.
_VERSION = re.compile(r"(?<![\w.])(\d+\.\d+)(?!\w)(?!\.\d)")

# The matrix, as a YAML flow sequence. A block sequence does not match, which is reported rather
# than read as an empty matrix.
_MATRIX = re.compile(
    r"^[ \t]*python-version:[ \t]*\[(?P<entries>[^\]\n]*)\][ \t]*(?:#.*)?$", re.MULTILINE
)

# EVERY `python-version:` line, whatever its value. Classifying the value here rather than matching
# only the shape we expect is what makes an unreadable pin visible: a regex that matched only
# quoted scalars would skip `python-version: 3.13` in silence, and the two remaining quoted pins
# would keep the sweep non-empty so the "refuse an empty result" rule never fired.
_PIN_LINE = re.compile(r"^[ \t]*python-version:[ \t]*(?P<value>\S[^\n]*?)[ \t]*$", re.MULTILINE)
_TRAILING_COMMENT = re.compile(r"\s+#.*$")

_QUOTED_VERSION = re.compile(r"^[\"'](\d+\.\d+)[\"']$")
_MATRIX_REFERENCE = re.compile(r"^\$\{\{\s*matrix\.python-version\s*\}\}$")

# Only the LOWER bound is bound to the matrix. `requires-python` carries no upper bound on purpose
# (CLAUDE.md), and adding one stays a decision this file does not pre-empt.
_REQUIRES_PYTHON = re.compile(r"^requires-python\s*=\s*[\"'](?P<spec>[^\"'\n]*)[\"']", re.MULTILINE)
_LOWER_BOUND = re.compile(r">=\s*(\d+\.\d+)")

# `[tool.mypy] python_version`. Anchored at column zero, which is what keeps it off the
# `python_version < '3.15'` environment marker inside a quoted dependency string.
_MYPY_PYTHON_VERSION = re.compile(r"^python_version\s*=\s*[\"'](?P<v>[^\"'\n]*)[\"']", re.MULTILINE)

# `[tool.ruff] target-version`, which this repo deliberately does not set -- ruff infers it from
# `requires-python`. See `check_ruff_target_version` for why a check that is silent today is not
# an empty one.
_RUFF_TARGET_VERSION = re.compile(r"^target-version\s*=\s*[\"'](?P<v>[^\"'\n]*)[\"']", re.MULTILINE)

_CLASSIFIER = re.compile(
    r"^\s*[\"']Programming Language :: Python :: (?P<rest>[^\"'\n]*)[\"']", re.MULTILINE
)

# The matrix job's `name:` -- the SUFFIX half of a branch-protection check name.
_JOB_NAME_TEMPLATE = re.compile(
    r"^[ \t]*name:[ \t]*test \(py\$\{\{[ \t]*matrix\.python-version[ \t]*\}\}\)[ \t]*$",
    re.MULTILINE,
)

# The job in `release.yml` that calls this workflow -- the PREFIX half of that check name. Comment
# lines between the job key and its `uses:` are skipped; `release.yml` has one today.
_CI_CALLER_JOB = re.compile(
    r"^[ \t]{2}(?P<job>[A-Za-z0-9_-]+):[ \t]*\n(?:[ \t]*#[^\n]*\n)*[ \t]+uses:[ \t]*"
    r"\./\.github/workflows/ci\.yml",
    re.MULTILINE,
)

_CHECK_NAME = re.compile(r"(?P<caller>[A-Za-z0-9_-]+) / test \(py(?P<v>\d+\.\d+)\)")


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
    python_rulebook: str
    bp_index: str
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


def _show(versions: Iterable[str]) -> str:
    """Render a version collection for a failure message, naming the empty case explicitly.

    Args:
      versions: Any iterable of version strings.

    Returns:
      A sorted, readable rendering, or `(none)`.

    Raises:
      None.
    """
    items = sorted(versions, key=_key)
    return str(items) if items else "(none)"


def _value(line_value: str) -> str:
    """Strip a trailing YAML comment from a scalar value.

    Args:
      line_value: The text after `python-version:`.

    Returns:
      The value alone.

    Raises:
      None.
    """
    return _TRAILING_COMMENT.sub("", line_value).strip()


def _parse_matrix(ci: str) -> tuple[list[str], str | None]:
    """The CI matrix versions, lowest first, or the reason it could not be read.

    Every refusal names what to change, and they are deliberately distinct: a matrix that did not
    match at all is a `_MATRIX` problem, while an entry that matched and is not a quoted
    `MAJOR.MINOR` is a `_QUOTED_VERSION` problem. A message that named only the first would send
    the next reader — very plausibly someone adding a free-threaded `"3.13t"` leg — to the wrong
    regex.

    Args:
      ci: The text of `ci.yml`.

    Returns:
      The versions and `None`, or `[]` and a complaint.

    Raises:
      None.
    """
    found = _MATRIX.findall(ci)
    if len(found) != 1:
        return [], (
            f"ci.yml: expected exactly one `python-version: [...]` matrix, found {len(found)}. A "
            'block sequence (`python-version:` then `- "3.12"`) reads as zero matches here. '
            "Rewrite `_MATRIX` for the new shape; do not leave this roster comparing every site "
            "in the repo against nothing."
        )
    entries = [entry.strip() for entry in found[0].split(",") if entry.strip()]
    if not entries:
        return [], "ci.yml's `python-version` matrix is empty."
    unreadable = [entry for entry in entries if not _QUOTED_VERSION.match(entry)]
    if unreadable:
        return [], (
            f"ci.yml: matrix entries {unreadable} are not quoted MAJOR.MINOR. YAML reads an "
            "unquoted 3.10 as the float 3.1, so the quotes are load-bearing rather than style.\n"
            '  A free-threaded leg (`"3.13t"`) is refused here too, and supporting one is more '
            "than a wider regex: `_QUOTED_VERSION`, `_VERSION` and `_key` all have to change "
            "together — `_key` parses each dotted part with `int()` and raises on `13t` — and "
            "`check_classifier_versions` becomes unsatisfiable, because there is no PyPI "
            "classifier for a free-threaded build. That is a decision to record, not a regex to "
            "widen."
        )
    versions = sorted((entry[1:-1] for entry in entries), key=_key)
    if len(set(versions)) != len(versions):
        return [], f"ci.yml's `python-version` matrix has a duplicate: {versions}"
    return versions, None


def _matrix(sources: Sources) -> list[str]:
    """The authority, or `[]` when it could not be read.

    `[]` rather than an exception: `check_matrix` reports the reason, and every other check then
    compares its site against an empty set and complains too. That is the loud failure a silently
    empty authority would not produce.

    Args:
      sources: The repository text.

    Returns:
      The matrix versions, lowest first, or `[]`.

    Raises:
      None.
    """
    return _parse_matrix(sources.ci)[0]


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
    to the drift it exists for. Two things it therefore does not catch, recorded rather than left
    to be discovered: a claim of support for a Python BELOW the floor (`2.7`) passes, and a tool
    version at or above the gated major (`pytest 8.4` on that row) is a false positive. The failure
    prints the line; the answer to the second is to re-scope that site, not to widen this filter.

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
        # `[≥>]=? ?\d` rather than `.*`: a second bullet opening "- **Python free-threading ..."
        # states no version and must not be mistaken for a rival copy of this claim. An anchor
        # that reddens a legitimate edit is an anchor someone deletes.
        re.compile(r"^- \*\*Python [≥>]=? ?\d.*$", re.MULTILINE),
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
    (
        "docs/best-practices/python/python.md, the Repo defaults bullet",
        "python_rulebook",
        re.compile(r"^- Repo defaults:.*$", re.MULTILINE),
    ),
    (
        "docs/best-practices/python/python.md, the rulebook's scope blockquote",
        "python_rulebook",
        # Anchored on the SHAPE OF THE CLAIM rather than on the sentence around it, so the line
        # is found wherever in the file it moves to -- and a claim deleted outright is a missing
        # anchor, which fails. CLAUDE.md routes every Python task through this file first, so a
        # stale floor here does not merely read wrong: it instructs the next agent to write a
        # 3.13-only call. It is also one of the two files that actually drifted -- `ae7447d`,
        # this branch's own parent, exists to correct a `>= 3.13` floor stated here twice.
        re.compile(r"^> .*\*\*Python [≥>]=? ?\d.*$", re.MULTILINE),
    ),
)

# The FLOOR claims. A site appears here as well as above when its sentence states both, and here
# ALONE when it states only a minimum -- `3.12+` names one version and is not a claim about the
# gated set. Every occurrence is checked, not just the first: two copies of a floor claim in one
# file drift independently, and `search` would have adjudicated only the earlier one.
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
    (
        "docs/best-practices/python/python.md, the rulebook's scope blockquote",
        "python_rulebook",
        re.compile(r"^> .*\*\*Python [≥>]=? ?(?P<v>\d+\.\d+)\*\*", re.MULTILINE),
    ),
    (
        "docs/best-practices/python/python.md, the Repo defaults bullet",
        "python_rulebook",
        # The rulebook states the floor TWICE, and binding only the scope blockquote left this
        # one free to drift alone. Found by sweeping the repo for version tokens rather than by
        # reading the file, which is the only way a second copy in a bound file turns up.
        re.compile(r"^- Repo defaults:.*Runtime floor = \*\*(?P<v>\d+\.\d+)\*\*", re.MULTILINE),
    ),
    (
        "docs/best-practices/INDEX.md, the Python row of the router table",
        "bp_index",
        re.compile(r"^\|\s*Python \((?P<v>\d+\.\d+)\+", re.MULTILINE),
    ),
    (
        "README.md, the `poetry install` line of the Development section",
        "readme",
        re.compile(r"^poetry install .*\(Python (?P<v>\d+\.\d+)\+\)", re.MULTILINE),
    ),
)


_NO_CLASSIFIERS = (
    "pyproject.toml declares no `Programming Language :: Python` classifiers. Left absent, "
    "poetry-core derives them from the `requires-python` RANGE — `>=3.12` puts a `:: 3.14` "
    "classifier in the built wheel, asserting support for a runtime no CI job runs. The explicit "
    "list is what stops that; it is not optional."
)


def check_matrix(sources: Sources, matrix: list[str]) -> list[str]:
    """`ci.yml`'s matrix parses to a duplicate-free list of quoted `MAJOR.MINOR` versions."""
    reason = _parse_matrix(sources.ci)[1]
    return [reason] if reason else []


def check_setup_python_consumes_matrix(sources: Sources, matrix: list[str]) -> list[str]:
    """Something in `ci.yml` installs the matrix, which is what makes the matrix the authority.

    Without this, `python-version: ${{ matrix.python-version }}` can be replaced by a literal and
    every other check in this file stays green while both legs run the same interpreter. The
    docstring above calls the matrix "the set of runtimes a gate actually ran on"; that sentence is
    true only while the `setup-python` step consumes it, and this is the check that keeps it true.
    """
    values = [_value(m.group("value")) for m in _PIN_LINE.finditer(sources.ci)]
    literals = [v for v in values if not v.startswith("[") and "${{" not in v]
    complaints = []
    if not any(_MATRIX_REFERENCE.match(v) for v in values):
        complaints.append(
            "ci.yml declares a `python-version` matrix that no step consumes — no "
            "`python-version: ${{ matrix.python-version }}` found. Both legs then install the "
            "same interpreter, and every version claim in this repository stays true on paper "
            "while one of the gated runtimes is never run."
        )
    if literals:
        complaints.append(
            f"ci.yml pins a literal interpreter {literals}. The matrix job must take its Python "
            "from the matrix; a literal here silently overrides it."
        )
    return complaints


def check_classifier_versions(sources: Sources, matrix: list[str]) -> list[str]:
    """`pyproject.toml`'s `Programming Language :: Python :: X.Y` set equals the CI matrix."""
    declared = [m.group("rest").strip() for m in _CLASSIFIER.finditer(sources.pyproject)]
    if not declared:
        return [_NO_CLASSIFIERS]
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
    if not declared:
        return [_NO_CLASSIFIERS]
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


def check_mypy_python_version(sources: Sources, matrix: list[str]) -> list[str]:
    """`[tool.mypy] python_version` is the floor, not the newest gated version.

    The rule is written beside the setting and was enforced by nothing: mypy checks against this
    version, so a 3.13-only API type-checks clean on a 3.12 floor and reaches a consumer as an
    AttributeError. Raising it to the newest gated version is the drift that silently removes the
    guarantee.
    """
    found = _MYPY_PYTHON_VERSION.findall(sources.pyproject)
    if len(found) != 1:
        return [
            (
                f'Expected exactly one line-anchored `python_version = "X.Y"` in pyproject.toml, '
                f"found {len(found)}: {found}. It is `[tool.mypy]`'s; re-anchor rather than drop."
            )
        ]
    floor = _floor(matrix)
    if found[0] != floor:
        return [
            (
                f"`[tool.mypy] python_version` is {found[0]}, but the lowest version ci.yml gates is "
                f"{floor}. mypy must check against the FLOOR: on a newer target, an API that does not "
                "exist on the oldest supported runtime type-checks clean and fails in production."
            )
        ]
    return []


def check_ruff_target_version(sources: Sources, matrix: list[str]) -> list[str]:
    """If `[tool.ruff] target-version` is ever set, it is the floor.

    Silent today by design: the repo deliberately sets no `target-version` so ruff infers it from
    `requires-python`, and `pyproject.toml` states in as many words that "if one is ever added it
    must match the floor above". That rule was enforced by nothing. A check that says nothing about
    the current tree is not the same as one that cannot fail — the corpus mutant below adds the key
    and watches this redden.
    """
    found = _RUFF_TARGET_VERSION.findall(sources.pyproject)
    if not found:
        return []
    floor = _floor(matrix)
    expected = "py" + floor.replace(".", "") if floor else None
    wrong = [value for value in found if value != expected]
    if wrong:
        return [
            (
                f"`target-version` is {wrong}, but the lowest version ci.yml gates is {floor} "
                f"(`{expected}`). pyproject.toml's own note says a `target-version`, if added, must "
                "match the floor; ruff otherwise lints against a runtime the matrix does not gate."
            )
        ]
    return []


def check_workflow_pins(sources: Sources, matrix: list[str]) -> list[str]:
    """Every workflow that pins one interpreter pins the lowest gated version.

    Swept rather than listed: `release.yml` builds the published wheel, `integration.yml` runs the
    live-service suite and `pip-audit.yml` audits the resolved environment, and each pins the floor
    deliberately. A workflow added tomorrow is covered without an edit here.

    Values that are neither a matrix, a `${{ }}` expression nor a quoted `MAJOR.MINOR` are reported
    rather than skipped. That is the *partly* unreadable case: an unquoted `python-version: 3.13`
    is a YAML float, and skipping it in silence leaves the other two pins keeping the sweep
    non-empty so the refusal below never fires.
    """
    floor = _floor(matrix)
    pins: dict[str, list[str]] = {}
    unreadable: dict[str, list[str]] = {}
    for name, text in sorted(sources.workflows.items()):
        for match in _PIN_LINE.finditer(text):
            value = _value(match.group("value"))
            if value.startswith("[") or "${{" in value:
                continue
            quoted = _QUOTED_VERSION.match(value)
            if quoted:
                pins.setdefault(name, []).append(quoted.group(1))
            else:
                unreadable.setdefault(name, []).append(value)
    complaints = []
    if unreadable:
        complaints.append(
            f"These workflows pin a `python-version` this sweep cannot read: {unreadable}. An "
            "unquoted 3.10 is the YAML float 3.1; quote it, or re-anchor `_QUOTED_VERSION`. An "
            "unreadable pin is an UNCHECKED pin, and the other pins keep the sweep non-empty."
        )
    if not pins and not unreadable:
        complaints.append(
            'No workflow pins a single `python-version: "X.Y"`. Three did when this was written; '
            "if the shape changed, re-anchor `_PIN_LINE` rather than letting this pass on an "
            "empty sweep."
        )
    wrong = {name: found for name, found in pins.items() if set(found) != {floor}}
    if wrong:
        complaints.append(
            f"These workflows pin a Python that is not the lowest gated version ({floor}): "
            f"{wrong}. A single pin is a floor claim — building or auditing on a runtime the "
            "matrix does not gate is the drift this roster exists to catch. If one of these "
            "deliberately wants the NEWEST gated version instead, that is a decision: record it "
            "and split this check."
        )
    return complaints


# Lines in a prose file that name a **gated** Python version for a reason OTHER than saying what
# this package supports. A classified list rather than a syntactic rule, for the reason
# `test_public_surface.py` keeps one: "we run on 3.13" and "`Queue.shutdown()` arrived in 3.13"
# are not distinguishable by the shape of the sentence, so a rule would have to guess. Anything
# neither anchored nor listed here FAILS, which makes this a roster rather than a backlog.
#
# The one entry is **precautionary and currently inert**: `X | None` names 3.10, which the sweep
# below never sees because it only looks for versions the matrix gates. It is here because the
# rulebook is where a gated-version language claim would land — `architecture.md` already carries
# one ("3.13 added `Queue.shutdown()`") and is not a swept file. The corpus proves the mechanism
# fires by classifying a gated-version line.
_NOT_A_SUPPORT_CLAIM = (
    (
        "python_rulebook",
        re.compile(r"^- ✅ \*\*Explicit `X \| None`\*\*"),
        "names when PEP 604 union syntax arrived, not what this package runs on",
    ),
    (
        "python_rulebook",
        re.compile(r"^- ✅ Prefer `Queue\.shutdown\(\)`"),
        "names when the method arrived; the corpus uses it to prove this list fires",
    ),
)


def check_version_lines_are_classified(sources: Sources, matrix: list[str]) -> list[str]:
    """Every line of a prose file naming a gated version is anchored, or classified as not a claim.

    The structural answer to a class that recurred three times: a version claim inside a *bound
    file*, on a line no anchor names. The rulebook's scope blockquote was bound while its Repo
    defaults bullet was not; that bullet's floor was then bound while its "CI gates 3.12 **and**
    3.13" was not. Each was closed by hand and the next review found the next, which is where
    CLAUDE.md says to change the mechanism rather than add another instance.

    **A floor anchor does not discharge a set claim** — that is the third instance exactly. A line
    naming anything beyond the floor must be reached by a `_PROSE_SITES` anchor, because only a set
    check reads the whole line.

    The population is lines naming a version the matrix **gates**, not every dotted number: outside
    an anchored line there is no context to tell `3.12` from `§3.1`, `timeout=5.0` or `300.18 s`,
    and a first draft using `_python_versions_in` file-wide reported all three. The cost is that a
    line left stale *after* a version stops being gated is not re-flagged; the value is at
    authoring time, where a new line naming 3.12 or 3.13 must be classified before it can pass.
    """
    floor = _floor(matrix)
    if floor is None:
        return []
    gated = set(matrix)
    fields = {attribute for _, attribute, _ in _PROSE_SITES} | {
        attribute for _, attribute, _ in _FLOOR_SITES
    }
    complaints = []
    for attribute in sorted(fields):
        exempt = [p for f, p, _ in _NOT_A_SUPPORT_CLAIM if f == attribute]
        set_anchors = [a for _, f, a in _PROSE_SITES if f == attribute]
        floor_anchors = [a for _, f, a in _FLOOR_SITES if f == attribute]
        for line in getattr(sources, attribute).splitlines():
            stated = {v for v in _VERSION.findall(line) if v in gated}
            if not stated or any(p.search(line) for p in exempt):
                continue
            anchors = set_anchors if stated != {floor} else set_anchors + floor_anchors
            if any(a.search(line) for a in anchors):
                continue
            complaints.append(
                f"{attribute}: this line names Python {_show(stated)} and no "
                f"{'set ' if stated != {floor} else ''}anchor reaches it, so it can drift alone:\n"
                f"  {line.strip()}\n"
                "  Bind it — `_PROSE_SITES` for a set claim, `_FLOOR_SITES` for a floor — or, if "
                "it names the version for some other reason (when a language feature arrived, "
                "say), classify it in `_NOT_A_SUPPORT_CLAIM` with that reason. A floor anchor "
                "does not discharge a set claim: only a set check reads the whole line."
            )
    return complaints


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
        stated = [m.group("v") for m in anchor.finditer(getattr(sources, attribute))]
        if not stated:
            complaints.append(
                f"{label}: no `>= MAJOR.MINOR` floor found by {anchor.pattern!r}. Re-anchor it "
                "rather than dropping the check."
            )
        wrong = sorted({v for v in stated if v != floor})
        if wrong:
            complaints.append(
                f"{label} claims a floor of Python {wrong}; the lowest version ci.yml "
                f"gates is {floor}."
            )
    return complaints


def check_readme_check_names(sources: Sources, matrix: list[str]) -> list[str]:
    """The `<caller> / test (pyX.Y)` names the README records are the matrix's.

    These are check-run names, which is what a branch-protection rule matches on. `main`'s ruleset
    requires no status checks today (`ci.yml` says so, having checked the ruleset rather than
    inferred it from the review requirement), so nothing breaks on the remote when the matrix
    moves. What the README holds is the string that would have to be required if one ever were,
    and no file here can read that setting to correct it after the fact.
    """
    named = {m.group("v") for m in _CHECK_NAME.finditer(sources.readme)}
    if not named:
        return [
            (
                "README.md names no `<caller> / test (pyX.Y)` check run. It recorded how the "
                "gate is named on `main` when this was written; re-anchor rather than drop."
            )
        ]
    if named != set(matrix):
        return [
            (
                f"README.md names the check runs Python {_show(named)}; ci.yml gates "
                f"{_show(matrix)}. Changing the matrix renames these check runs. `main` requires "
                "no status checks today, so nothing breaks on the remote — but if one is ever "
                "made required it is this string, and no file here can see that setting."
            )
        ]
    return []


def check_name_suffix(sources: Sources, matrix: list[str]) -> list[str]:
    """`ci.yml`'s matrix job names itself from the matrix — the SUFFIX of the check name."""
    if not _JOB_NAME_TEMPLATE.search(sources.ci):
        return [
            (
                "ci.yml's test job no longer names itself `test (py${{ matrix.python-version }})`. "
                "The branch-protection check names are derived from the matrix through that template; "
                "a literal name breaks the derivation `check_readme_check_names` relies on."
            )
        ]
    return []


def check_name_prefix(sources: Sources, matrix: list[str]) -> list[str]:
    """The README's check-name prefix is the id of the `release.yml` job that calls `ci.yml`.

    On `main` the gate runs as a reusable call, so the check run is named `<caller job id> / <called
    job name>`. Rename that job in `release.yml` and every check run on `main` is renamed with it,
    falsifying the README and moving the string any future branch-protection rule would have to
    match — with nothing else in the repo noticing.
    """
    callers = {
        m.group("job") for m in _CI_CALLER_JOB.finditer(sources.workflows.get("release.yml", ""))
    }
    if not callers:
        return [
            (
                "release.yml has no job that `uses: ./.github/workflows/ci.yml`. That call is what "
                "runs the gate on `main` and what gives the check runs their `<job> / ` prefix; "
                "re-anchor `_CI_CALLER_JOB` rather than dropping this."
            )
        ]
    named = {m.group("caller") for m in _CHECK_NAME.finditer(sources.readme)}
    if named and named != callers:
        return [
            (
                f"README.md names the check runs {sorted(named)}`/ test (pyX.Y)`, but the "
                f"release.yml job that calls ci.yml is {sorted(callers)}. The check-run name is "
                "`<caller job id> / <called job name>`, so renaming that job renames every check "
                "run on `main` — silently, and out from under any rule that comes to require one."
            )
        ]
    return []


_CHECKS: dict[str, Callable[[Sources, list[str]], list[str]]] = {
    "matrix": check_matrix,
    "setup_python_consumes_matrix": check_setup_python_consumes_matrix,
    "classifier_versions": check_classifier_versions,
    "classifier_majors": check_classifier_majors,
    "requires_python_floor": check_requires_python_floor,
    "mypy_python_version": check_mypy_python_version,
    "ruff_target_version": check_ruff_target_version,
    "workflow_pins": check_workflow_pins,
    "prose_versions": check_prose_versions,
    "version_lines_are_classified": check_version_lines_are_classified,
    "prose_floor": check_prose_floor,
    "readme_check_names": check_readme_check_names,
    "name_suffix": check_name_suffix,
    "name_prefix": check_name_prefix,
}


_MATRIX_UNREADABLE = (
    "Deferred: ci.yml's `python-version` matrix could not be read, so there is no authority to "
    "check this site against — see the `matrix` check for the reason and the fix. Nothing about "
    "this site is asserted to be wrong."
)


def _complaints(name: str, sources: Sources) -> list[str]:
    """Run one check, deferring rather than adjudicating against an unreadable authority.

    Without this, an unparseable matrix makes every downstream check compare its site against an
    empty set and say so — "ci.yml gates (none)", "the lowest version ci.yml gates is None" —
    which is loud, which is right, and which is also false: ci.yml plainly still gates the
    versions in it. One true message and fourteen lies is worse than one true message, because
    the true one is then just another line in the output.

    Args:
      name: The check to run.
      sources: The text to run it over.

    Returns:
      That check's complaints.

    Raises:
      None.
    """
    matrix = _matrix(sources)
    if not matrix and name != "matrix":
        return [_MATRIX_UNREADABLE]
    return _CHECKS[name](sources, matrix)


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
    workflows = sorted(_WORKFLOWS.glob("*.yml")) + sorted(_WORKFLOWS.glob("*.yaml"))
    if not workflows:
        pytest.fail(f"No workflows found under {_WORKFLOWS.relative_to(_ROOT)}.")
    return Sources(
        ci=_read(_WORKFLOWS / "ci.yml"),
        pyproject=_read(_ROOT / "pyproject.toml"),
        readme=_read(_ROOT / "README.md"),
        claude=_read(_ROOT / "CLAUDE.md"),
        python_rulebook=_read(_ROOT / "docs" / "best-practices" / "python" / "python.md"),
        bp_index=_read(_ROOT / "docs" / "best-practices" / "INDEX.md"),
        workflows={path.name: _read(path) for path in workflows},
    )


@pytest.mark.parametrize("name", sorted(_CHECKS), ids=sorted(_CHECKS))
def test_the_repo_agrees_with_the_ci_matrix(name: str) -> None:
    """Every site in this repository states the versions `ci.yml` actually gates."""
    complaints = _complaints(name, _repo_sources())
    assert not complaints, "\n".join(complaints)


# --------------------------------------------------------------------------------------------
# The fixture corpus.
#
# Synthetic on purpose. Deriving it from the live files would make the expectation come from the
# thing under test, and would tie proving the gate works to the repository being in a state the
# gate accepts. These fixtures are minimal, green by construction, and mutated below.
#
# A mutant only proves a guard exists if the guarded and unguarded code behave DIFFERENTLY on it.
# Several here are shaped for that and would be weaker written the obvious way: the unquoted-matrix
# case is only PARTLY unquoted (fully unquoted, both versions return `[]`), the duplicated row's
# copies disagree (identical copies, taking the first is indistinguishable from refusing), and each
# `_VERSION` lookaround is exercised on the ANCHORED line with a version the matrix does not gate
# (anywhere else, or with one it does, the scan agrees either way).
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
        "      - name: Set up Python\n"
        "        uses: actions/setup-python@abc\n"
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
        "\n"
        "[tool.ruff]\n"
        "line-length = 100\n"
        "\n"
        "[tool.mypy]\n"
        'python_version = "3.12"\n'
        "strict = true\n"
    ),
    readme=(
        "## Requirements\n"
        "\n"
        "- **Python ≥ 3.12** — the full gate (ruff, mypy, pytest) runs on 3.12 and 3.13 in CI.\n"
        "\n"
        "| Check | Does | When | Fails the build |\n"
        "|---|---|---|---|\n"
        "| [`ci.yml`](.github/workflows/ci.yml) | ruff → mypy → pytest, on 3.12 **and** 3.13 "
        "| every PR | yes |\n"
        "\n"
        "On main the checks are named `test / test (py3.12)` and `test / test (py3.13)`.\n"
        "\n"
        "```bash\n"
        "poetry install --with dev      # set up (Python 3.12+)\n"
        "poetry run pytest              # test\n"
        "```\n"
    ),
    claude=(
        "| Layer | Tech |\n"
        "|---|---|\n"
        "| Language | Python **>= 3.12**, fully typed (PEP 561 `py.typed`) — CI gates on "
        "3.12 **and** 3.13 |\n"
        "| Packaging | Poetry |\n"
    ),
    python_rulebook=(
        "# Python Best Practices — Agent Reference\n"
        "\n"
        "> Rulebook for agents writing Python in **log-foundry** (runtime **Python ≥ 3.12**, the "
        "floor in `pyproject.toml`; CI gates 3.12 **and** 3.13, so a 3.13-only call is a bug "
        "here). Distilled from **PEP 8** (+ PEP 257, PEP 484/526).\n"
        "\n"
        "## How to use this doc\n"
        "- Repo defaults: format/lint with **`ruff`**, type-check with **`mypy --strict`**. "
        "Runtime floor = **3.12** (`requires-python`); CI gates 3.12 **and** 3.13.\n"
        "- ✅ **Explicit `X | None`** for nullable args (3.10+ union syntax).\n"
    ),
    bp_index=(
        "| Domain | Doc | Load when you are… |\n"
        "|---|---|---|\n"
        "| Python (3.12+; PEP 8 + Google Python Style Guide) | `python/python.md` | writing "
        "Python |\n"
    ),
    workflows={
        "ci.yml": (
            '        python-version: ["3.12", "3.13"]\n'
            "          python-version: ${{ matrix.python-version }}\n"
        ),
        "release.yml": (
            "jobs:\n"
            "  # Gate: reuse the existing CI so a publish can never run on a red build.\n"
            "  test:\n"
            "    uses: ./.github/workflows/ci.yml\n"
            "\n"
            "  build:\n"
            "    steps:\n"
            "      - name: Set up Python\n"
            "        with:\n"
            '          python-version: "3.12"\n'
        ),
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


_BULLET = "runs on 3.12 and 3.13 in CI."

# (label, the check that must complain or None for a silence case, the mutated sources, a substring
# the complaint must contain). The substring is the point: a check that reddens for the wrong
# reason is not the check the corpus claims to prove, and it is what makes several of these
# discriminating — degrade `_one_line`'s `!= 1` to `< 1` and the duplicated-row case still reddens,
# but with a different message, which fails here.
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
        "matrix gains 3.14, seen from the check-run names",
        "readme_check_names",
        _mutate(ci=('["3.12", "3.13"]', '["3.12", "3.13", "3.14"]')),
        "names the check runs Python ['3.12', '3.13']",
    ),
    (
        "matrix drops 3.12, raising the floor",
        "requires_python_floor",
        _mutate(ci=('["3.12", "3.13"]', '["3.13"]')),
        "`requires-python` admits Python 3.12",
    ),
    (
        "matrix drops 3.12, seen from mypy's target",
        "mypy_python_version",
        _mutate(ci=('["3.12", "3.13"]', '["3.13"]')),
        "`[tool.mypy] python_version` is 3.12",
    ),
    (
        "matrix drops 3.12, seen from the prose floor",
        "prose_floor",
        _mutate(ci=('["3.12", "3.13"]', '["3.13"]')),
        "claims a floor of Python ['3.12']",
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
        "expected exactly one `python-version: [...]` matrix, found 0",
    ),
    (
        "matrix PARTLY unquoted, which YAML reads as a float",
        "matrix",
        _mutate(ci=('["3.12", "3.13"]', '["3.12", 3.13]')),
        "are not quoted MAJOR.MINOR",
    ),
    (
        "matrix lists a version twice",
        "matrix",
        _mutate(ci=('["3.12", "3.13"]', '["3.12", "3.12"]')),
        "has a duplicate",
    ),
    (
        "setup-python pinned to a literal, so the matrix is installed by nobody",
        "setup_python_consumes_matrix",
        _mutate(ci=("python-version: ${{ matrix.python-version }}", 'python-version: "3.12"')),
        "matrix that no step consumes",
    ),
    (
        "job name hardcoded, so the check-name suffix stops being derived",
        "name_suffix",
        _mutate(ci=("name: test (py${{ matrix.python-version }})", "name: test (py3.12)")),
        "no longer names itself",
    ),
    (
        "the release.yml job that calls ci.yml is renamed",
        "name_prefix",
        _mutate(workflows=("  test:\n    uses:", "  gate:\n    uses:")),
        "release.yml job that calls ci.yml is ['gate']",
    ),
    (
        "release.yml stops calling ci.yml at all",
        "name_prefix",
        _mutate(workflows=("    uses: ./.github/workflows/ci.yml\n", "")),
        "no job that `uses: ./.github/workflows/ci.yml`",
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
        "classifiers absent, seen from the majors check",
        "classifier_majors",
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
        "mypy's target raised to the newest gated version",
        "mypy_python_version",
        _mutate(pyproject=('python_version = "3.12"', 'python_version = "3.13"')),
        "mypy must check against the FLOOR",
    ),
    (
        "mypy's target deleted",
        "mypy_python_version",
        _mutate(pyproject=('python_version = "3.12"\n', "")),
        "found 0",
    ),
    (
        "ruff given a target-version off the floor",
        "ruff_target_version",
        _mutate(pyproject=("line-length = 100", 'line-length = 100\ntarget-version = "py313"')),
        "`target-version` is ['py313']",
    ),
    (
        "a workflow pin moves off the floor",
        "workflow_pins",
        _mutate(workflows=('python-version: "3.12"', 'python-version: "3.13"')),
        "not the lowest gated version (3.12)",
    ),
    (
        "a workflow pin is unquoted, so YAML reads it as a float",
        "workflow_pins",
        _mutate(workflows=('python-version: "3.12"', "python-version: 3.13")),
        "cannot read",
    ),
    (
        "every workflow pin removed, so the sweep finds nothing",
        "workflow_pins",
        _mutate(workflows=('          python-version: "3.12"\n', "")),
        "empty sweep",
    ),
    (
        "README Requirements bullet drops a version",
        "prose_versions",
        _mutate(readme=(_BULLET, "runs on 3.12 in CI.")),
        "Requirements bullet names Python ['3.12']",
    ),
    (
        "README Requirements floor raised while its set stays right",
        "prose_floor",
        _mutate(readme=("- **Python ≥ 3.12**", "- **Python ≥ 3.13**")),
        "claims a floor of Python ['3.13']",
    ),
    (
        "README ci.yml table row drops a version",
        "prose_versions",
        _mutate(readme=("on 3.12 **and** 3.13 ", "on 3.12 ")),
        "`ci.yml` row of the Continuous integration table names Python ['3.12']",
    ),
    (
        "README ci.yml table row de-anchored",
        "prose_versions",
        _mutate(readme=("| [`ci.yml`](.github/workflows/ci.yml) |", "| ci.yml |")),
        "expected exactly one line matching",
    ),
    (
        "README ci.yml table row duplicated, the copies disagreeing",
        "prose_versions",
        _mutate(
            readme=(
                "| [`ci.yml`](.github/workflows/ci.yml) | ruff",
                (
                    "| [`ci.yml`](.github/workflows/ci.yml) | on 3.12 only | every PR | yes |\n"
                    "| [`ci.yml`](.github/workflows/ci.yml) | ruff"
                ),
            )
        ),
        "found 2",
    ),
    (
        "README check-run names lose one leg",
        "readme_check_names",
        _mutate(
            readme=("`test / test (py3.12)` and `test / test (py3.13)`", "`test / test (py3.12)`")
        ),
        "names the check runs Python ['3.12']",
    ),
    (
        "README check-run names respelled past their anchor",
        "readme_check_names",
        _mutate(
            readme=(
                "`test / test (py3.12)` and `test / test (py3.13)`",
                "`test (3.12)` and `test (3.13)`",
            )
        ),
        "names no `<caller> / test (pyX.Y)` check run",
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
        "claims a floor of Python ['3.13']",
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
    (
        "the Python rulebook's scope blockquote drops a version",
        "prose_versions",
        _mutate(
            python_rulebook=(
                "CI gates 3.12 **and** 3.13, so a 3.13-only call is a bug here",
                "CI gates 3.12 only",
            )
        ),
        "rulebook's scope blockquote names Python ['3.12']",
    ),
    (
        "the Python rulebook's floor is raised while its set stays right",
        "prose_floor",
        _mutate(python_rulebook=("**Python ≥ 3.12**", "**Python ≥ 3.13**")),
        "rulebook's scope blockquote claims a floor of Python ['3.13']",
    ),
    (
        "the Python rulebook's scope blockquote is deleted",
        "prose_versions",
        _mutate(python_rulebook=("> Rulebook for agents", "Rulebook for agents")),
        "expected exactly one line matching",
    ),
    (
        "the rulebook's SECOND floor statement drifts while its first stays right",
        "prose_floor",
        _mutate(python_rulebook=("Runtime floor = **3.12**", "Runtime floor = **3.13**")),
        "Repo defaults bullet claims a floor of Python ['3.13']",
    ),
    (
        "the rulebook's Repo defaults bullet loses its floor",
        "prose_floor",
        _mutate(python_rulebook=("Runtime floor = **3.12**", "Runtime floor = the pyproject one")),
        "Repo defaults bullet: no `>= MAJOR.MINOR` floor found",
    ),
    (
        "a prose file gains an unanchored line naming a gated version",
        "version_lines_are_classified",
        _mutate(claude=("| Packaging | Poetry |", "| Runtime | smoke-tested on 3.13 too |")),
        "this line names Python ['3.13'] and no set anchor reaches it",
    ),
    (
        "a SET claim lands on a line only a FLOOR anchor reaches",
        "version_lines_are_classified",
        _mutate(
            bp_index=(
                "| Python (3.12+; PEP 8 + Google Python Style Guide)",
                "| Python (3.12+; PEP 8) | CI gates 3.12 **and** 3.13",
            )
        ),
        "no set anchor reaches it",
    ),
    (
        "the best-practices router claims a floor above the matrix",
        "prose_floor",
        _mutate(bp_index=("| Python (3.12+;", "| Python (3.13+;")),
        "router table claims a floor of Python ['3.13']",
    ),
    (
        "the best-practices router row is de-anchored",
        "prose_floor",
        _mutate(bp_index=("| Python (3.12+;", "| Py (3.12+;")),
        "router table: no `>= MAJOR.MINOR` floor found",
    ),
    (
        "README's `poetry install` line claims a floor above the matrix",
        "prose_floor",
        _mutate(readme=("# set up (Python 3.12+)", "# set up (Python 3.13+)")),
        "`poetry install` line of the Development section claims a floor of Python ['3.13']",
    ),
    (
        "a SECOND copy of a floor claim drifts while the first stays right",
        "prose_floor",
        _mutate(
            readme=(
                "poetry run pytest              # test\n",
                "poetry install --with security # extras (Python 3.13+)\n",
            )
        ),
        "`poetry install` line of the Development section claims a floor of Python ['3.13']",
    ),
    # Silence cases. Roughly half a gate's regressions are false positives, so the corpus has to
    # prove what it does NOT complain about. The three `_VERSION` ones each sit on the ANCHORED
    # line and name a version the matrix does not gate, so deleting the lookaround they guard
    # turns them red -- which is what makes them a proof rather than a hope.
    (
        "SILENCE: prose reworded around the same numbers",
        None,
        _mutate(readme=(_BULLET, "is exercised on 3.12 and 3.13, both of them.")),
        "",
    ),
    (
        "SILENCE: the sentence ENDS on a version, with no trailing word",
        None,
        _mutate(readme=(_BULLET, "is exercised on 3.12 and 3.13.")),
        "",
    ),
    (
        "SILENCE: a longer dotted version on the anchored line (leading lookbehind)",
        None,
        _mutate(readme=(_BULLET, _BULLET + " Pinned beside boto3 1.3.14.")),
        "",
    ),
    (
        "SILENCE: a PATCH version on the anchored line (trailing `(?!\\.\\d)`)",
        None,
        _mutate(readme=(_BULLET, _BULLET + " Not yet CPython 3.14.0.")),
        "",
    ),
    (
        "SILENCE: a free-threaded suffix on the anchored line (trailing `(?!\\w)`)",
        None,
        _mutate(readme=(_BULLET, _BULLET + " No 3.14t leg yet.")),
        "",
    ),
    (
        "SILENCE: a tool version below the gated major shares the CLAUDE.md row",
        None,
        _mutate(claude=("(PEP 561 `py.typed`)", "(PEP 561 `py.typed`, ruff 0.16, mypy 2.3)")),
        "",
    ),
    (
        "SILENCE: a second `- **Python ...` bullet that states no version",
        None,
        _mutate(
            readme=(
                "- **Python ≥ 3.12**",
                ("- **Python free-threading builds** are not supported yet.\n- **Python ≥ 3.12**"),
            )
        ),
        "",
    ),
    (
        "SILENCE: a GATED version named as language history, and classified",
        None,
        _mutate(
            python_rulebook=(
                "## How to use this doc\n",
                "## How to use this doc\n- ✅ Prefer `Queue.shutdown()` where available (3.13+).\n",
            )
        ),
        "",
    ),
    (
        "SILENCE: a classifier that is not a version claim is added",
        None,
        _mutate(pyproject=('    "Programming Language :: Python :: 3",', '    "Typing :: Typed",')),
        "",
    ),
    (
        "SILENCE: ruff is given the CORRECT target-version",
        None,
        _mutate(pyproject=("line-length = 100", 'line-length = 100\ntarget-version = "py312"')),
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
    """Every corpus mutant reddens the check that owns it, and says why.

    Not exclusivity: a real drift reddens several checks at once, which is the point of a roster.
    What is asserted is that the owning check is among them and that its message names the actual
    condition — a check that reddens for a different reason is not the check the corpus proves.
    """
    complaints = {name: _complaints(name, sources) for name in _CHECKS}
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


def test_an_unreadable_matrix_defers_rather_than_accusing_every_site() -> None:
    """With no authority to check against, the other twelve checks say so rather than assert.

    Before this, an unparseable matrix produced one true complaint and twelve false ones — "ci.yml
    gates (none)", "the lowest version ci.yml gates is None" — about a file that plainly still
    listed its versions. The true message was then just another line in the output.
    """
    sources = _mutate(ci=('python-version: ["3.12", "3.13"]', "python-version: []"))
    complaints = {name: _complaints(name, sources) for name in _CHECKS}

    assert complaints["matrix"] and "matrix is empty" in complaints["matrix"][0], complaints[
        "matrix"
    ]
    others = {name: found for name, found in complaints.items() if name != "matrix"}
    assert all(found == [_MATRIX_UNREADABLE] for found in others.values()), (
        f"These checks adjudicated a site against an authority that could not be read: "
        f"{ {n: c for n, c in others.items() if c != [_MATRIX_UNREADABLE]} }"
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
