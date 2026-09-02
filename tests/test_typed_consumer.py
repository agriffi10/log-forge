"""SPEC-051 FR-007 — the library type-checked as a consumer sees it, not as `src` sees it.

`pyproject.toml` sets `files = ["src"]`, so the repo's `mypy` gate cannot see a caller at all.
That is how `configure(defaults=)` stayed unusable from typed code for the life of the project
with the gate green: an invariant `dict` parameter is only wrong at a call site, and there were
no call sites inside `src`.

Two modules, deliberately: one that must check clean and one that must be rejected. The negative
half is what makes the positive half mean anything -- a probe that has stopped resolving
`log_foundry` reports nothing to say so.
"""

import os
import pathlib
import subprocess
import sys
import tokenize

import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_CONSUMER = _ROOT / "tests" / "typed_consumer"


def _mypy(
    path: pathlib.Path, cache: pathlib.Path, *, search: pathlib.Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Type-checks one consumer module against the library in this working tree.

    `MYPYPATH` is load-bearing rather than belt-and-braces. `sys.executable -m mypy` picks the
    interpreter running the suite, but resolution then goes through that environment's
    `log_foundry.pth`, which holds an absolute path baked in when `poetry install` last ran --
    in a fresh worktree that points at a *different* checkout, and the probe silently reports on
    a tree the branch never touched. `pytest` itself resolves via `pythonpath = ["src"]`, and
    this is how the subprocess is made to agree with it.

    Args:
      path: The consumer module to check.
      cache: A per-test cache directory, so the runs cannot share stale state.
      search: Where to resolve `log_foundry` from, defaulting to this tree's `src`. Only the
        test that proves this argument is consulted passes anything else.

    Returns:
      The finished process.

    Raises:
      None.
    """
    return subprocess.run(  # noqa: S603
        [
            sys.executable, "-m", "mypy", "--strict",
            "--python-version", "3.12",
            "--cache-dir", str(cache),
            "--no-error-summary", "--no-color-output",
            str(path),
        ],
        capture_output=True,
        text=True,
        cwd=_ROOT,
        env={**os.environ, "MYPYPATH": str(search or _ROOT / "src")},
    )


def _wants() -> list[str]:
    """Reads the planted errors out of `rejects.py`, so the corpus is the file itself.

    Read as COMMENT tokens rather than by regex: the module docstring explains the marker, and a
    regex over the raw text collected two of its own sentences as expectations.

    Args:
      None.

    Returns:
      The expected message substrings, in file order.

    Raises:
      None.
    """
    with (_CONSUMER / "rejects.py").open("rb") as handle:
        comments = [
            token.string
            for token in tokenize.tokenize(handle.readline)
            if token.type == tokenize.COMMENT
        ]
    return [
        comment.split("want:", 1)[1].strip() for comment in comments if "want:" in comment
    ]


def test_a_typed_consumer_type_checks_clean(tmp_path: pathlib.Path) -> None:
    """FR-007 AC-1. The whole exported surface, as a third party writes against it."""
    result = _mypy(_CONSUMER / "accepts.py", tmp_path / "cache")
    assert result.returncode == 0, f"the consumer no longer type-checks:\n{result.stdout}"


def test_the_probe_can_fail(tmp_path: pathlib.Path) -> None:
    """FR-007 AC-2. Every planted error is reported, by message, and no others are.

    The count matters as much as the matches. An *extra* error means the probe has drifted into
    failing for a reason nobody planted -- an unresolved import reports plenty of those, and
    would otherwise satisfy a "it was rejected" assertion on its own.
    """
    wants = _wants()
    assert len(wants) >= 13, f"the planted corpus shrank to {len(wants)} -- it cannot bite"

    result = _mypy(_CONSUMER / "rejects.py", tmp_path / "cache")
    assert result.returncode != 0, "every planted error has stopped being an error"

    reported = [line for line in result.stdout.splitlines() if ": error:" in line]
    missing = [want for want in wants if not any(want in line for line in reported)]
    assert not missing, "planted but not reported:\n" + "\n".join(missing) + \
        "\n--- mypy said ---\n" + result.stdout
    assert len(reported) == len(wants), (
        f"{len(reported)} errors reported for {len(wants)} planted:\n{result.stdout}"
    )


@pytest.mark.parametrize("name", ["accepts.py", "rejects.py"])
def test_the_consumer_modules_are_not_collected_as_tests(name: str) -> None:
    """FR-007 AC-4. They live under `tests/` and are inputs to a checker, not test modules."""
    assert (_CONSUMER / name).exists()
    assert not name.startswith("test_")
    assert not (_CONSUMER / "__init__.py").exists()


def test_the_probe_checks_the_library_in_this_tree(tmp_path: pathlib.Path) -> None:
    """FR-007 AC-3, made falsifiable rather than asserted.

    `MYPYPATH` is what pins resolution to this worktree's `src` instead of whatever absolute
    path the environment's `log_foundry.pth` was written with. Dropping it cannot be detected
    from a worktree whose own install is current -- both paths lead to the same files -- so the
    guard is proved the other way round: pointed at a library that lacks SPEC-051's additions,
    the consumer must fail, and name one of them.
    """
    stale = tmp_path / "stale"
    (stale / "log_foundry").mkdir(parents=True)
    (stale / "log_foundry" / "py.typed").touch()
    (stale / "log_foundry" / "__init__.py").write_text(
        '"""A library predating SPEC-051."""\n__all__: list[str] = []\n', encoding="utf-8"
    )

    result = _mypy(_CONSUMER / "accepts.py", tmp_path / "cache", search=stale)

    assert result.returncode != 0, "the search path is not consulted -- MYPYPATH is decorative"
    assert "DEFAULT_SWAP_TIMEOUT" in result.stdout, result.stdout
