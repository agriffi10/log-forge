"""SPEC-049 FR-007 — two layout rules over ``src/``, each closed as a class rather than an instance.

`python.md` §9 forbids `@staticmethod` (write a module-level function), and a docstring
continuation line at column 0 renders wrongly in every tool that reads it. Both were found once,
in `file.py` and `postgres.py`; these sweeps hold the whole package to the rule.
"""

from __future__ import annotations

import ast
import pathlib

import log_foundry

_SRC = pathlib.Path(log_foundry.__file__).resolve().parent

# The sweeps below assert absences, and an empty roster satisfies an absence perfectly; this
# floor is what proves the walker found the package rather than an empty directory.
_MIN_DOCSTRINGS = 400


def _modules() -> list[tuple[pathlib.Path, ast.Module, list[str]]]:
    found = []
    for path in sorted(_SRC.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        found.append((path, ast.parse(text), text.splitlines()))
    return found


def _def_docstrings() -> list[tuple[str, ast.Constant, list[str]]]:
    """Every class, function and method docstring node, with its module's source lines.

    Module docstrings and module-level attribute docstrings are excluded by construction: both
    legitimately sit at column 0, which is exactly what the sweep below refuses elsewhere.
    """
    out = []
    for path, tree, lines in _modules():
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                continue
            if not node.body:
                continue
            first = node.body[0]
            if (
                isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)
            ):
                out.append((f"{path.name}::{node.name}", first.value, lines))
    return out


def test_no_staticmethod_remains_in_the_package() -> None:
    """`_rollover_seconds` was the only one, and is a module-level function now."""
    offenders = [
        f"{path.name}::{node.name}"
        for path, tree, _ in _modules()
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        for dec in node.decorator_list
        if (isinstance(dec, ast.Name) and dec.id == "staticmethod")
        or (isinstance(dec, ast.Attribute) and dec.attr == "staticmethod")
    ]
    assert offenders == [], f"python.md §9: write a module-level function instead: {offenders}"


def test_no_def_docstring_has_a_continuation_line_at_column_zero() -> None:
    """`postgres.py::_reconnect_if_broken` had one paragraph dedented to column 0."""
    docstrings = _def_docstrings()
    assert len(docstrings) >= _MIN_DOCSTRINGS, f"the walker found only {len(docstrings)} docstrings"
    offenders = []
    for name, node, lines in docstrings:
        assert node.end_lineno is not None
        for lineno in range(node.lineno + 1, node.end_lineno + 1):
            line = lines[lineno - 1]
            if line and not line[0].isspace():
                offenders.append(f"{name} line {lineno}: {line[:60]!r}")
    assert offenders == [], "a docstring continuation line at column 0 renders wrongly: " + str(
        offenders
    )
