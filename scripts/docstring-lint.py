#!/usr/bin/env python3
"""docstring-lint.py — hold `src/` to the docstring rule CLAUDE.md states.

Why this exists as a script rather than a rule. Every check below was already written in
CLAUDE.md's Code Conventions, and nothing checked any of them. The part that had rotted
furthest was the sentence cap: it read "a description of <=3 sentences" while the bullet
beside it told authors that reasoning which would have been an inline comment belongs *in*
the docstring, and the code followed the second sentence. Measured at 451edf9, before
SPEC-052 re-scoped the cap: 158 of 506 documented defs exceeded it on the narrowest
reading of "description" and 492 on the widest. A rule practice violates that consistently
gets reconciled or deleted (`docs/process/completion-ritual.md`), so the cap moved to the summary line —
where the codebase already complied everywhere — and this script is what stops the
re-scoped rule going the same way.

FAIL (exit 1): a def has no docstring, a summary line is empty or multi-sentence or
  unterminated or over-long or not followed by a blank line, `src/` carries a comment that
  is not a directive, a function or method is missing one of Args/Returns/Raises, or a
  module docstring is absent or runs past one line.

Deliberately NOT checked here: anything about `tests/` (CLAUDE.md scopes the rule to
`src/`), and anything `ruff` already owns. A rule with two enforcement homes gets qualified
in one of them and read from the other.

Usage: python3 scripts/docstring-lint.py     (run from anywhere; resolves its own root)
Standard library only, so it runs in the no-extras environment CI uses.
"""

from __future__ import annotations

import ast
import pathlib
import re
import sys
import tokenize

# `src/` carries PEP 695 syntax (`decorator.py`'s `type` statement), so an older
# interpreter cannot parse the package at all and every module raises. Named here rather
# than left to a SyntaxError, because the corpus's fixtures parse on 3.9 and would report
# a clean run while the gate examined nothing real. Matches `requires-python`.
MIN_PYTHON = (3, 12)

# A sentence boundary is a terminator, then whitespace, then an optional opening quote or
# bracket, then a capital or a digit. The whitespace is the load-bearing part and the
# obvious implementation omits it: counting "." characters reports 39 of 506 summary lines
# as multi-sentence, every one a false positive from a dotted name or a Sphinx role —
# ``:class:`~log_foundry.sinks.base.Sink```, ``json.dumps``, ``arch section 9.2``. A dot
# with nothing after it never splits.
ABBREVIATION = re.compile(r"\b(?:e\.g|i\.e|cf|vs|etc|Dr|Mr|Ms|St|approx|resp)\.$")
SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[\"'`(\[]?[A-Z0-9])")

# Only the four the trio check consults. Listing sections nobody reads back would advertise
# a coverage this gate does not have. The trailing `$` is what stops an inline "Returns: none"
# satisfying the check from the middle of a sentence.
SECTION = re.compile(r"^(Args|Returns|Yields|Raises):$")

# A directive is a lowercase tool token followed by a colon — `type:`, `pragma:`, `ruff:`,
# `mypy:`, `fmt:`, `coding:` — plus the two that carry no colon and a shebang. Matched by
# SHAPE rather than enumerated: the enumeration was already wrong once by omission
# (`pragma:` was missing from CLAUDE.md's list while `src/` carried one), and a list is
# wrong again the first time anyone reaches for a tool nobody thought of. English prose
# opens on a capital, so "# Note: ..." is still caught.
DIRECTIVE = re.compile(r"^(?:!|noqa\b|nosec\b|[a-z][a-z0-9_-]*:)")

# Two module docstrings run to several lines because tests assert their text. Exempting
# them by path is deliberate: the alternative is exempting "long ones", which exempts the
# next accidental one too.
EXEMPT_MULTILINE_MODULE_DOC = frozenset({"_diag.py", "sinks/sqs.py"})

SUMMARY_MAX_CHARS = 100

DEF_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
CALLABLE_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)


def sentence_count(text: str) -> int:
    """Counts sentences in a single line of docstring text.

    Args:
      text: One line, leading and trailing whitespace insignificant.

    Returns:
      The number of sentences, 0 for an empty line.

    Raises:
      None.
    """
    joined = " ".join(text.split())
    if not joined:
        return 0
    parts = [part for part in SENTENCE_SPLIT.split(joined) if part.strip()]
    merged = 1
    for index in range(1, len(parts)):
        if not ABBREVIATION.search(parts[index - 1]):
            merged += 1
    return merged


def is_overload(node: ast.AST) -> bool:
    """True when a def carries an ``@overload`` decorator.

    An overload stub is a signature with no body, and the implementation below it carries
    the docstring — so requiring one here would demand prose nobody reads.

    Args:
      node: Any AST node; a non-def answers False.

    Returns:
      Whether the node is a def decorated with ``overload`` or ``typing.overload``.

    Raises:
      None.
    """
    if not isinstance(node, CALLABLE_NODES):
        return False
    for dec in node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == "overload":
            return True
        if isinstance(dec, ast.Attribute) and dec.attr == "overload":
            return True
    return False


def check_summary(
    rel: str, node: ast.AST, doc: str | None, raw: str | None
) -> list[tuple[str, int, str]]:
    """Checks that a def has a docstring and that its summary line is well-formed.

    The empty-first-line case needs the RAW docstring, not the cleaned one:
    ``ast.get_docstring(clean=True)`` runs ``inspect.cleandoc``, which strips a leading
    blank line, so a docstring opening on ``\"\"\"`` alone is indistinguishable from a
    well-formed one by the time it is cleaned. The corpus caught this on its first run.

    Args:
      rel: Path of the module, relative to the package root, for the finding.
      node: The def the docstring belongs to.
      doc: Its cleaned docstring, or None when it has none.
      raw: Its uncleaned docstring, or None when it has none.

    Returns:
      A list of (path, line, message) findings, empty when the def is well-formed.

    Raises:
      None.
    """
    line = getattr(node, "lineno", 0)
    if doc is None or raw is None:
        if is_overload(node):
            return []
        return [(rel, line, "def has no docstring")]

    if not raw.split("\n")[0].strip():
        return [(rel, line, "docstring starts with an empty line, not a summary")]

    lines = doc.split("\n")
    first = lines[0].strip()
    out: list[tuple[str, int, str]] = []
    if sentence_count(first) > 1:
        out.append((rel, line, "summary line is more than one sentence"))
    if not first.endswith("."):
        out.append((rel, line, "summary line does not end in '.'"))
    if len(first) > SUMMARY_MAX_CHARS:
        out.append((rel, line, f"summary line is {len(first)} chars, over {SUMMARY_MAX_CHARS}"))
    if len(lines) > 1 and lines[1].strip():
        out.append((rel, line, "no blank line after the summary line"))
    return out


def check_sections(rel: str, node: ast.AST, doc: str | None) -> list[tuple[str, int, str]]:
    """Checks that a function or method documents Args, Returns and Raises.

    Classes are exempt: their sections are ``Attributes:``, and all but one of the classes
    in this package carry none of the callable trio — 58 of 59 when SPEC-052 measured it at
    ``451edf9``. The exemption rests on the shape a class docstring takes, not on that
    count, which is why no current one is stated. ``Yields:`` stands in for ``Returns:``.

    Args:
      rel: Path of the module, relative to the package root, for the finding.
      node: The def the docstring belongs to.
      doc: Its cleaned docstring, or None when it has none.

    Returns:
      A list of (path, line, message) findings, one per missing section.

    Raises:
      None.
    """
    if doc is None or not isinstance(node, CALLABLE_NODES) or is_overload(node):
        return []
    have = {m.group(1) for m in (SECTION.match(ln.strip()) for ln in doc.split("\n")) if m}
    out = []
    for want in ("Args", "Returns", "Raises"):
        if want in have:
            continue
        if want == "Returns" and "Yields" in have:
            continue
        out.append((rel, node.lineno, f"missing '{want}:' section"))
    return out


def check_module_doc(rel: str, tree: ast.Module) -> list[tuple[str, int, str]]:
    """Checks that a module has a docstring and that it is one line.

    Args:
      rel: Path of the module, relative to the package root, for the finding.
      tree: The parsed module.

    Returns:
      A list of (path, line, message) findings, empty when the module complies.

    Raises:
      None.
    """
    doc = ast.get_docstring(tree, clean=True)
    if doc is None:
        return [(rel, 1, "module has no docstring")]
    if "\n" in doc.strip() and rel not in EXEMPT_MULTILINE_MODULE_DOC:
        message = (
            "module docstring is more than one line "
            "(add the path to EXEMPT_MULTILINE_MODULE_DOC only if a test asserts it)"
        )
        return [(rel, 1, message)]
    return []


def check_comments(rel: str, path: pathlib.Path) -> list[tuple[str, int, str]]:
    """Checks that a module carries no comment that is not a directive.

    Args:
      rel: Path of the module, relative to the package root, for the finding.
      path: The module on disk, opened here rather than passed as text so the checker
        needs no ``io`` import.

    Returns:
      A list of (path, line, message) findings, one per prose comment.

    Raises:
      None.
    """
    out = []
    with path.open(encoding="utf-8") as handle:
        for tok in tokenize.generate_tokens(handle.readline):
            if tok.type != tokenize.COMMENT:
                continue
            body = tok.string.lstrip("#").strip()
            if not DIRECTIVE.match(body):
                out.append((rel, tok.start[0], "comment in src/ that is not a directive"))
    return out


def main() -> int:
    """Lints every module under ``src/log_foundry`` and reports what fails.

    Args:
      None.

    Returns:
      0 when every module complies, 1 on any finding and on an empty run — a run that
      examined nothing must not read as a clean one.

    Raises:
      None.
    """
    if sys.version_info < MIN_PYTHON:
        want = ".".join(str(part) for part in MIN_PYTHON)
        print(f"docstring-lint: needs Python >= {want}, got {sys.version.split()[0]}.")
        print("Run it as `poetry run python scripts/docstring-lint.py`.")
        return 1
    root = pathlib.Path(__file__).resolve().parent.parent / "src" / "log_foundry"
    findings: list[tuple[str, int, str]] = []
    modules = 0
    defs = 0

    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        modules += 1
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        findings += check_module_doc(rel, tree)
        findings += check_comments(rel, path)
        for node in ast.walk(tree):
            if not isinstance(node, DEF_NODES):
                continue
            defs += 1
            doc = ast.get_docstring(node, clean=True)
            findings += check_summary(rel, node, doc, ast.get_docstring(node, clean=False))
            findings += check_sections(rel, node, doc)

    for rel, line, message in sorted(findings):
        print(f"FAIL  src/log_foundry/{rel}:{line}  {message}")
    print("----")
    if modules == 0:
        print(f"docstring-lint: examined no modules under {root} — nothing was checked.")
        return 1
    noun = "failure" if len(findings) == 1 else "failures"
    print(f"docstring-lint: {len(findings)} {noun} over {defs} defs in {modules} modules.")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
