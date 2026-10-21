"""SPEC-058 FR-006 — the claims the spec made false stay restated, in the source that carried them.

Cheap pins on the spec's own greps, each run over whitespace-collapsed text rather than lines: a
grep matches within a line, so a claim wrapped across the 100-column limit is invisible to it, and
the old wording could come back split over two lines with the line-based check still green.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import log_foundry

_SRC = Path(log_foundry.__file__).parent
_SINKS = _SRC / "sinks"


def _collapsed(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").split())


def _docstring(path: Path, qualname: str) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes: list[ast.stmt] = tree.body
    found: ast.AST | None = None
    for part in qualname.split("."):
        found = next(
            node
            for node in nodes
            if isinstance(node, ast.ClassDef | ast.FunctionDef) and node.name == part
        )
        nodes = found.body  # type: ignore[union-attr]
    return " ".join((ast.get_docstring(found) or "").split())  # type: ignore[arg-type]


_RENDER_SITES = [
    ("http.py", "HTTPSink._render"),
    ("loki.py", "LokiSink._render"),
    ("datadog.py", "DatadogSink._render"),
    ("elasticsearch.py", "ElasticsearchSink._render"),
    ("honeycomb.py", "HoneycombSink._render"),
    ("splunk.py", "SplunkHECSink._render"),
]
_ROW_SITES = [
    ("clickhouse.py", "ClickHouseSink._row"),
    ("postgres.py", "PostgresSink._row"),
    ("syslog.py", "SyslogSink._frame"),
]


def test_no_raises_line_claims_sanitize_alone_prevents_it() -> None:
    """The nine ``Raises:`` lines name both sources of the guarantee, and none says "prevents"."""
    offenders = [
        path.relative_to(_SRC).as_posix()
        for path in sorted(_SRC.rglob("*.py"))
        if re.search(r"sanitize\W*\s+prevents", _collapsed(path))
    ]
    assert offenders == [], offenders
    for module, qualname in _RENDER_SITES + _ROW_SITES:
        doc = _docstring(_SINKS / module, qualname)
        raises = doc.split("Raises:", 1)[1]
        assert "assembly" in raises and "TransformSink" in raises, (module, qualname, raises)
    for module, qualname in _RENDER_SITES:
        raises = _docstring(_SINKS / module, qualname).split("Raises:", 1)[1]
        assert "HTTPSink._items" in raises, (module, qualname, raises)


@pytest.mark.parametrize(
    "phrase", ["Whatever the mapping", "Whatever the predicate", "Whatever a caller"]
)
def test_no_wrapper_or_sqs_docstring_says_whatever_the_callable_raises(phrase: str) -> None:
    """A batch with any other event no longer fails with whatever the caller's callable raised."""
    hits = [path.name for path in sorted(_SINKS.glob("*.py")) if phrase in _collapsed(path)]
    assert hits == [], f"{phrase!r} is back in {hits}"


def test_left_to_report_itself_survives_only_struck_and_superseded() -> None:
    """SPEC-036's reversed clause stays readable in ``multi.py``, but only struck, naming SPEC-058."""
    text = _collapsed(_SINKS / "multi.py")
    struck = [(match.start(), match.end()) for match in re.finditer(r"~~.*?~~", text)]
    occurrences = [match.start() for match in re.finditer("left to report itself", text)]
    assert occurrences, "the old claim must stay readable, struck, rather than be deleted"
    for at in occurrences:
        span = next(((start, end) for start, end in struck if start <= at < end), None)
        assert span is not None, f"unstruck at {at}: {text[at - 80 : at + 40]!r}"
        assert text[span[1] : span[1] + 40].startswith(" — superseded by SPEC-058"), text[
            span[0] : span[1] + 40
        ]
