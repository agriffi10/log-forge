"""SPEC-071 FR-004 — which sinks draw their chunk boundaries with the shared helpers, by name.

Derived from the AST of `src/log_foundry/sinks/*.py`: a module is in the set when it **calls**
`chunk_items` or `chunk_list`, bare or as an attribute. Text would not do: `http.py`'s docstrings
name `chunk_items` twice, and a `grep` puts it in a set it has no place in.

The set is compared exactly with the six on SPEC-071's head. A sink that inlines its chunker back
leaves the set and is named; converting `HTTPSink._take` or `AzureEventHubsSink`'s
`create_batch` loop, whose limits do not fit a fixed count and byte bound, joins it and is named.

Serves invariants 2 and 7, by keeping one copy of the boundary rule.
"""

from __future__ import annotations

import ast
from pathlib import Path

_SINKS = Path(__file__).resolve().parents[1] / "src" / "log_foundry" / "sinks"
"""Derived from this file, never from ``log_foundry.__file__``."""

_HELPERS = frozenset({"chunk_items", "chunk_list"})

EXPECTED = frozenset({"clickhouse", "firehose", "kinesis", "postgres", "sns", "sqs"})
"""The modules calling a shared chunk helper; ``http`` and ``eventhubs`` keep their own batching."""


def chunking_modules(sources: dict[str, str]) -> set[str]:
    """Returns the modules that call a shared chunk helper.

    Args:
      sources: Source text keyed by module stem; ``_chunk`` itself is skipped.

    Returns:
      The stems of the modules with at least one call.

    Raises:
      SyntaxError: If a source does not parse.
    """
    found: set[str] = set()
    for stem, source in sources.items():
        if stem == "_chunk":
            continue
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.id
                if isinstance(func, ast.Name)
                else func.attr
                if isinstance(func, ast.Attribute)
                else None
            )
            if name in _HELPERS:
                found.add(stem)
    return found


def _tree() -> dict[str, str]:
    return {p.stem: p.read_text(encoding="utf-8") for p in sorted(_SINKS.glob("*.py"))}


def test_the_chunking_modules_are_the_six() -> None:
    assert chunking_modules(_tree()) == EXPECTED


def test_a_sink_that_inlines_its_chunker_back_is_named() -> None:
    tree = _tree()
    assert "chunk_items(" in tree["sqs"], "the premise: sqs calls the helper"
    tree["sqs"] = tree["sqs"].replace("chunk_items(", "_inline_chunker(")
    assert EXPECTED - chunking_modules(tree) == {"sqs"}


def test_converting_http_or_eventhubs_is_named() -> None:
    tree = _tree()
    tree["http"] += (
        "\n\ndef _converted(items):\n    return list(chunk_items(items, max_count=1, max_bytes=1, size_of=len))\n"
    )
    tree["eventhubs"] += "\n\ndef _converted(rows):\n    return list(_chunk.chunk_list(rows, 10))\n"
    assert chunking_modules(tree) - EXPECTED == {"eventhubs", "http"}


def test_a_docstring_naming_the_helper_is_not_a_call() -> None:
    assert "chunk_items" in _tree()["http"], "the premise: http.py's prose names the helper"
    source = 'def f():\n    """Mirrors ``chunk_items`` and ``chunk_list``."""\n    return 1\n'
    assert chunking_modules({"m": source}) == set()
