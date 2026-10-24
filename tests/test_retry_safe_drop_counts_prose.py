"""SPEC-059 FR-006 — the claims the spec made false stay restated, where they were written.

Each pin runs over whitespace-collapsed text rather than lines: a grep matches within a line, so a
claim wrapped across the 100-column limit is invisible to it, and the old wording could come back
split over two lines with a line-based check still green.

The docstring pin has two halves that fail independently. The positive half requires one fixed
phrase in every docstring the spec lists, which none of them carried before the restatement; the
negative half bans the exact retired wording, each phrase searched in the file it lived in. A pin
coarser than that — "contains *only when*" — was satisfied by text already present before the
spec, so it could not tell the restatement from its absence.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import log_foundry

_SRC = Path(log_foundry.__file__).parent
_SINKS = _SRC / "sinks"
_REPO = Path(__file__).resolve().parent.parent

_ON_RETURN = "counted and announced only when ``emit`` returns"
"""The phrase every restated docstring carries verbatim, so one pin reads them all."""

_LISTED = [
    ("_socket.py", "SocketTransport"),
    ("_socket.py", "SocketTransport.send_all"),
    ("_socket.py", "SocketTransport._sendable"),
    ("http.py", "HTTPSink.emit"),
    ("http.py", "HTTPSink._drop_refused"),
    ("kinesis.py", "KinesisSink._records"),
    ("firehose.py", "FirehoseSink._records"),
    ("sns.py", "SNSSink._bodies"),
    ("mongodb.py", "MongoDBSink._documents"),
    ("mongodb.py", "MongoDBSink.emit"),
    ("eventhubs.py", "AzureEventHubsSink._send_batch"),
    ("eventhubs.py", "AzureEventHubsSink.emit"),
    ("kafka.py", "KafkaSink.emit"),
    ("kafka.py", "KafkaSink.losses"),
    ("pubsub.py", "GooglePubSubSink.emit"),
    ("syslog.py", "SyslogSink.emit"),
    ("logstash.py", "LogstashSink.emit"),
]
"""Every docstring FR-006 lists as saying when a drop is counted, by sink module and qualname."""

_RETIRED = [
    ("src/log_foundry/sinks/_socket.py", "Nothing here can fix it"),
    ("src/log_foundry/sinks/_socket.py", "belongs one level up"),
    (
        "src/log_foundry/sinks/_socket.py",
        "Everywhere else ``dropped`` is an exact count, so the exception matters",
    ),
    ("src/log_foundry/sinks/http.py", "keep counting as they happen"),
    ("src/log_foundry/sinks/http.py", "a ``413`` drop is counted when it happens"),
    ("src/log_foundry/sinks/kafka.py", "A partial refusal is counted and left alone"),
    ("src/log_foundry/sinks/multi.py", "not yet deferring"),
    ("src/log_foundry/sinks/_deferral.py", "an immediate one from ``_drop_refused``"),
    ("src/log_foundry/sinks/_deferral.py", "one helper serves five owner types"),
    ("tests/test_deferral.py", "five sites, one helper"),
    ("docs/invariants.md", "nine sinks"),
    ("docs/architecture.md", "nine sinks"),
    ("README.md", "refused outright"),
]
"""The wording FR-006 retires, each with the file it lived in before SPEC-059."""


def _collapsed(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").split())


def _docstring(path: Path, qualname: str) -> str:
    """The collapsed docstring of a class or function in ``path``, found by dotted qualname."""
    nodes: list[ast.stmt] = ast.parse(path.read_text(encoding="utf-8")).body
    found: ast.ClassDef | ast.FunctionDef | None = None
    for part in qualname.split("."):
        found = next(
            node
            for node in nodes
            if isinstance(node, ast.ClassDef | ast.FunctionDef) and node.name == part
        )
        nodes = found.body
    assert found is not None
    return " ".join((ast.get_docstring(found) or "").split())


@pytest.mark.parametrize(("module", "qualname"), _LISTED, ids=[q for _, q in _LISTED])
def test_every_listed_docstring_says_the_drop_counts_on_return(module: str, qualname: str) -> None:
    """The positive half: each listed docstring carries the restatement's phrase verbatim."""
    assert _ON_RETURN in _docstring(_SINKS / module, qualname), (module, qualname)


@pytest.mark.parametrize(("module", "qualname"), _LISTED, ids=[q for _, q in _LISTED])
def test_no_listed_docstring_says_the_drop_counts_as_it_happens(module: str, qualname: str) -> None:
    """None of the listed docstrings says its drop is counted as it happens.

    A regression guard, not evidence of the restatement: before SPEC-059 only ``HTTPSink.emit``
    used the wording ("as they happen"), so the other rows were already silent. The positive half
    above is what tells the restated docstrings from the old ones.
    """
    doc = _docstring(_SINKS / module, qualname)
    assert "as it happens" not in doc and "as they happen" not in doc, (module, qualname)


@pytest.mark.parametrize(("where", "phrase"), _RETIRED, ids=[p for _, p in _RETIRED])
def test_the_retired_wording_is_gone(where: str, phrase: str) -> None:
    """The negative half: each claim SPEC-059 made false is absent from the file that carried it."""
    assert phrase not in _collapsed(_REPO / where), f"{phrase!r} is back in {where}"


def test_the_readme_says_a_batch_refused_whole_is_counted_by_the_worker() -> None:
    """The README's ``sink.dropped`` row (the PyPI page too) names each catcher of a whole refusal.

    Three code paths catch a raising ``emit`` and count its batch: the worker, the orphan path and
    a ``MultiSink`` whose other children delivered, which absorbs the raise rather than passing it
    to the worker, so a row naming only the first two sends a fan-out's reader to a field that
    never moves.
    """
    rows = [
        line
        for line in (_REPO / "README.md").read_text(encoding="utf-8").splitlines()
        if line.startswith("| `sink.dropped` |")
    ]
    assert len(rows) == 1, rows
    row = " ".join(rows[0].split())
    assert "refused **whole** is not counted here" in row, row
    assert "in `failed_batches` by the worker once its retries are spent" in row, row
    assert "in `orphan_lost` outside a span" in row, row
    assert "in the fan-out's `sink.failed` when a `MultiSink` sibling delivered" in row, row


def test_invariant_2_states_the_rule_at_every_shipped_site() -> None:
    """Section 2 states the rule at every shipped site, keeps ``failed``'s exception, is guarded."""
    page = (_REPO / "docs/invariants.md").read_text(encoding="utf-8")
    section = page.split("\n## 2. ", 1)[1].split("\n## 3. ", 1)[0]
    text = " ".join(section.split())
    assert (
        "At every shipped site that decides a per-event drop, a `dropped` count is made only by an "
        "`emit` that returned" in text
    )
    assert "The exception is a sink's own `failed` count, the upper bound above" in text
    guarded = text.split("*Guarded:*", 1)[1]
    assert "`tests/test_deferral.py`" in guarded, guarded
