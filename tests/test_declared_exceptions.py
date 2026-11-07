"""SPEC-070 FR-005 — the sinks' four declared exceptions to their shared rules, held to the tree.

Each row names a shared rule, the classes that depart from it, the decision that records why and
how the departure is found. Every population is derived from the tree — none is a hand list that
could shrink silently — and compared with the classes the row declares, so a fifth floor-side
timeout, a second retry-status override, a missing proof test or a fifth body reader that reads
an empty ``2xx`` differently fails here, naming its row. The table itself is the decision
``docs/decisions/sink-delivery/the-sinks-keep-four-declared-exceptions-to-their-shared-rules.md``;
the rows below are its data.

The rows change no behaviour and serve no invariant: they pin departures FR-001 to FR-004 leave
standing.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from log_foundry.sinks.http import HTTPSink
from test_sinks_http import FakeOpener
from test_sinks_http_render import _RECIPES, _shipped_http_subclasses

if TYPE_CHECKING:
    from collections.abc import Iterable

_REPO = Path(__file__).resolve().parents[1]
_SINKS = _REPO / "src" / "log_foundry" / "sinks"
_TESTS = _REPO / "tests"
_DECISIONS = _REPO / "docs" / "decisions" / "sink-delivery"
_TABLE = _DECISIONS / "the-sinks-keep-four-declared-exceptions-to-their-shared-rules.md"

_FLOORS = frozenset({"usable_timeout", "max", "floor_count"})
"""The calls that put a timeout on the floor side; anything else is a refusal or no judgement."""

_ROW_ONE_FLOOR = 4
"""Row 1's population when SPEC-070 was built. The scan sees one helper level, no more, so a
refactor that hides one of the four behind a second helper must fail here, not shrink the row."""

_PUBSUB_PROOF = "test_a_close_landing_mid_batch_counts_the_rest_unconfirmed_and_does_not_raise"


@dataclass(frozen=True)
class _Row:
    number: int
    rule: str
    declared: frozenset[str]
    departure: str
    recorded_in: str


ROWS = (
    _Row(
        1,
        "a timeout that is already broken is refused, one that works is floored",
        frozenset({"KafkaSink", "NATSSink", "GooglePubSubSink", "PostgresSink"}),
        "floor, not refuse, for 1.x",
        "a-bad-argument-is-floored-if-it-works-and-refused-if-it-is-already-broken-a-new-one-is-refused.md",
    ),
    _Row(
        2,
        "a status is retried if it is 429 or 5xx",
        frozenset({"OTLPSink"}),
        "a 500 is abandoned on the first request",
        "a-protocols-retry-set-is-a-private-predicate-hook-not-a-second-loop.md",
    ),
    _Row(
        3,
        "a sink that released its transport refuses; a close mid-batch raises nothing by itself",
        frozenset({"GooglePubSubSink"}),
        "the rest of the batch is counted unconfirmed, not raised",
        "a-sink-that-released-its-transport-refuses-one-that-released-nothing-keeps-accepting.md",
    ),
    _Row(
        4,
        "a body that cannot carry the verdict is counted",
        frozenset({"OTLPSink"}),
        "an OTLP success needs no partialSuccess, so an empty 2xx body is success",
        "a-2xx-this-sink-cannot-read-is-counted-and-the-bulk-reading-is-bulks-alone.md",
    ),
)
"""The table's rows as data: rule, departing classes, departure, the decision that records it."""


# --- the derivations -------------------------------------------------------------------------


def _callee(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _reaches_a_floor(function: ast.FunctionDef) -> bool:
    return any(
        isinstance(node, ast.Call) and _callee(node) in _FLOORS for node in ast.walk(function)
    )


def floor_side_timeouts(root: Path) -> set[str]:
    """Row 1: classes with an ``__init__`` timeout judged by a floor, not a refusal.

    For each class under ``root`` with an ``__init__``, every parameter whose name ends in
    ``timeout`` and every call receiving it positionally: ``usable_timeout``, ``max`` or
    ``floor_count`` puts the class in the row, and so does a module-level function of the same
    module whose body calls one of them (``kafka._usable_timeout``), resolved one level. A
    keyword forwarded to another constructor, a value handed to a driver unjudged, a refusal
    through ``require_timeout`` or a dict comprehension keeps it out. The stated bound: a floor
    two helpers deep, or reached through a keyword, is not seen — :data:`_ROW_ONE_FLOOR` is what
    makes that bound fail loudly.
    """
    found: set[str] = set()
    for path in sorted(root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        helpers = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
        for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
            init = next(
                (m for m in cls.body if isinstance(m, ast.FunctionDef) and m.name == "__init__"),
                None,
            )
            if init is None:
                continue
            params = {
                a.arg for a in [*init.args.args, *init.args.kwonlyargs] if a.arg.endswith("timeout")
            }
            for call in (n for n in ast.walk(init) if isinstance(n, ast.Call)):
                if not any(isinstance(a, ast.Name) and a.id in params for a in call.args):
                    continue
                name = _callee(call)
                if name in _FLOORS or (name in helpers and _reaches_a_floor(helpers[name])):
                    found.add(cls.name)
    return found


def retry_status_overrides(root: Path) -> set[str]:
    """Row 2: classes other than ``HTTPSink`` whose own body defines ``_retryable_status``."""
    found: set[str] = set()
    for path in sorted(root.glob("*.py")):
        for cls in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(cls, ast.ClassDef)
                and cls.name != "HTTPSink"
                and any(
                    isinstance(m, ast.FunctionDef) and m.name == "_retryable_status"
                    for m in cls.body
                )
            ):
                found.add(cls.name)
    return found


def _defines_test(path: Path, name: str) -> bool:
    if not path.exists():
        return False
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return any(isinstance(n, ast.FunctionDef) and n.name == name for n in tree.body)


def proof_test_classes(tests: Path) -> set[str]:
    """Row 3: the classes whose departure the named Pub/Sub test proves, if it is defined.

    "Collected" is read as "defined as a top-level test function in the file the default
    collection picks up"; a ``--collect-only`` run inside a test would be a second pytest session.
    """
    if _defines_test(tests / "test_sinks_pubsub.py", _PUBSUB_PROOF):
        return {"GooglePubSubSink"}
    return set()


def empty_body_successes(readers: Iterable[type[HTTPSink]]) -> set[str]:
    """Row 4: the body readers that settle an **empty** ``2xx`` body as success, counting nothing.

    Each reader is built from ``_RECIPES`` and handed ``b""`` for a two-event chunk; one whose
    ``losses()`` did not move and which reported the chunk delivered reads it as success.
    """
    found: set[str] = set()
    for cls in readers:
        build = _RECIPES.get(cls.__name__)
        sink: Any = build(opener=FakeOpener()) if build else cls("http://h", opener=FakeOpener())
        before = sink.losses()
        settled = sink._handle_response(b"", [object(), object()])
        if settled and sink.losses() == before:
            found.add(cls.__name__)
    return found


def _shipped_readers() -> list[type[HTTPSink]]:
    return [
        cls
        for cls in _shipped_http_subclasses()
        if cls._handle_response is not HTTPSink._handle_response
    ]


def row_findings(
    *,
    sinks: Path = _SINKS,
    tests: Path = _TESTS,
    decisions: Path = _DECISIONS,
    readers: Iterable[type[HTTPSink]] | None = None,
) -> list[str]:
    """Every way the tree disagrees with :data:`ROWS`, each naming its row."""
    derived = {
        1: floor_side_timeouts(sinks),
        2: retry_status_overrides(sinks),
        3: proof_test_classes(tests),
        4: empty_body_successes(_shipped_readers() if readers is None else readers),
    }
    findings: list[str] = []
    for row in ROWS:
        where = f"row {row.number} ({row.rule})"
        found = derived[row.number]
        if found != row.declared:
            findings.append(
                f"{where}: derived {sorted(found)} where the table declares {sorted(row.declared)}"
            )
        decision = decisions / row.recorded_in
        if not decision.exists():
            findings.append(f"{where}: its decision {row.recorded_in} does not exist")
            continue
        body = decision.read_text(encoding="utf-8")
        findings.extend(
            f"{where}: its decision {row.recorded_in} never names {cls}"
            for cls in sorted(row.declared)
            if cls not in body
        )
    if len(derived[1]) < _ROW_ONE_FLOOR:
        findings.append(
            f"row 1 ({ROWS[0].rule}): the scan found {len(derived[1])} classes, under its floor "
            f"of {_ROW_ONE_FLOOR}"
        )
    return findings


# --- the shipped tree ------------------------------------------------------------------------


def test_the_shipped_tree_matches_every_row() -> None:
    """FR-005 AC-2: each population derived, none a hand list, and each matching its row."""
    assert row_findings() == []


def test_row_one_finds_kafka_through_its_module_helper() -> None:
    """FR-005 AC-2: ``KafkaSink``'s ``flush_timeout`` reaches ``usable_timeout`` one level down."""
    assert "KafkaSink" in floor_side_timeouts(_SINKS)
    kafka = ast.parse((_SINKS / "kafka.py").read_text(encoding="utf-8"))
    helper = next(
        n for n in kafka.body if isinstance(n, ast.FunctionDef) and n.name == "_usable_timeout"
    )
    assert _reaches_a_floor(helper)


def test_row_two_has_the_otlp_roster_beside_it() -> None:
    """The existing roster in ``test_sinks_otlp.py`` still holds the same population."""
    assert _defines_test(
        _TESTS / "test_sinks_otlp.py",
        "test_the_siblings_keep_the_family_retry_set_and_the_unbounded_read",
    )


def test_the_table_decision_names_every_declared_class() -> None:
    """FR-005 AC-1/AC-2: the decision file is the table, so it names every row's classes."""
    body = _TABLE.read_text(encoding="utf-8")
    missing = sorted({cls for row in ROWS for cls in row.declared if cls not in body})
    assert missing == []
    assert _PUBSUB_PROOF in body


# --- the corpus: each row reports what breaks it ---------------------------------------------


def _copy_tree(tmp_path: Path) -> tuple[Path, Path, Path]:
    sinks, tests, decisions = tmp_path / "sinks", tmp_path / "tests", tmp_path / "decisions"
    for src, dst in ((_SINKS, sinks), (_DECISIONS, decisions)):
        dst.mkdir()
        for path in src.glob("*.py" if src is _SINKS else "*.md"):
            (dst / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    tests.mkdir()
    for name in ("test_sinks_pubsub.py",):
        (tests / name).write_text((_TESTS / name).read_text(encoding="utf-8"), encoding="utf-8")
    return sinks, tests, decisions


def _only(findings: list[str], row: int) -> str:
    assert len(findings) == 1, findings
    assert findings[0].startswith(f"row {row} ("), findings[0]
    return findings[0]


def test_the_copied_tree_is_clean(tmp_path) -> None:
    """The positive control for every corpus case below."""
    sinks, tests, decisions = _copy_tree(tmp_path)
    assert row_findings(sinks=sinks, tests=tests, decisions=decisions) == []


def test_a_fifth_floor_side_timeout_is_reported_by_row_one(tmp_path) -> None:
    sinks, tests, decisions = _copy_tree(tmp_path)
    (sinks / "fixture.py").write_text(
        '"""A fixture sink."""\n\n'
        "class FloorSink:\n"
        '    """Floors a timeout it should refuse."""\n\n'
        "    def __init__(self, send_timeout=5.0):\n"
        "        self.send_timeout = usable_timeout(send_timeout, 5.0)\n",
        encoding="utf-8",
    )
    finding = _only(row_findings(sinks=sinks, tests=tests, decisions=decisions), 1)
    assert "FloorSink" in finding


def test_a_floor_hidden_behind_a_second_helper_fails_row_ones_floor(tmp_path) -> None:
    sinks, tests, decisions = _copy_tree(tmp_path)
    path = sinks / "pubsub.py"
    text = path.read_text(encoding="utf-8")
    hidden = text.replace(
        "self.overflow_timeout = usable_timeout(overflow_timeout, DEFAULT_OVERFLOW_TIMEOUT)",
        "self.overflow_timeout = _outer(overflow_timeout)",
    )
    assert hidden != text, "the premise: pubsub.py floors overflow_timeout on that line"
    hidden += (
        "\n\ndef _outer(value):\n    return _inner(value)\n\n\n"
        "def _inner(value):\n    return usable_timeout(value, DEFAULT_OVERFLOW_TIMEOUT)\n"
    )
    path.write_text(hidden, encoding="utf-8")
    findings = row_findings(sinks=sinks, tests=tests, decisions=decisions)
    assert len(findings) == 2, findings
    assert all(f.startswith("row 1 (") for f in findings)
    assert any("under its floor of 4" in f for f in findings)


def test_a_second_retry_status_override_is_reported_by_row_two(tmp_path) -> None:
    sinks, tests, decisions = _copy_tree(tmp_path)
    (sinks / "fixture.py").write_text(
        '"""A fixture sink."""\n\n'
        "class NarrowSink(HTTPSink):\n"
        '    """Narrows the family retry set."""\n\n'
        "    def _retryable_status(self, status):\n"
        "        return status == 503\n",
        encoding="utf-8",
    )
    assert "NarrowSink" in _only(row_findings(sinks=sinks, tests=tests, decisions=decisions), 2)


def test_a_deleted_proof_test_is_reported_by_row_three(tmp_path) -> None:
    sinks, tests, decisions = _copy_tree(tmp_path)
    path = tests / "test_sinks_pubsub.py"
    path.write_text(
        path.read_text(encoding="utf-8").replace(f"def {_PUBSUB_PROOF}(", "def _renamed("),
        encoding="utf-8",
    )
    assert "derived []" in _only(row_findings(sinks=sinks, tests=tests, decisions=decisions), 3)


def test_a_decision_that_never_names_a_declared_class_is_reported_by_its_row(tmp_path) -> None:
    sinks, tests, decisions = _copy_tree(tmp_path)
    path = decisions / ROWS[1].recorded_in
    path.write_text(path.read_text(encoding="utf-8").replace("OTLPSink", "the sink"), "utf-8")
    finding = _only(row_findings(sinks=sinks, tests=tests, decisions=decisions), 2)
    assert finding.endswith(f"its decision {ROWS[1].recorded_in} never names OTLPSink")


def test_a_missing_decision_is_reported_by_its_row(tmp_path) -> None:
    sinks, tests, decisions = _copy_tree(tmp_path)
    (decisions / ROWS[2].recorded_in).rename(decisions / "renamed.md")
    finding = _only(row_findings(sinks=sinks, tests=tests, decisions=decisions), 3)
    assert "does not exist" in finding


class _ReadsEmptyAsSuccess(HTTPSink):
    """A fifth body reader that settles an empty ``2xx`` as success, as only OTLP may."""

    def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
        return True


def test_a_fifth_reader_reading_an_empty_body_as_success_is_reported_by_row_four() -> None:
    readers = [*_shipped_readers(), _ReadsEmptyAsSuccess]
    finding = _only(row_findings(readers=readers), 4)
    assert "_ReadsEmptyAsSuccess" in finding
