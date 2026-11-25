"""SPEC-099 FR-001 and FR-002: the known soak shapes, the reading of a red run, and their guards.

The late-write predicate's corpus isolates one clause per row, so each clause's mutant reddens its
own row and no other. ``test_no_known_row_explains_shape_a`` holds the live ``KNOWN`` to never
reading Shape A's record, or the forced ``fenced`` record, as known; the forced record is a fixed
copy of a measurement, and :func:`test_the_forced_fenced_choreography_records_the_fixed_record`
re-records it from the live choreography so the copy cannot go stale unseen. The write-site roster
holds the one fact about the library the late-write predicate rests on. This file sits outside the
soak's import closure, so editing it does not trigger the nightly workflow's pull-request run.
"""

from __future__ import annotations

import ast
import collections
import pathlib
import re
import threading
import time
from typing import TYPE_CHECKING

import pytest

import soak_ceilings
import soak_known
import soak_verdict
from log_foundry import _lifecycle
from model_oracles import (
    Arm,
    Close,
    Closes,
    Note,
    Take,
    Write,
    owed_between,
    take_of_each_close,
)
from soak_known import Known
from test_invariants_model import _install, _sink_record, _SoakRecord
from test_model_oracles import FENCED, SHAPE_A, closes_of, open_item_leads
from test_shutdown_mid_swap import Sink, _force_fenced

if TYPE_CHECKING:
    from collections.abc import Sequence

    from model_oracles import SinkEvent

_ROOT = pathlib.Path(__file__).resolve().parent.parent
T1, T2, WORKER = "Thread-1 (runner)", "Thread-2 (runner)", "log-foundry-worker"
LOG_ARM = Arm(by_writer=True, by="_note_orphan_emit")
OWED = Take(by="_close_owed")


def _close(count: int) -> Close:
    """Returns a close at one count, on the closer thread, at a fixed instant.

    Args:
      count: The write count at its entry.

    Returns:
      The event.

    Raises:
      None.
    """
    return Close(count, "log-foundry-sink-close", 0.0)


LATE = Closes(2, (1,), (1,))
"""Two writes, one close entered and returned at count 1: one late write."""

ORPHAN_LATE: list[SinkEvent] = [Note(T1), LOG_ARM, Write(T1), Note(T2), OWED, _close(1), Write(T2)]
"""§12's orphan late write: thread 2's log passed its owed check before the last take."""


@pytest.mark.parametrize(
    ("events", "matches"),
    [
        (ORPHAN_LATE, True),
        ([Note(WORKER), Write(WORKER), OWED, _close(1), Write(WORKER)], False),
        ([Note(T1), LOG_ARM, Write(T1), OWED, _close(1), Note(T2), Write(T2)], False),
        (
            [Note(T1), LOG_ARM, Write(T1), Note(T2), OWED, _close(1), LOG_ARM, Write(T2)],
            False,
        ),
    ],
    ids=["orphan-late-write", "on-the-worker-thread", "note-after-the-last-take", "arm-after-it"],
)
def test_the_late_write_predicate(events: list[SinkEvent], matches: bool) -> None:
    """FR-002: each no-match row fails exactly one of the predicate's three clauses.

    A late write on ``log-foundry-worker`` is never the orphan path's (invariant 6), even with a
    ``Note`` of that thread before the take; a log whose ``Note`` follows the last take entered
    after it; and anything arming the sink after the last take makes the write owed, not orphan.
    """
    assert soak_known.orphan_late_write(events, LATE) is matches
    expected = [soak_known.ORPHAN_LATE_WRITE] if matches else []
    assert soak_verdict.explain_run([(events, LATE)]) == expected


_WORKER_LATE: list[SinkEvent] = [Write(WORKER), OWED, _close(1), Write(WORKER)]
_UNTAKEN: list[SinkEvent] = [Write(T1), _close(1)]


@pytest.mark.parametrize(
    ("sinks", "expected"),
    [
        ([(ORPHAN_LATE, LATE)], ["orphan-write-after-close"]),
        ([(ORPHAN_LATE, LATE), (ORPHAN_LATE, LATE)], ["orphan-write-after-close"]),
        ([(ORPHAN_LATE, LATE), (_WORKER_LATE, LATE)], []),
        ([(_UNTAKEN, Closes(1, (1,), (1,)))], []),
        ([([Write(T1), OWED, _close(1)], Closes(1, (1,), (1,)))], []),
    ],
    ids=["known", "known-on-two-sinks", "known-beside-new", "untaken", "clean"],
)
def test_a_run_is_known_only_when_every_finding_is(
    sinks: list[tuple[list[SinkEvent], Closes]], expected: list[str]
) -> None:
    """FR-002: one known sink beside an unknown one, and ``untaken``, are never known."""
    assert [row.name for row in soak_verdict.explain_run(sinks)] == expected


def test_a_reason_names_each_shape_once() -> None:
    """FR-002: the reason's first line names each shape and its §12 item, once each."""
    row = soak_known.ORPHAN_LATE_WRITE
    text = soak_verdict.reason([row, row])
    assert text == f"known soak finding: {row.name} (§12: {row.item})", text


def _swap_predicate(events: Sequence[SinkEvent], closes: Closes) -> bool:
    """The swap row SPEC-097's closure dropped, kept here only as the hostile row to plant.

    Every unowed repeat's first take is ``_close_owed``'s, and every arm between the takes is a
    sink change's, ``_swap_sink``'s, with at least one.

    Args:
      events: One sink's events.
      closes: Its close record, unread.

    Returns:
      Whether the sink's every unowed repeat has the swap's shape.

    Raises:
      None.
    """
    del closes
    closes_at = [event for event in events if isinstance(event, Close)]
    owners = take_of_each_close(events)
    found = False
    for first, second, a, b in zip(closes_at, closes_at[1:], owners, owners[1:], strict=False):
        if first.count != second.count:
            continue
        if a is None or b is None:
            return False
        low, high = sorted((a, b))
        if owed_between(events[low + 1 : high]):
            continue
        take = events[low]
        arms = [e for e in events[low + 1 : high] if isinstance(e, Arm) and not e.by_writer]
        if not isinstance(take, Take) or take.by != "_close_owed":
            return False
        if not arms or any(arm.by != "_swap_sink" for arm in arms):
            return False
        found = True
    return found


FENCED_LEAD = (
    "A `fenced` retarget whose stop's wait ran out re-arms the sink the stop closed, so it is "
    "closed a second time at the same write count"
)
PLANTED = Known("fenced-rearm", FENCED_LEAD, "repeat", _swap_predicate)
"""The row a clearing pull request could add for the open ``fenced`` item, which must be refused."""


def _shape_a_explainers(known: Sequence[Known]) -> dict[str, list[str]]:
    """Returns, for Shape A's record and the forced ``fenced`` record, the rows that explain it.

    Args:
      known: The rows to read against.

    Returns:
      Each record's name and the names of the rows that read it as known.

    Raises:
      None.
    """
    records = {"shape-a": SHAPE_A, "fenced": FENCED}
    found: dict[str, list[str]] = {}
    for name, record in records.items():
        events = list(record)
        assert soak_verdict.signatures_of(events, closes_of(events)) == ["CloseBroken:repeat@1"], (
            f"{name} is no longer a repeat close, so explaining nothing here would prove nothing"
        )
        rows = soak_verdict.explain_run([(events, closes_of(events))], known)
        found[name] = [row.name for row in rows]
    return found


def test_no_known_row_explains_shape_a() -> None:
    """FR-002: no live ``KNOWN`` row reads Shape A's record, or the forced ``fenced`` one, as known.

    The open ``fenced`` item's record is Shape A's exactly, and a regression of the swap item
    SPEC-097 closed records the same, so a row for either would read the regression as known.
    """
    assert _shape_a_explainers(soak_known.KNOWN) == {"shape-a": [], "fenced": []}


def test_a_planted_fenced_row_is_refused_though_its_item_is_open() -> None:
    """FR-002: the row a clearing pull request could add passes the open-item check, and is caught.

    That is why ``test_no_known_row_explains_shape_a`` exists: the open-item check alone would
    admit it, since the ``fenced`` item is open.
    """
    assert PLANTED.item in open_item_leads()
    planted = (*soak_known.KNOWN, PLANTED)
    assert _shape_a_explainers(planted) == {"shape-a": ["fenced-rearm"], "fenced": ["fenced-rearm"]}


def test_a_soak_record_names_who_armed_and_took_and_what_came_after() -> None:
    """FR-001: the record labels each mark and ends with what came after the last close.

    Over the forced ``fenced`` record it names the orphan log's entry, its arm by
    ``_note_orphan_emit``, the ``_close_owed`` take and the sink change's arm; over a late write it
    ends with the write, and with ``nothing`` where nothing followed.
    """
    events = list(FENCED)
    text = _sink_record(0, ["CloseBroken:repeat@1"], events, closes_of(events))
    for mark in (
        "note(MainThread)",
        "arm(writer:_note_orphan_emit)",
        "take(_close_owed)",
        "arm(change:_swap_sink)",
    ):
        assert mark in text, text
    assert text.endswith("\n  after the last close: nothing"), text
    late = _sink_record(1, ["CloseBroken:late-write@1"], ORPHAN_LATE, LATE)
    assert late.endswith(f"\n  after the last close: write({T2})"), late


def test_a_fixed_records_closes_follow_from_its_events() -> None:
    """The fixed records' close record: one write, two closes entered and returned at count 1."""
    events = list(FENCED)
    assert closes_of(events) == Closes(1, (1, 1), (1, 1))


def _projection(events: Sequence[object]) -> list[tuple[object, ...]]:
    """Returns what a record says, without the instants and closer names that vary per run.

    Args:
      events: One sink's events.

    Returns:
      Each event's type and its labels: ``by`` and ``by_writer``, a thread, or a count.

    Raises:
      None.
    """
    shown: list[tuple[object, ...]] = []
    for event in events:
        if isinstance(event, Arm):
            shown.append(("arm", event.by_writer, event.by))
        elif isinstance(event, Take):
            shown.append(("take", event.by))
        elif isinstance(event, (Note, Write)):
            shown.append((type(event).__name__, event.thread))
        elif isinstance(event, Close):
            shown.append(("close", event.count))
    return shown


@pytest.mark.usefixtures("no_close_left_in_flight")
def test_the_forced_fenced_choreography_records_the_fixed_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ruling 4 of SPEC-099's plan: the fixed ``FENCED`` record is what the library records today.

    ``tests/test_shutdown_mid_swap.py``'s forced ``fenced`` choreography, run under the soak's
    wrappers with B's writes and closes fed to the record as the model's recorders feed it: a
    close's count is B's delivery calls so far. Then the exit's ``_close_owed`` closes B again.
    """
    soak = _SoakRecord()
    _install(monkeypatch, soak)
    a, b = Sink("A"), Sink("B")
    soak.register(b)
    calls: collections.Counter[int] = collections.Counter()
    real_emit, real_close = Sink.emit, Sink.close

    def emit(self: Sink, batch: list[dict[str, object]]) -> None:
        calls[id(self)] += 1
        soak.add(self, Write(threading.current_thread().name))
        real_emit(self, batch)

    def close(self: Sink) -> None:
        soak.add(self, Close(calls[id(self)], threading.current_thread().name, time.monotonic()))
        real_close(self)

    monkeypatch.setattr(Sink, "emit", emit)
    monkeypatch.setattr(Sink, "close", close)
    assert _force_fenced(monkeypatch, a, b) == ["fenced"], "the premise: the retarget fenced"
    deadline = time.monotonic() + 10.0
    while b.closes == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    _lifecycle._close_owed()
    assert _projection(soak.events_of(b)) == _projection(FENCED), soak.events_of(b)


_WRITE_SITES = collections.Counter(
    {
        ("api.py", "_log", "run_delivery"): 1,
        ("api.py", "_log", "emit"): 1,
        ("worker.py", "_emit", "run_delivery"): 1,
        ("worker.py", "_emit", "emit"): 1,
    }
)
_HANDERS = frozenset({"run_delivery", "emit", "deliver", "emit_via_deliver"})


def write_sites(sources: dict[str, str]) -> collections.Counter[tuple[str, str, str]]:
    """Returns every call that hands a sink a batch, by module, enclosing function and callee.

    Module-level calls are counted under ``"<module>"``, so a call outside any function is seen.

    Args:
      sources: Each module's file name and text.

    Returns:
      How many such calls each ``(file, function, callee)`` makes.

    Raises:
      SyntaxError: If a source does not parse.
    """
    found: collections.Counter[tuple[str, str, str]] = collections.Counter()
    for name, text in sources.items():
        tree = ast.parse(text)
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            callee = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if callee not in _HANDERS:
                continue
            owner: ast.AST = node
            where = "<module>"
            while owner in parents:
                owner = parents[owner]
                if isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    where = owner.name
                    break
            found[(name, where, str(callee))] += 1
    return found


def test_every_place_the_library_hands_a_sink_a_batch_is_known() -> None:
    """FR-002: outside ``sinks/`` the library hands a sink a batch at exactly four call sites.

    The orphan path's ``api._log`` calls ``_note_orphan_emit`` before either of its two, and the
    worker's ``_emit`` writes on ``log-foundry-worker``, so every caller-thread write follows its
    own ``Note``: the late-write predicate rests on that. A fifth site fails here, naming itself,
    and must be read against the predicate before it lands.
    """
    package = _ROOT / "src" / "log_foundry"
    sources = {path.name: path.read_text(encoding="utf-8") for path in package.glob("*.py")}
    found = write_sites(sources)
    assert found == _WRITE_SITES, sorted((found - _WRITE_SITES) + (_WRITE_SITES - found))


@pytest.mark.parametrize(
    ("source", "site"),
    [
        ("def f(sink):\n    sink.emit([])\n", ("x.py", "f", "emit")),
        ("sink.deliver([])\n", ("x.py", "<module>", "deliver")),
        ("async def g(s):\n    await s.emit_via_deliver([])\n", ("x.py", "g", "emit_via_deliver")),
    ],
    ids=["in-a-function", "at-module-level", "in-a-coroutine"],
)
def test_the_write_site_roster_can_actually_fail(source: str, site: tuple[str, str, str]) -> None:
    """The roster's planted samples, a call at module level included."""
    assert write_sites({"x.py": source}) == collections.Counter({site: 1})


_CEILING_LINE = re.compile(
    r'^\s*"(?P<name>[^"]+)":\s*(?P<value>\d+),\s*#(?P<comment>.*)$', re.MULTILINE
)


def ceiling_findings(text: str, rows: frozenset[str]) -> list[str]:
    """Returns what is wrong with ``tests/soak_ceilings.py``'s entries, naming each line.

    Every entry must name a ``KNOWN`` row, so renaming a row cannot shed its measured ceiling,
    and carry on its own line a measurement (``measured`` and a number), the commit it was
    measured at, and "x2 under, x3 over".

    Args:
      text: The file's text.
      rows: The ``KNOWN`` rows' names.

    Returns:
      One finding per fault, each naming its line; empty when every entry holds.

    Raises:
      None.
    """
    body = text.split("CEILINGS: dict[str, int] = {", 1)[1].split("\n}", 1)[0]
    findings: list[str] = []
    for line in (line for line in body.splitlines() if line.strip()):
        entry = _CEILING_LINE.match(line)
        if entry is None:
            findings.append(f"not an entry with a comment: {line!r}")
            continue
        comment = entry["comment"]
        if entry["name"] not in rows:
            findings.append(f"names no row: {line!r}")
        if not re.search(r"\bmeasured\b.*\d", comment):
            findings.append(f"no measurement: {line!r}")
        if not re.search(r"\b[0-9a-f]{8,40}\b", comment):
            findings.append(f"no commit: {line!r}")
        if "x2 under, x3 over" not in comment:
            findings.append(f'no "x2 under, x3 over": {line!r}')
    return findings


_CEILINGS_TEXT = (_ROOT / "tests" / "soak_ceilings.py").read_text(encoding="utf-8")
_ROWS = frozenset(row.name for row in soak_known.KNOWN)


def test_every_ceiling_names_a_row_and_carries_its_derivation() -> None:
    """FR-002: every measured ceiling names a row and says how it was measured, and when.

    A row ``CEILINGS`` does not name reads ``UNMEASURED_CEILING``.
    """
    assert ceiling_findings(_CEILINGS_TEXT, _ROWS) == []
    assert set(soak_ceilings.CEILINGS) <= _ROWS
    assert soak_ceilings.ceiling_of("no-such-row") == soak_ceilings.UNMEASURED_CEILING == 3


@pytest.mark.parametrize(
    ("old", "new", "says"),
    [
        ("measured 10 a night", "10 a night", "no measurement"),
        ("eda19f32", "a commit", None),
        ("macOS; x2 under, x3 over", "macOS; doubled", 'no "x2 under, x3 over"'),
        ('"orphan-write-after-close":', '"renamed-row":', "names no row"),
    ],
    ids=["no-measurement", "one-commit-lost", "no-derivation", "no-row"],
)
def test_the_ceiling_check_can_actually_fail(old: str, new: str, says: str | None) -> None:
    """Each planted fault in a copy of ``tests/soak_ceilings.py`` is named, with its line.

    Losing one of the two commits leaves the other, which still anchors the line; losing both is
    the ``no commit`` fault, planted second below.
    """
    assert _CEILINGS_TEXT.count(old) == 1, old
    planted = _CEILINGS_TEXT.replace(old, new)
    if says is None:
        assert ceiling_findings(planted, _ROWS) == []
        planted = planted.replace("d9f2f097", "a commit")
        says = "no commit"
    findings = ceiling_findings(planted, _ROWS)
    assert any(says in finding for finding in findings), findings
    assert all("orphan-write-after-close" in f or "renamed-row" in f for f in findings), findings


_FILED_LINE = re.compile(r'^    "[0-9]+-[0-9]+": ".+",$')


def filed_night_findings(text: str) -> list[str]:
    """Returns every line of ``tests/soak_known.py``'s text that breaks the filing's one-line form.

    ``scripts/pr-queue/queue.sh`` greps the file for the exact line, so a filing inside
    ``FILED_NIGHTS`` written another way would not clear its night, and a line of that shape
    anywhere else, a comment included, would clear one it should not.

    Args:
      text: The file's text.

    Returns:
      One finding per offending line; empty when the file holds.

    Raises:
      None.
    """
    head, _, rest = text.partition("FILED_NIGHTS: dict[str, str] = {")
    body, _, tail = rest.partition("}")
    findings = [
        f"a filing outside FILED_NIGHTS: {line!r}"
        for line in (head + tail).splitlines()
        if _FILED_LINE.match(line)
    ]
    findings += [
        f"not one filing a line: {line!r}"
        for line in body.splitlines()
        if line.strip() and not _FILED_LINE.match(line)
    ]
    return findings


_KNOWN_TEXT = (_ROOT / "tests" / "soak_known.py").read_text(encoding="utf-8")


def test_every_filed_night_is_one_line_the_queue_can_read() -> None:
    """FR-006: every filing is one line inside ``FILED_NIGHTS``, as the queue reads it."""
    assert filed_night_findings(_KNOWN_TEXT) == []
    body = _KNOWN_TEXT.partition("FILED_NIGHTS: dict[str, str] = {")[2].partition("}")[0]
    assert len([line for line in body.splitlines() if line.strip()]) == len(soak_known.FILED_NIGHTS)


@pytest.mark.parametrize(
    ("old", "new", "says"),
    [
        (
            "FILED_NIGHTS: dict[str, str] = {}",
            'FILED_NIGHTS: dict[str, str] = {\n  "1-1": "x",\n}',
            "not one",
        ),
        ("KNOWN: tuple", '    "1-1": "x",\nKNOWN: tuple', "outside"),
    ],
    ids=["wrong-indent-inside", "a-filing-outside"],
)
def test_the_filing_check_can_actually_fail(old: str, new: str, says: str) -> None:
    """A filing at the wrong indent, and a filing-shaped line outside the dict, are each named."""
    assert _KNOWN_TEXT.count(old) == 1, old
    findings = filed_night_findings(_KNOWN_TEXT.replace(old, new))
    assert len(findings) == 1 and says in findings[0], findings
