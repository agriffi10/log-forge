"""SPEC-074 FR-004 — NATSSink on the engine reproduces 1.x, case by case, as recorded at 93113e1b.

Each case of :mod:`nats_engine_matrix_harness` runs on the current tree, through ``emit`` and
through ``Worker(sink, max_retries=2)._emit``, and its record — ``losses().failed``,
``failed_batches``, the ``deliver`` calls, the publishes, the exact ``log-foundry:`` lines in order,
and the type ``emit`` raised — must equal the one the harness recorded against ``93113e1b``. The
expected values are **recorded, not hand-written**: ``tests/data/nats_engine_matrix_93113e1b.json``
names its commit and the command that regenerates it,
``poetry run python scripts/record_nats_engine_matrix.py --worktree <scratch>/wt-93113e1b``. The
mutant tests plant the other branch's verdict on ``_verdict_of`` and require a mismatch on the field
the plant should move. Serves invariants 2, 6 and 7.

**SPEC-102 FR-004: eight worker cells depart from 1.x, by name.** An event a round's deadline left
unattempted beside a published one is now ``RETRYABLE`` and its ``publish_timeout`` line ``LAST``,
so the worker's later rounds resend it. :data:`DEPARTURES` names the eight worker cells that moves
and :func:`_moved` gives, per moved field, the 1.x value and the new one from SPEC-102's table;
each departure must differ from the fixture on exactly those fields, equal the table there, and
equal the fixture everywhere else. Every other cell, every ``emit`` cell included, must still equal
the fixture, so a ninth departure fails, and so does a departure whose record goes back to 1.x.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

import log_foundry.sinks.nats as nats_module
from log_foundry.sinks.verdict import Outcome
from nats_engine_matrix_harness import CASES, run_case

FIXTURE = pathlib.Path(__file__).parent / "data" / "nats_engine_matrix_93113e1b.json"
RECORDED: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_the_fixture_names_its_commit_and_command() -> None:
    assert RECORDED["commit"].startswith("93113e1b")
    assert "scripts/record_nats_engine_matrix.py" in RECORDED["command"]
    assert set(RECORDED["cases"]) == set(CASES), "the harness and the fixture name the same cases"


TIMEOUT_LINE = "log-foundry: lost {} event(s); NATSSink publish_timeout 0.2s"
REFUSED_LINE = (
    "log-foundry: lost 1 event(s); refused by NATSSink after others landed, SinkDeliveryError"
)

DEPARTURES: frozenset[str] = frozenset(
    {
        "landed-beside-the-unattempted-core-injected",
        "landed-beside-the-unattempted-core-owned",
        "landed-beside-the-unattempted-js-injected",
        "landed-beside-the-unattempted-js-owned",
        "an-abandoned-publish-core-injected",
        "an-abandoned-publish-core-owned",
        "an-abandoned-publish-js-injected",
        "an-abandoned-publish-js-owned",
    }
)
"""The worker cells SPEC-102 moves away from the 1.x record; their ``emit`` cells do not move."""


def _moved(name: str, recorded: dict[str, Any]) -> dict[str, tuple[Any, Any]]:
    """Returns, per field SPEC-102 moves in one departure cell, its 1.x value and its new one.

    The values are SPEC-102 FR-004's table, written out rather than derived from the code. In the
    ``an-abandoned-publish`` cells only the first line moves, so the new lines are the recorded
    ones with that line replaced, and the recorded first line is pinned to the table's 1.x value.

    Args:
      name: A cell of :data:`DEPARTURES`.
      recorded: The fixture's ``worker`` record for that cell.

    Returns:
      ``{field: (1.x value, new value)}``.

    Raises:
      None.
    """
    if name.startswith("landed-beside-the-unattempted-"):
        return {
            "calls": (1, 3),
            "publishes": (1, 3),
            "failed": (4, 2),
            "lines": ([TIMEOUT_LINE.format(4)], [TIMEOUT_LINE.format(2)]),
        }
    lines = recorded["lines"]
    return {
        "calls": (1, 3),
        "lines": ([TIMEOUT_LINE.format(1), *lines[1:]], [REFUSED_LINE, *lines[1:]]),
    }


def test_the_departures_are_eight_cells_the_harness_names() -> None:
    assert len(DEPARTURES) == 8
    assert DEPARTURES.issubset(CASES)


@pytest.mark.parametrize("path", ["emit", "worker"])
@pytest.mark.parametrize("name", sorted(CASES))
def test_the_engine_matches_1x(name: str, path: str) -> None:
    observed = run_case(CASES[name], path)
    recorded = RECORDED["cases"][name][path]
    if path != "worker" or name not in DEPARTURES:
        assert observed == recorded
        return
    moved = _moved(name, recorded)
    for field, (was, now) in moved.items():
        assert recorded[field] == was, f"{field}: the fixture is not the 1.x value SPEC-102 names"
        assert now != was, f"{field}: a departure must move its field"
        assert observed[field] == now, f"{field}: {observed[field]!r} is not SPEC-102's {now!r}"
    for field in sorted(set(recorded) - set(moved)):
        assert observed[field] == recorded[field], f"{field} moved, and SPEC-102 moves only {moved}"
    assert set(observed) == set(recorded)


def _swapped(published: Any, unconfirmed: bool) -> tuple[tuple[Outcome, ...], frozenset[int]]:
    """The planted mutant: ``REJECTED`` read as ``RETRYABLE``, so a refused publish is resent."""
    outcomes, counted = _REAL(published, unconfirmed)
    return tuple(Outcome.RETRYABLE if o is Outcome.REJECTED else o for o in outcomes), counted


def _uncounted(published: Any, unconfirmed: bool) -> tuple[tuple[Outcome, ...], frozenset[int]]:
    """The planted mutant: nothing marked sink-counted, so the engine counts it a second time."""
    outcomes, _ = _REAL(published, unconfirmed)
    return outcomes, frozenset()


_REAL = nats_module._verdict_of


@pytest.mark.parametrize(
    ("mutant", "name", "path", "field"),
    [
        (_swapped, "landed-beside-a-raise-core-injected", "worker", "publishes"),
        (_uncounted, "every-publish-raising-js-injected", "worker", "failed"),
    ],
    ids=["retryable-for-rejected", "sink-counted-dropped"],
)
def test_a_planted_verdict_is_caught(
    mutant: Any, name: str, path: str, field: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(nats_module, "_verdict_of", mutant)
    observed = run_case(CASES[name], path)
    assert observed[field] != RECORDED["cases"][name][path][field]
