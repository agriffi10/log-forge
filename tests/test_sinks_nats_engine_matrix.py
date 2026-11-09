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


@pytest.mark.parametrize("path", ["emit", "worker"])
@pytest.mark.parametrize("name", sorted(CASES))
def test_the_engine_matches_1x(name: str, path: str) -> None:
    assert run_case(CASES[name], path) == RECORDED["cases"][name][path]


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
