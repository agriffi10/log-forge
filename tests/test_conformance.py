"""SPEC-092: every shipped sink, under every fault of its acknowledgement class, judged by one assertion.

One test per row. A row runs the public path the worker serves against its class's fake backend,
and `conformance_backend.judge` compares what the backend holds with what the sink and its
catcher reported, event by event. A row red on today's code is pinned to the open §12 item that
owns its defect, as a strict expected failure at its exact verdict, or held at a departure's exact
verdict; every other row must pass. So the suite stays green, and turns red the day a sink is
fixed or a new defect appears. The register and the corpus's own gates are in
`test_conformance_gate.py`.
"""

from __future__ import annotations

import pytest

from conformance_backend import run_row
from conformance_classes import Row, rows
from conformance_pins import pin_of, settle
from model_pins import PinnedVerdict


def _params() -> list[object]:
    """One parameter per row, a pinned one carrying the strict mark.

    Args:
      None.

    Returns:
      The parameters, in row order.

    Raises:
      None.
    """
    params: list[object] = []
    for row in rows():
        pin = pin_of(row.id)
        marks = (
            [
                pytest.mark.xfail(
                    strict=True, raises=PinnedVerdict, reason=f"pinned by {pin.name}: {pin.item}"
                )
            ]
            if pin is not None
            else []
        )
        params.append(pytest.param(row, id=row.id, marks=marks))
    return params


@pytest.mark.parametrize("row", _params())
def test_row(row: Row) -> None:
    """A row passes, or is held at its departure's verdict, or fails only at its pin's verdict."""
    settle(row.id, run_row(row).verdict)
