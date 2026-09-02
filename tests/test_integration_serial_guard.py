"""The integration suite's refusal to be distributed is itself checked here."""

from __future__ import annotations

import os
from typing import Any

import pytest


def _guard() -> Any:
    """Returns the underlying function of `integration.conftest._not_distributed`.

    Imported inside the helper rather than at module scope for the reason
    `test_sink_integration_roster.py` does the same: `integration.conftest` is reachable from
    this leg, but importing it at collection time would run its module body for every test here.

    Unwrapped via `_get_wrapped_function()`, which is pytest 9's accessor: the older
    `__pytest_wrapped__.obj` raises `AttributeError` on the `FixtureFunctionDefinition` this
    version returns. `pytest` is pinned `^9.0`, so the accessor cannot silently change under a
    permitted upgrade -- and it fails loudly rather than silently passing if it ever does.

    Args:
      None.

    Returns:
      The undecorated fixture function, callable with a fake request.

    Raises:
      None.
    """
    from integration.conftest import _not_distributed

    return _not_distributed._get_wrapped_function()


class _Config:
    """A stand-in for `pytest.Config` exposing only what the guard reads.

    Args:
      dist: What `getoption("dist")` should return.
      worker: Whether to expose a `workerinput` attribute, as an xdist worker does.

    Returns:
      None.

    Raises:
      None.
    """

    def __init__(self, dist: str = "no", worker: bool = False) -> None:
        self._dist = dist
        if worker:
            self.workerinput: dict[str, str] = {}

    def getoption(self, name: str, default: str | None = None) -> str | None:
        return self._dist if name == "dist" else default


class _Request:
    """A stand-in for `pytest.FixtureRequest` carrying only a config.

    Args:
      config: The config the guard should read.

    Returns:
      None.

    Raises:
      None.
    """

    def __init__(self, config: _Config) -> None:
        self.config = config


@pytest.fixture(autouse=True)
def _gate_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sets the integration gate, since the guard defers to `_gate_is_set` without it.

    Args:
      monkeypatch: Used to set the gate variable for the duration of a test.

    Returns:
      None.

    Raises:
      None.
    """
    monkeypatch.setenv("LOG_FOUNDRY_INTEGRATION", "1")


def test_a_worker_process_is_refused_even_though_its_dist_reads_no() -> None:
    """The case a `dist`-only predicate cannot see, and the reason `workerinput` is read.

    In an xdist **worker** `xdist/remote.py` sets `config.option.dist = "no"` and then assigns
    `config.workerinput`; the controller is what holds `"load"`. So this exact shape -- distributed
    but reporting `dist == "no"` -- is where the guard runs, and a predicate testing only `dist`
    would pass it through. Weakening the guard to that form was measured leaving CI's own
    integration step reading `19 passed`, exit 0.
    """
    with pytest.raises(RuntimeError, match="must not be distributed"):
        _guard()(_Request(_Config(dist="no", worker=True)))


def test_a_distributing_controller_is_refused() -> None:
    """The other half: the controller, which reports `dist` and carries no `workerinput`."""
    with pytest.raises(RuntimeError, match="must not be distributed"):
        _guard()(_Request(_Config(dist="load", worker=False)))


def test_a_serial_session_is_allowed() -> None:
    """`-n 0` must pass through, or the guard would break the run it exists to protect."""
    assert _guard()(_Request(_Config(dist="no", worker=False))) is None


def test_the_gate_being_unset_defers_rather_than_refusing() -> None:
    """A direct file run with no gate must get `_gate_is_set`'s diagnosis, not this one.

    Refusing here first told that caller to add `-n 0`, which then produced the gate error they
    should have seen first -- two steps for one mistake.
    """
    os.environ.pop("LOG_FOUNDRY_INTEGRATION", None)
    assert _guard()(_Request(_Config(dist="load", worker=True))) is None
