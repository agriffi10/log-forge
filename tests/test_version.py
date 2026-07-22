"""SPEC-012 — runtime ``__version__`` sourced from installed distribution metadata (FR-002)."""

import importlib
import importlib.metadata
import re

import pytest

log_forge = pytest.importorskip("log_forge")

# PEP 440 public versions we can actually produce: a release (0.1.0) or a dev build (0.1.1.dev3).
_PEP440_PUBLIC = re.compile(r"^\d+\.\d+\.\d+(\.dev\d+)?$")


def test_version_is_a_string() -> None:
    assert isinstance(log_forge.__version__, str)
    assert log_forge.__version__


def test_version_is_a_public_pep440_version() -> None:
    # No local segment (+<hash>) — PyPI and TestPyPI reject uploads that carry one.
    assert _PEP440_PUBLIC.match(log_forge.__version__), log_forge.__version__
    assert "+" not in log_forge.__version__


def test_version_matches_distribution_metadata() -> None:
    try:
        expected = importlib.metadata.version("log-forge")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("log-forge is not installed in this environment")
    assert log_forge.__version__ == expected


def test_version_is_exported() -> None:
    assert "__version__" in log_forge.__all__


def test_version_falls_back_when_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A source checkout with no installed distribution must not blow up on import."""

    def _raise(_name: str) -> str:
        raise importlib.metadata.PackageNotFoundError("log-forge")

    monkeypatch.setattr(importlib.metadata, "version", _raise)
    try:
        reloaded = importlib.reload(log_forge)
        assert reloaded.__version__ == "0.0.0"
    finally:
        monkeypatch.undo()
        importlib.reload(log_forge)  # restore the real version for later tests
