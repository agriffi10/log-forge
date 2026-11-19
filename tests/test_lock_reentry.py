"""SPEC-097 FR-005: nothing in the library re-enters one of its own locks, and a recorder sees it.

FR-004 made the eight locks a lifecycle call takes on its own thread ``RLock``s, read for
ownership. A plain ``Lock`` taken twice by mistake deadlocks, which a test notices; an ``RLock``
lets it through, which nothing notices. So these sessions run with each of the eight replaced by
``tests/_reentry.py``'s recording lock, which writes the call site whenever a thread that owns a
lock acquires it again. A clean run records nothing. With the spec reviewer's planted mutant,
``with self._lock, self._lock:`` in ``Worker.health``, the same run records the re-entry, naming
``worker.py`` and ``health``: at ``ead40798`` that mutant deadlocked these two modules, and with
the ``RLock``s it would pass silently were it not for the recorder.

Each session is a nested ``pytest`` over ``tests/test_runtime_state.py`` and ``tests/test_worker.py``,
run against a copy of ``src`` so the mutant is planted in no file this checkout imports. The copy
is put first on the path with ``-o pythonpath=``, and the recorder's first line names the library
file each process imported, which the test asserts is the copy's: a mutant planted in a copy the
run never imported proves nothing. The whole suite's recording run is a one-off measurement of the
build, recorded in the delivery doc, not a test: it costs the suite's own time again. Serves
invariant 1.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys

import pytest

import log_foundry

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC = pathlib.Path(log_foundry.__file__).resolve().parent.parent
_MODULES = ("tests/test_runtime_state.py", "tests/test_worker.py")
_HEALTH_LOCK = "    def health(self) -> Health:"


def _copy(tmp: pathlib.Path, *, planted: bool) -> pathlib.Path:
    """Copies ``src`` to ``tmp``, planting the double acquire in ``Worker.health`` if asked."""
    src = tmp / "src"
    shutil.copytree(_SRC, src, ignore=shutil.ignore_patterns("__pycache__"))
    if planted:
        path = src / "log_foundry" / "worker.py"
        text = path.read_text(encoding="utf-8")
        start = text.index(_HEALTH_LOCK)
        site = text.index("        with self._lock:", start)
        text = text[:site] + "        with self._lock, self._lock:" + text[site + 24 :]
        path.write_text(text, encoding="utf-8")
    return src


def _record(tmp: pathlib.Path, src: pathlib.Path) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Runs the two modules under the recorder against ``src``; returns the run and its lines."""
    record = tmp / "reentry.txt"
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("PYTEST_", "LOG_FOUNDRY_MODEL_"))
    }
    env["LOG_FOUNDRY_REENTRY_RECORD"] = str(record)
    env["PYTHONPATH"] = str(src)
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-q",
            "-n",
            "4",
            "-o",
            f"pythonpath={src}",
            *_MODULES,
        ],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    lines = record.read_text(encoding="utf-8").splitlines() if record.exists() else []
    return result, lines


def _libraries(lines: list[str]) -> set[str]:
    return {line.split(" ", 2)[2] for line in lines if line.split(" ", 2)[1] == "library"}


def _reentries(lines: list[str]) -> list[str]:
    return [line.split(" ", 2)[2] for line in lines if line.split(" ", 2)[1] == "reentry"]


@pytest.mark.parametrize("planted", [False, True], ids=["clean", "planted"])
def test_the_recording_run_sees_a_re_entry_only_where_one_is_made(
    planted: bool, tmp_path: pathlib.Path
) -> None:
    """FR-005: clean, the two modules record no re-entry; planted, every record is ``health``'s.

    The planted run is the recorder's positive control: it is what proves that a clean run's
    silence means no re-entry rather than a recorder that records nothing.
    """
    src = _copy(tmp_path, planted=planted)
    result, lines = _record(tmp_path, src)
    libraries = _libraries(lines)
    assert libraries and all(library.startswith(str(src)) for library in libraries), (
        f"the run did not import the copy: {libraries}\n{result.stdout[-2000:]}"
    )
    reentries = _reentries(lines)
    if not planted:
        assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-2000:]
        assert reentries == [], f"a library lock was re-entered: {sorted(set(reentries))}"
        return
    assert reentries, f"the planted re-entry was not recorded\n{result.stdout[-2000:]}"
    assert {site.rsplit(":", 1)[0] for site in reentries} == {"worker.py:health"}, sorted(
        set(reentries)
    )


_SIGNAL_ROWS = {
    "flush-section/shutdown": set(),
    "orphan-arm/shutdown": set(),
    "submit/shutdown": set(),
    "flush-section/flush": set(),
    "flush-section/configure": set(),
    "orphan-arm/info": {"_lifecycle.py:_arm_and_hand"},
    "submit/span": {"worker.py:_enqueue"},
    "flush-section/info": set(),
    "rebind/shutdown": set(),
    "swap/shutdown": set(),
    "health/shutdown": set(),
}
"""Each ``spec097_signal_in_section.py`` row and the re-entries its design makes, by site."""

_ROW_RUNNER = """
import os, runpy, sys
sys.path.insert(0, sys.argv[1])
import _reentry
_reentry.install()
sys.argv = ["spec097_signal_in_section.py", sys.argv[2]]
runpy.run_path(os.environ["PROBE"], run_name="__main__")
"""


@pytest.mark.parametrize("row", list(_SIGNAL_ROWS))
def test_a_handlers_level_or_traced_call_re_enters_only_where_designed(
    row: str, tmp_path: pathlib.Path
) -> None:
    """FR-005's positive half: the signal-in-section rows record exactly their designed re-entries.

    Each row of ``spec097_signal_in_section.py`` runs in a fresh interpreter with the eight oracle
    locks replaced by the recorder. A handler's lifecycle call is held and takes no lock, so it
    records nothing; a handler's level call or traced call is not held and re-enters the lock its
    thread holds, which is FR-005's admitted re-entry. Two rows make one each: the orphan log
    re-enters ``_state._lock`` in ``_arm_and_hand``, and the traced call's submit re-enters the
    queue's mutex in ``Worker._enqueue``, where its ``put`` runs. Every other row records none.
    """
    record = tmp_path / "reentry.txt"
    probe = _ROOT / "docs/audits/2026-11-14-probes/core-pipeline/spec097_signal_in_section.py"
    env = {**os.environ, "LOG_FOUNDRY_REENTRY_RECORD": str(record), "PYTHONPATH": str(_SRC)}
    env["PROBE"] = str(probe)
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-B", "-c", _ROW_RUNNER, str(_ROOT / "tests"), row],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]
    lines = record.read_text(encoding="utf-8").splitlines() if record.exists() else []
    assert _libraries(lines), "the recorder was not installed"
    sites = [site.rsplit(":", 1)[0] for site in _reentries(lines)]
    assert set(sites) == _SIGNAL_ROWS[row] and len(sites) == len(_SIGNAL_ROWS[row]), sites
