"""SPEC-082 FR-003 — ``RotatingFileSink`` keeps the byte and tail rules across a rotation.

The write-before-rotate ordering is guarded here, by the first two tests: SPEC-048 FR-006's
tests in ``tests/test_sinks_file.py`` cannot fail for it on an unbuffered handle. Expectations
are literals from the spec's criteria; ``tests/file_handles.py`` stages the failures over the
sink's real handles. Serves invariants 2 and 7.
"""

from __future__ import annotations

import errno
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry.sinks.file as file_mod
from conftest import install_worker
from file_handles import EVENTS, LINE, LINES, A, Budgeted, R, Rounds, U, budget, ids_of, run
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.file import RotatingFileSink
from log_foundry.worker import Worker

if TYPE_CHECKING:
    from pathlib import Path


def _fails_after_rotating(sink: RotatingFileSink, handles: list[Budgeted]) -> None:
    """Wraps ``_rotate`` so the fresh handle a rotation opens takes no bytes, once."""
    real = sink._rotate

    def rotate() -> None:
        real()
        handles.append(budget(sink, 0, once=True))

    sink._rotate = rotate  # type: ignore[method-assign]


def test_a_write_failing_after_a_rotation_leaves_the_earlier_lines_accepted_in_the_backup(
    tmp_path: Path,
) -> None:
    """The fourth event triggers a size rotation; the write after it raises ``ENOSPC``."""
    path = tmp_path / "r.ndjson"
    sink = RotatingFileSink(str(path), max_bytes=3 * LINE, backup_count=1)
    first = budget(sink, None)
    fresh: list[Budgeted] = []
    _fails_after_rotating(sink, fresh)
    seen = Rounds(sink)
    delivery = run(sink, list(EVENTS), retries=1)
    assert seen.outcomes == [(A, A, A, R, R), (A, A)]
    assert first.calls == [LINES[0] + LINES[1] + LINES[2]], (
        "the three lines ahead of the rotation were written before it, in one call"
    )
    assert (tmp_path / "r.ndjson.1").read_bytes() == LINES[0] + LINES[1] + LINES[2]
    assert path.read_bytes() == LINES[3] + LINES[4], "the rest, in the fresh file only, once"
    assert delivery.accepted == 5 and sink.losses() is None
    sink.close()


def test_a_write_failing_before_the_rotation_point_attempts_no_rotation(tmp_path: Path) -> None:
    path = tmp_path / "before.ndjson"
    sink = RotatingFileSink(str(path), max_bytes=3 * LINE, backup_count=1)
    budget(sink, LINE + 4)
    rotations: list[int] = []
    real = sink._rotate
    sink._rotate = lambda: (rotations.append(1), real())[1]  # type: ignore[method-assign]
    seen = Rounds(sink)
    run(sink, list(EVENTS), retries=0)
    assert seen.outcomes == [(A, U, R, R, R)]
    assert rotations == [], "_rotate was not called in that round"
    assert not (tmp_path / "before.ndjson.1").exists()
    assert path.read_bytes() == LINES[0] + LINES[1][:4]


def test_a_pending_tail_lands_in_its_own_file_before_any_rename(tmp_path: Path) -> None:
    path = tmp_path / "tail.ndjson"
    sink = RotatingFileSink(str(path), max_bytes=2 * LINE, backup_count=1)
    handle = budget(sink, LINE + 5)
    sink.emit(list(EVENTS[:2]))
    assert sink._tail is not None and sink._tail.remaining == LINES[1][5:]
    assert not (tmp_path / "tail.ndjson.1").exists()
    handle.lift()
    sink.emit(list(EVENTS[2:4]))
    sink.close()
    assert (tmp_path / "tail.ndjson.1").read_bytes() == LINES[0] + LINES[1], (
        "the tail landed after its prefix, in the file that was then rotated away"
    )
    assert path.read_bytes() == LINES[2] + LINES[3]


def test_a_tail_that_cannot_land_blocks_the_rotation(tmp_path: Path) -> None:
    path = tmp_path / "blocked.ndjson"
    sink = RotatingFileSink(str(path), max_bytes=2 * LINE, backup_count=1)
    budget(sink, LINE + 5)
    sink.emit(list(EVENTS[:2]))
    with pytest.raises(OSError):
        sink.emit(list(EVENTS[2:4]))
    assert not (tmp_path / "blocked.ndjson.1").exists(), "no rename was attempted"
    assert path.read_bytes() == LINES[0] + LINES[1][:5], "and nothing was removed or written"
    assert sink.losses() == SinkLosses(dropped=0, failed=1 + 2)


def test_a_reopen_failing_after_three_lines_reports_them_accepted_and_the_rest_are_resent_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reopen after a failed rotation raises ``EMFILE``; the worker resends only the rest."""
    path = tmp_path / "reopen.ndjson"
    sink = RotatingFileSink(str(path), max_bytes=3 * LINE, backup_count=1)
    failures = {"left": 2}
    real_open = open

    def opening(*args: Any, **kwargs: Any) -> Any:
        if failures["left"]:
            failures["left"] -= 1
            raise OSError(errno.EMFILE, "Too many open files")
        return real_open(*args, **kwargs)

    seen = Rounds(sink)
    worker = Worker(sink, batch_size=10, flush_interval=60.0, max_retries=3)
    install_worker(worker)
    monkeypatch.setattr(file_mod, "open", opening, raising=False)
    worker.submit(list(EVENTS))
    assert worker.flush(timeout=30.0)
    monkeypatch.undo()
    assert seen.outcomes[0] == (A, A, A, R, R), "the three lines before the rotation are ACCEPTED"
    assert seen.outcomes[-1] == (A, A)
    assert worker.failed_batches == 0 and sink.losses() is None
    sink.close()
    written = ids_of(tmp_path / "reopen.ndjson.1") + ids_of(path)
    assert sorted(written) == [1, 2, 3, 4, 5], "each of the batch's events on disk exactly once"


def test_size_grows_by_the_bytes_the_write_calls_took(tmp_path: Path) -> None:
    sink = RotatingFileSink(str(tmp_path / "size.ndjson"))
    handle = budget(sink, 2 * LINE + 7)
    sink.emit(list(EVENTS))
    assert sink._size == 2 * LINE + 7 == 55
    handle.refill(3)
    with pytest.raises(OSError):
        sink.emit([EVENTS[0]])
    assert sink._size == 58, "a tail's bytes are counted as they are taken"
    handle.lift()
    sink.emit([EVENTS[0]])
    assert sink._size == 3 * LINE + LINE == 96
    sink.close()


def test_a_round_that_fails_nowhere_stages_each_run_of_lines_between_rotations(
    tmp_path: Path,
) -> None:
    """The pending event is not lost across a rotation, and each line is written once."""
    path = tmp_path / "many.ndjson"
    sink = RotatingFileSink(str(path), max_bytes=2 * LINE, backup_count=5)
    sink.emit(list(EVENTS))
    sink.close()
    assert (tmp_path / "many.ndjson.2").read_bytes() == LINES[0] + LINES[1]
    assert (tmp_path / "many.ndjson.1").read_bytes() == LINES[2] + LINES[3]
    assert path.read_bytes() == LINES[4]
