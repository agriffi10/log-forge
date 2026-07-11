"""SPEC-008 — FileSink + RotatingFileSink: append-NDJSON, size/time rotation, backup retention.

All tests write to pytest's ``tmp_path`` (a real, throwaway directory) — the sinks do synchronous
stdlib file I/O, so there is nothing to fake. Events are read back by parsing the NDJSON lines.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from log_forge.sinks.base import Sink
from log_forge.sinks.file import FileSink, RotatingFileSink


def read_events(path: str) -> list[dict]:
    """Parse one NDJSON file into a list of events (empty list if the file is absent)."""
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def read_across_backups(path: str, backup_count: int) -> list[dict]:
    """All events across backups + active file, oldest first (``path.N`` … ``path.1``, ``path``)."""
    events: list[dict] = []
    for i in range(backup_count, 0, -1):
        events.extend(read_events(f"{path}.{i}"))
    events.extend(read_events(path))
    return events


# --- FR-001: FileSink -------------------------------------------------------------------


def test_filesink_is_a_sink(tmp_path) -> None:
    assert isinstance(FileSink(str(tmp_path / "a.ndjson")), Sink)


def test_writes_one_json_line_per_event(tmp_path) -> None:
    path = tmp_path / "a.ndjson"
    sink = FileSink(str(path))
    sink.emit([{"a": 1}, {"b": 2}])
    sink.close()
    assert path.read_text(encoding="utf-8") == '{"a": 1}\n{"b": 2}\n'
    assert read_events(str(path)) == [{"a": 1}, {"b": 2}]


def test_emit_flushes_so_content_is_visible_before_close(tmp_path) -> None:
    path = tmp_path / "a.ndjson"
    sink = FileSink(str(path))
    sink.emit([{"n": 1}])
    # Not closed yet: the per-emit flush must already have pushed the line to disk.
    assert read_events(str(path)) == [{"n": 1}]
    sink.close()


def test_creates_file_when_absent(tmp_path) -> None:
    path = tmp_path / "fresh.ndjson"
    assert not path.exists()
    FileSink(str(path)).close()
    assert path.exists()


def test_appends_across_reopen_never_truncates(tmp_path) -> None:
    path = str(tmp_path / "a.ndjson")
    first = FileSink(path)
    first.emit([{"n": 1}])
    first.close()
    second = FileSink(path)
    second.emit([{"n": 2}])
    second.close()
    assert read_events(path) == [{"n": 1}, {"n": 2}]


def test_double_close_is_noop(tmp_path) -> None:
    sink = FileSink(str(tmp_path / "a.ndjson"))
    sink.close()
    sink.close()  # must not raise


# --- FR-002: RotatingFileSink -----------------------------------------------------------


def test_rotating_is_a_sink(tmp_path) -> None:
    assert isinstance(RotatingFileSink(str(tmp_path / "r.ndjson")), Sink)


def test_size_trigger_keeps_active_file_bounded_and_rotates(tmp_path) -> None:
    path = str(tmp_path / "app.ndjson")
    sink = RotatingFileSink(path, max_bytes=200, backup_count=10)
    for i in range(20):
        sink.emit([{"i": i, "pad": "x" * 50}])
    sink.close()
    # The active file is rotated *before* a write would exceed the bound, so it never overshoots.
    assert os.path.getsize(path) <= 200
    # And rotation actually happened.
    assert os.path.exists(f"{path}.1")


def test_no_event_lost_across_rotations(tmp_path) -> None:
    path = str(tmp_path / "app.ndjson")
    backup_count = 30  # generous enough to retain every rotation for this test
    sink = RotatingFileSink(path, max_bytes=120, backup_count=backup_count)
    events = [{"i": i, "pad": "y" * 40} for i in range(25)]
    for event in events:
        sink.emit([event])
    sink.close()
    assert read_across_backups(path, backup_count) == events


def test_size_trigger_prunes_beyond_backup_count(tmp_path) -> None:
    path = str(tmp_path / "app.ndjson")
    sink = RotatingFileSink(path, max_bytes=120, backup_count=3)
    for i in range(40):
        sink.emit([{"i": i, "pad": "z" * 40}])
    sink.close()
    # Exactly backup_count numbered backups survive; nothing beyond it lingers.
    assert os.path.exists(f"{path}.3")
    assert not os.path.exists(f"{path}.4")


def test_backup_count_zero_keeps_no_backups(tmp_path) -> None:
    path = str(tmp_path / "z.ndjson")
    sink = RotatingFileSink(path, max_bytes=100, backup_count=0)
    for i in range(10):
        sink.emit([{"i": i, "pad": "x" * 30}])
    sink.close()
    assert not os.path.exists(f"{path}.1")
    assert os.path.getsize(path) <= 100


def test_time_trigger_rotates_once_interval_elapsed(tmp_path) -> None:
    path = str(tmp_path / "t.ndjson")
    sink = RotatingFileSink(path, when="S", interval=1, backup_count=1)
    sink.emit([{"n": 1}])
    # Force the interval to have elapsed rather than sleeping (deterministic).
    sink._next_rollover = time.time() - 1
    sink.emit([{"n": 2}])
    sink.close()
    assert read_events(f"{path}.1") == [{"n": 1}]
    assert read_events(path) == [{"n": 2}]


def test_invalid_when_unit_raises_before_opening(tmp_path) -> None:
    path = tmp_path / "never.ndjson"
    with pytest.raises(ValueError):
        RotatingFileSink(str(path), when="Q")
    assert not path.exists()  # failed at construction, before any file was opened
