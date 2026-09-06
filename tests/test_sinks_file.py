"""SPEC-008 — FileSink + RotatingFileSink: append-NDJSON, size/time rotation, backup retention.

All tests write to pytest's ``tmp_path`` (a real, throwaway directory) — the sinks do synchronous
stdlib file I/O, so there is nothing to fake. Events are read back by parsing the NDJSON lines.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from log_foundry.sinks.base import Sink
from log_foundry.sinks.file import FileSink, RotatingFileSink


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
    # Force the interval to have elapsed rather than sleeping (deterministic). The deadline is
    # a time.monotonic() reading since SPEC-031 FR-001, so it is stepped on that clock.
    sink._next_rollover = time.monotonic() - 1
    sink.emit([{"n": 2}])
    sink.close()
    assert read_events(f"{path}.1") == [{"n": 1}]
    assert read_events(path) == [{"n": 2}]


def test_a_backward_wall_clock_step_does_not_defer_rotation(tmp_path, monkeypatch) -> None:
    """SPEC-031 FR-001 — the defect: an NTP correction used to postpone every time rotation.

    The wall clock is stepped back by a day, far more than the one-second interval, while the
    monotonic clock advances by one interval of *real* elapsed time. Against ``time.time()``
    the deadline sat 86400 s in the future and nothing rotated.
    """
    path = str(tmp_path / "back.ndjson")
    sink = RotatingFileSink(path, when="S", interval=1, backup_count=1)
    sink.emit([{"n": 1}])

    elapsed = time.monotonic()
    monkeypatch.setattr(time, "time", lambda: 0.0)
    monkeypatch.setattr(time, "monotonic", lambda: elapsed + 1.5)
    sink.emit([{"n": 2}])
    sink.close()

    assert read_events(f"{path}.1") == [{"n": 1}]
    assert read_events(path) == [{"n": 2}]


def test_a_forward_wall_clock_step_does_not_rotate_early(tmp_path, monkeypatch) -> None:
    """SPEC-031 FR-001 — the trigger no longer tracks the wall clock in either direction.

    The step is a day, which clears the one-hour wall-clock deadline the old code computed, so
    it rotated. The monotonic deadline is untouched and an hour away, so this one does not.
    """
    path = str(tmp_path / "fwd.ndjson")
    sink = RotatingFileSink(path, when="H", interval=1, backup_count=1)
    sink.emit([{"n": 1}])

    stepped = time.time() + 86_400.0
    monkeypatch.setattr(time, "time", lambda: stepped)
    sink.emit([{"n": 2}])
    sink.close()

    assert not os.path.exists(f"{path}.1")
    assert read_events(path) == [{"n": 1}, {"n": 2}]


def test_the_rotation_deadline_is_a_monotonic_reading(tmp_path) -> None:
    """The two clocks are orders of magnitude apart, so this cannot pass on wall-clock."""
    path = str(tmp_path / "clock.ndjson")
    sink = RotatingFileSink(path, when="S", interval=60, backup_count=1)
    try:
        assert sink._next_rollover is not None
        assert abs(sink._next_rollover - (time.monotonic() + 60)) < 5
    finally:
        sink.close()


def test_invalid_when_unit_raises_before_opening(tmp_path) -> None:
    path = tmp_path / "never.ndjson"
    with pytest.raises(ValueError):
        RotatingFileSink(str(path), when="Q")
    assert not path.exists()  # failed at construction, before any file was opened


# --- SPEC-038 FR-012: the default must keep a generation ----------------------------------


def _lines(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def test_the_default_keeps_one_generation_instead_of_destroying_it(tmp_path) -> None:
    """AC-1. At `backup_count=0` a rotation called `os.remove` on the active file.

    `RotatingFileSink("app.log", max_bytes=10_000_000)` silently threw away 10 MB at every
    rollover — the default, and the shape a caller reaches for first. Every existing test in this
    file passes `backup_count` explicitly, so none of them covered it.
    """
    path = str(tmp_path / "app.ndjson")
    sink = RotatingFileSink(path)  # no backup_count: the default is what is under test
    sink.emit([{"n": i, "pad": "x" * 50} for i in range(6)])
    sink._rotate()
    sink.emit([{"n": 100}])
    sink.close()

    assert os.path.exists(path + ".1"), "the pre-rotation generation must survive"
    assert [event["n"] for event in _lines(path + ".1")] == list(range(6))
    assert [event["n"] for event in _lines(path)] == [100]


def test_backup_count_zero_still_truncates_unchanged(tmp_path) -> None:
    """AC-2. The old behaviour stays available to a caller who asks for it."""
    path = str(tmp_path / "app.ndjson")
    sink = RotatingFileSink(path, backup_count=0)
    sink.emit([{"n": 1}])
    sink._rotate()
    sink.emit([{"n": 2}])
    sink.close()
    assert not os.path.exists(path + ".1")
    assert [event["n"] for event in _lines(path)] == [2]


def test_two_rollovers_keep_the_second_generation_and_no_third(tmp_path) -> None:
    """AC-6. Written past `max_bytes` repeatedly: `.1` is the generation before the live one.

    Asserted on a monotonic sequence rather than on emit boundaries — `max_bytes` can rotate
    *within* one emit, so "the second batch" and "the second generation" are not the same thing,
    and an assertion phrased on batches passes or fails on padding arithmetic instead of on
    retention.
    """
    path = str(tmp_path / "app.ndjson")
    sink = RotatingFileSink(path, max_bytes=200)
    for group in range(3):
        sink.emit([{"n": group * 4 + i, "pad": "y" * 40} for i in range(4)])
    sink.close()

    assert os.path.exists(path + ".1"), "one generation is retained"
    assert not os.path.exists(path + ".2"), "and only one, at the default backup_count"

    retained = [event["n"] for event in _lines(path + ".1")]
    current = [event["n"] for event in _lines(path)]
    assert retained and current, f"both files carry events: {retained} / {current}"
    assert max(retained) + 1 == min(current), (
        f"`.1` must be the generation immediately before the live file, with nothing between "
        f"them: {retained} then {current}"
    )
    assert max(current) == 11, "and the live file holds the newest events"


def test_the_rotating_sink_reports_no_losses_for_what_retention_discards(tmp_path) -> None:
    """AC-3. A bounded ring counts nothing: retention working is not loss.

    `MemorySink(maxlen)` is the precedent — neither `dropped` (discarded *before* a delivery
    attempt) nor `failed` describes an event that was written and flushed to disk.
    """
    path = str(tmp_path / "app.ndjson")
    sink = RotatingFileSink(path, max_bytes=100, backup_count=0)
    sink.emit([{"pad": "z" * 60} for _ in range(10)])
    sink.close()
    assert not hasattr(sink, "losses"), "no losses() accessor, deliberately"


def _all_events(directory: str) -> list[dict]:
    """Every event across the active file and its backups, in no particular order."""
    events: list[dict] = []
    for name in sorted(os.listdir(directory)):
        events += read_events(os.path.join(directory, name))
    return events


def test_a_failed_rename_neither_loses_nor_duplicates_the_batch(tmp_path, capsys) -> None:
    """SPEC-048 FR-006. A rotation failure mid-batch used to cost the batch twice.

    `_rotate` closes the active stream first, so the events already written were on disk and the
    `OSError` propagated out of `emit` -- and the worker retries whole batches. Measured before
    the fix: an 8-event batch failing after 3 were written put **11 lines on disk, 3 of them
    duplicates**.

    The criterion that binds is that `emit` **returns**: that is what stops the retry at its
    source, so the duplicate is never created rather than being reconciled afterwards.
    """
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, max_bytes=120, backup_count=5)
    batch = [{"i": i, "pad": "x" * 20} for i in range(8)]

    real_replace = os.replace
    calls = {"n": 0}

    def flaky(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError(13, "Permission denied")
        return real_replace(src, dst)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("log_foundry.sinks.file.os.replace", flaky)
        sink.emit(batch)
    sink.close()

    seen = [event["i"] for event in _all_events(str(tmp_path))]
    assert sorted(seen) == list(range(8)), f"every event exactly once, got {sorted(seen)}"
    # The concrete OSError subclass is per-platform (`docs/process/operational-traps.md`), so derive it rather than
    # hardcode it: errno 13 is PermissionError here and need not be everywhere.
    expected = type(OSError(13, "Permission denied")).__name__
    assert f"rotating RotatingFileSink ({expected})" in capsys.readouterr().err, (
        "the absorbed failure is announced once, by type"
    )


def test_a_failed_flush_still_leaves_every_event_on_disk(tmp_path, capsys) -> None:
    """The full-disk shape, which the rename injection point cannot reach.

    `_rotate`'s first statement is `self._stream.close()`, which flushes, while `emit` otherwise
    flushes once at the end of the batch -- so on a full or read-only filesystem it is *that*
    flush that raises, and the batch's buffered lines are gone before any rename is attempted.
    Absorbing the error is then not enough: the events are already lost and `getsize` reports a
    file that never received them. `emit` flushes before attempting the rotation for this reason.
    """
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, max_bytes=120, backup_count=5)
    batch = [{"i": i, "pad": "x" * 20} for i in range(8)]

    real_close = sink._stream.close
    state = {"broken": True}

    def failing_close():
        if state["broken"]:
            raise OSError(28, "No space left on device")
        return real_close()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sink._stream, "close", failing_close)
        sink.emit(batch)
        state["broken"] = False
    sink.close()

    seen = [event["i"] for event in _all_events(str(tmp_path))]
    assert sorted(seen) == list(range(8)), (
        f"the pre-rotation flush is what makes this hold; got {sorted(seen)}"
    )


def test_the_sink_survives_a_persistent_rotation_failure(tmp_path, capsys) -> None:
    """A rotation that can never succeed must not break the sink or raise out of every batch.

    Before the fix `_rotate` left a **closed** stream behind, so every later batch raised a raw
    `PermissionError` -- not a `SinkDeliveryError`, and with no `losses()` behind it, since this
    class deliberately has none.
    """
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, max_bytes=120, backup_count=5)

    def always_fail(src, dst):
        raise OSError(13, "Permission denied")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("log_foundry.sinks.file.os.replace", always_fail)
        for round_ in range(3):
            sink.emit([{"i": f"{round_}-{i}", "pad": "x" * 20} for i in range(4)])
        assert not sink._stream.closed, "the sink keeps a usable stream"
        assert os.path.getsize(path) > 120, (
            "and grows past max_bytes rather than losing events -- the documented trade"
        )
    sink.close()
    assert len(_all_events(str(tmp_path))) == 12, "all three batches are on disk"


def test_rotation_resumes_once_the_failure_clears(tmp_path) -> None:
    """An absorbed failure re-arms the trigger, so recovery needs no restart."""
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, max_bytes=120, backup_count=5)

    def always_fail(src, dst):
        raise OSError(13, "Permission denied")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("log_foundry.sinks.file.os.replace", always_fail)
        sink.emit([{"i": i, "pad": "x" * 20} for i in range(6)])
    sink.emit([{"i": 100 + i, "pad": "x" * 20} for i in range(6)])
    sink.close()

    assert os.path.exists(path + ".1"), "the next triggering event rotates normally"
    assert len(_all_events(str(tmp_path))) == 12, "and nothing was lost on the way"


def test_a_persistent_time_trigger_failure_does_not_announce_once_per_event(
    tmp_path, capsys
) -> None:
    """`_next_rollover` is re-armed on an absorbed failure, so the retry is per interval.

    `_rotate` arms it on its last line, so an absorbed failure would otherwise leave a deadline
    permanently in the past: every subsequent event would retry the rotation and write another
    diagnostic, and `_diag` has no damping anywhere.
    """
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, when="H", interval=1, backup_count=5)
    sink._next_rollover = time.monotonic() - 1  # a deadline already past

    def always_fail(src, dst):
        raise OSError(13, "Permission denied")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("log_foundry.sinks.file.os.replace", always_fail)
        sink.emit([{"i": i} for i in range(20)])
    sink.close()

    lines = [line for line in capsys.readouterr().err.splitlines() if "rotating" in line]
    assert len(lines) == 1, f"one line per rotation attempt, not one per event; got {len(lines)}"
    assert len(_all_events(str(tmp_path))) == 20


def test_a_persistent_size_trigger_failure_announces_once_not_once_per_event(
    tmp_path, capsys
) -> None:
    """The size trigger is not damped by the `_next_rollover` re-arm, so the diagnostic is.

    `_size` is re-seeded from a file that is now over `max_bytes`, so `_should_rotate`'s size
    branch stays true and every later event attempts a rotation again — measured at 598 attempts
    over 600 events by a reviewer driving a real `@trace` workload. The attempts lose nothing, but
    an unthrottled stderr write per event on the drain thread is the flood
    `PostgresSink._reconnect_if_broken` already refuses, and it happens while `emit` holds the lock
    a `close()` waits on.
    """
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, max_bytes=200, backup_count=5)

    def always_fail(src, dst):
        raise OSError(13, "Permission denied")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("log_foundry.sinks.file.os.replace", always_fail)
        sink.emit([{"i": i, "pad": "x" * 30} for i in range(60)])
    sink.close()

    lines = [line for line in capsys.readouterr().err.splitlines() if "rotating" in line]
    assert len(lines) == 1, f"one line per outage, not one per event; got {len(lines)}"
    assert len(_all_events(str(tmp_path))) == 60, "and nothing was lost while it was failing"


def test_the_rotation_diagnostic_speaks_again_after_a_recovery(tmp_path, capsys) -> None:
    """Once-per-outage, not once-per-process: a second outage must still be announced.

    The flag clears on the next successful rotation, so a sink that recovers and fails again says
    so — otherwise the damping would silence the very diagnostic it exists to keep readable.
    """
    path = str(tmp_path / "app.log")
    sink = RotatingFileSink(path, max_bytes=200, backup_count=5)

    def always_fail(src, dst):
        raise OSError(13, "Permission denied")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("log_foundry.sinks.file.os.replace", always_fail)
        sink.emit([{"i": i, "pad": "x" * 30} for i in range(20)])
    sink.emit([{"i": 100 + i, "pad": "x" * 30} for i in range(20)])  # recovers, rotates
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("log_foundry.sinks.file.os.replace", always_fail)
        sink.emit([{"i": 200 + i, "pad": "x" * 30} for i in range(20)])
    sink.close()

    lines = [line for line in capsys.readouterr().err.splitlines() if "rotating" in line]
    assert len(lines) == 2, f"one line per outage, and there were two; got {len(lines)}"


# --- SPEC-049 FR-003: refuse what destroys data, floor what merely reads oddly -----------------


@pytest.mark.parametrize("bad", [0, -1, -60])
def test_a_non_positive_interval_is_refused(tmp_path, bad: int) -> None:
    """The one rotation bound that destroys data, so the one that is refused.

    Zero and negatives put the rollover deadline permanently in the past, so `_should_rotate`
    fires on every event and the backup ring eats the batch: measured, three emits of five events
    left **two** lines on disk out of fifteen.
    """
    with pytest.raises(ValueError, match="interval"):
        RotatingFileSink(str(tmp_path / "a.log"), when="S", interval=bad)


def test_an_interval_is_refused_even_with_no_time_trigger(tmp_path) -> None:
    """It is inert without `when`, and refused anyway: a caller who passes it believes one is armed."""
    with pytest.raises(ValueError, match="interval"):
        RotatingFileSink(str(tmp_path / "b.log"), interval=0)


def test_a_negative_size_or_backup_bound_is_floored_not_refused(tmp_path) -> None:
    """SPEC-049 FR-001's other half, and the correction its plan review forced.

    A negative `max_bytes` or `backup_count` **works** today: `_should_rotate` tests
    `self._max_bytes > 0` and `_rotate` tests `self._backup_count > 0`, so each behaves exactly as
    the documented `0` -- nothing lost, nothing raised, no counter moved. Under FR-001's rule they
    are on the floor side, and refusing a configuration that works would be a breaking change at
    1.0. The first draft of this spec refused them, which would have made its own register entry
    false in the same breath.

    Asserted on behaviour, not on the attribute alone: flooring must be indistinguishable from
    passing `0`.
    """
    negative = RotatingFileSink(str(tmp_path / "neg.log"), max_bytes=-1, backup_count=-1)
    zero = RotatingFileSink(str(tmp_path / "zero.log"), max_bytes=0, backup_count=0)
    assert (negative._max_bytes, negative._backup_count) == (0, 0)

    for sink in (negative, zero):
        sink.emit([{"i": i, "pad": "x" * 40} for i in range(10)])
        sink.close()
    assert read_events(str(tmp_path / "neg.log")) == read_events(str(tmp_path / "zero.log")), (
        "a floored negative must be indistinguishable from the documented zero"
    )
    assert not os.path.exists(str(tmp_path / "neg.log.1")), "the size trigger stays off, as at 0"
