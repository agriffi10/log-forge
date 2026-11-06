"""SPEC-069 FR-004 — SQLite and file driver errors are counted, announced and typed as both.

``SQLiteSink.emit`` let ``sqlite3.Error`` escape and the file sinks let ``OSError`` escape, with
nothing counted and nothing written. Each now counts the batch in ``failed``, writes one line, and
raises an exception that is both ``SinkDeliveryError`` and the driver's own class.
"""

from __future__ import annotations

import pickle
import sqlite3
import threading
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.file import FileSink, RotatingFileSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.sqlite import SQLiteSink

if TYPE_CHECKING:
    from pathlib import Path

BATCH = [{"log_id": "a", "message": "one"}, {"log_id": "b", "message": "two"}]


def _lost_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if "lost " in line]


# --- SQLite -----------------------------------------------------------------------------------


def _sqlite_with_its_table_dropped(tmp_path: Path) -> SQLiteSink:
    sink = SQLiteSink(str(tmp_path / "events.db"))
    sink._conn.execute('DROP TABLE "log_events"')
    return sink


def test_sqlite_raises_a_dual_typed_error_counted_and_announced(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-004 AC1: both types, the original as ``__cause__``, its code, text and args kept."""
    sink = _sqlite_with_its_table_dropped(tmp_path)
    with pytest.raises(SinkDeliveryError) as caught:
        sink.emit([dict(e) for e in BATCH])
    err = caught.value
    assert isinstance(err, sqlite3.OperationalError)
    assert isinstance(err, sqlite3.Error)
    cause = err.__cause__
    assert type(cause) is sqlite3.OperationalError
    assert (str(err), err.args) == (str(cause), cause.args)
    assert err.sqlite_errorcode == cause.sqlite_errorcode  # type: ignore[attr-defined]
    assert err.sqlite_errorname == cause.sqlite_errorname  # type: ignore[attr-defined]
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    assert _lost_lines(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); SQLiteSink, OperationalError"
    ]


# --- the file sinks ---------------------------------------------------------------------------


class FailingStream:
    """A stream whose write or flush raises the given error; the other call succeeds."""

    def __init__(self, error: OSError, *, at: str) -> None:
        self._error = error
        self._at = at
        self.written: list[str] = []

    def write(self, data: str) -> int:
        if self._at == "write":
            raise self._error
        self.written.append(data)
        return len(data)

    def flush(self) -> None:
        if self._at == "flush":
            raise self._error

    def close(self) -> None:
        pass


def _file_sink(cls: type, tmp_path: Path, stream: object) -> Any:
    sink = cls(str(tmp_path / f"{cls.__name__}.ndjson"))
    sink._stream.close()
    sink._stream = stream
    return sink


FILE_SINKS = [FileSink, RotatingFileSink]


@pytest.mark.parametrize("at", ["write", "flush"])
@pytest.mark.parametrize("cls", FILE_SINKS, ids=lambda c: c.__name__)
def test_a_full_disk_raises_a_dual_typed_oserror_counted_and_announced(
    cls: type, at: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-004 AC2: ``OSError(28)`` from a write, and separately a flush."""
    original = OSError(28, "No space left on device")
    sink = _file_sink(cls, tmp_path, FailingStream(original, at=at))
    with pytest.raises(OSError) as caught:
        sink.emit([dict(e) for e in BATCH])
    err = caught.value
    assert isinstance(err, SinkDeliveryError)
    assert err.errno == 28
    assert (str(err), err.args) == (str(original), original.args)
    assert err.__cause__ is original
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    assert _lost_lines(capsys.readouterr().err) == [
        f"log-foundry: lost 2 event(s); {cls.__name__}, OSError errno=28"
    ]


@pytest.mark.parametrize("cls", FILE_SINKS, ids=lambda c: c.__name__)
def test_a_permission_error_stays_a_permission_error_and_plain_text_stays_plain(
    cls: type, tmp_path: Path
) -> None:
    """FR-004 AC2: the subclass is kept, and no unset attribute corrupts ``str()``."""
    denied = PermissionError(13, "Permission denied", "/var/log/app.ndjson")
    sink = _file_sink(cls, tmp_path, FailingStream(denied, at="write"))
    with pytest.raises(PermissionError) as caught:
        sink.emit([dict(e) for e in BATCH])
    assert isinstance(caught.value, SinkDeliveryError)
    assert str(caught.value) == str(denied)
    assert caught.value.filename == "/var/log/app.ndjson"

    plain = OSError("plain")
    sink = _file_sink(cls, tmp_path, FailingStream(plain, at="write"))
    with pytest.raises(OSError) as caught_plain:
        sink.emit([dict(e) for e in BATCH])
    assert str(caught_plain.value) == "plain"
    assert caught_plain.value.args == ("plain",)


# --- what every one of the three raises -------------------------------------------------------


def _raise_from(build: str, tmp_path: Path) -> BaseException:
    if build == "sqlite":
        sink: Any = _sqlite_with_its_table_dropped(tmp_path)
    else:
        sink = _file_sink(
            FileSink, tmp_path, FailingStream(OSError(28, "No space left on device"), at="write")
        )
    try:
        sink.emit([dict(e) for e in BATCH])
    except BaseException as err:
        return err
    raise AssertionError("emit was expected to raise")


@pytest.mark.parametrize("build", ["sqlite", "file"])
def test_every_existing_except_clause_still_catches_it(build: str, tmp_path: Path) -> None:
    """FR-004 AC3."""
    err = _raise_from(build, tmp_path)
    driver = sqlite3.Error if build == "sqlite" else OSError
    for clause in (driver, SinkDeliveryError):
        try:
            raise err
        except clause:
            pass


@pytest.mark.parametrize("build", ["sqlite", "file"])
def test_the_type_name_is_the_drivers(build: str, tmp_path: Path) -> None:
    """FR-004 AC4: so the worker's abandon line reads as it did in 1.1.0."""
    err = _raise_from(build, tmp_path)
    assert type(err).__name__ == ("OperationalError" if build == "sqlite" else "OSError")


@pytest.mark.parametrize("build", ["sqlite", "file"])
def test_a_pickle_round_trip_gives_the_drivers_own_class(build: str, tmp_path: Path) -> None:
    """FR-004 AC5: no ``SinkDeliveryError`` base on the far side, ``str()`` and ``args`` equal."""
    err = _raise_from(build, tmp_path)
    back = pickle.loads(pickle.dumps(err))  # noqa: S301
    assert type(back) is (sqlite3.OperationalError if build == "sqlite" else OSError)
    assert not isinstance(back, SinkDeliveryError)
    assert (str(back), back.args) == (str(err), err.args)


def test_a_filename_survives_the_pickle() -> None:
    original = FileNotFoundError(2, "No such file or directory", "a.log")
    built = delivery_error(original)
    assert built is not None
    back = pickle.loads(pickle.dumps(built))  # noqa: S301
    assert type(back) is FileNotFoundError
    assert (str(back), back.filename) == (str(original), "a.log")


class Picky(OSError):
    """An ``OSError`` whose constructor rejects its own ``args``, so no dual class can be made."""

    def __init__(self, code: int, *, why: str) -> None:
        super().__init__(code, why)


@pytest.mark.parametrize("cls", FILE_SINKS, ids=lambda c: c.__name__)
def test_a_factory_that_cannot_build_leaves_the_original_raised(
    cls: type, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-004 AC6: the original, unchanged, still counted and announced."""
    original = Picky(28, why="full")
    assert delivery_error(original) is None
    sink = _file_sink(cls, tmp_path, FailingStream(original, at="write"))
    with pytest.raises(Picky) as caught:
        sink.emit([dict(e) for e in BATCH])
    assert caught.value is original
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    assert _lost_lines(capsys.readouterr().err) == [
        f"log-foundry: lost 2 event(s); {cls.__name__}, Picky errno=28"
    ]


def test_the_worker_path_is_as_before_and_the_sink_now_reports_failed(tmp_path: Path) -> None:
    """FR-004 AC7, worker path: ``failed_batches`` and ``flush()`` as before, ``failed`` moves."""
    sink = _file_sink(
        FileSink, tmp_path, FailingStream(OSError(28, "No space left on device"), at="write")
    )
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def work() -> None:
        log_foundry.info("x")

    work()
    result = log_foundry.flush()
    health = log_foundry.health()
    assert health.failed_batches == 1
    assert not result and result.reason == "abandoned"
    assert health.sink is not None and health.sink.failed > 0
    assert health.sink.dropped == 0


def test_the_orphan_path_counts_the_event_and_the_sink_moves_with_it(tmp_path: Path) -> None:
    """FR-004 AC7, orphan path: ``orphan_lost`` and the sink's ``failed`` both move."""
    sink = _file_sink(
        FileSink, tmp_path, FailingStream(OSError(28, "No space left on device"), at="write")
    )
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("outside any span")
    health = log_foundry.health()
    assert health.orphan_lost == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


@pytest.mark.parametrize("build", ["sqlite", "file", "rotating"])
def test_a_closed_sink_raises_a_plain_sink_delivery_error(build: str, tmp_path: Path) -> None:
    """FR-004 AC8: the closed refusal is untyped, and it moves no counter."""
    if build == "sqlite":
        sink: Any = SQLiteSink(str(tmp_path / "c.db"))
    elif build == "file":
        sink = FileSink(str(tmp_path / "c.ndjson"))
    else:
        sink = RotatingFileSink(str(tmp_path / "r.ndjson"))
    sink.close()
    with pytest.raises(SinkDeliveryError) as caught:
        sink.emit([dict(e) for e in BATCH])
    assert type(caught.value) is SinkDeliveryError
    assert sink.losses() is None


@pytest.mark.parametrize("build", ["sqlite", "file"])
def test_an_unencodable_event_beside_a_driver_error_is_in_failed(
    build: str, tmp_path: Path
) -> None:
    """``failed`` is the batch size, the unencodable event included; nothing is ``dropped``."""
    if build == "sqlite":
        sink: Any = _sqlite_with_its_table_dropped(tmp_path)
    else:
        sink = _file_sink(
            FileSink, tmp_path, FailingStream(OSError(28, "No space left on device"), at="write")
        )
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"log_id": "a"}, {"log_id": "b", "blob": b"x"}])
    assert sink.losses() == SinkLosses(dropped=0, failed=2)


def test_losses_returns_while_an_emit_holds_the_emit_lock(tmp_path: Path) -> None:
    """FR-004 AC9: the count never takes the emit lock, so a poll never waits behind a write."""
    parked, release = threading.Event(), threading.Event()

    class ParkingStream:
        def write(self, data: str) -> int:
            parked.set()
            release.wait(10)
            raise OSError(28, "No space left on device")

        def flush(self) -> None:
            pass

        def close(self) -> None:
            pass

    sink = _file_sink(FileSink, tmp_path, ParkingStream())
    failures: list[BaseException] = []

    def emitter() -> None:
        try:
            sink.emit([dict(e) for e in BATCH])
        except BaseException as err:
            failures.append(err)

    thread = threading.Thread(target=emitter)
    thread.start()
    try:
        assert parked.wait(10)
        assert sink._lock.locked()
        assert sink.losses() is None, "read while the emit lock is held"
    finally:
        release.set()
        thread.join(10)
    assert sink.losses() == SinkLosses(dropped=0, failed=2)
    assert len(failures) == 1


def test_multisink_counts_a_raising_sqlite_child_once_in_its_absorbed_term(
    tmp_path: Path,
) -> None:
    """FR-004 AC10: the sibling delivered, so ``MultiSink`` owns the batch in ``_absorbed``."""
    sqlite_sink = _sqlite_with_its_table_dropped(tmp_path)
    multi = MultiSink(sqlite_sink, MemorySink())
    multi.emit([dict(e) for e in BATCH])
    assert multi._absorbed == len(BATCH)
    assert sqlite_sink.losses() == SinkLosses(dropped=0, failed=2)


def test_the_public_names_did_not_change() -> None:
    """FR-004 AC11: the dual class and the factory are in no ``__all__``."""
    import log_foundry.sinks.base as base

    assert "delivery_error" not in log_foundry.__all__
    assert "delivery_error" not in base.__all__
    assert not any("Dual" in name or "Driver" in name for name in log_foundry.__all__)
