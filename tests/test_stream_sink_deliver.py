"""SPEC-084 FR-001 to FR-004 — the stream sinks report each event by what the stream was handed.

Every expectation is a literal from the spec's criterion: an outcome tuple, the text a stream must
hold, a count or a line. The doubles only stage a fault (a flush or a write call that raises) and
record the order of the calls, so what a test reads back is what the sink handed over. The end-to-end
proofs over real files and pipes are in ``tests/test_stream_sink_partial_write.py``.

Serves invariants 1, 2, 3, 5, 6, 7, 10, 11 and 12. The twin paths (invariant 6) each have a case:
the worker's budgeted run, the orphan path, the inline adapter and a direct ``deliver``.
"""

from __future__ import annotations

import contextlib
import errno
import io
import json
import os
import sys
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import install_worker, run_concurrently
from file_handles import A, D, J, R, Rounds, U, run
from log_foundry.sinks import _engine
from log_foundry.sinks import stdout as stdout_module
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.stdout import StderrSink, StdoutSink
from log_foundry.sinks.verdict import read_deliver
from log_foundry.worker import Worker
from verdict_sinks import WaitRecorder

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable
    from pathlib import Path

BOTH = pytest.mark.parametrize("cls", [StdoutSink, StderrSink], ids=lambda c: c.__name__)
SENTINEL = "SENTINEL-in-the-event"


def _events(n: int, tag: str = "e") -> list[dict[str, object]]:
    return [{"message": f"{SENTINEL}-{tag}{i}", "i": i} for i in range(n)]


def _bad() -> dict[str, object]:
    return {"message": "bad", "blob": b"unencodable"}


def _oserror(code: int) -> OSError:
    return OSError(code, os.strerror(code))


def _epipe() -> OSError:
    return BrokenPipeError(errno.EPIPE, os.strerror(errno.EPIPE))


class _Recorder:
    """A text stream double: records each ``flush`` and ``write`` in order, raising on cue.

    ``flush_faults`` and ``write_faults`` map a call's 0-based index, counted over the double's
    life, to a factory for the exception that call raises. ``every_flush`` raises on every flush.
    A raising call takes nothing.
    """

    def __init__(
        self,
        *,
        flush_faults: dict[int, Callable[[], BaseException]] | None = None,
        write_faults: dict[int, Callable[[], BaseException]] | None = None,
        every_flush: Callable[[], BaseException] | None = None,
    ) -> None:
        self.calls: list[tuple[str, str | None]] = []
        self.taken: list[str] = []
        self._flush_faults = flush_faults or {}
        self._write_faults = write_faults or {}
        self._every_flush = every_flush
        self._flushes = 0
        self._writes = 0

    def write(self, text: str) -> int:
        index = self._writes
        self._writes += 1
        self.calls.append(("write", text))
        fault = self._write_faults.get(index)
        if fault is not None:
            raise fault()
        self.taken.append(text)
        return len(text)

    def flush(self) -> None:
        index = self._flushes
        self._flushes += 1
        self.calls.append(("flush", None))
        fault = self._every_flush or self._flush_faults.get(index)
        if fault is not None:
            raise fault()

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.calls]


def _sink_lines(err: str, name: str = "StdoutSink") -> list[str]:
    return [line for line in err.splitlines() if f"; {name}, " in line]


def _worker(sink: Any, retries: int) -> Worker:
    worker = Worker(sink, batch_size=10, flush_interval=60.0, max_retries=retries)
    install_worker(worker)
    return worker


def _closed_string_io() -> io.StringIO:
    stream = io.StringIO()
    stream.close()
    return stream


def _closed_text_wrapper() -> io.TextIOWrapper:
    stream = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
    stream.close()
    return stream


CLOSED = pytest.mark.parametrize(
    "closed", [_closed_string_io, _closed_text_wrapper], ids=["StringIO", "TextIOWrapper"]
)


# --- FR-001: both sinks join the engine -------------------------------------------------------


@BOTH
def test_a_clean_batch_writes_the_bytes_1x_wrote(cls: type) -> None:
    """The text is a literal: what 1.x's ``json.dumps(event) + "\\n"`` per event wrote."""
    stream = io.StringIO()
    cls(stream).emit(
        [
            {"message": "h\u00e9llo", "n": 1, "fields": {"a": [1, 2.5, None, True]}},
            {"message": "x\ny", "u": "\u20ac"},
        ]
    )
    assert stream.getvalue() == (
        '{"message": "h\\u00e9llo", "n": 1, "fields": {"a": [1, 2.5, null, true]}}\n'
        '{"message": "x\\ny", "u": "\\u20ac"}\n'
    )


@BOTH
def test_losses_is_none_after_a_clean_batch_and_no_retired_member_remains(cls: type) -> None:
    sink = cls(io.StringIO())
    sink.emit(_events(3))
    assert sink.losses() is None
    for name in ("max_retries", "_unencodable", "_counter_lock"):
        assert not hasattr(sink, name), name
        assert not hasattr(cls, name), name


def test_stderr_binds_sys_stderr_at_construction() -> None:
    sink = StderrSink()
    assert sink._stream is sys.stderr
    swapped = io.StringIO()
    with contextlib.redirect_stderr(swapped):
        sink.emit(_events(1))
    assert swapped.getvalue() == ""


@BOTH
def test_the_worker_the_orphan_path_and_the_adapter_all_reach_deliver(cls: type) -> None:
    stream = io.StringIO()
    sink = cls(stream)
    assert read_deliver(sink) is not None
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(_events(2, "w"))
    assert worker.flush(timeout=30.0)
    assert len(seen.outcomes) == 1

    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("orphan")
    assert len(seen.outcomes) == 2 and log_foundry.health().orphan_lost == 0

    sink.emit(_events(1, "x"))
    assert len(seen.outcomes) == 3
    assert seen.outcomes[0] == (A, A) and seen.outcomes[1] == (A,) and seen.outcomes[2] == (A,)
    assert len(stream.getvalue().splitlines()) == 4


# --- FR-002: each event by what the stream took -----------------------------------------------


def test_a_raising_first_pre_flush_is_retryable_and_writes_nothing() -> None:
    """Only the first flush raises, so a double raising on every flush cannot pass this on 1.x."""
    stream = _Recorder(flush_faults={0: lambda: _oserror(errno.ENOSPC)})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    seen = Rounds(sink)
    run(sink, _events(3), retries=1)
    assert seen.outcomes == [(R, R, R), (A, A, A)]
    assert stream.kinds()[:2] == ["flush", "flush"], "no write between the raise and round 2"


def test_a_third_write_raising_enospc_leaves_one_to_three_unknown() -> None:
    stream = _Recorder(write_faults={2: lambda: _oserror(errno.ENOSPC)})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    assert sink.deliver(_events(5)).outcomes == (U, U, U, R, R)


def test_a_final_flush_raising_leaves_every_event_unknown() -> None:
    stream = _Recorder(flush_faults={1: lambda: _oserror(errno.ENOSPC)})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    assert sink.deliver(_events(4)).outcomes == (U, U, U, U)


def test_a_clean_round_flushes_once_before_and_once_after() -> None:
    stream = _Recorder()
    StdoutSink(stream).emit(_events(3))  # type: ignore[arg-type]
    assert stream.kinds() == ["flush", "write", "write", "write", "flush"]


def test_an_all_unencodable_batch_touches_the_stream_not_at_all() -> None:
    stream = _Recorder()
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    sink.emit([_bad(), _bad()])
    assert stream.calls == []
    assert sink.losses() == SinkLosses(dropped=2, failed=0)


@pytest.mark.parametrize(
    "raised",
    [lambda: ValueError("detached"), lambda: io.UnsupportedOperation("not writable")],
    ids=["ValueError", "UnsupportedOperation"],
)
def test_a_non_oserror_write_raise_took_nothing(raised: Callable[[], Exception]) -> None:
    """A raise from line 3's ``write`` that took nothing leaves lines 1 and 2 handed and 3 not.

    ``io.UnsupportedOperation`` is an ``OSError`` as well as a ``ValueError``, with no ``errno``,
    and is raised before any I/O, so it is judged as taking nothing too.
    """
    stream = _Recorder(write_faults={2: raised})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    assert sink.deliver(_events(5)).outcomes == (U, U, J, J, J)


@CLOSED
def test_a_closed_stream_fails_three_batches_in_a_row(closed: Callable[[], Any]) -> None:
    sink = StdoutSink(closed())
    seen = Rounds(sink)
    for _ in range(3):
        with pytest.raises(SinkDeliveryError):
            sink.emit(_events(3))
    assert seen.outcomes == [(J, J, J)] * 3


def test_a_read_only_stream_fails_every_batch_as_1x_raised(tmp_path: Path) -> None:
    """A stream opened for reading raises ``io.UnsupportedOperation`` at the first ``write``.

    The diff review's input: before the fix the first batch returned with three events counted
    ``failed`` and a line saying they may have landed, where 1.x raised on every call.
    """
    path = tmp_path / "read-only.txt"
    path.write_text("", encoding="utf-8")
    with open(path, encoding="utf-8") as stream:
        sink = StdoutSink(stream)
        seen = Rounds(sink)
        for _ in range(3):
            with pytest.raises(io.UnsupportedOperation) as raised:
                sink.emit(_events(3))
            assert isinstance(raised.value, SinkDeliveryError)
        assert seen.outcomes == [(J, J, J)] * 3
        assert not sink._faulted

        worker = _worker(sink, 3)
        worker.submit(_events(2, "w"))
        assert not worker.flush(timeout=30.0)
        assert seen.outcomes[3:] == [(J, J)], "one deliver: nothing a resend can cure"
        assert worker.failed_batches == 1


def test_the_latch_rejects_handed_lines_until_a_clean_round() -> None:
    enospc = lambda: _oserror(errno.ENOSPC)  # noqa: E731
    stream = _Recorder(write_faults={2: enospc, 5: enospc, 13: enospc})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    assert not sink._faulted
    assert sink.deliver(_events(5, "a")).outcomes == (U, U, U, R, R)
    assert sink._faulted
    assert sink.deliver(_events(5, "b")).outcomes == (J, J, J, R, R), (
        "write call 5 is this round's third"
    )
    assert sink._faulted
    assert sink.deliver(_events(5, "c")).outcomes == (A,) * 5
    assert not sink._faulted, "the clean round's final flush cleared it"
    assert sink.deliver(_events(5, "d")).outcomes == (U, U, U, R, R), "write call 13 is its third"
    assert sink._faulted


def test_a_latched_round_writes_its_own_line_for_the_handed_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The latched handed lines get a group of their own, which still says they may have landed."""
    enospc = lambda: _oserror(errno.ENOSPC)  # noqa: E731
    stream = _Recorder(write_faults={2: enospc, 5: enospc})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    sink.emit(_events(5, "a"))
    assert sink._faulted, "the premise: the first round left its handed lines unknown"
    capsys.readouterr()
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(5, "b"))
    err = capsys.readouterr().err
    assert _sink_lines(err) == [
        (
            "log-foundry: lost 3 event(s); StdoutSink, 1 attempt(s), OSError errno=28; "
            "after an earlier fault; they may have landed"
        ),
        "log-foundry: lost 2 event(s); StdoutSink, 1 attempt(s), OSError errno=28",
    ]
    assert SENTINEL not in err


def test_a_latched_run_lost_whole_announces_its_unencodable_events_with_the_handed_ones(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Every encodable line handed, the final flush raising, the latch set: one line for all.

    The run fails, so its unencodable event is counted in ``failed`` with the rest and the one
    line the round writes for the handed lines must cover it too: 2 handed and 1 unencodable.
    """
    enospc = lambda: _oserror(errno.ENOSPC)  # noqa: E731
    sink = StdoutSink(_Recorder(flush_faults={1: enospc, 3: enospc}))  # type: ignore[arg-type]
    sink.emit(_events(2, "a"))
    assert sink._faulted, "the premise: the first round's final flush left its lines unknown"
    capsys.readouterr()
    with pytest.raises(SinkDeliveryError):
        sink.emit([*_events(2, "b"), _bad()])
    assert _sink_lines(capsys.readouterr().err) == [
        (
            "log-foundry: lost 3 event(s); StdoutSink, 1 attempt(s), OSError errno=28; "
            "after an earlier fault; they may have landed"
        )
    ]
    assert sink.losses() == SinkLosses(dropped=0, failed=5)


def test_a_raising_final_flush_on_the_unbuffered_shape_accepts_every_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On the unbuffered shape every returned ``write`` reached the kernel, whatever flush says.

    The real shape's final flush has nothing to write and cannot be made to raise, so the shape
    check is patched to answer True over a double whose final flush raises.
    """
    monkeypatch.setattr(stdout_module, "_unbuffered", lambda stream: True)
    stream = _Recorder(flush_faults={1: lambda: _oserror(errno.EIO)})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    assert sink.deliver(_events(3)).outcomes == (A, A, A)
    assert not sink._faulted
    assert sink.losses() is None


def _present(*names: str) -> list[int]:
    return [code for name in names if (code := getattr(errno, name, None)) is not None]


def _curable(code: int) -> Callable[[], Exception]:
    return lambda: OSError(code, os.strerror(code))


_PREDICATE_TABLE: list[tuple[str, Callable[[], Exception], bool]] = [
    *[
        (name, _curable(code), True)
        for name in ("ENOSPC", "EDQUOT", "EFBIG", "EIO", "EAGAIN")
        if (code := getattr(errno, name, None)) is not None
    ],
    ("BlockingIOError", lambda: BlockingIOError(errno.EAGAIN, "would block"), True),
    ("EPIPE", lambda: BrokenPipeError(errno.EPIPE, "Broken pipe"), False),
    ("EBADF", lambda: OSError(errno.EBADF, "Bad file descriptor"), False),
    ("EMFILE", lambda: OSError(errno.EMFILE, "Too many open files"), False),
    ("errno-None", lambda: OSError("no errno"), False),
    ("ValueError", lambda: ValueError("I/O operation on closed file"), False),
    (
        "UnicodeEncodeError",
        lambda: UnicodeEncodeError("ascii", "\u20ac", 0, 1, "ordinal not in range"),
        False,
    ),
    ("UnsupportedOperation", lambda: io.UnsupportedOperation("not writable"), False),
    ("not-an-OSError", lambda: RuntimeError("not an OSError"), False),
]
"""FR-003's predicate, row by row; a curable ``errno`` this platform lacks is skipped."""


@pytest.mark.parametrize(
    ("error", "retried"),
    [(error, retried) for _, error, retried in _PREDICATE_TABLE],
    ids=[name for name, _, _ in _PREDICATE_TABLE],
)
def test_the_predicate_retries_exactly_the_curable_errnos(
    error: Callable[[], Exception], retried: bool
) -> None:
    built = error()
    assert StdoutSink(io.StringIO())._retryable_error(built) is retried, repr(built)


def test_the_curable_set_is_exactly_the_five_names() -> None:
    """The table above covers every member: an ``errno`` added to the set fails here."""
    expected = frozenset(_present("ENOSPC", "EDQUOT", "EFBIG", "EIO", "EAGAIN"))
    actual = stdout_module._RETRYABLE_ERRNOS
    assert actual == expected


def test_a_first_flush_raising_broken_pipe_fails_the_emit_and_one_worker_batch() -> None:
    stream = _Recorder(flush_faults={0: _epipe})
    with pytest.raises(BrokenPipeError):
        StdoutSink(stream).emit(_events(3))  # type: ignore[arg-type]
    assert stream.kinds() == ["flush"], "no write was handed"

    through = _Recorder(flush_faults={0: _epipe})
    worker = _worker(StdoutSink(through), 3)  # type: ignore[arg-type]
    worker.submit(_events(3))
    assert not worker.flush(timeout=30.0)
    assert worker.failed_batches == 1
    assert "write" not in through.kinds()


class _WrapperSubclass(io.TextIOWrapper):
    """A ``TextIOWrapper`` subclass: buffered as it likes, so never the exact shape."""


def test_the_shape_check_is_exact_and_safe(tmp_path: Path) -> None:
    def raw() -> io.FileIO:
        return io.FileIO(tmp_path / "shape.txt", "w")

    _unbuffered = stdout_module._unbuffered
    exact = io.TextIOWrapper(raw(), write_through=True)
    assert _unbuffered(exact)
    exact.close()
    assert _unbuffered(exact), "closed, it is still the shape: its flush raises first"

    buffered = io.TextIOWrapper(io.BufferedWriter(raw()), write_through=True)
    not_through = io.TextIOWrapper(raw(), write_through=False)
    sub = _WrapperSubclass(raw(), write_through=True)
    detached = io.TextIOWrapper(raw(), write_through=True)
    detached.detach().close()
    for stream in (buffered, not_through, sub, detached, io.StringIO(), _Recorder()):
        assert not _unbuffered(stream), type(stream).__name__
    for stream in (buffered, not_through, sub):
        stream.close()


# --- FR-003: retry what a resend can cure, count once per run ---------------------------------


@pytest.fixture
def no_wait(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Replaces the engine's wait with a recorder that returns at once."""
    entered: list[float] = []

    def recorded(delay: float, stop: threading.Event | None) -> threading.Event | None:
        entered.append(delay)
        return stop

    monkeypatch.setattr(_engine, "_wait", recorded)
    return entered


def test_a_curable_pre_flush_fault_counts_once_per_run_through_the_worker(
    no_wait: list[float], capsys: pytest.CaptureFixture[str]
) -> None:
    stream = _Recorder(every_flush=lambda: _oserror(errno.ENOSPC))
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(_events(3))
    assert not worker.flush(timeout=30.0)
    assert len(seen.outcomes) == 4
    assert sink.losses() == SinkLosses(dropped=0, failed=3), "n, not 4n"
    assert worker.failed_batches == 1
    err = capsys.readouterr().err
    assert _sink_lines(err) == [
        "log-foundry: lost 3 event(s); StdoutSink, 4 attempt(s), OSError errno=28"
    ]
    assert "batch abandoned after" in err
    assert SENTINEL not in err


def test_an_inline_third_write_fault_writes_two_lines(capsys: pytest.CaptureFixture[str]) -> None:
    stream = _Recorder(write_faults={2: lambda: _oserror(errno.ENOSPC)})
    StdoutSink(stream).emit(_events(5))  # type: ignore[arg-type]
    assert _sink_lines(capsys.readouterr().err) == [
        (
            "log-foundry: lost 3 event(s); StdoutSink, 1 attempt(s), OSError errno=28; "
            "outcome unknown; they may have landed"
        ),
        "log-foundry: lost 2 event(s); StdoutSink, 1 attempt(s), OSError errno=28",
    ]


def test_an_unknown_line_is_written_though_a_later_round_accepts_the_rest(
    capsys: pytest.CaptureFixture[str],
) -> None:
    stream = _Recorder(write_faults={2: lambda: _oserror(errno.ENOSPC)})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    seen = Rounds(sink)
    run(sink, _events(5), retries=1)
    assert seen.outcomes == [(U, U, U, R, R), (A, A)]
    assert _sink_lines(capsys.readouterr().err) == [
        (
            "log-foundry: lost 3 event(s); StdoutSink, 1 attempt(s), OSError errno=28; "
            "outcome unknown; they may have landed"
        )
    ]
    assert sink.losses() == SinkLosses(dropped=0, failed=3)


def test_broken_pipe_costs_one_deliver_and_no_wait(no_wait: list[float]) -> None:
    stream = _Recorder(every_flush=_epipe)
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(_events(2))
    assert not worker.flush(timeout=30.0)
    assert seen.outcomes == [(J, J)]
    assert no_wait == [], "the engine's wait was never entered"

    stop = WaitRecorder()
    direct = StdoutSink(_Recorder(every_flush=_epipe))  # type: ignore[arg-type]
    delivery = _engine.run_delivery(direct, _events(2), worker_retries=3, stop=stop)
    assert delivery is not None and delivery.failed
    assert stop.waits == []


@CLOSED
def test_a_closed_stream_costs_one_deliver_through_the_worker(closed: Callable[[], Any]) -> None:
    sink = StdoutSink(closed())
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(_events(3))
    assert not worker.flush(timeout=30.0)
    assert seen.outcomes == [(J, J, J)]
    assert worker.failed_batches == 1


def test_losses_after_clean_settled_and_failed_runs(no_wait: list[float]) -> None:
    clean = StdoutSink(io.StringIO())
    clean.emit(_events(2))
    assert clean.losses() is None

    settled = StdoutSink(_Recorder(write_faults={2: lambda: _oserror(errno.ENOSPC)}))  # type: ignore[arg-type]
    seen = Rounds(settled)
    settled.emit([*_events(1, "a"), _bad(), *_events(3, "b")])
    assert seen.outcomes == [(U, D, U, U, R)]
    assert settled.losses() == SinkLosses(dropped=1, failed=4)

    failing = StdoutSink(_Recorder(every_flush=lambda: _oserror(errno.ENOSPC)))  # type: ignore[arg-type]
    worker = _worker(failing, 3)
    worker.submit([*_events(2, "c"), _bad(), _bad()])
    assert not worker.flush(timeout=30.0)
    assert failing.losses() == SinkLosses(dropped=0, failed=4)


# --- FR-004: the inline raise, and accepting after close ----------------------------------------


def test_a_failed_run_raises_the_streams_class_and_sink_delivery_error() -> None:
    original = BrokenPipeError(errno.EPIPE, "Broken pipe")
    stream = _Recorder(flush_faults={0: lambda: original})
    with pytest.raises(BrokenPipeError) as raised:
        StdoutSink(stream).emit(_events(2))  # type: ignore[arg-type]
    assert isinstance(raised.value, OSError)
    assert isinstance(raised.value, SinkDeliveryError)
    assert raised.value.__cause__ is original


@CLOSED
def test_a_closed_stream_raises_value_error_and_sink_delivery_error(
    closed: Callable[[], Any],
) -> None:
    with pytest.raises(ValueError) as raised:
        StdoutSink(closed()).emit(_events(2))
    assert isinstance(raised.value, SinkDeliveryError)


def test_a_run_with_unknown_returns_and_counts_them() -> None:
    stream = _Recorder(write_faults={1: lambda: _oserror(errno.EIO)})
    sink = StdoutSink(stream)  # type: ignore[arg-type]
    sink.emit(_events(4))
    assert sink.losses() == SinkLosses(dropped=0, failed=4), "2 unknown, 2 lost on the last round"
    unknown = StdoutSink(_Recorder(flush_faults={1: lambda: _oserror(errno.EIO)}))  # type: ignore[arg-type]
    unknown.emit(_events(3))
    assert unknown.losses() == SinkLosses(dropped=0, failed=3)


@BOTH
def test_after_close_a_batch_lands_and_moves_no_counter(cls: type) -> None:
    stream = io.StringIO()
    sink = cls(stream)
    sink.close()
    sink.emit(_events(2))
    assert len(stream.getvalue().splitlines()) == 2
    assert sink.losses() is None


def test_a_raising_close_raises_and_counts_nothing() -> None:
    original = _oserror(errno.EIO)
    sink = StdoutSink(_Recorder(every_flush=lambda: original))  # type: ignore[arg-type]
    with pytest.raises(OSError) as raised:
        sink.close()
    assert raised.value is original
    assert sink.losses() is None


def test_two_threads_emitting_fifty_batches_each_land_each_event_once() -> None:
    stream = io.StringIO()
    sink = StdoutSink(stream)

    def work(thread: int, iteration: int) -> None:
        sink.emit([{"t": thread, "k": iteration, "j": j} for j in range(5)])

    assert run_concurrently(work, 2, per_thread=50) == []
    parsed = [json.loads(line) for line in stream.getvalue().splitlines()]
    keys = sorted((e["t"], e["k"], e["j"]) for e in parsed)
    assert keys == sorted((t, k, j) for t in range(2) for k in range(50) for j in range(5))
    assert sink.losses() is None


@BOTH
def test_neither_sink_has_a_fork_hook(cls: type) -> None:
    assert not hasattr(cls, "reacquire_after_fork")


@BOTH
def test_the_docstring_keeps_its_three_claims_and_drops_the_pending_one(cls: type) -> None:
    doc = " ".join((StdoutSink.__doc__ or "").split())
    assert "**no** transport lock (SPEC-028 FR-002)" in doc
    assert "**no** client buffer (SPEC-036 FR-002)" in doc
    assert "**adds no post-close guard**" in doc
    own = " ".join((cls.__doc__ or "").split())
    assert "**no** client buffer (SPEC-036 FR-002)" in own
    assert "left pending between calls" not in own
