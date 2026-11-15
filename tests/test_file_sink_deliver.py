"""SPEC-082 FR-001, FR-002 and FR-004 — the file sinks report each event by the bytes taken.

Every expectation here is a literal taken from the spec's criterion: an outcome tuple, the bytes
a file must hold, a count. The handle doubles in ``tests/file_handles.py`` only stage the failure
(a write call that takes some bytes, a later one that raises) over the sink's real handle, so
what a test reads back is what reached the file.

Serves invariants 2, 3, 5, 6, 7, 10 and 11. The twin paths (invariant 6) each have a case: the
worker's budgeted run, the orphan path, the inline adapter and so a ``MultiSink`` child, a
wrapper on the engine, and a direct ``deliver``.
"""

from __future__ import annotations

import errno
import json
import os
import stat
import sys
import threading
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
from conftest import install_worker, run_concurrently
from file_handles import (
    EVENTS,
    LINE,
    LINES,
    A,
    D,
    J,
    R,
    Rounds,
    U,
    budget,
    ids_of,
    lines_of,
    run,
)
from log_foundry.sinks import _engine
from log_foundry.sinks._refusal import is_final_refusal
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.file import FileSink, RotatingFileSink
from log_foundry.sinks.multi import MultiSink
from log_foundry.sinks.transform import TransformSink
from log_foundry.sinks.verdict import read_deliver
from log_foundry.worker import Worker
from verdict_sinks import WaitRecorder

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

BOTH = pytest.mark.parametrize("cls", [FileSink, RotatingFileSink], ids=lambda c: c.__name__)
WHOLE = b"".join(LINES)
"""The 120 bytes the five events make."""


def _lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost")]


def _worker(sink: Any, retries: int) -> Worker:
    worker = Worker(sink, batch_size=10, flush_interval=60.0, max_retries=retries)
    install_worker(worker)
    return worker


def _raises(code: int) -> Callable[[], OSError]:
    return lambda: OSError(code, os.strerror(code))


# --- FR-001: each event by the bytes taken ---------------------------------------------------


@pytest.mark.parametrize(
    ("taken", "outcomes", "tail"),
    [
        (0, (R, R, R, R, R), None),
        (52, (A, A, R, R, R), b': 3, "pad": "cccc"}\n'),
        (71, (A, A, R, R, R), b"\n"),
        (72, (A, A, A, R, R), None),
        (119, (A, A, A, A, R), b"\n"),
    ],
    ids=["nothing", "inside-the-third-line", "the-third-newline", "past-it", "one-byte-short"],
)
@BOTH
def test_a_round_reports_each_event_by_the_bytes_taken(
    cls: type, taken: int, outcomes: tuple[Any, ...], tail: bytes | None, tmp_path: Path
) -> None:
    """Inside a two-round run, a round that is not the last: ``k`` bytes, then ``ENOSPC``."""
    sink = cls(str(tmp_path / "k.ndjson"))
    budget(sink, taken)
    seen = Rounds(sink)
    run(sink, list(EVENTS), retries=1)
    assert seen.outcomes[0] == outcomes
    assert (tmp_path / "k.ndjson").read_bytes() == WHOLE[:taken]
    pending = sink._tail
    assert (None if pending is None else pending.remaining) == tail
    if tail == b"\n":
        assert len(pending.remaining) == 1, "the newline alone is the tail"


def test_a_direct_deliver_tear_is_unknown_and_a_callers_loop_writes_each_event_once(
    tmp_path: Path,
) -> None:
    """Outside any run the notes end with the call, so the torn event is never ``RETRYABLE``."""
    path = tmp_path / "direct.ndjson"
    sink = FileSink(str(path))
    budget(sink, 5, once=True)
    first = sink.deliver(EVENTS[:3])
    assert first.outcomes == (U, R, R)
    second = sink.deliver(EVENTS[1:3])
    assert second.outcomes == (A, A)
    sink.close()
    assert path.read_bytes() == LINES[0] + LINES[1] + LINES[2]


@pytest.mark.parametrize("kind", [ValueError, MemoryError])
def test_a_handle_that_raises_something_other_than_oserror_leaves_no_cut_line(
    kind: type[Exception], tmp_path: Path
) -> None:
    """Nine bytes, then not an ``OSError``: the tail is still recorded and nothing is resent."""
    path = tmp_path / "other.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 9, lambda: kind("the handle broke"), once=True)
    worker = _worker(MultiSink(sink), 3)
    worker.submit(list(EVENTS[:4]))
    assert worker.flush(timeout=30.0)
    assert handle.raised == 1
    assert sink._tail is not None and sink._tail.remaining == LINES[0][9:]
    sink.close()
    assert path.read_bytes() == LINES[0], "the cut line finished once, and nothing written twice"
    assert ids_of(path) == [1]


def _text_stream_bytes(path: Path, encoding: str) -> bytes:
    """What 1.x's text stream leaves in ``path`` for the five events: the criterion's oracle."""
    with open(path, "a", encoding=encoding) as stream:
        stream.writelines(json.dumps(event) + "\n" for event in EVENTS)
    return path.read_bytes()


@pytest.mark.parametrize(
    ("encoding", "seed"),
    [("utf-8", b""), ("utf-16", b""), ("utf-16", b"x\x00\n\x00")],
    ids=["utf-8", "utf-16-empty", "utf-16-non-empty"],
)
def test_a_clean_batch_writes_the_bytes_a_text_stream_wrote(
    encoding: str, seed: bytes, tmp_path: Path
) -> None:
    old, new = tmp_path / "old", tmp_path / "new"
    old.write_bytes(seed)
    new.write_bytes(seed)
    expected = _text_stream_bytes(old, encoding)
    sink = FileSink(str(new), encoding=encoding)
    sink.emit(list(EVENTS))
    sink.close()
    assert new.read_bytes() == expected
    mark = b"\xff\xfe" if sys.byteorder == "little" else b"\xfe\xff"
    if encoding == "utf-16":
        assert new.read_bytes().startswith(mark) is (seed == b"")
        assert new.read_bytes().count(mark) == (1 if seed == b"" else 0)
    assert sink.losses() is None


def _drain(path: Path, into: list[bytes]) -> threading.Thread:
    """Reads a FIFO to end of file on a thread, so a writer never blocks on it."""

    def read() -> None:
        with open(path, "rb") as stream:
            into.append(stream.read())

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    return thread


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs a FIFO")
def test_utf_16_into_a_fifo_constructs_and_writes_no_mark(tmp_path: Path) -> None:
    """A path that cannot seek owes no mark, and gets the bytes 1.x's text stream sent."""
    old, new = tmp_path / "old.fifo", tmp_path / "new.fifo"
    os.mkfifo(old)
    os.mkfifo(new)
    before: list[bytes] = []
    after: list[bytes] = []
    reading_old = _drain(old, before)
    with open(old, "a", encoding="utf-16") as stream:
        stream.writelines(json.dumps(event) + "\n" for event in EVENTS)
    reading_old.join(20.0)
    reading_new = _drain(new, after)
    sink = FileSink(str(new), encoding="utf-16")
    assert stat.S_ISFIFO(os.fstat(sink._stream.fileno()).st_mode)
    sink.emit(list(EVENTS))
    sink.close()
    reading_new.join(20.0)
    assert before and after == before
    assert b"\xff\xfe" not in after[0] and b"\xfe\xff" not in after[0]


def test_a_byte_order_mark_a_failed_round_did_not_land_is_written_once_by_the_resend(
    tmp_path: Path,
) -> None:
    """The mark belongs to the first line's bytes and stays owed until a write call takes it."""
    path = tmp_path / "bom.ndjson"
    sink = FileSink(str(path), encoding="utf-16")
    handle = budget(sink, 0, once=True)
    seen = Rounds(sink)
    run(sink, list(EVENTS), retries=1)
    sink.close()
    assert seen.outcomes == [(R, R, R, R, R), (A, A, A, A, A)]
    mark = b"\xff\xfe" if sys.byteorder == "little" else b"\xfe\xff"
    raw = path.read_bytes()
    assert raw.startswith(mark) and raw.count(mark) == 1
    assert raw.decode("utf-16") == WHOLE.decode()
    assert handle.calls[1].startswith(mark), "the resent round wrote the mark first"


@pytest.mark.parametrize("encoding", ["no-such-codec", "base64"])
def test_a_codec_that_is_not_a_text_encoding_is_refused_at_construction(
    encoding: str, tmp_path: Path
) -> None:
    path = tmp_path / "refused.ndjson"
    with pytest.raises(LookupError):
        FileSink(str(path), encoding=encoding)
    assert not path.exists(), "refused before any file is opened"


def test_no_accepted_event_is_written_by_a_later_round(tmp_path: Path) -> None:
    """Invariant 7: across the rounds, each line's bytes are handed to the handle once."""
    path = tmp_path / "once.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 72, once=True)
    run(sink, list(EVENTS), retries=1)
    sink.close()
    assert handle.calls == [WHOLE, WHOLE[72:], LINES[3] + LINES[4]]
    assert handle.took == [72, None, 48]
    assert path.read_bytes() == WHOLE


# --- FR-002: the tail ------------------------------------------------------------------------


@BOTH
def test_a_torn_line_is_finished_before_the_rest_of_the_resend(cls: type, tmp_path: Path) -> None:
    path = tmp_path / "tail.ndjson"
    sink = cls(str(path))
    handle = budget(sink, 52, once=True)
    seen = Rounds(sink)
    delivery = run(sink, list(EVENTS), retries=1)
    assert seen.outcomes == [(A, A, R, R, R), (A, A, A)]
    assert seen.batches[1] == EVENTS[2:]
    assert handle.calls[-2:] == [b': 3, "pad": "cccc"}\n', LINES[3] + LINES[4]], (
        "the tail alone first, then the rest, and never the third line whole again"
    )
    assert path.read_bytes() == WHOLE
    assert ids_of(path) == [1, 2, 3, 4, 5]
    assert (delivery.accepted, delivery.failed) == (5, False)
    assert sink.losses() is None
    sink.close()


@BOTH
def test_a_tail_that_fails_partway_shrinks_and_nothing_else_is_written(
    cls: type, tmp_path: Path
) -> None:
    path = tmp_path / "shrink.ndjson"
    sink = cls(str(path))
    handle = budget(sink, 52)
    before: list[int] = []

    def between(number: int) -> None:
        if number == 1:
            before.append(len(handle.calls))
            handle.refill(7)
        else:
            handle.lift()

    seen = Rounds(sink, between)
    run(sink, list(EVENTS), retries=2)
    assert seen.outcomes[1] == (R, R, R), "no ACCEPTED while the tail is pending"
    second = handle.calls[before[0] :][:2]
    assert second == [b': 3, "pad": "cccc"}\n', b'ad": "cccc"}\n'], "only the tail was offered"
    assert seen.outcomes[2] == (A, A, A)
    assert path.read_bytes() == WHOLE
    sink.close()


def test_a_pending_tail_shortens_by_exactly_what_a_failed_try_took(tmp_path: Path) -> None:
    sink = FileSink(str(tmp_path / "j.ndjson"))
    handle = budget(sink, 52)
    seen = Rounds(sink, lambda number: handle.refill(7) if number == 1 else None)
    run(sink, list(EVENTS), retries=1)
    assert seen.outcomes[1] == (U, R, R), "the last round: the torn event is UNKNOWN"
    assert sink._tail is not None and sink._tail.remaining == b'ad": "cccc"}\n'
    assert (tmp_path / "j.ndjson").read_bytes() == WHOLE[:59]


@BOTH
def test_a_round_of_nothing_encodable_leaves_a_pending_tail_unwritten(
    cls: type, tmp_path: Path
) -> None:
    """A round with no line to write makes no write call, so the tail waits for one that has."""
    path = tmp_path / "nothing.ndjson"
    sink = cls(str(path))
    handle = budget(sink, 9, once=True)
    sink.emit(list(EVENTS[:1]))
    assert sink._tail is not None and sink._tail.remaining == b'"pad": "aaaa"}\n'
    offered = len(handle.calls)
    verdict = sink.deliver([{"blob": b"unencodable"}, {"blob": b"nor this"}])
    assert verdict.outcomes == (D, D)
    assert len(handle.calls) == offered, "no write call, for the tail or for anything else"
    assert sink._tail is not None and sink._tail.remaining == b'"pad": "aaaa"}\n'
    assert path.read_bytes() == b'{"n": 1, ', "the healthy handle was not asked to finish it"
    sink.close()
    assert path.read_bytes() == b'{"n": 1, "pad": "aaaa"}\n'


def test_another_section_finishing_the_tail_leaves_the_run_nothing_to_write(
    tmp_path: Path,
) -> None:
    """Run A tears event 1; another thread's section finishes it before A's next round."""
    path = tmp_path / "two.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 5, once=True)
    other = {"n": 9, "pad": "zzzz"}

    def between(number: int) -> None:
        if number == 1:
            assert run_concurrently(lambda *_: sink.emit([other]), 1) == []

    seen = Rounds(sink, between)
    run(sink, list(EVENTS[:3]), retries=1)
    assert seen.outcomes[0] == (R, R, R)
    assert seen.outcomes[-1] == (A, A, A), "event 1 is ACCEPTED with nothing written for it"
    assert handle.calls == [
        LINES[0] + LINES[1] + LINES[2],
        (LINES[0] + LINES[1] + LINES[2])[5:],
        LINES[0][5:],
        b'{"n": 9, "pad": "zzzz"}\n',
        LINES[1] + LINES[2],
    ]
    assert ids_of(path) == [1, 9, 2, 3]
    sink.close()


def test_an_occurrence_whose_tail_another_section_finished_is_accepted_under_a_failing_tail(
    tmp_path: Path,
) -> None:
    """The occurrence is settled before the tail's failure is raised, seen in the outcome tuple.

    Run A tears event 1. Another section finishes A's tail and tears a line of its own, and the
    fault returns, so A's next round cannot write the tail now pending. A's event 1 has landed
    whole all the same, and reporting it ``RETRYABLE`` would have it written again.
    """
    path = tmp_path / "foreign.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 5)
    other = {"n": 9, "pad": "zzzz"}

    def between(number: int) -> None:
        if number == 1:
            handle.refill(19 + 6)
            assert run_concurrently(lambda *_: sink.emit([other]), 1) == []
        else:
            handle.lift()

    seen = Rounds(sink, between)
    run(sink, list(EVENTS[:3]), retries=2)
    assert seen.outcomes[1] == (A, R, R)
    assert seen.outcomes[2] == (A, A)
    assert ids_of(path) == [1, 9, 2, 3]
    assert path.read_bytes() == LINES[0] + b'{"n": 9, "pad": "zzzz"}\n' + LINES[1] + LINES[2]
    sink.close()


def test_a_multisink_child_that_tore_a_line_is_never_resent_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One round, so the tearing round is the last: counted, not resent, and not duplicated."""
    path = tmp_path / "multi.ndjson"
    sink = FileSink(str(path))
    budget(sink, 5, once=True)
    emits: list[int] = []
    real = FileSink.emit

    def counted(self: FileSink, batch: list[dict[str, object]]) -> None:
        emits.append(len(batch))
        real(self, batch)

    monkeypatch.setattr(FileSink, "emit", counted)
    worker = _worker(MultiSink(sink), 3)
    worker.submit(list(EVENTS))
    assert worker.flush(timeout=30.0)
    assert emits == [5], "emit was called once and returned"
    assert worker.failed_batches == 0
    sink.close()
    assert path.read_bytes() == LINES[0], "the torn event once, events 2 to 5 absent"
    assert sink.losses() == SinkLosses(dropped=0, failed=5)


def test_a_caller_resending_on_oserror_never_duplicates_a_torn_line(tmp_path: Path) -> None:
    """The inline twin: ``emit`` returned, so the caller's ``except OSError`` never resends."""
    path = tmp_path / "inline.ndjson"
    sink = FileSink(str(path))
    budget(sink, 5, once=True)
    resent = False
    try:
        sink.emit(list(EVENTS))
    except OSError:
        resent = True
        sink.emit(list(EVENTS))
    assert not resent
    sink.close()
    assert path.read_bytes() == LINES[0]
    assert sink.losses() == SinkLosses(dropped=0, failed=5)


def test_a_tear_under_an_error_no_resend_cures_is_unknown_and_the_run_settles(
    tmp_path: Path,
) -> None:
    sink = FileSink(str(tmp_path / "eacces.ndjson"))
    budget(sink, 5, _raises(errno.EACCES))
    seen = Rounds(sink)
    delivery = run(sink, list(EVENTS), retries=3)
    assert seen.outcomes == [(U, J, J, J, J)], "the first of four rounds, and the only one"
    assert delivery.failed is False

    inline = FileSink(str(tmp_path / "eacces-inline.ndjson"))
    budget(inline, 5, _raises(errno.EACCES))
    inline.emit(list(EVENTS))


def test_a_run_ending_on_the_tearing_round_counts_the_torn_event_once(tmp_path: Path) -> None:
    """Worker path: the torn event is in ``failed_events`` once and in no catcher's term."""
    path = tmp_path / "ends.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 5)
    seen = Rounds(sink)
    worker = _worker(sink, 0)
    worker.submit([EVENTS[0]])
    assert worker.flush(timeout=30.0), "the run settled"
    assert seen.outcomes == [(U,)]
    assert sink.log_foundry_counters.losses().failed == 1
    assert worker.failed_batches == 0
    handle.lift()
    worker.submit([EVENTS[1]])
    assert worker.flush(timeout=30.0)
    assert handle.calls[-2:] == [LINES[0][5:], LINES[1]], "the tail before the next batch's line"
    assert ids_of(path) == [1, 2]
    sink.close()


def test_the_orphan_path_counts_a_torn_event_once_and_not_in_orphan_lost(tmp_path: Path) -> None:
    path = tmp_path / "orphan.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 5)
    log_foundry.configure(service="orphan", sink=sink)
    log_foundry.info("torn")
    health = log_foundry.health()
    assert health.orphan_lost == 0
    assert sink.log_foundry_counters.losses().failed == 1
    handle.lift()
    log_foundry.info("next")
    lines = lines_of(path)
    assert len(lines) == 2
    assert [json.loads(line)["message"] for line in lines] == ["torn", "next"]


def test_a_declined_run_writes_nothing_more_and_close_finishes_the_torn_line(
    tmp_path: Path,
) -> None:
    """The residue: a torn event left ``RETRYABLE`` by a run the worker then declines."""
    path = tmp_path / "declined.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 5, once=True)
    seen = Rounds(sink)
    stop = WaitRecorder()
    stop.set()
    delivery = _engine.run_delivery(
        sink, list(EVENTS[:3]), worker_retries=3, stop=stop, decline=lambda longest: True
    )
    assert delivery is not None and delivery.declined
    assert seen.outcomes == [(R, R, R)]
    assert len(handle.calls) == 2, "nothing more was written in that run"
    assert path.read_bytes() == LINES[0][:5]
    sink.close()
    assert path.read_bytes() == LINES[0]


@BOTH
def test_one_object_twice_with_the_first_occurrence_torn(cls: type, tmp_path: Path) -> None:
    path = tmp_path / "twice.ndjson"
    sink = cls(str(path))
    handle = budget(sink, 5, once=True)
    same = EVENTS[0]
    seen = Rounds(sink)
    run(sink, [same, same, EVENTS[1]], retries=1)
    assert seen.outcomes == [(R, R, R), (A, A, A)]
    assert handle.calls[-2:] == [LINES[0][5:], LINES[0] + LINES[1]]
    assert path.read_bytes() == LINES[0] + LINES[0] + LINES[1]
    sink.close()


@pytest.mark.parametrize("lands", [True, False], ids=["lands", "fails"])
@BOTH
def test_close_tries_a_pending_tail_once_and_closes_whatever_it_does(
    cls: type, lands: bool, tmp_path: Path
) -> None:
    path = tmp_path / "close.ndjson"
    sink = cls(str(path))
    handle = budget(sink, 5)
    sink.emit([EVENTS[0]])
    tried = len(handle.calls)
    if lands:
        handle.lift()
        sink.close()
        assert path.read_bytes() == LINES[0]
    else:
        with pytest.raises(OSError) as caught:
            sink.close()
        assert caught.value.errno == 28
        assert path.read_bytes() == LINES[0][:5]
    assert len(handle.calls) == tried + 1, "one try"
    assert handle.real.closed, "the descriptor is closed either way"
    sink.close()
    assert len(handle.calls) == tried + 1, "a second close is a no-op"


def test_a_wrapped_sink_keeps_the_torn_occurrence_across_rounds(tmp_path: Path) -> None:
    """Through ``TransformSink``, whose function builds a new object for every event it is given.

    The wrapper runs the function once per event per run and hands the file sink the same object
    in every round, which is what lets the sink find its torn occurrence by identity. A
    transform that rebuilt its objects each round would have the third line written whole after
    its tail.
    """
    path = tmp_path / "wrapped.ndjson"
    inner = FileSink(str(path))
    handle = budget(inner, 52, once=True)
    sink = TransformSink(inner, lambda event: dict(event))
    assert read_deliver(sink) is not None, "the premise: the wrapper offers deliver"
    delivery = run(sink, list(EVENTS), retries=1)
    assert delivery.accepted == 5
    assert handle.calls[-2:] == [b': 3, "pad": "cccc"}\n', LINES[3] + LINES[4]]
    assert path.read_bytes() == WHOLE
    inner.close()


# --- FR-002: an exception that is not an OSError passing through a write section --------------


class _Fused(int):
    """A write call's count that raises ``KeyboardInterrupt`` the first time it is used.

    It stands for an interrupt that lands after the write call returned its count: the handle
    really wrote, the count is real, and the sink's first comparison or sum of it raises.
    """

    def _trip(self) -> None:
        if not vars(self).get("spent"):
            vars(self)["spent"] = True
            raise KeyboardInterrupt

    def __gt__(self, other: object) -> bool:
        self._trip()
        return int(self) > other  # type: ignore[operator]

    def __lt__(self, other: object) -> bool:
        self._trip()
        return int(self) < other  # type: ignore[operator]

    def __ge__(self, other: object) -> bool:
        self._trip()
        return int(self) >= other  # type: ignore[operator]

    def __le__(self, other: object) -> bool:
        self._trip()
        return int(self) <= other  # type: ignore[operator]

    def __add__(self, other: object) -> int:
        self._trip()
        return int(self) + other  # type: ignore[operator,no-any-return]

    def __radd__(self, other: object) -> int:
        self._trip()
        return other + int(self)  # type: ignore[operator,no-any-return]

    def __rsub__(self, other: object) -> int:
        self._trip()
        return other - int(self)  # type: ignore[operator,no-any-return]

    def __index__(self) -> int:
        self._trip()
        return int.__index__(self)


class _FusedHandle:
    """Really writes, and returns the count as a :class:`_Fused` for its first call only."""

    def __init__(self, real: Any) -> None:
        self.real = real
        self.armed = True

    def write(self, data: bytes) -> int:
        count = self.real.write(data)
        if self.armed:
            self.armed = False
            return _Fused(count)
        return int(count)

    def close(self) -> None:
        self.real.close()

    @property
    def closed(self) -> bool:
        return bool(self.real.closed)


def test_an_interrupt_after_the_count_is_stored_leaves_one_byte_order_mark(tmp_path: Path) -> None:
    path = tmp_path / "fused.ndjson"
    sink = FileSink(str(path), encoding="utf-16")
    sink._stream = _FusedHandle(sink._stream)  # type: ignore[assignment]
    with pytest.raises(KeyboardInterrupt):
        sink.emit(list(EVENTS[:2]))
    assert sink.losses() is None, "a BaseException moves no counter"
    sink.emit(list(EVENTS[2:]))
    sink.close()
    mark = b"\xff\xfe" if sys.byteorder == "little" else b"\xfe\xff"
    raw = path.read_bytes()
    assert raw.count(mark) == 1 and raw.startswith(mark)
    assert raw.decode("utf-16") == WHOLE.decode()


def test_an_interrupt_after_the_count_is_stored_still_grows_size_by_the_bytes_taken(
    tmp_path: Path,
) -> None:
    sink = RotatingFileSink(str(tmp_path / "fused-size.ndjson"))
    sink._stream = _FusedHandle(sink._stream)  # type: ignore[assignment]
    with pytest.raises(KeyboardInterrupt):
        sink.emit(list(EVENTS))
    assert sink._size == 5 * LINE
    assert (tmp_path / "fused-size.ndjson").read_bytes() == WHOLE
    sink.close()


def test_an_interrupt_before_any_byte_records_no_tail_and_leaves_the_mark_owed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "early.ndjson"
    sink = FileSink(str(path), encoding="utf-16")
    interrupt = KeyboardInterrupt()
    handle = budget(sink, 0, lambda: interrupt, once=True)
    with pytest.raises(KeyboardInterrupt) as caught:
        sink.emit(list(EVENTS))
    assert caught.value is interrupt
    assert sink._tail is None and sink.losses() is None
    sink.emit(list(EVENTS))
    sink.close()
    mark = b"\xff\xfe" if sys.byteorder == "little" else b"\xfe\xff"
    raw = path.read_bytes()
    assert raw.count(mark) == 1 and raw.decode("utf-16") == WHOLE.decode()
    assert handle.raised == 1


@BOTH
def test_a_keyboard_interrupt_from_the_handle_propagates_and_moves_no_counter(
    cls: type, tmp_path: Path
) -> None:
    """The retry sweep's third property, and FR-002: a tail is still recorded on the way out."""
    path = tmp_path / "interrupt.ndjson"
    sink = cls(str(path))
    interrupt = KeyboardInterrupt()
    handle = budget(sink, 5, lambda: interrupt, once=True)
    worker_stop = WaitRecorder()
    worker_stop.set()
    with pytest.raises(KeyboardInterrupt) as caught:
        _engine.run_delivery(sink, list(EVENTS[:2]), worker_retries=3, stop=worker_stop)
    assert caught.value is interrupt
    assert len(handle.calls) == 2, "one call that took five bytes, one that raised"
    assert sink.losses() is None
    assert sink._tail is not None and sink._tail.remaining == LINES[0][5:]
    sink.close()
    assert path.read_bytes() == LINES[0], "the cut line got its tail"


# --- FR-004: counts, the predicate, the line, the 1.x emit ------------------------------------


@BOTH
def test_a_lost_batch_is_counted_once_per_run_through_the_worker(cls: type, tmp_path: Path) -> None:
    sink = cls(str(tmp_path / "lost.ndjson"))
    budget(sink, 0)
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(list(EVENTS))
    assert not worker.flush(timeout=30.0)
    assert len(seen.outcomes) == 4, "max_retries + 1 calls for a curable errno"
    assert sink.losses() == SinkLosses(dropped=0, failed=5), "n, not 4n"
    assert worker.failed_batches == 1


@BOTH
def test_an_errno_no_resend_cures_costs_one_deliver_call_and_no_wait(
    cls: type, tmp_path: Path
) -> None:
    sink = cls(str(tmp_path / "eacces.ndjson"))
    budget(sink, 0, _raises(errno.EACCES))
    seen = Rounds(sink)
    stop = WaitRecorder()
    delivery = _engine.run_delivery(sink, list(EVENTS), worker_retries=3, stop=stop)
    assert delivery is not None and delivery.failed
    assert seen.outcomes == [(J, J, J, J, J)]
    assert stop.waits == [], "the engine's wait was never entered"

    curable = cls(str(tmp_path / "enospc.ndjson"))
    budget(curable, 0)
    again = Rounds(curable)
    waited = WaitRecorder()
    _engine.run_delivery(curable, list(EVENTS), worker_retries=3, stop=waited)
    assert len(again.outcomes) == 4 and len(waited.waits) == 3


@pytest.mark.parametrize(
    ("name", "calls", "waits"),
    [("EACCES", 1, 0), ("ENOSPC", 4, 3)],
    ids=["no-resend-cures-it", "a-resend-can-cure-it"],
)
@BOTH
def test_a_real_worker_resends_only_what_a_resend_can_cure(
    cls: type,
    name: str,
    calls: int,
    waits: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The criterion above on the worker's own drain thread, at ``max_retries=3``.

    The engine's wait is replaced by a recorder that returns at once, so the count of waits is
    read from the engine itself and the curable case does not sleep its backoff.
    """
    entered: list[float] = []

    def recorded(delay: float, stop: threading.Event | None) -> threading.Event | None:
        entered.append(delay)
        return stop

    monkeypatch.setattr(_engine, "_wait", recorded)
    sink = cls(str(tmp_path / "worker.ndjson"))
    budget(sink, 0, _raises(getattr(errno, name)))
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(list(EVENTS))
    assert not worker.flush(timeout=30.0)
    assert len(seen.outcomes) == calls
    assert len(entered) == waits
    assert worker.failed_batches == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=5)


@pytest.mark.parametrize(
    ("name", "retried"),
    [
        ("ENOSPC", True),
        ("EDQUOT", True),
        ("EFBIG", True),
        ("EIO", True),
        ("EAGAIN", True),
        ("EMFILE", True),
        ("ENFILE", True),
        ("EACCES", False),
        ("EPERM", False),
        ("EROFS", False),
        ("ENOENT", False),
    ],
)
@BOTH
def test_the_predicate_retries_only_what_a_resend_can_cure(
    cls: type, name: str, retried: bool, tmp_path: Path
) -> None:
    code = getattr(errno, name, None)
    if code is None:
        pytest.skip(f"this platform has no {name}")
    sink = cls(str(tmp_path / "p.ndjson"))
    try:
        assert sink._retryable_error(OSError(code, name)) is retried
        assert sink._retryable_error(OSError("no errno")) is False
        assert sink._retryable_error(ConnectionError("down")) is False
        assert sink._retryable_error(TimeoutError("slow")) is False
        assert sink._retryable_error(ValueError("not an OSError")) is False
    finally:
        sink.close()


@BOTH
def test_an_inline_emit_that_landed_nothing_raises_both_types(cls: type, tmp_path: Path) -> None:
    sink = cls(str(tmp_path / "denied.ndjson"))
    original = PermissionError(13, "Permission denied")
    budget(sink, 0, lambda: original)
    with pytest.raises(PermissionError) as caught:
        sink.emit(list(EVENTS))
    assert isinstance(caught.value, OSError)
    assert isinstance(caught.value, SinkDeliveryError)
    assert caught.value.__cause__ is original
    assert sink.losses() == SinkLosses(dropped=0, failed=5)


@BOTH
def test_an_inline_emit_that_landed_something_returns_and_counts(cls: type, tmp_path: Path) -> None:
    sink = cls(str(tmp_path / "some.ndjson"))
    budget(sink, 2 * LINE)
    sink.emit(list(EVENTS))
    assert sink.losses() == SinkLosses(dropped=0, failed=3)
    assert (tmp_path / "some.ndjson").read_bytes() == LINES[0] + LINES[1]


@BOTH
def test_an_inline_emit_that_landed_nothing_on_a_value_error_raises_both_types(
    cls: type, tmp_path: Path
) -> None:
    sink = cls(str(tmp_path / "value.ndjson"))
    original = ValueError("the handle broke")
    budget(sink, 0, lambda: original)
    with pytest.raises(ValueError) as caught:
        sink.emit(list(EVENTS))
    assert isinstance(caught.value, SinkDeliveryError)
    assert caught.value.__cause__ is original


def _exhausted(cls: type, tmp_path: Path, error: Callable[[], BaseException], name: str) -> Any:
    sink = cls(str(tmp_path / name))
    budget(sink, 0, error)
    seen = Rounds(sink)
    stop = WaitRecorder()
    delivery = _engine.run_delivery(sink, list(EVENTS), worker_retries=3, stop=stop)
    return sink, seen, stop, delivery


@BOTH
def test_a_type_error_is_one_attempt_counted_as_exhaustion_counts(
    cls: type, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The retry sweep's first property, for a sink with an ``errno`` predicate."""
    spent, _, _, spent_run = _exhausted(cls, tmp_path, _raises(errno.ENOSPC), "spent.ndjson")
    capsys.readouterr()
    sink, seen, stop, delivery = _exhausted(
        cls, tmp_path, lambda: TypeError("the caller's own code"), "type.ndjson"
    )
    assert len(seen.outcomes) == 1 and stop.waits == []
    assert sink.losses() == spent.losses() == SinkLosses(dropped=0, failed=5)
    assert (delivery.failed, spent_run.failed) == (True, True), "one failed batch each"
    lines = _lost(capsys.readouterr().err)
    assert lines == [f"log-foundry: lost 5 event(s); {cls.__name__}, 1 attempt(s), TypeError"]


def test_a_type_error_through_a_real_worker_is_one_failed_batch(tmp_path: Path) -> None:
    sink = FileSink(str(tmp_path / "type-worker.ndjson"))
    budget(sink, 0, lambda: TypeError("the caller's own code"))
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(list(EVENTS))
    assert not worker.flush(timeout=30.0)
    assert len(seen.outcomes) == 1
    assert worker.failed_batches == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=5)


@BOTH
def test_a_predicate_that_raises_fails_at_once_naming_the_original(
    cls: type, tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The retry sweep's fourth property."""
    sink = cls(str(tmp_path / "broken.ndjson"))
    budget(sink, 0)

    def broken(err: Exception) -> bool:
        raise LookupError("the predicate broke")

    monkeypatch.setattr(sink, "_retryable_error", broken)
    seen = Rounds(sink)
    run(sink, list(EVENTS), retries=3)
    assert seen.outcomes == [(J, J, J, J, J)]
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1 and "OSError errno=28" in lines[0] and "LookupError" not in lines[0]


@BOTH
def test_losses_is_none_until_something_is_counted(cls: type, tmp_path: Path) -> None:
    sink = cls(str(tmp_path / "clean.ndjson"))
    sink.emit(list(EVENTS))
    assert sink.losses() is None
    log_foundry.configure(service="clean", sink=sink)
    assert log_foundry.health().sink is None


@BOTH
def test_a_settled_run_counts_drops_and_losses_apart(cls: type, tmp_path: Path) -> None:
    sink = cls(str(tmp_path / "settled.ndjson"))
    budget(sink, LINE)
    seen = Rounds(sink)
    sink.emit([EVENTS[0], {"blob": b"unencodable"}, EVENTS[1], EVENTS[2]])
    assert seen.outcomes == [(A, D, R, R)]
    assert sink.losses() == SinkLosses(dropped=1, failed=2)


@BOTH
def test_a_failed_run_counts_its_unencodable_events_in_failed(cls: type, tmp_path: Path) -> None:
    sink = cls(str(tmp_path / "failed.ndjson"))
    budget(sink, 0)
    with pytest.raises(SinkDeliveryError):
        sink.emit([EVENTS[0], {"blob": b"unencodable"}, EVENTS[1]])
    assert sink.losses() == SinkLosses(dropped=0, failed=3)


@BOTH
def test_a_closed_sink_refuses_by_identity_and_counts_nothing(
    cls: type, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = cls(str(tmp_path / "closed.ndjson"))
    sink.close()
    built: list[SinkDeliveryError] = []
    from log_foundry.sinks import file as file_mod

    real = file_mod.final_refusal

    def recording(refuser: object, message: str) -> SinkDeliveryError:
        built.append(real(refuser, message))
        return built[-1]

    monkeypatch.setattr(file_mod, "final_refusal", recording)
    with pytest.raises(SinkDeliveryError) as caught:
        sink.emit(list(EVENTS[:2]))
    assert caught.value is built[0], "the refusal the round built, by identity"
    assert type(caught.value) is SinkDeliveryError
    assert str(caught.value) == f"{cls.__name__} wrote none of 2 event(s): the sink is closed"
    assert is_final_refusal(caught.value, sink)
    assert sink.losses() is None


@BOTH
def test_a_close_during_a_backoff_refuses_the_next_round(
    cls: type, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = cls(str(tmp_path / "backoff.ndjson"))
    handle = budget(sink, 0)
    seen = Rounds(sink)
    closed_before_round: list[int] = []

    def close_in_the_backoff(cycle: int) -> float:
        closer = threading.Thread(target=sink.close)
        closer.start()
        closer.join(20.0)
        assert not closer.is_alive(), "the close returned before the next round would start"
        closed_before_round.append(len(seen.outcomes))
        return 0.0

    monkeypatch.setattr(_engine, "cycle_backoff", close_in_the_backoff)
    delivery = run(sink, list(EVENTS[:2]), retries=3)
    assert closed_before_round == [1]
    assert seen.outcomes == [(R, R), (J, J)]
    assert len(handle.calls) == 1, "the refused round wrote nothing"
    assert delivery.failed and delivery.refused is not None
    assert is_final_refusal(delivery.refused, sink)
    assert sink.losses() is None, "no counter moved"


class _Reopening(FileSink):
    """A caller's subclass that inherits ``emit`` and re-opens itself after a close."""

    def reopen(self) -> None:
        FileSink.__init__(self, self._path)


def test_a_closed_subclass_that_inherits_emit_is_not_resent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Supersedes 1.x: on the engine a closed sink's round is final, a subclass's included."""
    sink = _Reopening(str(tmp_path / "subclass.ndjson"))
    sink.close()
    backoffs: list[int] = []

    def reopen_in_the_backoff(cycle: int) -> float:
        backoffs.append(cycle)
        sink.reopen()
        return 0.0

    monkeypatch.setattr(_engine, "cycle_backoff", reopen_in_the_backoff)
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(list(EVENTS[:2]))
    assert not worker.flush(timeout=30.0)
    assert len(seen.outcomes) == 1, "exactly one deliver call"
    assert backoffs == [], "no backoff was entered"
    assert worker.failed_batches == 1


def test_a_round_lost_to_enospc_registers_one_line_on_the_last_round(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sink = FileSink(str(tmp_path / "line.ndjson"))
    budget(sink, 0)
    with pytest.raises(OSError):
        sink.emit(list(EVENTS[:2]))
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); FileSink, 1 attempt(s), OSError errno=28"
    ]

    plain = FileSink(str(tmp_path / "plain.ndjson"))
    budget(plain, 0, lambda: OSError("no errno at all"))
    with pytest.raises(OSError):
        plain.emit(list(EVENTS[:2]))
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); FileSink, 1 attempt(s), OSError"
    ]

    rounds = FileSink(str(tmp_path / "rounds.ndjson"))
    budget(rounds, 0)
    run(rounds, list(EVENTS[:2]), retries=2)
    err = capsys.readouterr().err
    assert _lost(err) == ["log-foundry: lost 2 event(s); FileSink, 3 attempt(s), OSError errno=28"]
    assert '"n"' not in err and "pad" not in err, "no event data"


@BOTH
def test_the_worker_the_orphan_path_and_the_adapter_all_reach_deliver(
    cls: type, tmp_path: Path
) -> None:
    path = tmp_path / "paths.ndjson"
    sink = cls(str(path))
    assert read_deliver(sink) is not None
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    worker.submit(list(EVENTS[:2]))
    assert worker.flush(timeout=30.0)
    assert len(seen.outcomes) == 1

    log_foundry.configure(service="paths", sink=sink)
    log_foundry.info("orphan")
    assert len(seen.outcomes) == 2 and log_foundry.health().orphan_lost == 0

    sink.emit([EVENTS[2]])
    assert len(seen.outcomes) == 3
    assert seen.outcomes[0] == (A, A) and seen.outcomes[2] == (A,)
    assert len(lines_of(path)) == 4


# --- FR-002: the one step, on the paths the criteria above do not reach -----------------------


def test_an_interrupt_after_a_tails_count_is_stored_does_not_write_the_tail_again(
    tmp_path: Path,
) -> None:
    """The tail's write sequence has the same step: its count is stored, then an interrupt."""
    path = tmp_path / "fused-tail.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 5)
    sink.emit([EVENTS[0]])
    assert sink._tail is not None
    sink._stream = _FusedHandle(handle.real)  # type: ignore[assignment]
    with pytest.raises(KeyboardInterrupt):
        sink.emit([EVENTS[1]])
    assert sink._tail is None, "the tail the handle took is not pending any more"
    sink.emit([EVENTS[1]])
    sink.close()
    assert path.read_bytes() == LINES[0] + LINES[1]


def test_the_step_leaves_the_same_state_when_an_interrupt_lands_inside_it(tmp_path: Path) -> None:
    """Idempotent: run once more after an interrupt inside it, ``_size`` is still exact."""
    sink = RotatingFileSink(str(tmp_path / "twice.ndjson"))
    real = sink._follow
    fired: list[int] = []

    def follow_then_interrupt(baseline: int, taken: int) -> None:
        real(baseline, taken)
        if not fired:
            fired.append(taken)
            raise KeyboardInterrupt

    sink._follow = follow_then_interrupt  # type: ignore[method-assign]
    with pytest.raises(KeyboardInterrupt):
        sink.emit(list(EVENTS))
    assert fired == [5 * LINE]
    assert sink._size == 5 * LINE, "the bytes taken, counted once"
    sink.close()


@pytest.mark.parametrize("returned", [None, 0, -1, 10_000, "24"], ids=repr)
def test_a_write_call_that_returns_no_count_ends_the_round_as_a_stall(
    returned: object, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A handle that does not say what it took has taken nothing the sink can count."""

    class Silent:
        def __init__(self) -> None:
            self.calls = 0

        def write(self, data: bytes) -> object:
            self.calls += 1
            return returned

        def close(self) -> None:
            pass

    sink = FileSink(str(tmp_path / "silent.ndjson"))
    sink._stream.close()
    silent = Silent()
    sink._stream = silent  # type: ignore[assignment]
    seen = Rounds(sink)
    run(sink, list(EVENTS[:2]), retries=1)
    assert seen.outcomes == [(R, R), (R, R)], "retried as a would-block, and never spun"
    assert silent.calls == 2, "one call a round"
    stalled = type(OSError(errno.EAGAIN, "stalled")).__name__
    assert _lost(capsys.readouterr().err) == [
        f"log-foundry: lost 2 event(s); FileSink, 2 attempt(s), {stalled} errno={errno.EAGAIN}"
    ]


def test_a_later_occurrence_of_the_same_object_is_written_whole_after_the_first_landed(
    tmp_path: Path,
) -> None:
    """The run's note ends with the occurrence it names, so the next one is a line of its own.

    One object twice. The first occurrence is torn; the second round finishes its tail and then
    fails before the second occurrence's line; the third round is offered that occurrence alone.
    A note still naming the object would have it reported ``ACCEPTED`` with nothing written.
    """
    path = tmp_path / "later.ndjson"
    sink = FileSink(str(path))
    handle = budget(sink, 5)
    same = EVENTS[0]

    def between(number: int) -> None:
        if number == 1:
            handle.refill(LINE - 5)
        else:
            handle.lift()

    seen = Rounds(sink, between)
    run(sink, [same, same], retries=2)
    assert seen.outcomes == [(R, R), (A, R), (A,)]
    assert path.read_bytes() == LINES[0] + LINES[0]
    sink.close()


@BOTH
def test_a_closed_sinks_refusal_costs_one_deliver_call_through_the_worker(
    cls: type, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The engine's twin of the worker's 1.x rule: one call, one failed batch, no counter."""
    sink = cls(str(tmp_path / "closed-worker.ndjson"))
    sink.close()
    seen = Rounds(sink)
    worker = _worker(sink, 3)
    capsys.readouterr()
    worker.submit(list(EVENTS[:2]))
    assert not worker.flush(timeout=30.0)
    assert seen.outcomes == [(J, J)], "one deliver call for one refused batch"
    assert worker.failed_batches == 1
    assert sink.losses() is None
    abandoned = [line for line in capsys.readouterr().err.splitlines() if "abandoned" in line]
    assert abandoned == ["log-foundry: lost 2 event(s); batch abandoned after 1 emit attempts"]
