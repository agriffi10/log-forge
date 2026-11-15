"""SPEC-082 FR-005 — a forked child never finishes its parent's torn line.

Under a real ``os.fork``. The parent holds a pending tail when it forks: a line whose first nine
bytes are on disk and whose remainder, the sink's tail, is the parent's to write. The child's
``reacquire_after_fork`` drops that tail and opens a descriptor of its own, so the child's writes
carry none of it and the parent's next section finishes the line once.

These replace the SPEC-039 FR-004 tests that parked the parent "between a write and its flush":
on an unbuffered handle that window does not exist, and what a child could write twice is the
tail. Their two guards are kept. A line is on disk before the fork, so a child reopening in
``"w"`` mode would be seen truncating it; and the unrepaired behaviour is demonstrated with the
hook taken away, so the tests cannot pass against a fork that was never in danger.

The child's own line lands after the parent's torn prefix, so that one line does not parse. That
is the cross-process case the spec puts out of scope; these tests count markers in the bytes.
Serves invariant 12.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest

import log_foundry
import log_foundry.sinks.file as file_mod
from file_handles import budget
from log_foundry import _fork
from log_foundry.sinks.file import FileSink, RotatingFileSink
from log_foundry.sinks.multi import MultiSink
from test_fork_lifecycle import _log_in_child, run_in_child

if TYPE_CHECKING:
    import pathlib

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")

TAIL_MARKER = b"MARKER-ONLY-IN-THE-TAIL"
"""A string at the end of the torn line, so it is in the pending tail and nowhere else."""


def torn_sink(
    tmp_path: pathlib.Path, kind: str, *, name: str = "torn.ndjson"
) -> tuple[Any, Any, pathlib.Path]:
    """Builds a file-backed sink with a line cut partway, and makes it the process sink.

    One line is written whole first, so the file is not empty when a child reopens it. Then a
    write that takes nine bytes and fails once leaves the rest of the next line pending.

    Args:
      tmp_path: The directory to write into.
      kind: ``"file"``, ``"rotating"`` or ``"multi"``, the last a ``FileSink`` inside a
        ``MultiSink``, so the sink holding the tail is not the one the library was handed.
      name: The file to append to.

    Returns:
      The configured sink, the file sink inside it, and the path.
    """
    path = tmp_path / name
    inner: Any = RotatingFileSink(str(path)) if kind == "rotating" else FileSink(str(path))
    inner.emit([{"msg": "before-the-fork"}])
    budget(inner, 9, once=True)
    inner.emit([{"msg": "parent-torn", "end": TAIL_MARKER.decode()}])
    assert inner._tail is not None and TAIL_MARKER in inner._tail.remaining
    assert TAIL_MARKER not in path.read_bytes(), "the tail is not on disk, or this proves nothing"
    sink: Any = MultiSink(inner) if kind == "multi" else inner
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    return sink, inner, path


@pytest.mark.parametrize("kind", ["file", "rotating", "multi"])
def test_a_forked_child_writes_none_of_its_parents_tail(tmp_path: pathlib.Path, kind: str) -> None:
    """The child's batch carries none of the tail, and the parent's next round writes it once."""
    _sink, inner, path = torn_sink(tmp_path, kind, name=f"{kind}.ndjson")
    child = run_in_child(_log_in_child, timeout=6)
    assert child.output == "logged", child.output
    after_the_child = path.read_bytes()
    assert b'"message": "child"' in after_the_child, "the child wrote its own line"
    assert TAIL_MARKER not in after_the_child, "and none of the parent's tail"
    assert after_the_child.count(b"before-the-fork") == 1, "reopened in append mode"

    inner.emit([{"msg": "parent-next"}])
    written = path.read_bytes()
    assert written.count(TAIL_MARKER) == 1
    assert written.count(b"before-the-fork") == 1
    assert written.endswith(b'{"msg": "parent-next"}\n')


def test_the_same_child_writes_the_tail_when_the_hook_is_taken_away(
    tmp_path: pathlib.Path,
) -> None:
    """The unrepaired behaviour, demonstrated: the tail's bytes land twice.

    The hook step is removed for the length of one fork, so the child inherits the pending tail
    and its first section writes it, as the parent's next section then does again. Without this
    the test above could pass against a child that was never going to write the tail.
    """
    _sink, inner, path = torn_sink(tmp_path, "file", name="unrepaired.ndjson")
    original = _fork._reacquire_transports

    def keep_the_parents_tail(holders: list[Any]) -> None:
        pass

    _fork._reacquire_transports = keep_the_parents_tail  # type: ignore[assignment]
    try:
        child = run_in_child(_log_in_child, timeout=6)
    finally:
        _fork._reacquire_transports = original  # type: ignore[assignment]
    assert child.output == "logged", child.output
    inner.emit([{"msg": "parent-next"}])
    assert path.read_bytes().count(TAIL_MARKER) == 2


def test_the_hook_runs_before_any_registered_handler(tmp_path: pathlib.Path) -> None:
    """SPEC-039 FR-001 AC-2's third step, restated on the tail: locks, the hook, then handlers.

    ``decorator``'s rebuild registers first and has a live drain thread by the time any later
    handler runs, emitting into the very sink whose tail is still the parent's. The probe is
    registered at position 0, so it reads the state the first registered handler sees.
    """
    _sink, inner, _path = torn_sink(tmp_path, "file", name="ordering.ndjson")
    inherited = inner._stream
    seen: list[str] = []

    def probe() -> None:
        dropped = inner._tail is None and inner._stream is not inherited
        seen.append("reacquired" if dropped else "still-the-parents")

    _fork._child_handlers.insert(0, probe)
    try:
        child = run_in_child(lambda: ",".join(seen), timeout=6)
    finally:
        _fork._child_handlers.remove(probe)
    assert child.output == "reacquired", child.output


@pytest.mark.parametrize("kind", ["file", "rotating"])
def test_the_childs_close_leaves_the_parents_descriptor_open(
    tmp_path: pathlib.Path, kind: str
) -> None:
    """The child releases only the descriptor it opened, so the parent's next write succeeds."""
    _sink, inner, path = torn_sink(tmp_path, kind, name=f"close-{kind}.ndjson")
    parent_handle = inner._stream

    def log_then_close() -> str:
        log_foundry.info("child-0")
        owns_another = inner._stream is not parent_handle
        inner.close()
        return f"closed,{owns_another}"

    child = run_in_child(log_then_close, timeout=6)
    assert child.output == "closed,True", child.output
    assert not parent_handle.real.closed, "the parent's descriptor is still open"
    inner.emit([{"msg": "parent-next"}])
    written = path.read_bytes()
    assert written.count(TAIL_MARKER) == 1
    assert written.endswith(b'{"msg": "parent-next"}\n')


@pytest.mark.parametrize("cls", [FileSink, RotatingFileSink], ids=lambda c: c.__name__)
def test_a_closed_sinks_hook_opens_nothing(
    tmp_path: pathlib.Path, cls: type, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = cls(str(tmp_path / "closed.ndjson"))
    sink.close()
    closed_handle = sink._stream
    opens: list[object] = []

    def counting(*args: object, **kwargs: object) -> object:
        opens.append(args)
        raise AssertionError("a closed sink reopened its file")

    monkeypatch.setattr(file_mod, "open", counting, raising=False)
    sink.reacquire_after_fork()
    assert opens == [] and sink._stream is closed_handle


def test_a_hook_whose_reopen_fails_still_drops_the_tail_and_leaks_nothing(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The tail is dropped before anything can raise, and the inherited handle is kept."""
    sink = FileSink(str(tmp_path / "refused.ndjson"), encoding="utf-16")
    budget(sink, 9, once=True)
    sink.emit([{"msg": "torn"}])
    assert sink._tail is not None
    inherited = sink._stream

    def refuse(*args: object, **kwargs: object) -> object:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(file_mod, "open", refuse, raising=False)
    with pytest.raises(PermissionError):
        sink.reacquire_after_fork()
    assert sink._tail is None, "the parent's tail is dropped even so"
    assert sink._stream is inherited and not inherited.real.closed
    monkeypatch.undo()
    sink.close()


@pytest.mark.parametrize("cls", [FileSink, RotatingFileSink], ids=lambda c: c.__name__)
def test_the_hook_closes_the_handle_it_inherited(tmp_path: pathlib.Path, cls: type) -> None:
    """The inherited object is closed, not left for the collector: it holds nothing to flush."""
    path = tmp_path / "inherited.ndjson"
    sink = cls(str(path))
    sink.emit([{"msg": "before"}])
    inherited = sink._stream
    sink.reacquire_after_fork()
    assert sink._stream is not inherited and inherited.closed
    assert not sink._stream.closed
    sink.emit([{"msg": "after"}])
    sink.close()
    assert path.read_bytes() == b'{"msg": "before"}\n{"msg": "after"}\n'
