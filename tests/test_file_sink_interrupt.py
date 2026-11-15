"""SPEC-082 FR-002 — an interrupt that cuts a line on a pipe still leaves the line finished once.

A real FIFO and a real signal, POSIX only. A blocking write to a pipe nobody drains takes what
fits and blocks; a signal then makes the kernel return that partial count, and the handler's
``KeyboardInterrupt`` is raised at the next bytecode boundary. So the line really is cut, and the
only record of how much was taken is the count that write call returned. The sink stores it with
no bytecode boundary before the store (``counts.extend(map(stream.write, ...))``) and brings its
tail to the stored counts as the exception passes through, so the next section finishes the line.
With the count stored by a Python-level assignment the interrupt lands before the store, the
count is lost, and these tests fail: that mutation is run in the build, and the property is
CPython's, which is why the tests run on every Python version CI does.

Each case runs in a child process, because the signal is the process's and its handler raises in
the main thread: inside the test runner a late or early signal would interrupt pytest itself. The
child sends the signal to its own main thread only once the pipe has bytes in it and has stopped
growing, which is the state "blocked in the write" leaves; a real ``io.FileIO`` is on the sink
throughout, since a Python wrapper around ``write`` would add the very bytecode boundary the
store avoids. Serves invariants 2, 5 and 7.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    not hasattr(os, "mkfifo") or sys.platform == "win32", reason="needs a FIFO and POSIX signals"
)

_CHILD = r"""
import array
import fcntl
import json
import os
import signal
import sys
import termios
import threading
import time

from log_foundry.sinks.file import FileSink, RotatingFileSink

kind, scenario, fifo, out = sys.argv[1:5]
os.mkfifo(fifo)
reader = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
sink = (FileSink if kind == "file" else RotatingFileSink)(fifo)
assert type(sink._stream).__name__ == "FileIO", "the real handle, or the store is not under test"
interrupt = KeyboardInterrupt()
main = threading.main_thread().ident


def handler(signum, frame):
    raise interrupt


signal.signal(signal.SIGUSR1, handler)


def pending():
    # Bytes waiting in the pipe, as the kernel counts them.
    size = array.array("i", [0])
    fcntl.ioctl(reader, termios.FIONREAD, size)
    return size[0]


def interrupt_once_blocked():
    # Signals the main thread once the pipe holds bytes and has stopped growing.
    last = -1
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        now = pending()
        if now > 0 and now == last:
            signal.pthread_kill(main, signal.SIGUSR1)
            return
        last = now
        time.sleep(0.05)


def emit_interrupted(batch):
    # One emit that blocks on the full pipe and is interrupted there; returns what it raised.
    watcher = threading.Thread(target=interrupt_once_blocked, daemon=True)
    watcher.start()
    try:
        sink.emit(batch)
    except KeyboardInterrupt as err:
        watcher.join(60)
        return err
    watcher.join(60)
    return None


def event(i):
    # About 300 KB a line, more than any pipe holds, with a marker at the very end of the line.
    return {"i": i, "pad": "x" * 300_000, "end": f"MARKER-{i}-ENDS-THE-LINE"}


received = bytearray()


def take_what_is_there():
    while True:
        try:
            chunk = os.read(reader, 1 << 16)
        except BlockingIOError:
            return
        if not chunk:
            return
        received.extend(chunk)


report = {}
first = emit_interrupted([event(0), event(1)])
report["first_is_the_handlers"] = first is interrupt
report["losses_after_first"] = sink.losses() is None
report["tail_after_first"] = None if sink._tail is None else len(sink._tail.remaining)
report["in_pipe_after_first"] = pending()
if scenario == "tail":
    take_what_is_there()
    second = emit_interrupted([event(2)])
    report["second_is_the_handlers"] = second is interrupt
    report["tail_after_second"] = None if sink._tail is None else len(sink._tail.remaining)
    report["losses_after_second"] = sink.losses() is None

os.set_blocking(reader, True)


def drain():
    while True:
        chunk = os.read(reader, 1 << 16)
        if not chunk:
            return
        received.extend(chunk)


draining = threading.Thread(target=drain, daemon=True)
draining.start()
sink.close()
draining.join(60)
report["drained"] = not draining.is_alive()
with open(out, "wb") as stream:
    stream.write(bytes(received))
print(json.dumps(report))
"""


def _run(kind: str, scenario: str, tmp_path: Path) -> tuple[dict[str, object], bytes]:
    out = tmp_path / "received.bin"
    ran = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _CHILD, kind, scenario, str(tmp_path / "sink.fifo"), str(out)],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert ran.returncode == 0, ran.stderr
    return json.loads(ran.stdout.strip().splitlines()[-1]), out.read_bytes()


@pytest.mark.parametrize("kind", ["file", "rotating"])
def test_an_interrupt_in_a_blocked_write_leaves_the_cut_line_finished_once(
    kind: str, tmp_path: Path
) -> None:
    """The batch is larger than the pipe; the signal lands while the write is blocked."""
    report, received = _run(kind, "line", tmp_path)
    assert report["first_is_the_handlers"] is True, "the exception propagated by identity"
    assert report["losses_after_first"] is True, "no counter moved"
    assert report["in_pipe_after_first"], "the premise: the write had taken part of the batch"
    assert report["tail_after_first"], "and the cut line's remainder is the sink's tail"
    assert report["drained"] is True
    lines = received.split(b"\n")
    assert lines[-1] == b"", "what the reader received ends on a line boundary"
    assert [json.loads(line)["i"] for line in lines[:-1]] == [0], "every line parses"
    assert received.count(b"MARKER-0-ENDS-THE-LINE") == 1


@pytest.mark.parametrize("kind", ["file", "rotating"])
def test_an_interrupt_inside_a_pending_tails_write_leaves_the_line_finished_once(
    kind: str, tmp_path: Path
) -> None:
    """The same, with the second interrupt landing while the tail itself is being written."""
    report, received = _run(kind, "tail", tmp_path)
    assert report["first_is_the_handlers"] is True
    assert report["second_is_the_handlers"] is True
    assert report["losses_after_first"] is True and report["losses_after_second"] is True
    first, second = report["tail_after_first"], report["tail_after_second"]
    assert isinstance(first, int) and isinstance(second, int)
    assert 0 < second < first, "the tail's own write took some of it and was cut in turn"
    assert report["drained"] is True
    lines = received.split(b"\n")
    assert lines[-1] == b""
    assert [json.loads(line)["i"] for line in lines[:-1]] == [0], "every line parses"
    assert received.count(b"MARKER-0-ENDS-THE-LINE") == 1
