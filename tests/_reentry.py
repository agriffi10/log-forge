"""SPEC-097 FR-005: a recording stand-in for the library's re-entrant locks, for a recording run.

FR-004 made the locks a lifecycle call takes on its own thread ``threading.RLock``s, so a handler
or finalizer can be told it is inside one. A plain ``Lock`` deadlocks when library code takes it
twice by mistake; an ``RLock`` lets that through silently. So a run of the suite with every one of
the eight oracle locks replaced by :class:`RecordingLock` records the call site whenever a thread
that already owns a lock acquires it again, and the run is read for records.

It is installed by ``tests/conftest.py`` at import, in the controller and in each xdist worker,
when ``LOG_FOUNDRY_REENTRY_RECORD`` names a file, and only then; a normal run never loads it. Each
record is one line, ``<pid> <file>:<function>:<line>``, of the first frame outside this module and
``threading.py``, appended to that file with one ``os.write``. The first line a process writes
names the library file it imported, so a run against a planted copy can prove it ran the copy.
A forked child does not repair a recording lock (``_fork._fresh_primitive`` replaces only the
real types), so a run under it can hang a fork test the normal run passes; the records are what
it is read for, not its pass count.
"""

from __future__ import annotations

import os
import sys
import threading
from typing import Any

_RECORD = os.environ.get("LOG_FOUNDRY_REENTRY_RECORD")


def _write(line: str) -> None:
    if _RECORD is None:
        return
    fd = os.open(_RECORD, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, f"{os.getpid()} {line}\n".encode())
    finally:
        os.close(fd)


def _site() -> str:
    frame = sys._getframe(2)
    while frame is not None and (
        frame.f_code.co_filename == __file__
        or frame.f_code.co_filename.endswith(("threading.py", "queue.py"))
    ):
        frame = frame.f_back  # type: ignore[assignment]
    if frame is None:
        return "<unknown>"
    name = os.path.basename(frame.f_code.co_filename)
    return f"{name}:{frame.f_code.co_name}:{frame.f_lineno}"


class RecordingLock:
    """Stands in for a library ``RLock`` and records each acquire by a thread that owns it.

    Attributes:
      real: The ``RLock`` it forwards to.
    """

    def __init__(self, real: Any) -> None:
        self.real = real

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if self.real._is_owned():
            _write(f"reentry {_site()}")
        return bool(self.real.acquire(blocking, timeout))

    def release(self) -> None:
        self.real.release()

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.real.release()

    def _is_owned(self) -> bool:
        return bool(self.real._is_owned())

    def _release_save(self) -> Any:
        return self.real._release_save()

    def _acquire_restore(self, state: Any) -> None:
        self.real._acquire_restore(state)

    def _at_fork_reinit(self) -> None:
        self.real._at_fork_reinit()


def install() -> None:
    """Replaces the eight oracle locks with recording ones, now and in every worker built later."""
    from log_foundry import _lifecycle, config, decorator, worker

    _write(f"library {os.path.realpath(_lifecycle.__file__)}")
    state = _lifecycle._state
    state._lock = RecordingLock(state._lock)
    state._closes._lock = RecordingLock(state._closes._lock)
    state._deferred_lock = RecordingLock(state._deferred_lock)
    config._config_lock = RecordingLock(config._config_lock)
    decorator._sweep_lock = RecordingLock(decorator._sweep_lock)
    decorator._loss_lock = RecordingLock(decorator._loss_lock)
    real_worker = worker.Worker.__init__
    real_queue = worker._CountingQueue.__init__

    def worker_init(self: Any, *args: Any, **kwargs: Any) -> None:
        real_worker(self, *args, **kwargs)
        self._lock = RecordingLock(self._lock)

    def queue_init(self: Any, *args: Any, **kwargs: Any) -> None:
        real_queue(self, *args, **kwargs)
        self.mutex = RecordingLock(self.mutex)
        self.not_empty = threading.Condition(self.mutex)  # type: ignore[arg-type]
        self.not_full = threading.Condition(self.mutex)  # type: ignore[arg-type]
        self.all_tasks_done = threading.Condition(self.mutex)  # type: ignore[arg-type]

    worker.Worker.__init__ = worker_init  # type: ignore[method-assign]
    worker._CountingQueue.__init__ = queue_init  # type: ignore[method-assign]
