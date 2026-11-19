"""SPEC-097 FR-003: a ``flush()`` made on the drain thread returns at once, ``"re-entrant"``.

A ``flush()`` asks the drain thread to answer a marker, so one made on the drain thread itself,
from a sink's ``emit`` on the worker path, waited on itself: its whole timeout, or for ever with
``timeout=None`` (the pinned ``drain-flush`` cells). It now returns falsy with the reason
``"re-entrant"`` before it sweeps a span, queues a marker or opens its ledger window. The scenario
runs in a forked child, so a regression that waits for ever fails one test rather than hanging the
suite. Serves invariants 2 and 3.
"""

from __future__ import annotations

import gc
import json
import os
import select
import signal
import threading
import time

import log_foundry
from log_foundry import _lifecycle
from test_fork_lifecycle import run_in_child


def _reading(worker: object) -> dict[str, object]:
    """What a refused call must leave as it was: the queue's counts and every ledger term."""
    queue = worker._queue  # type: ignore[attr-defined]
    ledger = _lifecycle._ledger_snapshot()
    return {
        "entered": queue.entered,
        "queued": len(queue.queue),
        "markers": len(worker._taken_markers),  # type: ignore[attr-defined]
        "ledger": repr(ledger),
    }


def test_a_flush_from_a_sinks_emit_on_the_drain_thread_is_refused_at_once() -> None:
    """FR-003: ``flush(timeout=None)`` from ``emit`` on the worker path returns at once.

    It returns falsy with ``reason == "re-entrant"``, queues no submission and no marker, and leaves
    every ledger term as it read just before the call. The comparison is by the drain thread's
    identity: the call is made on ``worker._thread``, which the child asserts. The spec's
    criterion reads under 0.1 s; the suite's ceiling is the 0.5 s stall allowance
    ``tests/test_timing_floors.py`` holds every ceiling to, which still tells a refusal from a call
    that waits for ever.
    """

    def scenario() -> str:
        class FlushFromEmit:
            def __init__(self) -> None:
                self.armed = False
                self.report: dict[str, object] = {}

            def emit(self, batch: list[dict[str, object]]) -> None:
                if not self.armed or self.report:
                    return
                worker = _lifecycle._state.live_worker()
                assert worker is not None
                before = _reading(worker)
                started = time.monotonic()
                result = log_foundry.flush(timeout=None)
                elapsed = time.monotonic() - started
                after = _reading(worker)
                self.report = {
                    "on_drain": threading.current_thread() is worker._thread,
                    "elapsed_ok": elapsed < 0.5,
                    "ok": bool(result),
                    "reason": result.reason,
                    "lost": result.lost,
                    "unchanged": before == after,
                    "diff": [before, after] if before != after else None,
                }

            def close(self) -> None:
                return None

        sink = FlushFromEmit()
        log_foundry.configure(service="reentrant-flush", sink=sink)  # type: ignore[arg-type]

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("x")

        work()
        log_foundry.flush(timeout=5.0)
        sink.armed = True
        work()
        log_foundry.flush(timeout=5.0)
        return json.dumps(sink.report, sort_keys=True)

    child = run_in_child(scenario, timeout=30)
    report = json.loads(child.output or "{}")
    assert report == {
        "diff": None,
        "elapsed_ok": True,
        "lost": 0,
        "ok": False,
        "on_drain": True,
        "reason": "re-entrant",
        "unchanged": True,
    }, report


def test_a_child_forked_on_the_drain_thread_is_not_refused_for_the_threads_name() -> None:
    """FR-003: the refusal compares the drain thread by identity, never by its name.

    A sink's ``emit`` forks on the drain thread, so the child's only thread is the forking one
    and keeps the name ``log-foundry-worker``, while the child's worker gets a drain thread of its
    own. A ``flush()`` there is not on any drain thread and must run: a refusal by name returned
    ``"re-entrant"`` and left two forker cells ``ChildBroken:state`` on the prototype.
    """

    def scenario() -> str:
        read_fd, write_fd = os.pipe()
        parent = os.getpid()
        forked: list[int] = []

        class ForkOnEmit:
            armed = False

            def emit(self, batch: list[dict[str, object]]) -> None:
                if not self.armed or forked:
                    return
                gc.collect()
                pid = os.fork()
                forked.append(pid)
                if pid != 0:
                    return
                signal.alarm(15)
                try:
                    result = log_foundry.flush(timeout=5.0)
                    line = (
                        f"name={threading.current_thread().name} ok={bool(result)} "
                        f"reason={result.reason}"
                    )
                except BaseException as exc:
                    line = f"!{type(exc).__name__}: {exc}"
                os.write(write_fd, line.encode())
                os._exit(0)

            def close(self) -> None:
                return None

        sink = ForkOnEmit()
        log_foundry.configure(service="fork-on-drain", sink=sink)  # type: ignore[arg-type]

        @log_foundry.trace
        def work() -> None:
            log_foundry.info("x")

        work()
        log_foundry.flush(timeout=5.0)
        sink.armed = True
        work()
        log_foundry.flush(timeout=5.0)
        if os.getpid() != parent:
            os._exit(1)
        os.close(write_fd)
        if not forked:
            return "never forked"
        ready = select.select([read_fd], [], [], 15)[0]
        line = os.read(read_fd, 256).decode() if ready else "silent"
        if not ready:
            os.kill(forked[0], signal.SIGKILL)
        os.waitpid(forked[0], 0)
        return line

    child = run_in_child(scenario, timeout=30)
    assert child.output == "name=log-foundry-worker ok=True reason=None", child.output
