"""A forked child and the ``SQLiteSink`` it inherited, under a real ``os.fork``.

A SQLite connection cannot cross a fork. Used in the child it segfaulted on macOS on the first
insert, and freed in the child it segfaulted at the child's exit, because Apple's ``libsqlite3``
logs through ``os_log``, which is not fork-safe. The child therefore refuses every ``emit``,
``close`` touches nothing, and the inherited connection is stranded at the fork rather than freed
(``sqlite._strand_after_fork``).

On Linux neither crash happens, so the tests that would only see a signal also assert what the
child did instead: that it refused, wrote nothing into the parent's database, and holds the
inherited connection by a reference nothing releases.
"""

from __future__ import annotations

import gc
import os
import sqlite3
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import pytest

import log_foundry
from log_foundry.sinks import sqlite as sqlite_sink
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.sqlite import SQLiteSink
from test_fork_lifecycle import run_in_child

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable, Iterator

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")


def _messages(conn: sqlite3.Connection) -> list[str]:
    """The ``message`` of every row the sink inserted, in insertion order."""
    return [
        row[0]
        for row in conn.execute(
            "SELECT json_extract(event, '$.message') FROM log_events ORDER BY id"
        )
        if row[0] is not None
    ]


@pytest.fixture(params=["file", "memory", "borrowed"])
def inherited(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path
) -> Iterator[tuple[SQLiteSink, Callable[[], list[str]]]]:
    """A configured sink of one database kind, and how the parent reads what reached it.

    ``file`` is read through a connection of the parent's own, so it sees only committed rows;
    ``memory`` and ``borrowed`` are read through the sink's own connection, the only one that can
    see an in-memory database. Every connection is closed in the parent at teardown, which is
    also what keeps this module from leaking one into a later fork.
    """
    borrowed = None
    if request.param == "file":
        path = tmp_path / "events.db"
        sink = SQLiteSink(str(path))
        reader = sqlite3.connect(path)
        read = lambda: _messages(reader)  # noqa: E731
    elif request.param == "memory":
        sink = SQLiteSink(":memory:")
        read = lambda: _messages(sink._conn)  # noqa: E731
    else:
        borrowed = sqlite3.connect(":memory:", check_same_thread=False)
        sink = SQLiteSink("ignored", connection=borrowed)
        read = lambda: _messages(borrowed)  # noqa: E731
    log_foundry.configure(sink=sink)
    yield sink, read
    log_foundry.shutdown()
    if request.param == "file":
        reader.close()
    if borrowed is not None:
        borrowed.close()


def test_a_forked_child_refuses_its_inherited_sqlite_sink_and_survives(
    inherited: tuple[SQLiteSink, Callable[[], list[str]]],
) -> None:
    """The child's ``info`` and ``flush`` lose the event, counted and named, and touch nothing.

    Before the fix the child's first insert went through the inherited connection: on macOS it
    died of ``SIGSEGV`` there, and on Linux its row landed in the parent's database through a
    connection SQLite says must not cross a fork. Now the orphan path's loss counter moves, a
    direct ``emit`` raises the forked-child refusal, a direct ``close`` returns, and the parent's
    database holds only the parent's rows — and the parent keeps writing to it afterwards, which
    is what shows the child's ``close`` released nothing the parent still uses.
    """
    sink, read = inherited
    log_foundry.info("parent")
    assert log_foundry.flush().ok

    def work() -> str:
        log_foundry.info("child")
        log_foundry.flush()
        notes = [f"orphan_lost:{log_foundry.health().orphan_lost}"]
        try:
            sink.emit([{"message": "direct"}])
        except SinkDeliveryError as exc:
            notes.append(f"refused:{'forked child acquired no connection' in str(exc)}")
        else:
            notes.append("refused:no")
        sink.close()
        notes.append(f"closed:{sink._closed}")
        return ";".join(notes)

    child = run_in_child(work, timeout=10)

    assert child.status == 0, child.output
    assert child.output == "orphan_lost:1;refused:True;closed:False", child.output
    assert read() == ["parent"]
    log_foundry.info("after")
    assert log_foundry.flush().ok
    assert read() == ["parent", "after"]


def test_a_forked_childs_traced_batch_costs_one_emit_call_not_a_resend_each(
    inherited: tuple[SQLiteSink, Callable[[], list[str]]],
) -> None:
    """The measured case: a child's ``@trace`` calls reach the worker, which resends nothing.

    The refusal is final (``sinks/_refusal.py``), so each batch the child's worker abandons cost
    one ``emit``. Before, every batch cost ``max_retries + 1`` calls and their backoff waits: 2000
    ``@trace`` calls took 17.3 s to ``flush()``, about 85 ms a batch, at ``2ff9de9e`` (#342).
    The calls are counted through ``sys.monitoring``, since a wrapper around ``emit`` would put a
    frame of this module in the refusal's traceback and so make it a resent one.
    """
    _, read = inherited

    def work() -> str:
        calls = [0]
        monitoring = sys.monitoring
        tool = next(t for t in range(6) if monitoring.get_tool(t) is None)
        monitoring.use_tool_id(tool, "sqlite-fork-emits")
        code = SQLiteSink.emit.__code__

        def started(started_code: object, offset: int) -> None:
            if started_code is code:
                calls[0] += 1

        monitoring.register_callback(tool, monitoring.events.PY_START, started)
        monitoring.set_local_events(tool, code, monitoring.events.PY_START)

        @log_foundry.trace
        def step() -> None:
            pass

        for _ in range(20):
            step()
        log_foundry.flush(timeout=5)
        failed = log_foundry.health().failed_batches
        return f"emits:{calls[0]};batches:{failed};one-each:{calls[0] == failed > 0}"

    child = run_in_child(work, timeout=10)

    assert child.status == 0, child.output
    assert child.output.endswith("one-each:True"), child.output
    assert read() == []


def test_a_forked_child_holds_the_inherited_connection_by_a_reference_nothing_releases() -> None:
    """The fork handler leaks exactly one reference to the inherited connection, held by nothing.

    That reference is the fix for the crash at the child's exit: a connection the child can never
    drop is never finalized there, so ``sqlite3_close`` never reaches ``os_log``. It is asserted
    as a count because that is the one observable of it on a platform where the crash does not
    happen — and as the count of references **no object holds**, the reference count less the
    referrers the collector can see, because a container holding it would raise the plain count
    by one too, and a container is cleared at finalization, which is the remedy measured not to
    work. A sink the application holds and the library was never handed is stranded too, since
    the handler reads its registry of live sinks rather than walking the runtime's roots.
    """
    held = SQLiteSink(":memory:")

    def unheld() -> int:
        conn = held._conn
        return sys.getrefcount(conn) - len(gc.get_referrers(conn))

    try:
        before = unheld()

        def work() -> str:
            return str(unheld() - before)

        child = run_in_child(work, timeout=10)
    finally:
        held.close()

    assert child.status == 0, child.output
    assert child.output == "1"


_EXIT_SCRIPT = textwrap.dedent(
    """
    import gc, os, sys
    import log_foundry
    from log_foundry.sinks.sqlite import SQLiteSink

    database, how = sys.argv[1], sys.argv[2]
    log_foundry.configure(sink=SQLiteSink(database))
    log_foundry.info("parent")
    log_foundry.flush()

    def child() -> None:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, 2)
        if how in ("log", "grandchild"):
            log_foundry.info("child")
            log_foundry.flush()
        if how == "grandchild":
            gc.collect()
            pid = os.fork()
            if pid == 0:
                log_foundry.info("grandchild")
                log_foundry.flush()
                log_foundry.shutdown()
                sys.exit(0)
            _, status = os.waitpid(pid, 0)
            if status:
                sys.exit(3)
        log_foundry.shutdown()
        sys.exit(0)

    statuses = []
    for _ in range(3):
        gc.collect()
        pid = os.fork()
        if pid == 0:
            child()
        statuses.append(os.waitpid(pid, 0)[1])
    log_foundry.shutdown()
    print(statuses)
    if database != ":memory:":
        import sqlite3
        reader = sqlite3.connect(database)
        rows = reader.execute("SELECT json_extract(event, '$.message') FROM log_events").fetchall()
        reader.close()
        print(sorted({row[0] for row in rows if row[0] is not None}))
    """
)


@pytest.mark.parametrize("how", ["exit", "log", "grandchild"])
@pytest.mark.parametrize("database", ["file", ":memory:"])
def test_a_forked_child_exits_normally_without_freeing_the_inherited_connection(
    tmp_path: pathlib.Path, database: str, how: str
) -> None:
    """A child that exits through interpreter finalization does not die there.

    The crash this guards is at the child's **exit**, with or without any call into the sink:
    finalization runs the cyclic collector, which finalized the inherited connection, and on
    macOS that close faulted inside ``os_log`` — before the fix every child of this script died
    of ``SIGSEGV``. ``run_in_child`` cannot see it, since its children leave through
    ``os._exit``, so this runs a fresh interpreter whose children exit through ``sys.exit``, three
    per run, including a child that forks again. The exit itself discriminates on macOS only;
    on Linux the file rows of the logging children do, since there the child's row used to land in
    the parent's database, and the unheld reference count above is the exit fix's guard.
    """
    target = str(tmp_path / "events.db") if database == "file" else database
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _EXIT_SCRIPT, target, how],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == "[0, 0, 0]", result.stdout + result.stderr
    if database == "file":
        assert lines[1:] == ["['parent']"], result.stdout


def test_a_connection_is_held_in_a_list_where_py_incref_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fallback for an interpreter without ``ctypes.pythonapi`` still holds the connection.

    The weaker remedy, kept rather than doing nothing: it stops the collector freeing the
    connection while the child runs, which is the crash a child that keeps running would hit.
    """
    conn = sqlite3.connect(":memory:")
    monkeypatch.setattr(sqlite_sink, "_INCREF", None)
    monkeypatch.setattr(sqlite_sink, "_STRANDED", [])
    before = sys.getrefcount(conn)
    try:
        sqlite_sink._strand(conn)
        assert len(sqlite_sink._STRANDED) == 1
        assert sqlite_sink._STRANDED[0] is conn
        assert sys.getrefcount(conn) == before + 1
    finally:
        sqlite_sink._STRANDED.clear()
        conn.close()
