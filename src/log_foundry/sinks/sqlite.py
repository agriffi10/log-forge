"""SQLiteSink — persist events as queryable rows in embedded SQLite (arch §8, SPEC-008)."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import weakref
from typing import Any

from log_foundry import _diag, _fork
from log_foundry.sinks._chunk import valid_identifier
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._driver_error import delivery_error
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks.base import _ABSENT, SinkDeliveryError, SinkLosses, _static_value

__all__ = ["SQLiteSink"]

_COLUMNS = ("log_id", "trace_id", "span_id", "timestamp", "level", "function")

_FORKED = "a forked child acquired no connection"
"""Why a forked child's ``emit`` refuses, the tail ``NATSSink`` already uses for the same case."""


_LIVE: weakref.WeakValueDictionary[int, SQLiteSink] = weakref.WeakValueDictionary()
"""Every live sink, keyed by ``id`` so registering one never hashes it.

A ``WeakSet`` would call the sink's ``__hash__``, and a subclass defining ``__eq__`` has none, so
constructing it would raise. The connection cannot be held here instead, since
``sqlite3.Connection`` takes no weak reference, and a strong one would keep every sink's connection
open in the parent for the life of the process. An entry leaves when its sink is collected, and a
reused ``id`` is safe because the dictionary removes only an entry whose weak reference is dead.
"""


def _resolve_incref() -> Any:
    """Finds ``Py_IncRef`` and ``py_object`` once, in the parent, or ``None`` where it cannot.

    Resolved at import rather than in the child, so the fork handler makes no ``dlsym`` call after
    the fork, and guarded, so a CPython built without ``_ctypes`` or an interpreter with no
    ``pythonapi`` still imports this module and falls back to :data:`_STRANDED`.

    Args:
      None.

    Returns:
      ``(Py_IncRef, py_object)``, or ``None`` when either is unavailable.

    Raises:
      None.
    """
    try:
        import ctypes

        return (ctypes.pythonapi.Py_IncRef, ctypes.py_object)
    except (ImportError, AttributeError):
        return None


_INCREF: Any = _resolve_incref()
"""``Py_IncRef`` and the wrapper it takes, or ``None`` where ``ctypes`` cannot provide them."""

_STRANDED: list[object] = []
"""Where a connection is held when ``Py_IncRef`` is unavailable, the weaker of the two remedies.

It keeps the connection alive while the child runs, but interpreter finalization clears it, so on
macOS a child exiting through ``sys.exit`` can still fault. Only an interpreter without
``ctypes.pythonapi`` uses it.
"""


def _strand(conn: object) -> None:
    """Leaks one reference to a connection another process opened, so this one never frees it.

    **Freeing it is the crash.** On macOS a ``sqlite3.Connection`` the child inherited reaches
    ``sqlite3_close`` when it is finalized, which logs through Apple's ``os_log``, and ``os_log`` is
    not fork-safe: before this, a child of a process that had configured this sink and logged
    died of ``SIGSEGV`` on a plain ``sys.exit``, with no call into the sink at all, in every run
    measured. The native stack puts the free in the cyclic collector inside ``finalize_modules``.
    Neither a module-level list nor a ``__del__`` prevents it, both measured: interpreter
    finalization clears module globals, and the collector runs every finalizer in an unreachable
    set before it looks for resurrection, so the connection's own finalizer closes it whatever a
    sink's ``__del__`` does. A reference leaked through ``Py_IncRef`` is never reclaimed, so the
    connection is never unreachable and never finalized.

    The cost is one connection object and its descriptor, held until the child exits, which is
    what a child that never touched the sink held anyway. A second call leaks a second reference
    to the same object, which costs a count and nothing else. Where ``Py_IncRef`` could not be
    resolved, the connection goes to :data:`_STRANDED` instead.

    Args:
      conn: The inherited connection, or whatever a subclass stored in its place.

    Returns:
      None.

    Raises:
      None.
    """
    if _INCREF is None:
        _STRANDED.append(conn)
        return
    incref, wrap = _INCREF
    incref(wrap(conn))


def _strand_after_fork() -> None:
    """Strands every live sink's connection in a forked child, the moment the child exists.

    Registered with ``_fork``, so it runs in every child, after the repair walk and before the
    child runs any code of its own, for every sink this process holds — a configured one, one
    behind a wrapper, and one only the application holds, which no walk reaches. It has to be this
    early: the crash ``_strand`` describes happens at the child's exit with no call into the sink
    at all, so stranding on the child's first ``emit`` or ``close`` would miss the child that never
    makes either.

    Both attributes are read with ``_static_value``, so no property or ``__getattr__`` of a
    subclass runs in the fork handler (SPEC-079 FR-002), and the pid is compared only once its
    type is exactly ``int``, so a value whose ``__eq__`` is a subclass's own never runs there
    either. A sink missing either attribute, holding a pid of any other type, or whose connection
    was opened in this process, is left alone. A grandchild strands again, which leaks
    one more reference to an object already leaked.

    Args:
      None.

    Returns:
      None.

    Raises:
      None. ``_fork`` absorbs and announces a handler that raises.
    """
    pid = os.getpid()
    for sink in list(_LIVE.values()):
        opened_in = _static_value(sink, "_pid")
        conn = _static_value(sink, "_conn")
        if conn is _ABSENT or type(opened_in) is not int or opened_in == pid:
            continue
        _strand(conn)


_fork.register_child_handler(_strand_after_fork)


class SQLiteSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a SQLite table.

    For local dev, debugging, air-gapped hosts or simple archival, an embedded SQLite file is a
    durable sink you can open later and query with plain SQL. Each event is stored as its full
    JSON — the source of truth — plus a few columns projected out for cheap filtering, which are
    ``NULL`` when absent. Standard library only.

    The driver requirement satisfied (SPEC-028 FR-002): ``check_same_thread=False`` switches off
    ``sqlite3``'s own same-thread guard, and ``with connection`` is a transaction scope on the
    shared connection rather than a per-caller one — two threads inside it at once share a single
    implicit transaction, so one thread's rollback discards rows the other had already inserted.
    A lock restores the one-writer-at-a-time the guard used to enforce. Cross-*process* writers
    to one database file remain out of scope.

    **A forked child refuses every** ``emit`` **and never touches the connection it inherited.** A
    SQLite connection cannot cross a fork: used in the child it segfaulted on macOS on the first
    insert in every child measured. Opening a fresh one in the child is no remedy there, because
    Apple's ``libsqlite3`` logs through ``os_log``, which is not fork-safe once the parent has used
    it, and a fresh connection in the child died too, in anything from none to every child of a run
    depending on timing. So the child owns no connection: ``emit`` refuses with
    ``SinkDeliveryError`` before taking any lock, ``close`` returns having touched nothing, and the
    inherited connection is stranded at the fork rather than ever freed (``_strand_after_fork``).
    This is ``NATSSink``'s answer to the same question (invariant 12). On Linux a child that wants
    a SQLite database of its own builds a sink of its own, with ``configure(sink_factory=...)``; on
    macOS that sink's fresh connection carries the same ``os_log`` hazard, which no sink can remove.

    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` commits its own transaction, so
    nothing is uncommitted once it returns. The commit in ``close`` is belt and braces, not a
    buffer.
    """

    def __init__(
        self,
        database: str,
        *,
        table: str = "log_events",
        connection: sqlite3.Connection | None = None,
        create_table: bool = True,
    ) -> None:
        """Connects to the database and, by default, provisions the schema.

        The connection is opened with ``check_same_thread=False``: the background worker is a
        different thread from the one that ran ``configure()``, so SQLite's same-thread guard
        would reject every insert. It is not the sole writer — the orphan path emits on the
        caller's thread (SPEC-028) — which is why the lock below replaces what the guard gave up.

        Args:
          database: The database file to open, ignored when a connection is injected.
          table: The target table, validated as a plain SQL identifier.
          connection: A connection to borrow, such as an in-memory one for tests. A borrowed
            connection is committed but never closed by this sink, while one the sink opens
            itself is owned and closed on :meth:`close`.
          create_table: Whether the sink owns its schema. Pass ``False`` when the caller
            provisions the table via migrations; the sink then runs no DDL and a missing or
            incompatible table surfaces as a normal ``sqlite3`` error at insert time.

        Returns:
          None.

        Raises:
          ValueError: If the table name is not a plain SQL identifier.
          sqlite3.Error: If the database cannot be opened or the schema cannot be created.
        """
        self._table = valid_identifier(table)
        self._pid = os.getpid()
        self._owns_connection = connection is None
        self._conn = (
            connection
            if connection is not None
            else sqlite3.connect(database, check_same_thread=False)
        )
        _LIVE[id(self)] = self
        self._guard = ClosedGuard(GuardMode.SERIAL)
        self._unencodable = 0
        self._failed = 0
        self._counter_lock = threading.Lock()
        if create_table:
            self._ensure_schema()

    @property
    def _closed(self) -> bool:
        """Whether a close has begun: the 1.x name, read-only, answered by the guard (SPEC-076).

        A 1.x subclass reading ``self._closed`` keeps working. It reads true from the moment a
        close begins, while that close still waits, which is the guard's ``closed``; assigning it
        raises ``AttributeError``, since the guard owns the state.

        Args:
          None.

        Returns:
          The guard's ``closed``.

        Raises:
          None.
        """
        return self._guard.closed

    @property
    def _lock(self) -> threading.Lock:
        """The guard's transport lock under its 1.x name, read-only (SPEC-076 FR-005).

        A 1.x subclass taking ``with self._lock:`` around its own work serialises with the
        guard's sections and its close, as it did with the 1.x lock; assigning it raises
        ``AttributeError``.

        Args:
          None.

        Returns:
          The lock each admitted section and the close hold.

        Raises:
          None.
        """
        return self._guard._lock

    def losses(self) -> SinkLosses | None:
        """Reports unencodable events and driver failures, once it has any (SPEC-069).

        ``dropped`` is the events :meth:`emit` could not encode (FR-001); ``failed`` the events of
        a batch whose insert raised ``sqlite3.Error`` (FR-004), an upper bound. ``None`` until
        something is counted, the rule ``wrapped_losses`` applies. Read under the counter lock,
        never the emit lock, so a poll does not wait behind an insert (SPEC-028 FR-003).

        Args:
          None.

        Returns:
          ``None`` while nothing is counted, else the counters.

        Raises:
          None.
        """
        with self._counter_lock:
            if not (self._unencodable or self._failed):
                return None
            return SinkLosses(dropped=self._unencodable, failed=self._failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Inserts every event in one transaction (FR-003).

        ``with connection`` opens a transaction and commits on success or rolls back on error,
        so the whole batch lands atomically. The lock holds that transaction to one thread
        (SPEC-028 FR-002). Row-building is deliberately *outside* it: those lines touch nothing
        shared, so serializing the ``json.dumps`` of a whole batch would lengthen the critical
        section for no gain.

        Row-building runs through the encode helper (SPEC-069 FR-001), so an event whose row
        cannot be built costs only itself, counted and announced when this call returns, rather
        than the whole batch; a batch of nothing encodable runs no transaction and returns, once
        the closed check has passed (FR-002).

        **A driver error is counted, announced and raised as both types** (SPEC-069 FR-004).
        When the insert raises ``sqlite3.Error`` the batch size is added to ``failed`` at once,
        under the counter lock and never the emit lock, one line names the class and the
        exception's type, and the error is raised as an instance of both ``SinkDeliveryError``
        and the driver's own class (``_driver_error.delivery_error``), so ``except
        sqlite3.Error`` and ``except SinkDeliveryError`` both still catch it. Before, it
        escaped with nothing counted and nothing written. An event this call dropped as
        unencodable is discarded with the raise, like any other per-event drop.

        Args:
          batch: The events to insert.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the insert fails, as an instance of ``SinkDeliveryError`` too, with
            the original as its ``__cause__`` — or the original itself, unchanged, if the
            dual-typed exception cannot be built.
          SinkDeliveryError: In a forked child, before any lock is taken, since a lock a parent
            thread held at the fork may stay held in the child and the connection is not the
            child's to use; nothing is counted, because a refusal is a failure reported to the
            caller, who counts it. Or if the sink is closed — whether the close landed while this
            call was building its rows, or long before it. Either way the batch would otherwise reach
            the driver as "Cannot operate on a closed database", and the library has a word of
            its own for "none of this was delivered" (SPEC-026 FR-001). What the library should
            *signal* about logging after a completed shutdown is a separate question, and
            SPEC-030's; this only keeps driver internals out of the answer.
        """
        if not batch:
            return
        if os.getpid() != self._pid:
            raise SinkDeliveryError(f"SQLiteSink inserted none of {len(batch)} event(s): {_FORKED}")
        placeholders = ", ".join("?" * (len(_COLUMNS) + 1))
        columns = ", ".join((*_COLUMNS, "event"))
        with Deferral(self) as pending:
            encoded = encode_events(batch, self._row, pending, sink=type(self).__name__)
            try:
                with self._guard.enter() as section:
                    if not section:
                        raise SinkDeliveryError(
                            f"SQLiteSink inserted none of {len(batch)} event(s): "
                            "the sink is closed"
                        )
                    if not encoded:
                        return
                    self._insert([row for _, row in encoded], columns, placeholders)
            except sqlite3.Error as err:
                with self._counter_lock:
                    self._failed += len(batch)
                _diag.lost("event", len(batch), f"{type(self).__name__}, {type(err).__name__}")
                built = delivery_error(err)
                if built is None:
                    raise
                raise built from err

    def _row(self, event: dict[str, object]) -> tuple[object, ...]:
        """Builds one row: the projected columns, then the whole event as JSON.

        Args:
          event: The event to convert.

        Returns:
          The statement parameters, in column order.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return (*(event.get(col) for col in _COLUMNS), json.dumps(event))

    def _insert(
        self, rows: list[tuple[object, ...]], columns: str, placeholders: str
    ) -> None:
        """Runs the insert transaction, with the emit lock already held.

        Args:
          rows: The prepared row tuples.
          columns: The comma-separated column list.
          placeholders: The matching parameter placeholders.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the insert fails.
        """
        with self._conn:
            self._conn.executemany(
                f'INSERT INTO "{self._table}" ({columns}) VALUES ({placeholders})', rows
            )

    def close(self) -> None:
        """Commits pending work and closes only a connection the sink owns (FR-003).

        Idempotent, and takes the emit lock so it never closes the connection out from under an
        in-flight transaction (SPEC-028 FR-002).
        The lock is the guard's (SPEC-076 FR-002): once a close begins every later ``emit`` is
        refused at once, the one ``emit`` inside, if any, is waited for, and the release runs
        once; a second close waits for the first's release, then returns.

        In a forked child it returns at once, before the guard's lock, having committed, closed
        and marked nothing: the connection is the parent's, and the library does not close a
        sink a child inherited anyway (invariant 12), so this is the call a caller makes directly.

        Args:
          None.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the commit or close fails.
        """
        if os.getpid() != self._pid:
            return
        self._guard.close(self._release)

    def _release(self) -> None:
        """Commits pending work, then closes the connection if the sink owns it.

        :meth:`close` hands this to the guard, which runs it once, with the lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the commit or close fails.
        """
        self._conn.commit()
        if self._owns_connection:
            self._conn.close()

    def _ensure_schema(self) -> None:
        """Idempotently creates the target table.

        Args:
          None.

        Returns:
          None.

        Raises:
          sqlite3.Error: If the DDL fails.
        """
        columns = ", ".join(f"{col} TEXT" for col in _COLUMNS)
        with self._conn:
            self._conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{self._table}" '
                f"(id INTEGER PRIMARY KEY AUTOINCREMENT, {columns}, event TEXT NOT NULL)"
            )
