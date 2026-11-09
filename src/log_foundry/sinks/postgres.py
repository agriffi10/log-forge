"""PostgresSink — insert events into a Postgres table with a JSONB column (arch §8, SPEC-011)."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, Any

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._chunk import chunk_list, valid_identifier
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._encode import NUL_ESCAPE, encode_round, repair_nul
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import floor_count, require_positive
from log_foundry.sinks._rounds import Called, Lost, mark_sent, raised_line
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    from log_foundry.sinks.base import SinkLosses

__all__ = ["PostgresSink"]

DEFAULT_CONNECT_TIMEOUT = 5
"""Seconds libpq may spend opening one connection (SPEC-041 FR-002).

``psycopg.connect()`` takes no timeout unless one is given, and libpq's default is to wait
indefinitely. That call runs on the worker's single drain thread, holding this sink's emit lock,
and it consults no stop signal -- so against a host that blackholes packets rather than refusing
them it is exactly the unbounded, uninterruptible wait SPEC-027 exists to remove. Measured
against an unroutable address: **75.01 s** with no timeout, **2.03 s** with ``connect_timeout=2``.

Floored at libpq's own minimum of 2 rather than accepted verbatim, because ``0`` means "wait
forever" -- reinstating the defect -- the same reason ``KafkaSink._usable_timeout`` refuses its
own degenerate values (SPEC-038 FR-006).
"""

_MIN_CONNECT_TIMEOUT = 2

_COLUMNS = ("timestamp", "level", "trace_id", "span_id", "function", "service")

_RETRYABLE_CLASSES = frozenset({"08", "40", "53", "57"})
"""SQLSTATE classes retried: connection exception, transaction rollback, insufficient resources
and operator intervention (PostgreSQL's *Appendix A*; SPEC-070 FR-002)."""

_RETRYABLE_STATES = frozenset({"55P03", "55006"})
"""The two class-``55`` states retried: ``LockNotAvailable`` and ``ObjectInUse``."""

_REFUSALS: tuple[tuple[str, str], ...] = (
    ("FATAL:  password authentication failed", ""),
    ('FATAL:  role "', '" does not exist'),
    ('FATAL:  database "', '" does not exist'),
)
"""libpq's connect-time refusals, as a prefix and a suffix found after it (SPEC-070 FR-002).

A wrong password, an unknown user and an unknown database each reach the sink as a plain
``OperationalError`` with no SQLSTATE, and only this text tells them from a refused port: the
server answered and refused, and a resend asks the same question of the same configuration.
"""


def _refused(message: str) -> bool:
    """Reports whether a connect failure's message is one of libpq's refusals.

    Args:
      message: The exception's text, read inside the predicate and never printed.

    Returns:
      True when the message carries a :data:`_REFUSALS` prefix and, where the entry names one,
      its suffix somewhere after it.

    Raises:
      None.
    """
    for prefix, suffix in _REFUSALS:
        start = message.find(prefix)
        if start >= 0 and message.find(suffix, start + len(prefix)) >= 0:
            return True
    return False


class PostgresSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into a Postgres table.

    Each event is stored as a ``JSONB`` ``event`` column plus a few extracted columns for
    indexing. ``psycopg`` v3 is the optional ``postgres`` extra, imported lazily. The sink is
    write-only, and delivers through SPEC-072's engine (SPEC-074 FR-003): each round runs every
    chunk in one transaction, and the engine resends a rolled-back round in the next. The
    worst-case delay (SPEC-027 FR-005) has two halves, and only one of them is interruptible. The
    backoffs are ``max_retries`` waits per run — 0.7 s at the defaults — taken by the engine
    through ``_retry.wait`` on the stop event, so a shutdown cuts them short, and never under the
    transport lock, so a ``close()`` arriving during one takes the lock between rounds. The
    reconnects are up to ``max_retries + 1`` connects of ``connect_timeout`` each, 20 s more at
    the defaults, and they are **bounded but not interruptible**: the wait is inside libpq, which
    consults nothing. That is the settled line rather than a gap — a shutdown shortens a *wait*
    and never skips *work*, and a reconnect is the work an in-flight batch needs (SPEC-038
    FR-001 AC-4a). Both halves sit inside the existing retry budget rather than a loop of their
    own, which is what bounds them (FR-002 AC-3).

    The driver requirement satisfied (SPEC-028 FR-002): a ``psycopg`` connection carries one
    transaction, and this sink's unit of work is a ``cursor`` / ``commit`` / ``rollback`` sequence
    that assumes it owns it. Two unserialized emits share that transaction — one thread's
    ``commit`` publishes the other's half-written batch, and its ``rollback`` on a failure
    discards rows the other had already inserted and is about to report as delivered. A lock
    gives the sequence the exclusivity it was written for, for one round at a time.

    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` commits its own transaction, so
    nothing is uncommitted once it returns. The commit in ``close`` is belt and braces, not a
    buffer.

    Attributes:
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
    """

    def __init__(
        self,
        table: str,
        *,
        connection: Any = None,
        dsn: str | None = None,
        create_table: bool = False,
        chunk_size: int = 1000,
        max_retries: int = 3,
        connect_timeout: int | None = None,
    ) -> None:
        """Connects to the database and prepares the insert statement.

        Args:
          table: The target table, validated as a plain SQL identifier.
          connection: A ``psycopg``-shaped connection to borrow, or ``None`` to open one.
          dsn: The connection string used when opening a connection.
          create_table: An idempotent convenience, off by default because the user owns their
            schema and indexes.
          chunk_size: How many rows go in one driver statement. Refused when non-positive
            (SPEC-049 FR-002): it spent the full retry budget and its backoffs on every
            batch, failing the same way each time.
          max_retries: Retries per batch, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value returned having attempted no insert at all, and
            reported success. Anything but an ``int`` is refused with ``TypeError`` (SPEC-066
            FR-001); a ``bool`` or ``int`` subclass is read as the ``int`` it holds.
          connect_timeout: Seconds libpq may spend opening a connection. ``None``, the default,
            means :data:`DEFAULT_CONNECT_TIMEOUT` (5): the ``None`` lets the sink tell a value the
            caller gave from one left out (SPEC-071 FR-003), and the parameter is rebound to the
            default before the floor so the floor still receives it positionally. It is floored
            at libpq's own minimum of 2, since
            ``0`` means "wait forever" and would reinstate the unbounded connect this argument
            exists to remove. It is passed explicitly and therefore **overrides any
            ``connect_timeout`` in the DSN** — a DSN asking for 30 gets this value instead, so
            set it here rather than there. Applies to the connection opened at construction and
            to every reconnect. Anything but an ``int`` is refused with ``TypeError`` (SPEC-066
            FR-002) — libpq wants a decimal integer — and that holds with an injected
            ``connection=`` too, judged first: the declared type is already violated, and the
            rule against validating an unread value is about a value, not a type. A well-typed
            value given beside ``connection=`` is then refused with ``ValueError``, since an
            open connection never reads it (SPEC-071 FR-003).

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` or ``connect_timeout`` is not an ``int`` (SPEC-066),
            judged before any connection is opened.
          ValueError: If the table name is not a plain SQL identifier, the chunk size is not
            positive, or ``dsn`` or ``connect_timeout`` is passed alongside ``connection=``, which
            is already open and would never read either (SPEC-071 FR-003).
          ImportError: If the ``postgres`` extra is not installed.
        """
        self._table = valid_identifier(table)
        self._chunk_size = require_positive(chunk_size, "chunk_size", "PostgresSink")
        self.max_retries = floor_count(max_retries, 0, "max_retries", "PostgresSink")
        self.log_foundry_stop_signal: threading.Event | None = None
        self.log_foundry_counters = SinkCounters()
        self._closed = False
        self._lock = threading.Lock()
        self._owns_connection = connection is None
        self._dsn = dsn
        given_timeout = connect_timeout
        connect_timeout = DEFAULT_CONNECT_TIMEOUT if connect_timeout is None else connect_timeout
        self.connect_timeout = floor_count(
            connect_timeout, _MIN_CONNECT_TIMEOUT, "connect_timeout", "PostgresSink"
        )
        refuse_with_injected(
            "PostgresSink",
            "connection",
            connection,
            {"dsn": dsn, "connect_timeout": given_timeout},
        )
        if connection is None:
            connection = self._connect()
        self._conn = connection
        self._reconnect_announced = False
        columns = ", ".join((*_COLUMNS, "event"))
        placeholders = ", ".join(["%s"] * len(_COLUMNS) + ["%s::jsonb"])
        self._insert_sql = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"
        if create_table:
            self._ensure_schema()

    @property
    def _counter_lock(self) -> threading.Lock:
        """The counters' lock, which every commit and ``losses()`` read takes.

        Assigning it replaces the counters' lock, so a test installing a counting lock here
        counts every acquisition the engine makes (SPEC-074 FR-001).

        Args:
          None.

        Returns:
          The lock.

        Raises:
          None.
        """
        return self.log_foundry_counters._counter_lock

    @_counter_lock.setter
    def _counter_lock(self, lock: threading.Lock) -> None:
        """Replaces the counters' lock.

        Args:
          lock: The new lock.

        Returns:
          None.

        Raises:
          None.
        """
        self.log_foundry_counters._counter_lock = lock

    @property
    def failed(self) -> int:
        """Events not confirmed, the engine's ``failed_events``; assignable, as 1.x's attribute was.

        Each lost event once per run (SPEC-074 FR-001), not once per worker attempt.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return self.log_foundry_counters.failed_events

    @failed.setter
    def failed(self, value: int) -> None:
        """Sets the count, under the counter lock, as assigning the 1.x attribute did.

        Args:
          value: The new count.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.log_foundry_counters.failed_events = value

    def losses(self) -> SinkLosses:
        """Reports the engine's totals: unencodable events, and events never confirmed.

        ``dropped`` is the events the encode step could not encode (SPEC-069 FR-001), counted only
        by a run that settled; ``failed`` the encodable events of a run whose last transaction
        rolled back or, for a failure not worth retrying, of its first (SPEC-026 FR-002, SPEC-070),
        each once per run (SPEC-074). An unencodable event of a failed run is its catcher's, so
        ``failed`` no longer counts it as 1.x's whole-batch count did. Read under the counters'
        lock, never the transport lock (SPEC-028 FR-003).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        The diagnostic names the exception's type and never its repr: ``_row`` binds the whole
        serialized event as a statement parameter, and a psycopg error repr routinely reprints
        the failing statement and its parameters (SPEC-029 FR-002, arch §6).

        Args:
          batch: The events to insert. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed — every round's transaction rolled back, or one
            failed for a reason :meth:`_retryable_error` does not retry (SPEC-070) — with the
            engine's message; the rollback is what makes a resend safe here (SPEC-026 FR-001).
            When the sink is closed, or a non-empty batch produced no chunk (SPEC-049 FR-002),
            the round is a refusal and this raises the 1.x ``SinkDeliveryError`` by identity,
            with nothing counted.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-003).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the driver propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs every chunk of the round in one transaction, under the lock for this round only.

        A closed sink's round is a refusal carrying 1.x's closed error. Every event's row is built
        once per run (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one
        ``DROPPED`` with SPEC-069's line. The round's events are then all ``ACCEPTED`` on commit,
        or all ``RETRYABLE`` or ``REJECTED`` after :meth:`_rollback`, as ``_retryable_error``
        answers, the line naming the borrowed-broken connection exactly as 1.x wrote it. A
        non-empty round that produced no chunk is a refusal carrying 1.x's error: a chunker that
        yields nothing would yield nothing again.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the driver propagates.
        """
        context = run_context()
        with self._lock:
            if self._closed:
                context.refuse(
                    SinkDeliveryError(
                        f"PostgresSink inserted none of {len(batch)} event(s): the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            fates = [Outcome.DROPPED] * len(batch)
            pairs = encode_round(self, batch, self._row, sink=type(self).__name__)
            if not pairs:
                return Verdict(outcomes=tuple(fates))
            positions = [position for position, _ in pairs]
            carried = [batch[position] for position in positions]
            mark_sent(self, carried)
            called = self._insert_round([row for _, row in pairs])
            if called.error is None and not called.result:
                context.refuse(
                    SinkDeliveryError(
                        f"PostgresSink produced no chunk for {len(batch)} event(s); nothing was sent"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            if called.error is None:
                outcome = Outcome.ACCEPTED
            else:
                outcome = Outcome.RETRYABLE if called.retryable else Outcome.REJECTED
                suffix = (
                    ", borrowed connection is broken and this sink may not reopen it"
                    if not self._owns_connection and self._is_broken()
                    else ""
                )
                kind = type(called.error).__name__
                lost = Lost(self)
                lost.add(
                    carried,
                    when=When.LAST if called.retryable else When.ALWAYS,
                    key=kind + suffix,
                    line=raised_line("event", "PostgresSink", kind, suffix),
                )
                lost.register()
            for position in positions:
                fates[position] = outcome
        return Verdict(outcomes=tuple(fates))

    def _insert_round(self, rows: list[tuple[object, ...]]) -> Called[int]:
        """Runs the round's transaction once, with the transport lock already held.

        Args:
          rows: The encoded rows to insert, known non-empty.

        Returns:
          The call: how many chunks it committed, or its exception after :meth:`_rollback`.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker.
        """
        try:
            self._reconnect_if_broken()
            chunks = 0
            with self._conn.cursor() as cur:
                for chunk in chunk_list(rows, self._chunk_size):
                    chunks += 1
                    cur.executemany(self._insert_sql, chunk)
            if chunks:
                self._conn.commit()
        except Exception as err:
            self._rollback()
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(chunks)

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed attempt is worth another (SPEC-070 FR-002).

        psycopg files every server condition this could meet under ``OperationalError``, so the
        class alone does not separate a transient failure from a permanent one; the SQLSTATE
        does. Retried: an ``OperationalError`` whose SQLSTATE class is ``08`` (connection
        exception), ``40`` (transaction rollback), ``53`` (insufficient resources) or ``57``
        (operator intervention), ``55P03`` ``LockNotAvailable`` and ``55006`` ``ObjectInUse`` —
        the classes PostgreSQL's *Appendix A* defines as connection, rollback, resource and
        operator conditions — and one with no SQLSTATE at all, psycopg's own client-side failures
        (a failed connect, a timeout, "the connection is closed").

        **A connect-time refusal is the exception, recognised by its text** because nothing
        structured carries it: psycopg raises every failed connect as a plain ``OperationalError``
        with no SQLSTATE, and only libpq's message says the server answered and refused — see
        :data:`_REFUSALS`. The text follows the server's ``lc_messages``, so a server reporting in
        another language is not matched and keeps the retry; a transient ``FATAL`` ("the database
        system is starting up", "sorry, too many clients already") is deliberately not matched.
        The message is read here only; no line prints it.

        Every other ``OperationalError`` — class ``28``, ``54``, the rest of ``55`` and the others
        psycopg puts there — and ``DataError`` (``StringDataRightTruncation`` among them),
        ``IntegrityError``, ``ProgrammingError``, ``InternalError``, ``InterfaceError`` and
        ``NotSupportedError`` fail at once: the same rows meet the same refusal.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        if isinstance(err, TRANSIENT_BUILTINS):
            return True
        if not isinstance(err, driver_classes("psycopg.errors", "OperationalError")):
            return False
        state = getattr(err, "sqlstate", None)
        if state is None:
            return not _refused(str(err))
        if not isinstance(state, str):
            return False
        return state[:2] in _RETRYABLE_CLASSES or state in _RETRYABLE_STATES

    def _connect(self) -> Any:
        """Opens a connection to the configured DSN, bounded by :attr:`connect_timeout`.

        The bound is passed as a keyword rather than left to the DSN, so it holds **regardless of**
        what the caller's connection string says — which is the same fact ``__init__`` states from
        the caller's side, that this argument overrides a ``connect_timeout`` in the DSN. See
        :data:`DEFAULT_CONNECT_TIMEOUT` for why an unbounded connect here is the defect rather
        than the default.

        Args:
          None.

        Returns:
          The new connection.

        Raises:
          ImportError: If the ``postgres`` extra is not installed.
          Exception: Whatever the driver raises when connecting.
        """
        return import_extra("psycopg", "postgres").connect(
            self._dsn, connect_timeout=self.connect_timeout
        )

    def _reconnect_if_broken(self) -> None:
        """Replaces an unusable **owned** connection before an insert attempt (FR-002).

        A ``psycopg`` connection is permanently unusable once the server closes it, and this sink
        opened one in ``__init__`` and never reopened it — so a single restart, failover or idle
        timeout ended log delivery for the life of the process, with every in-batch retry running
        against the same dead handle. Measured against a real Postgres: one
        ``pg_terminate_backend`` and three subsequent batches were lost, one row delivered.
        Every sibling already recovers (``SocketTransport._reset``, boto3, clickhouse-connect's
        pool, pymongo's pool).

        **It runs at the top of each attempt, not in the retry branch.** ``max_retries`` is a
        public argument floored at zero, so a reconnect placed where a retry remains never runs
        at all at ``max_retries=0`` and the defect would survive at that setting forever. At the
        top of the attempt, the first emit after a failure reopens — which is FR-002 AC-1
        literally — at every value. It also avoids spending a connect immediately before the
        raise, where it can only cost time.

        **A borrowed connection is never reopened**, per ``architecture.md`` §13's borrowed-client
        constraint: the caller owns that object and its lifetime, and replacing it here would
        leak theirs and reconnect a session they may be sharing. The exhausted batch's existing
        ``_diag.lost`` line gains a clause rather than a new stderr site, and it is conditioned on
        the connection actually being broken — appending it to every borrowed-connection failure
        would put "not reopened" on a constraint violation, where reopening was never the
        question.

        The state is read from the connection rather than inferred: ``closed`` and ``broken`` are
        both ``False`` before the first failure and both ``True`` after it, so an ordinary SQL
        error — a constraint violation, a full disk — does not churn the connection. Both are
        probed by name because this sink accepts any ``psycopg``-shaped object it does not own.

        Both diagnostics here are **announced once per outage, not once per attempt**. Unthrottled
        they fire on every attempt of every batch, so a down server turned one stderr line into
        five per batch, indefinitely — a diagnostic that floods is one an operator stops reading,
        and the batch's own ``_diag.lost`` line already records the loss. The flag covers the
        failed ``close()`` as well as the failed connect, because a failed reconnect leaves the
        old object in place and the next attempt closes it again. It clears on a successful
        reconnect, so a later outage is announced again.

        **A failed connect is announced as pending, never as absorbed** (SPEC-070 FR-002): it is
        re-raised into the attempt, so "absorbed" would be false (invariant 11), and it is not a
        loss yet either — the batch's own handler retries it or counts it, and that line carries
        the count. :func:`~log_foundry._diag.pending` is the writer for exactly that: no ``lost``
        prefix, the exception's type and never its message. The line promises nothing it cannot
        keep: a retry that succeeds writes no further line, so it says only that a ``lost`` line
        follows if the batch is given up. The type is read as ``_diag.absorbed`` reads it, through
        ``type``'s stored name (SPEC-063 FR-004), so a metaclass ``__name__`` cannot rename or
        silence it. The failed ``close()`` of the old handle is still absorbed, and its line
        still says so.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the connect raised, after the once-per-outage line. It ends this
            attempt inside :meth:`_insert_batch`'s ``try``, so the batch's own handler sees the
            connect's failure rather than whatever the dead handle raises next (SPEC-070
            FR-002): :meth:`_retryable_error` then reads a refused password, role or database as
            final, and anything else as worth the next attempt. That keeps this inside the
            existing retry budget rather than adding a loop of its own (AC-3): the batch's bound
            is unchanged, and a connect that cannot succeed spends an attempt visibly, through
            the counters. Before SPEC-070 the failure was absorbed here and the attempt went on
            to fail on the dead handle, whose "the connection is closed" hid a refusal from the
            predicate. A failed ``close()`` of the old handle is still absorbed.
        """
        if not self._owns_connection or not self._is_broken():
            return
        announced, self._reconnect_announced = self._reconnect_announced, True
        try:
            self._conn.close()
        except Exception as err:
            if not announced:
                _diag.absorbed("PostgresSink.close of a broken connection", err)
        try:
            self._conn = self._connect()
            self._reconnect_announced = False
        except Exception as err:
            if not announced:
                kind = _diag._STR_STR(_diag._STORED_NAME(type(err)))
                _diag.pending(
                    f"PostgresSink could not reconnect ({kind}); this attempt fails, and a lost "
                    "line follows only if the batch is given up"
                )
            raise

    def _is_broken(self) -> bool:
        """Reports whether the held connection can no longer be used.

        Args:
          None.

        Returns:
          True when the driver reports the connection closed or broken.

        Raises:
          None. A driver whose attribute access raises is treated as usable, so a probe can
            never be the reason a batch fails.
        """
        try:
            return bool(getattr(self._conn, "closed", False)) or bool(
                getattr(self._conn, "broken", False)
            )
        except Exception:
            return False

    def _rollback(self) -> None:
        """Discards the failed transaction, absorbing a rollback that itself fails (FR-002).

        The bare call this replaces was the most common failure compounding itself: when the
        server has closed the session — the usual reason the insert failed — psycopg raises from
        ``rollback()`` too, and that escaped mid-handler. Measured at ``max_retries=3``, attempts
        dropped from 4 to 1, ``losses()`` reported ``failed=0`` after a totally lost batch, no
        ``_diag.lost`` line was written, and the worker received a raw driver exception instead
        of ``SinkDeliveryError``.

        Absorbing is right rather than merely convenient: the rollback is *cleanup* for a failure
        already being handled, so its own failure must not displace the original one, and a
        connection too broken to roll back is a connection the remaining attempts will fail on
        anyway — visibly, and through the counters.

        Args:
          None.

        Returns:
          None.

        Raises:
          None. ``Exception`` only, never ``BaseException``: a ``KeyboardInterrupt`` here is the
            operator's intent and must reach the caller (SPEC-025).
        """
        try:
            self._conn.rollback()
        except Exception as err:
            _diag.absorbed("PostgresSink.rollback", err)

    def close(self) -> None:
        """Commits pending work and closes only an owned connection (FR-005).

        Idempotent, and takes the emit lock so the final commit never lands in the middle of
        another thread's transaction (SPEC-028 FR-002).

        **The final commit is guarded** (SPEC-041 FR-002). It was unconditional, so a connection
        the server had closed made ``close()`` raise — reachable without any exotic timing, since
        a broken connection is only repaired inside an emit attempt and a process that breaks and
        then shuts down never has another. The release still has to happen, and a commit that
        cannot reach the server has nothing to publish, so the failure is announced by type and
        the close proceeds.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the driver raises on close. The commit no longer escapes.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._conn.commit()
            except Exception as err:
                _diag.absorbed("PostgresSink.close commit", err)
            if self._owns_connection:
                self._conn.close()

    def _row(self, event: dict[str, object]) -> tuple[object, ...]:
        """Builds one row: the extracted columns, then the whole event as JSON.

        **A U+0000 is repaired, not refused** (SPEC-069 FR-003). PostgreSQL refuses it in
        ``jsonb`` and psycopg refuses it in a text column, and either used to fail the whole
        transaction; assembly lets it through, and ``configure(service=...)`` stamps it on every
        event. The JSON text this builds anyway is searched with
        :data:`~log_foundry.sinks._encode.NUL_ESCAPE`, so a clean event costs one search, and
        only on a match is the event rebuilt by
        :func:`~log_foundry.sinks._encode.repair_nul` — every U+0000 in a string or key, at any
        depth, becomes U+FFFD on a copy marked ``truncated`` — and encoded again. The columns
        are read from the same copy. The six characters ``\\u0000`` written as text are left
        alone, since JSONB accepts them.

        Args:
          event: The event to convert.

        Returns:
          The statement parameters, in column order.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        text = json.dumps(event)
        source = event
        if NUL_ESCAPE.search(text):
            repaired = repair_nul(event)
            if repaired is not None:
                source, text = repaired, json.dumps(repaired)
        return (*(source.get(col) for col in _COLUMNS), text)

    def _ensure_schema(self) -> None:
        """Idempotently creates the target table.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the driver raises on the DDL.
        """
        columns = ", ".join(f"{col} TEXT" for col in _COLUMNS)
        with self._conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table} "
                f"(id BIGSERIAL PRIMARY KEY, {columns}, event JSONB NOT NULL)"
            )
        self._conn.commit()
