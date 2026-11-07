"""PostgresSink — insert events into a Postgres table with a JSONB column (arch §8, SPEC-011)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._chunk import chunk_list, valid_identifier
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import NUL_ESCAPE, encode_events, repair_nul
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import floor_count, require_positive, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["PostgresSink"]

_BACKOFF_BASE = 0.1

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
    write-only. The worst-case delay (SPEC-027 FR-005) has two halves, and only one of them is
    interruptible. The backoffs are ``max_retries`` waits per batch — 0.7 s at the defaults —
    taken through ``_retry.wait`` on the worker's stop event, so a shutdown cuts them short. The
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
    gives the sequence the exclusivity it was written for.


    It keeps **no** client buffer (SPEC-036 FR-002): ``emit`` commits its own transaction, so
    nothing is uncommitted once it returns. The commit in ``close`` is belt and braces, not a
    buffer.
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
        self.failed = 0
        self._unencodable = 0
        self._closed = False
        self._lock = threading.Lock()
        self._counter_lock = threading.Lock()
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

    def losses(self) -> SinkLosses:
        """Reports unencodable events, and events abandoned past the retry bound.

        ``dropped`` is the events :meth:`emit` could not encode (SPEC-069 FR-001); ``failed`` the
        events of a batch abandoned past the retry bound or, for a failure not worth retrying, at
        its first attempt (SPEC-026 FR-002, SPEC-070).

        Reads under the counter lock rather than the emit lock (SPEC-028 FR-003), so a poll
        never waits on an in-flight insert and its backoff.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self._unencodable, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Inserts the whole batch in one transaction, rolling back and retrying (FR-004).

        The diagnostic names the exception's type and never its repr: ``_row`` binds the whole
        serialized event as a statement parameter, and a psycopg error repr routinely reprints
        the failing statement and its parameters, so the old line reprinted the event, PII
        included, into a stream nobody was asked to secure (SPEC-029 FR-002, arch §6).

        Every event's row is built once, before the lock is taken and outside the retry loop
        (SPEC-069 FR-001), so the loop retries the same rows rather than re-encoding the batch on
        every attempt. An event whose row cannot be built is dropped, counted and announced only
        when this call returns; before, the encode sat inside the retry loop, so one such event
        spent every attempt and cost the whole batch, counted ``failed``. A batch of nothing
        encodable runs no transaction and returns, once the closed check has passed (FR-002).

        Args:
          batch: The events to insert. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the retry bound is spent, or at once for a failure
            :meth:`_retryable_error` does not retry (SPEC-070). One transaction, rolled back on
            failure, means such a batch inserted nothing, and the rollback is what makes the
            worker's retry safe here — there are no committed rows to duplicate (SPEC-026
            FR-001).
        """
        if not batch:
            return
        with Deferral(self) as pending:
            encoded = encode_events(batch, self._row, pending, sink=type(self).__name__)
            with self._lock:
                if self._closed:
                    raise SinkDeliveryError(
                        f"PostgresSink inserted none of {len(batch)} event(s): the sink is closed"
                    )
                if not encoded:
                    return
                self._insert_batch([row for _, row in encoded], len(batch))

    def _insert_batch(self, rows: list[tuple[object, ...]], batch_size: int) -> None:
        """Runs the transaction sequence, with the emit lock already held.

        Split out so the lock's extent is one line in :meth:`emit` rather than an extra
        indentation level over the whole retry loop.

        A spent bound counts the whole batch in ``failed``, unencodable events included, as
        before SPEC-069: ``failed`` is an upper bound (invariant 2), and the unencodable count is
        discarded with the raise and decided again by the call that finally returns.

        Args:
          rows: The encoded rows to insert, known non-empty.
          batch_size: How many events the calling ``emit`` was handed.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the retry bound is spent, at once for a failure
            :meth:`_retryable_error` does not retry (SPEC-070), or when a non-empty batch
            produced no chunk at all — the ``ClickHouseSink.emit`` guard's twin (SPEC-049
            FR-002, found by the system-frame review): with the chunker yielding nothing this
            committed an empty transaction and returned, having sent nothing, with every
            counter at zero. Raised outside the retry loop, because a chunker that yields nothing
            will yield nothing four times.
        """
        for attempt in range(self.max_retries + 1):
            try:
                self._reconnect_if_broken()
                chunks = 0
                with self._conn.cursor() as cur:
                    for chunk in chunk_list(rows, self._chunk_size):
                        chunks += 1
                        cur.executemany(self._insert_sql, chunk)
                if chunks:
                    self._conn.commit()
                    return
            except Exception as err:
                self._rollback()
                if attempt < self.max_retries and is_retryable(self._retryable_error, err):
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += batch_size
                _diag.lost(
                    "event",
                    batch_size,
                    f"PostgresSink, {attempt + 1} attempt(s), {type(err).__name__}"
                    + (
                        ", borrowed connection is broken and this sink may not reopen it"
                        if not self._owns_connection and self._is_broken()
                        else ""
                    ),
                )
                raise SinkDeliveryError(
                    f"PostgresSink inserted none of {batch_size} event(s)"
                ) from None
            raise SinkDeliveryError(
                f"PostgresSink produced no chunk for {batch_size} event(s); nothing was sent"
            )

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
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
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
