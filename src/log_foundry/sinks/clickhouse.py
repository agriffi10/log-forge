"""ClickHouseSink — batch-insert events into a ClickHouse table (arch §8, SPEC-011)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from log_foundry.sinks import _engine
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._chunk import chunk_list, valid_identifier
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count, require_positive, require_timeout
from log_foundry.sinks._rounds import Called, Lost, mark_sent, raised_line
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["ClickHouseSink"]

_COLUMNS = (
    "timestamp",
    "level",
    "trace_id",
    "span_id",
    "function",
    "service",
    "duration_ms",
    "status",
)
_COLUMN_NAMES = [*_COLUMNS, "event"]

_COLUMN_TYPES = {
    "timestamp": "Nullable(String)",
    "level": "Nullable(String)",
    "trace_id": "Nullable(String)",
    "span_id": "Nullable(String)",
    "function": "Nullable(String)",
    "service": "Nullable(String)",
    "duration_ms": "Nullable(Float64)",
    "status": "Nullable(String)",
}


class ClickHouseSink:
    """A :class:`~log_foundry.sinks.base.Sink` that batch-inserts events into ClickHouse.

    ClickHouse is the columnar, observability-scale favorite. ``clickhouse-connect`` is the
    optional ``clickhouse`` extra, imported lazily. Each event maps to a row of extracted typed
    columns plus the full event as a ``String`` column, inserted in a single call per chunk. The
    sink is write-only and delivers through SPEC-072's engine (SPEC-074 FR-003): each round
    inserts every remaining chunk once and the engine resends only the chunks that may land, so
    the worst-case delay (SPEC-027 FR-005) is ``max_retries`` interruptible waits per run, 0.7 s
    at the defaults. The lock is held for one round, never across the engine's wait, so a
    ``close()`` arriving during a backoff takes it between rounds and the next round is refused.

    The driver requirement satisfied (SPEC-028 FR-002): ``clickhouse-connect`` states that a
    client is **not** thread-safe when using session ids, and that concurrent queries in one
    session raise ``ProgrammingError``. This sink calls ``get_client(dsn=...)``, which takes the
    default auto-generated session, so it is squarely in that case and the lock is required
    rather than merely prudent. One client per thread would be the alternative, and that is the
    connection-pool design FR-002 puts out of scope.


    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.

    Attributes:
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
    """

    def __init__(
        self,
        table: str,
        *,
        client: Any = None,
        dsn: str | None = None,
        create_table: bool = False,
        chunk_size: int = 1000,
        max_retries: int = 3,
        send_receive_timeout: float | None = None,
    ) -> None:
        """Connects to the server and, optionally, provisions the schema.

        Args:
          table: The target table, validated as a plain SQL identifier.
          client: A ``clickhouse-connect``-shaped client to borrow, or ``None`` to open one.
          dsn: The connection string used when opening a client.
          create_table: An idempotent ``MergeTree`` convenience, off by default. Every extracted
            column is nullable, to tolerate missing keys — a span-start event has no
            ``duration_ms`` or ``status``.
          chunk_size: How many rows go in one insert call. Refused when non-positive
            (SPEC-049 FR-002): ``0`` raised out of ``range()`` on every batch, and a **negative**
            was the silent one — the chunker yielded nothing, so ``emit`` returned having inserted
            no rows with ``losses()`` at zero.
          max_retries: Retries per chunk, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value returned from ``_insert`` having attempted nothing, and
            reported success. Anything but an ``int`` is refused with ``TypeError`` (SPEC-066
            FR-001); a ``bool`` or ``int`` subclass is read as the ``int`` it holds.
          send_receive_timeout: Seconds one request to the server may take, forwarded to
            ``get_client`` only when given (SPEC-049 FR-005). ``clickhouse-connect``'s own default
            is a finite 300 s, so it is exposed rather than overridden — but ``_insert`` makes
            ``max_retries + 1`` such attempts per chunk on the drain thread, which is why the
            argument exists.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), judged before any client is
            opened.
          ValueError: If the table name is not a plain SQL identifier, the chunk size is not
            positive, ``send_receive_timeout`` cannot bound anything, or ``dsn`` or
            ``send_receive_timeout`` is passed alongside ``client=``, which is already built and
            cannot consume either (SPEC-043's rule; ``dsn`` since SPEC-071 FR-003).
          ImportError: If the ``clickhouse`` extra is not installed.
        """
        refuse_with_injected(
            "ClickHouseSink",
            "client",
            client,
            {"dsn": dsn, "send_receive_timeout": send_receive_timeout},
        )
        bounds: dict[str, float] = {}
        if send_receive_timeout is not None:
            bounds["send_receive_timeout"] = require_timeout(
                send_receive_timeout, "send_receive_timeout", "ClickHouseSink"
            )
        self._table = valid_identifier(table)
        self._chunk_size = require_positive(chunk_size, "chunk_size", "ClickHouseSink")
        self.max_retries = floor_count(max_retries, 0, "max_retries", "ClickHouseSink")
        self.log_foundry_stop_signal: threading.Event | None = None
        self.log_foundry_counters = SinkCounters()
        self._guard = ClosedGuard(GuardMode.SERIAL)
        self._owns_client = client is None
        if client is None:
            client = import_extra("clickhouse_connect", "clickhouse").get_client(dsn=dsn, **bounds)
        self.client = client
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
        """Reports the engine's totals: unencodable events, and rows never confirmed.

        ``dropped`` is the events the encode step could not encode (SPEC-069 FR-001), counted only
        by a run that settled; ``failed`` the rows of a chunk still failing when the run's rounds
        ran out or, for a failure not worth retrying, refused at its first attempt (SPEC-026
        FR-002, SPEC-070), each once per run (SPEC-074). Read under the counters' lock, never the
        transport lock (SPEC-028 FR-003), so a poll never waits on an in-flight insert.

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

        Args:
          batch: The events to insert. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed — no chunk inserted (SPEC-026 FR-001) — with the
            engine's message. A partially-inserted batch returns: the chunks that landed are
            committed. When the sink is closed, or a non-empty batch produced no chunk at all
            (SPEC-049 FR-002), the round is a refusal and this raises the 1.x
            ``SinkDeliveryError`` by identity, with nothing counted.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-003).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the client propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Inserts every remaining chunk once, under the transport lock for this round only.

        A closed sink's round is a refusal carrying 1.x's closed error, decided before any encode
        or client call. Every event is encoded once per run
        (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one ``DROPPED`` with
        SPEC-069's line; the rows are chunked with ``chunk_list`` and each chunk inserted once by
        :meth:`_insert`, ``ACCEPTED`` when it lands and ``RETRYABLE`` or ``REJECTED`` as
        ``_retryable_error`` answers, its line ``ClickHouseSink, N attempt(s), <Type>`` naming the
        calls that carried it. A non-empty round that produced no chunk is a refusal carrying
        1.x's error (SPEC-049 FR-002): it sent nothing.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the client propagates.
        """
        with self._guard.enter() as section:
            if not section:
                run_context().refuse(
                    final_refusal(
                        f"ClickHouseSink inserted none of {len(batch)} event(s): the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context = run_context()
            fates = [Outcome.DROPPED] * len(batch)
            pairs = encode_round(self, batch, self._row, sink=type(self).__name__)
            if not pairs:
                return Verdict(outcomes=tuple(fates))
            lost = Lost(self)
            chunks = 0
            for chunk in chunk_list(pairs, self._chunk_size):
                chunks += 1
                carried = [batch[position] for position, _ in chunk]
                mark_sent(self, carried)
                called = self._insert([row for _, row in chunk])
                if called.error is None:
                    outcome = Outcome.ACCEPTED
                else:
                    outcome = Outcome.RETRYABLE if called.retryable else Outcome.REJECTED
                    kind = type(called.error).__name__
                    lost.add(
                        carried,
                        when=When.LAST if called.retryable else When.ALWAYS,
                        key=kind,
                        line=raised_line("row", "ClickHouseSink", kind),
                    )
                for position, _ in chunk:
                    fates[position] = outcome
            if not chunks:
                context.refuse(
                    SinkDeliveryError(
                        f"ClickHouseSink produced no chunk for {len(batch)} event(s); nothing was "
                        "sent"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            lost.register()
        return Verdict(outcomes=tuple(fates))

    def close(self) -> None:
        """Closes the client only if the sink owns it (FR-005).

        Idempotent through the guard (SPEC-076 FR-002): once a close begins every later round is
        refused at once, the one round inside, if any, is waited for and never released under,
        and the release runs once; a second close waits for the first's release, then returns.
        The client is never closed mid-insert (SPEC-028 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on close.
        """
        self._guard.close(self._release)

    def _release(self) -> None:
        """Closes the client if the sink owns it.

        :meth:`close` hands this to the guard, which runs it once, with the lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on close.
        """
        if self._owns_client:
            self.client.close()

    def _row(self, event: dict[str, object]) -> list[object]:
        """Builds one row: the extracted columns, then the whole event as JSON.

        Args:
          event: The event to convert.

        Returns:
          The row values, in column order.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return [*(event.get(col) for col in _COLUMNS), json.dumps(event)]

    def _insert(self, rows: list[list[object]]) -> Called[None]:
        """Inserts one chunk once (SPEC-074 FR-003); the engine resends it in a later round.

        Args:
          rows: The chunk's rows.

        Returns:
          The call, with its exception when it raised.

        Raises:
          None. This is an isolation boundary: a driver fault must never crash the worker.
        """
        try:
            self.client.insert(self._table, data=rows, column_names=_COLUMN_NAMES)
        except Exception as err:
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(None)

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed insert is worth another attempt (SPEC-070 FR-002).

        ``OperationalError`` is clickhouse-connect's class for a failure "not necessarily under
        the control of the programmer, e.g. an unexpected disconnect" (``driver/exceptions.py``),
        and at the locked 1.4.2 ``driver/httpclient.py`` raises it for a transport error and for a
        ``429``, ``503`` or ``504`` it has already retried itself. ``DatabaseError`` itself,
        ``DataError``, ``ProgrammingError`` and ``InterfaceError`` fail at once: the same rows meet
        the same refusal. The class is resolved per call, never imported.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        retried = (
            *TRANSIENT_BUILTINS,
            *driver_classes(
                "clickhouse_connect.driver.exceptions",
                "OperationalError",
            ),
        )
        return isinstance(err, retried)

    def _ensure_schema(self) -> None:
        """Idempotently creates the target ``MergeTree`` table.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on the DDL.
        """
        columns = ", ".join(f"{col} {_COLUMN_TYPES[col]}" for col in _COLUMNS)
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            f"({columns}, event String) ENGINE = MergeTree ORDER BY tuple()"
        )
