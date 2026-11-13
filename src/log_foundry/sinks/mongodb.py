"""MongoDBSink — insert events into a MongoDB collection (arch §8, SPEC-011)."""

from __future__ import annotations

import json
import math
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._batch import adjudicate_keyed
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._encode import encode_round, encoded_landed, repair_bson
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count, require_timeout
from log_foundry.sinks._rounds import Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from log_foundry.sinks._deferral import Deferral
    from log_foundry.sinks._engine import _Drops
    from log_foundry.sinks.base import SinkLosses

__all__ = ["MongoDBSink"]

_MAX_DOC_BYTES = 16 * 1024 * 1024

DEFAULT_SOCKET_TIMEOUT = 30.0
"""Seconds one socket read or write may take, forwarded as ``socketTimeoutMS=30000`` (SPEC-049 FR-005).

``pymongo``'s own default is ``None`` — no bound at all — and every such wait happens on the
worker's single drain thread, which every other sink's delivery is queued behind. It is applied
only when neither the caller nor the URI's query names one.
"""


def _uri_option_names(uri: str | None) -> set[str]:
    """Returns the option names a MongoDB URI's query carries, lower-cased.

    Read with ``urllib`` rather than ``pymongo.uri_parser``, because the latter resolves a
    ``mongodb+srv://`` URI over DNS and a constructor must not touch the network. Lower-cased
    because ``pymongo`` matches option names case-insensitively, and split on both ``&`` and ``;``
    because it accepts either separator.

    Args:
      uri: The connection URI, or ``None`` for the driver's own localhost default.

    Returns:
      The option names named in the query, or an empty set.

    Raises:
      None.
    """
    if uri is None:
        return set()
    query = urlsplit(uri).query
    return {part.partition("=")[0].strip().lower() for part in re.split("[&;]", query) if part}


def _milliseconds(seconds: float) -> int:
    """Converts a seconds bound to the whole milliseconds ``pymongo`` takes, never rounding to zero.

    ``math.ceil`` rather than ``int``: a value between ``0`` and ``0.001`` would otherwise become
    ``socketTimeoutMS=0``, which ``pymongo`` reads as *no* timeout — the unbounded wait this
    argument exists to remove, reached through a value ``require_timeout`` accepted.

    Args:
      seconds: A positive, finite bound.

    Returns:
      The bound in milliseconds, at least ``1``.

    Raises:
      None.
    """
    return math.ceil(seconds * 1000)


def _client_bounds(
    uri: str | None, socket_timeout: float | None, server_selection_timeout: float | None
) -> dict[str, int]:
    """Builds the timeout keywords a client this sink opens for itself receives (SPEC-049 FR-005).

    Precedence is ``pika``'s, not ``psycopg``'s: an explicit ``socket_timeout=`` overrides a
    ``socketTimeoutMS`` the URI names, the library's default never does — measured,
    ``MongoClient(uri_with_socketTimeoutMS, socketTimeoutMS=30000)`` would otherwise let the
    default silently override a value the caller wrote. ``serverSelectionTimeoutMS`` is forwarded
    only when given, because the driver's own 30 s default is finite and stays the driver's.

    Args:
      uri: The connection URI, or ``None``.
      socket_timeout: The caller's socket bound in seconds, or ``None``.
      server_selection_timeout: The caller's server-selection bound in seconds, or ``None``.

    Returns:
      Keyword arguments for ``MongoClient``, possibly empty.

    Raises:
      ValueError: If a given bound cannot bound anything.
    """
    bounds: dict[str, int] = {}
    if socket_timeout is not None:
        bounds["socketTimeoutMS"] = _milliseconds(
            require_timeout(socket_timeout, "socket_timeout", "MongoDBSink")
        )
    elif "sockettimeoutms" not in _uri_option_names(uri):
        bounds["socketTimeoutMS"] = _milliseconds(DEFAULT_SOCKET_TIMEOUT)
    if server_selection_timeout is not None:
        bounds["serverSelectionTimeoutMS"] = _milliseconds(
            require_timeout(server_selection_timeout, "server_selection_timeout", "MongoDBSink")
        )
    return bounds


class MongoDBSink:
    """A :class:`~log_foundry.sinks.base.Sink` that inserts events into a MongoDB collection.

    The impedance is near zero: events are already dicts, so ``insert_many`` takes them as-is.
    ``pymongo`` is the optional ``mongo`` extra, imported lazily. Inserts are unordered so one bad
    document does not abort the rest of the batch, and the sink is write-only — querying is the
    downstream tool's job. It delivers through SPEC-072's engine (SPEC-074 FR-003): one
    ``insert_many`` per round, the engine resending only what may land, so the worst-case delay
    (SPEC-027 FR-005) is ``max_retries`` interruptible waits per run, 0.7 s at the defaults. The
    documents are built once per run, so a resend carries the very ``_id`` pymongo stamped into
    each on the first attempt.

    The driver requirement satisfied (SPEC-028 FR-002): ``pymongo``'s ``MongoClient`` is
    thread-safe and owns a connection pool, so this sink deliberately takes **no** transport lock
    — serializing it would funnel a driver built for concurrency through one caller at a time,
    and FR-002 asks for correctness under concurrent calls, not for parallelism to be removed.
    Only the loss counters are guarded, since those are the sink's own state rather than the
    driver's.

    Attributes:
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports.
      failed: Documents the server rejected or never confirmed, ``failed_events``; assignable.
      dropped_oversized: Documents dropped for exceeding MongoDB's 16 MB per-document limit,
        counted only by a run that settled.


    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.
    """

    def __init__(
        self,
        *,
        client: Any = None,
        uri: str | None = None,
        database: str,
        collection: str,
        max_retries: int = 3,
        socket_timeout: float | None = None,
        server_selection_timeout: float | None = None,
    ) -> None:
        """Binds the sink to a collection.

        Args:
          client: A ``pymongo``-shaped client to borrow, or ``None`` to open one.
          uri: The connection URI used when opening a client.
          database: The database holding the collection.
          collection: The collection to insert into.
          max_retries: Retries per batch, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value returned having attempted no insert, and reported
            success. Anything but an ``int`` is refused with ``TypeError`` (SPEC-066 FR-001),
            before any client is opened — a ``MongoClient`` starts monitor threads, so a refusal
            raised after it would leave one behind. A ``bool`` or ``int`` subclass is read as the
            ``int`` it holds.
          socket_timeout: Seconds one socket read or write may take, or ``None`` for
            :data:`DEFAULT_SOCKET_TIMEOUT` unless the URI's query names ``socketTimeoutMS``
            itself (SPEC-049 FR-005). ``pymongo``'s own default is unbounded. It is
            ``socketTimeoutMS`` in this library's units and case — seconds, snake case — because
            every sibling timeout in the package is seconds. A caller who wants the driver's
            unbounded original injects their own client.
          server_selection_timeout: Seconds to wait for a suitable server, or ``None`` to pass
            nothing and keep the driver's finite 30 s default.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066).
          ValueError: If either bound cannot bound anything, or if ``uri`` or a bound is passed
            alongside ``client=``, which is already connected and cannot consume it — SPEC-043's
            rule that an argument no backend can use is an error rather than a silent ignore
            (``uri`` since SPEC-071 FR-003).
          ImportError: If the ``mongo`` extra is not installed.
        """
        self.max_retries = floor_count(max_retries, 0, "max_retries", "MongoDBSink")
        refuse_with_injected(
            "MongoDBSink",
            "client",
            client,
            {
                "uri": uri,
                "socket_timeout": socket_timeout,
                "server_selection_timeout": server_selection_timeout,
            },
        )
        self._owns_client = client is None
        if client is None:
            client = import_extra("pymongo", "mongo").MongoClient(
                uri, **_client_bounds(uri, socket_timeout, server_selection_timeout)
            )
        self._client = client
        self._collection = client[database][collection]
        self.log_foundry_stop_signal: threading.Event | None = None
        self.log_foundry_counters = SinkCounters()
        self.dropped_oversized = 0
        self._guard = ClosedGuard(GuardMode.CLOSE_ONLY)
        self._bson_resolved = False
        self._bson_encode: Callable[[Any], object] | None = None

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
    def _close_lock(self) -> threading.Lock:
        """The guard's lock under its 1.x name, read-only (SPEC-076 FR-005).

        In ``CLOSE_ONLY`` mode it is held by a close for its whole run and by no section, so a
        1.x subclass taking it serialises with the close only, as it did with the 1.x lock;
        assigning it raises ``AttributeError``.

        Args:
          None.

        Returns:
          The lock the close holds.

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
        """Reports the engine's totals: oversized and unencodable drops, documents never confirmed.

        ``dropped`` folds in the events the encode step could not encode (SPEC-069 FR-001);
        ``dropped_oversized`` still means oversize only. One snapshot under the counters' lock
        (SPEC-028 FR-003).

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

        A bulk write error means the unordered insert stored what it could, so the rejects are
        counted and not resent: the successes are already in, and a resend would duplicate them
        or re-error. An oversize drop is counted and announced only when ``emit`` returns — on
        the engine, only when the run settles.

        Args:
          batch: The events to insert.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink is already closed, by identity, with nothing counted,
            since ``pymongo`` raises ``InvalidOperation`` on any use of a closed client. Also when
            the run failed: nothing inserted — a connection error past the budget or one not
            worth retrying (SPEC-070), or a bulk write that rejected every document. A bulk write
            that stored some is partial and never raises, and neither does a batch of nothing but
            oversized documents.
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
        """Makes one unordered ``insert_many`` of the round's documents and classifies each.

        A closed sink's round is a refusal carrying 1.x's closed error. Each document is built
        once per run (:func:`~log_foundry.sinks._encode.encode_round` keeps it in the run's
        notes, keyed by event identity), so a resend is the same object, carrying the ``_id``
        pymongo stamped into it; an occurrence that landed is never handed out again. A
        ``BulkWriteError`` whose ``details["writeErrors"]`` the keyed reader can pair by
        ``index`` makes each named document ``REJECTED``, with 1.x's line, and the rest
        ``ACCEPTED``, never resent; one it cannot pair makes the call's documents ``UNKNOWN``,
        counted once with a line. ``writeErrors`` keeps its 1.x precedence over the predicate:
        any other exception goes to ``_retryable_error``. No transport lock is taken, in 1.x or
        here: the client is thread-safe, and the round is a ``CLOSE_ONLY`` section of the guard,
        which a close waits for at most ``CLOSE_OVERLAP_WAIT`` (SPEC-076 FR-003).

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
                        f"MongoDBSink inserted none of {len(batch)} document(s): "
                        "the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context = run_context()
            self._resolve_bson()
            fates = [Outcome.DROPPED] * len(batch)
            pairs = encode_round(self, batch, self._encode, sink=type(self).__name__)
            documents = self._documents(pairs, context.drops(self))
            if not documents:
                return Verdict(outcomes=tuple(fates))
            positions = [position for position, _ in documents]
            carried = [batch[position] for position in positions]
            mark_sent(self, carried)
            try:
                self._collection.insert_many([document for _, document in documents], ordered=False)
            except Exception as err:
                details = getattr(err, "details", None)
                if isinstance(details, dict) and "writeErrors" in details:
                    self._settle_bulk(details["writeErrors"], positions, fates)
                else:
                    retryable = is_retryable(self._retryable_error, err)
                    for position in positions:
                        fates[position] = Outcome.RETRYABLE if retryable else Outcome.REJECTED
                    lost = Lost(self)
                    kind = type(err).__name__
                    lost.add(
                        carried,
                        when=When.LAST if retryable else When.ALWAYS,
                        key=kind,
                        line=raised_line("document", "MongoDBSink", kind),
                    )
                    lost.register()
            else:
                for position in positions:
                    fates[position] = Outcome.ACCEPTED
                encoded_landed(self, positions)
            return Verdict(outcomes=tuple(fates))

    def _settle_bulk(self, errors: object, positions: list[int], fates: list[Outcome]) -> None:
        """Reads a ``BulkWriteError``'s ``writeErrors`` by ``index`` through the keyed reader.

        A list it cannot pair makes every document ``UNKNOWN`` with one line, ``ALWAYS``, or
        ``LAST`` in a run that resends an ``UNKNOWN`` (``RunContext.resends_unknown``), so a
        document is announced once per run.

        Args:
          errors: The details' ``writeErrors``, of any shape.
          positions: Each inserted document's position in the round's batch, in insert order.
          fates: The round's outcomes, updated in place.

        Returns:
          None.

        Raises:
          None.
        """
        context = run_context()
        read = adjudicate_keyed(list(range(len(positions))), errors, key="index")
        if read.unadjudicated:
            for position in positions:
                fates[position] = Outcome.UNKNOWN
            unread = len(positions)
            context.announce(
                lambda: _diag.lost(
                    "document",
                    unread,
                    f"MongoDBSink could not pair a bulk write's writeErrors to the {unread} "
                    "document(s) sent; not retried",
                ),
                when=When.LAST if context.resends_unknown(self) else When.ALWAYS,
            )
            return
        landed: list[int] = []
        rejects = 0
        for position, named in zip(positions, read.failed, strict=True):
            if named is None:
                fates[position] = Outcome.ACCEPTED
                landed.append(position)
            else:
                fates[position] = Outcome.REJECTED
                rejects += 1
        encoded_landed(self, landed)
        total = rejects >= len(positions)
        context.announce(
            lambda: _diag.lost(
                "document",
                rejects,
                "MongoDBSink bulk write; none were inserted"
                if total
                else "MongoDBSink bulk write; the rest were inserted",
            ),
            when=When.ALWAYS,
        )

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed insert is worth another attempt (SPEC-070 FR-002).

        ``ConnectionFailure`` and so ``AutoReconnect`` ("the operation ... has not necessarily
        succeeded"), ``NetworkTimeout``, ``NotPrimaryError``, ``ServerSelectionTimeoutError`` and
        ``WaitQueueTimeoutError`` are retried, and so is any ``PyMongoError`` the driver or the
        server labelled ``RetryableWriteError`` (``pymongo/errors.py``,
        ``mongo_client_shared.py``). ``OperationFailure``, ``WriteError``, ``DuplicateKeyError``,
        ``WTimeoutError`` (the write was applied), ``InvalidDocument``, ``DocumentTooLarge``,
        ``InvalidOperation`` — what a client closed under the insert raises — and
        ``ConfigurationError`` fail at once. :meth:`emit` reads a ``BulkWriteError``'s
        ``writeErrors`` ahead of this, so one that names failed documents is still counted from
        its ``details``.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        retried = (*TRANSIENT_BUILTINS, *driver_classes("pymongo.errors", "ConnectionFailure"))
        if isinstance(err, retried):
            return True
        if not isinstance(err, driver_classes("pymongo.errors", "PyMongoError")):
            return False
        has_label = getattr(err, "has_error_label", None)
        return callable(has_label) and bool(has_label("RetryableWriteError"))

    def close(self) -> None:
        """Closes the client only if the sink owns it (FR-005).

        Idempotent through the guard, so two concurrent ``close()`` calls cannot both reach
        ``client.close()`` — ``atexit`` racing user code is the documented case — and the
        second returns only after the first's release has.

        **A close waits, for a bounded time, for an in-flight ``insert_many``** (SPEC-076
        FR-003). Rounds take no lock, because ``pymongo``'s client is thread-safe and a
        lock across every insert would remove the parallelism SPEC-028 FR-002 keeps; each
        is a ``CLOSE_ONLY`` section of the guard instead. A close refuses every later round
        at once, in the library's vocabulary rather than by reaching a closed client,
        waits idle for at most ``CLOSE_OVERLAP_WAIT`` for the rounds inside, then
        releases. A round still inside after that meets a closed client, which raises
        ``InvalidOperation``: not a ``ConnectionFailure`` and carrying no
        ``RetryableWriteError`` label, so it is ``REJECTED``, an attempted delivery
        counted in ``failed`` like any other — a reported loss, never a silent one — and
        the run's next round is refused, so no round after the release reaches it.

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

        :meth:`close` hands this to the guard, which runs it once, with its lock held and
        the state already ``CLOSED``.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on close.
        """
        if self._owns_client:
            self._client.close()

    def _resolve_bson(self) -> None:
        """Finds the driver's own ``bson.encode`` once, on the first ``emit`` that needs it.

        It is the detector :meth:`_encode` runs (SPEC-069 FR-003): what BSON refuses is decided
        by BSON. It is imported here rather than at construction, so a sink built over an
        injected client never forces an import, and read by name: a ``bson`` that is missing,
        or has no ``encode``, disables the check rather than raising — an injected client double
        with no driver installed is not a BSON encoder either. So does a ``bson`` whose import or
        attribute read raises anything else — a broken install, a module that fails at import —
        since that is not this sink's fault and must not fail every ``emit`` (invariant 1);
        ``insert_many`` still reports whatever that driver does after it. Two threads resolving
        at once both store the same answer.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if self._bson_resolved:
            return
        encode = None
        try:
            found = getattr(import_extra("bson", "mongo"), "encode", None)
        except Exception:
            found = None
        if callable(found):
            encode = found
        self._bson_encode = encode
        self._bson_resolved = True

    def _encode(self, event: dict[str, object]) -> tuple[dict[str, object], int]:
        """Encodes one event as the document to insert, with its JSON size.

        The copy is what keeps ``_id`` insertion by the driver from mutating the caller's dicts.
        The size is the existing ``json.dumps`` measure the 16 MB drop reads, taken of what is
        inserted.

        **What BSON refuses is repaired, not refused** (SPEC-069 FR-003). ``bson.encode`` refuses
        an integer outside the signed 64-bit range and a NUL in a key, both of which assembly
        lets through, and either used to fail the whole ``insert_many`` client-side. When the
        driver's own encoder (:meth:`_resolve_bson`) refuses the document,
        :func:`~log_foundry.sinks._encode.repair_bson` rebuilds it — the integer becomes the
        ``<int: ~N digits>`` placeholder, the NUL in a key U+FFFD, on a copy marked
        ``truncated`` — and the encoder is asked again. A document it still refuses, or one
        with nothing to repair, raises, and the encode helper drops that one event. With no
        encoder there is no check.

        Args:
          event: The event to encode.

        Returns:
          The document and its size in bytes.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
          Exception: Whatever ``bson.encode`` raises for a document the repair cannot fix — the
            same drop.
        """
        document = dict(event)
        check = self._bson_encode
        if check is not None:
            try:
                check(document)
            except Exception:
                repaired = repair_bson(event)
                if repaired is None:
                    raise
                check(repaired)
                document = repaired
        return document, len(json.dumps(document).encode("utf-8"))

    def _documents(
        self,
        encoded: list[tuple[int, tuple[dict[str, object], int]]],
        pending: Deferral | _Drops,
    ) -> list[tuple[int, dict[str, object]]]:
        """Keeps each encoded document, dropping any too large to ever fit.

        It takes what :meth:`_encode` built (SPEC-069 FR-001), so the size check runs over
        encoded forms and nothing here can fail on an event.

        A drop goes to ``pending`` rather than to the counter, so it is counted and announced
        only when ``emit`` returns (SPEC-059 FR-001) — on the engine, ``pending`` is the run
        context's ``SETTLED`` view, so only when the run settles (SPEC-074). The line names no per-event value,
        so each drop registers its own count and its own line, one per document as before.

        Args:
          encoded: Each encodable event's position in the round's batch with its document and
            size.
          pending: The run's ``SETTLED`` view, which holds every drop decided here until the run
            is known to settle.

        Returns:
          The position and document of each that can be inserted.

        Raises:
          None.
        """
        documents: list[tuple[int, dict[str, object]]] = []
        for position, (document, size) in encoded:
            if size > _MAX_DOC_BYTES:
                pending.count("dropped_oversized")
                pending.announce(
                    lambda: _diag.lost("document", 1, "MongoDBSink, exceeds the 16 MB limit")
                )
                continue
            documents.append((position, document))
        return documents
