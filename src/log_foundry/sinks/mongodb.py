"""MongoDBSink — insert events into a MongoDB collection (arch §8, SPEC-011)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["MongoDBSink"]

_BACKOFF_BASE = 0.1
_MAX_DOC_BYTES = 16 * 1024 * 1024


class MongoDBSink:
    """A :class:`~log_foundry.sinks.base.Sink` that inserts events into a MongoDB collection.

    The impedance is near zero: events are already dicts, so ``insert_many`` takes them as-is.
    ``pymongo`` is the optional ``mongo`` extra, imported lazily. Inserts are unordered so one bad
    document does not abort the rest of the batch, and the sink is write-only — querying is the
    downstream tool's job. The worst-case delay (SPEC-027 FR-005) is ``max_retries``
    interruptible waits per batch, 0.7 s at the defaults.

    The driver requirement satisfied (SPEC-028 FR-002): ``pymongo``'s ``MongoClient`` is
    thread-safe and owns a connection pool, so this sink deliberately takes **no** transport lock
    — serializing it would funnel a driver built for concurrency through one caller at a time,
    and FR-002 asks for correctness under concurrent calls, not for parallelism to be removed.
    Only the loss counters are guarded, since those are the sink's own state rather than the
    driver's.

    Attributes:
      failed: Documents the server rejected, or a whole batch abandoned past the retry bound.
      dropped_oversized: Documents dropped for exceeding MongoDB's 16 MB per-document limit.


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
    ) -> None:
        """Binds the sink to a collection.

        Args:
          client: A ``pymongo``-shaped client to borrow, or ``None`` to open one.
          uri: The connection URI used when opening a client.
          database: The database holding the collection.
          collection: The collection to insert into.
          max_retries: Retries per batch, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value returned having attempted no insert, and reported
            success.

        Returns:
          None.

        Raises:
          ImportError: If the ``mongo`` extra is not installed.
        """
        self._owns_client = client is None
        if client is None:
            from pymongo import MongoClient  # type: ignore[import-not-found]

            client = MongoClient(uri)
        self._client = client
        self._collection = client[database][collection]
        self.max_retries = max(max_retries, 0)
        self.log_foundry_stop_signal: threading.Event | None = None
        self.failed = 0
        self.dropped_oversized = 0
        self._closed = False
        self._counter_lock = threading.Lock()
        self._close_lock = threading.Lock()

    def losses(self) -> SinkLosses:
        """Reports oversized drops and documents the server rejected or never took (FR-002).

        Both fields are read under one lock so the pair comes from the same instant rather than
        straddling a concurrent emit's increments (SPEC-028 FR-003).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Inserts every event unordered, counting bulk failures and retrying errors (FR-003).

        A bulk write error means the unordered insert stored what it could, so the rejects are
        counted and not retried: the successes are already in, and a retry would duplicate them
        or re-error.

        Args:
          batch: The events to insert.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink is already closed, since ``pymongo`` raises
            ``InvalidOperation`` on any use of a closed client and the library has its own word
            for "none of this was delivered". Also when nothing was inserted (SPEC-026 FR-001) — a connection error
            past the retry bound, or a bulk write every one of whose documents the server
            rejected. A bulk write that stored some is partial and never raises. A batch of
            nothing but oversized documents does not raise either: they can never fit, so there
            is nothing to retry, and they are reported through :meth:`losses`.
        """
        if not batch:
            return
        if self._closed:
            raise SinkDeliveryError(
                f"MongoDBSink inserted none of {len(batch)} document(s): the sink is closed"
            )
        documents = self._documents(batch)
        if not documents:
            return
        for attempt in range(self.max_retries + 1):
            try:
                self._collection.insert_many(documents, ordered=False)
                return
            except Exception as err:
                details = getattr(err, "details", None)
                if isinstance(details, dict) and "writeErrors" in details:
                    rejects = len(details["writeErrors"])
                    with self._counter_lock:
                        self.failed += rejects
                    total = rejects >= len(documents)
                    _diag.lost(
                        "document",
                        rejects,
                        "MongoDBSink bulk write; none were inserted" if total
                        else "MongoDBSink bulk write; the rest were inserted",
                    )
                    if total:
                        raise SinkDeliveryError(
                            f"MongoDBSink inserted none of {len(documents)} document(s)"
                        ) from None
                    return
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += len(documents)
                _diag.lost(
                    "document",
                    len(documents),
                    f"MongoDBSink, {self.max_retries + 1} attempts, {type(err).__name__}",
                )
                raise SinkDeliveryError(
                    f"MongoDBSink inserted none of {len(documents)} document(s)"
                ) from None

    def close(self) -> None:
        """Closes the client only if the sink owns it (FR-005).

        Idempotent, with the flag set under a lock so two concurrent ``close()`` calls cannot
        both reach ``client.close()`` — ``atexit`` racing user code is the documented case.

        The lock does **not** make a close wait for an in-flight ``insert_many``, and saying so
        plainly matters: ``emit`` takes no lock here because ``pymongo``'s client is thread-safe,
        and a close that waited would have to serialize against every insert, which is the
        parallelism SPEC-028 FR-002 declines to remove. What covers the overlap instead is
        :meth:`emit`'s own ``_closed`` check, which refuses the batch in the library's
        vocabulary rather than letting it reach a closed client — pymongo raises
        ``InvalidOperation`` on any use after close. An insert that passes the check and is
        still in flight when the close lands can still see that error; it is counted as a failed
        batch like any other, which is a reported loss at process exit rather than a silent one.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on close.
        """
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            if self._owns_client:
                self._client.close()

    def _documents(self, batch: list[dict[str, object]]) -> list[dict[str, object]]:
        """Copies each event and drops any document too large to ever fit.

        The copy is what keeps ``_id`` insertion by the driver from mutating the caller's dicts.

        Args:
          batch: The events to convert.

        Returns:
          The documents that can be inserted.

        Raises:
          None.
        """
        documents: list[dict[str, object]] = []
        for event in batch:
            if len(json.dumps(event).encode("utf-8")) > _MAX_DOC_BYTES:
                with self._counter_lock:
                    self.dropped_oversized += 1
                _diag.lost("document", 1, "MongoDBSink, exceeds the 16 MB limit")
                continue
            documents.append(dict(event))
        return documents
