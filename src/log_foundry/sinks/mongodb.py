"""MongoDBSink — insert events into a MongoDB collection (arch §8, SPEC-011).

Near-zero impedance: events are already dicts, so ``insert_many`` takes them as-is. ``pymongo`` is
the optional ``mongo`` extra, imported lazily inside the sink (never at module top). Inserts are
unordered (``ordered=False``) so one bad document does not abort the rest of the batch; a
``BulkWriteError`` is caught, its failed-insert count recorded, and the successfully-inserted
documents retained. Write-only — querying is the downstream tool's job.
"""

from __future__ import annotations

import json
import time
from typing import Any

from log_foundry import _diag

__all__ = ["MongoDBSink"]

_BACKOFF_BASE = 0.1
_MAX_DOC_BYTES = 16 * 1024 * 1024  # MongoDB's hard 16 MB per-document limit


class MongoDBSink:
    """A :class:`~log_foundry.sinks.base.Sink` that inserts events into a MongoDB collection.

    Attributes:
        failed: Documents the server rejected (bulk write errors) or a whole batch abandoned past
            the retry bound.
        dropped_oversized: Documents dropped for exceeding the 16 MB limit.
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
        self._owns_client = client is None
        if client is None:
            from pymongo import MongoClient  # type: ignore[import-not-found]  # 'mongo' extra

            client = MongoClient(uri)
        self._client = client
        self._collection = client[database][collection]
        self.max_retries = max_retries
        self.failed = 0
        self.dropped_oversized = 0
        self._closed = False

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Insert every event unordered; count bulk failures, retry connection errors (FR-003)."""
        documents = self._documents(batch)
        if not documents:
            return
        for attempt in range(self.max_retries + 1):
            try:
                self._collection.insert_many(documents, ordered=False)
                return
            except Exception as err:  # isolation boundary: never crash the worker (FR-006)
                details = getattr(err, "details", None)
                if isinstance(details, dict) and "writeErrors" in details:
                    # BulkWriteError: unordered insert stored what it could; count the rejects and
                    # do not retry (successes are already in, a retry would duplicate/re-error).
                    rejects = len(details["writeErrors"])
                    self.failed += rejects
                    _diag.lost(
                        "document", rejects, "MongoDBSink bulk write; the rest were inserted"
                    )
                    return
                if attempt < self.max_retries:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += len(documents)
                _diag.lost(
                    "document",
                    len(documents),
                    f"MongoDBSink, {self.max_retries + 1} attempts, {type(err).__name__}",
                )
                return

    def close(self) -> None:
        """Close the client only if the sink owns it; idempotent (FR-005)."""
        if self._closed:
            return
        if self._owns_client:
            self._client.close()
        self._closed = True

    # -- internals ----------------------------------------------------------------------

    def _documents(self, batch: list[dict[str, object]]) -> list[dict[str, object]]:
        """Copy each event (so ``_id`` insertion doesn't mutate the caller's dicts), drop oversized."""
        documents: list[dict[str, object]] = []
        for event in batch:
            if len(json.dumps(event).encode("utf-8")) > _MAX_DOC_BYTES:
                self.dropped_oversized += 1
                _diag.lost("document", 1, "MongoDBSink, exceeds the 16 MB limit")
                continue
            documents.append(dict(event))
        return documents
