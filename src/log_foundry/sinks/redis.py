"""RedisStreamsSink / RedisListSink — buffer events in Redis (arch §8, §9.1, SPEC-010).

Two durable-buffer sinks on the ``redis`` extra (``redis-py``, imported lazily): one appends to a
Redis **stream** (``XADD``), the other pushes onto a **list** (``RPUSH``). Each pipelines the whole
batch into a single round trip. A connection error is retried within a bounded count then counted and
logged; ``close()`` releases the connection only when the sink opened it (an injected client is not
closed). The module is named ``redis`` to match the extra, and imports the driver lazily so it never
shadows or requires the real ``redis`` package at import time.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import threading

from log_foundry import _diag
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["RedisListSink", "RedisStreamsSink"]

_BACKOFF_BASE = 0.1


class _RedisSink:
    """Shared pipelining, bounded retry, and ownership-aware close for the Redis sinks.

    **Worst-case delay** (SPEC-027 FR-005): ``max_retries`` waits of ``0.1 * 2**n`` per batch —
    0.7 s at the default 3. The waits are interruptible, so ``shutdown()`` cuts one short.
    """

    def __init__(self, *, client: Any, url: str | None, max_retries: int) -> None:
        self._owns_client = client is None
        if client is None:
            import redis  # type: ignore[import-not-found]  # optional 'redis' extra

            client = redis.Redis.from_url(url) if url else redis.Redis()
        self.client = client
        # Floored as ``Worker._emit`` floors its own (SPEC-021): a negative value made the
        # retry range empty, so ``emit`` returned having attempted nothing — a silent success.
        self.max_retries = max(max_retries, 0)
        # Set by the worker when this sink is the configured one (SPEC-027 FR-002).
        self.stop_signal: threading.Event | None = None
        self.failed = 0

    def losses(self) -> SinkLosses:
        """Events abandoned past the retry bound (SPEC-026 FR-002). Never raises."""
        return SinkLosses(dropped=0, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Pipeline the whole batch into one round trip, retrying on connection error (FR-005).

        The batch travels as one pipeline, so a failure past the retry bound delivered *nothing*:
        it is counted and then raised, giving the worker its retry and ``health()`` the loss
        (SPEC-026 FR-001). There is no partial case to protect — the pipeline is all or nothing.
        """
        if not batch:
            return
        for attempt in range(self.max_retries + 1):
            try:
                pipe = self.client.pipeline()
                for event in batch:
                    self._stage(pipe, event)
                pipe.execute()
                return
            except Exception as err:  # isolation boundary: never crash the worker (FR-011)
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
                    continue
                self.failed += len(batch)
                _diag.lost(
                    "event",
                    len(batch),
                    f"{type(self).__name__}, {self.max_retries + 1} attempts, {type(err).__name__}",
                )
                raise SinkDeliveryError(
                    f"{type(self).__name__} delivered none of {len(batch)} event(s)"
                ) from None

    def close(self) -> None:
        """Close the connection only if the sink owns it (FR-005)."""
        if self._owns_client:
            self.client.close()

    def _stage(self, pipe: Any, event: dict[str, object]) -> None:
        raise NotImplementedError


class RedisStreamsSink(_RedisSink):
    """Append each event to a Redis stream via ``XADD``, pipelined per batch (FR-005)."""

    def __init__(
        self, stream: str, *, client: Any = None, url: str | None = None, max_retries: int = 3
    ) -> None:
        self._stream = stream
        super().__init__(client=client, url=url, max_retries=max_retries)

    def _stage(self, pipe: Any, event: dict[str, object]) -> None:
        pipe.xadd(self._stream, {"event": json.dumps(event)})


class RedisListSink(_RedisSink):
    """Push each event onto a Redis list via ``RPUSH``, pipelined per batch (FR-005)."""

    def __init__(
        self, key: str, *, client: Any = None, url: str | None = None, max_retries: int = 3
    ) -> None:
        self._key = key
        super().__init__(client=client, url=url, max_retries=max_retries)

    def _stage(self, pipe: Any, event: dict[str, object]) -> None:
        pipe.rpush(self._key, json.dumps(event))
