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
import sys
import time
from typing import Any

__all__ = ["RedisListSink", "RedisStreamsSink"]

_BACKOFF_BASE = 0.1


class _RedisSink:
    """Shared pipelining, bounded retry, and ownership-aware close for the Redis sinks."""

    def __init__(self, *, client: Any, url: str | None, max_retries: int) -> None:
        self._owns_client = client is None
        if client is None:
            import redis  # type: ignore[import-not-found]  # optional 'redis' extra

            client = redis.Redis.from_url(url) if url else redis.Redis()
        self.client = client
        self.max_retries = max_retries
        self.failed = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Pipeline the whole batch into one round trip, retrying on connection error (FR-005)."""
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
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += len(batch)
                sys.stderr.write(
                    f"log-foundry: {type(self).__name__} abandoned {len(batch)} event(s) after "
                    f"{self.max_retries + 1} attempts ({err!r})\n"
                )
                return

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
