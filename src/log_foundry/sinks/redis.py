"""RedisStreamsSink / RedisListSink — buffer events in Redis (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["RedisListSink", "RedisStreamsSink"]

_BACKOFF_BASE = 0.1


class _RedisSink:
    """Shared pipelining, bounded retry, and ownership-aware close for the Redis sinks.

    The module is named ``redis`` to match the extra and imports the driver lazily, so it never
    shadows or requires the real package at import time. The worst-case delay (SPEC-027 FR-005)
    is ``max_retries`` interruptible waits per batch, 0.7 s at the defaults.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``redis-py`` documents that a client may be shared between threads — a
    connection is taken from its pool only for the duration of a command, and command
    execution never mutates the client. Its one documented exception is that a ``Pipeline``
    must not be passed between
    threads, which is why the pipeline here is built and executed inside a single ``emit`` call
    and never stored on the instance.
    """

    def __init__(self, *, client: Any, url: str | None, max_retries: int) -> None:
        """Connects to Redis, recording whether the connection is the sink's to close.

        Args:
          client: A ``redis-py``-shaped client to borrow, or ``None`` to open one.
          url: The connection URL used when opening a client.
          max_retries: Retries per batch, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value made the retry range empty, so ``emit`` returned having
            attempted nothing, a silent success.

        Returns:
          None.

        Raises:
          ImportError: If the ``redis`` extra is not installed.
        """
        self._owns_client = client is None
        if client is None:
            import redis  # type: ignore[import-not-found]

            client = redis.Redis.from_url(url) if url else redis.Redis()
        self.client = client
        self.max_retries = max(max_retries, 0)
        self.stop_signal: threading.Event | None = None
        self.failed = 0
        self._counter_lock = threading.Lock()

    def losses(self) -> SinkLosses:
        """Reports events abandoned past the retry bound (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=0, failed=self.failed)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Pipelines the whole batch into one round trip, retrying on error (FR-005).

        Args:
          batch: The events to buffer. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the retry bound is spent. The batch travels as one pipeline,
            so such a failure delivered nothing: it is counted and then raised, giving the worker
            its retry and ``health()`` the loss (SPEC-026 FR-001). There is no partial case to
            protect, because the pipeline is all or nothing.
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
            except Exception as err:
                if attempt < self.max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
                    continue
                with self._counter_lock:
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
        """Closes the connection only if the sink owns it (FR-005).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises on close.
        """
        if self._owns_client:
            self.client.close()

    def _stage(self, pipe: Any, event: dict[str, object]) -> None:
        """Stages one event onto the pipeline, in whatever form the subclass writes.

        Args:
          pipe: The open pipeline.
          event: The event to stage.

        Returns:
          None.

        Raises:
          NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


class RedisStreamsSink(_RedisSink):
    """Appends each event to a Redis stream via ``XADD``, pipelined per batch (FR-005)."""

    def __init__(
        self, stream: str, *, client: Any = None, url: str | None = None, max_retries: int = 3
    ) -> None:
        """Binds the sink to a stream.

        Args:
          stream: The stream key to append to.
          client: A ``redis-py``-shaped client to borrow, or ``None`` to open one.
          url: The connection URL used when opening a client.
          max_retries: Retries per batch.

        Returns:
          None.

        Raises:
          ImportError: If the ``redis`` extra is not installed.
        """
        self._stream = stream
        super().__init__(client=client, url=url, max_retries=max_retries)

    def _stage(self, pipe: Any, event: dict[str, object]) -> None:
        """Stages one ``XADD`` onto the pipeline.

        Args:
          pipe: The open pipeline.
          event: The event to append.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises.
        """
        pipe.xadd(self._stream, {"event": json.dumps(event)})


class RedisListSink(_RedisSink):
    """Pushes each event onto a Redis list via ``RPUSH``, pipelined per batch (FR-005)."""

    def __init__(
        self, key: str, *, client: Any = None, url: str | None = None, max_retries: int = 3
    ) -> None:
        """Binds the sink to a list.

        Args:
          key: The list key to push onto.
          client: A ``redis-py``-shaped client to borrow, or ``None`` to open one.
          url: The connection URL used when opening a client.
          max_retries: Retries per batch.

        Returns:
          None.

        Raises:
          ImportError: If the ``redis`` extra is not installed.
        """
        self._key = key
        super().__init__(client=client, url=url, max_retries=max_retries)

    def _stage(self, pipe: Any, event: dict[str, object]) -> None:
        """Stages one ``RPUSH`` onto the pipeline.

        Args:
          pipe: The open pipeline.
          event: The event to push.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises.
        """
        pipe.rpush(self._key, json.dumps(event))
