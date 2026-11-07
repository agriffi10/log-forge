"""RedisStreamsSink / RedisListSink — buffer events in Redis (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
import threading
from typing import Any

from log_foundry import _diag
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._encode import encode_events
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._retry import floor_count, require_positive, wait
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
    execution never mutates the client. Its documented exceptions are ``PubSub`` and
    ``Pipeline`` objects, which must not be passed between threads — which is why the pipeline
    here is built and executed inside a single ``emit`` call and never stored on the instance.

    It refuses an emit after :meth:`close` (SPEC-032 FR-001). Left unguarded this sink did not
    fail after close, it *succeeded*: ``redis-py``'s pool reconnects transparently on the next
    command, so a batch emitted after ``shutdown()`` opened a connection nothing would ever
    reap — the same leak SPEC-028's review found in ``RabbitMQSink``, whose ``_active_channel``
    reopened whatever ``close()`` had released.


    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.
    """

    def __init__(self, *, client: Any, url: str | None, max_retries: int) -> None:
        """Connects to Redis, recording whether the connection is the sink's to close.

        Args:
          client: A ``redis-py``-shaped client to borrow, or ``None`` to open one.
          url: The connection URL used when opening a client.
          max_retries: Retries per batch, floored at zero as ``Worker._emit`` floors its own
            (SPEC-021) — a negative value made the retry range empty, so ``emit`` returned having
            attempted nothing, a silent success. Anything but an ``int`` is refused with
            ``TypeError`` (SPEC-066 FR-001), before any client is opened, the message naming the
            concrete subclass; a ``bool`` or ``int`` subclass is read as the ``int`` it holds.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066).
          ValueError: If ``url`` is passed alongside ``client=``, which is already built and
            would never read it (SPEC-071 FR-003).
          ImportError: If the ``redis`` extra is not installed.
        """
        self.max_retries = floor_count(max_retries, 0, "max_retries", type(self).__name__)
        refuse_with_injected(type(self).__name__, "client", client, {"url": url})
        self._owns_client = client is None
        if client is None:
            redis = import_extra("redis", "redis")
            client = redis.Redis.from_url(url) if url else redis.Redis()
        self.client = client
        self.log_foundry_stop_signal: threading.Event | None = None
        self.failed = 0
        self._unencodable = 0
        self._counter_lock = threading.Lock()
        self._closed = False
        self._close_lock = threading.Lock()

    def losses(self) -> SinkLosses:
        """Reports unencodable events, and events abandoned past the retry bound.

        ``dropped`` is the events :meth:`emit` could not encode (SPEC-069 FR-001); ``failed`` the
        events of a batch abandoned past the retry bound or, for a failure not worth retrying, at
        its first attempt (SPEC-026 FR-002, SPEC-070).

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
        """Pipelines the whole batch into one round trip, retrying on error (FR-005).

        A closed sink refuses the batch before asking for a pipeline (SPEC-032 FR-001), since
        asking would silently reopen the connection ``close()`` released. The refusal does not
        depend on whether the sink owned the client: a borrowed client surviving its sink is the
        caller's business, and is not permission to keep writing through a sink that has been
        released. Refusing moves no counter here — it is a failure reported to the worker, which
        records it in ``health().failed_batches``, not loss this sink absorbed.

        Every event is encoded once, outside the retry loop and before the first pipeline
        (SPEC-069 FR-001), so the loop stages the same payloads on every attempt. An event that
        cannot be encoded is dropped, counted and announced only when this call returns; before,
        the encode sat inside the loop, so one such event spent every attempt and cost the whole
        batch, counted ``failed``. A batch of nothing encodable asks for no pipeline and returns
        (FR-002). A spent bound still counts the whole batch in ``failed``, an upper bound.

        Args:
          batch: The events to buffer. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink is closed. Also when the retry bound is spent, or at
            once for a failure ``_retryable_error`` does not retry (SPEC-070): the
            batch travels as one pipeline, so such a failure delivered nothing: it is counted and
            then raised, giving the worker its retry and ``health()`` the loss (SPEC-026 FR-001).
            There is no partial case to protect, because the pipeline is all or nothing.
        """
        if not batch:
            return
        if self._closed:
            raise SinkDeliveryError(
                f"{type(self).__name__} delivered none of {len(batch)} event(s): "
                "the sink is closed"
            )
        with Deferral(self) as pending:
            encoded = encode_events(batch, self._encode, pending, sink=type(self).__name__)
            if not encoded:
                return
            self._send([payload for _, payload in encoded], len(batch))

    def _send(self, payloads: list[str], batch_size: int) -> None:
        """Pipelines the encoded payloads within the retry bound.

        Args:
          payloads: The encoded events, known non-empty.
          batch_size: How many events the calling ``emit`` was handed, the ``failed`` count when
            the bound is spent.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the retry bound is spent, or at once for a failure
            :meth:`_retryable_error` does not retry (SPEC-070).
        """
        for attempt in range(self.max_retries + 1):
            try:
                pipe = self.client.pipeline()
                for payload in payloads:
                    self._stage(pipe, payload)
                pipe.execute()
                return
            except Exception as err:
                if attempt < self.max_retries and is_retryable(self._retryable_error, err):
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += batch_size
                _diag.lost(
                    "event",
                    batch_size,
                    f"{type(self).__name__}, {attempt + 1} attempt(s), {type(err).__name__}",
                )
                raise SinkDeliveryError(
                    f"{type(self).__name__} delivered none of {batch_size} event(s)"
                ) from None

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failed pipeline is worth another attempt (SPEC-070 FR-002).

        redis-py's own default ``Retry`` retries ``ConnectionError``, ``TimeoutError`` and
        ``socket.timeout`` (``redis/retry.py``), and ``redis/cluster.py`` retries
        ``TryAgainError`` and ``ClusterDownError``; those are retried here. Three
        ``ConnectionError`` subclasses are narrowed out — ``AuthenticationError``,
        ``AuthorizationError`` and ``ExternalAuthProviderError``, the last skipped in a version
        that lacks it — because a resend cannot fix a credential. ``ResponseError`` and its other
        subclasses, and ``DataError``, fail at once.

        Args:
          err: The exception the attempt raised.

        Returns:
          True when another attempt may succeed.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        credential = driver_classes(
            "redis.exceptions",
            "AuthenticationError",
            "AuthorizationError",
            "ExternalAuthProviderError",
        )
        if isinstance(err, credential):
            return False
        retried = (
            *TRANSIENT_BUILTINS,
            *driver_classes(
                "redis.exceptions",
                "ConnectionError",
                "TimeoutError",
                "TryAgainError",
                "ClusterDownError",
            ),
        )
        return isinstance(err, retried)

    def close(self) -> None:
        """Closes the connection only if the sink owns it (FR-005).

        Idempotent, with the flag set under a lock so two concurrent calls cannot both reach
        ``client.close()`` — ``atexit`` racing user code is the documented case. The flag is set
        whether or not the client is owned, because it marks *this sink* as released rather than
        the connection (SPEC-032 FR-001).

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
                self.client.close()

    def _encode(self, event: dict[str, object]) -> str:
        """Encodes one event as the JSON text both Redis sinks write.

        Args:
          event: The event to encode.

        Returns:
          The JSON text.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_events`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        return json.dumps(event)

    def _stage(self, pipe: Any, payload: str) -> None:
        """Stages one encoded event onto the pipeline, in whatever form the subclass writes.

        Args:
          pipe: The open pipeline.
          payload: The event's JSON text, from :meth:`_encode`.

        Returns:
          None.

        Raises:
          NotImplementedError: Always, in the base class.
        """
        raise NotImplementedError


class RedisStreamsSink(_RedisSink):
    """Appends each event to a Redis stream via ``XADD``, pipelined per batch (FR-005).

    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.
    """

    def __init__(
        self,
        stream: str,
        *,
        client: Any = None,
        url: str | None = None,
        max_retries: int = 3,
        maxlen: int | None = None,
    ) -> None:
        """Binds the sink to a stream.

        Args:
          stream: The stream key to append to.
          client: A ``redis-py``-shaped client to borrow, or ``None`` to open one.
          url: The connection URL used when opening a client.
          max_retries: Retries per batch, an ``int`` floored at zero; anything else is refused
            with ``TypeError`` (SPEC-066 FR-001).
          maxlen: A ceiling on the destination's length, or ``None`` for no ceiling.

            The default is ``None`` — today's unbounded behaviour — because silently discarding
            a user's buffered logs is not a default this library may choose (SPEC-038 FR-008).
            Used as arch §9.1 recommends, as the durable buffer in front of a consumer, a stalled
            consumer otherwise OOMs the Redis instance with no ceiling the operator can set from
            here.

            **Trimming discards at the *destination*, outside anything :meth:`losses` can see.**
            Redis drops the oldest entries itself, after this sink has already reported the write
            as delivered, so those events are invisible to ``health()`` — which is the trade a
            bounded buffer makes, and is why it is opt-in.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), or ``maxlen`` is given and
            is not one.
          ValueError: If ``maxlen`` is given and not positive — ``redis-py`` refuses a negative
            ``XADD MAXLEN`` client-side on every emit, and ``0`` trims every entry the moment it
            lands, so neither has a working configuration to protect (SPEC-049, system-frame
            review).
          ImportError: If the ``redis`` extra is not installed.
        """
        self._stream = stream
        self.maxlen = (
            require_positive(maxlen, "maxlen", "RedisStreamsSink") if maxlen is not None else None
        )
        super().__init__(client=client, url=url, max_retries=max_retries)

    def _stage(self, pipe: Any, payload: str) -> None:
        """Stages one ``XADD`` onto the pipeline.

        Args:
          pipe: The open pipeline.
          payload: The event's JSON text, from :meth:`_encode`.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises.
        """
        if self.maxlen is None:
            pipe.xadd(self._stream, {"event": payload})
        else:
            pipe.xadd(self._stream, {"event": payload}, maxlen=self.maxlen, approximate=True)


class RedisListSink(_RedisSink):
    """Pushes each event onto a Redis list via ``RPUSH``, pipelined per batch (FR-005).

    It keeps **no** client buffer (SPEC-036 FR-002): the driver call returns only once the
    destination has the batch, so nothing is queued locally between emits.
    """

    def __init__(
        self,
        key: str,
        *,
        client: Any = None,
        url: str | None = None,
        max_retries: int = 3,
        maxlen: int | None = None,
    ) -> None:
        """Binds the sink to a list.

        Args:
          key: The list key to push onto.
          client: A ``redis-py``-shaped client to borrow, or ``None`` to open one.
          url: The connection URL used when opening a client.
          max_retries: Retries per batch, an ``int`` floored at zero; anything else is refused
            with ``TypeError`` (SPEC-066 FR-001).
          maxlen: A ceiling on the destination's length, or ``None`` for no ceiling.

            The default is ``None`` — today's unbounded behaviour — because silently discarding
            a user's buffered logs is not a default this library may choose (SPEC-038 FR-008).
            Used as arch §9.1 recommends, as the durable buffer in front of a consumer, a stalled
            consumer otherwise OOMs the Redis instance with no ceiling the operator can set from
            here.

            **Trimming discards at the *destination*, outside anything :meth:`losses` can see.**
            Redis drops the oldest entries itself, after this sink has already reported the write
            as delivered, so those events are invisible to ``health()`` — which is the trade a
            bounded buffer makes, and is why it is opt-in.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` is not an ``int`` (SPEC-066), or ``maxlen`` is given and
            is not one.
          ValueError: If ``maxlen`` is given and not positive. A negative became
            ``LTRIM key N -1`` after every push — removing the *oldest* N rather than keeping the
            newest, so a short list was emptied on every batch with every counter at zero — and
            ``0`` became ``LTRIM 0 -1``, a no-op that reads as a ceiling and is not (SPEC-049,
            system-frame review).
          ImportError: If the ``redis`` extra is not installed.
        """
        self._key = key
        self.maxlen = (
            require_positive(maxlen, "maxlen", "RedisListSink") if maxlen is not None else None
        )
        super().__init__(client=client, url=url, max_retries=max_retries)

    def _stage(self, pipe: Any, payload: str) -> None:
        """Stages one ``RPUSH`` onto the pipeline.

        Args:
          pipe: The open pipeline.
          payload: The event's JSON text, from :meth:`_encode`.

        Returns:
          None.

        Raises:
          Exception: Whatever the client raises.
        """
        pipe.rpush(self._key, payload)
        if self.maxlen is not None:
            pipe.ltrim(self._key, -self.maxlen, -1)
