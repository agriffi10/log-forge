"""RedisStreamsSink / RedisListSink — buffer events in Redis (arch §8, §9.1, SPEC-010)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from log_foundry.sinks import _engine
from log_foundry.sinks._args import refuse_with_injected
from log_foundry.sinks._classify import TRANSIENT_BUILTINS, driver_classes, is_retryable
from log_foundry.sinks._encode import encode_round, unencodable_in_run
from log_foundry.sinks._engine import When, emit_via_deliver, run_context
from log_foundry.sinks._extras import import_extra
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._refusal import final_refusal
from log_foundry.sinks._retry import floor_count, require_positive
from log_foundry.sinks._rounds import Lost, mark_sent, raised_line
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["RedisListSink", "RedisStreamsSink"]


class _RedisSink:
    """Shared pipelining, bounded retry, and ownership-aware close for the Redis sinks.

    The module is named ``redis`` to match the extra and imports the driver lazily, so it never
    shadows or requires the real package at import time. It delivers through SPEC-072's engine
    (SPEC-074 FR-004), one pipeline per round, so the worst-case delay (SPEC-027 FR-005) is
    ``max_retries`` interruptible waits per run, 0.7 s at the defaults.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. ``redis-py`` documents that a client may be shared between threads — a
    connection is taken from its pool only for the duration of a command, and command
    execution never mutates the client. Its documented exceptions are ``PubSub`` and
    ``Pipeline`` objects, which must not be passed between threads — which is why the pipeline
    here is built and executed inside a single round and never stored on the instance.

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
          max_retries: Further rounds the engine gives a batch whose pipeline failed, floored at
            zero as ``Worker._emit`` floors its own (SPEC-021). Anything but an ``int`` is refused with
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
        self.log_foundry_counters = SinkCounters()
        self._guard = ClosedGuard(GuardMode.CLOSE_ONLY)

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
        """Reports the engine's totals: unencodable events, and events never confirmed.

        ``dropped`` is the events the encode step could not encode (SPEC-069 FR-001), counted only
        by a run that settled; ``failed`` the encodable events of a run whose last pipeline
        failed, or, for a failure not worth retrying, its first (SPEC-026 FR-002, SPEC-070), each
        once per run (SPEC-074).

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
          batch: The events to buffer. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the sink is closed, by identity, with nothing counted. Also
            when the run failed — the budget spent, or at once for a failure ``_retryable_error``
            does not retry (SPEC-070) — with the engine's message: the batch travels as one
            pipeline, all or nothing, so such a run delivered nothing (SPEC-026 FR-001).
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-004).

        Defined here, on ``_RedisSink``, beside :meth:`emit`; neither subclass defines either, so
        SPEC-072's override rule offers ``deliver`` on both.

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the client propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Pipelines the round's encodable events in one round trip and classifies them all.

        A closed sink's round is a refusal carrying 1.x's closed error. Every event is encoded
        once per run (:func:`~log_foundry.sinks._encode.encode_round`), an unencodable one
        ``DROPPED`` with SPEC-069's line. The pipeline is built and executed inside this one call
        and never stored, since ``redis-py`` pipelines must not cross threads: all ``ACCEPTED``
        on ``execute()``, or all ``RETRYABLE`` or ``REJECTED`` as ``_retryable_error`` answers.
        A run that fails counts the run's unencodable events in ``failed`` with the rest and its
        line names them too, as 1.x's ``emit`` counted a failed batch whole
        (:attr:`~log_foundry.sinks._engine.When.FAILED`); a run that settles leaves them
        ``DROPPED``.

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
                        f"{type(self).__name__} delivered none of {len(batch)} event(s): "
                        "the sink is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context = run_context()
            fates = [Outcome.DROPPED] * len(batch)
            pairs = encode_round(self, batch, self._encode, sink=type(self).__name__)
            if not pairs:
                return Verdict(outcomes=tuple(fates))
            positions = [position for position, _ in pairs]
            carried = [batch[position] for position in positions]
            mark_sent(self, carried)
            try:
                pipe = self.client.pipeline()
                for _, payload in pairs:
                    self._stage(pipe, payload)
                pipe.execute()
            except Exception as err:
                retryable = is_retryable(self._retryable_error, err)
                kind = type(err).__name__
                whole = unencodable_in_run(self)
                context.count(self.log_foundry_counters, "failed_events", whole, when=When.FAILED)
                lost = Lost(self)
                lost.add(
                    carried,
                    when=When.LAST if retryable else When.ALWAYS,
                    key=kind,
                    line=raised_line("event", type(self).__name__, kind),
                    whole=whole,
                )
                lost.register()
                outcome = Outcome.RETRYABLE if retryable else Outcome.REJECTED
            else:
                outcome = Outcome.ACCEPTED
            for position in positions:
                fates[position] = outcome
            return Verdict(outcomes=tuple(fates))

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

        Idempotent through the guard, so two concurrent calls cannot both reach
        ``client.close()`` — ``atexit`` racing user code is the documented case. The guard
        closes whether or not the client is owned, because it marks *this sink* as
        released rather than the connection (SPEC-032 FR-001).

        **A close waits a bounded time for a round still inside** (SPEC-076 FR-003). The client
        is thread-safe, so rounds run concurrently and take no lock; each is a ``CLOSE_ONLY``
        section of the guard. A close refuses every later round at once, waits idle for at most
        ``CLOSE_OVERLAP_WAIT`` for the rounds inside, then releases once whether or not one is
        still inside; a second close waits for the first's release, then returns.
        A round still inside after that bound meets a closed client: ``redis-py`` raises
        ``ConnectionError``, which is ``RETRYABLE``, and the run's next round is refused,
        so a failed run commits nothing for it and its catcher counts the batch. Such a
        round can also reopen a connection nothing reaps, the leak SPEC-032 records for a
        post-close emit, accepted against waiting on a call bounded only by the client's
        own socket timeout, which the library neither sets nor sees.

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
            self.client.close()

    def _encode(self, event: dict[str, object]) -> str:
        """Encodes one event as the JSON text both Redis sinks write.

        Args:
          event: The event to encode.

        Returns:
          The JSON text.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
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
