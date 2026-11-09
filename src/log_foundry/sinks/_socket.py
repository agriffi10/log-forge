"""Shared raw-socket transport for LogstashSink (socket mode) and SyslogSink (SPEC-009)."""

from __future__ import annotations

import errno
import json
import socket
from typing import TYPE_CHECKING

from log_foundry import _diag
from log_foundry.sanitize import _int_text
from log_foundry.sinks import _engine
from log_foundry.sinks._args import ReadOnlyCount, store_read_only
from log_foundry.sinks._classify import is_retryable
from log_foundry.sinks._encode import encode_round
from log_foundry.sinks._engine import When, run_context
from log_foundry.sinks._guard import ClosedGuard, GuardMode
from log_foundry.sinks._retry import (
    exact_int,
    floor_count,
    require_positive,
    require_timeout,
)
from log_foundry.sinks._rounds import Called, Lost, mark_sent, raised_line
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from log_foundry.sinks._deferral import Deferral
    from log_foundry.sinks._engine import _Drops
    from log_foundry.sinks.base import SinkLosses

__all__ = ["SocketTransport"]

DEFAULT_MAX_DATAGRAM_BYTES = 65507
"""Bytes one UDP datagram may carry, the IPv4 payload maximum (SPEC-038 FR-007).

65,535 less the 20-byte IP header and the 8-byte UDP header. That is the **IPv4** figure, and
it is used for IPv6 too: IPv6 does not count its own header in the payload length, so its true
maximum is 65,527 — a 20-byte discrepancy no real path can reach, since no MTU carries either.
It is the ceiling the *protocol* imposes; a path's real limit is often far lower, which is why
it is configurable. TCP is a stream and has no such limit, so this bounds nothing there.
"""

_PERMANENT_ERRNOS = frozenset({errno.EMSGSIZE})
"""Socket errnos that describe the message rather than the destination (FR-007 AC-3).

``EMSGSIZE`` alone. Every other errno this transport can raise — ``ECONNREFUSED``,
``EHOSTUNREACH``, ``ENETDOWN``, ``EPIPE``, ``ETIMEDOUT`` — describes a destination that may come
back, and treating any of those as permanent would turn a transient outage into silent loss. A
set stated here rather than a condition inferred at the call site, so adding to it is a decision
someone makes deliberately.
"""


def _make_tcp(host: str, port: int, timeout: float) -> socket.socket:
    """Opens a connected TCP socket.

    This is a module-level seam so tests can substitute a fake socket without network access.

    Args:
      host: The destination host.
      port: The destination port.
      timeout: Seconds allowed for the connection.

    Returns:
      The connected socket.

    Raises:
      OSError: If the connection cannot be established.
    """
    return socket.create_connection((host, port), timeout=timeout)


def _make_udp(host: str) -> socket.socket:
    """Opens an unconnected UDP socket in an address family the host resolves to (SPEC-031).

    The family is resolved rather than assumed: a hardcoded ``AF_INET`` made every ``sendto``
    to an IPv6 destination fail, silently, until the retry bound abandoned the message. TCP
    never had the defect because ``socket.create_connection`` resolves for itself.

    **IPv4 wins when the host offers it**, and taking the first result instead was measured
    losing logs. ``getaddrinfo`` sorts by RFC 6724, which puts AAAA first, so a dual-stack
    name like ``localhost`` would move from IPv4 — where every deployment of this library has
    sent — to IPv6, and a collector bound to ``0.0.0.0:514`` would never see the datagram. UDP
    is unconnected, so that failure is *silent*: ``sendto`` succeeds locally, ``emit`` returns,
    and no counter moves. FR-002 AC-2 requires delivery to a hostname to be unchanged, and
    this is what makes it so while AC-1 still holds. It is a fixed preference, not
    happy-eyeballs, address caching, or a setting — none of which this FR builds.

    This is a module-level seam so tests can substitute a fake socket without network access.

    Args:
      host: The destination host, resolved to choose the family.

    Returns:
      The socket.

    Raises:
      OSError: If the host resolves to nothing, or the socket cannot be created. Both reach
        ``_send_one``'s handler, which counts and announces rather than raising — a
        ``gaierror`` is an ``OSError``, so an unresolvable host fails exactly as an
        unreachable one already did. The empty-result case is raised as one explicitly: CPython
        raises rather than returning ``[]``, but indexing it would produce an ``IndexError``,
        which is *not* an ``OSError`` and would escape that handler into the caller — the one
        thing SPEC-025 says this library may never do.
    """
    families = [entry[0] for entry in socket.getaddrinfo(host, None, type=socket.SOCK_DGRAM)]
    if not families:
        raise socket.gaierror(socket.EAI_NONAME, "resolution returned no address family")
    return socket.socket(
        socket.AF_INET if socket.AF_INET in families else families[0], socket.SOCK_DGRAM
    )


def _json_frame(event: dict[str, object]) -> bytes:
    """Frames an event as its UTF-8 JSON text, the transport's default when a caller gives none.

    Args:
      event: The event.

    Returns:
      Its frame.

    Raises:
      TypeError: If the event is not JSON-serializable, which the encode step turns into a drop.
    """
    return json.dumps(event).encode("utf-8")


class SocketTransport:
    """Frames events and sends them over a TCP or UDP socket, one round at a time (SPEC-074).

    The transport frames each event with the caller's ``frame`` function and puts the bytes on
    the wire. For TCP a single connection is opened and reused, reconnecting on error; for UDP
    each message is an independent datagram. It is the one engine object ``SyslogSink`` and
    socket-mode ``LogstashSink`` run (SPEC-074 FR-005), because it owns the ledger both read, but
    it is **not** a structural ``Sink``: it has ``deliver`` and no ``emit``, so the fork and
    ownership walks still see only the sink that holds it.

    It delivers through SPEC-072's engine: each round sends every remaining frame once and the
    engine resends only the frames that may land, so the worst-case delay (SPEC-027 FR-005) is
    ``max_retries`` waits per run, each interruptible by ``shutdown()``.

    Sends are serialized on a lock (SPEC-028 FR-002). One TCP connection is shared by every
    caller, and ``sendall`` gives no atomicity against a concurrent one: two interleaved calls
    splice their bytes into the stream, which turns octet-counted syslog framing into a sequence
    the receiver cannot resynchronize. The lock is held for one round and never across the
    engine's wait, so a ``close()`` during a backoff returns at once and the next round is refused.

    Attributes:
      log_foundry_counters: The engine's event totals, which :meth:`losses` reports and both
        socket sinks share.
      failed: Messages not confirmed, ``failed_events``; assignable.
      dropped_oversized: UDP datagrams discarded before any send for exceeding
        ``max_datagram_bytes``, counted and announced only when ``emit`` returns — on the engine,
        only when the run settles — so a batch the worker retries counts its oversized frame
        once, and one it abandons not at all.
      max_retries: The floored constructor value, read-only (SPEC-074 FR-006).

    It keeps **no** client buffer (SPEC-036 FR-002): a round puts the bytes on the socket before
    it returns, and no client object outlives it holding data.
    """

    max_retries: ReadOnlyCount[int] = ReadOnlyCount()

    def __init__(
        self,
        host: str,
        port: int,
        *,
        transport: str = "tcp",
        timeout: float = 5.0,
        max_retries: int = 3,
        max_datagram_bytes: int = DEFAULT_MAX_DATAGRAM_BYTES,
        frame: Callable[[dict[str, object]], bytes] | None = None,
        sink_name: str = "SocketTransport",
    ) -> None:
        """Configures the destination, framing and retry bound without opening a socket yet.

        Args:
          host: The destination host.
          port: The destination port, an ``int`` in ``0``–``65535``. Anything else is refused with
            ``TypeError`` (SPEC-066 FR-003): ``2.5`` passed the range check and raised a raw
            ``TypeError`` out of the first ``sendto`` or ``create_connection``. A ``bool`` or other
            ``int`` subclass is read as the ``int`` it holds, and the range is checked on that.
          transport: ``"tcp"`` or ``"udp"``.
          timeout: Seconds allowed for a TCP connection. Refused at construction when it cannot
            bound anything (SPEC-049 FR-002): it reached ``create_connection`` and raised from
            inside every send on every message. **This is a new refusal over UDP**, where the
            value was previously unused and a junk one was harmless — stated rather than
            discovered, since a UDP caller passing one has been silently fine until now.
          max_retries: Reconnect retries per message, an ``int``, floored at zero for the reason
            ``Worker._emit`` floors its own (SPEC-021) — a negative value made no attempt at all
            and abandoned the message without moving ``failed``. Anything else is refused with
            ``TypeError`` (SPEC-066 FR-001); a ``bool`` or ``int`` subclass is read as the ``int``
            it holds.
          max_datagram_bytes: The largest UDP datagram to attempt, defaulting to
            :data:`DEFAULT_MAX_DATAGRAM_BYTES`. Ignored for TCP, which is a stream.
          frame: The caller's per-event frame function (SPEC-074 FR-005), or ``None`` for an
            event's UTF-8 JSON text.
          sink_name: The class name SPEC-069's encode line names: the sink that built this.

        Returns:
          None.

        Raises:
          TypeError: If ``port`` or ``max_retries`` is not an ``int`` (SPEC-066); ``port``'s type
            is judged before its range, so the range check reads the exact ``int``.
          ValueError: If the transport is neither TCP nor UDP, the timeout cannot bound anything,
            the port is outside ``0``–``65535``, or the datagram bound is not positive. The last
            two are SPEC-049's system-frame findings: an out-of-range port raised a raw
            ``OverflowError`` out of every UDP ``sendto`` — not an ``OSError``, so the retry guard
            never saw it — where TCP counted the same value as a resolution failure; and a
            non-positive datagram bound made every UDP frame oversized, so ``emit`` returned having
            delivered nothing. Both are refused over TCP too, where they are inert or already
            counted, for the reason the timeout is. The refused port is named in the message
            through ``sanitize._int_text``, so one too long to convert to a string is named
            ``<int: ~N digits>`` rather than raising CPython's digit-limit ``ValueError`` out of
            the message itself, which named neither ``port`` nor this class (SPEC-066).
        """
        if transport not in ("tcp", "udp"):
            raise ValueError(f"invalid transport {transport!r}; expected 'tcp' or 'udp'")
        port = exact_int(port, "port", "SocketTransport")
        if not 0 <= port <= 65535:
            raise ValueError(f"SocketTransport port must be 0-65535, not {_int_text(port)}")
        self._host = host
        self._port = port
        self._transport = transport
        self._timeout = require_timeout(timeout, "timeout", "SocketTransport")
        store_read_only(
            self, "max_retries", floor_count(max_retries, 0, "max_retries", "SocketTransport")
        )
        self._max_datagram_bytes = require_positive(
            max_datagram_bytes, "max_datagram_bytes", "SocketTransport"
        )
        self._sock: socket.socket | None = None
        self._frame = frame if frame is not None else _json_frame
        self._sink_name = sink_name
        self.log_foundry_counters = SinkCounters()
        self.dropped_oversized = 0
        self.log_foundry_stop_signal: threading.Event | None = None
        self._guard = ClosedGuard(GuardMode.SERIAL)

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

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-074 FR-005).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from the socket propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Frames the round's events and sends every remaining frame once, under the lock.

        A closed transport's round is a refusal carrying the ``SinkDeliveryError`` 1.x's
        ``send_all`` raised, decided before anything is framed, so a batch of nothing encodable
        is still refused by a closed transport (SPEC-069 FR-002). Every event is framed once per
        run (:func:`~log_foundry.sinks._encode.encode_round`), an unframeable one ``DROPPED`` with
        SPEC-069's line naming the sink that built this transport. :meth:`_sendable` drops a UDP
        frame over the datagram limit, its count and line counted and announced only when
        ``emit`` returns — on the engine, only when the run settles. A send that raises runs
        :meth:`_reset`; one :meth:`_retryable_error` accepts makes that frame ``RETRYABLE``, any
        other ``REJECTED``, each line naming the type and errno as 1.x's did, and the round goes
        on to the next frame.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from the socket propagates.
        """
        with self._guard.enter() as section:
            if not section:
                run_context().refuse(
                    SinkDeliveryError(
                        f"SocketTransport delivered none of {len(batch)} message(s): "
                        f"the transport is closed"
                    )
                )
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context = run_context()
            fates = [Outcome.DROPPED] * len(batch)
            pairs = encode_round(self, batch, self._frame, sink=self._sink_name)
            lost = Lost(self)
            for position, message in self._sendable(pairs, context.drops(self)):
                mark_sent(self, [batch[position]])
                called = self._send_one(message)
                if called.error is None:
                    fates[position] = Outcome.ACCEPTED
                    continue
                fates[position] = Outcome.RETRYABLE if called.retryable else Outcome.REJECTED
                kind = f"{type(called.error).__name__} {_diag.errno_of(called.error)}".rstrip()
                lost.add(
                    [batch[position]],
                    when=When.LAST if called.retryable else When.ALWAYS,
                    key=kind,
                    line=raised_line("message", "SocketTransport", kind),
                )
            lost.register()
        return Verdict(outcomes=tuple(fates))

    def _sendable(
        self, messages: list[tuple[int, bytes]], pending: Deferral | _Drops
    ) -> list[tuple[int, bytes]]:
        """Discards UDP datagrams too large to send, before any attempt (FR-007 AC-1).

        A 70 KB event produced ``OSError errno=40`` (EMSGSIZE), which the retry loop treated as
        transient: four sends with backoff, counted as ``failed``, then a raise that sent the
        worker round for three more rounds. The size is knowable *before* sending, and every other
        size-limited sink here drops and counts first, so this one does too. Dropping is right
        rather than raising: the datagram is permanently unsendable on this path, so there is
        nothing for a retry to fix.

        **The drop is counted and announced only when ``emit`` returns** — on the engine,
        ``pending`` is the run context's ``SETTLED`` view, so only when the run settles (SPEC-059
        FR-002, SPEC-074). One unsendable frame is counted once by a run that settles, and not at
        all by one that fails, whose catcher's count is then its only one. The line keeps its
        shape, one per round that dropped anything, carrying the count.

        Args:
          messages: Each frame's position in the round's batch with its bytes, in order.
          pending: The run's ``SETTLED`` view, which holds the drop decided here until the run
            is known to settle.

        Returns:
          Those within the datagram limit. All of them under TCP, which is a stream.

        Raises:
          None.
        """
        if self._transport != "udp":
            return messages
        sendable = [(i, m) for i, m in messages if len(m) <= self._max_datagram_bytes]
        dropped = len(messages) - len(sendable)
        if dropped:
            pending.count("dropped_oversized", dropped)
            pending.announce(
                lambda: _diag.lost(
                    "message",
                    dropped,
                    f"SocketTransport, over the {self._max_datagram_bytes}-byte datagram limit",
                )
            )
        return sendable

    def losses(self) -> SinkLosses:
        """Reports the engine's totals, which ``SyslogSink`` and socket-mode Logstash both read.

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def close(self) -> None:
        """Closes the held socket, if any (FR-005, FR-012).

        Idempotent through the guard (SPEC-076 FR-002): once a close begins every later round is
        refused at once, the one round inside, if any, is waited for and never released under,
        and the release runs once; a second close waits for the first's release, then returns. It never closes the
        socket out from under an in-flight round (SPEC-028 FR-002), and a second close no
        longer repeats the release on the strength of ``_reset`` doing nothing twice. ``_reset`` does not take the lock itself,
        because ``_send_one`` calls it while :meth:`_round` already holds one — the lock is
        deliberately not re-entrant, so that path must stay the only unlocked caller.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._guard.close(self._reset)

    def _send_one(self, message: bytes) -> Called[None]:
        """Sends one frame once (SPEC-074 FR-005); the engine resends it in a later round.

        Args:
          message: The exact bytes to put on the wire.

        Returns:
          The call, with its exception when it raised, after :meth:`_reset`.

        Raises:
          None.
        """
        try:
            if self._transport == "udp":
                self._socket().sendto(message, (self._host, self._port))
            else:
                self._socket().sendall(message)
        except Exception as err:
            self._reset()
            return Called(None, error=err, retryable=is_retryable(self._retryable_error, err))
        return Called(None)

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether another send after this exception may succeed (SPEC-070 FR-001).

        An errno in ``_PERMANENT_ERRNOS`` is not retried (FR-007 AC-3): a re-send of identical
        bytes to the same destination can only earn the same answer, so retrying buys nothing and
        costs the drain thread its backoff. :meth:`_sendable` normally catches the oversized case
        first; this is the backstop for a path MTU smaller than the datagram limit, which no local
        check can know. The set used to be checked inline in :meth:`_send_one`'s loop.

        Args:
          err: The exception the send raised.

        Returns:
          True for an ``OSError`` whose errno is not permanent, False for anything else.

        Raises:
          None. :func:`~log_foundry.sinks._classify.is_retryable` reads a raise here as False.
        """
        return isinstance(err, OSError) and err.errno not in _PERMANENT_ERRNOS

    def _socket(self) -> socket.socket:
        """Returns the held socket, opening one if none is held.

        The UDP address family is resolved here rather than per message, because this is the
        only place a socket is created and the socket outlives every send made through it
        (SPEC-031 FR-002).

        Args:
          None.

        Returns:
          The socket.

        Raises:
          OSError: If the socket cannot be created, resolved or connected.
        """
        if self._sock is None:
            self._sock = (
                _make_udp(self._host) if self._transport == "udp" else _make_tcp(
                    self._host, self._port, self._timeout
                )
            )
        return self._sock

    def _reset(self) -> None:
        """Closes and forgets the held socket, forcing a fresh one on the next attempt.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
