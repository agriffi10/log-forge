"""Shared raw-socket transport for LogstashSink (socket mode) and SyslogSink (SPEC-009)."""

from __future__ import annotations

import errno
import socket
import threading

from log_foundry import _diag
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["SocketTransport"]

_BACKOFF_BASE = 0.1

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


class SocketTransport:
    """Sends pre-framed messages over a TCP or UDP socket, reconnecting within a bound.

    The transport is framing-agnostic — callers hand it the exact bytes to put on the wire. For
    TCP a single connection is opened and reused, reconnecting on error; for UDP each message is
    an independent datagram.

    The worst-case delay (SPEC-027 FR-005) is ``max_retries`` waits per message, so a
    100-message batch against a dead destination is roughly 70 s of backoff on the single drain
    thread at the defaults. The wait is interruptible, so ``shutdown()`` cuts it short.

    Sends are serialized on a lock (SPEC-028 FR-002). One TCP connection is shared by every
    caller, and ``sendall`` gives no atomicity against a concurrent one: two interleaved calls
    splice their bytes into the stream, which turns octet-counted syslog framing into a sequence
    the receiver cannot resynchronize — it reads the next frame's length from the middle of the
    previous frame's payload and is lost for the life of the connection. That the lock is held
    across the backoff waits is deliberate; they are interruptible, so a ``shutdown()`` releases
    it promptly.

    Attributes:
      failed: Messages abandoned past the reconnect-retry bound.
      dropped_oversized: UDP datagrams discarded before any send for exceeding
        ``max_datagram_bytes``.


    It keeps **no** client buffer (SPEC-036 FR-002): ``send_all`` puts the bytes on the
    socket before it returns, and no client object outlives it holding data.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        transport: str = "tcp",
        timeout: float = 5.0,
        max_retries: int = 3,
        max_datagram_bytes: int = DEFAULT_MAX_DATAGRAM_BYTES,
    ) -> None:
        """Configures the destination and retry bound without opening a socket yet.

        Args:
          host: The destination host.
          port: The destination port.
          transport: ``"tcp"`` or ``"udp"``.
          timeout: Seconds allowed for a TCP connection.
          max_retries: Reconnect retries per message, floored at zero for the reason
            ``Worker._emit`` floors its own (SPEC-021) — a negative value made no attempt at all
            and abandoned the message without moving ``failed``.
          max_datagram_bytes: The largest UDP datagram to attempt, defaulting to
            :data:`DEFAULT_MAX_DATAGRAM_BYTES`. Ignored for TCP, which is a stream.

        Returns:
          None.

        Raises:
          ValueError: If the transport is neither TCP nor UDP.
        """
        if transport not in ("tcp", "udp"):
            raise ValueError(f"invalid transport {transport!r}; expected 'tcp' or 'udp'")
        self._host = host
        self._port = port
        self._transport = transport
        self._timeout = timeout
        self._max_retries = max(max_retries, 0)
        self._max_datagram_bytes = max_datagram_bytes
        self._sock: socket.socket | None = None
        self.failed = 0
        self.dropped_oversized = 0
        self._counter_lock = threading.Lock()
        self.log_foundry_stop_signal: threading.Event | None = None
        self._lock = threading.Lock()
        self._closed = False

    def send_all(self, messages: list[bytes]) -> None:
        """Sends each pre-framed message, reconnecting on error (FR-005, FR-006).

        The lock spans the whole call, not each message: it also guards ``_sock``, which a
        reconnect rebinds, so releasing between messages would let another thread send on a
        socket this one is about to reset (SPEC-028 FR-002).

        A closed transport refuses rather than reconnecting. ``_socket`` opens a connection
        whenever it holds none and ``close`` only drops the one it has, so without this a single
        ``log_foundry.info()`` after ``shutdown()`` would open a TCP connection nothing will ever
        reap — measured, and the same leak ``RabbitMQSink`` had (SPEC-028).

        Args:
          messages: The exact bytes to put on the wire, one call per message.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When none of the messages reached the socket, so the sinks built on
            this transport propagate a dead destination to the worker instead of reporting
            success (SPEC-026 FR-001). A partial send does not raise, because the worker's retry
            would re-send the messages that already landed, and an empty call is a no-op rather
            than a total failure. Also when the transport is already closed.
        """
        if not messages:
            return
        with self._lock:
            if self._closed:
                raise SinkDeliveryError(
                    f"SocketTransport delivered none of {len(messages)} message(s): "
                    f"the transport is closed"
                )
            sendable = self._sendable(messages)
            delivered = 0
            for message in sendable:
                if self._send_one(message):
                    delivered += 1
        if sendable and delivered == 0:
            raise SinkDeliveryError(
                f"SocketTransport delivered none of {len(sendable)} message(s)"
            )

    def _sendable(self, messages: list[bytes]) -> list[bytes]:
        """Discards UDP datagrams too large to send, before any attempt (FR-007 AC-1).

        A 70 KB event produced ``OSError errno=40`` (EMSGSIZE), which the retry loop treated as
        transient: four sends with backoff, counted as ``failed``, then a raise that sent the
        worker round for three more rounds — roughly sixteen futile sends and seconds of backoff
        on the single drain thread, never converging. The size is knowable *before* sending, and
        every other size-limited sink here drops and counts first, so this one does too.

        Dropping is right rather than raising: the datagram is permanently unsendable on this
        path, so there is nothing for a retry to fix, and reporting it as a delivery failure
        would have the worker re-send the rest of the batch alongside it.

        **``dropped_oversized`` inflates in one case, and it is recorded rather than hidden.**
        If a batch holds an oversized frame *and* its sendable remainder then fails totally, the
        emit raises, the worker retries the whole batch, and this filter re-frames and re-drops
        the same event once per attempt — up to four times for one unsendable frame. Everywhere
        else ``dropped`` is an exact count, so the exception matters. Nothing here can fix it:
        the worker owns the retry and hands back the original events, so the sink cannot know it
        has seen them before. ``FirehoseSink._records`` and ``KinesisSink._records`` have the
        same shape and predate this, which is why the fix belongs one level up if it is ever
        taken.

        Args:
          messages: The framed messages, in order.

        Returns:
          Those within the datagram limit. All of them under TCP, which is a stream.

        Raises:
          None.
        """
        if self._transport != "udp":
            return messages
        sendable = [m for m in messages if len(m) <= self._max_datagram_bytes]
        dropped = len(messages) - len(sendable)
        if dropped:
            with self._counter_lock:
                self.dropped_oversized += dropped
            _diag.lost(
                "message",
                dropped,
                f"SocketTransport, over the {self._max_datagram_bytes}-byte datagram limit",
            )
        return sendable

    def losses(self) -> SinkLosses:
        """Reports messages abandoned past the reconnect-retry bound (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def close(self) -> None:
        """Closes the held socket, if any (FR-005, FR-012).

        Idempotent, and takes the send lock so it never closes the socket out from under an
        in-flight ``send_all`` (SPEC-028 FR-002). ``_reset`` does not take the lock itself,
        because ``_send_one`` calls it while ``send_all`` already holds one — the lock is
        deliberately not re-entrant, so that path must stay the only unlocked caller.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        with self._lock:
            self._closed = True
            self._reset()

    def _send_one(self, message: bytes) -> bool:
        """Sends one message within the retry bound.

        The abandonment line is written through ``_diag`` (SPEC-029 FR-003): this runs on the
        worker thread, and the bare ``sys.stderr.write`` it replaced could end delivery for good.
        It carries an ``errno`` because an ``OSError`` type name alone does not tell "connection
        refused" from "host unknown", and the code is an integer from the OS rather than caller
        data.

        An errno in ``_PERMANENT_ERRNOS`` skips the remaining attempts (FR-007 AC-3): a re-send
        of identical bytes to the same destination can only earn the same answer, so retrying
        buys nothing and costs the drain thread its backoff. :meth:`_sendable` normally catches
        the oversized case first; this is the backstop for a path MTU smaller than the datagram
        limit, which no local check can know.

        Args:
          message: The exact bytes to put on the wire.

        Returns:
          True when the message reached the socket, False once it is abandoned.

        Raises:
          None.
        """
        for attempt in range(self._max_retries + 1):
            try:
                if self._transport == "udp":
                    self._socket().sendto(message, (self._host, self._port))
                else:
                    self._socket().sendall(message)
                return True
            except OSError as err:
                self._reset()
                if attempt < self._max_retries and err.errno not in _PERMANENT_ERRNOS:
                    wait(_BACKOFF_BASE * (2**attempt), self.log_foundry_stop_signal)
                    continue
                with self._counter_lock:
                    self.failed += 1
                _diag.lost(
                    "message",
                    1,
                    f"SocketTransport, {self._max_retries + 1} attempt(s), "
                    f"{type(err).__name__} {_diag.errno_of(err)}".rstrip(),
                )
                return False
        return False

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
