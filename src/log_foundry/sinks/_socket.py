"""Shared raw-socket transport for LogstashSink (socket mode) and SyslogSink (SPEC-009)."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading

from log_foundry import _diag
from log_foundry.sinks._retry import wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["SocketTransport"]

_BACKOFF_BASE = 0.1


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


def _make_udp() -> socket.socket:
    """Opens an unconnected UDP socket.

    This is a module-level seam so tests can substitute a fake socket without network access.

    Args:
      None.

    Returns:
      The socket.

    Raises:
      OSError: If the socket cannot be created.
    """
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


class SocketTransport:
    """Sends pre-framed messages over a TCP or UDP socket, reconnecting within a bound.

    The transport is framing-agnostic — callers hand it the exact bytes to put on the wire. For
    TCP a single connection is opened and reused, reconnecting on error; for UDP each message is
    an independent datagram.

    The worst-case delay (SPEC-027 FR-005) is ``max_retries`` waits per message, so a
    100-message batch against a dead destination is roughly 70 s of backoff on the single drain
    thread at the defaults. The wait is interruptible, so ``shutdown()`` cuts it short.

    Attributes:
      failed: Messages abandoned past the reconnect-retry bound.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        transport: str = "tcp",
        timeout: float = 5.0,
        max_retries: int = 3,
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
        self._sock: socket.socket | None = None
        self.failed = 0
        self.stop_signal: threading.Event | None = None

    def send_all(self, messages: list[bytes]) -> None:
        """Sends each pre-framed message, reconnecting on error (FR-005, FR-006).

        Args:
          messages: The exact bytes to put on the wire, one call per message.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When none of the messages reached the socket, so the sinks built on
            this transport propagate a dead destination to the worker instead of reporting
            success (SPEC-026 FR-001). A partial send does not raise, because the worker's retry
            would re-send the messages that already landed, and an empty call is a no-op rather
            than a total failure.
        """
        delivered = 0
        for message in messages:
            if self._send_one(message):
                delivered += 1
        if messages and delivered == 0:
            raise SinkDeliveryError(
                f"SocketTransport delivered none of {len(messages)} message(s)"
            )

    def losses(self) -> SinkLosses:
        """Reports messages abandoned past the reconnect-retry bound (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters.

        Raises:
          None.
        """
        return SinkLosses(dropped=0, failed=self.failed)

    def close(self) -> None:
        """Closes the held socket, if any (FR-005, FR-012).

        Idempotent.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._reset()

    def _send_one(self, message: bytes) -> bool:
        """Sends one message within the retry bound.

        The abandonment line is written through ``_diag`` (SPEC-029 FR-003): this runs on the
        worker thread, and the bare ``sys.stderr.write`` it replaced could end delivery for good.
        It carries an ``errno`` because an ``OSError`` type name alone does not tell "connection
        refused" from "host unknown", and the code is an integer from the OS rather than caller
        data.

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
                if attempt < self._max_retries:
                    wait(_BACKOFF_BASE * (2**attempt), self.stop_signal)
                    continue
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

        Args:
          None.

        Returns:
          The socket.

        Raises:
          OSError: If the socket cannot be created or connected.
        """
        if self._sock is None:
            self._sock = (
                _make_udp() if self._transport == "udp" else _make_tcp(
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
