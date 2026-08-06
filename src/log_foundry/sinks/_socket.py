"""Shared raw-socket transport for LogstashSink (socket mode) and SyslogSink (SPEC-009).

Sends pre-framed byte messages over TCP or UDP with bounded reconnect retry. For TCP a single
connection is opened and reused (reconnecting on error); for UDP each message is an independent
datagram. The transport is framing-agnostic — callers hand it the exact bytes to put on the wire
(newline-terminated JSON for Logstash, RFC 5424 / octet-counted frames for Syslog).

Socket creation goes through the module-level ``_make_tcp`` / ``_make_udp`` seams so tests can
substitute a fake socket without any network access.
"""

from __future__ import annotations

import socket
import time

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = ["SocketTransport"]

_BACKOFF_BASE = 0.1  # seconds; delay for retry attempt n is _BACKOFF_BASE * 2**n


def _make_tcp(host: str, port: int, timeout: float) -> socket.socket:
    """Open a connected TCP socket (indirection seam for tests)."""
    return socket.create_connection((host, port), timeout=timeout)


def _make_udp() -> socket.socket:
    """Open an unconnected UDP socket (indirection seam for tests)."""
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


class SocketTransport:
    """Send pre-framed messages over a TCP or UDP socket, reconnecting on error within a bound.

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
        if transport not in ("tcp", "udp"):
            raise ValueError(f"invalid transport {transport!r}; expected 'tcp' or 'udp'")
        self._host = host
        self._port = port
        self._transport = transport
        self._timeout = timeout
        # Floored for the reason ``Worker._emit`` floors its own (SPEC-021): a negative value
        # made no attempt at all and abandoned the message without moving ``failed``.
        self._max_retries = max(max_retries, 0)
        self._sock: socket.socket | None = None
        self.failed = 0

    def send_all(self, messages: list[bytes]) -> None:
        """Send each pre-framed message, reconnecting on error (FR-005, FR-006).

        Raises :class:`~log_foundry.sinks.base.SinkDeliveryError` when **none** of the messages
        reached the socket, so the sinks built on this transport propagate a dead destination to
        the worker instead of reporting success (SPEC-026 FR-001). A partial send does not raise:
        the worker's retry would re-send the messages that already landed.

        An empty call is a no-op, not a total failure — ``delivered == 0`` there means there was
        nothing to deliver.
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
        """Messages abandoned past the reconnect-retry bound (SPEC-026 FR-002). Never raises."""
        return SinkLosses(dropped=0, failed=self.failed)

    def close(self) -> None:
        """Close the held socket, if any; idempotent (FR-005, FR-012)."""
        self._reset()

    # -- internals ----------------------------------------------------------------------

    def _send_one(self, message: bytes) -> bool:
        """Send one message within the retry bound; ``False`` once it is abandoned."""
        for attempt in range(self._max_retries + 1):
            try:
                if self._transport == "udp":
                    self._socket().sendto(message, (self._host, self._port))
                else:
                    self._socket().sendall(message)
                return True
            except OSError as err:
                self._reset()  # force a fresh connection on the next attempt
                if attempt < self._max_retries:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
                self.failed += 1
                # Guarded now (SPEC-029 FR-003): this runs on the worker thread, and the bare
                # ``sys.stderr.write`` this replaced could end delivery for good. ``errno``
                # because an ``OSError`` type name alone does not tell "connection refused" from
                # "host unknown", and the code is an integer from the OS — not caller data.
                _diag.lost(
                    "message",
                    1,
                    f"SocketTransport, {self._max_retries + 1} attempt(s), "
                    f"{type(err).__name__} {_diag.errno_of(err)}".rstrip(),
                )
                return False
        return False  # unreachable: the loop returns on every path (mypy needs the exit)

    def _socket(self) -> socket.socket:
        if self._sock is None:
            self._sock = (
                _make_udp() if self._transport == "udp" else _make_tcp(
                    self._host, self._port, self._timeout
                )
            )
        return self._sock

    def _reset(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
