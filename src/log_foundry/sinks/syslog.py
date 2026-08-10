"""SyslogSink — RFC 5424 syslog frames over UDP or TCP (arch §8, SPEC-009)."""

from __future__ import annotations

import json
import os
import socket
from typing import TYPE_CHECKING

from log_foundry.sinks._socket import DEFAULT_MAX_DATAGRAM_BYTES, SocketTransport

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["SyslogSink"]

_SEVERITY = {"DEBUG": 7, "INFO": 6, "NOTICE": 5, "WARNING": 4, "ERROR": 3, "CRITICAL": 2}
_DEFAULT_SEVERITY = 5

_FACILITY = {
    "kern": 0, "user": 1, "mail": 2, "daemon": 3, "auth": 4, "syslog": 5, "lpr": 6, "news": 7,
    "uucp": 8, "cron": 9, "authpriv": 10, "ftp": 11,
    "local0": 16, "local1": 17, "local2": 18, "local3": 19,
    "local4": 20, "local5": 21, "local6": 22, "local7": 23,
}


class SyslogSink:
    """A :class:`~log_foundry.sinks.base.Sink` emitting RFC 5424 frames over UDP or TCP (FR-006).

    Each event becomes an RFC 5424 message whose ``PRI`` is derived from a configurable facility
    and a severity mapped from the event's level. UDP sends one datagram per event, TCP uses
    octet-counted framing (RFC 6587), and the whole sink is dependency-free.

    Both IPv4 and IPv6 destinations are supported, on either transport (SPEC-031 FR-002): the
    UDP socket's address family is resolved from ``host`` rather than assumed, which is what an
    unconditional ``AF_INET`` used to make impossible.

    **A hostname that resolves to both families goes to IPv4**, because that is where every
    release before this one sent and moving it silently would strand a collector bound to
    ``0.0.0.0``. The consequence is the mirror case, stated because nothing else states it: a
    dual-stack *name* whose collector listens on IPv6 only will not be reached over UDP, and
    since UDP is unconnected that failure is silent — no exception, no counter. Give the IPv6
    literal, or a name with no ``A`` record, to select IPv6 for such a destination. TCP is
    unaffected either way; ``create_connection`` tries each candidate in turn.

    It takes **no** transport lock (SPEC-028 FR-002) of its own: the socket it holds is a
    :class:`~log_foundry.sinks._socket.SocketTransport`, which locks its own sends. Its
    post-close refusal comes from there too (SPEC-032 FR-004) — a batch emitted after
    ``close()`` reaches ``send_all`` and is refused with ``SinkDeliveryError`` without the
    socket being reopened, so a guard here would only duplicate one that already holds.
    """

    def __init__(
        self,
        host: str,
        port: int = 514,
        *,
        transport: str = "udp",
        facility: str = "user",
        app_name: str = "log-foundry",
        timeout: float = 5.0,
        max_retries: int = 3,
        max_datagram_bytes: int = DEFAULT_MAX_DATAGRAM_BYTES,
    ) -> None:
        """Configures the destination, framing and message identity.

        Args:
          host: The syslog host.
          port: The syslog port.
          transport: ``"udp"`` or ``"tcp"``, which also selects the framing.
          facility: An RFC 5424 facility keyword.
          app_name: The ``APP-NAME`` field of every message.
          timeout: Seconds allowed for a TCP connection.
          max_retries: Reconnect retries per message.
          max_datagram_bytes: The largest UDP datagram to attempt; a frame over it is dropped
            and counted rather than sent, retried and abandoned (SPEC-038 FR-007). Ignored for
            TCP, which is a stream.

        Returns:
          None.

        Raises:
          ValueError: If the facility is not one of the supported keywords.
        """
        if facility not in _FACILITY:
            raise ValueError(f"invalid facility {facility!r}; expected one of {sorted(_FACILITY)}")
        self._facility = _FACILITY[facility]
        self._app_name = app_name
        self._transport = transport
        self._hostname = socket.gethostname() or "-"
        self._procid = str(os.getpid())
        self._socket = SocketTransport(
            host,
            port,
            transport=transport,
            timeout=timeout,
            max_retries=max_retries,
            max_datagram_bytes=max_datagram_bytes,
        )
        self._stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Frames each event as RFC 5424 and sends it over the socket (FR-006).

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If no frame reached the socket.
        """
        if not batch:
            return
        self._socket.send_all([self._frame(event) for event in batch])

    def close(self) -> None:
        """Closes the underlying socket (FR-006).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self._socket.close()

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        """The worker's shutdown event, forwarded to whatever actually holds the retry loop.

        The worker sets this on the configured sink (SPEC-027 FR-002), and a wrapper is not
        where the waiting happens. Without the forward the attribute is set on an object that
        never waits, and the backoff one level down stays uninterruptible — which is the whole
        defect, moved rather than fixed.

        Args:
          None.

        Returns:
          The stop signal, or ``None`` if none was offered.

        Raises:
          None.
        """
        return self._stop_signal

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, signal: threading.Event | None) -> None:
        """Forwards the stop signal to the socket transport.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        self._stop_signal = signal
        self._socket.log_foundry_stop_signal = signal

    @property
    def failed(self) -> int:
        """Messages abandoned past the retry bound.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return self._socket.failed

    def losses(self) -> SinkLosses:
        """Delegates to the socket transport (SPEC-026 FR-002).

        The transport raises on total failure and :meth:`emit` hands it the whole batch in one
        call, so a dead syslog destination now reaches ``health().failed_batches`` — the reading
        SPEC-026 was written from, where ``flush()`` returned True and every frame was lost.

        Args:
          None.

        Returns:
          The transport's counters.

        Raises:
          None.
        """
        return self._socket.losses()

    def _frame(self, event: dict[str, object]) -> bytes:
        """Builds one RFC 5424 message, octet-counted when the transport is TCP.

        Args:
          event: The event to frame.

        Returns:
          The exact bytes to put on the wire.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``sanitize`` prevents.
        """
        level = event.get("level")
        severity = _SEVERITY.get(level.upper(), _DEFAULT_SEVERITY) if isinstance(level, str) else (
            _DEFAULT_SEVERITY
        )
        pri = self._facility * 8 + severity
        timestamp = event.get("timestamp")
        ts = timestamp if isinstance(timestamp, str) else "-"
        msg = json.dumps(event)
        frame = f"<{pri}>1 {ts} {self._hostname} {self._app_name} {self._procid} - - {msg}"
        data = frame.encode("utf-8")
        if self._transport == "tcp":
            return f"{len(data)} ".encode("ascii") + data
        return data
