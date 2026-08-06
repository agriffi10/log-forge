"""SyslogSink — RFC 5424 syslog frames over UDP or TCP (arch §8, SPEC-009).

Formats each event as an RFC 5424 message (``<PRI>1 TIMESTAMP HOST APP PROCID MSGID SD MSG``) with
``PRI`` derived from a configurable facility and a severity mapped from the event ``level``. UDP
sends one datagram per event; TCP uses octet-counted framing (RFC 6587). Dependency-free (stdlib
``socket``, via :class:`~log_foundry.sinks._socket.SocketTransport`).
"""

from __future__ import annotations

import json
import os
import socket
from typing import TYPE_CHECKING

from log_foundry.sinks._socket import SocketTransport

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses

__all__ = ["SyslogSink"]

# RFC 5424 numeric severities; log-foundry levels mapped onto them (unknown level -> notice/5).
_SEVERITY = {"DEBUG": 7, "INFO": 6, "NOTICE": 5, "WARNING": 4, "ERROR": 3, "CRITICAL": 2}
_DEFAULT_SEVERITY = 5

# A subset of RFC 5424 facility keywords -> numeric code.
_FACILITY = {
    "kern": 0, "user": 1, "mail": 2, "daemon": 3, "auth": 4, "syslog": 5, "lpr": 6, "news": 7,
    "uucp": 8, "cron": 9, "authpriv": 10, "ftp": 11,
    "local0": 16, "local1": 17, "local2": 18, "local3": 19,
    "local4": 20, "local5": 21, "local6": 22, "local7": 23,
}


class SyslogSink:
    """A :class:`~log_foundry.sinks.base.Sink` that emits RFC 5424 frames over UDP/TCP (FR-006)."""

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
    ) -> None:
        if facility not in _FACILITY:
            raise ValueError(f"invalid facility {facility!r}; expected one of {sorted(_FACILITY)}")
        self._facility = _FACILITY[facility]
        self._app_name = app_name
        self._transport = transport
        self._hostname = socket.gethostname() or "-"
        self._procid = str(os.getpid())
        self._socket = SocketTransport(
            host, port, transport=transport, timeout=timeout, max_retries=max_retries
        )
        self._stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Frame each event as RFC 5424 and send it over the socket (FR-006)."""
        if not batch:
            return
        self._socket.send_all([self._frame(event) for event in batch])

    def close(self) -> None:
        """Close the underlying socket (FR-006)."""
        self._socket.close()

    @property
    def stop_signal(self) -> threading.Event | None:
        """The worker's shutdown event, forwarded to whatever actually holds the retry loop.

        The worker sets this on the *configured* sink (SPEC-027 FR-002), and a wrapper is not
        where the waiting happens. Without the forward the attribute is set on an object that
        never waits, and the backoff one level down stays uninterruptible — which is the whole
        defect, moved rather than fixed.
        """
        return self._stop_signal

    @stop_signal.setter
    def stop_signal(self, signal: threading.Event | None) -> None:
        self._stop_signal = signal
        self._socket.stop_signal = signal

    @property
    def failed(self) -> int:
        """Messages abandoned past the retry bound."""
        return self._socket.failed

    def losses(self) -> SinkLosses:
        """Delegate to the socket transport (SPEC-026 FR-002). Never raises.

        The transport raises on total failure, and ``emit`` hands it the whole batch in one
        call, so a dead syslog destination now reaches ``health().failed_batches`` — the reading
        SPEC-026 was written from, where ``flush()`` returned ``True`` and every frame was lost.
        """
        return self._socket.losses()

    # -- internals ----------------------------------------------------------------------

    def _frame(self, event: dict[str, object]) -> bytes:
        """Build one RFC 5424 message; TCP gets octet-counted framing (RFC 6587)."""
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
            return f"{len(data)} ".encode("ascii") + data  # RFC 6587 octet counting
        return data
