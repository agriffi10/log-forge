"""SyslogSink — RFC 5424 syslog frames over UDP or TCP (arch §8, SPEC-009)."""

from __future__ import annotations

import json
import os
import socket
from typing import TYPE_CHECKING

from log_foundry.sinks._args import ReadOnlyCount, store_read_only
from log_foundry.sinks._engine import emit_via_deliver
from log_foundry.sinks._socket import DEFAULT_MAX_DATAGRAM_BYTES, SocketTransport

if TYPE_CHECKING:
    import threading

    from log_foundry.sinks.base import SinkLosses
    from log_foundry.sinks.verdict import Verdict

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
    ``close()`` reaches the transport's round, which refuses it with ``SinkDeliveryError``
    without the socket being reopened, so a guard here would only duplicate one that already
    holds. It delivers through SPEC-072's engine by forwarding :meth:`deliver` to the transport
    (SPEC-074 FR-005), one index space and nothing to map, and shares the transport's counters.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` hands its bytes to the
    socket before it returns, and no client object outlives it holding data. What the *network*
    then does with a datagram is not something a flush could hurry.

    Attributes:
      log_foundry_counters: The transport's counters object, so the engine counts into the one
        ledger both socket sinks read.
      log_foundry_stop_signal: The shutdown event, read by the engine statically (SPEC-074).
      max_retries: The floored constructor value, read-only (SPEC-074 FR-006).
    """

    max_retries: ReadOnlyCount[int] = ReadOnlyCount()

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
          port: The syslog port, an ``int`` in ``0``–``65535``, judged by the
            :class:`~log_foundry.sinks._socket.SocketTransport` this sink builds (SPEC-066
            FR-003).
          transport: ``"udp"`` or ``"tcp"``, which also selects the framing.
          facility: An RFC 5424 facility keyword.
          app_name: The ``APP-NAME`` field of every message. Whitespace and the empty string
            are refused (SPEC-049 FR-004): RFC 5424's header is space-delimited and this value
            is interpolated into it raw, so either shifts ``PROCID``, ``MSGID`` and
            ``STRUCTURED-DATA`` for every message the sink ever sends — corruption no counter
            sees, since the frame still arrives.
          timeout: Seconds allowed for a TCP connection.
          max_retries: Reconnect retries per message, an ``int`` floored at zero; anything else
            is refused with ``TypeError`` by the transport (SPEC-066 FR-001).
          max_datagram_bytes: The largest UDP datagram to attempt; a frame over it is dropped
            and counted rather than sent, retried and abandoned (SPEC-038 FR-007). Ignored for
            TCP, which is a stream.

        Returns:
          None.

        Raises:
          TypeError: If ``port`` or ``max_retries`` is not an ``int`` (SPEC-066), refused by the
            transport this sink builds.
          ValueError: If the facility is not one of the supported keywords, or ``app_name`` is
            empty or contains whitespace, or the transport refuses ``port``, ``timeout`` or
            ``max_datagram_bytes``.
        """
        if facility not in _FACILITY:
            raise ValueError(f"invalid facility {facility!r}; expected one of {sorted(_FACILITY)}")
        if not app_name or any(char.isspace() for char in app_name):
            raise ValueError(
                f"SyslogSink app_name must be non-empty and contain no whitespace, "
                f"not {app_name!r}"
            )
        self._facility = _FACILITY[facility]
        self._app_name = app_name
        self._transport = transport
        self._hostname = socket.gethostname() or "-"
        self._socket = SocketTransport(
            host,
            port,
            transport=transport,
            timeout=timeout,
            max_retries=max_retries,
            max_datagram_bytes=max_datagram_bytes,
            frame=self._frame,
            sink_name=type(self).__name__,
        )
        store_read_only(self, "max_retries", self._socket.max_retries)
        self.log_foundry_counters = self._socket.log_foundry_counters
        self.log_foundry_stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Delivers the batch through the engine inline, keeping the 1.x ``emit`` contract.

        A UDP frame over the datagram limit, and an event whose frame cannot be built (SPEC-069
        FR-001), are counted and announced only when ``emit`` returns — on the engine, only when
        the run settles — on the transport's ledger, which this sink reports.

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If no frame reached the socket, with the engine's message and
            nothing counted twice; when the transport is closed, its 1.x error by identity.
        """
        emit_via_deliver(self, batch)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Forwards the round to the socket transport: one index space, nothing to map.

        Args:
          batch: The events.

        Returns:
          The transport's verdict.

        Raises:
          None. A ``BaseException`` from the socket propagates.
        """
        return self._socket.deliver(batch)

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

    @failed.setter
    def failed(self, value: int) -> None:
        """Sets the transport's count, the one ledger this sink reads (SPEC-074 FR-006).

        Args:
          value: The new count.

        Returns:
          None.

        Raises:
          None.
        """
        self._socket.failed = value

    def losses(self) -> SinkLosses:
        """Reports the counters this sink shares with its socket transport (SPEC-026 FR-002).

        A run that delivers nothing fails, so a dead syslog destination reaches
        ``health().failed_batches`` — the reading SPEC-026 was written from, where ``flush()``
        returned True and every frame was lost.

        Args:
          None.

        Returns:
          The transport's counters.

        Raises:
          None.
        """
        return self.log_foundry_counters.losses()

    def _frame(self, event: dict[str, object]) -> bytes:
        """Builds one RFC 5424 message, octet-counted when the transport is TCP.

        The PROCID is read here, per message, rather than once at construction (SPEC-069
        FR-005): a child forked after the sink was built — a prefork server that configures once
        in the master — sent every message stamped with its parent's process id. The read costs
        a small fraction of the ``json.dumps`` beside it. The hostname is still read once, since
        a fork does not change it.

        Args:
          event: The event to frame.

        Returns:
          The exact bytes to put on the wire.

        Raises:
          TypeError: If the event is not JSON-serializable, which ``encode_round`` turns into a
            drop of this one event. Neither source of events produces one: assembly coerces every
            value (``sanitize``), and a ``TransformSink`` checks and coerces what its function
            returns (SPEC-058 FR-001).
        """
        level = event.get("level")
        severity = _SEVERITY.get(level.upper(), _DEFAULT_SEVERITY) if isinstance(level, str) else (
            _DEFAULT_SEVERITY
        )
        pri = self._facility * 8 + severity
        timestamp = event.get("timestamp")
        ts = timestamp if isinstance(timestamp, str) else "-"
        msg = json.dumps(event)
        procid = os.getpid()
        frame = f"<{pri}>1 {ts} {self._hostname} {self._app_name} {procid} - - {msg}"
        data = frame.encode("utf-8")
        if self._transport == "tcp":
            return f"{len(data)} ".encode("ascii") + data
        return data
