"""LogstashSink — JSON lines to Logstash over HTTP or a raw TCP/UDP socket (arch §8, SPEC-009)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading

from log_foundry.sinks._socket import SocketTransport
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import HTTPSink

__all__ = ["LogstashSink"]


class LogstashSink:
    """A :class:`~log_foundry.sinks.base.Sink` that ships JSON lines to Logstash (FR-005).

    Two mutually-exclusive modes are chosen at construction: with a URL the batch goes as JSON
    lines over HTTP, reusing the ``HTTPSink`` core, and with a host and port each event goes as
    one newline-terminated line over a raw TCP or UDP socket, reusing
    :class:`~log_foundry.sinks._socket.SocketTransport`. Either backend handles its own bounded
    retry and raises on total failure of its own accord, so ``emit`` needs no rule of its own.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        transport: str = "tcp",
        timeout: float = 5.0,
        max_retries: int = 3,
        **http_kwargs: object,
    ) -> None:
        """Selects and builds exactly one backend.

        Args:
          url: The HTTP endpoint, selecting HTTP mode.
          host: The destination host, selecting socket mode.
          port: The destination port, selecting socket mode.
          transport: ``"tcp"`` or ``"udp"``, in socket mode.
          timeout: Seconds allowed per request or connection.
          max_retries: Retries the chosen backend makes.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink` in HTTP mode.

        Returns:
          None.

        Raises:
          ValueError: If neither a URL nor a host and port were given.
        """
        if url is not None:
            self._http: HTTPSink | None = HTTPSink(
                url, body_format="ndjson", timeout=timeout, max_retries=max_retries,
                **http_kwargs,  # type: ignore[arg-type]
            )
            self._socket: SocketTransport | None = None
        elif host is not None and port is not None:
            self._http = None
            self._socket = SocketTransport(
                host, port, transport=transport, timeout=timeout, max_retries=max_retries
            )
        else:
            raise ValueError("LogstashSink requires either url= (HTTP) or host= + port= (socket)")
        self._stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Sends the batch over the configured backend (FR-005).

        The assertion narrows the type for mypy rather than checking at runtime: the constructor
        guarantees exactly one backend is set, and the branch above covers the other.

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If the backend delivered nothing.
        """
        if not batch:
            return
        if self._http is not None:
            self._http.emit(batch)
        else:
            assert self._socket is not None  # noqa: S101
            frames = [(json.dumps(event) + "\n").encode("utf-8") for event in batch]
            self._socket.send_all(frames)

    def close(self) -> None:
        """Closes whichever backend is held (FR-005).

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the backend raises on close.
        """
        if self._http is not None:
            self._http.close()
        elif self._socket is not None:
            self._socket.close()

    @property
    def stop_signal(self) -> threading.Event | None:
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

    @stop_signal.setter
    def stop_signal(self, signal: threading.Event | None) -> None:
        """Forwards the stop signal to the active backend.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        self._stop_signal = signal
        if self._http is not None:
            self._http.stop_signal = signal
        elif self._socket is not None:
            self._socket.stop_signal = signal

    @property
    def failed(self) -> int:
        """Requests or messages abandoned past the retry bound, from the active backend.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return self._http.failed if self._http is not None else (
            self._socket.failed if self._socket is not None else 0
        )

    def losses(self) -> SinkLosses:
        """Delegates to whichever backend is held (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The active backend's counters. The zeroed fallback is unreachable, since the
          constructor sets exactly one backend.

        Raises:
          None.
        """
        if self._http is not None:
            return self._http.losses()
        if self._socket is not None:
            return self._socket.losses()
        return SinkLosses(dropped=0, failed=0)
