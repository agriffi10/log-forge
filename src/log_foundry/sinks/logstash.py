"""LogstashSink — JSON lines to Logstash over HTTP or a raw TCP/UDP socket (arch §8, SPEC-009)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading

from log_foundry import _lifecycle
from log_foundry.sinks._socket import DEFAULT_MAX_DATAGRAM_BYTES, SocketTransport
from log_foundry.sinks.base import SinkLosses
from log_foundry.sinks.http import HTTPSink

__all__ = ["LogstashSink"]

_BODY_FORMATS = frozenset({"json_array", "ndjson"})
"""The two HTTP wire forms, validated rather than passed through.

``HTTPSink._body`` branches on ``== "json_array"`` and treats everything else as NDJSON, so a
typo -- ``"json-array"`` -- would silently ship the exact body FR-003 exists to stop shipping,
against a destination that cannot parse it. A parameter documented as taking one of two values
refuses the third.
"""


class LogstashSink:
    """A :class:`~log_foundry.sinks.base.Sink` that ships JSON lines to Logstash (FR-005).

    Two mutually-exclusive modes are chosen at construction: with a URL the batch goes over HTTP,
    reusing the ``HTTPSink`` core, and with a host and port each event goes as one
    newline-terminated line over a raw TCP or UDP socket, reusing
    :class:`~log_foundry.sinks._socket.SocketTransport`. Either backend handles its own bounded
    retry and raises on total failure of its own accord, so ``emit`` needs no rule of its own.

    **What the Logstash side must be configured as** (SPEC-041 FR-003). In HTTP mode the default
    ``body_format="json_array"`` posts a JSON array as ``application/json``, which a **stock**
    ``http`` input parses into one event per element with no configuration at all. That is a
    change: this sink used to post NDJSON as ``application/x-ndjson``, which the ``http`` input's
    default ``additional_codecs`` map (``{"application/json" => "json"}``) does not cover, so the
    body fell through to the ``plain`` codec. Measured against Logstash 8.15 — a batch of three
    arrived as **one** event with the whole payload as text in ``message`` and every structured
    field gone.

    ``body_format="ndjson"`` restores the old wire form, and it is not merely legacy: an input
    configured ``additional_codecs => {"application/x-ndjson" => "json_lines"}`` -- the documented
    workaround for the behaviour above -- needs it. That setting **replaces** the default map
    rather than merging with it, so such an input parses NDJSON correctly and a JSON array
    incorrectly. The two configurations are mutually exclusive unless the input lists both, which
    is why the wire form is a parameter rather than a silent change.

    Socket mode is unaffected: it is newline-delimited either way, and a ``tcp``/``udp`` input
    uses the ``line``/``json_lines`` codec rather than ``additional_codecs``.

    In socket mode both IPv4 and IPv6 destinations are supported, over either transport
    (SPEC-031 FR-002): the UDP socket's address family is resolved from ``host`` rather than
    assumed, which is what an unconditional ``AF_INET`` used to make impossible.

    **A hostname that resolves to both families goes to IPv4**, because that is where every
    release before this one sent and moving it silently would strand a collector bound to
    ``0.0.0.0``. The consequence is the mirror case, stated because nothing else states it: a
    dual-stack *name* whose collector listens on IPv6 only will not be reached over UDP, and
    since UDP is unconnected that failure is silent — no exception, no counter. Give the IPv6
    literal, or a name with no ``A`` record, to select IPv6 for such a destination. TCP is
    unaffected either way; ``create_connection`` tries each candidate in turn.

    It takes **no** transport lock (SPEC-028 FR-002) of its own: whichever backend it built owns
    that decision — ``SocketTransport`` locks its sends, ``HTTPSink`` holds no transport to
    guard. The post-close rule follows the same split (SPEC-032 FR-004), and the two modes
    genuinely differ: in socket mode a batch after ``close()`` is refused with
    ``SinkDeliveryError`` and reopens nothing, while in HTTP mode ``close()`` released nothing
    and the batch still ships. Both are the backend's answer, correctly, rather than one this
    class invents on top.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` hands its bytes to the
    socket before it returns, and no client object outlives it holding data. What the *network*
    then does with a datagram is not something a flush could hurry.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        transport: str = "tcp",
        body_format: str = "json_array",
        timeout: float = 5.0,
        max_retries: int = 3,
        max_datagram_bytes: int = DEFAULT_MAX_DATAGRAM_BYTES,
        **http_kwargs: object,
    ) -> None:
        """Selects and builds exactly one backend.

        Args:
          url: The HTTP endpoint, selecting HTTP mode.
          host: The destination host, selecting socket mode.
          port: The destination port, selecting socket mode.
          transport: ``"tcp"`` or ``"udp"``, in socket mode.
          body_format: ``"json_array"`` (the default) or ``"ndjson"``. It selects the HTTP wire
            form and has **no effect** in socket mode, which is always newline-delimited — but it
            is *validated* in both, because a value this class silently ignores in one mode and
            silently misreads in the other is worse than a refusal. See the class docstring for
            what each one requires of the Logstash side; ``"ndjson"`` is the escape hatch for an
            input already configured with ``additional_codecs``.
          timeout: Seconds allowed per request or connection.
          max_retries: Retries the chosen backend makes.
          max_datagram_bytes: In UDP socket mode, the largest datagram to attempt; a frame over
            it is dropped and counted rather than sent, retried and abandoned (SPEC-038 FR-007).
            Ignored in HTTP mode and over TCP, which is a stream.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink` in HTTP mode.

        Returns:
          None.

        Raises:
          ValueError: If neither a URL nor a host and port were given, or if ``body_format`` is
            neither ``"json_array"`` nor ``"ndjson"``.
        """
        if body_format not in _BODY_FORMATS:
            raise ValueError(
                f"LogstashSink body_format must be one of {sorted(_BODY_FORMATS)}, "
                f"not {body_format!r}"
            )
        if url is not None:
            self._http: HTTPSink | None = HTTPSink(
                url, body_format=body_format, timeout=timeout, max_retries=max_retries,
                **http_kwargs,  # type: ignore[arg-type]
            )
            self._socket: SocketTransport | None = None
        elif host is not None and port is not None:
            self._http = None
            self._socket = SocketTransport(
                host,
                port,
                transport=transport,
                timeout=timeout,
                max_retries=max_retries,
                max_datagram_bytes=max_datagram_bytes,
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

        The two branches are deliberately not symmetric. The HTTP backend is a ``Sink`` and goes
        through ``_lifecycle.release``, so a forked child cannot release one it inherited
        (SPEC-042 FR-002); the socket is a ``SocketTransport`` this sink built and owns
        outright, which no sink-ownership record describes.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the backend raises on close.
        """
        if self._http is not None:
            _lifecycle.release(self._http, owner=self)
        elif self._socket is not None:
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
            self._http.log_foundry_stop_signal = signal
        elif self._socket is not None:
            self._socket.log_foundry_stop_signal = signal

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
