"""SentrySink — route error-level events to Sentry (arch §8, SPEC-009)."""

from __future__ import annotations

import json
import threading
import uuid
from typing import Any
from urllib.parse import urlparse

from log_foundry import _diag, _lifecycle
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.http import HTTPSink

__all__ = ["SentrySink"]

_LEVEL_RANK = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
_SENTRY_LEVEL = {
    "DEBUG": "debug", "INFO": "info", "WARNING": "warning", "ERROR": "error", "CRITICAL": "fatal",
}


class SentrySink:
    """A :class:`~log_foundry.sinks.base.Sink` that captures qualifying events to Sentry (FR-011).

    It uses the ``sentry-sdk`` when the optional extra is installed, imported lazily inside the
    sink so importing this module never requires it. Without the SDK it falls back to POSTing a
    Sentry envelope over HTTP to the DSN's ingest URL. Only events at or above the configured
    minimum level are sent.

    Attributes:
      sent: Events captured or sent to Sentry.
      skipped: Events below the minimum level, or without a usable level, that were not sent.
      transport_errors: Events whose send raised something other than an already-counted
        abandonment — an SDK fault, or a response error ``HTTPSink`` does not retry.

    The driver requirement satisfied (SPEC-028 FR-002): this sink takes **no** transport
    lock. This sink owns no transport: it delegates to a ``sentry_sdk`` built for capture from any
    thread, or to an ``HTTPSink`` that builds a fresh request per call and rebinds nothing.
    Its counters take the counter lock like every other sink's.

    It also **adds no post-close guard** (SPEC-032 FR-003), for the same reason: neither backend
    holds anything ``close()`` releases, so a batch emitted afterwards still reaches Sentry.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        min_level: str = "ERROR",
        client: Any = None,
        opener: Any = None,
        max_retries: int = 3,
    ) -> None:
        """Selects the SDK or the HTTP-envelope fallback and sets the level floor.

        Args:
          dsn: The Sentry DSN. It is required for the fallback, which needs it to know where to
            POST.
          min_level: The lowest level worth sending.
          client: A ``sentry_sdk``-shaped object to use instead of importing one.
          opener: A ``urlopen``-shaped callable for the fallback, for tests.
          max_retries: Retries the fallback's HTTP transport makes.

        Returns:
          None.

        Raises:
          ValueError: If no SDK is available and no DSN was given.
        """
        self._dsn = dsn
        self._min_rank = _LEVEL_RANK.get(min_level.upper(), _LEVEL_RANK["ERROR"])
        self.sent = 0
        self.skipped = 0
        self.transport_errors = 0
        self._counter_lock = threading.Lock()
        self.client = client if client is not None else _import_sdk()
        self._http: HTTPSink | None = None
        self._auth_header = ""
        if self.client is None:
            if dsn is None:
                raise ValueError(
                    "SentrySink without sentry-sdk requires a dsn for the HTTP-envelope fallback"
                )
            ingest_url, self._auth_header = _parse_dsn(dsn)
            self._http = HTTPSink(ingest_url, opener=opener, max_retries=max_retries)
        self._stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Captures each qualifying event via the SDK or the HTTP fallback (FR-011).

        One envelope goes per event, so an abandoned request is a per-event outcome: it is
        caught, counted, and only re-raised when the whole batch got through to nothing (SPEC-026
        FR-001). Letting the first failure propagate would hand the worker a batch whose earlier
        events Sentry had already accepted, and the retry would duplicate them.

        Args:
          batch: The events to consider.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If every qualifying event failed to land. An event below the
            minimum level is skipped rather than lost, so a batch of nothing but skipped events
            is a successful emit — there was never anything to deliver.
        """
        attempted = delivered = 0
        for event in batch:
            if not self._qualifies(event):
                with self._counter_lock:
                    self.skipped += 1
                continue
            attempted += 1
            if not self._capture(event):
                continue
            with self._counter_lock:
                self.sent += 1
            delivered += 1
        if attempted and not delivered:
            raise SinkDeliveryError(
                f"SentrySink delivered none of {attempted} qualifying event(s)"
            )

    def flush(self) -> None:
        """Pushes the Sentry SDK's own transport queue, which nothing else here ever does.

        SPEC-036 FR-002, and a case SPEC-042's measured roster of five did not reach: that roster
        was derived from what a *refused close* costs, and :meth:`close` releases nothing here, so
        this sink never appeared in it. ``capture_event`` hands to the SDK's **background
        transport** and returns, so without this hook an event accepted by Sentry's client was
        unreachable through ``log_foundry.flush()`` and went out only when the SDK's own timer or
        interpreter exit got to it.

        Only the injected-or-imported SDK client has a queue. The ``urllib`` fallback posts an
        envelope per event and holds nothing, so with no client this is correctly a no-op.

        ``Client.flush`` is probed by name, as every optional member the library calls on an object
        it does not own is: a stand-in ``client=`` satisfying only ``capture_event`` stays valid,
        which is what the injected-client tests use.

        **It cannot report a failure, and that is the SDK's shape rather than a choice here.**
        ``sentry_sdk.Client.flush`` logs a warning and returns ``None`` when its own timeout
        expires, so a queue the SDK just gave up on is indistinguishable from one it drained, and
        ``log_foundry.flush()`` reports success either way. Recorded rather than worked around: the
        alternatives are reading a private attribute or timing the call, and both would invent a
        verdict the SDK declines to give.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the SDK raises while flushing.
        """
        if self.client is None:
            return
        flush = getattr(self.client, "flush", None)
        if callable(flush):
            flush()

    def close(self) -> None:
        """Forwards to the HTTP fallback, whose own close releases nothing (FR-012).

        Idempotent, and it releases nothing — which is why the class docstring's post-close claim
        holds despite this method calling a ``close()``. ``HTTPSink.close`` is a documented no-op,
        since ``urllib`` builds a fresh connection per request; the forward exists so a future
        ``HTTPSink`` that *did* hold a pool would be released here rather than leaked. It goes
        through ``_lifecycle.release`` for that same future: the day the forward releases
        something is the day a forked child must not perform it (SPEC-042 FR-002).

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        if self._http is not None:
            _lifecycle.release(self._http, owner=self)

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
        """Forwards the stop signal to the HTTP fallback, which is what waits.

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

    @property
    def failed(self) -> int:
        """Requests abandoned past the retry bound, on the HTTP fallback only.

        Args:
          None.

        Returns:
          The count.

        Raises:
          None.
        """
        return self._http.failed if self._http is not None else 0

    def losses(self) -> SinkLosses:
        """Reports envelopes abandoned past the retry bound (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters. ``skipped`` is deliberately not reported as ``dropped``: an event below
          the minimum level was never meant for Sentry, and reporting a configured filter as
          loss would make the alert idiom fire on every INFO log. What is reported is an
          abandoned envelope and an event whose send raised, on either transport; delivery the
          SDK accepts and then loses internally is the SDK's to report.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=0, failed=self.failed + self.transport_errors)

    def _capture(self, event: dict[str, object]) -> bool:
        """Sends one event by whichever transport is configured.

        One guard covers both branches, catching ``Exception`` rather than an enumerated set.
        Anything escaping here propagates mid-batch and hands the worker a batch Sentry has
        already accepted the earlier events of, and the retry duplicates them. Enumerating was
        tried and was wrong twice over: the SDK branch is third-party code that can raise
        anything, and ``HTTPSink._send`` catches ``(URLError, OSError)``, which does not cover
        ``http.client.HTTPException`` — an ``IncompleteRead`` off ``response.read()`` came
        straight through — while a caller-injected ``opener`` widens that further.

        Args:
          event: The event to send.

        Returns:
          True when it landed, False when it did not.

        Raises:
          None. ``SinkDeliveryError`` is caught separately only because ``HTTPSink._abandon``
            has already counted and announced it, and counting it again would double-report.
            Only the exception type is ever written (arch §6).
        """
        try:
            if self.client is not None:
                self.client.capture_event(self._sentry_event(event))
            else:
                self._post_envelope(event)
        except SinkDeliveryError:
            return False
        except Exception as err:
            with self._counter_lock:
                self.transport_errors += 1
            _diag.lost("event", 1, f"SentrySink, {type(err).__name__}")
            return False
        return True

    def _qualifies(self, event: dict[str, object]) -> bool:
        """Reports whether an event is at or above the configured level floor.

        Args:
          event: The event to test.

        Returns:
          True when it should be sent.

        Raises:
          None.
        """
        level = event.get("level")
        if not isinstance(level, str):
            return False
        return _LEVEL_RANK.get(level.upper(), -1) >= self._min_rank

    def _sentry_event(self, event: dict[str, object]) -> dict[str, object]:
        """Converts an event into Sentry's event shape, with the whole event as ``extra``.

        Args:
          event: The event to convert.

        Returns:
          The Sentry event.

        Raises:
          None.
        """
        level = event.get("level")
        sentry_level = _SENTRY_LEVEL.get(level.upper(), "error") if isinstance(level, str) else (
            "error"
        )
        return {
            "message": event.get("message", ""),
            "level": sentry_level,
            "logger": "log_foundry",
            "extra": event,
        }

    def _post_envelope(self, event: dict[str, object]) -> None:
        """POSTs one event as a Sentry envelope over the HTTP fallback.

        The assertion narrows the type for mypy rather than checking at runtime: the HTTP
        transport is set in the constructor whenever the path that reaches here is in use.

        Args:
          event: The event to send.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If the request was abandoned past the retry bound.
        """
        assert self._http is not None  # noqa: S101
        header = {"event_id": uuid.uuid4().hex, "dsn": self._dsn}
        item_header = {"type": "event"}
        body = (
            json.dumps(header) + "\n"
            + json.dumps(item_header) + "\n"
            + json.dumps(self._sentry_event(event)) + "\n"
        ).encode("utf-8")
        self._http._send(
            body,
            content_type="application/x-sentry-envelope",
            extra_headers={"X-Sentry-Auth": self._auth_header},
        )


def _import_sdk() -> Any:
    """Imports ``sentry_sdk`` lazily.

    Args:
      None.

    Returns:
      The module, or ``None`` if the optional extra is not installed.

    Raises:
      None.
    """
    try:
        import sentry_sdk  # type: ignore[import-not-found]
    except ImportError:
        return None
    return sentry_sdk


def _parse_dsn(dsn: str) -> tuple[str, str]:
    """Derives the ingest URL and auth header from a Sentry DSN.

    Args:
      dsn: The Sentry DSN.

    Returns:
      The envelope ingest URL and the ``X-Sentry-Auth`` header value.

    Raises:
      ValueError: If the DSN cannot be parsed as a URL.
    """
    parsed = urlparse(dsn)
    project = parsed.path.lstrip("/")
    port = f":{parsed.port}" if parsed.port else ""
    ingest_url = f"{parsed.scheme}://{parsed.hostname}{port}/api/{project}/envelope/"
    auth_header = f"Sentry sentry_key={parsed.username}, sentry_version=7, sentry_client=log-foundry"
    return ingest_url, auth_header
