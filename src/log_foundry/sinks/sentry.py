"""SentrySink — route error-level events to Sentry (arch §8, SPEC-009).

Uses the ``sentry-sdk`` when it is installed (the optional ``sentry`` extra): ``import sentry_sdk``
happens lazily inside the sink, never at module top, so importing this module does not require the
extra. When the SDK is absent the sink falls back to POSTing a Sentry *envelope* over HTTP to the
DSN's ingest URL (reusing :class:`~log_foundry.sinks.http.HTTPSink`). Only events at/above a
configurable ``min_level`` (default ``ERROR``) are sent; the rest are skipped.
"""

from __future__ import annotations

import json
import uuid
from typing import Any
from urllib.parse import urlparse

from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.http import HTTPSink

__all__ = ["SentrySink"]

# log-foundry level -> ordering rank (send when rank >= min_level's rank).
_LEVEL_RANK = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
# log-foundry level -> Sentry level keyword.
_SENTRY_LEVEL = {
    "DEBUG": "debug", "INFO": "info", "WARNING": "warning", "ERROR": "error", "CRITICAL": "fatal",
}


class SentrySink:
    """A :class:`~log_foundry.sinks.base.Sink` that captures qualifying events to Sentry (FR-011).

    Attributes:
        sent: Events captured/sent to Sentry.
        skipped: Events below ``min_level`` (or without a usable level) that were not sent.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        min_level: str = "ERROR",
        sdk: Any = None,
        opener: Any = None,
        max_retries: int = 3,
    ) -> None:
        self._dsn = dsn
        self._min_rank = _LEVEL_RANK.get(min_level.upper(), _LEVEL_RANK["ERROR"])
        self.sent = 0
        self.skipped = 0
        self._sdk = sdk if sdk is not None else _import_sdk()
        self._http: HTTPSink | None = None
        self._auth_header = ""
        if self._sdk is None:
            # No SDK: prepare the HTTP-envelope fallback, which needs a DSN to know where to POST.
            if dsn is None:
                raise ValueError(
                    "SentrySink without sentry-sdk requires a dsn for the HTTP-envelope fallback"
                )
            ingest_url, self._auth_header = _parse_dsn(dsn)
            self._http = HTTPSink(ingest_url, opener=opener, max_retries=max_retries)

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Capture each qualifying event via the SDK or the HTTP fallback (FR-011).

        One envelope per event, so an abandoned request is a *per-event* outcome: it is caught,
        counted, and only re-raised when the whole batch got through to nothing (SPEC-026
        FR-001). Letting the first failure propagate would hand the worker a batch whose earlier
        events Sentry had already accepted, and the retry would duplicate them.

        An event below ``min_level`` is skipped, not lost, so a batch of nothing but skipped
        events is a successful emit — there was never anything to deliver.
        """
        attempted = delivered = 0
        for event in batch:
            if not self._qualifies(event):
                self.skipped += 1
                continue
            attempted += 1
            if self._sdk is not None:
                self._sdk.capture_event(self._sentry_event(event))
            else:
                try:
                    self._post_envelope(event)
                except SinkDeliveryError:
                    continue  # already counted and logged by HTTPSink._abandon
            self.sent += 1
            delivered += 1
        if attempted and not delivered:
            raise SinkDeliveryError(
                f"SentrySink delivered none of {attempted} qualifying event(s)"
            )

    def close(self) -> None:
        """Release the HTTP fallback resource, if any; idempotent (FR-012)."""
        if self._http is not None:
            self._http.close()

    @property
    def failed(self) -> int:
        """Requests abandoned past the retry bound (HTTP fallback only)."""
        return self._http.failed if self._http is not None else 0

    def losses(self) -> SinkLosses:
        """Envelopes abandoned past the retry bound (SPEC-026 FR-002). Never raises.

        ``skipped`` is deliberately **not** ``dropped``: an event below ``min_level`` was never
        meant for Sentry, and reporting a configured filter as loss would make the alert idiom
        fire on every INFO log. The SDK path reports nothing — ``capture_event`` is fire-and-
        forget and the SDK owns its own delivery.
        """
        return SinkLosses(dropped=0, failed=self.failed)

    # -- internals ----------------------------------------------------------------------

    def _qualifies(self, event: dict[str, object]) -> bool:
        level = event.get("level")
        if not isinstance(level, str):
            return False
        return _LEVEL_RANK.get(level.upper(), -1) >= self._min_rank

    def _sentry_event(self, event: dict[str, object]) -> dict[str, object]:
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
        # Narrowing for mypy, not a runtime check: _http is set in __init__ whenever the
        # transport path that reaches here is in use.
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
    """Import ``sentry_sdk`` lazily; return ``None`` if the optional extra is not installed."""
    try:
        import sentry_sdk  # type: ignore[import-not-found]  # optional 'sentry' extra
    except ImportError:
        return None
    return sentry_sdk


def _parse_dsn(dsn: str) -> tuple[str, str]:
    """Derive ``(ingest_url, x_sentry_auth_header)`` from a Sentry DSN."""
    parsed = urlparse(dsn)
    project = parsed.path.lstrip("/")
    port = f":{parsed.port}" if parsed.port else ""
    ingest_url = f"{parsed.scheme}://{parsed.hostname}{port}/api/{project}/envelope/"
    auth_header = f"Sentry sentry_key={parsed.username}, sentry_version=7, sentry_client=log-foundry"
    return ingest_url, auth_header
