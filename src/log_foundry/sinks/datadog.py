"""DatadogSink — ship to Datadog's logs intake (arch §8, SPEC-009).

An :class:`~log_foundry.sinks.http.HTTPSink` specialization: POSTs the batch as a JSON array to the
region-specific logs intake with a ``DD-API-KEY`` header, enriching each entry with ``ddsource``,
``service``, and ``ddtags`` from configuration / the event.
"""

from __future__ import annotations

import json

from log_foundry.sinks.http import HTTPSink, merge_headers

__all__ = ["DatadogSink"]

_DDSOURCE = "log-foundry"


class DatadogSink(HTTPSink):
    """POST events to Datadog's logs intake (FR-007)."""

    def __init__(
        self,
        api_key: str,
        *,
        site: str = "datadoghq.com",
        service: str | None = None,
        ddtags: str | None = None,
        **http_kwargs: object,
    ) -> None:
        self._service = service
        self._ddtags = ddtags
        headers = merge_headers({"DD-API-KEY": api_key}, http_kwargs)
        super().__init__(
            f"https://http-intake.logs.{site}/api/v2/logs",
            headers=headers, body_format="json_array", **http_kwargs,  # type: ignore[arg-type]
        )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """POST each event (enriched) as one JSON array (FR-007)."""
        if not batch:
            return
        body = json.dumps([self._entry(event) for event in batch]).encode("utf-8")
        self._send(body, content_type="application/json")

    def _entry(self, event: dict[str, object]) -> dict[str, object]:
        entry = dict(event)
        entry["ddsource"] = _DDSOURCE
        if self._service is not None:
            entry["service"] = self._service
        if self._ddtags is not None:
            entry["ddtags"] = self._ddtags
        return entry
