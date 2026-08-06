"""DatadogSink — ship to Datadog's logs intake (arch §8, SPEC-009)."""

from __future__ import annotations

import json

from log_foundry.sinks.http import HTTPSink, merge_headers

__all__ = ["DatadogSink"]

_DDSOURCE = "log-foundry"


class DatadogSink(HTTPSink):
    """POSTs events to Datadog's logs intake (FR-007).

    The batch goes as a JSON array to the region-specific intake with a ``DD-API-KEY`` header,
    each entry enriched with ``ddsource``, ``service`` and ``ddtags``.
    """

    def __init__(
        self,
        api_key: str,
        *,
        site: str = "datadoghq.com",
        service: str | None = None,
        ddtags: str | None = None,
        **http_kwargs: object,
    ) -> None:
        """Points the sink at a Datadog site's logs intake.

        Args:
          api_key: The key sent as ``DD-API-KEY``.
          site: The Datadog site, which selects the intake host.
          service: Overrides the event's own ``service``, or ``None`` to keep it.
          ddtags: Tags applied to every entry, or ``None`` for none.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`.

        Returns:
          None.

        Raises:
          None.
        """
        self._service = service
        self._ddtags = ddtags
        headers = merge_headers({"DD-API-KEY": api_key}, http_kwargs)
        super().__init__(
            f"https://http-intake.logs.{site}/api/v2/logs",
            headers=headers, body_format="json_array", **http_kwargs,  # type: ignore[arg-type]
        )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """POSTs each enriched event as one JSON array (FR-007).

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If the request was abandoned past the retry bound.
        """
        if not batch:
            return
        body = json.dumps([self._entry(event) for event in batch]).encode("utf-8")
        self._send(body, content_type="application/json")

    def _entry(self, event: dict[str, object]) -> dict[str, object]:
        """Copies an event and applies the configured Datadog enrichment.

        Args:
          event: The event to enrich, which is not mutated.

        Returns:
          The enriched entry.

        Raises:
          None.
        """
        entry = dict(event)
        entry["ddsource"] = _DDSOURCE
        if self._service is not None:
            entry["service"] = self._service
        if self._ddtags is not None:
            entry["ddtags"] = self._ddtags
        return entry
