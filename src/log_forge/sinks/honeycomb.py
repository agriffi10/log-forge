"""HoneycombSink — ship to Honeycomb's batch events API (arch §8, SPEC-009).

An :class:`~log_forge.sinks.http.HTTPSink` specialization: POSTs to ``/1/batch/<dataset>`` with an
``X-Honeycomb-Team`` header, using Honeycomb's ``[{"data": <event>}, ...]`` batch shape.
"""

from __future__ import annotations

import json

from log_forge.sinks.http import HTTPSink, merge_headers

__all__ = ["HoneycombSink"]


class HoneycombSink(HTTPSink):
    """POST events to Honeycomb's batch API for a dataset (FR-010)."""

    def __init__(
        self,
        api_key: str,
        dataset: str,
        *,
        url: str = "https://api.honeycomb.io",
        **http_kwargs: object,
    ) -> None:
        headers = merge_headers({"X-Honeycomb-Team": api_key}, http_kwargs)
        super().__init__(
            f"{url.rstrip('/')}/1/batch/{dataset}",
            headers=headers, body_format="json_array", **http_kwargs,  # type: ignore[arg-type]
        )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """POST the batch as Honeycomb's ``[{"data": event}, ...]`` shape (FR-010)."""
        if not batch:
            return
        body = json.dumps([{"data": event} for event in batch]).encode("utf-8")
        self._send(body, content_type="application/json")
