"""HoneycombSink — ship to Honeycomb's batch events API (arch §8, SPEC-009)."""

from __future__ import annotations

import json

from log_foundry.sinks.http import HTTPSink, merge_headers

__all__ = ["HoneycombSink"]


class HoneycombSink(HTTPSink):
    """POSTs events to Honeycomb's batch API for a dataset (FR-010).

    The request goes to ``/1/batch/<dataset>`` with an ``X-Honeycomb-Team`` header, in
    Honeycomb's ``[{"data": <event>}, ...]`` batch shape.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003), for the reasons :class:`~log_foundry.sinks.http.HTTPSink` records: there
    is no transport held and ``close()`` releases nothing.
    """

    def __init__(
        self,
        api_key: str,
        dataset: str,
        *,
        url: str = "https://api.honeycomb.io",
        **http_kwargs: object,
    ) -> None:
        """Points the sink at a dataset's batch endpoint.

        Args:
          api_key: The key sent as ``X-Honeycomb-Team``.
          dataset: The target dataset, which forms part of the path.
          url: The API base URL.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`.

        Returns:
          None.

        Raises:
          None.
        """
        headers = merge_headers({"X-Honeycomb-Team": api_key}, http_kwargs)
        super().__init__(
            f"{url.rstrip('/')}/1/batch/{dataset}",
            headers=headers, body_format="json_array", **http_kwargs,  # type: ignore[arg-type]
        )

    def emit(self, batch: list[dict[str, object]]) -> None:
        """POSTs the batch in Honeycomb's ``[{"data": event}, ...]`` shape (FR-010).

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If the request was abandoned past the retry bound.
        """
        if not batch:
            return
        body = json.dumps([{"data": event} for event in batch]).encode("utf-8")
        self._send(body, content_type="application/json")
