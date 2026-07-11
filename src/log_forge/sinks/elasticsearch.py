"""ElasticsearchSink / OpenSearchSink — index events via the ``_bulk`` API (arch §8, SPEC-009).

A thin specialization of :class:`~log_forge.sinks.http.HTTPSink`: each event becomes an action line
(``{"index": {"_index": target}}``) followed by its source line, POSTed to ``_bulk`` as newline-
delimited JSON. The bulk response is inspected per item so a partial failure is counted and logged
without discarding the successfully-indexed items. OpenSearch speaks the same bulk protocol, so
``OpenSearchSink`` is a straight reuse (endpoint/auth differ only by configuration).
"""

from __future__ import annotations

import json
import sys

from log_forge.sinks.http import HTTPSink

__all__ = ["ElasticsearchSink", "OpenSearchSink"]


class ElasticsearchSink(HTTPSink):
    """POST events to an Elasticsearch ``_bulk`` endpoint, parsing per-item errors (FR-003).

    Attributes:
        item_errors: Count of bulk items the server reported as failed (distinct from ``failed``,
            which counts whole requests abandoned past the retry bound).
    """

    def __init__(self, url: str, *, index: str, auth: str | tuple[str, str] | None = None,
                 **http_kwargs: object) -> None:
        self._index = index
        super().__init__(
            url.rstrip("/") + "/_bulk", auth=auth, body_format="ndjson", **http_kwargs  # type: ignore[arg-type]
        )
        self.item_errors = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Build the ``_bulk`` NDJSON payload, POST it, and parse the response items (FR-003)."""
        if not batch:
            return
        lines: list[str] = []
        for event in batch:
            lines.append(json.dumps({"index": {"_index": self._index}}))
            lines.append(json.dumps(event))
        body = ("\n".join(lines) + "\n").encode("utf-8")  # bulk must be newline-terminated
        payload = self._send(body, content_type="application/x-ndjson")
        if payload is not None:
            self._parse_bulk_response(payload)

    def _parse_bulk_response(self, payload: bytes) -> None:
        """Count items the bulk response flagged as errors; a partial failure keeps the rest."""
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return
        if not isinstance(data, dict) or not data.get("errors"):
            return
        errors = 0
        for item in data.get("items", []):
            result: object = next(iter(item.values()), {}) if isinstance(item, dict) else {}
            if isinstance(result, dict) and result.get("error"):
                errors += 1
        if errors:
            self.item_errors += errors
            sys.stderr.write(f"log-forge: ElasticsearchSink saw {errors} failed bulk item(s)\n")


class OpenSearchSink(ElasticsearchSink):
    """OpenSearch reuses the Elasticsearch ``_bulk`` protocol verbatim (FR-003)."""
