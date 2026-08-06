"""ElasticsearchSink / OpenSearchSink — index events via the ``_bulk`` API (arch §8, SPEC-009).

A thin specialization of :class:`~log_foundry.sinks.http.HTTPSink`: each event becomes an action line
(``{"index": {"_index": target}}``) followed by its source line, POSTed to ``_bulk`` as newline-
delimited JSON. The bulk response is inspected per item so a partial failure is counted and logged
without discarding the successfully-indexed items. OpenSearch speaks the same bulk protocol, so
``OpenSearchSink`` is a straight reuse (endpoint/auth differ only by configuration).
"""

from __future__ import annotations

import json

from log_foundry import _diag
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.http import HTTPSink

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
        # An abandoned request raises out of ``_send`` now (SPEC-026 FR-001) — nothing was
        # indexed, so there is no response to parse and nothing downstream to duplicate.
        payload = self._send(body, content_type="application/x-ndjson")
        rejected = self._parse_bulk_response(payload)
        if rejected >= len(batch):
            # A 200 whose every item carries an error indexed nothing, so it is a total failure
            # like any other and must reach the worker (FR-001). A retry cannot duplicate — and
            # where the cause is permanent (a mapping conflict), the worker abandons the batch
            # after its bound and records it, which beats a silent success. A response
            # rejecting *some* items stays partial and is reported through ``losses()``.
            raise SinkDeliveryError(
                f"{type(self).__name__} indexed none of {len(batch)} event(s)"
            )

    def losses(self) -> SinkLosses:
        """Abandoned requests plus server-rejected bulk items (SPEC-026 FR-002). Never raises.

        The two are summed into ``failed`` because both are events the server did not confirm;
        they are kept apart on the instance (``failed`` / ``item_errors``) for anyone who needs
        to tell "the request never landed" from "the request landed and these items bounced".
        """
        return SinkLosses(dropped=self.dropped_oversized, failed=self.failed + self.item_errors)

    def _parse_bulk_response(self, payload: bytes) -> int:
        """Count items the bulk response flagged as errors; a partial failure keeps the rest.

        Returns how many items were rejected, so ``emit`` can tell a partial failure from one
        that indexed nothing. An unparseable or errors-free response returns ``0``: the request
        succeeded, and a body this sink cannot read is not evidence against that.
        """
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return 0
        if not isinstance(data, dict) or not data.get("errors"):
            return 0
        errors = 0
        for item in data.get("items", []):
            result: object = next(iter(item.values()), {}) if isinstance(item, dict) else {}
            if isinstance(result, dict) and result.get("error"):
                errors += 1
        if errors:
            self.item_errors += errors
            _diag.lost("bulk item", errors, f"{type(self).__name__}, rejected by the server")
        return errors


class OpenSearchSink(ElasticsearchSink):
    """OpenSearch reuses the Elasticsearch ``_bulk`` protocol verbatim (FR-003)."""
