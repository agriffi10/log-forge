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
        dropped_unadjudicated: Events whose outcome a ``_bulk`` response did not describe — its
            ``items`` array did not line up with the batch sent, so no event in it could be
            paired to an outcome. Abandoned rather than retried, for SPEC-018's reason: the
            request succeeded, so re-sending would duplicate whatever landed.
    """

    def __init__(self, url: str, *, index: str, auth: str | tuple[str, str] | None = None,
                 **http_kwargs: object) -> None:
        self._index = index
        super().__init__(
            url.rstrip("/") + "/_bulk", auth=auth, body_format="ndjson", **http_kwargs  # type: ignore[arg-type]
        )
        self.item_errors = 0
        self.dropped_unadjudicated = 0

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
        if self._parse_bulk_response(payload, len(batch)):
            # Every item carried an error, so the 200 indexed nothing: a total failure like any
            # other, and it must reach the worker (FR-001). A retry cannot duplicate, and where
            # the cause is permanent (a mapping conflict) the worker abandons the batch after
            # its bound and records it — which beats a silent success. A response rejecting
            # *some* items stays partial and is reported through ``losses()``.
            raise SinkDeliveryError(
                f"{type(self).__name__} indexed none of {len(batch)} event(s)"
            )

    def losses(self) -> SinkLosses:
        """Abandoned requests plus server-rejected bulk items (SPEC-026 FR-002). Never raises.

        All three are summed into ``failed`` because each is an event the server did not
        confirm; they stay apart on the instance (``failed`` / ``item_errors`` /
        ``dropped_unadjudicated``) for anyone who needs to tell "the request never landed" from
        "the request landed and these items bounced" from "the response did not say".
        """
        return SinkLosses(
            dropped=self.dropped_oversized,
            failed=self.failed + self.item_errors + self.dropped_unadjudicated,
        )

    def _parse_bulk_response(self, payload: bytes, sent: int) -> bool:
        """Count rejected items; return whether the response proves *nothing* was indexed.

        The ``items`` array is positional — entry *i* describes event *i* — so the rule
        ``sinks/_batch.py`` states applies here too (SPEC-018): a length disagreement is
        evidence the arrays do not describe each other, not an invitation to read the overlap.
        Counting errors does not depend on position, but concluding "all of them failed" does,
        so that conclusion is gated on ``len(items) == sent``. Without the gate both directions
        are wrong and both are reachable: a longer ``items`` array could make a partial success
        raise (and the worker's retry would duplicate what landed), and a shorter one could
        report a total failure as success.

        An array that cannot be adjudicated is counted on ``dropped_unadjudicated`` and left
        alone — like ``KinesisSink``'s, the request itself succeeded, so re-sending it would
        duplicate whatever did land. An unparseable or errors-free body returns ``False``: the
        request succeeded, and a body this sink cannot read is not evidence against that.
        """
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return False
        if not isinstance(data, dict) or not data.get("errors"):
            return False
        items = data.get("items")
        items = items if isinstance(items, list) else []
        if len(items) != sent:
            self.dropped_unadjudicated += sent
            _diag.lost(
                "event",
                sent,
                f"{type(self).__name__} could not adjudicate a _bulk response "
                f"({sent} event(s) sent, {len(items)} item(s) returned); not retried",
            )
            return False
        errors = 0
        for item in items:
            result: object = next(iter(item.values()), {}) if isinstance(item, dict) else {}
            if isinstance(result, dict) and result.get("error"):
                errors += 1
        if errors:
            self.item_errors += errors
            _diag.lost("bulk item", errors, f"{type(self).__name__}, rejected by the server")
        return errors == sent


class OpenSearchSink(ElasticsearchSink):
    """OpenSearch reuses the Elasticsearch ``_bulk`` protocol verbatim (FR-003)."""
