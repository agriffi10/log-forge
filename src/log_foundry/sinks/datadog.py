"""DatadogSink — ship to Datadog's logs intake (arch §8, SPEC-009)."""

from __future__ import annotations

import json
import re
from typing import Unpack

from log_foundry.sinks.http import HTTPPlatformKwargs, HTTPSink, _valid_url, merge_headers

__all__ = ["DatadogSink"]

_DDSOURCE = "log-foundry"

_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
"""One LDH label: letters, digits and inner hyphens, at most 63 characters."""

_SITE = re.compile(rf"{_LABEL}(?:\.{_LABEL})*")
"""A host name of dot-separated LDH labels, which every site Datadog names is.

``site`` is placed into the intake host, so anything else in it moved the request: ``#`` emptied
the path, ``?`` and ``/`` moved it, ``@`` put the batch on a host with an ``@`` in it, which
resolves nowhere, and ``""``, ``..`` and ``%2e`` named no host Datadog serves. A port and a trailing
dot are refused with them, as not a Datadog site, though either reached the intake.
"""


class DatadogSink(HTTPSink):
    """POSTs events to Datadog's logs intake (FR-007).

    The batch goes as a JSON array to the region-specific intake with a ``DD-API-KEY`` header,
    each entry enriched with ``ddsource``, ``service`` and ``ddtags``.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003), for the reasons :class:`~log_foundry.sinks.http.HTTPSink` records: there
    is no transport held and ``close()`` releases nothing.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — Datadog's documented maximum array size for the logs intake.
      MAX_BATCH_BYTES: 5,000,000 — its documented maximum uncompressed payload.
      MAX_EVENT_BYTES: 1,000,000 — its documented maximum for a *single* log. This is the one
        sink in the family whose per-event limit is stricter than its request limit, so without
        it a 2 MB event passes the 5 MB request budget and is rejected by a limit the budget
        cannot see. All three are the vendor's own figures, from the Logs API's send-logs limits.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_BATCH_COUNT = 1000
    MAX_EVENT_BYTES = 1_000_000
    MAX_BATCH_BYTES = 5_000_000

    def __init__(
        self,
        api_key: str,
        *,
        site: str = "datadoghq.com",
        service: str | None = None,
        ddtags: str | None = None,
        **http_kwargs: Unpack[HTTPPlatformKwargs],
    ) -> None:
        """Points the sink at a Datadog site's logs intake.

        Args:
          api_key: The key sent as ``DD-API-KEY``.
          site: The Datadog site, which selects the intake host.
          service: Overrides the event's own ``service``, or ``None`` to keep it.
          ddtags: Tags applied to every entry, or ``None`` for none.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`, typed as
            ``HTTPPlatformKwargs`` (SPEC-051 FR-005) — every keyword it takes except
            ``body_format``, which this sink pins to Datadog's JSON array.

        Returns:
          None.

        Raises:
          ValueError: If ``site`` is not a host name of dot-separated LDH labels (:data:`_SITE`),
            or the intake URL it builds cannot be sent — refused here so the message names
            ``site``, the argument the caller wrote, rather than the ``url`` this class built from
            it (SPEC-049 FR-001).
        """
        if not _SITE.fullmatch(site):
            raise ValueError(
                f"{type(self).__name__} site must be a Datadog site's host name, such as "
                f"'datadoghq.com' or 'us5.datadoghq.com': {site!r}"
            )
        url = _valid_url(
            f"https://http-intake.logs.{site}/api/v2/logs", type(self).__name__, name="site"
        )
        self._service = service
        self._ddtags = ddtags
        headers = merge_headers({"DD-API-KEY": api_key}, http_kwargs)
        super().__init__(
            url,
            headers=headers,
            body_format="json_array",
            **http_kwargs,  # type: ignore[misc]
        )

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one enriched entry for the JSON array (FR-007).

        Args:
          event: The event to enrich and serialize.

        Returns:
          The serialized entry.

        Raises:
          TypeError: If the event is not JSON-serializable. Neither source of events should
            produce one: assembly coerces every value (``sanitize``), and a ``TransformSink``
            checks and coerces what its function returns (SPEC-058 FR-001). If one does,
            ``HTTPSink._items`` drops that event alone and counts it (SPEC-058 FR-005).
        """
        return json.dumps(self._entry(event))

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
