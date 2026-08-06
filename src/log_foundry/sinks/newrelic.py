"""NewRelicSink — ship to the New Relic Log API (arch §8, SPEC-009)."""

from __future__ import annotations

from log_foundry.sinks.http import HTTPSink, merge_headers

__all__ = ["NewRelicSink"]

_HOSTS = {"US": "log-api.newrelic.com", "EU": "log-api.eu.newrelic.com"}


class NewRelicSink(HTTPSink):
    """POSTs events to the New Relic Log API (FR-009).

    The batch goes as a JSON array to the region-specific Log API endpoint with an ``Api-Key``
    header.
    """

    def __init__(self, api_key: str, *, region: str = "US", **http_kwargs: object) -> None:
        """Points the sink at a region's Log API endpoint.

        Args:
          api_key: The key sent as ``Api-Key``.
          region: The account region, matched case-insensitively, which selects the host.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`.

        Returns:
          None.

        Raises:
          ValueError: If the region is not one this API serves.
        """
        region = region.upper()
        if region not in _HOSTS:
            raise ValueError(f"invalid region {region!r}; expected one of {sorted(_HOSTS)}")
        headers = merge_headers({"Api-Key": api_key}, http_kwargs)
        super().__init__(
            f"https://{_HOSTS[region]}/log/v1",
            headers=headers, body_format="json_array", **http_kwargs,  # type: ignore[arg-type]
        )
