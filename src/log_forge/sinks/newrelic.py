"""NewRelicSink — ship to the New Relic Log API (arch §8, SPEC-009).

An :class:`~log_forge.sinks.http.HTTPSink` specialization: POSTs the batch as a JSON array to the
region-specific Log API endpoint with an ``Api-Key`` header.
"""

from __future__ import annotations

from log_forge.sinks.http import HTTPSink, merge_headers

__all__ = ["NewRelicSink"]

# region -> Log API host.
_HOSTS = {"US": "log-api.newrelic.com", "EU": "log-api.eu.newrelic.com"}


class NewRelicSink(HTTPSink):
    """POST events to the New Relic Log API (FR-009)."""

    def __init__(self, api_key: str, *, region: str = "US", **http_kwargs: object) -> None:
        region = region.upper()
        if region not in _HOSTS:
            raise ValueError(f"invalid region {region!r}; expected one of {sorted(_HOSTS)}")
        headers = merge_headers({"Api-Key": api_key}, http_kwargs)
        super().__init__(
            f"https://{_HOSTS[region]}/log/v1",
            headers=headers, body_format="json_array", **http_kwargs,  # type: ignore[arg-type]
        )
