"""NewRelicSink — ship to the New Relic Log API (arch §8, SPEC-009)."""

from __future__ import annotations

from typing import Unpack

from log_foundry.sinks.http import (
    HTTPPlatformKwargs,
    HTTPSink,
    _refuse_empty_credential,
    merge_headers,
)

__all__ = ["NewRelicSink"]

_HOSTS = {"US": "log-api.newrelic.com", "EU": "log-api.eu.newrelic.com"}


class NewRelicSink(HTTPSink):
    """POSTs events to the New Relic Log API (FR-009).

    The batch goes as a JSON array to the region-specific Log API endpoint with an ``Api-Key``
    header.

    Attributes:
      MAX_BATCH_COUNT: 1,000 — this library's conservative default, since New Relic documents no
        maximum entry count, only a payload size.
      MAX_BATCH_BYTES: 1,000,000 — the Log API's documented "1MB (10^6 bytes) maximum per POST".
        Measured uncompressed here, which is the conservative reading when ``gzip=True``.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_BATCH_COUNT = 1000
    MAX_BATCH_BYTES = 1_000_000

    def __init__(
        self, api_key: str, *, region: str = "US",
        **http_kwargs: Unpack[HTTPPlatformKwargs],
    ) -> None:
        """Points the sink at a region's Log API endpoint.

        Args:
          api_key: The key sent as ``Api-Key``.
          region: The account region, matched case-insensitively, which selects the host.
          **http_kwargs: Forwarded to :class:`~log_foundry.sinks.http.HTTPSink`, typed as
            ``HTTPPlatformKwargs`` (SPEC-051 FR-005) — every keyword it takes except
            ``body_format``, which this sink pins to New Relic's JSON array.

        Returns:
          None.

        Raises:
          TypeError: If a forwarded ``max_retries`` or ``max_batch_count`` is not an ``int``, or
            a forwarded ``max_retry_after`` is not an ``int`` or ``float``, refused by
            :class:`~log_foundry.sinks.http.HTTPSink` naming this class (SPEC-066).
          ValueError: If the region is not one this API serves, or a forwarded ``auth``
            accompanies an ``Authorization`` header in a forwarded ``headers``, in any case,
            refused by :class:`~log_foundry.sinks.http.HTTPSink` naming this class, or a
            forwarded header's name folds to ``Api-Key``, which would replace ``api_key`` on
            the wire (:func:`~log_foundry.sinks.http.merge_headers`), or, last, ``api_key`` is
            empty or only spaces and tabs, which sent an empty ``Api-Key`` on every request
            (1.2.0).
        """
        region = region.upper()
        if region not in _HOSTS:
            raise ValueError(
                f"{type(self).__name__} region must be one of {sorted(_HOSTS)}, not {region!r}"
            )
        headers = merge_headers(
            {"Api-Key": api_key},
            http_kwargs,
            owner=type(self).__name__,
            argument="api_key",
        )
        super().__init__(
            f"https://{_HOSTS[region]}/log/v1",
            headers=headers,
            body_format="json_array",
            **http_kwargs,  # type: ignore[misc]
        )
        _refuse_empty_credential(api_key, type(self).__name__, "api_key")
