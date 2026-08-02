"""HTTPSink — POST batches over the stdlib ``urllib`` (arch §8, §9.1, SPEC-009).

The dependency-free base every other HTTP platform sink here builds on: it serializes a batch as
NDJSON or a JSON array, applies headers/auth/optional gzip, POSTs it, and retries ``429``/``5xx``
with bounded exponential backoff (honoring ``Retry-After``) before counting and logging an abandoned
request. Like every sink it receives *already-built* event dicts and knows nothing about spans.

These are **terminal, direct-ship** sinks: per arch §9.1 they couple application delivery to the
destination's availability, so they are best-effort — for durability put a queue (SQS/SPEC-010) in
front. A test can inject a fake ``opener`` (a ``urlopen``-shaped callable) to assert on the request
without any network access.
"""

from __future__ import annotations

import gzip as _gzip
import json
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["HTTPSink", "merge_headers"]

_BACKOFF_BASE = 0.1  # seconds; delay for retry attempt n is _BACKOFF_BASE * 2**n


def merge_headers(base: dict[str, str], http_kwargs: dict[str, object]) -> dict[str, str]:
    """Merge a platform sink's own headers with any caller-supplied ``headers`` (caller wins).

    Pops ``headers`` out of ``http_kwargs`` (mutating it) so the remaining kwargs can be forwarded
    to :class:`HTTPSink` without a duplicate ``headers`` argument. Shared by the SaaS sinks that set
    a fixed auth header (Datadog/Splunk/New Relic/Honeycomb).
    """
    caller = http_kwargs.pop("headers", None)
    if isinstance(caller, dict):
        base.update(caller)
    return base


class HTTPSink:
    """A :class:`~log_foundry.sinks.base.Sink` that POSTs each batch to an HTTP endpoint.

    Credentials passed as ``auth`` are sent on every request to ``url`` as given. Nothing here
    requires ``https://`` — over a plaintext endpoint a bearer token, and a basic-auth pair (which
    is base64, not encryption), travel in the clear. Use ``https://`` for anything off the host.

    Attributes:
        failed: Requests abandoned past the retry bound.
        dropped_oversized: Events dropped for exceeding a destination's hard size limit (used by
            subclasses that enforce one; the generic core imposes no universal limit).
    """

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        auth: str | tuple[str, str] | None = None,
        body_format: str = "ndjson",
        timeout: float = 5.0,
        gzip: bool = False,
        max_retries: int = 3,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        self.url = url
        self.method = method
        self._headers = dict(headers) if headers else {}
        self._auth = auth
        self.body_format = body_format
        self.timeout = timeout
        self.gzip = gzip
        self.max_retries = max_retries
        self._opener = opener if opener is not None else urllib.request.urlopen
        self.failed = 0
        self.dropped_oversized = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Serialize ``batch`` per ``body_format`` and POST it (FR-001)."""
        if not batch:
            return
        body, content_type = self._encode(batch)
        self._send(body, content_type=content_type)

    def close(self) -> None:
        """No-op — ``urllib`` opens a fresh connection per request; idempotent (FR-012)."""

    # -- body building ------------------------------------------------------------------

    def _encode(self, batch: list[dict[str, object]]) -> tuple[bytes, str]:
        """Return ``(body_bytes, default_content_type)`` for the configured ``body_format``."""
        if self.body_format == "json_array":
            return json.dumps(batch).encode("utf-8"), "application/json"
        text = "".join(json.dumps(event) + "\n" for event in batch)
        return text.encode("utf-8"), "application/x-ndjson"

    # -- transport ----------------------------------------------------------------------

    def _send(
        self,
        body: bytes,
        *,
        content_type: str,
        extra_headers: dict[str, str] | None = None,
    ) -> bytes | None:
        """POST ``body`` with bounded retry; return the response bytes, or ``None`` if abandoned.

        Subclasses that must inspect the response (e.g. Elasticsearch ``_bulk`` ``items``) use the
        returned bytes; a ``None`` return means the request was retried to exhaustion and counted.
        """
        headers, data = self._prepare(body, content_type, extra_headers)
        for attempt in range(self.max_retries + 1):
            # Opening `self.url` is this class's entire purpose. The URL is the application's
            # own configured endpoint, never inbound data — an app that can set it can already
            # read its own files.
            request = urllib.request.Request(  # noqa: S310
                self.url, data=data, method=self.method, headers=headers
            )
            try:
                status, payload, retry_after = self._attempt(request)
            except (urllib.error.URLError, OSError) as err:
                if attempt < self.max_retries:
                    self._sleep_backoff(attempt, None)
                    continue
                self._abandon(f"connection error: {err!r}")
                return None
            if 200 <= status < 300:
                return payload
            if (status == 429 or 500 <= status < 600) and attempt < self.max_retries:
                self._sleep_backoff(attempt, retry_after)
                continue
            self._abandon(f"HTTP {status}")
            return None
        return None

    def _prepare(
        self,
        body: bytes,
        content_type: str,
        extra_headers: dict[str, str] | None,
    ) -> tuple[dict[str, str], bytes]:
        """Build the final header map + (optionally gzipped) body. Caller headers win (FR-002)."""
        headers: dict[str, str] = {"Content-Type": content_type}
        if extra_headers:
            headers.update(extra_headers)
        headers.update(self._headers)  # caller-provided headers override defaults (FR-002)
        data = body
        if self.gzip:
            data = _gzip.compress(body)
            headers["Content-Encoding"] = "gzip"
        self._apply_auth(headers)
        return headers, data

    def _apply_auth(self, headers: dict[str, str]) -> None:
        """Apply bearer-token (str) or basic-auth (user, pass) credentials, if not already set.

        The scheme of ``url`` is NOT checked here: an ``http://`` endpoint sends these credentials
        in cleartext, and basic auth is base64, not encryption. That is deliberate — a plaintext
        endpoint is a legitimate configuration (a sidecar collector on loopback, an in-cluster
        aggregator), and this library does not get to overrule the application's own deployment.
        The caller owns the choice; see the class docstring.
        """
        if self._auth is None or "Authorization" in headers:
            return
        if isinstance(self._auth, str):
            headers["Authorization"] = f"Bearer {self._auth}"
        else:
            user, password = self._auth
            token = b64encode(f"{user}:{password}".encode()).decode("ascii")
            headers["Authorization"] = f"Basic {token}"

    def _attempt(self, request: urllib.request.Request) -> tuple[int, bytes, float | None]:
        """Perform one request. Returns ``(status, payload, retry_after)``.

        A ``4xx``/``5xx`` arrives from ``urlopen`` as an ``HTTPError``, which *is* a readable
        response object, so it is unified here rather than raised. Connection-level failures
        (``URLError``/``OSError``) propagate for the retry loop to handle.
        """
        try:
            response = self._opener(request, timeout=self.timeout)
        except urllib.error.HTTPError as err:
            response = err
        status = getattr(response, "status", None)
        if status is None:
            status = response.getcode()
        payload = response.read()
        return int(status), payload, _parse_retry_after(getattr(response, "headers", None))

    def _sleep_backoff(self, attempt: int, retry_after: float | None) -> None:
        """Sleep before the next attempt: ``Retry-After`` if the server gave one, else backoff."""
        delay = retry_after if retry_after is not None else _BACKOFF_BASE * (2**attempt)
        time.sleep(delay)

    def _abandon(self, reason: str) -> None:
        """Count and log a request abandoned past the retry bound (FR-012)."""
        self.failed += 1
        sys.stderr.write(
            f"log-foundry: {type(self).__name__} abandoned a request after "
            f"{self.max_retries + 1} attempt(s) ({reason})\n"
        )


def _parse_retry_after(headers: Any) -> float | None:
    """Parse a ``Retry-After`` delay in seconds; ``None`` if absent or an HTTP-date we don't parse."""
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None  # HTTP-date form: fall back to exponential backoff
