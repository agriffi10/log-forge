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
import urllib.error
import urllib.request
from base64 import b64encode
from typing import TYPE_CHECKING, Any, NoReturn

from log_foundry import _diag
from log_foundry.sinks._retry import clamp_server_delay, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

__all__ = ["HTTPSink", "merge_headers"]

_BACKOFF_BASE = 0.1  # seconds; delay for retry attempt n is _BACKOFF_BASE * 2**n

DEFAULT_MAX_RETRY_AFTER = 30.0
"""Ceiling on a server-supplied ``Retry-After``, in seconds (SPEC-027 FR-001).

Thirty rather than something smaller because a rate-limited platform asking for half a minute is
making a reasonable request, and rather than something larger because the *total* is what a caller
with an execution deadline pays: with the default ``max_retries=3`` the worst case is three waits,
90 s, which stays inside a typical serverless timeout while leaving room for the request itself.
"""


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

    **Worst-case delay** (SPEC-027 FR-005): ``max_retries`` waits, each at most
    ``max_retry_after`` when the server sends a ``Retry-After`` and ``0.1 * 2**n`` otherwise —
    so 90 s at the defaults (3 × 30 s), plus the request timeouts themselves. That delay pauses
    the single drain thread, so it is a pause on *all* log delivery, not just this sink's.
    ``shutdown()``'s own timeout bounds the total (SPEC-027 FR-004).

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
        max_retry_after: float = DEFAULT_MAX_RETRY_AFTER,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        self.url = url
        self.method = method
        self._headers = dict(headers) if headers else {}
        self._auth = auth
        self.body_format = body_format
        self.timeout = timeout
        self.gzip = gzip
        # Floored, as ``Worker._emit`` floors its own (SPEC-021): a negative value otherwise
        # skipped the loop entirely, so the request was abandoned with no attempt made and no
        # counter moved — reachable only by misconfiguration, but reachable.
        self.max_retries = max(max_retries, 0)
        # Not floored or rejected here: ``clamp_server_delay`` refuses an unusable ceiling and
        # falls back to exponential backoff, which keeps the validation in one place (SPEC-027
        # FR-001). Stored as given so a caller can read back what they passed.
        self.max_retry_after = max_retry_after
        # Set by the worker when this sink is the configured one (SPEC-027 FR-002); ``None``
        # standalone, which backs off uninterruptibly exactly as before.
        self.stop_signal: threading.Event | None = None
        self._opener = opener if opener is not None else urllib.request.urlopen
        self.failed = 0
        self.dropped_oversized = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Serialize ``batch`` per ``body_format`` and POST it (FR-001)."""
        if not batch:
            return
        body, content_type = self._encode(batch)
        self._send(body, content_type=content_type)

    def losses(self) -> SinkLosses:
        """Oversized drops and abandoned requests (SPEC-026 FR-002). Never raises.

        ``failed`` counts abandoned *requests*, not events — this class has no per-event outcome
        to report, and an abandoned request took its whole body with it. Subclasses that do learn
        per-record outcomes (``ElasticsearchSink``) add them.
        """
        return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

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
    ) -> bytes:
        """POST ``body`` with bounded retry; return the response bytes.

        Subclasses that must inspect the response (e.g. Elasticsearch ``_bulk`` ``items``) use the
        returned bytes. A request retried to exhaustion is counted and then **raises**
        :class:`~log_foundry.sinks.base.SinkDeliveryError` (SPEC-026 FR-001) — it used to return
        ``None``, which every caller here spelled as "nothing to parse" and the worker read as a
        successful emit. One request carries the whole batch, so an abandoned one delivered
        nothing and is exactly the total failure the worker's retry exists for.

        Raising from here rather than from each ``emit`` is deliberate: every platform subclass
        (Datadog, Splunk, New Relic, Honeycomb, Loki, Logstash-HTTP) builds its own body and calls
        this, so the rule reaches them without a line of their own. The one caller that must not
        propagate is ``SentrySink``, which sends one envelope *per event* and therefore catches it.
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
                # The type plus the OS code, never the message: a ``URLError``'s text embeds the
                # URL and whatever the resolver said (SPEC-029 FR-002).
                self._abandon(
                    f"connection error, {type(err).__name__} {_diag.errno_of(err)}".rstrip()
                )
            if 200 <= status < 300:
                return payload
            if (status == 429 or 500 <= status < 600) and attempt < self.max_retries:
                self._sleep_backoff(attempt, retry_after)
                continue
            self._abandon(f"HTTP {status}")
        # Unreachable: every path through the loop returns or raises. mypy needs the exit.
        raise SinkDeliveryError(f"{type(self).__name__} made no attempt")

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
        """Wait before the next attempt: ``Retry-After`` if usable, else exponential backoff.

        The server's value is clamped and sign-checked first (SPEC-027 FR-001) — it is advice
        from the destination, not an instruction the application must obey. Unbounded, a measured
        ``Retry-After: 8`` held ``shutdown()`` for 22 s and a header of ``86400`` would have held
        it for a day; a negative one made ``time.sleep`` raise, which the orphan path handed to
        the caller. Anything rejected falls back to this sink's own backoff, which is what the
        destination would have got had it sent no header at all.
        """
        server = clamp_server_delay(retry_after, self.max_retry_after)
        delay = server if server is not None else _BACKOFF_BASE * (2**attempt)
        wait(delay, self.stop_signal)

    def _abandon(self, reason: str) -> NoReturn:
        """Count, log, then raise for a request abandoned past the retry bound (FR-012).

        ``reason`` carries only library-controlled values — an HTTP status, an exception type, an
        ``errno`` — never a server-supplied body or an exception's text (SPEC-029 FR-002). It is
        also the message of the raised error, for the same reason.

        The counter moves *before* the raise: ``failed`` is this sink's own record of what it
        could not put on the wire, and it must not depend on who catches what. It therefore
        counts every worker retry attempt too, so it is an upper bound on loss rather than a
        count of it — see :class:`~log_foundry.sinks.base.SinkLosses`.
        """
        self.failed += 1
        detail = f"{type(self).__name__}, {self.max_retries + 1} attempt(s), {reason}"
        _diag.lost("request", 1, detail)
        raise SinkDeliveryError(detail)


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
