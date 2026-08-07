"""HTTPSink — POST batches over the stdlib ``urllib`` (arch §8, §9.1, SPEC-009)."""

from __future__ import annotations

import gzip as _gzip
import json
import threading
import urllib.error
import urllib.request
from base64 import b64encode
from typing import TYPE_CHECKING, Any, NoReturn

from log_foundry import _diag
from log_foundry.sinks._retry import clamp_server_delay, wait
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["HTTPSink", "merge_headers"]

_BACKOFF_BASE = 0.1

DEFAULT_MAX_RETRY_AFTER = 30.0
"""Ceiling on a server-supplied ``Retry-After``, in seconds (SPEC-027 FR-001).

Thirty rather than something smaller because a rate-limited platform asking for half a minute is
making a reasonable request, and rather than something larger because the total is what a caller
with an execution deadline pays: at the default retries the worst case is 90 s, which stays
inside a typical serverless timeout while leaving room for the request itself.
"""


def merge_headers(base: dict[str, str], http_kwargs: dict[str, object]) -> dict[str, str]:
    """Merges a platform sink's own headers with any caller-supplied ones, caller winning.

    This is shared by the SaaS sinks that set a fixed auth header — Datadog, Splunk, New Relic,
    Honeycomb.

    Args:
      base: The sink's own headers, updated in place.
      http_kwargs: The caller's keyword arguments, mutated by popping ``headers`` out so the
        rest can be forwarded to :class:`HTTPSink` without a duplicate argument.

    Returns:
      The merged headers.

    Raises:
      None.
    """
    caller = http_kwargs.pop("headers", None)
    if isinstance(caller, dict):
        base.update(caller)
    return base


class HTTPSink:
    """A :class:`~log_foundry.sinks.base.Sink` that POSTs each batch to an HTTP endpoint.

    This is the dependency-free base every other HTTP platform sink builds on: it serializes a
    batch, applies headers, auth and optional gzip, POSTs it, and retries ``429`` and ``5xx``
    with bounded exponential backoff. These are terminal, direct-ship sinks — per arch §9.1 they
    couple application delivery to the destination's availability, so for durability put a queue
    in front.

    Credentials passed as ``auth`` are sent on every request to the URL as given, and nothing
    here requires ``https://``: over a plaintext endpoint a bearer token, and a basic-auth pair,
    travel in the clear. The worst-case delay is ``max_retries`` waits, each at most
    ``max_retry_after`` when the server sends a ``Retry-After`` and exponential otherwise — 90 s
    at the defaults, plus the request timeouts — and that pauses the single drain thread, so it
    is a pause on all log delivery. ``shutdown()``'s own timeout bounds the total (FR-004).

    Attributes:
      failed: Requests abandoned past the retry bound.
      dropped_oversized: Events dropped for exceeding a destination's hard size limit, used by
        subclasses that enforce one; the generic core imposes no universal limit.
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
        """Configures the endpoint, encoding, credentials and retry bounds.

        Args:
          url: The endpoint to POST to.
          method: The HTTP method.
          headers: Headers that override the sink's own defaults.
          auth: A bearer token, or a ``(user, password)`` pair for basic auth.
          body_format: ``"ndjson"`` or ``"json_array"``.
          timeout: Seconds allowed per request.
          gzip: Whether to compress the body.
          max_retries: Retries after a failing request, floored at zero as ``Worker._emit``
            floors its own (SPEC-021) — a negative value otherwise skipped the loop entirely, so
            the request was abandoned with no attempt made and no counter moved.
          max_retry_after: Ceiling on a server-supplied ``Retry-After``. It is neither floored
            nor rejected here, because ``clamp_server_delay`` refuses an unusable ceiling and
            falls back to exponential backoff, keeping the validation in one place; it is stored
            as given so a caller can read back what they passed.
          opener: A ``urlopen``-shaped callable, which a test can inject to assert on the
            request without any network access.

        Returns:
          None.

        Raises:
          None.
        """
        self.url = url
        self.method = method
        self._headers = dict(headers) if headers else {}
        self._auth = auth
        self.body_format = body_format
        self.timeout = timeout
        self.gzip = gzip
        self.max_retries = max(max_retries, 0)
        self.max_retry_after = max_retry_after
        self.stop_signal: threading.Event | None = None
        self._opener = opener if opener is not None else urllib.request.urlopen
        self.failed = 0
        self.dropped_oversized = 0
        self._counter_lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Serializes the batch per the configured body format and POSTs it (FR-001).

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: If the request was abandoned past the retry bound.
        """
        if not batch:
            return
        body, content_type = self._encode(batch)
        self._send(body, content_type=content_type)

    def losses(self) -> SinkLosses:
        """Reports oversized drops and abandoned requests (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters. ``failed`` counts abandoned requests rather than events, since this
          class has no per-event outcome to report and an abandoned request took its whole body
          with it; subclasses that do learn per-record outcomes add them.

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(dropped=self.dropped_oversized, failed=self.failed)

    def close(self) -> None:
        """Does nothing, since ``urllib`` opens a fresh connection per request (FR-012).

        Idempotent.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """

    def _encode(self, batch: list[dict[str, object]]) -> tuple[bytes, str]:
        """Serializes the batch in the configured body format.

        Args:
          batch: The events to serialize.

        Returns:
          The body bytes and the default content type for that format.

        Raises:
          TypeError: If an event is not JSON-serializable, which ``sanitize`` prevents.
        """
        if self.body_format == "json_array":
            return json.dumps(batch).encode("utf-8"), "application/json"
        text = "".join(json.dumps(event) + "\n" for event in batch)
        return text.encode("utf-8"), "application/x-ndjson"

    def _send(
        self,
        body: bytes,
        *,
        content_type: str,
        extra_headers: dict[str, str] | None = None,
    ) -> bytes:
        """POSTs a body with bounded retry and returns the response bytes.

        Raising from here rather than from each ``emit`` is deliberate: every platform subclass
        builds its own body and calls this, so the rule reaches them without a line of their
        own. The one caller that must not propagate is ``SentrySink``, which sends one envelope
        per event and therefore catches it.

        Args:
          body: The serialized batch.
          content_type: The default content type, overridable by caller headers.
          extra_headers: Per-request headers beneath the caller's own.

        Returns:
          The response bytes, which subclasses that must inspect the response — such as
          Elasticsearch's ``_bulk`` items — read.

        Raises:
          SinkDeliveryError: If the request was retried to exhaustion. It used to return
            ``None``, which every caller spelled as "nothing to parse" and the worker read as a
            successful emit; one request carries the whole batch, so an abandoned one delivered
            nothing and is exactly the total failure the worker's retry exists for (SPEC-026
            FR-001).
        """
        headers, data = self._prepare(body, content_type, extra_headers)
        for attempt in range(self.max_retries + 1):
            request = urllib.request.Request(  # noqa: S310
                self.url, data=data, method=self.method, headers=headers
            )
            try:
                status, payload, retry_after = self._attempt(request)
            except (urllib.error.URLError, OSError) as err:
                if attempt < self.max_retries:
                    self._sleep_backoff(attempt, None)
                    continue
                self._abandon(
                    f"connection error, {type(err).__name__} {_diag.errno_of(err)}".rstrip()
                )
            if 200 <= status < 300:
                return payload
            if (status == 429 or 500 <= status < 600) and attempt < self.max_retries:
                self._sleep_backoff(attempt, retry_after)
                continue
            self._abandon(f"HTTP {status}")
        raise SinkDeliveryError(f"{type(self).__name__} made no attempt")

    def _prepare(
        self,
        body: bytes,
        content_type: str,
        extra_headers: dict[str, str] | None,
    ) -> tuple[dict[str, str], bytes]:
        """Builds the final header map and the optionally gzipped body.

        Caller-provided headers override the sink's defaults (FR-002).

        Args:
          body: The serialized batch.
          content_type: The default content type.
          extra_headers: Per-request headers beneath the caller's own.

        Returns:
          The headers and the body to send.

        Raises:
          None.
        """
        headers: dict[str, str] = {"Content-Type": content_type}
        if extra_headers:
            headers.update(extra_headers)
        headers.update(self._headers)
        data = body
        if self.gzip:
            data = _gzip.compress(body)
            headers["Content-Encoding"] = "gzip"
        self._apply_auth(headers)
        return headers, data

    def _apply_auth(self, headers: dict[str, str]) -> None:
        """Applies bearer-token or basic-auth credentials, if not already set.

        The URL's scheme is deliberately not checked: an ``http://`` endpoint sends these
        credentials in cleartext, and basic auth is base64 rather than encryption, but a
        plaintext endpoint is a legitimate configuration — a sidecar collector on loopback, an
        in-cluster aggregator — and this library does not overrule the application's own
        deployment.

        Args:
          headers: The header map, updated in place.

        Returns:
          None.

        Raises:
          None.
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
        """Performs one request.

        A ``4xx`` or ``5xx`` arrives from ``urlopen`` as an ``HTTPError``, which is itself a
        readable response object, so it is unified here rather than raised.

        Args:
          request: The prepared request.

        Returns:
          The status, the response payload, and any ``Retry-After`` delay.

        Raises:
          urllib.error.URLError: On a connection-level failure, for the retry loop to handle.
          OSError: Likewise.
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
        """Waits before the next attempt, honouring a usable ``Retry-After``.

        The server's value is clamped and sign-checked first (SPEC-027 FR-001): it is advice
        from the destination, not an instruction the application must obey. Unbounded, a
        measured ``Retry-After: 8`` held ``shutdown()`` for 22 s and a header of ``86400`` would
        have held it for a day, while a negative one made ``time.sleep`` raise, which the orphan
        path handed to the caller.

        Args:
          attempt: The zero-based attempt just made, which sets the exponential delay.
          retry_after: The server's requested delay, or ``None``. Anything rejected falls back
            to this sink's own backoff, which is what the destination would have got had it sent
            no header at all.

        Returns:
          None.

        Raises:
          None.
        """
        server = clamp_server_delay(retry_after, self.max_retry_after)
        delay = server if server is not None else _BACKOFF_BASE * (2**attempt)
        wait(delay, self.stop_signal)

    def _abandon(self, reason: str) -> NoReturn:
        """Counts and logs a request abandoned past the retry bound, then raises (FR-012).

        The counter moves before the raise: ``failed`` is this sink's own record of what it
        could not put on the wire, and it must not depend on who catches what. It therefore
        counts every worker retry attempt too, making it an upper bound on loss rather than a
        count of it — see :class:`~log_foundry.sinks.base.SinkLosses`.

        Args:
          reason: Why the request was abandoned. It carries only library-controlled values — an
            HTTP status, an exception type, an ``errno`` — never a server-supplied body or an
            exception's text (SPEC-029 FR-002), and it is also the raised error's message for
            the same reason.

        Returns:
          Never returns.

        Raises:
          SinkDeliveryError: Always.
        """
        with self._counter_lock:
            self.failed += 1
        detail = f"{type(self).__name__}, {self.max_retries + 1} attempt(s), {reason}"
        _diag.lost("request", 1, detail)
        raise SinkDeliveryError(detail)


def _parse_retry_after(headers: Any) -> float | None:
    """Parses a ``Retry-After`` delay in seconds.

    Args:
      headers: The response headers, or ``None``.

    Returns:
      The delay, or ``None`` if the header is absent or in the HTTP-date form this does not
      parse, in which case the caller falls back to exponential backoff.

    Raises:
      None.
    """
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
