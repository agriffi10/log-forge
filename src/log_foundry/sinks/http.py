"""HTTPSink — POST batches over the stdlib ``urllib`` (arch §8, §9.1, SPEC-009)."""

from __future__ import annotations

import gzip as _gzip
import http.client
import json
import math
import re
import threading
import unicodedata
import urllib.error
import urllib.request
from base64 import b64encode
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, NoReturn, Protocol, TypedDict, cast
from urllib.parse import unquote, unwrap, urlparse

from log_foundry import _diag
from log_foundry.sinks import _engine
from log_foundry.sinks._args import require_callable
from log_foundry.sinks._classify import is_retryable
from log_foundry.sinks._engine import RunContext, When
from log_foundry.sinks._retry import (
    exact_seconds,
    floor_count,
    require_positive,
    require_timeout,
)
from log_foundry.sinks.base import (
    _ABSENT,
    _MRO,
    _NAMESPACE,
    SinkDeliveryError,
    SinkLosses,
    _class_member,
    _own_attribute,
)
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict

__all__ = [
    "HTTPAuthKwargs",
    "HTTPForwardKwargs",
    "HTTPKwargs",
    "HTTPPlatformKwargs",
    "HTTPRetryKwargs",
    "HTTPSink",
    "merge_headers",
]

DEFAULT_MAX_BATCH_COUNT = 1000
"""Entries one request may carry, unless a subclass documents its destination's own (FR-001).

A thousand is Datadog's published array limit and a conservative default everywhere else: no
destination in this family documents a *smaller* count, and a count bound is the cheap half of
the guard, since it holds whatever the per-event size turns out to be.
"""

DEFAULT_MAX_BATCH_BYTES = 5_000_000
"""Bytes one request's body may reach, unless a subclass documents its destination's own.

Five million is the largest limit published by any sink in this family (Datadog's uncompressed
payload ceiling), so it is the right *base-class* default for an arbitrary endpoint the library
knows nothing about, while every subclass that has a documented figure sets its own below it.
"""

MAX_BUDGET_REDUCTIONS = 24
"""``413``-driven halvings of the byte budget one run may make (FR-001 AC-4, SPEC-073 FR-001).

The search state — the budget, the smallest refused size and this count — is kept in the run's
notes (``RunContext.notes``), so a later round's resend starts from the budget already found and
one run searches no more than one 1.x ``emit`` did.

A ``413`` says the destination's real limit is below the configured budget, and the search for a
size it accepts is a **halving of the budget**, not a halving of the chunk. The distinction is
what bounds the cost. Halving the *chunk* recursively is ``2N-1`` requests for ``N`` events —
measured at 11,954 for one 5,980-event exit backlog against an endpoint refusing everything —
because each accepted size is rediscovered independently in every branch. Halving the *budget*
re-chunks the remaining work once per reduction, so a destination limit any ratio below the
budget is found in ``log2(ratio)`` wasted requests: 8 for a 5 MB default against a
20 kB endpoint, a ratio of 250, against ~2,000 for the recursion.

Each reduction halves **the refused body's own size**, not the nominal budget. Halving the budget
converges on whatever it was configured to be rather than on what was actually sent, so a 36-byte
body refused under a 5,000,000-byte budget took twenty reductions to reach the size that had
already been rejected. Halving what was refused reaches it immediately.

Twenty-four is a backstop, not a policy limit: the loop terminates on its own, because a chunk
reduced to a single item takes the permanent-drop path instead. It exists against a pathological
``_item_size``, which is why it sits far above what any real destination requires.
"""


@dataclass(frozen=True)
class _Item:
    """One event rendered into this sink's per-event wire form, with its measured size.

    Rendering once and chunking over the result is the ``KinesisSink``/``FirehoseSink`` idiom:
    it costs one ``json.dumps`` per event rather than one to measure and another to build the
    body, and it makes the byte budget exact rather than estimated.

    Attributes:
      event: The source event, kept so a :meth:`HTTPSink._body` that groups rather than
        concatenates — Loki's, which re-forms streams per chunk — can read the fields it groups
        on without re-parsing what :meth:`HTTPSink._render` just serialized.
      rendered: The event's serialized form, without the separator that joins it to its
        neighbours.
      size: ``rendered``'s UTF-8 length plus one byte for that separator. The two bytes of a
        JSON array's brackets are not modelled, which is why the budget is a bound on the body
        rather than its exact length.
    """

    event: dict[str, object]
    rendered: str
    size: int


class _PayloadTooLarge(SinkDeliveryError):
    """A ``413`` for a request the caller may be able to make smaller (FR-001 AC-4).

    It subclasses ``SinkDeliveryError`` so that every pre-existing caller of :meth:`HTTPSink._send`
    — ``SentrySink`` above all — keeps catching it exactly as before. Only a caller passing
    ``splittable=True`` ever sees it as its own type; the round reads a ``413`` from
    :meth:`HTTPSink._exchange` directly.

    Attributes:
      size: The refused body's length in bytes, which lets one run avoid building another
        request that large. Without it a destination refusing everything costs one request per
        event — ~5,980 for one exit backlog — each asking a question already answered.
    """

    def __init__(self, message: str, size: int = 0) -> None:
        """Records the refused size alongside the message.

        Args:
          message: The abandonment detail, carrying only library-controlled values (arch §6).
          size: The refused body's length in bytes.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init__(message)
        self.size = size


class _UnreadBody(bytes):
    """A ``2xx`` payload whose body could not be read: equal to ``b""``, and telling (FR-004).

    :meth:`HTTPSink._attempt` returns one in place of ``b""`` when a body read failed after the
    status line, so the flag travels in the value :meth:`HTTPSink._handle_response` already
    receives and the hook's signature is unchanged (SPEC-070 FR-004). A keyword parameter was the
    alternative and was rejected: an override with the two-argument signature, or a
    ``*args, **kwargs`` wrapper forwarding to one, would raise ``TypeError`` exactly when a body
    fails to read — under 1.x out of ``emit``, for the worker to re-send whole, and on the engine
    into the round's reader guard, which would count the chunk unadjudicated even where the
    override meant success. An override that does not ask :func:`_read_failed` sees ``b""``, as
    under ``1.x``; it has no content, and ``json.loads``, ``len`` and slicing behave on it as on
    ``b""``. ``_outcomes`` receives the same payload.
    """

    __slots__ = ()


def _read_failed(payload: bytes) -> bool:
    """Reports whether a ``2xx`` payload stands for a body that could not be read (FR-004).

    Args:
      payload: What :meth:`HTTPSink._handle_response` was handed.

    Returns:
      True for :class:`_UnreadBody`; False for any other payload, ``b""`` included, so an empty
      body and an unreadable one are told apart only where this is asked.

    Raises:
      None.
    """
    return isinstance(payload, _UnreadBody)


class _Pending(Protocol):
    """What :meth:`HTTPSink._items` and :meth:`HTTPSink._drop_refused` record a drop into.

    Both a ``Deferral`` and a run context's ``drops`` view fit it, so the two methods keep their
    1.x ``pending`` parameter: a drop recorded here is counted only by a run that settles, as it
    was only by an ``emit`` that returned.
    """

    def count(self, counter: str, n: int = 1) -> None:
        """Adds ``n`` to the owner's ``counter``, pending a settled run.

        Args:
          counter: The owner's attribute.
          n: How much to add.

        Returns:
          None.

        Raises:
          None.
        """

    def announce(self, line: Callable[[], None]) -> None:
        """Registers a line written once the counts are committed.

        Args:
          line: A ``lambda`` wrapping a ``_diag`` call.

        Returns:
          None.

        Raises:
          None.
        """


@dataclass(frozen=True)
class _Answer:
    """How :meth:`HTTPSink._exchange` classified one request: the one table both callers read.

    Attributes:
      outcome: The fate of the request's events; ``None`` for a ``413`` the caller may split.
      payload: The ``2xx`` body, or the response body otherwise.
      retry_after: The server's ``Retry-After``, for a ``RETRYABLE`` status.
      detail: The abandonment detail when the request was counted, else ``""``.
      error: An exception the predicate refused and the caller counts itself
        (``count_unexpected=False``), to be re-raised unchanged.
    """

    outcome: Outcome | None
    payload: bytes = b""
    retry_after: float | None = None
    detail: str = ""
    error: Exception | None = None


@dataclass
class _Sent:
    """What :meth:`HTTPSink._send`'s one-request rounds leave behind, read once the run ends.

    Attributes:
      answer: The last request's classification, or ``None`` before any.
      unbuilt: An exception building the request raised, re-raised unchanged after the run.
    """

    answer: _Answer | None = None
    unbuilt: Exception | None = None


DEFAULT_MAX_RETRY_AFTER = 30.0
"""Ceiling on a server-supplied ``Retry-After``, in seconds (SPEC-027 FR-001).

Thirty rather than something smaller because a rate-limited platform asking for half a minute is
making a reasonable request, and rather than something larger because the total is what a caller
with an execution deadline pays: at the default retries the worst case is 90 s, which stays
inside a typical serverless timeout while leaving room for the request itself.
"""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Declines every redirect, so a ``3xx`` reaches the sink as an ``HTTPError`` (SPEC-048).

    ``urlopen``'s default opener follows ``301``, ``302`` and ``303`` on a ``POST`` by rewriting
    the method to ``GET`` and dropping the body, while keeping every header — so a collector
    behind a load balancer that redirects to ``https://`` silently discarded every batch and
    forwarded the ``Authorization`` header to whatever host the redirect named, and the sink read
    the redirect target's ``200`` as delivery. ``307`` and ``308`` were already refused for a
    ``POST`` by the base class.
    """

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        """Declines the redirect, leaving the default error handler to raise ``HTTPError``.

        Returning ``None`` is ``urllib``'s documented "I cannot handle this, let another handler
        try", and no other handler does, so ``HTTPDefaultErrorHandler`` raises — which
        :meth:`HTTPSink._attempt` already unifies into a status, putting a ``3xx`` on the same
        counted, announced path as a ``4xx``.

        Args:
          req: The request that was redirected.
          fp: The response file object.
          code: The ``3xx`` status.
          msg: The status message.
          headers: The response headers.
          newurl: The redirect target, deliberately unused and never contacted.

        Returns:
          None, always — which is what declines the redirect. A bare ``return`` rather than
          ``return None`` only because ``ruff``'s ``RET501`` refuses the explicit form; the value
          urllib receives is the same.

        Raises:
          None.
        """
        return


def _no_redirect_opener() -> Callable[..., Any]:
    """Builds an opener that refuses redirects, for a sink the caller gave no ``opener=``.

    Built per sink at construction rather than once at import, for two reasons. ``python.md`` §15
    forbids a module doing real work at import time, which is why the ``StdoutSink`` default and
    the worker are lazy too. And ``build_opener`` snapshots ``ProxyHandler``'s environment, so an
    import-time opener would pin whatever ``http_proxy`` said when ``log_foundry`` was first
    imported — a sink built after the application sets its proxy would silently ignore it, where
    ``urlopen``'s lazily-built global opener would not. Construction time is when the caller is
    standing there, so it is the honest moment to read the environment.

    A sink is constructed once, so the cost is one object per sink and never per request.

    Args:
      None.

    Returns:
      A ``urlopen``-shaped callable that raises ``HTTPError`` on any ``3xx``.

    Raises:
      None.
    """
    return urllib.request.build_opener(_NoRedirect()).open


class HTTPForwardKwargs(TypedDict, total=False):
    """The :class:`HTTPSink` keywords every platform sink in this family can forward.

    Five shapes, composed by inheritance so each key is declared exactly once, replace the
    ``**http_kwargs: object`` that let ``timeout="not-a-float"`` past ``mypy`` and past
    construction (SPEC-051 FR-005). A platform sink cannot forward a keyword it sets itself or
    shadows with a parameter of its own — ``mypy`` refuses a ``**kwargs`` TypedDict key that
    collides with a named parameter — so this is the intersection and the four below widen it.
    Every annotation matches ``HTTPSink.__init__``'s, and a roster test holds them there.
    """

    method: str
    headers: dict[str, str] | None
    gzip: bool
    max_retry_after: float
    max_batch_count: int | None
    max_batch_bytes: int | None
    opener: Callable[..., Any] | None


class HTTPRetryKwargs(HTTPForwardKwargs, total=False):
    """:class:`HTTPForwardKwargs` plus the two a sink with a single transport does not own."""

    timeout: float
    max_retries: int


class HTTPAuthKwargs(HTTPForwardKwargs, total=False):
    """:class:`HTTPForwardKwargs` plus ``auth``, for a sink taking no credentials of its own."""

    auth: str | tuple[str, str] | None


class HTTPPlatformKwargs(HTTPRetryKwargs, HTTPAuthKwargs, total=False):
    """Both of those: for a sink that owns only the body format."""


class HTTPKwargs(HTTPPlatformKwargs, total=False):
    """Every keyword :class:`HTTPSink` accepts besides its positional ``url``."""

    body_format: str


_ALLOWED_SCHEMES = ("http", "https")

_BODY_FORMATS = ("ndjson", "json_array")

_CRLF = ("\r", "\n")

_GZIP_CODINGS = ("gzip", "x-gzip")
"""The content-codings that name gzip: RFC 9110 §8.4.1.3 has ``x-gzip`` read as ``gzip``."""

_NO_CODINGS = ("", "identity")
"""The values naming no content-coding: ``identity``, and the empty list (RFC 9110 §8.4.1)."""

_HeaderValue = str | int | bytes
"""A caller header value as stored: an exact ``str``, ``int``, ``bool`` or ``bytes``.

:func:`_sent_value` makes it. The public ``headers`` parameters stay ``dict[str, str]``, as 1.x
declared them; this is what the copy actually holds, since an ``int`` or ``bytes`` value has
always been sent as ``http.client`` sends it."""

_MAX_PORT = 65535

_AUTH_REMEDY = "auth=(user, password), auth=<token>, or an Authorization header in headers="
"""Where a caller's credentials go instead of a URL, for a sink taking none of its own."""

_AUTH_BESIDE_HEADER = (
    "auth and an Authorization header cannot both be given, since each sets the one "
    "Authorization header a request carries; drop auth= or the header"
)
"""Why ``auth`` is refused beside an ``Authorization`` header, for a sink setting none itself."""

_URLSPLIT_DROPS = {ord("\t"): None, ord("\r"): None, ord("\n"): None}
"""What ``urlsplit`` deletes from a URL before it parses; ``urllib`` keeps all three."""

_URLSPLIT_LEADING = "".join(chr(code) for code in range(0x21))
"""What ``urlsplit`` strips from the front of a URL first: the C0 controls and the space."""

_AUTHORITY = re.compile(r"(?:[^/:]*:)?(?://)?([^/?#]*)", re.DOTALL)
"""The run after a scheme's ``:`` and an optional ``//``, up to the first ``/``, ``?`` or ``#``.

Every part is optional, so it matches any string. Where both parsers find a scheme and a host it is
the netloc ``urlsplit`` reads and the host ``urllib``'s ``_splithost`` reads; where they do not, it
is the run that would have been the host, which is what :func:`_carries_userinfo` tests. It is not
what a refusal quotes: the scheme group mirrors ``urllib``'s ``_splittype`` and runs to the first
``:`` over ``?``, ``#`` and ``%``, so in ``es.example.com?api_key=Splunk:SECRET`` the run is text
after the key. A refusal quotes :data:`_QUOTABLE_HEAD` instead.
"""

_QUOTABLE_HEAD = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://([^/?#]*)")
"""A URL's scheme, its ``//`` and its host, at the very start of it: what a refusal may quote.

The scheme is RFC 3986's characters, so no ``?``, ``#``, ``%`` or ``:`` is read as part of one, and
the host — the group — runs to the first ``/``, ``?`` or ``#``, the delimiters ``urlsplit`` and
``_splithost`` both end it at. Those are ASCII only, so a host run holding a delimiter in another
form — a fullwidth question mark, number sign or solidus, which CJK input methods type and NFKC
turns into the ASCII one, or a percent-encoded one — may run past where a reader ends the host,
and is not quoted (:func:`_holds_delimiter`). It is matched with nothing stripped first: a URL
that starts with anything else — a control character, a tab inside ``http:\\t//`` — has no host
this can tell apart, and is not quoted at all.
"""

_URL_DELIMITERS = "/?#"
"""What ends a URL's host and starts its path, query or fragment."""


def _started_request(host: str, method: str, selector: str) -> http.client.HTTPConnection:
    """Starts a request head the way ``urllib`` does, without opening a socket.

    ``urllib`` sends its own ``Host`` header, so ``http.client`` skips the one it would otherwise
    write — and with it the IDNA encoding and port handling of that header. The head started here
    skips it too, so a caller then writes ``Host`` as ``urllib`` does: the raw host, through
    ``putheader``'s Latin-1 encoding. Nothing is sent: ``putrequest`` and ``putheader`` only
    validate, encode and buffer, and the connection is discarded unopened.

    Args:
      host: The host the connection is built for, port included.
      method: The request method.
      selector: The request target.

    Returns:
      A connection whose request line has been validated and buffered.

    Raises:
      ValueError: If ``http.client`` cannot encode or accept the request line.
      http.client.InvalidURL: If the host names a port ``http.client`` cannot read.
    """
    connection = http.client.HTTPConnection(host)
    connection.putrequest(method, selector, skip_host=True, skip_accept_encoding=True)
    return connection


def _carries_userinfo(url: str) -> bool:
    """Says whether either parser would read a user or password before the URL's host.

    ``urllib`` never turns userinfo into an ``Authorization`` header: it keeps it as part of the
    host, so ``http://u:pw@h:9200`` asked the resolver for ``u:pw@h`` on every request, and through
    an HTTP proxy put ``u:pw@`` in the request line and ``Host`` header. The run tested is
    :data:`_AUTHORITY`'s, after :func:`urllib.parse.unwrap` (which ``urllib`` applies first) and
    after what ``urlsplit`` does first: strip :data:`_URLSPLIT_LEADING` and delete
    :data:`_URLSPLIT_DROPS`. Neither removes an ``@`` or adds a delimiter, and a leading control
    that is stripped can only have hidden a ``//`` from the pattern, so this one view sees every
    ``@`` the tab-keeping ``urllib`` view does as well as the ones ``urlsplit`` finds in
    ``http:\\t//u@h`` and ``\\x00//u@h``. It counts as userinfo if it holds an ``@`` as written;
    once percent-decoded, because ``urllib`` unquotes the host once (``Request.host``), so
    ``http://u%3Apw%40h:9200`` connected to ``u:pw@h``; or under NFKC, which ``urlsplit`` applies
    to refuse a netloc that normalizes to an ``@`` — the fullwidth ``＠``. A ``%2540`` decodes
    once to ``%40``, which is a host character and not a separator, so it is not refused, and an
    ``@`` in a path, query or fragment is outside the run.

    Args:
      url: The URL as the caller wrote it.

    Returns:
      ``True`` if the run holds an ``@`` as written, unquoted once, or under NFKC of either.

    Raises:
      None.
    """
    found = _AUTHORITY.match(unwrap(url).lstrip(_URLSPLIT_LEADING).translate(_URLSPLIT_DROPS))
    return _holds_at(found.group(1) if found else "")


def _holds_at(text: str) -> bool:
    """Says whether text holds an ``@`` as written, unquoted once, or under NFKC of either.

    Args:
      text: The text to test — a URL's authority run, or a whole URL.

    Returns:
      ``True`` if any of the four forms contains ``@``.

    Raises:
      None.
    """
    return any(
        "@" in form or "@" in unicodedata.normalize("NFKC", form) for form in (text, unquote(text))
    )


def _holds_delimiter(text: str) -> bool:
    """Says whether text holds a ``/``, ``?`` or ``#`` as written, unquoted once, or under NFKC.

    The same four forms :func:`_holds_at` reads, for the same reason: a reader that normalizes or
    decodes before it splits ends a host at a delimiter the ASCII text does not show, and
    ``urlsplit`` refuses a netloc NFKC turns into one, quoting it.

    Args:
      text: The text to test — a host run, a Datadog site or a Honeycomb dataset.

    Returns:
      ``True`` if any of the four forms contains one of :data:`_URL_DELIMITERS`.

    Raises:
      None.
    """
    return any(
        char in form or char in unicodedata.normalize("NFKC", form)
        for form in (text, unquote(text))
        for char in _URL_DELIMITERS
    )


def _quotable_head(url: str) -> str | None:
    """Returns the URL's scheme and host as a refusal may quote them, or ``None`` if it may not.

    The head is :data:`_QUOTABLE_HEAD`'s match. A URL holding an ``@`` **anywhere**, in any form
    :func:`_holds_at` reads, has no quotable head, though the head itself holds none. The literal
    ``@`` is refused before any refusal that quotes, but a ``%40`` or a fullwidth ``＠`` is not,
    and either can end a password whose ``#``, ``?`` or ``/`` ended the host early:
    ``http://elastic:pa#ss%40es:9200/x`` has the head ``http://elastic:pa``, which is the user
    and the start of the password. Testing only the head would quote them; testing the whole URL
    quotes nothing whenever one may be there. A host run holding a delimiter in any form
    :func:`_holds_delimiter` reads has no quotable head either, since the host may end inside it:
    ``https://es.example.com？api_key=SECRET/x`` has the ASCII host run
    ``es.example.com？api_key=SECRET``, and ``http://h＃SECRET``, all head, was quoted whole with
    ``urlsplit``'s reason chained, which quotes the netloc.

    Args:
      url: The URL as ``urllib`` unwraps it.

    Returns:
      The head, or ``None`` when the URL does not start with one, holds an ``@`` anywhere, or its
      host run holds a delimiter in any form.

    Raises:
      None.
    """
    found = _QUOTABLE_HEAD.match(url)
    if found is None or _holds_at(url) or _holds_delimiter(found.group(1)):
        return None
    return found.group()


def _refuse_url(
    owner: str, name: str, reason: str, url: str, head: str | None, err: Exception | None = None
) -> NoReturn:
    """Raises a refusal of a URL that quotes no more of it than its scheme and host.

    A path, query or fragment is where a credential sits — ``?api_key=…``, ``#access_token=…``,
    ``/services/<token>`` — and a URL refused for any reason printed all of them, and the parser's
    reason it chained could quote them too: ``http.client``'s ``InvalidURL`` quotes the request
    target, query included, and a ``UnicodeEncodeError``'s ``object`` is the whole request line.
    So the message quotes the head (:func:`_quotable_head`) and nothing after it, and gives the
    parser's error by its type name alone, chaining nothing. A URL that is all head quotes it
    whole, with the parser's reason, chained, since there nothing follows the host to leak and the
    reason is the useful part. A URL with no quotable head is quoted not at all, and the message
    says which of :func:`_quotable_head`'s two reasons applied: no head to tell apart, or an ``@``
    somewhere. It is raised from a function called outside any ``except``, so a refusal that
    chains nothing has no ``__context__`` either.

    Args:
      owner: The class refusing the URL, for the message.
      name: The argument the message names.
      reason: Why it is refused, quoting nothing outside ``head``.
      url: The URL refused, as ``urllib`` unwraps it.
      head: :func:`_quotable_head` of ``url``.
      err: The parser's error behind the refusal, if one raised.

    Returns:
      None; it always raises.

    Raises:
      ValueError: Always.
    """
    if head is None:
        why = (
            "it holds an '@', and what precedes one may be a password"
            if _holds_at(url)
            else "its host cannot be told apart from the rest of it"
        )
        raise ValueError(f"{owner} {name} {reason}; the URL is not quoted, since {why}") from None
    if head == url:
        detail = f" ({err})" if err is not None else ""
        raise ValueError(f"{owner} {name} {reason}{detail}: {url!r}") from err
    detail = f" ({type(err).__name__})" if err is not None else ""
    raise ValueError(
        f"{owner} {name} {reason}{detail}: {head!r}; only the scheme and host are quoted, since a "
        f"path, query or fragment may hold a credential"
    ) from None


def _refuse_stray_at(url: str, owner: str, name: str) -> None:
    """Raises if the URL holds a literal ``@`` anywhere, never echoing the URL or chaining anything.

    It runs right after :func:`_refuse_userinfo`, which has refused every ``@`` in the host, so any
    ``@`` left is outside it — and one there may be a password that ended the host early:
    ``http://elastic:12#ss@es:9200/x`` reads as host ``elastic:12``, so it constructed and every
    request went to a host the caller never named, ``?`` and ``/`` alike. The two cannot be told
    apart, so the message says the ``@`` *may* be one. An ``@`` that really belongs in a path,
    query or fragment is written ``%40``, which sends the same request to any server that decodes
    it, so the message names that remedy. ``%40`` and a fullwidth ``＠`` are not refused here: the
    first is the remedy, and the second is no ``@`` to ``urllib``. In a path or query a fullwidth
    one is non-ASCII, which ``http.client`` refuses; in a fragment, which is never sent, it is not
    refused at all — ``HTTPSink("http://h/x#a＠b")`` constructs. A refusal of a URL holding either
    anywhere quotes nothing of it (:func:`_quotable_head`).

    Args:
      url: The URL as ``urllib`` unwraps it.
      owner: The class refusing it, for the message.
      name: The argument the message names.

    Returns:
      None.

    Raises:
      ValueError: If the URL holds an ``@``.
    """
    if "@" in url:
        raise ValueError(
            f"{owner} {name} must not hold an '@' outside its host: an '@' there may be a "
            f"password whose '#', '?' or '/' ended the host early, sending the request to another "
            f"host; write an '@' that belongs in a path or query as '%40'"
        )


def _refuse_userinfo(url: str, owner: str, name: str, credentials: str) -> None:
    """Raises if the URL carries a user or password, never echoing the URL or chaining anything.

    It runs before every other check of a URL, because every other refusal quotes the URL's host
    and ``urlsplit``'s own errors quote the netloc — a password included. The message names the
    argument the caller wrote and where credentials go instead; it holds nothing derived from the
    URL, and it is raised outside any ``except``, so neither ``__cause__`` nor ``__context__`` is
    set.

    Args:
      url: The URL as the caller wrote it.
      owner: The class refusing it, for the message.
      name: The argument the message names.
      credentials: Where this class takes credentials instead, for the message.

    Returns:
      None.

    Raises:
      ValueError: If :func:`_carries_userinfo` finds a user or password before the host.
    """
    if _carries_userinfo(url):
        raise ValueError(
            f"{owner} {name} must not contain a user or password (user:password@host): urllib "
            f"would send them as part of the host name, not as credentials; authenticate with "
            f"{credentials} instead"
        )


def _valid_url(
    url: str,
    owner: str,
    name: str = "url",
    credentials: str = _AUTH_REMEDY,
    *,
    path_at: bool = False,
) -> str:
    """Returns a URL this family can actually POST to, or raises (SPEC-049 FR-001).

    A scheme ``urllib`` will open but this sink cannot use is a configuration error that surfaced
    at the first emit and never stopped: ``file:///etc/passwd`` raised a raw ``TypeError`` out of
    every batch, and ``ftp://`` burned the whole retry budget on each one. Checked on the URL
    :class:`HTTPSink` receives, which is the URL every subclass has finished building, so no
    subclass can smuggle one past.

    Surrounding whitespace is **floored**, not refused: ``urllib`` passes every URL through
    :func:`urllib.parse.unwrap`, which strips it and a ``<URL:…>`` wrapper, before it parses, so
    such a URL already delivered. The floor is that same call, so the stored URL is exactly the one
    that went on the wire. Anything ``http.client`` cannot carry is **refused**, because it never
    delivered once. A lone surrogate or any other non-ASCII character outside the host raised
    ``UnicodeEncodeError`` out of every ``emit`` with ``failed == 0`` and no stderr line; a space or
    control character raises ``InvalidURL``, which :meth:`HTTPSink._send` counts as a connection
    error — on every request, for the life of the process. Rather than list those characters, the
    check builds the request line the way ``urllib`` would — the connection ``do_open`` builds for
    the request's host, then ``putrequest`` and the ``Host`` header ``urllib`` writes itself
    (:func:`_started_request`), which encode and validate without opening a socket — so the refusal
    tracks ``http.client``'s own rules instead of a copy of them. That header is why a host
    ``http.client`` would IDNA-encode is refused anyway when it is not Latin-1: ``urllib`` sends the
    raw host, and ``http://例.example/x`` raised ``UnicodeEncodeError`` out of every ``emit``. The
    host and the port are read from those same objects rather than from :func:`urlparse`, which
    drops a tab or newline that ``urllib`` keeps and parses a port more strictly than
    ``http.client`` does. A missing host, or a port of ``0`` or past ``65535``, is refused because
    every request fails, counted, for the life of the process, and that is knowable here.

    The checks run in a fixed order. A user or password before the host is refused first, by
    :func:`_refuse_userinfo`, because every refusal after it quotes the host; ``http.client`` read
    ``user:password@h`` as a port before that check existed, and the refusal it gave printed the
    password. A literal ``@`` anywhere else is refused second, by :func:`_refuse_stray_at`, because
    one there is most often a password whose ``#``, ``?`` or ``/`` ended the host before its ``@``,
    sending every request to a host the caller never named. Only then the rest, each of which
    quotes the URL's scheme and host and nothing after them (:func:`_refuse_url`). ``SentrySink``
    alone skips the second check, with ``path_at``: an ``@`` in a DSN's path prefix is a path the
    SDK delivers to, and the ingest URL it builds can hold no other literal ``@``.

    Args:
      url: The endpoint as constructed.
      owner: The class refusing it, for the message.
      name: The argument the message names — ``url`` here, the argument a subclass built the URL
        from where it checks that argument itself first.
      credentials: Where the class takes credentials instead, for a URL carrying userinfo.
      path_at: Whether a literal ``@`` outside the host is let through; ``SentrySink``'s only.

    Returns:
      The URL as ``urllib`` unwraps it, without surrounding whitespace.

    Raises:
      ValueError: If the URL carries a user or password before its host (:func:`_refuse_userinfo`)
        or, unless ``path_at``, an ``@`` anywhere else (:func:`_refuse_stray_at`) — both in fixed
        text that echoes nothing of the URL — or the scheme is not ``http`` or ``https``, the URL
        names no host — every request to ``http:///x`` fails with a counted ``URLError`` for the
        life of the process, which is knowable here (SPEC-049, system-frame review) — its port is
        not one a socket can reach, or ``http.client`` cannot build a request line for it. Each of
        those quotes the URL's scheme and host and gives the parser's error by its type alone,
        chaining nothing, unless nothing follows the host, when it quotes the URL with the parser's
        reason, chained, or the URL has no host it can tell apart, when it quotes nothing
        (:func:`_refuse_url`).
    """
    url = unwrap(url)
    _refuse_userinfo(url, owner, name, credentials)
    if not path_at:
        _refuse_stray_at(url, owner, name)
    head = _quotable_head(url)
    try:
        parsed = urlparse(url)
    except ValueError as err:
        unparsed: ValueError | None = err
    else:
        unparsed = None
    if unparsed is not None:
        _refuse_url(owner, name, "is not a URL", url, head, unparsed)
    scheme = parsed.scheme.lower()
    if scheme not in _ALLOWED_SCHEMES:
        reason = "must use http or https"
        if head is not None:
            reason = f"{reason}, not {scheme or 'no'} scheme"
        _refuse_url(owner, name, reason, url, head)
    request = urllib.request.Request(url)  # noqa: S310
    if request.type.lower() != scheme:
        reason = "must use http or https"
        if head is not None:
            reason = f"{reason}, not {request.type!r}"
        _refuse_url(owner, name, reason, url, head)
    if not parsed.netloc or not request.host:
        _refuse_url(owner, name, "names no host", url, head)
    try:
        connection = _started_request(request.host, "POST", request.selector)
        connection.putheader("Host", request.host)
    except (ValueError, http.client.InvalidURL) as err:
        unsendable: Exception | None = err
    else:
        unsendable = None
    if unsendable is not None:
        _refuse_url(owner, name, "cannot be sent over HTTP", url, head, unsendable)
    if not 0 < connection.port <= _MAX_PORT:
        _refuse_url(owner, name, "names no usable port", url, head)
    return url


def _valid_base_url(
    url: str, owner: str, suffix: str, name: str = "url", credentials: str = _AUTH_REMEDY
) -> str:
    """Returns a base URL a sink can append its own path to, or raises.

    A subclass that appends a path to the caller's base — ``/_bulk``, Loki's push path,
    Honeycomb's ``/1/batch/<dataset>`` — sent every event somewhere other than that path when the
    base carried a query or a fragment: ``http://h?x`` became ``/?x/loki/api/v1/push``, the path
    swallowed into the query string, and ``https://h#f`` became ``''``, the suffix dropped with
    the fragment, and whatever the server answered there was taken as the outcome (invariant 2).
    The test is a character test rather than a parse because ``urlsplit`` ends the path at the
    first ``?`` or ``#`` and ``urllib`` splits the fragment at a ``#``, so **any**
    occurrence moves the suffix out of the path — an empty query (``http://h?``), ``/#``, and a
    ``?`` in what reads as userinfo (``http://u?x@h``, whose netloc is ``u``) included — and
    ``urlsplit`` reports an empty query and an absent one identically. A sink that sends to its
    URL as written appends nothing, so its query goes where the caller put it and is not checked
    here. The URL is not echoed, since a query is where a credential most plausibly sits; the
    message names the argument and the reason instead. Splicing the suffix in front of the query
    was rejected: it sends a request the caller never wrote. A user or password is refused here
    too, on the base as written, rather than left to :func:`_valid_url`: appending the suffix to a
    wrapped base (``<URL:http://u:pw@h>``) leaves a string ``unwrap`` no longer strips, whose
    userinfo the composed URL hides and whose scheme refusal then echoed it.

    Args:
      url: The base URL as the caller wrote it.
      owner: The class refusing it, for the message.
      suffix: The path the class appends, for the message.
      name: The argument the message names.
      credentials: Where the class takes credentials instead, for a base carrying userinfo.

    Returns:
      The URL unchanged; the composed URL is still checked by :func:`_valid_url`.

    Raises:
      ValueError: If the URL carries a user or password before its host
        (:func:`_refuse_userinfo`), or contains ``?`` or ``#``.
    """
    _refuse_userinfo(url, owner, name, credentials)
    if "?" in url or "#" in url:
        raise ValueError(
            f"{owner} {name} cannot carry a query or fragment: the {suffix!r} this sink appends "
            f"would land inside it rather than in the path; pass the base URL alone"
        )
    return url


def _valid_method(method: str, owner: str) -> str:
    """Returns a method ``http.client`` can send, or raises (SPEC-049 FR-001).

    A control character or a non-ASCII character in the method made ``putrequest`` raise
    ``ValueError`` or ``UnicodeEncodeError`` out of every ``emit``, uncounted — the same gap
    :func:`_valid_url` closes, refused here by the same means: ``putrequest`` itself, with no
    socket opened.

    Args:
      method: The HTTP method as constructed.
      owner: The class refusing it, for the message.

    Returns:
      The method unchanged.

    Raises:
      ValueError: If ``http.client`` cannot put the method on a request line.
    """
    try:
        _started_request("localhost", method, "/")
    except ValueError as err:
        raise ValueError(f"{owner} method cannot be sent over HTTP ({err}): {method!r}") from err
    return method


def _valid_headers(headers: dict[str, str], owner: str) -> dict[str, _HeaderValue]:
    """Returns headers that cannot inject a second header, or raises (SPEC-049 FR-001).

    A ``CR`` or ``LF`` in a name or a value makes ``http.client`` raise from inside ``emit``, on
    every batch, uncounted. Rejecting it here also means the sink cannot be used to smuggle a
    header a caller did not intend, which is why the check covers names as well as values. Every
    other header ``putheader`` refuses — a value outside Latin-1, a name that is not a token — is
    the same uncounted raise, so each header is then put on a started head, under the name
    ``urllib`` sends: ``capitalize`` in ``Request.add_header``, then ``title`` in ``do_open``,
    which together turn ``x-ß`` into the ASCII ``X-Ss`` that delivered. The message names the
    header and never echoes its value, which is as likely to be a credential as not.

    **A name holding whitespace anywhere is refused** (1.2.0), last. ``http.client`` refuses
    whitespace only as a name's first character, and not even all of it there — its pattern's
    ``\\s`` is the ASCII six, so a leading ``0x1C``–``0x1F`` passed — and sends any other verbatim,
    ``Authorization : V`` included. RFC 9112 §5.1 has a server answer whitespace between a field
    name and its colon with ``400``, so such a name was already broken against any conforming
    server, and a lenient one may trim it into a header the caller never named. Every character
    ``str.isspace`` is true for is refused, in any position; any other character outside the
    token set is not, which is a separate decision. The check runs after the two above so a name
    they already refused keeps its message unchanged — a leading space or tab is still
    ``http.client``'s ``ValueError``, a non-ASCII space its ``UnicodeEncodeError``, a ``CR`` or
    ``LF`` the injection — and this message is the reason only for the names that used to pass.
    A value holding spaces is untouched: ``Bearer abc`` is what values look like.

    **Two names that fold to one are refused** (1.2.0), last of all for the second of them.
    ``urllib`` keeps one value per folded name, the last written, so ``{"X-Foo": "1", "x-foo":
    "2"}`` sent only ``2`` and the first was silently ignored — two ``Authorization`` spellings
    likewise. The message names both names and neither value. A pair colliding only once the
    whitespace is stripped (``"X-Foo"``, ``"X-Foo "``) meets the whitespace refusal first, on
    the second name. A sink's own header never reaches this check beside a caller's spelling of it
    when the sink merged through :func:`merge_headers`: with ``argument=``, as the four shipped
    platform sinks call it, that pair is refused there first, naming the argument the caller
    collided with; without it, the caller's spelling replaces the default there. Two caller names
    folding together are still both passed through, and meet this refusal.

    Every check reads the exact-``str`` copies :func:`_copied_headers` makes, and those copies
    are what is returned and stored, so none of a ``str`` subclass's methods runs after this.

    **A value that is not a** ``str`` **is copied once too** (1.2.0), into the value
    :func:`_sent_value` derives — the bytes ``putheader`` would have put on the wire — and the
    copy is what the ``putheader`` check judges and what is stored. It used to be stored as the
    caller's object, so its ``encode``, its ``__getattribute__``, or an ``int`` subclass's
    ``__str__``, ran inside ``putheader`` on every request, free to answer differently from the
    check here. The ``CR``/``LF`` check still reads ``str(value)`` once, as it always has, so
    which values it refuses is unchanged.

    Args:
      headers: The caller's headers.
      owner: The class refusing them, for the message.

    Returns:
      The headers, every name and ``str`` value copied as the exact ``str`` it stores, and every
      other value as :func:`_sent_value` copied it.

    Raises:
      TypeError: If a header name is not a ``str`` (:func:`_copied_headers`).
      ValueError: If any name or value contains ``CR`` or ``LF``, ``http.client`` cannot send
        the header, a name contains whitespace anywhere, or two names fold to one.
    """
    copied = _copied_headers(headers, owner)
    stored: list[tuple[str, _HeaderValue]] = []
    seen: dict[str, str] = {}
    for name, value in copied:
        for text, part in ((name, "name"), (str(value), "value")):
            if any(bad in text for bad in _CRLF):
                raise ValueError(
                    f"{owner} header {part} must not contain CR or LF: {name!r}"
                )
        sent, failure = _sent_value(value)
        if sent is not None:
            failure = _unsendable_header(_wire_name(name), sent)
        if failure or sent is None:
            raise ValueError(f"{owner} header cannot be sent over HTTP ({failure}): {name!r}")
        if any(char.isspace() for char in name):
            raise ValueError(f"{owner} header name must not contain whitespace: {name!r}")
        wire = _wire_name(name)
        if wire in seen:
            raise ValueError(
                f"{owner} headers {seen[wire]!r} and {name!r} name the same header, which is "
                "sent once; drop one"
            )
        seen[wire] = name
        stored.append((name, sent))
    return dict(stored)


_NOT_CONTIGUOUS = "TypeError"
"""What ``putheader`` raised for a buffer that is not C-contiguous, which its regex cannot read."""


def _sent_value(value: object) -> tuple[_HeaderValue | None, str | None]:
    """Copies a header value once into what ``putheader`` would send, or names why it cannot.

    ``putheader`` turns a value into bytes on every request, in three branches: anything with an
    ``encode`` attribute is ``value.encode("latin-1")``, an ``int`` is ``str(value)`` encoded as
    ASCII, and anything else is passed through as a bytes-like object. This runs the same three
    branches **once**, at construction, in the same order and with the same tests —
    ``hasattr`` and ``isinstance``, not the real-type routing used elsewhere in this module,
    because the point is to reproduce what ``putheader`` did with the value byte for byte — and
    copies the result into exact ``bytes`` through a ``memoryview``, the buffer ``putheader``'s
    own legality check and ``join`` read. ``putheader`` passes exact ``bytes`` through, so every
    request then sends the bytes this computed and runs none of the caller's code.

    An exact ``str`` — every ``str`` value, once :func:`_copied_headers` has copied it — and an
    exact ``int``, ``bool`` or ``bytes`` are returned as given: ``putheader`` runs no caller code
    for one, and none can change, so the caller's object is the plain value already — ``True``
    still goes out as ``True``, an ``int`` as its digits. The four are recognised by identity,
    ``type(value) is str``, never by ``==``, so a metaclass whose ``__eq__`` claims ``str``
    cannot pass the caller's object through. Every other value is the caller's: an ``int``
    subclass whose ``__str__`` is its own, an object whose ``encode`` or ``__getattribute__`` is,
    a ``bytearray`` or ``memoryview`` the caller can still mutate. Each now has those methods
    run once, here, and sends what they answered then, frozen as ``auth`` is.

    A buffer that is not C-contiguous, ``memoryview(b"abcdef")[::2]``, is refused with the
    ``TypeError`` ``putheader``'s legality check raised for it, since that check reads a simple
    buffer, although ``tobytes`` could flatten it: nothing ``putheader`` refused constructs here.

    A failure is returned as its type name rather than raised, as :func:`_unsendable_header`
    returns one, so the refusal chains nothing that carries the value. Only ``ValueError`` and
    ``TypeError`` are caught, the two that check catches, so a value the check refused before is
    refused with the same message, and any other exception a caller's method raises propagates
    as it did from that check.

    Args:
      value: A header value, a ``str`` one already copied as exact ``str``.

    Returns:
      The value to store and ``None``, or ``None`` and the type name of the exception deriving it
      raised.

    Raises:
      None.
    """
    if type(value) is str or type(value) is bytes:
        return value, None
    if type(value) is int or type(value) is bool:
        return value, None
    try:
        if hasattr(value, "encode"):
            derived = cast("Any", value).encode("latin-1")
        elif isinstance(value, int):
            derived = str(value).encode("ascii")
        else:
            derived = value
        with memoryview(derived) as view:
            if not view.c_contiguous:
                return None, _NOT_CONTIGUOUS
            return view.tobytes(), None
    except (ValueError, TypeError) as err:
        return None, type(err).__name__


def _copied_headers(headers: dict[str, str], owner: str) -> list[tuple[str, object]]:
    """Copies every header name, and every ``str`` value, as the exact ``str`` it stores.

    A ``str``-subclass name or value used to be stored as the caller's object, so its methods
    ran at every check and on every ``emit``: a name whose ``capitalize`` folded to ``X-foo``
    at construction and to ``Dd-api-key`` afterwards passed the credential rule and then sent
    the caller's value as ``DD-API-KEY``, and a refusal printed whatever its ``__repr__`` said.
    Every check now judges the copy :func:`_valid_headers` stores, so none of the caller's methods
    runs after this. A name that is not a ``str`` is refused here, naming its real type, where it
    used to raise a bare ``TypeError`` from the first check that read it; a value that is not a
    ``str`` is kept as given here, for :func:`_valid_headers` to copy through :func:`_sent_value`
    once its ``CR``/``LF`` check has read it. The
    copies are a list rather than a ``dict``, so two subclass names with one text cannot collapse
    into one silently: the duplicate check sees both.

    Args:
      headers: The caller's headers, read with ``dict()``.
      owner: The class refusing them, for the message.

    Returns:
      The copied name and value of every header, in order.

    Raises:
      TypeError: If a header name is not a ``str``.
    """
    copied: list[tuple[str, object]] = []
    for name, value in headers.items():
        if not issubclass(type(name), str):
            raise TypeError(f"{owner} header name must be a str, not {_type_name(name)}")
        text = _diag._STR_STR(value) if issubclass(type(value), str) else value
        copied.append((_diag._STR_STR(name), text))
    return copied


def _wire_name(name: str) -> str:
    """Returns the name ``urllib`` puts on the wire for a header the caller wrote as ``name``.

    ``Request.add_header`` stores it ``capitalize``-d and ``do_open`` then sends it ``title``-d,
    so ``authorization`` and ``AUTHORIZATION`` both go out as ``Authorization`` — HTTP header
    names are case-insensitive, and ``urllib`` keeps one value per folded name, the last written.

    Args:
      name: The header name as the caller wrote it.

    Returns:
      The folded name.

    Raises:
      None.
    """
    return name.capitalize().title()


def _caller_encoding(headers: dict[str, _HeaderValue]) -> tuple[str, str] | None:
    """Returns the caller's ``Content-Encoding`` name, and the text its value sends.

    Read once, at construction, by :func:`_valid_encoding` and by ``HTTPSink.__init__``, which
    fixes from it whether the body is compressed (:meth:`HTTPSink._compresses`), so the refusal
    and every request read the header one way. :func:`_valid_headers` has already refused two
    names that fold together, so at most one matches. The name comes back as exact ``str``. The
    value is read by **the bytes it sends**, never by its type (:func:`_wire_text`):
    ``b"identity"`` says ``identity`` on the wire, so it is read as ``identity``, and ``5`` says
    ``5``, a coding named ``5``. Judging by type used to read
    every value that was not a ``str`` as naming nothing, which refused ``b"identity"`` beside
    ``gzip=False`` and ``b"gzip"`` beside ``gzip=True`` though each label was true.

    Args:
      headers: The caller's headers, already judged and copied by :func:`_valid_headers`.

    Returns:
      The name as the caller spelled it and the value's wire text, or ``None`` when the caller
      set no ``Content-Encoding``.

    Raises:
      None.
    """
    for name, value in headers.items():
        if _wire_name(name) == "Content-Encoding":
            return _diag._STR_STR(name), _wire_text(value)
    return None


def _wire_text(value: _HeaderValue) -> str:
    """Returns the text a stored header value puts on the wire, running none of its code.

    :func:`_valid_headers` stores every value as an exact ``str``, ``int``, ``bool`` or ``bytes``
    (:func:`_sent_value`), and ``putheader`` sends each as fixed bytes: a ``str`` encoded as
    Latin-1, an ``int`` or ``bool`` as ``str(value)`` in ASCII — ``True`` as ``True`` — and
    ``bytes`` as they are, here decoded as Latin-1 so each byte is one character. ``bytes`` is
    told apart by exact type, and ``str()`` of an exact ``str``, ``int`` or ``bool`` runs no
    caller code.

    Args:
      value: A header value as :func:`_valid_headers` stored it.

    Returns:
      The text sent.

    Raises:
      None.
    """
    if type(value) is bytes:
        return value.decode("latin-1")
    return str(value)


def _valid_encoding(
    gzip: bool, headers: dict[str, _HeaderValue], owner: str, *, library_body: bool
) -> None:
    """Refuses a caller ``Content-Encoding`` that the ``gzip`` argument makes untrue.

    The two arguments contradict each other: ``gzip=True`` says compress, and the header says
    the body is in some other coding. SPEC-048 FR-007 resolved it by letting the header win and
    sending the body uncompressed, which silently ignored ``gzip=True`` — one argument
    discarded because another was given (invariant 13). Refused instead (1.2.0): ``identity``,
    ``br``, a list such as ``gzip, br``, the empty string and ``5`` all name something other than
    gzip (:func:`_names_gzip`). A value that names gzip, in any case and with optional whitespace,
    is no contradiction and is honoured by ``_prepare``. Every value is read by the text it sends
    (:func:`_caller_encoding`), so ``b"gzip"`` names gzip exactly as ``"gzip"`` does. The
    message names ``gzip`` and the caller's spelling of the header, never its value. It runs after
    :func:`_valid_headers`, so the header's own checks — and its folded duplicates — are judged
    first.

    **With** ``gzip=False`` **the header must name no coding** (1.2.0). The sink builds the body
    itself and sends it as built, so the caller cannot have encoded it, and a header naming
    ``gzip``, ``br``, a list such as ``identity, gzip``, or ``5`` labels the body with a coding it
    is not in, on every request; it used to construct and send that label. ``identity`` in any
    case, and a value that is empty once its space and tab are stripped — an empty list of
    codings, which names none — are true labels and still construct (:func:`_names_no_coding`),
    as ``bytes`` or as ``str``. The message names ``gzip`` and the caller's spelling, never the
    value, as the ``gzip=True`` one does.

    **Both refusals hold only for the library's own** ``_body`` (``library_body``,
    :func:`_library_body`). ``_body`` is a documented 1.x extension hook, and a subclass whose
    ``_body`` encodes the body itself — ``zlib.compress`` beside ``Content-Encoding: deflate``, or
    its own gzip beside ``gzip`` — owns both the body and its label, with ``gzip`` true or false;
    it constructed in 1.1.0, where a caller ``Content-Encoding`` suppressed compression, and
    refusing it would break a working sink. Such a sink keeps 1.1.0's bytes: the library never
    compresses its body beside a caller header (``HTTPSink._fixed_compression`` is ``False``).

    Args:
      gzip: The ``gzip`` argument, already copied as an exact ``bool``.
      headers: The caller's headers, already judged by :func:`_valid_headers`.
      owner: The class refusing it, for the message.
      library_body: Whether the body is built by the library's own ``_body``.

    Returns:
      None.

    Raises:
      ValueError: If the body is the library's own and either ``gzip`` is true and the caller's
        ``Content-Encoding`` names another coding, or ``gzip`` is false and the header names any
        coding but ``identity``.
    """
    caller = _caller_encoding(headers)
    if caller is None or not library_body:
        return
    if gzip and not _names_gzip(caller[1]):
        raise ValueError(
            f"{owner} gzip=True cannot apply beside header {caller[0]!r}, which names another "
            "encoding; drop gzip= or the header"
        )
    if not gzip and not _names_no_coding(caller[1]):
        raise ValueError(
            f"{owner} gzip=False, the default, cannot apply beside header {caller[0]!r}, which "
            "names an encoding the body is not in; drop the header, or pass gzip=True if it "
            "names gzip"
        )


def _library_body(sink: object) -> bool:
    """Says whether the sink's ``_body`` is the library's own, reading it statically.

    The test is the module of the class whose namespace supplies ``_body``: the library's when
    that class is defined in ``log_foundry`` or a module under it. The class is found by
    SPEC-072's static reader — ``_class_member`` along the MRO read through ``type``'s unbound
    accessors — and its ``__module__`` read from that class's own namespace, so no metaclass
    ``__getattribute__``, ``__getattr__``, ``__mro__`` or ``__module__`` property runs, and none
    can answer for the class. A ``_body`` set on the instance itself (``_own_attribute``) is the
    caller's. Every shipped sink that overrides ``_body`` is defined under ``log_foundry`` and so
    is the library's own; ``LogstashSink`` builds a plain :class:`HTTPSink`.

    A subclass that overrides ``_body`` only to return ``super()._body(...)`` is read as its own:
    a static read sees where ``_body`` is defined, not what it does, so it cannot tell a body that
    is encoded from one that is not, and it errs on the side 1.1.0 took — the sink constructs. A
    class outside the package that writes ``__module__ = "log_foundry..."`` into its own namespace
    is read as the library's, and so meets the refusal the shipped sinks meet; nothing is gained by
    it. A class whose MRO cannot be read this way is read as the caller's, which refuses nothing.

    **The cost the static read accepts.** A pass-through ``super()._body`` override built with
    ``gzip=True`` beside ``Content-Encoding: gzip`` is read as encoding its own body, so the
    library does not compress it and it sends an uncompressed body under a ``gzip`` label — as
    1.1.0 did; the read cannot tell it from an encoding hook, and that is the released behaviour.
    And the read runs during construction, so a ``_body`` a subclass assigns to the instance
    **after** ``super().__init__`` returns is invisible to it: such a sink is judged as the
    library's and meets its refusals, where 1.1.0 constructed it. Assign the hook before calling
    ``super().__init__``, or define it on the class.

    Args:
      sink: The sink under construction.

    Returns:
      Whether ``_body`` resolves to a class defined in ``log_foundry``.

    Raises:
      None.
    """
    try:
        cls = type(sink)
        found = _class_member(cls, "_body")
        if found is None or _own_attribute(sink, "_body", found) is not _ABSENT:
            return False
        module = _NAMESPACE(_MRO(cls)[found[1]]).get("__module__")
    except Exception:
        return False
    return type(module) is str and (module == "log_foundry" or module.startswith("log_foundry."))


def _names_gzip(encoding: str) -> bool:
    """Says whether a ``Content-Encoding`` value names gzip, the coding ``gzip=True`` applies.

    A content-coding is case-insensitive (RFC 9110 §8.4.1), and a field value's surrounding space
    and tab are optional whitespace, not part of it (§5.5), so ``GZIP`` and `` gzip `` name gzip,
    and so does ``x-gzip``, which §8.4.1.3 has a recipient read as ``gzip``. A list of codings,
    ``gzip, br``, does not: it says the body is gzipped and then brotli'd.

    Args:
      encoding: The text the caller's ``Content-Encoding`` value sends.

    Returns:
      Whether it names gzip alone.

    Raises:
      None.
    """
    return encoding.strip(" \t").lower() in _GZIP_CODINGS


def _names_no_coding(encoding: str) -> bool:
    """Says whether a ``Content-Encoding`` value is true of a body the sink did not encode.

    ``identity`` means no transformation (RFC 9110 §8.4.1), and a value that is empty once the
    optional whitespace is stripped (§5.5) is an empty list of codings, which names none. Both are
    read as :func:`_names_gzip` reads ``gzip``: case-insensitive, space and tab stripped, one
    token. A list, ``identity, identity`` included, is not read further.

    Args:
      encoding: The text the caller's ``Content-Encoding`` value sends.

    Returns:
      Whether it names no coding.

    Raises:
      None.
    """
    return encoding.strip(" \t").lower() in _NO_CODINGS


def _unsendable_header(name: str, value: _HeaderValue) -> str | None:
    """Names why ``http.client`` cannot send one header, or returns ``None`` when it can.

    It returns the exception's type name rather than raising, so a caller's refusal is raised
    outside the ``except`` with neither ``__cause__`` nor ``__context__`` set: the exception
    ``putheader`` raises carries the whole value — a ``UnicodeEncodeError``'s ``object``, a
    ``ValueError``'s message — and a header value is as likely to be a credential as not.

    Args:
      name: The header name, as it goes on the wire.
      value: The header value, a ``str`` or the copy :func:`_sent_value` made.

    Returns:
      The type name of the exception ``putheader`` raised, or ``None``.

    Raises:
      None.
    """
    try:
        _started_request("localhost", "POST", "/").putheader(name, value)
    except (ValueError, TypeError) as err:
        return type(err).__name__
    return None


def _valid_auth(
    auth: str | tuple[str, str] | None,
    owner: str,
    headers: dict[str, _HeaderValue],
    conflict: str,
) -> tuple[str | tuple[str, str] | None, str | None]:
    """Returns the credentials, copied once, and the ``Authorization`` value every request sends.

    **Beside an** ``Authorization`` **header,** ``auth`` **is refused** (1.2.0). Both set the one
    ``Authorization`` header a request carries, so one of them was silently never sent — the
    credentials when the header was spelled exactly ``Authorization``, the header under any other
    case, since ``urllib`` folds the two names together and keeps the value written last. The
    header is matched by the name ``urllib`` sends, :func:`_wire_name`, and no spelling escapes:
    a name reaching here has passed :func:`_valid_headers`, which refuses one holding whitespace
    anywhere — the spelling a lenient server might trim into ``Authorization`` — so every name
    that would put ``Authorization`` on the wire is exactly a name whose folded form equals it. A
    name that merely contains the word, ``X-Authorization``, is not a conflict. This check runs
    **first**: it reads only whether credentials were given and the header names, which
    :func:`_valid_headers` has already judged, so it cannot echo or chain a credential, and a
    caller who must drop one of the two is told so before being told anything about the value of
    one they may be about to drop. ``conflict`` is the class's own wording, because a sink that
    writes ``Authorization`` itself — Splunk's token — must not tell its caller they passed a
    header. Since nothing a sink holds can now override the credentials, every one given is the
    one sent, and every one is checked below.

    **No caller code runs after this returns** (SPEC-049 FR-001, the type-door rule). The value
    is judged by its real type and never asked: a token is copied as the exact ``str`` it holds
    (``_diag._STR_STR``), and a pair is unpacked **once** into a ``tuple`` of two exact ``str``
    (:func:`_copied_pair`). The ``Authorization`` value is then built from the copy, once, put on
    a started head, and returned for :meth:`HTTPSink._apply_auth` to write as is. It used to be
    rebuilt on every ``emit`` from whatever the caller passed, so a one-shot iterator constructed
    and then raised ``ValueError`` out of every ``emit``, uncounted, and a ``str`` subclass's
    ``__format__`` ran on every request, free to return a value the construction check never saw.

    Only the **bearer-token** form is checked for ``CR`` and ``LF``, and the asymmetry is
    deliberate rather than an oversight: a token is written into ``Authorization`` verbatim, so a
    ``CR`` in it is the same injection :func:`_valid_headers` refuses, in the argument with the
    highest consequence. The ``(user, password)`` form is base64-encoded before it reaches the
    header and cannot carry one. A token outside Latin-1, or a lone surrogate in either half of
    the pair, raised ``UnicodeEncodeError`` out of every ``emit``, uncounted, and is refused here.
    The message names neither, and chains nothing that would.

    **An empty token or an empty pair is refused** (1.2.0): ``""`` sent ``Authorization:
    Bearer `` and ``("", "")`` sent ``Basic Og==`` on every request, a credential with nothing in
    it. A token of only spaces and tabs is refused as empty too, since a recipient strips them and
    receives a bare ``Bearer`` (:func:`_blank`). It is judged on the copy, so no caller code runs
    for it, and after the conflict above, which is judged first by design. A pair with one empty
    half — a key as the user with an empty password, or the reverse — is legal RFC 7617 basic
    auth that real services take, and is not refused; nor is a pair of whitespace halves, which
    base64 turns into credential bytes that are not empty.

    Args:
      auth: The bearer token, the user/password pair, or ``None``.
      owner: The class refusing it, for the message.
      headers: The caller's headers, already judged by :func:`_valid_headers`.
      conflict: Why ``auth`` cannot accompany an ``Authorization`` header, in the class's words.

    Returns:
      The copied credentials and the ``Authorization`` value they build, or ``(None, None)``.

    Raises:
      ValueError: If ``auth`` is given beside an ``Authorization`` header in any case, a bearer
        token contains ``CR`` or ``LF``, a value that is not a token is not a pair of two
        ``str`` (:func:`_copied_pair`), the token is empty or only spaces and tabs or the pair is
        ``("", "")``, or
        ``http.client`` cannot send the ``Authorization`` header either form builds.
    """
    if auth is None:
        return None, None
    if any(_wire_name(name) == "Authorization" for name in headers):
        raise ValueError(f"{owner} {conflict}")
    stored: str | tuple[str, str]
    if issubclass(type(auth), str):
        stored = _diag._STR_STR(cast("str", auth))
        if any(bad in stored for bad in _CRLF):
            raise ValueError(f"{owner} auth token must not contain CR or LF")
    else:
        stored = _copied_pair(auth, owner)
    if (issubclass(type(stored), str) and _blank(cast("str", stored))) or stored == ("", ""):
        raise ValueError(
            f"{owner} auth is empty, so every request would carry an empty credential; drop "
            "auth= or give it a value"
        )
    value, failure = _built_authorization(stored)
    if value is None:
        raise ValueError(f"{owner} auth cannot be sent over HTTP ({failure})")
    failure = _unsendable_header("Authorization", value)
    if failure:
        raise ValueError(f"{owner} auth cannot be sent over HTTP ({failure})")
    return stored, value


_OWS = " \t"
"""Optional whitespace (RFC 9110 §5.5), which a recipient strips from either end of a value."""


def _blank(text: str) -> bool:
    """Says whether an exact ``str`` holds nothing once optional whitespace is stripped.

    A field value's leading and trailing space and tab are not part of it (RFC 9110 §5.5), so a
    credential of only those arrives as no credential: ``Dd-Api-Key:    `` carries none, and
    ``Bearer `` followed by spaces is a bare scheme.

    Args:
      text: The exact-``str`` copy of a credential.

    Returns:
      Whether it is empty or only spaces and tabs.

    Raises:
      None.
    """
    return not text.strip(_OWS)


def _refuse_empty_credential(value: object, owner: str, argument: str) -> None:
    """Refuses a credential argument that would put an empty credential on every request.

    The platform sinks send their credential argument as a header of their own —
    ``DatadogSink``'s ``api_key`` as ``DD-API-KEY``, ``SplunkHECSink``'s ``token`` as
    ``Authorization: Splunk …`` — and ``""`` used to construct and send that header with nothing
    in it, as ``auth=""`` sent ``Bearer `` (1.2.0, the rule :func:`_valid_auth` applies to
    ``auth``). A value of only spaces and tabs is refused too (:func:`_blank`). The value is
    judged **as sent**, by the rule the header path uses: a ``str`` copied as the exact ``str`` it
    stores, then :func:`_sent_value` and :func:`_wire_text`, so ``b""`` and ``b"  "``, which
    ``putheader`` sends as nothing, are refused as ``""`` is, and ``5`` reads as ``5``. An exact
    ``str`` or ``bytes`` runs no caller code; any other value is read once more as the header
    check reads it, and one that cannot be sent is left to that check. The message names the
    argument and never the value.

    Args:
      value: The credential argument as the caller passed it.
      owner: The class refusing it, for the message.
      argument: The argument's name, for the message.

    Returns:
      None.

    Raises:
      ValueError: If ``value`` sends nothing but spaces and tabs.
    """
    copied = _diag._STR_STR(cast("str", value)) if issubclass(type(value), str) else value
    sent, _failure = _sent_value(copied)
    if sent is not None and _blank(_wire_text(sent)):
        raise ValueError(
            f"{owner} {argument} is empty, so every request would carry an empty credential; "
            "give it a value"
        )


_NOT_A_PAIR = (set, frozenset, Mapping)
"""Containers that unpack into two items but hold no ``(user, password)`` order (SPEC-049)."""


def _copied_pair(auth: object, owner: str) -> tuple[str, str]:
    """Copies a ``(user, password)`` pair once into a ``tuple`` of two exact ``str``, or raises.

    A value outside the declared ``tuple[str, str]`` is refused rather than sent, breaking no
    type-checked caller. A ``set`` or ``frozenset`` unpacks in hash order, so ``{"alice", "pw"}``
    authenticated only when ``PYTHONHASHSEED`` happened to put the user first, and drew a
    ``401`` on every request otherwise; a mapping unpacks into its keys. Those containers hold no
    order, so they are judged by their real type **before** the unpack, and none of their
    methods — an ``__iter__`` that raises with the caller's text, say — runs; a wrong-length one
    gets this refusal rather than the unpack's.

    The unpack is then the one ``_authorization`` used to perform on every ``emit``, so what it
    refuses and the message it gets are unchanged for every other value — a non-iterable raises
    ``TypeError``, too few or too many items ``ValueError``, and no more than three items are
    ever read. Its failure is caught as a type name and the refusal raised outside the
    ``except``, so nothing chained carries the credential. A value that unpacks into two items
    that are not both ``str`` drew a ``401`` on every request: ``b"to"`` unpacks into two
    ``int``, and a ``bytes`` or ``None`` half was formatted as its ``repr``. Both halves are
    judged by their real types and copied with ``_diag._STR_STR``, which runs none of a
    subclass's methods. The message names only the real type names, never a value. It is a
    ``ValueError`` although the fault is a type, because every refusal of ``auth`` is one —
    ``auth=123`` has been ``ValueError(... (TypeError))`` since SPEC-049 — and a caller catching
    the one exception ``auth`` raises should not need a second.

    Args:
      auth: The credentials as the caller passed them, already known not to be a ``str``.
      owner: The class refusing it, for the message.

    Returns:
      The copied pair.

    Raises:
      ValueError: If the value does not unpack into exactly two items, is a ``set``,
        ``frozenset`` or mapping, or either item is not a ``str``.
    """
    if issubclass(type(auth), _NOT_A_PAIR):
        raise ValueError(  # noqa: TRY004
            f"{owner} auth must be a str token or a pair of two str, not {_type_name(auth)}"
        )
    failure: str | None = None
    try:
        user, password = cast("tuple[object, object]", auth)
    except (ValueError, TypeError) as err:
        failure = type(err).__name__
    if failure is not None:
        raise ValueError(f"{owner} auth cannot be sent over HTTP ({failure})")
    if not (issubclass(type(user), str) and issubclass(type(password), str)):
        raise ValueError(  # noqa: TRY004
            f"{owner} auth must be a str token or a pair of two str, not a pair of "
            f"({_type_name(user)}, {_type_name(password)})"
        )
    return _diag._STR_STR(cast("str", user)), _diag._STR_STR(cast("str", password))


def _type_name(value: object) -> str:
    """Returns the name ``value``'s real type stores, as an exact ``str``, asking the value nothing.

    Args:
      value: Any object.

    Returns:
      The type's stored name.

    Raises:
      None.
    """
    return _diag._STR_STR(_diag._STORED_NAME(type(value)))


def _built_authorization(auth: str | tuple[str, str]) -> tuple[str | None, str | None]:
    """Builds the ``Authorization`` value once, or names why it cannot be built.

    A lone surrogate in a pair fails the UTF-8 encode ahead of base64; the failure is returned as a
    type name, like :func:`_unsendable_header`, so nothing chained carries the credential.

    Args:
      auth: The copied bearer token or user/password pair, exact ``str`` throughout.

    Returns:
      The value and ``None``, or ``None`` and the type name of the exception building raised.

    Raises:
      None.
    """
    try:
        return _authorization(auth), None
    except UnicodeEncodeError as err:
        return None, type(err).__name__


def _authorization(auth: str | tuple[str, str]) -> str:
    """Builds the ``Authorization`` value for a bearer token or a ``(user, password)`` pair.

    Called once, at construction, by :func:`_built_authorization` on the copy
    :func:`_valid_auth` made, so the value checked is the value every request sends and no
    caller code runs to build it.

    Args:
      auth: The copied bearer token or user/password pair.

    Returns:
      The header value.

    Raises:
      UnicodeEncodeError: If the pair cannot be encoded as UTF-8 for base64.
    """
    if issubclass(type(auth), str):
        return f"Bearer {auth}"
    user, password = cast("tuple[str, str]", auth)
    token = b64encode(f"{user}:{password}".encode()).decode("ascii")
    return f"Basic {token}"


def merge_headers(
    base: dict[str, str],
    http_kwargs: HTTPForwardKwargs,
    *,
    owner: str = "HTTPSink",
    argument: str | None = None,
) -> dict[str, str]:
    """Lays the caller's headers over a sink's own; with ``argument=``, refuses replacing them.

    The caller's ``headers`` are read with ``dict()``, as :class:`HTTPSink` reads its own, so a
    ``MappingProxyType`` or a list of pairs is honoured — and then judged by every header and auth
    rule — rather than dropped: an ``isinstance(caller, dict)`` test here used to discard any other
    mapping silently, ``Authorization`` beside ``auth=`` included. Without ``argument=``, the
    caller's headers are laid over ``base`` and win, in any spelling, as on the wire before 1.2.0,
    because this function is public (invariant 10) and a sink built on it may hand it defaults a
    caller is meant to override: a caller name folding to a default replaces it, spelling and value,
    at its position (:func:`_respell_overridden`), so the merged map holds one spelling and meets
    no duplicate refusal. An exact-case override returns what it always did; a different-case one
    returns one key where it used to return two, which ``urllib`` folded to the same request.

    **With** ``argument=``, **every key of** ``base`` **is the sink's own credential header**, and
    a caller header whose name folds to one of them (:func:`_wire_name`, the fold ``urllib`` sends
    and the ``Authorization`` check uses) is refused (1.2.0). That is how Datadog, Splunk, New
    Relic and Honeycomb call it. The caller's used to win, under any case, since ``urllib`` keeps
    one value per folded name, the last written, so ``headers={"dd-api-key": ...}`` sent instead
    of the required ``api_key`` and the argument was silently ignored (invariant 13). The refusal
    names the caller's header, the argument it collides with and the name the sink sends, never a
    value, and it runs here, before :class:`HTTPSink` judges any header, so no later check can
    quote one first. Both sides are folded as the exact text they store (``_diag._STR_STR``), so a
    ``str`` subclass's ``capitalize`` cannot answer one thing here and another on the wire; a
    non-``str`` name is left to :func:`_valid_headers`, which refuses it. The non-credential
    defaults :class:`HTTPSink` writes per request — ``Content-Type``, ``Content-Encoding`` — sit
    beneath the caller's headers either way, as SPEC-009 FR-002 records.

    Args:
      base: The sink's own headers, updated in place; with ``argument=``, its credential headers.
      http_kwargs: The caller's keyword arguments, mutated by popping ``headers`` out so the
        rest can be forwarded to :class:`HTTPSink` without a duplicate argument. Typed as the
        narrowest shape (SPEC-051 FR-005); every wider one is assignable to it, and ``pop`` is
        allowed because each key is ``total=False``. The pop is why a caller may still pass
        ``headers``, and — since it happens *here*, in another function — why ``mypy`` reports
        the caller's own forwarding call as a possible duplicate anyway.
      owner: The class merging, for the refusal; read only with ``argument=``.
      argument: The constructor argument ``base`` carries, which opts in to the refusal, or
        ``None`` for the caller to win. Keyword-only and defaulted, so a caller of this public
        function written before 1.2.0 calls it, and is answered, as it was.

    Returns:
      The merged headers.

    Raises:
      TypeError: If the caller's ``headers`` is not a mapping or an iterable of pairs, as
        ``dict()`` raises for :class:`HTTPSink`'s own.
      ValueError: If an item of an iterable ``headers`` is not a pair, likewise, or, with
        ``argument=``, a caller header's name folds to a key of ``base``.
    """
    caller = http_kwargs.pop("headers", None)
    if caller is None:
        return base
    given = dict(caller)
    if argument is not None:
        _refuse_credential_collision(base, given, owner, argument)
        base.update(given)
    else:
        _respell_overridden(base, given)
    return base


def _respell_overridden(base: dict[str, str], given: dict[str, str]) -> None:
    """Lays ``given`` over ``base``, a caller name folding to a default taking its place, in place.

    Before 1.2.0 the merge kept both spellings, ``{"X-Default": "1", "x-default": "2"}``, and
    ``urllib`` folded them, sending the caller's value where the default had stood. 1.2.0 refuses
    two names that fold together (:func:`_valid_headers`), which would have turned that working
    override into a refusal, so the merge now does the fold itself: the caller's spelling and
    value take the default's place, at its position, and the request on the wire is byte for byte
    the one 1.x sent. Only a key that was in ``base`` is replaced, and each at most once, so two
    caller names that fold together are both kept and still meet the duplicate refusal: a caller's
    own contradiction is reported, never resolved here.

    No caller code runs. The two maps are read by iteration, which calls no key's ``__hash__``
    or ``__eq__``, every ``str`` name is folded and stored as the exact text it holds, and the
    result is rebuilt from positions rather than looked up by key: popping a caller's key by its
    own value raised a bare ``KeyError`` for a ``str`` subclass whose hash was not stable. A name
    that is not a ``str`` is kept as given, for :func:`_valid_headers` to refuse.

    Args:
      base: The sink's own headers, rewritten in place to the merged map.
      given: The caller's headers, read with ``dict()``.

    Returns:
      None.

    Raises:
      None.
    """
    defaults = [(_exact(name), value) for name, value in base.items()]
    callers = [(_exact(name), value) for name, value in given.items()]
    position = {
        _wire_name(cast("str", name)): index
        for index, (name, _value) in enumerate(defaults)
        if issubclass(type(name), str)
    }
    extra: list[tuple[object, str]] = []
    for name, value in callers:
        index = (
            position.pop(_wire_name(cast("str", name)), None)
            if issubclass(type(name), str)
            else None
        )
        if index is None:
            extra.append((name, value))
        else:
            defaults[index] = (name, value)
    base.clear()
    base.update(cast("list[tuple[str, str]]", defaults + extra))


def _exact(name: object) -> object:
    """Returns a ``str`` name as the exact text it stores, and any other name as given.

    Args:
      name: A header name.

    Returns:
      The exact ``str``, or the name unchanged.

    Raises:
      None.
    """
    return _diag._STR_STR(cast("str", name)) if issubclass(type(name), str) else name


def _refuse_credential_collision(
    base: dict[str, str], given: dict[str, str], owner: str, argument: str
) -> None:
    """Refuses a caller header whose name folds to one of a sink's own credential headers.

    Args:
      base: The sink's own credential headers.
      given: The caller's headers.
      owner: The class merging, for the message.
      argument: The constructor argument ``base`` carries, for the message.

    Returns:
      None.

    Raises:
      ValueError: If a caller header's name, as the exact text it stores, folds to a key of
        ``base``.
    """
    own = {
        _wire_name(_diag._STR_STR(name)): _diag._STR_STR(name)
        for name in base
        if issubclass(type(name), str)
    }
    for name in given:
        if not issubclass(type(name), str):
            continue
        written = _diag._STR_STR(name)
        sent = own.get(_wire_name(written))
        if sent is not None:
            raise ValueError(
                f"{owner} header {written!r} collides with {argument}, which this sink sends as "
                f"{sent}; drop the header"
            )


def _wholly_rejected(outcomes: tuple[Outcome, ...]) -> bool:
    """Whether a chunk's outcomes are all ``REJECTED``: a reader shim's 1.x ``False``.

    An empty chunk is not wholly rejected, so a direct ``_handle_response(payload, [])`` reads
    ``True``, as every existing call of that shape expects (SPEC-073 FR-003).

    Args:
      outcomes: One chunk's outcomes.

    Returns:
      True only for a non-empty chunk whose every event was refused for good.

    Raises:
      None.
    """
    return bool(outcomes) and all(outcome is Outcome.REJECTED for outcome in outcomes)


def _read_as_1x(reader: Callable[[], tuple[Outcome, ...]]) -> bool:
    """Runs a reader's ``_outcomes`` as a 1.x ``_handle_response`` call: committed, then a bool.

    The reader runs in a one-round context of its own, committed when it returns, so a direct
    1.x caller sees the counters move as they did; its run fails exactly when the chunk was
    wholly rejected, which is when 1.x returned ``False``.

    Args:
      reader: The bound ``_outcomes`` call, with its arguments.

    Returns:
      False exactly when the chunk is non-empty and every outcome is ``REJECTED``.

    Raises:
      Exception: Whatever the reader raises, after its context is committed as a failed run.
    """
    return not _wholly_rejected(_engine.detached(reader, _wholly_rejected))


class _Tally:
    """A ``_Pending`` that forwards every drop and counts how many events ``_items`` dropped.

    The round needs the number to tell the events ``_items`` dropped, and counted, from events
    an ``_items`` override merely returned as other objects (:func:`_positions`).

    Attributes:
      dropped: The events counted through this ledger.
    """

    def __init__(self, pending: _Pending) -> None:
        """Wraps the ledger the drops really go to.

        Args:
          pending: The run's ``SETTLED`` view.

        Returns:
          None.

        Raises:
          None.
        """
        self._pending = pending
        self.dropped = 0

    def count(self, counter: str, n: int = 1) -> None:
        """Forwards a drop count and adds it to :attr:`dropped`.

        Args:
          counter: The owner's attribute.
          n: How many events.

        Returns:
          None.

        Raises:
          None.
        """
        self.dropped += n
        self._pending.count(counter, n)

    def announce(self, line: Callable[[], None]) -> None:
        """Forwards a line.

        Args:
          line: A ``lambda`` wrapping a ``_diag`` call.

        Returns:
          None.

        Raises:
          None.
        """
        self._pending.announce(line)


def _positions(
    batch: list[dict[str, object]], items: list[_Item], dropped: int
) -> tuple[list[int | None], list[Outcome]]:
    """Maps each rendered item to its event's batch index, and gives every unplaced event a fate.

    ``_items`` builds every item from the batch's own event object, in order, skipping what it
    drops, so a pointer that only moves forward finds each one by identity; that is exact for
    every shipped sink. An ``_items`` override may return items built from other objects — a
    subclass enriching each event first — so an item the identity walk cannot place is given, in
    order, the next batch index no item claimed: an override returning one item per event, in
    order, maps exactly.

    An index still unclaimed after both passes is an event no request carries. When their number
    is the number ``_items`` dropped and counted, they are those drops: ``DROPPED``. Otherwise
    the mapping cannot say which they are, and each is ``REJECTED``: counted in the engine's
    ``failed_events``, an upper bound, never resent, and able to fail a run that delivered
    nothing else, so the loss is over-reported rather than hidden. Neither ``ACCEPTED`` nor
    ``DROPPED`` is ever a default for an event the sink did not itself drop.

    Args:
      batch: The round's events.
      items: What ``_items`` returned for them.
      dropped: How many events ``_items`` dropped and counted.

    Returns:
      Each item's batch index, ``None`` for an item past the batch's end, and the round's
      starting fate for every event: ``REJECTED`` for a placed one, which its chunk's outcome
      then replaces, so a path that forgot to settle a chunk would over-report, never hide.

    Raises:
      None.
    """
    found: list[int | None] = []
    claimed = [False] * len(batch)
    cursor = 0
    for item in items:
        probe = cursor
        while probe < len(batch) and batch[probe] is not item.event:
            probe += 1
        if probe < len(batch):
            found.append(probe)
            claimed[probe] = True
            cursor = probe + 1
        else:
            found.append(None)
    free = (index for index, taken in enumerate(claimed) if not taken)
    for at, position in enumerate(found):
        if position is None:
            index = next(free, None)
            if index is not None:
                found[at] = index
                claimed[index] = True
    unplaced = claimed.count(False)
    fallback = Outcome.DROPPED if unplaced == dropped else Outcome.REJECTED
    return found, [Outcome.REJECTED if taken else fallback for taken in claimed]


def _settle(fates: list[Outcome], where: list[int | None], outcomes: tuple[Outcome, ...]) -> None:
    """Records one chunk's outcomes at its events' batch indices.

    Args:
      fates: The round's outcomes by batch index, updated in place.
      where: Each chunk item's batch index, or ``None``.
      outcomes: One outcome per chunk item.

    Returns:
      None.

    Raises:
      None.
    """
    for position, outcome in zip(where, outcomes, strict=True):
        if position is not None:
            fates[position] = outcome


class HTTPSink:
    """A :class:`~log_foundry.sinks.base.Sink` that POSTs each batch to an HTTP endpoint.

    This is the dependency-free base every other HTTP platform sink builds on: it re-chunks a
    batch to the destination's limits, serializes each chunk, applies headers, auth and optional
    gzip, and POSTs it. These are terminal, direct-ship sinks — per arch §9.1 they couple
    application delivery to the destination's availability, so for durability put a queue in
    front.

    **It delivers in rounds on the delivery engine** (SPEC-073). :meth:`deliver` is one round:
    every event is rendered and chunked, each chunk goes in **one** request, and each event's
    fate is returned by index in a ``Verdict``. Nothing in a round waits or retries; the engine
    resends only the events a round called ``RETRYABLE``, in later rounds, so a chunk that landed
    or was refused for good is never sent again. Its budget keeps the 1.x total: on the worker
    path ``(max_retries + 1) * (worker retries + 1)`` rounds, on the orphan path and through
    :meth:`emit` ``max_retries + 1``. :meth:`emit` runs the same engine inline, so a caller of
    ``emit`` sees the 1.x contract. Every documented counter keeps its 1.x name and unit through
    the engine's run context: ``failed`` counts abandoned **requests**.

    **Subclasses extend it by overriding hooks, never** :meth:`emit` (FR-001). The chunk loop
    lives here, so a subclass that overrode ``emit`` would deliver whatever batch it was handed —
    which is what three shipped subclasses did, and why ``Worker._final_drain`` could hand this
    family an entire exit backlog (5,980 events measured) in one request that the destination
    then rejected whole. The hooks are :meth:`_render`, :meth:`_body`, :meth:`_outcomes` and
    :meth:`_handle_response`; a test asserts no subclass defines ``emit``. A third-party subclass
    that does override ``emit``, or whose ``_handle_response`` sits above its ``_outcomes``, is
    not offered ``deliver`` and keeps its 1.x path (:meth:`__init_subclass__`).

    Credentials passed as ``auth`` are sent on every request to the URL as given, and nothing
    here requires ``https://``: over a plaintext endpoint a bearer token, and a basic-auth pair,
    travel in the clear. Credentials written into the URL itself are refused, since ``urllib``
    sends them as part of the host name and never as credentials; the refusal names where this
    class takes them instead, ``_CREDENTIALS``, which a subclass with a credential argument of its
    own overrides.

    **The worst case is per run.** Each wait between rounds is at most ``max_retry_after`` when a
    server sends a ``Retry-After`` and exponential otherwise, and one round has one wait, however
    many of its chunks failed retryably. On the worker path it all runs on the single drain
    thread, so it is a pause on all log delivery; what bounds it is ``shutdown(timeout=)`` itself,
    through SPEC-062's prediction, plus ``MAX_BUDGET_REDUCTIONS`` on the ``413`` search; sizing
    ``max_batch_bytes`` to the destination is what keeps the chunk count low in the first place.
    Nothing here *skips* work because a shutdown is in progress — a revision that did was
    measured to disable both retries and the ``413`` search for the whole of
    ``Worker._final_drain``, since the stop event is set before the join. Every wait is the
    engine's, cut short by the stop signal rather than cancelling an attempt (SPEC-027).

    Attributes:
      MAX_BATCH_COUNT: Entries one request may carry. A subclass overrides it with its
        destination's documented figure.
      MAX_BATCH_BYTES: Bytes one request's body may reach, likewise.
      MAX_EVENT_BYTES: A destination's published limit on a *single* event, or ``None`` when it
        publishes none. Distinct from the request budget: Datadog accepts a 5 MB payload but
        caps one log at 1 MB, so an event between the two is rejected by a limit the request
        budget cannot see.
      failed: Requests abandoned, counted per chunk: one refused for good, and one still failing
        retryably when its run's rounds ran out. Committed when the run ends.
      dropped_oversized: Events dropped for exceeding the per-event ceiling, plus those a
        destination answered ``413`` to and no split could rescue. Both are counted and
        announced only when ``emit`` returns (SPEC-058 FR-005, SPEC-059 FR-004) — on the engine,
        only by a run that settles — so each is counted once. An event whose render raised, or
        whose chunk's request could not be built, is counted apart from this and folded into
        ``losses().dropped``, so this stays oversize only.
      dropped_unadjudicated: Events of a ``2xx`` this sink could not read — a body reader that
        raised, or a subclass reader giving up on a response (:meth:`_unadjudicated`) — folded
        into ``losses().failed``; they may have landed.
      log_foundry_counters: The engine's event-unit totals (SPEC-072); ``losses()`` does not
        read them, so ``read_losses`` and ``health().sink`` read what they read in 1.x.

    The two lifecycle decisions this class records, inherited by every platform subclass. It
    takes **no** transport lock (SPEC-028 FR-002): there is no transport to guard, since
    ``urllib`` opens a fresh connection per request and nothing is rebound after construction.
    And it **adds no post-close guard** (SPEC-032 FR-003), because ``close()`` releases nothing —
    a batch emitted afterwards still reaches the endpoint, and refusing it would be loss the
    library invented rather than loss it reported.


    It keeps **no** client buffer (SPEC-036 FR-002): each ``emit`` is a request that has
    completed by the time it returns, and no client object outlives it holding data.
    """

    MAX_BATCH_COUNT = DEFAULT_MAX_BATCH_COUNT
    MAX_BATCH_BYTES = DEFAULT_MAX_BATCH_BYTES
    MAX_EVENT_BYTES: int | None = None
    _CREDENTIALS = _AUTH_REMEDY
    _AUTH_CONFLICT = _AUTH_BESIDE_HEADER
    _READS_BOOL: ClassVar[bool] = False
    _DELIVER_WITHDRAWN: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Decides once per class whether it is offered ``deliver`` (SPEC-073 FR-004).

        A subclass written against 1.x extends the family through its documented hooks, and two
        of its overrides would change meaning if the worker called ``deliver`` around them. An
        ``emit`` override expects to be the call the worker makes; a bool ``_handle_response``
        meant "not settled" by its ``False``, and the worker resent such a batch. So a class is
        **ineligible** when a class in its MRO above the nearest ancestor offering ``deliver``
        defines ``emit``, or when the first class defining ``_handle_response`` comes before the
        first defining ``_outcomes``. The slice ends at the nearest ancestor whose own body
        defines a non-``None`` ``deliver`` — ``HTTPSink``, or a third party's engine-native
        subclass — so a plain child of the latter keeps its parent's ``deliver``.

        It assigns ``deliver`` only where the inherited value is wrong. An ineligible class gets
        ``deliver = None``, which SPEC-072's static probe never offers, marked as the base's own
        with ``_DELIVER_WITHDRAWN``. An eligible class whose inherited ``deliver`` is a ``None``
        the base assigned gets the nearest non-``None`` ``deliver`` back — a ``B(A)`` that adds
        ``_outcomes`` below ``A``'s bool reader. A third party's own ``deliver = None`` is an
        opt-out, and neither it nor any plain child of it is restored. A class whose own body
        defines ``deliver`` is left alone. ``_READS_BOOL`` is written on every class: whether its
        round reads a ``2xx`` through the base :meth:`_outcomes`, which is the bool override's
        verdict even below a shipped reader that defines ``_outcomes``.

        Everything is read through ``sinks.base``'s static accessors, so no metaclass hook runs
        and no class is compared by ``==``. The detection is by class, never by instance.

        Args:
          **kwargs: Passed to ``object.__init_subclass__``.

        Returns:
          None.

        Raises:
          None.
        """
        super().__init_subclass__(**kwargs)
        mro = _MRO(cls)
        base = next(index for index, klass in enumerate(mro) if klass is HTTPSink)
        reader = _class_member(cls, "_handle_response")
        outcomes = _class_member(cls, "_outcomes")
        reader_at = reader[1] if reader is not None else base
        outcomes_at = outcomes[1] if outcomes is not None else base
        cls._READS_BOOL = reader_at < outcomes_at
        if "deliver" in _NAMESPACE(cls):
            return
        offering = _class_member(cls, "deliver", skip_none=True)
        end = offering[1] if offering is not None else base
        emit = _class_member(cls, "emit")
        eligible = not (emit is not None and emit[1] < end) and not (
            reader_at < outcomes_at and reader_at < end
        )
        inherited = _class_member(cls, "deliver")
        if not eligible:
            setattr(cls, "deliver", None)  # noqa: B010
            cls._DELIVER_WITHDRAWN = True
            return
        if inherited is not None and inherited[0] is None and offering is not None:
            holder = _NAMESPACE(mro[inherited[1]])
            if holder.get("_DELIVER_WITHDRAWN") is True:
                setattr(cls, "deliver", offering[0])  # noqa: B010
                cls._DELIVER_WITHDRAWN = False

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
        max_batch_count: int | None = None,
        max_batch_bytes: int | None = None,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        """Configures the endpoint, encoding, credentials and retry bounds.

        Args:
          url: The endpoint to POST to. Surrounding whitespace is stripped, as ``urllib``
            already stripped it before sending, so :attr:`url` reads back what goes on the wire.
          method: The HTTP method.
          headers: Headers that override the sink's own defaults, read with ``dict()`` unless
            ``None``, as :func:`merge_headers` reads a platform sink's, so the two agree: an
            empty mapping is no headers, and ``0`` is refused rather than read as none.
          auth: A bearer token, or a ``(user, password)`` pair for basic auth. A token is a
            ``str``; a pair is any value — a ``tuple``, a ``list``, a one-shot iterator —
            unpacking into exactly two ``str``, other than a ``set``, ``frozenset`` or mapping,
            which hold no order. Either is read **once**, here: the ``Authorization`` value is
            built from an exact-``str`` copy and every request sends that value, so a list
            changed afterwards, an object yielding new values each time it is iterated, or a
            ``str`` subclass whose methods would answer differently later, is frozen at what it
            held at construction. ``""`` and ``("", "")`` are refused; a pair with one empty half
            is not.
          body_format: ``"ndjson"`` or ``"json_array"``.
          timeout: Seconds allowed per request.
          gzip: Whether to compress the body. Refused beside a caller ``Content-Encoding``, in
            any case, that names a coding other than gzip; beside one naming gzip the body is
            compressed and sent under the caller's header. ``False`` is refused beside one naming
            any coding but ``identity``, since the body is sent as built — when the body is the
            library's own ``_body``; a subclass whose ``_body`` encodes the body itself keeps
            1.1.0's freedom to label it. A value is read by the text it sends, ``bytes``
            included. Anything but an exact ``bool`` is read once, with ``bool()``, and stored as
            the ``bool`` it gave, so no ``__bool__`` runs per request. Once a caller
            ``Content-Encoding`` is set, whether the body is compressed is fixed here from this
            argument, and setting ``gzip`` later changes nothing (:meth:`_compresses`).
          max_retries: Retries after a failing request, floored at zero as ``Worker._emit``
            floors its own (SPEC-021) — a negative value otherwise skipped the loop entirely, so
            the request was abandoned with no attempt made and no counter moved. Anything but an
            ``int`` is refused with ``TypeError`` (SPEC-066 FR-001): ``2.5`` used to construct and
            then raise out of ``range()`` on every batch with nothing counted. A ``bool`` or
            ``int`` subclass is read as the ``int`` it holds.
          max_retry_after: Ceiling on a server-supplied ``Retry-After``, an ``int`` or a
            ``float``. Its *value* is neither floored nor rejected here: a ceiling that is zero,
            negative or ``NaN`` reaches ``clamp_server_delay``, which answers it by falling back
            to exponential backoff, keeping that judgement in one place. Its *type* is judged here
            (SPEC-066 FR-002): anything but an ``int`` or ``float`` is refused with ``TypeError``,
            because a ``str`` or ``None`` used to construct and then raise an uncounted
            ``TypeError`` out of every ``emit`` that met a ``Retry-After`` header. It is stored as
            the exact ``int`` or ``float`` it holds, so a subclass whose comparisons lie is judged
            by its number, and reads back as that number.
          max_batch_count: Entries one request may carry, or ``None`` for this class's
            :attr:`MAX_BATCH_COUNT`. Floored at one, since a zero or negative bound would make
            every event oversized and discard the batch. A supplied value that is not an ``int``
            is refused with ``TypeError`` (SPEC-066 FR-002) — ``2.5`` chunked by three, ``nan`` by
            one and ``inf`` without a limit, silently. The class attribute is the library's (or a
            subclass's) own figure, so it is floored but not judged.
          max_batch_bytes: Bytes one request's body may reach, or ``None`` for this class's
            :attr:`MAX_BATCH_BYTES`. **Refused** when not positive rather than floored (SPEC-049,
            system-frame review): the floor at one this used to apply is not the count floor's
            twin, because a one-byte body ceiling makes every event oversized and delivers
            nothing, where a count of one still delivers one event per request.
          opener: A ``urlopen``-shaped callable, which a test can inject to assert on the
            request without any network access. Defaults to :func:`_no_redirect_opener` rather
            than ``urlopen`` (SPEC-048 FR-001); an injected one is used exactly as given, since
            it is the caller's object and the library does not reshape it.

        Returns:
          None.

        Raises:
          TypeError: If ``max_retries`` or a supplied ``max_batch_count`` is not an ``int``, or
            ``max_retry_after`` is not an ``int`` or ``float`` (SPEC-066), the message carrying the
            subclass's own name as the ``ValueError`` does; or if ``opener`` is neither callable
            nor ``None`` (SPEC-071 FR-005), judged first; or, raised by ``dict()`` itself, if
            ``headers`` is neither ``None``, a mapping nor an iterable of pairs.
          ValueError: If the URL carries a user or password before its host or an ``@`` anywhere
            else — write one that belongs in a path or query as ``%40`` — its scheme is not
            ``http`` or ``https``, it names no host or no usable port, or ``http.client`` cannot
            build a request line for it or for the method,
            a header name or value or a bearer token contains CR or LF, a header name contains
            whitespace anywhere (refused after ``http.client``'s own check, so a name that already
            failed it keeps that message), two header names fold to one wire name, ``gzip`` is
            true beside a caller ``Content-Encoding`` naming another coding or false beside one
            naming any coding but ``identity``, ``auth`` is
            given beside an ``Authorization`` header in any case, ``auth`` cannot be sent, is
            neither a ``str`` token nor a pair of two ``str``, or is ``""`` or ``("", "")``, the
            timeout cannot bound anything,
            ``body_format`` is neither of the two this class renders, or ``max_batch_bytes`` is
            not positive — each refused here rather than raised out of every
            ``emit`` or silently ignored for the life of the process (SPEC-049 FR-001, FR-002 and
            FR-004), and inherited by every subclass, whose own name the message carries.
        """
        require_callable(opener, "opener", type(self).__name__)
        self.url = _valid_url(url, type(self).__name__, credentials=self._CREDENTIALS)
        if body_format not in _BODY_FORMATS:
            raise ValueError(
                f"{type(self).__name__} body_format must be one of {sorted(_BODY_FORMATS)}, "
                f"not {body_format!r}"
            )
        self.method = _valid_method(method, type(self).__name__)
        self._headers = _valid_headers(
            dict(headers) if headers is not None else {}, type(self).__name__
        )
        gzip = gzip if type(gzip) is bool else bool(gzip)
        library_body = _library_body(self)
        _valid_encoding(gzip, self._headers, type(self).__name__, library_body=library_body)
        self._fixed_compression = (
            None if _caller_encoding(self._headers) is None else gzip and library_body
        )
        self._auth, self._authorization_value = _valid_auth(
            auth, type(self).__name__, self._headers, self._AUTH_CONFLICT
        )
        self.body_format = body_format
        self.timeout = require_timeout(timeout, "timeout", type(self).__name__)
        self.gzip = gzip
        self.max_retries = floor_count(max_retries, 0, "max_retries", type(self).__name__)
        self.max_retry_after = exact_seconds(
            max_retry_after, "max_retry_after", type(self).__name__
        )
        self.max_batch_count = (
            floor_count(max_batch_count, 1, "max_batch_count", type(self).__name__)
            if max_batch_count is not None
            else max(self.MAX_BATCH_COUNT, 1)
        )
        self.max_batch_bytes = (
            require_positive(max_batch_bytes, "max_batch_bytes", type(self).__name__)
            if max_batch_bytes is not None
            else self.MAX_BATCH_BYTES
        )
        self.log_foundry_stop_signal: threading.Event | None = None
        self._opener = opener if opener is not None else _no_redirect_opener()
        self.failed = 0
        self.dropped_oversized = 0
        self._unrenderable = 0
        self.dropped_unadjudicated = 0
        self._counter_lock = threading.Lock()
        self.log_foundry_counters = SinkCounters()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Runs the delivery engine inline over :meth:`_round`, keeping the 1.x ``emit`` contract.

        Subclasses must not override this. They shape the request through :meth:`_render`,
        :meth:`_body`, :meth:`_outcomes` and :meth:`_handle_response`, which is what keeps the
        chunk loop on every path to the wire. It is ``emit_via_deliver`` with ``deliver=`` the
        round itself rather than :meth:`deliver`, so a subclass that overrides ``emit`` and calls
        ``super().emit(batch)``, and one not offered ``deliver``, still get the chunking, the
        sink's own ``max_retries`` as rounds, the ``413`` search and a raise on total failure.

        The drops a round decides — an oversized event, an unrenderable one, a chunk whose
        request could not be built, a lone item the destination answered ``413`` — are counted
        and announced only when ``emit`` returns (SPEC-058 FR-005, SPEC-059 FR-004): they go to
        the run's context as ``SETTLED``, so a run that fails, which is the call that raises,
        counts none of them and its catcher owns the batch. ``failed`` counts abandoned requests
        when the run ends: one refused for good whatever the outcome, one still failing
        retryably only for the run's last round.

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When the run failed: no event was accepted, at least one was
            rejected for good or still retryable when the rounds ran out, and none ended
            unknown. A partial outcome, an unadjudicated ``2xx`` and a batch whose every event
            was dropped return normally, because none is helped by a retry and a resend could
            duplicate what landed (SPEC-026 FR-001, SPEC-072).
        """
        _engine.emit_via_deliver(self, batch, deliver=self._round)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Runs one round and reports each event's fate by index (SPEC-073 FR-001).

        Inside an engine run the round counts into that run's context; called directly, outside
        any run, it is a one-round run of its own whose counts commit when it returns. A class
        that is not offered ``deliver`` holds ``None`` here instead (:meth:`__init_subclass__`).

        Args:
          batch: The events.

        Returns:
          The round's verdict.

        Raises:
          None. A ``BaseException`` from a hook or the opener propagates.
        """
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Sends every chunk of the batch in one request each, and classifies every event.

        ``_items`` renders and drops what cannot be sent alone; ``_take`` chunks the rest under
        the count limit and the run's byte budget; each chunk's request is built exactly as
        ``_send`` builds one — :meth:`_body`, then :meth:`_prepare`, which calls
        :meth:`_apply_auth`, then the ``Request`` — and made once by :meth:`_exchange`, which
        classifies it. Nothing here waits or retries: the engine resends the ``RETRYABLE``
        events in a later round.

        **The ``413`` search runs per run.** A ``413`` halves the budget to the refused body's
        size and re-chunks the round's remaining items; a lone refused item, or one
        :meth:`_known_refused` proves no smaller than one already refused, is ``DROPPED``; a
        multi-item chunk still refused after ``MAX_BUDGET_REDUCTIONS`` is ``REJECTED``. The
        budget, the smallest refused size and the reduction count live in the run's notes, so
        a later round starts from the budget already found.

        **A hook fault costs its chunk.** Everything that builds a chunk's request runs under a
        guard catching ``Exception``: a raise there makes the chunk's events ``DROPPED``, counted
        on ``_unrenderable`` and announced with the render fault's line, once per round with the
        first exception's type, since nothing was sent and the same inputs fail the same way. A
        ``2xx`` reader that raises sends its chunk through :meth:`_unadjudicated`: the request
        succeeded, so its events are ``UNKNOWN``. So no ``Exception`` escapes a round, which
        would fail the whole run although other chunks landed.

        The verdict's ``retry_after`` is the largest finite, positive ``Retry-After`` among the
        round's retryable responses, or ``None``; one round has one wait.

        Args:
          batch: The events this round is offered.

        Returns:
          A verdict with one outcome per event of ``batch``.

        Raises:
          None. A ``BaseException`` from any hook or the opener propagates.
        """
        context = _engine.run_context()
        notes = context.notes(self)
        drops = context.drops(self)
        budget = cast("int", notes.get("budget", self._chunk_budget()))
        refused_at = cast("int | None", notes.get("refused_at"))
        reductions = cast("int", notes.get("reductions", 0))
        tally = _Tally(drops)
        items = self._items(batch, tally)
        positions, fates = _positions(batch, items, tally.dropped)
        hints: list[float] = []
        faults = 0
        first: Exception | None = None
        index = 0
        while index < len(items):
            chunk = self._take(items, index, budget)
            where = positions[index : index + len(chunk)]
            try:
                known = self._known_refused(chunk, refused_at)
                if not known:
                    body, content_type = self._body(chunk)
                    headers, data = self._prepare(body, content_type, {})
                    request = urllib.request.Request(  # noqa: S310
                        self.url,
                        data=data,
                        method=self.method,
                        headers=cast("dict[str, str]", headers),
                    )
            except Exception as err:
                faults += len(chunk)
                first = first if first is not None else err
                _settle(fates, where, (Outcome.DROPPED,) * len(chunk))
                index += len(chunk)
                continue
            if known:
                self._drop_refused(chunk, drops)
                _settle(fates, where, (Outcome.DROPPED,))
                index += 1
                continue
            answer = self._exchange(request, splittable=True, count_unexpected=True)
            if answer.outcome is None:
                refused_at = len(body) if refused_at is None else min(refused_at, len(body))
                if len(chunk) > 1 and reductions < MAX_BUDGET_REDUCTIONS:
                    budget = max(min(budget, len(body)) // 2, 1)
                    reductions += 1
                    continue
                if len(chunk) == 1:
                    self._drop_refused(chunk, drops)
                    _settle(fates, where, (Outcome.DROPPED,))
                else:
                    self._abandon_unreduced(chunk)
                    _settle(fates, where, (Outcome.REJECTED,) * len(chunk))
                index += len(chunk)
                continue
            if answer.outcome is Outcome.ACCEPTED:
                outcomes = self._read(answer.payload, chunk)
            else:
                outcomes = (answer.outcome,) * len(chunk)
                hint = answer.retry_after
                if (
                    answer.outcome is Outcome.RETRYABLE
                    and hint is not None
                    and math.isfinite(hint)
                    and hint > 0
                ):
                    hints.append(hint)
            _settle(fates, where, outcomes)
            index += len(chunk)
        notes["budget"] = budget
        notes["refused_at"] = refused_at
        notes["reductions"] = reductions
        if first is not None:
            kind = type(first).__name__
            name = type(self).__name__
            lost = faults
            drops.count("_unrenderable", lost)
            drops.announce(lambda: _diag.lost("event", lost, f"{name}, rendering raised {kind}"))
        return Verdict(outcomes=tuple(fates), retry_after=max(hints) if hints else None)

    def _read(self, payload: bytes, chunk: list[_Item]) -> tuple[Outcome, ...]:
        """Reads a ``2xx`` through the class's reader, under the guard every reader runs in.

        A class whose bool ``_handle_response`` sits above its ``_outcomes`` is read through
        the base :meth:`_outcomes`, which is that override's verdict, even below a shipped reader
        that defines ``_outcomes`` (``_READS_BOOL``). A reader that raises makes the chunk
        unadjudicated with a line naming the exception's type; a result that is not one
        ``Outcome`` per event — not a sequence, the wrong length, or holding anything else — makes
        it unadjudicated with the line ``gave no outcome per event``. Either way the request
        succeeded, so its events may have landed.

        Args:
          payload: The response body; an ``_UnreadBody`` when it could not be read.
          chunk: The chunk the response describes.

        Returns:
          One outcome per item of ``chunk``.

        Raises:
          None. A ``BaseException`` from the reader propagates.
        """
        try:
            if type(self)._READS_BOOL:
                result = HTTPSink._outcomes(self, payload, chunk)
            else:
                result = self._outcomes(payload, chunk)
        except Exception as err:
            return self._unadjudicated(chunk, f"reading a 2xx response raised {type(err).__name__}")
        try:
            outcomes = _engine.outcomes_of(Verdict(outcomes=result), len(chunk))
        except Exception:
            outcomes = None
        if outcomes is None:
            return self._unadjudicated(chunk, "reading a 2xx response gave no outcome per event")
        return outcomes

    def _outcomes(self, payload: bytes, items: list[_Item]) -> tuple[Outcome, ...]:
        """Reports each event's fate from a ``2xx`` body: the hook a per-record reader overrides.

        The base asks :meth:`_handle_response`, so a 1.x override's bool keeps its meaning:
        ``True`` makes every event ``ACCEPTED`` and ``False`` makes every event ``REJECTED``,
        which a chunk of nothing delivered meant in 1.x. It counts nothing; an override counts
        what it reads through the run context, and gives up on a body it cannot read through
        :meth:`_unadjudicated`.

        Args:
          payload: The response body; an ``_UnreadBody``, equal to ``b""``, when it could not be
            read.
          items: The chunk the response describes.

        Returns:
          One outcome per item.

        Raises:
          Exception: Whatever :meth:`_handle_response` raises; the round's guard takes it.
        """
        verdict = Outcome.ACCEPTED if self._handle_response(payload, items) else Outcome.REJECTED
        return (verdict,) * len(items)

    def _unadjudicated(
        self, items: list[_Item], detail: str, noun: str = "event"
    ) -> tuple[Outcome, ...]:
        """Gives up on a ``2xx`` this sink cannot read: the one method every reader calls.

        The chunk is counted on ``dropped_unadjudicated`` — an upper bound folded into
        ``failed``, since its events may have landed — announced as ``lost <n> <noun>(s);
        <class> <detail>; they may have landed; not retried``, and every event is ``UNKNOWN``,
        which never fails a run. A shipped sink is not idempotent, so its ``UNKNOWN`` is final at
        once and this counts ``ALWAYS``. A subclass declaring ``log_foundry_idempotent = True``
        has an ``UNKNOWN`` chunk resent, so for it this counts ``LAST``: an event is counted once
        per run, and only when it ended the run unadjudicated.

        Args:
          items: The chunk the response describes.
          detail: What could not be read, carrying only counts and types, never the body.
          noun: What one item is, for the line.

        Returns:
          ``UNKNOWN`` for every item.

        Raises:
          None.
        """
        context = _engine.run_context()
        when = When.LAST if _engine._idempotent(self) else When.ALWAYS
        sent = len(items)
        name = type(self).__name__
        context.count(self, "dropped_unadjudicated", sent, when=when)
        context.announce(
            lambda: _diag.lost(noun, sent, f"{name} {detail}; they may have landed; not retried"),
            when=when,
        )
        return (Outcome.UNKNOWN,) * sent

    def _known_refused(self, chunk: list[_Item], refused_at: int | None) -> bool:
        """Reports whether this chunk is provably no smaller than one already refused.

        Only a lone item qualifies: a multi-item chunk can still be reduced, and reducing is
        cheaper than discarding. The point is to stop the tail of a hopeless run becoming
        one request per event once the budget has fallen below a single item.

        The comparison builds the item's **real body** rather than reading its charged size,
        because the two differ per body shape and one of them differs the wrong way: an item's
        charged size includes a separator ``SplunkHECSink``'s concatenated body never writes, so
        a lone item charged exactly ``refused_at`` builds a body one byte *smaller* than one
        already refused. Measured, that discarded an event whose 144-byte body fitted a 144-byte
        limit. The other shapes err the safe way (``+13`` for Loki, ``+1`` for a JSON array), but
        "provably no smaller" has to be provable for all of them, and ``refused_at`` is itself a
        body length — so comparing body lengths is the like-for-like test.

        **It is disabled whenever the request compresses** (:meth:`_compresses`, the rule
        :meth:`_prepare` compresses by), and that is the whole subtlety. ``_PayloadTooLarge``
        carries the *uncompressed* length, because that is what this sink measures and chunks by,
        while a gzipped request is judged by the destination on its compressed bytes. Compression
        ratio is per-event, so "larger uncompressed" stops implying "also refused": measured, one
        incompressible event set the mark at 532 bytes and a 6,020-byte event that gzipped to 64
        — against a 200-byte limit — was discarded without a single request being made. That is
        loss the library invented rather than loss it reported, which is what SPEC-025 and
        SPEC-026 exist to prevent, and it also inflated ``dropped``, whose contract is an exact
        count. Comparing like with like would mean gzipping each candidate purely to decide
        whether to ask, which costs more than the request it saves. It used to ask ``self.gzip``,
        which no longer decides once a caller ``Content-Encoding`` fixes compression at
        construction, so a sink set ``gzip = False`` afterwards beside ``Content-Encoding: gzip``
        compressed its body and yet discarded that 6,000-byte event unsent; the two reads are one
        predicate now.

        Args:
          chunk: The chunk about to be sent.
          refused_at: The smallest body this run has seen refused, or ``None``.

        Returns:
          True when the chunk can be discarded without offering it.

        Raises:
          None.
        """
        if self._compresses() or refused_at is None or refused_at <= 0 or len(chunk) != 1:
            return False
        return len(self._body(chunk)[0]) >= refused_at

    def _take(self, items: list[_Item], start: int, budget: int) -> list[_Item]:
        """Fills one chunk from ``start`` under the count limit and the current byte budget.

        Always yields at least one item, however small the budget: an item that cannot fit alone
        is the caller's to resolve, which :meth:`_round` does by dropping it once the destination
        has refused it. That mirrors ``chunk_items``, which this replaces here only because the
        budget can change between chunks and a generator cannot be re-aimed mid-iteration.

        Args:
          items: Every renderable item of the batch.
          start: The index to fill from.
          budget: The byte budget for this chunk.

        Returns:
          The chunk, never empty.

        Raises:
          None.
        """
        total = items[start].size
        end = start + 1
        while (
            end < len(items)
            and end - start < self.max_batch_count
            and total + items[end].size <= budget
        ):
            total += items[end].size
            end += 1
        return items[start:end]

    def _abandon_unreduced(self, chunk: list[_Item]) -> None:
        """Records a multi-item chunk the reduction budget could not shrink: ``REJECTED``.

        It is counted against ``failed`` and announced, ``ALWAYS``, but its events are not
        drops: they were never individually refused, so calling them permanent drops would invent
        loss, while saying nothing at all would leave them invisible whenever a sibling chunk
        delivered. Measured before this: 10 events in, 2 delivered, 8 gone, every counter at zero
        and stderr empty. It is final: a resend of the same events after the same search earns
        the same answer.

        Reaching it needs a pathological ``_item_size`` — with the default one, a multi-item
        chunk cannot survive ``MAX_BUDGET_REDUCTIONS`` halvings unless ``max_batch_count``
        exceeds 33 million.

        It keeps its 1.x signature, so a third-party override written against 1.x is still
        called; the count goes to the innermost run's context, read here through
        ``run_context()``, since it is only ever called inside a round.

        Args:
          chunk: The events that could not be placed.

        Returns:
          None.

        Raises:
          None.
        """
        context = _engine.run_context()
        name = type(self).__name__
        size = len(chunk)
        context.count(self, "failed", 1, when=When.ALWAYS)
        context.announce(
            lambda: _diag.lost(
                "request",
                1,
                f"{name}, {size} event(s) still 413 after "
                f"{MAX_BUDGET_REDUCTIONS} reduction(s); lower max_batch_bytes to match the "
                f"destination",
            ),
            when=When.ALWAYS,
        )

    def _drop_refused(self, chunk: list[_Item], pending: _Pending) -> None:
        """Discards a single item this destination will not accept at any size available.

        Only ever called with one item. It reaches here two ways: the item was offered alone and
        answered ``413``, or :meth:`_known_refused` proved its body is no smaller than one this
        run has already had refused — in which case it was **never offered**, which is the whole
        point of that shortcut. Either way it is permanently undeliverable here, so its event is
        ``DROPPED``, and the drop goes to ``pending`` against ``dropped_oversized``: it is
        counted and announced only when ``emit`` returns (SPEC-059 FR-004) — on the engine, only
        by a run that settles. Permanence alone never made ``dropped`` exact. Under 1.x a later
        chunk's raise sent the batch back to the worker, which re-ran the identical search and
        re-counted the identical drop once per attempt; now a run that fails — the drop beside a
        chunk refused for good — counts none of its drops, and its catcher owns the batch. It is
        the deferral that keeps ``dropped`` an exact count, while ``failed`` stays an upper
        bound. The line keeps its shape, one per drop.

        A *multi*-item chunk reaching the end of the reduction budget is deliberately **not**
        routed here. It needs a pathological ``_item_size`` to happen at all (measured: an
        override returning zero abandoned 20 events in whole chunks), and those events were never
        individually refused, so counting them as permanent drops would be loss the library
        invented. They are ``REJECTED`` instead (:meth:`_abandon_unreduced`).

        Args:
          chunk: The refused items.
          pending: The run's ``SETTLED`` view (``RunContext.drops``), or any ledger of the same
            shape, which holds the drop until the run is known to settle.

        Returns:
          None.

        Raises:
          None.
        """
        refused = len(chunk)
        pending.count("dropped_oversized", refused)
        pending.announce(
            lambda: _diag.lost(
                "event",
                refused,
                f"{type(self).__name__}, refused at the smallest request this sink can build",
            )
        )

    def _items(self, batch: list[dict[str, object]], pending: _Pending) -> list[_Item]:
        """Renders each event and discards any that cannot fit a request on its own.

        ``chunk_items`` documents oversized items as the caller's to filter, because a group it
        cannot bound is a group it would yield anyway; dropping here is what the AWS sinks
        already do in ``_records``. The drops are announced as **one** line carrying the count
        rather than one line per event, because a batch of oversized events is a configuration
        fault and 5,980 identical stderr lines describe it no better than one does.

        **Each event renders under its own guard** (SPEC-058 FR-005), around :meth:`_render` and
        :meth:`_item_size` both, catching ``Exception`` and never ``BaseException``: one event
        whose render raises is dropped and counted, rather than abandoning the batch. Unlike a
        wrapper's callable, a batch of nothing but unrenderable events is not re-raised —
        re-rendering identical input in the sink's own code fails identically, so a retry cannot
        help. The line names the class and the first exception's type, never its message. Both
        kinds of drop go to ``pending`` rather than to the counters.

        Args:
          batch: The events to render.
          pending: The run's ``SETTLED`` view (``RunContext.drops``), or a ``Deferral``, which
            holds every drop decided here until the run is known to settle, which is when
            ``emit`` returns (SPEC-058 FR-005).

        Returns:
          The items that can be sent, in order.

        Raises:
          None.
        """
        ceiling = self._event_ceiling()
        items: list[_Item] = []
        dropped = largest = failures = 0
        first: Exception | None = None
        for event in batch:
            try:
                rendered = self._render(event)
                size = self._item_size(rendered, event)
            except Exception as err:
                failures += 1
                first = first if first is not None else err
                continue
            if size > ceiling:
                dropped += 1
                largest = max(largest, size)
                continue
            items.append(_Item(event=event, rendered=rendered, size=size))
        if dropped:
            pending.count("dropped_oversized", dropped)
            pending.announce(
                lambda: _diag.lost(
                    "event",
                    dropped,
                    f"{type(self).__name__}, up to {largest} bytes exceeds the "
                    f"{ceiling}-byte per-event ceiling",
                )
            )
        if first is not None:
            kind = type(first).__name__
            pending.count("_unrenderable", failures)
            pending.announce(
                lambda: _diag.lost(
                    "event", failures, f"{type(self).__name__}, rendering raised {kind}"
                )
            )
        return items

    def _event_ceiling(self) -> int:
        """The most one rendered event may measure and still be sendable.

        Args:
          None.

        Returns:
          The smaller of the chunk budget and any per-event limit the destination publishes. The
          *chunk* budget rather than the raw configured one, because :meth:`_take` yields a
          lone oversized item regardless of budget, so an item admitted here at exactly
          ``max_batch_bytes`` produced a body that much plus :meth:`_body_reserve` — the one case
          where a body could still sit above the budget.

        Raises:
          None.
        """
        if self.MAX_EVENT_BYTES is None:
            return self._chunk_budget()
        return min(self._chunk_budget(), self.MAX_EVENT_BYTES)

    def _item_size(self, rendered: str, event: dict[str, object]) -> int:
        """Charges one rendered event its share of the request body.

        The default is its own bytes plus one for the separator that joins it to its neighbours.
        A subclass whose body adds framing *per group* rather than per item overrides this to
        charge that framing too — ``LokiSink`` does, because its streams carry a label map each,
        and without it a body ran 61% past the budget at high label cardinality.

        Args:
          rendered: The event's serialized form.
          event: The source event, for a subclass that sizes framing from its fields.

        Returns:
          The bytes to charge the chunker.

        Raises:
          None.
        """
        return len(rendered.encode("utf-8")) + 1

    def _body_reserve(self) -> int:
        """Bytes of whole-body framing not charged to any single item.

        A JSON array adds two brackets while the last item's separator goes unused, so its body
        is one byte longer than the item sizes sum to; NDJSON is exact, and Splunk's bare
        concatenation is shorter. Reserving keeps the budget an actual bound rather than a value
        the body is permitted to sit one byte above.

        Args:
          None.

        Returns:
          The bytes to withhold from the chunker's budget.

        Raises:
          None.
        """
        return 1 if self.body_format == "json_array" else 0

    def _chunk_budget(self) -> int:
        """The byte budget to chunk against, floored at one.

        The configured budget less :meth:`_body_reserve`. It is deliberately **not** adaptive.
        A revision that remembered a ``413``-refused size — so the halving cascade was not
        rediscovered on every batch — was measured to ratchet: one transient ``413`` took the
        budget from 1,000,000 to 44,345 permanently, and a five-minute proxy misconfiguration
        left a healthy sink issuing 200 requests where it had issued one. It also made the
        depth-bound abandonment recoverable-by-retry while the code reported it settled. The
        rediscovery it saved is real but is bounded by ``MAX_BUDGET_REDUCTIONS`` and only arises
        against a destination whose limit is below the configured budget, which has a direct
        remedy: pass ``max_batch_bytes=``. The ``413`` search's reduced budget is kept for one
        run, in the run's notes, so a later round's resend starts from it, and never carried into
        a later batch (SPEC-073 FR-001).

        Args:
          None.

        Returns:
          The budget in bytes.

        Raises:
          None.
        """
        return max(self.max_batch_bytes - self._body_reserve(), 1)

    def _render(self, event: dict[str, object]) -> str:
        """Serializes one event into this sink's per-event wire form.

        The hook a subclass overrides to wrap or enrich an event — Datadog's ``ddsource`` keys,
        Splunk's HEC envelope, Elasticsearch's action-plus-source pair. It must not include the
        separator that joins it to its neighbours; :meth:`_body` adds that.

        Args:
          event: The event to serialize.

        Returns:
          The serialized form.

        Raises:
          TypeError: If the event is not JSON-serializable. Neither source of events should
            produce one: assembly coerces every value (``sanitize``), and a ``TransformSink``
            checks and coerces what its function returns (SPEC-058 FR-001). If one does,
            ``HTTPSink._items`` drops that event alone and counts it (SPEC-058 FR-005).
        """
        return json.dumps(event)

    def _body(self, items: list[_Item]) -> tuple[bytes, str]:
        """Joins one chunk's rendered items into a request body.

        Args:
          items: The chunk's items, known non-empty.

        Returns:
          The body bytes and the default content type for the configured format.

        Raises:
          None.
        """
        if self.body_format == "json_array":
            return (
                ("[" + ",".join(item.rendered for item in items) + "]").encode("utf-8"),
                "application/json",
            )
        return (
            "".join(item.rendered + "\n" for item in items).encode("utf-8"),
            "application/x-ndjson",
        )

    def _handle_response(self, payload: bytes, items: list[_Item]) -> bool:
        """Reports whether a ``2xx`` response means the chunk was actually accepted.

        The hook for a destination whose ``2xx`` can still describe per-record failures —
        Elasticsearch's ``_bulk`` items, Honeycomb's per-event array, OTLP's partial success. The
        default ignores the body and reads a status in the success range as delivery; that is the
        whole answer only where the destination's ``2xx`` carries no per-record outcome.

        The payload equals ``b""`` both for an empty body and for one that could not be read
        after the status line — cut short, stalled past ``timeout`` or reset (:meth:`_attempt`).
        For the second it is an :class:`_UnreadBody`, which :func:`_read_failed` tells apart
        (SPEC-070 FR-004), so an override whose verdict is in the body can count an unreadable
        one even where it reads an empty one as success; the three shipped readers ask it. An
        override that does not ask reads the two the same way, as under ``1.x``. The default
        ignores the body and settles both as delivered: no body is its verdict.

        Args:
          payload: The response body; an :class:`_UnreadBody`, equal to ``b""``, when it could
            not be read.
          items: The chunk the response describes.

        Returns:
          True, meaning delivered.

        Raises:
          None.
        """
        return True

    def _retryable_status(self, status: int) -> bool:
        """Reports whether a status is worth another attempt (SPEC-056 FR-003).

        The family's default is ``429`` and every ``5xx``. It is a hook, read at one site,
        :meth:`_exchange`'s classification, so a subclass whose protocol names its own retryable set — OTLP's is
        ``429``, ``502``, ``503`` and ``504``, with a ``500`` abandoned on the first request —
        narrows it without touching the loop, and every other sink is unchanged by construction.

        Args:
          status: The HTTP status of the response.

        Returns:
          True when the request may be retried.

        Raises:
          None.
        """
        return status == 429 or 500 <= status < 600

    def _retryable_error(self, err: Exception) -> bool:
        """Reports whether a failure before any status line is worth another attempt (SPEC-070).

        The three classes are the ones :meth:`_send`'s ``except`` named before it widened to
        ``Exception``: a connection-level failure or a status line or header block that would not
        parse. Anything else — what a caller-injected ``opener`` can raise — fails at once.

        Args:
          err: The exception the attempt raised.

        Returns:
          True for a ``URLError``, an ``OSError`` or an ``http.client.HTTPException``.

        Raises:
          None.
        """
        return isinstance(err, (urllib.error.URLError, OSError, http.client.HTTPException))

    def _response_limit(self) -> int | None:
        """The most response bytes :meth:`_attempt` reads, or ``None`` for the unbounded read.

        ``None`` is the family's default and keeps :meth:`_attempt` calling a bare ``read()``,
        which is what every existing sink and every test double of a response relies on. A
        subclass returning a bound makes :meth:`_attempt` read at most one byte past it; a
        non-``2xx`` response longer than the bound is then abandoned by :meth:`_exchange` as
        non-retryable, whatever its status — a ``413`` included, which is read literally rather
        than as a request to split, since a server answering a refusal with megabytes is not
        describing the request — and a ``2xx`` reaches :meth:`_handle_response` as it was read,
        for that method to judge against the same bound (SPEC-056 FR-003).

        Args:
          None.

        Returns:
          The bound in bytes, or ``None``.

        Raises:
          None.
        """
        return None

    def losses(self) -> SinkLosses:
        """Reports oversized drops and abandoned requests (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters. ``failed`` counts abandoned requests rather than events, since this
          class has no per-event outcome to report and an abandoned request took its whole body
          with it, plus ``dropped_unadjudicated``'s events, whose ``2xx`` could not be read;
          subclasses that do learn per-record outcomes add them. ``dropped`` is the oversized
          events plus those whose render raised or whose chunk's request could not be built
          (SPEC-058 FR-005, SPEC-073 FR-001).

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized + self._unrenderable,
                failed=self.failed + self.dropped_unadjudicated,
            )

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

    def _send(
        self,
        body: bytes,
        *,
        content_type: str,
        extra_headers: dict[str, str] | None = None,
        splittable: bool = False,
        count_unexpected: bool = True,
    ) -> bytes:
        """POSTs one body with the sink's own retries, on the delivery engine, and returns the reply.

        The 1.x contract, kept for callers outside the chunk path — ``SentrySink``'s fallback,
        and a third-party subclass that builds its own body. The chunk path does not call it, so
        an override of ``_send`` no longer sees the chunks. Its attempts run as rounds of one
        request through the engine's ``_run``: the budget is ``max_retries``, the stop event
        ``log_foundry_stop_signal``, there is no decline predicate, and the classification is
        :meth:`_exchange`'s, the same one the round reads, so the two cannot drift. Its counts and
        lines are its own run's, committed when that run ends, so it makes ``max_retries + 1``
        attempts and counts as 1.x did even when called from inside another run's round, waiting
        inside that round. The engine's event totals for it go to a throwaway ``SinkCounters``:
        a request is not an event.

        A failure **before** any status line that :meth:`_retryable_error` accepts — an
        ``OSError``, a ``URLError``, an ``http.client.HTTPException`` such as ``BadStatusLine``
        — is retried, then abandoned and counted in ``failed``. **After** a status line the
        status decides, whatever happened to the body (:meth:`_attempt`). Any other exception —
        what a caller-injected ``opener`` can raise — is abandoned at its first attempt with no
        wait (SPEC-070 FR-001), through the same line, so its attempt count reads ``1``.

        The header map is handed to ``urllib`` under the ``str``-valued type its stub declares,
        the one cast on this path and the round's: ``Request`` stores values untouched and
        ``putheader`` sends an ``int`` or ``bytes`` value itself, which is how a caller's has
        always gone out.

        The header map is handed to ``urllib`` under the ``str``-valued type its stub declares,
        the one cast on this path: ``Request`` stores values untouched and ``putheader`` sends an
        ``int`` or ``bytes`` value itself, which is how a caller's has always gone out.

        Args:
          body: The serialized batch.
          content_type: The default content type, overridable by caller headers.
          extra_headers: Per-request headers beneath the caller's own.
          splittable: Whether the caller can respond to a ``413`` by sending smaller requests;
            only then is a ``413`` raised as ``_PayloadTooLarge``, uncounted. Every other caller —
            ``SentrySink``, which sends one envelope per event and has nothing left to split —
            keeps the counted, announced abandonment it has always had.
          count_unexpected: Whether an exception :meth:`_retryable_error` rejects is counted and
            abandoned here. ``SentrySink`` passes ``False``: it counts such an exception on its
            own ``.transport_errors``, a documented counter, so moving the count to this held
            sink's ``failed`` would reclassify it in a minor release. With ``False`` the
            exception is attempted once and re-raised unchanged, the same object, after the run
            ends, counted by neither this nor its run's line — unless it is a
            ``SinkDeliveryError`` (``_PayloadTooLarge`` included), which every caller reads as
            already counted and announced; that one is abandoned here, counted once in
            ``failed``, so an opener raising it is never counted nowhere.

        Returns:
          The response bytes, which subclasses that must inspect the response — such as
          Elasticsearch's ``_bulk`` items — read. An :class:`_UnreadBody`, equal to ``b""``, when
          a ``2xx`` body could not be read, which :func:`_read_failed` tells from an empty one.

        Raises:
          Exception: An exception :meth:`_retryable_error` rejects, unchanged, only when
            ``count_unexpected`` is False and it is not a ``SinkDeliveryError``; and anything
            building the ``Request`` raises, unchanged, as 1.x let it escape.
          _PayloadTooLarge: On a ``413`` when ``splittable``. It moves no counter and writes no
            line, because the caller is about to re-send the same events in smaller requests.
          SinkDeliveryError: When the request was abandoned: refused, or still failing when the
            rounds ran out. ``failed`` has moved by one and its line is written first.
        """
        headers, data = self._prepare(body, content_type, extra_headers)
        sent = _Sent()

        def one(batch: list[dict[str, object]]) -> Verdict:
            """Makes one attempt, as one round of the run.

            Args:
              batch: The run's single placeholder event.

            Returns:
              The attempt's outcome, with its ``Retry-After``.

            Raises:
              None. A ``BaseException`` from the opener propagates.
            """
            try:
                request = urllib.request.Request(  # noqa: S310
                    self.url, data=data, method=self.method, headers=cast("dict[str, str]", headers)
                )
            except Exception as err:
                sent.unbuilt = err
                return Verdict(outcomes=(Outcome.REJECTED,))
            answer = self._exchange(
                request, splittable=splittable, count_unexpected=count_unexpected
            )
            sent.answer = answer
            outcome = Outcome.REJECTED if answer.outcome is None else answer.outcome
            return Verdict(outcomes=(outcome,), retry_after=answer.retry_after)

        _engine._run(
            self,
            one,
            SinkCounters(),
            [{}],
            sink_retries=self.max_retries,
            worker_retries=0,
            stop=_engine.stop_signal_of(self),
            decline=None,
        )
        if sent.unbuilt is not None:
            raise sent.unbuilt
        answer = sent.answer
        if answer is None:
            raise SinkDeliveryError(f"{type(self).__name__} made no attempt")
        if answer.outcome is Outcome.ACCEPTED:
            return answer.payload
        if answer.error is not None:
            raise answer.error
        if answer.outcome is None:
            raise _PayloadTooLarge(f"{type(self).__name__}, HTTP 413", len(body))
        raise SinkDeliveryError(answer.detail)

    def _exchange(
        self, request: urllib.request.Request, *, splittable: bool, count_unexpected: bool
    ) -> _Answer:
        """Makes one request and classifies it: the one table the round and :meth:`_send` read.

        ========================================================  ==============  ================
        The request                                               Outcome         ``failed``
        ========================================================  ==============  ================
        an exception :meth:`_retryable_error` accepts             ``RETRYABLE``   ``LAST``
        an exception it refuses, ``count_unexpected`` false        ``REJECTED``    none, re-raised
        any other exception it refuses                            ``REJECTED``    ``ALWAYS``
        a ``2xx``                                                 ``ACCEPTED``    the reader's
        a non-``2xx`` longer than :meth:`_response_limit`          ``REJECTED``    ``ALWAYS``
        a status :meth:`_retryable_status` accepts                ``RETRYABLE``   ``LAST``
        a ``413``, ``splittable``                                 the search      none
        any other status                                          ``REJECTED``    ``ALWAYS``
        ========================================================  ==============  ================

        ``failed`` counts the **request**, its line naming the attempts its events have had,
        which is the run's round. A retryable request counts ``LAST``, only when its round is the
        run's last, so a request a later round lands is never counted; one refused for good
        counts ``ALWAYS``. The checks run in 1.x ``_send``'s order, so a subclass whose
        ``_retryable_status`` accepts ``413`` still retries it. A ``SinkDeliveryError`` an opener
        raises is always counted here, since every caller reads that type as already counted.

        Args:
          request: The request to make.
          splittable: Whether a ``413`` goes to the caller's search, uncounted.
          count_unexpected: Whether a refused exception is counted here or handed back.

        Returns:
          The classification.

        Raises:
          None. A ``BaseException`` from the opener propagates.
        """
        context = _engine.run_context()
        try:
            status, payload, retry_after = self._attempt(request)
        except Exception as err:
            reason = f"connection error, {type(err).__name__} {_diag.errno_of(err)}".rstrip()
            if is_retryable(self._retryable_error, err):
                return _Answer(Outcome.RETRYABLE, detail=self._give_up(context, reason, When.LAST))
            if not count_unexpected and not isinstance(err, SinkDeliveryError):
                return _Answer(Outcome.REJECTED, error=err)
            return _Answer(Outcome.REJECTED, detail=self._give_up(context, reason, When.ALWAYS))
        if 200 <= status < 300:
            return _Answer(Outcome.ACCEPTED, payload=payload)
        limit = self._response_limit()
        if limit is not None and len(payload) > limit:
            reason = f"HTTP {status}, response over {limit} bytes"
            return _Answer(Outcome.REJECTED, detail=self._give_up(context, reason, When.ALWAYS))
        if self._retryable_status(status):
            detail = self._give_up(context, f"HTTP {status}", When.LAST)
            return _Answer(Outcome.RETRYABLE, retry_after=retry_after, detail=detail)
        if status == 413 and splittable:
            return _Answer(None, payload=payload)
        return _Answer(
            Outcome.REJECTED, detail=self._give_up(context, f"HTTP {status}", When.ALWAYS)
        )

    def _give_up(self, context: RunContext, reason: str, when: When) -> str:
        """Counts one abandoned request on ``failed`` and registers its line, as ``when`` says.

        Args:
          context: The run's context, whose round is the attempt count the line names.
          reason: Why, carrying only library-controlled values — a status, an exception type, an
            ``errno`` — never a server's body or an exception's text (SPEC-029 FR-002).
          when: Which run endings commit it.

        Returns:
          The detail, which :meth:`_send` raises as its ``SinkDeliveryError`` message.

        Raises:
          None.
        """
        detail = f"{type(self).__name__}, {context.round} attempt(s), {reason}"
        context.count(self, "failed", 1, when=when)
        context.announce(lambda: _diag.lost("request", 1, detail), when=when)
        return detail

    def _prepare(
        self,
        body: bytes,
        content_type: str,
        extra_headers: dict[str, str] | None,
    ) -> tuple[dict[str, _HeaderValue], bytes]:
        """Builds the final header map and the optionally gzipped body.

        Caller-provided headers override the sink's defaults (SPEC-009 FR-002), and that includes
        ``Content-Encoding``: ``gzip=True`` used to overwrite one the caller had set, which made
        it the single header of theirs that did not win. A caller's own value both survives *and*
        suppresses the compression, because a header saying ``identity`` over a gzipped body
        makes the destination decode garbage. The test is against ``self._headers`` rather than
        the merged map, so a per-request ``extra_headers`` value — which sits *beneath* the
        caller's own — does not switch compression off (SPEC-048 FR-007). The caller's header is
        found by the name ``urllib`` sends (:func:`_caller_encoding`), not by exact case: a
        ``content-encoding: identity`` used to miss the test, so the body was gzipped and a
        second ``Content-Encoding: gzip`` written after it, which ``urllib`` folded over the
        caller's. The caller's own spelling is the one sent, and it is the only one.

        A caller's header that **names gzip** is the exception to "suppresses": the body is
        compressed and sent under the caller's header, since that is what both said
        (:func:`_names_gzip`). It used to be sent uncompressed and labelled ``gzip``, undecodable
        on every ``emit`` — in exact case since SPEC-048 FR-007, and in any case once the name was
        folded.

        **When the caller set a** ``Content-Encoding``, **whether the body is compressed is fixed
        at construction** (1.2.0, :meth:`_compresses`), from the ``gzip`` given then, and setting
        ``gzip`` afterwards changes nothing. For the library's own ``_body``,
        :func:`_valid_encoding` lets a header survive construction only when it names gzip beside
        ``gzip=True`` or names no coding beside ``gzip=False``, so the fixed decision is exactly
        "compressed when the header names gzip": a sink whose ``gzip`` is set true beside an
        ``identity`` header still sends the body as built, and one whose ``gzip`` is set false
        beside a ``gzip`` header still compresses — it used to send an uncompressed body under the
        caller's ``gzip`` label on every request. For a subclass whose own ``_body`` encodes the
        body the library never compresses beside a caller header, with ``gzip`` true or false, so
        it keeps 1.1.0's bytes, and is never gzipped a second time, which setting ``gzip`` true
        used to do. With no caller header, ``self.gzip`` decides per request, as it always has, and
        the sink writes its own ``Content-Encoding: gzip``.

        Args:
          body: The serialized batch.
          content_type: The default content type.
          extra_headers: Per-request headers beneath the caller's own.

        Returns:
          The headers and the body to send.

        Raises:
          None.
        """
        headers: dict[str, _HeaderValue] = {"Content-Type": content_type}
        if extra_headers:
            headers.update(extra_headers)
        headers.update(self._headers)
        data = body
        if self._compresses():
            data = _gzip.compress(body)
            if self._fixed_compression is None:
                headers["Content-Encoding"] = "gzip"
        self._apply_auth(headers)
        return headers, data

    def _compresses(self) -> bool:
        """Says whether this request's body is gzipped: the one rule every send-path read asks.

        With a caller ``Content-Encoding``, the decision fixed at construction answers
        (``_fixed_compression``: the ``gzip`` given then for the library's own ``_body``, which is
        exactly "the header names gzip", and ``False`` for a subclass's own ``_body``, which owns
        its coding); otherwise ``self.gzip`` does, read now, since it is
        a public attribute. Construction stores ``gzip`` as an exact ``bool``, so no caller
        ``__bool__`` runs here; only a non-``bool`` assigned to ``gzip`` afterwards is asked, by
        ``bool()``, as 1.x asked it. :meth:`_prepare` compresses by this and
        :meth:`_known_refused` stands down by it, so the refusal memory, which holds uncompressed
        lengths, is never applied to a body that went out compressed, and the two cannot disagree.

        Args:
          None.

        Returns:
          Whether the body is compressed.

        Raises:
          None.
        """
        if self._fixed_compression is not None:
            return self._fixed_compression
        gzip = self.gzip
        return gzip if type(gzip) is bool else bool(gzip)

    def _apply_auth(self, headers: dict[str, _HeaderValue]) -> None:
        """Applies bearer-token or basic-auth credentials, when the sink was given any.

        Nothing the caller wrote is overwritten: construction refuses ``auth`` beside an
        ``Authorization`` header in any case (:func:`_valid_auth`), so ``self._headers`` holds
        none whenever there are credentials to apply. The guard that used to defer to an
        ``Authorization`` already in the map is gone with the case it served. What it could
        still have seen is a per-request ``extra_headers`` value, and that sits *beneath* the
        caller's own (SPEC-048 FR-007) — ``auth`` is the caller's own — so the credentials are
        written last and win, under any case, as ``urllib`` keeps the value written last for a
        folded name. No shipped subclass passes one: ``SentrySink``, the only caller of
        ``extra_headers``, passes ``X-Sentry-Auth`` and builds its sink without ``auth``, and
        ``tests/test_sinks_http_auth.py`` holds every ``extra_headers=`` key in ``src/`` to that.

        The value written is the one :func:`_valid_auth` built and checked at construction, as a
        plain ``str``, so no caller code runs per request and nothing is rebuilt.

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
        if self._authorization_value is not None:
            headers["Authorization"] = self._authorization_value

    def _attempt(self, request: urllib.request.Request) -> tuple[int, bytes, float | None]:
        """Performs one request.

        A ``4xx`` or ``5xx`` arrives from ``urlopen`` as an ``HTTPError``, which is itself a
        readable response object, so it is unified here rather than raised.

        **The status line is the dividing line.** Everything before it — the connection, the
        request, a status line or header block the server garbled — raises, and
        :meth:`_exchange` makes its events ``RETRYABLE``, if :meth:`_retryable_error` accepts it,
        as a connection error. Once a status
        is in hand the server has answered, so a body read that fails afterwards is not a fault to
        retry but a body nobody can read: the payload is an :class:`_UnreadBody` and the status,
        with its ``Retry-After``, decides as it always did. A
        ``2xx`` therefore settles its chunk however its body ends, because a ``2xx`` is not
        provable non-delivery and a re-send could duplicate what landed (SPEC-018's rule); and a
        ``503`` or a ``413`` whose body was cut short is still a ``503`` or a ``413``. Measured
        before this guard: a ``2xx`` with a short body raised ``IncompleteRead`` out of ``emit``,
        so the worker re-sent the chunks that had landed and never sent the ones after; a stall
        or a reset mid-body was retried as a connection error, re-POSTing an accepted chunk once
        per retry and then counting it in ``failed``.

        An unreadable body is an :class:`_UnreadBody`, which equals ``b""``, rather than ``None``,
        which keeps :meth:`_handle_response`'s signature, a documented extension hook, exactly as
        it was. An override that does not ask :func:`_read_failed` cannot tell an empty body from
        an unreadable one and reads both the same way; the three shipped readers ask it and count
        an unreadable ``2xx`` body unadjudicated (SPEC-070 FR-004). An honestly empty body stays
        plain ``b""``.

        The guard catches ``Exception``, not the read's documented errors, for the reason
        the readers give for their own: anything escaping here would escape the round, fail the
        engine's run and report as undelivered a batch the destination may have accepted. It
        never catches ``BaseException``.

        The read takes no size argument unless :meth:`_response_limit` names a bound, and the
        bare form is load-bearing. CPython's ``read()`` raises ``IncompleteRead`` for a
        ``Content-Length`` body that arrives short and for a truncated chunked body, but
        ``read(amt)`` returns a short ``Content-Length`` body **silently**, leaving
        ``response.length`` above zero (measured on 3.12 and 3.13). So the bounded read, which
        asks for one byte past the bound so :meth:`_exchange` and the reader can tell
        "exactly the bound" from "over it", sits inside the same guard and checks
        ``response.length`` itself: fewer bytes than it asked for while the response still owes
        some is a body nobody can read, and becomes an :class:`_UnreadBody` exactly as a raise
        does. A truncated chunked body raises from ``read(amt)`` too and takes the guard. A body
        with neither length nor chunking is read to EOF, where truncation cannot be detected at
        all. With the default ``None`` every other sink runs the bare ``read()`` under the guard,
        unchanged (SPEC-056 FR-003).

        Args:
          request: The prepared request.

        Returns:
          The status, the response payload — an :class:`_UnreadBody` when the body could not be
          read — and any ``Retry-After`` delay.

        Raises:
          urllib.error.URLError: On a connection-level failure, for :meth:`_exchange` to classify.
          OSError: Likewise.
          http.client.HTTPException: When the status line or headers could not be parsed, such
            as ``BadStatusLine``, which :meth:`_exchange` classifies as a connection error.
        """
        try:
            response = self._opener(request, timeout=self.timeout)
        except urllib.error.HTTPError as err:
            response = err
        status = getattr(response, "status", None)
        if status is None:
            status = response.getcode()
        limit = self._response_limit()
        try:
            if limit is None:
                payload = response.read()
            else:
                payload = response.read(limit + 1)
                if len(payload) <= limit and (getattr(response, "length", None) or 0) > 0:
                    payload = _UnreadBody()
        except Exception:
            payload = _UnreadBody()
        return int(status), payload, _parse_retry_after(getattr(response, "headers", None))


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
