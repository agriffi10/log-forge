"""HTTPSink — POST batches over the stdlib ``urllib`` (arch §8, §9.1, SPEC-009)."""

from __future__ import annotations

import gzip as _gzip
import http.client
import json
import re
import threading
import unicodedata
import urllib.error
import urllib.request
from base64 import b64encode
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, NoReturn, TypedDict, cast
from urllib.parse import unquote, unwrap, urlparse

from log_foundry import _diag
from log_foundry.sinks._args import require_callable
from log_foundry.sinks._classify import is_retryable
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks._retry import (
    clamp_server_delay,
    exact_seconds,
    floor_count,
    require_positive,
    require_timeout,
    wait,
)
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses

__all__ = [
    "HTTPAuthKwargs",
    "HTTPForwardKwargs",
    "HTTPKwargs",
    "HTTPPlatformKwargs",
    "HTTPRetryKwargs",
    "HTTPSink",
    "merge_headers",
]

_BACKOFF_BASE = 0.1

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
"""``413``-driven halvings of the byte budget one ``emit`` may make (FR-001 AC-4).

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
    — ``SentrySink`` above all — keeps catching it exactly as before. Only the chunk loop, which
    passes ``splittable=True``, ever sees it as its own type.

    Attributes:
      size: The refused body's length in bytes, which lets one ``emit`` avoid building another
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
    ``*args, **kwargs`` wrapper forwarding to one, would raise ``TypeError`` out of ``emit``
    exactly when a body fails to read, and the worker re-sends anything escaping ``emit``. An
    override that does not ask :func:`_read_failed` sees ``b""``, as under ``1.x``; it has no
    content, and ``json.loads``, ``len`` and slicing behave on it as on ``b""``.
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


def _valid_headers(headers: dict[str, str], owner: str) -> dict[str, str]:
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

    Args:
      headers: The caller's headers.
      owner: The class refusing them, for the message.

    Returns:
      The headers unchanged.

    Raises:
      ValueError: If any name or value contains ``CR`` or ``LF``, ``http.client`` cannot send
        the header, or a name contains whitespace anywhere.
    """
    for name, value in headers.items():
        for text, part in ((name, "name"), (str(value), "value")):
            if any(bad in text for bad in _CRLF):
                raise ValueError(
                    f"{owner} header {part} must not contain CR or LF: {name!r}"
                )
        failure = _unsendable_header(_wire_name(name), value)
        if failure:
            raise ValueError(f"{owner} header cannot be sent over HTTP ({failure}): {name!r}")
        if any(char.isspace() for char in name):
            raise ValueError(f"{owner} header name must not contain whitespace: {name!r}")
    return headers


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


def _unsendable_header(name: str, value: str) -> str | None:
    """Names why ``http.client`` cannot send one header, or returns ``None`` when it can.

    It returns the exception's type name rather than raising, so a caller's refusal is raised
    outside the ``except`` with neither ``__cause__`` nor ``__context__`` set: the exception
    ``putheader`` raises carries the whole value — a ``UnicodeEncodeError``'s ``object``, a
    ``ValueError``'s message — and a header value is as likely to be a credential as not.

    Args:
      name: The header name, as it goes on the wire.
      value: The header value.

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
    auth: str | tuple[str, str] | None, owner: str, headers: dict[str, str], conflict: str
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
        ``str`` (:func:`_copied_pair`), or ``http.client`` cannot send the ``Authorization``
        header either form builds.
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
    value, failure = _built_authorization(stored)
    if value is None:
        raise ValueError(f"{owner} auth cannot be sent over HTTP ({failure})")
    failure = _unsendable_header("Authorization", value)
    if failure:
        raise ValueError(f"{owner} auth cannot be sent over HTTP ({failure})")
    return stored, value


_NOT_A_PAIR = (set, frozenset, Mapping)
"""Containers that unpack into two items but hold no ``(user, password)`` order (SPEC-049)."""


def _copied_pair(auth: object, owner: str) -> tuple[str, str]:
    """Copies a ``(user, password)`` pair once into a ``tuple`` of two exact ``str``, or raises.

    The unpack is the one ``_authorization`` used to perform on every ``emit``, so what it refuses
    and the message it gets are unchanged — a non-iterable raises ``TypeError``, too few or too
    many items ``ValueError``, and no more than three items are ever read. Its failure is caught
    as a type name and the refusal raised outside the ``except``, so nothing chained carries the
    credential.

    A value that unpacks but is outside the declared ``tuple[str, str]`` is then refused rather
    than sent, breaking no type-checked caller: each constructed and drew a ``401`` on every
    request. A ``set`` or ``frozenset`` of two unpacks in hash order, so ``{"alice", "pw"}`` sent
    either order depending on ``PYTHONHASHSEED``; a mapping unpacks into its keys; ``b"to"``
    unpacks into two ``int``; and a ``bytes`` or ``None`` half was formatted as its ``repr``. The
    container is judged after the unpack, so a container of the wrong length keeps the unpack's
    message; nothing derived from its order is ever used. Both halves are judged by their real
    types and copied with ``_diag._STR_STR``, which runs none of a subclass's methods. The message
    names only the real type names, never a value. It is a ``ValueError`` although the fault is a
    type, because every refusal of ``auth`` is one — ``auth=123`` has been
    ``ValueError(... (TypeError))`` since SPEC-049 — and a caller catching the one exception
    ``auth`` raises should not need a second.

    Args:
      auth: The credentials as the caller passed them, already known not to be a ``str``.
      owner: The class refusing it, for the message.

    Returns:
      The copied pair.

    Raises:
      ValueError: If the value does not unpack into exactly two items, is a ``set``,
        ``frozenset`` or mapping, or either item is not a ``str``.
    """
    failure: str | None = None
    try:
        user, password = cast("tuple[object, object]", auth)
    except (ValueError, TypeError) as err:
        failure = type(err).__name__
    if failure is not None:
        raise ValueError(f"{owner} auth cannot be sent over HTTP ({failure})")
    if issubclass(type(auth), _NOT_A_PAIR):
        raise ValueError(  # noqa: TRY004
            f"{owner} auth must be a str token or a pair of two str, not {_type_name(auth)}"
        )
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


def merge_headers(base: dict[str, str], http_kwargs: HTTPForwardKwargs) -> dict[str, str]:
    """Merges a platform sink's own headers with any caller-supplied ones, caller winning.

    This is shared by the SaaS sinks that set a fixed auth header — Datadog, Splunk, New Relic,
    Honeycomb. The caller's ``headers`` are read with ``dict()``, as :class:`HTTPSink` reads
    its own, so a ``MappingProxyType`` or a list of pairs is honoured — and then judged by every
    header and auth rule — rather than dropped: an ``isinstance(caller, dict)`` test here used
    to discard any other mapping silently, ``Authorization`` beside ``auth=`` included.

    Args:
      base: The sink's own headers, updated in place.
      http_kwargs: The caller's keyword arguments, mutated by popping ``headers`` out so the
        rest can be forwarded to :class:`HTTPSink` without a duplicate argument. Typed as the
        narrowest shape (SPEC-051 FR-005); every wider one is assignable to it, and ``pop`` is
        allowed because each key is ``total=False``. The pop is why a caller may still pass
        ``headers``, and — since it happens *here*, in another function — why ``mypy`` reports
        the caller's own forwarding call as a possible duplicate anyway.

    Returns:
      The merged headers.

    Raises:
      TypeError: If the caller's ``headers`` is not a mapping or an iterable of pairs, as
        ``dict()`` raises for :class:`HTTPSink`'s own.
      ValueError: If an item of an iterable ``headers`` is not a pair, likewise.
    """
    caller = http_kwargs.pop("headers", None)
    if caller is not None:
        base.update(dict(caller))
    return base


class HTTPSink:
    """A :class:`~log_foundry.sinks.base.Sink` that POSTs each batch to an HTTP endpoint.

    This is the dependency-free base every other HTTP platform sink builds on: it re-chunks a
    batch to the destination's limits, serializes each chunk, applies headers, auth and optional
    gzip, POSTs it, and retries ``429`` and ``5xx`` with bounded exponential backoff. These are
    terminal, direct-ship sinks — per arch §9.1 they couple application delivery to the
    destination's availability, so for durability put a queue in front.

    **Subclasses extend it by overriding hooks, never** :meth:`emit` (FR-001). The chunk loop
    lives here, so a subclass that overrode ``emit`` would deliver whatever batch it was handed —
    which is what three shipped subclasses did, and why ``Worker._final_drain`` could hand this
    family an entire exit backlog (5,980 events measured) in one request that the destination
    then rejected whole. The hooks are :meth:`_render`, :meth:`_body` and
    :meth:`_handle_response`; a test asserts no subclass defines ``emit``.

    Credentials passed as ``auth`` are sent on every request to the URL as given, and nothing
    here requires ``https://``: over a plaintext endpoint a bearer token, and a basic-auth pair,
    travel in the clear. Credentials written into the URL itself are refused, since ``urllib``
    sends them as part of the host name and never as credentials; the refusal names where this
    class takes them instead, ``_CREDENTIALS``, which a subclass with a credential argument of its
    own overrides.

    **The worst case is now per chunk, not per batch** (FR-001). One chunk costs ``max_retries``
    waits, each at most ``max_retry_after`` when the server sends a ``Retry-After`` and
    exponential otherwise — 90 s at the defaults, plus the request timeouts — and an ``emit``
    pays that for *each* chunk it produces. All of it runs on the single drain thread, so it is a
    pause on all log delivery, and a large enough backlog of failing chunks can outlast
    ``shutdown()``'s own timeout, which then expires and leaves the sink open by SPEC-027
    FR-004's rule. What bounds it is ``shutdown(timeout=)`` itself, plus
    ``MAX_BUDGET_REDUCTIONS`` on the ``413`` search; sizing ``max_batch_bytes`` to the
    destination is what keeps the chunk count low in the first place. Nothing here *skips* work
    because a shutdown is in progress — a revision that did was measured to disable both retries
    and the ``413`` search for the whole of ``Worker._final_drain``, since the stop event is set
    before the join, losing events that had been delivering. The signal is read at one site,
    :meth:`_sleep_backoff`, where SPEC-027 has it cut a *wait* short rather than cancel an
    attempt.

    Attributes:
      MAX_BATCH_COUNT: Entries one request may carry. A subclass overrides it with its
        destination's documented figure.
      MAX_BATCH_BYTES: Bytes one request's body may reach, likewise.
      MAX_EVENT_BYTES: A destination's published limit on a *single* event, or ``None`` when it
        publishes none. Distinct from the request budget: Datadog accepts a 5 MB payload but
        caps one log at 1 MB, so an event between the two is rejected by a limit the request
        budget cannot see.
      failed: Requests abandoned past the retry bound, counted per chunk.
      dropped_oversized: Events dropped for exceeding the per-event ceiling, plus those a
        destination answered ``413`` to and no split could rescue. Both are counted and
        announced only when ``emit`` returns (SPEC-058 FR-005, SPEC-059 FR-004), so a batch the
        worker retries counts each once. An event whose render raised is counted apart from this
        and folded into ``losses().dropped``, so this stays oversize only.

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
          headers: Headers that override the sink's own defaults.
          auth: A bearer token, or a ``(user, password)`` pair for basic auth. A token is a
            ``str``; a pair is any value — a ``tuple``, a ``list``, a one-shot iterator —
            unpacking into exactly two ``str``, other than a ``set``, ``frozenset`` or mapping,
            which hold no order. Either is read **once**, here: the ``Authorization`` value is
            built from an exact-``str`` copy and every request sends that value, so a list
            changed afterwards, an object yielding new values each time it is iterated, or a
            ``str`` subclass whose methods would answer differently later, is frozen at what it
            held at construction.
          body_format: ``"ndjson"`` or ``"json_array"``.
          timeout: Seconds allowed per request.
          gzip: Whether to compress the body.
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
            nor ``None`` (SPEC-071 FR-005), judged first.
          ValueError: If the URL carries a user or password before its host or an ``@`` anywhere
            else — write one that belongs in a path or query as ``%40`` — its scheme is not
            ``http`` or ``https``, it names no host or no usable port, or ``http.client`` cannot
            build a request line for it or for the method,
            a header name or value or a bearer token contains CR or LF, a header name contains
            whitespace anywhere (refused after ``http.client``'s own check, so a name that already
            failed it keeps that message), ``auth`` is given beside
            an ``Authorization`` header in any case, ``auth`` cannot be sent or is not a token
            and does not unpack into exactly two items, the timeout cannot bound
            anything, ``body_format`` is neither of the two this class renders, or
            ``max_batch_bytes`` is not positive — each refused here rather than raised out of every
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
        self._headers = _valid_headers(dict(headers) if headers else {}, type(self).__name__)
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
        self._counter_lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Re-chunks the batch to this destination's limits and POSTs each chunk (FR-001).

        Subclasses must not override this. They shape the request through :meth:`_render`,
        :meth:`_body` and :meth:`_handle_response`, which is what keeps the chunk loop on every
        path to the wire.

        The drops :meth:`_items` decides — an oversized event, an unrenderable one — are counted
        and announced through the deferral helper, so they land only if this call returns
        normally (SPEC-058 FR-005). They are decided before any request, and this call can still
        raise afterwards and be retried whole: counting them first counted one oversized event
        once per attempt. A ``413`` drop — a lone item the destination refused, or one
        :meth:`_known_refused` discards without offering it — is counted and announced only when
        ``emit`` returns as well, through :meth:`_drop_refused` and the same ledger (SPEC-059
        FR-004). Once a drop has settled a chunk, this call raises only through a later chunk's
        exception :meth:`_deliver` does not catch — one any subclass hook raises, since
        :meth:`_body` and :meth:`_handle_response` run outside :meth:`_send`'s guard, or a
        ``BaseException`` — and that raise now discards the drop rather than leaving it counted
        once per attempt. An exception a caller-injected ``opener`` raises is no longer such a
        route: :meth:`_send` catches every ``Exception`` and counts one it does not retry as an
        abandoned request (SPEC-070 FR-001). The abandonment
        counter stays immediate: ``failed`` is an upper bound neither spec recounts.

        Args:
          batch: The events to ship. An empty batch is a no-op.

        Returns:
          None.

        Raises:
          SinkDeliveryError: When no chunk was **settled**, which is the total failure the
            worker's retry exists for (SPEC-026 FR-001). A chunk is settled when it was
            delivered, when it was permanently discarded, *or* when a ``2xx`` gave no readable
            verdict — a body that could not be decoded, or could not be read at all — and
            :meth:`_handle_response` counted it, because none is helped by a retry
            — a ``413`` is a verdict on the bytes, not a transient fault, and re-sending
            identical bytes can only earn the same answer (the rule SPEC-016 and SPEC-018 both
            already apply), while re-sending a chunk that may have landed could duplicate it.
            A batch that settled *some* chunks does not raise either: the worker
            retries whole batches, so raising would re-send what landed. A raise leaves nothing
            from :meth:`_items` or :meth:`_drop_refused` counted, and a batch whose every event
            was unrenderable or oversized makes no request and does not raise.
        """
        if not batch:
            return
        with Deferral(self) as pending:
            items = self._items(batch, pending)
            if not items:
                return
            budget = self._chunk_budget()
            refused_at: int | None = None
            reductions = index = chunks = settled = 0
            while index < len(items):
                chunk = self._take(items, index, budget)
                if self._known_refused(chunk, refused_at):
                    self._drop_refused(chunk, pending)
                    chunks += 1
                    settled += 1
                    index += 1
                    continue
                try:
                    delivered = self._deliver(chunk)
                except _PayloadTooLarge as refusal:
                    refused_at = (
                        refusal.size if refused_at is None else min(refused_at, refusal.size)
                    )
                    if len(chunk) > 1 and reductions < MAX_BUDGET_REDUCTIONS:
                        budget = max(min(budget, refusal.size) // 2, 1)
                        reductions += 1
                        continue
                    chunks += 1
                    if len(chunk) == 1:
                        self._drop_refused(chunk, pending)
                        settled += 1
                    else:
                        self._abandon_unreduced(chunk)
                    index += len(chunk)
                    continue
                chunks += 1
                settled += delivered
                index += len(chunk)
            if chunks and not settled:
                raise SinkDeliveryError(
                    f"{type(self).__name__} delivered none of {chunks} chunk(s)"
                )

    def _known_refused(self, chunk: list[_Item], refused_at: int | None) -> bool:
        """Reports whether this chunk is provably no smaller than one already refused.

        Only a lone item qualifies: a multi-item chunk can still be reduced, and reducing is
        cheaper than discarding. The point is to stop the tail of a hopeless ``emit`` becoming
        one request per event once the budget has fallen below a single item.

        The comparison builds the item's **real body** rather than reading its charged size,
        because the two differ per body shape and one of them differs the wrong way: an item's
        charged size includes a separator ``SplunkHECSink``'s concatenated body never writes, so
        a lone item charged exactly ``refused_at`` builds a body one byte *smaller* than one
        already refused. Measured, that discarded an event whose 144-byte body fitted a 144-byte
        limit. The other shapes err the safe way (``+13`` for Loki, ``+1`` for a JSON array), but
        "provably no smaller" has to be provable for all of them, and ``refused_at`` is itself a
        body length — so comparing body lengths is the like-for-like test.

        **It is disabled under ``gzip``**, and that is the whole subtlety. ``_PayloadTooLarge``
        carries the *uncompressed* length, because that is what this sink measures and chunks by,
        while a gzipped request is judged by the destination on its compressed bytes. Compression
        ratio is per-event, so "larger uncompressed" stops implying "also refused": measured, one
        incompressible event set the mark at 532 bytes and a 6,020-byte event that gzipped to 64
        — against a 200-byte limit — was discarded without a single request being made. That is
        loss the library invented rather than loss it reported, which is what SPEC-025 and
        SPEC-026 exist to prevent, and it also inflated ``dropped``, whose contract is an exact
        count. Comparing like with like would mean gzipping each candidate purely to decide
        whether to ask, which costs more than the request it saves.

        Args:
          chunk: The chunk about to be sent.
          refused_at: The smallest body this ``emit`` has seen refused, or ``None``.

        Returns:
          True when the chunk can be discarded without offering it.

        Raises:
          None.
        """
        if self.gzip or refused_at is None or refused_at <= 0 or len(chunk) != 1:
            return False
        return len(self._body(chunk)[0]) >= refused_at

    def _take(self, items: list[_Item], start: int, budget: int) -> list[_Item]:
        """Fills one chunk from ``start`` under the count limit and the current byte budget.

        Always yields at least one item, however small the budget: an item that cannot fit alone
        is the caller's to resolve, which :meth:`emit` does by dropping it once the destination
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
        """Records a multi-item chunk the reduction budget could not shrink.

        It is counted against ``failed`` and announced, but **not** settled: those events were
        never individually refused, so calling them permanent drops would invent loss, while
        saying nothing at all would leave them invisible whenever a sibling chunk delivered and
        ``emit`` therefore returned normally. Measured before this: 10 events in, 2 delivered,
        8 gone, every counter at zero and stderr empty.

        Reaching it needs a pathological ``_item_size`` — with the default one, a multi-item
        chunk cannot survive ``MAX_BUDGET_REDUCTIONS`` halvings unless ``max_batch_count``
        exceeds 33 million.

        Args:
          chunk: The events that could not be placed.

        Returns:
          None.

        Raises:
          None.
        """
        with self._counter_lock:
            self.failed += 1
        _diag.lost(
            "request",
            1,
            f"{type(self).__name__}, {len(chunk)} event(s) still 413 after "
            f"{MAX_BUDGET_REDUCTIONS} reduction(s); lower max_batch_bytes to match the destination",
        )

    def _drop_refused(self, chunk: list[_Item], pending: Deferral) -> None:
        """Discards a single item this destination will not accept at any size available.

        Only ever called with one item. It reaches here two ways: the item was offered alone and
        answered ``413``, or :meth:`_known_refused` proved its body is no smaller than one this
        ``emit`` has already had refused — in which case it was **never offered**, which is the
        whole point of that shortcut. Either way it is permanently undeliverable here, so the
        chunk reports *settled*, and the drop goes to ``pending`` against ``dropped_oversized``:
        it is counted and announced only when ``emit`` returns (SPEC-059 FR-004). Permanence
        alone never made ``dropped`` exact. Once a drop has settled a chunk, ``emit`` can still
        raise through a later chunk's exception :meth:`_deliver` does not catch — one a
        caller-injected ``opener`` raises that :meth:`_send` does not catch, one any subclass
        hook raises, or a ``BaseException``; the
        worker then retried the batch, re-ran the identical search and re-counted the identical
        drop once per attempt. It is the deferral that keeps ``dropped`` an exact count, while
        ``failed`` stays an upper bound. The line keeps its shape, one per call.

        A *multi*-item chunk reaching the end of the reduction budget is deliberately **not**
        routed here. It needs a pathological ``_item_size`` to happen at all (measured: an
        override returning zero abandoned 20 events in whole chunks), and those events were never
        individually refused, so counting them as permanent drops would be loss the library
        invented. They are left unsettled and reach the worker as the failure they are.

        Args:
          chunk: The refused items.
          pending: The calling ``emit``'s deferral, which holds the drop until that ``emit`` is
            known not to raise.

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

    def _items(self, batch: list[dict[str, object]], pending: Deferral) -> list[_Item]:
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
          pending: The calling ``emit``'s deferral, which holds every drop decided here until
            that ``emit`` is known not to raise (SPEC-058 FR-005).

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
        remedy: pass ``max_batch_bytes=``.

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

        The family's default is ``429`` and every ``5xx``. It is a hook, read at one site in
        :meth:`_send`, so a subclass whose protocol names its own retryable set — OTLP's is
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
        non-``2xx`` response longer than the bound is then abandoned in :meth:`_send` as
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

    def _deliver(self, items: list[_Item]) -> bool:
        """Sends one chunk and reports whether it needs no further attention.

        A ``413`` is *not* handled here. It is a statement about the request size rather than
        about this chunk, so it propagates to :meth:`emit`, which lowers the byte budget and
        re-chunks the remaining work — see :data:`MAX_BUDGET_REDUCTIONS` for why that is the
        cheap shape and recursive chunk-halving is not. Re-sending after a ``413`` cannot
        duplicate anything: the rejection precedes ingestion, so nothing landed (SPEC-018's rule
        that only provable non-delivery may be re-sent).

        Args:
          items: The chunk to send, known non-empty.

        Returns:
          True when the chunk is settled as ``emit`` defines it, False for a failure a retry might
          yet fix.

        Raises:
          _PayloadTooLarge: When the destination refused the request as too large.
        """
        body, content_type = self._body(items)
        try:
            payload = self._send(body, content_type=content_type, splittable=True)
        except _PayloadTooLarge:
            raise
        except SinkDeliveryError:
            return False
        return self._handle_response(payload, items)

    def losses(self) -> SinkLosses:
        """Reports oversized drops and abandoned requests (SPEC-026 FR-002).

        Args:
          None.

        Returns:
          The counters. ``failed`` counts abandoned requests rather than events, since this
          class has no per-event outcome to report and an abandoned request took its whole body
          with it; subclasses that do learn per-record outcomes add them. ``dropped`` is the
          oversized events plus those whose render raised (SPEC-058 FR-005).

        Raises:
          None.
        """
        with self._counter_lock:
            return SinkLosses(
                dropped=self.dropped_oversized + self._unrenderable, failed=self.failed
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
        """POSTs a body with bounded retry and returns the response bytes.

        Raising from here rather than from each ``emit`` is deliberate: every platform subclass
        builds its own body and calls this, so the rule reaches them without a line of their
        own. The one caller that must not propagate is ``SentrySink``, which sends one envelope
        per event and therefore catches it.

        A failure **before** any status line is a connection error when :meth:`_retryable_error`
        accepts it: an ``OSError``, a ``URLError``, and an ``http.client.HTTPException`` such as
        ``BadStatusLine``, which is neither and used to escape ``emit`` raw, handing the worker a
        batch to re-send whole, chunks that had already landed included. Those three are retried,
        then abandoned and counted in ``failed``. A header block that fails to parse raises the
        same class *after* a status line the library never sees, which may have been a ``2xx``;
        it is retried like a read timeout awaiting the headers, on the expected-cost reasoning
        SPEC-048 records for those. **After** a status line the status decides, whatever happened
        to the body (:meth:`_attempt`).

        Any other exception — what a caller-injected ``opener`` can raise — also used to escape
        ``emit`` raw and uncounted. It is now caught and abandoned at its first attempt with no
        backoff wait (SPEC-070 FR-001), through the same ``_abandon`` line the exhausted
        connection error writes, so its attempt count reads ``1``.

        Args:
          body: The serialized batch.
          content_type: The default content type, overridable by caller headers.
          extra_headers: Per-request headers beneath the caller's own.
          splittable: Whether the caller can respond to a ``413`` by sending smaller requests.
            Only :meth:`_deliver` can, so it alone passes ``True`` and alone sees
            ``_PayloadTooLarge``. Every other caller — ``SentrySink``, which sends one envelope
            per event and has nothing left to split — keeps the counted, announced abandonment
            it has always had, because a ``413`` raised past it uncounted would be exactly the
            silent loss this file's counters exist to prevent.
          count_unexpected: Whether an exception :meth:`_retryable_error` rejects is counted and
            abandoned here. ``SentrySink`` passes ``False``: it counts such an exception on its
            own ``.transport_errors``, a documented counter, so moving the count to this held
            sink's ``failed`` would reclassify it in a minor release. With ``False`` the
            exception is re-raised unchanged after its one attempt, uncounted here — unless it is
            a ``SinkDeliveryError`` (``_PayloadTooLarge`` included), which every caller reads as
            already counted and announced; that one is abandoned here, counted once in
            ``failed``, so an opener raising it is never counted nowhere.

        Returns:
          The response bytes, which subclasses that must inspect the response — such as
          Elasticsearch's ``_bulk`` items — read. An :class:`_UnreadBody`, equal to ``b""``, when
          a ``2xx`` body could not be read, which :func:`_read_failed` tells from an empty one.

        Raises:
          Exception: An exception :meth:`_retryable_error` rejects, unchanged, only when
            ``count_unexpected`` is False and it is not a ``SinkDeliveryError``.
          _PayloadTooLarge: On a ``413`` when ``splittable``. It moves no counter and writes no
            line, because the caller is about to re-send the same events in smaller requests and
            only what finally fails is a loss.
          SinkDeliveryError: If the request was retried to exhaustion. It used to return
            ``None``, which every caller spelled as "nothing to parse" and the worker read as a
            successful emit; an abandoned request delivered nothing, which is the total failure
            the worker's retry exists for (SPEC-026 FR-001). Since FR-001 a request carries one
            *chunk* rather than the whole batch, so it is :meth:`emit` that decides whether the
            batch as a whole failed — one abandoned chunk beside a delivered one is partial
            failure, and partial failure must not raise.
        """
        headers, data = self._prepare(body, content_type, extra_headers)
        retries = self.max_retries
        limit = self._response_limit()
        for attempt in range(retries + 1):
            request = urllib.request.Request(  # noqa: S310
                self.url, data=data, method=self.method, headers=headers
            )
            try:
                status, payload, retry_after = self._attempt(request)
            except Exception as err:
                retryable = is_retryable(self._retryable_error, err)
                if retryable and attempt < retries:
                    self._sleep_backoff(attempt, None)
                    continue
                counted_by_caller = not count_unexpected and not isinstance(err, SinkDeliveryError)
                if not retryable and counted_by_caller:
                    raise
                self._abandon(
                    f"connection error, {type(err).__name__} {_diag.errno_of(err)}".rstrip(),
                    attempt + 1,
                )
            if 200 <= status < 300:
                return payload
            if limit is not None and len(payload) > limit:
                self._abandon(f"HTTP {status}, response over {limit} bytes", attempt + 1)
            if self._retryable_status(status) and attempt < retries:
                self._sleep_backoff(attempt, retry_after)
                continue
            if status == 413 and splittable:
                raise _PayloadTooLarge(f"{type(self).__name__}, HTTP 413", len(body))
            self._abandon(f"HTTP {status}", attempt + 1)
        raise SinkDeliveryError(f"{type(self).__name__} made no attempt")

    def _prepare(
        self,
        body: bytes,
        content_type: str,
        extra_headers: dict[str, str] | None,
    ) -> tuple[dict[str, str], bytes]:
        """Builds the final header map and the optionally gzipped body.

        Caller-provided headers override the sink's defaults (FR-002), and that now includes
        ``Content-Encoding``: ``gzip=True`` used to overwrite one the caller had set, which made
        it the single header of theirs that did not win. A caller's own value both survives *and*
        suppresses the compression, because a header saying ``identity`` over a gzipped body
        makes the destination decode garbage. The test is against ``self._headers`` rather than
        the merged map, so a per-request ``extra_headers`` value — which sits *beneath* the
        caller's own — does not switch compression off (SPEC-048 FR-007). The caller's header is
        found by the name ``urllib`` sends (:func:`_wire_name`), not by exact case: a
        ``content-encoding: identity`` used to miss the test, so the body was gzipped and a
        second ``Content-Encoding: gzip`` written after it, which ``urllib`` folded over the
        caller's. The caller's own spelling is now the one sent, over an uncompressed body.

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
        caller_encodes = any(_wire_name(name) == "Content-Encoding" for name in self._headers)
        if self.gzip and not caller_encodes:
            data = _gzip.compress(body)
            headers["Content-Encoding"] = "gzip"
        self._apply_auth(headers)
        return headers, data

    def _apply_auth(self, headers: dict[str, str]) -> None:
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
        request, a status line or header block the server garbled — raises, and :meth:`_send`
        retries it, if :meth:`_retryable_error` accepts it, as a connection error. Once a status
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
        ``ElasticsearchSink._parse_bulk_response`` gives for its own: anything escaping here
        escapes ``emit``, and the worker re-sends a batch the destination may have accepted. It
        never catches ``BaseException``.

        The read takes no size argument unless :meth:`_response_limit` names a bound, and the
        bare form is load-bearing. CPython's ``read()`` raises ``IncompleteRead`` for a
        ``Content-Length`` body that arrives short and for a truncated chunked body, but
        ``read(amt)`` returns a short ``Content-Length`` body **silently**, leaving
        ``response.length`` above zero (measured on 3.12 and 3.13). So the bounded read, which
        asks for one byte past the bound so :meth:`_send` and :meth:`_handle_response` can tell
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
          urllib.error.URLError: On a connection-level failure, for the retry loop to handle.
          OSError: Likewise.
          http.client.HTTPException: When the status line or headers could not be parsed, such
            as ``BadStatusLine``, for the retry loop to handle as a connection error.
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
        wait(delay, self.log_foundry_stop_signal)

    def _abandon(self, reason: str, attempts: int) -> NoReturn:
        """Counts and logs a request abandoned past the retry bound, then raises (FR-012).

        The counter moves before the raise: ``failed`` is this sink's own record of what it
        could not put on the wire, and it must not depend on who catches what. It therefore
        counts every worker retry attempt too, and now every *chunk* of a batch, making it an
        upper bound on loss rather than a count of it — see
        :class:`~log_foundry.sinks.base.SinkLosses`.

        Args:
          reason: Why the request was abandoned. It carries only library-controlled values — an
            HTTP status, an exception type, an ``errno`` — never a server-supplied body or an
            exception's text (SPEC-029 FR-002), and it is also the raised error's message for
            the same reason.
          attempts: How many requests were actually made. Passed in rather than derived from
            ``max_retries``, because a non-retryable status abandons on the first attempt: the
            derived form reported "4 attempt(s)" for an HTTP 400 that was sent once.

        Returns:
          Never returns.

        Raises:
          SinkDeliveryError: Always.
        """
        with self._counter_lock:
            self.failed += 1
        detail = f"{type(self).__name__}, {attempts} attempt(s), {reason}"
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
