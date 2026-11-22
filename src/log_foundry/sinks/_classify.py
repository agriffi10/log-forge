"""What a sink retries: the retry predicates, the shared helpers and the AWS rules (SPEC-070)."""

from __future__ import annotations

import socket
import sys
import urllib.error
from typing import TYPE_CHECKING, Any, Final, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

type ErrorPredicate = Callable[[Exception], bool]
"""Answers whether another attempt after this exception may succeed; total, it never raises.

The arc's one definition (SPEC-070 FR-001): each loop class's ``_retryable_error`` has this
shape, and SPEC-072's engine takes it as an argument rather than defining its own.
"""

type EntryPredicate = Callable[[Mapping[str, object]], bool]
"""Answers whether a resend of one rejected batch entry may succeed (SPEC-070 FR-003)."""

TRANSIENT_BUILTINS: Final[tuple[type[Exception], ...]] = (ConnectionError, TimeoutError)
"""The builtin classes every driver predicate accepts, whatever the driver.

An injected client or a stand-in raises these for a dropped connection or a timeout, and
redis-py's own default ``Retry`` lists ``socket.timeout``, which is ``TimeoutError``.
"""

_AWS_RETRY_STATUSES: Final = frozenset({429, 500, 502, 503, 504})
"""HTTP statuses botocore's standard retry mode treats as transient, plus ``429``."""

_AWS_RETRY_CODES: Final = frozenset(
    {
        "RequestTimeout",
        "RequestTimeoutException",
        "PriorRequestNotComplete",
        "BandwidthLimitExceeded",
        "EC2ThrottledException",
        "LimitExceededException",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "RequestThrottled",
        "RequestThrottledException",
        "SlowDown",
        "ThrottledException",
        "Throttling",
        "ThrottlingException",
        "TooManyRequestsException",
        "TransactionInProgressException",
        "KmsThrottled",
        "KMSThrottling",
        "KMSThrottlingException",
    }
)
"""``Error.Code`` values a ``ClientError`` is retried on (SPEC-070 FR-002).

The first three are ``TransientRetryableChecker._TRANSIENT_ERROR_CODES`` and the next thirteen
``ThrottledRetryableChecker._THROTTLED_ERROR_CODES``, both in ``botocore/retries/standard.py`` at
the locked botocore 1.43.119; the last three are the KMS throttling codes the four services' API
references name. Copied rather than read at runtime, because the source is a private attribute
of a module the sinks never import.
"""


def driver_classes(module: str, *names: str) -> tuple[type[BaseException], ...]:
    """Returns the named exception classes of a driver module that is already loaded.

    Resolution reads ``sys.modules`` and imports nothing: an exception instance of a driver
    class proves its module is loaded, so a sink handed an injected client with no extra
    installed never imports one. It is meant to be called inside a predicate, once per call and
    never cached, because the drivers are imported lazily — ``PostgresSink._connect`` imports
    ``psycopg`` on first connect — and a set resolved earlier would stay empty after the driver
    loaded, failing every transient error at once.

    Args:
      module: The dotted name of the module that defines the classes.
      names: The class names to look up.

    Returns:
      The classes found, in the order named. ``()`` when the module is absent or mapped to
      ``None``; a name the installed version lacks, or one bound to anything but an exception
      class, is skipped.

    Raises:
      None. An attribute lookup that raises, as a lazy module ``__getattr__`` can, reads as a
        missing name.
    """
    loaded = sys.modules.get(module)
    if loaded is None:
        return ()
    found: list[type[BaseException]] = []
    for name in names:
        try:
            candidate = getattr(loaded, name)
        except Exception:
            candidate = None
        if issubclass(type(candidate), type) and issubclass(candidate, BaseException):
            found.append(candidate)
    return tuple(found)


def is_retryable(predicate: ErrorPredicate, err: Exception) -> bool:
    """Asks a predicate about an exception, reading a predicate that raises as "not retryable".

    Every retry loop asks through this wrapper rather than calling the predicate directly, which
    is what makes the predicate total: a malformed attribute on the exception — a ``response``
    whose property raises, a ``__str__`` that fails — degrades to "fails at once", which the loop
    counts and announces with the original exception's type, never silently.

    Args:
      predicate: The sink's ``_retryable_error``.
      err: The exception the attempt raised.

    Returns:
      What the predicate answered, or False when it raised or its answer could not be read as
      a truth value.

    Raises:
      None. A ``BaseException`` from the predicate still propagates.
    """
    try:
        return bool(predicate(err))
    except Exception:
        return False


def aws_retryable(err: Exception) -> bool:
    """The four AWS sinks' predicate: botocore's transient and throttling failures (FR-002).

    A ``ClientError`` is read on its HTTP status and its ``Error.Code``, each only when it has
    the type the documented shape gives it: a status that is not an ``int`` (a ``bool`` is not
    one) and a code that is not a ``str`` read as neither, because a ``list`` code would raise
    on the membership test. Anything else botocore raises — ``ParamValidationError``,
    ``NoCredentialsError`` — and any exception from another library fails at once.

    Args:
      err: The exception ``put_records``, ``put_record_batch``, ``publish_batch`` or
        ``send_message_batch`` raised.

    Returns:
      True for a builtin connection error or timeout, botocore's ``ConnectionError`` and
      ``HTTPClientError``, and a ``ClientError`` whose status or code is transient.

    Raises:
      None.
    """
    kind = type(err)
    if issubclass(kind, TRANSIENT_BUILTINS):
        return True
    if issubclass(
        kind, driver_classes("botocore.exceptions", "ConnectionError", "HTTPClientError")
    ):
        return True
    if not issubclass(kind, driver_classes("botocore.exceptions", "ClientError")):
        return False
    response = getattr(err, "response", None)
    if not issubclass(type(response), dict):
        return False
    metadata = cast("dict[str, Any]", response).get("ResponseMetadata")
    status = (
        cast("dict[str, Any]", metadata).get("HTTPStatusCode")
        if issubclass(type(metadata), dict)
        else None
    )
    if type(status) is int and status in _AWS_RETRY_STATUSES:
        return True
    error = cast("dict[str, Any]", response).get("Error")
    code = cast("dict[str, Any]", error).get("Code") if issubclass(type(error), dict) else None
    return issubclass(type(code), str) and code in _AWS_RETRY_CODES


def sender_fault(entry: Mapping[str, object]) -> bool:
    """Reports whether an SQS or SNS batch entry was rejected for the caller's fault (FR-003).

    Truthiness, exactly the test ``SQSSink._send`` made before it was shared, so ``SQSSink`` is
    unchanged byte for byte: absent, ``None``, ``False``, ``0`` and ``""`` are retried, and
    ``True``, ``1``, ``"true"`` and ``"false"`` are permanent. A strict ``is True`` was rejected:
    botocore parses the documented ``Boolean`` to a ``bool``, so the two tests differ only on a
    fake's reply, and the difference would change ``SQSSink`` for nothing.

    Args:
      entry: One item of the reply's ``Failed`` list.

    Returns:
      True when the entry is a sender fault, which a byte-identical resend fails again.

    Raises:
      AttributeError: When the entry is not a mapping. The SQS and SNS sinks never ask it about
        one: their keyed reader reads a ``Failed`` list holding a non-mapping as adjudicating none
        of the chunk, counted once and never resent (SPEC-074 FR-002).
    """
    return bool(entry.get("SenderFault"))


def may_have_landed(err: Exception) -> bool:
    """Whether a call that raised ``err`` may have reached its destination (SPEC-095 FR-002).

    False only for a class that shows the request never left, as the single-attempt transports
    raise it: a builtin ``ConnectionRefusedError`` (a subclass included, which is still a
    refusal), a ``socket.gaierror`` (the name never resolved), and a ``urllib.error.URLError``
    other than an ``HTTPError``, which ``urllib`` raises only around connecting and writing the
    request (``do_open``). Everything else may have landed: a timeout or a connection dropped
    after the request was written cannot say whether the destination stored it. A sink that can
    tell by where the call raised passes its own answer instead. Types are read with
    ``issubclass(type(err), …)``, as :func:`aws_retryable` reads them, so this module stays a
    leaf.

    Args:
      err: The exception the call raised.

    Returns:
      False only when the request provably never left.

    Raises:
      None.
    """
    kind = type(err)
    if issubclass(kind, urllib.error.HTTPError):
        return True
    return not issubclass(kind, (ConnectionRefusedError, socket.gaierror, urllib.error.URLError))
