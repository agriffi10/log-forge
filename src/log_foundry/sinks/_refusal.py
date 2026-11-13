"""A shipped sink's refusal that no retry can cure, marked so the worker does not resend it."""

from __future__ import annotations

from typing import Final

from log_foundry.sinks.base import SinkDeliveryError

__all__ = ["final_refusal", "is_final_refusal"]

_MARK: Final = "_log_foundry_final_refusal"
"""The instance attribute the mark is stored under, read back only by :func:`is_final_refusal`."""


def final_refusal(message: str) -> SinkDeliveryError:
    """Builds the ``SinkDeliveryError`` a shipped sink raises for a batch no retry can deliver.

    A closed or closing sink refuses every batch for the rest of its life (SPEC-032, SPEC-076),
    and a forked child's ``SQLiteSink`` or ``NATSSink`` acquired nothing to deliver with
    (invariant 12), so a resend cannot succeed and only costs the batch its backoff waits. The
    worker re-sends a raised batch up to its ``max_retries``, which is the 1.x ``emit`` contract
    for every sink and stays so for a third-party one; an error built here carries a private mark
    the worker reads to abandon the batch at the attempt that raised it instead, counted and
    announced as an exhausted batch is.

    The error is exactly ``SinkDeliveryError``, with the message given and no other difference a
    caller can see: ``type(err) is SinkDeliveryError``, ``str(err)`` and every diagnostic that
    names its type are unchanged. The mark is an instance attribute, never a class, so nothing in
    the public surface grows (invariant 10), and a sink of the caller's own cannot opt in by
    accident.

    Args:
      message: The refusal's text, which names a type and counts, never a value (invariant 11).

    Returns:
      The marked error, for the caller to raise or to hand to ``RunContext.refuse``.

    Raises:
      None.
    """
    err = SinkDeliveryError(message)
    vars(err)[_MARK] = True
    return err


def is_final_refusal(err: BaseException) -> bool:
    """Answers whether an exception is a refusal :func:`final_refusal` built, so never retried.

    Total by construction: the exact-type test runs first and cannot raise, so an exception of
    any other class, a hostile ``__getattribute__`` or ``__dict__`` included, is never read
    further. A subclass of ``SinkDeliveryError`` is not one, which keeps SPEC-069's dual-typed
    driver errors, a transient failure among them, on the worker's resend.

    Args:
      err: The exception a sink's ``emit`` raised.

    Returns:
      True only for an exact ``SinkDeliveryError`` carrying the mark.

    Raises:
      None.
    """
    return type(err) is SinkDeliveryError and vars(err).get(_MARK) is True
