"""A shipped sink's refusal that no retry can cure, marked so the worker does not resend it."""

from __future__ import annotations

from typing import Final

from log_foundry.sinks.base import SinkDeliveryError

__all__ = ["final_refusal", "is_final_refusal"]

_MARK: Final = "_log_foundry_final_refusal"
"""The instance attribute the mark is stored under, read back only by :func:`is_final_refusal`."""

_PACKAGE: Final = "log_foundry"
"""The package whose frames alone may carry a final refusal from its raise to its catcher."""


def final_refusal(message: str) -> SinkDeliveryError:
    """Builds the ``SinkDeliveryError`` a shipped sink raises for a batch no retry can deliver.

    A closed or closing sink refuses every batch for the rest of its life (SPEC-032, SPEC-076),
    and a forked child's ``SQLiteSink`` or ``NATSSink`` acquired nothing to deliver with
    (invariant 12), so a resend cannot succeed and only costs the batch its backoff waits. The
    worker re-sends a raised batch up to its ``max_retries``, which is the 1.x ``emit`` contract
    for every sink and stays so for a third-party one; an error built here carries a private mark
    the worker reads to abandon the batch at the attempt that raised it instead, counted and
    announced as an exhausted batch is, provided only this package's code carried it there
    (:func:`is_final_refusal`).

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
    """Answers whether an exception is a final refusal that only the library's own code carried.

    Two tests, both required. The error is exactly ``SinkDeliveryError`` carrying the mark
    :func:`final_refusal` sets: a subclass is never one, which keeps SPEC-069's dual-typed driver
    errors, a transient ``database is locked`` among them, on the worker's resend. And every frame
    of its traceback below the catcher's own, the first entry, down to the raise, belongs to a
    module of this package: a sink of the caller's own that catches a shipped sink's refusal and
    re-raises it by identity puts its own frame in that chain, so the batch keeps the 1.x resend,
    which that sink may have arranged to succeed (swapping in a fresh sink before it re-raised,
    say), and so does a subclass whose ``emit`` override calls ``super().emit``. The catcher's own
    frame is skipped because it is the one asking, the worker or ``MultiSink``. Measured on the
    shipped paths, the chain holds only the catcher's frame and this package's, with no
    ``contextlib`` frame for the closed guard's ``with``: a direct closed sink, and one behind
    ``MultiSink`` or ``TransformSink``. An error never raised has no traceback and passes.

    Total by construction: the exact-type test runs first and cannot raise, so an exception of
    any other class, a hostile ``__getattribute__`` or ``__dict__`` included, is never read
    further, and a frame's ``__name__`` is compared only when it is exactly a ``str``.

    Args:
      err: The exception a sink's ``emit`` raised, as its catcher holds it.

    Returns:
      True only for an exact marked ``SinkDeliveryError`` raised and carried by this package.

    Raises:
      None.
    """
    if type(err) is not SinkDeliveryError or vars(err).get(_MARK) is not True:
        return False
    caught = err.__traceback__
    frame = None if caught is None else caught.tb_next
    while frame is not None:
        module = frame.tb_frame.f_globals.get("__name__")
        if type(module) is not str or not (
            module == _PACKAGE or module.startswith(f"{_PACKAGE}.")
        ):
            return False
        frame = frame.tb_next
    return True
