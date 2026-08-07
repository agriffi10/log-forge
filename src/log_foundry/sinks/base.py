"""The sink interface (arch §8)."""

from __future__ import annotations

from typing import NamedTuple, Protocol, runtime_checkable

__all__ = ["Sink", "SinkDeliveryError", "SinkLosses", "read_losses"]


class SinkDeliveryError(Exception):
    """Raised by a sink whose ``emit`` delivered none of the batch (SPEC-026 FR-001).

    It is a distinct type so an operator reading a ``stopped_reason`` or a diagnostic line can
    tell "the destination refused everything" from "the sink itself has a bug". Sinks that
    already have a natural exception to re-raise — a driver error, a ``MultiSink`` child's —
    re-raise that instead: the contract is that something propagates, not that it must be this
    type.
    """


class SinkLosses(NamedTuple):
    """What a sink discarded or could not confirm, cumulative for its lifetime (FR-002).

    ``failed`` is an upper bound on loss, not a count of it: a sink that also raises on total
    failure counts the attempt here and hands the batch back to the worker, whose retry may
    then deliver it, so a transient outage leaves ``failed`` non-zero with nothing actually
    lost. ``health().failed_batches`` is the worker-level record of a batch given up on for
    good; this is the sink-level record of everything that did not go through first time.

    Attributes:
      dropped: An event the sink discarded before attempting delivery, usually one the
        destination could never have accepted as built, so the fix is upstream in what the
        application logs. For a sink whose client owns a local buffer it also covers what that
        buffer refused, which is backpressure instead; the stderr line names which.
      failed: Delivery was attempted and the destination did not confirm it, so the fix is the
        destination or the network.
    """

    dropped: int
    failed: int


@runtime_checkable
class Sink(Protocol):
    """The swappable output transport (arch §8).

    A sink receives already-built, batched event dicts from the worker and knows nothing about
    spans or context — that dumbness is what makes sinks trivially interchangeable. Beyond
    "put these somewhere" it carries the two obligations :meth:`emit` documents, because the
    library's whole loss-reporting apparatus is built on them: a sink that absorbs a total
    failure and returns normally is a sink the worker believes, so the retry never engages,
    ``failed_batches`` stays at zero and ``flush()`` returns True while every event is lost.

    A third obligation is optional and deliberately not declared on this Protocol, which is
    structural: a sink may also offer ``losses() -> SinkLosses | None``, reporting the
    cumulative loss it absorbed, never raising and safe to call during an emit. Returning
    ``None`` is the same answer as having no method at all, which is what keeps a third-party
    sink written against the pre-SPEC-026 interface satisfying this one. :func:`read_losses` is
    the probe.
    """

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Ships a batch of serialized event dicts.

        Raise when the batch delivered nothing and it was non-empty — the worker's bounded
        retry and ``health().failed_batches`` depend on that signal, and a retry there cannot
        duplicate anything (SPEC-026 FR-001). Raise after the sink's own retries are spent, so
        the worker's retry composes on top rather than replacing it; a sink whose own budget
        makes the worker's redundant should say so in its docstring rather than absorb
        silently.

        Do not raise on partial failure. A batch where some records landed would be
        re-delivered wholesale by the worker's retry, and duplicates downstream are worse than
        the counted loss (SPEC-017 FR-004, SPEC-018). Report that through ``losses()`` instead.

        Args:
          batch: The events to ship. ``emit([])`` is a no-op and never raises, since an empty
            batch has not failed to deliver.

        Returns:
          None.

        Raises:
          Exception: When the batch delivered nothing.
        """
        ...

    def close(self) -> None:
        """Flushes and releases any resources.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever releasing the transport raises; the worker absorbs it.
        """
        ...


def read_losses(sink: object) -> SinkLosses | None:
    """Reads a sink's optional ``losses()``.

    This is the single reader for the optional half of the protocol (SPEC-026 FR-002), shared
    by ``Worker.health`` and ``MultiSink.losses`` so the probe and its guarantees are written
    once. Nothing is written to stderr — a poll happens as often as the caller likes, and a
    broken accessor is a sink bug rather than a loss.

    Args:
      sink: The sink to probe, of any type.

    Returns:
      The sink's losses, or ``None`` when it reports nothing. ``None`` covers four cases
      deliberately treated alike: no ``losses`` attribute, one that is not callable, one that
      raised, and one that returned anything other than a ``SinkLosses`` — including ``None``
      itself, which the shipped wrapper sinks return when what they wrap reports nothing.

    Raises:
      None. ``health()`` is documented as never raising and is the call an operator makes when
        things are already going wrong, so a third-party sink with a broken accessor must not
        take the snapshot down with it. The returned shape is checked as well as the call.
    """
    try:
        accessor = getattr(sink, "losses", None)
        losses = accessor() if callable(accessor) else None
    except Exception:
        return None
    return losses if isinstance(losses, SinkLosses) else None
