"""The sink interface (arch §8).

A sink is the swappable output transport. It receives *already-built, batched* event
dicts from the worker and knows nothing about spans or context — that dumbness is what
makes sinks trivially interchangeable (StdoutSink, SQSSink, …). Only the Protocol lives
here; concrete sinks arrive in later phases.

The interface carries two obligations beyond "put these somewhere" (SPEC-026), because the
library's whole loss-reporting apparatus is built on them:

* **Total failure raises.** A sink that delivered *none* of a batch must let something
  propagate out of ``emit``. That is the signal the worker's bounded retry and
  ``health().failed_batches`` are built on, and the one case where a retry cannot create
  duplicates — nothing landed, so there is nothing downstream to duplicate.
* **Partial failure does not.** A batch where some records landed must not be retried
  wholesale; it is counted and made readable through the optional ``losses()``.

A sink that absorbs a total failure and returns normally is a sink the worker believes:
the retry never engages, ``failed_batches`` stays at zero and ``flush()`` returns ``True``
while every event is lost. That reading is precisely what SPEC-017 existed to make
impossible, and what SPEC-026 generalized from ``MultiSink`` to the whole family.
"""

from __future__ import annotations

from typing import NamedTuple, Protocol, runtime_checkable

__all__ = ["Sink", "SinkDeliveryError", "SinkLosses", "read_losses"]


class SinkDeliveryError(Exception):
    """Raised by a sink whose ``emit`` delivered none of the batch (SPEC-026 FR-001).

    A distinct type so an operator reading a ``stopped_reason`` or a diagnostic line can tell
    "the destination refused everything" from "the sink itself has a bug". Sinks that already
    have a natural exception to re-raise — a driver error, a ``MultiSink`` child's — re-raise
    that instead: the contract is that *something* propagates, not that it must be this type.
    """


class SinkLosses(NamedTuple):
    """What a sink discarded or could not confirm, cumulative for its lifetime (SPEC-026 FR-002).

    Two fields rather than one because the remedies differ. ``dropped`` is an event the sink
    discarded *before* attempting delivery — usually one the destination could never have accepted
    as built (an oversized record), so the fix is upstream in what the application logs; for a sink
    whose client owns a local buffer it also covers what that buffer refused, which is
    backpressure. The stderr line names which. ``failed`` means delivery was attempted and the
    destination did not confirm it, so the fix is the destination or the network.

    ``failed`` is an **upper bound** on loss, not a count of it. A sink that also raises on total
    failure counts the attempt here *and* hands the batch back to the worker, whose retry may
    then deliver it — so a transient outage leaves ``failed`` non-zero with nothing actually
    lost. ``health().failed_batches`` is the worker-level record of a batch given up on for good;
    this is the sink-level record of everything that did not go through first time.
    """

    dropped: int
    failed: int


@runtime_checkable
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None:
        """Ship a batch of serialized event dicts.

        **Raise when the batch delivered nothing** and it was non-empty — the worker's bounded
        retry and ``health().failed_batches`` depend on that signal, and a retry there cannot
        duplicate anything (SPEC-026 FR-001). Raise *after* the sink's own retries are spent, so
        the worker's retry composes on top rather than replacing it; a sink whose own budget
        makes the worker's redundant should say so in its docstring rather than absorb silently.

        **Do not raise on partial failure.** A batch where some records landed would be
        re-delivered wholesale by the worker's retry, and duplicates downstream are worse than
        the counted loss (SPEC-017 FR-004, SPEC-018). Report that through ``losses()`` instead.

        ``emit([])`` is a no-op and never raises: an empty batch has not failed to deliver.
        """
        ...

    def close(self) -> None:
        """Flush and release any resources."""
        ...

    # ``losses()`` is optional, and deliberately *not* declared here: ``Sink`` is structural, and
    # a third-party sink written against the pre-SPEC-026 interface must keep satisfying it.
    # ``read_losses`` below is the probe; an absent method reads as "reports nothing".
    #
    #     def losses(self) -> SinkLosses | None:
    #         """Cumulative loss this sink absorbed. Never raises; safe to call during emit.
    #
    #         ``None`` is the same answer as having no method at all — "this sink reports
    #         nothing". A wrapper returns it when what it wraps reports nothing.
    #         """
    #         return SinkLosses(dropped=self._dropped, failed=self._failed)


def read_losses(sink: object) -> SinkLosses | None:
    """Read a sink's optional ``losses()``; ``None`` when it reports nothing.

    ``None`` covers four cases deliberately treated alike: no ``losses`` attribute, one that is
    not callable, one that raised, and one that returned anything other than a ``SinkLosses`` —
    including ``None`` itself, which the shipped wrapper sinks return when what they wrap
    reports nothing.

    The single reader for the optional half of the protocol (SPEC-026 FR-002), shared by
    ``Worker.health`` and ``MultiSink.losses`` so the probe and its guarantees are written once.

    Total by design. ``health()`` is documented "Never raises" and is the call an operator makes
    when things are *already* going wrong, so a third-party sink with a broken accessor must not
    be able to take the snapshot down with it. The returned shape is checked as well as the call:
    a ``losses()`` returning something else would otherwise put an arbitrary object where callers
    read two integers. Nothing is written to stderr — a poll happens as often as the caller likes,
    and a broken accessor is a sink bug rather than a loss.
    """
    try:
        accessor = getattr(sink, "losses", None)
        losses = accessor() if callable(accessor) else None
    except Exception:
        return None
    return losses if isinstance(losses, SinkLosses) else None
