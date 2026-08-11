"""The sink interface (arch §8)."""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

__all__ = ["Sink", "SinkDeliveryError", "SinkLosses", "read_losses"]


class SinkDeliveryError(Exception):
    """Raised by a sink whose ``emit`` delivered none of the batch (SPEC-026 FR-001).

    It is a distinct type so an operator reading a ``stopped_reason`` or a diagnostic line can
    tell "the destination refused everything" from "the sink itself has a bug". Sinks that
    already have a natural exception to re-raise — a driver error, a ``MultiSink`` child's —
    re-raise that instead: the contract is that something propagates, not that it must be this
    type.
    """


@dataclass(frozen=True)
class SinkLosses:
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

    :meth:`emit` and :meth:`close` are ``@abstractmethod`` (SPEC-034 FR-005). A ``Protocol`` is
    normally satisfied *structurally* and that is still how every shipped sink satisfies this
    one — but making it a public export invites inheritance, and an incomplete subclass of a
    protocol whose members have empty bodies instantiates happily and returns ``None`` from the
    method it failed to define. Reproduced before the decorators were added: one typo
    (``def emmit``) and three events were gone with ``flush()`` reporting ``True`` and every
    counter at zero — the "sink the worker believes" failure this file exists to prevent.
    ``mypy`` already refused it; only the runtime did not. Inheriting is still not required, and
    ``isinstance`` remains structural.

    A fourth is optional in the same way and is an attribute rather than a method:
    ``log_foundry_stop_signal``. A sink that defines it — a plain
    ``threading.Event | None``, initialised to ``None`` — is handed the worker's shutdown event
    whenever the library takes ownership of it, and a sink that does not is simply never
    offered one. **Honouring it is how a retrying sink stays interruptible** (SPEC-027): the
    worker owns a single drain thread, so a sink's backoff is a global pause on log delivery
    held across ``shutdown()``, which joins that thread. Pass it to ``sinks/_retry.wait`` — or
    wait on it directly — rather than calling ``time.sleep``, or a shutdown cannot cut the
    wait short and a slow destination holds process exit for the length of its own backoff.
    A **wrapper** sink must forward it to whatever actually holds the retry loop; set on a
    wrapper and stopped there, the signal reaches nothing.

    The name is namespaced deliberately (SPEC-034 FR-006). The library assigns this attribute
    onto an object it does not own, by ``hasattr`` probe, so a bare ``stop_signal`` — which is
    what shipped before 1.0 — silently overwrote any attribute of that name a third-party sink
    already used, with a ``threading.Event``. Reproduced. A prefixed name cannot collide by
    accident, and the cost is one verbose attribute on the sinks that want interruptibility.

    A fifth is optional in the same way and exists for one event only:
    ``discard_buffered_after_fork() -> None``. A sink that owns a **buffered** stream — one it
    opened itself, rather than the process's ``sys.stdout`` — inherits the parent's unflushed
    bytes in a forked child, and both processes then write them (SPEC-039 FR-004). Defining this
    method is how a sink says it can strand what it inherited; a sink holding no buffer of its
    own needs nothing here. It is *called* rather than assigned, which is why it carries no
    ``log_foundry_`` prefix: the collision the fourth member's name guards against is an
    attribute the library writes onto an object it does not own.

    **Which sinks are actually asked is narrower than "whichever define it", and the boundary is
    the same one every fork repair has.** The child's repair walks this package's own objects —
    a sink whose class is defined here or **inherits** one that is, ``Sink`` included, and the
    **owned** sinks such an object holds, ``MultiSink``'s children among them. A third-party
    sink satisfying this Protocol *structurally*, which is how every shipped sink satisfies it,
    is outside that walk exactly as its locks are (SPEC-039 FR-005): its hook is never called,
    and a fork mid-batch there can still duplicate — including when a wrapper this library owns
    is holding it. Inheriting from ``Sink`` or from a shipped sink is what brings it inside. A
    **wrapper** sink the walk enters need do nothing itself, because the owned sinks it holds
    are reached directly rather than through it.

    **It runs in a child that has not yet returned from ``fork``, on that child's only thread,
    and must not block.** Nothing else is running there, so there is no holder a lock could be
    waiting for — and no watchdog to end a wait taken anyway, since a pending alarm does not
    survive the fork and this runs before any application code can arm one. It may raise: the
    library absorbs and announces that, and the cost is a child that may write the parent's
    pending bytes a second time.

    "Safe to call during an emit" is a concurrency requirement once :meth:`emit` is (SPEC-028
    FR-003). The shipped sinks keep their loss counters under a **dedicated** lock, separate
    from whatever guards their transport: an increment is a read-modify-write that Python does
    not promise is atomic, and the pair read must come from one instant rather than straddling
    another thread's increments. It is deliberately not the transport lock — ``health()`` is
    the call an operator makes when a destination is already hanging, and sharing one lock
    would make that poll wait for an in-flight emit and its retry backoff. Where a sink holds
    both, the order is always transport then counter, never the reverse.
    """

    @abstractmethod
    def emit(self, batch: list[dict[str, object]]) -> None:
        """Ships a batch of serialized event dicts.

        **May be called concurrently from more than one thread, and must tolerate it**
        (SPEC-028 FR-001). The background worker drains on its own thread, and a level call
        made with no active span emits synchronously on the *caller's* thread (arch §12), which
        may be any of the application's — an audit observed one sink object entered by two
        application threads and the worker at once. An implementation holding mutable transport
        state, such as a stream it rebinds, a reused socket or a connection with transaction
        scope, must serialize access to it for the whole span of the operation that assumes
        exclusivity. One that holds no such state need do nothing.

        This is a requirement on implementations, not a promise the library serializes on their
        behalf. It cannot: the orphan path runs on a thread the library does not own.

        Raise when the batch delivered nothing and it was non-empty — the worker's bounded
        retry and ``health().failed_batches`` depend on that signal, and a retry there cannot
        duplicate anything (SPEC-026 FR-001). Raise after the sink's own retries are spent, so
        the worker's retry composes on top rather than replacing it; a sink whose own budget
        makes the worker's redundant should say so in its docstring rather than absorb
        silently.

        Do not raise on partial failure. A batch where some records landed would be
        re-delivered wholesale by the worker's retry, and duplicates downstream are worse than
        the counted loss (SPEC-017 FR-004, SPEC-018). Report that through ``losses()`` instead.

        **After ``close()`` has released or invalidated something, raise rather than absorb**
        (SPEC-032 FR-002). This is the same obligation applied to the sink's own lifecycle
        rather than the destination's: an absorbed batch is one the worker believes, so the
        retry never engages, ``failed_batches`` stays at zero and ``flush()`` returns True while
        the events are gone — reached not through a destination that is down but through a
        transport the library itself has already let go. Three shipped sinks failed this rule
        for four specs, in three different ways: a produce into a batch nothing would flush
        again, a future nothing would resolve, and a client that quietly reconnected.

        The converse is equally binding: a sink holding nothing to release **keeps accepting**.
        ``close()`` on a sink that opens a fresh connection per request, or whose client belongs
        to the caller, has invalidated nothing, so a later batch still delivers and refusing it
        would be loss the library invented rather than loss it reported. Which of the two
        applies is a property of the sink, so each shipped sink records its answer in its class
        docstring and a test holds it to it.

        Args:
          batch: The events to ship. **Borrowed, not given** — the list and the dicts inside it
            may be handed to other sinks afterwards, which ``MultiSink`` does to every child in
            turn, so mutating either in place silently changes or empties what a later sink
            receives. Copy before reshaping or redacting. Reproduced: a child that cleared the
            list left the next child with nothing and no error anywhere. ``emit([])`` is a no-op and never raises, since an empty
            batch has not failed to deliver — closed or not.

        Returns:
          None.

        Raises:
          Exception: When the batch delivered nothing.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Flushes and releases any resources.

        **May be called while an ``emit`` is in flight on another thread** (SPEC-028 FR-001).
        It must therefore either wait for that emit or become a no-op, and must never release a
        resource a concurrent ``emit`` is about to use — a half-released transport is worse than
        an unreleased one, because the emit then fails against a closed handle instead of
        succeeding. Taking the same lock ``emit`` takes satisfies this; so does an idempotent
        guard checked under that lock.

        The flag this sets is the one a later :meth:`emit` reads, so the two are one decision
        rather than two (SPEC-032 FR-002). Set it *before* releasing anything: an emit arriving
        during a release must be refused rather than handed a half-released transport. Make the
        release idempotent for the same reason a second ``close()`` is expected at all —
        ``atexit`` racing a caller's own cleanup is the documented case.

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
