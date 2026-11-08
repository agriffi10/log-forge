"""The verdict contract: what one ``deliver`` call reports per event (SPEC-072 FR-001)."""

from __future__ import annotations

import enum
import threading
import types
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self, cast, final

from log_foundry import _diag
from log_foundry.sanitize import _int_text
from log_foundry.sinks._retry import exact_int
from log_foundry.sinks.base import (
    _ABSENT,
    SinkLosses,
    _class_member,
    _own_attribute,
    _static_value,
)

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["Outcome", "SinkCounters", "Verdict", "emit_via_deliver", "read_deliver"]


class Outcome(enum.Enum):
    """What happened to one event of a batch handed to ``deliver``; closed for 1.x.

    ``outcomes[i]`` of a :class:`Verdict` is the fate of ``batch[i]``. ``REJECTED`` and
    ``DROPPED`` are the two forms of "rejected for good" and differ only in which
    :class:`~log_foundry.SinkLosses` term the engine counts them in: ``failed`` for an event the
    destination refused, ``dropped`` for one the sink refused to send because it could never be
    accepted as built. ``UNKNOWN`` is an event that may have landed — a ``2xx`` whose body
    cannot be read — and is resent only by a sink that declares itself idempotent. ``SKIPPED``
    is a deliberate non-send that the engine counts nowhere and never resends, and it is not
    ``ACCEPTED`` for the run-failed rule, so a batch of one declined event and a rejected rest
    still fails.

    **Closed for 1.x.** It is an input a sink writes, not a result a caller reads, so the rule
    that results grow by new values (invariant 10) does not reach it, and a new member would
    break a typed sink's exhaustive ``match`` at type-check time.

    Attributes:
      ACCEPTED: The destination took the event.
      RETRYABLE: It did not land and a resend can succeed.
      REJECTED: The destination refused it for good.
      DROPPED: The sink refused to send it, because it could never be accepted as built.
      UNKNOWN: It may have landed; resent only by an idempotent sink.
      SKIPPED: Not sent, deliberately; no engine counter, never resent.
    """

    ACCEPTED = "accepted"
    RETRYABLE = "retryable"
    REJECTED = "rejected"
    DROPPED = "dropped"
    UNKNOWN = "unknown"
    SKIPPED = "skipped"


@dataclass(frozen=True, kw_only=True)
class Verdict:
    """One ``deliver`` call's report: a fate per event, and the server's retry-after hint.

    Construction is keyword-only, as every public dataclass here is, and the class grows only by
    appending fields. ``outcomes`` is typed ``tuple[Outcome, ...]``; at run time a ``list`` is
    accepted too and stored as a ``tuple``, for an untyped caller, and any other container is
    refused. The container and each element are routed on their **real** type —
    ``issubclass(type(x), ...)``, never ``isinstance`` — and a refusal names the type through
    the stored name (``_diag._STORED_NAME``), so a metaclass ``__name__`` property can neither
    rename it nor run, and an element whose ``__class__`` claims ``Outcome`` is refused with its
    ``__class__`` never read. A ``tuple`` or ``list`` subclass is copied through the base
    class's own ``__iter__``, so a subclass's iteration hook never runs. The refusal names a
    position or a type, never a value, since a value can carry an event's data.

    ``retry_after`` is stored as given: the engine checks its type before it bounds it, and an
    unusable hint is answered with the backoff rather than refused here, because a sink that
    built a ``Verdict`` from a server header has already delivered or not by then.

    Attributes:
      outcomes: ``outcomes[i]`` is the fate of ``batch[i]`` in the list ``deliver`` was given.
      retry_after: The server's hint in seconds, advisory only, or ``None``.
    """

    outcomes: tuple[Outcome, ...]
    retry_after: float | None = None

    def __post_init__(self) -> None:
        """Refuses a container or element that is not an ``Outcome`` sequence, storing a tuple.

        Args:
          None.

        Returns:
          None.

        Raises:
          TypeError: When ``outcomes`` is not a ``tuple`` or ``list``, naming its type, or when an
            element is not an ``Outcome``, naming its position and its type.
        """
        given = self.outcomes
        kind = type(given)
        if issubclass(kind, tuple):
            items = tuple(tuple.__iter__(given))
        elif issubclass(kind, list):
            items = tuple(list.__iter__(cast("list[Outcome]", given)))
        else:
            raise TypeError(
                "Verdict outcomes must be a tuple of Outcome members, "
                f"not {_diag._STR_STR(_diag._STORED_NAME(kind))}"
            )
        for index, item in enumerate(items):
            if not issubclass(type(item), Outcome):
                raise TypeError(
                    f"Verdict outcomes[{index}] must be an Outcome, "
                    f"not {_diag._STR_STR(_diag._STORED_NAME(type(item)))}"
                )
        object.__setattr__(self, "outcomes", items)

    @classmethod
    def uniform(cls, outcome: Outcome, count: int, *, retry_after: float | None = None) -> Self:
        """Builds a verdict giving every one of ``count`` events the same outcome.

        The outcome is validated before the count is used, by building a one-element verdict
        first, so ``uniform("x", 0)`` is refused rather than yielding an empty verdict that
        hides the mistake until a batch is non-empty.

        Args:
          outcome: The fate of every event.
          count: How many events; an exact ``int`` (``True`` reads as ``1``).
          retry_after: The server's hint in seconds, or ``None``.

        Returns:
          The verdict.

        Raises:
          TypeError: When ``count`` is not an ``int``, or ``outcome`` is not an ``Outcome``.
          ValueError: When ``count`` is negative.
        """
        n = exact_int(count, "count", "Verdict.uniform")
        if n < 0:
            raise ValueError(f"Verdict.uniform count must not be negative, not {_int_text(n)}")
        cls(outcomes=(outcome,))
        return cls(outcomes=(outcome,) * n, retry_after=retry_after)


@final
class SinkCounters:
    """The counter object a sink owns for the engine, exposed as ``log_foundry_counters``.

    The engine is its one writer after construction: it commits each run's counts through one
    ``Deferral`` at the end of the run, which reads ``_counter_lock`` at commit time, and a
    sink's ``losses()`` returns :meth:`losses`. It lives on the sink, not the worker, so it is
    the same object for every worker, every swap back to that sink and every process the sink
    is forked into, and the fork repair reaches its lock on a third-party sink through the one
    attribute name. The names are unique in the tree: ``tests/test_deferral.py`` derives the
    counters some ``losses()`` reports as ``dropped`` by attribute name, and a plain ``dropped``
    would meet ``NullSink.dropped``, which no ``losses()`` reports.

    The engine accepts only this exact class (``type(v) is SinkCounters``), so a subclass whose
    counters are properties can never make a commit misbehave; it is marked final for a type
    checker to say so too.

    Attributes:
      dropped_events: Events the sink refused to send, counted only on a run that settled;
        exact, each event once and in no other term.
      failed_events: Events attempted and not confirmed; an upper bound, like every sink's
        ``failed``.
    """

    def __init__(self) -> None:
        """Starts both counters at zero with a fresh counter lock.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.dropped_events = 0
        self.failed_events = 0
        self._counter_lock = threading.Lock()

    def losses(self) -> SinkLosses:
        """Returns both counters as one snapshot, read under the counter lock.

        Args:
          None.

        Returns:
          The counters as a ``SinkLosses``.

        Raises:
          None.
        """
        with self._counter_lock:
            dropped = self.dropped_events
            failed = self.failed_events
        return SinkLosses(dropped=dropped, failed=failed)


def _bound_deliver(sink: object, *, override_rule: bool, skip_none: bool) -> object | None:
    """Returns the sink's class-defined ``deliver`` bound to it, or ``None``.

    Args:
      sink: The sink.
      override_rule: Whether the offer requires that ``sink.emit``, as ordinary lookup would
        resolve it, is a plain function defined on or below the class defining ``deliver``. So
        each of these refuses it: no class defining ``emit`` at all (a ``__getattr__`` that
        supplies one included, since the orphan fallback would raise without it); an ``emit`` in
        an earlier class; one that is not a plain function (a property, a ``staticmethod``); one
        set on the instance (``sink.emit = scrub``, ``mock.patch.object(sink, "emit")``), read
        through :func:`~log_foundry.sinks.base._own_attribute` so no hook runs; and any class
        in the MRO defining ``__getattribute__`` as a plain function, which could hand the
        worker a different ``emit`` than the class's and cannot be asked without running it.
      skip_none: Whether a ``deliver = None`` is passed over (the adapter) or refuses (the probe).

    Returns:
      The bound method, or ``None``.

    Raises:
      Exception: Whatever an unusual class object raises; callers guard.
    """
    cls = type(sink)
    found = _class_member(cls, "deliver", skip_none=skip_none)
    if found is None:
        return None
    function, index = found
    if type(function) is not types.FunctionType:
        return None
    if override_rule:
        hooked = _class_member(cls, "__getattribute__")
        if hooked is not None and type(hooked[0]) is types.FunctionType:
            return None
        emit = _class_member(cls, "emit")
        if emit is None or type(emit[0]) is not types.FunctionType or emit[1] < index:
            return None
        if _own_attribute(sink, "emit", emit) is not _ABSENT:
            return None
    return types.FunctionType.__get__(function, sink, cls)


def _probe(
    sink: object,
) -> tuple[Callable[[list[dict[str, object]]], Verdict], SinkCounters] | None:
    """Decides whether a sink offers ``deliver``, returning it bound with the sink's counters.

    The one probe :func:`read_deliver` and the engine share, so the engine counts into the very
    counters the offer was decided on rather than reading the attribute a second time. Every
    step reads the sink's **type** or reads statically: ``deliver`` must be a plain function in
    some class's namespace, no class earlier in the MRO than that one may define ``emit``, and
    ``log_foundry_counters`` must be exactly a :class:`SinkCounters`.

    Args:
      sink: The sink to probe, of any type.

    Returns:
      ``(deliver, counters)``, or ``None`` when the sink does not offer ``deliver``.

    Raises:
      None.
    """
    try:
        bound = _bound_deliver(sink, override_rule=True, skip_none=False)
        if bound is None:
            return None
        counters = _static_value(sink, "log_foundry_counters")
        if type(counters) is not SinkCounters:
            return None
    except Exception:
        return None
    return cast("Callable[[list[dict[str, object]]], Verdict]", bound), counters


def read_deliver(sink: object) -> Callable[[list[dict[str, object]]], Verdict] | None:
    """Returns a sink's bound ``deliver`` when it offers one, else ``None``; never raises.

    The probe for the optional ``deliver`` member, the sibling of ``read_losses``. It reads the
    sink's **type**, never the instance's dynamic attributes, so no third-party ``__getattr__``,
    property or metaclass hook runs and nothing is written, stderr included:

    - ``deliver`` is found in a class namespace along ``type(sink).__mro__`` and must be a plain
      function. A proxy whose ``__getattr__`` delegates to an engine sink therefore offers
      nothing, and its own ``emit`` runs. A class attribute ``deliver = None`` is a declared
      opt-out, as is any ``deliver`` that is not a function (a ``staticmethod``, a property).
    - ``deliver`` is offered only when no class before the defining one in the MRO defines
      ``emit``, some class does define it, the ``emit`` found is a plain function (a property
      or a ``staticmethod`` refuses), none is set on the instance itself, and no class defines
      ``__getattribute__`` as a plain function, so a subclass that overrides ``emit`` to
      stamp or scrub and then calls ``super().emit()`` keeps running its override, and so does an
      ``emit`` assigned on the instance (``sink.emit = scrub``, ``mock.patch.object``).
    - ``log_foundry_counters`` must be exactly a :class:`SinkCounters`, read statically, so a sink
      that defines ``deliver`` with no counters keeps delivering through its ``emit``.

    It is public for the reason ``read_losses`` is: a third-party wrapper that offers ``deliver``
    itself asks it whether its inner sink offers one.

    Args:
      sink: The sink to probe, of any type.

    Returns:
      The bound ``deliver``, or ``None`` when the sink does not offer it.

    Raises:
      None.
    """
    probe = _probe(sink)
    return None if probe is None else probe[0]


def emit_via_deliver(
    sink: object,
    batch: list[dict[str, object]],
    *,
    deliver: Callable[[list[dict[str, object]]], Verdict] | None = None,
) -> None:
    """Delivers a batch through the sink's own ``deliver``, as that sink's ``emit`` should.

    **What it is for.** A sink that defines ``deliver(batch) -> Verdict`` still has to define
    ``emit``, because ``emit`` is the 1.x contract every caller outside the worker uses: a
    wrapper calling ``inner.emit(batch)``, a subclass calling ``super().emit(batch)``, a test, an
    application that ships one batch by hand. Make that ``emit`` this call, so both routes run the
    same delivery::

        def emit(self, batch: list[dict[str, object]]) -> None:
            emit_via_deliver(self, batch)

    **What it does.** It runs the delivery engine the worker runs, inline on the calling thread:
    ``deliver`` is called, and only the events its verdict marks ``RETRYABLE`` are resent — plus,
    for a sink declaring ``log_foundry_idempotent = True``, the ones it marks ``UNKNOWN``, a
    verdict it could not read, and an exception its ``_retryable_error`` accepts — within
    ``max_retries + 1`` rounds, the sink's own ``max_retries`` read as an exact ``int`` floored
    at zero (zero when it has none). Between rounds it waits the 1.x backoff, cut short by the
    sink's ``log_foundry_stop_signal``, or a verdict's ``retry_after`` bounded by the sink's
    ``max_retry_after``. Once, when the run ends, it counts every event that did not land into
    the sink's ``log_foundry_counters``, which ``losses()`` then reports; ``DROPPED`` events are
    counted only when the call returns rather than raises, as a 1.x ``emit``'s were.

    **The 1.x** ``emit`` **contract it keeps.** It returns normally when anything landed, when the
    verdict could not be read, and when an event's outcome is ``UNKNOWN`` — a partial outcome is
    counted, never raised — and it raises :class:`~log_foundry.SinkDeliveryError` only when the run
    failed outright, naming counts and the sink's type, never an event's data. An empty batch
    returns at once, before anything about the sink is checked, since ``emit([])`` never raises.
    To the worker's retry rule the call is one attempt, as any 1.x ``emit`` is, and it is never
    declined.

    **What it asks of the sink.** A ``deliver`` it can find, and ``log_foundry_counters`` set to
    exactly a :class:`SinkCounters`; without either it raises ``TypeError`` naming the sink's type
    and the missing member, so a misspelt member is a failure the worker counts rather than events
    that vanish. With ``deliver=None`` the sink's class is searched for one the way
    :func:`read_deliver` searches, except that an ``emit`` the class defines does not hide it —
    this *is* that ``emit`` — and a ``deliver = None`` opt-out is passed over for one further up.
    ``deliver=`` hands it the callable to run instead. Every member it reads from the sink is
    read statically, so no ``__getattr__``, ``__getattribute__`` or property of the sink runs; a
    property in place of a member reads as that member missing.

    **Its surface.** ``sink`` and ``batch`` are its only positional parameters, and anything it
    gains in 1.x is keyword-only with a default, as ``deliver`` is; it returns ``None``. This is
    the documented import path, beside :class:`Verdict`; the engine behind it is private and free
    to move.

    Args:
      sink: The sink whose ``emit`` this is — normally ``self``.
      batch: The events, as ``emit`` received them. The list is not mutated.
      deliver: The ``deliver`` to run, or ``None`` to find the sink's own.

    Returns:
      None.

    Raises:
      TypeError: When the sink has no usable ``deliver`` or no ``log_foundry_counters`` that is
        exactly a ``SinkCounters``, naming its type's stored name and the missing member.
      SinkDeliveryError: When the run delivered nothing and something was rejected, unresolved
        or raised, naming counts and the sink's type, never a value.
      BaseException: Whatever ``deliver`` raises that is not an ``Exception``, unchanged.
    """
    from log_foundry.sinks import _engine

    _engine.emit_via_deliver(sink, batch, deliver=deliver)
