"""What a delegating wrapper needs to stand on the delivery engine (SPEC-075 FR-001, FR-002)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, cast

from log_foundry.sinks._engine import _seconds, declared_count, outcomes_of
from log_foundry.sinks.base import _static_value
from log_foundry.sinks.verdict import Outcome, SinkCounters, _probe

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Sequence

    from log_foundry.sinks.verdict import Verdict

    type Deliver = Callable[[list[dict[str, object]]], Verdict]


class _Forwards(Protocol):
    """A wrapper that hands its stop signal on to what it wraps."""

    def _forward_stop_signal(self, signal: threading.Event | None) -> None:
        """Hands the signal on.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """


class ForwardedStopSignal:
    """A ``log_foundry_stop_signal`` stored where the static reader finds it, then forwarded.

    The engine reads a run's stop signal with ``_engine.stop_signal_of``, which reads statically
    and so reads a property as no signal: a wrapper keeping the signal behind a property would
    wait out its own rounds' backoff uninterrupted. This descriptor defines ``__set__`` and no
    ``__get__``, as ``_args.ReadOnlyCount`` does, so ordinary access and the static reader both
    find the value in the instance's own ``__dict__``, and nothing is evaluated to read it. An
    assignment stores the value there first and then calls the owner's
    ``_forward_stop_signal``, which hands it to the sink or backend the owner wraps, as the
    property's setter did (SPEC-027). The owner seeds the ``__dict__`` entry with ``None`` at
    construction, without forwarding, so the name reads ``None`` before the worker offers one.
    The ``__get__`` below exists for the type checker only.

    Attributes:
      None.
    """

    __slots__ = ("_name",)

    if TYPE_CHECKING:

        def __get__(self, obj: object, owner: type | None = None) -> threading.Event | None:
            """Types a read as the stored signal; never defined at run time.

            Args:
              obj: The instance.
              owner: Its class.

            Returns:
              The stored signal.

            Raises:
              None.
            """
            ...

    def __set_name__(self, owner: type, name: str) -> None:
        """Records the attribute's name, for the instance ``__dict__`` key.

        Args:
          owner: The class the descriptor is assigned in.
          name: The attribute's name.

        Returns:
          None.

        Raises:
          None.
        """
        self._name = name

    def __set__(self, obj: _Forwards, value: threading.Event | None) -> None:
        """Stores the signal in the instance's ``__dict__``, then forwards it.

        Args:
          obj: The wrapper.
          value: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          Exception: Whatever the owner's forward raises; each shipped owner's absorbs or never
            raises.
        """
        vars(obj)[self._name] = value
        obj._forward_stop_signal(value)


def offered_counters(inner: object) -> SinkCounters | None:
    """Returns the wrapped sink's own counters when it offers ``deliver``, else ``None``.

    The probe is SPEC-072's, taken once so the offer and the counters cannot disagree: the
    wrapper's ``log_foundry_counters`` is the very object the bare sink's run would count into.

    Args:
      inner: The wrapped sink, of any type.

    Returns:
      The wrapped sink's ``SinkCounters``, or ``None`` when it offers nothing.

    Raises:
      None.
    """
    probe = _probe(inner)
    return None if probe is None else probe[1]


def copy_budget(wrapper: object, inner: object | None) -> None:
    """Stores on the wrapper the budget the engine would read off the sink it wraps.

    Each is read with ``_static_value``, exactly as the engine reads it on the bare sink, and
    passed through the engine's own reader for that name — ``declared_count`` for
    ``max_retries``, ``_seconds`` for ``max_retry_after`` — so what is stored is what the engine
    would use, or ``None`` where it would use its default, and the two cannot drift. A property,
    a ``bool``, a ``str`` or any other value the engine reads as "declared none" is stored as
    ``None``; an exact count is stored floored, as the engine floors it; and the engine then
    reads the stored value to the same rounds and the same ceiling, so the wrapped sink gets
    what it gets alone. A name the wrapped sink lacks, or whose read fails, and both names when
    ``inner`` is ``None`` (a wrapper that offers nothing), are ``None`` too. The value goes in
    the wrapper's own ``__dict__``, past the read-only descriptor, so a public read never returns
    the descriptor object. A later assignment to the wrapped sink is not seen.

    Args:
      wrapper: The wrapper being built.
      inner: The wrapped sink or backend, or ``None`` when the wrapper offers no ``deliver``.

    Returns:
      None.

    Raises:
      None.
    """
    retries = None if inner is None else _static_value(inner, "max_retries")
    ceiling = None if inner is None else _static_value(inner, "max_retry_after")
    vars(wrapper)["max_retries"] = declared_count(retries)
    vars(wrapper)["max_retry_after"] = _seconds(ceiling)


def current_deliver(inner: object, counters: SinkCounters | None) -> Deliver | None:
    """Returns the wrapped sink's ``deliver`` for this round, or ``None`` when it stopped offering.

    Taken on every round, so a wrapped sink whose counters were replaced since construction —
    by ``None`` or by another ``SinkCounters`` — is caught here: the engine commits into the
    counters the wrapper holds and ``losses()`` reads the wrapped sink's, so a run over two
    objects would lose its counts. The caller raises, naming the type.

    Args:
      inner: The wrapped sink.
      counters: The counters the wrapper holds.

    Returns:
      The bound ``deliver``, or ``None``.

    Raises:
      None.
    """
    probe = _probe(inner)
    if probe is None or probe[1] is not counters:
        return None
    return probe[0]


def map_forwarded(verdict: object, positions: Sequence[int], outcomes: list[Outcome]) -> object:
    """Writes a wrapped sink's verdict into the wrapper's outcomes, returning its hint.

    ``outcomes_of`` is the engine's one definition of a malformed verdict. A malformed one turns
    every forwarded position ``UNKNOWN`` and gives no hint, so the run settles as the bare sink's
    would. A well-formed one writes each outcome at its forwarded position, and its
    ``retry_after`` is read once more inside an ``except Exception`` that substitutes ``None``,
    since a subclass may answer differently twice and a raise escaping ``deliver`` would fail a
    run the bare sink's run settles. The hint is passed through as read; the engine judges it.

    Args:
      verdict: What the wrapped sink's ``deliver`` returned.
      positions: For each forwarded event, its index in the wrapper's batch.
      outcomes: The wrapper's outcomes, one per event of its batch, written in place.

    Returns:
      The hint as read, or ``None``.

    Raises:
      None. A ``BaseException`` from a field propagates.
    """
    found = outcomes_of(verdict, len(positions))
    if found is None:
        for position in positions:
            outcomes[position] = Outcome.UNKNOWN
        return None
    for position, outcome in zip(positions, found, strict=True):
        outcomes[position] = outcome
    try:
        return cast("Verdict", verdict).retry_after
    except Exception:
        return None
