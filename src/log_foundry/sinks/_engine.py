"""The delivery engine: bounded rounds over a sink's ``deliver``, counted once (SPEC-072)."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

from log_foundry import _diag
from log_foundry.sinks import _retry
from log_foundry.sinks._classify import ErrorPredicate, is_retryable
from log_foundry.sinks._deferral import Deferral
from log_foundry.sinks.base import _ABSENT, SinkDeliveryError, _class_member, _static_value
from log_foundry.sinks.verdict import Outcome, SinkCounters, Verdict, _bound_deliver, _probe

if TYPE_CHECKING:
    from collections.abc import Callable

    type Deliver = Callable[[list[dict[str, object]]], object]

BACKOFF_BASE: Final = 0.1
"""The sinks' 1.x backoff within a cycle: the wait before attempt ``i + 1`` is ``0.1 * 2**i``.

It is the formula twelve sink modules apply today with their own ``_BACKOFF_BASE``, restarting
at ``i = 0`` in every cycle, written here once for every sink that adopts the engine.
"""

CYCLE_BACKOFF_BASE: Final = 0.01
"""The worker's 1.x wait between whole-batch attempts: ``min(0.01 * 2**c, CYCLE_BACKOFF_CAP)``."""

CYCLE_BACKOFF_CAP: Final = 0.5
"""The ceiling of :func:`cycle_backoff`, as ``Worker._emit`` capped it in 1.x."""

DEFAULT_MAX_RETRY_AFTER: Final = 30.0
"""The ceiling on a verdict's ``retry_after`` for a sink that declares no usable one.

The number ``HTTPSink`` carries today. A sink's own ``max_retry_after`` replaces it when its
type is exactly ``int`` or ``float``, so ``inf`` honours a hint in full, as 1.x ``HTTPSink`` does.
"""

_EXPONENT_CAP: Final = 60
"""The largest exponent either backoff takes, so no ``max_retries`` raises ``OverflowError``.

``0.1 * 2**60`` is far past ``_retry.MAX_WAIT`` and ``0.01 * 2**60`` far past the cycle cap, so
the bound never changes a wait the existing caps would not already have set. In 1.x the worker's
own loop computed ``0.01 * 2**attempt`` unbounded and raised ``OverflowError`` on the drain thread
from attempt 1025; its loop now calls :func:`cycle_backoff` too.
"""

_MAX_WAIT_INT: Final = int(_retry.MAX_WAIT)
""":data:`_retry.MAX_WAIT` as an ``int``, so a huge ``int`` hint is compared unconverted."""

_RAISED: Final = object()
"""The fate of an event in a round whose ``deliver`` raised an ``Exception``."""

_UNREAD: Final = object()
"""The fate of an event in a round whose verdict could not be read."""

_RESENDABLE_IF_IDEMPOTENT: Final = (Outcome.UNKNOWN, _UNREAD, _RAISED)
"""Fates resent only by an idempotent sink; ``_RAISED`` also asks the sink's predicate."""


@dataclass(frozen=True, kw_only=True)
class Delivery:
    """What one engine run did with a batch, by final fate, and whether the run failed.

    A run **fails** when no event was ``ACCEPTED`` (``SKIPPED`` is not accepted), at least one
    ended ``REJECTED``, unresolved or in a raising round, and none ended ``UNKNOWN`` or in an
    unreadable round. Otherwise it **settles**. Whoever receives a failed run owns the batch's
    count: the worker's abandon or declined line, the orphan path's ``orphan_lost``, or the
    adapter's raise and its catcher.

    Attributes:
      attempts: ``deliver`` calls made.
      cycles: Cycles started; the worker's abandon line names it as "emit attempts".
      accepted: Events that ended ``ACCEPTED``.
      rejected: Events that ended ``REJECTED``.
      dropped: Events that ended ``DROPPED``.
      unresolved: Events still ``RETRYABLE`` when the rounds ran out or one was declined.
      raised: Events in a round whose ``deliver`` raised, not resent.
      unknown: Events ``UNKNOWN`` or in an unreadable round, not resent.
      declined: Whether a further round was refused by the decline predicate.
      skipped: Events that ended ``SKIPPED``; in no engine counter.
      failed: Whether the run failed, by the rule above.
    """

    attempts: int
    cycles: int
    accepted: int
    rejected: int
    dropped: int
    unresolved: int
    raised: int
    unknown: int
    declined: bool
    skipped: int
    failed: bool


_SETTLED_EMPTY: Final = Delivery(
    attempts=0,
    cycles=0,
    accepted=0,
    rejected=0,
    dropped=0,
    unresolved=0,
    raised=0,
    unknown=0,
    declined=False,
    skipped=0,
    failed=False,
)
"""An empty batch's run: nothing called, nothing counted, settled."""


def cycle_backoff(cycle: int) -> float:
    """Returns the wait after whole-batch cycle ``cycle``: the worker's 1.x backoff, written once.

    ``Worker._emit``'s own ``emit`` loop calls this in place of its literal, so the engine and the
    1.x loop cannot drift apart. The exponent is bounded by :data:`_EXPONENT_CAP`.

    Args:
      cycle: The zero-based cycle that just ended; a negative reads as zero.

    Returns:
      ``min(CYCLE_BACKOFF_BASE * 2**cycle, CYCLE_BACKOFF_CAP)``.

    Raises:
      None.
    """
    return min(math.ldexp(CYCLE_BACKOFF_BASE, min(max(cycle, 0), _EXPONENT_CAP)), CYCLE_BACKOFF_CAP)


def _backoff(position: int) -> float:
    """Returns the within-cycle wait before attempt ``position + 1``, the sinks' 1.x backoff.

    Args:
      position: The zero-based attempt within the cycle that just failed.

    Returns:
      ``BACKOFF_BASE * 2**position`` with a bounded exponent; ``_retry.wait`` caps the wait.

    Raises:
      None.
    """
    return math.ldexp(BACKOFF_BASE, min(max(position, 0), _EXPONENT_CAP))


def _count(value: object) -> int:
    """Reads a retry count as an exact ``int`` floored at zero, else zero.

    Only an exact ``int`` counts, so a ``bool``, a ``float``, a ``str``, an ``int`` subclass and
    a property object all read as "declared none", which on every path gives the same rounds as
    zero. The floor matches the 1.x worker's ``max(max_retries, 0)``.

    Args:
      value: The sink's ``max_retries`` as read statically, or the worker's.

    Returns:
      The count.

    Raises:
      None.
    """
    if type(value) is int:
        return max(value, 0)
    return 0


def _seconds(value: object) -> float | None:
    """Reads a duration whose type is exactly ``int`` or ``float``, else ``None``.

    An ``int`` is brought into ``[-1, MAX_WAIT]`` by exact integer comparison before it is
    converted, because ``clamp_server_delay`` raises ``OverflowError`` on ``10**400``
    (``math.isinf`` converts it), and an exact-``int`` hint passes the type rule. A ``float`` is
    passed through, ``nan`` and ``inf`` included, for ``clamp_server_delay`` to answer.

    Args:
      value: A hint or a ceiling, of any type.

    Returns:
      The duration as a ``float``, or ``None`` when its type is not exactly ``int`` or ``float``.

    Raises:
      None.
    """
    kind = type(value)
    if kind is int:
        whole = cast("int", value)
        if whole > _MAX_WAIT_INT:
            return _retry.MAX_WAIT
        if whole < -1:
            return -1.0
        return float(whole)
    if kind is float:
        return cast("float", value)
    return None


def _ceiling(sink: object) -> float:
    """Returns the sink's ``max_retry_after`` when it is exactly an ``int`` or ``float``.

    Args:
      sink: The sink, read statically.

    Returns:
      The ceiling, or :data:`DEFAULT_MAX_RETRY_AFTER` when the sink declares none or a non-number.

    Raises:
      None.
    """
    found = _seconds(_static_value(sink, "max_retry_after"))
    return DEFAULT_MAX_RETRY_AFTER if found is None else found


def stop_signal_of(sink: object) -> threading.Event | None:
    """Returns the sink's ``log_foundry_stop_signal`` when it is a ``threading.Event``.

    Read statically, so a property is never called and reads as no event.

    Args:
      sink: The sink.

    Returns:
      The event, or ``None``.

    Raises:
      None.
    """
    found = _static_value(sink, "log_foundry_stop_signal")
    if issubclass(type(found), threading.Event):
        return cast("threading.Event", found)
    return None


def _idempotent(sink: object) -> bool:
    """Whether the sink declares ``log_foundry_idempotent`` exactly ``True``, read statically.

    Args:
      sink: The sink.

    Returns:
      True only for the ``True`` object; a truthy non-``bool`` is not a declaration.

    Raises:
      None.
    """
    return _static_value(sink, "log_foundry_idempotent") is True


def _predicate_of(sink: object) -> ErrorPredicate | None:
    """Returns the sink's ``_retryable_error`` ready to call, or ``None``.

    Found statically on an instance, a method comes back as the class's **unbound** function,
    which ``is_retryable`` would call one argument short: it raises inside the wrapper, answers
    False, and every predicate would refuse silently. So a predicate found on the class is bound
    through its type's ``__get__`` (which also unwraps a ``staticmethod``), as SPEC-070's
    contract hands the engine the bound ``sink._retryable_error``; one in the instance's own
    ``__dict__`` is used as found, as ordinary access would. A separate function so the unbound
    mutant can be pinned as a test.

    Args:
      sink: The sink.

    Returns:
      A callable predicate, or ``None`` when there is none or binding it raised.

    Raises:
      None.
    """
    try:
        found = _static_value(sink, "_retryable_error")
        if found is _ABSENT:
            return None
        member = _class_member(type(sink), "_retryable_error")
        if member is not None and member[0] is found:
            binder = _class_member(type(found), "__get__")
            if binder is not None:
                found = cast("Callable[..., object]", binder[0])(found, sink, type(sink))
        if not callable(found):
            return None
    except Exception:
        return None
    return cast("ErrorPredicate", found)


def outcomes_of(verdict: object, n: int) -> tuple[Outcome, ...] | None:
    """Returns a verdict's outcomes when it is well formed for ``n`` events, else ``None``.

    The engine's one definition of a malformed verdict, and its only reader of ``.outcomes``:
    not a ``Verdict``, outcomes that are not an exact tuple of exact ``Outcome`` members, a
    length other than ``n``, or a field that raises when read. Both fields are read inside the
    guard, so a ``retry_after`` that raises makes the verdict malformed too. SPEC-075's
    wrappers share it.

    Args:
      verdict: What ``deliver`` returned.
      n: How many events the round handed it.

    Returns:
      The exact tuple, or ``None``.

    Raises:
      None. A ``BaseException`` from a field still propagates.
    """
    try:
        if not issubclass(type(verdict), Verdict):
            return None
        held = cast("Verdict", verdict)
        outcomes = held.outcomes
        _ = held.retry_after
        if type(outcomes) is not tuple:
            return None
        if tuple.__len__(outcomes) != n:
            return None
        for item in tuple.__iter__(outcomes):
            if type(item) is not Outcome:
                return None
    except Exception:
        return None
    return outcomes


def _hint_of(verdict: object) -> float | None:
    """Reads an accepted verdict's ``retry_after`` as a usable number, or ``None``.

    Asked only after :func:`outcomes_of` accepted the verdict, so a malformed one never supplies
    a wait; read again under its own guard, since a subclass may answer differently twice.

    Args:
      verdict: A verdict :func:`outcomes_of` accepted.

    Returns:
      The hint as a ``float``, or ``None``.

    Raises:
      None.
    """
    try:
        value = cast("Verdict", verdict).retry_after
    except Exception:
        return None
    return _seconds(value)


def _snapshot(batch: list[dict[str, object]]) -> list[dict[str, object]]:
    """Copies the caller's batch once, so a ``deliver`` that mutates it cannot move an index.

    A ``list`` (a subclass included) is copied through ``list.__iter__``, so no subclass hook
    runs; anything else is iterated as given.

    Args:
      batch: The batch as handed to the engine.

    Returns:
      A new list of the same event objects.

    Raises:
      Exception: Whatever iterating a non-list batch raises.
    """
    if issubclass(type(batch), list):
        return list(list.__iter__(batch))
    return list(batch)


def _wait(delay: float, stop: threading.Event | None) -> threading.Event | None:
    """Waits between rounds on the stop event, falling back to a sleep if waiting on it raises.

    The event is the sink's or the worker's; a subclass whose ``wait`` raises is announced once
    and dropped for the rest of the run, so no ``Exception`` escapes the engine and no round
    spins without its wait.

    Args:
      delay: Seconds to wait.
      stop: The stop event, or ``None``.

    Returns:
      The event to use for the next wait.

    Raises:
      None.
    """
    try:
        _retry.wait(delay, stop)
    except Exception as exc:
        _diag.absorbed("waiting between delivery rounds", exc, "later waits sleep instead")
        _retry.wait(delay, None)
        return None
    return stop


def _run(
    sink: object,
    deliver: Deliver,
    counters: SinkCounters,
    batch: list[dict[str, object]],
    *,
    sink_retries: object,
    worker_retries: object,
    stop: threading.Event | None,
    decline: Callable[[float], bool] | None,
) -> Delivery:
    """Runs the rounds for one batch, resending only what may land, and commits once.

    The batch is copied at entry and only the copy is indexed, so a ``deliver`` that clears or
    extends the list it was given changes nothing here. Round one hands ``deliver`` the batch as
    given; each later round a new list of the events still to send, in their original order.
    Each verdict's indices are mapped back to the copy, and an event with a final outcome is never
    offered again.

    **The budget keeps the 1.x total**: ``(s + 1) * (w + 1)`` rounds, ``s`` the sink's and ``w``
    the worker's ``max_retries``, each an exact ``int`` floored at zero or else zero, run as
    ``w + 1`` cycles of ``s + 1``. The orphan path and the adapter pass ``w`` as zero. Before a
    later round the engine waits: a usable hint from the previous verdict, bounded by the sink's
    ceiling; else :func:`cycle_backoff` at a cycle boundary; else the sinks' backoff, restarting
    each cycle. After the wait the decline predicate is asked with the run's longest round, and a
    ``True`` ends the run. The first round is never declined. No lock is held while ``deliver``
    runs; the counters' lock is a leaf taken only by the commit.

    **An ``Exception`` from ``deliver``** fails its round's events and is resent only by an
    idempotent sink whose predicate accepts it; an unreadable verdict or ``UNKNOWN`` is resent
    only by an idempotent sink. A ``BaseException`` propagates and nothing is committed.

    **The commit** is one ``Deferral`` on the counters at the end: ``failed_events`` takes every
    event that ended rejected, unresolved, raised or unknown, and ``dropped_events`` the dropped
    ones only when the run settled, since a failed run's drops belong to its catcher. A settled
    run's loss is announced in one ``_diag.lost`` line naming counts and rounds, after the
    commit; then each distinct exception type that escaped ``deliver`` gets one ``absorbed`` line,
    record first, announce second, by type and never by text.

    Args:
      sink: The sink, read statically for its idempotence and predicate.
      deliver: The callable to run each round.
      counters: The counters the commit writes.
      batch: The events.
      sink_retries: The sink's ``max_retries`` as read statically.
      worker_retries: The worker's ``max_retries``, or zero/``None`` for one cycle.
      stop: The event every wait is cut short by, or ``None`` to sleep.
      decline: Asked before each further round with the run's longest round; True ends the run.

    Returns:
      What the run did.

    Raises:
      BaseException: Whatever ``deliver`` raises that is not an ``Exception``, uncommitted.
    """
    events = _snapshot(batch)
    total_events = len(events)
    if not total_events:
        return _SETTLED_EMPTY
    per_cycle = _count(sink_retries) + 1
    rounds_allowed = per_cycle * (_count(worker_retries) + 1)
    idempotent = _idempotent(sink)
    ceiling = _ceiling(sink)
    fate: list[object] = [Outcome.RETRYABLE] * total_events
    pending = list(range(total_events))
    escaped: list[Exception] = []
    attempts = 0
    longest = 0.0
    hint: float | None = None
    declined = False
    while True:
        if attempts:
            cycle, position = divmod(attempts, per_cycle)
            delay = None if hint is None else _retry.clamp_server_delay(hint, ceiling)
            if delay is None:
                delay = cycle_backoff(cycle - 1) if position == 0 else _backoff(position - 1)
            stop = _wait(delay, stop)
            if decline is not None and decline(longest):
                declined = True
                break
        offered = batch if not attempts else [events[index] for index in pending]
        n = len(pending)
        attempts += 1
        hint = None
        started = time.monotonic()
        try:
            result = deliver(offered)
        except Exception as exc:
            longest = max(longest, time.monotonic() - started)
            if not any(type(seen) is type(exc) for seen in escaped):
                escaped.append(exc)
            predicate = _predicate_of(sink) if idempotent else None
            resend = predicate is not None and is_retryable(predicate, exc)
            for index in pending:
                fate[index] = _RAISED
            if not resend:
                pending = []
        else:
            longest = max(longest, time.monotonic() - started)
            outcomes = outcomes_of(result, n)
            if outcomes is None:
                for index in pending:
                    fate[index] = _UNREAD
                if not idempotent:
                    pending = []
            else:
                hint = _hint_of(result)
                still: list[int] = []
                for index, outcome in zip(pending, outcomes, strict=True):
                    fate[index] = outcome
                    if outcome is Outcome.RETRYABLE or (idempotent and outcome is Outcome.UNKNOWN):
                        still.append(index)
                pending = still
        if not pending or attempts >= rounds_allowed:
            break
    delivery = _tally(fate, attempts, -(-attempts // per_cycle), declined)
    unconfirmed = delivery.rejected + delivery.unresolved + delivery.raised + delivery.unknown
    dropped = 0 if delivery.failed else delivery.dropped
    name = _diag._STR_STR(_diag._STORED_NAME(type(sink)))
    try:
        with Deferral(counters) as ledger:
            ledger.count("failed_events", unconfirmed)
            ledger.count("dropped_events", dropped)
            if not delivery.failed and unconfirmed + dropped:
                ledger.announce(
                    lambda: _diag.lost(
                        "event",
                        unconfirmed + dropped,
                        f"rejected {delivery.rejected}, unresolved {delivery.unresolved}, "
                        f"raised {delivery.raised}, unknown {delivery.unknown}, dropped {dropped} "
                        f"after {delivery.attempts} delivery round(s) to {name}",
                    )
                )
    except Exception as fault:
        _diag.absorbed(
            "counting a delivery", fault, f"{unconfirmed + dropped} event(s) not counted"
        )
    for one in escaped:
        _diag.absorbed("delivering a batch", one, f"its round's events not confirmed by {name}")
    return delivery


def _tally(fate: list[object], attempts: int, cycles: int, declined: bool) -> Delivery:
    """Counts the final fates of one run and applies the run-failed rule.

    Args:
      fate: Each event's final fate, by index in the run's copy of the batch.
      attempts: ``deliver`` calls made.
      cycles: Cycles started.
      declined: Whether the run was declined.

    Returns:
      The run's record.

    Raises:
      None.
    """
    accepted = sum(1 for one in fate if one is Outcome.ACCEPTED)
    rejected = sum(1 for one in fate if one is Outcome.REJECTED)
    unresolved = sum(1 for one in fate if one is Outcome.RETRYABLE)
    raised = sum(1 for one in fate if one is _RAISED)
    unknown = sum(1 for one in fate if one is Outcome.UNKNOWN or one is _UNREAD)
    return Delivery(
        attempts=attempts,
        cycles=cycles,
        accepted=accepted,
        rejected=rejected,
        dropped=sum(1 for one in fate if one is Outcome.DROPPED),
        unresolved=unresolved,
        raised=raised,
        unknown=unknown,
        declined=declined,
        skipped=sum(1 for one in fate if one is Outcome.SKIPPED),
        failed=not accepted and bool(rejected + unresolved + raised) and not unknown,
    )


def run_delivery(
    sink: object,
    batch: list[dict[str, object]],
    *,
    worker_retries: int | None,
    stop: threading.Event | None,
    decline: Callable[[float], bool] | None = None,
) -> Delivery | None:
    """Delivers a batch through the sink's ``deliver`` when it offers one, else returns ``None``.

    The probe runs once, here, so a swap cannot change the sink between it and the run, and the
    run counts into the counters the offer was decided on. ``None`` tells the caller to use
    ``emit``, unchanged.

    Args:
      sink: The sink, of any type.
      batch: The events.
      worker_retries: The worker's ``max_retries``, or ``None`` on the orphan path.
      stop: The event every wait is cut short by.
      decline: The worker's prediction, asked before each further round.

    Returns:
      What the run did, or ``None`` when the sink does not offer ``deliver``.

    Raises:
      BaseException: Whatever ``deliver`` raises that is not an ``Exception``.
    """
    probe = _probe(sink)
    if probe is None:
        return None
    deliver, counters = probe
    return _run(
        sink,
        deliver,
        counters,
        batch,
        sink_retries=_static_value(sink, "max_retries"),
        worker_retries=worker_retries,
        stop=stop,
        decline=decline,
    )


def emit_via_deliver(
    sink: object,
    batch: list[dict[str, object]],
    *,
    deliver: Callable[[list[dict[str, object]]], Verdict] | None = None,
) -> None:
    """Runs the engine inline as an engine sink's ``emit``, keeping the 1.x ``emit`` contract.

    What a sink that defines ``deliver`` makes its ``emit``, so a third-party wrapper calling
    ``inner.emit(batch)``, or a subclass overriding ``emit`` and calling ``super().emit(batch)``,
    sees what 1.x showed it: a normal return on a partial outcome, an unreadable verdict or an
    ``UNKNOWN`` event, the sink's own retries spent first, ``dropped`` counted only when the call
    returns, and a raise when the run fails. An empty batch returns first, before any member is
    checked, since ``emit([])`` never raises. ``deliver`` is the given callable, or the class's
    found by the probe's static lookup **without** the ``emit``-override rule (this is the
    ``emit`` an override reaches through ``super()``), where a ``deliver = None`` is passed over
    and the walk goes on: an opt-out means "do not offer", not "do not have". It runs one cycle
    of the sink's own ``max_retries``, waits on the sink's ``log_foundry_stop_signal`` and is
    never declined; to the worker's prediction it is one attempt, as a 1.x ``emit`` is.

    Args:
      sink: The sink whose ``emit`` this is.
      batch: The events.
      deliver: The ``deliver`` to run, or ``None`` for the static lookup.

    Returns:
      None.

    Raises:
      TypeError: When the sink has no usable ``deliver`` or no ``log_foundry_counters``
        ``SinkCounters``, naming its stored type name and the missing member, so a misspelt
        member is a counted failure rather than events that vanish.
      SinkDeliveryError: When the run failed, naming counts and the sink's type, never a value.
      BaseException: Whatever ``deliver`` raises that is not an ``Exception``.
    """
    events = _snapshot(batch)
    if not events:
        return
    found: object = deliver if callable(deliver) else None
    if deliver is None:
        try:
            found = _bound_deliver(sink, override_rule=False, skip_none=True)
        except Exception:
            found = None
    if found is None:
        raise TypeError(
            f"{_diag._STR_STR(_diag._STORED_NAME(type(sink)))} reaches emit_via_deliver with no "
            "usable deliver"
        )
    counters = _static_value(sink, "log_foundry_counters")
    if type(counters) is not SinkCounters:
        raise TypeError(
            f"{_diag._STR_STR(_diag._STORED_NAME(type(sink)))} reaches emit_via_deliver with no "
            "log_foundry_counters SinkCounters"
        )
    delivery = _run(
        sink,
        cast("Deliver", found),
        counters,
        events,
        sink_retries=_static_value(sink, "max_retries"),
        worker_retries=0,
        stop=stop_signal_of(sink),
        decline=None,
    )
    if delivery.failed:
        raise SinkDeliveryError(
            f"delivered none of {len(events)} event(s): rejected {delivery.rejected}, "
            f"unresolved {delivery.unresolved}, raised {delivery.raised} after "
            f"{delivery.attempts} delivery round(s) to "
            f"{_diag._STR_STR(_diag._STORED_NAME(type(sink)))}"
        )
