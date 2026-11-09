"""TransformSink — reshape or redact each event before forwarding (arch §8, SPEC-006 FR-004)."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING, cast

from log_foundry import _diag, _lifecycle
from log_foundry.sanitize import _coerce_unbounded
from log_foundry.sinks import _engine
from log_foundry.sinks._args import ReadOnlyCount, ReadOnlySeconds, require_callable
from log_foundry.sinks._deferral import Deferral, wrapped_losses
from log_foundry.sinks._wrapper import (
    ForwardedStopSignal,
    copy_budget,
    current_deliver,
    map_forwarded,
    offered_counters,
)
from log_foundry.sinks.base import flush_sink, read_losses
from log_foundry.sinks.verdict import Outcome, Verdict

if TYPE_CHECKING:
    from collections.abc import Callable

    from log_foundry.sinks.base import Sink, SinkLosses

__all__ = ["TransformSink"]


def _check(result: object) -> None:
    """Raises unless ``result`` passes invariant 8's JSON check, in one expression (SPEC-058).

    The JSON half of the observable, not its exact-type clause (SPEC-063): a result that passes is
    delivered as returned, so what a transform adds may hold a builtin subclass, as
    ``docs/invariants.md`` §8 records.

    ``allow_nan=False`` and the UTF-8 encode are what make it the stricter of the two readings:
    a NaN and a lone surrogate both pass a bare ``json.dumps``, and the second is what the SQL
    sinks' drivers reject once it reaches a column (SPEC-055).

    Args:
      result: What the transform returned, or its coercion.

    Returns:
      None.

    Raises:
      Exception: Whatever ``json.dumps`` or the encode raises — ``TypeError``, ``ValueError``,
        ``UnicodeEncodeError``, a ``RecursionError`` on a nest deeper than the stack allows, or a
        ``dict`` subclass's own ``items()`` failure.
    """
    json.dumps(result, allow_nan=False, ensure_ascii=False).encode("utf-8")


def _passes(result: object) -> bool:
    """Reports whether ``result`` passes invariant 8's JSON check, as a verdict rather than a raise.

    The catch is broad on purpose: SPEC-058 FR-001 makes *any* ``Exception`` from the check a
    failure, a ``RecursionError`` and a ``dict`` subclass's own ``items()`` failure included, and
    the failure is converted here to ``False`` rather than swallowed — :func:`_json_safe` acts on
    it by coercing. Answering with a ``bool`` is also what keeps the coercion out of any
    ``except`` block: an exception the coercion or the post-coercion check raises is then not
    chained onto the check's own, so it is the exception FR-002's rule carries and its type is
    the one the wrapper's line names. ``BaseException`` is not caught.

    Args:
      result: What the transform returned.

    Returns:
      True when the check passed.

    Raises:
      None.
    """
    try:
        _check(result)
    except Exception:
        return False
    return True


def _json_safe(result: dict[str, object]) -> dict[str, object]:
    """Returns ``result`` itself when it passes the check, else a new types-only coercion of it.

    Check first, so a valid result is untouched by construction and only one that already failed
    ever meets the coercer. :func:`_passes` answers the first check as a ``bool``, so the
    coercion runs outside any ``except`` block and what it, or the post-coercion check, raises is
    not chained onto the first check's exception. ``truncated`` is set only when the coercion made
    a substitution assembly marks; a coercion is not loss, so nothing is counted and nothing is
    announced here.

    Args:
      result: The mapping the transform returned. It is never mutated.

    Returns:
      ``result`` when it passes, or the coerced mapping.

    Raises:
      Exception: Whatever the coercion raises, or the post-coercion check when the coercion did
        not rescue the result.
    """
    if _passes(result):
        return result
    coerced, marked = _coerce_unbounded(result)
    if marked:
        coerced["truncated"] = True
    _check(coerced)
    return coerced


class TransformSink:
    """A :class:`~log_foundry.sinks.base.Sink` that maps each event before forwarding.

    The function runs on every event on its way out — to redact a field, add host metadata,
    rename keys — and returning ``None`` drops that event. The caller's batch and event dicts
    are never mutated in place: only the function's return values are forwarded, so a transform
    must copy before mutating.

    **What the function returns is checked, and coerced when it fails** (SPEC-058 FR-001). The
    function runs after assembly, so it can put a value into an event that assembly would have
    coerced — ``bytes``, a ``datetime``, a NaN — and invariant 8 promised every sink an event it
    could serialize bare. Each result is checked with that promise's JSON check; one that passes
    is forwarded as the same object, and one that fails is replaced by a new mapping coerced for
    type only, under no configured ceiling and one fixed depth bound. A coercion is not loss:
    nothing is counted and nothing is announced.

    **A function that fails for one event costs only that event** (SPEC-058 FR-002). A raise, a
    result that is neither ``None`` nor a ``dict``, or a result the coercion could not rescue
    drops that event; the rest of the batch is forwarded, and the failures are counted in
    :meth:`losses` and announced once per batch, only once the inner sink's ``emit`` returned.
    When every event of a batch failed, the first exception is re-raised unchanged and nothing is
    counted, because whoever catches it — the worker, the orphan path or a ``MultiSink`` — owns
    the batch.

    It takes **no** transport lock (SPEC-028 FR-002) and **adds no post-close guard**
    (SPEC-032 FR-003): it holds no transport and its ``close()`` only forwards, so both decisions
    belong to the inner sink. A guard here would refuse batches the inner sink would have taken.


    It holds **no** client buffer of its own, but it **forwards** ``flush()`` to what it wraps
    (SPEC-036 FR-002). Holding nothing is not the same as having nothing to do: a wrapper that
    did not forward would leave a buffering child unreachable through ``log_foundry.flush()``
    while looking fine — the SPEC-027 lesson about ``log_foundry_stop_signal``, that a signal
    stopped at a wrapper reaches nothing and moves the defect rather than fixing it.

    **It offers** :meth:`deliver` **exactly when the inner sink does** (SPEC-075 FR-001), decided
    once at construction by SPEC-072's probe. When it offers, ``log_foundry_counters`` is the
    inner sink's own ``SinkCounters`` object, so the engine's totals for a run through this
    wrapper land exactly where the bare inner sink's would, and ``max_retries`` and
    ``max_retry_after`` are the inner sink's, copied statically, so a wrapped engine sink gets
    the rounds it gets alone. It declares no idempotence, so nothing that may have landed is
    resent through it. Within one engine run each event is transformed once: a later round
    forwards the very object the function returned the first time (SPEC-075 FR-002), so the
    inner sink recognises it as the event it already holds notes for. Over an emit-only inner sink, a ``MultiSink``, a third-party wrapper
    or a ``__getattr__`` proxy it offers nothing and the worker calls :meth:`emit`, its 1.x body,
    unchanged. A subclass that overrides ``emit`` is not offered ``deliver`` either, so its
    ``emit`` runs.

    Attributes:
      log_foundry_counters: The inner sink's own counters when it offers ``deliver``, else
        ``None``, fixed at construction.
      log_foundry_stop_signal: The worker's shutdown event, stored where the engine reads it
        statically and forwarded to the inner sink.
      max_retries: The ``max_retries`` the engine reads off the inner sink, copied at
        construction and read-only; ``None`` when this wrapper offers nothing or the engine would
        read the inner sink's as declaring none.
      max_retry_after: The inner sink's ``max_retry_after`` as the engine reads it, copied and
        read-only as ``max_retries`` is; ``None`` where the engine would use its default.
    """

    log_foundry_stop_signal = ForwardedStopSignal()
    max_retries: ReadOnlyCount[int | None] = ReadOnlyCount("it is copied from the wrapped sink")
    max_retry_after = ReadOnlySeconds("it is copied from the wrapped sink")

    def __init__(
        self,
        inner: Sink,
        fn: Callable[[dict[str, object]], dict[str, object] | None],
    ) -> None:
        """Binds the transform to an inner sink and its mapping function.

        Args:
          inner: The sink that receives the transformed events.
          fn: Applied to every event, returning the replacement or ``None`` to drop it.

        Returns:
          None.

        Raises:
          TypeError: If ``fn`` is not callable, refused here rather than raised out of every
            ``emit`` (SPEC-071 FR-005).
        """
        require_callable(fn, "fn", type(self).__name__, optional=False)
        self._inner = inner
        vars(self)["log_foundry_stop_signal"] = None
        self.log_foundry_counters = offered_counters(inner)
        copy_budget(self, inner if self.log_foundry_counters is not None else None)
        self._fn = fn
        self._counter_lock = threading.Lock()
        self._dropped = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Applies the function to each event, forwarding the non-``None`` results (FR-004).

        Each event runs under its own guard (SPEC-058 FR-002), which catches ``Exception`` and
        never ``BaseException``: a raise, a result that is neither ``None`` nor a ``dict``, or a
        result whose coercion could not rescue it drops that event only. A ``None`` declines an
        event, which is the configuration working rather than loss. When nothing is left to
        forward, the inner sink's ``emit`` is not called at all.

        The failures are counted and announced through the deferral helper, so they land only
        if this call returns normally. An inner sink that raises propagates with nothing
        counted: the worker retries the whole batch and the function runs again, so counting now
        would count the same event once per attempt. An event whose function failed transiently
        is therefore lost without a retry when its siblings were delivered — retrying would
        re-deliver the siblings, and a duplicate is worse than one counted loss.

        Args:
          batch: The events to transform.

        Returns:
          None.

        Raises:
          Exception: The first failure's own exception, unchanged and with nothing counted, when
            every event of the batch failed and none was declined — the function's, the
            ``TypeError`` built for a non-``dict`` result, or the coercion's. Otherwise whatever
            the inner sink's ``emit`` raises.
        """
        forwarded: list[dict[str, object]] = []
        declined = False
        failures = 0
        first: Exception | None = None
        with Deferral(self) as pending:
            for event in batch:
                try:
                    result = self._apply(event)
                except Exception as err:
                    failures += 1
                    first = first if first is not None else err
                    continue
                if result is None:
                    declined = True
                else:
                    forwarded.append(result)
            if first is not None and not forwarded and not declined:
                raise first
            if failures:
                kind = type(first).__name__
                pending.count("_dropped", failures)
                pending.announce(
                    lambda: _diag.lost(
                        "event", failures, f"TransformSink, the transform failed with {kind}"
                    )
                )
            if forwarded:
                self._inner.emit(forwarded)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Transforms one round and forwards the results to the inner sink's ``deliver`` (SPEC-075).

        One round, as every shipped engine sink's ``deliver`` is: inside an engine run it is
        part of that run, and a direct call outside one is a one-round run of its own whose
        count commits only if its verdict settles. :meth:`_round` says what the round does.

        Args:
          batch: The events. The list and its events are not mutated.

        Returns:
          One outcome per event of ``batch``.

        Raises:
          TypeError: When this wrapper offers no ``deliver`` (its inner sink offered none at
            construction), naming this class; or when the inner sink stopped offering since,
            naming the inner sink's type.
          Exception: Whatever the inner sink's ``deliver`` raises, unchanged.
        """
        if self.log_foundry_counters is None:
            raise TypeError(
                f"{_diag._STR_STR(_diag._STORED_NAME(type(self)))} offers no deliver: its inner "
                "sink offered none at construction"
            )
        return _engine.one_round(self._round, batch)

    def _round(self, batch: list[dict[str, object]]) -> Verdict:
        """Gives every event of the batch one outcome: its own decision, or the inner sink's.

        The 1.x per-event step runs first, each event under its own guard, through
        :meth:`_round_step`: the function, then the check and the coercion of its result
        (SPEC-058 FR-001). A ``None`` result declines the event, which is ``SKIPPED`` and counted
        nowhere, since declining is the configuration working. An event the function failed on
        is ``SKIPPED`` too, and the failures are registered against this wrapper's ``_dropped``
        through the run context, ``SETTLED``, so they land under its own counter lock only if
        the run settles — SPEC-058's count at SPEC-058's moment. ``SKIPPED`` rather than
        ``DROPPED``, because the engine would commit a ``DROPPED`` into the shared counters, which
        the inner sink's ``losses()`` reports and this wrapper's adds its own count to, so the
        event would be counted twice.

        When the function failed on every event, nothing is forwarded and the batch is
        ``REJECTED`` with the first exception as the run context's refusal marker (SPEC-074), so
        the run fails at once and its catcher owns it, and no sink counter moves — the caller's
        code is not something any sink declared retryable. A call with any failure writes one
        ``absorbed`` line at once, naming this class, the first exception's type and how many
        events were not forwarded, never a value; it is not registered with the run context,
        because a registered line suppresses the engine's own settled line for the whole run.

        The results go to the inner sink's ``deliver``, taken again on every round, as one list
        in batch order, through the run context's ``delegate``, which moves any mark the inner
        round registered into this batch's positions. Its verdict is mapped back to those
        positions: a malformed one turns them ``UNKNOWN``. An exception from it propagates
        unchanged, and so the engine applies SPEC-072's rule to it as it would to the bare inner
        sink's. When nothing is forwarded the inner sink is not called. The engine resends this
        wrapper's original events, and a later round of the same run forwards the result the
        function made for each of them the first time, the same object, rather than running it
        again (:meth:`_round_step`). Nothing here takes a lock, enters a ``Deferral`` or stores
        an attribute — the kept results live in the run context, which the run owns — so
        concurrent calls share nothing (SPEC-028).

        Args:
          batch: The events.

        Returns:
          One outcome per event of ``batch``, with the inner sink's hint.

        Raises:
          TypeError: When the inner sink no longer offers ``deliver`` with the counters this
            wrapper holds, naming its type.
          Exception: Whatever the inner sink's ``deliver`` raises, unchanged.
        """
        deliver = current_deliver(self._inner, self.log_foundry_counters)
        if deliver is None:
            raise TypeError(
                f"{_diag._STR_STR(_diag._STORED_NAME(type(self._inner)))} no longer offers "
                "deliver with the counters its wrapper holds"
            )
        outcomes = [Outcome.SKIPPED] * len(batch)
        forwarded: list[dict[str, object]] = []
        positions: list[int] = []
        declined = False
        failures = 0
        first: Exception | None = None
        context = _engine.run_context()
        held = cast(
            "dict[int, tuple[dict[str, object], dict[str, object]]]",
            context.notes(self).setdefault("transformed", {}),
        )
        for index, event in enumerate(batch):
            try:
                result = self._round_step(event, held)
            except Exception as err:
                failures += 1
                first = first if first is not None else err
                continue
            if result is None:
                declined = True
            else:
                forwarded.append(result)
                positions.append(index)
        if first is not None:
            _diag.absorbed(
                "transforming a batch",
                first,
                f"{_diag._STR_STR(_diag._STORED_NAME(type(self)))} did not forward "
                f"{failures} event(s) its function failed on",
            )
            if not forwarded and not declined:
                context.refuse(first)
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context.count(self, "_dropped", failures, when=_engine.When.SETTLED)
        hint: object = None
        if forwarded:
            hint = map_forwarded(
                context.delegate(deliver, forwarded, positions), positions, outcomes
            )
        return Verdict(outcomes=tuple(outcomes), retry_after=cast("float | None", hint))

    def _round_step(
        self,
        event: dict[str, object],
        held: dict[int, tuple[dict[str, object], dict[str, object]]],
    ) -> dict[str, object] | None:
        """Returns what an event of an engine round forwards: once made, the same object all run.

        The engine resends this wrapper's original events, so without this a later round would
        run the function again and forward a new object for an event already offered. A
        transform that copies before it mutates, as this class asks, then hands the inner sink a
        stranger on every round: its run notes, keyed by event identity, no longer recognise the
        event, so ``SQSSink`` lands a FIFO group out of order and its abandoned line counts one
        attempt where the message made four. So the result of :meth:`_apply` — the step
        :meth:`emit` takes too — is kept for the life of one engine run in the run context's
        notes for this wrapper, keyed by the original event's identity as ``encode_round`` keys
        an encoding, and a later round forwards that very object (SPEC-075 FR-002, Andrew's call
        of 2026-11-09). The entry holds the original event, and a hit must be that object, so an
        identity reused by another event within the run can never take its result. The notes
        end with the run, so the next run of the same event runs the function afresh, and a
        direct :meth:`deliver` outside any run is a run of its own.

        Only a result that is forwarded is kept. A ``None`` declines its event and a raise fails
        it, and either makes the event ``SKIPPED``, which the engine never resends, so no later
        round asks for it. An identity that appears more than once in one batch shares the one
        result, and the inner sink then meets that object as often as the event appeared, as it
        would meet the event itself bare.

        Args:
          event: One event of the wrapper's batch.
          held: This run's kept results: the original event and its result, by the event's
            identity.

        Returns:
          ``None`` when the function declined the event, else what is forwarded.

        Raises:
          Exception: Whatever :meth:`_apply` raises; nothing is kept for it.
        """
        found = held.get(id(event))
        if found is not None and found[0] is event:
            return found[1]
        result = self._apply(event)
        if result is not None:
            held[id(event)] = (event, result)
        return result

    def _apply(self, event: dict[str, object]) -> dict[str, object] | None:
        """Runs the function on one event and makes its result safe to forward (SPEC-058).

        A result that is neither ``None`` nor a ``dict`` is the same failure as a raise, so the
        ``TypeError`` is built here — naming this class and the type returned, never the value —
        and raised into :meth:`emit`'s per-event guard. Raising in a helper rather than inside
        that guard's ``try`` keeps the ``try`` body to the one call that can fail.

        Args:
          event: The event to transform.

        Returns:
          ``None`` when the function declined the event, else its result, checked and, when it
          fails the check, coerced (FR-001).

        Raises:
          TypeError: If the function returned neither ``None`` nor a ``dict``.
          Exception: Whatever the function raises, or the coercion when it cannot rescue the
            result.
        """
        result = self._fn(event)
        if result is None:
            return None
        if not isinstance(result, dict):
            raise TypeError(
                f"TransformSink's function returned {type(result).__name__}, not a dict or None"
            )
        return _json_safe(result)

    def _forward_stop_signal(self, signal: threading.Event | None) -> None:
        """Forwards the stop signal to the inner sink, which is where the waiting happens.

        The worker sets ``log_foundry_stop_signal`` on the configured sink (SPEC-027 FR-002).
        The engine's rounds over this wrapper read it from here, but the inner sink's own
        ``emit`` waits too, on the 1.x path. Without the forward that backoff one level down
        stays uninterruptible — which is the whole defect, moved rather than fixed.
        :class:`~log_foundry.sinks._wrapper.ForwardedStopSignal` has already stored the signal
        where the engine's static read finds it.

        Args:
          signal: The worker's shutdown event, or ``None``.

        Returns:
          None.

        Raises:
          None.
        """
        try:
            self._inner.log_foundry_stop_signal = signal  # type: ignore[attr-defined]
        except Exception as err:
            _diag.absorbed(
                "handing the inner sink its stop signal",
                err,
                f"{type(self._inner).__name__} stays uninterruptible",
            )

    def flush(self) -> None:
        """Forwards the flush to the wrapped sink (SPEC-036 FR-002).

        This wrapper holds nothing itself, but a wrapper that did not forward would leave a
        buffering inner sink unreachable through ``log_foundry.flush()`` while looking fine —
        the SPEC-027 lesson that a signal stopped at a wrapper reaches nothing.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the wrapped sink raises, so the failure reaches the caller as a
            ``FlushResult`` reason rather than being swallowed here.
        """
        flush_sink(self._inner)

    def losses(self) -> SinkLosses | None:
        """Reports the inner sink's losses plus the events this wrapper's function failed on.

        A wrapper that reported nothing would hide the destination it wraps: ``health().sink``
        would read ``None`` for a transform in front of a sink that counts perfectly well
        (SPEC-026 FR-002). Events this sink itself declines to forward are not loss — they are
        the configuration working, the same reason ``NullSink`` reports nothing. Events the
        function *failed* on are: :meth:`emit` counts them once the batch is settled, and they
        are added to ``dropped`` here (SPEC-058 FR-003).

        Args:
          None.

        Returns:
          ``None`` only when the inner sink reports ``None`` and this wrapper has counted
          nothing: FR-003 distinguishes "the sink reports nothing" from "the sink reports no
          loss", and flattening the two would claim a clean bill of health on a sink that never
          gave one. Otherwise the inner sink's losses, an inner ``None`` read as zeros, with this
          wrapper's own count added to ``dropped`` (SPEC-058 FR-003).

        Raises:
          None.
        """
        inner = read_losses(self._inner)
        with self._counter_lock:
            own = self._dropped
        return wrapped_losses(inner, own)

    def close(self) -> None:
        """Closes the inner sink (FR-004).

        Routed through ``_lifecycle.release`` so a sink this process may not release is refused
        here as it is at the lifecycle's own closers (SPEC-042 FR-002). The propagation is
        unchanged: the helper re-raises, and this method's ``Raises:`` is the reason it does.

        Args:
          None.

        Returns:
          None.

        Raises:
          Exception: Whatever the inner sink raises on close.
        """
        _lifecycle.release(self._inner, owner=self)
