"""FilteringSink — drop events by predicate and/or minimum level (arch §8, SPEC-006 FR-003)."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, cast

from log_foundry import _diag, _lifecycle
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

__all__ = ["FilteringSink"]

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


class FilteringSink:
    """A :class:`~log_foundry.sinks.base.Sink` that forwards only events passing a filter.

    This is a static, emit-time filter in front of an inner sink, not the reserved
    tail-sampling ``should_send`` seam (arch §10) — that stays deferred and owns rate policy at
    span-decision time, while this only reshapes an already-built batch on its way to a sink.

    **A predicate that fails for one event costs only that event** (SPEC-058 FR-002). The
    predicate runs before the level floor, as it always has, so a below-floor event whose
    predicate raises is a failure rather than a quiet drop. A failure drops that event, the rest
    of the batch is forwarded, and the failures are counted in :meth:`losses` and announced once
    per batch, only once the inner sink's ``emit`` returned. When every event of a batch failed,
    the first exception is re-raised unchanged and nothing is counted, because whoever catches it
    owns the batch.

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
    the rounds it gets alone. It declares no idempotence, so nothing is resent through it that
    would re-run the predicate on an event that may have landed. Over an emit-only inner sink,
    a ``MultiSink``, a third-party wrapper or a ``__getattr__`` proxy it offers nothing and the
    worker calls :meth:`emit`, its 1.x body, unchanged. A subclass that overrides ``emit`` is not
    offered ``deliver`` either, so its ``emit`` runs.

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
        *,
        predicate: Callable[[dict[str, object]], bool] | None = None,
        min_level: str | None = None,
    ) -> None:
        """Binds the filter to an inner sink and its criteria.

        Args:
          inner: The sink that receives whatever passes.
          predicate: An event-level test, or ``None`` to apply none.
          min_level: The lowest severity to forward, compared case-insensitively, or ``None``.

        Returns:
          None.

        Raises:
          TypeError: If ``predicate`` is neither callable nor ``None``, refused here rather than
            raised out of every ``emit`` (SPEC-071 FR-005).
          ValueError: If the minimum level is not one of the five standard names.
        """
        require_callable(predicate, "predicate", type(self).__name__)
        self._inner = inner
        vars(self)["log_foundry_stop_signal"] = None
        self.log_foundry_counters = offered_counters(inner)
        copy_budget(self, inner if self.log_foundry_counters is not None else None)
        self._predicate = predicate
        self._min_rank: int | None = None
        self._counter_lock = threading.Lock()
        self._dropped = 0
        if min_level is not None:
            rank = _LEVELS.get(min_level.upper())
            if rank is None:
                raise ValueError(
                    f"unknown min_level: {min_level!r} (expected one of {list(_LEVELS)})"
                )
            self._min_rank = rank

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Forwards the events that clear both the predicate and the level floor, in order.

        When nothing passes, the inner sink's ``emit`` is not called at all (FR-003). Each event
        is tested under its own guard (SPEC-058 FR-002), which catches ``Exception`` and never
        ``BaseException``, so a predicate that raises for one event drops that event only. A
        ``False`` predicate or the level floor declines an event, which is not loss.

        The failures are counted and announced through the deferral helper, so they land only
        if this call returns normally: an inner sink that raises propagates with nothing counted,
        because the worker retries the whole batch and the predicate runs again.

        Args:
          batch: The events to filter.

        Returns:
          None.

        Raises:
          Exception: The predicate's first exception, unchanged and with nothing counted, when
            every event of the batch failed and none was declined. Otherwise whatever the inner
            sink's ``emit`` raises.
        """
        kept: list[dict[str, object]] = []
        declined = False
        failures = 0
        first: Exception | None = None
        with Deferral(self) as pending:
            for event in batch:
                try:
                    passes = self._passes(event)
                except Exception as err:
                    failures += 1
                    first = first if first is not None else err
                    continue
                if passes:
                    kept.append(event)
                else:
                    declined = True
            if first is not None and not kept and not declined:
                raise first
            if failures:
                kind = type(first).__name__
                pending.count("_dropped", failures)
                pending.announce(
                    lambda: _diag.lost(
                        "event", failures, f"FilteringSink, the predicate raised {kind}"
                    )
                )
            if kept:
                self._inner.emit(kept)

    def deliver(self, batch: list[dict[str, object]]) -> Verdict:
        """Filters one round and forwards the rest to the inner sink's ``deliver`` (SPEC-075).

        One round, as every shipped engine sink's ``deliver`` is: inside an engine run it is
        part of that run, and a direct call outside one is a one-round run of its own whose
        count commits only if its verdict settles. :meth:`_round` says what the round does.

        Args:
          batch: The events. The list is not mutated.

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
        :meth:`_round_step`: the predicate, then the level floor, asked once per event per run — an
        event forwarded on an earlier round of the run is forwarded again without asking. A declined
        event is ``SKIPPED`` and counted by this wrapper nowhere, since declining is the
        configuration working, and by the engine nowhere except in a round the engine fails as
        raising — the inner ``deliver`` raised, or its marks did not fit — where SPEC-072's
        raising-round upper bound counts every event of the round in the shared ``failed_events``,
        this one included. An event the predicate failed on is ``SKIPPED`` too, and the failures are
        registered against this wrapper's ``_dropped`` through the run context, ``SETTLED``, so they
        land under its own counter lock only if the run settles — SPEC-058's count at SPEC-058's
        moment. ``SKIPPED`` rather than ``DROPPED``, because the engine would commit a ``DROPPED``
        into the shared counters, which the inner sink's ``losses()`` reports and this wrapper's
        adds its own count to, so the event would be counted twice.

        When the predicate failed on every event, nothing is forwarded and the batch is
        ``REJECTED`` with the first exception as the run context's refusal marker (SPEC-074), so
        the run fails at once and its catcher owns it, and no sink counter moves — the caller's
        code is not something any sink declared retryable. A call with any failure writes one
        ``absorbed`` line at once, naming this class, the first exception's type and how many
        events were not forwarded, never a value. It is not registered with the run context:
        since PR 319 a round that registers a line owns the announcement of every loss its
        verdict decided, so a registered line here would take the engine's settled line away
        from this round's losses, a third-party inner sink's among them, and leave them
        unannounced.

        The kept events go to the inner sink's ``deliver``, taken again on every round, as one
        list in batch order, through the run context's ``delegate``, which moves any mark the
        inner round registered into this batch's positions. Its verdict is mapped back to those
        positions: a malformed one turns them ``UNKNOWN``, and ``delegate`` has discarded what the
        inner call registered, as the engine discards a bare sink's set-aside round (PR 319),
        keeping this wrapper's own registrations. An exception from it propagates
        unchanged, and so the engine applies SPEC-072's rule to it as it would to the bare inner
        sink's. When nothing is kept the inner sink is not called. Nothing here takes a lock,
        enters a ``Deferral`` or stores an attribute — the remembered decisions live in the run
        context, which the run owns — so concurrent calls share nothing (SPEC-028).

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
        kept: list[dict[str, object]] = []
        positions: list[int] = []
        declined = False
        failures = 0
        first: Exception | None = None
        context = _engine.run_context()
        held = cast("dict[int, dict[str, object]]", context.notes(self).setdefault("forwarded", {}))
        for index, event in enumerate(batch):
            try:
                passes = self._round_step(event, held)
            except Exception as err:
                failures += 1
                first = first if first is not None else err
                continue
            if passes:
                kept.append(event)
                positions.append(index)
            else:
                declined = True
        if first is not None:
            _diag.absorbed(
                "filtering a batch",
                first,
                f"{_diag._STR_STR(_diag._STORED_NAME(type(self)))} did not forward "
                f"{failures} event(s) its predicate failed on",
            )
            if not kept and not declined:
                context.refuse(first)
                return Verdict.uniform(Outcome.REJECTED, len(batch))
            context.count(self, "_dropped", failures, when=_engine.When.SETTLED)
        hint: object = None
        if kept:
            hint = map_forwarded(context.delegate(deliver, kept, positions), positions, outcomes)
        return Verdict(outcomes=tuple(outcomes), retry_after=cast("float | None", hint))

    def _round_step(self, event: dict[str, object], held: dict[int, dict[str, object]]) -> bool:
        """Decides whether an event of an engine round is forwarded: once forwarded, all run.

        The engine resends this wrapper's original events, so without this a later round would
        ask the predicate again about an event it already forwarded, and a predicate that is not
        a pure function of the event — one that drops duplicates, limits a rate or samples —
        could now decline it: ``SKIPPED``, which the engine never resends nor counts, so
        an event the inner sink answered ``RETRYABLE`` would be lost silently, where the worker's
        1.x retry of :meth:`emit` sent it again. A predicate raising on the resend would cost a
        forwarded event too. So an event forwarded once is remembered for the life of one engine
        run, in the run context's notes for this wrapper, keyed by the event's identity as
        ``TransformSink`` keeps its results (SPEC-075 FR-002, from the frame-2 review), and a
        later round forwards it without asking again. The entry holds the event, and a hit must
        be that object, so an identity reused by another event within the run never takes its
        decision. The level floor is asked through the same path; it is pure, so remembering it
        changes nothing. The notes end with the run, so the next run asks afresh, and a direct
        :meth:`deliver` outside any run is a run of its own.

        Only a forwarded event is remembered. A declined or failed one is ``SKIPPED``, which the
        engine never resends, so no later round asks about it. The same event object twice in one
        batch shares one decision, where :meth:`emit` asks per occurrence — the one exception to
        the ``deliver``/``emit`` twin pair; a batch the library builds never repeats an identity.

        Args:
          event: One event of the wrapper's batch.
          held: This run's forwarded events, by identity.

        Returns:
          True when the event is forwarded.

        Raises:
          Exception: The predicate's own exception, from :meth:`_passes`; nothing is kept.
        """
        if held.get(id(event)) is event:
            return True
        passes = self._passes(event)
        if passes:
            held[id(event)] = event
        return passes

    def _passes(self, event: dict[str, object]) -> bool:
        """Reports whether an event clears the predicate and the minimum level.

        A known level below the floor is dropped, while an unknown or missing level fails open
        and is forwarded.

        The predicate runs through ``_engine.outside_runs``, so it sees no engine run on the
        ``deliver`` path, as on the ``emit`` path: a ``deliver`` it calls on another sink is a
        detached run of its own, never folded into this wrapper's round (SPEC-075 FR-002).

        Args:
          event: The event to test.

        Returns:
          True when the event should be forwarded.

        Raises:
          Exception: The predicate's own exception, propagated so :meth:`emit` can count it as
            that one event's failure (SPEC-058 FR-002).
        """
        if self._predicate is not None and not _engine.outside_runs(self._predicate, event):
            return False
        if self._min_rank is not None:
            level = event.get("level")
            rank = _LEVELS.get(level.upper()) if isinstance(level, str) else None
            if rank is not None and rank < self._min_rank:
                return False
        return True

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
        """Reports the inner sink's losses plus the events this wrapper's predicate failed on.

        A wrapper that reported nothing would hide the destination it wraps: ``health().sink``
        would read ``None`` for a filter in front of a sink that counts perfectly well
        (SPEC-026 FR-002). Events this sink itself declines to forward are not loss — they are
        the configuration working, the same reason ``NullSink`` reports nothing. Events the
        predicate *failed* on are: :meth:`emit` counts them once the batch is settled, and they
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
        """Closes the inner sink (FR-003).

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
