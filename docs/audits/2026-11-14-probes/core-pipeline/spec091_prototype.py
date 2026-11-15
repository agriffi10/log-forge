"""SPEC-091, written on 2026-11-15 against a2cd4b36: the prototype the spec's mechanism and its Evidence come from.
Not a probe and not the build. It edits a scratch copy of src/log_foundry as the spec describes: an open-span registry written
without a lock, a worker built before the first span opens, a drain-thread sweep on its own clock that takes each open span's
buffer in place past its held span.start, hands each span's take in as its own submission and makes the time trigger due,
debits a take whose hand-in raised to in_span_lost, and writes every stderr line only after _sweep_lock is released (submit
split into Worker._enqueue and Worker._announce); one sweep at the start of the exit drain; a flush() that also sweeps every
registered span; continue_trace() under the sweep lock; a close that drops its span from the registry before it appends
span.end and submits no empty batch; a backfill over a snapshot; and a forked child that forgets the registry, from a handler
registered in _lifecycle ahead of the worker's rebuild. It keeps SPEC-088's ledger out: that spec is not built at a2cd4b36.
Never run it on a checkout's own src/.
Usage: git archive a2cd4b36 src | tar -x -C <dir>; python spec091_prototype.py <dir>/src/log_foundry;
       then run a probe or the suite with PYTHONPATH=<dir>/src.
"""

import os
import sys

root = sys.argv[1]


def patch(name: str, pairs: list[tuple[str, str]]) -> None:
    path = os.path.join(root, name)
    text = open(path).read()
    for old, new in pairs:
        assert text.count(old) == 1, (name, old[:80])
        text = text.replace(old, new)
    open(path, "w").write(text)


patch(
    "_lifecycle.py",
    [
        (
            "                    deferred = bool(_state._stops_running) and _state._closes.defer_hand(sink)\n                    if not deferred and not handing_here(sink):\n                        token = _open_hand()\n                    worker = Worker(sink, offer_signal=False)\n                    _state._worker = worker\n                    if _state._shutdown_running > 0:\n                        _state._late_worker = worker\n",
            "                    deferred = bool(_state._stops_running) and _state._closes.defer_hand(sink)\n                    if not deferred and not handing_here(sink):\n                        token = _open_hand()\n                    from log_foundry.decorator import _sweep_registered\n\n                    worker = Worker(sink, offer_signal=False, sweep=_sweep_registered)\n                    _state._worker = worker\n                    if _state._shutdown_running > 0:\n                        _state._late_worker = worker\n",
        ),
        (
            '        _diag.absorbed("sweeping open spans for a flush", exc, "buffered events were not swept")\n        swept = False\n    worker = _state.worker_exists()\n    drained: FlushResult = FlushResult(ok=True)\n    thread_died = False\n    if worker is not None:\n',
            '        _diag.absorbed("sweeping open spans for a flush", exc, "buffered events were not swept")\n        swept = False\n    worker = _state.worker_exists()\n    if worker is not None and swept:\n        from log_foundry.decorator import _sweep_registered\n\n        try:\n            _, pending, faults = _sweep_registered(\n                lambda events: worker._enqueue(events, count_retired=True)\n            )\n        except Exception as exc:\n            _diag.absorbed("sweeping open spans for a flush", exc, "buffered events were not swept")\n            swept = False\n        else:\n            for result in pending:\n                worker._announce(result)\n            for fault in faults:\n                _diag.absorbed("handing in an open span\'s events", fault, "the events taken were lost")\n            if faults:\n                swept = False\n    drained: FlushResult = FlushResult(ok=True)\n    thread_died = False\n    if worker is not None:\n',
        ),
        (
            "_fork.register_child_handler(_reclaim_after_fork)\n_fork.register_child_handler(_clear_after_fork)\n_fork.register_child_handler(_adopt_factory_after_fork)\n_fork.register_child_handler(_rebuild_worker_after_fork)\n",
            '_fork.register_child_handler(_reclaim_after_fork)\n_fork.register_child_handler(_clear_after_fork)\n_fork.register_child_handler(_adopt_factory_after_fork)\n\n\ndef _forget_open_spans_after_fork() -> None:\n    """PROTOTYPE SPEC-091: the child sweeps none of the spans it inherited."""\n    from log_foundry.decorator import _forget_open_spans_in_child\n\n    _forget_open_spans_in_child()\n\n\n_fork.register_child_handler(_forget_open_spans_after_fork)\n_fork.register_child_handler(_rebuild_worker_after_fork)\n',
        ),
    ],
)
patch(
    "decorator.py",
    [
        (
            '"""\n_orphan_lost = 0\n_in_span_lost = 0\n\nF = TypeVar("F", bound=Callable[..., Any])\n\n\n',
            '"""\n_orphan_lost = 0\n_in_span_lost = 0\n_open_spans: dict[int, Span] = {}\n"""PROTOTYPE SPEC-091: every open span, keyed by id, written without a lock (one dict op)."""\n\n\ndef _forget_open_spans_in_child() -> None:\n    """PROTOTYPE: a forked child sweeps none of the spans it inherited."""\n    _open_spans.clear()\n\n\n\ndef _sweep_registered(\n    hand_in: Callable[[list[dict[str, object]]], tuple[int, int]],\n) -> tuple[int, list[tuple[int, int]], list[Exception]]:\n    """PROTOTYPE: hands in each open span\'s buffer but its held span.start, one span at a time."""\n    taken = 0\n    pending: list[tuple[int, int]] = []\n    faults: list[Exception] = []\n    with _sweep_lock:\n        for span in list(_open_spans.copy().values()):\n            events = span.events\n            n = len(events)\n            if _open_spans.get(id(span)) is not span or span.closed:\n                continue\n            first = span.held\n            if n <= first:\n                continue\n            batch = events[first:n]\n            del events[first:n]\n            try:\n                span.swept = True\n                pending.append(hand_in(batch))\n                taken += len(batch)\n            except Exception as exc:\n                _note_in_span_loss(len(batch))\n                faults.append(exc)\n    return taken, pending, faults\n\nF = TypeVar("F", bound=Callable[..., Any])\n\n\n',
        ),
        (
            "        defaults=defaults or {},\n    )\n    span.events.append(start_event(span))\n    return span\n\n\n",
            "        defaults=defaults or {},\n    )\n    span.events.append(start_event(span))\n    span.held = 1\n    return span\n\n\n",
        ),
        (
            '        _diag.rejected("parent_span_id given with no trace_id to join", parent_span_id)\n        announced = True\n\n    if adopted is not None and _current_span_was_swept():\n        _diag.rejected(\n            "the current span has already been flushed; trace context refused",\n            traceparent if traceparent is not None else str(trace_id),\n',
            '        _diag.rejected("parent_span_id given with no trace_id to join", parent_span_id)\n        announced = True\n\n    refused = False\n    if adopted is not None:\n        with _sweep_lock:\n            if _current_span_was_swept():\n                refused = True\n            else:\n                context.set_adopted_context(*adopted)\n                _reparent_current_span(*adopted)\n    if refused:\n        _diag.rejected(\n            "the current span has already been flushed; trace context refused",\n            traceparent if traceparent is not None else str(trace_id),\n',
        ),
        (
            "        adopted = None\n        announced = True\n\n    if adopted is not None:\n        context.set_adopted_context(*adopted)\n        _reparent_current_span(*adopted)\n\n    if baggage is not None:\n        parsed_baggage = context.parse_baggage_header(baggage)\n        if parsed_baggage is None:\n",
            "        adopted = None\n        announced = True\n\n    if baggage is not None:\n        parsed_baggage = context.parse_baggage_header(baggage)\n        if parsed_baggage is None:\n",
        ),
        (
            '        flush is the call most likely to be made in a ``finally``.\n    """\n    worker: Worker | None = None\n    while True:\n        with _sweep_lock:\n            needs_worker = False\n',
            '        flush is the call most likely to be made in a ``finally``.\n    """\n    worker: Worker | None = None\n    pending: list[tuple[int, int]] = []\n    while True:\n        with _sweep_lock:\n            needs_worker = False\n',
        ),
        (
            "                    break\n                backfill_baggage(span, context._live_baggage())\n                span.swept = True\n                buffered, span.events = span.events, []\n                worker.submit(buffered)\n            if not needs_worker:\n                return\n        worker = _lifecycle._get_worker()\n\n\n\n",
            "                    break\n                backfill_baggage(span, context._live_baggage())\n                span.swept = True\n                span.held = 0\n                buffered, span.events = span.events, []\n                pending.append(worker._enqueue(buffered, count_retired=True))\n            if not needs_worker:\n                break\n        worker = _lifecycle._get_worker()\n    if worker is not None:\n        for result in pending:\n            worker._announce(result)\n\n\n\n",
        ),
        (
            "    worker = _lifecycle._get_worker()\n    with _sweep_lock:\n        events, span.events = span.events, []\n    worker.submit(events)\n\n\ntype _SpanScope = tuple[\n",
            "    worker = _lifecycle._get_worker()\n    with _sweep_lock:\n        events, span.events = span.events, []\n    if events:\n        worker.submit(events)\n\n\ntype _SpanScope = tuple[\n",
        ),
        (
            '    span: Span | None = None\n    token: contextvars.Token[tuple[Span, ...]] | None = None\n    scope: contextvars.Token[dict[str, object]] | None = None\n    try:\n        if context.current_span() is None:\n            scope = context.push_baggage_scope()\n        span = _open_span(name, defaults)\n        token = context.push_span(span)\n    except Exception as exc:\n        _diag.absorbed(\n            "opening a span",\n',
            '    span: Span | None = None\n    token: contextvars.Token[tuple[Span, ...]] | None = None\n    scope: contextvars.Token[dict[str, object]] | None = None\n    if _lifecycle._state._worker is None:\n        try:\n            _lifecycle._get_worker()\n        except Exception as exc:\n            _diag.absorbed("starting the worker for a span", exc, "its events wait for its close")\n    try:\n        if context.current_span() is None:\n            scope = context.push_baggage_scope()\n        span = _open_span(name, defaults)\n        token = context.push_span(span)\n        _open_spans[id(span)] = span\n    except Exception as exc:\n        _diag.absorbed(\n            "opening a span",\n',
        ),
        (
            '      Exception: Whatever building the end event or flushing raises; :func:`_end` is the\n        guard.\n    """\n    span.events.append(end_event(span, status, exc))\n    backfill_baggage(span, context._live_baggage())\n    span.closed = True\n',
            '      Exception: Whatever building the end event or flushing raises; :func:`_end` is the\n        guard.\n    """\n    _open_spans.pop(id(span), None)\n    span.events.append(end_event(span, status, exc))\n    backfill_baggage(span, context._live_baggage())\n    span.closed = True\n',
        ),
    ],
)
patch(
    "model.py",
    [
        (
            "    events: list[dict[str, object]] = field(default_factory=list)\n    closed: bool = False\n    swept: bool = False\n\n\ndef _iso_now() -> str:\n",
            "    events: list[dict[str, object]] = field(default_factory=list)\n    closed: bool = False\n    swept: bool = False\n    held: int = 0\n\n\ndef _iso_now() -> str:\n",
        ),
        (
            '    if not baggage:\n        return\n    safe, clipped = sanitize_fields(baggage, cfg=_live_config())\n    for event in span.events:\n        if event.get("message") in (_START_MESSAGE, _END_MESSAGE):\n            fields = event.get("fields")\n            if isinstance(fields, dict):\n',
            '    if not baggage:\n        return\n    safe, clipped = sanitize_fields(baggage, cfg=_live_config())\n    for event in list(span.events):\n        if event.get("message") in (_START_MESSAGE, _END_MESSAGE):\n            fields = event.get("fields")\n            if isinstance(fields, dict):\n',
        ),
    ],
)
patch(
    "worker.py",
    [
        (
            'from log_foundry.sinks._retry import floor_count\n\nif TYPE_CHECKING:\n    from log_foundry.sinks.base import Sink, SinkLosses\n\n__all__ = ["Health", "Worker"]\n',
            'from log_foundry.sinks._retry import floor_count\n\nif TYPE_CHECKING:\n    from collections.abc import Callable\n\n    from log_foundry.sinks.base import Sink, SinkLosses\n\n__all__ = ["Health", "Worker"]\n',
        ),
        (
            '        max_queue: int = 10_000,\n        max_retries: int = 3,\n        offer_signal: bool = True,\n    ) -> None:\n        """Starts the drain thread and, unless told otherwise, offers the sink its stop signal.\n\n',
            '        max_queue: int = 10_000,\n        max_retries: int = 3,\n        offer_signal: bool = True,\n        sweep: Callable[[Callable[[list[dict[str, object]]], None]], int] | None = None,\n    ) -> None:\n        """Starts the drain thread and, unless told otherwise, offers the sink its stop signal.\n\n',
        ),
        (
            '        self._taken_markers: list[_FlushMarker] = []\n        self._held: list[Sink] = [sink]\n        self._lock = threading.Lock()\n        if offer_signal:\n            _lifecycle.offer_stop_signal(sink, self._stop)\n        self._thread = threading.Thread(target=self._run, name="log-foundry-worker", daemon=True)\n',
            '        self._taken_markers: list[_FlushMarker] = []\n        self._held: list[Sink] = [sink]\n        self._lock = threading.Lock()\n        self._sweep = sweep\n        if offer_signal:\n            _lifecycle.offer_stop_signal(sink, self._stop)\n        self._thread = threading.Thread(target=self._run, name="log-foundry-worker", daemon=True)\n',
        ),
        (
            '        Raises:\n          None.\n        """\n        retired = _lifecycle_state.retirements > self._epoch\n        if retired:\n            self._count_undeliverable()\n        try:\n            self._queue.put_nowait(events)\n        except queue.Full:\n            with self._lock:\n                self.dropped += 1\n                total = self.dropped\n            if total == 1 or total % _diag.WARN_EVERY == 0:\n                _diag.lost("submission", total, "log queue full; count is cumulative")\n            return\n        if not retired and _lifecycle_state.retirements > self._epoch:\n            self._count_undeliverable()\n\n    def _count_undeliverable(self) -> None:\n        """Counts a post-shutdown submission and warns on the same throttle as overflow.\n\n        The two conditions warrant the same treatment for the same reason (FR-002): a caller\n',
            '        Raises:\n          None.\n        """\n        self._announce(self._enqueue(events, count_retired=True))\n\n    def _enqueue(self, events: list[dict[str, object]], *, count_retired: bool) -> tuple[int, int]:\n        """PROTOTYPE SPEC-091: submit\'s counting half, which writes nothing to stderr."""\n        overflowed = undeliverable = 0\n        retired = count_retired and _lifecycle_state.retirements > self._epoch\n        if retired:\n            undeliverable = self._count_undeliverable()\n        try:\n            self._queue.put_nowait(events)\n        except queue.Full:\n            with self._lock:\n                self.dropped += 1\n                overflowed = self.dropped\n            return overflowed, undeliverable\n        if count_retired and not retired and _lifecycle_state.retirements > self._epoch:\n            undeliverable = self._count_undeliverable()\n        return overflowed, undeliverable\n\n    def _announce(self, pending: tuple[int, int]) -> None:\n        """PROTOTYPE SPEC-091: submit\'s writing half, run with no library lock held."""\n        overflowed, undeliverable = pending\n        if overflowed and (overflowed == 1 or overflowed % _diag.WARN_EVERY == 0):\n            _diag.lost("submission", overflowed, "log queue full; count is cumulative")\n        if undeliverable and (undeliverable == 1 or undeliverable % _diag.WARN_EVERY == 0):\n            _diag.lost(\n                "submission",\n                undeliverable,\n                "logged after shutdown(), which is terminal; nothing will drain these. Use "\n                "flush() in a process that logs again. Count is cumulative",\n            )\n\n    def _count_undeliverable(self) -> int:\n        """Counts a post-shutdown submission and warns on the same throttle as overflow.\n\n        The two conditions warrant the same treatment for the same reason (FR-002): a caller\n',
        ),
        (
            '        """\n        with self._lock:\n            self.submitted_after_shutdown += 1\n            total = self.submitted_after_shutdown\n        if total == 1 or total % _diag.WARN_EVERY == 0:\n            _diag.lost(\n                "submission",\n                total,\n                "logged after shutdown(), which is terminal; nothing will drain these. Use "\n                "flush() in a process that logs again. Count is cumulative",\n            )\n\n    @property\n    def draining(self) -> bool:\n',
            '        """\n        with self._lock:\n            self.submitted_after_shutdown += 1\n            return self.submitted_after_shutdown\n\n    @property\n    def draining(self) -> bool:\n',
        ),
        (
            '        Raises:\n          Exception: Whatever the queue or a final drain raises; :meth:`_run` is the guard.\n        """\n        last_flush = time.monotonic()\n        while not self._stop.is_set():\n            timeout = max(0.0, self.flush_interval - (time.monotonic() - last_flush))\n            try:\n                item = self._queue.get(timeout=timeout)\n            except queue.Empty:\n',
            '        Raises:\n          Exception: Whatever the queue or a final drain raises; :meth:`_run` is the guard.\n        """\n        last_flush = last_sweep = time.monotonic()\n        while not self._stop.is_set():\n            now = time.monotonic()\n            if self._sweep is not None and now - last_sweep >= self.flush_interval:\n                last_sweep = now\n                if self._run_sweep():\n                    last_flush = now - self.flush_interval\n            now = time.monotonic()\n            timeout = self.flush_interval - (now - last_flush)\n            if self._sweep is not None:\n                timeout = min(timeout, self.flush_interval - (now - last_sweep))\n            timeout = max(0.0, timeout)\n            try:\n                item = self._queue.get(timeout=timeout)\n            except queue.Empty:\n',
        ),
        (
            '                last_flush = now\n        self._final_drain(pending)\n\n    def _emit_pending(self, pending: list[list[dict[str, object]]]) -> None:\n        """Emits the pending event-lists if there are any, then clears them.\n\n',
            '                last_flush = now\n        self._final_drain(pending)\n\n    def _run_sweep(self) -> int:\n        """PROTOTYPE: sweeps open spans into this queue from the drain thread, absorbing a fault."""\n        sweep = self._sweep\n        if sweep is None:\n            return 0\n        try:\n            taken, pending, faults = sweep(self._hand_in)\n        except Exception as exc:\n            _diag.absorbed("sweeping open spans", exc, "events not yet taken stay on their spans")\n            return 0\n        for result in pending:\n            self._announce(result)\n        for fault in faults:\n            _diag.absorbed("handing in an open span\'s events", fault, "the events taken were lost")\n        return taken\n\n    def _hand_in(self, events: list[dict[str, object]]) -> tuple[int, int]:\n        """PROTOTYPE: the drain thread\'s own enqueue, which its own final drain always carries."""\n        return self._enqueue(events, count_retired=False)\n\n    def _emit_pending(self, pending: list[list[dict[str, object]]]) -> None:\n        """Emits the pending event-lists if there are any, then clears them.\n\n',
        ),
        (
            '          BaseException: Whatever the final emit raises; :meth:`_run` is the guard.\n        """\n        markers: list[_FlushMarker] = []\n        while True:\n            try:\n                item = self._queue.get_nowait()\n',
            '          BaseException: Whatever the final emit raises; :meth:`_run` is the guard.\n        """\n        markers: list[_FlushMarker] = []\n        if self._sweep is not None:\n            self._run_sweep()\n        while True:\n            try:\n                item = self._queue.get_nowait()\n',
        ),
    ],
)
