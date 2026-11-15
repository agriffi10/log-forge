"""SPEC-090, written on 2026-11-15 against 3e27a0a6: the prototype of SPEC-090's rule that the spec's "rule" numbers come from.
Not a probe and not the build. It rewrites a scratch copy of worker.py as the spec describes: the size target and its cap
(LF_CAP, default 1,000), the carry of a submission that would cross the target, the swap generation, the exit drain through the
same method with its later batches declined once one is predicted to end past the deadline, the exit tally lines, and an
expiry line whose in-flight figure counts every event taken and not finished. It does not reset the target in a forked child.
Never run it on a checkout's own src/.
Usage: git archive 3e27a0a6 src | tar -x -C <dir>; python spec090_prototype.py <dir>/src/log_foundry/worker.py;
       then run a probe with PYTHONPATH=<dir>/src. LF_TRACE=1 prints each emit and the target it sets.
"""

import sys

p = sys.argv[1]
s = open(p).read()


def rep(a, b):
    global s
    assert s.count(a) == 1, a[:80]
    s = s.replace(a, b)


rep(
    "_JOIN_MARGIN: float = 0.1\n",
    "_JOIN_MARGIN: float = 0.1\nimport os as _os\n_BACKLOG_BATCH_CAP: int = int(_os.environ.get('LF_CAP', '1000'))\n",
)

# generation bump at the reassign
rep(
    "            self.sink = new_sink\n            self._held = [held for held in self._held if held is not new_sink] + [new_sink]\n",
    "            self.sink = new_sink\n            self._sink_gen = getattr(self, '_sink_gen', 0) + 1\n            self._held = [held for held in self._held if held is not new_sink] + [new_sink]\n",
)

# expiry line: in-flight is everything taken and not finished; tally written first
rep(
    "            in_flight = self._in_flight\n",
    "            in_flight = getattr(self, '_taken', 0)\n            self._write_exit_tally()\n",
)
rep(
    'detail += f"; {in_flight} event(s) in flight, counted when the emit returns"',
    'detail += f"; {in_flight} event(s) in flight"',
)

# steady drain
rep(
    """            if item is not None:
                pending.append(cast("list[dict[str, object]]", item))
            now = time.monotonic()
            if len(pending) >= self.batch_size or now - last_flush >= self.flush_interval:
                self._emit_pending(pending)
                last_flush = now
        self._final_drain(pending)""",
    """            if item is not None:
                self._take(pending, cast("list[dict[str, object]]", item))
            now = time.monotonic()
            if self._complete(pending, not self._queue.empty()) or now - last_flush >= self.flush_interval:
                self._emit_pending(pending)
                last_flush = now
        self._final_drain(pending)

    def _target_now(self):
        if getattr(self, "_measured_gen", -1) != getattr(self, "_sink_gen", 0):
            return 0.0
        return getattr(self, "_target", 0.0)

    def _take(self, pending, item):
        if not pending:
            self._pending_events = 0
        self._taken = getattr(self, "_taken", 0) + len(item)
        if len(pending) >= self.batch_size and self._pending_events + len(item) > self._target_now():
            self._emit_pending(pending)
            self._pending_events = 0
        pending.append(item)
        self._pending_events += len(item)

    def _complete(self, pending, waiting):
        if len(pending) < self.batch_size:
            return False
        return (not waiting) or self._pending_events >= self._target_now()

    def _write_exit_tally(self):
        with self._lock:
            ab = getattr(self, "_tally_ab", (0, 0)); de = getattr(self, "_tally_de", (0, 0))
            self._tally_ab = (0, 0); self._tally_de = (0, 0)
        if ab[1]:
            _diag.lost("event", ab[0], f"{ab[1]} more exit batch(es) abandoned")
        if de[1]:
            _diag.lost("event", de[0], f"{de[1]} exit batch(es) declined unattempted; each was predicted to end after the shutdown() deadline")""",
)

# _emit_pending: exit decline, measurement
rep(
    """        if pending:
            self._emit(pending)
            pending.clear()""",
    """        if pending:
            size = sum(len(x) for x in pending)
            gen = getattr(self, "_sink_gen", 0)
            exiting = self._exit_deadline is not None
            if exiting and getattr(self, "_exit_batches", 0) > 0 and (
                getattr(self, "_exit_declining", False)
                or self._retry_declined(getattr(self, "_exit_longest", 0.0))
            ):
                self._exit_declining = True
                with self._lock:
                    self.failed_batches += 1
                    n, k = getattr(self, "_tally_de", (0, 0))
                    self._tally_de = (n + size, k + 1)
                self._taken -= size
                pending.clear()
                self._pending_events = 0
                return
            if exiting:
                self._exit_batches = getattr(self, "_exit_batches", 0) + 1
            before = self.failed_batches
            started = time.monotonic()
            self._emit(pending)
            took = time.monotonic() - started
            self._taken -= size
            pending.clear()
            self._pending_events = 0
            if self.failed_batches != before or gen != getattr(self, "_sink_gen", 0) or size == 0:
                self._target = 0.0
            elif took <= 0:
                self._target = float(_BACKLOG_BATCH_CAP)
            else:
                self._target = min(float(_BACKLOG_BATCH_CAP), size / took * self.flush_interval / 2)
            self._measured_gen = gen
            if _os.environ.get("LF_TRACE"):
                print(f"emit {size} in {took:.3f}s -> target {self._target:.0f}", file=__import__('sys').__stdout__)""",
)

# exit drain
rep(
    """        markers: list[_FlushMarker] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, _FlushMarker):
                markers.append(item)
                self._take_marker(item)
                continue
            if item is not None and item is not _SHUTDOWN:
                pending.append(cast("list[dict[str, object]]", item))
        try:
            self._emit_pending(pending)
            for marker in markers:
                marker.delivered = self._nothing_lost_since(marker)
        finally:
            for marker in markers:
                marker.event.set()
                self._release_marker(marker)""",
    """        budget = self._queue.qsize()
        try:
            while budget > 0:
                budget -= 1
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if isinstance(item, _FlushMarker):
                    self._take_marker(item)
                    try:
                        self._emit_pending(pending)
                        item.delivered = self._nothing_lost_since(item)
                    finally:
                        item.event.set()
                        self._release_marker(item)
                    continue
                if item is not None and item is not _SHUTDOWN:
                    self._take(pending, cast("list[dict[str, object]]", item))
                    if self._complete(pending, True):
                        self._emit_pending(pending)
            self._emit_pending(pending)
        finally:
            self._write_exit_tally()""",
)

# _emit: record longest at exit, decline flag, tally lines after the first abandon
rep(
    """            if delivery is not None:
                if delivery.failed:""",
    """            if delivery is not None:
                if self._exit_deadline is not None:
                    self._exit_longest = max(getattr(self, "_exit_longest", 0.0), time.monotonic() - started_run)
                    if delivery.declined:
                        self._exit_declining = True
                if delivery.failed:""",
)
rep(
    """        self._in_flight = size
        try:
            delivery = _engine.run_delivery(""",
    """        self._in_flight = size
        started_run = time.monotonic()
        try:
            delivery = _engine.run_delivery(""",
)
rep(
    """                except Exception as err:
                    longest = max(longest, time.monotonic() - started)
                    if attempt >= retries or is_final_refusal(err, sink):
                        with self._lock:
                            self.failed_batches += 1
                        _diag.lost(
                            "event",
                            len(batch),
                            f"batch abandoned after {attempt + 1} emit attempts",
                        )
                        return""",
    """                except Exception as err:
                    longest = max(longest, time.monotonic() - started)
                    if self._exit_deadline is not None:
                        self._exit_longest = max(getattr(self, "_exit_longest", 0.0), longest)
                    if attempt >= retries or is_final_refusal(err, sink):
                        tallied = False
                        with self._lock:
                            self.failed_batches += 1
                            if self._exit_deadline is not None:
                                if getattr(self, "_exit_abandon_seen", False):
                                    n, k = getattr(self, "_tally_ab", (0, 0))
                                    self._tally_ab = (n + len(batch), k + 1)
                                    tallied = True
                                self._exit_abandon_seen = True
                        if not tallied:
                            _diag.lost(
                                "event",
                                len(batch),
                                f"batch abandoned after {attempt + 1} emit attempts",
                            )
                        return""",
)
rep(
    """                self._stop.wait(_engine.cycle_backoff(attempt))
                if self._retry_declined(longest):
                    with self._lock:""",
    """                self._stop.wait(_engine.cycle_backoff(attempt))
                if self._retry_declined(longest):
                    self._exit_declining = True
                    with self._lock:""",
)
# a successful attempt at exit records its duration too
rep(
    """                try:
                    sink.emit(batch)
                    return""",
    """                try:
                    sink.emit(batch)
                    if self._exit_deadline is not None:
                        self._exit_longest = max(getattr(self, "_exit_longest", 0.0), time.monotonic() - started)
                    return""",
)
open(p, "w").write(s)
