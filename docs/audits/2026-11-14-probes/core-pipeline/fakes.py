"""Fake sinks and helpers shared by the scout-core probes."""
import io
import re
import sys
import threading


class CountingSink:
    """emit-only sink: records every event; can gate (stall) or raise on demand."""

    def __init__(self, name="S"):
        self.name = name
        self.events = []
        self.calls = 0
        self.gate = threading.Event()
        self.gate.set()
        self.entered = threading.Event()
        self.raise_when = None  # callable(call_no, batch) -> exception or None
        self.closed = 0
        self._lock = threading.Lock()

    def emit(self, batch):
        with self._lock:
            self.calls += 1
            call = self.calls
        self.entered.set()
        self.gate.wait()
        if self.raise_when is not None:
            exc = self.raise_when(call, batch)
            if exc is not None:
                raise exc
        with self._lock:
            self.events.extend(batch)

    def close(self):
        self.closed += 1

    def count(self, message=None):
        with self._lock:
            if message is None:
                return len(self.events)
            return sum(1 for e in self.events if e.get("message") == message)

    def log_ids(self):
        with self._lock:
            return [e["log_id"] for e in self.events]


class StderrCapture:
    """Tee stderr so the library's diagnostic lines can be counted."""

    def __enter__(self):
        self._real = sys.stderr
        self.buf = io.StringIO()
        sys.stderr = self.buf
        return self

    def __exit__(self, *a):
        sys.stderr = self._real

    def lines(self):
        return [l for l in self.buf.getvalue().splitlines() if l.startswith("log-foundry")]

    def lost_events(self):
        total = 0
        for l in self.lines():
            m = re.match(r"log-foundry: lost (\d+) event\(s\)", l)
            if m:
                total += int(m.group(1))
        return total


def show_health(lf, label="health"):
    h = lf.health()
    print(
        f"{label}: queued={h.queued} dropped={h.dropped} failed_batches={h.failed_batches} "
        f"stopped_reason={h.stopped_reason} retired={h.retired} "
        f"submitted_after_shutdown={h.submitted_after_shutdown} orphan_lost={h.orphan_lost} "
        f"in_span_lost={h.in_span_lost} incomplete_swaps={h.incomplete_swaps} sink={h.sink}"
    )
    return h
