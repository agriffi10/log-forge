"""P14, written while authoring SPEC-088 and not one of the audit's own probes: a flush() on a thread sharing the span, landing while the close builds span.end.
`decorator._close_span` runs `span.events.append(end_event(span, status, exc))`. `span.events` is read before `end_event` is
called, so a sweep that detaches the buffer during that call hands the old list to the worker and the append then lands on a
list that has already been submitted. The interleaving is forced by running the other thread's call from inside `end_event`,
which is where a thread switch can fall; nothing else in the library is patched. Row 1 only sweeps (the list is still queued
when the append lands). Row 2 calls flush(), so the list has been delivered by then."""
import contextvars, threading
import log_foundry as lf
from log_foundry import decorator

class Sink:
    def __init__(self): self.got = []
    def emit(self, batch): self.got.extend(e["message"] for e in batch)
    def close(self): pass

def row(label, during_the_close):
    sink = Sink()
    lf.configure(sink=sink)
    holder = {}
    real = decorator.end_event
    def end_event_with_a_sweep_inside(span, status, exc):
        t = threading.Thread(target=lambda: holder["ctx"].run(during_the_close)); t.start(); t.join()
        return real(span, status, exc)
    decorator.end_event = end_event_with_a_sweep_inside
    try:
        @lf.trace
        def work():
            lf.info("in-span")
            holder["ctx"] = contextvars.copy_context()   # copied inside the span, so the other thread sees it open
        work()
    finally:
        decorator.end_event = real
    result = lf.flush()
    h = lf.health()
    print(f"[{label}] sink got {sink.got}; flush()={result}; in_span_lost={h.in_span_lost} dropped={h.dropped} "
          f"failed_batches={h.failed_batches} events={getattr(h, 'events', 'no such field')}")

row("another thread sweeps the span during the close", decorator._sweep_open_spans)
row("another thread calls flush() during the close", lf.flush)
