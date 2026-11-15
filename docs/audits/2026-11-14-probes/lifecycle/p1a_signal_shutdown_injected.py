"""P1a: a signal handler that calls shutdown(), delivered while the main thread is inside a
_state._lock critical section. The delivery point is injected with sys.settrace (no library code is
patched): at the first line inside _flush_live_sink's `with _state._lock` block, SIGTERM is raised.
A watchdog dumps every stack and exits 1 if the process is still running after 4 s."""
import faulthandler, signal, sys, time
import log_foundry
from log_foundry import _lifecycle

faulthandler.dump_traceback_later(4, exit=True)

class Sink:
    def __init__(self): self.events = []; self.closes = 0
    def emit(self, batch): self.events.extend(batch)
    def close(self): self.closes += 1

sink = Sink()
log_foundry.configure(service="p1a", sink=sink)
log_foundry.info("armed")            # orphan path: the sink is now owed a close

handled = []
def on_term(signum, frame):
    handled.append("entered")
    print("handler: calling shutdown(timeout=2)", flush=True)
    log_foundry.shutdown(timeout=2)
    handled.append("returned")
    print("handler: shutdown returned", flush=True)
signal.signal(signal.SIGTERM, on_term)

TARGET = _lifecycle._flush_live_sink.__code__
fired = []
def tracer(frame, event, arg):
    if frame.f_code is TARGET:
        def local(frame, event, arg):
            # the first line executed with the lock held: `debt = _state._signal_owed`
            if event == "line" and not fired and _lifecycle._state._lock.locked():
                fired.append(frame.f_lineno)
                signal.raise_signal(signal.SIGTERM)
            return local
        return local
    return None
sys.settrace(tracer)
started = time.monotonic()
result = log_foundry.flush(timeout=1)
sys.settrace(None)
print(f"flush returned {result} after {time.monotonic()-started:.2f}s; signal raised at line {fired}; handler={handled}")
