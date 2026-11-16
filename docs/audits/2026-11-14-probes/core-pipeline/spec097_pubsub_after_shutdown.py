"""SPEC-098, written after its spec review: a GooglePubSubSink reused after shutdown(), whose stop
signal stays set, against one handed a fresh signal. Does any event's fate change, or only timing?

Usage: spec098_pubsub_after_shutdown.py <confirm_after_seconds> [overflow_timeout]
Run with PYTHONPATH=<tree>/src. A fake publisher's futures confirm `confirm_after` seconds after
publish. P is handed directly, then held by a caller's wrapper W that forwards the signal and
does not close P, so the shutdown sets P's signal without closing P. After the shutdown P is
configured again, five orphan logs publish, flush() runs, and the process exits through the exit
drain, which closes P. The atexit line prints how many publishes confirmed, how many P counted
failed, and flush()'s verdict.
"""

import atexit
import sys
import threading
import time

import log_foundry
from log_foundry.sinks.pubsub import GooglePubSubSink

CONFIRM = float(sys.argv[1])
OVERFLOW = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0
confirmed: list[int] = []
out: dict[str, object] = {}


class Future:
    def __init__(self) -> None:
        self.ready_at = time.monotonic() + CONFIRM

    def result(self, timeout=None):  # type: ignore[no-untyped-def]
        left = self.ready_at - time.monotonic()
        if left > 0:
            if timeout is not None and timeout < left:
                time.sleep(max(timeout, 0))
                raise TimeoutError("not yet")
            time.sleep(left)
        confirmed.append(1)
        return "id"


class Publisher:
    def publish(self, topic, data=None, **attrs):  # type: ignore[no-untyped-def]
        return Future()


class W:
    """A caller's wrapper: forwards the signal, delivers to P, and leaves P open on close."""

    def __init__(self, inner: object) -> None:
        self._inner = inner
        self._sig: threading.Event | None = None

    @property
    def log_foundry_stop_signal(self) -> threading.Event | None:
        return self._sig

    @log_foundry_stop_signal.setter
    def log_foundry_stop_signal(self, sig: threading.Event | None) -> None:
        self._sig = sig
        self._inner.log_foundry_stop_signal = sig  # type: ignore[attr-defined]

    def emit(self, batch):  # type: ignore[no-untyped-def]
        self._inner.emit(batch)  # type: ignore[attr-defined]

    def close(self) -> None:
        pass


P = GooglePubSubSink("projects/p/topics/t", client=Publisher(), overflow_timeout=OVERFLOW)


def last() -> None:
    print(
        f"confirm_after={CONFIRM}s overflow_timeout={OVERFLOW}s confirmed={len(confirmed)} "
        f"failed={P.failed} {out} exit_at={time.monotonic() - T0:.2f}s",
        flush=True,
    )


atexit.register(last)
T0 = time.monotonic()
log_foundry.configure(service="t", sink=P)
log_foundry.info("before")
log_foundry.configure(sink=W(P))
log_foundry.info("via w")
log_foundry.shutdown(timeout=5)
confirmed.clear()
before_failed = P.failed
log_foundry.configure(sink=P)
sig = P.log_foundry_stop_signal
out["signal_set_after_reuse"] = sig is not None and sig.is_set()
for i in range(5):
    log_foundry.info(f"after {i}")
t = time.monotonic()
r = log_foundry.flush(timeout=10)
out["flush"] = (bool(r), r.reason)
out["flush_s"] = round(time.monotonic() - t, 2)
out["failed_before_reuse"] = before_failed
T0 = time.monotonic()
