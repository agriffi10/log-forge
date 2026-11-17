"""Written for SPEC-101 on 2026-11-17 against a38bd80c, not one of the audit's probes.

Runs every sequence in which a caller's wrapper is the holder of a kept sink A, for a wrapper that
leaves its member alone (Leaves) and its twin that closes it (Mine), each in a fresh process, and
prints per row A's close count, overlapping closes, closes made while a drain thread was inside A,
refused events, delivered events against those written, the loss terms, and the verdict of
SPEC-086's close oracles (tests/model_oracles.py: never-closed, repeat, late-write) on A's record.

  installed    configure(A), 1 write, configure(W(A)), 2 writes, shutdown()
  multi        configure(MultiSink(A, B)), 1 write, configure(W(A)), 2 writes, shutdown()
  retired      leaves_remaining_holder.py's choreography with W: a retired drain thread inside
               W(A), then configure(MultiSink(A, B)), a log, shutdown(timeout=0.3), the emit
               released, shutdown()
  reverse      configure(W(A)), 1 write, configure(A), 2 writes, shutdown()
  rearmed      configure(A), 1 write, configure(W(A)), 1 write, shutdown(), 1 write, shutdown():
               the holder is armed again after its close, so A is owed a second close (SPEC-045);
               here A accepts a write after its close, as SPEC-086's recorder does
  unconfirmed  configure(A), a span outlasting a 0.1 s swap budget into configure(W(A)), 2 writes,
               shutdown()
  exit         installed, then the interpreter exits with no shutdown(): the exit drain closes
  fork         installed with no span, then a child forked: 1 write and shutdown() in the child,
               os._exit; the parent writes once more and shuts down. Child and parent rows.
  file-*       installed and multi with A a FileSink subclass that counts its close() calls, so
               the shipped closed guard (SPEC-076) is present on A

Run from the repository root against a tree, src and tests on the path, never -I:

  PYTHONPATH=<tree>/src:<tree>/tests python -B docs/audits/2026-11-14-probes/lifecycle/spec101_sequences.py

With no argument it runs every row in a child process, for the wrappers named in SPEC101_WRAPPERS
(default Leaves,Mine; MineDeclared is Mine with option (c)'s class attribute); with three
arguments (row, path, wrapper) it runs one. On spec101_prototype.patch's tree SPEC101_OPTION
selects the option: a (default), b (skip a member whose shipped closed guard reads closed) or c
(trust a holder's log_foundry_closes_members = True); other trees ignore it. Every
child is bounded by faulthandler.dump_traceback_later and a 60 s subprocess timeout, and a forked
child leaves by os._exit.
"""
import atexit
import os
import subprocess
import sys
import tempfile
import threading
import time

ROWS = [
    ("installed", "orphan"), ("installed", "spans"),
    ("multi", "orphan"), ("multi", "spans"),
    ("retired", "spans"),
    ("reverse", "orphan"), ("reverse", "spans"),
    ("rearmed", "orphan"),
    ("unconfirmed", "spans"),
    ("exit", "orphan"), ("exit", "spans"),
    ("fork", "orphan"),
    ("file-installed", "orphan"), ("file-multi", "orphan"),
]

WRAPPERS = os.environ.get("SPEC101_WRAPPERS", "Leaves,Mine").split(",")

if len(sys.argv) == 1:
    for seq, path in ROWS:
        for wrapper in WRAPPERS:
            r = subprocess.run(
                [sys.executable, "-B", __file__, seq, path, wrapper],
                capture_output=True, text=True, timeout=60,
            )
            out = " | ".join(r.stdout.strip().splitlines())
            err = [line for line in r.stderr.strip().splitlines() if line]
            print(f"{seq:15s} {path:6s} {wrapper:6s} exit={r.returncode} {out}")
            for line in err[:3]:
                print(f"{'':30s} stderr: {line[:160]}")
    sys.exit(0)

SEQ, PATH, WRAPPER = sys.argv[1:4]
AT_EXIT: list = []


def _report_at_exit() -> None:
    """Registered before the library is imported, so atexit runs it after the exit drain."""
    for make in AT_EXIT:
        print(make(), flush=True)


atexit.register(_report_at_exit)

import faulthandler  # noqa: E402
import warnings  # noqa: E402

warnings.filterwarnings("ignore", "This process", DeprecationWarning)

import log_foundry  # noqa: E402
from log_foundry import worker as worker_mod  # noqa: E402
from log_foundry.sinks.file import FileSink  # noqa: E402
from log_foundry.sinks.multi import MultiSink  # noqa: E402

try:
    from model_oracles import CloseBroken, Closes, check_closed, check_late_write, check_repeat
except ImportError:
    CloseBroken = None

faulthandler.dump_traceback_later(40, exit=True)


class Rec:
    """Refuses after close(); counts writes at emit's first statement and each close's counts."""

    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.writes = 0
        self.events = 0
        self.refused = 0
        self.closed = False
        self.inside = 0
        self.closing = 0
        self.overlaps = 0
        self.closed_while_inside = 0
        self.at_entry: list[int] = []
        self.at_return: list[int] = []
        self.park = None
        self.parked = threading.Event()
        self.slow_emit = 0.0
        self.refuse_after_close = True

    def emit(self, batch):
        with self.lock:
            self.writes += 1
            if self.closed and self.refuse_after_close:
                self.refused += len(batch)
                raise RuntimeError(f"{self.name} is closed")
            self.inside += 1
        try:
            if self.park is not None and threading.current_thread().name == "log-foundry-worker":
                self.parked.set()
                self.park.wait(10)
            if self.slow_emit:
                time.sleep(self.slow_emit)
            with self.lock:
                self.events += len(batch)
        finally:
            with self.lock:
                self.inside -= 1

    def close(self):
        with self.lock:
            self.at_entry.append(self.writes)
            if self.inside:
                self.closed_while_inside += 1
            self.closing += 1
            if self.closing > 1:
                self.overlaps += 1
        time.sleep(0.05)
        with self.lock:
            self.closing -= 1
            self.closed = True
            self.at_return.append(self.writes)

    def closes(self):
        return len(self.at_entry)


class CountingFile(FileSink):
    """A FileSink that counts its close() calls; its closed guard is SPEC-076's."""

    def __init__(self, target):
        super().__init__(target)
        self.calls = 0
        self.overlaps = 0
        self.closing = 0
        self.at_entry: list[int] = []
        self.at_return: list[int] = []
        self.writes = 0
        self.refused = 0
        self.events = 0
        self.closed_while_inside = 0

    def emit(self, batch):
        self.writes += 1
        self.events += len(batch)
        super().emit(batch)

    def close(self):
        self.at_entry.append(self.writes)
        self.calls += 1
        self.closing += 1
        if self.closing > 1:
            self.overlaps += 1
        try:
            super().close()
            time.sleep(0.05)
        finally:
            self.closing -= 1
            self.at_return.append(self.writes)

    def closes(self):
        return len(self.at_entry)


class Leaves:
    """A caller's wrapper whose close() leaves its member alone."""

    def __init__(self, inner):
        self.inner = inner

    def emit(self, batch):
        self.inner.emit(batch)

    def close(self):
        pass


class Mine:
    """A caller's wrapper whose close() closes its member, the natural one to write."""

    def __init__(self, inner):
        self.inner = inner

    def emit(self, batch):
        self.inner.emit(batch)

    def close(self):
        self.inner.close()


class MineDeclared(Mine):
    """Mine, declaring that its close() closes its members (option (c)'s attribute)."""

    log_foundry_closes_members = True


WRAP = {"Leaves": Leaves, "Mine": Mine, "MineDeclared": MineDeclared}[WRAPPER]


@log_foundry.trace
def span():
    pass


def write(n=1, path=PATH):
    for _ in range(n):
        if path == "spans":
            span()
        else:
            log_foundry.info("x")
    if path == "spans":
        log_foundry.flush(timeout=5)


def per(path=PATH):
    return 2 if path == "spans" else 1


def verdict(a):
    if CloseBroken is None:
        return "oracle=unavailable"
    rec = Closes(a.writes, tuple(a.at_entry), tuple(a.at_return))
    found = []
    for check in (check_closed, check_repeat, check_late_write):
        try:
            check(rec)
        except CloseBroken as exc:
            found.extend(exc.signatures)
    return "oracle=" + (",".join(found) if found else "green")


def row(a, written, label="", judge=True):
    h = log_foundry.health()
    losses = (h.dropped, h.failed_batches, h.orphan_lost, h.in_span_lost)
    return (
        f"{label}A.closes={a.closes()} overlaps={a.overlaps} under_drain={a.closed_while_inside} "
        f"refused={a.refused} events={a.events}/{written} losses={losses} "
        f"at_entry={a.at_entry} "
        + (verdict(a) if judge else "oracle=not judged: a child closes nothing it inherited")
    )


log_foundry.configure(service="spec101")
target = os.path.join(tempfile.mkdtemp(prefix="spec101-"), "a.jsonl")
a = CountingFile(target) if SEQ.startswith("file-") else Rec("A")
b = Rec("B")
seq = SEQ.removeprefix("file-")

if seq in ("installed", "exit", "fork"):
    log_foundry.configure(sink=a)
    write()
    log_foundry.configure(sink=WRAP(a))
    write(2)
    if seq == "installed":
        log_foundry.shutdown(timeout=5)
        time.sleep(0.3)
        print(row(a, 3 * per()))
    elif seq == "exit":
        AT_EXIT.append(lambda: row(a, 3 * per()))
    else:
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            try:
                os.close(r)
                write(1, "orphan")
                log_foundry.shutdown(timeout=5)
                os.write(w, row(a, 4, "child ", judge=False).encode())
            finally:
                os._exit(0)
        os.close(w)
        child = b""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            chunk = os.read(r, 4096)
            if not chunk:
                break
            child += chunk
        os.waitpid(pid, 0)
        write(1, "orphan")
        log_foundry.shutdown(timeout=5)
        time.sleep(0.3)
        print(child.decode())
        print(row(a, 4, "parent "))
elif seq == "multi":
    log_foundry.configure(sink=MultiSink(a, b))
    write()
    log_foundry.configure(sink=WRAP(a))
    write(2)
    log_foundry.shutdown(timeout=5)
    time.sleep(0.3)
    print(row(a, 3 * per()) + f" B.closes={b.closes()}")
elif seq == "rearmed":
    a.refuse_after_close = False
    log_foundry.configure(sink=a)
    write()
    log_foundry.configure(sink=WRAP(a))
    write()
    log_foundry.shutdown(timeout=5)
    write()
    log_foundry.shutdown(timeout=5)
    time.sleep(0.3)
    print(row(a, 3 * per()))
elif seq == "reverse":
    log_foundry.configure(sink=WRAP(a))
    write()
    log_foundry.configure(sink=a)
    write(2)
    log_foundry.shutdown(timeout=5)
    time.sleep(0.3)
    print(row(a, 3 * per()))
elif seq == "unconfirmed":
    log_foundry.configure(sink=a)
    write()
    a.slow_emit = 0.5
    span()
    worker_mod.DEFAULT_SWAP_TIMEOUT = 0.1
    log_foundry.configure(sink=WRAP(a))
    worker_mod.DEFAULT_SWAP_TIMEOUT = 5.0
    premise = log_foundry.health().incomplete_swaps
    while a.inside:
        time.sleep(0.01)
    a.slow_emit = 0.0
    write(2)
    log_foundry.shutdown(timeout=5)
    time.sleep(0.3)
    print(row(a, 4 * per()) + f" incomplete_swaps={premise}")
elif seq == "retired":
    log_foundry.configure(sink=WRAP(a))
    span()
    log_foundry.flush(timeout=3)
    a.park = threading.Event()
    span()
    a.parked.wait(5)
    log_foundry.shutdown(timeout=0.2)
    log_foundry.configure(sink=MultiSink(a, b))
    log_foundry.info("orphan")
    log_foundry.shutdown(timeout=0.3)
    while_parked = a.closes()
    a.park.set()
    while a.inside:
        time.sleep(0.01)
    log_foundry.shutdown(timeout=5)
    time.sleep(0.3)
    print(row(a, 5) + f" closes_while_parked={while_parked} B.closes={b.closes()}")
