"""Partial-write probe for FileSink / RotatingFileSink / StdoutSink at a0f44b5f.

A file-size cap (RLIMIT_FSIZE, SIGXFSZ ignored so the write raises EFBIG) cuts a 200-event
batch partway. The cap is lifted after the first failed emit and the batch is resent whole,
which is what the worker's 1.x retry does for a sink with no `deliver`. Then: what is on disk
against what the sink counted. Part 2 repeats one cap through the real worker.
"""
import collections, contextlib, glob, io, json, os, resource, signal, sys, tempfile
from log_foundry.sinks.file import FileSink, RotatingFileSink
from log_foundry.sinks.stdout import StdoutSink

signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
SOFT0, HARD0 = resource.getrlimit(resource.RLIMIT_FSIZE)
N = 200
TMP = tempfile.mkdtemp(prefix="probe-file-", dir=os.path.dirname(os.path.abspath(__file__)))


def cap(n):
    resource.setrlimit(resource.RLIMIT_FSIZE, (n, HARD0))


def uncap():
    resource.setrlimit(resource.RLIMIT_FSIZE, (SOFT0, HARD0))


def events():
    return [{"n": i, "message": "x" * 60, "log_id": f"id-{i:03d}"} for i in range(N)]


def read(paths):
    seen = collections.Counter(); bad = 0; total = 0
    for p in paths:
        with open(p, "rb") as fh:
            for raw in fh.read().split(b"\n"):
                if not raw:
                    continue
                total += 1
                try:
                    seen[json.loads(raw)["n"]] += 1
                except Exception:
                    bad += 1
    return seen, bad, total


def one(label, make, limit, paths_of):
    path = os.path.join(TMP, f"{label}-{limit}.ndjson".replace(" ", "_"))
    sink, closer = make(path)
    err = io.StringIO(); attempts = 0; raises = []
    with contextlib.redirect_stderr(err):
        cap(limit)
        try:
            for attempts in range(1, 5):               # the worker: 1 try + max_retries=3
                try:
                    sink.emit(events())
                    break
                except Exception as e:                  # noqa: BLE001
                    raises.append(type(e).__name__)
                    uncap()                             # the fault clears before the resend
        finally:
            uncap()
        closer()
    seen, bad, total = read(paths_of(path))
    twice = sum(1 for c in seen.values() if c > 1)
    missing = N - len(seen)
    losses = sink.losses()
    led = "None" if losses is None else f"failed={losses.failed} dropped={losses.dropped}"
    lines = [l for l in err.getvalue().splitlines() if l.startswith("log-foundry")]
    print(f"{label:<34} cap={limit:<6} emits={attempts} raised={len(raises)} lines_on_disk={total:<4} "
          f"events_twice={twice:<4} missing={missing:<3} unparseable={bad}  losses()={led}  stderr={len(lines)}")
    return lines


def mk_file(path):
    s = FileSink(path); return s, s.close

def mk_rot(path):
    s = RotatingFileSink(path); return s, s.close

def mk_rot_small(path):
    s = RotatingFileSink(path, max_bytes=6000, backup_count=50); return s, s.close

def mk_stdout_block(path):
    fh = open(path, "a", encoding="utf-8")            # block-buffered text stream, as a redirected stdout
    s = StdoutSink(fh)
    return s, fh.close

def mk_stdout_line(path):
    fh = open(path, "a", encoding="utf-8", buffering=1)
    s = StdoutSink(fh)
    return s, fh.close

LINE = len(json.dumps(events()[0])) + 1
print(f"python {sys.version.split()[0]}; one line = {LINE} bytes; batch = {N} events = {LINE*N} bytes\n")
last = None
for limit in (4000, 8192, 10500, 12000, 16384):
    last = one("FileSink", mk_file, limit, lambda p: [p])
print("   sample line:", last[0] if last else None)
for limit in (4000, 10500):
    one("RotatingFileSink (no trigger)", mk_rot, limit, lambda p: [p])
for limit in (4000,):
    one("RotatingFileSink max_bytes=6000", mk_rot_small, limit, lambda p: sorted(glob.glob(p + "*")))
for limit in (4000, 8192, 10500, 12000):
    last = one("StdoutSink(block-buffered file)", mk_stdout_block, limit, lambda p: [p])
for limit in (4000, 10500):
    last = one("StdoutSink(line-buffered file)", mk_stdout_line, limit, lambda p: [p])
print("   StdoutSink stderr lines:", last)

# Part 2: the same fault through the real worker (configure -> span -> flush).
import log_foundry
path = os.path.join(TMP, "worker.ndjson")
inner = FileSink(path)

class LiftOnRaise:
    """A caller's pass-through sink: it lifts the cap when the inner emit raises, then re-raises."""
    def __init__(self): self.calls = 0; self.sizes = []
    def emit(self, batch):
        self.calls += 1; self.sizes.append(len(batch))
        try:
            inner.emit(batch)
        except Exception:
            uncap(); raise
    def close(self): inner.close()

wrap = LiftOnRaise()
err = io.StringIO()
with contextlib.redirect_stderr(err):
    log_foundry.configure(service="probe", sink=wrap)

    @log_foundry.trace
    def work():
        for i in range(N - 2):
            log_foundry.info("x" * 60, n=i)

    cap(10500)
    try:
        work()
        result = log_foundry.flush(timeout=30)
    finally:
        uncap()
    h = log_foundry.health()
    log_foundry.shutdown()
ids = collections.Counter(); bad = 0
for raw in open(path, "rb").read().split(b"\n"):
    if raw:
        try: ids[json.loads(raw)["log_id"]] += 1
        except Exception: bad += 1
print(f"\nworker path: emit calls={wrap.calls} batch sizes={wrap.sizes} flush={bool(result)} reason={result.reason!r}")
print(f"   distinct events on disk={len(ids)} events_twice={sum(1 for c in ids.values() if c>1)} unparseable={bad}")
print(f"   health: failed_batches={h.failed_batches} dropped={h.dropped} sink={h.sink}")
print("   stderr:", [l for l in err.getvalue().splitlines() if l.startswith('log-foundry')][:3])
