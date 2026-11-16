"""P11, written while tracing SPEC-086's soak findings and not one of the audit's own probes: a sink change arms the sink it installs after a concurrent shutdown() took and closed it, so the sink is closed twice with nothing written between.
Usage: p11_swap_arms_a_sink_a_shutdown_closed.py [forced|control-no-shutdown|control-no-stale|all] [runs]   (defaults: all, 7)
Each run is a fresh process with no test harness. configure(sink=S) writes S to the config before its swap decides
(config._change_sink: _rebind, then _swap_live_sink). The probe parks the changing thread on its way into
_lifecycle._swap_sink(S), which config imports by name at call time in every tree from 31dda162 (the parent of #339)
to 9d70f782, so the same file runs against older trees. In that window an orphan log arms S and writes to it, and shutdown() on this thread takes S and
closes it. The swap is then released and, with no live worker, arms S as the sink it installs in its one critical section
(_swap_sink, `_state._closes.arm(new_sink)` on the no-worker branch). A final shutdown() takes S again and closes it at
the same write count. The worker branch's arm after Worker.retarget makes the same decision; it is not forced here.
`forced` needs one other sink owed when the swap decides, or the swap returns early with nothing owed and arms nothing.
It gets one the way SPEC-086's soak did at deliver-seed1456: an orphan log that resolved the old sink A before the config
write is parked at its owed check and released after the first shutdown, so A is written after its close and owed again.
The swap's stale loop closes A at its second write, which is owed; S's second close is not.
The two controls drop one step each: without the first shutdown S is closed once, at one write; without the stale log
the swap finds nothing owed, returns early, and S is closed once.
Each child prints one JSON line: S's closes as the write count each entered at and the thread that ran it, the arms and
takes of S with the function that made each, whether each park took, and whether a second close entered at the count of
the first. The parent tallies, per variant, the runs that repeated, every list of S's close counts with how many runs
gave it (so a control that closed S never cannot read as clean), the runs where a park did not take, and the children
that printed nothing, which stay in the denominator."""
import json, subprocess, sys, threading, time

VARIANTS = ("forced", "control-no-shutdown", "control-no-stale")

if len(sys.argv) < 4:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    for variant in VARIANTS if which == "all" else (which,):
        hits, shown, failed, unparked, closes = 0, 0, 0, 0, {}
        for _ in range(runs):
            r = subprocess.run([sys.executable, "-B", __file__, variant, "1", "child"], capture_output=True, text=True, timeout=60)
            lines = r.stdout.strip().splitlines()
            if not lines:
                failed += 1
                print(f"{variant}: child printed nothing; stderr tail: {r.stderr[-400:]!r}")
                continue
            report = json.loads(lines[-1])
            hits += report["repeat"]
            unparked += not report["parked"] or report["stale_parked"] is False
            key = str([count for count, _ in report["S_closes"]])
            closes[key] = closes.get(key, 0) + 1
            if shown < 1:
                shown += 1
                print(json.dumps(report))
        print(f"{variant}: {hits} of {runs} runs closed S twice at one write count; S's closes entered at, "
              f"by runs: {closes}; runs where a park did not take: {unparked}; children that printed nothing: {failed}")
    sys.exit(0)

VARIANT = sys.argv[1]
import io
import log_foundry
from log_foundry import _lifecycle as _lc, api as _api

captured = io.StringIO()
sys.stderr = captured
trace = []


def _who(depth=8):
    f, out = sys._getframe(2), []
    while f is not None and len(out) < depth:
        if "log_foundry" in f.f_code.co_filename:
            out.append(f"{f.f_code.co_name}:{f.f_lineno}")
        f = f.f_back
    return "<".join(out[:3])


class Recorder:
    log_foundry_stop_signal = None

    def __init__(self, name):
        self.name, self.writes, self.closes, self._lock = name, 0, [], threading.Lock()

    def emit(self, batch):
        with self._lock:
            self.writes += 1

    def close(self):
        with self._lock:
            self.closes.append((self.writes, threading.current_thread().name))
        time.sleep(0.002)


A, S = Recorder("A"), Recorder("S")
_arm, _take = _lc._CloseRegistry.arm, _lc._CloseRegistry.take


def arm(self, sink):
    if sink is S:
        trace.append(("arm", S.writes, threading.current_thread().name, _who()))
    return _arm(self, sink)


def take(self, sink, rule):
    if sink is S:
        trace.append(("take", S.writes, threading.current_thread().name, _who()))
    return _take(self, sink, rule)


_lc._CloseRegistry.arm, _lc._CloseRegistry.take = arm, take

swap_parked, swap_go = threading.Event(), threading.Event()
real_swap = _lc._swap_sink


def swap_sink(new_sink, *args, **kw):
    if new_sink is S and threading.current_thread().name == "changer" and not swap_go.is_set():
        swap_parked.set()
        swap_go.wait(10)
    return real_swap(new_sink, *args, **kw)


_lc._swap_sink = swap_sink

stale_parked, stale_go = threading.Event(), threading.Event()
real_note = _api._note_orphan_emit


def note(sink):
    if sink is A and threading.current_thread().name == "stale":
        stale_parked.set()
        stale_go.wait(10)
    return real_note(sink)


_api._note_orphan_emit = note

log_foundry.configure(service="probe", sink=A)
log_foundry.info("a")

stale, stale_took = None, None
if VARIANT in ("forced", "control-no-shutdown"):
    stale = threading.Thread(target=log_foundry.info, args=("late",), name="stale")
    stale.start()
    stale_took = stale_parked.wait(5)

changer = threading.Thread(target=log_foundry.configure, kwargs={"service": "probe", "sink": S}, name="changer")
changer.start()
parked = swap_parked.wait(5)
log_foundry.info("s")
if VARIANT != "control-no-shutdown":
    log_foundry.shutdown(timeout=2.0)
if stale is not None:
    stale_go.set()
    stale.join(5)
swap_go.set()
changer.join(10)
log_foundry.shutdown(timeout=2.0)
counts = [count for count, _ in S.closes]
sys.stdout.write(json.dumps({
    "variant": VARIANT,
    "parked": parked,
    "stale_parked": stale_took,
    "S_writes": S.writes,
    "S_closes": S.closes,
    "A_closes": A.closes,
    "S_arms_and_takes": trace,
    "repeat": int(any(a == b for a, b in zip(counts, counts[1:]))),
    "stderr": [line for line in captured.getvalue().splitlines() if line.startswith("log-foundry")][:4],
    "file": log_foundry.__file__,
}) + "\n")
