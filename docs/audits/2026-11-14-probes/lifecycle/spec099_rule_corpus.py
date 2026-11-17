"""Written while authoring SPEC-099, not one of the audit's probes: which soak rule hides a swap's arm after a shutdown's take.

Usage: PYTHONPATH=src:tests python -B spec099_rule_corpus.py [runs]   (default 7), from a tree carrying SPEC-099's
prototype (docs/audits/2026-11-14-probes/lifecycle/spec099_prototype.patch), whose tests/model_oracles.py has Note,
the labelled Arm and Take, and owed_between, and whose tests/soak_known.py has the known shapes.

Part 1 runs p11_swap_arms_a_sink_a_shutdown_closed.py's `forced` choreography in a fresh child per run, recording S's
events as the soak's wrappers record them (Note at _note_orphan_emit's entry, Arm and Take labelled with their caller,
Write at emit's first statement, Close at close's entry), and judges each record by three rules:
  writer-arm   SPEC-086's rule: a writer's arm between the two takes owes the second close;
  any-write    the hand-over's suggestion: a writer's arm, or any Write, between the takes;
  entered      SPEC-099's: a writer's arm, or a Write by a thread whose _note_orphan_emit entered after the first take.
and asks soak_known which known shape explains it. Part 2 judges records written from docs/architecture.md §12's text
for the named seeds: Shape A as at deliver-seed962, the boundary record at deliver-seed600 (the writer armed before the
take and wrote after it), and Shape B as at emit-seed1582 (the swap's arm came first, so the log made none).
Each line prints the rule verdicts; a rule that reads Shape A or the boundary record as owed hides a defect."""

import json
import subprocess
import sys
import threading
import time

RULES = ("writer-arm", "any-write", "entered")


def verdicts(events):
    from model_oracles import Arm, Close, Write, owed_between, take_of_each_close

    def writer_arm(between):
        return any(isinstance(e, Arm) and e.by_writer for e in between)

    def any_write(between):
        return writer_arm(between) or any(isinstance(e, Write) for e in between)

    closes = [e for e in events if isinstance(e, Close)]
    owners = take_of_each_close(events)
    out = {}
    for rule, owed in zip(RULES, (writer_arm, any_write, owed_between)):
        flagged = False
        for first, second, a, b in zip(closes, closes[1:], owners, owners[1:]):
            if first.count != second.count:
                continue
            if a is not None and b is not None and owed(events[min(a, b) + 1 : max(a, b)]):
                continue
            flagged = True
        out[rule] = "repeat" if flagged else "owed"
    return out


def child():
    import log_foundry
    from log_foundry import _lifecycle as _lc, api as _api
    from model_oracles import Arm, Close, Note, Take, Write

    events = []
    lock = threading.Lock()

    class Recorder:
        log_foundry_stop_signal = None

        def __init__(self, name):
            self.name, self.writes = name, 0

        def emit(self, batch):
            with lock:
                self.writes += 1
                if self is S:
                    events.append(Write(threading.current_thread().name))

        def close(self):
            with lock:
                if self is S:
                    events.append(
                        Close(self.writes, threading.current_thread().name, time.monotonic())
                    )
            time.sleep(0.002)

    A, S = Recorder("A"), Recorder("S")
    inside = threading.local()
    real_arm, real_take = _lc._CloseRegistry.arm, _lc._CloseRegistry.take

    def arm(self, sink):
        if sink is S:
            with lock:
                events.append(
                    Arm(
                        by_writer=getattr(inside, "sink", None) is S,
                        by=sys._getframe(1).f_code.co_name,
                    )
                )
        return real_arm(self, sink)

    def take(self, sink, rule):
        if sink is S:
            with lock:
                events.append(Take(by=sys._getframe(1).f_code.co_name))
        return real_take(self, sink, rule)

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
        if sink is S:
            with lock:
                events.append(Note(threading.current_thread().name))
        if sink is A and threading.current_thread().name == "stale":
            stale_parked.set()
            stale_go.wait(10)
        inside.sink = sink
        try:
            return real_note(sink)
        finally:
            inside.sink = None

    _api._note_orphan_emit = note
    import io

    sys.stderr = io.StringIO()
    log_foundry.configure(service="probe", sink=A)
    log_foundry.info("a")
    stale = threading.Thread(target=log_foundry.info, args=("late",), name="stale")
    stale.start()
    stale_took = stale_parked.wait(5)
    changer = threading.Thread(
        target=log_foundry.configure, kwargs={"service": "probe", "sink": S}, name="changer"
    )
    changer.start()
    parked = swap_parked.wait(5)
    log_foundry.info("s")
    log_foundry.shutdown(timeout=2.0)
    stale_go.set()
    stale.join(5)
    swap_go.set()
    changer.join(10)
    log_foundry.shutdown(timeout=2.0)
    import soak_known

    class _Closes:
        def __init__(self, writes, at_return):
            self.writes, self.at_return = writes, at_return

    shapes = soak_known.explain_run(
        [(events, _Closes(S.writes, [c.count for c in events if isinstance(c, Close)]))]
    )
    sys.stdout.write(
        json.dumps(
            {
                "parked": parked and stale_took,
                "record": " ".join(_label(e) for e in events),
                "rules": verdicts(events),
                "known": [k.name for k in shapes],
                "file": log_foundry.__file__,
            }
        )
        + "\n"
    )


def _label(e):
    name = type(e).__name__
    if name == "Arm":
        return f"arm({'writer' if e.by_writer else 'change'}:{e.by})"
    if name == "Take":
        return f"take({e.by})"
    if name == "Write":
        return f"write({e.thread})"
    if name == "Note":
        return f"note({e.thread})"
    return f"close@{e.count}({e.thread})"


def corpus():
    import soak_known
    from model_oracles import Arm, Close, Note, Take, Write

    def close(n, thread="Thread-1 (runner)"):
        return Close(n, thread, 0.0)

    w, w2 = "Thread-1 (runner)", "Thread-2 (runner)"
    rows = {
        "shape-a-deliver-seed962": [
            Note(w),
            Arm(True, "_arm_and_hand"),
            Write(w),
            Take("_close_owed"),
            close(1),
            Arm(False, "_swap_sink"),
            Take("_close_owed"),
            close(1, "log-foundry-sink-close"),
        ],
        "boundary-deliver-seed600": [
            Note(w),
            Arm(True, "_arm_and_hand"),
            Take("_close_owed"),
            Write(w),
            Arm(False, "_swap_sink"),
            close(1),
            Take("_swap_sink"),
            close(1),
        ],
        "shape-b-emit-seed1582": [
            Note(w),
            Arm(True, "_arm_and_hand"),
            Write(w),
            Take("_close_owed"),
            Arm(False, "_swap_sink"),
            Note(w2),
            Write(w2),
            close(2),
            Take("_close_owed"),
            close(2),
        ],
        "seed-1653-traced-rearm": [
            Note(w),
            Arm(True, "_arm_and_hand"),
            Take("_swap_sink"),
            Arm(True, "rearm_if_released"),
            Write(w),
            close(1),
            Take("_swap_sink"),
            close(1),
        ],
        "issue-339-change-arm": [
            Note(w),
            Arm(True, "_arm_and_hand"),
            Write(w),
            Take("_swap_sink"),
            Arm(False, "_swap_sink"),
            close(1),
            Take("_swap_sink"),
            close(1),
        ],
    }

    class _Closes:
        def __init__(self, writes, at_return):
            self.writes, self.at_return = writes, at_return

    for name, events in rows.items():
        writes = sum(1 for e in events if isinstance(e, Write))
        shapes = soak_known.explain_run(
            [(events, _Closes(writes, [c.count for c in events if isinstance(c, Close)]))]
        )
        print(
            json.dumps({"row": name, "rules": verdicts(events), "known": [k.name for k in shapes]})
        )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "child":
        child()
        sys.exit(0)
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    tally = {}
    for _ in range(runs):
        r = subprocess.run(
            [sys.executable, "-B", __file__, "child"], capture_output=True, text=True, timeout=60
        )
        lines = r.stdout.strip().splitlines()
        if not lines:
            print(f"child printed nothing; stderr tail: {r.stderr[-400:]!r}")
            continue
        report = json.loads(lines[-1])
        key = json.dumps(
            {"rules": report["rules"], "known": report["known"], "parked": report["parked"]}
        )
        tally[key] = tally.get(key, 0) + 1
        if tally[key] == 1:
            print(json.dumps(report))
    for key, count in tally.items():
        print(f"p11 forced, {count} of {runs} runs: {key}")
    corpus()
