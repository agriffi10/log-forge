"""SPEC-091: what the exit drain and an explicit shutdown() take from spans still open.

``exit``: a daemon thread opens a span, logs 5 events and blocks for good; the main thread returns
at once, well inside one interval, so only the exit drain can move the daemon's events. ``inside``:
a traced function logs 5 events and calls ``shutdown()`` from inside itself, then logs once more and
returns; ``inside-warm`` makes one traced call first, so a worker exists before the handler opens.
Each mode runs in a child process writing to a file; the parent prints what arrived and
the child's ``health()`` as it exits.
Run: PYTHONPATH=<src> python spec091_exit.py
"""

import json
import os
import subprocess
import sys
import tempfile

CHILD = r"""
import json, os, sys, threading
import log_foundry as lf

OUT, MODE = sys.argv[1], sys.argv[2]

class FileSink:
    def emit(self, batch):
        with open(OUT, "a") as f:
            for e in batch:
                f.write(json.dumps({"fn": e["function"], "msg": e["message"]}) + "\n")
    def close(self):
        pass

lf.configure(sink=FileSink())

if MODE == "exit":
    started = threading.Event()

    @lf.trace
    def stuck():
        for i in range(5):
            lf.info("daemon-log", i=i)
        started.set()
        threading.Event().wait()

    threading.Thread(target=stuck, daemon=True).start()
    started.wait()
else:
    @lf.trace
    def warm():
        lf.info("warm")

    if MODE == "inside-warm":
        warm()

    @lf.trace
    def handler():
        for i in range(5):
            lf.info("before-shutdown", i=i)
        lf.shutdown(timeout=5.0)
        lf.info("after-shutdown")

    handler()
    h = lf.health()
    print(json.dumps({"queued": h.queued, "after_shutdown": h.submitted_after_shutdown}))
"""


def run(mode: str) -> None:
    out = os.path.join(tempfile.mkdtemp(), "out.jsonl")
    proc = subprocess.run(
        [sys.executable, "-c", CHILD, out, mode], capture_output=True, text=True, timeout=60
    )
    rows = [json.loads(line) for line in open(out)] if os.path.exists(out) else []
    got: dict[str, int] = {}
    for r in rows:
        got[r["msg"]] = got.get(r["msg"], 0) + 1
    print(f"[{mode}] rc={proc.returncode} delivered={got} child={proc.stdout.strip()}")


if __name__ == "__main__":
    run("exit")
    run("inside")
    run("inside-warm")
