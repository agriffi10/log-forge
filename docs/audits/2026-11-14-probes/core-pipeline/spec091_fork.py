"""SPEC-091: a forked child never ships an open span's events it inherited from its parent.

The parent has two open spans when it forks: one on another thread (``other``, 5 logs, still
open), and the forking thread's own (``forking``, 3 logs before the fork). The child logs one more
event on the forking thread's span, waits 2.5 s (two of the worker's intervals) and leaves with
``os._exit``, as a ``multiprocessing`` child does, so it never closes the inherited span. The parent
then closes both spans. Every write goes to one file tagged with the writing pid. Prints how many
of each span's events the child and the parent wrote; on a correct tree the child writes none of
the parent's pre-fork events, and the parent writes each of its own events once.
Run: PYTHONPATH=<src> python spec091_fork.py
"""

import collections
import json
import os
import tempfile
import threading
import time

import log_foundry as lf

OUT = os.path.join(tempfile.mkdtemp(), "fork.jsonl")


class Tagged:
    def emit(self, batch: list[dict[str, object]]) -> None:
        fd = os.open(OUT, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        try:
            for e in batch:
                row = {"pid": os.getpid(), "fn": e["function"], "msg": e["message"]}
                os.write(fd, (json.dumps(row) + "\n").encode())
        finally:
            os.close(fd)

    def close(self) -> None:
        pass


def main() -> None:
    lf.configure(sink=Tagged())
    opened = threading.Event()
    release = threading.Event()

    @lf.trace
    def other() -> None:
        for i in range(5):
            lf.info("other-log", i=i)
        opened.set()
        release.wait()

    @lf.trace
    def forking() -> int:
        for i in range(3):
            lf.info("forking-log", i=i)
        pid = os.fork()
        if pid == 0:
            lf.info("child-log")
            time.sleep(2.5)
            os._exit(0)
        return pid

    t = threading.Thread(target=other)
    t.start()
    opened.wait()
    child = forking()
    os.waitpid(child, 0)
    release.set()
    t.join()
    lf.shutdown(timeout=5.0)
    parent = os.getpid()
    rows = [json.loads(line) for line in open(OUT)]
    got = collections.Counter(
        ("parent" if r["pid"] == parent else "child", r["fn"], r["msg"]) for r in rows
    )
    for key in sorted(got):
        print(key, got[key])


if __name__ == "__main__":
    main()
