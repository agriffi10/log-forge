"""Does this kernel ever count a thread's sleep as time waiting for a CPU?

Written on 2026-11-27, with no spec, by the second diff review of a rejected change to the model
test's timing bound (``timing_bound_own_time_rejected.patch`` beside this file); it is the probe
behind the open item *Two timing tests fail* in ``docs/architecture.md`` §12. It imports nothing
from the repository.

In each of PROCS processes, ROUNDS times: three sibling threads spin for a quarter of a second
while a fourth reads ``/proc/thread-self/schedstat``, sleeps SECONDS, and reads it again. Field
two is documented as time runnable and waiting for a CPU, so a sleep should add nothing to it.
Prints how many sleeps added more than half their own length. On kernel 6.12.54-linuxkit, in a
container held to four CPUs, it printed between 7 and 33 of 360 over five runs, by two reviews
and the session that asked for them.

Usage: python timing_bound_schedstat_sleep.py [PROCS=12] [ROUNDS=30] [SECONDS=1.3]
"""
import multiprocessing
import os
import sys
import threading
import time

PROCS = int(sys.argv[1]) if len(sys.argv) > 1 else 12
ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 30
SECONDS = float(sys.argv[3]) if len(sys.argv) > 3 else 1.3


def wait_ns():
    with open("/proc/thread-self/schedstat", "rb") as handle:
        return int(handle.read().split()[1])


def spin():
    end = time.monotonic() + 0.25
    while time.monotonic() < end:
        pass


def child(queue):
    counted = 0
    worst = 0.0
    for _ in range(ROUNDS):
        box = {}

        def sleeper():
            before = wait_ns()
            began = time.monotonic()
            time.sleep(SECONDS)
            box["elapsed"] = time.monotonic() - began
            box["waited"] = (wait_ns() - before) / 1e9

        threads = [threading.Thread(target=spin) for _ in range(3)]
        threads.append(threading.Thread(target=sleeper))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        worst = max(worst, box["waited"])
        counted += box["waited"] > SECONDS / 2
    queue.put((counted, worst))


if __name__ == "__main__":
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    procs = [context.Process(target=child, args=(queue,)) for _ in range(PROCS)]
    for proc in procs:
        proc.start()
    rows = [queue.get() for _ in procs]
    for proc in procs:
        proc.join()
    print(
        f"kernel {os.uname().release}, {len(os.sched_getaffinity(0))} CPUs, python "
        f"{sys.version.split()[0]}: {sum(r[0] for r in rows)} of {PROCS * ROUNDS} sleeps of "
        f"{SECONDS} s were counted as waiting for a CPU (worst {max(r[1] for r in rows):.3f} s)"
    )
