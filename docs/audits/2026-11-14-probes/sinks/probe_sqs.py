"""Fault-injection probe for SQSSink at a0f44b5f. Fake boto3-shaped client; no network.

For each scenario: N distinct events in, then compare what the fake queue HOLDS against the
sink's ledger (losses()), whether emit raised, and the stderr lines written.
"""
import contextlib, io, collections, sys
from log_foundry.sinks.sqs import SQSSink
from log_foundry import SinkDeliveryError

N = 25


class FakeSQS:
    """Stores bodies; `script(call_no, entries)` decides the reply shape per call."""

    def __init__(self, script, fifo_dedup=False):
        self.stored = []          # every body the queue holds, duplicates included
        self.calls = 0
        self.script = script
        self.fifo_dedup = fifo_dedup
        self.seen_dedup = set()

    def store(self, entries):
        for e in entries:
            if self.fifo_dedup:
                d = e["MessageDeduplicationId"]
                if d in self.seen_dedup:
                    continue
                self.seen_dedup.add(d)
            self.stored.append(e["MessageBody"])

    def send_message_batch(self, QueueUrl, Entries):
        self.calls += 1
        return self.script(self, self.calls, Entries)


def events():
    return [{"log_id": f"id-{i:02d}", "trace_id": f"t{i % 3}", "n": i, "message": f"m{i}"} for i in range(N)]


def run(name, script, *, fifo=False, fifo_dedup=False, max_retries=3):
    client = FakeSQS(script, fifo_dedup=fifo_dedup)
    url = "https://sqs.example/q.fifo" if fifo else "https://sqs.example/q"
    sink = SQSSink(url, client=client, max_retries=max_retries)
    import threading
    stop = threading.Event(); stop.set()          # cut the backoff waits short; changes no verdict
    sink.log_foundry_stop_signal = stop
    err = io.StringIO()
    raised = None
    with contextlib.redirect_stderr(err):
        try:
            sink.emit(events())
        except SinkDeliveryError as e:
            raised = str(e)
    import json
    held = collections.Counter(json.loads(b)["n"] for b in client.stored)
    unique = len(held)
    dupes = sum(c - 1 for c in held.values())
    missing = N - unique
    losses = sink.losses()
    lines = [l for l in err.getvalue().splitlines() if l.strip()]
    silent = missing - (losses.failed + losses.dropped) if not raised else 0
    print(f"\n== {name}")
    print(f"   calls={client.calls} queue_holds={sum(held.values())} unique={unique} "
          f"duplicate_copies={dupes} missing={missing}")
    print(f"   ledger: failed={losses.failed} dropped={losses.dropped} raised={raised!r}")
    print(f"   uncounted_missing={max(silent,0)}  stderr_lines={len(lines)}")
    for l in lines[:4]:
        print("     |", l[:170])


def ok(c, n, entries):
    c.store(entries)
    return {"Successful": [{"Id": e["Id"]} for e in entries]}


# A. transient per-entry failure (not sender fault) on first call of each chunk, entries 2 and 5
def a(c, n, entries):
    if n <= 3:
        bad = {entries[i]["Id"] for i in (2, 5) if i < len(entries)}
        c.store([e for e in entries if e["Id"] not in bad])
        return {"Successful": [{"Id": e["Id"]} for e in entries if e["Id"] not in bad],
                "Failed": [{"Id": i, "SenderFault": False, "Code": "InternalError"} for i in bad]}
    return ok(c, n, entries)


# B. persistent per-entry failure for events n in {3, 14}
def b(c, n, entries):
    import json
    bad = {e["Id"] for e in entries if json.loads(e["MessageBody"])["n"] in (3, 14)}
    c.store([e for e in entries if e["Id"] not in bad])
    return {"Successful": [{"Id": e["Id"]} for e in entries if e["Id"] not in bad],
            "Failed": [{"Id": i, "SenderFault": False, "Code": "InternalError"} for i in bad]}


# C. sender fault for events n in {3, 14}
def cc(c, n, entries):
    import json
    bad = {e["Id"] for e in entries if json.loads(e["MessageBody"])["n"] in (3, 14)}
    c.store([e for e in entries if e["Id"] not in bad])
    return {"Successful": [{"Id": e["Id"]} for e in entries if e["Id"] not in bad],
            "Failed": [{"Id": i, "SenderFault": True, "Code": "InvalidMessageContents"} for i in bad]}


# D. response lost ONCE: call 2 stores the chunk, then the read times out
def d(c, n, entries):
    c.store(entries)
    if n == 2:
        raise TimeoutError("read timed out after the request was accepted")
    return {"Successful": [{"Id": e["Id"]} for e in entries]}


# E. response lost on EVERY call (the queue takes everything, the client never hears)
def e_(c, n, entries):
    c.store(entries)
    raise TimeoutError("read timed out after the request was accepted")


# F. garbage replies while the queue stores everything
def f_none(c, n, entries):
    c.store(entries); return None
def f_failed_str(c, n, entries):
    c.store(entries); return {"Failed": "oops"}
def f_unknown_id(c, n, entries):
    c.store(entries); return {"Failed": [{"Id": "99", "SenderFault": False}]}


# G. a reply that names an entry in NEITHER list: the queue took 7 of each 10, says so in
#    Successful, and sends no Failed.
def g_partial_successful(c, n, entries):
    took = entries[:7]
    c.store(took)
    return {"Successful": [{"Id": e["Id"]} for e in took]}
def g_empty(c, n, entries):
    return {}


# H. total outage: every call raises ConnectionError before storing
def h(c, n, entries):
    raise ConnectionError("no route")


run("A transient per-entry Failed on first call of each chunk", a)
run("B persistent per-entry Failed (2 events)", b)
run("C SenderFault=True (2 events)", cc)
run("D response lost once after the queue accepted (standard queue)", d)
run("D' same, FIFO queue with dedup honoured by the fake", d, fifo=True, fifo_dedup=True)
run("E response lost on every call (standard queue)", e_)
run("F1 reply None, queue stored all", f_none)
run("F2 reply Failed='oops', queue stored all", f_failed_str)
run("F3 reply Failed names unknown Id, queue stored all", f_unknown_id)
run("G1 reply lists 7 of 10 in Successful and no Failed; queue stored those 7", g_partial_successful)
run("G2 reply {} and the queue stored nothing", g_empty)
run("H every call raises ConnectionError", h)
