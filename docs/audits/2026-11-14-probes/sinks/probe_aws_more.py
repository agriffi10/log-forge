"""Follow-ups at a0f44b5f (fake clients):
  1. SQSSink through the real worker when every response is lost after the queue accepted.
  2. KinesisSink: a reply of {} while the stream stored nothing.
  3. SNSSink: Successful lists 7 of 10, no Failed.
"""
import collections, contextlib, io, json, time
import log_foundry
from log_foundry.sinks.sqs import SQSSink
from log_foundry.sinks.kinesis import KinesisSink
from log_foundry.sinks.sns import SNSSink

def evs(n): return [{"n": i, "message": f"m{i}", "trace_id": "t", "log_id": f"id{i}"} for i in range(n)]
def lines_of(err): return [l for l in err.getvalue().splitlines() if l.startswith("log-foundry")]

class LostReply:
    def __init__(self): self.stored = []; self.calls = 0
    def send_message_batch(self, QueueUrl, Entries):
        self.calls += 1
        self.stored.extend(e["MessageBody"] for e in Entries)
        raise TimeoutError("read timed out")

client = LostReply()
sink = SQSSink("https://sqs.example/q", client=client)
err = io.StringIO(); t0 = time.monotonic()
with contextlib.redirect_stderr(err):
    log_foundry.configure(service="probe", sink=sink)
    @log_foundry.trace
    def work():
        for i in range(8): log_foundry.info("m", n=i)
    work()                                      # 10 events: span.start + 8 + span.end
    r = log_foundry.flush(timeout=60)
    h = log_foundry.health()
    log_foundry.shutdown()
ids = collections.Counter(json.loads(b)["log_id"] for b in client.stored)
print(f"1. SQSSink via worker, every reply lost after acceptance: events={len(ids)} calls={client.calls} "
      f"queue_holds={len(client.stored)} copies_each={sorted(set(ids.values()))} elapsed={time.monotonic()-t0:.1f}s")
print(f"   flush()={bool(r)} reason={r.reason!r} failed_batches={h.failed_batches} sink={h.sink}")
for l in lines_of(err)[:3]: print("   |", l[:200])

class EmptyKinesis:
    def __init__(self): self.calls = 0
    def put_records(self, **kw):
        self.calls += 1
        return {}
k = KinesisSink("stream", client=EmptyKinesis())
err = io.StringIO(); raised = None
with contextlib.redirect_stderr(err):
    try: k.emit(evs(25))
    except Exception as e: raised = type(e).__name__
l = k.losses()
print(f"2. KinesisSink, reply {{}} and nothing stored: stored=0/25 raised={raised} losses: failed={l.failed} "
      f"dropped={l.dropped} stderr_lines={len(lines_of(err))}")

class PartialSNS:
    def __init__(self): self.stored = []
    def publish_batch(self, **kw):
        entries = kw["PublishBatchRequestEntries"]
        took = entries[:7]; self.stored.extend(took)
        return {"Successful": [{"Id": e["Id"]} for e in took]}
c = PartialSNS()
s = SNSSink("arn:aws:sns:us-east-1:1:t", client=c)
err = io.StringIO(); raised = None
with contextlib.redirect_stderr(err):
    try: s.emit(evs(25))
    except Exception as e: raised = type(e).__name__
l = s.losses()
print(f"3. SNSSink, Successful names 7 of each 10 and no Failed: stored={len(c.stored)}/25 raised={raised} "
      f"losses: failed={l.failed} dropped={l.dropped} stderr_lines={len(lines_of(err))}")
