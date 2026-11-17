"""SPEC-103, written on 2026-11-17: a black-holed SQSSink (or SNSSink) that turns slow mid-exit, through the shipped shutdown.
A real SQSSink(max_retries=0) whose client's requests each cost 0.5 ms until the clock passes <slow_at>, then 0.3 s each; every
request raises. Its in-flight batch fails as the stop lands, so the size target is zero at the stop. Worker batch_size=10,
max_retries=0, 2,000 one-event spans queued at exit, log_foundry.shutdown(<budget>). The clock is fake and only the client moves
it: time.monotonic and the monotonic _lifecycle imported are both patched, so the deadline and the worker's predictions share it.
It prints the drain's end relative to the deadline on that clock, whether the join completed, close() calls and request sizes.
The last diff review's numbers: on the probe that grew exit batches (7f400f51), slow_at 0.004, 0.01 and 0.02 s ended the drain
1.60, 2.81 and 6.42 s past a 0.5 s deadline; on e7b437f9, 0.0 at every budget. Runs against whatever worker.py is importable.
Usage: PYTHONPATH=src python spec103_sqs_slowdown_exit.py <slow_at> <budget> [sqs|sns]
"""

import io, sys, threading, time

import log_foundry
from log_foundry import _lifecycle
from log_foundry.worker import Worker

SLOW_AT, BUDGET = float(sys.argv[1]), float(sys.argv[2])
MODE = sys.argv[3] if len(sys.argv) > 3 else "sqs"
SPANS = 2000


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


clock = Clock()
time.monotonic = clock
_lifecycle.monotonic = clock
stop = []
sizes = []


def request(entries):
    sizes.append(len(entries))
    if len(sizes) == 1:
        stop[0].wait(30)
    else:
        clock.now += 0.3 if clock.now >= SLOW_AT else 0.0005
    raise ConnectionError("black-holed")


class SQSClient:
    def send_message_batch(self, *, QueueUrl, Entries):
        request(Entries)


class SNSClient:
    def publish_batch(self, *, TopicArn, PublishBatchRequestEntries):
        request(PublishBatchRequestEntries)


if MODE == "sns":
    from log_foundry.sinks.sns import SNSSink

    sink = SNSSink("arn:aws:sns:us-east-1:1:t", client=SNSClient(), max_retries=0)
else:
    from log_foundry.sinks.sqs import SQSSink

    sink = SQSSink("https://sqs/q", client=SQSClient(), max_retries=0)
closes = [0]
sink.close = lambda: closes.__setitem__(0, closes[0] + 1)
worker = Worker(sink, batch_size=10, flush_interval=1.0, max_retries=0)
stop.append(worker._stop)
with _lifecycle._state._lock:
    _lifecycle._state._worker = worker
    _lifecycle._state._closes.arm(sink)
    _lifecycle._state._move()
for seq in range(10):
    worker.submit([{"message": "m", "seq": seq}])
for _ in range(5000):
    if sizes:
        break
    threading.Event().wait(0.001)
for seq in range(10, 10 + SPANS):
    worker.submit([{"message": "m", "seq": seq}])
ended = []
watcher = threading.Thread(target=lambda: (worker._drain_finished.wait(60), ended.append(clock.now)))
watcher.start()
buf = io.StringIO()
old = sys.stderr
sys.stderr = buf
log_foundry.shutdown(BUDGET)
closes_at_return = closes[0]
worker._thread.join(60)
watcher.join(60)
sys.stderr = old
deadline = worker._exit_deadline
past = ended[0] - deadline if ended and deadline is not None else float("nan")
print(
    f"{MODE} slow_at={SLOW_AT} budget={BUDGET}: drain ended {past:+.2f}s from the deadline; "
    f"join completed={worker.stopped_reason is None} close()={closes_at_return} "
    f"requests={len(sizes) - 1} sizes={sorted(set(sizes[1:]))} largest={max(sizes[1:] or [0])}"
)
