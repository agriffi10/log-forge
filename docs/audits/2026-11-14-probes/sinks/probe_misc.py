"""Smaller probes at a0f44b5f, all with fakes or in-process loopback:
  1. PostgresSink: commit() applies the rows, then the connection drops before the ack.
  2. PostgresSink: an injected autocommit connection, chunk 2 fails transiently.
  3. KafkaSink through the real worker: every delivery report is an error; what does flush() say?
  4. KafkaSink: local queue fills partway through a batch (BufferError).
  5. SentrySink (SDK backend): capture_event returns None (the SDK dropped the event).
  6. HTTPSink: the server stores the body, then closes without a status line, once.
  7. SQLiteSink: injected autocommit connection, a row the caller's table refuses mid-batch.
  8. RabbitMQSink: a channel whose basic_publish returns while the broker routes nothing.
"""
import collections, contextlib, http.server, io, json, sqlite3, sys, threading

def lines_of(err):
    return [l for l in err.getvalue().splitlines() if l.startswith("log-foundry")]

def evs(n):
    return [{"n": i, "message": f"m{i}", "level": "ERROR", "trace_id": "t", "log_id": f"id{i}"} for i in range(n)]

STOP = threading.Event(); STOP.set()      # cuts backoff waits short; changes no verdict

# ---------------------------------------------------------------- 1, 2: Postgres
from log_foundry.sinks.postgres import PostgresSink

class Cur:
    def __init__(self, conn): self.c = conn
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def executemany(self, sql, rows):
        self.c.calls += 1
        self.c.on_execute(self.c, rows)

class Conn:
    closed = False; broken = False
    def __init__(self, on_execute, on_commit, autocommit=False):
        self.table = []; self.txn = []; self.calls = 0; self.commits = 0
        self.on_execute = on_execute; self.on_commit = on_commit; self.autocommit = autocommit
    def cursor(self): return Cur(self)
    def commit(self):
        self.commits += 1
        self.on_commit(self)
    def rollback(self): self.txn = []
    def close(self): pass

def pg(label, conn, n=25, **kw):
    sink = PostgresSink("log_events", connection=conn, **kw)
    sink.log_foundry_stop_signal = STOP
    err = io.StringIO(); raised = None
    with contextlib.redirect_stderr(err):
        try: sink.emit(evs(n))
        except Exception as e: raised = type(e).__name__
    held = collections.Counter(json.loads(r[-1])["n"] for r in conn.table)
    l = sink.losses()
    print(f"{label}\n   rows_in_table={len(conn.table)} unique={len(held)} duplicate_rows={len(conn.table)-len(held)} "
          f"raised={raised} losses: failed={l.failed} dropped={l.dropped} stderr_lines={len(lines_of(err))}")

def buffered(c, rows): c.txn.extend(rows)
def commit_then_drop_once(c):
    c.table.extend(c.txn); c.txn = []
    if c.commits == 1:
        raise ConnectionError("server closed the connection unexpectedly (after COMMIT was applied)")
pg("1. PostgresSink, commit applied then connection lost once", Conn(buffered, commit_then_drop_once))

def autocommit_exec(c, rows):
    if c.calls == 2:
        raise ConnectionError("transient failure on chunk 2")
    c.table.extend(rows)                      # autocommit: each executemany is durable at once
pg("2. PostgresSink, injected autocommit connection, chunk 2 of 3 fails once (chunk_size=10)",
   Conn(autocommit_exec, lambda c: None, autocommit=True), chunk_size=10)

# ---------------------------------------------------------------- 3, 4: Kafka
from log_foundry.sinks.kafka import KafkaSink

class KErr:
    def code(self): return -192               # _MSG_TIMED_OUT

class Producer:
    def __init__(self, capacity=None, fail=True):
        self.q = []; self.capacity = capacity; self.fail = fail; self.landed = []
    def produce(self, topic, value=None, key=None, callback=None):
        if self.capacity is not None and len(self.q) >= self.capacity:
            raise BufferError("Local: Queue full")
        self.q.append((value, callback))
    def poll(self, t): return 0
    def flush(self, timeout=None):
        for value, cb in self.q:
            if self.fail: cb(KErr(), None)
            else: self.landed.append(value); cb(None, None)
        self.q = []
        return 0

import log_foundry
prod = Producer(fail=True)
sink = KafkaSink("logs", producer=prod)
err = io.StringIO()
with contextlib.redirect_stderr(err):
    log_foundry.configure(service="probe", sink=sink)
    @log_foundry.trace
    def work():
        for i in range(23): log_foundry.info("m", n=i)
    work()
    r = log_foundry.flush(timeout=10)
    h = log_foundry.health()
print(f"3. KafkaSink via worker, broker never acknowledges (25 events)\n   landed={len(prod.landed)} flush()={bool(r)} "
      f"reason={r.reason!r} failed_batches={h.failed_batches} health.sink={h.sink} stderr_lines={len(lines_of(err))}")
with contextlib.redirect_stderr(io.StringIO()):
    log_foundry.shutdown()

prod = Producer(capacity=10, fail=False)
sink = KafkaSink("logs", producer=prod)
err = io.StringIO(); raised = None
with contextlib.redirect_stderr(err):
    try: sink.emit(evs(25))
    except Exception as e: raised = type(e).__name__
    sink.close()
l = sink.losses()
print(f"4. KafkaSink, local queue full after 10 of 25\n   landed={len(prod.landed)} raised={raised} "
      f"losses: failed={l.failed} dropped={l.dropped} stderr_lines={len(lines_of(err))} (never retried)")

# ---------------------------------------------------------------- 5: Sentry SDK backend
from log_foundry.sinks.sentry import SentrySink

class SDK:
    def __init__(self): self.kept = 0; self.calls = 0
    def capture_event(self, event):
        self.calls += 1
        if self.calls % 5:            # 4 of every 5 are dropped inside the SDK (sampling, before_send, a full queue)
            return None
        self.kept += 1
        return "event-id"

sdk = SDK()
sink = SentrySink(backend="sdk", client=sdk)
err = io.StringIO(); raised = None
with contextlib.redirect_stderr(err):
    try: sink.emit(evs(25))
    except Exception as e: raised = type(e).__name__
l = sink.losses()
print(f"5. SentrySink sdk backend, the SDK keeps 1 in 5\n   sdk_kept={sdk.kept}/25 sink.sent={sink.sent} raised={raised} "
      f"losses: failed={l.failed} dropped={l.dropped} stderr_lines={len(lines_of(err))}")

# ---------------------------------------------------------------- 6: HTTP
from log_foundry.sinks.http import HTTPSink
stored = []; hits = [0]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        hits[0] += 1
        body = self.rfile.read(int(self.headers["Content-Length"]))
        stored.extend(json.loads(x)["n"] for x in body.decode().splitlines() if x)
        if hits[0] == 1:
            self.connection.close(); return            # body processed; no status line
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()
    def log_message(self, *a): pass
srv = http.server.HTTPServer(("127.0.0.1", 0), H)
t = threading.Thread(target=srv.serve_forever, daemon=True); t.start()
sink = HTTPSink(f"http://127.0.0.1:{srv.server_address[1]}/in")
sink.log_foundry_stop_signal = STOP
err = io.StringIO(); raised = None
with contextlib.redirect_stderr(err):
    try: sink.emit(evs(25))
    except Exception as e: raised = type(e).__name__
srv.shutdown(); srv.server_close()
l = sink.losses()
c = collections.Counter(stored)
print(f"6. HTTPSink, server stored the body then closed with no status line, once\n   requests={hits[0]} stored={len(stored)} "
      f"unique={len(c)} duplicate_copies={len(stored)-len(c)} raised={raised} losses: failed={l.failed} dropped={l.dropped} "
      f"stderr_lines={len(lines_of(err))}")

# ---------------------------------------------------------------- 7: SQLite autocommit
from log_foundry.sinks.sqlite import SQLiteSink
conn = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
conn.execute('CREATE TABLE log_events (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, level TEXT, service TEXT, '
             'event TEXT NOT NULL CHECK (json_extract(event, "$.n") != 17))')
cols = [r[1] for r in conn.execute("PRAGMA table_info(log_events)")]
from log_foundry.sinks import sqlite as _sq
for col in _sq._COLUMNS:
    if col not in cols: conn.execute(f"ALTER TABLE log_events ADD COLUMN {col} TEXT")
sink = SQLiteSink(":memory:", connection=conn, create_table=False)
err = io.StringIO(); raises = 0
with contextlib.redirect_stderr(err):
    for attempt in range(4):                  # the worker: 1 try + max_retries=3, whole batch each time
        try: sink.emit(evs(25)); break
        except Exception: raises += 1
rows = [json.loads(r[0])["n"] for r in conn.execute("SELECT event FROM log_events")]
c = collections.Counter(rows); l = sink.losses()
print(f"7. SQLiteSink, injected autocommit connection, row 17 refused by the caller's CHECK\n   attempts_raised={raises} rows={len(rows)} "
      f"unique={len(c)} max_copies={max(c.values()) if c else 0} losses: failed={l.failed} dropped={l.dropped} "
      f"stderr_lines={len(lines_of(err))}")

# ---------------------------------------------------------------- 8: RabbitMQ
try:
    from log_foundry.sinks.rabbitmq import RabbitMQSink
    import inspect
    print("8. RabbitMQSink signature:", inspect.signature(RabbitMQSink.__init__))
except Exception as e:
    print("8. RabbitMQSink import:", type(e).__name__, e)
