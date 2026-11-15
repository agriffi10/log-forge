"""Socket-transport probe at a0f44b5f: TCP write success is not delivery; UDP is nothing.

In-process loopback listener (no external service). Batch 1 lands. The peer then closes the
connection the sink still holds (a collector restart or an idle timeout). Batch 2 of N events is
emitted; the listener counts what it actually received on any connection.
"""
import contextlib, io, re, socket, sys, threading, time
from log_foundry.sinks.syslog import SyslogSink
from log_foundry.sinks.logstash import LogstashSink

N = 50


class Listener:
    def __init__(self, mode):
        self.mode = mode                     # "fin" | "rst" | "stall"
        self.srv = socket.socket(); self.srv.bind(("127.0.0.1", 0)); self.srv.listen(8)
        self.port = self.srv.getsockname()[1]
        self.buf = bytearray(); self.lock = threading.Lock()
        self.first = None; self.first_ready = threading.Event()
        self.stop = False
        self.t = threading.Thread(target=self.accept_loop, daemon=True); self.t.start()

    def accept_loop(self):
        n = 0
        while not self.stop:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            n += 1
            if n == 1:
                self.first = conn; self.first_ready.set()
                threading.Thread(target=self.read_first, args=(conn,), daemon=True).start()
            else:
                threading.Thread(target=self.read_all, args=(conn,), daemon=True).start()

    def read_first(self, conn):
        conn.settimeout(0.3)
        try:
            while True:
                data = conn.recv(65536)
                if not data: break
                with self.lock: self.buf += data
        except OSError:
            pass

    def read_all(self, conn):
        try:
            while True:
                data = conn.recv(65536)
                if not data: break
                with self.lock: self.buf += data
        except OSError:
            pass

    def kill_first(self):
        time.sleep(0.5)                       # read_first has timed out and stopped reading
        if self.mode == "rst":
            import struct
            self.first.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.first.close()
        time.sleep(0.2)

    def marks(self):
        time.sleep(0.5)
        with self.lock:
            return re.findall(rb"MARK-(\d)-(\d{3})", bytes(self.buf))

    def close(self):
        self.stop = True; self.srv.close()


def batch(tag, n):
    return [{"message": f"MARK-{tag}-{i:03d}", "level": "INFO", "n": i} for i in range(n)]


def run(label, make, mode):
    lst = Listener(mode)
    sink = make(lst.port)
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        sink.emit(batch(1, 10))
        lst.first_ready.wait(2)
        lst.kill_first()
        raised = None
        try:
            sink.emit(batch(2, N))
        except Exception as e:                       # noqa: BLE001
            raised = type(e).__name__
        got = lst.marks()
        sink.close()
    lst.close()
    b1 = {m for t, m in got if t == b"1"}; b2 = [m for t, m in got if t == b"2"]
    losses = sink.losses()
    lines = [l for l in err.getvalue().splitlines() if l.startswith("log-foundry")]
    print(f"{label:<44} batch1 received={len(b1)}/10  batch2 received={len(set(b2))}/{N} "
          f"(copies={len(b2)})  missing={N-len(set(b2))}  raised={raised}  "
          f"losses: failed={losses.failed} dropped={losses.dropped}  stderr_lines={len(lines)}")


print("python", sys.version.split()[0], sys.platform)
for mode in ("fin", "rst"):
    run(f"SyslogSink tcp, peer closes ({mode})", lambda p: SyslogSink("127.0.0.1", p, transport="tcp"), mode)
    run(f"LogstashSink tcp, peer closes ({mode})", lambda p: LogstashSink(host="127.0.0.1", port=p, transport="tcp"), mode)

# UDP to a port nobody listens on.
probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); probe.bind(("127.0.0.1", 0))
dead = probe.getsockname()[1]; probe.close()
sink = SyslogSink("127.0.0.1", dead)                 # transport defaults to udp
err = io.StringIO(); raised = None
with contextlib.redirect_stderr(err):
    try:
        sink.emit(batch(3, N)); sink.emit(batch(4, N))
    except Exception as e:                           # noqa: BLE001
        raised = type(e).__name__
losses = sink.losses()
print(f"{'SyslogSink udp (default), no listener':<44} sent=2x{N} received=0  raised={raised}  "
      f"losses: failed={losses.failed} dropped={losses.dropped}  stderr_lines={len(err.getvalue().splitlines())}")
sink.close()
