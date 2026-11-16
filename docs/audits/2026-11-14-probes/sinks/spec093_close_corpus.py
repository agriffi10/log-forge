"""SPEC-093 probe: the TCP close corpus, driven through the real SocketTransport with no settle.

Written on 2026-11-16 while revising SPEC-093 against 97a03f45; not one of the audit's probes.
Rebuilt after the execution-frame review: the first version modelled the transport and slept 50 ms
before each check, which hid the timing that decides the rule. This one drives `LogstashSink` in
TCP mode, from whichever tree is first on `PYTHONPATH` (apply `spec093_prototype.patch` to one),
against a loopback peer. It arranges each scenario through the transport's two seams,
`_socket._peer_state` (the pre-round check) and `_socket._end_state` (the end-of-round check), and
never sleeps to let a packet arrive. `--wait SECONDS` sets `_socket._RESET_WAIT`, so `--wait 0`
measures the rule without the wait for a reset. Per scenario and run it prints what the peer
received, what the sink counted, what each check read, and `ok`, `SILENT-LOSS` or `FALSE-COUNT`; a
scenario marked `unknowable` expects its first round's loss to go uncounted, which it prints as
`residual`, and every later round to be right. A round loses frames silently when more frames
went missing than it counted, and counts falsely when it counted more than went missing, except
where the peer closes mid-round: a close with frames still arriving becomes a reset, which
discards whatever the peer had not read, and nothing the sender sees says how much that was, so
frames it had read may honestly read `UNKNOWN`, which prints as `unknown-but-read`. It also prints
the median time of one round, which is what the end-of-round check costs. Run it on macOS and on Linux, under a delay:

    PYTHONPATH=<tree>/src python -B spec093_close_corpus.py [runs] [--wait SECONDS]
    docker run --rm --cap-add NET_ADMIN -v ...:/w -w /w <image with iproute2> sh -c \\
        'tc qdisc add dev lo root netem delay 10ms; PYTHONPATH=<tree>/src python -B ...'
"""

from __future__ import annotations

import contextlib
import io
import platform
import socket
import struct
import sys
import threading
import time

from log_foundry.sinks import _socket as S
from log_foundry.sinks.logstash import LogstashSink

HOOKS: dict[str, object] = {}
TIMES: list[float] = []
LOG: list[tuple[str, str]] = []
_peer_state = S._peer_state
_end_state = S._end_state
_inside_end = threading.local()


def peer_state(sock: socket.socket) -> str:
    if getattr(_inside_end, "on", False):
        return _peer_state(sock)
    hook = HOOKS.pop("before_pre", None)
    if hook:
        hook()
    state = _peer_state(sock)
    LOG.append(("pre", state))
    hook = HOOKS.pop("after_pre", None)
    if hook:
        hook()
    return state


def end_state(sock: socket.socket) -> str:
    hook = HOOKS.pop("before_end", None)
    if hook:
        hook()
    _inside_end.on = True
    try:
        state = _end_state(sock)
    finally:
        _inside_end.on = False
    LOG.append(("end", state))
    hook = HOOKS.pop("after_end", None)
    if hook:
        hook()
    return state


S._peer_state = peer_state
S._end_state = end_state


class Peer:
    """The accepted end: reads into a buffer until told to stop or close."""

    def __init__(self, conn: socket.socket) -> None:
        self.conn = conn
        self.buf = bytearray()
        self.reading = True
        self.done = False
        self.lock = threading.Lock()
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self) -> None:
        self.conn.settimeout(0.005)
        while not self.done:
            if not self.reading:
                time.sleep(0.001)
                continue
            try:
                data = self.conn.recv(65536)
            except TimeoutError:
                continue
            except OSError:
                return
            if not data:
                return
            with self.lock:
                self.buf += data

    def stop_reading(self) -> None:
        """Stops the reader and waits until it is out of its `recv`, so it takes nothing more."""
        self.reading = False
        time.sleep(0.02)

    def fin(self) -> None:
        self.done = True
        self.conn.close()

    def rst(self) -> None:
        self.conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.done = True
        self.conn.close()


class Server:
    def __init__(self, on_accept=None) -> None:
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(8)
        self.port = self.srv.getsockname()[1]
        self.peers: list[Peer] = []
        self.on_accept = on_accept
        self.accepted = threading.Event()
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self) -> None:
        while True:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            peer = Peer(conn)
            if self.on_accept:
                self.on_accept(peer)
            self.peers.append(peer)
            self.accepted.set()

    def received(self, tag: str) -> int:
        return sum(bytes(p.buf).count(tag.encode()) for p in self.peers)

    def close(self) -> None:
        self.srv.close()
        for p in self.peers:
            p.done = True
            with contextlib.suppress(OSError):
                p.conn.close()


def until(condition, limit: float = 2.0) -> None:
    """Waits for a condition the scenario needs, never for a packet the rule must see."""
    end = time.monotonic() + limit
    while not condition() and time.monotonic() < end:
        time.sleep(0.0005)


def run(name, n=5, setup=None, on_accept=None, rounds=1, unknowable=False, ambiguous=False):
    HOOKS.clear()
    LOG.clear()
    srv = Server(on_accept)
    sink = LogstashSink(host="127.0.0.1", port=srv.port, transport="tcp")
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        sink.emit([{"message": "warm"}])
        srv.accepted.wait(2)
        until(lambda: srv.received("warm") == 1)
        if setup:
            setup(srv)
        LOG.clear()
        counted = []
        took = []
        for r in range(rounds):
            before = sink.losses().failed
            began = time.monotonic()
            with contextlib.suppress(Exception):
                sink.emit([{"message": f"R{r}X-{i:04d}"} for i in range(n)])
            took.append(time.monotonic() - began)
            counted.append(sink.losses().failed - before)
        until(lambda: False, 0.3)
        got = [srv.received(f"R{r}X") for r in range(rounds)]
        sink.close()
    srv.close()
    TIMES.extend(took)
    verdicts = []
    for r in range(rounds):
        lost = n - min(got[r], n)
        if unknowable and r == 0:
            verdicts.append("residual" if lost > counted[r] else "ok")
        elif lost > counted[r]:
            verdicts.append("SILENT-LOSS")
        elif counted[r] > lost:
            verdicts.append("unknown-but-read" if ambiguous else "FALSE-COUNT")
        else:
            verdicts.append("ok")
    quiet = ("ok", "residual", "unknown-but-read")
    bad = [v for v in verdicts if v not in quiet]
    verdict = bad[0] if bad else next((v for v in verdicts if v != "ok"), "ok")
    return verdict, f"recv={got}x{n} counted={counted} checks={LOG}"


def cur(srv: Server) -> Peer:
    return srv.peers[-1]


def fin_after_pre(srv):
    def hook():
        cur(srv).stop_reading()
        cur(srv).fin()

    HOOKS["after_pre"] = hook


def rst_before_end(srv):
    cur(srv).stop_reading()

    def hook():
        cur(srv).rst()

    HOOKS["before_end"] = hook


def fin_before_end_read_all(n):
    def setup(srv):
        def hook():
            until(lambda: srv.received("R0X") >= n)
            cur(srv).fin()

        HOOKS["before_end"] = hook

    return setup


def rst_after_end(srv):
    cur(srv).stop_reading()

    def hook():
        cur(srv).rst()

    HOOKS["after_end"] = hook


def half_close(peer: Peer) -> None:
    peer.conn.shutdown(socket.SHUT_WR)


def natural(mode: str, after: int):
    def setup(srv):
        peer = cur(srv)
        base = len(peer.buf)

        def watch():
            until(lambda: len(peer.buf) - base >= after, 5.0)
            if mode == "rst":
                peer.rst()
            else:
                peer.fin()

        threading.Thread(target=watch, daemon=True).start()

    return setup


SCENARIOS = (
    ("clean", {"n": 5}),
    ("half-close, 3 rounds", {"on_accept": half_close, "rounds": 3}),
    ("FIN after the pre-check, 1 frame", {"n": 1, "setup": fin_after_pre}),
    ("FIN after the pre-check, 5 frames", {"n": 5, "setup": fin_after_pre}),
    ("RST before the end check, frames unread", {"n": 5, "setup": rst_before_end}),
    ("FIN before the end check, all read", {"n": 5, "setup": fin_before_end_read_all(5)}),
    (
        "RST after the end check (unknowable)",
        {"n": 5, "setup": rst_after_end, "rounds": 2, "unknowable": True},
    ),
    (
        "peer FINs mid-round after 2000 bytes",
        {"n": 1000, "setup": natural("fin", 2000), "ambiguous": True},
    ),
    (
        "peer RSTs mid-round after 2000 bytes",
        {"n": 1000, "setup": natural("rst", 2000), "ambiguous": True},
    ),
)


def main() -> int:
    args = sys.argv[1:]
    if "--wait" in args:
        i = args.index("--wait")
        S._RESET_WAIT = float(args[i + 1])
        del args[i : i + 2]
    runs = int(args[0]) if args else 5
    print(
        platform.system(),
        platform.release(),
        "python",
        sys.version.split()[0],
        "wait",
        S._RESET_WAIT,
    )
    for name, kw in SCENARIOS:
        TIMES.clear()
        verdicts = [run(name, **kw) for _ in range(runs)]
        tally: dict[str, int] = {}
        for v, _ in verdicts:
            tally[v] = tally.get(v, 0) + 1
        median = sorted(TIMES)[len(TIMES) // 2] * 1000
        print(f"{name:<44} {tally}  median round {median:.1f} ms")
        for v, detail in verdicts:
            if v != "ok":
                print(f"    {v}: {detail[:200]}")
                break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
