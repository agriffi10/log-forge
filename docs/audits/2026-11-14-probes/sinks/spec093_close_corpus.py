"""SPEC-093 probe: a scenario corpus for the TCP end-of-round check, with each rule's verdict.

Written on 2026-11-16 while revising SPEC-093 against 97a03f45; not one of the audit's probes. It
measures TCP, not the library, and imports nothing from it. Each scenario opens a loopback
connection, lets the writer pass its pre-round check, writes a round of frames, has the peer do one
thing, and then runs the end-of-round check under each candidate rule, on a fresh connection per
rule so that one rule's reads cannot change another's. It prints, per scenario and rule, the rule's
verdict (`count` or `silent`), what the peer really received, and `ok` when the verdict matches the
truth: `count` when a frame was lost, `silent` when every frame arrived. A scenario marked
`unknowable` loses frames after the check has run, so no rule can see it, and `silent` is expected.
Run it on each platform, for Linux in Docker:

    python -B spec093_close_corpus.py [runs]
    docker run --rm -v "$PWD":/p -w /p python:3.12-slim python -B spec093_close_corpus.py [runs]
"""

from __future__ import annotations

import platform
import select
import socket
import struct
import sys
import threading
import time

SETTLE = 0.05
FRAMES = 5


def peek(sock: socket.socket) -> str:
    """`open`, `fin` (readable with nothing to read) or `reset` (a peek that raises)."""
    try:
        if not select.select([sock], [], [], 0)[0]:
            return "open"
        return "fin" if sock.recv(1, socket.MSG_PEEK) == b"" else "open"
    except OSError:
        return "reset"


def rule_fin(sock: socket.socket) -> bool:
    """SPEC-093's first draft: count on a FIN or a reset."""
    return peek(sock) != "open"


def rule_reset(sock: socket.socket) -> bool:
    """SPEC-093's second draft: count only on a peek that raises."""
    return peek(sock) == "reset"


def rule_so_error(sock: socket.socket) -> bool:
    """Count when the socket holds a pending error, read by `SO_ERROR`, or a peek raises."""
    try:
        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
            return True
    except OSError:
        return True
    return peek(sock) == "reset"


RULES = {"fin": rule_fin, "reset": rule_reset, "so_error": rule_so_error}


class Peer:
    """The accepted end: reads into a buffer on a thread until told otherwise."""

    def __init__(self, conn: socket.socket) -> None:
        self.conn = conn
        self.buf = bytearray()
        self.reading = threading.Event()
        self.reading.set()
        self.done = False
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self) -> None:
        self.conn.settimeout(0.01)
        while not self.done:
            if not self.reading.is_set():
                time.sleep(0.005)
                continue
            try:
                data = self.conn.recv(65536)
            except TimeoutError:
                continue
            except OSError:
                return
            if not data:
                return
            self.buf += data

    def frames(self) -> int:
        return bytes(self.buf).count(b"\n")


def scenario(name: str, rule: str) -> tuple[bool, int, int]:
    """Runs one scenario under one rule: whether it counted, frames received, frames sent."""
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    cli = socket.create_connection(srv.getsockname(), timeout=1.0)
    conn, _ = srv.accept()
    peer = Peer(conn)
    sent = 0
    frames = 1 if name == "fin-then-rst" else FRAMES
    if name == "half-close-reads-all":
        conn.shutdown(socket.SHUT_WR)
        time.sleep(SETTLE)
    if name in ("rst-no-fin", "reset-after-check"):
        peer.reading.clear()
        time.sleep(SETTLE)
    if name == "fin-then-rst":
        time.sleep(SETTLE)
        peer.done = True
        conn.close()
        time.sleep(SETTLE)
    for i in range(frames):
        cli.sendall(f"frame-{i}\n".encode())
        sent += 1
    time.sleep(SETTLE)
    if name == "rst-no-fin":
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        peer.done = True
        conn.close()
        time.sleep(SETTLE)
    if name == "fin-after-reading-all":
        peer.done = True
        time.sleep(SETTLE)
        conn.close()
        time.sleep(SETTLE)
    counted = RULES[rule](cli)
    if name == "reset-after-check":
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        peer.done = True
        conn.close()
    time.sleep(SETTLE)
    received = peer.frames()
    for s in (cli, srv):
        s.close()
    if not peer.done:
        peer.done = True
        conn.close()
    return counted, received, sent


SCENARIOS = (
    ("clean", False),
    ("half-close-reads-all", False),
    ("fin-then-rst", False),
    ("rst-no-fin", False),
    ("fin-after-reading-all", False),
    ("reset-after-check", True),
)


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    print(platform.system(), platform.release(), "python", sys.version.split()[0])
    for name, unknowable in SCENARIOS:
        for rule in RULES:
            verdicts = []
            for _ in range(runs):
                counted, received, sent = scenario(name, rule)
                lost = received < sent
                right = counted == lost if not unknowable else not counted
                verdicts.append(
                    f"{'count' if counted else 'silent'}:{received}/{sent}:{'ok' if right else 'WRONG'}"
                )
            note = " (unknowable)" if unknowable else ""
            print(f"{name + note:<36} {rule:<9} {'  '.join(verdicts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
