"""SPEC-093 probe: what a TCP writer can know when its peer closes, under five detection options.

Written on 2026-11-15 while authoring SPEC-093, against 33582484; not one of the audit's probes.
It drives a frame loop shaped like `SocketTransport._round` (one `sendall` per frame, reconnect
on a raise) against an in-process loopback listener, so it measures TCP, not the library. Each
scenario sends a first batch, has the listener close the connection the writer holds, and sends
a second batch of 50. Per option it prints what the listener received of the second batch and
how the writer would report each frame: `accepted`, `unknown` (written since the last check on
a connection then found closed) or `retried` (a raise, resent on a new connection). `silent` is
frames reported accepted that never arrived. Run from anywhere: `python -B <this file> [runs]`.
"""

from __future__ import annotations

import re
import select
import socket
import struct
import sys
import threading
import time

N = 50
OPTIONS = (
    "today",
    "check-per-round",
    "check-per-frame",
    "unknown-on-raise",
    "round+raise",
    "round+raise+end",
)


def closed_by_peer(sock: socket.socket) -> bool:
    """A zero-timeout readiness check, then a peek: readable with nothing to read is a close."""
    try:
        readable, _, _ = select.select([sock], [], [], 0)
        if not readable:
            return False
        return sock.recv(1, socket.MSG_PEEK) == b""
    except OSError:
        return True


class Listener:
    """Accepts connections; the first is read until `close_first` closes it."""

    def __init__(self, mode: str, after: int | None) -> None:
        self.mode = mode
        self.after = after
        self.srv = socket.socket()
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(8)
        self.port = self.srv.getsockname()[1]
        self.buf = bytearray()
        self.lock = threading.Lock()
        self.first: socket.socket | None = None
        self.ready = threading.Event()
        self.closed = threading.Event()
        threading.Thread(target=self.accept_loop, daemon=True).start()

    def accept_loop(self) -> None:
        count = 0
        while True:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            count += 1
            target = self.read_first if count == 1 else self.read_all
            threading.Thread(target=target, args=(conn,), daemon=True).start()

    def read_first(self, conn: socket.socket) -> None:
        self.first = conn
        self.ready.set()
        conn.settimeout(0.2)
        try:
            while not self.closed.is_set():
                try:
                    data = conn.recv(65536)
                except TimeoutError:
                    continue
                if not data:
                    break
                with self.lock:
                    self.buf += data
                    if self.after is not None and len(self.marks(b"2")) >= self.after:
                        self.close_first()
                        return
        except OSError:
            pass

    def read_all(self, conn: socket.socket) -> None:
        try:
            while data := conn.recv(65536):
                with self.lock:
                    self.buf += data
        except OSError:
            pass

    def close_first(self) -> None:
        if self.first is None or self.closed.is_set():
            return
        if self.mode == "rst":
            self.first.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.first.close()
        self.closed.set()

    def marks(self, tag: bytes) -> list[bytes]:
        return re.findall(rb"MARK-" + tag + rb"-(\d{3})", bytes(self.buf))

    def stop(self) -> None:
        self.srv.close()


def frames(tag: str, n: int) -> list[bytes]:
    return [f'{{"message": "MARK-{tag}-{i:03d}"}}\n'.encode() for i in range(n)]


def run(option: str, mode: str, after: int | None, n: int = N, gap: float = 0.0) -> dict[str, int]:
    lst = Listener(mode, after)
    sock = socket.create_connection(("127.0.0.1", lst.port), timeout=5.0)
    for frame in frames("1", 10):
        sock.sendall(frame)
    lst.ready.wait(2)
    if after is None:
        time.sleep(0.3)
        lst.close_first()
        time.sleep(0.2)
    report: dict[int, str] = {}
    since_check: list[int] = []
    if option in ("check-per-round", "round+raise", "round+raise+end") and closed_by_peer(sock):
        sock.close()
        sock = socket.create_connection(("127.0.0.1", lst.port), timeout=5.0)
    pending = list(enumerate(frames("2", n)))
    for _ in range(4):
        retry = []
        for index, frame in pending:
            if option == "check-per-frame" and closed_by_peer(sock):
                sock.close()
                sock = socket.create_connection(("127.0.0.1", lst.port), timeout=5.0)
            try:
                sock.sendall(frame)
            except OSError:
                if option in ("unknown-on-raise", "round+raise", "round+raise+end"):
                    for earlier in since_check:
                        report[earlier] = "unknown"
                since_check = []
                sock.close()
                sock = socket.create_connection(("127.0.0.1", lst.port), timeout=5.0)
                retry.append((index, frame))
                continue
            report[index] = "accepted"
            since_check.append(index)
            if gap:
                time.sleep(gap)
        if option == "round+raise+end" and since_check and closed_by_peer(sock):
            for earlier in since_check:
                report[earlier] = "unknown"
            since_check = []
            sock.close()
            sock = socket.create_connection(("127.0.0.1", lst.port), timeout=5.0)
        pending = retry
        if not pending:
            break
    time.sleep(0.4)
    got = lst.marks(b"2")
    received = {int(m) for m in got}
    sock.close()
    lst.stop()
    return {
        "received": len(received),
        "copies": len(got),
        "accepted": sum(1 for v in report.values() if v == "accepted"),
        "unknown": sum(1 for v in report.values() if v == "unknown"),
        "unknown_not_received": sum(
            1 for k, v in report.items() if v == "unknown" and k not in received
        ),
        "silent": sum(1 for k, v in report.items() if v == "accepted" and k not in received),
    }


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    print("python", sys.version.split()[0], sys.platform)
    scenarios = [
        ("between batches, FIN", "fin", None, N, 0.0),
        ("between batches, RST", "rst", None, N, 0.0),
        ("mid-batch after 10 of 50, FIN", "fin", 10, N, 0.0),
        ("mid-batch after 2 of 3, 1 ms gap", "fin", 2, 3, 0.001),
    ]
    for label, mode, after, n, gap in scenarios:
        for option in OPTIONS:
            results = [run(option, mode, after, n, gap) for _ in range(runs)]
            cols = ("received", "copies", "accepted", "unknown", "unknown_not_received", "silent")
            text = "  ".join(f"{c}=" + "/".join(str(r[c]) for r in results) for c in cols)
            print(f"{label:<33} {option:<16} {text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
