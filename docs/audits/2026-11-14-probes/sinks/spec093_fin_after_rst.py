"""SPEC-093 probe: what a socket shows after its peer closed and our next frame drew a reset.

Written on 2026-11-16 while revising SPEC-093 after the execution-frame review, against 97a03f45;
not one of the audit's probes. The peer reads a first frame and closes; we write a second frame,
which the peer's kernel answers with a reset. Then, every 50 ms for half a second, it prints the
pending socket error, the bytes the peer has not acknowledged (Linux `SIOCOUTQ`, macOS
`SO_NWRITE`), whether the socket is readable, and what a one-byte peek reads. It shows that the
error is visible once, to its first reader, and that the second frame is never acknowledged:

    python -B spec093_fin_after_rst.py
"""

from __future__ import annotations

import select
import socket
import struct
import sys
import time


def unacked(sock: socket.socket) -> int | None:
    """Bytes the peer's kernel has not acknowledged, where the platform can say."""
    if sys.platform == "darwin":
        return int(sock.getsockopt(socket.SOL_SOCKET, 0x1024))
    try:
        import fcntl
        import termios
    except ImportError:
        return None
    raw = fcntl.ioctl(sock.fileno(), termios.TIOCOUTQ, b"\0" * 4)
    return int(struct.unpack("i", raw)[0])


def main() -> int:
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    cli = socket.create_connection(srv.getsockname(), timeout=5.0)
    peer, _ = srv.accept()
    cli.sendall(b"warm\n")
    peer.recv(100)
    peer.close()
    began = time.monotonic()
    cli.sendall(b"frame\n")
    for _ in range(11):
        err = cli.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        readable = bool(select.select([cli], [], [], 0)[0])
        try:
            peek: object = cli.recv(1, socket.MSG_PEEK) if readable else None
        except OSError as exc:
            peek = type(exc).__name__
        elapsed = (time.monotonic() - began) * 1000
        print(
            f"{elapsed:7.1f} ms SO_ERROR={err} unacked={unacked(cli)} "
            f"readable={readable} peek={peek!r}"
        )
        time.sleep(0.05)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
