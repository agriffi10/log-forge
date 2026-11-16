"""SPEC-093 probe: how much a peer that stops reading acknowledges, the most a later reset can lose.

Written on 2026-11-16 while revising SPEC-093 after the execution-frame review, against 97a03f45;
not one of the audit's probes. The peer accepts and never reads; we write 157-byte frames without
blocking until the send buffer is full, wait half a second, and read how many bytes the peer has
not acknowledged. The rest were acknowledged by the peer's kernel and sit unread in its receive
buffer: if the peer then reset, they would be lost after an end-of-round check had seen them
acknowledged, which is SPEC-093's residual window. It uses the transport's own `_unacked`, so run
it with a patched tree first on `PYTHONPATH`:

    PYTHONPATH=<tree>/src python -B spec093_unread_buffer.py
"""

from __future__ import annotations

import platform
import socket
import time

from log_foundry.sinks._socket import _unacked


def main() -> int:
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    cli = socket.create_connection(srv.getsockname())
    peer, _ = srv.accept()
    cli.setblocking(False)
    frame = b"x" * 156 + b"\n"
    sent = 0
    while True:
        try:
            sent += cli.send(frame)
        except BlockingIOError:
            break
    time.sleep(0.5)
    queued = _unacked(cli)
    if queued is None:
        print(platform.system(), "cannot read the send queue")
        return 1
    acked = sent - queued
    print(
        f"{platform.system()} sent {sent} bytes, unacknowledged {queued}, acknowledged and unread "
        f"{acked} bytes = {acked // len(frame)} frames of {len(frame)} bytes; peer SO_RCVBUF "
        f"{peer.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
