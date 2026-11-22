"""SPEC-093 FR-001 — the TCP transport's checks, held at their seams and on their edges.

``tests/test_socket_close_corpus.py`` drives the close scenarios over loopback. This file holds
what that corpus cannot show on every platform or cannot arrange: the end check's wait, driven
with ``_peer_state`` and ``_unacked`` stubbed so it runs alike wherever CI runs it; what
``ack_wait`` does to that wait on each sink; how the keyword is judged; a soft ICMP error; a peer
closed between rounds; a later write that raises; a held object that is not a socket; and UDP,
which makes no check. Serves invariants 1, 2, 3, 7, 10, 11 and 13.
"""

from __future__ import annotations

import errno
import socket
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks import _engine, _socket, logstash, syslog
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.syslog import SyslogSink

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

LINE = "SocketTransport, written to a connection its peer closed; may have landed, not retried"


class _Script:
    """A stub seam that answers from a script, the last entry repeated, and counts its calls."""

    def __init__(self, *answers: object) -> None:
        """Keeps the script.

        Args:
          *answers: One answer per call.

        Returns:
          None.

        Raises:
          None.
        """
        self.answers = list(answers)
        self.calls = 0

    def __call__(self, sock: object) -> object:
        """Answers the next entry.

        Args:
          sock: Ignored.

        Returns:
          The scripted answer.

        Raises:
          None.
        """
        answer = self.answers[min(self.calls, len(self.answers) - 1)]
        self.calls += 1
        return answer


def _stub(monkeypatch: pytest.MonkeyPatch, peer: _Script, unacked: _Script) -> None:
    """Replaces both seams ``_end_state`` reads.

    Args:
      monkeypatch: The test's patch.
      peer: The ``_peer_state`` stub.
      unacked: The ``_unacked`` stub.

    Returns:
      None.

    Raises:
      None.
    """
    monkeypatch.setattr(_socket, "_peer_state", peer)
    monkeypatch.setattr(_socket, "_unacked", unacked)


def test_the_end_check_waits_until_every_byte_is_acknowledged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seam case 1: ``_unacked`` answers 6 three times, then 0; ``open`` after the fourth read."""
    peer, unacked = _Script("open"), _Script(6, 6, 6, 0)
    _stub(monkeypatch, peer, unacked)
    assert _socket._end_state(object()) == "open"
    assert (peer.calls, unacked.calls) == (4, 4)


def test_the_end_check_returns_on_a_reset_without_waiting_further(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seam case 2: a reset on the second read is answered at once, the queue still unread."""
    peer, unacked = _Script("open", "reset"), _Script(6)
    _stub(monkeypatch, peer, unacked)
    assert _socket._end_state(object()) == "reset"
    assert (peer.calls, unacked.calls) == (2, 1)


def test_the_end_check_gives_up_at_its_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    """Seam case 3: a queue that never drains is waited on for ``_RESET_WAIT``, and no longer."""
    peer, unacked = _Script("fin"), _Script(6)
    _stub(monkeypatch, peer, unacked)
    monkeypatch.setattr(_socket, "_RESET_WAIT", 0.1)
    began = time.monotonic()
    state = _socket._end_state(object())
    took = time.monotonic() - began
    assert state == "fin"
    assert 0.1 <= took <= 1.1, took
    assert unacked.calls > 1


def test_the_end_check_does_not_wait_where_the_queue_cannot_be_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seam case 4: ``_unacked`` answering ``None`` ends the check after one read."""
    peer, unacked = _Script("open"), _Script(None)
    _stub(monkeypatch, peer, unacked)
    assert _socket._end_state(object()) == "open"
    assert (peer.calls, unacked.calls) == (1, 1)


class _Listener:
    """A loopback TCP listener that reads whatever it is sent, on every connection."""

    def __init__(self) -> None:
        """Binds, listens and accepts on its own thread.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.srv = socket.socket()
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(8)
        self.port = self.srv.getsockname()[1]
        self.conns: list[socket.socket] = []
        self.data: list[bytearray] = []
        self.accepted = threading.Semaphore(0)
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        """Accepts until closed, reading each connection on its own thread.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        while True:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            buf = bytearray()
            self.conns.append(conn)
            self.data.append(buf)
            threading.Thread(target=self._read, args=(conn, buf), daemon=True).start()
            self.accepted.release()

    @staticmethod
    def _read(conn: socket.socket, buf: bytearray) -> None:
        """Reads one connection until it closes.

        Args:
          conn: The connection.
          buf: Where its bytes go.

        Returns:
          None.

        Raises:
          None.
        """
        while True:
            try:
                data = conn.recv(65536)
            except OSError:
                return
            if not data:
                return
            buf.extend(data)

    def count(self, tag: str, conn: int | None = None) -> int:
        """Frames carrying ``tag`` received, on one connection or on all of them.

        Args:
          tag: The tag.
          conn: The connection's index, or ``None`` for all.

        Returns:
          The count.

        Raises:
          None.
        """
        bufs = self.data if conn is None else [self.data[conn]]
        return sum(bytes(buf).count(tag.encode()) for buf in bufs)

    def close(self) -> None:
        """Closes the listener and every connection.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.srv.close()
        for conn in self.conns:
            conn.close()


@pytest.fixture
def listener() -> Iterator[_Listener]:
    """A loopback listener, closed after the test.

    Args:
      None.

    Returns:
      The listener.

    Raises:
      None.
    """
    server = _Listener()
    try:
        yield server
    finally:
        server.close()


def _until(condition: Callable[[], bool], limit: float = 2.0) -> None:
    """Waits for a condition the arrangement needs.

    Args:
      condition: The condition.
      limit: Seconds at most.

    Returns:
      None.

    Raises:
      None.
    """
    end = time.monotonic() + limit
    while not condition() and time.monotonic() < end:
        time.sleep(0.001)


def _socket_sink(kind: str, port: int, **kwargs: Any) -> Any:
    """A TCP ``SyslogSink`` or socket-mode ``LogstashSink`` aimed at a local port.

    Args:
      kind: ``syslog`` or ``logstash``.
      port: The port.
      **kwargs: More constructor arguments.

    Returns:
      The sink.

    Raises:
      None.
    """
    if kind == "syslog":
        return SyslogSink("127.0.0.1", port, transport="tcp", **kwargs)
    return LogstashSink(host="127.0.0.1", port=port, **kwargs)


_KINDS = ["syslog", "logstash"]


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("given", [{}, {"ack_wait": True}], ids=["default", "true"])
def test_both_sinks_wait_at_the_end_check_by_default_and_when_told(
    kind: str, given: dict[str, Any], listener: _Listener, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Under seam case 1, each sink built with no ``ack_wait`` or ``True`` reads the queue 4 times."""
    sink = _socket_sink(kind, listener.port, **given)
    peer, unacked = _Script("open"), _Script(6, 6, 6, 0)
    _stub(monkeypatch, peer, unacked)
    try:
        sink.emit([{"message": "a"}])
    finally:
        sink.close()
    assert unacked.calls == 4


@pytest.mark.parametrize("kind", _KINDS)
def test_ack_wait_false_reads_the_peer_once_and_the_queue_never(
    kind: str, listener: _Listener, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ack_wait=False``: the end check is one ``_peer_state`` read, and ``_unacked`` never runs."""
    sink = _socket_sink(kind, listener.port, ack_wait=False)
    peer, unacked = _Script("open"), _Script(6, 6, 6, 0)
    _stub(monkeypatch, peer, unacked)
    try:
        sink.emit([{"message": "a"}])
    finally:
        sink.close()
    assert (peer.calls, unacked.calls) == (1, 0)


def _no_transport(*args: Any, **kwargs: Any) -> None:
    """Fails the test if a sink builds its transport.

    Args:
      *args: Ignored.
      **kwargs: Ignored.

    Returns:
      None.

    Raises:
      AssertionError: Always.
    """
    raise AssertionError("the transport was built before ack_wait was judged")


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("value", ["false", 0, 1.0], ids=["str", "int", "float"])
def test_a_non_bool_ack_wait_is_refused_naming_it_before_anything_is_built(
    kind: str, value: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``"false"``, ``0`` and ``1.0`` raise ``TypeError`` naming ``ack_wait`` and the type."""
    monkeypatch.setattr(syslog, "SocketTransport", _no_transport)
    monkeypatch.setattr(logstash, "SocketTransport", _no_transport)
    with pytest.raises(TypeError) as raised:
        _socket_sink(kind, 1, ack_wait=value)
    message = str(raised.value)
    assert "ack_wait" in message and type(value).__name__ in message, message


@pytest.mark.parametrize("value", [True, False])
@pytest.mark.parametrize("kind", _KINDS)
def test_a_bool_ack_wait_is_refused_for_udp(kind: str, value: bool) -> None:
    """A datagram has no acknowledgement to wait for, so ``ack_wait`` is refused naming itself."""
    with pytest.raises(ValueError, match="ack_wait"):
        if kind == "syslog":
            SyslogSink("127.0.0.1", 1, transport="udp", ack_wait=value)
        else:
            LogstashSink(host="127.0.0.1", port=1, transport="udp", ack_wait=value)


@pytest.mark.parametrize("value", [True, False])
def test_ack_wait_beside_url_is_refused_by_the_socket_only_rule(value: bool) -> None:
    """On ``LogstashSink`` in HTTP mode, the existing rule names ``ack_wait`` as socket-only."""
    with pytest.raises(ValueError, match="cannot apply ack_wait to the HTTP backend"):
        LogstashSink("http://logstash.invalid", ack_wait=value)


@pytest.mark.parametrize("value", ["false", 0, 1.0], ids=["str", "int", "float"])
def test_a_non_bool_ack_wait_beside_url_is_a_type_error_not_the_socket_only_rule(
    value: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The type is judged first, in every mode: beside ``url=`` too it is ``TypeError``."""
    monkeypatch.setattr(logstash, "HTTPSink", _no_transport)
    with pytest.raises(TypeError) as raised:
        LogstashSink("http://logstash.invalid", ack_wait=value)
    message = str(raised.value)
    assert "ack_wait" in message and type(value).__name__ in message, message


def test_ack_wait_none_is_accepted_in_every_mode() -> None:
    """``None``, the default, is accepted for TCP, for UDP, and beside ``url=``."""
    for sink in (
        SyslogSink("127.0.0.1", 1, transport="tcp", ack_wait=None),
        SyslogSink("127.0.0.1", 1, transport="udp", ack_wait=None),
        LogstashSink(host="127.0.0.1", port=1, ack_wait=None),
        LogstashSink(host="127.0.0.1", port=1, transport="udp", ack_wait=None),
        LogstashSink("http://logstash.invalid", ack_wait=None),
    ):
        sink.close()


def test_a_1x_call_builds_the_same_sink_with_the_wait_on() -> None:
    """Invariant 10: a call written before ``ack_wait`` existed waits at the end check."""
    sinks = (
        SyslogSink("127.0.0.1", 1, transport="tcp"),
        LogstashSink(host="127.0.0.1", port=1),
    )
    for sink in sinks:
        assert sink._socket._ack_wait is True
        sink.close()


class _SoftError(socket.socket):
    """A real connected socket whose ``SO_ERROR`` holds ``EHOSTUNREACH``, as Linux reports one."""

    def getsockopt(self, level: int, option: int, *args: Any) -> Any:
        """Answers ``EHOSTUNREACH`` for ``SO_ERROR``, and the real option otherwise.

        Args:
          level: The level.
          option: The option.
          *args: A buffer length, passed through.

        Returns:
          The option's value.

        Raises:
          OSError: As the real call.
        """
        if level == socket.SOL_SOCKET and option == socket.SO_ERROR:
            return errno.EHOSTUNREACH
        return super().getsockopt(level, option, *args)


def test_a_soft_icmp_error_on_a_live_connection_reads_open(
    listener: _Listener, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``EHOSTUNREACH`` is not a reset: the peer is open and the frames are ``ACCEPTED``."""
    held: list[_SoftError] = []

    def make(host: str, port: int, timeout: float) -> socket.socket:
        conn = socket.create_connection((host, port), timeout=timeout)
        soft = _SoftError(fileno=conn.detach())
        held.append(soft)
        return soft

    monkeypatch.setattr(_socket, "_make_tcp", make)
    sink = _socket_sink("logstash", listener.port)
    try:
        sink.emit([{"message": "warm"}])
        assert _socket._peer_state(held[0]) == "open"
        sink.emit([{"message": f"S-{i}"} for i in range(3)])
        _until(lambda: listener.count("S-") == 3)
    finally:
        sink.close()
    assert listener.count("S-") == 3
    assert sink.losses().failed == 0


def test_a_peer_closed_between_rounds_costs_a_new_connection_and_nothing_else(
    listener: _Listener, capsys: pytest.CaptureFixture[str]
) -> None:
    """The pre-check sees the close and replaces the connection: no count, no line."""
    sink = _socket_sink("syslog", listener.port)
    try:
        sink.emit([{"message": "first"}])
        listener.accepted.acquire(timeout=2)
        _until(lambda: listener.count("first") == 1)
        listener.conns[0].shutdown(socket.SHUT_RDWR)
        listener.conns[0].close()
        held = sink._socket._sock
        _until(lambda: _socket._peer_state(held) != "open")
        capsys.readouterr()
        sink.emit([{"message": f"N-{i}"} for i in range(4)])
        listener.accepted.acquire(timeout=2)
        _until(lambda: listener.count("N-") == 4)
    finally:
        sink.close()
    assert listener.count("N-", conn=1) == 4
    assert listener.count("N-", conn=0) == 0
    assert sink.losses().failed == 0
    assert [x for x in capsys.readouterr().err.splitlines() if "lost" in x] == []


class _Fake:
    """A held "socket" that is no socket: it records writes, and raises where told."""

    def __init__(self, writes: list[tuple[int, bytes]], number: int, raise_on: int | None) -> None:
        """Keeps the shared write log, its connection number and the write that raises.

        Args:
          writes: Every write, as ``(connection, bytes)``.
          number: This connection's number.
          raise_on: The index of this connection's write that raises ``BrokenPipeError``.

        Returns:
          None.

        Raises:
          None.
        """
        self.writes = writes
        self.number = number
        self.raise_on = raise_on
        self.sent = 0

    def sendall(self, data: bytes) -> None:
        """Records the write, or raises where told.

        Args:
          data: The frame.

        Returns:
          None.

        Raises:
          BrokenPipeError: On the scripted write.
        """
        index = self.sent
        self.sent += 1
        if index == self.raise_on:
            raise BrokenPipeError(errno.EPIPE, "Broken pipe")
        self.writes.append((self.number, data))

    def getsockopt(self, *args: Any) -> int:
        """Raises ``TypeError``, as an object that is not a real socket may.

        Args:
          *args: Ignored.

        Returns:
          Never.

        Raises:
          TypeError: Always.
        """
        raise TypeError("not a socket")

    def close(self) -> None:
        """Nothing to release.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


def test_a_held_object_whose_check_raises_type_error_reads_open() -> None:
    """Invariant 1: a check that cannot be made keeps 1.x's behaviour, ``open``."""
    fake = _Fake([], 0, None)
    assert _socket._peer_state(fake) == "open"  # type: ignore[arg-type]
    assert _socket._unacked(fake) is None  # type: ignore[arg-type]


def test_frames_on_a_connection_a_later_write_finds_closed_are_unknown_and_never_resent(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Rule 4: the frames before the raise are ``UNKNOWN`` once; the raising one is resent.

    The first connection takes frames 0 and 1, then raises ``BrokenPipeError`` on frame 2. Frames
    0 and 1 are counted once in ``failed`` on one line and never written again; frame 2 keeps its
    ``RETRYABLE`` outcome and is written again on the next connection, after frames 3 and 4. The
    stand-in raises ``TypeError`` from ``getsockopt``, so every check reads ``open`` and the
    criterion's other half, an object whose check raises keeping 1.x's ``ACCEPTED``, holds too.
    """
    writes: list[tuple[int, bytes]] = []
    made: list[_Fake] = []

    def make(host: str, port: int, timeout: float) -> _Fake:
        fake = _Fake(writes, len(made), 2 if not made else None)
        made.append(fake)
        return fake

    monkeypatch.setattr(_socket, "_make_tcp", make)
    monkeypatch.setattr(_engine, "cycle_backoff", lambda attempt: 0.0)
    sink = _socket_sink("syslog", 1)
    capsys.readouterr()
    sink.emit([{"message": f"F{i}-"} for i in range(5)])
    sink.close()
    frames = [
        (conn, next(f"F{i}-" for i in range(5) if f"F{i}-".encode() in data))
        for conn, data in writes
    ]
    assert frames == [(0, "F0-"), (0, "F1-"), (1, "F3-"), (1, "F4-"), (1, "F2-")]
    assert sink.losses().failed == 2
    lines = [x for x in capsys.readouterr().err.splitlines() if "lost" in x]
    assert len(lines) == 1 and f"lost 2 message(s); {LINE}" in lines[0], lines


def test_udp_rounds_make_no_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """A datagram is fire-and-forget: neither seam is read on a UDP round."""
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver.bind(("127.0.0.1", 0))
    port = receiver.getsockname()[1]
    peer, unacked = _Script("open"), _Script(0)
    ended = _Script("open")
    monkeypatch.setattr(_socket, "_peer_state", peer)
    monkeypatch.setattr(_socket, "_unacked", unacked)
    monkeypatch.setattr(_socket, "_end_state", ended)
    sink = SyslogSink("127.0.0.1", port, transport="udp")
    try:
        sink.emit([{"message": "a"}, {"message": "b"}])
        sink.emit([{"message": "c"}])
    finally:
        sink.close()
        receiver.close()
    assert (peer.calls, unacked.calls, ended.calls) == (0, 0, 0)
