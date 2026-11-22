"""SPEC-093 FR-001 — the TCP close corpus, driven through the real ``SocketTransport``.

Each scenario runs ``LogstashSink`` in TCP mode against a loopback peer, through the real
transport, and arranges the peer's close through the transport's two seams: ``_peer_state``, the
check before a round's first frame, and ``_end_state``, the check after its last. A hook runs the
close at the seam it names, and the rule must decide on what the kernel shows when it asks. The one
wait before a check is for a close the scenario sent to land on the connection it closed, read
without consuming it (:attr:`Seams.await_close`): never for an answer the rule itself waits for. The scenarios and their expected verdicts are
FR-001's table, ported from ``docs/audits/2026-11-14-probes/sinks/spec093_close_corpus.py``.

A row is right when no frame was lost that the round did not count and, outside the two
mid-round rows, nothing was counted that arrived. The reset-after-the-end-check row is the
residual window FR-006 records: its first round goes uncounted, and only its second round is held.
The same rows run with ``ack_wait=False`` on Linux, each awaiting its close before the end check,
since the opt-out leaves a reset in flight at its one read uncounted and a loaded runner can hold
one in flight; with the close landed, the corpus measured the unwaited check right on loopback. Elsewhere that half is skipped, since on macOS the corpus
measured it silent on three rows, which is the opt-out's recorded cost. Serves invariants 2, 7 and 11.
"""

from __future__ import annotations

import contextlib
import socket
import struct
import sys
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks import _socket
from log_foundry.sinks.logstash import LogstashSink

if TYPE_CHECKING:
    from collections.abc import Callable

LINE = "SocketTransport, written to a connection its peer closed; may have landed, not retried"


class Peer:
    """The accepted end: reads into a buffer until told to stop, then closes as asked."""

    def __init__(self, conn: socket.socket) -> None:
        """Starts reading on its own thread.

        Args:
          conn: The accepted connection.

        Returns:
          None.

        Raises:
          None.
        """
        self.conn = conn
        self.buf = bytearray()
        self.reading = True
        self.done = False
        self.lock = threading.Lock()
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        """Reads until stopped, closed or reset.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
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
        """Stops the reader and waits until it is out of its ``recv``, so it takes nothing more.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.reading = False
        time.sleep(0.02)

    def fin(self) -> None:
        """Closes the connection cleanly.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.done = True
        self.conn.close()

    def rst(self) -> None:
        """Closes the connection with a reset, discarding what it had not read.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.done = True
        self.conn.close()


class Server:
    """A loopback listener that keeps every peer it accepted."""

    def __init__(self, on_accept: Callable[[Peer], None] | None = None) -> None:
        """Binds, listens and accepts on its own thread.

        Args:
          on_accept: Called with each peer as it is accepted.

        Returns:
          None.

        Raises:
          None.
        """
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(8)
        self.port = self.srv.getsockname()[1]
        self.peers: list[Peer] = []
        self.on_accept = on_accept
        self.accepted = threading.Event()
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        """Accepts until closed.

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
            peer = Peer(conn)
            if self.on_accept:
                self.on_accept(peer)
            self.peers.append(peer)
            self.accepted.set()

    def received(self, tag: str) -> int:
        """How many frames carrying ``tag`` reached any peer.

        Args:
          tag: The frame tag.

        Returns:
          The count.

        Raises:
          None.
        """
        return sum(bytes(p.buf).count(tag.encode()) for p in self.peers)

    def close(self) -> None:
        """Closes the listener and every peer.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """
        self.srv.close()
        for p in self.peers:
            p.done = True
            with contextlib.suppress(OSError):
                p.conn.close()


def until(condition: Callable[[], bool], limit: float = 2.0) -> None:
    """Waits for a condition the scenario needs, or for a close the scenario sent to land.

    Args:
      condition: The condition.
      limit: Seconds to wait at most.

    Returns:
      None.

    Raises:
      None.
    """
    end = time.monotonic() + limit
    while not condition() and time.monotonic() < end:
        time.sleep(0.0005)


class Seams:
    """Wraps the two seams so a scenario's hook runs at the moment it names.

    ``_end_state`` calls ``_peer_state`` itself, so a thread-local marks the end check and the
    pre-check's hooks do not fire inside it.

    Attributes:
      hooks: The hooks by moment, each run once.
      ack_wait: The sink's ``ack_wait``, which a scenario reads to decide whether to await.
      await_close: Whether the end check, after its ``before_end`` hook, first waits up to 2 s
        until :func:`_close_landed` says the peer's close has reached the round's connection. A
        scenario whose peer closes before the end check sets it: for ``ack_wait=False``, which
        reads the connection once and leaves a reset still in flight at that read uncounted, as
        SPEC-093 says of the opt-out; and on the mid-round rows in both modes, since a peer's
        kernel acknowledges frames its reader has not taken, so a round whose every frame was
        acknowledged and then reset can have the reset in flight at the check, the window FR-006
        records for both modes. CI read 850 frames lost and none counted that way on the
        ``fin-no-ack-wait`` mid-round row, and the system-frame review reproduced it on
        ``fin-ack-wait`` in 1 of 250 loaded runs. Awaiting makes each such row the promise the
        rule keeps, that a close which has landed by the end of the round is counted. The other
        ``ack_wait=True`` rows do not await, so they still prove the sink's own wait.
      closed_sock: The socket the first pre-check outside an end check was given: the warm
        connection, which is the one a scenario's peer closes. A write that raised moves the run
        to a new connection, and a later round of the same run, which the engine runs to resend,
        checks that one; neither replaces it, so only the closed connection is awaited.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, ack_wait: bool = True) -> None:
        """Installs the wrappers.

        Args:
          monkeypatch: The test's patch, which restores both seams.
          ack_wait: The sink's ``ack_wait``.

        Returns:
          None.

        Raises:
          None.
        """
        self.hooks: dict[str, Callable[[], None]] = {}
        self.ack_wait = ack_wait
        self.await_close = False
        self.closed_sock: socket.socket | None = None
        self._peer_state = _socket._peer_state
        self._end_state = _socket._end_state
        self._inside_end = threading.local()
        monkeypatch.setattr(_socket, "_peer_state", self.peer_state)
        monkeypatch.setattr(_socket, "_end_state", self.end_state)

    def _fire(self, name: str) -> None:
        """Runs and forgets the hook named ``name``, if any.

        Args:
          name: ``before_pre``, ``after_pre``, ``before_end`` or ``after_end``.

        Returns:
          None.

        Raises:
          None.
        """
        hook = self.hooks.pop(name, None)
        if hook is not None:
            hook()

    def peer_state(self, sock: socket.socket) -> Any:
        """The pre-check, between its hooks.

        Args:
          sock: The held socket.

        Returns:
          What the real seam answers.

        Raises:
          None.
        """
        if getattr(self._inside_end, "on", False):
            return self._peer_state(sock)
        if self.closed_sock is None:
            self.closed_sock = sock
        self._fire("before_pre")
        state = self._peer_state(sock)
        self._fire("after_pre")
        return state

    def end_state(self, sock: socket.socket, *args: Any) -> Any:
        """The end check, between its hooks.

        Args:
          sock: The held socket.
          *args: Passed through.

        Returns:
          What the real seam answers.

        Raises:
          None.
        """
        self._fire("before_end")
        if self.await_close:
            until(lambda: _close_landed(sock, self.closed_sock), 2.0)
        self._inside_end.on = True
        try:
            state = self._end_state(sock, *args)
        finally:
            self._inside_end.on = False
        self._fire("after_end")
        return state


def _cur(srv: Server) -> Peer:
    """The peer of the connection the sink holds now.

    Args:
      srv: The server.

    Returns:
      The latest peer.

    Raises:
      None.
    """
    return srv.peers[-1]


def _fin_after_pre(seams: Seams, srv: Server) -> None:
    """The peer closes (FIN) after the pre-check, before any frame of the round reaches it.

    Args:
      seams: The seams.
      srv: The server.

    Returns:
      None.

    Raises:
      None.
    """

    def hook() -> None:
        _cur(srv).stop_reading()
        _cur(srv).fin()

    seams.hooks["after_pre"] = hook
    seams.await_close = not seams.ack_wait


def _rst_before_end(seams: Seams, srv: Server) -> None:
    """The peer stops reading, then resets just before the end check, frames unread.

    Args:
      seams: The seams.
      srv: The server.

    Returns:
      None.

    Raises:
      None.
    """
    _cur(srv).stop_reading()
    seams.hooks["before_end"] = lambda: _cur(srv).rst()
    seams.await_close = not seams.ack_wait


def _fin_before_end_read_all(n: int) -> Callable[[Seams, Server], None]:
    """The peer reads every frame of the round, then closes (FIN) just before the end check.

    Args:
      n: The round's frames.

    Returns:
      The setup.

    Raises:
      None.
    """

    def setup(seams: Seams, srv: Server) -> None:
        def hook() -> None:
            until(lambda: srv.received("R0X") >= n)
            _cur(srv).fin()

        seams.hooks["before_end"] = hook

    return setup


def _rst_after_end(seams: Seams, srv: Server) -> None:
    """The peer stops reading and resets just after the end check: the residual window.

    Args:
      seams: The seams.
      srv: The server.

    Returns:
      None.

    Raises:
      None.
    """
    _cur(srv).stop_reading()
    seams.hooks["after_end"] = lambda: _cur(srv).rst()


def _half_close(peer: Peer) -> None:
    """The peer shuts down its write side and keeps reading.

    Args:
      peer: The accepted peer.

    Returns:
      None.

    Raises:
      None.
    """
    peer.conn.shutdown(socket.SHUT_WR)


_TCP_STATE = (0x106, 0, 5) if sys.platform == "darwin" else (11, 7, 8)
"""``TCP_INFO`` (``TCP_CONNECTION_INFO`` on macOS), then its state byte's close and close-wait.

The reply to that option opens with the connection's state: ``TCP_CLOSE`` and ``TCP_CLOSE_WAIT``
on Linux, ``TCPS_CLOSED`` and ``TCPS_CLOSE_WAIT`` on macOS.
"""


def _close_landed(sock: socket.socket, closed: socket.socket | None) -> bool:
    """Whether the close the scenario's peer sent has reached the connection it closed.

    Read through the connection's TCP state and :func:`~log_foundry.sinks._socket._unacked`,
    never through ``SO_ERROR`` or a peek, both of which clear a pending reset that the end check
    under test must still find. The close has landed once a reset has closed the connection, or
    once its FIN has and every byte is acknowledged, since a peer that closed before frames
    arrived answers them with a reset and never with an acknowledgement. An acknowledgement alone
    is not the answer: the peer's kernel acknowledges frames its reader has not taken, before it
    closes. A held socket that is not the closed connection lands at once, since a write that
    raised moved the run and the raise rule decided it; so does an ``OSError``, or an
    ``_unacked`` that answers ``None``, and the end check then runs and decides.

    Args:
      sock: The socket the end check was given.
      closed: The connection the peer closed.

    Returns:
      ``True`` once the close has landed.

    Raises:
      None.
    """
    if sock is not closed:
        return True
    option, close, close_wait = _TCP_STATE
    try:
        state = sock.getsockopt(socket.IPPROTO_TCP, option, 8)[0]
    except OSError:
        return True
    return state == close or (state == close_wait and not _socket._unacked(sock))


def _mid_round(mode: str, after: int) -> Callable[[Seams, Server], None]:
    """The peer closes or resets on its own once it has read ``after`` bytes of the round.

    The close runs on a watcher thread, and the end check waits for that thread before it asks,
    so the close lands before the check on every run. Left to its own schedule, the watcher can
    wake after the round's end check on a loaded runner, which turns the row into the
    reset-after-the-end-check window, uncounted by design: CI read 617 frames lost and none
    counted that way on the ``rst-no-ack-wait`` row.

    Joining the watcher orders the peer's ``close()``, not the arrival of what it sends, so the
    row also sets :attr:`Seams.await_close` in both modes, and the end check reads the close once
    it has landed.

    Args:
      mode: ``fin`` or ``rst``.
      after: Bytes read before the close.

    Returns:
      The setup.

    Raises:
      None.
    """

    def setup(seams: Seams, srv: Server) -> None:
        peer = _cur(srv)
        base = len(peer.buf)

        def watch() -> None:
            until(lambda: len(peer.buf) - base >= after, 5.0)
            if mode == "rst":
                peer.rst()
            else:
                peer.fin()

        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        seams.hooks["before_end"] = lambda: watcher.join(6.0)
        seams.await_close = True

    return setup


class Run:
    """What one scenario's rounds showed: frames received, counted and announced, per round.

    Attributes:
      n: Frames per round.
      got: Frames of each round any peer received.
      counted: What each round added to ``failed``.
      lines: The ``lost`` lines written over the scenario.
      resent: The ``resent`` lines, SPEC-095's count of a resend after a write that may have
        landed, which is not a loss and is kept apart from ``lines``.
    """

    def __init__(
        self, n: int, got: list[int], counted: list[int], lines: list[str], resent: list[str]
    ) -> None:
        """Keeps the figures.

        Args:
          n: Frames per round.
          got: Received per round.
          counted: Counted per round.
          lines: The loss lines.
          resent: The resend lines.

        Returns:
          None.

        Raises:
          None.
        """
        self.n = n
        self.resent = resent
        self.got = got
        self.counted = counted
        self.lines = lines

    def lost(self, r: int) -> int:
        """Frames of round ``r`` no peer received.

        Args:
          r: The round.

        Returns:
          The count.

        Raises:
          None.
        """
        return self.n - min(self.got[r], self.n)


def _run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    ack_wait: bool,
    *,
    n: int = 5,
    setup: Callable[[Seams, Server], None] | None = None,
    on_accept: Callable[[Peer], None] | None = None,
    rounds: int = 1,
) -> Run:
    """Runs one scenario: a warm frame, the setup, then ``rounds`` rounds of ``n`` frames.

    Args:
      monkeypatch: The test's patch.
      capsys: The test's capture.
      ack_wait: The sink's ``ack_wait``.
      n: Frames per round.
      setup: Arranges the close, once the connection is warm.
      on_accept: Applied to each accepted peer.
      rounds: The rounds.

    Returns:
      What the rounds showed.

    Raises:
      None.
    """
    seams = Seams(monkeypatch, ack_wait)
    srv = Server(on_accept)
    sink = LogstashSink(host="127.0.0.1", port=srv.port, transport="tcp", ack_wait=ack_wait)
    try:
        sink.emit([{"message": "warm"}])
        srv.accepted.wait(2)
        until(lambda: srv.received("warm") == 1)
        capsys.readouterr()
        if setup is not None:
            setup(seams, srv)
        counted: list[int] = []
        for r in range(rounds):
            before = sink.losses().failed
            with contextlib.suppress(Exception):
                sink.emit([{"message": f"R{r}X-{i:04d}"} for i in range(n)])
            counted.append(sink.losses().failed - before)
        until(lambda: sum(srv.received(f"R{r}X") for r in range(rounds)) >= n * rounds, 0.3)
        got = [srv.received(f"R{r}X") for r in range(rounds)]
    finally:
        sink.close()
        srv.close()
    err = capsys.readouterr().err.splitlines()
    lines = [x for x in err if ": lost " in x]
    return Run(n, got, counted, lines, [x for x in err if ": resent " in x])


_ACK_WAIT = [
    pytest.param(True, id="ack-wait"),
    pytest.param(
        False,
        id="no-ack-wait",
        marks=pytest.mark.skipif(
            not sys.platform.startswith("linux"),
            reason="the unwaited check, once the close lands, is right on Linux loopback only",
        ),
    ),
]


@pytest.mark.parametrize("ack_wait", _ACK_WAIT)
@pytest.mark.parametrize(
    ("n", "setup", "on_accept", "rounds"),
    [
        pytest.param(5, None, None, 1, id="clean"),
        pytest.param(5, None, _half_close, 3, id="half-close-3-rounds"),
        pytest.param(5, _fin_before_end_read_all(5), None, 1, id="fin-before-end-all-read"),
    ],
)
def test_a_peer_that_received_everything_draws_no_count_and_no_line(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    ack_wait: bool,
    n: int,
    setup: Any,
    on_accept: Any,
    rounds: int,
) -> None:
    """Clean, half-close and FIN-after-reading: every frame arrives, so nothing is counted."""
    run = _run(monkeypatch, capsys, ack_wait, n=n, setup=setup, on_accept=on_accept, rounds=rounds)
    assert run.got == [n] * rounds, run.got
    assert run.counted == [0] * rounds, run.counted
    assert run.lines == [] and run.resent == [], (run.lines, run.resent)


@pytest.mark.parametrize("ack_wait", _ACK_WAIT)
@pytest.mark.parametrize(
    ("n", "setup"),
    [
        pytest.param(1, _fin_after_pre, id="fin-after-pre-1-frame"),
        pytest.param(5, _fin_after_pre, id="fin-after-pre-5-frames"),
        pytest.param(5, _rst_before_end, id="rst-before-end-unread"),
    ],
)
def test_frames_a_closed_peer_discarded_are_every_one_counted_on_one_line(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    ack_wait: bool,
    n: int,
    setup: Any,
) -> None:
    """FIN after the pre-check and RST before the end check: every lost frame counted, once.

    Where the peer's reset reaches the sink before the round's next write, as on Linux loopback,
    that write raises, so the frames before it are ``UNKNOWN`` by rule 4 and the raising frame and
    those after it go out again on a new connection and arrive; where it lands later, as on
    macOS, every frame of the round is written to the dead connection and the end check counts
    them all. Either way the round is right as the corpus judges one: every frame the peer lost
    is counted, nothing it received is, and the count is named on one line.
    """
    run = _run(monkeypatch, capsys, ack_wait, n=n, setup=setup)
    lost = run.lost(0)
    assert lost >= 1, (run.got, run.counted)
    assert run.counted == [lost], (run.got, run.counted)
    assert len(run.lines) == 1, run.lines
    assert f"lost {lost} message(s); {LINE}" in run.lines[0]


@pytest.mark.parametrize("ack_wait", _ACK_WAIT)
def test_a_reset_after_the_end_check_leaves_only_the_residual_round_uncounted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], ack_wait: bool
) -> None:
    """The residual window: round one uncounted, round two's frames received or counted."""
    run = _run(monkeypatch, capsys, ack_wait, n=5, setup=_rst_after_end, rounds=2)
    assert run.counted[0] == 0, run.counted
    assert run.got[1] + run.counted[1] >= run.n, (run.got, run.counted)


@pytest.mark.parametrize("ack_wait", _ACK_WAIT)
@pytest.mark.parametrize("mode", ["fin", "rst"])
def test_a_close_mid_round_loses_no_frame_it_does_not_count(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    ack_wait: bool,
    mode: str,
) -> None:
    """A close after 2,000 bytes of 1,000 frames: no more frames lost than counted."""
    run = _run(monkeypatch, capsys, ack_wait, n=1000, setup=_mid_round(mode, 2000))
    assert run.lost(0) <= run.counted[0], (run.got, run.counted)
