"""SPEC-009 — SyslogSink RFC 5424 framing over UDP and octet-counted TCP (fake socket)."""

from __future__ import annotations

import pytest

import log_foundry.sinks._socket as socket_mod
from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.syslog import SyslogSink
from test_sinks_logstash import FakeSocket


def test_is_a_sink(monkeypatch) -> None:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: FakeSocket())
    assert isinstance(SyslogSink("loghost"), Sink)


def test_udp_frame_is_rfc5424_with_derived_pri(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    sink = SyslogSink("loghost", 514, transport="udp", facility="local0")
    sink.emit([{"level": "ERROR", "timestamp": "2026-07-11T00:00:00.000Z", "message": "boom"}])
    data, addr = fake.sent[0]
    text = data.decode("utf-8")
    # local0=16, ERROR severity=3 -> PRI = 16*8 + 3 = 131
    assert text.startswith("<131>1 2026-07-11T00:00:00.000Z ")
    assert '"message": "boom"' in text
    assert addr == ("loghost", 514)


def test_tcp_uses_octet_counted_framing(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: fake)
    SyslogSink("loghost", transport="tcp", facility="user").emit(
        [{"level": "INFO", "message": "x"}]
    )
    frame = fake.sent[0]
    prefix, _, message = frame.partition(b" ")
    assert prefix.isdigit()
    assert len(message) == int(prefix)  # the count prefixes exactly the message byte length


def test_unknown_level_uses_default_severity(monkeypatch) -> None:
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    SyslogSink("loghost", facility="user").emit([{"level": "WEIRD", "message": "x"}])
    # user=1, default severity=5 -> PRI = 1*8 + 5 = 13
    assert fake.sent[0][0].decode("utf-8").startswith("<13>1 ")


def test_invalid_facility_raises() -> None:
    with pytest.raises(ValueError):
        SyslogSink("loghost", facility="bogus")


# -- SPEC-031 FR-002: the UDP address family is resolved, not assumed ------------------------


def _loopback_receiver(family: int):
    """Bind a real UDP socket on the loopback of one family, or skip if it is unavailable."""
    import socket as socket_lib

    address = "::1" if family == socket_lib.AF_INET6 else "127.0.0.1"
    try:
        sock = socket_lib.socket(family, socket_lib.SOCK_DGRAM)
        sock.bind((address, 0))
    except OSError as exc:  # pragma: no cover - depends on the host's stack
        pytest.skip(f"no {address} loopback here: {exc}")
    sock.settimeout(5.0)
    return sock, sock.getsockname()[1]


def test_make_udp_picks_the_family_the_host_resolves_to() -> None:
    """The defect itself: an unconditional AF_INET made every IPv6 send fail.

    This is the one piece of AC-1 evidence that cannot skip — it needs no listener, only a
    resolvable literal — so an IPv6-less runner still fails here rather than going green on
    skips.
    """
    import socket as socket_lib

    v6 = socket_mod._make_udp("::1")
    v4 = socket_mod._make_udp("127.0.0.1")
    try:
        assert v6.family == socket_lib.AF_INET6
        assert v4.family == socket_lib.AF_INET
    finally:
        v6.close()
        v4.close()


def _dual_stack_localhost() -> set[int]:
    """The families ``localhost`` resolves to here, skipping when it offers no IPv4."""
    import socket as socket_lib

    families = {
        entry[0]
        for entry in socket_lib.getaddrinfo("localhost", None, type=socket_lib.SOCK_DGRAM)
    }
    if socket_lib.AF_INET not in families:  # pragma: no cover - depends on the host's stack
        pytest.skip("localhost does not resolve to IPv4 here")
    return families


@pytest.mark.parametrize("v6_first", [True, False])
def test_ipv4_wins_whatever_order_the_resolver_returns(monkeypatch, v6_first) -> None:
    """AC-2, independent of this machine's resolver — which is what makes it a real guard.

    Both live-resolver tests below assert "the code picks IPv4 *here*", and on a host whose
    RFC 6724 policy already puts ``A`` first — as a review demonstrated — an implementation
    that simply took the *last* result would satisfy them while reintroducing the regression
    everywhere else. Fabricating both orderings pins the rule rather than the platform, and it
    needs no network stack at all, so it cannot skip.
    """
    import socket as socket_lib

    v6 = (socket_lib.AF_INET6, socket_lib.SOCK_DGRAM, 0, "", ("::1", 0, 0, 0))
    v4 = (socket_lib.AF_INET, socket_lib.SOCK_DGRAM, 0, "", ("127.0.0.1", 0))
    order = [v6, v4] if v6_first else [v4, v6]
    monkeypatch.setattr(socket_lib, "getaddrinfo", lambda *a, **k: order)

    sock = socket_mod._make_udp("dual.example")
    try:
        assert sock.family == socket_lib.AF_INET, "IPv4 wins wherever the host offers it"
    finally:
        sock.close()


def test_a_dual_stack_hostname_still_goes_to_ipv4() -> None:
    """The same rule against the real resolver, so the fabricated one above stays honest."""
    import socket as socket_lib

    _dual_stack_localhost()
    sock = socket_mod._make_udp("localhost")
    try:
        assert sock.family == socket_lib.AF_INET
    finally:
        sock.close()


def test_a_dual_stack_hostname_reaches_an_ipv4_only_collector() -> None:
    """The same criterion end to end, against a receiver bound to IPv4 alone.

    Deliberately *not* written as "bind wherever the code decided to send" — that expression
    mirrors ``_make_udp``'s own, so a family mismatch would be unrepresentable and the test
    would assert self-consistency rather than AC-2. The receiver's family is fixed here and the
    sink must come to it.
    """
    import socket as socket_lib

    _dual_stack_localhost()
    receiver, port = _loopback_receiver(socket_lib.AF_INET)
    try:
        SyslogSink("localhost", port, transport="udp").emit(
            [{"level": "INFO", "message": "to-the-v4-collector"}]
        )
        assert b"to-the-v4-collector" in receiver.recv(4096)
    finally:
        receiver.close()


def test_an_ipv6_only_host_still_selects_ipv6(monkeypatch) -> None:
    """The IPv4 preference must not become the old unconditional AF_INET."""
    import socket as socket_lib

    monkeypatch.setattr(
        socket_lib,
        "getaddrinfo",
        lambda *a, **k: [(socket_lib.AF_INET6, socket_lib.SOCK_DGRAM, 0, "", ("::1", 0, 0, 0))],
    )

    sock = socket_mod._make_udp("v6-only.example")
    try:
        assert sock.family == socket_lib.AF_INET6
    finally:
        sock.close()


def test_a_resolution_with_no_families_raises_an_oserror(monkeypatch, capsys) -> None:
    """An ``IndexError`` here would escape ``_send_one``'s ``OSError`` handler into the caller.

    CPython raises rather than returning ``[]``, so this is unreachable in practice — guarded
    anyway, because the alternative is the library inventing an exception for the application
    (SPEC-025), and AC-3 requires this to be counted and announced instead.
    """
    import socket as socket_lib

    monkeypatch.setattr(socket_lib, "getaddrinfo", lambda *a, **k: [])
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)

    with pytest.raises(OSError) as caught:
        socket_mod._make_udp("nothing.example")
    assert not isinstance(caught.value, IndexError)

    sink = SyslogSink("nothing.example", transport="udp", max_retries=0)
    with pytest.raises(SinkDeliveryError):  # SPEC-026 FR-001, not the IndexError
        sink.emit([{"level": "INFO", "message": "x"}])
    assert sink._socket.losses().failed == 1
    assert "lost 1 message(s)" in capsys.readouterr().err


def test_udp_syslog_delivers_to_an_ipv6_destination() -> None:
    import socket as socket_lib

    receiver, port = _loopback_receiver(socket_lib.AF_INET6)
    try:
        SyslogSink("::1", port, transport="udp", facility="user").emit(
            [{"level": "INFO", "message": "over-v6"}]
        )
        assert b"over-v6" in receiver.recv(4096)
    finally:
        receiver.close()


def test_udp_logstash_delivers_to_an_ipv6_destination() -> None:
    import socket as socket_lib

    from log_foundry.sinks.logstash import LogstashSink

    receiver, port = _loopback_receiver(socket_lib.AF_INET6)
    try:
        LogstashSink(host="::1", port=port, transport="udp").emit([{"message": "ls-v6"}])
        assert b"ls-v6" in receiver.recv(4096)
    finally:
        receiver.close()


def test_udp_delivery_to_ipv4_is_unchanged() -> None:
    import socket as socket_lib

    receiver, port = _loopback_receiver(socket_lib.AF_INET)
    try:
        SyslogSink("127.0.0.1", port, transport="udp").emit(
            [{"level": "INFO", "message": "over-v4"}]
        )
        assert b"over-v4" in receiver.recv(4096)
    finally:
        receiver.close()


def test_a_host_resolving_to_neither_family_is_counted_and_announced(
    monkeypatch, capsys
) -> None:
    """A ``gaierror`` is an ``OSError``, so it fails exactly as an unreachable host already did."""
    import socket as socket_lib

    def unresolvable(*args: object, **kwargs: object) -> list:
        raise socket_lib.gaierror(-2, "Name or service not known")

    monkeypatch.setattr(socket_lib, "getaddrinfo", unresolvable)
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)
    sink = SyslogSink("nowhere.invalid", transport="udp", max_retries=0)

    with pytest.raises(SinkDeliveryError):  # SPEC-026 FR-001, not the gaierror itself
        sink.emit([{"level": "INFO", "message": "x"}])

    assert sink._socket.losses().failed == 1
    err = capsys.readouterr().err
    assert "lost 1 message(s)" in err
    assert "Name or service not known" not in err, "the exception's message is never written"


def test_the_family_is_resolved_once_per_socket_not_once_per_message(monkeypatch) -> None:
    import socket as socket_lib

    real = socket_lib.getaddrinfo
    calls: list[object] = []

    def counting(*args: object, **kwargs: object) -> list:
        calls.append(args)
        return real(*args, **kwargs)  # type: ignore[arg-type]

    receiver, port = _loopback_receiver(socket_lib.AF_INET)
    monkeypatch.setattr(socket_lib, "getaddrinfo", counting)
    try:
        sink = SyslogSink("127.0.0.1", port, transport="udp")
        sink.emit([{"level": "INFO", "message": f"m{i}"} for i in range(3)])
        sink.emit([{"level": "INFO", "message": "m3"}])
    finally:
        receiver.close()
    assert len(calls) == 1, "the socket outlives every send made through it"


# -- SPEC-029 FR-002/FR-003: SocketTransport's abandonment line ------------------------------


class RefusingSocket(FakeSocket):
    """A socket whose sends fail with a real ``OSError``, message and all."""

    def __init__(self, errno: int = 111) -> None:
        super().__init__()
        self._exc = OSError(errno, "Connection refused to loghost.internal:514")

    def sendall(self, data: bytes) -> None:
        raise self._exc

    def sendto(self, data: bytes, addr: tuple) -> None:
        raise self._exc


class RaisingStderr:
    """A ``sys.stderr`` whose ``write`` fails, as a closed fd or a broken pipe does."""

    def __init__(self) -> None:
        self.calls = 0

    def write(self, text: str) -> int:
        self.calls += 1
        raise ValueError("I/O operation on closed file")

    def flush(self) -> None:
        return None


def _refusing_sink(monkeypatch) -> SyslogSink:
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: RefusingSocket())
    monkeypatch.setattr(socket_mod, "_BACKOFF_BASE", 0.0)  # no real sleeping in the retry loop
    return SyslogSink("loghost", transport="udp", max_retries=1)


def test_an_abandoned_message_reports_the_errno_not_the_message(monkeypatch, capsys) -> None:
    """An ``OSError`` type name alone cannot tell "refused" from "host unknown"; the code can."""
    sink = _refusing_sink(monkeypatch)

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "INFO", "message": "x"}])  # nothing landed (SPEC-026 FR-001)

    err = capsys.readouterr().err
    assert "Connection refused" not in err, "the exception's message is never written (arch §6)"
    assert "loghost.internal" not in err
    # Derived, not hardcoded: CPython maps an errno to an ``OSError`` *subclass* at construction,
    # and the mapping is per-platform — 111 is ECONNREFUSED on Linux (so the type is
    # ``ConnectionRefusedError``) and something else on macOS (so it stays ``OSError``).
    assert type(RefusingSocket()._exc).__name__ in err
    assert "errno=111" in err, "the OS code is what makes the line actionable"
    assert "lost 1 message(s)" in err
    assert "2 attempt(s)" in err, "the attempt count survived the conversion"
    assert sink._socket.failed == 1


def test_a_broken_stderr_does_not_reach_the_caller(monkeypatch) -> None:
    """This runs on the worker thread, where the write used to be bare (SPEC-029 FR-003)."""
    import sys

    sink = _refusing_sink(monkeypatch)
    stream = RaisingStderr()
    monkeypatch.setattr(sys, "stderr", stream)

    # The *delivery* failure propagates (SPEC-026 FR-001); the stderr fault must not — a
    # ``RuntimeError`` here would mean the diagnostic became the failure.
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "INFO", "message": "x"}])

    assert stream.calls == 1, "it tried, and the guard absorbed the fault"
    assert sink._socket.failed == 1, "the counter moved before the announcement"


# --- SPEC-038 FR-007: an oversized datagram is permanent, not transient -------------------


def test_an_oversized_datagram_is_dropped_before_the_send_and_counted(monkeypatch, capsys) -> None:
    """AC-1. A 70 KB event produced EMSGSIZE, retried 4x with backoff, then sent the worker
    round three more times: ~16 futile sends and seconds of backoff on the single drain thread,
    never converging. The size is knowable before sending, as every other size-limited sink here
    already knows it.
    """
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    sink = SyslogSink("loghost", transport="udp", max_datagram_bytes=1000)
    sink.emit([{"level": "INFO", "message": "x" * 5000}, {"level": "INFO", "message": "ok"}])

    assert len(fake.sent) == 1, "only the sendable frame reached the socket"
    assert b"ok" in fake.sent[0][0]
    losses = sink.losses()
    assert losses.dropped == 1 and losses.failed == 0, (
        "an unsendable datagram is a drop, not a delivery failure"
    )
    assert "over the 1000-byte datagram limit" in capsys.readouterr().err


def test_a_batch_of_only_oversized_datagrams_does_not_report_a_delivery_failure(
    monkeypatch,
) -> None:
    """Nothing was attempted, so there is nothing for the worker's retry to fix."""
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: fake)
    sink = SyslogSink("loghost", transport="udp", max_datagram_bytes=100)
    sink.emit([{"level": "INFO", "message": "x" * 5000}])
    assert fake.sent == []
    assert sink.losses().dropped == 1


def test_tcp_is_unaffected_by_the_datagram_limit(monkeypatch) -> None:
    """AC-2. TCP is a stream; the limit bounds nothing there."""
    fake = FakeSocket()
    monkeypatch.setattr(socket_mod, "_make_tcp", lambda host, port, timeout: fake)
    sink = SyslogSink("loghost", transport="tcp", max_datagram_bytes=100)
    sink.emit([{"level": "INFO", "message": "x" * 5000}])
    assert len(fake.sent) == 1, "a large frame still goes out over TCP"
    assert sink.losses().dropped == 0


def test_emsgsize_is_not_retried_while_a_transient_errno_still_is(monkeypatch, capsys) -> None:
    """AC-3. The permanent set is EMSGSIZE only.

    Every other socket errno describes a destination that may come back, so treating one of them
    as permanent would turn a transient outage into silent loss.
    """
    import errno as errno_mod

    def failing(code):
        class Failing(FakeSocket):
            def sendto(self, data, addr):
                self.sent.append((data, addr))
                raise OSError(code, "nope")

        return Failing()

    permanent = failing(errno_mod.EMSGSIZE)
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: permanent)
    sink = SyslogSink("loghost", transport="udp", max_retries=3, max_datagram_bytes=10**6)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"level": "INFO", "message": "x"}])
    assert len(permanent.sent) == 1, "EMSGSIZE is a verdict on the message: one attempt only"

    transient = failing(errno_mod.ECONNREFUSED)
    monkeypatch.setattr(socket_mod, "_make_udp", lambda host: transient)
    retrying = SyslogSink("loghost", transport="udp", max_retries=3, max_datagram_bytes=10**6)
    with pytest.raises(SinkDeliveryError):
        retrying.emit([{"level": "INFO", "message": "x"}])
    assert len(transient.sent) == 4, "a destination that may come back is still retried"
