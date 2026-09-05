"""SPEC-049 FR-002 — `SocketTransport` refuses a timeout that cannot bound anything."""

from __future__ import annotations

import pytest

from log_foundry.sinks._socket import SocketTransport


@pytest.mark.parametrize("transport", ["tcp", "udp"])
@pytest.mark.parametrize("bad", [-1.0, 0.0, float("nan"), float("inf")])
def test_an_unusable_timeout_is_refused_for_both_transports(transport: str, bad: float) -> None:
    """Over TCP it reached `create_connection` and raised from inside `send_all` on every message.

    Over UDP the value was never used, so this is a **new** refusal of a previously harmless call
    — stated in the spec rather than discovered, and pinned here so it does not read as a slip.
    """
    with pytest.raises(ValueError, match="SocketTransport timeout") as info:
        SocketTransport("127.0.0.1", 9, transport=transport, timeout=bad)
    assert repr(bad) in str(info.value)


@pytest.mark.parametrize("transport", ["tcp", "udp"])
def test_a_usable_timeout_constructs(transport: str) -> None:
    assert SocketTransport("127.0.0.1", 9, transport=transport, timeout=2.5)._timeout == 2.5
