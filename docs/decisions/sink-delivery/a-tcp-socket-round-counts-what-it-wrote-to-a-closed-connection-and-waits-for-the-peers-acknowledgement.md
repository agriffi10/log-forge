---
type: Decision
title: "A TCP socket round counts what it wrote to a connection its peer closed, and waits for the peer's acknowledgement unless told not to"
description: "`SocketTransport` over TCP replaces a held connection that is not open before a round, makes every frame written since the last open check `UNKNOWN` when a later write raises or the end check reads a reset, and ends each round waiting up to 0.25 s for the peer to acknowledge what it wrote; a FIN alone is never a loss, a reset the next round sees is not counted, and `ack_wait=False` keeps the checks without the wait. (SPEC-093)"
---

**What a TCP round can know, and how it learns it.** A `SyslogSink` or `LogstashSink` over TCP used
to call a frame `ACCEPTED` the moment `sendall` returned, so frames written after the peer closed
went nowhere with `failed=0` and no line (2026-11-14 audit, finding 1). The transport now reads the
peer's state through two seams beside `_make_tcp`. `_peer_state` answers `"reset"` when `SO_ERROR`
holds one of `_RESET_ERRNOS` (`ECONNRESET`, `EPIPE`, `ECONNABORTED`, `ETIMEDOUT`), and otherwise a
zero-timeout readiness test and a one-byte peek answer `"fin"` or `"open"`; any other pending error,
such as Linux's soft `EHOSTUNREACH` on a live connection, reads as open. `_unacked` reads the bytes
the peer's kernel has not acknowledged (`SIOCOUTQ` on Linux, `SO_NWRITE` on macOS), or `None` where
neither exists. Before a round's first frame a held connection that is not open is replaced, so the
round's frames go on a new one and nothing is counted. The frames written since the connection's
last open check are unconfirmed: when a later write raises they become `UNKNOWN`, and the raising
frame keeps its 1.x outcome and goes out again on a new connection. After the last frame,
`_end_state` waits, in 1 ms pauses through `_retry.wait`, until every byte is acknowledged or a reset
arrives, for at most `_RESET_WAIT`, 0.25 s; a reset makes the round's unconfirmed frames `UNKNOWN`.
Each such group is counted once in `failed` and announced on one `ALWAYS` line, `SocketTransport,
written to a connection its peer closed; may have landed, not retried`, and never sent again.

**The rule came from a corpus, after three reasoned rules were each wrong where they had not been
run.** Counting on a FIN reported a half-closing peer's delivered frames as may-have-landed
(`halfclose.py`, SPEC-093's authoring measurement: all 60 frames received, `failed=60`; the
script is not in the tree, and the `half-close-3-rounds` row of `tests/test_socket_close_corpus.py`
holds the behaviour in CI). Counting only on a peek that raises was silent
on Linux, where a FIN followed by the reset our later write draws still peeks as a FIN. Reading
`SO_ERROR` at once after the round was right on idle Linux loopback and silent under delay, and in
some runs on a loaded Linux runner, because the reset landed after the check. `docs/audits/2026-11-14-probes/sinks/spec093_close_corpus.py` drove the
real transport through the two seams, with no sleep before any check, on macOS and on Linux on
loopback and under 1 ms and 10 ms of added delay, and only the rule above was right on every row.
**A FIN alone is not a loss**, because it is also a legal half-close; a reset, which says the peer
discarded data, is. **The end check waits** because a peer that closed before our frames reached it
answers them with a reset and never with an acknowledgement, while an open or half-closed peer
acknowledges them, so the round is decided on the peer kernel's answer to its own bytes whenever that
answer lands.

**What it costs, and what stays unknowable.** One round trip plus however long the peer holds its
acknowledgement on every TCP round: a collector that never writes back acknowledges on its
delayed-acknowledgement timer, which Linux starts at 40 ms and lets grow to 200 ms. The corpus's clean
round took a median of 0.5 to 0.6 ms on loopback, 7.9 ms with 1 ms of delay each way and 44.2 ms with
10 ms. A peer that neither acknowledges nor resets costs each round the whole 0.25 s. **The 0.25 s
bound was decided in session, not by Andrew:** it covers a 50 ms round trip under Linux's longest
delayed acknowledgement, and a longer one would close more of the window below at that much more cost
per round against a silent peer. **A reset that reaches us after the end check is not counted**, by
this round or the next: the round already settled its frames `ACCEPTED`, and SPEC-088's ledger lets
an event leave through one term only, so a later count would put it in `delivered` and in
`sink_lost` both. That window is at most the peer's receive buffer, and `docs/architecture.md` §13
records it with a round trip longer than the bound and a platform where `_unacked` answers `None`,
Windows among them, where the check does not wait.

**The wait is on by default, and `ack_wait=False` turns it off.** Andrew answered on 2026-11-16, asked
whether the wait should be on by default: **"On, opt out (Recommended)"**. His answer of 2026-11-15
on `RabbitMQSink(confirm=)`, "Off, opt in", is the precedent the other way, and the two differ in
cost: a broker confirm costs a round trip per message, unbounded but by the connection, while this
wait costs one round trip per round, bounded at 0.25 s. `SyslogSink` and `LogstashSink` each take a
keyword-only `ack_wait: bool | None = None`; `None` and `True` wait, and `False` reads the peer's state
once at the end, which keeps the pre-round check, the raise rule and an end check that sees a reset
already landed, and gives up the rows the corpus measured silent under delay and on macOS loopback,
and the mid-round rows in some runs on a loaded Linux runner (SPEC-093, 2026-11-22).
The name says what it controls, the wait for the peer's TCP acknowledgement; `confirm` was not reused,
because a broker's confirm is an application-level answer and this acknowledgement is the peer
kernel's, which says nothing about whether the collector kept the bytes. `_socket.require_ack_wait`
judges it before anything is built: a value that is neither `None` nor a `bool` is a `TypeError`,
never read for its truth, since a new keyword has no 1.x caller and `"false"` would turn the wait on
(*A bad argument is floored if it works and refused if it is already broken; a new one is refused*);
a `bool` with the UDP transport, which has no acknowledgement, is a `ValueError`, and on
`LogstashSink` a value beside `url=` is refused by its socket-only rule.

**Do NOT** count a FIN as a loss; do NOT count a previous round's frames on a reset the next round's
check reads; do NOT pause the end check anywhere but `_retry.wait`, which the only-the-engine-waits
gates watch (`_socket` is in `tests/test_sink_retry.py`'s `WAIT_EXCLUSIONS`); do NOT read a
non-`bool` `ack_wait` for its truth. UDP is unchanged: a datagram is fire-and-forget.

## History

### 2026-11-20

- **Settled** by SPEC-093 FR-001, with Andrew's answer of 2026-11-16 on the default. The close corpus
  test, `tests/test_socket_close_corpus.py`, and the seam test, `tests/test_socket_ack_check.py`, hold
  it; the build's re-plants are in
  `docs/audits/2026-11-14-probes/sinks/spec093_094_build_results.txt`.
