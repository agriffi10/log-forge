"""SPEC-046 — the exit close waits for the slowest owed sink, not for all of them.

SPEC-045 made the owed-close record hold every sink, which is what stopped the live sink going
unclosed, and left `_close_orphan_sink` draining that record in sequence. Measured on `main` at
`dcb07c3` against `shutdown(timeout=1.0)` with 2-second closes: one owed sink 2.00 s, two 4.01 s,
four 8.02 s — linear in the number owed.

The closes now run concurrently and **every one is joined**. A design that detached them under
`DEFAULT_CLOSER_GRACE` was built during the spec review and measured completing 1 of 4, so the
assertions here are on closes having *completed*, never on elapsed time alone: a shorter total is
also what dropping a close produces.
"""

from __future__ import annotations

import threading
import time

import pytest

import log_foundry
from log_foundry import _lifecycle

api = pytest.importorskip("log_foundry.api")

CLOSE_SECONDS = 2.0
"""Long enough to separate `max` from `sum` at four sinks, short enough to keep the suite quick.

It is deliberately **above** `DEFAULT_CLOSER_GRACE` (2.0 s) nowhere by itself — FR-002 AC-1 uses a
longer one for that — but four of them in sequence is 8 s against a 1 s budget, which is the
measurement this file exists to move.
"""


class SlowCloseSink:
    """Records which thread closed it, when, and how long its buffer went undelivered.

    `close()` is its delivery and it keeps accepting afterwards, which `sinks/base.py` permits and
    nineteen shipped sink modules do. That is the shape for which an abandoned close is lost data,
    so it is the shape these tests use — a double that refuses post-close work cannot strand
    anything by construction.
    """

    log_foundry_stop_signal: threading.Event | None = None

    def __init__(self, name: str = "sink", seconds: float = CLOSE_SECONDS) -> None:
        self.name = name
        self.seconds = seconds
        self.closes = 0
        self.buffered = 0
        self.delivered = 0
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.closed_on: str | None = None
        self._lock = threading.Lock()

    def emit(self, batch: list[dict[str, object]]) -> None:
        """Buffers, as a client-batching sink does."""
        with self._lock:
            self.buffered += len(batch)

    def close(self) -> None:
        """Takes its time, then delivers — so a close that never completes is a loss."""
        self.started_at = time.monotonic()
        self.closed_on = threading.current_thread().name
        time.sleep(self.seconds)
        with self._lock:
            self.closes += 1
            self.delivered += self.buffered
            self.buffered = 0
        self.finished_at = time.monotonic()

    def __repr__(self) -> str:
        """Names the sink and its counts, so a failure reads without a debugger."""
        return f"<{self.name} closes={self.closes} delivered={self.delivered} buf={self.buffered}>"


def _arm(*sinks: SlowCloseSink) -> None:
    """Arms every sink as owed a close, the first through a real emit.

    Args:
      *sinks: The sinks to arm, in the order they should appear in the record.

    Returns:
      None.

    Raises:
      None.
    """
    log_foundry.configure(service="t", sink=sinks[0])
    log_foundry.info("arm the first")
    for sink in sinks[1:]:
        _lifecycle._note_orphan_emit(sink)
        sink.emit([{"message": f"to {sink.name}"}])


# --------------------------------------------------------------------------- FR-001


def test_four_owed_closes_cost_the_slowest_not_their_sum() -> None:
    """FR-001 AC-1/AC-2/AC-3. The three halves are asserted together, deliberately.

    Elapsed time alone is not the observable: a fan-out that never joins is *faster* still and
    abandons three of the four closes. So this asserts the cost fell, that every close completed,
    and that they overlapped — the mutant that drops the join fails the second.

    The inline sink's close is deliberately **short**. With all four the same length, the inline
    close is itself a 2 s wait that acts as an implicit join, and the dropped-join mutant passes
    this test — measured. A 0.2 s inline close against 2 s threaded ones is what makes AC-2
    load-bearing here rather than only in the two tests below.
    """
    sinks = [SlowCloseSink("inline", seconds=0.2)] + [
        SlowCloseSink(f"s{i}") for i in range(3)
    ]
    _arm(*sinks)
    assert len(_lifecycle._state._orphan_owed) == 4, "four are owed, or this measures nothing"
    assert _lifecycle._live_config_sink() is sinks[0], (
        "the short close is the inline one, or a dropped join is hidden behind a 2 s wait"
    )

    started = time.monotonic()
    log_foundry.shutdown(timeout=1.0)
    elapsed = time.monotonic() - started

    assert elapsed < 4.0, (
        f"the exit close costs the slowest owed close, not their sum — took {elapsed:.2f}s "
        f"against {0.2 + 3 * CLOSE_SECONDS:.1f}s in sequence"
    )
    assert all(sink.closes == 1 for sink in sinks), (
        f"and every owed sink completed its close — got {sinks}"
    )
    assert all(sink.buffered == 0 for sink in sinks), (
        f"so nothing any of them held is stranded — got {sinks}"
    )
    starts = [sink.started_at for sink in sinks]
    assert all(s is not None for s in starts), f"every close began — got {sinks}"
    assert max(starts) - min(starts) < CLOSE_SECONDS, (  # type: ignore[type-var]
        f"the closes overlapped rather than queued — starts spanned "
        f"{max(starts) - min(starts):.2f}s"  # type: ignore[operator]
    )


def test_the_configured_sink_closes_on_the_calling_thread() -> None:
    """FR-001 AC-4. `shutdown()`'s own close stays inline (SPEC-030), and the config picks it.

    Asserted on the **thread**, not on ordering or timing: routed through a thread the close still
    completes and still finishes first often enough for a timing assertion to pass.

    The configured sink is armed **first** here, so the choice is distinguishable from the
    most-recently-armed fallback. An earlier draft used `configure(sink=live)` to point the
    config at the second sink, which swaps — leaving exactly one sink owed, so the test measured
    the single-sink path and passed against an inline choice that ignored the config entirely.
    Measured: it did.
    """
    live, other = SlowCloseSink("live"), SlowCloseSink("other")
    _arm(live, other)
    owed = _lifecycle._state._orphan_owed
    assert list(owed.values()) == [live, other], "the config's sink is armed first, not last"
    assert _lifecycle._live_config_sink() is live, "and the config still names it"

    caller = threading.current_thread().name
    log_foundry.shutdown(timeout=1.0)

    assert live.closed_on == caller, (
        f"the configured sink closed on the calling thread, got {live.closed_on!r}"
    )
    assert other.closed_on is not None and other.closed_on != caller, (
        f"and the superseded one did not, got {other.closed_on!r}"
    )


def test_the_last_armed_sink_closes_inline_when_the_config_is_not_owed() -> None:
    """FR-001 AC-5. The fallback, and it is what keeps the single-sink case thread-free.

    Reached by arming two sinks and then pointing the config at neither, which is the state a
    `configure()` racing an emit produces.
    """
    first, last = SlowCloseSink("first"), SlowCloseSink("last")
    elsewhere = SlowCloseSink("elsewhere")
    _arm(first, last)
    _lifecycle.stamp(elsewhere)
    from log_foundry import config as config_module

    config_module._rebind(sink=elsewhere)
    owed = _lifecycle._state._orphan_owed
    assert id(elsewhere) not in owed, "the configured sink is not among those owed"
    assert list(owed.values())[-1] is last, "and `last` is the most recently armed"

    caller = threading.current_thread().name
    log_foundry.shutdown(timeout=1.0)

    assert last.closed_on == caller, (
        f"the most recently armed sink ran inline, got {last.closed_on!r}"
    )
    assert first.closed_on is not None and first.closed_on != caller, (
        f"and the earlier one ran on a thread — `!= caller` alone also passes when it was never "
        f"closed at all. Got {first.closed_on!r}"
    )


def test_a_sink_that_merely_compares_equal_is_not_taken_for_the_configured_one() -> None:
    """FR-001 AC-4, the identity half. `x in list` is value equality, and that is the trap.

    A sink with a value `__eq__` — a dataclass, say — makes `configured in owed` true for an
    object that is not owed at all. The inline close would then run against a sink the record
    never armed and never latched, leaving every owed sink on a thread and admitting a second
    close of the impostor. No shipped sink is a dataclass, so nothing in-tree would catch it.

    **Two owed sinks, not one.** There are two identity tests, and the second was unpinned: the
    fan-out loop's `sink is inline`. Mutated to `==` it survives the whole suite with one owed
    sink — `==` skips nothing there — and silently drops an owed close as soon as there are two.
    Measured, then pinned.
    """
    import dataclasses

    @dataclasses.dataclass
    class EqualSink:
        """A sink that compares equal to any other with the same field values.

        Standalone rather than a subclass: `@dataclass` over `SlowCloseSink` generates an
        `__init__` taking no arguments, which silently replaces the parent's and leaves every
        counter unset.

        `log_foundry_stop_signal` is `compare=False` because the library **writes** it on the
        sink it arms (`_offer_orphan_signal`), so a generated `__eq__` that included it would
        make the two objects stop comparing equal before the close is chosen — and this test
        would then pass against the value-equality bug it exists to catch. Measured: it did.
        """

        log_foundry_stop_signal: threading.Event | None = dataclasses.field(
            default=None, compare=False
        )
        closes: int = 0

        def emit(self, batch: list[dict[str, object]]) -> None:
            """Keeps nothing; this test asserts on which object gets closed."""

        def close(self) -> None:
            """Counts the close."""
            self.closes += 1

    owed_sink = EqualSink()
    second_owed = EqualSink()
    twin = EqualSink()
    assert owed_sink == twin == second_owed, "all three compare equal"
    assert owed_sink is not twin and owed_sink is not second_owed, "and none is another"


    _arm(owed_sink)
    _lifecycle._note_orphan_emit(second_owed)
    from log_foundry import config as config_module

    _lifecycle.stamp(twin)
    config_module._rebind(sink=twin)

    log_foundry.shutdown(timeout=1.0)

    assert owed_sink.closes == 1 and second_owed.closes == 1, (
        f"both owed sinks were closed — got {owed_sink!r} {second_owed!r}. Two of them is what "
        "exercises the fan-out loop's own identity test: with one, `sink == inline` skips "
        "nothing and the loss does not appear"
    )
    assert twin.closes == 0, (
        f"and the equal-but-distinct object the config names is not closed, got {twin!r} — "
        "membership must be identity, since `in` and `==` are value equality"
    )


# --------------------------------------------------------------------------- FR-002


def test_a_close_longer_than_the_closer_grace_still_delivers() -> None:
    """FR-002 AC-1. The case the rejected design lost, pinned so it cannot come back.

    `join_closers` caps at `DEFAULT_CLOSER_GRACE`, so releasing these detached abandons any close
    that exceeds it — measured during the spec review at 3 s: delivered nothing, where today it
    delivers. Joining every close is what makes the duration irrelevant.
    """
    grace = _lifecycle.DEFAULT_CLOSER_GRACE
    slow = SlowCloseSink("slow", seconds=grace + 1.0)
    quick = SlowCloseSink("quick", seconds=0.1)
    _arm(quick, slow)
    assert slow.seconds > grace, "the close outlasts the grace, or this proves nothing"

    log_foundry.shutdown(timeout=1.0)

    assert slow.closes == 1 and slow.buffered == 0, (
        f"a close longer than DEFAULT_CLOSER_GRACE ({grace}s) still completed and delivered — "
        f"got {slow!r}"
    )
    assert quick.closes == 1, f"and so did the quick one, got {quick!r}"


def test_a_raising_close_does_not_stop_the_others() -> None:
    """FR-002 AC-3, the inline half — the raising sink here is the configured one.

    The fan-out half is the test below, and it is the one that is new in this change: this
    behaviour was already guarded before it.
    """

    class RaisingSink(SlowCloseSink):
        """Fails its close, as a sink whose destination is gone does."""

        def close(self) -> None:
            """Records the attempt, then fails."""
            self.closed_on = threading.current_thread().name
            self.closes += 1
            raise RuntimeError("the destination is gone")

    raiser = RaisingSink("raiser", seconds=0.0)
    survivor = SlowCloseSink("survivor", seconds=0.1)
    _arm(raiser, survivor)

    log_foundry.shutdown(timeout=1.0)

    assert raiser.closes == 1, "the failing close was attempted"
    assert survivor.closes == 1 and survivor.buffered == 0, (
        f"and the other owed sink was still closed and still delivered — got {survivor!r}"
    )


def test_a_close_that_raises_on_a_fan_out_thread_is_absorbed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """FR-002 AC-3, the half that is new in this change — and the half nothing covered.

    The sibling test above puts the raising sink on the calling thread, because `_arm` makes the
    first sink the configured one and the configured one closes inline. So the guard that matters
    here — the one inside a fan-out thread — was untested: a `_close_owed` that re-raises on a
    thread and absorbs on the main one passed the **entire** suite, measured.

    An exception escaping a thread body reaches CPython's bootstrap, which prints a traceback
    carrying the exception's message. That is the arch §6 rule `_close_owed` was factored out to
    satisfy, so the assertion is on **stderr**: counters alone still pass unguarded, since the
    close happens before it raises.
    """

    class RaisingSink(SlowCloseSink):
        """Fails its close with a message no diagnostic may reproduce."""

        def close(self) -> None:
            """Records the attempt and the thread, then fails."""
            self.closed_on = threading.current_thread().name
            self.closes += 1
            raise RuntimeError("SECRET-user-data-in-the-message")

    survivor = SlowCloseSink("survivor", seconds=0.1)
    raiser = RaisingSink("raiser", seconds=0.0)
    _arm(survivor, raiser)
    assert _lifecycle._live_config_sink() is survivor, "the raiser is not the inline one"

    caller = threading.current_thread().name
    log_foundry.shutdown(timeout=1.0)

    assert raiser.closed_on is not None and raiser.closed_on != caller, (
        f"the failing close ran on a fan-out thread, got {raiser.closed_on!r}"
    )
    assert raiser.closes == 1, "and it was attempted"
    assert survivor.closes == 1, f"while the other owed sink still closed, got {survivor!r}"

    err = capsys.readouterr().err
    assert "absorbed a failure while closing the sink" in err, (
        f"the thread body absorbed and announced it through _diag — got {err!r}"
    )
    assert "Traceback" not in err and "SECRET-user-data" not in err, (
        "and nothing printed the exception's message, which is what an unguarded thread body "
        f"does through CPython's bootstrap — got {err!r}"
    )


# --------------------------------------------------------------------------- FR-003


def test_one_owed_sink_creates_no_thread() -> None:
    """FR-003 AC-1/AC-3. The common case must not acquire a concurrency it does not need.

    Asserted on the thread the close ran on rather than on a thread census: a census over
    `threading.enumerate()` is process-global and passes or fails on what another test file
    leaked, which is how three SPEC-044 tests came to depend on file-name sort order.
    """
    only = SlowCloseSink("only", seconds=0.1)
    _arm(only)
    assert len(_lifecycle._state._orphan_owed) == 1, "exactly one is owed"

    caller = threading.current_thread().name
    log_foundry.shutdown(timeout=1.0)

    assert only.closed_on == caller, (
        f"the single owed close ran on the calling thread, with no thread created for it — "
        f"got {only.closed_on!r}"
    )
    assert only.closes == 1 and only.buffered == 0, f"and it delivered, got {only!r}"
    assert log_foundry.health().closing_sinks == 0, (
        "these closes are joined before shutdown() returns, so none is ever outstanding after it"
    )
    # `closing_sinks` counts `_closers`, which this path never registers in — so the assertion
    # above is structural rather than a live check, and it is here to fail if a later change
    # routes these closes through `_start_closer` after all.
