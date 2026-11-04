"""SPEC-067 — `continue_trace` is total over any argument, and a refusal is always announced.

`continue_trace` is the library's one inbound path, and its four arguments are `str` only by
annotation. The rejection echo used to `repr` the whole value before cutting it, so a shared-sublist
list hung the caller's thread and a raising `__repr__` lost the line while `reason="rejected"` said
it was written; and the parsers called the value's own methods, so a `str` subclass or a spoofed
`__class__` raised into the caller. These tests hand each argument every such value.

Two instruments bound the echo's cost (FR-002), because neither sees all of it: CPU time against
`repr` of the same value in the same test catches a pass over the value on any machine, and the
`tracemalloc` peak catches a full copy followed by a slice, which CPU time is too coarse to see.
The deadline raises a `BaseException`, because an alarm raising an `Exception` would be swallowed
by `rejected`'s own guard and read as a pass.

Every expected echo is a literal, never computed from `_diag`.
"""

from __future__ import annotations

import contextlib
import signal
import threading
import time
import tracemalloc
from typing import TYPE_CHECKING

import pytest

from conftest import fan_out
from log_foundry import _diag

if TYPE_CHECKING:
    from collections.abc import Iterator

_PREFIX = "log-foundry: ignoring inbound trace context"


# -- the deadline (SPEC-064's alarm harness, copied: test modules here do not import each other) --


class _Deadline(BaseException):
    """What the alarm raises: a `BaseException`, because `rejected`'s guard catches `Exception`."""


@contextlib.contextmanager
def _deadline(seconds: float, what: str) -> Iterator[None]:
    """Fails the test, instead of hanging the run, when the body has not returned in time."""
    assert threading.current_thread() is threading.main_thread(), (
        "SIGALRM reaches only the main thread, so the deadline cannot fire here"
    )

    def _ring(signum: int, frame: object) -> None:
        raise _Deadline

    previous = signal.signal(signal.SIGALRM, _ring)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    except _Deadline:
        pytest.fail(f"{what} did not return within {seconds} s", pytrace=False)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class _Plain(str):
    """A `str` subclass that overrides nothing, so `repr` walks it at base rather than failing early."""

    __slots__ = ()


# -- FR-002: the echo's cost does not grow with the value -----------------------------------------


def test_rejected_on_a_long_str_costs_a_fraction_of_its_repr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    value = "0" * 10**8

    started = time.process_time()
    _diag.rejected("x", value)
    echo_cost = time.process_time() - started

    started = time.process_time()
    repr(value)
    repr_cost = time.process_time() - started

    assert echo_cost < repr_cost / 10, f"rejected {echo_cost:.4f} s against repr {repr_cost:.4f} s"
    assert capsys.readouterr().err == (
        f"{_PREFIX} (x): '000000000000000000000000000000000000000000000000000000000000000…\n"
    ), "the line must be written, or an early return would pass the ratio"


def test_rejected_on_long_bytes_costs_a_fraction_of_its_repr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    value = b"0" * 10**8

    started = time.process_time()
    _diag.rejected("x", value)
    echo_cost = time.process_time() - started

    started = time.process_time()
    repr(value)
    repr_cost = time.process_time() - started

    assert echo_cost < repr_cost / 10, f"rejected {echo_cost:.4f} s against repr {repr_cost:.4f} s"
    assert capsys.readouterr().err == f"{_PREFIX} (x): <bytes>\n", "the line must be written"


@pytest.mark.parametrize(
    ("kind", "echo"),
    [
        ("str", "'000000000000000000000000000000000000000000000000000000000000000…"),
        ("str_subclass", "'000000000000000000000000000000000000000000000000000000000000000…"),
        ("bytes", "<bytes>"),
    ],
    ids=["str", "str_subclass", "bytes"],
)
def test_rejected_allocates_nothing_proportional_to_the_value(
    capsys: pytest.CaptureFixture[str], kind: str, echo: str
) -> None:
    """64 KiB sits some 600 times above the slot slice's measured peak and 1,500 below a copy."""
    value: object = {
        "str": lambda: "0" * 10**8,
        "str_subclass": lambda: _Plain("0" * 10**8),
        "bytes": lambda: b"0" * 10**8,
    }[kind]()

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        _diag.rejected("x", value)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert peak < 65_536, f"rejected peaked at {peak:,} bytes on a {kind} of 10^8"
    assert capsys.readouterr().err == f"{_PREFIX} (x): {echo}\n", "the line must be written"


def test_rejected_returns_on_a_shared_sublist_dag(capsys: pytest.CaptureFixture[str]) -> None:
    value = fan_out()

    with _deadline(5, "rejected on the DAG"):
        _diag.rejected("x", value)

    assert capsys.readouterr().err == f"{_PREFIX} (x): <list>\n"
