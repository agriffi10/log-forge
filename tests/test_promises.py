"""The library's stated promises, asserted against every entry path (2026-08-07 audit).

Every other test file asks "does this change work?". This one asks "does the promise hold
*everywhere*?" — which is a different question, and the one eight rounds of diff-scoped review
could not answer. SPEC-025 tested "the library never fails the caller" on the orphan path and
shipped a `@trace` that fails the caller; SPEC-026 made loss visible in the sinks and left the
synchronous path reporting all-clear over total loss. Neither is a subtle defect. Both are a
promise verified on one path out of four.

So the matrix is the point: each promise below runs against `orphan`, `traced`, `async` and
`post_shutdown`, and a cell that does not hold is marked ``xfail(strict=True)`` naming the audit
finding it belongs to.

**A fixed cell fails this file.** ``strict=True`` turns an unexpected pass into an error, so a
spec that closes a finding cannot land without deleting its marker — which is what stops the
audit's conclusions decaying into prose that no longer matches the code.
"""

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import threading

log_foundry = pytest.importorskip("log_foundry")
decorator = pytest.importorskip("log_foundry.decorator")

PATHS = ["orphan", "traced", "async", "post_shutdown"]


class Recorder:
    """Captures what actually reached a sink, and can be told to fail."""

    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[dict] = []
        self.fail = fail
        self.stop_signal: threading.Event | None = None

    def emit(self, batch: list[dict]) -> None:
        if self.fail:
            raise RuntimeError("the destination is unreachable")
        self.events.extend(batch)

    def close(self) -> None: ...


async def _emit_on(path: str, call) -> None:
    """Runs ``call`` — a zero-arg emitter — from the entry path named.

    Args:
      path: One of ``PATHS``.
      call: The emitting callable to run there.

    Returns:
      None.

    Raises:
      Exception: Whatever ``call`` raises, deliberately unguarded — several promises here are
        precisely about whether anything escapes.
    """
    if path == "orphan":
        call()
    elif path == "traced":

        @log_foundry.trace
        def body() -> None:
            call()

        body()
    elif path == "async":

        @log_foundry.trace
        async def abody() -> None:
            call()

        await abody()
    elif path == "post_shutdown":
        log_foundry.shutdown()
        call()
    else:  # pragma: no cover - guards a typo in PATHS
        raise AssertionError(f"unknown path {path!r}")


def _xfail(path: str, broken: dict[str, str]):
    """Marks a path whose cell is a known audit finding, so a fix breaks this file loudly."""
    if path in broken:
        return pytest.param(path, marks=pytest.mark.xfail(strict=True, reason=broken[path]))
    return path


# -- Promise 1: a fault at the destination never reaches the caller ---------------------------
# README "Reliability": "A destination can never fail your call." SPEC-025 FR-003.

P1_SINK_BROKEN: dict[str, str] = {}


@pytest.mark.parametrize("path", [_xfail(p, P1_SINK_BROKEN) for p in PATHS])
async def test_a_failing_sink_never_reaches_the_caller(path: str) -> None:
    log_foundry.configure(service="t", sink=Recorder(fail=True))
    await _emit_on(path, lambda: log_foundry.info("the sink will raise on this"))


P1_VALUE_BROKEN = {
    "traced": "audit A2 — the in-span branch of api._log is unguarded, so build_event's "
    "truncate_str raises AttributeError into the caller",
    "async": "audit A2 — same unguarded in-span branch",
}


@pytest.mark.parametrize("path", [_xfail(p, P1_VALUE_BROKEN) for p in PATHS])
async def test_a_bad_message_value_never_reaches_the_caller(path: str) -> None:
    """The same promise, broken by an ordinary slip rather than a broken destination.

    `info(exc)` is a slip `mypy` catches only at typed call sites. On the orphan path it is
    absorbed; inside a span it kills the decorated function *and* records the span with an
    `error.type` the caller never raised.
    """
    log_foundry.configure(service="t", sink=Recorder())
    await _emit_on(path, lambda: log_foundry.info(ValueError("not a string")))


# -- Promise 2: loss is visible ---------------------------------------------------------------
# README "Reliability": "Silence is not success anywhere."

P2_BROKEN = {
    "orphan": "audit L6 / SPEC-034 FR-004 — the synchronous path has no worker to report "
    "through, so health() reads all zeros over total loss",
    "post_shutdown": "audit L6 — same synchronous path, after the worker is retired",
}


@pytest.mark.parametrize("path", [_xfail(p, P2_BROKEN) for p in PATHS])
async def test_lost_events_are_visible_in_health(path: str) -> None:
    log_foundry.configure(service="t", sink=Recorder(fail=True))
    await _emit_on(path, lambda: log_foundry.info("this will be lost"))
    log_foundry.flush(timeout=2.0)

    h = log_foundry.health()
    assert h.failed_batches or h.dropped or h.stopped_reason or (h.sink and h.sink.failed), (
        f"events were lost on the {path} path and health() reports nothing: {h}"
    )


# -- Promise 3: every emitted event is valid JSON ---------------------------------------------
# `build_event`'s docstring: "safe for any sink to serialize". SPEC-017, SPEC-020.


def _strict_json(event: dict) -> None:
    """Rejects the JSON constants that RFC 8259 does not allow, as a strict consumer does."""

    def reject(token: str) -> None:
        raise ValueError(f"not valid JSON: {token}")

    json.loads(json.dumps(event), parse_constant=reject)


P3_BROKEN = dict.fromkeys(
    PATHS,
    "audit S1 — sanitize returns float unchanged, so NaN/Infinity reach json.dumps and "
    "produce output a strict consumer rejects, with no truncated marker",
)


@pytest.mark.parametrize("path", [_xfail(p, P3_BROKEN) for p in PATHS])
async def test_every_emitted_event_is_strictly_serializable(path: str) -> None:
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)
    await _emit_on(path, lambda: log_foundry.info("m", ratio=float("nan"), rate=float("inf")))
    log_foundry.flush(timeout=2.0)

    assert sink.events, f"nothing reached the sink on the {path} path"
    for event in sink.events:
        _strict_json(event)


# -- Promise 4: arguments and return values are never captured --------------------------------
# README: "never captures your arguments or return values". architecture.md §6.

ARG_MARKER = "unmistakable-argument-value-marker"


@pytest.mark.parametrize("path", PATHS)
async def test_arguments_are_never_captured(path: str) -> None:
    sink = Recorder()
    log_foundry.configure(service="t", sink=sink)

    @log_foundry.trace
    def with_args(token: str) -> str:
        log_foundry.info("inside")
        return token

    if path == "post_shutdown":
        log_foundry.shutdown()
    with_args(ARG_MARKER)
    log_foundry.flush(timeout=2.0)

    rendered = json.dumps(sink.events, default=str)
    assert ARG_MARKER not in rendered, f"an argument value reached the event stream on {path}"


# -- Promise 5: the library's own diagnostics name types, never messages ----------------------
# architecture.md §6, SPEC-029.

MESSAGE_MARKER = "psycopg-style-statement-with-bound-parameters"


@pytest.mark.parametrize("path", PATHS)
async def test_diagnostics_never_carry_an_exception_message(path: str, capsys) -> None:
    class Leaky(Recorder):
        def emit(self, batch: list[dict]) -> None:
            raise RuntimeError(MESSAGE_MARKER)

    log_foundry.configure(service="t", sink=Leaky())
    capsys.readouterr()
    try:
        await _emit_on(path, lambda: log_foundry.info("trigger the failure"))
    except Exception:
        pass  # promise 1 covers whether this escapes; this test is about what is written
    log_foundry.flush(timeout=2.0)

    assert MESSAGE_MARKER not in capsys.readouterr().err, (
        f"the library wrote an exception message to stderr on the {path} path"
    )
