"""SPEC-074 FR-004's NATS differential matrix: one harness run against 1.x and against the engine.

Each case drives a real ``NATSSink`` over a self-contained double, through ``emit`` and through
``Worker(sink, max_retries=2)._emit``, and records what a caller can observe: ``losses().failed``,
``failed_batches`` as ``health()`` reports it, the ``deliver`` calls (on the engine) or ``emit``
calls (on 1.x, which has no ``deliver``), the publishes the double saw, the exact ``log-foundry:``
stderr lines in order, and the exception ``emit`` raised. ``scripts/record_nats_engine_matrix.py``
runs it against ``93113e1b`` — 1.x, before the arc — and writes the fixture
``tests/data/nats_engine_matrix_93113e1b.json``; ``tests/test_sinks_nats_engine_matrix.py`` runs it
against the current tree and compares. Nothing here imports another test module, or any name
``93113e1b`` lacks, so the one file runs unchanged on both trees.

Time is the double's: the module's ``time`` is replaced by a clock the double can advance, so "a
publish raising after the deadline" is a publish that moves the clock past it, never a race.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import sys
import time as real_time
import types
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

PUBLISH_TIMEOUT = 0.2
"""The ``publish_timeout`` every case runs under."""

LINE_PREFIX = "log-foundry:"


class Clock:
    """A ``time`` stand-in for ``log_foundry.sinks.nats``: real time plus an offset.

    Attributes:
      offset: Seconds added to every reading; a double moves it to pass a deadline at once.
    """

    def __init__(self) -> None:
        self.offset = 0.0

    def monotonic(self) -> float:
        return real_time.monotonic() + self.offset

    def __getattr__(self, name: str) -> Any:
        return getattr(real_time, name)


@dataclass
class Script:
    """What each publish does, by its 0-based number within one call.

    Steps: ``"ok"``, ``"raise"`` (``RuntimeError``), ``"ack"`` (``TimeoutError``, a JetStream
    ack wait the driver expired), ``"hold"`` (never returns until close), ``"late-raise"`` and
    ``"late-ok"`` (move the clock past the deadline first). Every call of the sink restarts at
    step 0, so a worker's three attempts behave alike.
    """

    steps: list[str]
    default: str = "ok"
    publishes: int = 0
    calls: int = 0
    _index: int = 0
    clock: Clock = field(default_factory=Clock)
    gate: asyncio.Event | None = None

    def begin_call(self) -> None:
        self._index = 0
        self.calls += 1

    async def publish(self) -> None:
        step = self.steps[self._index] if self._index < len(self.steps) else self.default
        self._index += 1
        self.publishes += 1
        if step.startswith("late"):
            self.clock.offset += 10.0
        if step in ("raise", "late-raise"):
            raise RuntimeError("no responders")
        if step == "ack":
            raise TimeoutError("ack wait expired")
        if step == "hold":
            if self.gate is None:
                self.gate = asyncio.Event()
            await self.gate.wait()


class JetStream:
    def __init__(self, script: Script) -> None:
        self.script = script

    async def publish(self, subject: str, payload: bytes, *, timeout: float | None = None) -> None:  # noqa: ASYNC109
        await self.script.publish()


class Client:
    """A ``nats-py``-shaped client: answered PINGs, an empty pending buffer, no reconnects."""

    def __init__(self, script: Script) -> None:
        self.script = script
        self.is_connected = True
        self.pending_data_size = 0
        self.stats = {"reconnects": 0}

    async def publish(self, subject: str, payload: bytes) -> None:
        await self.script.publish()

    def jetstream(self) -> JetStream:
        return JetStream(self.script)

    async def flush(self, timeout: float | None = None) -> None:  # noqa: ASYNC109
        return None

    async def close(self) -> None:
        return None

    async def drain(self) -> None:
        return None


@dataclass(frozen=True)
class Case:
    """One row: the publishes' script, the batch, the client's mode and whether the sink owns it."""

    steps: tuple[str, ...]
    events: tuple[str, ...]
    jetstream: bool
    owned: bool
    default: str = "ok"
    prelude: bool = False


def _events(kinds: tuple[str, ...]) -> list[dict[str, object]]:
    return [
        {"n": i, "blob": b"\x00raw"} if kind == "bad" else {"n": i} for i, kind in enumerate(kinds)
    ]


def _cases() -> dict[str, Case]:
    rows: dict[str, tuple[tuple[str, ...], tuple[str, ...], str, tuple[str, ...]]] = {
        "every-publish-raising": (("raise",) * 3, ("ok",) * 3, "raise", ("core", "js")),
        "every-ack-wait-expiring": (("ack",) * 3, ("ok",) * 3, "ack", ("js",)),
        "every-event-unencodable": ((), ("bad",) * 3, "ok", ("core", "js")),
        "raise-after-the-deadline": (("late-raise",), ("ok",) * 5, "ok", ("core", "js")),
        "landed-beside-a-raise": (("ok", "raise", "ok"), ("ok",) * 3, "ok", ("core", "js")),
        "landed-beside-an-unencodable": (("ok", "ok"), ("ok", "bad", "ok"), "ok", ("core", "js")),
        "landed-beside-the-unattempted": (("late-ok",), ("ok",) * 5, "ok", ("core", "js")),
        "an-abandoned-publish": (("ok", "hold"), ("ok",) * 3, "ok", ("core", "js")),
        "refuse-while-stuck": ((), ("ok",) * 3, "ok", ("core", "js")),
    }
    cases: dict[str, Case] = {}
    for name, (steps, kinds, default, modes) in rows.items():
        for mode in modes:
            for owned in (False, True):
                if name == "raise-after-the-deadline" and owned:
                    continue
                cases[f"{name}-{mode}-{'owned' if owned else 'injected'}"] = Case(
                    steps,
                    kinds,
                    mode == "js",
                    owned,
                    default,
                    prelude=name == "refuse-while-stuck",
                )
    return cases


CASES = _cases()


@contextlib.contextmanager
def _patched(target: Any, name: str, value: Any) -> Iterator[None]:
    missing = object()
    old = getattr(target, name, missing)
    setattr(target, name, value)
    try:
        yield
    finally:
        if old is missing:
            delattr(target, name)
        else:
            setattr(target, name, old)


def _build(case: Case, script: Script) -> Any:
    from log_foundry.sinks.nats import NATSSink

    client = Client(script)
    if not case.owned:
        return NATSSink(
            "logs", client=client, jetstream=case.jetstream, publish_timeout=PUBLISH_TIMEOUT
        )
    module = types.ModuleType("nats")

    async def connect(*args: Any, **kwargs: Any) -> Client:
        return client

    module.connect = connect  # type: ignore[attr-defined]
    saved = sys.modules.get("nats")
    sys.modules["nats"] = module
    try:
        return NATSSink(
            "logs",
            servers="nats://example:4222",
            jetstream=case.jetstream,
            publish_timeout=PUBLISH_TIMEOUT,
            drain_timeout=0.2,
        )
    finally:
        if saved is None:
            sys.modules.pop("nats", None)
        else:
            sys.modules["nats"] = saved


def run_case(case: Case, path: str) -> dict[str, Any]:
    """Runs one case on one path and returns its observable record."""
    from log_foundry.sinks import nats as nats_module
    from log_foundry.sinks.nats import NATSSink
    from log_foundry.worker import Worker

    script = Script(list(case.steps), case.default)
    entry = "deliver" if "deliver" in vars(NATSSink) else "emit"
    original = vars(NATSSink)[entry]
    calls = [0]

    def counted(self: Any, batch: list[dict[str, object]]) -> Any:
        calls[0] += 1
        script.begin_call()
        return original(self, batch)

    record: dict[str, Any] = {}
    with _patched(nats_module, "time", script.clock), _patched(NATSSink, entry, counted):
        sink = _build(case, script)
        try:
            if case.prelude:
                script.steps = ["hold"]
                with contextlib.redirect_stderr(io.StringIO()):
                    sink.emit([{"n": "prelude"}])
                script.steps = list(case.steps)
                script.publishes = 0
                calls[0] = 0
            batch = _events(case.events)
            stderr = io.StringIO()
            raised: str | None = None
            failed_batches = 0
            with contextlib.redirect_stderr(stderr):
                if path == "emit":
                    try:
                        sink.emit(batch)
                    except Exception as err:
                        raised = type(err).__name__
                else:
                    worker = Worker(sink, max_retries=2)
                    try:
                        worker._emit([batch])
                        failed_batches = worker.health().failed_batches
                    finally:
                        worker.stop()
            record = {
                "failed": sink.losses().failed,
                "failed_batches": failed_batches,
                "calls": calls[0],
                "publishes": script.publishes,
                "lines": [
                    line for line in stderr.getvalue().splitlines() if line.startswith(LINE_PREFIX)
                ],
                "raised": raised,
            }
        finally:
            if script.gate is not None:
                loop = getattr(sink, "_loop", None)
                if loop is not None and loop.is_running():
                    loop.call_soon_threadsafe(script.gate.set)
            with contextlib.redirect_stderr(io.StringIO()), contextlib.suppress(Exception):
                sink.close()
    return record


def run_all() -> dict[str, dict[str, Any]]:
    """Runs every case on both paths."""
    return {
        name: {path: run_case(case, path) for path in ("emit", "worker")}
        for name, case in CASES.items()
    }


def record(out: str, commit: str, command: str) -> None:
    """Writes the fixture for ``commit`` to ``out``; refuses unless it is running 1.x."""
    import importlib.util

    import log_foundry

    if importlib.util.find_spec("log_foundry.sinks._engine") is not None:
        raise SystemExit(
            f"refusing: {log_foundry.__file__} has the delivery engine; this is not 1.x"
        )
    fixture = {
        "commit": commit,
        "command": command,
        "python": sys.version.split()[0],
        "cases": run_all(),
    }
    with open(out, "w", encoding="utf-8") as handle:
        json.dump(fixture, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    record(sys.argv[1], sys.argv[2], sys.argv[3])
