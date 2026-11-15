"""SPEC-084 FR-002 — a real stream cut partway never writes an event twice, and a dead one raises.

The end-to-end proofs, POSIX only. A child process lowers ``RLIMIT_FSIZE`` so the kernel itself
takes part of a 200-event batch through a ``StdoutSink`` and then refuses with ``EFBIG``, in three
stream shapes: a ``TextIOWrapper`` over a 4,096-byte ``BufferedWriter``, block- and line-buffered,
whose raw handle lifts the limit after its first refusal, and the unbuffered shape ``python -u``
gives ``sys.stdout``, whose limit a recording subclass lifts when the first ``deliver`` returns.
The batch goes through a real ``Worker`` at its default ``max_retries``. Before this spec the sink
raised and the worker resent the whole batch, writing again every line the stream had taken.

A child process, because the limit is the process's and a test run must not inherit it. The dead
pipe runs in this process: ``SIGPIPE`` is ignored by CPython at startup, so a write to a pipe
whose reader has gone raises ``BrokenPipeError``. Serves invariants 2 and 7.
"""

from __future__ import annotations

import contextlib
import errno
import io
import json
import os
import re
import signal
import subprocess
import sys
from typing import TYPE_CHECKING, Any

import pytest

from file_handles import Rounds
from log_foundry.sinks.base import SinkDeliveryError
from log_foundry.sinks.stdout import StdoutSink
from log_foundry.sinks.verdict import Outcome

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="RLIMIT_FSIZE and SIGPIPE are POSIX"
)

_PRELUDE = r"""
import errno
import io
import json
import resource
import signal
import sys

from log_foundry.sinks import _engine
from log_foundry.sinks.stdout import StdoutSink
from log_foundry.worker import Worker

signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
_engine._wait = lambda delay, stop=None: stop


def event(i):
    # 120 bytes a line, newline included, whatever the width of i.
    bare = len(json.dumps({"i": i, "pad": ""})) + 1
    return {"i": i, "pad": "x" * (120 - bare)}


def lift():
    resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))


def cap_at(limit):
    resource.setrlimit(resource.RLIMIT_FSIZE, (limit, hard))
"""

_CAPPED = (
    _PRELUDE
    + r"""
cap, path, mode = int(sys.argv[1]), sys.argv[2], sys.argv[3]
rounds = []


class Lifting(io.FileIO):
    # The raw handle under the buffered shapes; lifts the limit after the kernel's first EFBIG.
    refused = 0

    def write(self, data):
        try:
            return super().write(data)
        except OSError as err:
            if err.errno == errno.EFBIG and not Lifting.refused:
                Lifting.refused += 1
                lift()
            raise


class Recording(StdoutSink):
    # Reads each round's verdict; on the unbuffered shape, lifts the limit after the first.
    def deliver(self, batch):
        verdict = super().deliver(batch)
        rounds.append([[e["i"] for e in batch], [o.value for o in verdict.outcomes]])
        if mode == "unbuffered" and len(rounds) == 1:
            lift()
        return verdict


if mode == "unbuffered":
    stream = io.TextIOWrapper(io.FileIO(path, "w"), write_through=True)
else:
    raw = Lifting(path, "w")
    stream = io.TextIOWrapper(
        io.BufferedWriter(raw, buffer_size=4096), line_buffering=(mode == "line")
    )
sink = Recording(stream)
worker = Worker(sink, batch_size=200, flush_interval=60.0)
cap_at(cap)
worker.submit([event(i) for i in range(200)])
flushed = bool(worker.flush(timeout=60.0))
lift()
losses = sink.losses()
stream.close()
print(json.dumps({
    "flushed": flushed,
    "failed_batches": worker.failed_batches,
    "refused": Lifting.refused,
    "rounds": rounds,
    "losses": None if losses is None else [losses.dropped, losses.failed],
}))
"""
)

_SHAPE = (
    _PRELUDE
    + r"""
path, kind = sys.argv[1], sys.argv[2]


class Sub(io.TextIOWrapper):
    pass


wrapper = io.TextIOWrapper if kind == "exact" else Sub
stream = wrapper(io.FileIO(path, "w"), write_through=True)
cap_at(240)
verdict = StdoutSink(stream).deliver([event(i) for i in range(5)])
lift()
print(json.dumps([o.value for o in verdict.outcomes]))
"""
)

_SUSTAINED = (
    _PRELUDE
    + r"""
from log_foundry.sinks.base import SinkDeliveryError

path = sys.argv[1]
with open(path, "w") as seed:
    seed.write(json.dumps(event(-1)) + "\n")
stream = io.TextIOWrapper(io.FileIO(path, "a"), write_through=True)
sink = StdoutSink(stream)
cap_at(120)
worker = Worker(sink, batch_size=200, flush_interval=60.0)
flushes = []
for b in range(3):
    worker.submit([event(b * 10 + i) for i in range(10)])
    flushes.append(bool(worker.flush(timeout=60.0)))
raised = []
for b in range(3):
    try:
        sink.emit([event(100 + b * 10 + i) for i in range(10)])
        raised.append("returned")
    except OSError as err:
        raised.append([type(err).__name__, isinstance(err, SinkDeliveryError), err.errno])
lift()
print(json.dumps({"flushes": flushes, "failed_batches": worker.failed_batches, "raised": raised}))
"""
)

_I = re.compile(rb'"i": (-?\d+)')


def _child(source: str, *args: object) -> Any:
    ran = subprocess.run(  # noqa: S603
        [sys.executable, "-c", source, *map(str, args)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert ran.returncode == 0, ran.stderr
    return json.loads(ran.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("mode", ["block", "line", "unbuffered"])
@pytest.mark.parametrize("cap", [5_000, 8_192, 10_500, 12_000, 16_000, 20_000])
def test_a_capped_stream_never_writes_an_event_twice(cap: int, mode: str, tmp_path: Path) -> None:
    """No event twice, at most one torn line, and every ``ACCEPTED`` event on disk."""
    path = tmp_path / "capped.ndjson"
    report = _child(_CAPPED, cap, path, mode)
    rounds = report["rounds"]
    assert len(rounds[0][0]) == 200, "the premise: one round carried the whole batch"
    if mode == "unbuffered":
        assert rounds[0][1] != ["accepted"] * 200, "the premise: the cap cut the first round"
    else:
        assert report["refused"] == 1, "the premise: the kernel refused once, partway through"
    assert report["flushed"] is True
    assert report["failed_batches"] == 0

    raw = path.read_bytes()
    seen = [int(n) for n in _I.findall(raw)]
    assert len(seen) == len(set(seen)), "an event appears in the file twice"

    parsed: set[int] = set()
    torn: list[bytes] = []
    for line in raw.split(b"\n"):
        if not line:
            continue
        try:
            parsed.add(json.loads(line)["i"])
        except ValueError:
            torn.append(line)
    assert len(torn) <= 1, torn
    if mode == "block" and cap == 10_500:
        assert len(torn) == 1, "the measured torn prefix at block-buffered 10,500"
    in_torn = {int(n) for line in torn for n in _I.findall(line)}

    accepted = {
        i
        for ids, outcomes in rounds
        for i, outcome in zip(ids, outcomes, strict=True)
        if outcome == Outcome.ACCEPTED.value
    }
    assert accepted - parsed <= in_torn, sorted(accepted - parsed - in_torn)

    unknown = sum(
        outcome == Outcome.UNKNOWN.value for _, outcomes in rounds for outcome in outcomes
    )
    if unknown == 0:
        assert report["losses"] is None
    else:
        assert report["losses"] == [0, unknown]


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("exact", ["accepted", "accepted", "retryable", "retryable", "retryable"]),
        ("subclass", ["unknown", "unknown", "unknown", "retryable", "retryable"]),
    ],
)
def test_the_unbuffered_shape_reports_what_the_kernel_took(
    kind: str, expected: list[str], tmp_path: Path
) -> None:
    """A file capped at exactly two lines: the exact shape knows, a subclass gets the general rule."""
    assert _child(_SHAPE, tmp_path / "shape.ndjson", kind) == expected


def test_a_sustained_fault_on_the_unbuffered_shape_fails_every_batch(tmp_path: Path) -> None:
    report = _child(_SUSTAINED, tmp_path / "full.ndjson")
    assert report["flushes"] == [False, False, False]
    assert report["failed_batches"] == 3
    assert [entry[:2] for entry in report["raised"]] == [["OSError", True]] * 3
    assert [entry[2] for entry in report["raised"]] == [errno.EFBIG] * 3


def _pipe_event(batch: int, i: int) -> dict[str, object]:
    return {"b": batch, "i": i, "pad": "x" * 90}


@pytest.mark.parametrize("line_buffering", [False, True], ids=["block", "line"])
def test_a_dead_pipe_settles_the_first_batch_and_raises_from_the_second(
    line_buffering: bool,
) -> None:
    assert signal.getsignal(signal.SIGPIPE) == signal.SIG_IGN, "the premise: SIGPIPE ignored"
    reader, writer = os.pipe()
    os.close(reader)
    stream = io.TextIOWrapper(
        io.BufferedWriter(io.FileIO(writer, "w"), buffer_size=4096),
        line_buffering=line_buffering,
    )
    sink = StdoutSink(stream)
    seen = Rounds(sink)
    try:
        sink.emit([_pipe_event(0, i) for i in range(200)])
        first = seen.outcomes[0]
        handed = first.count(Outcome.UNKNOWN)
        assert handed > 0, first
        assert first == (Outcome.UNKNOWN,) * handed + (Outcome.REJECTED,) * (200 - handed)
        for batch in (1, 2):
            with pytest.raises(BrokenPipeError) as raised:
                sink.emit([_pipe_event(batch, i) for i in range(200)])
            assert isinstance(raised.value, SinkDeliveryError)
    finally:
        with contextlib.suppress(OSError, ValueError):
            stream.close()
