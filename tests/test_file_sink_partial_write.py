"""SPEC-082 FR-001 — a file-size limit cuts a batch partway, and each event still lands once.

The end-to-end proof, POSIX only: a child process lowers ``RLIMIT_FSIZE`` so the kernel itself
takes part of a 200-event batch and then refuses with ``EFBIG``, and the sink's handle restores
the limit after that first refusal, as a disk that was full and then was not. The batch goes
through a real ``Worker`` at its default ``max_retries``. Before this spec the sink raised, the
worker resent the whole batch, and 69 to 200 of the 200 events landed twice depending on the cap;
at 10,500 bytes a line was also cut and the resend glued the next line to it.

A child process, because the limit is the process's and a test run must not inherit it. Serves
invariants 2 and 7.
"""

from __future__ import annotations

import json
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="RLIMIT_FSIZE is POSIX: the resource module is not on Windows"
)

_CHILD = r"""
import errno
import json
import resource
import sys

import log_foundry
from log_foundry import _lifecycle
from log_foundry.sinks.file import FileSink
from log_foundry.worker import Worker

cap, path = int(sys.argv[1]), sys.argv[2]
soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)


class Lifting:
    # The sink's real handle; restores the soft limit after the kernel's first EFBIG.
    def __init__(self, real):
        self.real = real
        self.refused = 0

    def write(self, data):
        try:
            return self.real.write(data)
        except OSError as err:
            if err.errno == errno.EFBIG:
                self.refused += 1
                resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
            raise

    def close(self):
        self.real.close()


def event(i):
    # 120 bytes a line, newline included, whatever the width of i.
    bare = len(json.dumps({"i": i, "pad": ""})) + 1
    return {"i": i, "pad": "x" * (120 - bare)}


sink = FileSink(path)
handle = Lifting(sink._stream)
sink._stream = handle
worker = Worker(sink, batch_size=10, flush_interval=60.0)
with _lifecycle._state._lock:
    _lifecycle._state._worker = worker
    _lifecycle._state._closes.arm(worker.sink)
    _lifecycle._state._move()
resource.setrlimit(resource.RLIMIT_FSIZE, (cap, hard))
worker.submit([event(i) for i in range(200)])
flushed = bool(worker.flush(timeout=60.0))
resource.setrlimit(resource.RLIMIT_FSIZE, (soft, hard))
health = log_foundry.health()
print(json.dumps({
    "flushed": flushed,
    "failed_batches": worker.failed_batches,
    "health_sink_is_none": health.sink is None,
    "refused": handle.refused,
    "max_retries": worker.max_retries,
}))
sink.close()
"""


@pytest.mark.parametrize("cap", [5_000, 8_192, 10_500, 12_000, 16_000, 20_000])
def test_a_capped_file_holds_each_event_once_after_the_worker_resends(
    cap: int, tmp_path: Path
) -> None:
    """Each of the 200 events is on disk exactly once and every line parses, at every cap."""
    path = tmp_path / "capped.ndjson"
    ran = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _CHILD, str(cap), str(path)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert ran.returncode == 0, ran.stderr
    report = json.loads(ran.stdout.strip().splitlines()[-1])
    assert report["refused"] == 1, "the premise: the kernel refused once, partway through"
    assert report["flushed"] is True
    assert report["failed_batches"] == 0
    assert report["health_sink_is_none"] is True
    lines = path.read_bytes().split(b"\n")
    assert lines[-1] == b"", "the file ends on a line boundary"
    assert all(len(line) == 119 for line in lines[:-1]), "no line was cut or glued to another"
    assert [json.loads(line)["i"] for line in lines[:-1]] == list(range(200))
