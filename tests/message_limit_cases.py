"""SPEC-096: the requests SQSSink and SNSSink make for the six default-limit cases, recorded.

The cases are A, B, C, E, F1 and F2 of
``docs/audits/2026-11-14-probes/sinks/spec096_limits_today.py``, built with nothing but the 1.x
API, so the same code runs against any tree. Each request is recorded entry by entry: a sha256
of the entry's canonical JSON without its ``Id``, the entry's keys without ``Id``, and its costed
bytes. Beside the requests go ``dropped_oversized``, ``losses().failed`` and the stderr lines
verbatim. ``tests/data/spec096_requests_10ba21ee.json`` is this module's output against a
``git archive 10ba21ee src`` export, written by running it as a script:

    PYTHONPATH=<export>/src:tests python -B tests/message_limit_cases.py <export>/src <out.json>

The script refuses to record unless ``log_foundry`` was imported from under the directory named,
since an editable install of this checkout would otherwise answer for the export.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import pathlib
import sys
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

KIB = 1024
DEFAULT_LIMIT = 262_144
"""The 1.x message limit, written out rather than read from a sink, so case E is sized against
the number the recording was made at and not against whatever the tree under test holds."""


class Recorder:
    """A boto3-shaped SQS and SNS client that takes every entry and records each request."""

    def __init__(self) -> None:
        self.requests: list[list[dict[str, str]]] = []

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, str]]) -> dict[str, Any]:
        self.requests.append([dict(entry) for entry in Entries])
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    def publish_batch(
        self,
        TopicArn: str,
        PublishBatchRequestEntries: list[dict[str, str]],
    ) -> dict[str, Any]:
        self.requests.append([dict(entry) for entry in PublishBatchRequestEntries])
        return {"Successful": [{"Id": e["Id"]} for e in PublishBatchRequestEntries], "Failed": []}


def event(i: int, pad: int) -> dict[str, object]:
    """The probe's event: fixed ids, so a FIFO entry's group and dedup ids are reproducible."""
    return {"log_id": f"l{i:04d}", "trace_id": "t" * 32, "n": i, "pad": "x" * pad}


def costed(entry: dict[str, str]) -> int:
    """An entry's cost as the sinks state it: the UTF-8 body plus, on FIFO, both ids."""
    keys = ("MessageBody", "Message", "MessageGroupId", "MessageDeduplicationId")
    return sum(len(entry[key].encode("utf-8")) for key in keys if key in entry)


def entry_record(entry: dict[str, str]) -> dict[str, object]:
    """One entry as the recording keeps it: everything but its ``Id``, hashed, keyed and costed."""
    rest = {key: value for key, value in entry.items() if key != "Id"}
    canonical = json.dumps(rest, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {
        "sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "keys": sorted(rest),
        "cost": costed(entry),
    }


def request_records(requests: list[list[dict[str, str]]]) -> list[list[dict[str, object]]]:
    """Every request as the recording keeps it, entry by entry and in order."""
    return [[entry_record(entry) for entry in request] for request in requests]


def run(sink: Any, batch: list[dict[str, object]]) -> dict[str, object]:
    """Emits one batch through a sink holding a ``Recorder`` and returns what it did."""
    stop = threading.Event()
    stop.set()
    sink.log_foundry_stop_signal = stop
    err = io.StringIO()
    raised: str | None = None
    with contextlib.redirect_stderr(err):
        try:
            sink.emit(batch)
        except Exception as exc:
            raised = type(exc).__name__
    return {
        "requests": request_records(sink.client.requests),
        "dropped_oversized": sink.dropped_oversized,
        "failed": sink.losses().failed,
        "raised": raised,
        "lines": [line for line in err.getvalue().splitlines() if line.strip()],
    }


def _fifo_edge() -> dict[str, object]:
    """Case E's event: its body alone ten bytes under 256 KiB, its body plus FIFO ids over."""
    bare = len(json.dumps(event(0, 0)).encode("utf-8"))
    return event(0, DEFAULT_LIMIT - bare - 10)


def cases() -> dict[str, tuple[Callable[[], Any], list[dict[str, object]]]]:
    """Each case's sink factory, through the 1.x constructors only, and its batch."""
    from log_foundry.sinks.sns import SNSSink
    from log_foundry.sinks.sqs import SQSSink

    return {
        "A": (
            lambda: SQSSink("https://q", client=Recorder()),
            [event(i, 100 * KIB) for i in range(10)],
        ),
        "B": (
            lambda: SQSSink("https://q", client=Recorder()),
            [event(i, 30 * KIB) for i in range(10)],
        ),
        "C": (
            lambda: SQSSink("https://q", client=Recorder()),
            [event(0, 10), event(1, 300 * KIB), event(2, 10)],
        ),
        "E": (lambda: SQSSink("https://q.fifo", client=Recorder()), [_fifo_edge()]),
        "F1": (
            lambda: SNSSink("arn:t", client=Recorder()),
            [event(i, 100 * KIB) for i in range(10)],
        ),
        "F2": (
            lambda: SNSSink("arn:t", client=Recorder()),
            [event(0, 10), event(1, 300 * KIB), event(2, 10)],
        ),
    }


def record() -> dict[str, dict[str, object]]:
    """Runs every case against the ``log_foundry`` this process imports."""
    return {name: run(factory(), batch) for name, (factory, batch) in cases().items()}


def dumps(recording: dict[str, dict[str, object]]) -> str:
    """The recording's file form: sorted keys and a trailing newline, so equal runs are equal bytes."""
    return json.dumps(recording, sort_keys=True, indent=1) + "\n"


def main(argv: list[str]) -> int:
    """Records against the tree under ``argv[1]`` and writes the JSON to ``argv[2]``."""
    import log_foundry

    source = pathlib.Path(argv[1]).resolve()
    imported = pathlib.Path(log_foundry.__file__).resolve()
    if source not in imported.parents:
        print(f"log_foundry was imported from {imported}, not from under {source}", file=sys.stderr)
        return 2
    pathlib.Path(argv[2]).write_text(dumps(record()), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
