"""SPEC-095 probe: when an engine resend of an SQS FIFO entry can fall past the 5-minute window.

Written on 2026-11-16 while authoring SPEC-095, against 97a03f45; not one of the audit's probes.
It reads the engine's own wait functions (`_engine._backoff`, `_engine.cycle_backoff`) and the
budget `_run` computes, so it measures the tree on the path. Run from the repository root:

    PYTHONPATH=src python -B docs/audits/2026-11-14-probes/sinks/spec095_fifo_window.py

For each case it prints the earliest moment each round can start, counted from the start of the
first, when every round before it ends in an exception: the waits the engine adds, plus each
earlier round's own duration. A round's duration is the case's assumption, not a measurement:
`0` is a client that fails at once; `300` is five botocore attempts and `180` three, each ending
at the 60-second read timeout, botocore's default `read_timeout` (`botocore/endpoint.py`,
`DEFAULT_TIMEOUT = 60`). At botocore 1.43.127 the default retry mode is `legacy`, five attempts,
unless `AWS_NEW_RETRIES_2026` selects `standard`, three (`botocore/configprovider.py:23-41`;
boto3's retries guide gives both counts). Revised on 2026-11-16 after SPEC-095's review, which
found the first revision had assumed `standard` was the default. It marks the first round that
starts more than 300 seconds
after round one, which is where a resend of an entry first sent in round one is outside the
deduplication interval SQS documents for `MessageDeduplicationId`.
"""

from __future__ import annotations

from log_foundry.sinks import _engine

WINDOW = 300.0
"""The 5-minute deduplication interval the SendMessage reference gives."""


def starts(sink_retries: int, worker_retries: int, duration: float) -> list[float]:
    """The earliest start of each round of one run, every earlier round failing."""
    per_cycle = sink_retries + 1
    rounds = per_cycle * (worker_retries + 1)
    moments = [0.0]
    for attempts in range(1, rounds):
        cycle, position = divmod(attempts, per_cycle)
        wait = _engine.cycle_backoff(cycle - 1) if position == 0 else _engine._backoff(position - 1)
        moments.append(moments[-1] + duration + wait)
    return moments


def main() -> int:
    cases = [
        ("SQSSink defaults, calls fail at once", 3, 3, 0.0),
        ("SQSSink defaults, legacy mode: each call times out five times", 3, 3, 300.0),
        ("SQSSink defaults, standard mode: each call times out three times", 3, 3, 180.0),
        ("max_retries=20, calls fail at once", 20, 3, 0.0),
    ]
    for label, sink_retries, worker_retries, duration in cases:
        moments = starts(sink_retries, worker_retries, duration)
        past = next((i + 1 for i, at in enumerate(moments) if at > WINDOW), None)
        print(f"{label}: {len(moments)} rounds, last starts at {moments[-1]:.2f} s")
        where = f"round {past}" if past is not None else "no round"
        print(f"  first round past {WINDOW:.0f} s after round one: {where}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
