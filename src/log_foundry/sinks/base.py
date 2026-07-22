"""The sink interface (arch §8).

A sink is the swappable output transport. It receives *already-built, batched* event
dicts from the worker and knows nothing about spans or context — that dumbness is what
makes sinks trivially interchangeable (StdoutSink, SQSSink, …). Only the Protocol lives
here; concrete sinks arrive in later phases.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Sink(Protocol):
    def emit(self, batch: list[dict[str, object]]) -> None:
        """Ship a batch of serialized event dicts."""
        ...

    def close(self) -> None:
        """Flush and release any resources."""
        ...
