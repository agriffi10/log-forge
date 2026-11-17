"""SPEC-100 probe: SPEC-092's keyed-reply rows with an `SNSSink[fifo]` variant beside `SQSSink[fifo]`.

Written on 2026-11-17 while authoring SPEC-100, against 03dbf502; not one of the audit's probes.
It adds two variants to SPEC-092's prototype table: `SNSSink[standard]`, today's `SNSSink` row
under the name a second variant forces, and `SNSSink[fifo]`, built on a FIFO topic ARN with an SNS
double that, like the `SQSSink[fifo]` double, de-duplicates on `MessageDeduplicationId`. It runs
every `keyed-reply` row of the SQS and SNS variants and prints each verdict, so `SNSSink[fifo]`
can be read against `SQSSink[fifo]` fault by fault. The `SNSSink[fifo]` rows run only when the
tree's `SNSSink` takes `fifo` (the SPEC-100 prototype). Run from the repository root:

    PYTHONPATH=src:tests:docs/audits/2026-11-14-probes/sinks python -B \\
        docs/audits/2026-11-14-probes/sinks/spec100_corpus_rows.py

It also prints `SNSSink`'s `__init__` parameters as SPEC-092's `init_parameters` reads them.

**One adaptation, where it is still needed.** SPEC-092's prototype predates SPEC-090, after which
`Worker._emit` takes an `exit_batch` keyword. At 03dbf502 the prototype's `_emit` wrapper does not
accept it, so every row reads `harness-error:batch-count`; SPEC-096's build (`dd77b3fb`) made the
wrapper forward `**kw` itself. This probe loads `spec092_corpus.py` with that one wrapper taking and
forwarding `**kw` when the file does not already, and changes nothing else. Either way the forwarding
form is asserted present exactly once, so a later edit of the prototype that moves the line fails
here rather than passing unadapted.
"""

from __future__ import annotations

import importlib.util
import inspect
import pathlib
import sys
import types
from typing import Any


def _adapted_corpus() -> types.ModuleType:
    """`spec092_corpus`, with its `_emit` wrapper forwarding SPEC-090's `exit_batch` keyword."""
    spec = importlib.util.find_spec("spec092_corpus")
    assert spec is not None and spec.origin is not None
    source = pathlib.Path(spec.origin).read_text(encoding="utf-8")
    pairs = [
        (
            "def emit(self: Worker, event_lists: list[list[dict[str, object]]]) -> None:",
            "def emit(self: Worker, event_lists: list[list[dict[str, object]]], **kw: Any) -> None:",
        ),
        ("original_emit(self, event_lists)", "original_emit(self, event_lists, **kw)"),
    ]
    if "original_emit(self, event_lists, **kw)" not in source:
        for old, new in pairs:
            assert source.count(old) == 1, f"the adaptation's premise moved: {old!r}"
            source = source.replace(old, new)
    assert source.count("original_emit(self, event_lists, **kw)") == 1, "the wrapper forwards no kw"
    module = types.ModuleType("spec092_corpus")
    module.__file__ = spec.origin
    sys.modules["spec092_corpus"] = module
    exec(compile(source, spec.origin, "exec"), module.__dict__)
    return module


corpus = _adapted_corpus()

import spec092_shims as shims  # noqa: E402
import spec092_table as table  # noqa: E402

Backend, run = corpus.Backend, corpus.run

from log_foundry.sinks.sns import SNSSink


class SNSFifoClient:
    """`publish_batch` with a FIFO topic's de-duplication, as `SQSClient(fifo=True)` has it."""

    def __init__(self, backend: Backend) -> None:
        self.backend = backend
        self.dedup: set[str] = set()

    def publish_batch(self, *, TopicArn: str, PublishBatchRequestEntries: list[Any]) -> Any:
        return shims._keyed(self.backend, PublishBatchRequestEntries, "Message", self._store)

    def _store(self, entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            key = entry.get("MessageDeduplicationId")
            if key is not None:
                if key in self.dedup:
                    continue
                self.dedup.add(key)
            self.backend.take(shims.tokens(entry["Message"]))


def main() -> None:
    standard = table.VARIANTS.pop(("SNSSink", ""))
    table.VARIANTS[("SNSSink", "standard")] = standard
    fifo_ok = "fifo" in inspect.signature(SNSSink.__init__).parameters
    if fifo_ok:
        table.VARIANTS[("SNSSink", "fifo")] = (
            "keyed-reply",
            table._same(lambda b: SNSSink("arn:aws:sns:r:1:t.fifo", client=SNSFifoClient(b))),
        )
    else:
        print("SNSSink[fifo] skipped: this tree's SNSSink takes no `fifo`")
    wanted = [r for r in table.rows() if r.sink in ("SQSSink", "SNSSink")]
    for row in wanted:
        verdict, _ = run(row)
        print(f"{row.name:40} {'green' if not verdict else 'red ' + ' '.join(verdict)}")
    print(f"SNSSink parameters: {table.init_parameters()['SNSSink']}")


if __name__ == "__main__":
    main()
