"""Written for SPEC-101 on 2026-11-17 against a38bd80c, not one of the audit's probes.

Option (a) gives a member of a caller's wrapper that closes its member a second close() in
sequence. This constructs each shipped sink that needs no network, extra or server, writes one
event, closes it twice in sequence, and prints per sink whether either close raised, whether the
sink refuses after the first, and whether it carries SPEC-076's ClosedGuard at _guard, which is
what option (b) reads. Run from the repository root, never -I:

  PYTHONPATH=<tree>/src python -B docs/audits/2026-11-14-probes/lifecycle/spec101_second_close.py
"""
import faulthandler
import logging
import os
import tempfile

from log_foundry.sinks._guard import ClosedGuard
from log_foundry.sinks.callback import CallbackSink
from log_foundry.sinks.file import FileSink, RotatingFileSink
from log_foundry.sinks.http import HTTPSink
from log_foundry.sinks.logging_sink import LoggingSink
from log_foundry.sinks.memory import MemorySink
from log_foundry.sinks.null import NullSink
from log_foundry.sinks.sqlite import SQLiteSink
from log_foundry.sinks.stdout import StdoutSink

faulthandler.dump_traceback_later(30, exit=True)
d = tempfile.mkdtemp(prefix="spec101-close-")
EVENT = {"event": "x", "level": "info", "log_id": "0" * 32}
BUILD = [
    ("StdoutSink", lambda: StdoutSink()),
    ("MemorySink", lambda: MemorySink()),
    ("NullSink", lambda: NullSink()),
    ("CallbackSink", lambda: CallbackSink(lambda batch: None)),
    ("LoggingSink", lambda: LoggingSink(logging.getLogger("spec101"))),
    ("FileSink", lambda: FileSink(os.path.join(d, "f.jsonl"))),
    ("RotatingFileSink", lambda: RotatingFileSink(os.path.join(d, "r.jsonl"), max_bytes=10_000)),
    ("SQLiteSink", lambda: SQLiteSink(os.path.join(d, "s.db"))),
    ("HTTPSink", lambda: HTTPSink("http://127.0.0.1:9/")),
]
for name, build in BUILD:
    try:
        sink = build()
    except Exception as exc:
        print(f"{name:17s} not built: {type(exc).__name__}: {str(exc)[:80]}")
        continue
    if name != "HTTPSink":
        sink.emit([dict(EVENT)])
    raised = []
    for _ in range(2):
        try:
            sink.close()
            raised.append("-")
        except Exception as exc:
            raised.append(type(exc).__name__)
    after = "not tried"
    if name != "HTTPSink":
        try:
            sink.emit([dict(EVENT)])
            after = "accepts"
        except Exception as exc:
            after = f"refuses ({type(exc).__name__})"
    guard = vars(sink).get("_guard")
    guarded = isinstance(guard, ClosedGuard)
    print(
        f"{name:17s} first close raised={raised[0]} second close raised={raised[1]} "
        f"emit after close {after}; closed guard at _guard={guarded}"
        + (f" reads closed={guard.closed}" if guarded else "")
    )
