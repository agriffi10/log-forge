"""P17, written while revising SPEC-088 after its second review and not one of the audit's own probes: which ids a log made after an interrupted close ships on.
Row 1 logs in the caller's except handler; row 2 from an asyncio task created inside the span after
the close was interrupted; row 3 is the control, a task that outlives a span that closed normally.
Each row runs in a fresh context. Only `decorator.end_event` is patched, to raise `KeyboardInterrupt`."""
import asyncio, contextvars
import log_foundry as lf
from log_foundry import context, decorator
got = []
lf.configure(sink=type("S", (), {"emit": lambda s, b: got.extend((e["message"], e["trace_id"][:6], e["span_id"][:6]) for e in b), "close": lambda s: None})())
REAL = decorator.end_event
def ki(*a, **k): raise KeyboardInterrupt
ids = {}
@lf.trace
def work():
    s = context.current_span(); ids["span"] = (s.trace_id[:6], s.span_id[:6]); lf.info("in-span")
def row1():
    decorator.end_event = ki
    try: work()
    except KeyboardInterrupt:
        decorator.end_event = REAL
        lf.info("logged in the except handler")
contextvars.Context().run(row1)
lf.flush()
print("row 1: span", ids["span"], "| sink", got); got.clear()
async def main(interrupt):
    later = asyncio.Event(); out = {}
    @lf.trace
    async def traced():
        s = context.current_span(); out["span"] = (s.trace_id[:6], s.span_id[:6])
        async def child():
            await later.wait(); lf.info("from a task made inside the span")
        out["t"] = asyncio.create_task(child())
    if interrupt: decorator.end_event = ki
    try: await traced()
    except KeyboardInterrupt: pass
    finally: decorator.end_event = REAL
    later.set(); await out["t"]
    return out["span"]
span = contextvars.Context().run(asyncio.run, main(True)); lf.flush(); print("row 2: span", span, "| sink", [g for g in got if g[0].startswith("from")]); got.clear()
span = contextvars.Context().run(asyncio.run, main(False)); lf.flush(); print("row 3 (control, normal close): span", span, "| sink", [g for g in got if g[0].startswith("from")])
