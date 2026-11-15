"""P11: assembly faults that are swallowed to protect the caller: are they counted?"""
import sys
import log_foundry as lf
from log_foundry import decorator, model
from fakes import CountingSink, StderrCapture, show_health

sink = CountingSink()
lf.configure(sink=sink)

@lf.trace
def work(i):
    lf.info("in-span", i=i)
    return i

# (a) INJECTED fault: start_event raises while the span is being opened
real_start = decorator.start_event
def boom(span): raise MemoryError()
with StderrCapture() as err:
    decorator.start_event = boom
    out = work(1)
    decorator.start_event = real_start
    lf.flush(5)
    h = show_health(lf, "(a) span open failed [injected]")
    print(f"    call returned {out}; sink has start={sink.count('span.start')} end={sink.count('span.end')} in-span={sink.count('in-span')}")
    for l in err.lines(): print("    stderr:", l)

# (b) INJECTED fault: end_event raises while the span is being closed
real_end = decorator.end_event
def boom2(span, status, exc=None): raise MemoryError()
sink.events.clear()
with StderrCapture() as err:
    decorator.end_event = boom2
    work(2)
    decorator.end_event = real_end
    lf.flush(5)
    h2 = show_health(lf, "(b) span close failed [injected]")
    print(f"    events the span would have carried=3 (start, in-span, end); in_span_lost moved by {h2.in_span_lost - h.in_span_lost}; delivered={sink.count()}")
    for l in err.lines(): print("    stderr:", l)

# (c) NOT injected: a traced recursive function that runs into the recursion limit
sink.events.clear()
entered = [0]
@lf.trace
def deep(n):
    entered[0] += 1
    return deep(n + 1)

before = lf.health()
with StderrCapture() as err:
    sys.setrecursionlimit(400)
    try:
        deep(0)
    except RecursionError:
        pass
    sys.setrecursionlimit(1000)
    lf.flush(10)
    h3 = show_health(lf, "(c) RecursionError inside a traced recursion [not injected]")
    starts, ends = sink.count("span.start"), sink.count("span.end")
    lost = h3.in_span_lost - before.in_span_lost
    print(f"    function bodies entered={entered[0]}; delivered span.start={starts} span.end={ends}; in_span_lost +{lost}; dropped +{h3.dropped-before.dropped}")
    print(f"    events expected if every entered call were traced={2*entered[0]}; delivered+counted={starts+ends+lost}; unaccounted={2*entered[0]-(starts+ends+lost)}")
    kinds = {}
    for l in err.lines():
        k = l.split("(")[0][:80]
        kinds[l] = kinds.get(l, 0) + 1
    for l, c in kinds.items(): print(f"    stderr x{c}:", l)
lf.shutdown()
