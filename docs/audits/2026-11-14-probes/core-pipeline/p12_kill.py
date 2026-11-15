"""P12: SIGKILL while a root span is open (the Lambda-timeout shape); SIGTERM right after spans closed."""
import collections, json, os, signal, subprocess, sys, time
here = os.path.dirname(os.path.abspath(__file__))

def rows(out):
    return [json.loads(l) for l in open(out)] if os.path.exists(out) else []

def run(mode, sig, settle):
    out = os.path.join(here, f"p12_{mode}.jsonl")
    for p in (out, out + ".ready"):
        if os.path.exists(p): os.remove(p)
    p = subprocess.Popen([sys.executable, "-B", os.path.join(here, "p12_kill_child.py"), out, mode],
                         stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        if sig is None:
            p.wait(30)
        else:
            while not os.path.exists(out + ".ready"):
                if p.poll() is not None: break
            if settle:
                time.sleep(settle)      # several 1 s flush intervals: anything the worker will send, it has sent
            p.send_signal(sig)
            p.wait(30)
    finally:
        if p.poll() is None:
            p.kill()
    err = p.stderr.read().decode()
    got = collections.Counter((r["function"], r["message"]) for r in rows(out))
    return p.returncode, got, err

rc, got, err = run("midspan", signal.SIGKILL, 3.5)
root = sum(c for (fn, m), c in got.items() if fn == "handler")
child = sum(c for (fn, m), c in got.items() if fn == "step")
print(f"[SIGKILL 3.5 s into an open root span] rc={rc}; root span accepted events=51 (span.start + 50 logs) delivered={root}; "
      f"3 closed child spans accepted events=9 delivered={child}; stderr lines={len(err.splitlines())}")
rc, got, err = run("sigterm", signal.SIGTERM, 0)
print(f"[SIGTERM straight after 5 spans closed] rc={rc}; accepted events=15 delivered={sum(got.values())}; stderr lines={len(err.splitlines())}")
rc, got, err = run("normal-exit", None, 0)
print(f"[control: same 5 spans, normal exit] rc={rc}; accepted events=15 delivered={sum(got.values())}")
