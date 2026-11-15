"""P4: interpreter exit with a slow sink; what the default atexit budget carries."""
import os, subprocess, sys, time
here = os.path.dirname(os.path.abspath(__file__))
out = os.path.join(here, "p04_out.txt")
for p in (out, out + ".closed"):
    if os.path.exists(p): os.remove(p)
per_chunk, spans = sys.argv[1], sys.argv[2]
t0 = time.monotonic()
p = subprocess.run([sys.executable, "-B", os.path.join(here, "p04_exit_child.py"), out, per_chunk, spans],
                   capture_output=True, text=True, timeout=120)
took = time.monotonic() - t0
print(p.stdout.strip())
sent, calls, biggest = open(out).read().split()
print(f"exit code={p.returncode}; process took {took:.1f}s to exit; sink received {sent} of {2*int(spans)} events "
      f"in {calls} emit call(s), largest batch {biggest} events; sink.close() ran={os.path.exists(out + '.closed')}")
print(f"lost at exit = {2*int(spans) - int(sent)} events; no counter survives the process")
for l in p.stderr.splitlines():
    print("  stderr:", l)
