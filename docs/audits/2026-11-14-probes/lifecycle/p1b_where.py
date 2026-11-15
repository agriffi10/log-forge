"""P1b follow-up: where the hung trials are stuck. Prints, per hung trial, the lock line the handler's
shutdown() is blocked on and the interrupted library frame that already holds a lock."""
import collections, re, subprocess, sys, concurrent.futures as cf
py, child, n = sys.executable, sys.argv[1], int(sys.argv[2])
def one(args):
    mode, seed = args
    r = subprocess.run([py, "-B", child, mode, str(seed)], timeout=10, capture_output=True, text=True)
    if r.returncode != 1:
        return None
    frames = re.findall(r'File ".*?/(?:src/log_foundry|lib/python3\.\d+)/([\w/]+\.py)", line (\d+) in (\w+)', r.stderr)
    main = r.stderr.split("Thread 0x")[-1]          # the last block is the main thread
    frames = re.findall(r'/([\w]+\.py)", line (\d+) in (\w+)', main)
    i = next(k for k, f in enumerate(frames) if f[2] == "on_alarm")
    return (mode, " <- ".join(f"{a}:{b} {c}" for a, b, c in frames[:i][-1:]+frames[:1]), " interrupted at " + " <- ".join(f"{a}:{b} {c}" for a, b, c in frames[i+1:i+3]))
tally = collections.Counter()
with cf.ThreadPoolExecutor(8) as pool:
    for res in pool.map(one, [(m, s) for m in ("flush", "span", "restart") for s in range(n)]):
        if res: tally[res] += 1
for (mode, blocked, held), count in sorted(tally.items()):
    print(f"{count:3d}  {mode:8s} blocked: {blocked}\n     {held}")
