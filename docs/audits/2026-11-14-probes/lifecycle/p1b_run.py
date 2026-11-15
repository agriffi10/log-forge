"""P1b driver: N fresh processes per mode, 8 at a time; counts the watchdog exits."""
import subprocess, sys, concurrent.futures as cf
py, child, n = sys.executable, sys.argv[1], int(sys.argv[2])
def one(args):
    mode, seed = args
    try:
        return subprocess.run([py, "-B", child, mode, str(seed)], timeout=10, capture_output=True).returncode
    except subprocess.TimeoutExpired:
        return "timeout"
for mode in ("flush", "span", "restart"):
    with cf.ThreadPoolExecutor(8) as pool:
        codes = list(pool.map(one, [(mode, s) for s in range(n)]))
    print(f"mode={mode}: {codes.count(1)} of {n} hung (watchdog), {codes.count(0)} returned, other={[c for c in codes if c not in (0,1)]}")
