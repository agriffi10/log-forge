"""P18, filed by the orchestrating session from the SPEC-082 build session: slows the NATS sink's first encode past
the engine-matrix harness's 0.2 s round deadline, as a loaded CI runner did in run 43726021314, so two events are
left unattempted. Run from the repo root with the `nats` extra installed."""
import json as real_json, sys, time, types
sys.path.insert(0, "tests")
from log_foundry.sinks import nats as nats_module
import log_foundry
from nats_engine_matrix_harness import CASES, run_case

calls = [0]
def slow_dumps(obj, *a, **k):
    calls[0] += 1
    if calls[0] == 1:
        time.sleep(0.25)
    return real_json.dumps(obj, *a, **k)
nats_module.json = types.SimpleNamespace(dumps=slow_dumps, loads=real_json.loads)
r = run_case(CASES["every-event-unencodable-js-injected"], "worker")
print(log_foundry.__file__)
print(r["lines"][-1]); print({k: r[k] for k in ("failed", "failed_batches", "calls", "publishes")}, len(r["lines"]), "lines")
