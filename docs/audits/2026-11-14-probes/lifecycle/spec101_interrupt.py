"""Written for SPEC-101 on 2026-11-17, from its spec reviewer's probe; not one of the audit's.

A KeyboardInterrupt injected into _inline_close_choice, after shutdown()'s take of a caller's
wrapper Leaves(A) that holds a kept A and before the hand-off, puts Leaves(A) back in the record.
Prints whether it is owed again and what the prototype's after-holder record then holds, then runs
a second shutdown() and prints A's closes. On SPEC-101's prototype A must still be attached to the
put-back holder and closed by that second shutdown(). Run from the repository root, never -I:

  PYTHONPATH=<tree>/src python -B docs/audits/2026-11-14-probes/lifecycle/spec101_interrupt.py
"""
import faulthandler
import warnings

warnings.filterwarnings("ignore")
faulthandler.dump_traceback_later(30, exit=True)
import log_foundry  # noqa: E402
from log_foundry import _lifecycle  # noqa: E402


class Rec:
    def __init__(self):
        self.writes = 0
        self.at_entry = []

    def emit(self, batch):
        self.writes += 1

    def close(self):
        self.at_entry.append(self.writes)


class Leaves:
    def __init__(self, m):
        self.m = m

    def emit(self, batch):
        self.m.emit(batch)

    def close(self):
        pass


A = Rec()
log_foundry.configure(service="interrupt", sink=A)
log_foundry.info("one")
L = Leaves(A)
log_foundry.configure(sink=L)
log_foundry.info("two")
real = _lifecycle._inline_close_choice
fired = []


def boom(*args, **kwargs):
    if not fired:
        fired.append(1)
        raise KeyboardInterrupt("injected after the take, before the hand-off")
    return real(*args, **kwargs)


_lifecycle._inline_close_choice = boom
try:
    log_foundry.shutdown(timeout=2)
except KeyboardInterrupt as exc:
    print("interrupted:", exc)
_lifecycle._inline_close_choice = real
record = getattr(_lifecycle, "_AFTER", None)
held = (
    "absent" if record is None
    else [[type(m).__name__ for m, *_ in members] for _ref, members in record.values()]
)
print("Leaves(A) owed again:", _lifecycle._state._closes.owes(L), "| after-holder record:", held)
log_foundry.shutdown(timeout=2)
print(f"after a second shutdown(): A.writes={A.writes} A.at_entry={A.at_entry}")
