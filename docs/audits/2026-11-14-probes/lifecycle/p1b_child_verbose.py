"""P1b child: no injection. The main thread loops over ordinary calls; a one-shot real-time timer
delivers SIGALRM at a pseudo-random instant, and the handler does what a SIGTERM handler would:
shutdown(timeout=2), then exit 0. Exit 0 = handler returned; exit 1 = watchdog fired (hang)."""
import faulthandler, os, random, signal, sys
import log_foundry

mode, seed = sys.argv[1], int(sys.argv[2])
faulthandler.dump_traceback_later(3, exit=True, file=sys.stderr)

class Sink:
    def emit(self, batch): pass
    def close(self): pass

log_foundry.configure(service="p1b", sink=Sink())

@log_foundry.trace
def work(): log_foundry.info("in")

def on_alarm(signum, frame):
    log_foundry.shutdown(timeout=2)
    os._exit(0)
signal.signal(signal.SIGALRM, on_alarm)
random.seed(seed)
signal.setitimer(signal.ITIMER_REAL, 0.02 + random.random() * 0.03)
while True:
    if mode == "flush":       # an orphan-only process that flushes (a Lambda-style handler loop)
        log_foundry.info("x"); log_foundry.flush(timeout=1)
    elif mode == "span":      # a traced call per iteration, then a flush
        work(); log_foundry.flush(timeout=1)
    else:                     # 'restart': info() after a shutdown() takes the locked path each time
        log_foundry.info("x"); log_foundry.shutdown(timeout=1)
