"""P12 child: a process that is killed while a span is open, or right after spans closed."""
import json, os, sys
import log_foundry as lf

OUT, MODE = sys.argv[1], sys.argv[2]

class FileSink:
    def emit(self, batch):
        fd = os.open(OUT, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
        try:
            for e in batch:
                os.write(fd, (json.dumps({"message": e["message"], "function": e["function"]}) + "\n").encode())
        finally:
            os.close(fd)
    def close(self):
        pass

lf.configure(sink=FileSink())

@lf.trace
def step(i):
    lf.info("step-log", i=i)

def ready_and_block():
    open(OUT + ".ready", "w").close()
    sys.stdin.read()          # blocks until the parent kills this process

@lf.trace
def handler():
    for i in range(50):
        lf.info("handler-log", i=i)
    for i in range(3):
        step(i)
    ready_and_block()

if MODE == "midspan":
    handler()
elif MODE == "sigterm":
    for i in range(5):
        step(i)
    ready_and_block()
elif MODE == "normal-exit":
    for i in range(5):
        step(i)
