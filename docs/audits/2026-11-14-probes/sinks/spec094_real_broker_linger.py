"""SPEC-094 probe: what the round's wait costs against a real Kafka broker, across `linger.ms`.

Written on 2026-11-16 while revising SPEC-094 after the execution-frame review, against 97a03f45;
not one of the audit's probes. It drives `KafkaSink.deliver` with a real confluent-kafka `Producer`
against the broker `tests/integration/docker-compose.yml` pins (apache/kafka 3.9.1), and prints per
case the wall and CPU milliseconds of each `deliver`, its outcomes (`A` accepted, `U` unknown,
`R` rejected), the sink's losses and its lines. Cases: no broker at all, with and without a set
stop; a live broker at `linger.ms` 5 (the default), 100, 1000 and 3000, the last above
`flush_timeout`; `delivery.report.only.error`; a set stop under `linger.ms` 1000; and a 2 MB
message under a producer `message.max.bytes` above the topic's limit. Run it from a container on
the broker's network, with a patched tree first on `PYTHONPATH`, against two trees to compare:

    docker network create probe-net
    docker run -d --rm --name probe-kafka --network probe-net -e KAFKA_NODE_ID=1 \\
        -e KAFKA_PROCESS_ROLES=broker,controller \\
        -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \\
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://probe-kafka:9092 \\
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \\
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \\
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \\
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 apache/kafka@sha256:4ceccc57...
    docker run --rm --network probe-net -v ...:/w <image with confluent-kafka 2.16.1> \\
        sh -c 'PYTHONPATH=<tree>/src python -B spec094_real_broker_linger.py probe-kafka:9092'
"""

from __future__ import annotations

import contextlib
import io
import resource
import sys
import threading
import time

from confluent_kafka import Producer, libversion

from log_foundry.sinks.kafka import KafkaSink


def cpu() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def run(label, conf, n=10, emits=5, flush_timeout=2.0, big=None, stop_set=False, topic="t1"):
    sink = KafkaSink(topic, producer=Producer(conf), flush_timeout=flush_timeout)
    if stop_set:
        sink.log_foundry_stop_signal = threading.Event()
        sink.log_foundry_stop_signal.set()
    times, outs = [], []
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        for i in range(emits):
            batch = [{"message": f"{label}-{i}-{j}"} for j in range(n)]
            if big:
                batch[0]["pad"] = "x" * big
            c0, t0 = cpu(), time.monotonic()
            verdict = sink.deliver(batch)
            times.append((round((time.monotonic() - t0) * 1000, 1), round((cpu() - c0) * 1000, 1)))
            outs.append("".join(o.name[0] for o in verdict.outcomes))
    lines = [x for x in err.getvalue().splitlines() if "log-foundry" in x]
    print(f"{label:<34} wall/cpu ms per deliver={times}")
    print(f"{'':<34} outcomes={outs} losses={sink.losses()} lines={len(lines)}")
    for line in lines[:2]:
        print("      ", line[:160])
    with contextlib.suppress(Exception):
        sink.close()


def main() -> int:
    broker = sys.argv[1] if len(sys.argv) > 1 else "probe-kafka:9092"
    base = {"bootstrap.servers": broker}
    dead = {"bootstrap.servers": "127.0.0.1:1"}
    print("librdkafka", libversion()[0])
    run("no broker, timeout 0.5", dead, flush_timeout=0.5, emits=3)
    run("no broker, stop set", dead, flush_timeout=5, emits=3, stop_set=True)
    run("warmup (topic autocreate)", base, emits=3, flush_timeout=10)
    run("linger default (5 ms)", base, emits=8)
    run("linger 100", {**base, "linger.ms": 100}, emits=6)
    run("linger 1000, flush_timeout 5", {**base, "linger.ms": 1000}, emits=4, flush_timeout=5)
    run("linger 3000, flush_timeout 1", {**base, "linger.ms": 3000}, emits=4, flush_timeout=1)
    only_error = {**base, "delivery.report.only.error": True, "linger.ms": 200}
    run("report only error, linger 200", only_error, emits=5)
    linger = {**base, "linger.ms": 1000}
    run("stop set, linger 1000", linger, emits=4, flush_timeout=5, stop_set=True)
    large = {**base, "message.max.bytes": 5000000}
    run("2 MB message over the topic limit", large, n=3, emits=2, big=2_000_000)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
