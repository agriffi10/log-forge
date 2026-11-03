---
type: Decision
title: "A read-only finding is not closed until it has been run, and the job that runs it needs a floor rather than an exit code"
description: "fourteen sink modules reach a third party through eleven optional extras and none was ever executed, so the sinks most likely to be in production were the least verified. **A floor, not an exit code:** a fixture that skips on an absent service exits **0**, so a dropped module reads as a smaller pass count and nothing fails — an absent service must fail, and a per-module floor makes a silent shrink loud. (SPEC-041)"
---

**A read-only finding is not closed until it has been run, and the job that runs it needs a floor rather than an exit code** — fourteen sink modules reach a third party through one of eleven optional extras and none was ever executed, so the sinks most likely to be in production were the ones least verified. All three of SPEC-041's inherited "read-only" findings held up, and each was *reproduced before being fixed*: one `pg_terminate_backend` ended `PostgresSink` delivery for the process, a stock Logstash turned a batch of three into **one** event with every field as text in `message`, and NATS delivered **one of six** with every counter at zero. The guard on such a job is the repo's roster idiom, not `pytest`'s exit 5: exit 5 catches only a forgotten gate variable, while a fixture that skips on an absent service exits **0** — measured, a dropped module reads `13 passed` and a skip reads `15 passed, 1 skipped`. So an absent service **fails**, and a per-module floor makes a silent shrink loud. What stays unverified is a **derived** roster (`tests/test_sink_integration_roster.py`), and its population is the lazy-import modules ∪ those importing *or defining* `HTTPSink`/`SocketTransport`, because the obvious single-marker derivation silently omits `logstash` — the spec's own named minimum. **The job earned itself on its first run**, failing three tests no local run could: a service that has bound its port has not necessarily begun serving, so readiness is a real request, never a connect. (SPEC-041, arch §8)

## History

### 2026-09-02

- **Settled.**
