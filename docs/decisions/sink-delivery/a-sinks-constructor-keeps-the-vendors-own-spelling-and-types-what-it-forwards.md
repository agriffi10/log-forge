---
type: Decision
title: "A sink's constructor keeps the vendor's own spelling, and types what it forwards"
description: "`servers`, `queue_url` and `bootstrap_servers` are the vendors' own words, so the names are frozen rather than normalised; the forwarded HTTP keywords are typed instead. (SPEC-051)"
---

**A sink's constructor keeps the vendor's own spelling, and types what it forwards** — the family's parameter names are **frozen as they are** (2026-09-02). Most of the apparent drift is not the library's: a connection parameter carries the vendor's own vocabulary, literally where the client takes that keyword — `servers` is what `nats.connect` takes, `dsn` what `clickhouse_connect.get_client` takes — and as the Python spelling of it where the wire or config key cannot be one: `queue_url` for boto3's `QueueUrl=`, `bootstrap_servers` for confluent-kafka's `bootstrap.servers`. A house vocabulary would put those at odds with the vendor doc the user has open beside them, which is worse than the inconsistency, and renaming across the family is breaking for every consumer. **Checked rather than assumed, because the first draft of this entry claimed more than was true:** `connection_str` and `uri` are *ours*, not Azure's and pymongo's — both are passed positionally to clients whose own parameters are `conn_str` and `host` — and `dsn` for Postgres is the domain's word, since psycopg's is `conninfo`. Those join the residues recorded rather than fixed: the injected pre-built client is `client`/`producer`/`connection`/`opener`, and the destination is keyword-only in a few sinks and positional in most. All of them are addressable additively by an alias if they ever cost anything. `create_table` is **not** in that set: `True` for SQLite and `False` for Postgres and ClickHouse is principled, an embedded file you own against a shared server where DDL needs privileges. What did change is the **types**: the seven platform sinks forward `Unpack[TypedDict]` rather than `**kwargs: object`, so `timeout="not-a-float"` is refused instead of failing at the first request, and each shape excludes exactly what its sink sets or shadows. (SPEC-051 FR-005)

## History

### 2026-09-02

- **Settled.**
