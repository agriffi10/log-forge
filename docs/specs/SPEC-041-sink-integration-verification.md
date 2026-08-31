# Spec: Sink Integration Verification

**ID:** SPEC-041  
**Status:** In Progress  
**Last Updated:** 2026-08-30  
**Depends On:** SPEC-026, SPEC-027, SPEC-038

## Overview

Eleven of this library's sinks talk to a third-party service through an optional extra, and **not
one of them is executed anywhere** — not in the development environment and not in CI, where the
no-extras environment is deliberately the contract (`CLAUDE.md`, because `mypy --strict`'s
`type: ignore[import-not-found]` comments depend on it). That is right for the *type* gate and
wrong for correctness: the sinks most likely to be run in production are the ones this project can
least verify.

The gap is not hypothetical. Six of the 2026-08-07 audit's ten sink findings were reached by
**reading, not running**, and four of them cannot be honestly closed without a real service —
so they are here, with the CI job they need, rather than in SPEC-038 where they were written.

They were split out because they are a different kind of work. Standing up Postgres, Redis, Kafka
and a Logstash in CI is infrastructure with its own flakiness budget and its own "when does this
earn the merge gate" decision, and putting it in front of SPEC-038 FR-001 — a measured
5,980-event single `emit` that abandons the whole exit backlog — made the urgent wait on the slow.
Split, both halves move.

**FR-001 is built first and everything else here depends on it**, which is the same ordering
SPEC-038 had internally; what changed is that nothing outside this spec waits on it any more.

## Scope

### In Scope

- A CI job that installs the extras and runs the sink suite against real services in containers.
- `PostgresSink`'s permanently broken connection (audit K3).
- `LogstashSink`'s content type (audit K10).
- Whether `KafkaSink`, `NATSSink` and `GooglePubSubSink` satisfy SPEC-027's retry guarantee
  through their clients, or need bounded retry adding.
- Writing down what is *still* unverified after the job exists.

### Out of Scope

- **Changing the gating no-extras environment.** It stays exactly as it is: `mypy --strict`'s
  ignore comments depend on it, and `CLAUDE.md` records that CI's no-extras environment is the
  contract.
- **Every sink fix a fake can settle.** Those are SPEC-038, which builds first and independently.
- **Making this job required for a merge on day one.** FR-001 AC-2.
- **New sinks or new transports.**

---

## Functional Requirements

### FR-001: The extras-backed sinks are executable in CI

#### Description:

Carried unchanged from SPEC-038 FR-011. A correctness spec for sinks nobody can execute is a spec
that can only be half-verified, and three of the FRs below say so in their own acceptance
criteria.

#### Acceptance Criteria:

- [ ] AC-1: A CI job, **separate from the gating no-extras one**, installs the extras and runs the
      sink suite against real services in containers — at minimum Postgres, Redis, Kafka and a
      Logstash, which are the four with findings here that a fake cannot settle.
- [ ] AC-2: It is **not** required for a merge initially. A new, flaky-by-nature integration job
      that blocks every PR is a job that gets disabled; it earns the gate by being green for a
      while first, and this FR states when that decision gets revisited and who makes it.
- [ ] AC-3: The no-extras environment stays exactly as it is (`CLAUDE.md`).
- [ ] AC-4: Which sinks remain unverified after this job exists is written down, so the next audit
      knows what it is still reading rather than running.
- [ ] AC-5: The job is pinned and least-privileged like every other workflow here (SPEC-022):
      actions at a commit SHA with a `# vX.Y.Z` comment Dependabot can rewrite, and service
      containers pinned by digest. A new workflow that reintroduces mutable tags undoes that spec
      in the file nobody re-reads.

### FR-002: `PostgresSink` recovers from a broken connection

#### Description:

Carried from SPEC-038 FR-003. **Read-only analysis** — `sinks/postgres.py:76-80, 149-170`.

The connection is opened once in `__init__` and never reopened. A psycopg `Connection` is
permanently unusable after the server closes it, so one restart, failover or idle timeout ends log
delivery for the life of the process — the in-batch retries all run against the same dead handle.
Every sibling recovers: `SocketTransport._reset`, boto3, clickhouse-connect's pool, pymongo's
pool.

#### Acceptance Criteria:

- [ ] AC-1: After a connection failure on an **owned** connection, the sink reconnects on the next
      attempt and delivery resumes.
- [ ] AC-2: An **injected** connection is never reconnected — it is the caller's object, per
      arch §13's borrowed-client constraint — and the sink says so rather than failing silently.
- [ ] AC-3: Reconnection is bounded by the existing retry budget, not a new unbounded loop.
- [ ] AC-4: Verified against a real Postgres, since a fake connection cannot show that psycopg's
      handle is unusable. Closed only against a **green** run of FR-001's job, not merely against
      its existence.

### FR-003: `LogstashSink` sends a content type Logstash parses

#### Description:

Carried from SPEC-038 FR-010. **Read-only analysis, and the lowest-confidence finding in the
audit** — `sinks/logstash.py:76-79`.

HTTP mode sends `application/x-ndjson`. Logstash's `http` input maps only `application/json` to a
JSON codec by default, falling back to `plain` — so the whole body would arrive as a single event
with every line stuffed into `message`. It works only if the user has set `codec => json_lines`.

#### Acceptance Criteria:

- [ ] AC-1: Verified against a real Logstash **before changing anything**. This is the one finding
      where the fix could be worse than the defect if the analysis is wrong, which is also why it
      could never have been closed in SPEC-038. Closed against a green run of FR-001's job.
- [ ] AC-2: Whichever way it resolves, the class docstring states the Logstash-side configuration
      the sink expects.
- [ ] AC-3: If the analysis is wrong, that is recorded — in this spec and in the audit's own row —
      rather than the FR quietly disappearing. A finding that was investigated and found not to be
      one is a result, and the next audit must not re-find it as new.

### FR-004: Three queue sinks have no bounded retry, and the docs said they did

#### Description:

Carried from SPEC-038 FR-014. `KafkaSink`, `NATSSink` and `GooglePubSubSink` neither import
`sinks/_retry` nor accept `max_retries`. The README's "All publish + retry within a bound" was
false for three of the seven queue/stream sinks, and the README PR narrows the prose — which fixes
the *claim* and leaves the *gap*, so three sinks would ship at 1.0 outside SPEC-027's guarantee.

That may be the right answer: each of the three has a client with its own retry and its own
delivery timeout, and layering a second retry on top of `librdkafka`'s can multiply the worst-case
delay rather than bound it. But it has to be a decision, not a documentation edit — and it is a
decision about how each *client* behaves, which is a question only a real service can answer.

#### Acceptance Criteria:

- [ ] AC-1: For each of the three, this FR states whether the client's own retry satisfies
      SPEC-027's guarantee — bounded, **and** cut short by a shutdown — with the setting that
      makes it so. Closed against a green run of FR-001's job.
- [ ] AC-2: Where it does, the class docstring states the worst-case total delay as every other
      retrying sink does, and the stop signal is honoured — which is what "cut short by a
      shutdown" requires, and what SPEC-034 FR-006 makes a documented contract member.
- [ ] AC-3: Where it does not, bounded retry is added through `sinks/_retry`, not a second
      mechanism.
- [ ] AC-4: `README.md`'s claim matches the outcome, whichever way each resolves.

---

## Implementation Phases

### Phase 1: The job (FR-001)

Everything else here is closed against a green run of it.

### Phase 2: FR-002, FR-003, FR-004

Independent of each other once the job exists. FR-003 may end in "no change, recorded", and that
is a result rather than a failure to deliver.
