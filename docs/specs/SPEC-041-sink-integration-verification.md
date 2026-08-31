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
  through their clients, or need bounded retry adding — and, for `NATSSink`, the loss-visibility
  defect that answering the question exposed (FR-004 AC-5).
- Tests that pass only because the extras are absent. Running the existing suite with the extras
  installed is part of what FR-001 buys, and it was already red before the job landed.
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

**When the gate decision is revisited, and by whom.** AC-2 requires this FR to say, and it did
not. The job earns the merge gate by being green on its own: after **four consecutive green
weekly scheduled runs** on `main`, Andrew decides whether to add it to the `main` ruleset. Four
is chosen against what the job is exposed to rather than as a round number — nine service
containers, an image-pull path and a broker that has to elect itself a controller are all
flakiness sources a single run cannot characterise. It is Andrew's call because it is a
branch-protection change, which is a repository setting no agent can make.

#### Acceptance Criteria:

- [ ] AC-1: A CI job, **separate from the gating no-extras one**, installs the extras and runs the
      sink suite against real services in containers — at minimum Postgres, Redis, Kafka and a
      Logstash, which are the four with findings here that a fake cannot settle.
- [ ] AC-2: It is **not** required for a merge initially. A new, flaky-by-nature integration job
      that blocks every PR is a job that gets disabled; it earns the gate by being green for a
      while first, and this FR states when that decision gets revisited and who makes it.
- [ ] AC-3: The no-extras environment stays exactly as it is (`CLAUDE.md`).
- [ ] AC-4: Which sinks remain unverified after this job exists is written down, so the next audit
      knows what it is still reading rather than running. It is written down as a **derived
      roster** (`tests/test_sink_integration_roster.py`), not as prose: a hand-listed set is one
      this repo has twice found rots, and the roster fails when a sink joins the population with
      no answer.
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
      one is a result, and the next audit must not re-find it as new. **Resolved the other way:
      the analysis was confirmed by measurement**, so the recording obligation is to strike
      "lower confidence" from the audit's K10 row rather than to record a miss. Against a stock
      `http` input the sink's current body arrives as **one** event with the whole batch in
      `message`; a JSON array arrives as **three**.

### FR-004: Three queue sinks have no bounded retry, and the docs said they did

#### Description:

Carried from SPEC-038 FR-014. `KafkaSink`, `NATSSink` and `GooglePubSubSink` ~~neither import
`sinks/_retry` nor accept `max_retries`~~.

> **Superseded by measurement (SPEC-041).** True of `KafkaSink` and `NATSSink`; **false of
> `GooglePubSubSink`** by the time this FR was built. SPEC-038 FR-004 gave that sink `_retry`'s
> `wait`, a `log_foundry_stop_signal` and a deadline-bounded overflow loop — after SPEC-038
> FR-014 was written and before this spec was scheduled. So this is a two-sink question plus a
> confirmation. Struck in place rather than corrected silently, per SPEC-021's rule: a reader who
> greps the old sentence must see the reversal. The README's "All publish + retry within a bound" was
false for three of the seven queue/stream sinks, and the README PR narrows the prose — which fixes
the *claim* and leaves the *gap*, so three sinks would ship at 1.0 outside SPEC-027's guarantee.

That may be the right answer: each of the three has a client with its own retry and its own
delivery timeout, and layering a second retry on top of `librdkafka`'s can multiply the worst-case
delay rather than bound it. But it has to be a decision, not a documentation edit — and it is a
decision about how each *client* behaves, which is a question only a real service can answer.

**The widening, and why it is here rather than in a later spec.** Answering AC-1 for `NATSSink`
required running it against a real server, and the answer — "bounded, because it never waits" —
is true for a reason that is itself a defect. Core `publish()` writes into the client's outbound
buffer and returns, so with the server stopped five successive `emit`s each returned in 0.00 s
with `losses()` reading `dropped=0, failed=0`, 96 bytes sat in the client's pending buffer, and
when the reconnect budget (60 attempts × 2 s) expired first, **one of six events reached the
destination with every counter still at zero**. That is SPEC-026 FR-001's shape — a sink that
delivered nothing and reported success is a sink the worker believes, so its retry never engages
and `failed_batches` never moves. Fixed here rather than deferred because the measurement that
found it is this FR's own, and because `nats-py` exposes `is_connected`, making the fix an
application of an already-settled rule rather than a new one (AC-5).

#### Acceptance Criteria:

- [ ] AC-1: For each of the three, this FR states whether the client's own retry satisfies
      SPEC-027's guarantee — bounded, **and** cut short by a shutdown — with the setting that
      makes it so. Closed against a green run of FR-001's job.
- [ ] AC-2: Where it does, the class docstring states the worst-case total delay as every other
      retrying sink does, and the stop signal is honoured — which is what "cut short by a
      shutdown" requires, and what SPEC-034 FR-006 makes a documented contract member.
      **"Cut short by a shutdown" governs a wait *between attempts*, never work in progress.**
      A sink with no inter-attempt wait satisfies it vacuously and correctly, and must not have
      one added: `KafkaSink._flush_bounded` states in the imperative that the stop signal is
      deliberately not consulted, because `produce()` is a local hand-off and `flush()` is the
      only thing that drains the producer — SPEC-038 FR-006 AC-3 measured the alternative at
      **zero of eleven** delivered. `KafkaSink` has no `log_foundry_stop_signal` attribute, and
      since `_lifecycle.offer_stop_signal` probes by `hasattr`, that absence *is* the opt-out.
      Read without this clause the criterion reinstates a defect a previous spec removed.
- [ ] AC-3: Where it does not, bounded retry is added through `sinks/_retry`, not a second
      mechanism.
- [ ] AC-4: `README.md`'s claim matches the outcome, whichever way each resolves.
- [ ] AC-5: `NATSSink.emit` raises `SinkDeliveryError` when the client is disconnected, so the
      worker's retry and `health().failed_batches` engage instead of the batch being reported
      delivered. It moves no `losses()` counter — a refusal is a failure *reported* to the worker,
      not one absorbed, and SPEC-032 settled that counting both reports one loss twice. The
      docstring states the limit rather than overclaiming: the guard catches the sustained outage,
      not the first batch in the window before `is_connected` flips. Verified across a real outage.
- [ ] AC-6: The four `SentrySink` tests that pass only because CI never installs the `sentry`
      extra select their backend explicitly instead. They are green in **both** environments, and
      the extras job runs the whole existing suite, not only the integration modules.

---

## Implementation Phases

### Phase 1: The job (FR-001)

Everything else here is closed against a green run of it.

### Phase 2: FR-002, FR-003, FR-004

Independent of each other once the job exists. FR-003 may end in "no change, recorded", and that
is a result rather than a failure to deliver.
