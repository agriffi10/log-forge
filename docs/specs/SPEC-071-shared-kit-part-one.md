---
type: Spec
id: SPEC-071
title: "Shared Kit, Part One: import_extra, One Injected-Client Refusal and the Chunker Rule"
description: "Moves the sinks' lazy extra imports, their injected-client refusal and SQS's chunk boundary into shared helpers, refuses the build options eight sinks silently ignore beside an injected client, and pins first the rosters those moves can reach."
spec_status: Draft
depends_on: [SPEC-041, SPEC-043, SPEC-049, SPEC-059]
arc: stability-redesign
arc_order: 3
---

## Overview

The sinks repeat three small pieces of code. There are sixteen lazy imports of an optional
client, and fourteen of them raise a bare `ModuleNotFoundError: No module named 'boto3'` that never
says which extra to install. Five constructors refuse an option the caller passed beside an injected
client, each with its own copy of that check. And `SQSSink` re-implements the boundary arithmetic of
`chunk_items`. This spec moves each of the three into one helper, so a new site cannot be written
locally. It is part one of the arc's shared kit; the retry engine, the loss counters and the
closed guard come in later specs.

**The refusal is also widened, because the same rule is broken silently elsewhere.** SPEC-043's
rule, as the decision
`docs/decisions/sink-delivery/a-bad-argument-is-floored-if-it-works-and-refused-if-it-is-already-broken-a-new-one-is-refused.md`
records it, is that an argument the selected backend cannot use raises at construction. The five
copies apply it to the bounds they name. The options a sink *builds its client from* are ignored
silently beside an injected one: `RedisListSink(client=c, url=...)` and
`AzureEventHubsSink(producer=p, connection_str=...)` construct and never read the argument (both
probed at `fda134a`), and `PostgresSink(connection=c, dsn=...)` does the same by code
(`postgres.py`, `self._dsn` is read only by `_connect`, which an injected connection never reaches).
That breaks invariant 13's observable, "no argument is silently ignored because another was given".
FR-003 refuses every such option, found by a derived sweep rather than by its message.

**And one defect is fixed in passing.** `AzureEventHubsSink(producer=p)` with `azure-eventhub` not
installed constructs, and then raises `ModuleNotFoundError` out of every `emit`, because it resolves
the `EventData` class per batch (probe-confirmed at `fda134a`). That too is invariant 13's
observable broken. FR-002 resolves the class through `import_extra` at construction, so the
configuration is refused where it is written.

**What changes for a caller:** a missing extra's message names the extra; the five existing
refusals share one message tail; eight options that were ignored beside an injected client are now
refused; and an injected-producer `AzureEventHubsSink` with no extra installed is refused at the
constructor. Each is a release-notes line. Nothing else changes in what a sink delivers, counts or
retries. Tests that already exist and are **not edited**, plus characterisation tests written first
against the unmodified code, are what show that.

This spec builds on SPEC-066, which lands first in 1.1.0 and is not in `depends_on` because it is not
yet in `docs/specs/`. SPEC-066 adds `exact_int` and `floor_count` to `sinks/_retry.py` and routes
every `max_retries` floor through them. **That is why the plan's "`max_retries` floor" row is absent
here:** its validator already exists by the time this spec builds. This spec adds no function to
`sinks/_retry.py` and edits no line of it. `_BACKOFF_BASE` and the retry loop belong to SPEC-072.

**Built after SPEC-069 and SPEC-070, so every pinned set is derived on the commit the build starts
from**, and written out by name there; the figures below are anchored to `fda134a`, the tree this
spec was drafted on, and say what the sibling specs are expected to add. SPEC-069 adds a soft `bson`
import to `MongoDBSink` (a seventeenth site) and an encode step to `SQSSink._chunks`. SPEC-070 adds a
roster of the twelve retry loops with a floor of twelve. SPEC-061 (NATS liveness, landing in 1.1.0)
moves `import nats` inside a `try` whose `except BaseException:` stops the sink's loop thread and
re-raises, so that site still propagates.

**What was measured at `fda134a`.**

- **Sixteen lazy imports of an extra, fourteen bare.** An AST scan finds sixteen sites in fourteen
  modules. The scan takes every `Import` and `ImportFrom` node in `src/log_foundry/**/*.py` outside
  an `if TYPE_CHECKING:` block whose top-level module is not in `sys.stdlib_module_names`, not
  `log_foundry` and not `__future__`. Fourteen are bare (the plan counted fifteen). Two sit inside a
  `try` whose handler catches `ImportError` and falls back: `RabbitMQSink._persistent_properties`
  and `sentry._import_sdk`. The import name in the table is the module `import_extra` is given:

  | Module | Function | `import_extra` module | Extra |
  |---|---|---|---|
  | `clickhouse.py` | `__init__` | `clickhouse_connect` | `clickhouse` |
  | `eventhubs.py` | `__init__`, `_event_data_cls` | `azure.eventhub` | `azure-eventhubs` |
  | `firehose.py`, `kinesis.py`, `sns.py`, `sqs.py` | `__init__` | `boto3` | `aws` |
  | `kafka.py` | `__init__` | `confluent_kafka` | `kafka` |
  | `mongodb.py` | `__init__` | `pymongo` | `mongo` |
  | `nats.py` | `__init__` | `nats` | `nats` |
  | `postgres.py` | `_connect` | `psycopg` | `postgres` |
  | `pubsub.py` | `__init__` | `google.cloud.pubsub_v1` (today `from google.cloud import pubsub_v1`) | `gcp-pubsub` |
  | `rabbitmq.py` | `_connect`, `_persistent_properties` (soft) | `pika` | `amqp` |
  | `redis.py` | `__init__` | `redis` | `redis` |
  | `sentry.py` | `_import_sdk` (soft) | `sentry_sdk` | `sentry` |

- **Injected-client constructors and their build options.** An AST scan of every `__init__` in
  `src/log_foundry/sinks/*.py` for a parameter named `client`, `producer` or `connection` finds
  sixteen public classes, the fifteen below and `SentrySink`. Reading each one's build path (the
  branch taken when the injected parameter is `None`, and the `_connect` it calls), the options the
  client is built from are:

  | Sink | Injected | Refused beside it today | Ignored beside it today |
  |---|---|---|---|
  | `ClickHouseSink` | `client` | `send_receive_timeout` | `dsn` |
  | `AzureEventHubsSink` | `producer` | none | `connection_str`, `eventhub` |
  | `KafkaSink` | `producer` | `producer_config` | `bootstrap_servers` |
  | `MongoDBSink` | `client` | `socket_timeout`, `server_selection_timeout` | `uri` |
  | `NATSSink` | `client` | the four connect options, `servers` | none |
  | `PostgresSink` | `connection` | none | `dsn`; `connect_timeout` (default `5`) |
  | `RabbitMQSink` | `connection` | the three bounds | `url` |
  | `RedisStreamsSink`, `RedisListSink` | `client` | none | `url` |
  | `SQLiteSink` | `connection` | none | `database` (required, positional) |
  | `SQSSink`, `SNSSink`, `KinesisSink`, `FirehoseSink`, `GooglePubSubSink` | `client` | none | none: the client is built from no option |

  `SentrySink` takes an SDK `client=` that replaces the imported module rather than being built from
  options, and its backend refusals are SPEC-043's own; it is outside this table. `LogstashSink`'s
  two "cannot apply" refusals refuse an option aimed at the *other backend*, a different rule.
- **Three local chunkers, not four.** `grep -n "chunk_items\|chunk_list" src/log_foundry/sinks/*.py`
  shows `ClickHouseSink` and `PostgresSink` on `chunk_list`, `KinesisSink`, `FirehoseSink` and
  `SNSSink` on `chunk_items`, and `SQSSink` on neither. A scan for a hand-rolled boundary (a slice by
  index, a running byte total, the SDK's own `create_batch`) finds three local chunkers:
  - `SQSSink._chunks`, whose inline loop is `chunk_items`' rule line for line.
  - `HTTPSink._take`, which its docstring says exists because the budget changes between chunks
    after a `413`.
  - `AzureEventHubsSink`'s `create_batch`/`add` loop, where the SDK knows when a batch is full.
- **The rosters.** Twenty-four test modules import `ast` (`grep -lE "^\s*import ast" tests/*.py`).
  The two the plan names already carry pins: `DROPPED_FLOOR` in `tests/test_deferral.py` equals the
  derived counter set (five names), and `_SITE_FLOOR` in `tests/test_worker_predicate_roster.py` is
  keyed by module, with its `_lifecycle` entry one below the derived count (43 against 44).
  The roster this spec's moves actually reach is `tests/test_sink_integration_roster.py`.
  - It derives part of its population from the *text* `import-not-found` in a sink module (fourteen
    modules).
  - `import_extra` deletes that text, so the roster would fall from 24 modules to 11. Its `>= 23`
    floor would then fail, and the tempting repair is to lower it.
- **Text pins.** Tests that assert source text read no AST, so a scan of the AST-derived rosters
  cannot find them. `grep -rnE "import-not-found|already built|already connected|from (pymongo|confluent_kafka|azure\.eventhub|google\.cloud) import" tests/*.py`
  finds two that the lines this spec edits would break.
  - `tests/test_sink_integration_roster.py:67`, the marker above.
  - `tests/test_otel_envelope.py:517-527` (`test_the_mypy_accommodation_is_in_place`). It requires
    the exact line `from google.cloud import pubsub_v1  # type: ignore[import-not-found,
    unused-ignore]` and the `google.cloud.*` mypy override in `pyproject.toml`.

## Scope

### In Scope

- Pinning the rosters and text pins the moves reach, re-derived on the build's base commit (FR-001).
- `import_extra`, its private error type, every lazy-import site, the Azure constructor refusal, a
  derived guard, and the docs and config that describe the comments this removes (FR-002).
- One refusal for every build option passed beside an injected client, at every constructor a
  derived sweep finds, with a guard that catches a sink that *ignores* the option (FR-003).
- `SQSSink._chunks` on `chunk_items` (FR-004).
- The v1.2.0 release-notes lines, and running the Integration workflow, which the build's edits to
  `src/log_foundry/sinks/**` trigger.

### Out of Scope

- **The `max_retries` floor, `_BACKOFF_BASE`, the retry loop and the `SinkLosses` builds.** The first
  is SPEC-066's; the rest are the engine's (SPEC-072 onward).
- **`HTTPSink._take` and `AzureEventHubsSink`'s batching.** Neither has limits that fit
  `chunk_items`' fixed count and byte bound (Overview), so converting either would change
  behaviour.
- **Build options with a non-`None` default or no default**: `PostgresSink.connect_timeout` (default
  `5`) and `SQLiteSink.database` (required and positional). Neither can be told apart from an
  omitted argument without changing a frozen signature or default (invariant 10). Every existing
  injected-connection `SQLiteSink` caller must pass a `database`, so refusing it would break them
  all. FR-003's guard pins these two by name, so a third is noticed.
- **`LogstashSink`'s two backend refusals**, a different rule (Overview).
- **Making the error type public.** It is private (FR-002). Promoting it later only adds to the
  surface, while the reverse would be a break.
- **Which exceptions a sink retries** (SPEC-070) and **how it encodes an event** (SPEC-069). FR-004
  changes only the boundary arithmetic of `_chunks`, as it stands on the build's base commit.
- **Peer branches.** SPEC-061 rewrites the `import nats` site and SPEC-066 moves the constructors'
  floors above their builds; both land first in 1.1.0, and the build rebases onto them, keeping
  both edits.

---

## Functional Requirements

### FR-001: The rosters and text pins the moves can reach are pinned before any code moves

#### Description:

Phase 1 changes no file under `src/`. It adds pins that are green on the unmodified tree, and the
later phases must keep them green without editing them. That way a refactor that moves a site out
of a roster's reach reddens a test instead of shrinking the roster. Every set is derived on the commit the
build branches from and then written out by name, because SPEC-069 and SPEC-070 land between
`fda134a` and that commit.

- **The integration roster's lazy-import population** is pinned by name: at `fda134a` it is
  `clickhouse`, `eventhubs`, `firehose`, `kafka`, `kinesis`, `mongodb`, `nats`, `postgres`,
  `pubsub`, `rabbitmq`, `redis`, `sentry`, `sns` and `sqs`. The pin compares `_population()`'s
  `marker` set exactly. FR-002 changes what `marker` reads without touching the pin, and that is what
  makes the move checkable.
- **`_SITE_FLOOR`** is raised to the derived per-module count on the build's base commit (44 and 2
  at `fda134a`). The runtime specs later in the arc move that population, and this way they start
  from a floor with no slack. It is a floor, so an added site needs no edit; only a removed one does.
- **The text-pin sweep.** The build re-runs the Overview's text grep on its base commit, extended
  with every source line Phases 2 to 4 rewrite. Each hit is either kept true or named in the PR body
  as edited, with the reason. At `fda134a` the hits are the integration marker (FR-002 re-aims it)
  and `test_the_mypy_accommodation_is_in_place` (FR-002 deletes it).

The retry-loop roster needs no pin from this spec, because SPEC-070's roster floors it at twelve.
`DROPPED_FLOOR` needs none either, because it already equals the derived set.

#### Acceptance Criteria:

- [ ] A test compares `_population()["marker"]` with the modules named on the build's base commit
      as an exact set. It is green on the unmodified tree, and it fails naming the module when one
      is removed from, or one added to, the population fed to it.
- [ ] `_SITE_FLOOR` equals `_per_module_sites()` for `log_foundry._lifecycle` and
      `log_foundry.decorator` on the build's base commit. The existing floor test still fails when
      `decorator` is dropped from `_WALKED`.
- [ ] Each pin was shown to redden once against a planted mutant and was then restored, and the PR
      body says so. The two mutants are a module's lazy import removed in a scratch copy of the
      population, and `decorator` dropped from `_WALKED`.
- [ ] The PR body lists the text-pin sweep's command, the commit it ran on, and each hit with its
      disposition.
- [ ] Phase 1 touches no file under `src/`, and the whole suite is green at the end of it.
- [ ] Serves invariants 2, 6 and 7. The rosters guard the ledger's drop sites
      (`tests/test_deferral.py`), the twin-path guard sites (`tests/test_worker_predicate_roster.py`)
      and the sinks verified against a real destination (`tests/test_sink_integration_roster.py`).

### FR-002: One `import_extra` names the extra, refuses a missing one at the constructor, and a guard keeps new sites out

#### Description:

`sinks/_extras.py` gains `import_extra(module, extra)`. It imports `module` by name with
`importlib.import_module` and returns it. When the import fails with a `ModuleNotFoundError` whose
`name` is that module or one of its parents (for example `google.cloud` for
`google.cloud.pubsub_v1`), it raises `MissingExtraError`, a subclass of `ModuleNotFoundError`.
`MissingExtraError` behaves like this:

- **Its message** names the extra and the command, and keeps the interpreter's own text, for example
  `The "aws" extra is not installed (No module named 'boto3'): pip install "log-foundry[aws]"`.
- **Its other attributes** come from the original: the original is its `__cause__`, and its `.name`
  and `.path` are the original's.

Anything else propagates **unchanged**. That covers a `ModuleNotFoundError` for another module (the
client's own dependency is missing, which the extra would not fix), an `ImportError` that is not a
`ModuleNotFoundError` (a broken binary), and a `ModuleNotFoundError` with no `name`.

**`from X import Y` becomes an attribute read.** `import_extra("pymongo", "mongo").MongoClient`
reads the class off the module. A module that is present but lacks the name now raises
`AttributeError` where `from ... import` raised `ImportError: cannot import name`. Only a stub or an
incompatible version reaches that, no shipped test does, and the spec accepts it. Pub/Sub's module
is a submodule, so it is passed whole (`google.cloud.pubsub_v1`) and is never an attribute read.

**The type is private.** `MissingExtraError` lives in a private module, is in no `__all__`, and is
not re-exported. A caller catches `ModuleNotFoundError` or `ImportError`, as every constructor's
`Raises:` already says, and reads `.name`, exactly as today. A public name would be frozen for all
of 1.x (invariant 10), and a private one can still be promoted later by an addition.

**Every site goes through it.** That is the sixteen above plus any the build's base commit adds
(SPEC-069's `bson`). The bare ones read `import_extra("boto3", "aws").client(...)`. The soft ones
wrap the call in `except ImportError` and keep their fallbacks. `_event_data_cls`, `_import_sdk`,
`RabbitMQSink._connect` and `RabbitMQSink._persistent_properties` keep their names and signatures,
because existing tests patch them.

**Azure is refused at construction.** `AzureEventHubsSink.__init__` calls `_event_data_cls()` once,
also when a producer is injected, so a missing extra raises there. `emit` keeps resolving it per
batch, which keeps the patch seam the tests use working.
`tests/test_sink_retry.py:402` (`_one_of_each_retrying_sink`) and
`tests/test_sink_concurrency.py:1554` build the sink with a fake producer and no patch, so both are
edited to patch `_event_data_cls` first, and so is any other test the suite shows doing the same.

**What the removed comments leave behind.** The `# type: ignore[import-not-found]` comments go,
since `import_module` returns a module whose attributes are untyped. No import statement for
`google.cloud` is then left under `src/`, and `mypy` checks only `src/`, so the `google.cloud.*`
override in `pyproject.toml` and its comment are removed as dead. With them goes
`tests/test_otel_envelope.py::test_the_mypy_accommodation_is_in_place`, which pins exactly that
mechanism. Two docs describe the comments and are rewritten:
- `CLAUDE.md`'s *After installing extras locally* paragraph. The build runs `mypy` in an environment
  with every extra installed, and the paragraph states that result, anchored to the commit.
- `.github/workflows/integration.yml`'s header comment.

**The guard.** `tests/test_extras_roster.py` derives the sites from the AST of
`src/log_foundry/**/*.py`. It checks four things:
- It reports any import of a module outside the standard library and the package, outside
  `if TYPE_CHECKING:`, naming `file:line`.
- It compares the derived `(module file, function, imported module, extra)` tuples with the sites
  the Phase 1 table enumerates on the build's base commit, exactly.
- It requires every extra to be a key of `[project.optional-dependencies]`.
- It requires both arguments to be string literals.

The guard has its own fixture corpus, and a derived mutation sweep rewrites each call into the bare
import in memory and requires the scan to report that site.
`tests/test_sink_integration_roster.py`'s `marker` derivation is re-aimed from the text
`import-not-found` to a call to `import_extra`. Its floors (`>= 23`, `>= 10`), `VERIFIED` and
`UNVERIFIED` do not change.

**Phase 1's characterisation.** `tests/test_extras_sites.py` enumerates the sites on the base
commit as a table. For each one it makes the module absent and asserts only what holds today: an
`ImportError` whose `.name` is the module, or the two soft fallbacks. Absence is made by a
`sys.meta_path` finder, installed first in the list, whose `find_spec` raises
`ModuleNotFoundError(f"No module named {name!r}", name=name)` for the module and its submodules,
with them removed from `sys.modules`. It is not made with `sys.modules[name] = None`, whose message
is `import of boto3 halted; None in sys.modules`.

#### Acceptance Criteria:

- [ ] With a fake module in `sys.modules`, `import_extra` returns that object.
- [ ] With the module made absent by the raising finder, `import_extra` raises `MissingExtraError`.
      It is an instance of `ModuleNotFoundError` and `ImportError`, its message contains
      `pip install "log-foundry[aws]"` and `No module named 'boto3'`, its `.name` is `boto3`, and its
      `__cause__` is the finder's exception.
- [ ] A `ModuleNotFoundError` for a different module, one with no `name`, and an `ImportError` that
      is not a `ModuleNotFoundError`, each raised by the finder, propagate as the same object, and
      no `MissingExtraError` is made.
- [ ] `tests/test_extras_sites.py` is committed in Phase 1, green against the unmodified code.
      After Phase 2 each bare site raises `MissingExtraError` naming its table's extra, and each soft
      site still falls back without raising: `_persistent_properties` returns its stand-in,
      `_import_sdk()` returns `None`, and the `bson` site, as SPEC-069 left it, does whatever it did
      without the module.
- [ ] `AzureEventHubsSink(producer=FakeProducer())` with `azure.eventhub` absent raises
      `MissingExtraError` naming `azure-eventhubs` at construction, and constructs when
      `_event_data_cls` is patched.
- [ ] With an injected client, producer or connection, a sink imports nothing it did not import on
      the base commit, except `AzureEventHubsSink`'s `EventData`. `"boto3" not in sys.modules` after
      `SQSSink(client=...)`, which `tests/test_sinks_sqs.py` already asserts.
- [ ] No `# type: ignore[import-not-found]` remains under `src/`, and the `google.cloud.*` override
      is gone from `pyproject.toml`. `poetry run mypy` is clean, and `CLAUDE.md`'s extras paragraph
      states the result of `mypy` run with every extra installed, naming the commit.
- [ ] `tests/test_extras_roster.py` reports `file:line` for three shapes: a bare function-level
      import, a `try: import x / except ImportError` pair, and a module-level `from pymongo import X`.
- [ ] It stays silent on `import threading` inside a function, an import of `log_foundry`, an import
      under `TYPE_CHECKING`, and a call to `import_extra`.
- [ ] It reports an extra that is not a `[project.optional-dependencies]` key and a non-literal
      argument, and it reports every site its mutation sweep plants.
- [ ] `_population()["marker"]` still equals FR-001's pinned set, and the integration roster's
      floors, `VERIFIED` and `UNVERIFIED` are byte-identical.
- [ ] The only existing tests edited are the integration roster's derivation, the deleted
      `test_the_mypy_accommodation_is_in_place`, and the Azure constructions named above.
- [ ] `MissingExtraError` is in no `__all__`, and `tests/test_public_surface.py` and
      `tests/typed_consumer/` pass unedited.
- [ ] Serves invariants 10, 11 and 13. No public name is added; the message names an extra and a
      module, never a caller value; and a missing extra is refused at the constructor that needs it,
      `AzureEventHubsSink` with an injected producer included, at every site.

### FR-003: Every option a sink builds its client from is refused beside an injected client

#### Description:

`sinks/_args.py` gains `refuse_with_injected(owner, kind, injected, supplied)`. When `injected` is
`None` it returns. Otherwise it collects the names in `supplied` whose value is not `None`. If there
are any, it raises `ValueError` whose message begins `<owner> cannot apply <names, sorted and
comma-joined> to an injected <kind>` and ends naming the keyword to drop (`drop client=`). The test
is `is not None`, never truthiness, as at all five sites today, so `KafkaSink(producer=p,
producer_config={})` is still refused.

Every constructor in the Overview's table calls it once, where its own check stands now (or, for the
three with none, before anything is built). `supplied` is every option the sink would build its
client from that has a `None` default:
- `ClickHouseSink`: `dsn` and `send_receive_timeout`.
- `AzureEventHubsSink`: `connection_str` and `eventhub`.
- `KafkaSink`: `bootstrap_servers` and `producer_config`.
- `MongoDBSink`: `uri` and its two bounds.
- `NATSSink`: its four options and `servers`.
- `PostgresSink`: `dsn`.
- `RabbitMQSink`: `url` and its three bounds.
- The Redis pair: `url`, refused in `_RedisSink.__init__`.

The five existing refusals keep the prefix the existing tests match, so those tests pass unedited.
Their tail is the one text that changes: today two say "already built", three "already connected",
and `KafkaSink` has no "drop" clause. The eight newly refused options are a behaviour change, and
the release note lists them. `NATSSink`'s `options` dict literal stays in its `__init__`, because
`tests/test_sinks_nats.py` derives the forwarded names from it.

**The guard catches a sink that ignores an option, not only a copy of the message.**
`tests/test_injected_client_roster.py` walks every `__init__` in `src/log_foundry/sinks/*.py` that
takes `client`, `producer` or `connection`. Its *build options* are the parameters that reach the
arguments of the call building that object. That call is found in the branch taken when the
parameter is `None`, or in a method that branch calls (`_connect`). The scan follows assignments to a
local or a `self` attribute, call arguments (`max(x, ...)`, `floor_count(x, ...)`), dict literals and
comprehensions over them. Defaults are read from each concrete class's `inspect.signature`, so
`_RedisSink`'s subclasses supply them. The guard checks three things:
- Every build option whose default is `None` appears as a key of that `__init__`'s
  `refuse_with_injected` `supplied` mapping, or it is reported as `<file>:<Class>.<param>`.
- Build options with any other default, or none, form a second derived set compared exactly with
  the two named in *Out of Scope*.
- The derived population of classes is compared exactly with the table's fifteen plus `SentrySink`
  (whose client is built from no option), re-derived on the build's base commit.

`_args.py` is a new module rather than a function in `_retry.py`, because that file is SPEC-066's and
is about bounding waits.

#### Acceptance Criteria:

- [ ] `refuse_with_injected("S", "client", None, {"a": 1})` returns, and so does a call with an
      injected object and every value `None`.
- [ ] With an injected object and `{"b": 0, "a": None, "c": {}}` it raises `ValueError` whose message
      begins `S cannot apply b, c to an injected client` and names `drop client=`.
- [ ] `RedisListSink(client=c, url="redis://x")`, `RedisStreamsSink(...)` likewise,
      `AzureEventHubsSink(producer=p, connection_str="x")`, `AzureEventHubsSink(producer=p,
      eventhub="h")`, `PostgresSink("t", connection=c, dsn="x")`, `ClickHouseSink("t", client=c,
      dsn="x")`, `KafkaSink("t", producer=p, bootstrap_servers="b")`, `MongoDBSink(client=c,
      uri="mongodb://x", database="d", collection="c")` and `RabbitMQSink(..., connection=c,
      url="amqp://x")` each raise `ValueError` naming the argument. At `fda134a` each constructed.
- [ ] Each of those constructs when the same argument is passed without the injected object (a
      fake module standing in for the driver).
- [ ] With a factory patched to count calls, every refused construction leaves the count at 0, and
      `NATSSink` builds no event loop.
- [ ] The five existing refusal tests (`test_sinks_clickhouse.py`, `test_sinks_kafka.py`,
      `test_sinks_mongodb.py`, `test_sinks_nats.py`, `test_sinks_rabbitmq.py`) pass unedited, and
      `_forwarded_connect_kwargs()` still returns four names.
- [ ] `tests/test_injected_client_roster.py` reports `<file>:<Class>.url` on two fixtures: one whose
      `if client is None: client = make(url)` refuses nothing, and one that stores `self._url = url`
      and reads it only in `_connect`.
- [ ] It is silent when `url` is in the `supplied` mapping, and for a parameter used outside the
      build (`MongoDBSink`'s `database`).
- [ ] Its mutation sweep drops each name from each `supplied` mapping in memory, and the scan
      reports it.
- [ ] Its second set is exactly `PostgresSink.connect_timeout` and `SQLiteSink.database`, and its
      class population is exactly the table's fifteen plus
      `SentrySink`.
- [ ] No `ValueError` raised in `sinks/*.py` outside `_args.py` carries `to an injected`.
      `LogstashSink`'s two backend refusals are unchanged, and `git diff` shows no change to
      `sinks/_retry.py`.
- [ ] Serves invariant 13, whose observable "no argument is silently ignored because another was
      given" this closes at every constructor the sweep finds, and invariant 10. No signature or
      default changes, which is why the two options in the second set stay accepted.

### FR-004: `SQSSink` uses `chunk_items`, and the two other local chunkers stay

#### Description:

`SQSSink._chunks` serialises each event, resolves its FIFO ids, and drops an oversize, id-failed or
(after SPEC-069) unencodable event into the calling `emit`'s deferral, all exactly as on the build's
base commit. It then builds its chunks with `chunk_items(prepared, max_count=self.MAX_BATCH,
max_bytes=self.MAX_BYTES, size_of=...)` instead of its own accumulator.

The two are the same rule. The inline loop drops an oversize event *before* the boundary test, so
filtering first and chunking after yields the same groups. The costed size (the body plus both ids
on a FIFO queue) is carried on `_Prepared` as a new field, so `size_of` reads it and does not
recompute it.

`_chunks` keeps its name and its `(batch, pending)` signature, and it still returns a **list**.
Every per-event drop is decided and handed to `pending` before the first send, so an `emit` that
raises still counts none of them (invariant 2). `tests/test_deferral.py`'s `DEFERRING_SITES` names
`_chunks` and is not edited.

**Proving no behaviour change.** `tests/test_sinks_sqs_chunks.py` is a corpus of named boundary
cases, recorded in Phase 1 against the *unmodified* `_chunks` of the build's base commit, with the
expected chunk lengths written as literals. The phase that swaps the implementation does not edit
it. The cases:
- Exactly `MAX_BATCH` events, and one more.
- Costed sizes summing to exactly `MAX_BYTES`, and to one byte over.
- An oversize event between two that fit.
- A FIFO queue where the ids' bytes are what push a chunk over.
- A batch of only oversize events: no chunk and no raise.
- A batch whose every id callable raises: the first exception re-raised unchanged.
- An unencodable event, if SPEC-069 has landed.

The table is recorded from the old code and holds literals, not a copy of the old algorithm, which
keeps the check from mirroring what it checks. `HTTPSink._take` and `AzureEventHubsSink` keep their
own batching (*Out of Scope*).

#### Acceptance Criteria:

- [ ] Each corpus case asserts the literal lengths of the chunks `_chunks` returns: `[10, 1]` for
      eleven small events, `[2]` for two whose costed sizes sum to exactly `MAX_BYTES`, `[1, 1]` for
      one byte over, and so on. The corpus is committed in Phase 1 green against the unmodified
      code, and the commit that changes `_chunks` does not touch it.
- [ ] `tests/test_sinks_sqs.py`, `tests/test_sinks_sqs_fifo.py`, `tests/test_sinks_sqs_id_failures.py`
      and `tests/test_deferral.py` pass unedited.
- [ ] A FIFO batch whose last event is oversize and whose first send is made to raise counts
      nothing, because `_chunks` returned a list with every drop already in the deferral.
- [ ] The derived set of sink modules that call `chunk_items` or `chunk_list` is exactly
      `clickhouse`, `firehose`, `kinesis`, `postgres`, `sns` and `sqs`. Inlining one back, or
      converting `http.py` or `eventhubs.py`, is reported by name.
- [ ] `tests/test_sinks_http*.py`, `tests/test_sinks_eventhubs.py`, `tests/test_sinks_batch.py` and
      `tests/test_sink_retry.py` pass, unedited except for FR-002's one Azure construction.
- [ ] Serves invariants 2 and 7. A dropped event is counted once, on return, and an all-oversize
      batch still settles counted without raising.

---

## Data Model

```
// src/log_foundry/sinks/_extras.py
class MissingExtraError(ModuleNotFoundError): ...        // private; in no __all__
import_extra(module: str, extra: str) -> types.ModuleType
    // import_module(module); ModuleNotFoundError whose name is module or a parent of it
    //   -> raise MissingExtraError(
    //        f'The "{extra}" extra is not installed ({exc.msg}): pip install "log-foundry[{extra}]"',
    //        name=exc.name, path=exc.path) from exc
    // any other ImportError (another module, no name, not a ModuleNotFoundError) -> propagates

// src/log_foundry/sinks/_args.py
refuse_with_injected(owner: str, kind: str, injected: object | None,
                     supplied: Mapping[str, object | None]) -> None
    // injected is None, or every value is None -> returns
    // else -> ValueError(f"{owner} cannot apply {', '.join(sorted(names))} to an injected {kind}; ... drop {kind}=")

// src/log_foundry/sinks/sqs.py
class _Prepared(NamedTuple): body: str; group_id: str | None; dedup_id: str | None; size: int
```

## API / Interface Contract

```
SQSSink("https://q")            # extra absent: MissingExtraError: The "aws" extra is not installed
                                #   (No module named 'boto3'): pip install "log-foundry[aws]"
except ImportError: ...         # still matches; so does ModuleNotFoundError, and .name == "boto3"

RedisListSink("k", client=c, url="redis://x")
                                # ValueError: RedisListSink cannot apply url to an injected client; ... drop client=
AzureEventHubsSink(producer=p)  # azure-eventhub absent: MissingExtraError at construction
```

## File & Folder Structure

```
src/log_foundry/sinks/_extras.py        # import_extra, MissingExtraError           (new)
src/log_foundry/sinks/_args.py          # refuse_with_injected                      (new)
src/log_foundry/sinks/{clickhouse,eventhubs,firehose,kafka,kinesis,mongodb,nats,postgres,
                       pubsub,rabbitmq,redis,sentry,sns,sqs}.py   # the lazy-import sites
src/log_foundry/sinks/{clickhouse,eventhubs,kafka,mongodb,nats,postgres,rabbitmq,redis}.py
                                                                  # the refusals
src/log_foundry/sinks/sqs.py            # _chunks on chunk_items, _Prepared.size
pyproject.toml                          # google.cloud.* override removed
CLAUDE.md, .github/workflows/integration.yml   # the extras paragraph and header comment
tests/test_extras_sites.py              # Phase 1 characterisation of the sites     (new)
tests/test_extras_roster.py             # FR-002 guard and its corpus               (new)
tests/test_injected_client_roster.py    # FR-003 guard and its corpus               (new)
tests/test_sinks_args.py                # FR-003 helper table                       (new)
tests/test_sinks_sqs_chunks.py          # FR-004 boundary corpus                    (new)
tests/test_sink_integration_roster.py   # FR-001 pin; FR-002 re-aims the derivation (edited)
tests/test_worker_predicate_roster.py   # _SITE_FLOOR raised                        (edited)
tests/test_otel_envelope.py             # the mypy-accommodation test deleted       (edited)
tests/test_sink_retry.py, tests/test_sink_concurrency.py   # patch _event_data_cls  (edited)
docs/release-notes/v1.2.0.md
```

## Implementation Phases

### Phase 1: Pin and characterise, with no change under `src/`

- Derive every pinned set on the base commit and write it out by name (FR-001).
- The integration-roster pin, the `_SITE_FLOOR` raise, and the text-pin sweep.
- `tests/test_extras_sites.py` and `tests/test_sinks_sqs_chunks.py`, recorded from the unmodified
  code.
- Mutation-test each pin once, restoring from the scratchpad, and say so in the PR body.

### Phase 2: `import_extra`

- `_extras.py`, its tests, every lazy-import site, the Azure constructor resolution and the two
  test patches, the guard and its corpus, and the re-aimed integration derivation.
- The `pyproject.toml` override, the deleted test, and the `CLAUDE.md` and workflow prose.

### Phase 3: The injected-client refusal

- `_args.py`, its table test, the eight constructors, and `tests/test_injected_client_roster.py`.

### Phase 4: The SQS chunker, and the landing

- `_Prepared.size`, `_chunks` on `chunk_items`, and the derived module-set check.
- The release-notes entries, and the component-inventory rows for `_extras` and `_args`.
- The edits to `src/log_foundry/sinks/**` trigger the Integration workflow on the PR. Its run is owed
  before the merge and is named in the PR body as owed, since it cannot settle before the push.

## Revision history

- **Drafted at `fda134a`.** The plan's counts were re-measured: fourteen bare imports, not fifteen,
  and three local chunkers, not four. Its instruction to pin `test_deferral.py` and
  `test_worker_predicate_roster.py` found both already pinned. The roster the moves reach, the
  integration roster's text marker, was not among the ones it named.
- **Revised after the spec review.** A text-matching pin (`test_the_mypy_accommodation_is_in_place`)
  that the AST sweep could not see is now in FR-001, and its knock-ons are in FR-002. The
  absence-of-a-module mechanism changed, because `sys.modules[name] = None` gives a different message.
  Every pinned set is re-derived on the build's base commit, after SPEC-069 and SPEC-070. The retry-loop pin was
  dropped, because SPEC-070 floors that roster. The Azure emit-time import defect and the silently
  ignored build options were folded in rather than deferred. FR-003 and FR-004 no longer cite
  invariant 6, because five constructors or four chunkers are not twin paths in its sense.
