> **Frame report: sinks.** The body below this note is the frame's handback as the orchestrating
> session recorded it, and this note is the only text added. The audit it belongs to is
> [`2026-11-14-post-1.2.0-audit.md`](../2026-11-14-post-1.2.0-audit.md), which corrects the audit
> where it and this report disagree and never edits the report.
>
> **Verbatim except for one path redaction.** The report named three absolute paths on the
> auditing machine, and this is a public repository, so each long prefix is replaced by a
> placeholder and nothing else is changed: `<repo>` for the audited worktree, `<scratch>` for the
> scratch directory that held the probes, and `<venv>/bin/python` for the interpreter.
>
> **Reading its paths.** The audited tree, written `<wt-audit>` or "the audit tree", is this
> repository at `a0f44b5f` (tag `v1.2.0`). A bare `file.py:N` is a line of
> `src/log_foundry/sinks/file.py` at that commit unless a longer path is given, and `README.md` and
> `tests/` paths are from the repository root there. `<scratch>/scout-sinks/`, also written
> `<scout-sinks>`, is `docs/audits/2026-11-14-probes/sinks/`, so `probe_socket.py` is
> `docs/audits/2026-11-14-probes/sinks/probe_socket.py`.
> `docs/audits/2026-11-14-probes/README.md` gives the command that runs a probe from the
> repository root.

## Sinks audit at a0f44b5f (v1.2.0): delivered once, or counted and announced once?

**Short answer.** The engine does its own bookkeeping correctly: in every probe where the backend answered in its own failure shape, each event was delivered once or counted once with a line. The defects sit at the edges, in three classes: an adapter that calls a write "accepted" with no acknowledgement behind it, an adapter that calls a raised exception "not sent", and the emit-only sinks the two Draft specs already describe. Both Draft specs' claims reproduced.

**Two corrections to the brief.** No extra is installed in that venv (all twelve driver imports fail), so every SDK probe uses a fake client, and builtin `TimeoutError`/`ConnectionError` stand in for driver classes. The directory holds 35 sink classes and 4 wrappers, not about thirty.

**Ran versus read.** Ran: SQS, SNS, Kinesis, File, RotatingFile, Stdout, Syslog and Logstash over TCP and UDP, Postgres, SQLite, Kafka, Sentry (SDK backend) and HTTPSink. Read only: everything else. NATS and the HTTP core were read from docstrings, fences and targeted functions, not line by line. `FilteringSink` and `TransformSink` were read from their fences only.

Probes are in `<scratch>/scout-sinks/`. Each runs as `cd <scout-sinks> && PYTHONPATH=<wt-audit>/src <venv>/bin/python -B <script>`. All exited 0, no process is left running, and `git status` in the audit tree is clean. `probe_file.py` leaves a `probe-file-*` output directory there.

## Roster

Derived with `grep -n 'def deliver\|def emit\|def close\|^class ' src/log_foundry/sinks/*.py`; 39 classes covered.

Common to every engine sink: a total failure fails the run (`emit` raises, the worker moves `failed_batches`), a partial failure settles and counts, and an unreadable reply is `UNKNOWN`, counted and never resent. The retry bound is `max_retries + 1` rounds, multiplied by the worker's 4 cycles, so 16 sends at defaults. Every adapter maps a raised retryable exception to `RETRYABLE`.

| Sink | Path | "Delivered" means | Gap | `close()` and client buffer |
|---|---|---|---|---|
| SQSSink, SNSSink | engine | entry absent from `Failed` | lost reply duplicates; `Successful` never read | no-op, none |
| KinesisSink, FirehoseSink | engine | count 0 or absent and no coded record | lost reply duplicates; `{}` reads as delivered | no-op, none |
| HTTPSink, Datadog, Splunk, NewRelic, Loki | engine | any 2xx | no status line means resend | no-op, none |
| Elasticsearch, OpenSearch | engine | 2xx without truthy `errors` | per-item 429 is `REJECTED` | same |
| Honeycomb, OTLP | engine | per-event status / `partialSuccess` | as fenced | same |
| PostgresSink | engine | `commit()` returned | commit raise is `RETRYABLE` | commit then close |
| ClickHouseSink | engine | `insert` returned, per chunk | typed-column gap (known) | closes owned client |
| MongoDBSink | engine | not named in `writeErrors` | resend becomes a miscounted rejection (fenced) | closes owned client |
| RedisStreams, RedisList | engine | pipeline `execute()` returned | lost reply duplicates | closes owned client |
| RabbitMQSink | engine | `basic_publish` returned; no confirms, no `mandatory` | unroutable or dead-socket messages read accepted | closes channel |
| AzureEventHubsSink | engine | `send_batch` returned | `create_batch` raise escapes the round | `producer.close()` |
| NATSSink | engine | core: PONG after last publish; JetStream: ack | asymmetry recorded in invariant 2 | bounded close |
| SyslogSink, LogstashSink (socket) | engine | `sendall`/`sendto` returned | defect 1 | closes socket |
| FilteringSink, TransformSink | engine iff inner | inner verdict mapped | none found (fences only) | forwards |
| KafkaSink | emit-only | `produce()` enqueued | defect 6 | `flush(10 s)`, remainder counted |
| GooglePubSubSink | emit-only | `publish()` returned a future | same shape | bounded drain, remainder counted |
| SentrySink | emit-only | SDK: `capture_event` returned; HTTP: 2xx | defect 5 | SDK flush cannot report failure |
| FileSink, RotatingFileSink | emit-only | text-stream `flush()` returned; no fsync | SPEC-082 | flush and close |
| StdoutSink, StderrSink | emit-only | stream `flush()` returned | SPEC-084; counts no `failed` | flush only |
| SQLiteSink | emit-only | `with conn` committed | atomic unless autocommit is injected | commit, close if owned |
| LoggingSink, CallbackSink | emit-only | `logger.handle` / the callable returned | fire-and-forget | nothing |
| MemorySink, NullSink | emit-only | appended (ring) / counted as dropped | by design | nothing |
| MultiSink | emit-only | any child returned | failed child counted, never retried | closes each child |
| _FactorySink | wrapper | built sink's own | not examined further | forwards |

## Live defects, ranked

**1. TCP socket sinks lose events silently after the peer closes. Reproduced; no spec.**
`_socket.py:517` calls `sendall`, and `_socket.py:406-407` marks the frame `ACCEPTED`. In `probe_socket.py`, batch 1 lands, the listener closes the connection, and batch 2 of 50 is emitted. SyslogSink over TCP delivered 38 of 50 and LogstashSink 36 of 50, with `failed=0`, no raise and no stderr line. When the peer reset instead of closing, all 50 landed. The trigger is ordinary: a collector restart or a load balancer's idle timeout. SyslogSink's default UDP transport to a port with no listener lost 100 of 100 with nothing counted, which is inherent to UDP.

**2. Lost-reply duplicates on every engine adapter. Reproduced on three; no spec.**
- **SQS standard queue** (`sqs.py:796-797`, `probe_sqs.py` D): one reply lost after the queue accepted gave 35 messages for 25 events, with nothing counted or written.
- **SQS through the real worker** (`probe_aws_more.py` 1): with every reply lost, each of 10 events landed 16 times (160 messages) while the ledger read `failed=10`, `flush()` was False and `failed_batches=1`.
- **SQS FIFO** with dedup honoured: 0 duplicates.
- **HTTPSink** (`http.py:3301-3305`, `probe_misc.py` 6): the server stored the body and closed with no status line; 50 stored for 25 events, nothing counted.
- **PostgresSink** (`postgres.py:465-468`, `probe_misc.py` 1): the commit applied and the connection then dropped; 50 rows for 25 events, nothing counted.

The AWS `_send` docstrings accept this "on expected cost". Nothing user-facing says it.

**3. Stream sinks duplicate with no signal at all. Reproduced; SPEC-084 (Draft) covers it.**
`stdout.py:112-114`. With a file-size cap cutting a 200-event batch and lifted before the resend (`probe_file.py`), 37 to 113 events landed twice on a block-buffered stream and 39 to 101 on a line-buffered one. `losses()` stayed `None` and there were zero stderr lines. One unparseable line appeared at three of four block-buffered caps. This is the default sink.

**4. File sinks duplicate and also count the batch as lost. Reproduced; SPEC-082 (Draft) covers it.**
`file.py:280-291` and `file.py:617-632`. FileSink put 37 to 157 of 200 events on disk twice across five caps, with one torn line at three of them, and `losses().failed` read 200 although all 200 were on disk. RotatingFileSink gave 37 and 99 twice with no trigger, and 57 twice with `max_bytes=6000`. Through the real worker: two emits of 200, 26 events twice, one unparseable line, `flush()` True and `failed_batches=0`. The spec's 69-to-200 range differs only because my lines are shorter.

**5. SentrySink's SDK backend counts dropped events as sent. Reproduced; no spec.**
`sentry.py:448-458` ignores what `capture_event` returns. With a fake SDK keeping 1 in 5, `sent` read 25 and losses read 0 while 5 were kept. Sampling, `before_send`, rate limits and the SDK's bounded queue all take this path.

**6. Kafka and Pub/Sub issue their verdict on enqueue. Reproduced for Kafka; no spec.**
`kafka.py:312-318`. With a producer whose every delivery report was an error (`probe_misc.py` 3), 0 of 25 landed. `health().sink.failed` read 25 with 25 lines, so the loss is counted. But `flush()` returned True with `reason=None` and `failed_batches=0`, because `kafka.py:431-436` raises only for messages still queued. With the local queue full after 10 of 25, 15 were counted `dropped` and never retried, although a full queue is the transient case.

**7. Replies read by absence. Reproduced; no spec, deliberate per the fence.**
`_rounds.py:262-271` reads an entry not named in `Failed` as `ACCEPTED` and never reads `Successful`. `_batch.py:216-217` reads a Kinesis reply with no count and no records as delivered.
- SQS reply listing 7 of each 10 in `Successful` with no `Failed`: 6 of 25 missing, nothing counted.
- SQS reply `{}`: 25 of 25 missing, nothing counted.
- Kinesis reply `{}`: 0 of 25 stored, nothing counted.
- SNS, 7 of each 10: 19 of 25 stored, nothing counted.

Real AWS names every entry, so this needs a proxy, emulator or broken mock. It is fully silent when it happens.

**8. An injected autocommit connection breaks atomicity. Reproduced; no spec; unlikely configuration.**
- Postgres (`postgres.py:457-468`): chunk 2 of 3 failed once; 35 rows for 25 events, nothing counted.
- SQLite (`sqlite.py:409-412`): the caller's CHECK refused row 17; over four worker-style attempts 17 distinct rows landed 4 times each (68 rows) and `failed` read 100.

Neither sink checks or documents autocommit.

**9. Read only, not run.**
- RabbitMQ without confirms (`rabbitmq.py:429`); its docstring at lines 60-62 admits it.
- An Elasticsearch per-item 429 is final (`elasticsearch.py:206-208`), counted and announced.
- When every MultiSink child raises, the whole-batch resend rewrites a partly written file child; SPEC-084 lists this as residue 5.

**What held (`probe_sqs.py`).** Transient per-entry failures: 25 of 25 once. Two persistent failures and two sender faults: 23 landed and `failed=2` with lines. Three garbage replies: counted once as unadjudicated, not resent. Total outage: `failed=25`, raised, one line.

## Where the code delivers less than the docs imply

- `README.md:629-632` says a sink that delivered none raises and "there is nothing downstream to duplicate". Defect 2 contradicts it.
- The fence *A client exception costs its chunk, and is provable non-delivery for it* is false for any timeout after the bytes left.
- `SocketTransport.failed` is documented as "Messages not confirmed" (`_socket.py`, class Attributes), but nothing is ever confirmed.
- `SentrySink.sent` is documented as "Events captured or sent to Sentry".
- `FlushResult` gives a truthy `flush()` for a Kafka queue that drained by giving up.
- Neither file sink says that a retried partial write leaves `failed` counting events that are on disk.
- Unverified, from memory: `SQSSink.MAX_BYTES` (`sqs.py:186`) is 256 KiB and I believe AWS raised the limit to 1 MiB in 2025. Check before acting.

## Conformance

There is no single fault script with one expected ledger that every sink must pass.

- **Closest thing:** `tests/sink_rigs.py`, whose `RIGS` table covers 20 record-building sinks and is held equal to a derived roster. Its doubles fail whole calls only (`down`, `fail_next`, `error`), and it mainly drives the encode step in `tests/test_sinks_encode.py`.
- **Not in it:** the nine HTTP sinks, NATS, Sentry, Logging, Callback, Memory, Null and the wrappers.
- **Engine roster:** `tests/test_engine_sinks_roster.py` derives the thirteen engine sinks by AST and holds them to a structural rule (counters, `emit` is the adapter, no private wait, where counters may be written).
- **Fault scripts:** partial failure, garbage replies and budgets are hand-written per family in `test_sinks_queue_engine.py`, `test_sinks_db_engine.py`, `test_sinks_broker_engine.py`, `test_socket_engine.py` and the HTTP files.
- **Missing everywhere:** no double models "accepted, then the call raised", "peer closed between batches" or "client buffer never acknowledged", and none compares what the backend holds with the ledger.
- **`tests/test_extras_roster.py`** holds only the import rule.

A new sink would meet these gates (read from module docstrings and test names, not run): the import, injected-client, integration, release, fork and retry-loop rosters; the encode roster and a rig if it uses the encode helper; the deferral gate; the engine roster by name if it defines `deliver`; and docstring-lint. An emit-only sink that calls no helper meets almost none, and nothing asks any sink what "delivered" means.

## Structural recommendation (judgement, beyond finding and counting)

1. **One conformance suite keyed on acknowledgement class.** Each sink declares its class as data: keyed per entry, positional per entry, transactional all-or-nothing, synchronous per message, client-buffered asynchronous, unacknowledged stream, or fire-and-forget. Each class gets one stateful fake that records what it holds.
2. **One fault script per class:** reject a subset, accept then raise, accept then time out, garbage, a reply omitting entries, peer closed between batches, never acknowledge.
3. **One assertion:** landed once plus counted once equals N, with a line per loss. Each sink's permitted departures are rows in the table, as the four declared exceptions already are.
4. **A separate "ambiguous" answer in the adapters.** Defect 2 exists because "retryable" and "provably not sent" are one predicate. A timeout after the request left would become `UNKNOWN` unless the destination de-duplicates. The fence *No shipped sink declares itself idempotent* already argues this for verdicts but not for exceptions.

On this evidence the suite would have caught defects 1, 2, 5, 6, 7 and 8 mechanically.

## Detail cut for length

Everything above this heading is the report as handed back, verbatim. Everything below was measured or read during the same run and left out for the word limit, except where a line says it was added afterwards. No finding above is changed by anything below. All `file:line` references are to the audit tree at `a0f44b5f`, relative to `src/log_foundry/sinks/` unless a longer path is given.

### A. Exact output of each probe script

These are the outputs of the runs the report's numbers came from, copied as printed. Every script exited 0. In every probe that sets a stop signal, the signal is set before the call so the engine's backoff waits return at once; that shortens the run and changes no verdict.

#### `probe_sqs.py` (fake boto3-shaped client, N = 25, standard queue unless stated, `max_retries=3`, inline `emit`)

```
== A transient per-entry Failed on first call of each chunk
   calls=4 queue_holds=25 unique=25 duplicate_copies=0 missing=0
   ledger: failed=0 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=0

== B persistent per-entry Failed (2 events)
   calls=6 queue_holds=23 unique=23 duplicate_copies=0 missing=2
   ledger: failed=2 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=1
     | log-foundry: lost 2 message(s); SQSSink, still failing after 4 attempt(s); abandoned

== C SenderFault=True (2 events)
   calls=3 queue_holds=23 unique=23 duplicate_copies=0 missing=2
   ledger: failed=2 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=2
     | log-foundry: lost 1 message(s); SQSSink, rejected as invalid (first code: InvalidMessageContents); not retried
     | log-foundry: lost 1 message(s); SQSSink, rejected as invalid (first code: InvalidMessageContents); not retried

== D response lost once after the queue accepted (standard queue)
   calls=4 queue_holds=35 unique=25 duplicate_copies=10 missing=0
   ledger: failed=0 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=0

== D' same, FIFO queue with dedup honoured by the fake
   calls=5 queue_holds=25 unique=25 duplicate_copies=0 missing=0
   ledger: failed=0 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=0

== E response lost on every call (standard queue)
   calls=12 queue_holds=100 unique=25 duplicate_copies=75 missing=0
   ledger: failed=25 dropped=0 raised='delivered none of 25 event(s): rejected 0, unresolved 25, raised 0 after 4 delivery round(s) to SQSSink'
   uncounted_missing=0  stderr_lines=1
     | log-foundry: lost 25 message(s); SQSSink, 4 attempt(s), TimeoutError

== F1 reply None, queue stored all
   calls=3 queue_holds=25 unique=25 duplicate_copies=0 missing=0
   ledger: failed=25 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=3
     | log-foundry: lost 10 message(s); SQSSink could not adjudicate a send_message_batch reply (10 entry(ies) sent); not retried
     | log-foundry: lost 10 message(s); SQSSink could not adjudicate a send_message_batch reply (10 entry(ies) sent); not retried
     | log-foundry: lost 5 message(s); SQSSink could not adjudicate a send_message_batch reply (5 entry(ies) sent); not retried

== F2 reply Failed='oops', queue stored all
   calls=3 queue_holds=25 unique=25 duplicate_copies=0 missing=0
   ledger: failed=25 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=3
     | log-foundry: lost 10 message(s); SQSSink could not adjudicate a send_message_batch reply (10 entry(ies) sent); not retried
     | log-foundry: lost 10 message(s); SQSSink could not adjudicate a send_message_batch reply (10 entry(ies) sent); not retried
     | log-foundry: lost 5 message(s); SQSSink could not adjudicate a send_message_batch reply (5 entry(ies) sent); not retried

== F3 reply Failed names unknown Id, queue stored all
   calls=3 queue_holds=25 unique=25 duplicate_copies=0 missing=0
   ledger: failed=25 dropped=0 raised=None
   uncounted_missing=0  stderr_lines=3
     | log-foundry: lost 10 message(s); SQSSink could not adjudicate a send_message_batch reply (10 entry(ies) sent); not retried
     | log-foundry: lost 10 message(s); SQSSink could not adjudicate a send_message_batch reply (10 entry(ies) sent); not retried
     | log-foundry: lost 5 message(s); SQSSink could not adjudicate a send_message_batch reply (5 entry(ies) sent); not retried

== G1 reply lists 7 of 10 in Successful and no Failed; queue stored those 7
   calls=3 queue_holds=19 unique=19 duplicate_copies=0 missing=6
   ledger: failed=0 dropped=0 raised=None
   uncounted_missing=6  stderr_lines=0

== G2 reply {} and the queue stored nothing
   calls=3 queue_holds=0 unique=0 duplicate_copies=0 missing=25
   ledger: failed=0 dropped=0 raised=None
   uncounted_missing=25  stderr_lines=0

== H every call raises ConnectionError
   calls=12 queue_holds=0 unique=0 duplicate_copies=0 missing=25
   ledger: failed=25 dropped=0 raised='delivered none of 25 event(s): rejected 0, unresolved 25, raised 0 after 4 delivery round(s) to SQSSink'
   uncounted_missing=0  stderr_lines=1
     | log-foundry: lost 25 message(s); SQSSink, 4 attempt(s), ConnectionError
exit=0
```

Three things in that output the report did not spell out. In F1 to F3 the queue held all 25 events and the ledger read `failed=25`: the unadjudicated count is an upper bound, as invariant 2 records, and the lines say "not retried" rather than "may have landed". In G1 the script prints only the first four stderr lines of a scenario, and G1 and G2 wrote none. In D' the FIFO run made 5 calls rather than 4 because a FIFO group holds a later message back behind an earlier one that is being resent (`sqs.py:801-834`).

#### `probe_aws_more.py` (fake clients; item 1 goes through `configure`, a traced function, `flush` and `shutdown`)

```
1. SQSSink via worker, every reply lost after acceptance: events=10 calls=16 queue_holds=160 copies_each=[16] elapsed=2.9s
   flush()=False reason='abandoned' failed_batches=1 sink=SinkLosses(dropped=0, failed=10)
   | log-foundry: lost 10 message(s); SQSSink, 16 attempt(s), TimeoutError
   | log-foundry: lost 10 event(s); batch abandoned after 4 emit attempts
2. KinesisSink, reply {} and nothing stored: stored=0/25 raised=None losses: failed=0 dropped=0 stderr_lines=0
3. SNSSink, Successful names 7 of each 10 and no Failed: stored=19/25 raised=None losses: failed=0 dropped=0 stderr_lines=0
exit=0
```

#### `probe_file.py` (RLIMIT_FSIZE with SIGXFSZ ignored, so the write raises EFBIG, errno 27 on macOS; the cap is lifted after the first failed `emit` and the batch is resent whole)

```
python 3.13.1; one line = 104 bytes; batch = 200 events = 20800 bytes

FileSink                           cap=4000   emits=2 raised=1 lines_on_disk=238  events_twice=37   missing=0   unparseable=1  losses()=failed=200 dropped=0  stderr=1
FileSink                           cap=8192   emits=2 raised=1 lines_on_disk=279  events_twice=79   missing=0   unparseable=0  losses()=failed=200 dropped=0  stderr=1
FileSink                           cap=10500  emits=2 raised=1 lines_on_disk=300  events_twice=99   missing=0   unparseable=1  losses()=failed=200 dropped=0  stderr=1
FileSink                           cap=12000  emits=2 raised=1 lines_on_disk=314  events_twice=113  missing=0   unparseable=1  losses()=failed=200 dropped=0  stderr=1
FileSink                           cap=16384  emits=2 raised=1 lines_on_disk=357  events_twice=157  missing=0   unparseable=0  losses()=failed=200 dropped=0  stderr=1
   sample line: log-foundry: lost 200 event(s); FileSink, OSError errno=27
RotatingFileSink (no trigger)      cap=4000   emits=2 raised=1 lines_on_disk=238  events_twice=37   missing=0   unparseable=1  losses()=failed=200 dropped=0  stderr=1
RotatingFileSink (no trigger)      cap=10500  emits=2 raised=1 lines_on_disk=300  events_twice=99   missing=0   unparseable=1  losses()=failed=200 dropped=0  stderr=1
RotatingFileSink max_bytes=6000    cap=4000   emits=2 raised=1 lines_on_disk=257  events_twice=57   missing=0   unparseable=0  losses()=failed=200 dropped=0  stderr=1
StdoutSink(block-buffered file)    cap=4000   emits=2 raised=1 lines_on_disk=238  events_twice=37   missing=0   unparseable=1  losses()=None  stderr=0
StdoutSink(block-buffered file)    cap=8192   emits=2 raised=1 lines_on_disk=279  events_twice=79   missing=0   unparseable=0  losses()=None  stderr=0
StdoutSink(block-buffered file)    cap=10500  emits=2 raised=1 lines_on_disk=300  events_twice=99   missing=0   unparseable=1  losses()=None  stderr=0
StdoutSink(block-buffered file)    cap=12000  emits=2 raised=1 lines_on_disk=314  events_twice=113  missing=0   unparseable=1  losses()=None  stderr=0
StdoutSink(line-buffered file)     cap=4000   emits=2 raised=1 lines_on_disk=239  events_twice=39   missing=0   unparseable=0  losses()=None  stderr=0
StdoutSink(line-buffered file)     cap=10500  emits=2 raised=1 lines_on_disk=301  events_twice=101  missing=0   unparseable=0  losses()=None  stderr=0
   StdoutSink stderr lines: []

worker path: emit calls=2 batch sizes=[200, 200] flush=True reason=None
   distinct events on disk=200 events_twice=26 unparseable=1
   health: failed_batches=0 dropped=0 sink=None
   stderr: ['log-foundry: lost 200 event(s); FileSink, OSError errno=27']
exit=0
```

Two notes on reading it. In the first fifteen rows the resend is a loop in the probe that calls `emit` again after lifting the cap, standing in for the worker's whole-batch retry of a sink with no `deliver`; the last block is the same fault through the real worker, where a pass-through sink of the probe's own lifts the cap as the inner raise passes through it. In that block `health().sink` is `None` only because that pass-through sink has no `losses()`; a `FileSink` configured directly would report its `failed=200` there. The `RotatingFileSink max_bytes=6000` row reads every generation (`backup_count=50`), so its 57 duplicates span rotated backups and the active file.

#### `probe_socket.py` (in-process loopback listener; batch 1 of 10 lands, the listener closes the accepted connection, batch 2 of 50 is emitted)

```
python 3.13.1 darwin
SyslogSink tcp, peer closes (fin)            batch1 received=10/10  batch2 received=38/50 (copies=38)  missing=12  raised=None  losses: failed=0 dropped=0  stderr_lines=0
LogstashSink tcp, peer closes (fin)          batch1 received=10/10  batch2 received=36/50 (copies=36)  missing=14  raised=None  losses: failed=0 dropped=0  stderr_lines=0
SyslogSink tcp, peer closes (rst)            batch1 received=10/10  batch2 received=50/50 (copies=50)  missing=0  raised=None  losses: failed=0 dropped=0  stderr_lines=0
LogstashSink tcp, peer closes (rst)          batch1 received=10/10  batch2 received=50/50 (copies=50)  missing=0  raised=None  losses: failed=0 dropped=0  stderr_lines=0
SyslogSink udp (default), no listener        sent=2x50 received=0  raised=None  losses: failed=0 dropped=0  stderr_lines=0
exit=0
```

Added afterwards, while writing this file: the number lost in the FIN case depends on how many `sendall` calls return before the peer's reset reaches the sender, so it varies from run to run. Three further runs of the same script gave batch 2 received, for SyslogSink then LogstashSink: 48 and 45, 46 and 46, 45 and 38 of 50. Across the four runs the loss was between 2 and 14 of 50 and never zero, and the ledger, the raise and the stderr count were the same every time (0, none, 0). The RST case delivered 50 of 50 in every run. The finding stands as reported; only the size of the loss moves.

#### `probe_misc.py` (fakes, plus an in-process `http.server` for item 6 and an in-memory SQLite database for item 7)

```
1. PostgresSink, commit applied then connection lost once
   rows_in_table=50 unique=25 duplicate_rows=25 raised=None losses: failed=0 dropped=0 stderr_lines=0
2. PostgresSink, injected autocommit connection, chunk 2 of 3 fails once (chunk_size=10)
   rows_in_table=35 unique=25 duplicate_rows=10 raised=None losses: failed=0 dropped=0 stderr_lines=0
3. KafkaSink via worker, broker never acknowledges (25 events)
   landed=0 flush()=True reason=None failed_batches=0 health.sink=SinkLosses(dropped=0, failed=25) stderr_lines=25
4. KafkaSink, local queue full after 10 of 25
   landed=10 raised=None losses: failed=0 dropped=15 stderr_lines=15 (never retried)
5. SentrySink sdk backend, the SDK keeps 1 in 5
   sdk_kept=5/25 sink.sent=25 raised=None losses: failed=0 dropped=0 stderr_lines=0
6. HTTPSink, server stored the body then closed with no status line, once
   requests=2 stored=50 unique=25 duplicate_copies=25 raised=None losses: failed=0 dropped=0 stderr_lines=0
7. SQLiteSink, injected autocommit connection, row 17 refused by the caller's CHECK
   attempts_raised=4 rows=68 unique=17 max_copies=4 losses: failed=100 dropped=0 stderr_lines=4
8. RabbitMQSink signature: (self, *, exchange: 'str', routing_key: 'str', connection: 'Any' = None, url: 'str | None' = None, max_retries: 'int' = 3, blocked_connection_timeout: 'float | None' = None, socket_timeout: 'float | None' = None, stack_timeout: 'float | None' = None) -> 'None'
exit=0
```

Item 8 only prints a signature: RabbitMQ was read, not driven. Item 3 writes one stderr line per message (25 lines for 25 messages), from `kafka.py:518-521`. Item 7's four attempts are a loop in the probe standing in for the worker's whole-batch retry.

### B. The full per-sink roster, with the line that decides "delivered"

One row per class: the 39 classes of the report's roster, plus `SocketTransport` as row 22, so 40 rows. "Decided at" is the line that turns a backend's answer into an accepted event; "Not-sent decided at" is where a raised exception becomes a resend. A row marked *inherits* has no decision of its own.

| # | Class | Path | Delivered decided at | Not-sent decided at |
|---|---|---|---|---|
| 1 | SQSSink | engine | `sqs.py:798` into `_rounds.py:270-271` (an entry `Failed` does not name), applied at `_rounds.py:408-411` | `sqs.py:796-797`, `_rounds.py:379-382` |
| 2 | SNSSink | engine | `sns.py:400-405` into the same `_rounds.py:270-271`; settled from `sns.py:246` | `sns.py` `_send` except clause beside line 400, `_rounds.py:379-382` |
| 3 | KinesisSink | engine | `kinesis.py:445`, settled from `kinesis.py:262` into `_rounds.py:527-534`; the rule is `_batch.py:215-225` | `_rounds.py:485-488` |
| 4 | FirehoseSink | engine | `firehose.py:446`, settled from `firehose.py:258` into the same lines | `_rounds.py:485-488` |
| 5 | HTTPSink | engine | `http.py:3309-3310` (any 2xx), then `http.py:2620` for a subclass with a 1.x bool reader | `http.py:3301-3305` |
| 6 | DatadogSink | engine | inherits 5 (`datadog.py:42`; no reader of its own) | inherits |
| 7 | SplunkHECSink | engine | inherits 5 (`splunk.py:21`) | inherits |
| 8 | NewRelicSink | engine | inherits 5 (`newrelic.py:19`) | inherits |
| 9 | LokiSink | engine | inherits 5 (`loki.py:24`) | inherits |
| 10 | ElasticsearchSink | engine | `elasticsearch.py:193-194` (no truthy `errors`), per item at `elasticsearch.py:206-208` | inherits |
| 11 | OpenSearchSink | engine | inherits 10 (`elasticsearch.py:274`) | inherits |
| 12 | HoneycombSink | engine | `honeycomb.py:374` (per-event integer status in 2xx) | inherits |
| 13 | OTLPSink | engine | `otlp.py:295` and `otlp.py:323` | inherits |
| 14 | PostgresSink | engine | `postgres.py:420-421`, after `postgres.py:457-469` returned | `postgres.py:466-468`, `postgres.py:423` |
| 15 | ClickHouseSink | engine | `clickhouse.py:348-349`, after `clickhouse.py:438-442` | `clickhouse.py:440-441`, `clickhouse.py:351` |
| 16 | MongoDBSink | engine | `mongodb.py:446-448` (no raise), `mongodb.py:488-491` (not named in `writeErrors`) | `mongodb.py:434-436` |
| 17 | RedisStreamsSink | engine | `redis.py:302-303`, after `redis.py:283-286`; staged at `redis.py:499` or `501` | `redis.py:287-301` |
| 18 | RedisListSink | engine | same `_RedisSink._round`; staged at `redis.py:573-575` | same |
| 19 | RabbitMQSink | engine | `rabbitmq.py:344-345`, after `rabbitmq.py:428-438` | `rabbitmq.py:435-437`, `rabbitmq.py:347` |
| 20 | AzureEventHubsSink | engine | `eventhubs.py:396`, after `eventhubs.py:492-496` | `eventhubs.py:494-495`, `eventhubs.py:398` |
| 21 | NATSSink | engine | `nats.py:272-276` (a publish that completed, in a batch whose confirmation was not missed); publish at `nats.py:1690-1695` | `nats.py:268-269` (nothing published in the round) |
| 22 | SocketTransport (not itself a sink) | engine | `_socket.py:406-407`, after `_socket.py:513-521` | `_socket.py:518-520`, `_socket.py:409` |
| 23 | SyslogSink | engine | forwards to 22 from `syslog.py:161` | as 22 |
| 24 | LogstashSink | engine | socket mode forwards to 22, HTTP mode to its `HTTPSink` backend, from `logstash.py:283` | as its backend |
| 25 | FilteringSink | engine iff inner | maps the inner verdict from `filtering.py:183` (fences only, not read) | as inner |
| 26 | TransformSink | engine iff inner | maps the inner verdict from `transform.py:267` (fences only, not read) | as inner |
| 27 | KafkaSink | emit-only | `kafka.py:312-318` (`produce()` returned); the later verdict is the callback at `kafka.py:518-521` and the close flush at `kafka.py:470-479` | `kafka.py:323-326` (every message refused) |
| 28 | GooglePubSubSink | emit-only | `pubsub.py:268-275` (a future was appended); futures are read in `_reap`, `pubsub.py:365-374` | `pubsub.py:288-291` (every publish refused) |
| 29 | SentrySink | emit-only | `sentry.py:448` then `sentry.py:458`, counted at `sentry.py:245`; HTTP fallback through `sentry.py:608-613` | `sentry.py:247-250` region (every qualifying event failed) |
| 30 | FileSink | emit-only | `file.py:280-282` (writes and one flush returned) | `file.py:283-291` |
| 31 | RotatingFileSink | emit-only | `file.py:617-623` | `file.py:624-632` |
| 32 | StdoutSink | emit-only | `stdout.py:112-114` | none of its own: the stream's exception escapes uncounted |
| 33 | StderrSink | emit-only | inherits 32 (`stdout.py:148`) | inherits |
| 34 | SQLiteSink | emit-only | `sqlite.py:409-412` (`with self._conn` exited without raising) | `sqlite.py:356-363` |
| 35 | LoggingSink | emit-only | `logging_sink.py:93-94` (`logger.handle` returned) | none of its own |
| 36 | CallbackSink | emit-only | `callback.py:72` (the callable returned) | none of its own |
| 37 | MemorySink | emit-only | `memory.py:55-57` (appended, then trimmed to `maxlen`) | none |
| 38 | NullSink | emit-only | never: `null.py:55-56` counts every event as dropped | none |
| 39 | MultiSink | wrapper, emit-only | `multi.py:162-165` (at least one child returned) | `multi.py:164-165` (every child raised) |
| 40 | _FactorySink | wrapper | forwards from `_factory.py:166`; not examined further | as the built sink |

That is 39 classes plus `SocketTransport`, which the tree's own engine roster counts as an engine object but which is not a structural sink (`_socket.py:134`). Two cells are approximate and say so: the SNS except clause and the Sentry total-failure raise were located by reading the function, and I did not pin their exact line numbers. Where a row says "none of its own", an exception from the stream, logger or callable escapes `emit` and the worker's retry and `failed_batches` own the batch.

### C. Where the 16-sends bound comes from

The bound is the engine's rounds per cycle multiplied by the worker's cycles, and the multiplication is deliberate.

- **The arithmetic.** `_engine.py:1619` sets `per_cycle = _count(sink_retries) + 1` and `_engine.py:1620` sets `rounds_allowed = per_cycle * (_count(worker_retries) + 1)`. The loop stops at `_engine.py:1724` when `attempts >= rounds_allowed`.
- **The sink's term.** `sink_retries` is the sink's own `max_retries`, read statically. It defaults to 3 on SQS (`sqs.py:193`) and on every other engine sink I checked (`sns.py:72`, `kinesis.py:74`, `firehose.py:73`, `clickhouse.py:88`, `mongodb.py:161`, `eventhubs.py:53`, `redis.py:443` and `517`, `http.py:2199`, and `postgres.py`, `syslog.py:83`, `logstash.py:130` in their constructors). That gives 4 rounds per cycle.
- **The worker's term.** The worker passes `worker_retries=self.max_retries` at `worker.py:1933-1936`, and `Worker`'s `max_retries` defaults to 3 at `worker.py:364`. That gives 4 cycles.
- **The other two callers pass no worker term.** The inline adapter passes `worker_retries=0` at `_engine.py:1995`, and the orphan path passes `None` (`_engine.py:1907`), so `emit` called directly, and a log call outside a span, get 4 rounds, not 16. That is why `probe_sqs.py` E shows 12 calls for three chunks and `probe_aws_more.py` 1 shows 16 calls for one chunk.
- **The wait schedule.** Inside a cycle the wait is `_backoff(position - 1)`, and at a cycle boundary `cycle_backoff(cycle - 1)` (`_engine.py:1664-1667`). The 16-round run in `probe_aws_more.py` took 2.9 s with real waits.
- **That it is intended.** The `_run` docstring states it at `_engine.py:1542-1544`: "**The budget keeps the 1.x total**: `(s + 1) * (w + 1)` rounds, `s` the sink's and `w` the worker's `max_retries` ... run as `w + 1` cycles of `s + 1`." Two fences in the `sink-delivery` area say the same thing. `docs/decisions/index.yaml:249-250`, *A sink may report per event: `deliver` returns a `Verdict`, and one engine resends only what may land*, reads "one private engine resends only `RETRYABLE` events within the 1.x total of attempts and the 1.x wait schedule" (file: `docs/decisions/sink-delivery/a-sink-may-report-per-event-deliver-returns-a-verdict-and-one-engine-resends-only-what-may-land.md`). `docs/decisions/index.yaml:253-254`, *A sink round resends only what failed, and an SQS FIFO group keeps its order*, reads "the delivery engine resends only what may land within 1.x's total budget" (file: `docs/decisions/sink-delivery/a-sink-round-resends-only-what-failed-and-an-sqs-fifo-group-keeps-its-order.md`). The 1.x total it preserves is the worker retrying a raising `emit` four times, each `emit` running the sink's own four attempts.
- **What that means for defect 2.** The bound was chosen to keep the retry budget, on the premise that a resent event is one that did not land. Where that premise fails, the same number is the worst-case copy count: 16 on the worker path and 4 on the inline and orphan paths, as measured.

### D. De-duplication keys: what each backend offers and whether the sink passes one

Every event carries a per-event UUID, `log_id`, assigned at assembly (`src/log_foundry/model.py:206`), so a natural key exists for every sink below. The "sink passes it" column is from the code and carries a `file:line`. The "backend offers" column is from my memory of each vendor's documentation and was not checked against live documentation during this run; treat each entry there as a claim to verify before designing on it.

| Sink | Backend offers (from memory, unverified) | Sink passes it today |
|---|---|---|
| SQSSink, FIFO queue | `MessageDeduplicationId`, honoured for 5 minutes | Yes. `sqs.py:791-792`; defaults to `log_id` (`sqs.py:558-559`), resolved once per run so every resend carries the same id (`sqs.py:591-597`). Measured: 0 duplicates in D'. |
| SQSSink, standard queue | Nothing | Not applicable. The entry carries `Id` and `MessageBody` only (`sqs.py:788`). |
| SNSSink | `MessageDeduplicationId` and `MessageGroupId` on FIFO topics only | No. `sns.py` names neither (the grep for both in `sns.py` returns nothing), so a FIFO topic gets no id from the sink. |
| KinesisSink | Nothing on `PutRecords`; duplicates are the consumer's to handle | No key exists. The record is `Data` and `PartitionKey` (`kinesis.py:343`). |
| FirehoseSink | Nothing | No key exists (`firehose.py:446`). |
| HTTPSink | Whatever the endpoint defines; an `Idempotency-Key` header is a convention some APIs honour | No. No file under `sinks/` contains the word "idempotency" in a header (the grep returns nothing); a caller could add a constant header through `headers=`, which is the same for every request and so identifies nothing. |
| ElasticsearchSink, OpenSearchSink | An `_id` on the bulk action: `index` overwrites the same document, `create` answers 409 for a repeat | No. The action line is `{"index": {"_index": ...}}` with no `_id` (`elasticsearch.py:127`), so a resend indexes a second document. |
| LokiSink | Loki drops an entry identical in stream, timestamp and line to one it already holds | Not passed as a key; a byte-identical resend may be absorbed for that reason. Not tested. |
| DatadogSink, SplunkHECSink, NewRelicSink, HoneycombSink | Nothing I know of on the ingest path | No. |
| OTLPSink | Nothing in the protocol; `log.record.uid` is an attribute a backend may or may not use | The `log_id` travels as `log.record.uid` (`src/log_foundry/_otel.py:626`), as data rather than as a de-duplication key. |
| PostgresSink | A unique column with `ON CONFLICT DO NOTHING` | No. The statement is a plain `INSERT` (`postgres.py:206`), and the table the sink can create has only `id BIGSERIAL PRIMARY KEY` (`postgres.py:791`), so no unique `log_id` exists to conflict on. |
| SQLiteSink | A unique column with `INSERT OR IGNORE` | No. Plain `INSERT` (`sqlite.py:410-411`); the created table has only `id INTEGER PRIMARY KEY AUTOINCREMENT` (`sqlite.py:475`). |
| ClickHouseSink | Block de-duplication on replicated tables, and an `insert_deduplication_token` setting | No. `client.insert` is called with the table, rows and column names only (`clickhouse.py:439`), and the table the sink can create is a plain `MergeTree ORDER BY tuple()` (`clickhouse.py:487`), which de-duplicates nothing. |
| MongoDBSink | `_id` is unique, so a repeat is a duplicate-key error | Partly. The sink sets no `_id` from `log_id`; the driver stamps one into the document, and the sink keeps the document for the run so a resend carries that same `_id` (`mongodb.py:133`, `mongodb.py:388`). A resend of a document that landed is therefore refused by the server and counted as a rejection rather than duplicated, which is the fence's "miscounted rejection". |
| RedisStreamsSink | An explicit entry id on `XADD`, which must increase; no general de-duplication | No. `XADD` with an auto id (`redis.py:499`, `501`). |
| RedisListSink | Nothing | No (`redis.py:573`). |
| RabbitMQSink | Nothing in the broker by default; `message_id` is a property a consumer or a plugin can use | No. The properties are `delivery_mode` only (`rabbitmq.py:564`). |
| KafkaSink | The idempotent producer (`enable.idempotence`) covers the client's own internal retries; the message key is a partition key, not a de-duplication key | The key is `trace_id` (`kafka.py:494-497`), which groups a trace on one partition and identifies no message. `enable.idempotence` is not set by the sink and is reachable through `producer_config=` (`kafka.py:167-169`). The sink itself never re-produces a message it handed over, so its own duplicate window is the client's. |
| GooglePubSubSink | Nothing on publish; exactly-once is a subscription-side feature | No. `publish(topic, data=...)` only (`pubsub.py:268`). |
| AzureEventHubsSink | Nothing I know of in the Python producer | No. `EventData(body)` only (`eventhubs.py:349`). |
| NATSSink, JetStream | The `Nats-Msg-Id` header, within the stream's duplicate window | No. `publish(subject, payload, timeout=...)` with no headers (`nats.py:1690-1693`). `docs/architecture.md:1087` and `nats.py:358` already name this header as the closure of the JetStream asymmetry. |
| NATSSink, core | Nothing | No (`nats.py:1695`). |
| SyslogSink, LogstashSink (socket) | Nothing in the protocol; a Logstash pipeline could fingerprint on `log_id` downstream | Not applicable. |
| SentrySink | `event_id`: Sentry discards a second event with an id it has seen | Partly. The HTTP fallback mints a fresh `uuid4` per envelope (`sentry.py:601`), so the retries inside one `_send` reuse it and a worker resend of the batch does not. The SDK path passes no `event_id` (`sentry.py:563-568`) and the SDK assigns its own. Neither derives it from `log_id`. |
| FileSink, RotatingFileSink, StdoutSink, StderrSink | Nothing; a file has no key | Not applicable. SPEC-082 and SPEC-084 close the window by byte and line accounting instead. |
| LoggingSink, CallbackSink, MemorySink, NullSink, MultiSink, FilteringSink, TransformSink, _FactorySink | Not applicable | Not applicable; the wrappers pass through whatever the inner sink does. |

Counted from that table: of the backends where I believe a key exists (SQS FIFO, SNS FIFO, Elasticsearch and OpenSearch, PostgreSQL, SQLite, ClickHouse, MongoDB, JetStream, Sentry), the sink passes a stable per-event key on one path only, SQS FIFO. MongoDB and Sentry's HTTP fallback reuse a key within a run or a request but not from `log_id`. This is the evidence behind recommendation 4 in the report; whether to pass a key on any of the others is a design decision, not a finding.

### E. Smaller things measured or read and left out

- **`RotatingFileSink` retention.** With `backup_count=1` (the default) a batch larger than two generations keeps only the newest two; the rest was written, flushed and then deleted by rotation, with no counter. The class docstring states this as the configuration working (`file.py:402-408`), so it is documented behaviour and not listed as a defect.
- **`KafkaSink.emit` and `poll(0)`.** `producer.poll(0)` at `kafka.py:319` is outside the `try`, so an exception from it escapes `emit` after earlier messages were handed over and the worker would produce them again. It needs a producer callback of the caller's that raises. Read, not run.
- **`AzureEventHubsSink` and `create_batch`.** `producer.create_batch()` at `eventhubs.py:346` and `355` is outside the guarded send, so a raise there after an earlier pack was sent makes the engine mark the whole round as raised: the pack that landed is counted in `failed` and not resent on the worker path. That is an over-count, not a duplicate. Read, not run.
- **`SQLiteSink` and a borrowed connection.** `with self._conn` (`sqlite.py:409`) commits or rolls back whatever transaction the caller had open on a borrowed connection. Read, not run.
- **`MultiSink` beside a non-engine child.** A child that raises beside a sibling that delivered is counted (`multi.py:152-154`) and never retried, because a whole-batch retry would duplicate into the sibling. For an engine child the child's own adapter has already run its four rounds; for `FileSink`, `StdoutSink` or `KafkaSink` as a child, one failure is final. Read, not run.
- **Elasticsearch `{}`.** A 2xx object with no truthy `errors` reads as full success (`elasticsearch.py:193-194`), which the fence records as "unverified". It is the same shape as defect 7 and I did not list it there because the fence already names it.
- **The engine sinks that held.** Beyond the SQS cases in the report, nothing in the probes showed the engine itself miscounting: every loss the adapter reported was committed once and announced once, on the inline path and through the worker.
