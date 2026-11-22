# Completed Spec — SPEC-093: Acknowledgement Fixes: a Write the Sink Cannot Confirm Is Counted, Not Believed

## What was completed?

Built with SPEC-094 on one branch and landed in one PR.

- **TCP socket checks** (FR-001), in `sinks/_socket.py`: `_peer_state`, `_unacked` and `_end_state`
  seams, `_RESET_ERRNOS`, `_RESET_WAIT` (0.25 s) and `_RESET_STEP`, the pre-round replacement of a
  connection that is not open, rule 4's `UNKNOWN` frames on a later raise, the end check's wait for
  the peer's acknowledgement, and `require_ack_wait`. `SyslogSink` and `LogstashSink` take
  `ack_wait: bool | None = None`; `SocketTransport` takes `ack_wait: bool = True`. `_socket` joins
  `WAIT_EXCLUSIONS`.
- **Sentry discards** (FR-002): `SentrySink.discarded`, counted through `emit`'s `Deferral`,
  reported as `losses().dropped`, one line per `emit`.
- **Replies that name no event** (FR-003): `_rounds.read_keyed` reads `Successful`,
  `adjudicate_positional(absent=)`, the `_bulk` reader's neither-key object, and
  `dropped_unadjudicated` on `SQSSink` and `SNSSink`.
- **RabbitMQ confirms** (FR-004): `RabbitMQSink(confirm=False)`, `confirm_delivery()` per channel,
  `mandatory`, the owned-connection liveness probe, and the publish outcomes.
- **The corpus** (FR-005): `Variant.adds` and `adds_faults`, the two confirmed RabbitMQ variants, the
  RabbitMQ double's confirm mode, five pins removed and the `publish-unconfirmed` and
  `confirm-borrowed-unprobed` departures. SPEC-095 landed first, so on the merged tree, with
  SPEC-094's: 268 rows, 6 pinned under 4 pins, 34 departure rows under 14 departures, 228 that
  must pass. This spec's own settlement before then read 268 rows, 62 pinned under 7 pins, 15
  departure rows under 9 departures and 191 that must pass, measured at `a17a28a4` on base
  `c67931cd`, a local commit of the build that is not on the remote after the squash merge.
  Against `295ba803`'s corpus and `src`, fourteen shared rows move, this spec's ten and
  SPEC-094's four, and the twelve confirmed rows are new
  (`docs/audits/2026-11-14-probes/sinks/spec093_094_build_results.txt`).
- **New tests:** `tests/test_socket_close_corpus.py`, `test_socket_ack_check.py`,
  `test_sentry_discard.py`, `test_reply_names_no_event.py`, `test_rabbitmq_confirm.py`.
- **The record** (FR-006): `architecture.md` §12 (four items struck and moved under *Resolved*, the
  `ElasticsearchSink` and finding-5 items rewritten, and an open item found by this ritual: tagging
  the pending release `v1.3.0` without renaming `docs/release-notes/v1.2.1.md` silently ships
  generated notes, and one the second diff review found: a Kinesis or Firehose `{"Records": []}`
  with no failed count, and a `_bulk` `2xx` `{"items": []}`, still read a non-empty chunk as
  delivered, outside FR-003's criteria and unfixed under the audit scope freeze; and one the
  rebase review found, present since SPEC-095 and unfixed under the freeze: a `RabbitMQSink`
  publish whose channel never opened, a `StreamLostError` from `connection.channel()` or
  `confirm_delivery()`, is counted as a resend that may have landed, `resent=1`, though nothing
  was published) and five §13
  bullets; four new decisions and two
  refined in `docs/decisions/sink-delivery/`; `invariants.md` 2, 3, 4, 7 and 13; the README; and
  `docs/release-notes/v1.2.1.md`, whose title and opener were rewritten.

**Deviations, each recorded in the spec:**

- FR-001's close-corpus criterion asked for every frame `UNKNOWN` at 5 frames, which holds on macOS
  only: on Linux the reset makes the second write raise, so rule 4 counts the first frame and the
  rest arrive on a new connection. The criterion was corrected to the corpus's verdict (*Revision
  history*, 2026-11-20).
- `confirm` refuses a non-`bool` with `TypeError`, and `ack_wait` sits in the corpus's
  `PARAMETERS` and `NEUTRAL` (both recorded in the spec's 2026-11-19 correction).

## What changed from earlier specs?

- SPEC-092: `test_a_check_removed_from_judge_frees_the_row_it_holds`'s silent-loss case moved to
  `SyslogSink[udp]-peer-closed`, since the TCP rows it used are green.
- SPEC-070 and SPEC-074: an SQS or SNS reply that lists `Successful` is read, and a Kinesis or
  Firehose reply with neither a count nor records is unadjudicated; the unread-reply line ends `may
  have landed, not retried`.
- `SentrySink`'s SDK backend: a `capture_event` returning `None` is no longer counted in `sent`.

## Verification

Gates green at `ea882c91`. The re-plants, verdict comparisons, probes and the local integration run
are in `docs/audits/2026-11-14-probes/sinks/spec093_094_build_results.txt`, produced by
`spec093_094_build_mutants.py` beside it. Against SPEC-092's delivered corpus exactly the ten rows
FR-001 to FR-003 name moved, and every FR-001 and FR-005 re-plant reddened its named check.
`probe_socket.py`, `probe_sqs.py`, `probe_aws_more.py` and `probe_misc.py` case 5 read as the criteria
ask.

**Owed until the PR's `integration.yml` run is green:** FR-004's integration criterion. It ran by
hand at `1b562822` against the compose broker, green, and its `mandatory` re-plant went red ("DID NOT
RAISE SinkDeliveryError"); the PR's own run, which `integration.yml` triggers on `pull_request` for
`src/log_foundry/sinks/**`, settles it.
