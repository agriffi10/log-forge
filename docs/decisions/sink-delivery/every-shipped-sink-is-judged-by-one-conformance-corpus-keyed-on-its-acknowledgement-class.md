---
type: Decision
title: "Every shipped sink is judged by one conformance corpus keyed on its acknowledgement class"
description: "every shipped sink's acknowledgement class is declared per constructor variant in `tests/conformance_classes.py`, never on the sink; every row is judged by what its fake backend holds against the fates the library reported, with `ambiguous` a fate of its own read from the engine's verdicts and a check that refuses it where the call was knowable; a red row is pinned to its §12 open item at its exact verdict or is a departure quoting its record; and no library-side count of an ambiguous outcome is added here. (SPEC-092)"
---

**What a sink can know about whether its backend took an event is its acknowledgement class**, and
it belongs to a constructor variant rather than a Python class, because the constructor picks the
transport: one `LogstashSink` is a `socket-stream` and another a `request-reply`, and `NATSSink`
over JetStream acknowledges each publish while core NATS does not. Ten classes cover the tree. The
declaration lives in `tests/conformance_classes.py` and is held to the sink roster
`tests/test_public_surface.py` derives and to every rostered class's constructor parameters, so a
new sink cannot ship without a class and a new constructor argument cannot ship without a verdict on
whether it makes a variant. **Rejected: a public attribute on the sink.** It would add to the
surface of every shipped sink for good (invariant 10 lets the surface grow, never shrink), it could
not tell two variants of one class apart without becoming an instance property, and the corpus is
its only reader, since a third-party sink is not in it.

**One assertion judges every row** of `tests/test_conformance.py`: each event's fate (landed, lost,
ambiguous or skipped) is read from the rounds the engine ran, through `fate_of` alone, and proved
against the engine's own `Delivery` run by run; a sink with no `deliver` is read per batch. The
seven checks (`partition`, `silent-loss`, `duplicate`, `lost-but-held`, `unannounced`,
`ambiguous-when-knowable`, `ledger`) compare that with what the fake backend durably holds. An
outcome the sink cannot know is `ambiguous`, an outcome of its own rather than a pass, and
`ambiguous-when-knowable` is what keeps it from being a way out: a sink that called every raise
ambiguous would pass the faults whose outcome is unknowable and fail the ones that are not.
`unannounced` asks for a loss line per batch, not for the counts the lines name to add up, because
the HTTP family names its losses in requests. **Rejected: summing the counts the lines name**,
which made `MultiSink[over-http]-refused-always` red for its unit rather than for a defect.
**Rejected: extending `tests/sink_rigs.py`**, whose doubles fail whole calls only and which leaves
out the HTTP family, NATS, Sentry, the in-process sinks and the wrappers.

**A row red on today's code is never fixed by this corpus and never skipped.** It is pinned to the
bold lead of an open `docs/architecture.md` §12 bullet at its exact verdict, as a strict expected
failure, or it is a permitted departure that passes only at its exact verdict and quotes its record
verbatim from a named file. Which test outcome a verdict gives is decided in one place,
`settle` in `tests/conformance_pins.py`, and the register is checked by `register_faults`; both are
pure functions with standing fixture corpora, so the suite stays green and turns red the day a
sink is fixed or a new defect appears. A pin moves only after the owner hears why (SPEC-085's
rule).

**No counter of an ambiguous outcome is added here.** Today the library expresses it as `UNKNOWN`,
counted among failures; the corpus reads it test-side per event. A library-side count of what may
have landed or may have duplicated belongs to the spec that adds idempotency keys (finding 5), and
`fate_of` is the one place it would be read.

**Do NOT** declare an acknowledgement class as a sink attribute; do NOT judge a row by summing the
counts its loss lines name; do NOT skip or loosen a red row (a non-strict `xfail`, `skip`, or a
verdict wider than the one measured) instead of pinning it to its §12 item or recording a
departure with its quoted record; do NOT add a library-side ambiguous count outside the
idempotency-key spec.

## History

### 2026-11-18

- **Settled** by SPEC-092 FR-001 to FR-004: 256 rows over 53 variants and ten classes, 74 pinned
  under 12 pins, 12 departure rows under 7 departures, 170 that pass.
