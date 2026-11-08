---
type: Decision
title: "The response is read under a bound, and an oversized non-2xx is abandoned whatever its status"
description: "`HTTPSink._response_limit()` defaults to `None`, the bare `read()` every sink and test double relies on; `OTLPSink` returns 4 MiB, `_attempt` reads one byte past it, `_exchange` (the classification the round and `_send` share) abandons a longer non-`2xx` as `REJECTED` whatever its status — a `413` included, literally — and the reader judges a `2xx` against the same bound. No instance flag: the orphan path shares one sink across threads. (SPEC-056, SPEC-073)"
---

**The response is read under a bound, and an oversized non-2xx is abandoned whatever its status**
— OTLP says a client must limit the response body it parses (4 MiB recommended) and treat an
oversized one as a non-retryable error. The family had the unbounded read every sink relies on, so
the bound is a second private hook, `HTTPSink._response_limit() -> int | None`, defaulting to
`None`: with `None`, `_attempt` keeps calling a bare `read()` under #283's guard, which is what
every existing sink and every response double in the suite expects; with a bound, it reads at most
one byte past it, so `_exchange` and the reader can tell "exactly the bound" from "over it"
without the read ever growing with the response. The bounded read sits inside the same guard and
checks `response.length` itself, because `read(amt)` returns a short `Content-Length` body
silently where `read()` raises: fewer bytes than asked for with bytes still owed is `b""`, exactly
as a raise is (*After a status line the status decides*, #283). A non-`2xx` longer than the bound is abandoned on the spot — in `_send`
until SPEC-073, in `HTTPSink._exchange` since, `REJECTED`, counted and announced — **whatever its
status**: the check sits ahead of the retryable test and
ahead of the `413` branch, read literally, since a server answering a refusal with megabytes is not
describing the request; a `413` under the bound still halves and re-sends. A `2xx` reaches
the reader (`OTLPSink._outcomes` since SPEC-073) as it was read, and it tests
`len(payload) > limit` itself: a body of
exactly the bound is adjudicated, one byte more is unadjudicated. **Rejected: an instance flag set in
`_attempt` for the reader to read.** The orphan path emits from application threads into one
shared sink (invariant 4), so a flag would carry one thread's response into another's chunk.
`urllib` asks for `Accept-Encoding: identity`, so the body is read as sent and there is no
decompression to bound. The request side of the same specification — a server bounds what it parses
at 64 MiB and answers `413` — is met by `max_batch_bytes`, whose 5,000,000 default is far under it.
What `b""` means to this sink is recorded in *A 2xx this sink cannot read is counted*.
(SPEC-056 FR-003)

## History

### 2026-11-08

- **Refined** by SPEC-073: the bound is applied at `HTTPSink._exchange`, the one classification
  the round and `_send` share, which marks an oversized non-`2xx` `REJECTED`; the `2xx` side is
  judged by `OTLPSink._outcomes`. The rule and its order are unchanged.

### 2026-11-03

- **Settled.** Shipped with `OTLPSink`; `FakeResponse.read` and `_UnreadableResponse.read` took the
  optional size with it.
