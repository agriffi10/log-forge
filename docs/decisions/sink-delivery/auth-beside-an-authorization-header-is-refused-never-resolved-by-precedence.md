---
type: Decision
title: "`auth` beside an `Authorization` header is refused, never resolved by precedence"
description: "`auth=` beside an `Authorization` header in any case, matched by the name `urllib` sends (`http._wire_name`), is a `ValueError` at construction from `HTTPSink` and every sink forwarding `auth`, judged before the credential and naming no value; `SplunkHECSink`, which sends its token as that header, refuses any `auth=`. Credentials are copied once, at construction, as exact `str`, and every request sends the `Authorization` value built from the copy then. Do NOT resolve the conflict by letting either side win, or match the header by exact case. A caller header replacing a platform sink's own credential header through `merge_headers` is not covered: `architecture.md` §12 carries it open. (HTTP auth fix, no spec)"
---

**Credentials and an `Authorization` header given together are refused at construction, because
both set the one header a request carries and one of them was always silently dropped.** Until
this fix `HTTPSink(url, auth=..., headers={"Authorization": ...})` constructed and sent one of the
two, and which one depended on the header's case. Spelled exactly `Authorization`, the header was
sent and `auth` dropped, since `_apply_auth` deferred to an exact-case key already in the map. In
any other case `auth` was sent and the header dropped, because `urllib` folds header names and
keeps the value written last. `SplunkHECSink` writes its token as `Authorization` itself, so any
`auth=` it was given was dropped in every case. Each is invariant 13's "no argument is silently
ignored because another was given", and `_valid_auth` and `_apply_auth` had documented the
exact-case precedence as deliberate without the invariant ever recording it as an exception.

**What the code does.** `http._valid_auth` refuses `auth` when any caller header's folded name,
`http._wire_name` (the name `urllib` puts on the wire, shared with `_valid_headers`), is
`Authorization`, with `<Owner> auth and an Authorization header cannot both be given, since each
sets the one Authorization header a request carries; drop auth= or the header`. The wording is the
class attribute `_AUTH_CONFLICT`, on the `_CREDENTIALS` pattern, so `SplunkHECSink`, whose own
token header is what the check sees, says `SplunkHECSink auth cannot be given, since this sink sends
its token as the Authorization header; drop auth=` rather than telling its caller they passed a
header. It is one check in one place: Datadog, Elasticsearch, OpenSearch, Honeycomb, Loki, New
Relic and OTLP forward `auth` and `headers` to the same constructor, and `LogstashSink`'s HTTP mode
reports it under `HTTPSink`'s name, as it does every HTTP-mode refusal it does not make first.
The four platform sinks that merge `headers=` over their own read it with `dict()`, as `HTTPSink`
does, so an `Authorization` in a `MappingProxyType` or a list of pairs meets the check too; an
`isinstance(caller, dict)` test used to drop such headers silently. With nothing a sink holds able
to override the credentials, `_apply_auth` writes the value built at construction
unconditionally; the only map that could still carry an `Authorization` is a per-request
`extra_headers`, which sits beneath the caller's own (SPEC-048 FR-007), and no `extra_headers=` key
in `src/` folds to `Authorization`.

**Why refuse rather than pick a winner.** Either precedence leaves an argument the caller wrote
silently never sent, which is the failure the invariant names, and neither side is the obvious
intent: a caller passing both has made a mistake the library cannot repair by guessing. Andrew
decided on 2026-11-07 that 1.2.0 refuses the pairing, following SPEC-071's refusal of a build option
beside an injected client — the same shape, an argument that constructed and was never read. The
refused configurations were not delivering what they were given, since the overridden side was
never sent, so the break lands only on a program that was already wrong.

**Why the folded name.** Matching the exact string `Authorization` is what let the old precedence
flip with case. HTTP header names are case-insensitive and `urllib` sends one value per folded
name, so the check asks the question the wire answers. No spelling escapes it, because a name
reaching it has passed `_valid_headers`, which refuses a name holding whitespace anywhere — the
spelling (`Authorization `) a lenient server might trim into `Authorization` — so every name that
would put `Authorization` on the wire is one whose folded form equals it. A name that only
contains the word, `X-Authorization`, is not a conflict.

**Why the conflict is judged before the credential.** It reads only whether `auth` was given and
the header names, which `_valid_headers` has already judged, so it cannot echo or chain a
credential (invariant 11), and a caller who must drop one of the two is told so before being told
anything about the value of one they may be about to drop.

**Credentials are built once.** A value that is not a real `str` used to be unpacked by the
construction check and again on every `emit`, so a one-shot iterator such as `iter(("u", "p"))`
constructed and then raised `not enough values to unpack` out of every `emit`, uncounted, and the
`Authorization` value was rebuilt per request, so a `str` subclass's `__format__` ran on every one,
free to return a value the check never saw. A token is now copied as the exact `str` it holds
(`_diag._STR_STR`); `http._copied_pair` performs the old unpack once, keeping its refusals and
reading no more than three items, then copies each half the same way. The `Authorization` value is
built from the copy and checked once, at construction, and `_apply_auth` writes that stored value,
so no caller code runs per request: a list changed afterwards, an object yielding new values each
time it is iterated, or a `str` subclass answering differently later, is frozen at what it held. A
pair outside `tuple[str, str]` is refused by real type (*An argument door asks the type, never the
value*).

**Not covered.** `merge_headers` lays a caller's `headers=` over a platform sink's own, so a caller
header naming the sink's credential header (Splunk's `Authorization`, Datadog's `DD-API-KEY`, New
Relic's `Api-Key`, Honeycomb's `X-Honeycomb-Team`), in any case, replaces the required `token` or
`api_key` silently, and two caller header names folding to one send only the last. That is the
same class of gap, found during this fix's build and left open: `architecture.md` §12 carries it,
with what would close it.

**Rejected:**

- *The caller's header wins.* It was the old exact-case behaviour, and it silently drops `auth`.
- *`auth` wins.* It silently drops the header, and would have made `SplunkHECSink`'s `auth` replace
  its own token.
- *Recording the precedence as an exception to invariant 13.* An exception is for an argument
  whose refusal would break working callers; nothing here was working as written.

**The guard.** `tests/test_sinks_http_auth.py` sweeps every shipped `HTTPSink` subclass, found
transitively and held to the render sweep's recipes, plus `HTTPSink` and `LogstashSink`, for the
refusal in each case, Splunk's own wording, a token the check never reaches, `auth` sent beside a
header that only contains the word, and a one-shot pair sent on every request; a whitespace
spelling of `Authorization` is refused as a header name, before the conflict is reached.
It also scans every `extra_headers=` key in `src/` and fails naming one that folds to
`Authorization` or is not a literal. `tests/test_sinks_http_header_names.py` holds the whitespace
refusal itself, and `tests/test_sinks_http_header_merge.py` the platform sinks' non-`dict`
`headers=`.

## History

### 2026-11-07

- **Settled (HTTP auth fix, no spec).** Closed the two `auth` entries SPEC-071's frame-2 review
  left in `architecture.md` §12, at Andrew's direction for 1.2.0. Written as its own decision
  rather than as a refinement of *A bad argument is floored if it works and refused if it is already
  broken; a new one is refused*, whose fence has no room for it.
