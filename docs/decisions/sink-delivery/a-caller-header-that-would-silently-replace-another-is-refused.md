---
type: Decision
title: "A caller header that would silently replace another is refused"
description: "A caller `headers=` name folding (`http._wire_name`) to a platform sink's own credential header — every key of the base it hands `merge_headers` with `argument=` — is a `ValueError` at construction naming the argument it would replace; two caller names folding to one are refused by `_valid_headers`; `gzip=True` beside a caller `Content-Encoding` naming another coding is refused. None quotes a value. Without `argument=`, `merge_headers` lets the caller win, as in 1.x. Do NOT let either side win silently on a shipped sink, or hand-list the credential headers: a run-time roster replays every header each sink sends and requires a refusal. (HTTP auth fix, no spec; reverses SPEC-048 FR-007)"
---

**A caller header is refused at construction when it would silently replace another header,
because `urllib` sends one value per folded name, the last written, and the replaced one was
never sent.** Two gaps had that shape. A platform sink laid the caller's `headers=` over its own
credential header in `merge_headers`, so `DatadogSink(api_key=k, headers={"dd-api-key": x})`
constructed and sent `x`, never `api_key`; Splunk's `Authorization: Splunk <token>`, New Relic's
`Api-Key` and Honeycomb's `X-Honeycomb-Team` did the same, in their own, lower and upper case,
reproduced at `7845f66f`. And two caller names folding to one, `{"x-a": "1", "X-A": "2"}` or
`authorization` beside `Authorization`, sent only the last, reproduced at `a8d87119`. Each is
invariant 13's "no argument is silently ignored because another was given"; `merge_headers`
documented "caller winning" without the invariant ever recording an exception.

**What the code does.** `merge_headers` takes keyword-only `owner=` and `argument=`, and only a call
passing `argument=` — each of the four platform sinks, naming `api_key` or `token` — refuses a
caller name whose folded form is a key of `base`, with `<Owner> header 'dd-api-key' collides with
api_key, which this sink sends as DD-API-KEY; drop the header`. It runs before `HTTPSink` judges any
header, so no later check can quote one first, and it folds both sides as the exact text they store.
`_valid_headers` copies every header name and `str` value as the exact `str` it stores before any
check and stores the copies, so a `str` subclass's `capitalize` cannot fold one way at construction
and another on the wire, and a refusal prints the stored text, never a `__repr__`; a name that is
not a `str` is a `TypeError` naming its type. It then refuses the second of two names folding
together, `<Owner> headers 'x-a' and 'X-A' name the same header, which is sent once; drop one`,
after the whitespace refusal, so names differing only by whitespace meet that one instead. A
platform sink's own header beside a caller's spelling of it gets the credential message, never the
duplicate one. With the caller's `Authorization` refused on `SplunkHECSink`, the token is always the
`Authorization` sent, so its `auth=` refusal's wording holds in every case. `SentrySink` takes no
`headers=`, so its `X-Sentry-Auth` cannot be overridden.

**Why refuse.** It is the same call as *`auth` beside an `Authorization` header is refused, never
resolved by precedence*: either winner drops an argument the caller wrote, and a configuration that
was not sending what it was given breaks only a program that was already wrong. Andrew decided on
2026-11-07 that 1.2.0 refuses both gaps.

**`gzip=True` and a `Content-Encoding` header.** They are two arguments setting one header, so
the same rule settles them. SPEC-048 FR-007 let the caller's header win and sent the body
uncompressed, which silently ignored `gzip=True`; Andrew decided on 2026-11-07 to refuse the
pairing instead. `_valid_encoding` runs after `_valid_headers` and finds the header by folded name
(`_caller_encoding`), so `gzip=True` beside a `Content-Encoding` in any case whose value does not
name gzip — `identity`, `br`, a list such as `gzip, br`, the empty string, a value that is not a
`str` — raises `<Owner> gzip=True cannot apply beside header 'content-encoding', which names
another encoding; drop gzip= or the header`, naming the caller's spelling and never the value.
A value naming gzip, `gzip` or `x-gzip` (RFC 9110 §8.4.1.3), in any case and with surrounding
spaces and tabs stripped (`_names_gzip`),
is no contradiction: the body is compressed and sent under the caller's header. `gzip=False`
beside any `Content-Encoding` sends the caller's header over an uncompressed body. The check
reaches every class forwarding `gzip`, `LogstashSink`'s HTTP mode under `HTTPSink`'s name;
`SentrySink` takes no `gzip`. `_prepare` keeps its per-request suppression for a non-gzip value,
read through the same `_caller_encoding`, because `gzip` is a public attribute: set true after
construction beside an `identity` header, the body is still sent uncompressed rather than
compressed under a label that says otherwise.

**What stays overridable.** Only a credential header is protected from the caller. `Content-Type`
and the `Content-Encoding` `HTTPSink` writes per request sit beneath the caller's headers
(SPEC-009 FR-002).

**Asked of the running code, not of the source.** The credential set is every key of the dict
literal each sink hands `merge_headers` with `argument=`, so a new platform sink is covered by
calling it that way. The guard is `tests/test_sinks_http_header_roster.py`, a run-time roster: it
builds every shipped `HTTPSink` subclass and every class that builds an `HTTPSink` (`LogstashSink`,
`SentrySink`), emits once through a recording opener, takes every header the request carried
(`Request.header_items()`) minus what `urllib` and the transport own, passes each back as a caller
header in lower and upper case and requires a refusal — `auth=` too, for one folding to
`Authorization` — and pins each class's own headers by name, failing by name on a class it has no
builder for. The two AST scans in `tests/test_sinks_http_header_collisions.py`, the route roster
and the `extra_headers` scan, are kept as supplementary: they read every code path where the
run-time roster sees only what one `emit` exercises, but they were blind to routes their patterns
do not name — `self._headers[...] = ...` after `super().__init__`, an `.update`, a
`**{"headers": ...}` splat, an `_attempt` override calling `add_header` — which the run-time
roster catches. The same class of miss recurred across two scans, which is why the guard moved to
the running code rather than gaining another pattern.

**`merge_headers` is public** (`sinks.http.__all__`, held by the typed consumer), so the refusal is
opt-in and its two new parameters are keyword-only and defaulted (invariant 10). Without `argument=`
the caller wins, as in 1.x: a sink outside the library may hand `merge_headers` a default its caller
is meant to override, and refusing that would break a working program. A caller name folding to a
`base` key replaces it in place, spelling and value (`_respell_overridden`), so the merged map holds
one spelling, meets no duplicate refusal, and the request on the wire is byte for byte what 1.x
sent; the returned map differs only for a different-case override, which now holds one key where it
held two. A caller's own two spellings of one name are still refused by `_valid_headers`. Making
every `base` key protected, as the first version of this fix did, broke exactly such a sink, with
nobody's approval, and was reverted as a technical correction.

**Rejected:**

- *The caller wins, on a shipped sink.* The old behaviour: the required `token` or `api_key` is
  silently never sent.
- *The sink wins.* The caller's header is silently dropped instead.
- *Keeping SPEC-048 FR-007's header-wins for `Content-Encoding`.* It silently ignores `gzip=True`,
  the failure this decision refuses everywhere else.
- *A hand-written list of credential headers.* It goes stale the day a sink is added; the derived
  set and the run-time roster cannot.
- *Protecting every key of `base` for every caller of `merge_headers`.* It refuses a third-party
  sink's working override of its own default, a break in a public function's behaviour.
- *Another AST pattern for each route found.* The scans missed a new route each round; the
  run-time roster reads what is actually sent.

## History

### 2026-11-07

- **Refined (HTTP auth fix, no spec).** The credential refusal is opt-in through `argument=`, and
  without it the caller wins as in 1.x, replacing the default in place, a technical correction
  keeping `merge_headers`' public behaviour (invariant 10); the guard is now a run-time roster,
  with the AST scans supplementary. Header names and `str` values are copied as exact `str`
  before any check, and a non-`str` name is a `TypeError` naming its type. `x-gzip` names gzip.
- **Refined (HTTP auth fix, no spec).** `gzip=True` beside a caller `Content-Encoding` naming
  another coding is refused at construction, Andrew's call, reversing SPEC-048 FR-007, which let
  the header win and sent the body uncompressed. A value naming gzip is compressed under the
  caller's header. Superseded markers stand in SPEC-048's spec and delivery doc.
- **Settled (HTTP auth fix, no spec).** Closed the `architecture.md` §12 entry the fix's own build
  recorded, at Andrew's direction for 1.2.0. Written as its own decision rather than inside the
  `auth` one, since the rule is about any two headers that fold together, not only credentials.
