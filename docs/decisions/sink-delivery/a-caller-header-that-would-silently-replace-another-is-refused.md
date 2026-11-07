---
type: Decision
title: "A caller header that would silently replace another is refused"
description: "A caller `headers=` name folding (`http._wire_name`) to a platform sink's own credential header — every key of the base it hands `merge_headers`: Datadog's `DD-API-KEY`, New Relic's `Api-Key`, Honeycomb's `X-Honeycomb-Team`, Splunk's `Authorization` — is a `ValueError` at construction naming the argument it would replace, and two caller names folding to one are refused by `_valid_headers`; neither quotes a value. A non-credential default such as `Content-Type` stays the caller's to override. Do NOT let either side win silently, or hand-list the credential headers: two AST rosters derive them and fail naming a header set by a route they cannot see. (HTTP auth fix, no spec)"
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

**What the code does.** `merge_headers` takes keyword-only `owner=` and `argument=` and refuses a
caller name whose folded form is a key of `base`, with `<Owner> header 'dd-api-key' collides with
api_key, which this sink sends as DD-API-KEY; drop the header`. It runs before `HTTPSink` judges
any header, so no later check can quote one first, and it folds a `str` subclass as the exact text
it stores. `_valid_headers` then refuses the second of two names folding together, `<Owner>
headers 'x-a' and 'X-A' name the same header, which is sent once; drop one`, after the whitespace
refusal, so names differing only by whitespace meet that one instead. A platform sink's own
header beside a caller's spelling of it gets the credential message, never the duplicate one. With
the caller's `Authorization` refused on `SplunkHECSink`, the token is always the `Authorization`
sent, so its `auth=` refusal's wording holds in every case. `SentrySink` takes no `headers=`, so
its `X-Sentry-Auth` cannot be overridden.

**Why refuse.** It is the same call as *`auth` beside an `Authorization` header is refused, never
resolved by precedence*: either winner drops an argument the caller wrote, and a configuration that
was not sending what it was given breaks only a program that was already wrong. Andrew decided on
2026-11-07 that 1.2.0 refuses both gaps.

**What stays overridable.** Only a credential header is protected. `Content-Type` and the
`Content-Encoding` `HTTPSink` writes per request sit beneath the caller's headers (SPEC-009
FR-002), and a caller's `Content-Encoding` beside `gzip=True` suppresses the compression and is
sent as written (SPEC-048 FR-007, now matched by folded name), except that a value naming gzip
compresses the body under the caller's header. That `Content-Encoding` behaviour is recorded here
as it stands, not settled by this decision.

**Derived, not listed.** The credential set is every key of the dict literal each sink hands
`merge_headers`, so a new platform sink is covered by calling it.
`tests/test_sinks_http_header_collisions.py` derives that set from the AST, pins it to the four
platform sinks and refuses a caller's spelling of each, and
`test_no_shipped_sink_sets_a_header_by_a_route_the_rule_does_not_see` fails naming a sink that
sets a header of its own any other way — a non-literal base, a `headers=` not built by
`merge_headers` — since such a header would escape the rule.

**`merge_headers` is public** (`sinks.http.__all__`, held by the typed consumer), so its two new
parameters are keyword-only and defaulted, and a caller written against 1.x still calls it
(invariant 10). Its meaning changed with them: every key of `base` is now a header the caller may
not name, so a subclass outside the library that passes a non-credential default in `base` has
that default protected too.

**Rejected:**

- *The caller wins.* The old behaviour: the required `token` or `api_key` is silently never sent.
- *The sink wins.* The caller's header is silently dropped instead.
- *A hand-written list of credential headers.* It goes stale the day a sink is added; the derived
  set and the route roster cannot.

## History

### 2026-11-07

- **Settled (HTTP auth fix, no spec).** Closed the `architecture.md` §12 entry the fix's own build
  recorded, at Andrew's direction for 1.2.0. Written as its own decision rather than inside the
  `auth` one, since the rule is about any two headers that fold together, not only credentials.
