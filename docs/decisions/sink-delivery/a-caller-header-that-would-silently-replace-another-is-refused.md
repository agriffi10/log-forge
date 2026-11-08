---
type: Decision
title: "A caller header that would silently replace another is refused"
description: "A caller `headers=` name folding (`http._wire_name`) to a platform sink's own credential header — a key of the base it hands `merge_headers` with `argument=` — is a `ValueError` at construction naming the argument it would replace; two caller names folding to one are refused; a caller `Content-Encoding`, read by the bytes it sends, is refused beside `gzip=True` unless it names gzip and beside `gzip=False` unless it is `identity` or empty, and then decides every body. None quotes a value. Without `argument=`, `merge_headers` lets the caller win, as in 1.x. Do NOT let either side win silently on a shipped sink, or hand-list the credential headers: a run-time roster and an AST scan derive them. (HTTP auth fix and the 1.2.0 follow-up fix, no spec; reverses SPEC-048 FR-007)"
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
not a `str` is a `TypeError` naming its type. **A value that is not a `str` is copied once too**
(the 1.2.0 follow-up fix), by `_sent_value`, into what `putheader` would send. An exact `int`,
`bool` or `bytes` is kept as given, since `putheader` runs no caller code for one. Any other value
— an `int` subclass with its own `__str__`, an object with its own `encode` or `__getattribute__`,
a `bytearray` or `memoryview` — goes through `putheader`'s own three branches once, at
construction, and is stored as exact `bytes`. So a caller object's methods run only at
construction, never per `emit`, and a `bytearray` changed afterwards, or an object that would
answer differently later, sends what it held then, as `auth` does. It used to be stored as the
caller's object, its code run inside `putheader` on every request, free to answer differently from
the check. The wire bytes are unchanged: `tests/test_sinks_http_header_values.py` pins, byte for
byte, a raw-socket request head captured at `c4e51fbd` with fifteen value shapes. Nothing that
constructed before is refused: the `putheader` check judges the copy and catches the same two
exceptions, and the `CR`/`LF` check still reads `str(value)` once, so a `bytes` value holding `CRLF`
followed by a space or tab, the obsolete line folding `putheader` admits, is still admitted.
`_valid_headers` then refuses the second of two names folding
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

**`gzip` and a `Content-Encoding` header.** They are two arguments setting one header, so the
same rule settles them: the caller's header must be true of the body the sink sends. SPEC-048
FR-007 let the caller's header win and sent the body uncompressed, which silently ignored
`gzip=True`; Andrew decided on 2026-11-07 to refuse that pairing, and on 2026-11-08 to refuse its
twin, `gzip=False` beside a header naming a coding. `_valid_encoding` runs after `_valid_headers`
and finds the header by folded name (`_caller_encoding`). It reads the value by **the bytes it
sends**, never by its type (`_wire_text`): a `str` as itself, `bytes` decoded as Latin-1, an `int`
or `bool` as `putheader` formats it, so `b"gzip"` names gzip exactly as `"gzip"` does and `5` or
`True` is a coding of that name. A value is read as one token, in any case and with surrounding
spaces and tabs stripped; a list is not read further.

- Beside `gzip=True`, a value naming gzip, `gzip` or `x-gzip` (RFC 9110 §8.4.1.3; `_names_gzip`),
  is no contradiction: the body is compressed and sent under the caller's header. Any other —
  `identity`, `br`, a list such as `gzip, br`, an empty value, `5` — raises `<Owner> gzip=True
  cannot apply beside header 'content-encoding', which names another encoding; drop gzip= or the
  header`.
- Beside `gzip=False`, the default, the sink sends the body as built, so the caller cannot have
  encoded it. A value naming any coding other than `identity` — `gzip`, `br`, any list
  (`identity, identity` included), `5` — raises `<Owner> gzip=False, the default, cannot apply
  beside header 'content-encoding', which names an encoding the body is not in; drop the header,
  or pass gzip=True if it names gzip`. `identity`, and a value empty once stripped (an empty list
  of codings, which names none), are true labels and construct (`_names_no_coding`), as `bytes` or
  as `str`. Until this fix it constructed and sent the caller's label over an uncompressed body on
  every request, which a destination honouring the label cannot decode, so the configuration was
  already broken and the refusal breaks only a program that was already wrong.

Both messages name `gzip` and the caller's spelling of the header, never its value. The check
reaches every class forwarding `gzip`, `LogstashSink`'s HTTP mode under `HTTPSink`'s name;
`SentrySink` takes no `gzip`.

**Once a caller `Content-Encoding` is set, it decides the body on every request.** `gzip` is a
public attribute `_prepare` reads per request, so a sink built with `gzip=True` beside
`Content-Encoding: gzip` and then set `gzip = False` sent an uncompressed body under the gzip label
on every request. `_prepare` now compresses exactly when the caller's header names gzip, read
through the same `_caller_encoding` construction judged, whatever `self.gzip` says; set true beside
an `identity` header, the body is still sent as built. With no caller header `self.gzip` decides,
as it always has, and the sink writes its own `Content-Encoding: gzip`. Construction lets a header
through only when the two agree; this rule keeps them agreeing after it.

**What stays overridable.** Only a credential header is protected from the caller. `Content-Type`
and the `Content-Encoding` `HTTPSink` writes per request sit beneath the caller's headers
(SPEC-009 FR-002).

**Asked of the running code, and of the source.** The credential set is every key of the dict
literal each sink hands `merge_headers` with `argument=`, so a new platform sink is covered by
calling it that way. Two guards hold the rule, each covering what the other cannot.
`tests/test_sinks_http_header_roster.py` is a run-time roster: it builds every shipped `HTTPSink`
subclass and every class that builds an `HTTPSink` (`LogstashSink`, `SentrySink`) with default
options, emits once through a recording opener, takes the headers that first attempt carried
(`Request.header_items()`) minus what `urllib`, the transport and the body's encoding own,
`Content-Type` among them, passes each back as a caller header in lower and upper case and requires
a refusal — `auth=` too, for one folding to `Authorization` — and pins each class's own headers by
name, failing by name on a class it has no builder for. It sees only that one request, so a header a
sink writes only under `gzip=True`, on a retry, on a 413 split or for a non-default option, or a
`Content-Type` the sink writes itself, escapes it. The AST scans in
`tests/test_sinks_http_header_collisions.py` read every code path instead. The route scan reports a
subscript write to, or an `.update` or `.setdefault` call on, any name or attribute holding
`headers` — `self._headers[...]`, `self._http._headers.update(...)`, conditional or in any method —
and a write of a `"headers"` key, as well as a `merge_headers` base that is not a literal and a
`headers=` not built by `merge_headers`; the `extra_headers` scan names any per-request key folding
to `Authorization`. Each scan once missed a route its patterns did not name (`self._headers[...] =
...` after `super().__init__`, an `.update`, a `**{"headers": ...}` splat, an `_attempt` override
calling `add_header`), which is why neither is the only guard: the roster catches what is actually
sent by a route no pattern names, and the scan catches what the one request never exercises.
`merge_headers` itself runs no caller `__hash__` or `__eq__`: it reads both maps by iteration and
rebuilds the merged map by position.

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
  set and the two guards cannot.
- *Protecting every key of `base` for every caller of `merge_headers`.* It refuses a third-party
  sink's working override of its own default, a break in a public function's behaviour.
- *Either guard alone.* The scans missed a new route each round, and the run-time roster sees one
  default request; together each covers the other's blind spot.
- *`gzip=False` beside any `Content-Encoding` sends the caller's header,* as this decision first
  pinned. A header naming `gzip` or `br` over a body the sink did not encode is a label the body is
  not in, on every request — one argument made untrue by another, the failure refused here.
- *Judging a `Content-Encoding` value by its type.* The follow-up fix's first version read every
  value that was not a `str` as naming nothing, which refused `b"identity"` beside `gzip=False` and
  `b"gzip"` beside `gzip=True` though each label was true: a refusal of a configuration that was
  not broken, which SPEC-049 does not allow.
- *`self.gzip` deciding the body after construction beside a caller header.* Setting the public
  attribute could then put a body under a label it is not in, which construction had just refused.

## History

### 2026-11-08

- **Revised (the 1.2.0 follow-up fix, no spec), at Andrew's direction.** `gzip=False`, the
  default, beside a caller `Content-Encoding` naming any coding other than `identity` is refused
  at construction, reversing the case this decision pinned on 2026-11-07, that `gzip=False` beside
  any `Content-Encoding` sends the caller's header over an uncompressed body. `identity`, an empty
  or whitespace value, and no header construct. Two technical refinements landed with it: a value
  is read by the bytes it sends, so `b"identity"` beside `gzip=False` and `b"gzip"` beside
  `gzip=True` construct while `5` and `True` name a coding and are refused by both; and a caller
  `Content-Encoding` decides the body on every request, whatever `gzip` is set to later.
- **Refined (the 1.2.0 follow-up fix, no spec).** A header value that is not a `str` is copied
  once, at construction, into the bytes `putheader` would send (`_sent_value`), so a caller
  object's code runs only at construction; an exact `int`, `bool` or `bytes` is kept as given.
  The wire bytes and the set of refused values are unchanged.
- **Corrected (HTTP auth fix, no spec).** The run-time roster sees one default first-attempt
  request per sink, not every header a sink can send, so the AST route scan, which now names any
  write to a `headers` map, is a guard beside it rather than a supplement. `merge_headers` runs no
  caller `__hash__` or `__eq__`.

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
