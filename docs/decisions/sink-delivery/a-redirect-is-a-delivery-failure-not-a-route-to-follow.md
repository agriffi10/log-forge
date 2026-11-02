---
type: Decision
title: "A redirect is a delivery failure, not a route to follow"
description: "`urlopen`'s default opener rewrote a redirected `POST` into a body-less `GET`, losing every batch and forwarding the bearer token to a host the caller never configured, while the redirect target's `200` read as delivery. (SPEC-048)"
---

**A redirect is a delivery failure, not a route to follow** — `urlopen`'s default opener follows `301`/`302`/`303` on a `POST` by rewriting the method to `GET` and **dropping the body while keeping every header**, so an `http://` collector behind a load balancer redirecting to `https://` lost every batch, forwarded the `Authorization` header to a host the caller never configured, and read the redirect target's `200` as delivery — `losses()` at zero, forever. Every sink in the family now opens through a private `build_opener` whose `redirect_request` returns `None`, so a `3xx` arrives as an `HTTPError` and takes the existing counted-and-announced `_abandon` path. **Refused, not followed correctly**: re-issuing the `POST` against the `Location` would make the library decide that an unverified host may have the batch and the credential, which is the caller's decision and not a logging library's. One site sets the opener and one calls it, so six subclasses, `LogstashSink`'s HTTP backend and `SentrySink`'s fallback are covered without a line of their own; an injected `opener=` is the caller's object and is used as given. **`307` and `308` were already refused** for a `POST` by the stdlib — the audit and the spec's first draft both said all five were followed, and the two extra parameters in the test are labelled regression pins rather than evidence, because an acceptance criterion that passes against the unfixed code proves nothing. The opener is built **per sink at construction**, not once at import: `python.md` forbids a module doing real work at import time, and `build_opener` snapshots `ProxyHandler`'s environment, so an import-time opener would pin whatever `http_proxy` said when `log_foundry` was first imported and silently ignore a proxy the application set afterwards — where `urlopen`'s lazily-built global opener would not. A sink is built once, so the cost is one object per sink and never per request. (SPEC-048 FR-001)

## History

### 2026-09-02

- **Settled.**
