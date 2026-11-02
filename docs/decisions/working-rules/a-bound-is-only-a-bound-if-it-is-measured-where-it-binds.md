---
type: Decision
title: "A bound is only a bound if it is measured where it binds"
description: "assert **CPU** time, not wall clock, or a busy-spin passes; a timeout applied per *item* is `n × timeout`, not a bound. (SPEC-038)"
---

**A bound is only a bound if it is measured where it binds** — the recurring shape behind five of this spec's seven blocking defects. A wall-clock assertion cannot see a busy-spin (a slice loop bounded at 1.00 s wall burned 3.5 M calls and a pegged core, so the test asserts **CPU** time); a timeout applied per *item* is `n × timeout`, not a bound; and a chunk size is a floor division, so a byte-charging test only bites at sizes where the division tips — the same test was vacuous at 1,012 bytes (the count limit binds first) and again at 9,000, and now asserts its own sensitivity as a precondition. (SPEC-038 FR-004, FR-005)

## History

### 2026-08-10

- **Settled.**
